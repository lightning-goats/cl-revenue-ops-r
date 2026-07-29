//! Tests for `revops::fee_execution` -- the guarded CLN fee broadcaster
//! (stateful-shadow revision plan, Task 9). This is the ONLY component
//! that may ever send a live `setchannel` to CLN; every test here proves
//! either (a) exactly one such call happens on a fully-authorized batch,
//! or (b) some denial/failure path sends ZERO calls, or (c) an ambiguous
//! transport outcome persistently quarantines execution across a
//! restart, or (d) a failure to persist that quarantine still fails
//! closed (fix round 1, review finding 1).
//!
//! A fake CLN Unix-socket JSON-RPC server (below) stands in for
//! `lightning-rpc` -- it never resolves to a real lightningd socket path
//! (always a fresh tempdir path), mirroring the same test-only pattern
//! `tests/hydration.rs` and `tests/fee_evidence.rs` already use for
//! read-only RPC.

use revops::cutover_arm::{self, validate_and_consume, RunningIdentity};
use revops::fee_execution::{
    BroadcastError, ClnFeeBroadcaster, LiveBatchAuthorization, LiveBatchDenyReason,
    PersistedFeeRequest,
};
use revops::fee_mode::{validate_fee_mode, ModeFlags, ValidatedFeeMode};
use revops::python_authority::{OpenBracket, PythonAuthorityClient, PythonAuthorityDenyReason};
use revops_db::fee_runway::{BroadcastAttemptIntent, FeeStateSnapshot, QuarantineEntry};
use revops_db::owner::{spawn_read_write, ObserverHandle};
use revops_fees::execution::SetChannelRequest;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

const NODE_ID: &str = "lnnode";
const SOURCE_COMMIT: &str = "7d8e79ec307fd10bd1a775a236148a642a0a506f";
const BINARY_SHA256: &str = "ff648376758b9a97de7642adbf1c258494744c54e33c31a712dcc8c742d1428c";

// ---------------------------------------------------------------------------
// fixtures: a genuine, consumed LiveSessionArm/LiveMode (mirrors
// tests/fee_mode.rs's own helper -- the only way to obtain one).
// ---------------------------------------------------------------------------

fn matching_identity(owner_uid: u32) -> RunningIdentity {
    RunningIdentity {
        node_id: NODE_ID.to_string(),
        subsystem: cutover_arm::CUTOVER_SUBSYSTEM_FEES.to_string(),
        source_commit: SOURCE_COMMIT.to_string(),
        binary_sha256: BINARY_SHA256.to_string(),
        owner_uid,
        now: 1_000_000,
    }
}

fn valid_arm_json(nonce: &str) -> String {
    format!(
        r#"{{
            "schema": "{schema}",
            "node_id": "{node}",
            "subsystem": "{subsystem}",
            "source_commit": "{commit}",
            "binary_sha256": "{hash}",
            "not_before": 999900,
            "expires_at": 1000100,
            "nonce": "{nonce}"
        }}"#,
        schema = cutover_arm::CUTOVER_ARM_SCHEMA,
        node = NODE_ID,
        subsystem = cutover_arm::CUTOVER_SUBSYSTEM_FEES,
        commit = SOURCE_COMMIT,
        hash = BINARY_SHA256,
        nonce = nonce,
    )
}

fn write_arm(dir: &Path, name: &str, json: &str) -> PathBuf {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let path = dir.join(name);
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .expect("create arm file")
        .write_all(json.as_bytes())
        .expect("write arm json");
    path
}

/// A seeded (non-virgin, seed-event-recorded) state snapshot -- coordinator
/// ruling I-6: live authority may only be constructed over a store that is
/// already seeded, never a virgin one.
fn seeded_state() -> FeeStateSnapshot {
    FeeStateSnapshot {
        generation: 1,
        rows: vec![],
    }
}

fn real_live_mode(tmp: &Path, nonce: &str) -> revops::fee_mode::LiveMode {
    use std::os::unix::fs::MetadataExt;

    let arm_path = write_arm(tmp, &format!("{nonce}.json"), &valid_arm_json(nonce));
    let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();
    let consumed_dir = tmp.join("consumed");
    let identity = matching_identity(owner_uid);
    let arm =
        validate_and_consume(&arm_path, &consumed_dir, &identity).expect("valid arm consumes");

    let flags = ModeFlags {
        observer: false,
        fee_dryrun: false,
        fee_broadcast: true,
        fee_stateful_shadow: false,
    };
    match validate_fee_mode(
        flags,
        Some(arm),
        &seeded_state(),
        &revops_db::fee_runway::SeedBindingState::VerifiedBound {
            cycle_id: "live-fixture-seed-cycle".to_string(),
        },
    ) {
        Ok(ValidatedFeeMode::LiveAuthority(live)) => live,
        other => panic!("expected LiveAuthority, got {other:?}"),
    }
}

async fn observer(dir: &Path) -> ObserverHandle {
    spawn_read_write(&dir.join("observer.db"))
        .await
        .expect("spawn observer db")
}

/// Build a broadcaster over a fresh arm, asserting reconciliation
/// succeeded (no orphaned intents in a freshly-created store).
async fn broadcaster(socket_path: PathBuf, store: ObserverHandle, tmp: &Path) -> ClnFeeBroadcaster {
    let live_mode = real_live_mode(tmp, &format!("nonce-{}", uuid_ish()));
    ClnFeeBroadcaster::new(socket_path, store, 5, live_mode)
        .await
        .expect("no orphaned intents to reconcile in a fresh store")
}

/// A cheap, collision-avoiding-enough nonce generator for tests that need
/// more than one consumed arm per process (the cutover arm's nonce must
/// be filesystem-unique within `consumed_dir`).
fn uuid_ish() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering as AtoOrdering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, AtoOrdering::SeqCst)
}

// ---------------------------------------------------------------------------
// fake CLN Unix-socket JSON-RPC server. Never resolves to a real
// lightning-rpc path (always a fresh tempdir socket).
// ---------------------------------------------------------------------------

#[derive(Clone)]
enum FakeBehavior {
    Success(Value),
    Rejected {
        code: i64,
        message: String,
    },
    /// Reads the full request, then closes the connection without ever
    /// writing a response -- the "disconnect after full request receipt"
    /// scenario: bytes were received, the true outcome is unknown.
    DisconnectAfterReceipt,
    /// Writes back a syntactically valid JSON object that is neither a
    /// `result` nor an `error` response -- undecodable as a CLN reply.
    Malformed,
    /// Accepts and reads the request, then never writes anything and
    /// never closes -- exercises the broadcaster's own timeout budget.
    HangForever,
    /// Task 59 F4: park until `gate` flips, then answer with `result` --
    /// lets a test wedge the store BETWEEN the intent write and the
    /// terminal result write, deterministically.
    GatedSuccess(Value, Arc<AtomicBool>),
    /// Task 59 F4: park until `gate` flips, then reject -- same wedge
    /// window as [`Self::GatedSuccess`] for the explicit-rejection arm.
    GatedRejected {
        code: i64,
        message: String,
        gate: Arc<AtomicBool>,
    },
}

struct FakeClnServer {
    _dir: tempfile::TempDir,
    path: PathBuf,
    connections: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Value>>>,
}

impl FakeClnServer {
    /// Bind a fresh socket and serve each accepted connection with the
    /// next behavior off `behaviors` (in order). A connection past the
    /// end of the queue gets [`FakeBehavior::Malformed`] -- loud enough to
    /// fail any test that (incorrectly) makes more calls than it queued
    /// behaviors for, without ever panicking the server task itself.
    fn spawn(behaviors: Vec<FakeBehavior>) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("lightning-rpc");
        let listener = UnixListener::bind(&path).expect("bind fake cln socket");
        let connections = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));
        let behaviors = Arc::new(Mutex::new(behaviors.into_iter()));

        let connections_task = connections.clone();
        let received_task = received.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                connections_task.fetch_add(1, Ordering::SeqCst);
                let received_task = received_task.clone();
                let behavior = behaviors
                    .lock()
                    .unwrap()
                    .next()
                    .unwrap_or(FakeBehavior::Malformed);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 8192];
                    loop {
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return; // peer closed before a full request arrived
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Ok(v) = serde_json::from_slice::<Value>(&buf) {
                            received_task.lock().unwrap().push(v);
                            break;
                        }
                    }
                    match behavior {
                        FakeBehavior::Success(result) => {
                            let body = json!({"jsonrpc": "2.0", "id": 1, "result": result});
                            let mut out = serde_json::to_vec(&body).unwrap();
                            out.extend_from_slice(b"\n\n");
                            let _ = stream.write_all(&out).await;
                        }
                        FakeBehavior::Rejected { code, message } => {
                            let body = json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "error": {"code": code, "message": message},
                            });
                            let mut out = serde_json::to_vec(&body).unwrap();
                            out.extend_from_slice(b"\n\n");
                            let _ = stream.write_all(&out).await;
                        }
                        FakeBehavior::DisconnectAfterReceipt => {
                            drop(stream);
                        }
                        FakeBehavior::Malformed => {
                            let body = json!({"jsonrpc": "2.0", "id": 1, "unexpected": true});
                            let mut out = serde_json::to_vec(&body).unwrap();
                            out.extend_from_slice(b"\n\n");
                            let _ = stream.write_all(&out).await;
                        }
                        FakeBehavior::HangForever => {
                            std::future::pending::<()>().await;
                        }
                        FakeBehavior::GatedSuccess(result, gate) => {
                            while !gate.load(Ordering::SeqCst) {
                                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                            }
                            let body = json!({"jsonrpc": "2.0", "id": 1, "result": result});
                            let mut out = serde_json::to_vec(&body).unwrap();
                            out.extend_from_slice(b"\n\n");
                            let _ = stream.write_all(&out).await;
                        }
                        FakeBehavior::GatedRejected {
                            code,
                            message,
                            gate,
                        } => {
                            while !gate.load(Ordering::SeqCst) {
                                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                            }
                            let body = json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "error": {"code": code, "message": message},
                            });
                            let mut out = serde_json::to_vec(&body).unwrap();
                            out.extend_from_slice(b"\n\n");
                            let _ = stream.write_all(&out).await;
                        }
                    }
                });
            }
        });

        FakeClnServer {
            _dir: dir,
            path,
            connections,
            received,
        }
    }

    fn socket_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn received_methods(&self) -> Vec<String> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .map(|v| v["method"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    fn received_params(&self) -> Vec<Value> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .map(|v| v["params"].clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// fixtures: requests + authorizations
// ---------------------------------------------------------------------------

fn one_request() -> PersistedFeeRequest {
    PersistedFeeRequest {
        cycle_id: Some("live-cycle-1".to_string()),
        channel_id: "1x1x0".to_string(),
        request_id: "req-1".to_string(),
        params: SetChannelRequest {
            id: "1x1x0".to_string(),
            feebase: 0,
            feeppm: 150,
            htlcmin: Some(1000),
            htlcmax: None,
        },
    }
}

/// Task 59 F5: a fresh two-behavior fake authority endpoint plus an OPEN
/// bracket over it (one fetch consumed). The server must outlive the
/// authorize call -- the bracket's close re-reads it.
async fn open_bracket_ok() -> (FakeClnServer, OpenBracket) {
    let authority = FakeClnServer::spawn(vec![
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 2)),
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 1)),
    ]);
    let client = PythonAuthorityClient::new(authority.socket_path(), 5);
    let bracket = client
        .open_bracket(AUTHORITY_NOW, AUTHORITY_MAX_AGE)
        .await
        .expect("open bracket");
    (authority, bracket)
}

/// Fix round 1 (review finding 4): `authorize` now reads the
/// quarantine-empty observation and the current state generation
/// directly from `store` -- a fresh (never-committed) store's generation
/// is always `0`, so every fixture below authorizes against `0` unless a
/// test deliberately wants a mismatch.
async fn authorize_ok(store: &ObserverHandle) -> LiveBatchAuthorization {
    let (_authority, bracket) = open_bracket_ok().await;
    LiveBatchAuthorization::authorize(
        store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect("fully authorized batch")
}

// ---------------------------------------------------------------------------
// Step 2: exactly one setchannel call with the typed payload
// ---------------------------------------------------------------------------

#[tokio::test]
async fn valid_live_mode_sends_exactly_one_setchannel_call_with_typed_payload() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::Success(json!({"channels": []}))]);
    let store = observer(tmp.path()).await;
    let broadcaster = broadcaster(server.socket_path(), store.clone(), tmp.path()).await;

    let receipt = broadcaster
        .broadcast_batch(authorize_ok(&store).await, &[one_request()])
        .await
        .expect("authorized single-request batch succeeds");

    assert_eq!(receipt.outcomes.len(), 1);
    assert_eq!(server.connection_count(), 1, "exactly one connection");
    assert_eq!(server.received_methods(), vec!["setchannel".to_string()]);
    assert_eq!(
        server.received_params(),
        vec![json!({"id": "1x1x0", "feebase": 0, "feeppm": 150, "htlcmin": 1000})]
    );
}

// ---------------------------------------------------------------------------
// Step 2: denial paths send zero calls
// ---------------------------------------------------------------------------

#[tokio::test]
async fn quarantine_active_denies_authorization_construction_with_zero_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![]);
    let store = observer(tmp.path()).await;
    store
        .insert_execution_quarantine(QuarantineEntry {
            reason: "ambiguous post-submission transport outcome".to_string(),
            cycle_id: None,
            channel_id: Some("1x1x0".to_string()),
            request_id: Some("req-0".to_string()),
            entered_at: 1_800_000_000,
        })
        .await
        .unwrap();

    let (_authority, bracket) = open_bracket_ok().await;
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect_err("active quarantine must deny authorization");
    assert!(matches!(err, LiveBatchDenyReason::QuarantineActive { .. }));
    assert_eq!(server.connection_count(), 0);
}

#[tokio::test]
async fn stale_state_generation_denies_authorization_with_zero_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![]);
    let store = observer(tmp.path()).await;
    let (_authority, bracket) = open_bracket_ok().await;
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        6, // candidate was built against generation 6; the store is still 0
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect_err("stale state generation must deny authorization");
    assert!(matches!(
        err,
        LiveBatchDenyReason::StateGenerationStale {
            authorized_against: 6,
            current: 0,
        }
    ));
    assert_eq!(server.connection_count(), 0);
}

#[tokio::test]
async fn unstable_python_authority_epoch_denies_authorization_with_zero_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![]);
    let store = observer(tmp.path()).await;
    // Python's authority state moved during the bracket window: the
    // close's second fetch observes a different epoch.
    let authority = FakeClnServer::spawn(vec![
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 2)),
        FakeBehavior::Success(json!({
            "enabled": false,
            "generation": 4,
            "transitioned_at": 1_799_500_000,
            "observed_at": AUTHORITY_NOW - 1,
        })),
    ]);
    let client = PythonAuthorityClient::new(authority.socket_path(), 5);
    let bracket = client
        .open_bracket(AUTHORITY_NOW, AUTHORITY_MAX_AGE)
        .await
        .expect("open bracket");
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect_err("unstable epoch must deny authorization");
    assert!(matches!(err, LiveBatchDenyReason::PythonAuthority(_)));
    assert_eq!(server.connection_count(), 0);
}

#[tokio::test]
async fn governor_denial_denies_authorization_with_zero_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![]);
    let store = observer(tmp.path()).await;
    let (_authority, bracket) = open_bracket_ok().await;
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        false,
        "paused",
        "idem-1",
    )
    .await
    .expect_err("governor denial must deny authorization");
    assert!(matches!(
        err,
        LiveBatchDenyReason::GovernorDenied { reason_code } if reason_code == "paused"
    ));
    assert_eq!(server.connection_count(), 0);
}

#[tokio::test]
async fn missing_ledger_reservation_denies_authorization_with_zero_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![]);
    let store = observer(tmp.path()).await;
    let (_authority, bracket) = open_bracket_ok().await;
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "", // no reservation id
    )
    .await
    .expect_err("missing ledger reservation must deny authorization");
    assert!(matches!(err, LiveBatchDenyReason::LedgerReservationMissing));
    assert_eq!(server.connection_count(), 0);
}

/// Fix round 2 (re-review): `authorize`'s own quarantine READ can fail
/// (a store error, not "quarantine present/absent") -- this must deny
/// closed (`QuarantineCheckFailed`), never silently treat a failed read
/// as "no active quarantine". Sabotages ONLY the read by DROPPING
/// `rust_execution_quarantine` via a second raw connection to the same
/// observer db file: a `BEFORE INSERT` trigger (used elsewhere in this
/// file to fail an INSERT) would never fire for a `SELECT`, so dropping
/// the table is the seam that actually breaks this specific read.
#[tokio::test]
async fn quarantine_check_failure_denies_authorization_with_zero_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("observer.db");
    let server = FakeClnServer::spawn(vec![]);
    let store = observer(tmp.path()).await;

    {
        let raw = rusqlite::Connection::open(&db_path).expect("open raw connection to sabotage");
        raw.execute_batch("DROP TABLE rust_execution_quarantine;")
            .expect("drop the quarantine table out from under the actor");
    }

    let (_authority, bracket) = open_bracket_ok().await;
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect_err("a failed quarantine read must deny authorization, never succeed or panic");
    assert!(
        matches!(err, LiveBatchDenyReason::QuarantineCheckFailed(_)),
        "expected QuarantineCheckFailed, got {err:?}"
    );
    assert_eq!(server.connection_count(), 0);
}

/// Fix round 2 (re-review): the sibling of the test above for the
/// state-generation READ inside `authorize` -- a failed read must deny
/// closed (`StateGenerationCheckFailed`), never be treated as "matches
/// the candidate". Sabotages ONLY `load_latest_fee_state`'s read by
/// dropping `rust_fee_state_generation` (the first table it queries);
/// `rust_execution_quarantine` is left untouched, so this test isolates
/// the state-generation check specifically (the quarantine check ahead
/// of it in `authorize`'s check order still passes normally).
#[tokio::test]
async fn state_generation_check_failure_denies_authorization_with_zero_calls() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("observer.db");
    let server = FakeClnServer::spawn(vec![]);
    let store = observer(tmp.path()).await;

    {
        let raw = rusqlite::Connection::open(&db_path).expect("open raw connection to sabotage");
        raw.execute_batch("DROP TABLE rust_fee_state_generation;")
            .expect("drop the fee state generation table out from under the actor");
    }

    let (_authority, bracket) = open_bracket_ok().await;
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect_err("a failed state-generation read must deny authorization, never succeed or panic");
    assert!(
        matches!(err, LiveBatchDenyReason::StateGenerationCheckFailed(_)),
        "expected StateGenerationCheckFailed, got {err:?}"
    );
    assert_eq!(server.connection_count(), 0);
}

/// Fix round 1, review finding 3: the store-backed quarantine re-check
/// INSIDE `broadcast_batch` is the ONLY store-backed quarantine
/// enforcement actually on the send path -- `authorization` was minted
/// legitimately (the store showed no active quarantine at that moment),
/// but a quarantine landed afterward (e.g. a concurrent caller observing
/// an ambiguous outcome). Deleting `broadcast_batch`'s re-check would
/// leave every OTHER test in this file green; this is the one that would
/// catch it.
#[tokio::test]
async fn broadcast_batch_denies_on_store_backed_quarantine_even_when_authorization_predates_it() {
    let tmp = tempfile::tempdir().unwrap();
    // A `Success` behavior queued but never reached -- if this fires, the
    // re-check failed to stop the call.
    let server = FakeClnServer::spawn(vec![FakeBehavior::Success(json!({}))]);
    let store = observer(tmp.path()).await;
    let broadcaster = broadcaster(server.socket_path(), store.clone(), tmp.path()).await;

    let authorization = authorize_ok(&store).await;

    store
        .insert_execution_quarantine(QuarantineEntry {
            reason: "race: inserted after authorization was minted".to_string(),
            cycle_id: None,
            channel_id: Some("1x1x0".to_string()),
            request_id: Some("req-race".to_string()),
            entered_at: 1_800_000_000,
        })
        .await
        .unwrap();

    let err = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect_err("the store-backed re-check must still deny the batch");
    assert!(matches!(err, BroadcastError::Quarantined));
    assert_eq!(
        server.connection_count(),
        0,
        "the re-check must happen BEFORE any RPC call"
    );
}

// ---------------------------------------------------------------------------
// Step 2: explicit CLN rejection is a reconciled failure row (no quarantine)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn explicit_rejection_is_reconciled_as_a_failure_row_without_quarantine() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::Rejected {
        code: 301,
        message: "Rate exceeds maximum".to_string(),
    }]);
    let store = observer(tmp.path()).await;
    let broadcaster = broadcaster(server.socket_path(), store.clone(), tmp.path()).await;

    let err = broadcaster
        .broadcast_batch(authorize_ok(&store).await, &[one_request()])
        .await
        .expect_err("explicit rejection must fail the batch");
    assert!(matches!(err, BroadcastError::Rejected { .. }));
    assert_eq!(server.connection_count(), 1);
    assert!(
        store.active_execution_quarantine().await.unwrap().is_none(),
        "an explicit rejection must NEVER quarantine execution"
    );
}

// ---------------------------------------------------------------------------
// Step 2: disconnect/timeout after submission persistently quarantines and
// blocks the NEXT batch.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disconnect_after_submission_quarantines_and_blocks_next_batch() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::DisconnectAfterReceipt]);
    let store = observer(tmp.path()).await;
    let broadcaster = broadcaster(server.socket_path(), store.clone(), tmp.path()).await;

    let err = broadcaster
        .broadcast_batch(authorize_ok(&store).await, &[one_request()])
        .await
        .expect_err("disconnect after receipt must be ambiguous");
    assert!(matches!(err, BroadcastError::Ambiguous { .. }));
    assert_eq!(server.connection_count(), 1, "exactly one attempt was made");

    store
        .active_execution_quarantine()
        .await
        .unwrap()
        .expect("ambiguous transport outcome must quarantine execution");

    // The NEXT batch is blocked: constructing a fresh authorization must
    // now deny on the active quarantine (read straight from the store),
    // with zero further calls.
    let (_authority, bracket) = open_bracket_ok().await;
    let deny = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-def",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-2",
    )
    .await
    .expect_err("the next batch must be denied while quarantine is active");
    assert!(matches!(deny, LiveBatchDenyReason::QuarantineActive { .. }));
    assert_eq!(
        server.connection_count(),
        1,
        "the blocked next batch must never reach the fake server"
    );
}

/// Same ambiguity classification, exercised via a genuine wall-clock
/// timeout (the fake server accepts, reads the full request, then never
/// responds and never closes) rather than an explicit disconnect --
/// proves the broadcaster's own timeout budget also lands on Ambiguous,
/// never a silent hang.
#[tokio::test]
async fn hang_after_submission_times_out_as_ambiguous_and_quarantines() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::HangForever]);
    let store = observer(tmp.path()).await;
    // Short timeout budget so the test doesn't wait out a production budget.
    let live_mode = real_live_mode(tmp.path(), "nonce-hang");
    let broadcaster = ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 1, live_mode)
        .await
        .expect("no orphaned intents to reconcile");

    let err = broadcaster
        .broadcast_batch(authorize_ok(&store).await, &[one_request()])
        .await
        .expect_err("a hang past the timeout budget must be ambiguous");
    assert!(matches!(err, BroadcastError::Ambiguous { .. }));
    assert!(store.active_execution_quarantine().await.unwrap().is_some());
}

/// Fix round 1 (review finding 1, CRITICAL): if the quarantine insert
/// ITSELF fails after an ambiguous outcome, the prior code still recorded
/// the intent row as `outcome = 'ambiguous'` (never reconciled again on
/// restart) and had no in-memory signal either -- fail-open both ways.
/// This test sabotages ONLY the quarantine INSERT path (a `BEFORE INSERT`
/// trigger installed via a second raw connection to the SAME observer db
/// file, so no prod-only test hook is needed and every plain `SELECT`
/// -- including this broadcaster's own re-checks -- keeps working
/// normally) and proves: the intent row is left unresolved, and the
/// broadcaster poisons itself in memory, refusing every further batch
/// with zero additional RPC calls.
#[tokio::test]
async fn ambiguous_outcome_with_failed_quarantine_insert_fails_closed_in_process() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("observer.db");
    let server = FakeClnServer::spawn(vec![FakeBehavior::DisconnectAfterReceipt]);
    let store = observer(tmp.path()).await;
    let broadcaster = broadcaster(server.socket_path(), store.clone(), tmp.path()).await;

    // Sabotage ONLY the quarantine table's INSERT path. A plain SELECT
    // (every quarantine READ this module performs) is completely
    // unaffected -- this is deliberately NOT "drop the table" (which
    // would also break the SELECT-based re-checks and misrepresent what
    // this test is proving).
    {
        let raw = rusqlite::Connection::open(&db_path).expect("open raw connection to sabotage");
        raw.execute_batch(
            "CREATE TRIGGER quarantine_insert_fails
                 BEFORE INSERT ON rust_execution_quarantine
             BEGIN
                 SELECT RAISE(ABORT, 'test-induced quarantine insert failure');
             END;",
        )
        .expect("install failing trigger");
    }

    let err = broadcaster
        .broadcast_batch(authorize_ok(&store).await, &[one_request()])
        .await
        .expect_err("an ambiguous outcome must still fail the batch");
    assert!(matches!(err, BroadcastError::Ambiguous { .. }));
    let message = err.to_string();
    assert!(
        message.contains("quarantine insert ALSO failed"),
        "the error must surface the quarantine-insert failure: {message}"
    );

    // The store itself shows NO active quarantine -- the insert failed,
    // so there is genuinely nothing there (proving this isn't just
    // redundant with the happy-path quarantine test above).
    assert!(
        store.active_execution_quarantine().await.unwrap().is_none(),
        "the quarantine insert failed -- the store must not show one"
    );

    // The intent row is left UNRESOLVED (`outcome IS NULL`), never marked
    // 'ambiguous' -- exactly what restart reconciliation keys on.
    let raw = rusqlite::Connection::open(&db_path).expect("reopen raw connection");
    let outcome: Option<String> = raw
        .query_row(
            "SELECT outcome FROM rust_broadcast_attempts WHERE request_id = 'req-1'",
            [],
            |r| r.get(0),
        )
        .expect("the attempt's intent row exists");
    assert!(
        outcome.is_none(),
        "the intent row must stay unresolved when the quarantine insert failed, got {outcome:?}"
    );
    drop(raw);

    // In-process poison: the broadcaster now refuses EVERY further batch
    // immediately -- zero further connections -- even on a fresh,
    // otherwise-valid authorization, and even though the store's own
    // quarantine read above would (misleadingly) report "none active".
    let before = server.connection_count();
    let second_request = PersistedFeeRequest {
        request_id: "req-2".to_string(),
        ..one_request()
    };
    let poisoned_err = broadcaster
        .broadcast_batch(authorize_ok(&store).await, &[second_request])
        .await
        .expect_err("a poisoned broadcaster must refuse every further batch");
    assert!(matches!(poisoned_err, BroadcastError::Poisoned));
    assert_eq!(
        server.connection_count(),
        before,
        "a poisoned broadcaster must make zero further RPC calls"
    );
}

// ---------------------------------------------------------------------------
// Step 2: restart restores quarantine BEFORE any arm is accepted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_restores_quarantine_before_the_arm_is_accepted() {
    let tmp = tempfile::tempdir().unwrap();
    let store = observer(tmp.path()).await;

    // Simulate a prior process that persisted an intent and then crashed
    // before ever recording a result.
    store
        .insert_broadcast_attempt(BroadcastAttemptIntent {
            cycle_id: Some("live-cycle-crash".to_string()),
            channel_id: "1x1x0".to_string(),
            request_id: "req-crash".to_string(),
            method: "setchannel".to_string(),
            params_json: r#"{"id":"1x1x0","feebase":0,"feeppm":150}"#.to_string(),
            submitted_at: 1_800_000_000,
        })
        .await
        .expect("seed orphaned intent");
    assert!(store.active_execution_quarantine().await.unwrap().is_none());

    // A fresh process constructs the broadcaster over a FRESH arm --
    // reconciliation must happen as part of accepting that arm, before
    // the broadcaster is usable at all.
    let server = FakeClnServer::spawn(vec![]);
    let live_mode = real_live_mode(tmp.path(), "nonce-restart");
    let _broadcaster = ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 5, live_mode)
        .await
        .expect("reconciliation of a real orphaned intent must succeed");

    store
        .active_execution_quarantine()
        .await
        .unwrap()
        .expect("restart reconciliation must have restored quarantine");
    assert_eq!(
        server.connection_count(),
        0,
        "restoring quarantine on restart must never itself dial CLN"
    );

    // And the arm that was just accepted still cannot authorize anything
    // while that restored quarantine is active.
    let (_authority, bracket) = open_bracket_ok().await;
    let deny = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-ghi",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-3",
    )
    .await
    .expect_err("a freshly-accepted arm must still respect a restored quarantine");
    assert!(matches!(deny, LiveBatchDenyReason::QuarantineActive { .. }));
}

/// Fix round 1 (review finding 2): if restart reconciliation itself
/// fails, `ClnFeeBroadcaster::new` must REFUSE construction outright --
/// never hand back a usable broadcaster over an arm whose quarantine
/// state could not be trusted. Sabotages ONLY the quarantine INSERT path
/// (same seam as the poison test above) so `reconcile_quarantine_on_restart`
/// finds a real orphaned intent, tries to insert the resulting
/// quarantine, and fails.
#[tokio::test]
async fn new_refuses_construction_when_restart_reconciliation_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("observer.db");
    let store = observer(tmp.path()).await;

    store
        .insert_broadcast_attempt(BroadcastAttemptIntent {
            cycle_id: None,
            channel_id: "1x1x0".to_string(),
            request_id: "req-crash-2".to_string(),
            method: "setchannel".to_string(),
            params_json: r#"{"id":"1x1x0","feebase":0,"feeppm":150}"#.to_string(),
            submitted_at: 1_800_000_000,
        })
        .await
        .expect("seed orphaned intent");

    {
        let raw = rusqlite::Connection::open(&db_path).expect("open raw connection to sabotage");
        raw.execute_batch(
            "CREATE TRIGGER quarantine_insert_fails
                 BEFORE INSERT ON rust_execution_quarantine
             BEGIN
                 SELECT RAISE(ABORT, 'test-induced quarantine insert failure');
             END;",
        )
        .expect("install failing trigger");
    }

    let server = FakeClnServer::spawn(vec![]);
    let live_mode = real_live_mode(tmp.path(), "nonce-restart-fails");
    let result = ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 5, live_mode).await;
    assert!(
        result.is_err(),
        "construction must be refused when reconciliation cannot persist the quarantine"
    );
    assert_eq!(
        server.connection_count(),
        0,
        "a refused construction must never dial CLN either"
    );
}

// ---------------------------------------------------------------------------
// Final-review finding I7 (2026-07-26): live-path items that must not
// survive to cutover. All inert today (zero production callers), all on the
// one path that will eventually touch real funds.
// ---------------------------------------------------------------------------

/// I7a: the audit row's `params_json` used to be
/// `serde_json::to_string(...).unwrap_or_default()` -- an EMPTY string on
/// any serialization failure -- while the wire call re-serializes the real
/// params independently. The ledger and the socket must never be able to
/// disagree about what was sent.
#[tokio::test]
async fn broadcast_attempt_row_records_exactly_the_params_sent_on_the_wire() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::Success(json!({"channels": []}))]);
    let store = observer(tmp.path()).await;
    let broadcaster = broadcaster(server.socket_path(), store.clone(), tmp.path()).await;

    broadcaster
        .broadcast_batch(authorize_ok(&store).await, &[one_request()])
        .await
        .expect("authorized single-request batch succeeds");

    let conn = rusqlite::Connection::open(tmp.path().join("observer.db")).unwrap();
    let recorded: String = conn
        .query_row(
            "SELECT params_json FROM rust_broadcast_attempts ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .expect("the broadcast attempt intent row");
    assert!(!recorded.is_empty(), "an empty params_json is never valid");

    let on_the_wire = server.received_params();
    assert_eq!(on_the_wire.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(&recorded).expect("params_json is valid JSON"),
        on_the_wire[0],
        "the audit ledger and the socket must agree on the params"
    );
}

/// I7a, second half — the honest limit of the test above, pinned rather
/// than left implicit.
///
/// The task-40 verifier's audit recorded that 947b2b3's `params_json`
/// fail-closed branch is "unreachable by type, so the fix is untestable as
/// written", and that is correct: `to_params` returns a `serde_json::Value`,
/// and serializing a `Value` cannot fail. So `serde_json::to_string(..)?` and
/// the `unwrap_or_default()` it replaced are behaviourally IDENTICAL today.
/// No runtime test can tell them apart, and the test above passes under both.
///
/// What can be defended is the premise. These pin the two signatures that
/// make the branch unreachable. If either ever becomes fallible — a custom
/// `Serialize`, a map with non-string keys, an arbitrary-precision number —
/// this stops compiling, and that is the signal that the fail-closed branch
/// has become live code needing a real test. The alternative was to record
/// the branch as covered when nothing covers it, which is the exact failure
/// class this branch has now produced six times.
///
/// Note also that `SetChannelRequest::to_params` reacts to a serialization
/// failure by PANICKING (`.expect`), not by failing closed. That is a
/// pre-existing choice, unchanged here, and it is the reason the pin is worth
/// more than a runtime test would be: the first thing to break is the build,
/// not a live cycle.
const _PARAMS_SERIALIZATION_IS_INFALLIBLE_BY_TYPE: () = {
    const _: fn(&PersistedFeeRequest) -> Value = PersistedFeeRequest::to_params;
    const _: fn(&SetChannelRequest) -> Value = SetChannelRequest::to_params;
};

/// I7b: `authorize` read the CURRENT state generation via
/// `load_latest_fee_state()`, which materialises every channel's state row
/// through the same single-owner actor the cycle loop writes through, then
/// throws all of them away. `current_state_generation()` exists precisely
/// to avoid that head-of-line stall. The swap must not change the answer,
/// including once the store has actually committed rows.
#[tokio::test]
async fn state_generation_check_agrees_with_the_committed_generation_after_a_real_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![]);
    let store = observer(tmp.path()).await;

    store
        .commit_fee_cycle(revops_db::fee_runway::FeeCycleCommit {
            cycle_id: "live-gen-1".to_string(),
            started_at: 1_800_000_000,
            completed_at: 1_800_000_001,
            source_commit: SOURCE_COMMIT.to_string(),
            binary_sha256: BINARY_SHA256.to_string(),
            state_rows: vec![revops_db::fee_runway::FeeStateRow {
                channel_id: "1x1x0".to_string(),
                v2_state_json: r#"{"fee_state": {}, "cycle_state": {}}"#.to_string(),
                last_update: 1_800_000_000,
            }],
            ..Default::default()
        })
        .await
        .expect("commit one generation");

    // Authorizing against the pre-commit generation is now stale, and the
    // reported `current` is the generation the commit produced.
    let (_authority, bracket) = open_bracket_ok().await;
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect_err("a candidate built against generation 0 is stale after a commit");
    assert_eq!(
        err,
        LiveBatchDenyReason::StateGenerationStale {
            authorized_against: 0,
            current: 1,
        }
    );
    assert_eq!(server.connection_count(), 0);

    // ... and authorizing against the committed generation still passes
    // (a bracket is single-use, so this takes a fresh one).
    let (_authority, bracket) = open_bracket_ok().await;
    LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        1,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect("the committed generation authorizes");
}

/// Task 59 test plumbing: wait (from a side thread) until `expected`
/// intent rows exist, take a write lock on the observer db, optionally
/// open a fake-server gate, and hold the lock until `release` flips.
fn wedge_after_intent(
    db_path: PathBuf,
    expected: i64,
    gate: Option<Arc<AtomicBool>>,
    release: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.busy_timeout(std::time::Duration::from_secs(60))
            .unwrap();
        loop {
            let n: i64 = conn
                .query_row("SELECT COUNT(*) FROM rust_broadcast_attempts", [], |r| {
                    r.get(0)
                })
                .unwrap();
            if n >= expected {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        if let Some(gate) = gate {
            gate.store(true, Ordering::SeqCst);
        }
        while !release.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = conn.execute_batch("ROLLBACK");
    })
}

/// T1a (§3.2): the clamped store budget survives a REAL 2 s lock wait.
/// Pre-fix red: today's 1 s operator budget denies the batch (and leaves
/// an orphan intent) even though sqlite itself would have waited the
/// lock out well inside `BUSY_TIMEOUT_MS`.
#[tokio::test]
async fn store_budget_never_undercuts_a_real_lock_wait() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::Success(json!({"channels": []}))]);
    let store = observer(tmp.path()).await;
    let live_mode = real_live_mode(tmp.path(), &format!("nonce-{}", uuid_ish()));
    let broadcaster = ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 1, live_mode)
        .await
        .expect("no orphaned intents in a fresh store");
    let authorization = authorize_ok(&store).await;

    // A 2 s held write lock: a legitimate lock wait, not a wedge.
    let db_path = tmp.path().join("observer.db");
    let locker = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(db_path).unwrap();
        conn.busy_timeout(std::time::Duration::from_secs(60))
            .unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));
        conn.execute_batch("ROLLBACK").unwrap();
    });
    // Let the locker win the race for the write lock.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let receipt = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect("a 2 s legitimate lock wait must never deny the batch");
    assert_eq!(receipt.outcomes.len(), 1);
    assert_eq!(server.connection_count(), 1);
    locker.join().unwrap();
}

/// T1b (§3.2, F9): an actor unresponsive past the clamped budget denies
/// the batch as OUTCOME UNKNOWN -- the admitted intent write is queued
/// and uncancellable, so "no write happened" would be a lie. The process
/// poisons itself (conservative fail-closed) and refuses further batches.
#[tokio::test]
async fn unresponsive_actor_denies_within_clamped_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::Success(json!({"channels": []}))]);
    let store = observer(tmp.path()).await;
    let live_mode = real_live_mode(tmp.path(), &format!("nonce-{}", uuid_ish()));
    let broadcaster = ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 1, live_mode)
        .await
        .expect("no orphaned intents in a fresh store");
    let authorization = authorize_ok(&store).await;

    // Wedge held for the whole test + TWO queued writes interleaved so
    // the send-path quarantine READ still answers inside its own budget
    // (behind one 5 s busy-error) while the intent write lands behind
    // ~10 s of actor work -- its 7 s receipt budget fires first (F9).
    let blocker = rusqlite::Connection::open(tmp.path().join("observer.db")).unwrap();
    blocker
        .busy_timeout(std::time::Duration::from_secs(60))
        .unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let ahead = store.clone();
    let queued_ahead = tokio::spawn(async move {
        let _ = ahead
            .insert_peer_connection_event("peer-ahead".into(), "connect".into(), 1_800_000_000)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let ahead2 = store.clone();
    let queued_ahead2 = tokio::spawn(async move {
        // Enqueued while the quarantine read waits, so it sits BETWEEN
        // that read and the intent write in the owner queue.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let _ = ahead2
            .insert_peer_connection_event("peer-ahead-2".into(), "connect".into(), 1_800_000_001)
            .await;
    });

    let started = std::time::Instant::now();
    let err = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect_err("an unresponsive actor must deny the batch");
    let elapsed = started.elapsed();

    match &err {
        BroadcastError::Persistence(detail) => assert!(
            detail.contains("store_intent_outcome_unknown"),
            "an admitted-then-expired intent write is UNKNOWN, got {detail}"
        ),
        other => panic!("expected the typed unknown denial, got {other:?}"),
    }
    assert!(
        elapsed >= std::time::Duration::from_secs(6),
        "the clamped floor must be respected, took only {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(25),
        "the deny must arrive within the clamped budget plus the quarantine attempt, took {elapsed:?}"
    );
    assert_eq!(
        server.connection_count(),
        0,
        "a batch whose intent outcome is unknown must send zero RPC calls"
    );

    // Conservative fail-closed: the process is poisoned now.
    blocker.execute_batch("ROLLBACK").unwrap();
    queued_ahead.await.unwrap();
    queued_ahead2.await.unwrap();
    // The budget-expired quarantine write was uncancellable and lands
    // once the wedge lifts. Flush the actor queue, then clear it
    // directly: the POISON must hold in memory, independent of any
    // store-visible quarantine state.
    store.last_forward_ts().await.unwrap();
    let cleaner = rusqlite::Connection::open(tmp.path().join("observer.db")).unwrap();
    cleaner
        .execute(
            "UPDATE rust_execution_quarantine SET cleared_at = entered_at \
             WHERE cleared_at IS NULL",
            [],
        )
        .unwrap();
    let authorization = authorize_ok(&store).await;
    let err = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect_err("poisoned after an unknown intent outcome");
    assert!(matches!(err, BroadcastError::Poisoned), "{err:?}");
    assert_eq!(server.connection_count(), 0);
}

/// T1c (F9): a FULL owner queue refuses admission as a CLEAN typed
/// non-write (`store_admission_refused`, provably nothing enqueued, no
/// poison) -- distinct from post-admission expiry, which is UNKNOWN.
#[tokio::test]
async fn admission_full_is_clean_deny_post_admission_expiry_is_unknown() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::Success(json!({"channels": []}))]);
    let store = observer(tmp.path()).await;
    let live_mode = real_live_mode(tmp.path(), &format!("nonce-{}", uuid_ish()));
    let broadcaster = Arc::new(
        ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 1, live_mode)
            .await
            .expect("no orphaned intents in a fresh store"),
    );
    let authorization = authorize_ok(&store).await;

    // Hold the actor on ONE slow write (5 s busy-error under the wedge),
    // pre-fill the queue with 64 cheap READ commands, park the batch's
    // quarantine read as the FIRST waiting sender, then park thousands
    // more reads behind it. When the slow write ends, the drain admits
    // the quarantine read early (FIFO) while the parked pool keeps the
    // queue provably full for the intent `try_send`.
    let blocker = rusqlite::Connection::open(tmp.path().join("observer.db")).unwrap();
    blocker
        .busy_timeout(std::time::Duration::from_secs(60))
        .unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let ahead = store.clone();
    let queued_ahead = tokio::spawn(async move {
        let _ = ahead
            .insert_peer_connection_event("peer-ahead".into(), "connect".into(), 1_800_000_000)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut fillers = Vec::new();
    for _ in 0..64 {
        let handle = store.clone();
        fillers.push(tokio::spawn(async move {
            let _ = handle.last_forward_ts().await;
        }));
        tokio::task::yield_now().await;
    }

    let batch_broadcaster = Arc::clone(&broadcaster);
    let batch = tokio::spawn(async move {
        batch_broadcaster
            .broadcast_batch(authorization, &[one_request()])
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    for _ in 0..3000 {
        let handle = store.clone();
        fillers.push(tokio::spawn(async move {
            let _ = handle.last_forward_ts().await;
        }));
    }
    tokio::task::yield_now().await;

    let err = batch
        .await
        .unwrap()
        .expect_err("a full owner queue must refuse admission");
    match &err {
        BroadcastError::Persistence(detail) => assert!(
            detail.contains("store_admission_refused"),
            "a refused admission is a clean typed non-write, got {detail}"
        ),
        other => panic!("expected the typed admission refusal, got {other:?}"),
    }
    assert_eq!(server.connection_count(), 0);

    // NOT poisoned: nothing was enqueued, so nothing is uncertain.
    blocker.execute_batch("ROLLBACK").unwrap();
    queued_ahead.await.unwrap();
    for filler in fillers {
        let _ = filler.await;
    }
    let authorization = authorize_ok(&store).await;
    broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect("a clean admission refusal must not poison the broadcaster");
    assert_eq!(server.connection_count(), 1);
}

/// F4a (§3.4): a wedged result-write after RPC SUCCESS stops the batch,
/// poisons the process, and returns the typed
/// `ResultPersistenceUnknown` naming the outcome that could not be
/// durably recorded.
#[tokio::test]
async fn wedged_result_write_after_success_stops_poisons_and_types() {
    let tmp = tempfile::tempdir().unwrap();
    let gate = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let server = FakeClnServer::spawn(vec![FakeBehavior::GatedSuccess(
        json!({"channels": []}),
        gate.clone(),
    )]);
    let store = observer(tmp.path()).await;
    let live_mode = real_live_mode(tmp.path(), &format!("nonce-{}", uuid_ish()));
    let broadcaster = ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 30, live_mode)
        .await
        .expect("no orphaned intents in a fresh store");
    let authorization = authorize_ok(&store).await;
    let wedge = wedge_after_intent(
        tmp.path().join("observer.db"),
        1,
        Some(gate),
        release.clone(),
    );

    let mut second = one_request();
    second.request_id = "req-2".to_string();
    let requests = [one_request(), second];
    let err = broadcaster
        .broadcast_batch(authorization, &requests)
        .await
        .expect_err("an unrecordable terminal result must fail the batch");
    match &err {
        BroadcastError::ResultPersistenceUnknown {
            request_id,
            rpc_outcome,
            detail,
        } => {
            assert_eq!(request_id, "req-1");
            assert_eq!(rpc_outcome, "success");
            assert!(!detail.is_empty());
        }
        other => panic!("expected ResultPersistenceUnknown, got {other:?}"),
    }
    assert_eq!(
        server.connection_count(),
        1,
        "the batch must STOP: the second request is never attempted"
    );

    release.store(true, Ordering::SeqCst);
    wedge.join().unwrap();
    let authorization = authorize_ok(&store).await;
    let err = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect_err("poisoned after an unrecordable terminal result");
    assert!(matches!(err, BroadcastError::Poisoned), "{err:?}");
    assert_eq!(server.connection_count(), 1);
}

/// F4b (§3.4): same contract after an EXPLICIT CLN rejection.
#[tokio::test]
async fn wedged_result_write_after_rejection_stops_poisons_and_types() {
    let tmp = tempfile::tempdir().unwrap();
    let gate = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let server = FakeClnServer::spawn(vec![FakeBehavior::GatedRejected {
        code: -32602,
        message: "invalid channel".to_string(),
        gate: gate.clone(),
    }]);
    let store = observer(tmp.path()).await;
    let live_mode = real_live_mode(tmp.path(), &format!("nonce-{}", uuid_ish()));
    let broadcaster = ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 30, live_mode)
        .await
        .expect("no orphaned intents in a fresh store");
    let authorization = authorize_ok(&store).await;
    let wedge = wedge_after_intent(
        tmp.path().join("observer.db"),
        1,
        Some(gate),
        release.clone(),
    );

    let err = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect_err("an unrecordable terminal result must fail the batch");
    match &err {
        BroadcastError::ResultPersistenceUnknown {
            request_id,
            rpc_outcome,
            ..
        } => {
            assert_eq!(request_id, "req-1");
            assert_eq!(rpc_outcome, "rejected");
        }
        other => panic!("expected ResultPersistenceUnknown, got {other:?}"),
    }

    release.store(true, Ordering::SeqCst);
    wedge.join().unwrap();
    let authorization = authorize_ok(&store).await;
    let err = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect_err("poisoned after an unrecordable terminal result");
    assert!(matches!(err, BroadcastError::Poisoned), "{err:?}");
}

/// F4c (§3.4): same contract after a CLEAN transport failure (connect
/// error -- no bytes ever left this process).
///
/// Timing: the wedge is held while the intent write is admitted, lifted
/// for exactly long enough for the intent to commit, and re-taken before
/// the batch task can run again -- guaranteed by FREEZING the
/// single-threaded test executor (a synchronous sleep) across the
/// release/re-take, so the batch cannot advance from the intent receipt
/// to the result write while the store is briefly writable.
#[tokio::test]
async fn wedged_result_write_after_clean_failure_stops_poisons_and_types() {
    let tmp = tempfile::tempdir().unwrap();
    let store = observer(tmp.path()).await;
    let live_mode = real_live_mode(tmp.path(), &format!("nonce-{}", uuid_ish()));

    // A nonexistent socket path: connect fails immediately and cleanly.
    let sock_path = tmp.path().join("no-such-lightning-rpc");
    let broadcaster = Arc::new(
        ClnFeeBroadcaster::new(sock_path, store.clone(), 1, live_mode)
            .await
            .expect("no orphaned intents in a fresh store"),
    );
    let authorization = authorize_ok(&store).await;

    let db_path = tmp.path().join("observer.db");
    let release = Arc::new(AtomicBool::new(false));
    let wedge_release = release.clone();
    // Take the wedge HERE, synchronously, before the batch exists --
    // then hand the held transaction to the timing thread.
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.busy_timeout(std::time::Duration::from_secs(60))
        .unwrap();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    let wedge = std::thread::spawn(move || {
        // Hold through the intent write's admission, then let exactly
        // that write commit...
        std::thread::sleep(std::time::Duration::from_secs(1));
        conn.execute_batch("ROLLBACK").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let n: i64 = conn
                .query_row("SELECT COUNT(*) FROM rust_broadcast_attempts", [], |r| {
                    r.get(0)
                })
                .unwrap();
            if n >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the intent write never landed"
            );
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        // ...and re-take the wedge before the batch task can reach the
        // result write (the test executor is frozen right now).
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        while !wedge_release.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let _ = conn.execute_batch("ROLLBACK");
    });

    let batch_broadcaster = Arc::clone(&broadcaster);
    let batch = tokio::spawn(async move {
        batch_broadcaster
            .broadcast_batch(authorization, &[one_request()])
            .await
    });
    // Let the batch run up to the intent receipt (the actor is
    // busy-waiting on the wedge), then freeze this single-threaded
    // executor across the wedge's release/re-take window.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    std::thread::sleep(std::time::Duration::from_millis(2_000));

    let err = batch
        .await
        .unwrap()
        .expect_err("an unrecordable terminal result must fail the batch");
    match &err {
        BroadcastError::ResultPersistenceUnknown {
            request_id,
            rpc_outcome,
            ..
        } => {
            assert_eq!(request_id, "req-1");
            assert_eq!(rpc_outcome, "clean_failure");
        }
        other => panic!("expected ResultPersistenceUnknown, got {other:?}"),
    }

    release.store(true, Ordering::SeqCst);
    wedge.join().unwrap();
    let authorization = authorize_ok(&store).await;
    let err = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect_err("poisoned after an unrecordable terminal result");
    assert!(matches!(err, BroadcastError::Poisoned), "{err:?}");
}

// ---------------------------------------------------------------------------
// Task 59 Area F: endpoint-bound, single-use authority bracketing
// ---------------------------------------------------------------------------

const AUTHORITY_NOW: i64 = 1_800_000_020;
const AUTHORITY_MAX_AGE: i64 = 30;

fn authority_payload(observed_at: i64) -> Value {
    json!({
        "enabled": false,
        "generation": 3,
        "transitioned_at": 1_799_000_000,
        "observed_at": observed_at,
    })
}

/// F5c (and F5a's same-endpoint contract): `open_bracket` performs
/// exactly ONE fetch; the SECOND fetch happens inside `authorize` --
/// against the same originating endpoint (`close` has no client
/// parameter to point anywhere else) -- strictly during authorization,
/// immediately before minting.
#[tokio::test]
async fn second_fetch_happens_inside_authorize() {
    let tmp = tempfile::tempdir().unwrap();
    let store = observer(tmp.path()).await;
    let authority = FakeClnServer::spawn(vec![
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 2)),
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 1)),
    ]);
    let client = PythonAuthorityClient::new(authority.socket_path(), 5);
    let bracket = client
        .open_bracket(AUTHORITY_NOW, AUTHORITY_MAX_AGE)
        .await
        .expect("open bracket");
    assert_eq!(
        authority.connection_count(),
        1,
        "open performs exactly one fetch"
    );

    let authorization = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect("bracketed authorization");
    assert_eq!(
        authority.connection_count(),
        2,
        "the second fetch happens INSIDE authorize, immediately before minting"
    );
    assert_eq!(authorization.python_authority_generation(), 3);
}

/// F5d (R2-F2): a stale OPEN bracket is refused BEFORE the second fetch
/// -- exactly ONE total fetch for a refused stale bracket, and the NEW
/// typed deny (never a reworded existing code).
#[tokio::test]
async fn stale_open_bracket_refused_before_second_fetch() {
    let tmp = tempfile::tempdir().unwrap();
    let store = observer(tmp.path()).await;
    let authority = FakeClnServer::spawn(vec![
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 2)),
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 1)),
    ]);
    let client = PythonAuthorityClient::new(authority.socket_path(), 5);
    let bracket = client
        .open_bracket(AUTHORITY_NOW, AUTHORITY_MAX_AGE)
        .await
        .expect("open bracket");

    // Authorization-time `now` far past the first reading's max age: the
    // bracket is stale-open and must refuse without fetch #2.
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW + AUTHORITY_MAX_AGE + 31,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect_err("a stale open bracket must refuse");
    match &err {
        LiveBatchDenyReason::PythonAuthority(reason) => {
            assert_eq!(reason.code(), "python_authority_stale_open_bracket");
            assert!(matches!(
                reason,
                PythonAuthorityDenyReason::StaleOpenBracket { .. }
            ));
        }
        other => panic!("expected the stale-open-bracket deny, got {other:?}"),
    }
    assert_eq!(
        authority.connection_count(),
        1,
        "a refused stale bracket performs exactly ONE total fetch (the open)"
    );
}

/// F14: two honest fetches inside Python's 1 s `observed_at` resolution
/// deny as `NonAdvancingObservation` -- exactly TWO fetches, no
/// automatic tight-loop retry anywhere.
#[tokio::test]
async fn same_second_second_read_denies_without_retry() {
    let tmp = tempfile::tempdir().unwrap();
    let store = observer(tmp.path()).await;
    let authority = FakeClnServer::spawn(vec![
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 1)),
        FakeBehavior::Success(authority_payload(AUTHORITY_NOW - 1)),
    ]);
    let client = PythonAuthorityClient::new(authority.socket_path(), 5);
    let bracket = client
        .open_bracket(AUTHORITY_NOW, AUTHORITY_MAX_AGE)
        .await
        .expect("open bracket");

    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        bracket,
        AUTHORITY_NOW,
        true,
        "authorized",
        "idem-1",
    )
    .await
    .expect_err("a non-advancing second read must deny");
    match &err {
        LiveBatchDenyReason::PythonAuthority(reason) => assert!(
            matches!(
                reason,
                PythonAuthorityDenyReason::NonAdvancingObservation { .. }
            ),
            "{reason:?}"
        ),
        other => panic!("expected the non-advancing deny, got {other:?}"),
    }
    assert_eq!(
        authority.connection_count(),
        2,
        "denied means denied: exactly two fetches, no retry loop"
    );
}

/// F5b: a minted authorization parked past
/// `AUTHORIZATION_DISPATCH_FRESHNESS` is refused AT DISPATCH with the
/// typed `AuthorizationStale` -- capping the wall-clock window in which
/// Python could re-enable behind a parked authorization.
#[tokio::test]
async fn stale_authorization_refused_at_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let server = FakeClnServer::spawn(vec![FakeBehavior::Success(json!({"channels": []}))]);
    let store = observer(tmp.path()).await;
    let live_mode = real_live_mode(tmp.path(), &format!("nonce-{}", uuid_ish()));
    let broadcaster = ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 2, live_mode)
        .await
        .expect("no orphaned intents in a fresh store");
    let authorization = authorize_ok(&store).await;

    // Park the authorization past the dispatch freshness bound. The
    // staleness gate is in-memory and checked before any store read, so
    // the paused clock never interacts with a store budget.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    tokio::time::resume();

    let err = broadcaster
        .broadcast_batch(authorization, &[one_request()])
        .await
        .expect_err("a parked authorization must refuse at dispatch");
    assert!(
        matches!(err, BroadcastError::AuthorizationStale { .. }),
        "{err:?}"
    );
    assert_eq!(
        server.connection_count(),
        0,
        "a stale authorization sends zero RPC calls"
    );
}
