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
use revops::python_authority::PythonAuthorityOff;
use revops_db::fee_runway::{BroadcastAttemptIntent, FeeStateSnapshot, QuarantineEntry};
use revops_db::owner::{spawn_read_write, ObserverHandle};
use revops_fees::execution::SetChannelRequest;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
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
    match validate_fee_mode(flags, Some(arm), &FeeStateSnapshot::default(), None) {
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

fn stable_readings() -> (PythonAuthorityOff, PythonAuthorityOff) {
    let first = PythonAuthorityOff {
        generation: 3,
        transitioned_at: 1_799_000_000,
        observed_at: 1_800_000_000,
    };
    let second = PythonAuthorityOff {
        generation: 3,
        transitioned_at: 1_799_000_000,
        observed_at: 1_800_000_010,
    };
    (first, second)
}

/// Fix round 1 (review finding 4): `authorize` now reads the
/// quarantine-empty observation and the current state generation
/// directly from `store` -- a fresh (never-committed) store's generation
/// is always `0`, so every fixture below authorizes against `0` unless a
/// test deliberately wants a mismatch.
async fn authorize_ok(store: &ObserverHandle) -> LiveBatchAuthorization {
    let (first, second) = stable_readings();
    LiveBatchAuthorization::authorize(
        store,
        "candidate-sha-abc",
        0,
        &first,
        &second,
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

    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        &first,
        &second,
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
    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        6, // candidate was built against generation 6; the store is still 0
        &first,
        &second,
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
    let first = PythonAuthorityOff {
        generation: 3,
        transitioned_at: 1_799_000_000,
        observed_at: 1_800_000_000,
    };
    let second = PythonAuthorityOff {
        generation: 4, // Python's authority state moved during the batch window
        transitioned_at: 1_799_500_000,
        observed_at: 1_800_000_010,
    };
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        &first,
        &second,
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
    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        &first,
        &second,
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
    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        &first,
        &second,
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

    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        &first,
        &second,
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

    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-abc",
        0,
        &first,
        &second,
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
    let (first, second) = stable_readings();
    let deny = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-def",
        0,
        &first,
        &second,
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
    let (first, second) = stable_readings();
    let deny = LiveBatchAuthorization::authorize(
        &store,
        "candidate-sha-ghi",
        0,
        &first,
        &second,
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
