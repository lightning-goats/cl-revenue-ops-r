//! Tests for `revops::fee_execution` -- the guarded CLN fee broadcaster
//! (stateful-shadow revision plan, Task 9). This is the ONLY component
//! that may ever send a live `setchannel` to CLN; every test here proves
//! either (a) exactly one such call happens on a fully-authorized batch,
//! or (b) some denial/failure path sends ZERO calls, or (c) an ambiguous
//! transport outcome persistently quarantines execution across a
//! restart.
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
use revops_db::fee_runway::{BroadcastAttemptIntent, FeeStateSnapshot};
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

fn authorize_ok(
    active_quarantine: Option<revops_db::fee_runway::QuarantineRow>,
) -> LiveBatchAuthorization {
    let (first, second) = stable_readings();
    LiveBatchAuthorization::authorize(
        "candidate-sha-abc",
        7,
        7,
        &first,
        &second,
        true,
        "authorized",
        "idem-1",
        active_quarantine,
    )
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
    let live_mode = real_live_mode(tmp.path(), "nonce-success");
    let broadcaster =
        ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 5, live_mode).await;

    let receipt = broadcaster
        .broadcast_batch(authorize_ok(None), &[one_request()])
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
    let server = FakeClnServer::spawn(vec![]);
    let active = revops_db::fee_runway::QuarantineRow {
        id: 1,
        reason: "ambiguous post-submission transport outcome".to_string(),
        cycle_id: None,
        channel_id: Some("1x1x0".to_string()),
        request_id: Some("req-0".to_string()),
        entered_at: 1_800_000_000,
    };
    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        "candidate-sha-abc",
        7,
        7,
        &first,
        &second,
        true,
        "authorized",
        "idem-1",
        Some(active),
    )
    .expect_err("active quarantine must deny authorization");
    assert!(matches!(err, LiveBatchDenyReason::QuarantineActive { .. }));
    assert_eq!(server.connection_count(), 0);
}

#[tokio::test]
async fn stale_state_generation_denies_authorization_with_zero_calls() {
    let server = FakeClnServer::spawn(vec![]);
    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        "candidate-sha-abc",
        6, // candidate was built against generation 6
        7, // current generation has already advanced to 7
        &first,
        &second,
        true,
        "authorized",
        "idem-1",
        None,
    )
    .expect_err("stale state generation must deny authorization");
    assert!(matches!(
        err,
        LiveBatchDenyReason::StateGenerationStale {
            authorized_against: 6,
            current: 7,
        }
    ));
    assert_eq!(server.connection_count(), 0);
}

#[tokio::test]
async fn unstable_python_authority_epoch_denies_authorization_with_zero_calls() {
    let server = FakeClnServer::spawn(vec![]);
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
        "candidate-sha-abc",
        7,
        7,
        &first,
        &second,
        true,
        "authorized",
        "idem-1",
        None,
    )
    .expect_err("unstable epoch must deny authorization");
    assert!(matches!(err, LiveBatchDenyReason::PythonAuthority(_)));
    assert_eq!(server.connection_count(), 0);
}

#[tokio::test]
async fn governor_denial_denies_authorization_with_zero_calls() {
    let server = FakeClnServer::spawn(vec![]);
    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        "candidate-sha-abc",
        7,
        7,
        &first,
        &second,
        false,
        "paused",
        "idem-1",
        None,
    )
    .expect_err("governor denial must deny authorization");
    assert!(matches!(
        err,
        LiveBatchDenyReason::GovernorDenied { reason_code } if reason_code == "paused"
    ));
    assert_eq!(server.connection_count(), 0);
}

#[tokio::test]
async fn missing_ledger_reservation_denies_authorization_with_zero_calls() {
    let server = FakeClnServer::spawn(vec![]);
    let (first, second) = stable_readings();
    let err = LiveBatchAuthorization::authorize(
        "candidate-sha-abc",
        7,
        7,
        &first,
        &second,
        true,
        "authorized",
        "", // no reservation id
        None,
    )
    .expect_err("missing ledger reservation must deny authorization");
    assert!(matches!(err, LiveBatchDenyReason::LedgerReservationMissing));
    assert_eq!(server.connection_count(), 0);
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
    let live_mode = real_live_mode(tmp.path(), "nonce-rejected");
    let broadcaster =
        ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 5, live_mode).await;

    let err = broadcaster
        .broadcast_batch(authorize_ok(None), &[one_request()])
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
    let live_mode = real_live_mode(tmp.path(), "nonce-ambiguous");
    let broadcaster =
        ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 5, live_mode).await;

    let err = broadcaster
        .broadcast_batch(authorize_ok(None), &[one_request()])
        .await
        .expect_err("disconnect after receipt must be ambiguous");
    assert!(matches!(err, BroadcastError::Ambiguous { .. }));
    assert_eq!(server.connection_count(), 1, "exactly one attempt was made");

    let active = store
        .active_execution_quarantine()
        .await
        .unwrap()
        .expect("ambiguous transport outcome must quarantine execution");
    assert_eq!(active.request_id.as_deref(), Some("req-1"));

    // The NEXT batch is blocked: constructing a fresh authorization must
    // now deny on the active quarantine, with zero further calls.
    let (first, second) = stable_readings();
    let deny = LiveBatchAuthorization::authorize(
        "candidate-sha-def",
        7,
        7,
        &first,
        &second,
        true,
        "authorized",
        "idem-2",
        Some(active),
    )
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
    let live_mode = real_live_mode(tmp.path(), "nonce-hang");
    // Short timeout budget so the test doesn't wait out a production budget.
    let broadcaster =
        ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 1, live_mode).await;

    let err = broadcaster
        .broadcast_batch(authorize_ok(None), &[one_request()])
        .await
        .expect_err("a hang past the timeout budget must be ambiguous");
    assert!(matches!(err, BroadcastError::Ambiguous { .. }));
    assert!(store.active_execution_quarantine().await.unwrap().is_some());
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
    let _broadcaster =
        ClnFeeBroadcaster::new(server.socket_path(), store.clone(), 5, live_mode).await;

    let active = store
        .active_execution_quarantine()
        .await
        .unwrap()
        .expect("restart reconciliation must have restored quarantine");
    assert_eq!(active.request_id.as_deref(), Some("req-crash"));
    assert_eq!(
        server.connection_count(),
        0,
        "restoring quarantine on restart must never itself dial CLN"
    );

    // And the arm that was just accepted still cannot authorize anything
    // while that restored quarantine is active.
    let (first, second) = stable_readings();
    let deny = LiveBatchAuthorization::authorize(
        "candidate-sha-ghi",
        7,
        7,
        &first,
        &second,
        true,
        "authorized",
        "idem-3",
        Some(active),
    )
    .expect_err("a freshly-accepted arm must still respect a restored quarantine");
    assert!(matches!(deny, LiveBatchDenyReason::QuarantineActive { .. }));
}
