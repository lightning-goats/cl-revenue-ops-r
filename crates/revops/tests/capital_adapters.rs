//! Task 62 slice 4: fundchannel/close socket adapters (blocking fakes
//! only) and the four-way capital classification.

use revops::capital_adapters::{
    classify_capital_submit, ClnCloseRpc, ClnFundchannelRpc, CloseRpc, FundchannelRpc,
};
use revops::capital_boundaries::CapitalSubmitOutcome;
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

enum Behavior {
    Success(Value),
    Error(Value),
    NeverReply,
}

struct FakeCln {
    _dir: tempfile::TempDir,
    path: PathBuf,
    received: Arc<Mutex<Vec<Value>>>,
}

impl FakeCln {
    fn spawn(behaviors: Vec<Behavior>) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fake-cln.sock");
        let listener = UnixListener::bind(&path).expect("bind");
        let received = Arc::new(Mutex::new(Vec::new()));
        let received_task = received.clone();
        std::thread::spawn(move || {
            let mut behaviors = behaviors.into_iter();
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let behavior = behaviors.next();
                let received = received_task.clone();
                std::thread::spawn(move || {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 8192];
                    let request: Value = loop {
                        let n = stream.read(&mut chunk).unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Ok(v) = serde_json::from_slice::<Value>(&buf) {
                            break v;
                        }
                    };
                    let id = request.get("id").cloned().unwrap_or(json!(1));
                    received.lock().unwrap().push(request);
                    let body = match behavior {
                        Some(Behavior::Success(result)) => {
                            json!({"jsonrpc": "2.0", "id": id, "result": result})
                        }
                        Some(Behavior::Error(error)) => {
                            json!({"jsonrpc": "2.0", "id": id, "error": error})
                        }
                        Some(Behavior::NeverReply) => loop {
                            std::thread::sleep(std::time::Duration::from_secs(3600));
                        },
                        None => json!({"jsonrpc": "2.0", "id": id, "unexpected": true}),
                    };
                    let mut out = serde_json::to_vec(&body).unwrap();
                    out.extend_from_slice(b"\n\n");
                    let _ = stream.write_all(&out);
                });
            }
        });
        FakeCln {
            _dir: dir,
            path,
            received,
        }
    }
}

/// fundchannel wire shape: id/amount plus the optional request_amt and
/// compact_lease passthrough (py `_rpc_fundchannel:3054`).
#[test]
fn fundchannel_wire_shape_is_exact() {
    let fake = FakeCln::spawn(vec![Behavior::Success(
        json!({"txid": "deadbeef", "channel_id": "abc"}),
    )]);
    let rpc = ClnFundchannelRpc::new(fake.path.clone(), 5);
    let value = rpc
        .fundchannel("02aa", 1_000_000, Some(500_000), Some("lease".into()))
        .expect("scripted success");
    assert_eq!(value["txid"], "deadbeef");
    let received = fake.received.lock().unwrap().clone();
    assert_eq!(received[0]["method"], "fundchannel");
    assert_eq!(received[0]["params"]["id"], "02aa");
    assert_eq!(received[0]["params"]["amount"], 1_000_000);
    assert_eq!(received[0]["params"]["request_amt"], 500_000);
    assert_eq!(received[0]["params"]["compact_lease"], "lease");

    // Optionals omitted when None.
    let fake2 = FakeCln::spawn(vec![Behavior::Success(json!({"txid": "beef"}))]);
    let rpc2 = ClnFundchannelRpc::new(fake2.path.clone(), 5);
    rpc2.fundchannel("02bb", 2_000_000, None, None).unwrap();
    let received = fake2.received.lock().unwrap().clone();
    assert!(received[0]["params"].get("request_amt").is_none());
    assert!(received[0]["params"].get("compact_lease").is_none());
}

/// close wire shape with the optional unilateral timeout.
#[test]
fn close_wire_shape_is_exact() {
    let fake = FakeCln::spawn(vec![Behavior::Success(json!({"type": "mutual"}))]);
    let rpc = ClnCloseRpc::new(fake.path.clone(), 5);
    rpc.close("700x1x0", Some(30)).expect("scripted");
    let received = fake.received.lock().unwrap().clone();
    assert_eq!(received[0]["method"], "close");
    assert_eq!(received[0]["params"]["id"], "700x1x0");
    assert_eq!(received[0]["params"]["unilateraltimeout"], 30);
}

/// Classification: pre-write refusals are clean, definite CLN errors are
/// rejected-with-proof, replies with txid succeed, and EVERYTHING
/// ambiguous (deadline expiry, shapeless replies) is unknown -- an
/// on-chain submit whose reply was lost MAY have broadcast.
#[test]
fn classification_is_fail_closed_for_onchain_ambiguity() {
    // Connect failure (no socket): provably nothing sent.
    let dir = tempfile::tempdir().unwrap();
    let rpc = ClnFundchannelRpc::new(dir.path().join("nope.sock"), 1);
    let result = rpc.fundchannel("02aa", 1_000_000, None, None);
    assert!(matches!(
        classify_capital_submit(&result),
        CapitalSubmitOutcome::CleanRefusal { .. }
    ));

    // Definite CLN error: rejected with proof.
    let fake = FakeCln::spawn(vec![Behavior::Error(
        json!({"code": 301, "message": "Insufficient funds"}),
    )]);
    let rpc = ClnFundchannelRpc::new(fake.path.clone(), 5);
    let result = rpc.fundchannel("02aa", 1_000_000, None, None);
    match classify_capital_submit(&result) {
        CapitalSubmitOutcome::Rejected { detail } => {
            assert!(detail.contains("Insufficient funds"), "{detail}")
        }
        other => panic!("{other:?}"),
    }

    // Success with txid.
    let fake = FakeCln::spawn(vec![Behavior::Success(json!({"txid": "feed"}))]);
    let rpc = ClnFundchannelRpc::new(fake.path.clone(), 5);
    let result = rpc.fundchannel("02aa", 1_000_000, None, None);
    match classify_capital_submit(&result) {
        CapitalSubmitOutcome::Success { txid } => assert_eq!(txid.as_deref(), Some("feed")),
        other => panic!("{other:?}"),
    }

    // Deadline expiry after the request was written: UNKNOWN.
    let fake = FakeCln::spawn(vec![Behavior::NeverReply]);
    let rpc = ClnFundchannelRpc::new(fake.path.clone(), 1);
    let result = rpc.fundchannel("02aa", 1_000_000, None, None);
    assert!(matches!(
        classify_capital_submit(&result),
        CapitalSubmitOutcome::OutcomeUnknown { .. }
    ));

    // A success-shaped reply WITHOUT a txid is ambiguous, not success.
    let fake = FakeCln::spawn(vec![Behavior::Success(json!({"status": "??"}))]);
    let rpc = ClnFundchannelRpc::new(fake.path.clone(), 5);
    let result = rpc.fundchannel("02aa", 1_000_000, None, None);
    assert!(matches!(
        classify_capital_submit(&result),
        CapitalSubmitOutcome::OutcomeUnknown { .. }
    ));

    // A close reply carries the closing txid too (CLN returns it for
    // both mutual and unilateral) -- the txid-required rule is uniform.
    let fake = FakeCln::spawn(vec![Behavior::Success(
        json!({"type": "mutual", "txid": "cc"}),
    )]);
    let rpc = ClnCloseRpc::new(fake.path.clone(), 5);
    let result = rpc.close("700x1x0", None);
    assert!(matches!(
        classify_capital_submit(&result),
        CapitalSubmitOutcome::Success { .. }
    ));
}

/// No capability leak: production never constructs the adapters
/// (runtime/lnplus_runtime/main never name them).
#[test]
fn adapters_unreachable_from_observer_surfaces() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for file in ["src/runtime.rs", "src/lnplus_runtime.rs", "src/main.rs"] {
        let source = std::fs::read_to_string(root.join(file)).unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap();
        for name in ["ClnFundchannelRpc", "ClnCloseRpc"] {
            assert!(
                !production.contains(name),
                "{file} must not name {name} before Task 69 authority assembly"
            );
        }
    }
}
