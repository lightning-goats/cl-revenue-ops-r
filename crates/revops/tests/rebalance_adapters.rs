//! Task 60 slice 3: concrete PaymentRpc/ReconcileRpc adapters, proven
//! ONLY against a local blocking Unix-socket fake -- never a live node.
//!
//! The adapters must follow the executor seam's error-encoding
//! conventions (`revops_rebalance::executor` module docs): a CLN JSON-RPC
//! error is carried as `RpcFailure{ message: <the error dict as JSON
//! text> }`, and a proxy deadline is plain text containing "rpc timeout".

use revops::rebalance_adapters::{ClnPaymentRpc, ClnReconcileRpc};
use revops_rebalance::executor::PaymentRpc;
use revops_rebalance::router::SendpayHop;
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
        let listener = UnixListener::bind(&path).expect("bind fake socket");
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

    fn received(&self) -> Vec<Value> {
        self.received.lock().unwrap().clone()
    }
}

fn one_hop() -> SendpayHop {
    SendpayHop {
        id: "02aa".repeat(16),
        channel: "100x1x0".into(),
        direction: 1,
        delay: 34,
        amount_msat: 250_000_500,
        style: "tlv",
    }
}

/// sendpay carries route/payment_hash/bolt11/payment_secret exactly, with
/// hops serialized field-for-field.
#[test]
fn sendpay_wire_shape_is_exact() {
    let fake = FakeCln::spawn(vec![Behavior::Success(json!({"status": "pending"}))]);
    let rpc = ClnPaymentRpc::new(fake.path.clone(), 5);
    let value = rpc
        .sendpay(&[one_hop()], "deadbeef", "lnbc1invoice", "secret123")
        .expect("scripted success");
    assert_eq!(value["status"], "pending");

    let received = fake.received();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["method"], "sendpay");
    let params = &received[0]["params"];
    assert_eq!(params["payment_hash"], "deadbeef");
    assert_eq!(params["bolt11"], "lnbc1invoice");
    assert_eq!(params["payment_secret"], "secret123");
    let hop = &params["route"][0];
    assert_eq!(hop["channel"], "100x1x0");
    assert_eq!(hop["direction"], 1);
    assert_eq!(hop["delay"], 34);
    assert_eq!(hop["amount_msat"], 250_000_500i64);
    assert_eq!(hop["style"], "tlv");
}

/// A CLN JSON-RPC error is surfaced as the ERROR DICT AS JSON TEXT (the
/// seam convention `error_details` parses), code preserved.
#[test]
fn jsonrpc_error_is_carried_as_json_text_with_code() {
    let fake = FakeCln::spawn(vec![Behavior::Error(
        json!({"code": 204, "message": "failed: WIRE_TEMPORARY_CHANNEL_FAILURE", "data": {"erring_index": 1}}),
    )]);
    let rpc = ClnPaymentRpc::new(fake.path.clone(), 5);
    let err = rpc.waitsendpay("deadbeef", 60).expect_err("scripted error");
    let parsed: Value =
        serde_json::from_str(&err.message).expect("message must BE the error dict as JSON");
    assert_eq!(parsed["code"], 204);
    assert_eq!(parsed["message"], "failed: WIRE_TEMPORARY_CHANNEL_FAILURE");
    assert_eq!(parsed["data"]["erring_index"], 1);

    let received = fake.received();
    assert_eq!(received[0]["method"], "waitsendpay");
    assert_eq!(received[0]["params"]["payment_hash"], "deadbeef");
    assert_eq!(received[0]["params"]["timeout"], 60);
}

/// A proxy deadline (no reply within the budget) is plain text containing
/// "rpc timeout" -- the executor's pending detector keys on it for
/// sendpay/waitsendpay, leaving the HTLC state unknown.
#[test]
fn no_reply_within_budget_is_a_plain_rpc_timeout() {
    let fake = FakeCln::spawn(vec![Behavior::NeverReply]);
    let rpc = ClnPaymentRpc::new(fake.path.clone(), 1);
    let started = std::time::Instant::now();
    // waitsendpay's socket deadline is CLN's own wait window + 5s (it must
    // outlive a legitimate wait) -- with a 1s window the deadline is 6s.
    let err = rpc
        .waitsendpay("deadbeef", 1)
        .expect_err("no reply must time out");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the deadline must actually bound the wait"
    );
    assert!(
        err.message.to_lowercase().contains("rpc timeout"),
        "convention: plain-text proxy deadline, got {}",
        err.message
    );
    assert!(
        serde_json::from_str::<Value>(&err.message).is_err(),
        "a proxy deadline is NOT a structured CLN error"
    );
}

/// A missing socket is a plain transport error, NOT an "rpc timeout" (it
/// must never read as payment-possibly-pending upstream).
#[test]
fn missing_socket_is_a_transport_error_not_a_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let rpc = ClnPaymentRpc::new(dir.path().join("nope.sock"), 1);
    let err = rpc.getinfo_id().expect_err("no socket");
    assert!(
        !err.message.to_lowercase().contains("rpc timeout"),
        "a connect failure is provably-nothing-sent, got {}",
        err.message
    );
}

/// invoice/delpay/delinvoice/getinfo parameter shapes.
#[test]
fn remaining_payment_rpc_wire_shapes() {
    let fake = FakeCln::spawn(vec![
        Behavior::Success(json!({"id": "02ab".repeat(16)})),
        Behavior::Success(json!({"bolt11": "lnbc1", "payment_hash": "ph", "payment_secret": "ps"})),
        Behavior::Success(json!({})),
        Behavior::Success(json!({})),
    ]);
    let rpc = ClnPaymentRpc::new(fake.path.clone(), 5);

    let id = rpc.getinfo_id().expect("getinfo");
    assert_eq!(id, "02ab".repeat(16));
    rpc.invoice(250_000_500, "rebal-native-1-200x2x0", 3600)
        .expect("invoice");
    rpc.delpay("ph", "failed").expect("delpay");
    rpc.delinvoice("rebal-native-1-200x2x0", "unpaid")
        .expect("delinvoice");

    let received = fake.received();
    assert_eq!(received[0]["method"], "getinfo");
    assert_eq!(received[1]["method"], "invoice");
    assert_eq!(received[1]["params"]["amount_msat"], 250_000_500i64);
    assert_eq!(received[1]["params"]["label"], "rebal-native-1-200x2x0");
    assert_eq!(received[1]["params"]["expiry"], 3600);
    assert_eq!(received[2]["method"], "delpay");
    assert_eq!(received[2]["params"]["payment_hash"], "ph");
    assert_eq!(received[2]["params"]["status"], "failed");
    assert_eq!(received[3]["method"], "delinvoice");
    assert_eq!(received[3]["params"]["label"], "rebal-native-1-200x2x0");
    assert_eq!(received[3]["params"]["status"], "unpaid");
}

/// The reconciliation read: listsendpays by payment_hash, result passed
/// through verbatim for the owner's definite/pending disambiguation.
#[test]
fn reconcile_listsendpays_wire_shape() {
    let fake = FakeCln::spawn(vec![Behavior::Success(
        json!({"payments": [{"status": "complete", "payment_hash": "ph1"}]}),
    )]);
    let rpc = ClnReconcileRpc::new(fake.path.clone(), 5);
    let value = rpc.listsendpays("ph1").expect("scripted");
    assert_eq!(value["payments"][0]["status"], "complete");
    let received = fake.received();
    assert_eq!(received[0]["method"], "listsendpays");
    assert_eq!(received[0]["params"]["payment_hash"], "ph1");
}
