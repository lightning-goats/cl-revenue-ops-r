//! Task 61 4C — the concrete CLN signer/chain adapters against a LOCAL
//! Unix-socket fake lightningd (same fixture shape as
//! `tests/fee_execution.rs`; a fresh tempdir socket, never a real
//! `lightning-rpc`). Covers exact request shape, success parsing,
//! definite rejection (clean), disconnect/timeout after receipt
//! (OutcomeUnknown), malformed framing (OutcomeUnknown), unreachable
//! socket (clean), and single-attempt (no internal resubmit).

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use revops::lnplus_adapters::{ClnChainAdapter, ClnSigner};
use revops_lnplus::http::Signer;
use revops_lnplus::ports::{ChainPort, Feerate, FundChannelOutcome};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

#[derive(Clone)]
enum FakeBehavior {
    Success(Value),
    Rejected {
        code: i64,
        message: String,
    },
    /// Read the full request, then close without responding — bytes were
    /// received; the true outcome is unknown.
    DisconnectAfterReceipt,
    /// A syntactically valid JSON object that is neither `result` nor
    /// `error` — undecodable as a CLN reply.
    Malformed,
    /// Read the request, never respond, never close — exercises the
    /// adapter's call-phase timeout budget.
    HangForever,
}

struct FakeClnServer {
    _dir: tempfile::TempDir,
    path: PathBuf,
    connections: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<Value>>>,
}

impl FakeClnServer {
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
                            return;
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
                        FakeBehavior::DisconnectAfterReceipt => drop(stream),
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

    fn received_method_params(&self, index: usize) -> (String, Value) {
        let received = self.received.lock().unwrap();
        let v = received.get(index).expect("request recorded").clone();
        (
            v.get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            v.get("params").cloned().unwrap_or(Value::Null),
        )
    }
}

/// The adapters own a private runtime and must be driven from a sync
/// (blocking) context; the fake server needs a live tokio runtime. Keep
/// the server runtime alive for the test body's duration.
fn with_server(behaviors: Vec<FakeBehavior>) -> (tokio::runtime::Runtime, FakeClnServer) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("server runtime");
    let server = {
        let _guard = rt.enter();
        FakeClnServer::spawn(behaviors)
    };
    (rt, server)
}

fn timeout() -> Duration {
    Duration::from_secs(5)
}

// ------------------------------------------------------------------ signer

#[test]
fn signer_sends_signmessage_and_returns_the_zbase_field() {
    let (_rt, server) = with_server(vec![FakeBehavior::Success(json!({"zbase": "zsig123"}))]);
    let signer = ClnSigner::new(server.path.clone(), timeout()).unwrap();
    let sig = signer.signmessage("lnplus login 42").expect("signed");
    assert_eq!(sig, "zsig123");
    let (method, params) = server.received_method_params(0);
    assert_eq!(method, "signmessage");
    assert_eq!(
        params.get("message").and_then(Value::as_str),
        Some("lnplus login 42")
    );
}

#[test]
fn signer_missing_zbase_is_an_error() {
    let (_rt, server) = with_server(vec![FakeBehavior::Success(json!({"signature": "raw"}))]);
    let signer = ClnSigner::new(server.path.clone(), timeout()).unwrap();
    assert!(signer.signmessage("m").is_err());
}

// ------------------------------------------------------------- chain reads

#[test]
fn chain_reads_parse_cln_shapes() {
    let (_rt, server) = with_server(vec![
        FakeBehavior::Success(json!({"id": "02abc"})),
        FakeBehavior::Success(json!({"channels": [
            {"peer_id": "02aa", "state": "CHANNELD_NORMAL", "total_msat": 5_000_000_000i64,
             "to_us_msat": 1_000_000_000i64, "funding_txid": "txid-1"},
            {"peer_id": "02bb", "state": "OPENINGD", "total_msat": 7_000i64, "to_us_msat": 0}
        ]})),
        FakeBehavior::Success(json!({"perkw": {"opening": 1234}})),
        FakeBehavior::Success(json!({"outputs": [
            {"status": "confirmed", "reserved": false, "amount_msat": 2_000_000i64},
            {"status": "confirmed", "reserved": true,  "amount_msat": 9_000_000i64},
            {"status": "unconfirmed", "reserved": false, "amount_msat": 5_000_000i64}
        ]})),
    ]);
    let chain = ClnChainAdapter::new(server.path.clone(), timeout()).unwrap();

    assert_eq!(chain.our_node_id().unwrap(), "02abc");

    let channels = chain.list_peer_channels(Some("02aa")).unwrap();
    assert_eq!(channels.len(), 2);
    assert_eq!(channels[0].peer_id, "02aa");
    assert_eq!(channels[0].total_msat, 5_000_000_000);
    assert_eq!(channels[0].funding_txid.as_deref(), Some("txid-1"));
    assert_eq!(channels[1].funding_txid, None);
    let (method, params) = server.received_method_params(1);
    assert_eq!(method, "listpeerchannels");
    assert_eq!(params.get("id").and_then(Value::as_str), Some("02aa"));

    assert_eq!(chain.opening_feerate_perkw().unwrap(), 1234);

    // Only confirmed + unreserved counts: 2_000_000 msat = 2000 sats.
    assert_eq!(chain.confirmed_unreserved_sats().unwrap(), 2000);
}

#[test]
fn connect_sends_the_target_and_maps_rejection_cleanly() {
    let (_rt, server) = with_server(vec![
        FakeBehavior::Success(json!({"id": "02aa"})),
        FakeBehavior::Rejected {
            code: 401,
            message: "unable to connect".to_string(),
        },
    ]);
    let chain = ClnChainAdapter::new(server.path.clone(), timeout()).unwrap();
    chain.connect("02aa@203.0.113.9:9735").expect("connected");
    let (method, params) = server.received_method_params(0);
    assert_eq!(method, "connect");
    assert_eq!(
        params.get("id").and_then(Value::as_str),
        Some("02aa@203.0.113.9:9735")
    );
    assert!(chain.connect("02bb").is_err());
}

// ---------------------------------------------------------- fund_channel

#[test]
fn fund_channel_success_sends_exact_params_and_returns_the_txid() {
    let (_rt, server) = with_server(vec![FakeBehavior::Success(
        json!({"txid": "aabb", "channel_id": "cc"}),
    )]);
    let chain = ClnChainAdapter::new(server.path.clone(), timeout()).unwrap();
    let outcome = chain
        .fund_channel("02aa", 1_000_000, Feerate::Slow)
        .expect("submitted");
    match outcome {
        FundChannelOutcome::Funded(result) => {
            assert_eq!(result.txid.as_deref(), Some("aabb"));
        }
        other => panic!("expected Funded, got {other:?}"),
    }
    let (method, params) = server.received_method_params(0);
    assert_eq!(method, "fundchannel");
    assert_eq!(params.get("id").and_then(Value::as_str), Some("02aa"));
    assert_eq!(
        params.get("amount").and_then(Value::as_str),
        Some("1000000sat")
    );
    assert_eq!(params.get("feerate").and_then(Value::as_str), Some("slow"));
}

#[test]
fn fund_channel_definite_rejection_is_a_clean_error() {
    let (_rt, server) = with_server(vec![FakeBehavior::Rejected {
        code: 301,
        message: "Cannot afford transaction".to_string(),
    }]);
    let chain = ClnChainAdapter::new(server.path.clone(), timeout()).unwrap();
    let err = chain
        .fund_channel("02aa", 1_000_000, Feerate::Normal)
        .unwrap_err();
    assert!(
        err.to_string().contains("rejected"),
        "a JSON-RPC error object is a KNOWN rejection: {err}"
    );
}

#[test]
fn fund_channel_disconnect_after_receipt_is_outcome_unknown() {
    let (_rt, server) = with_server(vec![FakeBehavior::DisconnectAfterReceipt]);
    let chain = ClnChainAdapter::new(server.path.clone(), timeout()).unwrap();
    let outcome = chain
        .fund_channel("02aa", 1_000_000, Feerate::Urgent)
        .expect("ambiguity is a typed outcome, not an Err");
    assert!(
        matches!(outcome, FundChannelOutcome::OutcomeUnknown { .. }),
        "bytes were received by the node — the outcome is UNKNOWN: {outcome:?}"
    );
}

#[test]
fn fund_channel_timeout_after_receipt_is_outcome_unknown_with_no_resubmit() {
    let (_rt, server) = with_server(vec![FakeBehavior::HangForever]);
    let chain = ClnChainAdapter::new(server.path.clone(), Duration::from_millis(400)).unwrap();
    let outcome = chain
        .fund_channel("02aa", 1_000_000, Feerate::Normal)
        .expect("typed outcome");
    assert!(matches!(outcome, FundChannelOutcome::OutcomeUnknown { .. }));
    assert_eq!(
        server.connections.load(Ordering::SeqCst),
        1,
        "the adapter must NOT retry an irreversible submit on its own"
    );
}

#[test]
fn fund_channel_malformed_reply_is_outcome_unknown() {
    let (_rt, server) = with_server(vec![FakeBehavior::Malformed]);
    let chain = ClnChainAdapter::new(server.path.clone(), timeout()).unwrap();
    let outcome = chain
        .fund_channel("02aa", 1_000_000, Feerate::Normal)
        .expect("typed outcome");
    assert!(matches!(outcome, FundChannelOutcome::OutcomeUnknown { .. }));
}

#[test]
fn unreachable_socket_is_a_clean_pre_submit_failure() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("no-such-lightning-rpc");
    let chain = ClnChainAdapter::new(missing.clone(), Duration::from_millis(400)).unwrap();
    let err = chain
        .fund_channel("02aa", 1_000_000, Feerate::Normal)
        .unwrap_err();
    assert!(
        err.to_string().contains("not submitted"),
        "no socket = nothing could have reached lightningd: {err}"
    );
    let signer = ClnSigner::new(missing, Duration::from_millis(400)).unwrap();
    assert!(signer.signmessage("m").is_err());
}
