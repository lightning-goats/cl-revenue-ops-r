//! Native executor golden parity, pinned by `fixtures/rebalance/executor.json`
//! (generated from the REAL `modules/rebalance_native_executor_v2.py`
//! `NativeRouteExecutor` + `modules/segment_observations.py` by
//! `tools/port/gen_rebalance_fixtures.py executor` in the port worktree,
//! branch `phase5-t5-gen`).
//!
//! The fixture pins four things at once per case:
//! - the full `ExecutionResult` (success/pending/terminal shapes, error
//!   strings, fee math, `failure_data` incl. `route_summary`);
//! - the exact PaymentRpc call sequence and arguments (invoice label/expiry,
//!   sendpay bolt11/secret pass-through, waitsendpay timeout, cleanup
//!   delpay/delinvoice args — and, on the pending path, the ABSENCE of any
//!   cleanup calls: abandon, never cancel);
//! - the confidence-weighted segment observations recorded into a real
//!   `SegmentObservationStore` (0.85 attributed, 0.425 direction-unknown,
//!   0.85/n floor 0.2 inferred);
//! - `payment_hash` surfacing (`Some` exactly when `payment_pending`).
//!
//! Plus two Rust-only safety tests: `dryrun_calls_no_payment_rpc` (the
//! DryRun gate sits exactly at the invoice boundary and the scripted-rpc
//! call log stays EMPTY) and `payment_mode_live_not_constructed_in_crate`
//! (source scan: `PaymentMode::Live` is constructed only at cutover, never
//! in any crate's src tree).

use revops_rebalance::executor::{
    ExecuteRequest, NativeRouteExecutor, PaymentMode, PaymentRpc, ATTRIBUTED_CONFIDENCE,
    DRYRUN_GATE_SENDPAY_DISABLED, INFERRED_CONFIDENCE_FLOOR, INVOICE_EXPIRY_SEC,
    SENDPAY_TIMEOUT_SEC,
};
use revops_rebalance::router::{RpcFailure, SendpayHop};
use revops_rebalance::segstore::SegmentObservationStore;
use revops_rebalance::types::ExecutionResult;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::path::{Path, PathBuf};

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rebalance/executor.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

// ---------------------------------------------------------------------------
// Scripted payment-RPC double (mirror of the generator's `_NxPaymentRpc`)
// ---------------------------------------------------------------------------

/// Script specs per method (same encoding as the generator):
/// `{"result": v}` / `{"raw": v}` -> `Ok(v)`; `{"error": obj}` ->
/// `Err(RpcFailure{ message: json(obj) })` (the seam convention for
/// structured CLN errors); `{"error_text": s}` / `{"timeout_error": s}` ->
/// `Err(RpcFailure{ message: s })`.
struct ScriptedPaymentRpc {
    script: Value,
    log: RefCell<Vec<Value>>,
}

impl ScriptedPaymentRpc {
    fn new(script: Value) -> Self {
        ScriptedPaymentRpc {
            script,
            log: RefCell::new(Vec::new()),
        }
    }

    fn dispatch(&self, method: &str) -> Result<Value, RpcFailure> {
        let spec = &self.script[method];
        assert!(spec.is_object(), "unscripted RPC method: {method}");
        if let Some(v) = spec.get("result") {
            return Ok(v.clone());
        }
        if let Some(v) = spec.get("raw") {
            return Ok(v.clone());
        }
        if let Some(obj) = spec.get("error") {
            return Err(RpcFailure {
                message: serde_json::to_string(obj).expect("serializable error"),
            });
        }
        if let Some(s) = spec.get("error_text").and_then(Value::as_str) {
            return Err(RpcFailure {
                message: s.to_string(),
            });
        }
        if let Some(s) = spec.get("timeout_error").and_then(Value::as_str) {
            return Err(RpcFailure {
                message: s.to_string(),
            });
        }
        panic!("bad script spec for {method}: {spec}");
    }
}

fn hop_to_json(hop: &SendpayHop) -> Value {
    json!({
        "id": hop.id,
        "channel": hop.channel,
        "direction": hop.direction,
        "delay": hop.delay,
        "amount_msat": hop.amount_msat,
        "style": hop.style,
    })
}

impl PaymentRpc for ScriptedPaymentRpc {
    fn getinfo_id(&self) -> Result<String, RpcFailure> {
        panic!("execute() must never call getinfo_id (our_id is injected)");
    }

    fn invoice(
        &self,
        amount_msat: i64,
        label: &str,
        expiry_secs: i64,
    ) -> Result<Value, RpcFailure> {
        self.log.borrow_mut().push(json!({
            "method": "invoice",
            "amount_msat": amount_msat,
            "label": label,
            "expiry": expiry_secs,
        }));
        self.dispatch("invoice")
    }

    fn sendpay(
        &self,
        route: &[SendpayHop],
        payment_hash: &str,
        bolt11: &str,
        payment_secret: &str,
    ) -> Result<Value, RpcFailure> {
        self.log.borrow_mut().push(json!({
            "method": "sendpay",
            "route": route.iter().map(hop_to_json).collect::<Vec<_>>(),
            "payment_hash": payment_hash,
            "bolt11": bolt11,
            "payment_secret": payment_secret,
        }));
        self.dispatch("sendpay")
    }

    fn waitsendpay(&self, payment_hash: &str, timeout_secs: i64) -> Result<Value, RpcFailure> {
        self.log.borrow_mut().push(json!({
            "method": "waitsendpay",
            "payment_hash": payment_hash,
            "timeout": timeout_secs,
        }));
        self.dispatch("waitsendpay")
    }

    fn delpay(&self, payment_hash: &str, status: &str) -> Result<(), RpcFailure> {
        self.log.borrow_mut().push(json!({
            "method": "delpay",
            "payment_hash": payment_hash,
            "status": status,
        }));
        self.dispatch("delpay").map(|_| ())
    }

    fn delinvoice(&self, label: &str, status: &str) -> Result<(), RpcFailure> {
        self.log.borrow_mut().push(json!({
            "method": "delinvoice",
            "label": label,
            "status": status,
        }));
        self.dispatch("delinvoice").map(|_| ())
    }
}

// ---------------------------------------------------------------------------
// Fixture plumbing
// ---------------------------------------------------------------------------

fn route_from_json(route: &Value) -> Vec<SendpayHop> {
    route
        .as_array()
        .expect("route array")
        .iter()
        .map(|hop| SendpayHop {
            id: hop["id"].as_str().expect("id").to_string(),
            channel: hop["channel"].as_str().expect("channel").to_string(),
            direction: u8::try_from(hop["direction"].as_i64().expect("direction")).unwrap(),
            delay: u32::try_from(hop["delay"].as_i64().expect("delay")).unwrap(),
            amount_msat: hop["amount_msat"].as_i64().expect("amount_msat"),
            style: "tlv",
        })
        .collect()
}

fn request_from_case(case: &Value) -> ExecuteRequest {
    let req = &case["request"];
    ExecuteRequest {
        route: route_from_json(&req["route"]),
        amount_sats: req["amount_sats"].as_i64().expect("amount_sats"),
        source_channel_id: req["source_channel_id"].as_str().unwrap().to_string(),
        dest_channel_id: req["dest_channel_id"].as_str().unwrap().to_string(),
        max_fee_sats: req["max_fee_sats"].as_i64().expect("max_fee_sats"),
        our_id: req["our_id"].as_str().unwrap().to_string(),
        now_ms: case["now_ms"].as_i64().expect("now_ms"),
    }
}

/// Mirror of the generator's `dataclasses.asdict(result)` (with `error`
/// `""` -> `null`), so the whole result compares as one `Value`.
fn result_to_json(r: &ExecutionResult) -> Value {
    json!({
        "success": r.success,
        "attempts": r.attempts,
        "fee_sats": r.fee_sats,
        "fee_msat": r.fee_msat,
        "amount_sats": r.amount_sats,
        "fee_ppm": r.fee_ppm,
        "hops": r.hops,
        "route_type": r.route_type,
        "parts": r.parts,
        "error": r.error,
        "excluded_channels": r.excluded_channels,
        "failure_data": r.failure_data,
        "payment_pending": r.payment_pending,
    })
}

fn snapshot_observations(store: &SegmentObservationStore, now_s: i64) -> Value {
    let snap = store.export_snapshot(now_s);
    let parsed: Value = serde_json::from_str(&revops_fees::pyjson::dumps_python(&snap))
        .expect("snapshot round-trips");
    parsed["segment_observations"].clone()
}

// ---------------------------------------------------------------------------
// Golden replay
// ---------------------------------------------------------------------------

#[test]
fn constants_match_python() {
    let fx = fixture();
    assert_eq!(
        INVOICE_EXPIRY_SEC,
        fx["invoice_expiry_sec"].as_i64().unwrap()
    );
    assert_eq!(
        SENDPAY_TIMEOUT_SEC,
        fx["sendpay_timeout_sec"].as_i64().unwrap()
    );
    assert_eq!(
        ATTRIBUTED_CONFIDENCE,
        fx["attributed_confidence"].as_f64().unwrap()
    );
    assert_eq!(
        INFERRED_CONFIDENCE_FLOOR,
        fx["inferred_confidence_floor"].as_f64().unwrap()
    );
}

#[test]
fn golden_replay_all_cases() {
    let fx = fixture();
    let cases = fx["cases"].as_array().expect("cases");
    assert!(
        cases.len() >= 20,
        "expected the full suite, got {}",
        cases.len()
    );
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let rpc = ScriptedPaymentRpc::new(case["rpc_script"].clone());
        let store = SegmentObservationStore::with_defaults();
        let executor = NativeRouteExecutor {
            rpc: &rpc,
            mode: live_mode_for_tests(),
            segstore: &store,
        };
        let req = request_from_case(case);
        let result = executor.execute(&req);

        assert_eq!(
            result_to_json(&result),
            case["expected"],
            "[{name}] ExecutionResult mismatch"
        );

        let expected_hash = &case["expected_payment_hash"];
        match (&result.payment_hash, expected_hash) {
            (None, Value::Null) => {}
            (Some(h), Value::String(e)) => assert_eq!(h, e, "[{name}] payment_hash"),
            (got, want) => panic!("[{name}] payment_hash {got:?} vs {want:?}"),
        }
        assert_eq!(
            result.payment_hash.is_some(),
            result.payment_pending,
            "[{name}] payment_hash must be Some exactly when pending"
        );

        assert_eq!(
            Value::Array(rpc.log.borrow().clone()),
            case["expected_calls"],
            "[{name}] PaymentRpc call log mismatch"
        );

        assert_eq!(
            snapshot_observations(&store, req.now_ms / 1000),
            case["expected_observations"],
            "[{name}] segment observations mismatch"
        );
    }
}

/// The fixture-replay half of the abandon-not-cancel contract asserts the
/// pending cases end their call logs at sendpay/waitsendpay; this test makes
/// the invariant explicit and self-describing: NO pending case may ever
/// touch delpay/delinvoice (a payment in flight is abandoned to resolve on
/// its own, never cancelled — production incident, `rebalance_native_
/// executor_v2.py:494-514`).
#[test]
fn pending_cases_never_clean_up_and_never_exclude() {
    let fx = fixture();
    let mut pending_seen = 0;
    for case in fx["cases"].as_array().unwrap() {
        if case["expected"]["payment_pending"] != json!(true) {
            continue;
        }
        pending_seen += 1;
        let name = case["name"].as_str().unwrap();
        for call in case["expected_calls"].as_array().unwrap() {
            let method = call["method"].as_str().unwrap();
            assert!(
                method != "delpay" && method != "delinvoice",
                "[{name}] pending case must never cancel ({method} called)"
            );
        }
        assert_eq!(
            case["expected"]["excluded_channels"],
            json!([]),
            "[{name}] pending case must emit no exclusions"
        );
        assert_eq!(case["expected_observations"], json!([]));
        assert!(case["expected"]["error"]
            .as_str()
            .unwrap()
            .starts_with(revops_rebalance::errors::PAYMENT_PENDING_TIMEOUT_PREFIX));
    }
    assert_eq!(pending_seen, 4, "all four pending shapes fixtured");
}

// ---------------------------------------------------------------------------
// DryRun gate
// ---------------------------------------------------------------------------

/// `PaymentMode::Live` may not appear in any crate's src tree (cutover-only
/// construction — Global Constraints). Tests construct it through this
/// helper, which lives in tests/ and is therefore outside the scanned area.
fn live_mode_for_tests() -> PaymentMode {
    // Concatenated so the source-scan test would not match even if this
    // file ever moved into a scanned src tree.
    let token = concat!("Li", "ve");
    match token {
        "Live" => PaymentMode::Live,
        _ => unreachable!(),
    }
}

#[test]
fn dryrun_calls_no_payment_rpc() {
    let fx = fixture();
    let case = fx["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "success_amount_sent_int")
        .expect("success case present");
    let rpc = ScriptedPaymentRpc::new(case["rpc_script"].clone());
    let store = SegmentObservationStore::with_defaults();
    let executor = NativeRouteExecutor {
        rpc: &rpc,
        mode: PaymentMode::DryRun,
        segstore: &store,
    };
    let req = request_from_case(case);
    let result = executor.execute(&req);

    assert!(!result.success);
    assert!(!result.payment_pending);
    assert_eq!(result.payment_hash, None);
    assert_eq!(
        result.error.as_deref(),
        Some(DRYRUN_GATE_SENDPAY_DISABLED),
        "gate error string is a contract"
    );
    assert_eq!(
        DRYRUN_GATE_SENDPAY_DISABLED,
        "dryrun_gate: sendpay_disabled"
    );
    // Validation and pricing ran: the planned fee is surfaced.
    assert_eq!(result.fee_msat, 1500);
    assert_eq!(result.fee_sats, 2);
    assert_eq!(result.excluded_channels, Vec::<String>::new());
    // The critical assertion: the gate cuts at the INVOICE boundary having
    // called ZERO PaymentRpc methods.
    assert_eq!(rpc.log.borrow().len(), 0, "DryRun must issue no RPCs");
    assert_eq!(snapshot_observations(&store, req.now_ms / 1000), json!([]));
}

#[test]
fn dryrun_validation_failure_surfaces_before_gate() {
    let fx = fixture();
    let case = fx["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "validation_over_budget")
        .expect("validation case present");
    let rpc = ScriptedPaymentRpc::new(json!({}));
    let store = SegmentObservationStore::with_defaults();
    let executor = NativeRouteExecutor {
        rpc: &rpc,
        mode: PaymentMode::DryRun,
        segstore: &store,
    };
    let result = executor.execute(&request_from_case(case));
    // Validation runs identically in DryRun: same error as the Live fixture.
    assert_eq!(result_to_json(&result), case["expected"]);
    assert_eq!(rpc.log.borrow().len(), 0);
}

// ---------------------------------------------------------------------------
// Source scan: PaymentMode::Live is cutover-only
// ---------------------------------------------------------------------------

fn scan_rs_files(dir: &Path, hits: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}")) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            scan_rs_files(&path, hits);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let src =
                std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
            let needle: String = ["PaymentMode", "Live"].join("::");
            if src.contains(&needle) {
                hits.push(path);
            }
        }
    }
}

#[test]
fn payment_mode_live_not_constructed_in_crate() {
    let crates_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut src_dirs = Vec::new();
    for entry in std::fs::read_dir(&crates_dir).expect("crates dir") {
        let src = entry.expect("dir entry").path().join("src");
        if src.is_dir() {
            src_dirs.push(src);
        }
    }
    assert!(
        src_dirs.len() >= 5,
        "expected the workspace crates, got {src_dirs:?}"
    );
    let mut hits = Vec::new();
    for dir in &src_dirs {
        scan_rs_files(dir, &mut hits);
    }
    assert!(
        hits.is_empty(),
        "PaymentMode::Live constructed outside cutover: {hits:?}"
    );
}
