//! Task 60 slice 5: the three operator RPCs' Python-equivalent contracts
//! (validation strings verbatim against cl-revenue-ops.py; uninitialized
//! arms exact; rate limiter parity; success arms pinned as Rust shapes).

use revops::rebalance_adapters::ReconcileLookup;
use revops::rebalance_owner::{
    spawn_rebalance_owner, RebalanceOwnerConfig, RebalanceOwnerDeps, RebalanceOwnerHandle,
};
use revops::rpc_rebalance_ops::{
    handle_manual_rebalance, handle_rebalance_cycle, handle_rebalance_debug, ForceRateLimiter,
};
use revops_db::owner::spawn_read_write;
use revops_rebalance::engine::CycleResult;
use revops_rebalance::executor::DRYRUN_GATE_SENDPAY_DISABLED;
use revops_rebalance::facade::{CandidateExecutor, FacadeRpc};
use revops_rebalance::modes::EngineKwargs;
use revops_rebalance::router::RpcFailure;
use revops_rebalance::types::{ExecutionResult, RebalanceCandidate};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

struct ScriptedEngine {
    results: Mutex<Vec<ExecutionResult>>,
}

impl CandidateExecutor for ScriptedEngine {
    fn execute_candidate(
        &self,
        _candidate: &RebalanceCandidate,
        _rebalance_id: i64,
        _kw: EngineKwargs,
    ) -> ExecutionResult {
        self.results
            .lock()
            .unwrap()
            .pop()
            .expect("scripted engine exhausted")
    }
    fn run_cycle(&self) -> CycleResult {
        CycleResult::default()
    }
}

struct HealthyEvidence;
impl FacadeRpc for HealthyEvidence {
    fn get_funds(&self) -> Result<Value, RpcFailure> {
        Ok(json!({"channels": []}))
    }
    fn get_peer_channels(&self) -> Result<Value, RpcFailure> {
        Ok(json!({"channels": []}))
    }
    fn get_channels_source(&self, _source: &str) -> Result<Value, RpcFailure> {
        Ok(json!({"channels": []}))
    }
    fn get_node_id(&self) -> Result<String, RpcFailure> {
        Ok("02aa".repeat(16))
    }
}

struct EmptyReconcile;
impl ReconcileLookup for EmptyReconcile {
    fn listsendpays(&self, _payment_hash: &str) -> Result<Value, RpcFailure> {
        Ok(json!({"payments": []}))
    }
}

fn config() -> RebalanceOwnerConfig {
    RebalanceOwnerConfig {
        daily_budget_sats: 5_000_000,
        budget_window_hours: 24,
        rebalance_max_amount: 5_000_000,
        pair_cooldown_seconds: 3_600,
    }
}

async fn owner_with_engine(
    dir: &tempfile::TempDir,
    engine: Option<Arc<dyn CandidateExecutor>>,
) -> RebalanceOwnerHandle {
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    spawn_rebalance_owner(RebalanceOwnerDeps {
        engine,
        evidence: Arc::new(HealthyEvidence),
        store,
        reconcile: Arc::new(EmptyReconcile),
        config: config(),
        clock: Box::new(|| 1_800_000_000),
    })
}

fn dryrun_engine() -> Arc<dyn CandidateExecutor> {
    let mut r = ExecutionResult {
        success: false,
        attempts: 1,
        fee_sats: 3,
        fee_msat: 2_500,
        fee_ppm: 12,
        hops: 3,
        parts: 1,
        error: Some(DRYRUN_GATE_SENDPAY_DISABLED.to_string()),
        amount_sats: 250_000,
        payment_pending: false,
        payment_hash: None,
        excluded_channels: Vec::new(),
        route_type: "native",
        failure_data: json!({}),
    };
    r.amount_sats = 250_000;
    Arc::new(ScriptedEngine {
        results: Mutex::new(vec![r]),
    })
}

/// Verbatim Python strings on the manual validation path.
#[tokio::test]
async fn manual_validation_strings_are_python_verbatim() {
    let dir = tempfile::tempdir().unwrap();
    let owner = owner_with_engine(&dir, Some(dryrun_engine())).await;
    let limiter = ForceRateLimiter::production();

    // Usage.
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        0.0,
        None,
        None,
        None,
        None,
        false,
    )
    .await;
    assert_eq!(
        v["error"],
        "usage: revenue-rebalance from_channel to_channel amount_sats [max_fee_sats] [force=false]"
    );

    // SCID format, string repr quoting.
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        1.0,
        Some(&json!("not-a-scid")),
        Some(&json!("200x2x0")),
        Some(&json!(1000)),
        None,
        false,
    )
    .await;
    assert_eq!(v["status"], "error");
    assert_eq!(
        v["error"],
        "Invalid channel format for 'not-a-scid'. Use SCID format (e.g., 123x456x789)."
    );

    // P1-012: non-string channel arg reprs without quotes.
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        2.0,
        Some(&json!(123)),
        Some(&json!("200x2x0")),
        Some(&json!(1000)),
        None,
        false,
    )
    .await;
    assert_eq!(
        v["error"],
        "Invalid channel format for 123. Use SCID format (e.g., 123x456x789)."
    );

    // Amount coercion + minimum.
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        3.0,
        Some(&json!("100x1x0")),
        Some(&json!("200x2x0")),
        Some(&json!("abc")),
        None,
        false,
    )
    .await;
    assert_eq!(v["error"], "amount_sats must be an integer");
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        4.0,
        Some(&json!("100x1x0")),
        Some(&json!("200x2x0")),
        Some(&json!(0)),
        None,
        false,
    )
    .await;
    assert_eq!(v["error"], "amount_sats must be at least 1");

    // Hard cap shape: "rejected even under force."
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        5.0,
        Some(&json!("100x1x0")),
        Some(&json!("200x2x0")),
        Some(&json!(5_000_001)),
        None,
        true,
    )
    .await;
    assert_eq!(v["status"], "error");
    assert_eq!(v["requested_sats"], 5_000_001);
    assert_eq!(v["max_amount_sats"], 5_000_000);
    assert_eq!(
        v["error"],
        "amount_sats 5000001 exceeds hard rebalance cap 5000000 (rebalance_max_amount); \
         rejected even under force."
    );

    // max_fee_sats coercion.
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        6.0,
        Some(&json!("100x1x0")),
        Some(&json!("200x2x0")),
        Some(&json!(1000)),
        Some(&json!("xyz")),
        false,
    )
    .await;
    assert_eq!(v["error"], "max_fee_sats must be an integer or null");
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        7.0,
        Some(&json!("100x1x0")),
        Some(&json!("200x2x0")),
        Some(&json!(1000)),
        Some(&json!(-1)),
        false,
    )
    .await;
    assert_eq!(v["error"], "max_fee_sats must be non-negative");
}

/// The rate limiter refuses BOTH force values with the verbatim message,
/// and the window drains.
#[tokio::test]
async fn rate_limit_applies_to_both_force_values_verbatim() {
    let limiter = ForceRateLimiter::new(2, 60.0);
    assert!(limiter.check_rate_limit("revenue-rebalance", 100.0).is_ok());
    assert!(limiter.check_rate_limit("revenue-rebalance", 101.0).is_ok());
    let msg = limiter
        .check_rate_limit("revenue-rebalance", 102.0)
        .expect_err("third call in window refuses");
    assert_eq!(
        msg,
        "Rate limit exceeded for force=revenue-rebalance. Try again in 58s. (2 calls per 60s)"
    );
    // Window drains.
    assert!(limiter.check_rate_limit("revenue-rebalance", 161.0).is_ok());

    // Through the handler: refusal envelope is {"status":"error",...} and
    // hits force and non-force alike.
    let dir = tempfile::tempdir().unwrap();
    let owner = owner_with_engine(&dir, Some(dryrun_engine())).await;
    let tight = ForceRateLimiter::new(0, 60.0);
    for force in [false, true] {
        let v = handle_manual_rebalance(
            Some(&owner),
            &tight,
            5_000_000,
            200.0,
            Some(&json!("100x1x0")),
            Some(&json!("200x2x0")),
            Some(&json!(1000)),
            None,
            force,
        )
        .await;
        assert_eq!(v["status"], "error");
        assert!(
            v["error"]
                .as_str()
                .unwrap()
                .starts_with("Rate limit exceeded for force=revenue-rebalance."),
            "{v}"
        );
    }
}

/// Uninitialized arms: cycle/debug say "Rebalancer not initialized",
/// manual says "Plugin not fully initialized" -- both for a missing owner
/// AND for an owner without an assembled engine (production pre-cutover).
#[tokio::test]
async fn uninitialized_arms_match_python_exactly() {
    let limiter = ForceRateLimiter::production();
    // Missing owner entirely.
    let v = handle_rebalance_cycle(None).await;
    assert_eq!(v, json!({"error": "Rebalancer not initialized"}));
    let v = handle_rebalance_debug(None, None, None, false, true, None).await;
    assert_eq!(v, json!({"error": "Rebalancer not initialized"}));
    let v = handle_manual_rebalance(
        None,
        &limiter,
        5_000_000,
        0.0,
        Some(&json!("100x1x0")),
        Some(&json!("200x2x0")),
        Some(&json!(1000)),
        None,
        false,
    )
    .await;
    assert_eq!(v, json!({"error": "Plugin not fully initialized"}));

    // Owner without an engine (the production state today).
    let dir = tempfile::tempdir().unwrap();
    let owner = owner_with_engine(&dir, None).await;
    let v = handle_rebalance_cycle(Some(&owner)).await;
    assert_eq!(v, json!({"error": "Rebalancer not initialized"}));
    let v = handle_rebalance_debug(Some(&owner), None, None, false, true, None).await;
    assert_eq!(v, json!({"error": "Rebalancer not initialized"}));
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        1.0,
        Some(&json!("100x1x0")),
        Some(&json!("200x2x0")),
        Some(&json!(1000)),
        None,
        false,
    )
    .await;
    assert_eq!(v, json!({"error": "Plugin not fully initialized"}));
}

/// Debug filter coercion parity: peer lowercased/trimmed, hot markers
/// forced off under summary_only, non-int max_candidates coerces to 0
/// (surfaced as null) -- never an exception out of a diagnostic.
#[tokio::test]
async fn debug_filter_coercion_is_python_parity() {
    let dir = tempfile::tempdir().unwrap();
    let owner = owner_with_engine(&dir, Some(dryrun_engine())).await;
    let v = handle_rebalance_debug(
        Some(&owner),
        Some(&json!("  100x1x0  ")),
        Some(&json!("  02AABB  ")),
        true,
        true,
        Some(&json!("not-a-number")),
    )
    .await;
    assert_eq!(v["executor_available"], true);
    assert_eq!(v["dry_run"], true);
    assert_eq!(v["filters"]["channel_id"], "100x1x0");
    assert_eq!(v["filters"]["peer_id"], "02aabb");
    assert_eq!(v["filters"]["summary_only"], true);
    assert_eq!(
        v["filters"]["include_hot_markers"], false,
        "summary_only forces hot markers off"
    );
    assert_eq!(v["filters"]["max_candidates"], Value::Null);
    assert_eq!(v["rust_owner"]["engine_assembled"], true);
}

/// The manual success-envelope (Rust shape, pinned): a dry-run submission
/// reports status error with the clean-failure outcome and request id.
#[tokio::test]
async fn manual_dryrun_envelope_is_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let owner = owner_with_engine(&dir, Some(dryrun_engine())).await;
    let limiter = ForceRateLimiter::production();
    let v = handle_manual_rebalance(
        Some(&owner),
        &limiter,
        5_000_000,
        0.0,
        Some(&json!("100x1x0")),
        Some(&json!("200x2x0")),
        Some(&json!(250_000)),
        Some(&json!(300)),
        false,
    )
    .await;
    assert_eq!(v["status"], "error");
    assert_eq!(v["outcome"], "clean_failure_before_write");
    assert_eq!(v["error"], DRYRUN_GATE_SENDPAY_DISABLED);
    assert!(v["request_id"].as_str().unwrap().starts_with("manual-"));
}
