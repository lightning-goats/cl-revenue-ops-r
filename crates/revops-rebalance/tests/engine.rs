//! Task 7 engine orchestration tests — scripted store/router/executor
//! doubles per the plan's Step 1 list (phase5 plan, Task 7).
//!
//! Python truth: `modules/rebalance_engine_v2.py` (branch `port`,
//! v2.18.1). No live CLN anywhere: every RPC-shaped seam is a scripted
//! double.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use revops_db::budget::ReserveRequest;
use revops_fees::pyjson::OValue;
use revops_rebalance::engine::{
    native_partial_amounts, BudgetBlock, CycleResult, DryRunStore, EngineClock, EngineConfig,
    EngineDeps, EngineRouter, EvProvider, EvTerms, HistoryRow, HistorySuccess, InflightDests,
    PairCandidate, PairExecutor, PeerPolicy, PolicyManager, RebalanceCost, RebalanceEngine,
    RebalanceStore, ReconcileRpc, ReservationId, RouterFactory, SnapshotProvider,
};
use revops_rebalance::errors::{
    CYCLE_ALREADY_RUNNING, DEST_INFLIGHT, ENGINE_BUSY, LOCAL_BUDGET_BLOCK,
};
use revops_rebalance::executor::{ExecuteRequest, PaymentRpc};
use revops_rebalance::modes::{engine_kwargs, EngineKwargs};
use revops_rebalance::planner::PlannerChannel;
use revops_rebalance::router::{PlannedPairCtx, RouteResult, RpcFailure, SendpayHop};
use revops_rebalance::segstore::SegmentObservationStore;
use revops_rebalance::types::{ExecutionResult, RebalanceCandidate};

// ---------------------------------------------------------------------------
// Scripted doubles
// ---------------------------------------------------------------------------

/// Call-log entry for the scripted store: formatted strings keep assertions
/// readable ("reserve_budget:2000", "update_history:1:skipped:...").
#[derive(Default)]
struct StoreState {
    calls: Vec<String>,
    next_id: i64,
    reserve_script: VecDeque<Result<(), BudgetBlock>>,
    pending_rows: Vec<HistoryRow>,
}

struct MockStore {
    state: Mutex<StoreState>,
}

impl MockStore {
    fn new() -> Arc<Self> {
        Arc::new(MockStore {
            state: Mutex::new(StoreState {
                next_id: 0,
                ..StoreState::default()
            }),
        })
    }

    fn script_reserve(&self, outcome: Result<(), BudgetBlock>) {
        self.state.lock().unwrap().reserve_script.push_back(outcome);
    }

    fn push_pending_row(&self, row: HistoryRow) {
        self.state.lock().unwrap().pending_rows.push(row);
    }

    fn calls(&self) -> Vec<String> {
        self.state.lock().unwrap().calls.clone()
    }

    fn calls_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter(|c| c.starts_with(prefix))
            .collect()
    }
}

impl RebalanceStore for MockStore {
    fn insert_or_adopt_history(&self, row: &HistoryRow) -> i64 {
        let mut st = self.state.lock().unwrap();
        let id = if row.id > 0 {
            row.id
        } else {
            st.next_id += 1;
            st.next_id
        };
        st.calls.push(format!(
            "insert_or_adopt_history:{id}:{}:{}:{}:{}",
            row.from_channel, row.to_channel, row.amount_sats, row.status
        ));
        id
    }

    fn update_history(&self, id: i64, status: &str, err: Option<&str>) {
        self.state.lock().unwrap().calls.push(format!(
            "update_history:{id}:{status}:{}",
            err.unwrap_or("")
        ));
    }

    fn update_history_success(&self, id: i64, update: &HistorySuccess) {
        self.state.lock().unwrap().calls.push(format!(
            "update_history_success:{id}:fee_sats={}:fee_msat={}:amount={:?}:ratio={:?}",
            update.actual_fee_sats,
            update.actual_fee_msat,
            update.amount_sats,
            update.post_local_ratio
        ));
    }

    fn update_history_pending_settlement(&self, id: i64, error: &str, payment_hash: &str) {
        self.state.lock().unwrap().calls.push(format!(
            "update_history_pending_settlement:{id}:{payment_hash}:{error}"
        ));
    }

    fn reserve_budget(&self, req: &ReserveRequest) -> Result<ReservationId, BudgetBlock> {
        let mut st = self.state.lock().unwrap();
        st.calls.push(format!(
            "reserve_budget:{}:{}:{}",
            req.reservation_id,
            req.amount_sats,
            req.effective_budget_sats.unwrap_or(-1)
        ));
        match st.reserve_script.pop_front() {
            None | Some(Ok(())) => Ok(ReservationId(req.reservation_id.clone())),
            Some(Err(block)) => Err(block),
        }
    }

    fn mark_budget_spent(&self, rid: &ReservationId, actual_fee_sats: i64) {
        self.state
            .lock()
            .unwrap()
            .calls
            .push(format!("mark_budget_spent:{}:{actual_fee_sats}", rid.0));
    }

    fn release_reservation(&self, rid: &ReservationId) {
        self.state
            .lock()
            .unwrap()
            .calls
            .push(format!("release_reservation:{}", rid.0));
    }

    fn record_rebalance_cost(&self, cost: &RebalanceCost) {
        self.state.lock().unwrap().calls.push(format!(
            "record_rebalance_cost:{}:{}:{}:{}:{}",
            cost.channel_id, cost.peer_id, cost.cost_sats, cost.cost_msat, cost.amount_sats
        ));
    }

    fn record_pair_failure(
        &self,
        source_channel_id: &str,
        dest_channel_id: &str,
        failure_kind: &str,
        base_cooldown_secs: i64,
        _now: i64,
    ) {
        self.state.lock().unwrap().calls.push(format!(
            "record_pair_failure:{source_channel_id}:{dest_channel_id}:{failure_kind}:{base_cooldown_secs}"
        ));
    }

    fn clear_pair_failures(&self, source_channel_id: &str, dest_channel_id: &str) {
        self.state.lock().unwrap().calls.push(format!(
            "clear_pair_failures:{source_channel_id}:{dest_channel_id}"
        ));
    }

    fn pair_cooldown_until(
        &self,
        _source_channel_id: &str,
        _dest_channel_id: &str,
        _now: i64,
    ) -> Option<i64> {
        None
    }

    fn pending_settlement_rows(&self) -> Vec<HistoryRow> {
        self.state.lock().unwrap().pending_rows.clone()
    }

    fn datastore_export(&self, key: &[&str], _payload: &OValue) {
        self.state
            .lock()
            .unwrap()
            .calls
            .push(format!("datastore_export:{}", key.join("/")));
    }
}

/// Scripted router: pops queued results per `price_pair` call. `Err` is the
/// Python-exception path; `Ok(RouteResult{success:false,..})` is the failed
/// RouteResult path — the engine must wrap the two DIFFERENTLY.
struct ScriptRouterFactory {
    script: Mutex<VecDeque<Result<RouteResult, RpcFailure>>>,
    calls: Mutex<Vec<(PlannedPairCtx, Vec<String>)>>,
    available: bool,
}

impl ScriptRouterFactory {
    fn new(script: Vec<Result<RouteResult, RpcFailure>>) -> Arc<Self> {
        Arc::new(ScriptRouterFactory {
            script: Mutex::new(script.into()),
            calls: Mutex::new(Vec::new()),
            available: true,
        })
    }
}

struct ScriptRouter<'a> {
    factory: &'a ScriptRouterFactory,
}

impl EngineRouter for ScriptRouter<'_> {
    fn price_pair(
        &mut self,
        ctx: &PlannedPairCtx,
        exclude: &[String],
    ) -> Result<RouteResult, RpcFailure> {
        self.factory
            .calls
            .lock()
            .unwrap()
            .push((ctx.clone(), exclude.to_vec()));
        match self.factory.script.lock().unwrap().pop_front() {
            Some(outcome) => outcome,
            None => Ok(RouteResult {
                success: false,
                error: Some("script_exhausted".to_string()),
                ..RouteResult::default()
            }),
        }
    }
}

impl RouterFactory for ScriptRouterFactory {
    fn begin_cycle(&self) -> Option<Box<dyn EngineRouter + '_>> {
        if !self.available {
            return None;
        }
        Some(Box::new(ScriptRouter { factory: self }))
    }
}

/// Scripted executor double, with an optional blocking gate for the
/// single-flight / timeout-abandonment tests.
struct ScriptExecutor {
    script: Mutex<VecDeque<ExecutionResult>>,
    calls: Mutex<Vec<ExecuteRequest>>,
    entered: AtomicUsize,
    gate: Mutex<bool>, // true = blocked
    cond: Condvar,
}

impl ScriptExecutor {
    fn new(script: Vec<ExecutionResult>) -> Arc<Self> {
        Arc::new(ScriptExecutor {
            script: Mutex::new(script.into()),
            calls: Mutex::new(Vec::new()),
            entered: AtomicUsize::new(0),
            gate: Mutex::new(false),
            cond: Condvar::new(),
        })
    }

    fn new_blocked(script: Vec<ExecutionResult>) -> Arc<Self> {
        let ex = Self::new(script);
        *ex.gate.lock().unwrap() = true;
        ex
    }

    fn release(&self) {
        *self.gate.lock().unwrap() = false;
        self.cond.notify_all();
    }

    fn wait_entered(&self, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.entered.load(Ordering::SeqCst) < n {
            assert!(Instant::now() < deadline, "executor never entered");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn calls(&self) -> Vec<ExecuteRequest> {
        self.calls.lock().unwrap().clone()
    }
}

impl PairExecutor for ScriptExecutor {
    fn execute(&self, req: &ExecuteRequest) -> ExecutionResult {
        self.calls.lock().unwrap().push(req.clone());
        self.entered.fetch_add(1, Ordering::SeqCst);
        let mut blocked = self.gate.lock().unwrap();
        while *blocked {
            blocked = self.cond.wait(blocked).unwrap();
        }
        drop(blocked);
        self.script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| exec_failed("script_exhausted", req.amount_sats))
    }
}

/// Payment RPC double: only `getinfo_id` (our-id self-heal) and `delpay`
/// (reconcile) are engine-reachable.
struct MockPaymentRpc {
    our_id: String,
    delpay_calls: Mutex<Vec<(String, String)>>,
}

impl MockPaymentRpc {
    fn new() -> Arc<Self> {
        Arc::new(MockPaymentRpc {
            our_id: "our-node".to_string(),
            delpay_calls: Mutex::new(Vec::new()),
        })
    }
}

impl PaymentRpc for MockPaymentRpc {
    fn getinfo_id(&self) -> Result<String, RpcFailure> {
        Ok(self.our_id.clone())
    }
    fn invoice(&self, _: i64, _: &str, _: i64) -> Result<Value, RpcFailure> {
        panic!("engine must not reach PaymentRpc::invoice (executor seam owns payment)")
    }
    fn sendpay(&self, _: &[SendpayHop], _: &str, _: &str, _: &str) -> Result<Value, RpcFailure> {
        panic!("engine must not reach PaymentRpc::sendpay")
    }
    fn waitsendpay(&self, _: &str, _: i64) -> Result<Value, RpcFailure> {
        panic!("engine must not reach PaymentRpc::waitsendpay")
    }
    fn delpay(&self, payment_hash: &str, status: &str) -> Result<(), RpcFailure> {
        self.delpay_calls
            .lock()
            .unwrap()
            .push((payment_hash.to_string(), status.to_string()));
        Ok(())
    }
    fn delinvoice(&self, _: &str, _: &str) -> Result<(), RpcFailure> {
        panic!("engine must not reach PaymentRpc::delinvoice")
    }
}

struct MockReconcileRpc {
    listsendpays: HashMap<String, Value>,
}

impl ReconcileRpc for MockReconcileRpc {
    fn listsendpays(&self, payment_hash: &str) -> Result<Value, RpcFailure> {
        self.listsendpays
            .get(payment_hash)
            .cloned()
            .ok_or_else(|| RpcFailure {
                message: format!("unknown hash {payment_hash}"),
            })
    }
    fn listpeerchannels_full(&self) -> Result<Value, RpcFailure> {
        Ok(json!({"channels": [
            {"short_channel_id": "dst-chan", "peer_id": "peer-dst"},
            {"short_channel_id": "src-chan", "peer_id": "peer-src"},
        ]}))
    }
}

struct FixedSnapshot {
    channels: Vec<PlannerChannel>,
}

impl SnapshotProvider for FixedSnapshot {
    fn channels(&self) -> Option<Vec<PlannerChannel>> {
        Some(self.channels.clone())
    }
}

struct AllowAllPolicy;

impl PolicyManager for AllowAllPolicy {
    fn get_policy(&self, _peer_id: &str) -> Result<PeerPolicy, String> {
        Ok(PeerPolicy {
            strategy: "balanced".to_string(),
            rebalance_mode: "enabled".to_string(),
        })
    }
}

struct FixedEv(EvTerms);

impl EvProvider for FixedEv {
    fn ev_terms(&self, _pair: &PairCandidate) -> Option<EvTerms> {
        Some(self.0.clone())
    }
}

struct SystemTestClock;

impl EngineClock for SystemTestClock {
    fn now(&self) -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn exec_success(amount_sats: i64, fee_sats: i64) -> ExecutionResult {
    ExecutionResult {
        success: true,
        attempts: 1,
        fee_sats,
        fee_msat: fee_sats * 1000,
        fee_ppm: 0,
        hops: 3,
        parts: 1,
        error: None,
        amount_sats,
        payment_pending: false,
        payment_hash: None,
        excluded_channels: Vec::new(),
        route_type: "native",
        failure_data: json!({}),
    }
}

fn exec_failed(error: &str, amount_sats: i64) -> ExecutionResult {
    ExecutionResult {
        success: false,
        attempts: 1,
        fee_sats: 0,
        fee_msat: 0,
        fee_ppm: 0,
        hops: 0,
        parts: 1,
        error: Some(error.to_string()),
        amount_sats,
        payment_pending: false,
        payment_hash: None,
        excluded_channels: Vec::new(),
        route_type: "native",
        failure_data: json!({}),
    }
}

fn exec_pending(amount_sats: i64, payment_hash: Option<&str>) -> ExecutionResult {
    let mut r = exec_failed("payment_pending_timeout: waitsendpay code 200", amount_sats);
    r.payment_pending = true;
    r.payment_hash = payment_hash.map(str::to_string);
    r.failure_data = match payment_hash {
        Some(h) => json!({"failure_class": "pending", "payment_hash": h}),
        None => json!({"failure_class": "pending"}),
    };
    r
}

fn exec_liquidity_failure(amount_sats: i64, excluded: &[&str]) -> ExecutionResult {
    let mut r = exec_failed(
        "native_sendpay_error: WIRE_TEMPORARY_CHANNEL_FAILURE (failcode=4103)",
        amount_sats,
    );
    r.excluded_channels = excluded.iter().map(|s| s.to_string()).collect();
    r.failure_data = json!({"failure_class": "liquidity"});
    r
}

fn route_ok(cost_sats: i64) -> Result<RouteResult, RpcFailure> {
    Ok(RouteResult {
        success: true,
        error: None,
        route_cost_sats: cost_sats,
        final_hop_fee_ppm: 0,
        hops: 3,
        route: vec![SendpayHop {
            id: "peer-src".to_string(),
            channel: "src-chan".to_string(),
            direction: 0,
            delay: 20,
            amount_msat: 1_000_000,
            style: "tlv",
        }],
        probability_ppm: 0,
    })
}

fn snapshot_two_channels() -> Vec<PlannerChannel> {
    vec![
        PlannerChannel {
            channel_id: "src-chan".to_string(),
            peer_id: "peer-src".to_string(),
            capacity_sats: 10_000_000,
            spendable_sats: 9_000_000,
            receivable_sats: 1_000_000,
            band_low: 0.35,
            band_high: 0.65,
            inbound_ppm: 0,
            value_class: "neutral".to_string(),
            urgency: 0.0,
            drain: 0.0,
            capex_remaining_sats: 0,
        },
        PlannerChannel {
            channel_id: "dst-chan".to_string(),
            peer_id: "peer-dst".to_string(),
            capacity_sats: 10_000_000,
            spendable_sats: 1_000_000,
            receivable_sats: 9_000_000,
            band_low: 0.35,
            band_high: 0.65,
            inbound_ppm: 0,
            value_class: "neutral".to_string(),
            urgency: 0.0,
            drain: 0.0,
            capex_remaining_sats: 0,
        },
    ]
}

/// Three independent surplus/deficit pairs, distinct peers throughout, all
/// identically scored (same bands/urgency/drain/value_class/inbound_ppm) so
/// the planner's stable tie-break preserves source-major generation order:
/// `[(src-chan-1,dst-chan-1), (src-chan-2,dst-chan-2), (src-chan-3,
/// dst-chan-3)]`. Used to drive a mid-cycle pricing exception on the LAST
/// pair after the first two have already priced successfully.
fn snapshot_three_pairs() -> Vec<PlannerChannel> {
    let mut channels = Vec::new();
    for i in 1..=3 {
        channels.push(PlannerChannel {
            channel_id: format!("src-chan-{i}"),
            peer_id: format!("peer-src-{i}"),
            capacity_sats: 10_000_000,
            spendable_sats: 9_000_000,
            receivable_sats: 1_000_000,
            band_low: 0.35,
            band_high: 0.65,
            inbound_ppm: 0,
            value_class: "neutral".to_string(),
            urgency: 0.0,
            drain: 0.0,
            capex_remaining_sats: 0,
        });
        channels.push(PlannerChannel {
            channel_id: format!("dst-chan-{i}"),
            peer_id: format!("peer-dst-{i}"),
            capacity_sats: 10_000_000,
            spendable_sats: 1_000_000,
            receivable_sats: 9_000_000,
            band_low: 0.35,
            band_high: 0.65,
            inbound_ppm: 0,
            value_class: "neutral".to_string(),
            urgency: 0.0,
            drain: 0.0,
            capex_remaining_sats: 0,
        });
    }
    channels
}

fn base_config() -> EngineConfig {
    EngineConfig {
        rebalance_max_amount: 2_000_000,
        max_concurrent_jobs: 5,
        pair_fee_cap_ppm: 1_000,
        daily_budget_sats: 10_000,
        ..EngineConfig::default()
    }
}

struct Harness {
    engine: RebalanceEngine,
    store: Arc<MockStore>,
    router: Arc<ScriptRouterFactory>,
    executor: Arc<ScriptExecutor>,
    payment: Arc<MockPaymentRpc>,
}

fn build_engine(
    config: EngineConfig,
    store: Arc<MockStore>,
    router: Arc<ScriptRouterFactory>,
    executor: Arc<ScriptExecutor>,
    reconcile: HashMap<String, Value>,
    ev: Option<Arc<dyn EvProvider>>,
    policy: Option<Arc<dyn PolicyManager>>,
) -> Harness {
    let payment = MockPaymentRpc::new();
    let engine = RebalanceEngine::new(EngineDeps {
        config,
        store: store.clone(),
        router_factory: router.clone(),
        executor: executor.clone(),
        payment_rpc: payment.clone(),
        reconcile_rpc: Arc::new(MockReconcileRpc {
            listsendpays: reconcile,
        }),
        segstore: Arc::new(SegmentObservationStore::with_defaults()),
        snapshot: Arc::new(FixedSnapshot {
            channels: snapshot_two_channels(),
        }),
        clock: Arc::new(SystemTestClock),
        policy,
        arbiter: None,
        ev,
    });
    Harness {
        engine,
        store,
        router,
        executor,
        payment,
    }
}

fn default_harness(
    router_script: Vec<Result<RouteResult, RpcFailure>>,
    executor_script: Vec<ExecutionResult>,
) -> Harness {
    build_engine(
        base_config(),
        MockStore::new(),
        ScriptRouterFactory::new(router_script),
        ScriptExecutor::new(executor_script),
        HashMap::new(),
        None,
        Some(Arc::new(AllowAllPolicy)),
    )
}

fn manual_candidate(amount_sats: i64, max_fee_sats: i64) -> RebalanceCandidate {
    RebalanceCandidate {
        source_candidates: vec!["src-chan".to_string()],
        to_channel: "dst-chan".to_string(),
        primary_source_peer_id: "peer-src".to_string(),
        to_peer_id: "peer-dst".to_string(),
        amount_sats,
        amount_msat: amount_sats * 1000,
        outbound_fee_ppm: 0,
        inbound_fee_ppm: 0,
        source_fee_ppm: 0,
        weighted_opp_cost_ppm: 0,
        spread_ppm: 0,
        max_budget_sats: max_fee_sats,
        max_budget_msat: max_fee_sats * 1000,
        max_fee_ppm: 0,
        expected_profit_sats: 0,
        liquidity_ratio: 0.5,
        dest_flow_state: "balanced".to_string(),
        dest_turnover_rate: 0.0,
        source_turnover_rate: 0.0,
    }
}

fn reserve_kwargs() -> EngineKwargs {
    // diagnostic: reserve_budget=true, account_costs=true (bounded spend).
    engine_kwargs("diagnostic")
}

// ---------------------------------------------------------------------------
// Plan Step 1 tests
// ---------------------------------------------------------------------------

/// Budget-reservation blocks record history status `skipped` (never
/// `failed`) and never charge futility strikes or persisted cooldowns
/// (`_is_budget_block`, `rebalance_engine_v2.py:2876-2884`, 3565-3570).
#[test]
fn budget_block_records_skipped_never_futility() {
    let store = MockStore::new();
    store.script_reserve(Err(BudgetBlock::Refused { remaining: 5 }));
    store.script_reserve(Err(BudgetBlock::Refused { remaining: 5 }));
    store.script_reserve(Err(BudgetBlock::Refused { remaining: 5 }));
    let h = build_engine(
        base_config(),
        store,
        ScriptRouterFactory::new(vec![route_ok(10), route_ok(10), route_ok(10)]),
        ScriptExecutor::new(vec![]),
        HashMap::new(),
        None,
        Some(Arc::new(AllowAllPolicy)),
    );

    for cycle in 0..3 {
        let result = h.engine.run_cycle();
        assert_eq!(result.executions.len(), 1, "cycle {cycle}");
        let err = result.executions[0].error.clone().unwrap_or_default();
        assert_eq!(
            err, "local_budget_block: 5 sats remaining of 10000 unified budget",
            "cycle {cycle}"
        );
        assert!(err.starts_with(LOCAL_BUDGET_BLOCK));
    }

    // History rows land as skipped, never failed.
    let updates = h.store.calls_with_prefix("update_history:");
    assert_eq!(updates.len(), 3);
    for u in &updates {
        assert!(u.contains(":skipped:"), "expected skipped status: {u}");
    }
    // No route was attempted: neither persisted cooldowns nor in-memory
    // futility may be charged.
    assert!(h.store.calls_with_prefix("record_pair_failure:").is_empty());
    assert!(!h.engine.is_pair_in_futility("src-chan", "dst-chan"));
    // And the executor never ran.
    assert!(h.executor.calls().is_empty());
}

/// P4-007/P4-009: a pending payment WITH a payment_hash parks the history
/// row as pending_settlement and HOLDS the reservation (no release, no
/// mark-spent) until the reconcile sweep resolves it.
#[test]
fn pending_with_hash_holds_reservation() {
    let h = default_harness(
        vec![route_ok(10)],
        vec![exec_pending(100_000, Some("hash-1"))],
    );
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());

    assert!(!result.success);
    assert!(result.payment_pending);
    let pends = h
        .store
        .calls_with_prefix("update_history_pending_settlement:");
    assert_eq!(pends.len(), 1);
    assert!(pends[0].contains(":hash-1:"), "hash recorded: {}", pends[0]);
    assert_eq!(h.store.calls_with_prefix("reserve_budget:").len(), 1);
    assert!(
        h.store.calls_with_prefix("release_reservation:").is_empty(),
        "reservation must be HELD for the reconcile sweep"
    );
    assert!(h.store.calls_with_prefix("mark_budget_spent:").is_empty());
}

/// A pending payment WITHOUT a payment_hash can never be swept: record it
/// as a terminal failure and release the reservation now
/// (`_finish_execution_budget` / `_record_rebalance_result`).
#[test]
fn pending_without_hash_releases_and_records_failed() {
    let h = default_harness(vec![route_ok(10)], vec![exec_pending(100_000, None)]);
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());

    assert!(!result.success);
    let updates = h.store.calls_with_prefix("update_history:");
    assert_eq!(updates.len(), 1);
    assert!(updates[0].contains(":failed:"), "{}", updates[0]);
    assert!(
        updates[0].contains("(payment pending but missing payment_hash; not sweepable)"),
        "{}",
        updates[0]
    );
    assert_eq!(h.store.calls_with_prefix("release_reservation:").len(), 1);
    assert!(h.store.calls_with_prefix("mark_budget_spent:").is_empty());
    assert!(h
        .store
        .calls_with_prefix("update_history_pending_settlement:")
        .is_empty());
}

/// v2.18.1: a failed pricing early-returns `route_pricing_failed: ...`
/// BEFORE any history insert or budget reservation — and the two Python
/// wrap formats stay distinct (exception vs failed RouteResult,
/// `rebalance_engine_v2.py:3314` vs `:3336`).
#[test]
fn route_pricing_failure_early_returns_and_burns_no_reservation() {
    // (a) Exception path (Err from the router seam): no "(label)" suffix.
    let h = default_harness(
        vec![Err(RpcFailure {
            message: "askrene-create-layer: boom".to_string(),
        })],
        vec![],
    );
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());
    assert!(!result.success);
    assert_eq!(
        result.error.as_deref(),
        Some("route_pricing_failed: askrene-create-layer: boom")
    );
    assert_eq!(result.amount_sats, 100_000);
    assert!(
        h.store.calls().is_empty(),
        "v2.18.1: pricing failure must burn NO reservation and insert NO row; got {:?}",
        h.store.calls()
    );
    assert!(h.executor.calls().is_empty());

    // (b) Failed-RouteResult path: wraps WITH the route label.
    let h = default_harness(
        vec![Ok(RouteResult {
            success: false,
            error: Some("no_route: getroutes returned empty".to_string()),
            ..RouteResult::default()
        })],
        vec![],
    );
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());
    assert_eq!(
        result.error.as_deref(),
        Some("route_pricing_failed: no_route: getroutes returned empty (market)")
    );
    assert!(h.store.calls().is_empty());

    // (c) Empty error string on the failed RouteResult falls back to
    // "no_route" (Python `route_result.error or 'no_route'`).
    let h = default_harness(
        vec![Ok(RouteResult {
            success: false,
            error: None,
            ..RouteResult::default()
        })],
        vec![],
    );
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());
    assert_eq!(
        result.error.as_deref(),
        Some("route_pricing_failed: no_route (market)")
    );
}

/// P8-001: an intermediate partial-fill attempt that ends payment_pending
/// BREAKS the halving ladder — never another executor dispatch on top of an
/// in-flight HTLC — and the reservation stays held.
#[test]
fn partial_ladder_breaks_on_pending() {
    let h = default_harness(
        // Initial pricing + first partial repricing only.
        vec![route_ok(10), route_ok(8)],
        vec![
            exec_liquidity_failure(100_000, &[]), // no exclusions -> straight to ladder
            exec_pending(50_000, Some("hash-p")),
        ],
    );
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());

    assert!(result.payment_pending);
    let calls = h.executor.calls();
    assert_eq!(
        calls.len(),
        2,
        "ladder must break on pending: no third executor dispatch"
    );
    assert_eq!(calls[0].amount_sats, 100_000);
    assert_eq!(calls[1].amount_sats, 50_000, "first halving step");
    // partial_fill trail records the pending stop.
    let attempts = &result.failure_data["partial_fill"]["attempts"];
    let last = attempts.as_array().and_then(|a| a.last()).unwrap();
    assert_eq!(last["amount_sats"], 50_000);
    assert_eq!(last["status"], "payment_pending");
    assert_eq!(
        result.failure_data["partial_fill"]["planned_amount_sats"],
        100_000
    );
    // Reservation held (pending WITH hash).
    assert!(h.store.calls_with_prefix("release_reservation:").is_empty());
    assert!(h.store.calls_with_prefix("mark_budget_spent:").is_empty());
    assert_eq!(
        h.store
            .calls_with_prefix("update_history_pending_settlement:")
            .len(),
        1
    );
}

/// The halving ladder floor/steps match Python `_native_partial_amounts`
/// (fixture-pinned separately); here the engine is driven end-to-end and
/// must try amounts in ladder order after a liquidity failure.
#[test]
fn partial_ladder_amounts_and_budget_scaling() {
    // All partial repricings fail -> ladder walks every amount, then the
    // prior (liquidity) failure is returned annotated.
    let router_script = vec![
        route_ok(10), // initial
        Ok(RouteResult {
            success: false,
            error: Some("no_route: dry".to_string()),
            ..RouteResult::default()
        }),
        Ok(RouteResult {
            success: false,
            error: Some("no_route: dry".to_string()),
            ..RouteResult::default()
        }),
        Ok(RouteResult {
            success: false,
            error: Some("no_route: dry".to_string()),
            ..RouteResult::default()
        }),
        Ok(RouteResult {
            success: false,
            error: Some("no_route: dry".to_string()),
            ..RouteResult::default()
        }),
        Ok(RouteResult {
            success: false,
            error: Some("no_route: dry".to_string()),
            ..RouteResult::default()
        }),
    ];
    let h = default_harness(router_script, vec![exec_liquidity_failure(100_000, &[])]);
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());

    assert!(!result.success);
    assert_eq!(
        native_partial_amounts(100_000),
        vec![50_000, 25_000, 12_500, 6_250, 5_000]
    );
    let attempts = result.failure_data["partial_fill"]["attempts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["amount_sats"].as_i64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(attempts, vec![50_000, 25_000, 12_500, 6_250, 5_000]);
    assert!(result
        .error
        .as_deref()
        .unwrap()
        .ends_with("; partial_retry_failed: no_route"));
    // Failure releases the reservation.
    assert_eq!(h.store.calls_with_prefix("release_reservation:").len(), 1);
}

/// One exclusion retry after a terminal failure naming route segments; a
/// payment_pending first result must NEVER be retried on top of
/// (`_retry_native_pair_with_exclusions`).
#[test]
fn exclusion_retry_once_and_never_on_pending() {
    // Terminal failure with a named erring segment -> one repricing with
    // that exclusion, one more execution.
    let mut fee_failure = exec_failed(
        "native_sendpay_error: WIRE_FEE_INSUFFICIENT (failcode=4108)",
        100_000,
    );
    fee_failure.excluded_channels = vec!["222x2x2/1".to_string()];
    fee_failure.failure_data = json!({"failure_class": "fee"});
    let h = default_harness(
        vec![route_ok(10), route_ok(12)],
        vec![fee_failure, exec_success(100_000, 9)],
    );
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());
    assert!(result.success);
    assert_eq!(result.attempts, 2);
    assert_eq!(h.executor.calls().len(), 2);
    let router_calls = h.router.calls.lock().unwrap().clone();
    assert_eq!(router_calls.len(), 2);
    assert_eq!(router_calls[1].1, vec!["222x2x2/1".to_string()]);
    // Success settles the reservation with the ACTUAL fee.
    assert_eq!(
        h.store.calls_with_prefix("mark_budget_spent:"),
        vec!["mark_budget_spent:1:9".to_string()]
    );

    // Pending first result: retry gate must not fire even with exclusions.
    let mut pending = exec_pending(100_000, Some("hash-x"));
    pending.excluded_channels = vec!["222x2x2/1".to_string()];
    let h = default_harness(vec![route_ok(10)], vec![pending]);
    let result = h
        .engine
        .execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());
    assert!(result.payment_pending);
    assert_eq!(h.executor.calls().len(), 1, "never pay again on top");
}

/// Single-flight: `execute_candidate` fails fast with `engine_busy` and a
/// concurrent `run_cycle` skips with a `cycle_already_running` audit marker
/// while the engine lock is held.
#[test]
fn single_flight_engine_busy_and_cycle_already_running() {
    let store = MockStore::new();
    let executor = ScriptExecutor::new_blocked(vec![exec_success(2_000_000, 3)]);
    let h = build_engine(
        base_config(),
        store,
        ScriptRouterFactory::new(vec![route_ok(10)]),
        executor,
        HashMap::new(),
        None,
        Some(Arc::new(AllowAllPolicy)),
    );

    let engine = h.engine.clone();
    let cycle = std::thread::spawn(move || engine.run_cycle());
    h.executor.wait_entered(1);

    // Manual execution: immediate engine_busy, amount echoed.
    let busy = h
        .engine
        .execute_candidate(&manual_candidate(77_000, 500), 0, reserve_kwargs());
    assert!(!busy.success);
    assert_eq!(busy.error.as_deref(), Some(ENGINE_BUSY));
    assert_eq!(busy.amount_sats, 77_000);

    // Second cycle: skips with the audit marker, empty result.
    let skipped: CycleResult = h.engine.run_cycle();
    assert!(skipped.candidates.is_empty());
    assert!(skipped.executions.is_empty());
    assert_eq!(skipped.audit_records.len(), 1);
    assert_eq!(skipped.audit_records[0].reason, CYCLE_ALREADY_RUNNING);
    assert_eq!(skipped.audit_records[0].value_class, "none");
    assert_eq!(
        skipped.audit_records[0].detail.as_deref(),
        Some("engine cycle lock held by another caller")
    );

    h.executor.release();
    let result = cycle.join().unwrap();
    assert_eq!(result.executions.len(), 1);
    assert!(result.executions[0].success);
}

/// P4-008: the in-flight-destination guard is a COUNTED map, not a set —
/// overlapping executions to one destination release correctly.
#[test]
fn inflight_dest_map_is_counted_not_set() {
    let guard = InflightDests::default();
    guard.register("dst");
    guard.register("dst");
    guard.unregister("dst");
    assert!(
        guard.snapshot().contains("dst"),
        "one of two in-flight executions finished; dest must stay guarded"
    );
    guard.unregister("dst");
    assert!(guard.snapshot().is_empty());
    // Unregister below zero never wedges the map.
    guard.unregister("dst");
    guard.register("dst");
    assert!(guard.snapshot().contains("dst"));
    guard.unregister("dst");
    assert!(guard.snapshot().is_empty());
    // Empty ids are ignored (Python guard).
    guard.register("");
    assert!(guard.snapshot().is_empty());
}

/// The 120s cycle ceiling ABANDONS an unfinished worker (run_cycle returns)
/// but never cancels its reserve+pay critical section: the worker finishes
/// bookkeeping asynchronously — reservation settles, inflight-dest clears.
#[test]
fn timeout_abandons_but_reservation_settles_when_worker_finishes() {
    let store = MockStore::new();
    let executor = ScriptExecutor::new_blocked(vec![exec_success(2_000_000, 7)]);
    let config = EngineConfig {
        cycle_timeout_secs: 0.2,
        ..base_config()
    };
    let h = build_engine(
        config,
        store,
        // Initial pricing + the post-abandon execute_candidate probe.
        ScriptRouterFactory::new(vec![route_ok(10), route_ok(10)]),
        executor,
        HashMap::new(),
        None,
        Some(Arc::new(AllowAllPolicy)),
    );

    let result = h.engine.run_cycle();
    // Worker was abandoned: no execution collected, budget reserved but not
    // yet settled.
    assert!(result.executions.is_empty());
    assert_eq!(h.store.calls_with_prefix("reserve_budget:").len(), 1);
    assert!(h.store.calls_with_prefix("mark_budget_spent:").is_empty());

    // The abandoned worker still guards its destination (P4-008): explicit
    // executions to the same dest fail dest_inflight.
    let blocked = h
        .engine
        .execute_candidate(&manual_candidate(50_000, 100), 0, reserve_kwargs());
    assert_eq!(blocked.error.as_deref(), Some(DEST_INFLIGHT));

    // Let the worker finish: the reservation settles with the actual fee and
    // the history row flips to success.
    h.executor.release();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if !h.store.calls_with_prefix("mark_budget_spent:").is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "abandoned worker never settled its reservation; calls={:?}",
            h.store.calls()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        h.store.calls_with_prefix("mark_budget_spent:"),
        vec!["mark_budget_spent:1:7".to_string()]
    );
    assert_eq!(
        h.store.calls_with_prefix("update_history_success:").len(),
        1
    );

    // The worker's self-clean releases the counted inflight guard.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let probe = h
            .engine
            .execute_candidate(&manual_candidate(50_000, 100), 0, reserve_kwargs());
        if probe.error.as_deref() != Some(DEST_INFLIGHT) {
            break;
        }
        assert!(Instant::now() < deadline, "inflight guard never cleared");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Reconcile sweep: a `complete` listsendpays entry settles the row — fee =
/// amount_sent_msat - amount_msat, cost recorded, reservation marked spent,
/// in-memory pair failures cleared; a failed/absent payment releases the
/// reservation and delpays; still-pending rows are left alone.
#[test]
fn reconcile_sweep_marks_spent_and_clears_pair_failures() {
    let store = MockStore::new();
    store.push_pending_row(HistoryRow {
        id: 42,
        from_channel: "src-chan".to_string(),
        to_channel: "dst-chan".to_string(),
        amount_sats: 200_000,
        max_fee_sats: 500,
        expected_profit_sats: 0,
        status: "pending_settlement".to_string(),
        rebalance_type: "normal".to_string(),
        reason_code: "ev_positive".to_string(),
        payment_hash: Some("h-complete".to_string()),
    });
    store.push_pending_row(HistoryRow {
        id: 43,
        from_channel: "src-chan".to_string(),
        to_channel: "dst-chan".to_string(),
        amount_sats: 100_000,
        max_fee_sats: 500,
        expected_profit_sats: 0,
        status: "pending_settlement".to_string(),
        rebalance_type: "normal".to_string(),
        reason_code: "ev_positive".to_string(),
        payment_hash: Some("h-failed".to_string()),
    });
    store.push_pending_row(HistoryRow {
        id: 44,
        from_channel: "src-chan".to_string(),
        to_channel: "dst-chan".to_string(),
        amount_sats: 100_000,
        max_fee_sats: 500,
        expected_profit_sats: 0,
        status: "pending_settlement".to_string(),
        rebalance_type: "normal".to_string(),
        reason_code: "ev_positive".to_string(),
        payment_hash: Some("h-pending".to_string()),
    });

    let mut listsendpays = HashMap::new();
    listsendpays.insert(
        "h-complete".to_string(),
        // Partial fill settled for 100k of the planned 200k; "Nmsat" string
        // and bare-int msat fields both parse.
        json!({"payments": [
            {"status": "complete", "amount_msat": "100000000msat", "amount_sent_msat": 100005000},
        ]}),
    );
    listsendpays.insert(
        "h-failed".to_string(),
        json!({"payments": [{"status": "failed"}]}),
    );
    listsendpays.insert(
        "h-pending".to_string(),
        json!({"payments": [{"status": "pending"}]}),
    );

    let h = build_engine(
        base_config(),
        store,
        ScriptRouterFactory::new(vec![]),
        ScriptExecutor::new(vec![]),
        listsendpays,
        None,
        Some(Arc::new(AllowAllPolicy)),
    );

    // Charge the pair into futility so the sweep's success-clear is visible.
    for _ in 0..3 {
        h.engine.debug_note_pair_failure("src-chan", "dst-chan");
    }
    assert!(h.engine.is_pair_in_futility("src-chan", "dst-chan"));

    let resolved = h.engine.reconcile_pending_settlements();
    assert_eq!(resolved, 2, "complete + failed resolve; pending stays");

    // Complete row: settled amount corrects the history row (partial fill),
    // fee 5000 msat -> 5 sats ceil.
    let success = h.store.calls_with_prefix("update_history_success:42");
    assert_eq!(success.len(), 1);
    assert!(
        success[0].contains("fee_sats=5:fee_msat=5000"),
        "{}",
        success[0]
    );
    assert!(success[0].contains("amount=Some(100000)"), "{}", success[0]);
    assert_eq!(
        h.store.calls_with_prefix("record_rebalance_cost:"),
        vec!["record_rebalance_cost:dst-chan:peer-dst:5:5000:100000".to_string()]
    );
    assert_eq!(
        h.store.calls_with_prefix("mark_budget_spent:"),
        vec!["mark_budget_spent:42:5".to_string()]
    );
    // Python truth check (`_reconcile_pending_row`): settlement clears the
    // IN-MEMORY pair tracker (`_record_pair_success`); the persisted
    // clear_pair_failures write belongs to the cycle success path only.
    assert!(!h.engine.is_pair_in_futility("src-chan", "dst-chan"));

    // Failed row: released + history failed + delpay.
    let failed = h.store.calls_with_prefix("update_history:43");
    assert_eq!(failed.len(), 1);
    assert!(
        failed[0].contains(":failed:payment_pending_resolved_failed"),
        "{}",
        failed[0]
    );
    assert_eq!(
        h.store.calls_with_prefix("release_reservation:"),
        vec!["release_reservation:43".to_string()]
    );
    assert_eq!(
        h.payment.delpay_calls.lock().unwrap().clone(),
        vec![("h-failed".to_string(), "failed".to_string())]
    );

    // Pending row untouched.
    assert!(h.store.calls_with_prefix("update_history:44").is_empty());
    assert!(h
        .store
        .calls_with_prefix("update_history_success:44")
        .is_empty());
}

// ---------------------------------------------------------------------------
// Supporting contracts beyond the Step-1 list
// ---------------------------------------------------------------------------

/// Policy gate fails CLOSED without a policy manager: no execution runs.
#[test]
fn policy_gate_fails_closed_without_manager() {
    let h = build_engine(
        base_config(),
        MockStore::new(),
        ScriptRouterFactory::new(vec![route_ok(10)]),
        ScriptExecutor::new(vec![exec_success(2_000_000, 3)]),
        HashMap::new(),
        None,
        None, // no policy manager
    );
    let result = h.engine.run_cycle();
    assert!(result.candidates.is_empty());
    assert!(result.executions.is_empty());
    assert!(h.executor.calls().is_empty());
    assert!(h.store.calls_with_prefix("reserve_budget:").is_empty());
}

/// Zero unified budget blocks automatic rebalances outright
/// (`zero_budget_blocks_auto_rebalance`) — status skipped, never futility.
#[test]
fn zero_budget_blocks_auto_rebalance() {
    let config = EngineConfig {
        daily_budget_sats: 0,
        ..base_config()
    };
    let h = build_engine(
        config,
        MockStore::new(),
        ScriptRouterFactory::new(vec![route_ok(10)]),
        ScriptExecutor::new(vec![]),
        HashMap::new(),
        None,
        Some(Arc::new(AllowAllPolicy)),
    );
    let result = h.engine.run_cycle();
    assert_eq!(result.executions.len(), 1);
    assert_eq!(
        result.executions[0].error.as_deref(),
        Some("zero_budget_blocks_auto_rebalance")
    );
    // Never reached the budget rail at all.
    assert!(h.store.calls_with_prefix("reserve_budget:").is_empty());
    let updates = h.store.calls_with_prefix("update_history:");
    assert_eq!(updates.len(), 1);
    assert!(updates[0].contains(":skipped:"));
    assert!(!h.engine.is_pair_in_futility("src-chan", "dst-chan"));
}

/// The sats-EV hold-margin gate rejects priced pairs below the margin
/// (audit F2). The failure penalty is passed to `sats_ev_gate` as a
/// separate term (`failure_penalty_sats`) and subtracted in Python's exact
/// left-to-right order — never folded into the activity penalty.
#[test]
fn ev_gate_below_hold_margin_skips_pair() {
    let config = EngineConfig {
        rebalance_hold_margin: 1.0,
        ..base_config()
    };
    let h = build_engine(
        config,
        MockStore::new(),
        ScriptRouterFactory::new(vec![route_ok(10)]),
        ScriptExecutor::new(vec![]),
        HashMap::new(),
        Some(Arc::new(FixedEv(EvTerms {
            dest_attempts: 0,
            dest_success_rate: 0.0,
            efv_sats: 10.0,
            source_opportunity_sats: 0.0,
            activity_penalty_sats: 0.0,
        }))),
        Some(Arc::new(AllowAllPolicy)),
    );
    let result = h.engine.run_cycle();
    // p_success=0.5 prior, score = 0.5*10 - 10 = -5.0 < margin 1.0.
    assert!(result.executions.is_empty());
    let skip = result
        .audit_records
        .iter()
        .find(|s| s.reason == "below_hold_margin")
        .expect("below_hold_margin skip recorded");
    assert_eq!(skip.channel_id, "dst-chan");
    let detail = skip.detail.as_deref().unwrap_or("");
    assert!(
        detail.contains("score=-5.0000") && detail.contains("margin=1.0000"),
        "detail: {detail}"
    );
    assert!(h.store.calls_with_prefix("reserve_budget:").is_empty());
}

/// Route cost above the per-attempt ceiling is skipped as
/// `route_over_budget` — the ceiling is `min(prob-adjusted budget,
/// ceil(amount*pair_fee_cap_ppm/1e6))` (audit F1).
#[test]
fn route_over_budget_skip_uses_per_attempt_ceiling() {
    // amount 2_000_000, pair_fee_cap_ppm 1000 -> ppm ceiling 2000 sats;
    // pair budget 2000; route cost 2001 -> over budget.
    let h = default_harness(vec![route_ok(2_001)], vec![]);
    let result = h.engine.run_cycle();
    assert!(result.executions.is_empty());
    let skip = result
        .audit_records
        .iter()
        .find(|s| s.reason == "route_over_budget")
        .expect("route_over_budget skip");
    let detail = skip.detail.as_deref().unwrap_or("");
    assert!(
        detail.contains("route_cost=2001") && detail.contains("effective_budget=2000"),
        "detail: {detail}"
    );
}

/// `native_partial_amounts` replays the committed fixture generated from
/// the real Python `_native_partial_amounts` (byte-parity discipline).
#[test]
fn native_partial_amounts_replays_python_fixture() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/rebalance/partial_amounts.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let fx: Value = serde_json::from_str(&raw).expect("valid JSON");
    let cases = fx["cases"].as_array().expect("cases array");
    assert!(cases.len() >= 30, "expect a healthy amount spread");
    for case in cases {
        let amount = case["amount_sats"].as_i64().expect("amount_sats");
        let expected: Vec<i64> = case["amounts"]
            .as_array()
            .expect("amounts")
            .iter()
            .map(|v| v.as_i64().expect("int"))
            .collect();
        assert_eq!(
            native_partial_amounts(amount),
            expected,
            "amount_sats={amount}"
        );
    }
}

/// The dry-run store journals every write as JSONL and holds reservations
/// in memory with no-double-spend accounting (active + spent both count
/// against the unified budget).
#[test]
fn dryrun_store_journals_and_holds_reservations() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = DryRunStore::new(dir.path());

    let row_id = store.insert_or_adopt_history(&HistoryRow {
        id: 0,
        from_channel: "src-chan".to_string(),
        to_channel: "dst-chan".to_string(),
        amount_sats: 100_000,
        max_fee_sats: 600,
        expected_profit_sats: 0,
        status: "pending".to_string(),
        rebalance_type: "normal".to_string(),
        reason_code: "ev_positive".to_string(),
        payment_hash: None,
    });
    assert!(row_id > 0);

    let req = |rid: &str, amount: i64| ReserveRequest {
        reservation_id: rid.to_string(),
        amount_sats: amount,
        category: "rebalance".to_string(),
        channel_id: Some("dst-chan".to_string()),
        effective_budget_sats: Some(1_000),
        ..ReserveRequest::default()
    };

    // 600 reserved of 1000.
    let rid1 = store.reserve_budget(&req("r1", 600)).expect("granted");
    // 500 > remaining 400 -> refused with the true remaining.
    match store.reserve_budget(&req("r2", 500)) {
        Err(BudgetBlock::Refused { remaining }) => assert_eq!(remaining, 400),
        other => panic!("expected refusal, got {other:?}"),
    }
    // Spend r1 for 550: spent stays committed, so 500 still refuses.
    store.mark_budget_spent(&rid1, 550);
    match store.reserve_budget(&req("r3", 500)) {
        Err(BudgetBlock::Refused { remaining }) => assert_eq!(remaining, 450),
        other => panic!("expected refusal, got {other:?}"),
    }
    // 400 fits (1000 - 550 spent = 450 remaining).
    let rid4 = store.reserve_budget(&req("r4", 400)).expect("granted");
    // Released reservations return their headroom.
    store.release_reservation(&rid4);
    store
        .reserve_budget(&req("r5", 400))
        .expect("granted after release");

    store.update_history(row_id, "skipped", Some("local_budget_block: test"));
    store.datastore_export(
        &["revenue", "segment-observations"],
        &OValue::obj(vec![("generated_at".to_string(), OValue::Int(1))]),
    );

    let journal = std::fs::read_to_string(dir.path().join("rebalance_dryrun_journal.jsonl"))
        .expect("journal exists");
    let lines: Vec<&str> = journal.lines().collect();
    assert!(
        lines.len() >= 8,
        "one JSONL line per write, got {}",
        lines.len()
    );
    for line in &lines {
        let v: Value = serde_json::from_str(line).expect("every journal line is JSON");
        assert!(v["event"].is_string(), "journal line carries an event tag");
    }
    // Reservation lifecycle events all journaled.
    for needle in [
        "history_insert",
        "budget_reserve",
        "budget_refused",
        "budget_spent",
        "budget_release",
        "history_update",
        "datastore_export",
    ] {
        assert!(
            journal.contains(needle),
            "journal missing event {needle}: {journal}"
        );
    }
}

/// A dest with an in-flight execution is dropped at candidate selection
/// with a `dest_inflight` skip record (P4-008 at find_candidates).
#[test]
fn find_candidates_drops_inflight_dest_with_skip() {
    let store = MockStore::new();
    let executor = ScriptExecutor::new_blocked(vec![exec_success(2_000_000, 3)]);
    let config = EngineConfig {
        cycle_timeout_secs: 0.2,
        ..base_config()
    };
    let h = build_engine(
        config,
        store,
        ScriptRouterFactory::new(vec![route_ok(10), route_ok(10)]),
        executor,
        HashMap::new(),
        None,
        Some(Arc::new(AllowAllPolicy)),
    );
    // First cycle abandons a worker holding the dest.
    let first = h.engine.run_cycle();
    assert!(first.executions.is_empty());

    // Second cycle: the planner still selects the pair, but the inflight
    // guard drops it with a skip record.
    let second = h.engine.run_cycle();
    assert!(second.candidates.is_empty());
    let skip = second
        .audit_records
        .iter()
        .find(|s| s.reason == DEST_INFLIGHT)
        .expect("dest_inflight skip recorded");
    assert_eq!(skip.channel_id, "dst-chan");

    h.executor.release();
}

/// Parity fix: a pricing EXCEPTION (the router seam's `Err` -- Python's
/// `_route_pair` -> `_market_price_pair` -> `router.price_pair`, unprotected
/// in `find_candidates`'s loop) mid-cycle must abort the WHOLE cycle with
/// ZERO executions, matching Python's uncaught raise propagating through
/// `_run_cycle_locked` and `rebalancer.py` (which only has `finally`, never
/// `except`). Two pairs price successfully before the third raises: on a
/// mid-cycle askrene flake, the port must NOT pay the pairs that already
/// priced -- Python pays nothing.
#[test]
fn pricing_exception_mid_cycle_aborts_whole_cycle_zero_executions() {
    let store = MockStore::new();
    let payment = MockPaymentRpc::new();
    let executor = ScriptExecutor::new(vec![]);
    let engine = RebalanceEngine::new(EngineDeps {
        config: base_config(),
        store: store.clone(),
        router_factory: ScriptRouterFactory::new(vec![
            route_ok(10),
            route_ok(10),
            Err(RpcFailure {
                message: "askrene-create-layer: boom".to_string(),
            }),
        ]),
        executor: executor.clone(),
        payment_rpc: payment,
        reconcile_rpc: Arc::new(MockReconcileRpc {
            listsendpays: HashMap::new(),
        }),
        segstore: Arc::new(SegmentObservationStore::with_defaults()),
        snapshot: Arc::new(FixedSnapshot {
            channels: snapshot_three_pairs(),
        }),
        clock: Arc::new(SystemTestClock),
        policy: Some(Arc::new(AllowAllPolicy)),
        arbiter: None,
        ev: None,
    });

    let result = engine.run_cycle();

    assert!(
        result.executions.is_empty(),
        "pricing exception on pair 3 must abort the whole cycle (Python-true), \
         not skip-and-continue; got {} executions",
        result.executions.len()
    );
    assert!(
        result.candidates.is_empty(),
        "Python never returns candidates when find_candidates raises"
    );
    assert_eq!(result.audit_records.len(), 1);
    assert_eq!(result.audit_records[0].reason, "route_pricing_failed");
    assert_eq!(
        result.audit_records[0].detail.as_deref(),
        Some("route_pricing_failed: askrene-create-layer: boom"),
        "abort surface must use the EXCEPTION wrap form (no route label) -- \
         the same template as execute_candidate_locked's py `:3314` site"
    );

    // Zero executions means zero side effects: the two pairs that priced
    // successfully before the abort must never reach the budget rail, the
    // history store, or the executor.
    assert!(
        store.calls().is_empty(),
        "no reservation/history activity may leak from a cycle Python never \
         completed; got {:?}",
        store.calls()
    );
    assert!(executor.calls().is_empty());
}

/// Failed executions push the segment-observation snapshot to the CLN
/// datastore key ["revenue","segment-observations"], with the engine
/// merging in observer_member_id (T3 scope note).
#[test]
fn failed_execution_exports_segment_snapshot() {
    let segstore = Arc::new(SegmentObservationStore::with_defaults());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    segstore.record("999x9x9", 1, 100_000, "liquidity", 0.85, now);

    let store = MockStore::new();
    let payment = MockPaymentRpc::new();
    let engine = RebalanceEngine::new(EngineDeps {
        config: base_config(),
        store: store.clone(),
        router_factory: ScriptRouterFactory::new(vec![route_ok(10)]),
        executor: ScriptExecutor::new(vec![
            exec_liquidity_failure(100_000, &[]), /* ladder dries out via script exhaustion */
        ]),
        payment_rpc: payment,
        reconcile_rpc: Arc::new(MockReconcileRpc {
            listsendpays: HashMap::new(),
        }),
        segstore: segstore.clone(),
        snapshot: Arc::new(FixedSnapshot {
            channels: snapshot_two_channels(),
        }),
        clock: Arc::new(SystemTestClock),
        policy: Some(Arc::new(AllowAllPolicy)),
        arbiter: None,
        ev: None,
    });
    let result = engine.execute_candidate(&manual_candidate(100_000, 500), 0, reserve_kwargs());
    assert!(!result.success);
    let exports = store.calls_with_prefix("datastore_export:");
    assert_eq!(
        exports,
        vec!["datastore_export:revenue/segment-observations".to_string()]
    );
}
