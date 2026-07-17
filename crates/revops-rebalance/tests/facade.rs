//! Task 8 facade + defibrillator tests — golden parity against
//! `fixtures/rebalance/{inbound_fee,defib}.json` (generated from the REAL
//! `modules/rebalancer.py` `EVRebalancer`, v2.18.1, by
//! `tools/port/gen_rebalance_fixtures.py {inbound_fee,defib}` in the port
//! worktree, branch `phase5-t8-gen`), plus scripted-double behavior tests
//! per the plan's Step 1 list (phase5 plan, Task 8).
//!
//! The defib fixture pins the v2.18.1 ranked-source fallback loop
//! end-to-end: source ranking (spendable desc, stable ties), the
//! DIAGNOSTIC_MAX_SOURCE_ATTEMPTS cap, advance-ONLY-on
//! `_is_source_route_failure`, honest shock_status, byte-preserved RPC
//! message strings, and the exact history-row write sequence (shared row:
//! the engine owns the success fee — P4-025 — so the facade records NO fee
//! on success).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use revops_db::budget::ReserveRequest;
use revops_fees::pyjson::{parse as pyjson_parse, OValue};
use revops_rebalance::defib::{
    is_source_route_failure, shock_fee_envelope, DIAGNOSTIC_FEE_CAP_CEILING_SATS,
    DIAGNOSTIC_MAX_SOURCE_ATTEMPTS, SHOCK_AMOUNT_SATS,
};
use revops_rebalance::engine::{
    BudgetBlock, CycleResult, EngineClock, HistoryRow, HistorySuccess, RebalanceCost,
    RebalanceStore, ReservationId,
};
use revops_rebalance::facade::{
    CandidateExecutor, EvRebalancer, EvRebalancerDeps, ExecuteOpts, FacadeConfig, FacadeRpc,
    FacadeStore, HistoricalInboundFee, PeerRebalanceRecord,
};
use revops_rebalance::modes::EngineKwargs;
use revops_rebalance::router::RpcFailure;
use revops_rebalance::types::{ExecutionResult, RebalanceCandidate};

const NOW: f64 = 1_700_000_000.0;

fn fixture(name: &str) -> OValue {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../fixtures/rebalance/{name}.json"));
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    pyjson_parse(&raw).expect("valid fixture JSON")
}

fn get<'a>(v: &'a OValue, key: &str) -> &'a OValue {
    v.get(key).unwrap_or_else(|| panic!("missing key {key}"))
}

fn get_i64(v: &OValue, key: &str) -> i64 {
    get(v, key)
        .as_i64()
        .unwrap_or_else(|| panic!("{key} not int"))
}

fn get_str<'a>(v: &'a OValue, key: &str) -> &'a str {
    get(v, key)
        .as_str()
        .unwrap_or_else(|| panic!("{key} not str"))
}

fn opt_str(v: &OValue, key: &str) -> Option<String> {
    match v.get(key) {
        None | Some(OValue::Null) => None,
        Some(o) => Some(o.as_str().expect("str").to_string()),
    }
}

// ---------------------------------------------------------------------------
// Scripted doubles
// ---------------------------------------------------------------------------

/// One shared, ordered call log across the store doubles: the Python
/// fixture's `db_calls` interleaves `set_channel_probe` (facade store) with
/// `record_rebalance`/`update_rebalance_result` (history store), so ordering
/// is only comparable on a single log.
type SharedLog = Arc<Mutex<Vec<String>>>;

struct MockStore {
    log: SharedLog,
    next_id: Mutex<i64>,
    reserve_script: Mutex<Vec<Result<(), BudgetBlock>>>,
    reserve_requests: Mutex<Vec<ReserveRequest>>,
}

impl MockStore {
    fn new(log: SharedLog) -> Arc<Self> {
        Arc::new(MockStore {
            log,
            next_id: Mutex::new(0),
            reserve_script: Mutex::new(Vec::new()),
            reserve_requests: Mutex::new(Vec::new()),
        })
    }

    fn script_reserve(&self, outcome: Result<(), BudgetBlock>) {
        self.reserve_script.lock().unwrap().push(outcome);
    }

    fn reserve_requests(&self) -> Vec<ReserveRequest> {
        self.reserve_requests.lock().unwrap().clone()
    }
}

impl RebalanceStore for MockStore {
    fn insert_or_adopt_history(&self, row: &HistoryRow) -> i64 {
        let mut next = self.next_id.lock().unwrap();
        *next += 1;
        let id = *next;
        self.log.lock().unwrap().push(format!(
            "record_rebalance:{id}:{}:{}:{}:{}:{}:{}:{}",
            row.from_channel,
            row.to_channel,
            row.amount_sats,
            row.max_fee_sats,
            row.expected_profit_sats,
            row.rebalance_type,
            row.reason_code
        ));
        id
    }

    fn update_history(&self, id: i64, status: &str, err: Option<&str>) {
        self.log.lock().unwrap().push(format!(
            "update_rebalance_result:{id}:{status}:{}",
            err.unwrap_or("")
        ));
    }

    fn update_history_success(&self, id: i64, update: &HistorySuccess) {
        self.log.lock().unwrap().push(format!(
            "update_history_success:{id}:fee_sats={}:fee_msat={}:amount={:?}:ratio={:?}",
            update.actual_fee_sats,
            update.actual_fee_msat,
            update.amount_sats,
            update.post_local_ratio
        ));
    }

    fn update_history_pending_settlement(&self, id: i64, error: &str, payment_hash: &str) {
        self.log.lock().unwrap().push(format!(
            "update_history_pending_settlement:{id}:{payment_hash}:{error}"
        ));
    }

    fn reserve_budget(&self, req: &ReserveRequest) -> Result<ReservationId, BudgetBlock> {
        self.reserve_requests.lock().unwrap().push(req.clone());
        self.log.lock().unwrap().push(format!(
            "reserve_budget:{}:{}:{}",
            req.reservation_id,
            req.amount_sats,
            req.effective_budget_sats.unwrap_or(-1)
        ));
        let mut script = self.reserve_script.lock().unwrap();
        if script.is_empty() {
            Ok(ReservationId(req.reservation_id.clone()))
        } else {
            script
                .remove(0)
                .map(|()| ReservationId(req.reservation_id.clone()))
        }
    }

    fn mark_budget_spent(&self, rid: &ReservationId, actual_fee_sats: i64) {
        self.log
            .lock()
            .unwrap()
            .push(format!("mark_budget_spent:{}:{actual_fee_sats}", rid.0));
    }

    fn release_reservation(&self, rid: &ReservationId) {
        self.log
            .lock()
            .unwrap()
            .push(format!("release_reservation:{}", rid.0));
    }

    fn record_rebalance_cost(&self, cost: &RebalanceCost) {
        self.log.lock().unwrap().push(format!(
            "record_rebalance_cost:{}:{}:{}:{}:{}",
            cost.channel_id, cost.peer_id, cost.cost_sats, cost.cost_msat, cost.amount_sats
        ));
    }

    fn record_pair_failure(&self, _s: &str, _d: &str, _k: &str, _b: i64, _n: i64) {}

    fn clear_pair_failures(&self, _s: &str, _d: &str) {}

    fn pair_cooldown_until(&self, _s: &str, _d: &str, _n: i64) -> Option<i64> {
        None
    }

    fn pending_settlement_rows(&self) -> Vec<HistoryRow> {
        Vec::new()
    }

    fn datastore_export(&self, _key: &[&str], _payload: &OValue) {}
}

struct MockFacadeStore {
    log: SharedLog,
    hist_inbound: Option<HistoricalInboundFee>,
    peer_history: Vec<PeerRebalanceRecord>,
    /// `since_timestamp -> total fees`; missing key = 0 (py returns 0).
    fees_by_since: HashMap<i64, i64>,
}

impl FacadeStore for MockFacadeStore {
    fn cleanup_stale_reservations(&self, timeout_seconds: i64) -> i64 {
        self.log
            .lock()
            .unwrap()
            .push(format!("cleanup_stale_reservations:{timeout_seconds}"));
        0
    }

    fn get_total_rebalance_fees(&self, since_timestamp: i64) -> i64 {
        self.fees_by_since
            .get(&since_timestamp)
            .copied()
            .unwrap_or(0)
    }

    fn get_historical_inbound_fee_ppm(&self, _peer_id: &str) -> Option<HistoricalInboundFee> {
        self.hist_inbound.clone()
    }

    fn get_rebalance_history_by_peer(
        &self,
        _peer_id: &str,
        limit: i64,
    ) -> Vec<PeerRebalanceRecord> {
        assert_eq!(limit, 20, "py _estimate_inbound_fee passes limit=20");
        self.peer_history.clone()
    }

    fn set_channel_probe(&self, channel_id: &str, probe_type: &str) {
        self.log
            .lock()
            .unwrap()
            .push(format!("set_channel_probe:{channel_id}:{probe_type}"));
    }

    fn reset_failure_count(&self, key: &str) {
        self.log
            .lock()
            .unwrap()
            .push(format!("reset_failure_count:{key}"));
    }

    fn increment_failure_count(
        &self,
        key: &str,
        attempted_ppm: i64,
        attempted_amount: i64,
        error_type: &str,
    ) {
        self.log.lock().unwrap().push(format!(
            "increment_failure_count:{key}:{attempted_ppm}:{attempted_amount}:{error_type}"
        ));
    }
}

#[derive(Default)]
struct ScriptedRpc {
    funds: Option<Value>,
    funds_error: bool,
    peer_channels: Option<Value>,
    channels_by_source: HashMap<String, Value>,
    node_id: Option<String>,
    gossip_calls: Mutex<usize>,
}

impl FacadeRpc for ScriptedRpc {
    fn get_funds(&self) -> Result<Value, RpcFailure> {
        if self.funds_error {
            return Err(RpcFailure {
                message: "listfunds timed out".to_string(),
            });
        }
        Ok(self
            .funds
            .clone()
            .unwrap_or_else(|| serde_json::json!({"outputs": [], "channels": []})))
    }

    fn get_peer_channels(&self) -> Result<Value, RpcFailure> {
        Ok(self
            .peer_channels
            .clone()
            .unwrap_or_else(|| serde_json::json!({"channels": []})))
    }

    fn get_channels_source(&self, source: &str) -> Result<Value, RpcFailure> {
        *self.gossip_calls.lock().unwrap() += 1;
        Ok(self
            .channels_by_source
            .get(source)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"channels": []})))
    }

    fn get_node_id(&self) -> Result<String, RpcFailure> {
        match &self.node_id {
            Some(id) => Ok(id.clone()),
            None => Err(RpcFailure {
                message: "getinfo failed".to_string(),
            }),
        }
    }
}

struct CapturedCall {
    candidate: RebalanceCandidate,
    rebalance_id: i64,
    kw: EngineKwargs,
}

#[derive(Default)]
struct MockEngine {
    script: Mutex<Vec<ExecutionResult>>,
    calls: Mutex<Vec<CapturedCall>>,
    run_cycles: Mutex<usize>,
    cycle_result: Mutex<Option<CycleResult>>,
}

impl MockEngine {
    fn new(script: Vec<ExecutionResult>) -> Arc<Self> {
        Arc::new(MockEngine {
            script: Mutex::new(script),
            ..MockEngine::default()
        })
    }
}

impl CandidateExecutor for MockEngine {
    fn execute_candidate(
        &self,
        candidate: &RebalanceCandidate,
        rebalance_id: i64,
        kw: EngineKwargs,
    ) -> ExecutionResult {
        let mut script = self.script.lock().unwrap();
        let result = if script.is_empty() {
            failed("unscripted_engine_call")
        } else {
            script.remove(0)
        };
        self.calls.lock().unwrap().push(CapturedCall {
            candidate: candidate.clone(),
            rebalance_id,
            kw,
        });
        result
    }

    fn run_cycle(&self) -> CycleResult {
        *self.run_cycles.lock().unwrap() += 1;
        self.cycle_result.lock().unwrap().take().unwrap_or_default()
    }
}

struct TestClock;

impl EngineClock for TestClock {
    fn now(&self) -> f64 {
        NOW
    }
}

fn failed(error: &str) -> ExecutionResult {
    ExecutionResult {
        success: false,
        attempts: 1,
        fee_sats: 0,
        fee_msat: 0,
        fee_ppm: 0,
        hops: 0,
        parts: 1,
        error: Some(error.to_string()),
        amount_sats: 0,
        payment_pending: false,
        payment_hash: None,
        excluded_channels: Vec::new(),
        route_type: "native",
        failure_data: serde_json::json!({}),
    }
}

fn success(fee_msat: i64) -> ExecutionResult {
    ExecutionResult {
        success: true,
        fee_sats: (fee_msat + 999) / 1000,
        fee_msat,
        error: None,
        ..failed("")
    }
}

fn pending(hash: Option<&str>) -> ExecutionResult {
    ExecutionResult {
        payment_pending: true,
        payment_hash: hash.map(str::to_string),
        ..failed("payment_pending_timeout: waitsendpay code 200")
    }
}

struct World {
    facade: EvRebalancer,
    store: Arc<MockStore>,
    engine: Arc<MockEngine>,
    rpc: Arc<ScriptedRpc>,
    log: SharedLog,
}

fn build(
    config: FacadeConfig,
    rpc: ScriptedRpc,
    engine_script: Vec<ExecutionResult>,
    customize: impl FnOnce(&mut MockFacadeStore),
) -> World {
    let log: SharedLog = Arc::new(Mutex::new(Vec::new()));
    let store = MockStore::new(log.clone());
    let mut fs = MockFacadeStore {
        log: log.clone(),
        hist_inbound: None,
        peer_history: Vec::new(),
        fees_by_since: HashMap::new(),
    };
    customize(&mut fs);
    let facade_store = Arc::new(fs);
    let engine = MockEngine::new(engine_script);
    let rpc = Arc::new(rpc);
    let facade = EvRebalancer::new(EvRebalancerDeps {
        engine: engine.clone(),
        config,
        store: store.clone(),
        facade_store,
        rpc: rpc.clone(),
        clock: Arc::new(TestClock),
    });
    World {
        facade,
        store,
        engine,
        rpc,
        log,
    }
}

fn log_lines(log: &SharedLog) -> Vec<String> {
    log.lock().unwrap().clone()
}

/// A funds/peer-channels world equivalent to the generator's `_defib_world`
/// but hand-built (used by the standalone behavior tests; the fixture
/// scenarios carry their own JSON).
fn world_rpc(spendables: &[(&str, i64)]) -> ScriptedRpc {
    let mut funds_channels = Vec::new();
    let mut peer_channels = Vec::new();
    for (i, (scid, spendable)) in spendables.iter().enumerate() {
        let peer_id = format!("02{i:02x}{}", "f".repeat(62));
        funds_channels.push(serde_json::json!({
            "state": "CHANNELD_NORMAL",
            "short_channel_id": scid,
            "our_amount_msat": spendable * 1000,
            "amount_msat": 10_000_000_000_i64,
            "peer_id": peer_id,
        }));
        peer_channels.push(serde_json::json!({
            "short_channel_id": scid,
            "state": "CHANNELD_NORMAL",
            "peer_id": peer_id,
            "fee_proportional_millionths": 50 + i as i64,
            "fee_base_msat": 0,
            "htlcs": [],
            "updates": {"remote": {
                "fee_proportional_millionths": 100,
                "fee_base_msat": 0,
            }},
        }));
    }
    ScriptedRpc {
        funds: Some(serde_json::json!({
            "outputs": [{"status": "confirmed", "amount_msat": 2_000_000_000_i64}],
            "channels": funds_channels,
        })),
        peer_channels: Some(serde_json::json!({"channels": peer_channels})),
        node_id: Some(format!("03aa{}", "0".repeat(62))),
        ..ScriptedRpc::default()
    }
}

fn candidate(amount_sats: i64, max_budget_sats: i64) -> RebalanceCandidate {
    RebalanceCandidate {
        source_candidates: vec!["100x1x0".to_string()],
        to_channel: "200x1x0".to_string(),
        primary_source_peer_id: "src_peer".to_string(),
        to_peer_id: "dest_peer".to_string(),
        amount_sats,
        amount_msat: amount_sats * 1000,
        outbound_fee_ppm: 500,
        inbound_fee_ppm: 100,
        source_fee_ppm: 50,
        weighted_opp_cost_ppm: 0,
        spread_ppm: 350,
        max_budget_sats,
        max_budget_msat: max_budget_sats * 1000,
        max_fee_ppm: 1000,
        expected_profit_sats: 10,
        liquidity_ratio: 0.5,
        dest_flow_state: "sink".to_string(),
        dest_turnover_rate: 0.0,
        source_turnover_rate: 0.0,
    }
}

// ---------------------------------------------------------------------------
// Fixture replays
// ---------------------------------------------------------------------------

#[test]
fn inbound_fee_replays_python_fixture() {
    let fx = fixture("inbound_fee");
    let cases = get(&fx, "cases").as_arr().expect("cases");
    assert!(
        cases.len() >= 15,
        "expected ~15+ cases, got {}",
        cases.len()
    );
    for case in cases {
        let name = get_str(case, "name");
        let peer_id = get_str(case, "peer_id");
        let amount_msat = get_i64(case, "amount_msat");
        let mut rpc = ScriptedRpc {
            node_id: opt_str(case, "our_id"),
            ..ScriptedRpc::default()
        };
        let gossip = get(case, "gossip_channels").to_serde_json();
        rpc.channels_by_source.insert(
            peer_id.to_string(),
            serde_json::json!({ "channels": gossip }),
        );
        let config = FacadeConfig {
            inbound_fee_estimate_ppm: get_i64(case, "inbound_fee_estimate_ppm"),
            ..FacadeConfig::default()
        };
        let hist = match get(case, "hist") {
            OValue::Null => None,
            h => Some(HistoricalInboundFee {
                confidence: get_str(h, "confidence").to_string(),
                median_fee_ppm: get_i64(h, "median_fee_ppm"),
                avg_fee_ppm: get_i64(h, "avg_fee_ppm"),
                sample_count: get_i64(h, "sample_count"),
            }),
        };
        let history: Vec<PeerRebalanceRecord> = get(case, "peer_history")
            .as_arr()
            .expect("peer_history")
            .iter()
            .map(|r| PeerRebalanceRecord {
                status: get_str(r, "status").to_string(),
                max_fee_sats: get_i64(r, "max_fee_sats"),
                amount_sats: get_i64(r, "amount_sats"),
            })
            .collect();
        let world = build(config, rpc, Vec::new(), |fs| {
            fs.hist_inbound = hist;
            fs.peer_history = history;
        });
        if let Some(pi) = case.get("peer_inbound").filter(|v| !v.is_null()) {
            world.facade.debug_set_peer_inbound_fee(
                peer_id,
                get_i64(pi, "fee_ppm"),
                get_i64(pi, "base_msat"),
            );
        }
        let got = world.facade.estimate_inbound_fee_at(peer_id, amount_msat);
        assert_eq!(got, get_i64(case, "expected_ppm"), "case {name}");
    }
}

#[test]
fn inbound_fee_memoizes_gossip_lookup_per_run() {
    let peer = "02peer";
    let mut rpc = ScriptedRpc {
        node_id: Some("03our".to_string()),
        ..ScriptedRpc::default()
    };
    rpc.channels_by_source.insert(
        peer.to_string(),
        serde_json::json!({"channels": [{"destination": "03our",
            "fee_per_millionth": 75, "base_fee_millisatoshi": 0}]}),
    );
    let world = build(FacadeConfig::default(), rpc, Vec::new(), |_| {});
    assert_eq!(world.facade.estimate_inbound_fee(peer), 125);
    assert_eq!(world.facade.estimate_inbound_fee(peer), 125);
    assert_eq!(
        *world.rpc.gossip_calls.lock().unwrap(),
        1,
        "second lookup must hit the memo cache (py _fee_cache)"
    );
}

#[test]
fn shock_fee_envelope_replays_python_fixture() {
    let fx = fixture("defib");
    let constants = get(&fx, "constants");
    assert_eq!(
        get_i64(constants, "DIAGNOSTIC_FEE_CAP_CEILING_SATS"),
        DIAGNOSTIC_FEE_CAP_CEILING_SATS
    );
    assert_eq!(
        get_i64(constants, "DIAGNOSTIC_MAX_SOURCE_ATTEMPTS"),
        DIAGNOSTIC_MAX_SOURCE_ATTEMPTS as i64
    );
    assert_eq!(get_i64(constants, "SHOCK_AMOUNT_SATS"), SHOCK_AMOUNT_SATS);
    let cases = get(&fx, "envelope_cases").as_arr().expect("cases");
    assert!(
        cases.len() >= 70,
        "envelope grid too small: {}",
        cases.len()
    );
    let mut saw_zero_daily = false;
    for case in cases {
        let diag = get_i64(case, "diag_max_fee_sats");
        let daily = get_i64(case, "daily_budget_sats");
        if daily == 0 {
            saw_zero_daily = true;
            // The plan's empirical question: a zero daily budget clamps the
            // cap to max(1, 0) = 1 sat. The fixture is the truth.
            assert_eq!(get_i64(case, "max_fee_sats"), 1, "diag={diag}");
        }
        let (sats, ppm) = shock_fee_envelope(diag, daily);
        assert_eq!(
            sats,
            get_i64(case, "max_fee_sats"),
            "diag={diag} daily={daily}"
        );
        assert_eq!(
            ppm,
            get_i64(case, "max_fee_ppm"),
            "diag={diag} daily={daily}"
        );
    }
    assert!(saw_zero_daily, "grid must include daily_budget_sats=0");
}

#[test]
fn is_source_route_failure_replays_python_fixture() {
    let fx = fixture("defib");
    let cases = get(&fx, "is_source_route_failure_cases")
        .as_arr()
        .expect("cases");
    assert!(cases.len() >= 15);
    for case in cases {
        let error = opt_str(case, "error");
        let expected = matches!(get(case, "expected"), OValue::Bool(true));
        assert_eq!(
            is_source_route_failure(error.as_deref()),
            expected,
            "error={error:?}"
        );
    }
}

#[test]
fn defib_scenarios_replay_python_fixture() {
    let fx = fixture("defib");
    let scenarios = get(&fx, "scenarios").as_arr().expect("scenarios");
    assert!(scenarios.len() >= 10);
    for scenario in scenarios {
        let name = get_str(scenario, "name");
        let cfg_fx = get(scenario, "config");
        let config = FacadeConfig {
            daily_budget_sats: get_i64(cfg_fx, "daily_budget_sats"),
            weekly_budget_sats: get_i64(cfg_fx, "weekly_budget_sats"),
            min_wallet_reserve: get_i64(cfg_fx, "min_wallet_reserve"),
            total_cost_budget_window_hours: get_i64(cfg_fx, "total_cost_budget_window_hours"),
            inbound_fee_estimate_ppm: get_i64(cfg_fx, "inbound_fee_estimate_ppm"),
            diagnostic_rebalance_max_fee_sats: get_i64(cfg_fx, "diagnostic_rebalance_max_fee_sats"),
            ..FacadeConfig::default()
        };
        let rpc = ScriptedRpc {
            funds: Some(get(scenario, "listfunds").to_serde_json()),
            peer_channels: Some(get(scenario, "listpeerchannels").to_serde_json()),
            node_id: Some(format!("03aa{}", "0".repeat(62))),
            ..ScriptedRpc::default()
        };
        let script: Vec<ExecutionResult> = get(scenario, "engine_script")
            .as_arr()
            .expect("script")
            .iter()
            .map(|spec| {
                let is_pending = matches!(spec.get("payment_pending"), Some(OValue::Bool(true)));
                ExecutionResult {
                    success: matches!(spec.get("success"), Some(OValue::Bool(true))),
                    fee_msat: spec.get("fee_msat").and_then(|v| v.as_i64()).unwrap_or(0),
                    payment_pending: is_pending,
                    payment_hash: is_pending.then(|| "ab".repeat(32)),
                    error: opt_str(spec, "error"),
                    ..failed("")
                }
            })
            .collect();
        let world = build(config, rpc, script, |_| {});

        let result = world
            .facade
            .diagnostic_rebalance(get_str(scenario, "channel_id"));

        let expected = get(scenario, "expected");
        assert_eq!(
            result,
            get(expected, "result").clone(),
            "case {name}: result"
        );

        let expected_calls = get(expected, "engine_calls").as_arr().expect("calls");
        let calls = world.engine.calls.lock().unwrap();
        assert_eq!(calls.len(), expected_calls.len(), "case {name}: call count");
        for (got, want) in calls.iter().zip(expected_calls) {
            let c = &got.candidate;
            assert_eq!(
                c.source_candidates,
                vec![get_str(want, "source")],
                "case {name}"
            );
            assert_eq!(c.to_channel, get_str(want, "to_channel"), "case {name}");
            assert_eq!(
                c.primary_source_peer_id,
                get_str(want, "source_peer_id"),
                "case {name}"
            );
            assert_eq!(c.to_peer_id, get_str(want, "to_peer_id"), "case {name}");
            assert_eq!(c.amount_sats, get_i64(want, "amount_sats"), "case {name}");
            assert_eq!(c.amount_msat, get_i64(want, "amount_msat"), "case {name}");
            assert_eq!(
                c.outbound_fee_ppm,
                get_i64(want, "outbound_fee_ppm"),
                "case {name}"
            );
            assert_eq!(
                c.inbound_fee_ppm,
                get_i64(want, "inbound_fee_ppm"),
                "case {name}"
            );
            assert_eq!(
                c.source_fee_ppm,
                get_i64(want, "source_fee_ppm"),
                "case {name}"
            );
            assert_eq!(
                c.max_budget_sats,
                get_i64(want, "max_budget_sats"),
                "case {name}"
            );
            assert_eq!(
                c.max_budget_msat,
                get_i64(want, "max_budget_msat"),
                "case {name}"
            );
            assert_eq!(c.max_fee_ppm, get_i64(want, "max_fee_ppm"), "case {name}");
            assert_eq!(
                c.expected_profit_sats,
                get_i64(want, "expected_profit_sats"),
                "case {name}"
            );
            assert_eq!(
                c.dest_flow_state,
                get_str(want, "dest_flow_state"),
                "case {name}"
            );
            assert_eq!(
                got.rebalance_id,
                get_i64(want, "rebalance_id"),
                "case {name}"
            );
            assert_eq!(
                got.kw.reserve_budget,
                matches!(get(want, "reserve_budget"), OValue::Bool(true)),
                "case {name}"
            );
            assert_eq!(
                got.kw.account_costs,
                matches!(get(want, "account_costs"), OValue::Bool(true)),
                "case {name}"
            );
        }

        let expected_db: Vec<String> = get(expected, "db_calls")
            .as_arr()
            .expect("db_calls")
            .iter()
            .map(|call| match get_str(call, "call") {
                "set_channel_probe" => format!(
                    "set_channel_probe:{}:{}",
                    get_str(call, "channel_id"),
                    get_str(call, "probe_type")
                ),
                "record_rebalance" => format!(
                    "record_rebalance:{}:{}:{}:{}:{}:{}:{}:{}",
                    get_i64(call, "id"),
                    get_str(call, "from_channel"),
                    get_str(call, "to_channel"),
                    get_i64(call, "amount_sats"),
                    get_i64(call, "max_fee_sats"),
                    get_i64(call, "expected_profit_sats"),
                    get_str(call, "rebalance_type"),
                    get_str(call, "reason_code")
                ),
                "update_rebalance_result" => format!(
                    "update_rebalance_result:{}:{}:{}",
                    get_i64(call, "id"),
                    get_str(call, "status"),
                    opt_str(call, "error_message").unwrap_or_default()
                ),
                other => panic!("unexpected db call {other}"),
            })
            .collect();
        assert_eq!(log_lines(&world.log), expected_db, "case {name}: db calls");
    }
}

// ---------------------------------------------------------------------------
// Standalone defibrillator behavior tests (plan Step 1 names)
// ---------------------------------------------------------------------------

const TARGET: &str = "700x1x0";

fn rich_world() -> ScriptedRpc {
    world_rpc(&[
        (TARGET, 500_000),
        ("101x1x0", 3_000_000),
        ("102x1x0", 9_000_000),
        ("103x1x0", 3_000_000),
        ("104x1x0", 100_000),
        ("105x1x0", 2_000_000),
    ])
}

#[test]
fn defib_advances_only_on_route_failure() {
    // source1 route_pricing_failed -> source2 native_sendpay_error -> STOP,
    // source3 untouched.
    let world = build(
        FacadeConfig::default(),
        rich_world(),
        vec![
            failed("route_pricing_failed: no_route (market)"),
            failed("native_sendpay_error: WIRE_TEMPORARY_CHANNEL_FAILURE (failcode=4103)"),
        ],
        |_| {},
    );
    let result = world.facade.diagnostic_rebalance(TARGET);
    let calls = world.engine.calls.lock().unwrap();
    assert_eq!(calls.len(), 2, "non-source failure must stop the sequence");
    // Ranked by spendable desc: 102 (9M) first, then 101 (3M; stable tie
    // ahead of 103).
    assert_eq!(calls[0].candidate.source_candidates, vec!["102x1x0"]);
    assert_eq!(calls[1].candidate.source_candidates, vec!["101x1x0"]);
    assert_eq!(
        result.get("shock_status").and_then(|v| v.as_str()),
        Some("failed")
    );
    assert_eq!(
        result.get("message").and_then(|v| v.as_str()),
        Some("Defibrillator active: Zero-Fee flag set + Shock failed")
    );
}

#[test]
fn defib_caps_ranked_sources_at_three() {
    let world = build(
        FacadeConfig::default(),
        rich_world(),
        vec![
            failed("route_pricing_failed: no_route (market)"),
            failed("native_route_invalid: first_hop_mismatch"),
            failed("no_route"),
        ],
        |_| {},
    );
    let _ = world.facade.diagnostic_rebalance(TARGET);
    let calls = world.engine.calls.lock().unwrap();
    assert_eq!(calls.len(), DIAGNOSTIC_MAX_SOURCE_ATTEMPTS);
    let sources: Vec<_> = calls
        .iter()
        .map(|c| c.candidate.source_candidates[0].clone())
        .collect();
    assert_eq!(sources, vec!["102x1x0", "101x1x0", "103x1x0"]);
}

#[test]
fn defib_budget_block_reports_blocked() {
    let world = build(
        FacadeConfig::default(),
        rich_world(),
        vec![failed(
            "local_budget_block: 0 sats remaining of 5000 unified budget",
        )],
        |_| {},
    );
    let result = world.facade.diagnostic_rebalance(TARGET);
    assert_eq!(world.engine.calls.lock().unwrap().len(), 1);
    assert_eq!(
        result.get("shock_status").and_then(|v| v.as_str()),
        Some("blocked")
    );
    assert_eq!(
        result.get("message").and_then(|v| v.as_str()),
        Some("Defibrillator active: Zero-Fee flag set + Shock blocked")
    );
}

#[test]
fn defib_pending_reports_pending_and_stops() {
    let world = build(
        FacadeConfig::default(),
        rich_world(),
        vec![pending(Some("aa"))],
        |_| {},
    );
    let result = world.facade.diagnostic_rebalance(TARGET);
    assert_eq!(world.engine.calls.lock().unwrap().len(), 1);
    assert_eq!(
        result.get("shock_status").and_then(|v| v.as_str()),
        Some("pending")
    );
    // The engine parked the shared row as pending_settlement; the facade
    // must NOT flip it to failed.
    assert!(
        !log_lines(&world.log)
            .iter()
            .any(|l| l.starts_with("update_rebalance_result:")),
        "pending row must be left for the reconcile sweep"
    );
}

#[test]
fn defib_capital_pre_gate_blocks_before_any_attempt() {
    let world = build(
        FacadeConfig {
            daily_budget_sats: 0,
            ..FacadeConfig::default()
        },
        rich_world(),
        Vec::new(),
        |_| {},
    );
    let result = world.facade.diagnostic_rebalance(TARGET);
    assert_eq!(world.engine.calls.lock().unwrap().len(), 0);
    assert_eq!(
        result.get("shock_status").and_then(|v| v.as_str()),
        Some("blocked")
    );
    assert_eq!(
        result.get("message").and_then(|v| v.as_str()),
        Some("Zero-Fee flag set, but Active Shock blocked: daily budget exhausted or reserve too low")
    );
    // The probe flag is set BEFORE the gate (py order).
    assert_eq!(
        log_lines(&world.log),
        vec![format!("set_channel_probe:{TARGET}:bounded_low_fee")]
    );
}

// ---------------------------------------------------------------------------
// execute_rebalance
// ---------------------------------------------------------------------------

#[test]
fn execute_rebalance_releases_only_when_not_sweepable_pending() {
    // (a) pending WITH hash: sweepable -> reservation HELD (P4-009).
    let world = build(
        FacadeConfig::default(),
        world_rpc(&[]),
        vec![pending(Some("beef"))],
        |_| {},
    );
    let result =
        world
            .facade
            .execute_rebalance(&candidate(100_000, 200), true, &ExecuteOpts::default());
    assert!(!result.success);
    assert!(result.payment_pending);
    let lines = log_lines(&world.log);
    assert!(
        !lines.iter().any(|l| l.starts_with("release_reservation:")),
        "sweepable pending must HOLD the reservation: {lines:?}"
    );
    assert!(!lines.iter().any(|l| l.starts_with("mark_budget_spent:")));
    assert!(
        !lines
            .iter()
            .any(|l| l.starts_with("update_rebalance_result:") && l.contains(":failed:")),
        "pending row stays pending_settlement: {lines:?}"
    );
    assert!(lines
        .iter()
        .any(|l| l.starts_with("increment_failure_count:dest_peer:")));

    // (b) pending WITHOUT hash: not sweepable -> released.
    let world = build(
        FacadeConfig::default(),
        world_rpc(&[]),
        vec![pending(None)],
        |_| {},
    );
    let _ = world
        .facade
        .execute_rebalance(&candidate(100_000, 200), true, &ExecuteOpts::default());
    let lines = log_lines(&world.log);
    assert!(
        lines.iter().any(|l| l == "release_reservation:1"),
        "no-hash pending must release: {lines:?}"
    );

    // (c) terminal failure: released, history failed (py writes the status
    // twice: rebalancer.py:1566 and :1643).
    let world = build(
        FacadeConfig::default(),
        world_rpc(&[]),
        vec![failed("native_sendpay_error: boom")],
        |_| {},
    );
    let _ = world
        .facade
        .execute_rebalance(&candidate(100_000, 200), true, &ExecuteOpts::default());
    let lines = log_lines(&world.log);
    assert!(lines.iter().any(|l| l == "release_reservation:1"));
    assert_eq!(
        lines
            .iter()
            .filter(|l| **l == "update_rebalance_result:1:failed:native_sendpay_error: boom")
            .count(),
        2,
        "py double-writes the failed status: {lines:?}"
    );
}

#[test]
fn execute_rebalance_success_marks_spent_and_records_cost() {
    let world = build(
        FacadeConfig::default(),
        world_rpc(&[]),
        vec![success(12_345)],
        |_| {},
    );
    let result =
        world
            .facade
            .execute_rebalance(&candidate(100_000, 200), true, &ExecuteOpts::default());
    assert!(result.success);
    let lines = log_lines(&world.log);
    let expected = vec![
        "record_rebalance:1:100x1x0:200x1x0:100000:200:10:normal:ev_positive".to_string(),
        "reserve_budget:1:200:5000".to_string(),
        "update_history_success:1:fee_sats=13:fee_msat=12345:amount=None:ratio=None".to_string(),
        "record_rebalance_cost:200x1x0:dest_peer:13:12345:100000".to_string(),
        "reset_failure_count:dest_peer".to_string(),
        "mark_budget_spent:1:13".to_string(),
    ];
    assert_eq!(lines, expected);
    let summary = world.facade.last_decision_summary();
    assert_eq!(summary.action, "rebalance");
    assert_eq!(summary.reason, "rebalance_completed");
}

#[test]
fn execute_rebalance_budget_block_attribution_and_message() {
    // remaining == 0 -> daily attribution; the history error message is the
    // byte-preserved Python template.
    let world = build(FacadeConfig::default(), world_rpc(&[]), Vec::new(), |_| {});
    world
        .store
        .script_reserve(Err(BudgetBlock::Refused { remaining: 0 }));
    let result =
        world
            .facade
            .execute_rebalance(&candidate(100_000, 200), true, &ExecuteOpts::default());
    assert!(!result.success);
    assert_eq!(result.error.as_deref(), Some("local_budget_block"));
    assert_eq!(world.engine.calls.lock().unwrap().len(), 0);
    let lines = log_lines(&world.log);
    assert!(
        lines.contains(
            &"update_rebalance_result:1:failed:Unified liquidity budget exhausted: \
              0 sats remaining for rebalances after external costs (0 spent + 0 reserved) \
              of total 5000"
                .to_string()
        ),
        "{lines:?}"
    );
    assert_eq!(
        world.facade.last_decision_summary().dominant_input,
        "daily_budget_sats"
    );

    // remaining > 0 but < limit -> weekly was the binding constraint.
    let world = build(FacadeConfig::default(), world_rpc(&[]), Vec::new(), |_| {});
    world
        .store
        .script_reserve(Err(BudgetBlock::Refused { remaining: 3 }));
    let _ = world
        .facade
        .execute_rebalance(&candidate(100_000, 200), true, &ExecuteOpts::default());
    assert_eq!(
        world.facade.last_decision_summary().dominant_input,
        "weekly_budget_sats"
    );
}

#[test]
fn execute_rebalance_budget_limits_capex_and_hot_override() {
    // Capex with daily > 0: limit = min(capex, daily).
    let world = build(
        FacadeConfig::default(),
        world_rpc(&[]),
        vec![success(0)],
        |_| {},
    );
    let opts = ExecuteOpts {
        reason_code: "capex_fallback".to_string(),
        ..ExecuteOpts::default()
    };
    let _ = world
        .facade
        .execute_rebalance(&candidate(100_000, 300), true, &opts);
    assert_eq!(
        world.store.reserve_requests()[0].effective_budget_sats,
        Some(300)
    );

    // Capex with daily == 0: capex budget alone is the limit.
    let world = build(
        FacadeConfig {
            daily_budget_sats: 0,
            ..FacadeConfig::default()
        },
        world_rpc(&[]),
        vec![success(0)],
        |_| {},
    );
    let _ = world
        .facade
        .execute_rebalance(&candidate(100_000, 300), true, &opts);
    assert_eq!(
        world.store.reserve_requests()[0].effective_budget_sats,
        Some(300)
    );

    // Hot override raises a lower limit but is capped at the effective
    // daily budget.
    let world = build(
        FacadeConfig::default(),
        world_rpc(&[]),
        vec![success(0)],
        |_| {},
    );
    let opts = ExecuteOpts {
        reason_code: "capex_fallback".to_string(),
        dynamic_budget_override_sats: 8_000,
        ..ExecuteOpts::default()
    };
    let _ = world
        .facade
        .execute_rebalance(&candidate(100_000, 300), true, &opts);
    assert_eq!(
        world.store.reserve_requests()[0].effective_budget_sats,
        Some(5_000),
        "override 8000 capped at daily 5000, raising the capex limit 300"
    );

    // Weekly rail params flow through (py passes weekly limit + 7d window).
    let req = &world.store.reserve_requests()[0];
    assert_eq!(req.weekly_budget_limit, Some(35_000));
    assert_eq!(req.weekly_since_timestamp, Some(NOW as i64 - 7 * 86_400));
    assert_eq!(req.since_timestamp, Some(NOW as i64 - 24 * 3600));
    assert_eq!(req.amount_sats, 300);
    assert_eq!(req.channel_id.as_deref(), Some("200x1x0"));
}

#[test]
fn execute_rebalance_invalid_ids_short_circuits() {
    let world = build(FacadeConfig::default(), world_rpc(&[]), Vec::new(), |_| {});
    let mut c = candidate(100_000, 200);
    c.source_candidates = Vec::new();
    let result = world
        .facade
        .execute_rebalance(&c, true, &ExecuteOpts::default());
    assert!(!result.success);
    assert_eq!(
        result.error.as_deref(),
        Some("Invalid channel IDs - from_channel or to_channel is empty")
    );
    assert!(log_lines(&world.log).is_empty(), "no store calls");
    assert_eq!(world.engine.calls.lock().unwrap().len(), 0);
}

#[test]
fn execute_rebalance_manual_mode_skips_reservation_when_not_enforced() {
    let world = build(
        FacadeConfig::default(),
        world_rpc(&[]),
        vec![success(1_000)],
        |_| {},
    );
    let _ =
        world
            .facade
            .execute_rebalance(&candidate(100_000, 200), false, &ExecuteOpts::default());
    let lines = log_lines(&world.log);
    assert!(!lines.iter().any(|l| l.starts_with("reserve_budget:")));
    assert!(!lines.iter().any(|l| l.starts_with("mark_budget_spent:")));
}

// ---------------------------------------------------------------------------
// Capital controls
// ---------------------------------------------------------------------------

#[test]
fn capital_controls_wallet_reserve_fails_open_on_rpc_error() {
    let rpc = ScriptedRpc {
        funds_error: true,
        ..ScriptedRpc::default()
    };
    let world = build(FacadeConfig::default(), rpc, Vec::new(), |_| {});
    assert!(
        world.facade.check_capital_controls(),
        "listfunds failure skips the reserve check; budget still enforced"
    );
}

#[test]
fn capital_controls_wallet_reserve_blocks() {
    let world = build(
        FacadeConfig {
            min_wallet_reserve: 10_000_000_000,
            ..FacadeConfig::default()
        },
        rich_world(),
        Vec::new(),
        |_| {},
    );
    assert!(!world.facade.check_capital_controls());
    assert_eq!(world.facade.capital_control_blocker(), "min_wallet_reserve");
}

#[test]
fn capital_controls_daily_budget_blocks_fail_closed() {
    let world = build(FacadeConfig::default(), rich_world(), Vec::new(), |fs| {
        fs.fees_by_since.insert(NOW as i64 - 24 * 3600, 5_000);
    });
    assert!(!world.facade.check_capital_controls());
    assert_eq!(world.facade.capital_control_blocker(), "daily_budget_sats");
}

#[test]
fn capital_controls_weekly_budget_blocks() {
    let world = build(FacadeConfig::default(), rich_world(), Vec::new(), |fs| {
        fs.fees_by_since.insert(NOW as i64 - 24 * 3600, 100);
        fs.fees_by_since.insert(NOW as i64 - 7 * 86_400, 35_000);
    });
    assert!(!world.facade.check_capital_controls());
    assert_eq!(world.facade.capital_control_blocker(), "weekly_budget_sats");
}

#[test]
fn capital_controls_zero_daily_budget_blocks() {
    // 0 spent >= 0 budget: Python's >= comparison blocks a zero budget.
    let world = build(
        FacadeConfig {
            daily_budget_sats: 0,
            ..FacadeConfig::default()
        },
        rich_world(),
        Vec::new(),
        |_| {},
    );
    assert!(!world.facade.check_capital_controls());
    assert_eq!(world.facade.capital_control_blocker(), "daily_budget_sats");
}

// ---------------------------------------------------------------------------
// find_rebalance_candidates
// ---------------------------------------------------------------------------

#[test]
fn find_rebalance_candidates_always_empty_and_delegates_to_run_cycle() {
    let world = build(FacadeConfig::default(), rich_world(), Vec::new(), |_| {});
    let out = world.facade.find_rebalance_candidates();
    assert!(out.is_empty(), "E-4.5: ALWAYS []");
    assert_eq!(*world.engine.run_cycles.lock().unwrap(), 1);
    // Stale-reservation cleanup ran first with reservation_timeout_hours*3600.
    assert_eq!(log_lines(&world.log)[0], "cleanup_stale_reservations:14400");
}

#[test]
fn find_rebalance_candidates_capital_blocked_skips_engine() {
    let world = build(
        FacadeConfig {
            daily_budget_sats: 0,
            ..FacadeConfig::default()
        },
        rich_world(),
        Vec::new(),
        |_| {},
    );
    let out = world.facade.find_rebalance_candidates();
    assert!(out.is_empty());
    assert_eq!(*world.engine.run_cycles.lock().unwrap(), 0);
    let summary = world.facade.last_decision_summary();
    assert_eq!(summary.action, "suppressed");
    assert_eq!(summary.reason, "capital_controls_blocked");
    assert_eq!(summary.dominant_input, "daily_budget_sats");
    assert!(summary.budget_blocked);
}

// ---------------------------------------------------------------------------
// manual_rebalance
// ---------------------------------------------------------------------------

#[test]
fn manual_rebalance_clamps_amount_regardless_of_force() {
    let world = build(
        FacadeConfig::default(),
        rich_world(),
        vec![success(2_000)],
        |_| {},
    );
    let result = world.facade.manual_rebalance(
        "101:1:0", // ':' SCIDs normalize to 'x'
        "700:1:0",
        9_000_000, // over rebalance_max_amount 5_000_000
        Some(500),
        true,
    );
    let calls = world.engine.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let c = &calls[0].candidate;
    assert_eq!(c.source_candidates, vec!["101x1x0"]);
    assert_eq!(c.to_channel, "700x1x0");
    assert_eq!(
        c.amount_sats, 5_000_000,
        "DD2/P1-004 hard cap binds even with force"
    );
    assert_eq!(c.dest_flow_state, "manual");
    assert_eq!(
        result.get("success"),
        Some(&OValue::Bool(true)),
        "{result:?}"
    );
    assert_eq!(
        result.get("message").and_then(|v| v.as_str()),
        Some("completed")
    );
    assert_eq!(
        result.get("actual_fee_sats").and_then(|v| v.as_i64()),
        Some(2)
    );
}

#[test]
fn manual_rebalance_default_fee_floors_at_100_when_spread_negative() {
    let world = build(
        FacadeConfig::default(),
        rich_world(),
        vec![success(0)],
        |_| {},
    );
    // dest fee_ppm 50 (idx 0 is target: 50+0=50); est_in = 100 remote + 50
    // buffer = 150; src fee 51 -> spread negative -> floor 100.
    let _ = world
        .facade
        .manual_rebalance("101x1x0", TARGET, 1_000_000, None, false);
    let calls = world.engine.calls.lock().unwrap();
    assert_eq!(calls[0].candidate.max_budget_sats, 100);
    assert_eq!(calls[0].candidate.max_fee_ppm, 100); // int(100*1e6/1_000_000)
}

#[test]
fn manual_rebalance_missing_channels_errors() {
    let world = build(FacadeConfig::default(), rich_world(), Vec::new(), |_| {});
    let result = world
        .facade
        .manual_rebalance("999x9x9", TARGET, 100_000, None, false);
    assert_eq!(
        result.get("error").and_then(|v| v.as_str()),
        Some("Channels not found")
    );
}

#[test]
fn manual_rebalance_warns_when_capital_blocked_without_force() {
    let world = build(
        FacadeConfig {
            daily_budget_sats: 0, // capital controls block
            ..FacadeConfig::default()
        },
        rich_world(),
        vec![failed("no_route")],
        |_| {},
    );
    let result = world
        .facade
        .manual_rebalance("101x1x0", TARGET, 100_000, Some(50), false);
    assert_eq!(
        result
            .get("capital_controls_warning")
            .and_then(|v| v.as_str()),
        Some("Budget exhausted or reserve low (manual override)")
    );
    // force=true suppresses the warning key.
    let world = build(
        FacadeConfig {
            daily_budget_sats: 0,
            ..FacadeConfig::default()
        },
        rich_world(),
        vec![failed("no_route")],
        |_| {},
    );
    let result = world
        .facade
        .manual_rebalance("101x1x0", TARGET, 100_000, Some(50), true);
    assert!(result.get("capital_controls_warning").is_none());
}
