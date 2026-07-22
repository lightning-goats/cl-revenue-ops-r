//! End-to-end fee-cycle parity tests (Phase 4 Task 10).
//!
//! `fixtures/fees/cycle/scenarios.json` holds 18 seeded single-channel
//! scenarios generated from the REAL Python
//! `_adjust_all_fees_channel_loop`/`_adjust_channel_fee` (port worktree,
//! `tools/port/gen_fees_fixtures.py cycle`) with a frozen module clock and
//! scripted doubles. Each cycle pins:
//! - the full `FeeAdjustment.to_dict()` (or the loop skip reason) —
//!   compared CONTENT-identically with floats as py_repr (the fixture
//!   encodes every float as `{"__f__": repr}`; both sides render through
//!   `pyjson::dumps_python`, so int/float distinction is enforced);
//! - the serialized post-cycle state (`_serialize_cycle_state_payload`);
//! - a DTS/PID post-state subset (posterior mean/std, streak, PID dict);
//! - the NEXT `random()` draw from the same stream (draw-count desync
//!   canary — one missed or extra gauss()/random() call fails this pin).
//!
//! Plus: execution-layer clamp/gossip-governed unit tests and the
//! 3-channel synthetic integration cycle (journal parses, would_broadcast
//! consistency, serialize-once state flush).

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::path::PathBuf;

use revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode};
use revops_econ::pyfloat::py_repr;
use revops_fees::cycle::{
    self, process_channel, run_fee_cycle, ChannelCycleState, ChannelFeeState, ChannelInfo,
    ChannelOutcome, ChannelResult, ChannelStateRow, ControllerState, CycleDeps, DecisionClock,
    FeeCfgSnapshot, FeeEvidence, FixedDecisionClock, GossipRow, PeerFeeHistory, SkipGateEpoch,
    StateSink,
};
use revops_fees::execution::{
    decide_set_channel_fee, fail_closed, governed_authorize_fee_broadcast, FeeAuthorizationRequest,
    FeeAuthorizationResult, FeeAuthorizer, GovernedDeps, PureFeeExecutor, SetFeeRequest,
};
use revops_fees::drain::NodeChannel;
use revops_fees::floors::{ChainCosts, FlowWindow, PeerLatency, RebalanceCostSample};
use revops_fees::journal::{FeeDecision, Journal};
use revops_fees::pid::pid_from_dict;
use revops_fees::pid::pid_to_dict;
use revops_fees::pyjson::{dumps_python, parse, OValue};
use revops_fees::pyrand::{DecisionInputError, PyRandom};
use revops_fees::thompson::serde::gts_from_dict;

const PURE_EXECUTOR: PureFeeExecutor = PureFeeExecutor;

fn fixture_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("../../fixtures/fees/{rel}"))
}

fn load_scenarios() -> OValue {
    let raw = std::fs::read_to_string(fixture_path("cycle/scenarios.json")).expect("fixture");
    let v = parse(&raw).expect("valid JSON");
    decode_floats(&v)
}

/// Decode the generator's `{"__f__": "<repr>"}` float tags into
/// `OValue::Float` (bit-exact: repr strings are shortest-round-trip).
fn decode_floats(v: &OValue) -> OValue {
    match v {
        OValue::Obj(entries) => {
            if entries.len() == 1 && entries[0].0 == "__f__" {
                if let OValue::Str(s) = &entries[0].1 {
                    let f: f64 = match s.as_str() {
                        "inf" => f64::INFINITY,
                        "-inf" => f64::NEG_INFINITY,
                        "nan" => f64::NAN,
                        _ => s.parse().expect("float repr"),
                    };
                    return OValue::Float(f);
                }
            }
            OValue::Obj(
                entries
                    .iter()
                    .map(|(k, val)| (k.clone(), decode_floats(val)))
                    .collect(),
            )
        }
        OValue::Arr(items) => OValue::Arr(items.iter().map(decode_floats).collect()),
        other => other.clone(),
    }
}

/// Coerce `posterior_mean`/`posterior_std` Int -> Float (see call site).
fn numeric_normalize_posterior(v: &OValue) -> OValue {
    match v {
        OValue::Obj(entries) => OValue::Obj(
            entries
                .iter()
                .map(|(k, val)| {
                    let val = match (k.as_str(), val) {
                        ("posterior_mean" | "posterior_std", OValue::Int(i)) => {
                            OValue::Float(*i as f64)
                        }
                        _ => val.clone(),
                    };
                    (k.clone(), val)
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

fn geti(v: &OValue, key: &str) -> i64 {
    v.get(key).and_then(|x| x.as_i64()).unwrap_or_default()
}

fn getf(v: &OValue, key: &str) -> f64 {
    v.get(key).and_then(|x| x.as_f64()).unwrap_or_default()
}

fn gets(v: &OValue, key: &str) -> String {
    v.get(key)
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string()
}

fn getb(v: &OValue, key: &str) -> bool {
    matches!(v.get(key), Some(OValue::Bool(true)))
}

fn opt_i64(v: &OValue, key: &str) -> Option<i64> {
    match v.get(key) {
        None | Some(OValue::Null) => None,
        Some(x) => x.as_i64(),
    }
}

fn opt_f64(v: &OValue, key: &str) -> Option<f64> {
    match v.get(key) {
        None | Some(OValue::Null) => None,
        Some(x) => x.as_f64(),
    }
}

// ---------------------------------------------------------------------------
// Fixture-scripted evidence
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FixtureEvidence {
    our_id: String,
    volume_map: RefCell<BTreeMap<String, i64>>,
    forward_map: RefCell<BTreeMap<String, i64>>,
    probe_flag: Cell<bool>,
    gossip: Vec<GossipRow>,
    gossip_calls: Cell<usize>,
    captured_neighbor_median: Option<Option<i64>>,
    latency: Option<PeerLatency>,
    cost_history: Vec<RebalanceCostSample>,
    peer_fee_history: Option<PeerFeeHistory>,
    channel_cost_history_calls: Cell<usize>,
    peer_fee_history_calls: Cell<usize>,
    last_forward: Cell<Option<i64>>,
    last_forward_calls: Cell<usize>,
    flow_window_calls: Cell<usize>,
    policy: Option<PeerPolicy>,
    marginal_roi: Option<f64>,
    overlay_active: Cell<bool>,
}

impl FixtureEvidence {
    fn lookup(map: &BTreeMap<String, i64>, since: i64) -> i64 {
        if let Some(v) = map.get(&since.to_string()) {
            return *v;
        }
        map.get("default").copied().unwrap_or(0)
    }
}

impl FixtureEvidence {
    fn our_node_id(&self) -> String {
        self.our_id.clone()
    }
    fn channel_states(&self) -> Vec<ChannelStateRow> {
        Vec::new()
    }
    fn channels_info(&self) -> BTreeMap<String, ChannelInfo> {
        BTreeMap::new()
    }
    fn chain_costs(&self) -> Option<ChainCosts> {
        None
    }
    fn volume_since(&self, _channel_id: &str, since: i64) -> i64 {
        Self::lookup(&self.volume_map.borrow(), since)
    }
    fn forward_count_since(&self, _channel_id: &str, since: i64) -> i64 {
        Self::lookup(&self.forward_map.borrow(), since)
    }
    fn exploration_flag(&self, _channel_id: &str) -> bool {
        self.probe_flag.get()
    }
    fn clear_exploration_flag(&self, _channel_id: &str) {
        self.probe_flag.set(false);
    }
    fn gossip_channels(&self, _peer_id: &str) -> Vec<GossipRow> {
        self.gossip_calls
            .set(self.gossip_calls.get().saturating_add(1));
        self.gossip.clone()
    }
    fn captured_neighbor_fee_median(&self, _peer_id: &str) -> Option<Option<i64>> {
        self.captured_neighbor_median
    }
    fn peer_latency(&self, _peer_id: &str) -> Option<PeerLatency> {
        self.latency
    }
    fn channel_cost_history(&self, _channel_id: &str, _since: i64) -> Vec<RebalanceCostSample> {
        self.channel_cost_history_calls
            .set(self.channel_cost_history_calls.get().saturating_add(1));
        self.cost_history.clone()
    }
    fn peer_fee_history(&self, _peer_id: &str) -> Option<PeerFeeHistory> {
        self.peer_fee_history_calls
            .set(self.peer_fee_history_calls.get().saturating_add(1));
        self.peer_fee_history.clone()
    }
    fn last_forward_time(&self, _channel_id: &str) -> Option<i64> {
        self.last_forward_calls
            .set(self.last_forward_calls.get().saturating_add(1));
        self.last_forward.get()
    }
    fn flow_window(&self, _channel_id: &str) -> Option<FlowWindow> {
        self.flow_window_calls
            .set(self.flow_window_calls.get().saturating_add(1));
        None
    }
    fn policy(&self, _peer_id: &str) -> Option<PeerPolicy> {
        self.policy.clone()
    }
    fn marginal_roi_percent(&self, _channel_id: &str) -> Option<f64> {
        self.marginal_roi
    }
    fn temporary_overlay_active(&self, _channel_id: &str) -> bool {
        self.overlay_active.get()
    }
    fn node_channels(&self) -> Vec<NodeChannel> {
        Vec::new()
    }
}

fn parse_cfg(v: &OValue) -> FeeCfgSnapshot {
    FeeCfgSnapshot {
        min_fee_ppm: geti(v, "min_fee_ppm"),
        max_fee_ppm: geti(v, "max_fee_ppm"),
        min_fee_ppm_saturated: geti(v, "min_fee_ppm_saturated"),
        fee_interval: geti(v, "fee_interval"),
        flow_interval: geti(v, "flow_interval"),
        htlc_congestion_threshold: getf(v, "htlc_congestion_threshold"),
        market_fee_mode: gets(v, "market_fee_mode"),
        drain_fee_discount_max: getf(v, "drain_fee_discount_max"),
        high_liquidity_threshold: getf(v, "high_liquidity_threshold"),
        fee_profile: gets(v, "fee_profile"),
        base_fee_msat: geti(v, "base_fee_msat"),
        enable_vegas_reflex: getb(v, "enable_vegas_reflex"),
        enable_dynamic_htlcmax: serde_json::Value::Bool(getb(v, "enable_dynamic_htlcmax")),
        htlcmax_source_pct: getf(v, "htlcmax_source_pct"),
        htlcmax_sink_pct: getf(v, "htlcmax_sink_pct"),
        htlcmax_balanced_pct: getf(v, "htlcmax_balanced_pct"),
        paused: getb(v, "paused"),
        node_drain_bias_enabled: getb(v, "node_drain_bias_enabled"),
        // Fixture cfg blobs predate this field; the generating Python
        // Config always carried its 0.3 default (config.py:537).
        node_drain_bias_max: v
            .get("node_drain_bias_max")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.3),
        receivable_ratio_target: 0.30,
        receivable_ratio_floor: 0.20,
        econ_governor_fees_enabled: getb(v, "econ_governor_fees_enabled"),
        authority_level: None,
    }
}

/// `neighbor_median_min_competitors` (Phase 4b Task 8a) is NOT a
/// `FeeCfgSnapshot` field (see `CycleDeps::min_competitors`'s doc), so it
/// is parsed separately from the same fixture `cfg` blob. Falls back to
/// `market::MIN_COMPETITORS` (mirroring Python's own inline
/// `getattr(cfg, 'neighbor_median_min_competitors', 3)` fallback-of-last-
/// resort) when the fixture's `cfg` predates this field.
fn parse_min_competitors(v: &OValue) -> usize {
    v.get("neighbor_median_min_competitors")
        .and_then(|x| x.as_i64())
        .filter(|n| *n > 0)
        .map(|n| n as usize)
        .unwrap_or(revops_fees::market::MIN_COMPETITORS)
}

fn parse_policy(v: &OValue) -> Option<PeerPolicy> {
    match v {
        OValue::Null => None,
        obj => {
            let strategy = match gets(obj, "strategy").as_str() {
                "static" => FeeStrategy::Static,
                "passive" => FeeStrategy::Passive,
                _ => FeeStrategy::Dynamic,
            };
            Some(PeerPolicy {
                peer_id: String::new(),
                strategy,
                rebalance_mode: RebalanceMode::Enabled,
                fee_ppm_target: opt_i64(obj, "fee_ppm_target"),
                tags: Vec::new(),
                updated_at: 0,
                fee_multiplier_min: opt_f64(obj, "fee_multiplier_min"),
                fee_multiplier_max: opt_f64(obj, "fee_multiplier_max"),
                expires_at: None,
            })
        }
    }
}

fn parse_gossip(v: &OValue) -> Vec<GossipRow> {
    let rows = match v.as_arr() {
        Some(a) => a,
        None => return Vec::new(),
    };
    rows.iter()
        .map(|r| GossipRow {
            source: gets(r, "source"),
            active: getb(r, "active"),
            fee_per_millionth: geti(r, "fee_per_millionth"),
            satoshis: opt_i64(r, "satoshis"),
            amount_msat: opt_i64(r, "amount_msat"),
            last_update: geti(r, "last_update"),
            base_fee_msat: opt_i64(r, "base_fee_millisatoshi"),
        })
        .collect()
}

fn parse_cost_history(v: &OValue) -> Vec<RebalanceCostSample> {
    let rows = match v.as_arr() {
        Some(a) => a,
        None => return Vec::new(),
    };
    rows.iter()
        .map(|r| RebalanceCostSample {
            cost_sats: geti(r, "cost_sats"),
            amount_sats: geti(r, "amount_sats"),
            timestamp: geti(r, "timestamp"),
        })
        .collect()
}

fn parse_map(v: Option<&OValue>) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    if let Some(OValue::Obj(entries)) = v {
        for (k, val) in entries {
            if let Some(i) = val.as_i64() {
                out.insert(k.clone(), i);
            }
        }
    }
    out
}

fn parse_cycle_state(v: &OValue) -> ChannelCycleState {
    // Post-unification (T9 state_store types) the shared scalars are
    // private: hydrate them through the setters, then clear the
    // explicit-shared markers — fixture hydration mirrors a LOAD, not a
    // caller-initiated write.
    let mut s = ChannelCycleState::default();
    s.last_revenue_rate = getf(v, "last_revenue_rate");
    s.last_fee_ppm = geti(v, "last_fee_ppm");
    s.trend_direction = geti(v, "trend_direction");
    s.step_ppm = geti(v, "step_ppm");
    s.last_update = geti(v, "last_update");
    s.set_last_broadcast_at(geti(v, "last_broadcast_at"));
    s.consecutive_same_direction = geti(v, "consecutive_same_direction");
    s.is_sleeping = getb(v, "is_sleeping");
    s.sleep_until = geti(v, "sleep_until");
    s.stable_cycles = geti(v, "stable_cycles");
    s.last_broadcast_fee_ppm = geti(v, "last_broadcast_fee_ppm");
    s.last_state = gets(v, "last_state");
    s.forward_count_since_update = geti(v, "forward_count_since_update");
    s.last_volume_sats = geti(v, "last_volume_sats");
    s.congestion_active = getb(v, "congestion_active");
    s.congestion_quiet_cycles = geti(v, "congestion_quiet_cycles");
    s.congestion_entry_fee_ppm = geti(v, "congestion_entry_fee_ppm");
    s.pending_target_ppm = geti(v, "pending_target_ppm");
    s.set_last_gossip_refresh(geti(v, "last_gossip_refresh"));
    s.set_dynamic_htlcmin_baseline_msat(opt_i64(v, "dynamic_htlcmin_baseline_msat"));
    s.clear_explicit_shared_fields();
    s
}

fn parse_fee_state(v: &OValue) -> ChannelFeeState {
    let thompson = gts_from_dict(v.get("thompson_state").expect("thompson_state"));
    let pid = pid_from_dict(v.get("pid_state").expect("pid_state"));
    let sc = v.get("scalars").expect("scalars");
    // Same load-shaped hydration discipline as `parse_cycle_state`.
    let mut s = ChannelFeeState::default();
    s.thompson = thompson;
    s.pid = pid;
    s.last_revenue_rate = getf(sc, "last_revenue_rate");
    s.last_fee_ppm = geti(sc, "last_fee_ppm");
    s.last_broadcast_fee_ppm = geti(sc, "last_broadcast_fee_ppm");
    s.last_update = geti(sc, "last_update");
    s.set_last_broadcast_at(geti(sc, "last_broadcast_at"));
    s.last_state = gets(sc, "last_state");
    s.is_sleeping = getb(sc, "is_sleeping");
    s.sleep_until = geti(sc, "sleep_until");
    s.stable_cycles = geti(sc, "stable_cycles");
    s.forward_count_since_update = geti(sc, "forward_count_since_update");
    s.last_volume_sats = geti(sc, "last_volume_sats");
    s.set_last_gossip_refresh(geti(sc, "last_gossip_refresh"));
    s.last_vegas_multiplier = getf(sc, "last_vegas_multiplier");
    s.clear_explicit_shared_fields();
    s
}

fn parse_channel_info(v: &OValue) -> ChannelInfo {
    ChannelInfo {
        channel_id: gets(v, "channel_id"),
        short_channel_id: gets(v, "short_channel_id"),
        full_channel_id: gets(v, "full_channel_id"),
        peer_id: gets(v, "peer_id"),
        capacity_sats: geti(v, "capacity"),
        spendable_msat: geti(v, "spendable_msat"),
        receivable_msat: geti(v, "receivable_msat"),
        fee_base_msat: geti(v, "fee_base_msat"),
        fee_proportional_millionths: geti(v, "fee_proportional_millionths"),
        htlc_minimum_msat: geti(v, "htlc_minimum_msat"),
        htlc_min_msat: geti(v, "htlc_min_msat"),
        htlc_maximum_msat: geti(v, "htlc_maximum_msat"),
        htlc_max_msat: geti(v, "htlc_max_msat"),
        opener: gets(v, "opener"),
        has_htlc_data: getb(v, "has_htlc_data"),
        max_accepted_htlcs: geti(v, "max_accepted_htlcs"),
        our_htlcs_in_flight: geti(v, "our_htlcs_in_flight"),
    }
}

fn parse_state_row(v: &OValue, channel_id: &str, peer_id: &str) -> ChannelStateRow {
    ChannelStateRow {
        channel_id: channel_id.to_string(),
        peer_id: peer_id.to_string(),
        state: gets(v, "state"),
        updated_at: opt_i64(v, "updated_at"),
        kalman_flow_ratio: opt_f64(v, "kalman_flow_ratio"),
        kalman_velocity: opt_f64(v, "kalman_velocity"),
    }
}

/// The generator's `_post_fee_state` shape (same key order).
fn post_fee_state(ts: &ChannelFeeState) -> OValue {
    OValue::obj(vec![
        (
            "posterior_mean".to_string(),
            OValue::Float(ts.thompson.posterior_mean),
        ),
        (
            "posterior_std".to_string(),
            OValue::Float(ts.thompson.posterior_std),
        ),
        (
            "zero_revenue_streak".to_string(),
            OValue::Int(ts.thompson.zero_revenue_streak),
        ),
        (
            "observation_count".to_string(),
            OValue::Int(ts.thompson.observations.len() as i64),
        ),
        (
            "last_sampled_fee".to_string(),
            OValue::Int(ts.thompson.last_sampled_fee),
        ),
        ("pid_state".to_string(), pid_to_dict(&ts.pid)),
        (
            "scalars".to_string(),
            OValue::obj(vec![
                (
                    "last_revenue_rate".to_string(),
                    OValue::Float(ts.last_revenue_rate),
                ),
                ("last_fee_ppm".to_string(), OValue::Int(ts.last_fee_ppm)),
                (
                    "last_broadcast_fee_ppm".to_string(),
                    OValue::Int(ts.last_broadcast_fee_ppm),
                ),
                ("last_update".to_string(), OValue::Int(ts.last_update)),
                (
                    "last_broadcast_at".to_string(),
                    OValue::Int(ts.last_broadcast_at()),
                ),
                ("last_state".to_string(), OValue::str(ts.last_state.clone())),
                ("is_sleeping".to_string(), OValue::Bool(ts.is_sleeping)),
                ("sleep_until".to_string(), OValue::Int(ts.sleep_until)),
                ("stable_cycles".to_string(), OValue::Int(ts.stable_cycles)),
                (
                    "forward_count_since_update".to_string(),
                    OValue::Int(ts.forward_count_since_update),
                ),
                (
                    "last_volume_sats".to_string(),
                    OValue::Int(ts.last_volume_sats),
                ),
                (
                    "last_gossip_refresh".to_string(),
                    OValue::Int(ts.last_gossip_refresh()),
                ),
                (
                    "last_vegas_multiplier".to_string(),
                    OValue::Float(ts.last_vegas_multiplier),
                ),
            ]),
        ),
        (
            "last_context_key".to_string(),
            OValue::str(ts.last_context_key.clone()),
        ),
        (
            "last_contextual_sample_used".to_string(),
            OValue::Bool(ts.last_contextual_sample_used),
        ),
    ])
}

// ---------------------------------------------------------------------------
// The 18-scenario replay
// ---------------------------------------------------------------------------

#[test]
fn cycle_scenarios_replay_bit_for_bit() {
    let root = load_scenarios();
    let scenarios = root
        .get("scenarios")
        .and_then(|s| s.as_arr())
        .expect("scenarios");
    assert_eq!(scenarios.len(), 18, "scenario count");

    for scn in scenarios {
        let name = gets(scn, "name");
        let seed = geti(scn, "seed") as u64;
        let channel_id = gets(scn, "channel_id");
        let peer_id = gets(scn, "peer_id");
        let cfg = parse_cfg(scn.get("cfg").expect("cfg"));
        let min_competitors = parse_min_competitors(scn.get("cfg").expect("cfg"));

        let mut policy = parse_policy(scn.get("policy").unwrap_or(&OValue::Null));
        if let Some(p) = &mut policy {
            p.peer_id = peer_id.clone();
        }
        let evidence = FixtureEvidence {
            our_id: gets(scn, "our_id"),
            gossip: parse_gossip(scn.get("gossip").unwrap_or(&OValue::Null)),
            latency: scn.get("latency").map(|l| PeerLatency {
                avg: getf(l, "avg"),
                std: getf(l, "std"),
            }),
            cost_history: parse_cost_history(scn.get("cost_history").unwrap_or(&OValue::Null)),
            peer_fee_history: match scn.get("peer_fee_history") {
                Some(OValue::Obj(_)) => {
                    let h = scn.get("peer_fee_history").expect("checked");
                    Some(PeerFeeHistory {
                        confidence: gets(h, "confidence"),
                        avg_fee_ppm: geti(h, "avg_fee_ppm"),
                    })
                }
                _ => None,
            },
            policy,
            marginal_roi: opt_f64(scn, "marginal_roi"),
            ..FixtureEvidence::default()
        };
        evidence.probe_flag.set(getb(scn, "probe_flag"));
        evidence.last_forward.set(opt_i64(scn, "last_forward_time"));

        let initial = scn.get("initial_state").expect("initial_state");
        let mut state = ControllerState::new();
        state.vegas.intensity = getf(scn, "vegas_intensity");
        state.cycle_states.insert(
            channel_id.clone(),
            parse_cycle_state(initial.get("cycle").expect("cycle")),
        );
        state.fee_states.insert(
            channel_id.clone(),
            parse_fee_state(initial.get("fee").expect("fee")),
        );

        let mut rng = PyRandom::seed_from_u64(seed);

        for (i, cyc) in scn
            .get("cycles")
            .and_then(|c| c.as_arr())
            .expect("cycles")
            .iter()
            .enumerate()
        {
            let tag = format!("{name}/cycle{i}");
            let now = geti(cyc, "now");
            *evidence.volume_map.borrow_mut() = parse_map(cyc.get("volume_since"));
            *evidence.forward_map.borrow_mut() = parse_map(cyc.get("forward_count_since"));
            if let Some(OValue::Bool(b)) = cyc.get("probe_flag") {
                evidence.probe_flag.set(*b);
            }
            evidence.last_forward.set(opt_i64(cyc, "last_forward_time"));

            let info = parse_channel_info(cyc.get("channel_info").expect("channel_info"));
            let row = parse_state_row(
                cyc.get("state_row").expect("state_row"),
                &channel_id,
                &peer_id,
            );
            let chain_costs: Option<ChainCosts> = match cyc.get("chain_costs") {
                Some(OValue::Obj(_)) => {
                    let c = cyc.get("chain_costs").expect("checked");
                    Some(ChainCosts {
                        open_cost_sats: geti(c, "open_cost_sats"),
                        close_cost_sats: geti(c, "close_cost_sats"),
                        sat_per_vbyte: getf(c, "sat_per_vbyte"),
                    })
                }
                _ => None,
            };

            let result = {
                let mut clock = FixedDecisionClock::new(now);
                let mut deps = CycleDeps {
                    evidence: &evidence,
                    cfg: &cfg,
                    rng: &mut rng,
                    clock: &mut clock,
                    authorizer: None,
                    executor: &PURE_EXECUTOR,
                    journal: None,
                    state_sink: None,
                    min_competitors,
                };
                process_channel(
                    &mut state,
                    &mut deps,
                    &row,
                    &info,
                    chain_costs.as_ref(),
                    None,
                    None,
                    None,
                )
                .expect("fixed decision inputs")
            };

            let expected = cyc.get("expected").expect("expected");
            let expected_adjustment = expected.get("adjustment").expect("adjustment key");
            let expected_skip = expected.get("skip_reason").expect("skip key");

            match &result.outcome {
                ChannelOutcome::Adjusted(adj) => {
                    assert!(
                        !expected_adjustment.is_null(),
                        "{tag}: Rust adjusted but Python skipped ({:?})",
                        expected_skip
                    );
                    assert_eq!(
                        dumps_python(&adj.to_dict()),
                        dumps_python(expected_adjustment),
                        "{tag}: FeeAdjustment.to_dict mismatch"
                    );
                }
                ChannelOutcome::Skipped(reason) => {
                    assert!(
                        expected_adjustment.is_null(),
                        "{tag}: Rust skipped ({reason}) but Python adjusted"
                    );
                    assert_eq!(
                        Some(*reason),
                        expected_skip.as_str(),
                        "{tag}: skip reason mismatch"
                    );
                }
            }

            // Post-cycle state pins.
            let post_cycle = state.cycle_states.get(&channel_id).expect("cycle state");
            assert_eq!(
                dumps_python(&cycle::serialize_cycle_state_payload(post_cycle)),
                dumps_python(expected.get("post_cycle_state").expect("post_cycle_state")),
                "{tag}: post_cycle_state mismatch"
            );
            let post_fee = state.fee_states.get(&channel_id).expect("fee state");
            // Python's posterior_mean/std may be ints (`max(MIN_STD, x)`
            // resolves to the int literal when it wins); the Rust struct
            // is f64. Numeric identity is the contract here — coerce the
            // EXPECTED ints to floats for those two keys only (the wire
            // blob's int-ness is T3 serde's pinned concern, not this
            // diagnostic pin's).
            let expected_fee = numeric_normalize_posterior(
                expected.get("post_fee_state").expect("post_fee_state"),
            );
            assert_eq!(
                dumps_python(&post_fee_state(post_fee)),
                dumps_python(&expected_fee),
                "{tag}: post_fee_state mismatch"
            );

            // RNG stream pin: the NEXT random() draw, bit-for-bit.
            let expected_draw = expected
                .get("rng_next_random")
                .and_then(|v| v.as_str())
                .expect("rng_next_random");
            assert_eq!(
                py_repr(rng.random()),
                expected_draw,
                "{tag}: RNG stream desync (draw-count parity broken)"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// execution.rs unit tests
// ---------------------------------------------------------------------------

fn base_cfg() -> FeeCfgSnapshot {
    FeeCfgSnapshot {
        min_fee_ppm: 10,
        max_fee_ppm: 5000,
        ..FeeCfgSnapshot::default()
    }
}

#[test]
fn set_fee_clamp_log_byte_exact() {
    let cfg = base_cfg();
    let d = decide_set_channel_fee(
        &SetFeeRequest {
            channel_id: "820x1x0".to_string(),
            fee_ppm: 9999999,
            enforce_limits: true,
            effective_min_fee_ppm: None,
            htlcmax_msat: None,
            base_fee_msat: 0,
        },
        &cfg,
        None,
    );
    assert!(d.success);
    assert_eq!(d.clamped_fee_ppm, 5000);
    assert_eq!(d.message, "Fee set to 5000 PPM");
    // Frozen string (py 7683-7687): `{channel_id[:16]}...` keeps short ids.
    assert_eq!(
        d.clamp_log.as_deref(),
        Some("FEE_LIMIT: Clamped fee for 820x1x0... from 9999999 to 5000 (limits: 10-5000 PPM)")
    );
}

#[test]
fn set_fee_abs_clamp_when_limits_bypassed() {
    let cfg = base_cfg();
    let d = decide_set_channel_fee(
        &SetFeeRequest {
            channel_id: "820x1x0".to_string(),
            fee_ppm: 250_000,
            enforce_limits: false,
            effective_min_fee_ppm: None,
            htlcmax_msat: None,
            base_fee_msat: 0,
        },
        &cfg,
        None,
    );
    assert_eq!(d.clamped_fee_ppm, 100_000);
    assert_eq!(
        d.clamp_log.as_deref(),
        Some(
            "FEE_LIMIT: Clamped fee for 820x1x0... from 250000 to 100000 \
             (absolute: 0-100000 PPM; economic limits bypassed)"
        )
    );
}

#[test]
fn effective_min_only_lowers_the_min_term() {
    let cfg = base_cfg();
    // effective_min above the global min is capped AT the global min.
    let d = decide_set_channel_fee(
        &SetFeeRequest {
            channel_id: "c".to_string(),
            fee_ppm: 1,
            enforce_limits: true,
            effective_min_fee_ppm: Some(500),
            htlcmax_msat: None,
            base_fee_msat: 0,
        },
        &cfg,
        None,
    );
    assert_eq!(d.clamped_fee_ppm, 10, "may never RAISE the min term");
    // effective_min below the global min lowers it (E-2).
    let d = decide_set_channel_fee(
        &SetFeeRequest {
            channel_id: "c".to_string(),
            fee_ppm: 3,
            enforce_limits: true,
            effective_min_fee_ppm: Some(2),
            htlcmax_msat: None,
            base_fee_msat: 0,
        },
        &cfg,
        None,
    );
    assert_eq!(d.clamped_fee_ppm, 3);
}

#[test]
fn passive_skips_static_pins() {
    let cfg = base_cfg();
    let mut policy = PeerPolicy::default_for("p");
    policy.strategy = FeeStrategy::Passive;
    let req = SetFeeRequest {
        channel_id: "c".to_string(),
        fee_ppm: 777,
        enforce_limits: true,
        effective_min_fee_ppm: None,
        htlcmax_msat: None,
        base_fee_msat: 0,
    };
    let d = decide_set_channel_fee(&req, &cfg, Some(&policy));
    assert!(!d.success);

    policy.strategy = FeeStrategy::Static;
    policy.fee_ppm_target = Some(9000);
    let d = decide_set_channel_fee(&req, &cfg, Some(&policy));
    assert!(d.success);
    assert_eq!(d.clamped_fee_ppm, 5000, "STATIC pins then clamps");
}

#[test]
fn governed_fail_closed_internal_error_string() {
    // The frozen wrapper (py 7625): f"internal_error ({e})".
    let (ok, code, trace) = fail_closed(&revops_econ::types::EconError {
        msg: "boom".to_string(),
    });
    assert!(!ok);
    assert_eq!(code, "internal_error (boom)");
    assert!(trace.is_none());

    // A real error path (invalid clock) also fails closed with the shape.
    let deps = GovernedDeps {
        ledger: None,
        registry: None,
        paused: false,
        authority_level: None,
    };
    let (ok, code, trace) =
        governed_authorize_fee_broadcast(&deps, "820x1x0", 100, Some(50), "r", Some("c"), -5);
    assert!(!ok);
    assert!(code.starts_with("internal_error ("), "got: {code}");
    assert!(trace.is_none());
}

#[test]
fn governed_zero_cost_emits_no_budget_reserved_event() {
    let dir = tempfile::tempdir().expect("tmpdir");
    let ledger =
        revops_econ::ledger::EconLedger::open(dir.path().join("econ_ledger.db")).expect("ledger");
    let deps = GovernedDeps {
        ledger: Some(&ledger),
        registry: None,
        paused: false,
        authority_level: Some("capital".to_string()),
    };
    let (ok, code, trace) = governed_authorize_fee_broadcast(
        &deps,
        "820x1x0",
        150,
        Some(100),
        "DTS+PID: test",
        Some("dts_pid_sample"),
        1_752_400_000,
    );
    assert!(ok, "authorize failed: {code}");
    assert_eq!(code, "");
    let trace = trace.expect("trace");
    assert!(trace.authorized);
    assert!(trace.intent_id.starts_with("int-"));

    // Zero-cost SET_FEE: intent_proposed + intent_authorized, and NO
    // budget_reserved event (Phase 2 governor contract).
    assert_eq!(
        ledger.count_events(Some("intent_proposed")).expect("count"),
        1
    );
    assert_eq!(
        ledger
            .count_events(Some("intent_authorized"))
            .expect("count"),
        1
    );
    assert_eq!(
        ledger.count_events(Some("budget_reserved")).expect("count"),
        0
    );
}

#[test]
fn default_cfg_authority_level_authorizes_governed_fees_broadcast() {
    // Important-2 review finding: `FeeCfgSnapshot::default()` documents
    // itself as mirroring Python `Config` defaults verbatim, and Python's
    // default IS "capital" (config.py:572, `authority_level: str =
    // "capital"`) — not a missing/None attribute. A `GovernedDeps` wired
    // straight from the default snapshot must therefore authorize a
    // "fees"-level broadcast, not fail closed to AUTHORITY_LEVEL_BLOCKED.
    let cfg = FeeCfgSnapshot::default();
    assert_eq!(cfg.authority_level.as_deref(), Some("capital"));

    let deps = GovernedDeps {
        ledger: None,
        registry: None,
        paused: false,
        authority_level: cfg.authority_level.clone(),
    };
    let (ok, code, trace) = governed_authorize_fee_broadcast(
        &deps,
        "820x1x0",
        150,
        Some(100),
        "r",
        None,
        1_752_400_000,
    );
    assert!(
        ok,
        "default-config governed broadcast should authorize: {code}"
    );
    assert_eq!(code, "");
    assert!(trace.expect("trace").authorized);
}

#[test]
fn default_cfg_matches_python_config_defaults() {
    // Important finding fix (Phase 4 final review, cycle.rs:155-184 /
    // fbc9ef2-era): `FeeCfgSnapshot::default()`'s doc claims it mirrors
    // Python `Config` defaults, but 4 fields had silently drifted to the
    // Rust fixture generator's `_base_cfg` test-helper values instead
    // (max_fee_ppm 5000 vs 2000, htlcmax_sink_pct 0.9 vs 0.25,
    // htlcmax_balanced_pct 0.75 vs 0.45, min_fee_ppm 0 vs 10). A 4b wiring
    // fallback to `Default` would have silently run a 2.5x-wrong ceiling.
    //
    // `fixtures/config_types.json` only carries field *types*/ranges/enums,
    // not default VALUES, so it cannot drive this comparison — this is the
    // cited inline table the review's fallback directive asks for. Every
    // entry is a `(field, python_default, config.py_line)` triple; keep
    // this table AND `FeeCfgSnapshot::default()`'s per-field `// py
    // config.py:N` comments in sync on every `Config` change.
    let cfg = FeeCfgSnapshot::default();

    // (field name, python default repr, config.py line) — verify against
    // ~/bin/cl_revenue_ops-port modules/config.py, branch `port` == `main`.
    assert_eq!(cfg.min_fee_ppm, 10, "config.py:596 min_fee_ppm");
    assert_eq!(cfg.max_fee_ppm, 2000, "config.py:605 max_fee_ppm");
    assert_eq!(
        cfg.min_fee_ppm_saturated, 0,
        "config.py:604 min_fee_ppm_saturated"
    );
    assert_eq!(cfg.fee_interval, 1800, "config.py:506 fee_interval");
    assert_eq!(cfg.flow_interval, 3600, "config.py:505 flow_interval");
    assert_eq!(
        cfg.htlc_congestion_threshold, 0.8,
        "config.py:738 htlc_congestion_threshold"
    );
    assert_eq!(
        cfg.market_fee_mode, "undercut",
        "config.py:630 market_fee_mode"
    );
    assert_eq!(
        cfg.drain_fee_discount_max, 0.0,
        "config.py:529 drain_fee_discount_max"
    );
    assert_eq!(
        cfg.high_liquidity_threshold, 0.7,
        "config.py:653 high_liquidity_threshold"
    );
    assert_eq!(cfg.fee_profile, "active", "config.py:631 fee_profile");
    assert_eq!(cfg.base_fee_msat, 0, "config.py:606 base_fee_msat");
    assert!(cfg.enable_vegas_reflex, "config.py:765 enable_vegas_reflex");
    assert_eq!(
        cfg.enable_dynamic_htlcmax,
        serde_json::Value::Bool(false),
        "config.py:542 enable_dynamic_htlcmax"
    );
    assert_eq!(
        cfg.htlcmax_source_pct, 0.50,
        "config.py:543 htlcmax_source_pct"
    );
    assert_eq!(cfg.htlcmax_sink_pct, 0.25, "config.py:544 htlcmax_sink_pct");
    assert_eq!(
        cfg.htlcmax_balanced_pct, 0.45,
        "config.py:545 htlcmax_balanced_pct"
    );
    assert!(!cfg.paused, "config.py:761 paused");
    assert!(
        !cfg.node_drain_bias_enabled,
        "config.py:535 node_drain_bias_enabled"
    );
    assert_eq!(
        cfg.node_drain_bias_max, 0.3,
        "config.py:537 node_drain_bias_max"
    );
    assert_eq!(
        cfg.receivable_ratio_target, 0.30,
        "config.py:522 receivable_ratio_target"
    );
    assert_eq!(
        cfg.receivable_ratio_floor, 0.20,
        "config.py:523 receivable_ratio_floor"
    );
    assert!(
        !cfg.econ_governor_fees_enabled,
        "config.py:560 econ_governor_fees_enabled"
    );
    assert_eq!(
        cfg.authority_level.as_deref(),
        Some("capital"),
        "config.py:572 authority_level"
    );
}

#[test]
fn governed_paused_refuses_without_reservation() {
    let deps = GovernedDeps {
        ledger: None,
        registry: None,
        paused: true,
        authority_level: Some("capital".to_string()),
    };
    let (ok, code, trace) = governed_authorize_fee_broadcast(
        &deps,
        "820x1x0",
        150,
        Some(100),
        "r",
        None,
        1_752_400_000,
    );
    assert!(!ok);
    assert_eq!(code, "PAUSED");
    assert_eq!(trace.expect("trace").reason_code, "PAUSED");
}

#[test]
fn py_float_component_renders_via_py_repr() {
    // Phase 2 carry: floats must be py_repr strings before render().
    let v = revops_fees::execution::py_float_component(0.1 + 0.2);
    assert_eq!(v, serde_json::Value::String("0.30000000000000004".into()));
    let e = revops_econ::intents::Explanation {
        kind: "fee_broadcast".to_string(),
        components: vec![("ratio".to_string(), v)],
    };
    assert_eq!(e.render(), "fee_broadcast: ratio=0.30000000000000004");
}

// ---------------------------------------------------------------------------
// Journal + integration (Step 3): 3-channel synthetic end-to-end cycle
// ---------------------------------------------------------------------------

struct SyntheticEvidence {
    rows: Vec<ChannelStateRow>,
    infos: BTreeMap<String, ChannelInfo>,
    volumes: BTreeMap<String, i64>,
    forwards: BTreeMap<String, i64>,
    passive_peer: String,
    node_channels: Vec<NodeChannel>,
}

impl SyntheticEvidence {
    fn our_node_id(&self) -> String {
        "02".to_string() + &"aa".repeat(32)
    }
    fn channel_states(&self) -> Vec<ChannelStateRow> {
        self.rows.clone()
    }
    fn channels_info(&self) -> BTreeMap<String, ChannelInfo> {
        self.infos.clone()
    }
    fn chain_costs(&self) -> Option<ChainCosts> {
        None
    }
    fn volume_since(&self, channel_id: &str, _since: i64) -> i64 {
        self.volumes.get(channel_id).copied().unwrap_or(0)
    }
    fn forward_count_since(&self, channel_id: &str, _since: i64) -> i64 {
        self.forwards.get(channel_id).copied().unwrap_or(0)
    }
    fn exploration_flag(&self, _channel_id: &str) -> bool {
        false
    }
    fn clear_exploration_flag(&self, _channel_id: &str) {}
    fn gossip_channels(&self, _peer_id: &str) -> Vec<GossipRow> {
        Vec::new()
    }
    fn captured_neighbor_fee_median(&self, _peer_id: &str) -> Option<Option<i64>> {
        None
    }
    fn peer_latency(&self, _peer_id: &str) -> Option<PeerLatency> {
        None
    }
    fn channel_cost_history(&self, _channel_id: &str, _since: i64) -> Vec<RebalanceCostSample> {
        Vec::new()
    }
    fn peer_fee_history(&self, _peer_id: &str) -> Option<PeerFeeHistory> {
        None
    }
    fn last_forward_time(&self, _channel_id: &str) -> Option<i64> {
        None
    }
    fn flow_window(&self, _channel_id: &str) -> Option<FlowWindow> {
        None
    }
    fn policy(&self, peer_id: &str) -> Option<PeerPolicy> {
        if peer_id == self.passive_peer {
            let mut p = PeerPolicy::default_for(peer_id);
            p.strategy = FeeStrategy::Passive;
            Some(p)
        } else {
            None
        }
    }
    fn marginal_roi_percent(&self, _channel_id: &str) -> Option<f64> {
        None
    }
    fn temporary_overlay_active(&self, _channel_id: &str) -> bool {
        false
    }
    fn node_channels(&self) -> Vec<NodeChannel> {
        self.node_channels.clone()
    }
}

#[derive(Default)]
struct CountingSink {
    calls: Cell<usize>,
    rows: Cell<usize>,
}

impl StateSink for CountingSink {
    fn flush_batch(&self, rows: &[(String, ChannelCycleState, ChannelFeeState)]) {
        self.calls.set(self.calls.get() + 1);
        self.rows.set(rows.len());
    }
}

struct RecordingClock {
    now: i64,
    labels: Vec<String>,
}

impl DecisionClock for RecordingClock {
    fn now(&mut self, label: &str) -> Result<i64, DecisionInputError> {
        self.labels.push(label.to_string());
        Ok(self.now)
    }
}

struct SemanticClock {
    base: i64,
    labels: Vec<String>,
}

impl SemanticClock {
    fn new(base: i64) -> Self {
        Self {
            base,
            labels: Vec::new(),
        }
    }
}

impl DecisionClock for SemanticClock {
    fn now(&mut self, label: &str) -> Result<i64, DecisionInputError> {
        self.labels.push(label.to_string());
        Ok(match label {
            "fee.apply" => self.base + 10,
            "fee.state_sync" => self.base + 20,
            _ => self.base,
        })
    }
}

struct FixtureCycleCase {
    channel_id: String,
    cfg: FeeCfgSnapshot,
    min_competitors: usize,
    evidence: FixtureEvidence,
    state: ControllerState,
    rng: PyRandom,
    row: ChannelStateRow,
    info: ChannelInfo,
    chain_costs: Option<ChainCosts>,
    now: i64,
}

fn fixture_cycle_case(name: &str) -> FixtureCycleCase {
    let root = load_scenarios();
    let scenario = root
        .get("scenarios")
        .and_then(|s| s.as_arr())
        .expect("scenarios")
        .iter()
        .find(|s| gets(s, "name") == name)
        .unwrap_or_else(|| panic!("missing fixture scenario {name}"));
    let cycle = scenario
        .get("cycles")
        .and_then(|c| c.as_arr())
        .expect("cycles")
        .first()
        .expect("first cycle");
    let channel_id = gets(scenario, "channel_id");
    let peer_id = gets(scenario, "peer_id");
    let cfg = parse_cfg(scenario.get("cfg").expect("cfg"));
    let min_competitors = parse_min_competitors(scenario.get("cfg").expect("cfg"));
    let mut policy = parse_policy(scenario.get("policy").unwrap_or(&OValue::Null));
    if let Some(policy) = &mut policy {
        policy.peer_id = peer_id.clone();
    }
    let evidence = FixtureEvidence {
        our_id: gets(scenario, "our_id"),
        volume_map: RefCell::new(parse_map(cycle.get("volume_since"))),
        forward_map: RefCell::new(parse_map(cycle.get("forward_count_since"))),
        probe_flag: Cell::new(
            cycle
                .get("probe_flag")
                .and_then(|v| match v {
                    OValue::Bool(value) => Some(*value),
                    _ => None,
                })
                .unwrap_or_else(|| getb(scenario, "probe_flag")),
        ),
        gossip: parse_gossip(scenario.get("gossip").unwrap_or(&OValue::Null)),
        gossip_calls: Cell::new(0),
        captured_neighbor_median: None,
        latency: scenario.get("latency").map(|latency| PeerLatency {
            avg: getf(latency, "avg"),
            std: getf(latency, "std"),
        }),
        cost_history: parse_cost_history(scenario.get("cost_history").unwrap_or(&OValue::Null)),
        peer_fee_history: match scenario.get("peer_fee_history") {
            Some(OValue::Obj(_)) => {
                let history = scenario.get("peer_fee_history").expect("checked");
                Some(PeerFeeHistory {
                    confidence: gets(history, "confidence"),
                    avg_fee_ppm: geti(history, "avg_fee_ppm"),
                })
            }
            _ => None,
        },
        last_forward: Cell::new(opt_i64(cycle, "last_forward_time")),
        last_forward_calls: Cell::new(0),
        channel_cost_history_calls: Cell::new(0),
        peer_fee_history_calls: Cell::new(0),
        flow_window_calls: Cell::new(0),
        policy,
        marginal_roi: opt_f64(scenario, "marginal_roi"),
        overlay_active: Cell::new(false),
    };
    let initial = scenario.get("initial_state").expect("initial_state");
    let mut state = ControllerState::new();
    state.vegas.intensity = getf(scenario, "vegas_intensity");
    state.cycle_states.insert(
        channel_id.clone(),
        parse_cycle_state(initial.get("cycle").expect("cycle")),
    );
    state.fee_states.insert(
        channel_id.clone(),
        parse_fee_state(initial.get("fee").expect("fee")),
    );
    let chain_costs = match cycle.get("chain_costs") {
        Some(OValue::Obj(_)) => {
            let costs = cycle.get("chain_costs").expect("checked");
            Some(ChainCosts {
                open_cost_sats: geti(costs, "open_cost_sats"),
                close_cost_sats: geti(costs, "close_cost_sats"),
                sat_per_vbyte: getf(costs, "sat_per_vbyte"),
            })
        }
        _ => None,
    };
    let row = parse_state_row(
        cycle.get("state_row").expect("state_row"),
        &channel_id,
        &peer_id,
    );
    FixtureCycleCase {
        channel_id,
        cfg,
        min_competitors,
        evidence,
        state,
        rng: PyRandom::seed_from_u64(geti(scenario, "seed") as u64),
        row,
        info: parse_channel_info(cycle.get("channel_info").expect("channel_info")),
        chain_costs,
        now: geti(cycle, "now"),
    }
}

fn run_fixture_case(case: &mut FixtureCycleCase, clock: &mut dyn DecisionClock) -> ChannelResult {
    let mut deps = CycleDeps {
        evidence: &case.evidence,
        cfg: &case.cfg,
        rng: &mut case.rng,
        clock,
        authorizer: None,
        executor: &PURE_EXECUTOR,
        journal: None,
        state_sink: None,
        min_competitors: case.min_competitors,
    };
    process_channel(
        &mut case.state,
        &mut deps,
        &case.row,
        &case.info,
        case.chain_costs.as_ref(),
        None,
        None,
        None,
    )
    .expect("scripted decision inputs")
}

fn assert_optimizer_timestamps(case: &FixtureCycleCase, expected: i64) {
    let cycle = case
        .state
        .cycle_states
        .get(&case.channel_id)
        .expect("cycle state");
    let fee = case
        .state
        .fee_states
        .get(&case.channel_id)
        .expect("fee state");
    assert_eq!(cycle.last_update, expected, "cycle last_update");
    assert_eq!(cycle.last_broadcast_at(), expected, "cycle broadcast time");
    assert_eq!(fee.last_update, expected, "fee last_update");
    assert_eq!(fee.last_broadcast_at(), expected, "fee broadcast time");
}

#[test]
fn balanced_non_saturated_channel_does_not_read_flow_window() {
    let mut case = fixture_cycle_case("dts_pid_undercut");
    let mut clock = SemanticClock::new(case.now);

    run_fixture_case(&mut case, &mut clock);

    assert_eq!(
        case.evidence.flow_window_calls.get(),
        0,
        "Python only reads flow-window evidence for source or saturated channels"
    );
}

#[test]
fn fee_below_zero_flow_threshold_does_not_read_last_forward() {
    let mut case = fixture_cycle_case("congestion_episode");
    let mut clock = SemanticClock::new(case.now);

    run_fixture_case(&mut case, &mut clock);

    assert_eq!(case.evidence.last_forward_calls.get(), 0);
}

#[test]
fn inactive_hard_floor_reads_python_cost_evidence_and_clocks_in_order() {
    let mut case = fixture_cycle_case("dts_pid_undercut");
    let mut clock = SemanticClock::new(case.now);

    run_fixture_case(&mut case, &mut clock);

    assert_eq!(case.evidence.channel_cost_history_calls.get(), 2);
    assert_eq!(case.evidence.peer_fee_history_calls.get(), 1);
    assert_eq!(
        clock.labels,
        [
            "cycle.channel.evaluate",
            "channel.adjust",
            "rebalance_cost_floor.cutoff",
            "rebalance_cost_history.cutoff",
            "thompson.posterior.update",
            "thompson.posterior.recompute",
            "thompson.contextual.update",
            "thompson.last_sample_time",
            "pid.calculate",
            "thompson.supported_fee_ceiling",
            "thompson.earning_region",
            "thompson.meaningful_rate",
            "fee.apply"
        ]
    );
}

#[test]
fn active_hard_floor_skips_peer_fallback_and_soft_cost_history() {
    let mut case = fixture_cycle_case("floor_inversion");
    let mut clock = SemanticClock::new(case.now);

    run_fixture_case(&mut case, &mut clock);

    assert_eq!(case.evidence.channel_cost_history_calls.get(), 1);
    assert_eq!(case.evidence.peer_fee_history_calls.get(), 0);
    assert!(clock
        .labels
        .iter()
        .any(|label| label == "rebalance_cost_floor.cutoff"));
    assert!(!clock
        .labels
        .iter()
        .any(|label| label == "rebalance_cost_history.cutoff"));
}

#[test]
fn temporary_overlay_does_not_consume_channel_evaluation_time() {
    let mut case = fixture_cycle_case("policy_passive");
    case.evidence.overlay_active.set(true);
    let mut clock = SemanticClock::new(case.now);
    let result = run_fixture_case(&mut case, &mut clock);
    assert!(matches!(
        result.outcome,
        ChannelOutcome::Skipped("temporary_overlay")
    ));
    assert!(clock.labels.is_empty(), "{:?}", clock.labels);
}

#[test]
fn passive_policy_does_not_consume_channel_evaluation_time() {
    let mut case = fixture_cycle_case("policy_passive");
    let mut clock = SemanticClock::new(case.now);
    let result = run_fixture_case(&mut case, &mut clock);
    assert!(matches!(
        result.outcome,
        ChannelOutcome::Skipped("policy_passive")
    ));
    assert!(clock.labels.is_empty(), "{:?}", clock.labels);
}

#[test]
fn static_policy_starts_its_clock_at_the_explicit_set_fee_path() {
    let mut case = fixture_cycle_case("policy_static");
    let mut clock = SemanticClock::new(case.now);
    let result = run_fixture_case(&mut case, &mut clock);
    assert!(matches!(result.outcome, ChannelOutcome::Adjusted(_)));
    assert_eq!(clock.labels, ["fee.apply", "fee.state_sync"]);
    assert_optimizer_timestamps(&case, case.now + 20);
}

#[test]
fn sparse_neighbor_nudge_consumes_its_semantic_clock() {
    let mut case = fixture_cycle_case("sparse_median_nudge");
    case.info.spendable_msat = 0;
    for volume in case.evidence.volume_map.borrow_mut().values_mut() {
        *volume = 0;
    }
    case.state
        .fee_states
        .get_mut(&case.channel_id)
        .expect("fee state")
        .thompson
        .posterior_std = 500.0;
    let mut clock = SemanticClock::new(case.now);

    run_fixture_case(&mut case, &mut clock);

    assert_eq!(
        clock
            .labels
            .iter()
            .filter(|label| label.as_str() == "thompson.posterior_nudge")
            .count(),
        1
    );
}

#[test]
fn captured_none_neighbor_median_skips_gossip_like_python_cache_hit() {
    let mut case = fixture_cycle_case("dts_pid_undercut");
    case.evidence.captured_neighbor_median = Some(None);
    let mut clock = SemanticClock::new(case.now);

    run_fixture_case(&mut case, &mut clock);

    assert_eq!(
        case.evidence.gossip_calls.get(),
        0,
        "a captured effective None is a Python cache hit and must not trigger nested gossip"
    );
}

#[test]
fn high_fee_with_prior_forward_consumes_flow_ceiling_clock_before_thompson() {
    let mut case = fixture_cycle_case("dts_pid_undercut");
    case.info.fee_proportional_millionths = revops_fees::rails::ZERO_FLOW_FEE_THRESHOLD;
    case.evidence.last_forward.set(Some(case.now - 86_400));
    let mut clock = SemanticClock::new(case.now);

    run_fixture_case(&mut case, &mut clock);

    let flow_index = clock
        .labels
        .iter()
        .position(|label| label == "flow_ceiling.last_forward_age")
        .expect("high-fee channel with a prior forward must read the flow-ceiling clock");
    let thompson_index = clock
        .labels
        .iter()
        .position(|label| label.starts_with("thompson."))
        .expect("fixture must reach Thompson sampling");
    assert!(
        flow_index < thompson_index,
        "flow-ceiling clock must precede Thompson clocks: {:?}",
        clock.labels
    );
}

#[test]
fn target_above_supported_cap_consumes_upward_probe_clock() {
    let mut case = fixture_cycle_case("dts_pid_undercut");
    case.info.spendable_msat = 0;
    for volume in case.evidence.volume_map.borrow_mut().values_mut() {
        *volume = 0;
    }
    let thompson = &mut case
        .state
        .fee_states
        .get_mut(&case.channel_id)
        .expect("fee state")
        .thompson;
    for observation in &mut thompson.observations {
        observation.fee = 50.0;
    }
    let mut clock = SemanticClock::new(case.now);

    run_fixture_case(&mut case, &mut clock);

    assert_eq!(
        clock
            .labels
            .iter()
            .filter(|label| label.as_str() == "thompson.upward_probe_cap")
            .count(),
        1
    );
}

#[test]
fn ordinary_dts_success_does_not_consume_fee_state_sync() {
    let mut case = fixture_cycle_case("dts_pid_undercut");
    let mut clock = SemanticClock::new(case.now);
    let result = run_fixture_case(&mut case, &mut clock);
    assert!(matches!(result.outcome, ChannelOutcome::Adjusted(_)));
    assert_eq!(
        clock.labels,
        [
            "cycle.channel.evaluate",
            "channel.adjust",
            "rebalance_cost_floor.cutoff",
            "rebalance_cost_history.cutoff",
            "thompson.posterior.update",
            "thompson.posterior.recompute",
            "thompson.contextual.update",
            "thompson.last_sample_time",
            "pid.calculate",
            "thompson.supported_fee_ceiling",
            "thompson.earning_region",
            "thompson.meaningful_rate",
            "fee.apply"
        ]
    );
    assert_optimizer_timestamps(&case, case.now);
}

#[test]
fn congestion_success_does_not_consume_fee_state_sync() {
    let mut case = fixture_cycle_case("congestion_episode");
    let mut clock = SemanticClock::new(case.now);
    let result = run_fixture_case(&mut case, &mut clock);
    assert!(matches!(result.outcome, ChannelOutcome::Adjusted(_)));
    assert_eq!(
        clock.labels,
        [
            "cycle.channel.evaluate",
            "channel.adjust",
            "rebalance_cost_floor.cutoff",
            "rebalance_cost_history.cutoff",
            "thompson.posterior.update",
            "thompson.posterior.recompute",
            "fee.apply"
        ]
    );
    assert_optimizer_timestamps(&case, case.now);
}

#[test]
fn exploration_consumes_inner_state_sync_but_outer_state_keeps_evaluation_time() {
    let mut case = fixture_cycle_case("exploration_low_fee");
    let mut clock = SemanticClock::new(case.now);
    let result = run_fixture_case(&mut case, &mut clock);
    assert!(matches!(result.outcome, ChannelOutcome::Adjusted(_)));
    assert_eq!(
        clock.labels,
        [
            "cycle.channel.evaluate",
            "channel.adjust",
            "rebalance_cost_floor.cutoff",
            "rebalance_cost_history.cutoff",
            "thompson.posterior.update",
            "thompson.posterior.recompute",
            "fee.apply",
            "fee.state_sync"
        ]
    );
    assert_optimizer_timestamps(&case, case.now);
}

#[test]
fn gossip_consumes_inner_state_sync_but_outer_state_keeps_evaluation_time() {
    let mut case = fixture_cycle_case("gossip_refresh_nudge");
    let mut clock = SemanticClock::new(case.now);
    let result = run_fixture_case(&mut case, &mut clock);
    assert!(matches!(result.outcome, ChannelOutcome::Adjusted(_)));
    assert_eq!(
        clock.labels,
        [
            "cycle.channel.evaluate",
            "channel.adjust",
            "rebalance_cost_floor.cutoff",
            "rebalance_cost_history.cutoff",
            "thompson.posterior.update",
            "thompson.posterior.recompute",
            "thompson.contextual.update",
            "thompson.last_sample_time",
            "pid.calculate",
            "thompson.supported_fee_ceiling",
            "thompson.earning_region",
            "thompson.meaningful_rate",
            "fee.apply",
            "fee.state_sync"
        ]
    );
    assert_optimizer_timestamps(&case, case.now);
}

#[derive(Default)]
struct RecordingAuthorizer {
    requests: RefCell<Vec<FeeAuthorizationRequest>>,
}

impl FeeAuthorizer for RecordingAuthorizer {
    fn authorize(
        &self,
        request: &FeeAuthorizationRequest,
    ) -> Result<FeeAuthorizationResult, DecisionInputError> {
        self.requests.borrow_mut().push(request.clone());
        Ok(FeeAuthorizationResult {
            authorized: true,
            reason_code: String::new(),
            trace: None,
        })
    }
}

fn synthetic_info(cid: &str, peer: &str, fee: i64) -> ChannelInfo {
    ChannelInfo {
        channel_id: cid.to_string(),
        short_channel_id: cid.to_string(),
        full_channel_id: cid.to_string(),
        peer_id: peer.to_string(),
        capacity_sats: 2_000_000,
        spendable_msat: 1_000_000_000,
        receivable_msat: 1_000_000_000,
        fee_base_msat: 0,
        fee_proportional_millionths: fee,
        htlc_minimum_msat: 0,
        htlc_min_msat: 0,
        htlc_maximum_msat: 0,
        htlc_max_msat: 0,
        opener: "local".to_string(),
        has_htlc_data: false,
        max_accepted_htlcs: 483,
        our_htlcs_in_flight: 0,
    }
}

fn synthetic_row(cid: &str, peer: &str) -> ChannelStateRow {
    ChannelStateRow {
        channel_id: cid.to_string(),
        peer_id: peer.to_string(),
        state: "balanced".to_string(),
        updated_at: Some(1_752_400_000 - 600),
        kalman_flow_ratio: Some(0.2),
        kalman_velocity: Some(0.01),
    }
}

#[test]
fn three_channel_synthetic_cycle_end_to_end() {
    let now = 1_752_400_000i64;
    let peer_a = "03".to_string() + &"01".repeat(32);
    let peer_b = "03".to_string() + &"02".repeat(32);
    let peer_c = "03".to_string() + &"03".repeat(32);

    let mut infos = BTreeMap::new();
    infos.insert(
        "100x1x0".to_string(),
        synthetic_info("100x1x0", &peer_a, 200),
    );
    infos.insert(
        "200x1x0".to_string(),
        synthetic_info("200x1x0", &peer_b, 300),
    );
    infos.insert(
        "300x1x0".to_string(),
        synthetic_info("300x1x0", &peer_c, 400),
    );
    let evidence = SyntheticEvidence {
        rows: vec![
            synthetic_row("100x1x0", &peer_a),
            synthetic_row("200x1x0", &peer_b),
            synthetic_row("300x1x0", &peer_c),
        ],
        infos,
        volumes: BTreeMap::from([("100x1x0".to_string(), 2_000_000)]),
        forwards: BTreeMap::from([("100x1x0".to_string(), 8)]),
        passive_peer: peer_c.clone(),
        node_channels: Vec::new(),
    };

    let mut state = ControllerState::new();
    // Channel A: active DTS channel with an open observation window.
    // Functional-update syntax is unavailable now that the unified
    // state_store types carry private shared scalars; plain field writes.
    let mut cyc_a = ChannelCycleState::default();
    cyc_a.last_update = now - 7200;
    cyc_a.last_fee_ppm = 200;
    cyc_a.last_broadcast_fee_ppm = 200;
    cyc_a.last_revenue_rate = 5.0;
    state.cycle_states.insert("100x1x0".to_string(), cyc_a);
    let mut fee_a = ChannelFeeState::default();
    fee_a.last_update = now - 7200;
    fee_a.last_fee_ppm = 200;
    fee_a.last_broadcast_fee_ppm = 200;
    fee_a.last_revenue_rate = 5.0;
    state.fee_states.insert("100x1x0".to_string(), fee_a);
    // Channel B: sleeping with the timer still running.
    let mut cyc_b = ChannelCycleState::default();
    cyc_b.is_sleeping = true;
    cyc_b.sleep_until = now + 7200;
    cyc_b.last_update = now - 3600;
    cyc_b.last_fee_ppm = 300;
    cyc_b.last_broadcast_fee_ppm = 300;
    state.cycle_states.insert("200x1x0".to_string(), cyc_b);
    let mut fee_b = ChannelFeeState::default();
    fee_b.is_sleeping = true;
    fee_b.sleep_until = now + 7200;
    fee_b.last_update = now - 3600;
    fee_b.last_fee_ppm = 300;
    fee_b.last_broadcast_fee_ppm = 300;
    fee_b.last_revenue_rate = 0.0;
    state.fee_states.insert("200x1x0".to_string(), fee_b);

    let dir = tempfile::tempdir().expect("tmpdir");
    let journal = Journal::open_dir(dir.path()).expect("journal");
    let sink = CountingSink::default();
    let mut cfg = base_cfg();
    cfg.econ_governor_fees_enabled = true;
    let authorizer = RecordingAuthorizer::default();
    let mut rng = PyRandom::seed_from_u64(4242);
    let mut clock = RecordingClock {
        now,
        labels: Vec::new(),
    };
    let decisions = {
        let mut deps = CycleDeps {
            evidence: &evidence,
            cfg: &cfg,
            rng: &mut rng,
            clock: &mut clock,
            authorizer: Some(&authorizer),
            executor: &PURE_EXECUTOR,
            journal: Some(&journal),
            state_sink: Some(&sink),
            // No gossip in `SyntheticEvidence` (`gossip_channels` returns
            // empty) -- the neighbor-market path is always `None`
            // regardless of this value, so the baked-default constant is
            // fine here.
            min_competitors: revops_fees::market::MIN_COMPETITORS,
        };
        run_fee_cycle(&mut state, &mut deps).expect("scripted decision inputs")
    };

    assert_eq!(
        clock.labels,
        [
            "cycle.started_at",
            "cycle.channel.evaluate",
            "channel.adjust",
            "rebalance_cost_floor.cutoff",
            "rebalance_cost_history.cutoff",
            "thompson.posterior.update",
            "thompson.posterior.recompute",
            "thompson.contextual.update",
            "thompson.last_sample_time",
            "pid.calculate",
            "thompson.supported_fee_ceiling",
            "thompson.upward_probe_cap",
            "thompson.earning_region",
            "thompson.meaningful_rate",
            "governor.authorize",
            "fee.apply",
            "cycle.channel.evaluate",
            "channel.adjust",
        ],
        "clock transcript must follow Python semantic branch order"
    );
    assert_eq!(decisions.len(), 3, "one journal decision per channel");
    let requests = authorizer.requests.borrow();
    assert_eq!(requests.len(), 1, "only the adjusted channel is authorized");
    assert_eq!(requests[0].channel_id, "100x1x0");

    // Serialize-once assertion: exactly ONE flush batch, 3 rows.
    assert_eq!(
        sink.calls.get(),
        1,
        "state store must receive ONE flush batch"
    );
    assert_eq!(sink.rows.get(), 3);

    // Journal lines parse and stay consistent with the gossip-gate
    // disposition.
    let raw = std::fs::read_to_string(journal.path()).expect("journal file");
    let lines: Vec<&str> = raw.lines().collect();
    assert_eq!(lines.len(), 3);
    for line in lines {
        let v = parse(line).expect("journal line parses");
        let would = matches!(v.get("would_broadcast"), Some(OValue::Bool(true)));
        let disposition = v
            .get("trace")
            .and_then(|t| t.get("disposition"))
            .and_then(|d| d.as_str())
            .unwrap_or("");
        let broadcast_dispositions = ["broadcast", "policy_static", "gossip_refresh"];
        assert_eq!(
            would,
            broadcast_dispositions.contains(&disposition),
            "would_broadcast inconsistent with disposition {disposition:?}: {line}"
        );
        assert!(v.get("cycle_id").is_some() && v.get("at").is_some());
    }

    // The passive channel skipped; the sleeping channel held; decision
    // summary reflects the LAST adjustment or dominant skip.
    let codes: Vec<String> = decisions.iter().map(|d| d.reason_code.clone()).collect();
    assert!(codes.contains(&"policy_passive".to_string()), "{codes:?}");
    assert!(codes.contains(&"skip_sleeping".to_string()), "{codes:?}");

    // FeeDecision journal round-trip: to_ovalue parses back identically.
    let d0: &FeeDecision = &decisions[0];
    let reparsed = parse(&d0.to_jsonl_line()).expect("reparse");
    assert_eq!(dumps_python(&reparsed), dumps_python(&d0.to_ovalue()));
}

#[test]
fn dts_summary_and_wake_helpers() {
    let mut state = ControllerState::new();
    assert!(state.dts_summary("nope").is_none());
    let mut cyc_c1 = ChannelCycleState::default();
    cyc_c1.is_sleeping = true;
    cyc_c1.sleep_until = 99;
    cyc_c1.last_broadcast_fee_ppm = 123;
    state.cycle_states.insert("c1".to_string(), cyc_c1);
    let summary = state.dts_summary("c1").expect("summary");
    assert_eq!(
        summary.get("broadcast_fee_ppm").and_then(|v| v.as_i64()),
        Some(123)
    );

    let (_, profile) = revops_fees::profiles::fee_profile("active");
    let woken = cycle::wake_all_sleeping_channels(&mut state, profile, 1_752_400_000);
    assert_eq!(woken, 1);
    assert!(!state.cycle_states["c1"].is_sleeping);
}

// ---------------------------------------------------------------------------
// Phase 4b Task 8b: skip-gate cross-cycle memory (Design Note 1 addendum).
//
// The bug (fee-window diagnosis H1): a triggered Rust cycle rehydrates AFTER
// Python's end-of-cycle flush, so the freshly-loaded `cycle.last_update` is
// the timestamp Python JUST WROTE for the cycle Rust is reproducing -- NOT
// the pre-decision timestamp Python's skip gate was actually conditioned on.
// With a fresh (seconds-old) `last_update`, `pre_hours_elapsed` is ~0 and the
// waiting-time gate skips EVERY channel, EVERY cycle. The fix feeds the gate
// the value Rust's OWN previous cycle hydrated (`skip_gate_prev`), which is
// Python's real pre-decision epoch.
// ---------------------------------------------------------------------------

/// Drive ONE channel through `run_fee_cycle` with a fresh (`now`)
/// `last_update` in `cycle_states` -- i.e. exactly what `rehydrate` loads
/// after Python's current-cycle flush -- and the given `skip_gate_prev`
/// cache seeded. Returns the channel's `reason_code`.
fn run_skip_gate_channel(now: i64, cached_prev: Option<SkipGateEpoch>) -> String {
    let cid = "700x1x0";
    let peer = "03".to_string() + &"07".repeat(32);
    let mut infos = BTreeMap::new();
    infos.insert(cid.to_string(), synthetic_info(cid, &peer, 200));
    let evidence = SyntheticEvidence {
        rows: vec![synthetic_row(cid, &peer)],
        infos,
        // No forwards: isolate the TIME gate (forwards_ok stays false, so
        // the outcome turns purely on pre_hours_elapsed).
        volumes: BTreeMap::new(),
        forwards: BTreeMap::new(),
        passive_peer: String::new(),
        node_channels: Vec::new(),
    };

    let mut state = ControllerState::new();
    // Python's just-flushed row: last_update == now (the WRONG epoch for the
    // gate). This is what `rehydrate` loads into `cycle_states`.
    let mut cyc = ChannelCycleState::default();
    cyc.last_update = now;
    cyc.last_fee_ppm = 200;
    cyc.last_broadcast_fee_ppm = 200;
    cyc.last_revenue_rate = 5.0;
    state.cycle_states.insert(cid.to_string(), cyc);
    let mut fee = ChannelFeeState::default();
    fee.last_update = now;
    fee.last_fee_ppm = 200;
    fee.last_broadcast_fee_ppm = 200;
    fee.last_revenue_rate = 5.0;
    state.fee_states.insert(cid.to_string(), fee);

    if let Some(epoch) = cached_prev {
        state.skip_gate_prev.insert(cid.to_string(), epoch);
    }

    let cfg = base_cfg();
    let mut rng = PyRandom::seed_from_u64(4242);
    let mut clock = FixedDecisionClock::new(now);
    let decisions = {
        let mut deps = CycleDeps {
            evidence: &evidence,
            cfg: &cfg,
            rng: &mut rng,
            clock: &mut clock,
            authorizer: None,
            executor: &PURE_EXECUTOR,
            journal: None,
            state_sink: None,
            min_competitors: revops_fees::market::MIN_COMPETITORS,
        };
        run_fee_cycle(&mut state, &mut deps).expect("fixed decision inputs")
    };
    assert_eq!(decisions.len(), 1, "one decision for the one channel");
    decisions[0].reason_code.clone()
}

/// REPRODUCE: with the pre-decision epoch as fresh as the flush (what the
/// old rehydrate-after-flush wiring effectively fed the gate), the channel
/// is skipped for waiting_time even though it is due to act -- the all-skip
/// bug, watched here.
#[test]
fn skip_gate_all_skip_when_pre_decision_epoch_is_fresh() {
    let now = 1_752_400_000i64;
    let fresh = SkipGateEpoch {
        last_update: now, // seconds old == Python's just-written flush
        is_sleeping: false,
    };
    let reason = run_skip_gate_channel(now, Some(fresh));
    assert_eq!(
        reason, "skip_waiting_time",
        "a fresh (post-flush) pre-decision epoch wrongly skips: pre_hours_elapsed ~0"
    );
}

/// FIX: with the TRUE pre-decision epoch (the value Rust's own previous
/// cycle hydrated, hours old), the gate no longer skips for waiting_time --
/// the channel is evaluated, exactly as Python evaluated it.
#[test]
fn skip_gate_evaluates_when_cached_pre_decision_epoch_is_old() {
    let now = 1_752_400_000i64;
    let old = SkipGateEpoch {
        last_update: now - 7200, // 2h ago: well past min_observation_hours
        is_sleeping: false,
    };
    let reason = run_skip_gate_channel(now, Some(old));
    assert_ne!(
        reason, "skip_waiting_time",
        "the cached pre-decision epoch is hours old -> time_ok -> gate must NOT skip waiting_time"
    );
}

/// BOOTSTRAP: on a triggered cycle with NO cached prior (first cycle after
/// (re)start, or a channel's first appearance), the skip gate is
/// non-comparable -- flagged `skip_gate_comparable: false` in the trace so
/// the diff harness excludes it instead of counting a spurious miss.
#[test]
fn skip_gate_bootstrap_marks_channel_non_comparable() {
    let now = 1_752_400_000i64;
    let cid = "700x1x0";
    let peer = "03".to_string() + &"07".repeat(32);
    let mut infos = BTreeMap::new();
    infos.insert(cid.to_string(), synthetic_info(cid, &peer, 200));
    let evidence = SyntheticEvidence {
        rows: vec![synthetic_row(cid, &peer)],
        infos,
        volumes: BTreeMap::new(),
        forwards: BTreeMap::new(),
        passive_peer: String::new(),
        node_channels: Vec::new(),
    };

    let mut state = ControllerState::new();
    let mut cyc = ChannelCycleState::default();
    cyc.last_update = now;
    cyc.last_fee_ppm = 200;
    cyc.last_broadcast_fee_ppm = 200;
    state.cycle_states.insert(cid.to_string(), cyc);
    let mut fee = ChannelFeeState::default();
    fee.last_update = now;
    fee.last_fee_ppm = 200;
    fee.last_broadcast_fee_ppm = 200;
    state.fee_states.insert(cid.to_string(), fee);
    // NO skip_gate_prev entry -> bootstrap.

    let cfg = base_cfg();
    let mut rng = PyRandom::seed_from_u64(4242);
    let mut clock = FixedDecisionClock::new(now);
    let decisions = {
        let mut deps = CycleDeps {
            evidence: &evidence,
            cfg: &cfg,
            rng: &mut rng,
            clock: &mut clock,
            authorizer: None,
            executor: &PURE_EXECUTOR,
            journal: None,
            state_sink: None,
            min_competitors: revops_fees::market::MIN_COMPETITORS,
        };
        run_fee_cycle(&mut state, &mut deps).expect("fixed decision inputs")
    };
    assert_eq!(decisions.len(), 1);
    let comparable = decisions[0]
        .trace
        .get("skip_gate_comparable")
        .and_then(|v| match v {
            OValue::Bool(b) => Some(*b),
            _ => None,
        });
    assert_eq!(
        comparable,
        Some(false),
        "a bootstrap channel (no cached prior) must be flagged non-comparable in its trace"
    );
}

macro_rules! wrap_test_evidence {
    ($ty:ty) => {
        impl FeeEvidence for $ty {
            fn our_node_id(&self) -> Result<String, DecisionInputError> {
                Ok(Self::our_node_id(self))
            }
            fn channel_states(&self) -> Result<Vec<ChannelStateRow>, DecisionInputError> {
                Ok(Self::channel_states(self))
            }
            fn channels_info(&self) -> Result<BTreeMap<String, ChannelInfo>, DecisionInputError> {
                Ok(Self::channels_info(self))
            }
            fn chain_costs(&self) -> Result<Option<ChainCosts>, DecisionInputError> {
                Ok(Self::chain_costs(self))
            }
            fn volume_since(
                &self,
                channel_id: &str,
                since: i64,
            ) -> Result<i64, DecisionInputError> {
                Ok(Self::volume_since(self, channel_id, since))
            }
            fn forward_count_since(
                &self,
                channel_id: &str,
                since: i64,
            ) -> Result<i64, DecisionInputError> {
                Ok(Self::forward_count_since(self, channel_id, since))
            }
            fn exploration_flag(&self, channel_id: &str) -> Result<bool, DecisionInputError> {
                Ok(Self::exploration_flag(self, channel_id))
            }
            fn clear_exploration_flag(&self, channel_id: &str) -> Result<(), DecisionInputError> {
                Self::clear_exploration_flag(self, channel_id);
                Ok(())
            }
            fn gossip_channels(&self, peer_id: &str) -> Result<Vec<GossipRow>, DecisionInputError> {
                Ok(Self::gossip_channels(self, peer_id))
            }
            fn captured_neighbor_fee_median(
                &self,
                peer_id: &str,
            ) -> Result<Option<Option<i64>>, DecisionInputError> {
                Ok(Self::captured_neighbor_fee_median(self, peer_id))
            }
            fn peer_latency(
                &self,
                peer_id: &str,
            ) -> Result<Option<PeerLatency>, DecisionInputError> {
                Ok(Self::peer_latency(self, peer_id))
            }
            fn channel_cost_history(
                &self,
                channel_id: &str,
                since: i64,
            ) -> Result<Vec<RebalanceCostSample>, DecisionInputError> {
                Ok(Self::channel_cost_history(self, channel_id, since))
            }
            fn peer_fee_history(
                &self,
                peer_id: &str,
            ) -> Result<Option<PeerFeeHistory>, DecisionInputError> {
                Ok(Self::peer_fee_history(self, peer_id))
            }
            fn last_forward_time(
                &self,
                channel_id: &str,
            ) -> Result<Option<i64>, DecisionInputError> {
                Ok(Self::last_forward_time(self, channel_id))
            }
            fn flow_window(
                &self,
                channel_id: &str,
            ) -> Result<Option<FlowWindow>, DecisionInputError> {
                Ok(Self::flow_window(self, channel_id))
            }
            fn policy(&self, peer_id: &str) -> Result<Option<PeerPolicy>, DecisionInputError> {
                Ok(Self::policy(self, peer_id))
            }
            fn marginal_roi_percent(
                &self,
                channel_id: &str,
            ) -> Result<Option<f64>, DecisionInputError> {
                Ok(Self::marginal_roi_percent(self, channel_id))
            }
            fn temporary_overlay_active(
                &self,
                channel_id: &str,
            ) -> Result<bool, DecisionInputError> {
                Ok(Self::temporary_overlay_active(self, channel_id))
            }
            fn node_channels(&self) -> Result<Vec<NodeChannel>, DecisionInputError> {
                Ok(Self::node_channels(self))
            }
        }
    };
}

wrap_test_evidence!(FixtureEvidence);
wrap_test_evidence!(SyntheticEvidence);

/// H2 (2026-07-22 audit): the node-drain-bias effective cap must scale by
/// the SEPARATE `node_drain_bias_max` knob (py 184: `_cfg_float(cfg_like,
/// "node_drain_bias_max", 0.0)`, Config default 0.3), not by the static
/// `drain_fee_discount_max`. With the static cap at its 0.0 default and a
/// fully source-heavy node (receivable ratio 0.0 -> pressure 1.0), Python
/// computes max(0.0, 0.3 * 1.0) = 0.3; wiring the static knob into the
/// bias slot yields 0.0 forever — the feature's designed use-case dead.
#[test]
fn node_drain_bias_cap_scales_by_bias_knob_not_static_max() {
    let peer = "03".to_string() + &"01".repeat(32);
    let evidence = SyntheticEvidence {
        // One channel row so the cycle survives the empty-states hold
        // gate and reaches the node-drain aggregate.
        rows: vec![synthetic_row("100x1x0", &peer)],
        infos: BTreeMap::from([("100x1x0".to_string(), synthetic_info("100x1x0", &peer, 300))]),
        volumes: BTreeMap::new(),
        forwards: BTreeMap::new(),
        passive_peer: String::new(),
        // One all-local channel: receivable ratio 0.0, below the 0.2
        // floor -> node_drain_pressure = 1.0.
        node_channels: vec![NodeChannel {
            state: "CHANNELD_NORMAL".to_string(),
            to_us_msat: 5_000_000_000,
            total_msat: 5_000_000_000,
        }],
    };
    let mut cfg = base_cfg();
    cfg.node_drain_bias_enabled = true;
    assert_eq!(
        cfg.drain_fee_discount_max, 0.0,
        "precondition: static cap at its config.py:529 default"
    );

    let mut state = ControllerState::default();
    let mut rng = PyRandom::seed_from_u64(4242);
    let mut clock = FixedDecisionClock::new(1_752_400_000);
    {
        let mut deps = CycleDeps {
            evidence: &evidence,
            cfg: &cfg,
            rng: &mut rng,
            clock: &mut clock,
            authorizer: None,
            executor: &PURE_EXECUTOR,
            journal: None,
            state_sink: None,
            min_competitors: revops_fees::market::MIN_COMPETITORS,
        };
        run_fee_cycle(&mut state, &mut deps).expect("fixed decision inputs");
    }

    assert_eq!(state.last_node_drain_pressure, Some(1.0));
    assert_eq!(
        state.last_effective_drain_discount_max,
        Some(0.3),
        "effective cap must be node_drain_bias_max (0.3) * pressure (1.0)"
    );
}
