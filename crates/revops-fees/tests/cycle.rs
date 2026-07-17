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
    ChannelOutcome, ChannelStateRow, ControllerState, CycleDeps, FeeCfgSnapshot, FeeEvidence,
    GossipRow, PeerFeeHistory, StateSink,
};
use revops_fees::execution::{
    decide_set_channel_fee, fail_closed, governed_authorize_fee_broadcast, GovernedDeps,
    SetFeeRequest,
};
use revops_fees::floors::{ChainCosts, FlowWindow, PeerLatency, RebalanceCostSample};
use revops_fees::journal::{FeeDecision, Journal};
use revops_fees::pid::pid_from_dict;
use revops_fees::pid::pid_to_dict;
use revops_fees::pyjson::{dumps_python, parse, OValue};
use revops_fees::pyrand::PyRandom;
use revops_fees::thompson::serde::gts_from_dict;

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
    latency: Option<PeerLatency>,
    cost_history: Vec<RebalanceCostSample>,
    peer_fee_history: Option<PeerFeeHistory>,
    last_forward: Cell<Option<i64>>,
    policy: Option<PeerPolicy>,
    marginal_roi: Option<f64>,
}

impl FixtureEvidence {
    fn lookup(map: &BTreeMap<String, i64>, since: i64) -> i64 {
        if let Some(v) = map.get(&since.to_string()) {
            return *v;
        }
        map.get("default").copied().unwrap_or(0)
    }
}

impl FeeEvidence for FixtureEvidence {
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
        self.gossip.clone()
    }
    fn peer_latency(&self, _peer_id: &str) -> Option<PeerLatency> {
        self.latency
    }
    fn channel_cost_history(&self, _channel_id: &str, _since: i64) -> Vec<RebalanceCostSample> {
        self.cost_history.clone()
    }
    fn peer_fee_history(&self, _peer_id: &str) -> Option<PeerFeeHistory> {
        self.peer_fee_history.clone()
    }
    fn last_forward_time(&self, _channel_id: &str) -> Option<i64> {
        self.last_forward.get()
    }
    fn flow_window(&self, _channel_id: &str) -> Option<FlowWindow> {
        None
    }
    fn policy(&self, _peer_id: &str) -> Option<PeerPolicy> {
        self.policy.clone()
    }
    fn marginal_roi_percent(&self, _channel_id: &str) -> Option<f64> {
        self.marginal_roi
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
        receivable_ratio_target: 0.30,
        receivable_ratio_floor: 0.20,
        econ_governor_fees_enabled: getb(v, "econ_governor_fees_enabled"),
        authority_level: None,
    }
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
        peer_id: gets(v, "peer_id"),
        capacity_sats: geti(v, "capacity"),
        spendable_msat: geti(v, "spendable_msat"),
        receivable_msat: geti(v, "receivable_msat"),
        fee_base_msat: geti(v, "fee_base_msat"),
        fee_proportional_millionths: geti(v, "fee_proportional_millionths"),
        htlc_minimum_msat: geti(v, "htlc_minimum_msat"),
        htlc_maximum_msat: geti(v, "htlc_maximum_msat"),
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
                let mut deps = CycleDeps {
                    evidence: &evidence,
                    cfg: &cfg,
                    rng: &mut rng,
                    now,
                    governed: None,
                    journal: None,
                    state_sink: None,
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
}

impl FeeEvidence for SyntheticEvidence {
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

fn synthetic_info(cid: &str, peer: &str, fee: i64) -> ChannelInfo {
    ChannelInfo {
        channel_id: cid.to_string(),
        short_channel_id: cid.to_string(),
        peer_id: peer.to_string(),
        capacity_sats: 2_000_000,
        spendable_msat: 1_000_000_000,
        receivable_msat: 1_000_000_000,
        fee_base_msat: 0,
        fee_proportional_millionths: fee,
        htlc_minimum_msat: 0,
        htlc_maximum_msat: 0,
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
    let cfg = base_cfg();
    let mut rng = PyRandom::seed_from_u64(4242);
    let decisions = {
        let mut deps = CycleDeps {
            evidence: &evidence,
            cfg: &cfg,
            rng: &mut rng,
            now,
            governed: None,
            journal: Some(&journal),
            state_sink: Some(&sink),
        };
        run_fee_cycle(&mut state, &mut deps)
    };

    assert_eq!(decisions.len(), 3, "one journal decision per channel");

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
