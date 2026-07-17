//! Dry-run fee cycle orchestrator: the decision core that strings every
//! merged fee module into one cycle, journaling decisions WITHOUT ever
//! broadcasting a `setchannel`.
//!
//! Ports (all line anchors into `modules/fee_controller.py`, branch
//! `port` == `main`):
//! - `_adjust_channel_fee` (5504-7355) — the frozen ADR-001 stage order:
//!   profile/congestion/sleep (stage 4 cooldown, 5617-5710) → observation
//!   ingest (update → discount, 5965-6373) → floors/ceiling (stage 1
//!   rails, 5844-5955) → priority chain congestion > exploration >
//!   DTS+PID sample (6373-6465) → bound/blend → zero-flow guard →
//!   `apply_damped_fee_target` (stage 2 rate_limit, 6863) → htlcmax valve
//!   (6907) → alpha guard + gossip gate (stage 3 deadband, 7019-7161) →
//!   decision emit (7203: dry-run `execution::decide_set_channel_fee`
//!   instead of the RPC).
//! - `_adjust_all_fees_inner`/`_adjust_all_fees_channel_loop` (4532-4884),
//!   `_classify_no_adjustment_skip_reason` (4885-4921),
//!   `_should_force_gossip_refresh`/`_create_gossip_refresh_adjustment`
//!   (4923-5082), `wake_all_sleeping_channels`/`_maybe_wake_for_vegas_spike`
//!   (4295-4411), `_handle_policy_change` (7356-7400), `get_dts_summary`
//!   (5087-5122), `_set_last_decision_summary` (3031-3048).
//!
//! ## Threading translation (single-owner path)
//!
//! Python guards `_cycle_states`/`_channel_fee_states` with an RLock held
//! across the whole channel loop. This port is a SINGLE-OWNER path: one
//! `&mut ControllerState`, no locks — the plugin binary later wraps it in
//! an actor task per the spec's threading translation rule. Never share
//! `ControllerState` across threads.
//!
//! ## Type seam (T9 coordination)
//!
//! [`ChannelCycleState`]/[`ChannelFeeState`] are defined HERE; Task 9's
//! `state_store.rs` (v2_state_json lossless round-trip) loads/saves the
//! same shapes. If T9 lands its own copies, unify at integration merge on
//! these definitions (T2/T3 `Observation` precedent).

use std::collections::BTreeMap;

use crate::admission::{self, HtlcmaxCfg};
use crate::drain;
use crate::execution::{self, decide_set_channel_fee, GovernedDeps, GovernedTrace, SetFeeRequest};
use crate::floors::{
    self, ChainCosts, FlowStateRow, FlowWindow, LiveHtlc, MinFeeCfg, PeerLatency,
    RebalanceCostSample, SATURATED_OUTBOUND_RATIO,
};
use crate::journal::{FeeDecision, Journal};
use crate::market::{self, GossipChannel};
use crate::pid;
use crate::profiles::{fee_profile, FeeProfileSettings};
use crate::pyjson::OValue;
use crate::pyrand::PyRandom;
use crate::rails;
use crate::reason::FeeReasonCode;
use crate::thompson::recompute::MIN_OBSERVATIONS;
use crate::thompson::{dynamics, recompute, sampling};
use crate::vegas::{self, VegasReflexState};
use revops_analytics::policy::{FeeStrategy, PeerPolicy};

// ---------------------------------------------------------------------------
// Class constants (py 2504-2688, 2901-2918)
// ---------------------------------------------------------------------------

/// `VOLATILITY_THRESHOLD` (py 2504).
pub const VOLATILITY_THRESHOLD: f64 = 0.50;
/// `STABILITY_THRESHOLD` (py 2508).
pub const STABILITY_THRESHOLD: f64 = 0.01;
/// `WAKE_UP_THRESHOLD` (py 2509).
pub const WAKE_UP_THRESHOLD: f64 = 0.20;
/// `SLEEP_CYCLES` (py 2510).
pub const SLEEP_CYCLES: i64 = 2;
/// `STABLE_CYCLES_REQUIRED` (py 2511).
pub const STABLE_CYCLES_REQUIRED: i64 = 3;
/// `CONGESTION_FEE_MAX_MULTIPLIER` (py 2655).
pub const CONGESTION_FEE_MAX_MULTIPLIER: f64 = 2.0;
/// `CONGESTION_FEE_MIN_HEADROOM_PPM` (py 2656).
pub const CONGESTION_FEE_MIN_HEADROOM_PPM: i64 = 250;
/// `CONGESTION_FLOOR_MULTIPLIER` (py 2657).
pub const CONGESTION_FLOOR_MULTIPLIER: f64 = 1.5;
/// `CONGESTION_EPISODE_MAX_MULTIPLIER` (py 2664).
pub const CONGESTION_EPISODE_MAX_MULTIPLIER: f64 = 4.0;
/// `CONGESTION_EXIT_QUIET_CYCLES` (py 2668).
pub const CONGESTION_EXIT_QUIET_CYCLES: i64 = 2;
/// `VEGAS_WAKE_INTENSITY_THRESHOLD` (py 2687).
pub const VEGAS_WAKE_INTENSITY_THRESHOLD: f64 = 0.5;
/// `VEGAS_WAKE_REARM_INTENSITY` (py 2688).
pub const VEGAS_WAKE_REARM_INTENSITY: f64 = 0.3;
/// `UNDERCUT_MIN_OUTBOUND_RATIO` (py 2754).
pub const UNDERCUT_MIN_OUTBOUND_RATIO: f64 = 0.35;
/// `ENABLE_GOSSIP_REFRESH` (py 2901).
pub const ENABLE_GOSSIP_REFRESH: bool = true;
/// `GOSSIP_REFRESH_MIN_BROADCAST_AGE_HOURS` (py 2904).
pub const GOSSIP_REFRESH_MIN_BROADCAST_AGE_HOURS: f64 = 24.0;
/// `GOSSIP_REFRESH_MIN_IDLE_HOURS` (py 2907).
pub const GOSSIP_REFRESH_MIN_IDLE_HOURS: f64 = 24.0;
/// `GOSSIP_REFRESH_COOLDOWN_HOURS` (py 2910).
pub const GOSSIP_REFRESH_COOLDOWN_HOURS: f64 = 24.0;
/// `GOSSIP_REFRESH_NUDGE_PPM` (py 2913).
pub const GOSSIP_REFRESH_NUDGE_PPM: i64 = 1;
/// `REBALANCE_FLOOR_WINDOW_DAYS` (shared with floors.rs; py class const).
pub const REBALANCE_FLOOR_WINDOW_DAYS: i64 = floors::REBALANCE_FLOOR_WINDOW_DAYS;

// ---------------------------------------------------------------------------
// Config snapshot
// ---------------------------------------------------------------------------

/// The `ConfigSnapshot` slice the fee cycle reads (py `cfg.*` accesses in
/// `_adjust_all_fees*` / `_adjust_channel_fee` / `set_channel_fee`).
#[derive(Debug, Clone, PartialEq)]
pub struct FeeCfgSnapshot {
    pub min_fee_ppm: i64,
    pub max_fee_ppm: i64,
    /// `min_fee_ppm_saturated` (E-2 class-aware floor; 0 disables).
    pub min_fee_ppm_saturated: i64,
    pub fee_interval: i64,
    pub flow_interval: i64,
    pub htlc_congestion_threshold: f64,
    /// `market_fee_mode`: "undercut" | "match" | "premium" |
    /// "competition_aware" (py default "undercut").
    pub market_fee_mode: String,
    pub drain_fee_discount_max: f64,
    pub high_liquidity_threshold: f64,
    pub fee_profile: String,
    pub base_fee_msat: i64,
    pub enable_vegas_reflex: bool,
    /// Raw config value — admission's enable check has narrow Python
    /// truthiness (see `admission::HtlcmaxCfg`).
    pub enable_dynamic_htlcmax: serde_json::Value,
    pub htlcmax_source_pct: f64,
    pub htlcmax_sink_pct: f64,
    pub htlcmax_balanced_pct: f64,
    pub paused: bool,
    pub node_drain_bias_enabled: bool,
    pub receivable_ratio_target: f64,
    pub receivable_ratio_floor: f64,
    /// `econ_governor_fees_enabled is True` (py 7521-7529).
    pub econ_governor_fees_enabled: bool,
    /// `Config.authority_level` (py `config.py:572`, `"capital"`) — the
    /// CONFIGURED level, distinct from `execution::GovernedDeps`'s
    /// `getattr(cfg, "authority_level", "capital")` fallback (that
    /// fallback only fires when the attribute is literally missing from
    /// an object that isn't a real `Config`; a real `Config` instance
    /// always carries `"capital"` unless a caller overrides it).
    /// `None` here is reserved for callers who explicitly construct a
    /// snapshot without reading `Config` (e.g. an unconfigured/erroring
    /// path) — `governor::authority_allows` fails CLOSED to `observe` on
    /// `None`/unknown strings, matching Python's fail-closed posture for
    /// a missing or garbled level. `Default` must mirror the CONFIG
    /// default (`Some("capital")`), not the fail-closed sentinel, or
    /// every governed broadcast built from an unconfigured snapshot would
    /// wrongly read as `AUTHORITY_LEVEL_BLOCKED`.
    pub authority_level: Option<String>,
}

impl Default for FeeCfgSnapshot {
    /// Python `Config` defaults for the fields this cycle reads. Every
    /// field below is cited against `modules/config.py` (branch `port` ==
    /// `main`) and covered by the `default_cfg_matches_python_config_*`
    /// drift-guard tests in `tests/cycle.rs`, which walk this struct
    /// against a cited table of the same line numbers — keep both in sync.
    ///
    /// Important finding fix (Phase 4 final review): four fields
    /// previously mirrored the Rust fixture generator's `_base_cfg` test
    /// helper instead of the real `Config` class defaults (`max_fee_ppm`
    /// 5000 vs 2000, `htlcmax_sink_pct` 0.9 vs 0.25, `htlcmax_balanced_pct`
    /// 0.75 vs 0.45, `min_fee_ppm` 0 vs 10) — corrected here.
    fn default() -> Self {
        FeeCfgSnapshot {
            // py config.py:596: `min_fee_ppm: int = 10`.
            min_fee_ppm: 10,
            // py config.py:605: `max_fee_ppm: int = 2000`.
            max_fee_ppm: 2000,
            // py config.py:604: `min_fee_ppm_saturated: int = 0`.
            min_fee_ppm_saturated: 0,
            // py config.py:506: `fee_interval: int = 1800`.
            fee_interval: 1800,
            // py config.py:505: `flow_interval: int = 3600`.
            flow_interval: 3600,
            // py config.py:738: `htlc_congestion_threshold: float = 0.8`.
            htlc_congestion_threshold: 0.8,
            // py config.py:630: `market_fee_mode: str = "undercut"`.
            market_fee_mode: "undercut".to_string(),
            // py config.py:529: `drain_fee_discount_max: float = 0.0`.
            drain_fee_discount_max: 0.0,
            // py config.py:653: `high_liquidity_threshold: float = 0.7`.
            high_liquidity_threshold: 0.7,
            // py config.py:631: `fee_profile: str = 'active'`.
            fee_profile: "active".to_string(),
            // py config.py:606: `base_fee_msat: int = 0`.
            base_fee_msat: 0,
            // py config.py:765: `enable_vegas_reflex: bool = True`.
            enable_vegas_reflex: true,
            // py config.py:542: `enable_dynamic_htlcmax: bool = False`.
            enable_dynamic_htlcmax: serde_json::Value::Bool(false),
            // py config.py:543: `htlcmax_source_pct: float = 0.50`.
            htlcmax_source_pct: 0.5,
            // py config.py:544: `htlcmax_sink_pct: float = 0.25`.
            htlcmax_sink_pct: 0.25,
            // py config.py:545: `htlcmax_balanced_pct: float = 0.45`.
            htlcmax_balanced_pct: 0.45,
            // py config.py:761: `paused: bool = False`.
            paused: false,
            // py config.py:535: `node_drain_bias_enabled: bool = False`.
            node_drain_bias_enabled: false,
            // py config.py:522: `receivable_ratio_target: float = 0.30`.
            receivable_ratio_target: 0.30,
            // py config.py:523: `receivable_ratio_floor: float = 0.20`.
            receivable_ratio_floor: 0.20,
            // py config.py:560: `econ_governor_fees_enabled: bool = False`.
            econ_governor_fees_enabled: false,
            // py `config.py:572`: `authority_level: str = "capital"`.
            authority_level: Some("capital".to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Evidence inputs
// ---------------------------------------------------------------------------

/// One channel's live info (py `_get_channels_info_live` row, 8412-8489).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelInfo {
    pub channel_id: String,
    pub short_channel_id: String,
    pub peer_id: String,
    /// py `capacity` (sats); `0` means "missing/falsy" — use sites apply
    /// Python's `or 2_000_000` default.
    pub capacity_sats: i64,
    pub spendable_msat: i64,
    pub receivable_msat: i64,
    pub fee_base_msat: i64,
    pub fee_proportional_millionths: i64,
    pub htlc_minimum_msat: i64,
    pub htlc_maximum_msat: i64,
    pub opener: String,
    pub has_htlc_data: bool,
    pub max_accepted_htlcs: i64,
    pub our_htlcs_in_flight: i64,
}

/// One flow-analysis channel-state row (py `get_all_channel_states` row
/// subset read by the cycle: `state`, `updated_at`, kalman fields).
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelStateRow {
    pub channel_id: String,
    pub peer_id: String,
    /// Flow state; py `state.get("state", "balanced")`.
    pub state: String,
    pub updated_at: Option<i64>,
    /// py `state.get("kalman_flow_ratio", state.get("flow_ratio", 0.0))`.
    pub kalman_flow_ratio: Option<f64>,
    pub kalman_velocity: Option<f64>,
}

/// One trimmed gossip channel row (py `_GOSSIP_CHANNEL_FIELDS`, 3242-3251).
#[derive(Debug, Clone, PartialEq)]
pub struct GossipRow {
    pub source: String,
    pub active: bool,
    pub fee_per_millionth: i64,
    /// py `ch.get("satoshis", ...)` — `None` falls back to
    /// `base_to_sats_floor(amount_msat)` with a context-dependent default.
    pub satoshis: Option<i64>,
    pub amount_msat: Option<i64>,
    pub last_update: i64,
    /// `base_fee_millisatoshi`/`fee_base_msat`; `None` = missing (kept in
    /// the competitor pool by `_is_cln_default_fee`'s conservative rule).
    pub base_fee_msat: Option<i64>,
}

impl GossipRow {
    /// Capacity with the MEDIAN path's default (py 3372:
    /// `max(1, ch.get("satoshis", base_to_sats_floor(ch.get("amount_msat",
    /// 1000000))))`).
    fn capacity_for_median(&self) -> i64 {
        let cap = self
            .satoshis
            .unwrap_or_else(|| self.amount_msat.unwrap_or(1_000_000).div_euclid(1000));
        cap.max(1)
    }

    /// Capacity with the RANK path's default (py 3516: `ch.get("satoshis",
    /// base_to_sats_floor(ch.get("amount_msat", 0)))` — note the 0 msat
    /// default, then `if not cap or cap <= 0: continue`).
    fn capacity_for_rank(&self) -> i64 {
        self.satoshis
            .unwrap_or_else(|| self.amount_msat.unwrap_or(0).div_euclid(1000))
    }

    fn to_gossip_channel(&self, destination: &str) -> GossipChannel {
        GossipChannel {
            source: self.source.clone(),
            destination: destination.to_string(),
            fee_ppm: self.fee_per_millionth,
            // `None` (missing) must NOT look like the CLN default 1000
            // (py `_is_cln_default_fee` returns False on a missing base).
            base_fee_msat: self.base_fee_msat.unwrap_or(-1),
            capacity_sats: self.capacity_for_median(),
            last_update_ts: self.last_update,
        }
    }
}

/// Per-peer fallback rebalance-fee history (py
/// `get_historical_inbound_fee_ppm` result subset, 4142-4151).
#[derive(Debug, Clone, PartialEq)]
pub struct PeerFeeHistory {
    pub confidence: String,
    pub avg_fee_ppm: i64,
}

/// Injected evidence: NO hidden clock/RNG/IO. Implementations read the
/// production DB read-only + `revops-rpc` snapshots; tests script it.
///
/// `neighbor_median_min_competitors` note: the market functions bake the
/// default of 3 (`market::MIN_COMPETITORS`); a config override is not yet
/// plumbed (diff-harness watch item).
pub trait FeeEvidence {
    fn our_node_id(&self) -> String;
    fn channel_states(&self) -> Vec<ChannelStateRow>;
    fn channels_info(&self) -> BTreeMap<String, ChannelInfo>;
    fn chain_costs(&self) -> Option<ChainCosts>;
    /// py `database.get_volume_since(channel_id, since)` (sats).
    fn volume_since(&self, channel_id: &str, since: i64) -> i64;
    /// py `database.get_forward_count_since(channel_id, since)`.
    fn forward_count_since(&self, channel_id: &str, since: i64) -> i64;
    /// py `database.get_channel_probe(channel_id) is not None`.
    fn exploration_flag(&self, channel_id: &str) -> bool;
    /// py `database.clear_channel_probe` — interior mutability allowed.
    /// MUST be a no-op over the read-only evidence surface: this trait
    /// documents itself as reading a DB snapshot (`FeeEvidence: NO hidden
    /// clock/RNG/IO`, module docs above), and `clear_exploration_flag` is
    /// the one deliberate exception — implementations may mutate their
    /// OWN probe-flag bookkeeping (e.g. an interior `RefCell`/DB write)
    /// but must never mutate anything `channels_info`/`gossip_channels`/
    /// etc. observed THIS cycle, and must never change what any other
    /// `FeeEvidence` method returns for the remainder of the cycle
    /// (per-cycle observations stay frozen). Per the T10 review
    /// adjudication.
    fn clear_exploration_flag(&self, channel_id: &str);
    /// py `_get_peer_inbound_channels` (trimmed gossip rows; per-cycle
    /// frozen — implementations should memoize per cycle like PR 3e's
    /// `FrozenObservations`).
    fn gossip_channels(&self, peer_id: &str) -> Vec<GossipRow>;
    /// py `database.get_peer_latency_stats(peer_id, 86400)`.
    fn peer_latency(&self, peer_id: &str) -> Option<PeerLatency>;
    /// py `database.get_channel_cost_history(channel_id, since)`.
    fn channel_cost_history(&self, channel_id: &str, since: i64) -> Vec<RebalanceCostSample>;
    /// py `database.get_historical_inbound_fee_ppm(peer_id, ...)`.
    fn peer_fee_history(&self, peer_id: &str) -> Option<PeerFeeHistory>;
    /// py `database.get_last_forward_time(channel_id)`.
    fn last_forward_time(&self, channel_id: &str) -> Option<i64>;
    /// py `_get_flow_window_map()[channel_id]` (7d directional flow).
    fn flow_window(&self, channel_id: &str) -> Option<FlowWindow>;
    /// py `policy_manager.get_policy(peer_id)`; `None` = no policy manager.
    fn policy(&self, peer_id: &str) -> Option<PeerPolicy>;
    /// py `profitability.get_profitability(cid).marginal_roi_percent`.
    fn marginal_roi_percent(&self, channel_id: &str) -> Option<f64>;
    /// py `temporary_fee_overlay_active(channel_id)`.
    fn temporary_overlay_active(&self, _channel_id: &str) -> bool {
        false
    }
    /// py `database.get_mempool_ma(86400)` for the Vegas update.
    fn mempool_ma_24h(&self) -> f64 {
        0.0
    }
    /// py `listpeerchannels` rows for the node-drain-bias aggregate.
    fn node_channels(&self) -> Vec<drain::NodeChannel> {
        Vec::new()
    }
}

/// Batched end-of-cycle state flush target (py
/// `_flush_pending_fee_strategy_rows`, 4030-4058). Serialization is the
/// sink's job (T9's `state_store` wires the v2 envelope) — this seam only
/// guarantees ONE flush per cycle with last-write-wins rows.
pub trait StateSink {
    fn flush_batch(&self, rows: &[(String, ChannelCycleState, ChannelFeeState)]);
}

/// Injected cycle dependencies (plan Task 10 sketch).
pub struct CycleDeps<'a> {
    pub evidence: &'a dyn FeeEvidence,
    pub cfg: &'a FeeCfgSnapshot,
    pub rng: &'a mut PyRandom,
    pub now: i64,
    /// Governor plumbing; consulted only when
    /// `cfg.econ_governor_fees_enabled`.
    pub governed: Option<&'a GovernedDeps<'a>>,
    pub journal: Option<&'a Journal>,
    pub state_sink: Option<&'a dyn StateSink>,
}

// ---------------------------------------------------------------------------
// Per-channel state (py ChannelCycleState 2227-2325 / ChannelFeeState
// 2053-2223)
// ---------------------------------------------------------------------------
//
// Unified onto T9's `state_store` definitions at the Phase 4 integration
// merge (T10 review contract item 3, T2/T3 `Observation` precedent): the
// shared scalars (`last_broadcast_at`/`last_gossip_refresh`/
// `dynamic_htlcmin_baseline_msat`) are PRIVATE there and every orchestrator
// write in this module goes through the explicit-shared-recording setters,
// so a later `build_merged_row` save classifies them as caller-explicit
// rather than untouched defaults. `serialize_cycle_state_payload` is
// re-exported so the T10 fixture replay (and future callers) keep the
// `cycle::` path; the payload shape/key order is identical to the copy this
// module carried pre-merge.
pub use crate::state_store::{serialize_cycle_state_payload, ChannelCycleState, ChannelFeeState};

/// `_set_last_decision_summary` payload (py 3031-3048).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionSummary {
    pub action: String,
    pub reason: String,
    pub dominant_input: Option<String>,
    pub safety_block: bool,
}

impl Default for DecisionSummary {
    /// py 3024-3030 startup summary.
    fn default() -> Self {
        DecisionSummary {
            action: "hold".to_string(),
            reason: "not_run".to_string(),
            dominant_input: Some("startup".to_string()),
            safety_block: false,
        }
    }
}

/// The single-owner controller state (py instance dicts + Vegas globals).
#[derive(Debug, Default)]
pub struct ControllerState {
    pub cycle_states: BTreeMap<String, ChannelCycleState>,
    pub fee_states: BTreeMap<String, ChannelFeeState>,
    pub vegas: VegasReflexState,
    /// P8 edge trigger (py `_vegas_wake_armed`, init True).
    pub vegas_wake_armed: bool,
    pub last_decision_summary: DecisionSummary,
}

impl ControllerState {
    pub fn new() -> Self {
        ControllerState {
            cycle_states: BTreeMap::new(),
            fee_states: BTreeMap::new(),
            vegas: VegasReflexState::default(),
            vegas_wake_armed: true,
            last_decision_summary: DecisionSummary::default(),
        }
    }

    /// `get_dts_summary` (py 5087-5122) — single-owner port (the bounded
    /// lock acquire + fallback snapshot is thread plumbing with no
    /// decision content here).
    pub fn dts_summary(&self, channel_id: &str) -> Option<OValue> {
        let fee_state = self.fee_states.get(channel_id);
        let cycle_state = self.cycle_states.get(channel_id);
        if fee_state.is_none() && cycle_state.is_none() {
            return None;
        }
        let (mean, std) = match fee_state {
            Some(fs) => (
                OValue::Float(fs.thompson.posterior_mean),
                OValue::Float(fs.thompson.posterior_std),
            ),
            None => (OValue::Null, OValue::Null),
        };
        let broadcast_fee = fee_state
            .map(|f| f.last_broadcast_fee_ppm)
            .or_else(|| cycle_state.map(|c| c.last_broadcast_fee_ppm))
            .unwrap_or(0);
        let forward_count = fee_state
            .map(|f| f.forward_count_since_update)
            .or_else(|| cycle_state.map(|c| c.forward_count_since_update))
            .unwrap_or(0);
        Some(OValue::obj(vec![
            ("posterior_mean".to_string(), mean),
            ("posterior_std".to_string(), std),
            ("broadcast_fee_ppm".to_string(), OValue::Int(broadcast_fee)),
            ("forward_count".to_string(), OValue::Int(forward_count)),
        ]))
    }

    fn set_summary(&mut self, action: &str, reason: &str, dominant: Option<&str>, block: bool) {
        self.last_decision_summary = DecisionSummary {
            action: action.to_string(),
            reason: reason.to_string(),
            dominant_input: dominant.map(str::to_string),
            safety_block: block,
        };
    }
}

/// Fee-desync resync rule (py 8329-8338 / 3911-3920): applied to a tracked
/// broadcast fee against the actual on-chain fee.
fn resync_broadcast_fee(tracked: &mut i64, actual_fee_ppm: i64) {
    if actual_fee_ppm > 0
        && *tracked > 0
        && ((actual_fee_ppm - *tracked).abs() as f64) > (100.0f64).max(*tracked as f64 * 0.5)
    {
        *tracked = actual_fee_ppm;
    }
}

/// `_get_cycle_state` (py 8313-8397) over the in-memory map (DB hydration
/// is T9's store; a fresh channel starts from dataclass defaults).
fn get_cycle_state<'a>(
    states: &'a mut BTreeMap<String, ChannelCycleState>,
    channel_id: &str,
    actual_fee_ppm: Option<i64>,
) -> &'a mut ChannelCycleState {
    let st = states.entry(channel_id.to_string()).or_default();
    if let Some(actual) = actual_fee_ppm {
        resync_broadcast_fee(&mut st.last_broadcast_fee_ppm, actual);
    }
    st
}

/// `_get_channel_fee_state` (py 3875-3961) over the in-memory map.
fn get_fee_state<'a>(
    states: &'a mut BTreeMap<String, ChannelFeeState>,
    channel_id: &str,
    actual_fee_ppm: Option<i64>,
) -> &'a mut ChannelFeeState {
    let st = states.entry(channel_id.to_string()).or_default();
    if let Some(actual) = actual_fee_ppm {
        resync_broadcast_fee(&mut st.last_broadcast_fee_ppm, actual);
    }
    st
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// `_utc_hour` (py 3111-3125): `time.gmtime().tm_hour`. For non-negative
/// unix seconds `gmtime(now).tm_hour == (now % 86400) // 3600` (UTC has no
/// leap-second smearing in unix time).
pub fn utc_hour(now: i64) -> i64 {
    now.rem_euclid(86_400) / 3_600
}

/// `_get_context_with_values` (py 3597-3637): `(context_key, time_bucket,
/// corridor_role)`.
pub fn context_with_values(
    now: i64,
    outbound_ratio: f64,
    flow_state: &str,
) -> (String, String, String) {
    let balance = if outbound_ratio < 0.15 {
        "depleted"
    } else if outbound_ratio < 0.35 {
        "low"
    } else if outbound_ratio < 0.65 {
        "balanced"
    } else if outbound_ratio < SATURATED_OUTBOUND_RATIO {
        "high"
    } else {
        "saturated"
    };
    let hour = utc_hour(now);
    let time_bucket = if hour < 6 {
        "low"
    } else if hour >= 18 {
        "peak"
    } else {
        "normal"
    };
    let role = if matches!(flow_state, "sink" | "dormant" | "unknown") {
        "S"
    } else {
        "P"
    };
    (
        format!("{balance}:{time_bucket}:{role}"),
        time_bucket.to_string(),
        role.to_string(),
    )
}

/// `LiquidityBuckets.get_bucket` (config.py 1492-1514).
pub fn liquidity_bucket(outbound_ratio: f64) -> &'static str {
    if outbound_ratio < 0.1 {
        "very_low"
    } else if outbound_ratio < 0.25 {
        "low"
    } else if outbound_ratio < 0.4 {
        "balanced_low"
    } else if outbound_ratio < 0.6 {
        "balanced"
    } else if outbound_ratio < 0.75 {
        "balanced_high"
    } else if outbound_ratio < 0.9 {
        "high"
    } else {
        "very_high"
    }
}

/// `_is_sparse_data_channel` (py 5145-5161).
fn is_sparse_data_channel(
    observation_count: i64,
    forward_count: i64,
    hours_elapsed: f64,
    current_revenue_rate: f64,
    profile: &FeeProfileSettings,
) -> bool {
    if observation_count < MIN_OBSERVATIONS {
        return true;
    }
    if forward_count < profile.min_forwards_for_signal {
        return true;
    }
    if hours_elapsed >= 1.0 && current_revenue_rate <= 0.0 {
        return true;
    }
    false
}

/// Capacity-rank computation carried from Task 8 (py
/// `_get_competitive_undercut_pct` 3506-3531): returns
/// `Some((larger_than_us, total_competitors))`, or `None` when there is no
/// competitor capacity data or our own capacity is unknown (py early
/// `return 0.10`).
pub fn channel_capacity_rank(rows: &[GossipRow], our_id: &str) -> Option<(usize, usize)> {
    let mut our_capacity: i64 = 0;
    let mut competitor_capacities: Vec<i64> = Vec::new();
    for ch in rows {
        let cap = ch.capacity_for_rank();
        if cap <= 0 {
            continue;
        }
        if ch.source == our_id {
            our_capacity = our_capacity.max(cap);
        } else if ch.active {
            competitor_capacities.push(cap);
        }
    }
    if competitor_capacities.is_empty() || our_capacity <= 0 {
        return None;
    }
    let total = competitor_capacities.len();
    let larger = competitor_capacities
        .iter()
        .filter(|&&c| c > our_capacity)
        .count();
    Some((larger, total))
}

/// `_get_competitive_undercut_pct` (py 3484-3552) composed from the rank
/// carry + `market::competitive_undercut_pct` (T8's pure corridor math).
fn competitive_undercut_pct(
    rows: &[GossipRow],
    our_id: &str,
    neighbor_median: Option<i64>,
    invert_rank: bool,
) -> f64 {
    match channel_capacity_rank(rows, our_id) {
        None => 0.10, // py 3524-3525: default 10% when no data
        Some((rank, count)) => {
            // py 3540-3546: corridor adjustment only when a median exists;
            // market::competitive_undercut_pct's i64 median treats the
            // 100..=300 band as a no-op, so a missing median maps to any
            // in-band value (we use 100 — strictly between the `< 100`
            // and `> 300` gates... note 100 is NOT < 100, so no-op).
            let median = neighbor_median.unwrap_or(100);
            market::competitive_undercut_pct(rank, count, median, invert_rank)
        }
    }
}

/// `_get_channel_rebalance_cost_ppm` (py 3554-3594): dest-only windowed
/// aggregate for the SOFT nudge (distinct from the hard floor). Note the
/// float division + `int()` truncation (py 3590), unlike the floor's `//`.
fn channel_rebalance_cost_ppm(flow_state: &str, history: &[RebalanceCostSample], now: i64) -> i64 {
    if flow_state == "sink" || flow_state == "dormant" {
        return 0;
    }
    let cutoff = now - REBALANCE_FLOOR_WINDOW_DAYS * 86400;
    let recent: Vec<&RebalanceCostSample> =
        history.iter().filter(|c| c.timestamp >= cutoff).collect();
    let total_cost: i64 = recent.iter().map(|c| c.cost_sats).sum();
    let total_volume: i64 = recent.iter().map(|c| c.amount_sats).sum();
    if total_volume <= 0 || total_cost <= 0 {
        return 0;
    }
    let cost_ppm = ((total_cost * 1_000_000) as f64 / total_volume as f64) as i64;
    cost_ppm.min(5000)
}

/// Python `"True"`/`"False"` capitalization for f-string bools.
fn py_bool_str(b: bool) -> &'static str {
    if b {
        "True"
    } else {
        "False"
    }
}

// ---------------------------------------------------------------------------
// Per-channel outcome shapes
// ---------------------------------------------------------------------------

/// py `FeeAdjustment` (2404-2435).
#[derive(Debug, Clone, PartialEq)]
pub struct FeeAdjustmentRec {
    pub channel_id: String,
    pub peer_id: String,
    pub old_fee_ppm: i64,
    pub new_fee_ppm: i64,
    pub reason: String,
    pub algorithm_values: OValue,
    pub reason_code: String,
}

impl FeeAdjustmentRec {
    /// `FeeAdjustment.to_dict` (py 2426-2435), key order frozen.
    pub fn to_dict(&self) -> OValue {
        OValue::obj(vec![
            (
                "channel_id".to_string(),
                OValue::str(self.channel_id.clone()),
            ),
            ("peer_id".to_string(), OValue::str(self.peer_id.clone())),
            ("old_fee_ppm".to_string(), OValue::Int(self.old_fee_ppm)),
            ("new_fee_ppm".to_string(), OValue::Int(self.new_fee_ppm)),
            ("reason".to_string(), OValue::str(self.reason.clone())),
            (
                "algorithm_values".to_string(),
                self.algorithm_values.clone(),
            ),
            (
                "reason_code".to_string(),
                OValue::str(self.reason_code.clone()),
            ),
        ])
    }
}

/// Outcome of one channel through the loop body.
#[derive(Debug, Clone, PartialEq)]
pub enum ChannelOutcome {
    Adjusted(Box<FeeAdjustmentRec>),
    /// One of the loop's `skip_reasons` keys (py 4541-4553).
    Skipped(&'static str),
}

/// Full per-channel result: outcome + dry-run trace + governed audit.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelResult {
    pub outcome: ChannelOutcome,
    /// Dry-run superset diagnostics for the journal `trace` field.
    pub trace: OValue,
    pub governed: Option<GovernedTrace>,
}

// ---------------------------------------------------------------------------
// process_channel — the loop body (py 4746-4883)
// ---------------------------------------------------------------------------

/// One channel through `_adjust_all_fees_channel_loop`'s body: overlay
/// gate, policy handling (PASSIVE skip / STATIC pin), then
/// `_adjust_channel_fee` with skip classification on a None return.
#[allow(clippy::too_many_arguments)]
pub fn process_channel(
    state: &mut ControllerState,
    deps: &mut CycleDeps<'_>,
    row: &ChannelStateRow,
    info: &ChannelInfo,
    chain_costs: Option<&ChainCosts>,
    node_drain_bias_effective_cap: Option<f64>,
    node_receivable_ratio: Option<f64>,
    node_drain_pressure: Option<f64>,
) -> ChannelResult {
    let channel_id = row.channel_id.as_str();
    let peer_id = row.peer_id.as_str();
    let now = deps.now;
    let (profile_name, profile) = fee_profile(&deps.cfg.fee_profile);

    if deps.evidence.temporary_overlay_active(channel_id) {
        return ChannelResult {
            outcome: ChannelOutcome::Skipped("temporary_overlay"),
            trace: OValue::obj(vec![(
                "skip_reason".to_string(),
                OValue::str("temporary_overlay"),
            )]),
            governed: None,
        };
    }

    // Policy gate (py 4764-4818).
    let policy = deps.evidence.policy(peer_id);
    if let Some(p) = &policy {
        match p.strategy {
            FeeStrategy::Passive => {
                return ChannelResult {
                    outcome: ChannelOutcome::Skipped("policy_passive"),
                    trace: OValue::obj(vec![(
                        "skip_reason".to_string(),
                        OValue::str("policy_passive"),
                    )]),
                    governed: None,
                };
            }
            FeeStrategy::Static => {
                if let Some(target) = p.fee_ppm_target {
                    return static_policy_branch(state, deps, info, peer_id, target, now);
                }
                // No target: falls through to DYNAMIC optimization like
                // Python (the STATIC arm requires fee_ppm_target).
            }
            FeeStrategy::Dynamic => {}
        }
    }

    // py 4828-4846: pre-call snapshot for skip classification.
    let actual_fee = info.fee_proportional_millionths;
    {
        let cycle = get_cycle_state(&mut state.cycle_states, channel_id, Some(actual_fee));
        let _ = cycle;
    }
    let (pre_is_sleeping, pre_last_update, pre_last_broadcast_fee) = {
        let cycle = state.cycle_states.get(channel_id).expect("just inserted");
        (
            cycle.is_sleeping,
            cycle.last_update,
            cycle.last_broadcast_fee_ppm,
        )
    };
    let mut pre_forward_count = 0i64;
    let mut pre_hours_elapsed = 0.0f64;
    let mut forward_count_hint: Option<i64> = None;
    if pre_last_update > 0 {
        pre_hours_elapsed = (now - pre_last_update) as f64 / 3600.0;
        pre_forward_count = deps
            .evidence
            .forward_count_since(channel_id, pre_last_update);
        forward_count_hint = Some(pre_forward_count);
    }

    let adjust = adjust_channel_fee(
        state,
        deps,
        AdjustCtx {
            row,
            info,
            chain_costs,
            policy: policy.as_ref(),
            forward_count_hint,
            forward_count_hint_since: pre_last_update,
            node_drain_bias_effective_cap,
            node_receivable_ratio,
            node_drain_pressure,
            force_reprice_reason: None,
        },
        profile_name,
        profile,
    );

    match adjust.adjustment {
        Some(adj) => ChannelResult {
            outcome: ChannelOutcome::Adjusted(Box::new(adj)),
            trace: adjust.trace,
            governed: adjust.governed,
        },
        None => {
            let cycle = state.cycle_states.get(channel_id).expect("state exists");
            let reason = classify_no_adjustment_skip_reason(
                cycle,
                now,
                pre_is_sleeping,
                pre_last_update,
                pre_hours_elapsed,
                pre_forward_count,
                actual_fee,
                pre_last_broadcast_fee,
                profile,
            );
            ChannelResult {
                outcome: ChannelOutcome::Skipped(reason),
                trace: adjust.trace,
                governed: adjust.governed,
            }
        }
    }
}

/// STATIC strategy branch (py 4774-4818): apply fixed fee via the dry-run
/// execution decision, mirroring `set_channel_fee`'s should_sync_state
/// bookkeeping (py 7853-7892).
fn static_policy_branch(
    state: &mut ControllerState,
    deps: &mut CycleDeps<'_>,
    info: &ChannelInfo,
    peer_id: &str,
    target: i64,
    now: i64,
) -> ChannelResult {
    let cfg = deps.cfg;
    let channel_id = info.channel_id.as_str();
    let current_fee = info.fee_proportional_millionths;
    let requested_static_fee = target;
    let effective_static_fee = cfg
        .min_fee_ppm
        .max(cfg.max_fee_ppm.min(requested_static_fee));
    if current_fee == effective_static_fee {
        return ChannelResult {
            outcome: ChannelOutcome::Skipped("policy_static"),
            trace: OValue::obj(vec![(
                "skip_reason".to_string(),
                OValue::str("policy_static"),
            )]),
            governed: None,
        };
    }

    let decision = decide_set_channel_fee(
        &SetFeeRequest {
            channel_id: channel_id.to_string(),
            fee_ppm: requested_static_fee,
            enforce_limits: true,
            effective_min_fee_ppm: None,
            htlcmax_msat: None,
            base_fee_msat: cfg.base_fee_msat,
        },
        cfg,
        None,
    );
    let mut governed = None;
    let mut success = decision.success;
    if success && cfg.econ_governor_fees_enabled {
        if let Some(gdeps) = deps.governed {
            let (ok, _code, trace) = execution::governed_authorize_fee_broadcast(
                gdeps,
                channel_id,
                decision.clamped_fee_ppm,
                Some(current_fee),
                "Policy: STATIC",
                Some(FeeReasonCode::PolicyStatic.as_str()),
                now,
            );
            governed = trace;
            if !ok {
                success = false;
            }
        }
    }
    if !success {
        return ChannelResult {
            outcome: ChannelOutcome::Skipped("error"),
            trace: OValue::obj(vec![("skip_reason".to_string(), OValue::str("error"))]),
            governed,
        };
    }
    let applied_fee_ppm = decision.clamped_fee_ppm;

    // set_channel_fee should_sync_state (py 7862-7892).
    {
        let cycle = get_cycle_state(&mut state.cycle_states, channel_id, Some(applied_fee_ppm));
        cycle.is_sleeping = false;
        cycle.sleep_until = 0;
        cycle.stable_cycles = 0;
        cycle.last_fee_ppm = applied_fee_ppm;
        cycle.last_broadcast_fee_ppm = applied_fee_ppm;
        cycle.set_last_broadcast_at(now);
        cycle.last_update = now;
        cycle.last_state = FeeReasonCode::PolicyStatic.as_str().to_string();
    }
    {
        let ts = get_fee_state(&mut state.fee_states, channel_id, Some(applied_fee_ppm));
        ts.is_sleeping = false;
        ts.sleep_until = 0;
        ts.stable_cycles = 0;
        ts.last_fee_ppm = applied_fee_ppm;
        ts.last_broadcast_fee_ppm = applied_fee_ppm;
        ts.set_last_broadcast_at(now);
        ts.last_update = now;
        ts.last_state = FeeReasonCode::PolicyStatic.as_str().to_string();
    }

    let adj = FeeAdjustmentRec {
        channel_id: channel_id.to_string(),
        peer_id: peer_id.to_string(),
        old_fee_ppm: current_fee,
        new_fee_ppm: applied_fee_ppm,
        reason: "Policy: STATIC fee override".to_string(),
        algorithm_values: OValue::obj(vec![
            ("policy".to_string(), OValue::str("static")),
            (
                "requested_fee_ppm".to_string(),
                OValue::Int(requested_static_fee),
            ),
            (
                "effective_fee_ppm".to_string(),
                OValue::Int(applied_fee_ppm),
            ),
        ]),
        reason_code: FeeReasonCode::PolicyStatic.as_str().to_string(),
    };
    ChannelResult {
        outcome: ChannelOutcome::Adjusted(Box::new(adj)),
        trace: OValue::obj(vec![
            ("disposition".to_string(), OValue::str("policy_static")),
            ("would_broadcast".to_string(), OValue::Bool(true)),
        ]),
        governed,
    }
}

/// `_classify_no_adjustment_skip_reason` (py 4885-4921), verbatim.
#[allow(clippy::too_many_arguments)]
fn classify_no_adjustment_skip_reason(
    cycle: &ChannelCycleState,
    now: i64,
    pre_is_sleeping: bool,
    pre_last_update: i64,
    pre_hours_elapsed: f64,
    pre_forward_count: i64,
    actual_fee_ppm: i64,
    pre_last_broadcast_fee_ppm: i64,
    profile: &FeeProfileSettings,
) -> &'static str {
    if pre_is_sleeping {
        return "sleeping";
    }
    if pre_last_update > 0 {
        let time_ok = pre_hours_elapsed >= profile.min_observation_hours;
        let forwards_ok = pre_forward_count >= profile.min_forwards_for_signal;
        if !time_ok && !forwards_ok {
            return "waiting_time";
        }
    }
    if cycle.last_update >= now {
        if cycle.last_fee_ppm != actual_fee_ppm
            && cycle.last_broadcast_fee_ppm == pre_last_broadcast_fee_ppm
        {
            return "gossip_hysteresis";
        }
        if cycle.last_fee_ppm == actual_fee_ppm && cycle.last_broadcast_fee_ppm == actual_fee_ppm {
            return "idempotent";
        }
        return "alpha_guard";
    }
    "fee_unchanged"
}

// ---------------------------------------------------------------------------
// adjust_channel_fee — py 5504-7355
// ---------------------------------------------------------------------------

/// Per-channel inputs to [`adjust_channel_fee`].
pub struct AdjustCtx<'a> {
    pub row: &'a ChannelStateRow,
    pub info: &'a ChannelInfo,
    pub chain_costs: Option<&'a ChainCosts>,
    pub policy: Option<&'a PeerPolicy>,
    pub forward_count_hint: Option<i64>,
    pub forward_count_hint_since: i64,
    pub node_drain_bias_effective_cap: Option<f64>,
    pub node_receivable_ratio: Option<f64>,
    pub node_drain_pressure: Option<f64>,
    pub force_reprice_reason: Option<&'a str>,
}

/// [`adjust_channel_fee`] result: the would-be `FeeAdjustment` (None on
/// any suppressed/skip path, like Python) plus dry-run diagnostics.
pub struct AdjustResult {
    pub adjustment: Option<FeeAdjustmentRec>,
    pub trace: OValue,
    pub governed: Option<GovernedTrace>,
}

struct TraceBuilder(Vec<(String, OValue)>);

impl TraceBuilder {
    fn new() -> Self {
        TraceBuilder(Vec::new())
    }
    fn set(&mut self, key: &str, v: OValue) {
        if let Some(slot) = self.0.iter_mut().find(|(k, _)| k == key) {
            slot.1 = v;
        } else {
            self.0.push((key.to_string(), v));
        }
    }
    fn finish(self) -> OValue {
        OValue::Obj(self.0)
    }
}

/// `_adjust_channel_fee` (py 5504-7355) as a dry-run decision. Returns the
/// would-be `FeeAdjustment` or `None` exactly where Python does; instead
/// of `self.set_channel_fee(...)` (py 7203) the broadcast is decided by
/// `execution::decide_set_channel_fee` (+ the governed gate when enabled).
/// NO RPC side effects anywhere.
pub fn adjust_channel_fee(
    state: &mut ControllerState,
    deps: &mut CycleDeps<'_>,
    ctx: AdjustCtx<'_>,
    fee_profile_name: &str,
    profile: &FeeProfileSettings,
) -> AdjustResult {
    let cfg = deps.cfg;
    let evidence = deps.evidence;
    let now = deps.now;
    let channel_id = ctx.row.channel_id.as_str();
    let peer_id = ctx.row.peer_id.as_str();
    let info = ctx.info;
    let mut trace = TraceBuilder::new();
    let mut governed_trace: Option<GovernedTrace> = None;

    // py 5547-5574: explainability defaults.
    let mut original_step_ppm: i64;
    let mut woke_from_sleep = false;
    let mut wake_reason: String = "none".to_string();
    let mut raw_dts_target_ppm: Option<i64> = None;
    let mut post_pid_target_ppm: Option<i64> = None;
    let mut bounded_target_ppm: Option<i64> = None;
    let mut blended_target_ppm: Option<i64> = None;
    let mut applied_target_ppm: Option<i64> = None;
    let mut zero_flow_guard_reason: Option<&'static str> = None;
    let mut zero_flow_guard_target_ppm: Option<i64> = None;
    let mut zero_revenue_streak: Option<i64> = None;
    let mut supported_cap_ppm: Option<i64> = None;
    let mut upward_probe_pre_cap_ppm: Option<i64> = None;
    let mut bound_reason = "none";
    let mut delta_cap_reason: &'static str = "none";
    let mut delta_cap_ppm: i64 = 0;
    let mut delta_cap_applied = false;
    let mut sparse_data_conservative = false;
    let mut target_blend_ratio = profile.normal_target_blend_ratio;
    let mut exploration_mode = "none";
    let mut context_key = String::new();
    let mut time_bucket = "normal".to_string();
    let mut corridor_role = "P".to_string();
    let mut contextual_sample_used = false;
    let mut context_observation_count: i64 = 0;
    let mut drain_multiplier = 1.0f64;
    let mut effective_discount_max = cfg.drain_fee_discount_max;

    // py 5579: congestion detect (live HTLC recompute + staleness TTL).
    let flow_row = FlowStateRow {
        state: Some(ctx.row.state.clone()),
        updated_at: ctx.row.updated_at,
    };
    let live = LiveHtlc {
        has_htlc_data: info.has_htlc_data,
        max_accepted_htlcs: info.max_accepted_htlcs,
        our_htlcs_in_flight: info.our_htlcs_in_flight,
    };
    let is_congested = floors::detect_congestion(
        Some(&flow_row),
        Some(&live),
        cfg.htlc_congestion_threshold,
        cfg.flow_interval,
        now,
    );

    // py 5586-5587.
    let is_under_exploration = evidence.exploration_flag(channel_id);

    // py 5592-5597.
    let raw_chain_fee = info.fee_proportional_millionths;
    let mut current_fee_ppm = raw_chain_fee;
    if current_fee_ppm == 0 && !is_under_exploration {
        current_fee_ppm = cfg.min_fee_ppm;
    }

    // Ensure both states exist, run desync checks (py 5600, 5618), then
    // split-borrow the two maps for the rest of the function.
    get_cycle_state(&mut state.cycle_states, channel_id, Some(raw_chain_fee));
    get_fee_state(&mut state.fee_states, channel_id, Some(raw_chain_fee));
    let ControllerState {
        cycle_states,
        fee_states,
        vegas: vegas_state,
        ..
    } = state;
    let cycle = cycle_states.get_mut(channel_id).expect("inserted above");
    let ts = fee_states.get_mut(channel_id).expect("inserted above");

    // py 5605: previous broadcast direction, captured before overwrites.
    let prev_trend_direction = cycle.trend_direction;

    // =====================================================================
    // Sleep status check (py 5617-5711) — ADR stage 4 cooldown.
    // =====================================================================
    let mut sleep_is_sleeping = ts.is_sleeping;
    let sleep_until = ts.sleep_until;
    let sleep_last_update = ts.last_update;
    let sleep_last_revenue_rate = ts.last_revenue_rate;

    if sleep_is_sleeping && ctx.force_reprice_reason.is_some() {
        woke_from_sleep = true;
        wake_reason = ctx.force_reprice_reason.unwrap_or("none").to_string();
        sleep_is_sleeping = false;
        ts.is_sleeping = false;
        ts.sleep_until = 0;
        ts.stable_cycles = 0;
        cycle.is_sleeping = false;
        cycle.sleep_until = 0;
        cycle.stable_cycles = 0;
    }

    if sleep_is_sleeping {
        if now > sleep_until {
            // Timer expired — wake up (py 5643-5658).
            woke_from_sleep = true;
            wake_reason = "sleep_timer_expired".to_string();
            ts.is_sleeping = false;
            ts.sleep_until = 0;
            ts.stable_cycles = 0;
            cycle.is_sleeping = false;
            cycle.sleep_until = 0;
            cycle.stable_cycles = 0;
        } else {
            // Still sleeping — spike / congestion wake check (py 5659-5711).
            let volume_since_sats = evidence.volume_since(channel_id, sleep_last_update);
            let mut hours_elapsed = if sleep_last_update > 0 {
                (now - sleep_last_update) as f64 / 3600.0
            } else {
                1.0
            };
            hours_elapsed = hours_elapsed.max(0.1);
            let revenue_sats = (volume_since_sats * raw_chain_fee) as f64 / 1_000_000.0;
            let current_revenue_rate = revenue_sats / hours_elapsed;
            let mut percent_change = if sleep_last_revenue_rate <= 0.0 {
                if current_revenue_rate > 0.0 {
                    1.0
                } else {
                    0.0
                }
            } else {
                (current_revenue_rate - sleep_last_revenue_rate).abs() / sleep_last_revenue_rate
            };
            if is_congested {
                percent_change = 1.0; // L4 congestion wake
            }
            if percent_change > WAKE_UP_THRESHOLD {
                woke_from_sleep = true;
                wake_reason = if is_congested {
                    "congestion".to_string()
                } else {
                    "revenue_spike".to_string()
                };
                ts.is_sleeping = false;
                ts.sleep_until = 0;
                ts.stable_cycles = 0;
                cycle.is_sleeping = false;
                cycle.sleep_until = 0;
                cycle.stable_cycles = 0;
            } else {
                trace.set("disposition", OValue::str("sleeping_hold"));
                return AdjustResult {
                    adjustment: None,
                    trace: trace.finish(),
                    governed: governed_trace,
                };
            }
        }
    }

    // =====================================================================
    // Observation window (py 5713-5804).
    // =====================================================================
    let mut observation_cursor = cycle.last_update;
    if observation_cursor <= 0 {
        let interval = if cfg.fee_interval != 0 {
            cfg.fee_interval
        } else {
            1800
        };
        observation_cursor = now - interval;
    }
    let volume_since_sats = evidence.volume_since(channel_id, observation_cursor);

    let mut hours_elapsed = if cycle.last_update > 0 {
        (now - cycle.last_update) as f64 / 3600.0
    } else {
        0.0
    };

    let forward_count = if ctx.forward_count_hint.is_some()
        && ctx.forward_count_hint_since == cycle.last_update
        && cycle.last_update > 0
    {
        ctx.forward_count_hint.unwrap_or(0)
    } else {
        evidence.forward_count_since(channel_id, observation_cursor)
    };
    cycle.forward_count_since_update = forward_count;

    if cycle.last_update > 0 {
        let time_ok = hours_elapsed >= profile.min_observation_hours;
        let forwards_ok = forward_count >= profile.min_forwards_for_signal;
        if ctx.force_reprice_reason.is_some() || time_ok || forwards_ok {
            // window closed — proceed (py 5768-5782)
        } else {
            trace.set("disposition", OValue::str("waiting_window"));
            return AdjustResult {
                adjustment: None,
                trace: trace.finish(),
                governed: governed_trace,
            };
        }
    }

    if hours_elapsed <= 0.0 {
        hours_elapsed = 1.0;
    }

    // py 5815-5820: revenue rate on the TRUE on-chain fee.
    let revenue_sats = (volume_since_sats * raw_chain_fee) as f64 / 1_000_000.0;
    let raw_revenue_rate = if hours_elapsed > 0.0 {
        revenue_sats / hours_elapsed
    } else {
        0.0
    };
    let current_revenue_rate = raw_revenue_rate;

    // py 5822-5827.
    let capacity = if info.capacity_sats != 0 {
        info.capacity_sats
    } else {
        2_000_000
    };
    let spendable = info.spendable_msat.div_euclid(1000);
    let outbound_ratio = if capacity > 0 {
        spendable as f64 / capacity as f64
    } else {
        0.5
    };
    let bucket = liquidity_bucket(outbound_ratio);

    // py 5830-5835.
    let flow_state = ctx.row.state.as_str();
    let flow_state_multiplier = match flow_state {
        "source" => 1.10,
        "sink" => 0.75,
        _ => 1.0,
    };

    // py 5837-5842.
    let marginal_roi_info = match evidence.marginal_roi_percent(channel_id) {
        Some(x) => format!("marginal_roi={x:.1}%"),
        None => "unknown".to_string(),
    };

    // =====================================================================
    // Floors / ceiling (py 5844-5955) — ADR stage 1 rails.
    // =====================================================================
    let effective_min_fee_ppm = floors::effective_min_fee_ppm(
        &MinFeeCfg {
            min_fee_ppm: cfg.min_fee_ppm,
            min_fee_ppm_saturated: cfg.min_fee_ppm_saturated,
        },
        Some(flow_state),
        Some(outbound_ratio),
        capacity,
        evidence.flow_window(channel_id).as_ref(),
    );
    let opener = info.opener.as_str();
    let latency = evidence.peer_latency(peer_id);
    let mut base_floor_ppm =
        floors::calculate_floor(capacity, ctx.chain_costs, latency.as_ref(), opener);
    base_floor_ppm = base_floor_ppm.max(effective_min_fee_ppm);
    base_floor_ppm = (base_floor_ppm as f64 * flow_state_multiplier) as i64;
    base_floor_ppm = base_floor_ppm.max(effective_min_fee_ppm);

    let vegas_multiplier = vegas::vegas_floor_multiplier(vegas_state);
    if vegas_multiplier > 1.0 {
        base_floor_ppm = (base_floor_ppm as f64 * vegas_multiplier) as i64;
    }

    // py 5866-5884: rebalance cost-aware hard floor.
    let cost_cutoff = now - REBALANCE_FLOOR_WINDOW_DAYS * 86400;
    let cost_history = evidence.channel_cost_history(channel_id, cost_cutoff);
    let peer_history = evidence.peer_fee_history(peer_id);
    let peer_fallback = peer_history
        .as_ref()
        .map(|h| (h.confidence.as_str(), h.avg_fee_ppm));
    let rebalance_floor_ppm =
        floors::rebalance_cost_floor(flow_state, &cost_history, peer_fallback, now);
    let rebalance_floor_active = rebalance_floor_ppm.is_some();
    if let Some(rf) = rebalance_floor_ppm {
        if rf > base_floor_ppm {
            base_floor_ppm = rf;
        }
    }

    // py 5886-5898: soft nudge input (only when the hard floor is off).
    let rebalance_cost_ppm = if !rebalance_floor_active {
        channel_rebalance_cost_ppm(flow_state, &cost_history, now)
    } else {
        0
    };

    // py 5900-5912: flow-adjusted ceiling.
    let base_ceiling_ppm = floors::flow_adjusted_ceiling(
        current_fee_ppm,
        cfg.max_fee_ppm,
        evidence.last_forward_time(channel_id),
        now,
    );

    let mut floor_ppm = base_floor_ppm;
    let mut ceiling_ppm = base_ceiling_ppm;

    // py 5914-5935: per-peer dynamic fee bounds (M-3b).
    if let Some(p) = ctx.policy {
        let policy_anchor = p.fee_ppm_target.unwrap_or(0);
        if policy_anchor > 0 && (p.fee_multiplier_min.is_some() || p.fee_multiplier_max.is_some()) {
            let (mult_min, mult_max) = p.fee_multiplier_bounds();
            if p.fee_multiplier_min.is_some() {
                floor_ppm = floor_ppm.max((policy_anchor as f64 * mult_min) as i64);
            }
            if p.fee_multiplier_max.is_some() {
                ceiling_ppm = ceiling_ppm.min((policy_anchor as f64 * mult_max) as i64);
            }
        }
    }

    // py 5937-5955: floor/ceiling inversion guard (ceiling wins).
    if floor_ppm >= ceiling_ppm {
        let overridden_floor_ppm = effective_min_fee_ppm.max(ceiling_ppm - 10);
        floor_ppm = overridden_floor_ppm;
        if floor_ppm >= ceiling_ppm {
            ceiling_ppm = floor_ppm + 10;
        }
        trace.set("floor_inversion", OValue::Bool(true));
    }

    // =====================================================================
    // Target decision block (py 5957-6899).
    // =====================================================================
    let mut decision_reason: String = "unknown".to_string();
    let mut new_fee_ppm: i64 = 0;
    let mut target_found = false;
    let mut volatility_reset = false;
    let mut new_direction: i64 = 0;
    let mut step_ppm: i64;
    let mut rate_change = 0.0f64;
    let mut previous_rate = cycle.last_revenue_rate;

    // py 5970: the observation describes the PREVIOUS window (M3).
    let prev_congestion_active = cycle.congestion_active;

    if !is_congested && cycle.congestion_active {
        // M2: quiet-cycle bookkeeping (py 5972-5982).
        cycle.congestion_quiet_cycles += 1;
        if cycle.congestion_quiet_cycles >= CONGESTION_EXIT_QUIET_CYCLES {
            cycle.congestion_active = false;
            cycle.congestion_entry_fee_ppm = 0;
            cycle.congestion_quiet_cycles = 0;
        }
    }

    // Priority 1: congestion (py 5984-6096).
    if is_congested {
        cycle.congestion_quiet_cycles = 0;
        decision_reason = "CONGESTION".to_string();

        if raw_chain_fee > 0 {
            // (c) always record the window (raw rate; congestion IS the
            // demand signal); SL-1 censoring applies.
            let (_ck, congestion_time_bucket, _role) =
                context_with_values(now, outbound_ratio, flow_state);
            if !rails::is_unroutable_zero_window(current_revenue_rate, spendable as f64) {
                dynamics::update_posterior(
                    &mut ts.thompson,
                    raw_chain_fee as f64,
                    current_revenue_rate,
                    hours_elapsed,
                    &congestion_time_bucket,
                    prev_congestion_active,
                    now,
                );
            }
        }

        let first_trip = !cycle.congestion_active;
        cycle.congestion_active = true;
        if first_trip || cycle.congestion_entry_fee_ppm <= 0 {
            cycle.congestion_entry_fee_ppm = current_fee_ppm.max(1);
        }

        let episode_cap_ppm = ((cycle.congestion_entry_fee_ppm as f64
            * CONGESTION_EPISODE_MAX_MULTIPLIER) as i64)
            .max(cycle.congestion_entry_fee_ppm + CONGESTION_FEE_MIN_HEADROOM_PPM);
        let mut congestion_cap_ppm = ceiling_ppm.min(episode_cap_ppm).min(
            ((current_fee_ppm as f64 * CONGESTION_FEE_MAX_MULTIPLIER) as i64)
                .max(current_fee_ppm + CONGESTION_FEE_MIN_HEADROOM_PPM),
        );
        congestion_cap_ppm = congestion_cap_ppm.max(current_fee_ppm);

        if first_trip {
            // Undamped entry-edge jump (py 6050-6054).
            new_fee_ppm = floor_ppm.max(ceiling_ppm.min(congestion_cap_ppm));
            bounded_target_ppm = Some(new_fee_ppm);
            applied_target_ppm = Some(new_fee_ppm);
        } else {
            // Damped follow-up (py 6056-6088).
            let congestion_floor_ppm = floor_ppm.max(
                ((current_fee_ppm as f64 * CONGESTION_FLOOR_MULTIPLIER) as i64)
                    .min(congestion_cap_ppm),
            );
            floor_ppm = congestion_floor_ppm;
            if floor_ppm >= ceiling_ppm {
                ceiling_ppm = floor_ppm + 10;
            }
            let bounded = floor_ppm.max(ceiling_ppm.min(congestion_cap_ppm));
            bounded_target_ppm = Some(bounded);
            let (blended, blend_info) = rails::blend_fee_target(
                current_fee_ppm,
                bounded,
                woke_from_sleep,
                false,
                ts.thompson.posterior_std,
                fee_profile_name,
                profile,
            );
            blended_target_ppm = Some(blended);
            target_blend_ratio = blend_info.blend_ratio;
            let (applied, damping) =
                rails::apply_damped_fee_target(current_fee_ppm, blended, woke_from_sleep, profile);
            new_fee_ppm = applied;
            applied_target_ppm = Some(new_fee_ppm);
            delta_cap_reason = damping.cap_reason;
            delta_cap_ppm = damping.max_delta_ppm;
            delta_cap_applied = damping.cap_applied;
        }

        new_direction = (new_fee_ppm - current_fee_ppm).signum();
        step_ppm = (new_fee_ppm - current_fee_ppm).abs();
        original_step_ppm = step_ppm;
        volatility_reset = false;
        rate_change = 0.0;
        previous_rate = cycle.last_revenue_rate;
        target_found = true;
    } else {
        step_ppm = 0;
        original_step_ppm = 0;
    }

    // Priority 2: bounded low-fee exploration (py 6098-6189).
    if !target_found && is_under_exploration {
        let exploration_success = volume_since_sats > 0 || forward_count > 0;

        // M6: record the exploration window (py 6110-6125).
        if raw_chain_fee > 0
            && !rails::is_unroutable_zero_window(current_revenue_rate, spendable as f64)
        {
            let (_ck, exploration_time_bucket, _role) =
                context_with_values(now, outbound_ratio, flow_state);
            dynamics::update_posterior(
                &mut ts.thompson,
                raw_chain_fee as f64,
                current_revenue_rate,
                hours_elapsed,
                &exploration_time_bucket,
                false,
                now,
            );
        }

        sparse_data_conservative = is_sparse_data_channel(
            0,
            forward_count,
            hours_elapsed,
            current_revenue_rate,
            profile,
        );
        if exploration_success {
            evidence.clear_exploration_flag(channel_id);
            new_fee_ppm = rails::exploration_fee_target(
                current_fee_ppm.max(floor_ppm),
                floor_ppm,
                cfg.min_fee_ppm,
                sparse_data_conservative,
                Some(effective_min_fee_ppm),
            );
            exploration_mode = "bounded_low_fee_success";
            decision_reason = "LOW_FEE_EXPLORATION_SUCCESS".to_string();
            new_direction = (new_fee_ppm - current_fee_ppm).signum();
            step_ppm = (new_fee_ppm - current_fee_ppm).abs();
        } else {
            new_fee_ppm = rails::exploration_fee_target(
                current_fee_ppm,
                floor_ppm,
                cfg.min_fee_ppm,
                sparse_data_conservative,
                Some(effective_min_fee_ppm),
            );
            exploration_mode = "bounded_low_fee";
            decision_reason = "LOW_FEE_EXPLORATION".to_string();
            new_direction = cycle.trend_direction;
            step_ppm = cycle.step_ppm;
        }
        original_step_ppm = step_ppm;
        volatility_reset = false;
        rate_change = 0.0;
        previous_rate = cycle.last_revenue_rate;
        target_found = true;
    }

    // Priority 4: DTS+PID (py 6192-6899).
    if !target_found {
        let observation_count = ts.thompson.observations.len() as i64;
        sparse_data_conservative = is_sparse_data_channel(
            observation_count,
            forward_count,
            hours_elapsed,
            current_revenue_rate,
            profile,
        );
        rate_change = current_revenue_rate - ts.last_revenue_rate;
        previous_rate = ts.last_revenue_rate;

        let (ck, tb, role) = context_with_values(now, outbound_ratio, flow_state);
        context_key = ck;
        time_bucket = tb;
        corridor_role = role;

        // Demand-adjusted reward signal (py 6232-6253).
        let mut expected_demand = 0.5f64;
        {
            let kr = ctx.row.kalman_flow_ratio.unwrap_or(0.0);
            let kv = ctx.row.kalman_velocity.unwrap_or(0.0);
            if kr.is_finite() && kv.is_finite() {
                expected_demand = kr.abs() + (kv * 24.0).abs();
            }
        }
        let demand_factor = rails::kalman_demand_factor(expected_demand);
        let adjusted_revenue_rate = current_revenue_rate / demand_factor;

        // Volatility & hysteresis (py 6288-6355).
        volatility_reset = false;
        let mut rate_change_ratio = 0.0f64;
        if ts.last_update > 0 && ts.last_revenue_rate > 0.0 {
            let delta_rate = (current_revenue_rate - ts.last_revenue_rate).abs();
            rate_change_ratio = delta_rate / 1.0f64.max(ts.last_revenue_rate);
            if rate_change_ratio > VOLATILITY_THRESHOLD {
                volatility_reset = true;
                ts.stable_cycles = 0;
            }
        } else if ts.last_update > 0 && ts.last_revenue_rate <= 0.0 && current_revenue_rate > 0.0 {
            // M1: revenue reappearing after silence.
            volatility_reset = true;
            ts.stable_cycles = 0;
            rate_change_ratio = VOLATILITY_THRESHOLD + 1.0;
        }

        // Shared single recording site (py 6255-6285).
        let record_window = |ts: &mut ChannelFeeState, now: i64| -> bool {
            if raw_chain_fee > 0
                && !rails::is_unroutable_zero_window(adjusted_revenue_rate, spendable as f64)
            {
                dynamics::update_posterior(
                    &mut ts.thompson,
                    raw_chain_fee as f64,
                    adjusted_revenue_rate,
                    hours_elapsed,
                    &time_bucket,
                    prev_congestion_active,
                    now,
                );
                dynamics::update_contextual(
                    &mut ts.thompson,
                    &context_key,
                    raw_chain_fee as f64,
                    adjusted_revenue_rate,
                    &time_bucket,
                    now,
                );
                true
            } else {
                false
            }
        };

        // Sleep-mode entry (py 6314-6352).
        if ts.last_update > 0 && rate_change_ratio < STABILITY_THRESHOLD {
            ts.stable_cycles += 1;
            let zero_rev_exploring = current_revenue_rate <= 0.0 && current_fee_ppm > floor_ppm;
            if ts.stable_cycles >= STABLE_CYCLES_REQUIRED && !zero_rev_exploring {
                record_window(ts, now);
                let sleep_duration_seconds = cfg.fee_interval * SLEEP_CYCLES;
                ts.is_sleeping = true;
                ts.sleep_until = now + sleep_duration_seconds;
                ts.last_revenue_rate = current_revenue_rate;
                ts.last_fee_ppm = current_fee_ppm;
                ts.last_volume_sats = volume_since_sats;
                ts.last_update = now;
                cycle.is_sleeping = true;
                cycle.sleep_until = ts.sleep_until;
                cycle.stable_cycles = ts.stable_cycles;
                cycle.last_update = now;
                cycle.last_revenue_rate = current_revenue_rate;
                cycle.last_fee_ppm = current_fee_ppm;
                trace.set("disposition", OValue::str("sleep_entry"));
                return AdjustResult {
                    adjustment: None,
                    trace: trace.finish(),
                    governed: governed_trace,
                };
            }
        } else if rate_change_ratio >= STABILITY_THRESHOLD {
            ts.stable_cycles = 0;
        }

        // Update posterior + contextual (py 6357-6363).
        record_window(ts, now);

        // DTS discount before sampling (py 6365-6373) — frozen order:
        // update → discount → sample.
        let discount_gamma = if sparse_data_conservative {
            profile.dts_sparse_discount_gamma
        } else {
            profile.dts_discount_gamma
        };
        recompute::apply_dts_discount(&mut ts.thompson, discount_gamma);

        // Vegas-DTS interaction (py 6375-6382).
        if vegas_multiplier > 1.2 {
            dynamics::apply_vegas_adjustment(
                &mut ts.thompson,
                vegas_multiplier,
                base_floor_ppm as f64,
                now,
            );
            ts.last_vegas_multiplier = vegas_multiplier;
        }

        // Sample fee (py 6384-6400).
        if let Some((_k, ctxp)) = ts
            .thompson
            .contextual_posteriors
            .iter()
            .find(|(k, _)| k == &context_key)
        {
            context_observation_count = ctxp.count;
        }
        let ctx_exists = ts
            .thompson
            .contextual_posteriors
            .iter()
            .any(|(k, _)| k == &context_key);
        let dts_fee = sampling::sample_fee_contextual(
            &mut ts.thompson,
            &context_key,
            floor_ppm,
            ceiling_ppm,
            None,
            deps.rng,
            now,
        );
        contextual_sample_used = ctx_exists && context_observation_count >= MIN_OBSERVATIONS;
        ts.last_fee_profile = fee_profile_name.to_string();
        ts.last_context_key = context_key.clone();
        ts.last_time_bucket = time_bucket.clone();
        ts.last_corridor_role = corridor_role.clone();
        ts.last_contextual_sample_used = contextual_sample_used;

        // PID multiplier (py 6402-6416).
        let pid_multiplier =
            pid::calculate_multiplier(&mut ts.pid, outbound_ratio, capacity, flow_state, now);
        raw_dts_target_ppm = Some(dts_fee);
        let mut post_pid = (dts_fee as f64 * pid_multiplier) as i64;

        // Drain multiplier (py 6417-6457).
        effective_discount_max = ctx
            .node_drain_bias_effective_cap
            .unwrap_or(cfg.drain_fee_discount_max);
        drain_multiplier = drain::drain_fee_multiplier(
            outbound_ratio,
            forward_count,
            cfg.high_liquidity_threshold,
            effective_discount_max,
        );
        if drain_multiplier != 1.0 {
            post_pid = (post_pid as f64 * drain_multiplier) as i64;
        }

        // Neighbor market context (py 6458-6676).
        let gossip_rows = evidence.gossip_channels(peer_id);
        let our_id = evidence.our_node_id();
        let active_channels: Vec<GossipChannel> = gossip_rows
            .iter()
            .filter(|r| r.active)
            .map(|r| r.to_gossip_channel(peer_id))
            .collect();
        let neighbor_median = market::neighbor_fee_median(&active_channels, &our_id, now);
        let neighbor_market_usable = matches!(neighbor_median, Some(m) if m > floor_ppm);

        let median_pull_mode = cfg.market_fee_mode.as_str();
        let explore_threshold = rails::exploration_std_threshold(current_fee_ppm);
        let median_pull_exploring = ts.thompson.posterior_std > explore_threshold;
        if neighbor_market_usable
            && matches!(median_pull_mode, "undercut" | "match" | "competition_aware")
            && !median_pull_exploring
        {
            let median = neighbor_median.unwrap_or(0);
            if post_pid > median * 2 {
                post_pid = (post_pid as f64 * 0.8 + median as f64 * 0.2) as i64;
            }
        }

        // Sparse-channel neighbor-median nudge (py 6500-6511).
        if neighbor_market_usable && sparse_data_conservative {
            let median = neighbor_median.unwrap_or(0);
            if ts.thompson.posterior_std > rails::exploration_std_threshold(current_fee_ppm) {
                dynamics::record_posterior_nudge(&mut ts.thompson, median as f64, 0.15, now);
            }
        }

        // Market-fee policy modes (py 6513-6676).
        if neighbor_market_usable {
            let median = neighbor_median.unwrap_or(0);
            let mode = cfg.market_fee_mode.as_str();
            let undercut_pct =
                competitive_undercut_pct(&gossip_rows, &our_id, neighbor_median, mode == "premium");

            if mode == "premium" {
                let mut target = Some((median as f64 * (1.0 + undercut_pct)) as i64);
                if let Some(t) = target {
                    if t <= floor_ppm {
                        target = None;
                    } else {
                        target = Some(cfg.max_fee_ppm.min(t));
                    }
                }
                if let Some(t) = target {
                    if post_pid < t {
                        post_pid = t;
                    }
                }
            } else if mode == "match" {
                let target = cfg.max_fee_ppm.min(median);
                if (post_pid - target).abs() > 0 {
                    post_pid = target;
                }
            } else if mode == "competition_aware" {
                let preserve_threshold =
                    market::neighbor_fee_percentile(&active_channels, &our_id, 0.25, now);
                let undercut_target = (median as f64 * (1.0 - undercut_pct)) as i64;
                let exploring =
                    ts.thompson.posterior_std > rails::exploration_std_threshold(current_fee_ppm);
                if outbound_ratio < UNDERCUT_MIN_OUTBOUND_RATIO {
                    // M5: depleted — skip.
                } else if undercut_target <= floor_ppm {
                    // ignored
                } else if matches!(preserve_threshold, Some(p) if post_pid < p) {
                    // preserve DTS target
                } else if exploring {
                    // Phase B.3: exploring — preserve
                } else if post_pid > undercut_target {
                    post_pid = undercut_target;
                }
            } else {
                // undercut (default / back-compat, py 6623-6676)
                let undercut_target = (median as f64 * (1.0 - undercut_pct)) as i64;
                let exploring =
                    ts.thompson.posterior_std > rails::exploration_std_threshold(current_fee_ppm);
                if outbound_ratio < UNDERCUT_MIN_OUTBOUND_RATIO {
                    // M5: depleted — skip (nudge below skipped too)
                } else if undercut_target <= floor_ppm {
                    // ignored
                } else if exploring {
                    // exploring — preserve
                } else if post_pid > undercut_target {
                    post_pid = undercut_target;
                }

                // Posterior bias (undercut mode only, py 6665-6676).
                if undercut_target > floor_ppm
                    && outbound_ratio >= UNDERCUT_MIN_OUTBOUND_RATIO
                    && sparse_data_conservative
                    && ts.thompson.posterior_mean > undercut_target as f64
                    && ts.thompson.posterior_std >= 50.0
                {
                    dynamics::record_posterior_nudge(
                        &mut ts.thompson,
                        undercut_target as f64,
                        0.10,
                        now,
                    );
                }
            }
        }

        // Rebalance cost nudge (py 6678-6705).
        if rebalance_cost_ppm > 0 && post_pid < rebalance_cost_ppm {
            let nudge_strength = if current_revenue_rate >= 10.0 {
                0.30
            } else if current_revenue_rate >= 1.0 {
                0.15 + 0.15 * (current_revenue_rate / 10.0)
            } else {
                0.05
            };
            post_pid = (post_pid as f64 * (1.0 - nudge_strength)
                + rebalance_cost_ppm as f64 * nudge_strength) as i64;
        }

        // Supported-fee ceiling + upward-probe stretch (py 6713-6763).
        let mut supported_cap =
            dynamics::supported_fee_ceiling(&ts.thompson, now, Some(floor_ppm as f64));
        if let Some(cap) = supported_cap {
            if (post_pid as f64) > cap {
                if let Some(probe_cap) = dynamics::maybe_upward_probe_cap(&ts.thompson, now, cap) {
                    if probe_cap > cap {
                        // L1: budget consumed only after the applied fee
                        // actually crosses the pre-stretch cap (py 7224).
                        upward_probe_pre_cap_ppm = Some(cap as i64);
                        supported_cap = Some(probe_cap);
                    }
                }
            }
        }
        if let Some(cap) = supported_cap {
            supported_cap_ppm = Some(1.max(cap as i64));
            if (post_pid as f64) > cap {
                post_pid = supported_cap_ppm.unwrap_or(post_pid);
            }
        }

        post_pid_target_ppm = Some(post_pid);
        let bounded = floor_ppm.max(ceiling_ppm.min(post_pid));
        bounded_target_ppm = Some(bounded);
        if bounded != post_pid {
            bound_reason = if post_pid < floor_ppm {
                "floor"
            } else {
                "ceiling"
            };
        }

        let mut blend_posterior_std = ts.thompson.posterior_std;
        if observation_count < MIN_OBSERVATIONS {
            blend_posterior_std = blend_posterior_std.max(200.0);
        }

        // Pending-target blend anchor (py 6773-6803, P2 fix).
        let mut blend_anchor_ppm = current_fee_ppm;
        let pending_target_ppm = cycle.pending_target_ppm;
        if pending_target_ppm > 0 {
            let gate_ref_ppm = cycle.last_broadcast_fee_ppm;
            let back_in_band = gate_ref_ppm > 0
                && ((bounded - gate_ref_ppm).abs() as f64)
                    <= gate_ref_ppm as f64 * rails::GOSSIP_GATE_SUPPRESSION_RATIO;
            let anchor_candidate = floor_ppm.max(ceiling_ppm.min(pending_target_ppm));
            let anchor_on_path = current_fee_ppm.min(bounded) <= anchor_candidate
                && anchor_candidate <= current_fee_ppm.max(bounded);
            if back_in_band || !anchor_on_path {
                cycle.pending_target_ppm = 0;
            } else {
                blend_anchor_ppm = anchor_candidate;
            }
        }

        let (blended, blend_info) = rails::blend_fee_target(
            blend_anchor_ppm,
            bounded,
            woke_from_sleep,
            sparse_data_conservative,
            blend_posterior_std,
            fee_profile_name,
            profile,
        );
        let mut blended_val = blended;
        target_blend_ratio = blend_info.blend_ratio;
        zero_revenue_streak = Some(ts.thompson.zero_revenue_streak);
        let earning_anchor_ppm = recompute::earning_region_fee(&ts.thompson, now);
        let cycle_hours = {
            let interval = if cfg.fee_interval != 0 {
                cfg.fee_interval
            } else {
                1800
            };
            (interval as f64).max(60.0) / 3600.0
        };
        let (guard_streak, downshift_streak) =
            rails::zero_flow_streak_thresholds(ts.thompson.meaningful_gap_ema_hours, cycle_hours);
        let rate_is_meaningful = Some(dynamics::is_meaningful_rate(
            &ts.thompson,
            adjusted_revenue_rate,
            now,
        ));
        let (guarded, guard_reason) =
            rails::apply_zero_flow_ratchet_guard(&rails::ZeroFlowInputs {
                current_fee: current_fee_ppm,
                target_fee: blended_val,
                min_fee: floor_ppm,
                zero_revenue_streak: ts.thompson.zero_revenue_streak,
                forwards_since_update: forward_count,
                revenue_rate: adjusted_revenue_rate,
                supported_fee_ceiling: supported_cap_ppm.map(|v| v as f64),
                earning_anchor_ppm,
                guard_streak: Some(guard_streak),
                downshift_streak: Some(downshift_streak),
                rate_is_meaningful,
            });
        blended_val = guarded;
        zero_flow_guard_reason = guard_reason;
        if guard_reason.is_some() {
            zero_flow_guard_target_ppm = Some(blended_val);
        }
        blended_target_ppm = Some(blended_val);

        // ADR stage 2 rate_limit (py 6863-6872).
        let (applied, damping) =
            rails::apply_damped_fee_target(current_fee_ppm, blended_val, woke_from_sleep, profile);
        new_fee_ppm = applied;
        applied_target_ppm = Some(new_fee_ppm);
        delta_cap_reason = damping.cap_reason;
        delta_cap_ppm = damping.max_delta_ppm;
        delta_cap_applied = damping.cap_applied;

        let zero_flow_tag = match zero_flow_guard_reason {
            Some(r) => format!(", guard={r}"),
            None => String::new(),
        };
        decision_reason = format!(
            "dts_pid (dts={dts_fee}, pid={pid_multiplier:.2}, flow={flow_state}{zero_flow_tag})"
        );

        ts.last_volume_sats = volume_since_sats;
        target_found = true;

        new_direction = (new_fee_ppm - current_fee_ppm).signum();
        step_ppm = (new_fee_ppm - current_fee_ppm).abs();
        original_step_ppm = step_ppm;

        // py 6891-6899: update the loaded cycle state.
        cycle.last_revenue_rate = current_revenue_rate;
        cycle.last_fee_ppm = current_fee_ppm;
        cycle.trend_direction = new_direction;
        cycle.step_ppm = step_ppm;
        cycle.forward_count_since_update = forward_count;
        cycle.last_volume_sats = volume_since_sats;
    }

    // =====================================================================
    // Dynamic HTLC policy targets (py 6901-6943).
    // =====================================================================
    let htlcmax_msat = admission::compute_htlcmax_msat(
        &HtlcmaxCfg {
            enable_dynamic_htlcmax: cfg.enable_dynamic_htlcmax.clone(),
            htlcmax_source_pct: cfg.htlcmax_source_pct,
            htlcmax_sink_pct: cfg.htlcmax_sink_pct,
            htlcmax_balanced_pct: cfg.htlcmax_balanced_pct,
        },
        info.capacity_sats,
        info.spendable_msat,
        flow_state,
    );
    let htlcmin_msat: Option<i64> = None;
    let current_base_fee_msat = info.fee_base_msat;
    let target_base_fee_msat = cfg.base_fee_msat; // _resolve_base_fee_msat (py 3088-3108)
    let base_fee_policy_change = current_base_fee_msat != target_base_fee_msat;
    let current_htlcmax_msat = info.htlc_maximum_msat;
    let htlcmin_policy_change = htlcmin_msat.is_some();
    let htlcmax_policy_change = matches!(
        htlcmax_msat,
        Some(new_msat) if admission::delta_exceeds_deadband(new_msat, current_htlcmax_msat)
    );
    let channel_policy_change =
        base_fee_policy_change || htlcmin_policy_change || htlcmax_policy_change;
    if let Some(h) = htlcmax_msat {
        trace.set("htlcmax_msat", OValue::Int(h));
    }

    // =====================================================================
    // Alpha guard (py 6945-7012).
    // =====================================================================
    let raw_zero_fee_recovery = raw_chain_fee <= 0 && new_fee_ppm > 0 && !is_under_exploration;
    let fee_change = (new_fee_ppm - current_fee_ppm).abs();
    let min_change = if current_fee_ppm < 100 {
        1
    } else {
        5.max((current_fee_ppm * 3 + 99).div_euclid(100))
    };

    if fee_change < min_change && !is_congested && !channel_policy_change && !raw_zero_fee_recovery
    {
        cycle.pending_target_ppm = if new_fee_ppm != current_fee_ppm {
            new_fee_ppm
        } else {
            0
        };

        // L3: converged channels must still honor gossip refresh.
        if should_force_gossip_refresh(cycle, evidence, channel_id, now) {
            if let Some(res) = create_gossip_refresh_adjustment(
                cycle,
                ts,
                cfg,
                deps.governed,
                channel_id,
                peer_id,
                evidence,
                current_fee_ppm,
                now,
                &mut governed_trace,
            ) {
                trace.set("disposition", OValue::str("gossip_refresh"));
                return AdjustResult {
                    adjustment: Some(res),
                    trace: trace.finish(),
                    governed: governed_trace,
                };
            }
        }

        cycle.last_revenue_rate = current_revenue_rate;
        cycle.last_fee_ppm = current_fee_ppm;
        cycle.last_update = now;
        ts.last_revenue_rate = current_revenue_rate;
        ts.last_fee_ppm = current_fee_ppm;
        ts.last_update = now;
        trace.set("disposition", OValue::str("alpha_guard"));
        trace.set("pending_target_ppm", OValue::Int(cycle.pending_target_ppm));
        return AdjustResult {
            adjustment: None,
            trace: trace.finish(),
            governed: governed_trace,
        };
    }

    // =====================================================================
    // Gossip hysteresis — the 5% gate (py 7014-7106), ADR stage 3 deadband.
    // =====================================================================
    let delta_broadcast = (new_fee_ppm - cycle.last_broadcast_fee_ppm).abs();
    let threshold = cycle.last_broadcast_fee_ppm as f64 * rails::GOSSIP_GATE_SUPPRESSION_RATIO;

    let last_state_category = cycle
        .last_state
        .split(" (")
        .next()
        .unwrap_or("")
        .to_string();
    let current_state_category = decision_reason.split(" (").next().unwrap_or("").to_string();
    let legacy_zero_fee_transition = cycle.last_broadcast_fee_ppm <= 0 || raw_chain_fee <= 0;
    let significant_change = (delta_broadcast as f64) > threshold
        || legacy_zero_fee_transition
        || (target_found && last_state_category != current_state_category)
        || (!target_found && cycle.last_state == "CONGESTION");

    if !significant_change && !channel_policy_change {
        cycle.pending_target_ppm = if new_fee_ppm != current_fee_ppm {
            new_fee_ppm
        } else {
            0
        };

        if should_force_gossip_refresh(cycle, evidence, channel_id, now) {
            if let Some(res) = create_gossip_refresh_adjustment(
                cycle,
                ts,
                cfg,
                deps.governed,
                channel_id,
                peer_id,
                evidence,
                current_fee_ppm,
                now,
                &mut governed_trace,
            ) {
                trace.set("disposition", OValue::str("gossip_refresh"));
                return AdjustResult {
                    adjustment: Some(res),
                    trace: trace.finish(),
                    governed: governed_trace,
                };
            }
            // FC-I16: no safe nudge — fall through to the hysteresis reset.
        }

        cycle.last_fee_ppm = new_fee_ppm;
        cycle.last_revenue_rate = current_revenue_rate;
        cycle.trend_direction = new_direction;
        cycle.step_ppm = step_ppm;
        cycle.last_update = now;
        ts.last_fee_ppm = new_fee_ppm;
        ts.last_revenue_rate = current_revenue_rate;
        ts.last_state = decision_reason.clone();
        ts.last_update = now;
        trace.set("disposition", OValue::str("gossip_suppressed"));
        trace.set("pending_target_ppm", OValue::Int(cycle.pending_target_ppm));
        return AdjustResult {
            adjustment: None,
            trace: trace.finish(),
            governed: governed_trace,
        };
    }

    // =====================================================================
    // Reason string (py 7108-7157) — THE WIRE CONTRACT for T11.
    // =====================================================================
    let volatility_note = if volatility_reset {
        " [VOLATILITY_RESET]"
    } else {
        ""
    };
    let applied_delta = new_fee_ppm - current_fee_ppm;
    let applied_dir = if applied_delta > 0 {
        "up"
    } else if applied_delta < 0 {
        "down"
    } else {
        "flat"
    };
    let rebal_cost_tag = if rebalance_cost_ppm > 0 {
        format!(", rebal_cost_nudge:{rebalance_cost_ppm}ppm")
    } else {
        String::new()
    };
    let target_summary = if let Some(raw_dts) = raw_dts_target_ppm {
        format!(
            "targets=dts:{raw_dts}, post_pid:{}, bounded:{}, blended:{}, applied:{}, \
             blend:{target_blend_ratio:.2}, bound:{bound_reason}, \
             cap:{delta_cap_reason}({delta_cap_ppm}ppm), wake:{wake_reason}, \
             sparse:{}, exploration:{exploration_mode}, zero_flow_guard:{}{rebal_cost_tag}",
            fmt_opt_int(post_pid_target_ppm),
            fmt_opt_int(bounded_target_ppm),
            fmt_opt_int(blended_target_ppm),
            fmt_opt_int(applied_target_ppm),
            py_bool_str(sparse_data_conservative),
            zero_flow_guard_reason.unwrap_or("none"),
        )
    } else {
        format!(
            "targets=n/a, blend:{target_blend_ratio:.2}, wake:{wake_reason}, sparse:{}, \
             exploration:{exploration_mode}{rebal_cost_tag}",
            py_bool_str(sparse_data_conservative),
        )
    };

    let common_reason_suffix = format!(
        "rate={current_revenue_rate:.2}sats/hr ({decision_reason}){volatility_note}, \
         {target_summary}, applied={applied_dir}({applied_delta:+}ppm), \
         state={flow_state}, liquidity={bucket} ({:.0}%), {marginal_roi_info}",
        outbound_ratio * 100.0,
    );
    let reason = if decision_reason == "CONGESTION" {
        format!("CONGESTION: bounded emergency override active, {common_reason_suffix}")
    } else if matches!(
        decision_reason.as_str(),
        "LOW_FEE_EXPLORATION"
            | "LOW_FEE_EXPLORATION_SUCCESS"
            | "ZERO_FEE_PROBE"
            | "ZERO_FEE_PROBE_SUCCESS"
    ) {
        let exploration_label = if matches!(
            decision_reason.as_str(),
            "LOW_FEE_EXPLORATION_SUCCESS" | "ZERO_FEE_PROBE_SUCCESS"
        ) {
            "holding safe low-fee after exploration traffic"
        } else {
            "bounded low-fee discovery mode"
        };
        format!("EXPLORATION: {exploration_label}, {common_reason_suffix}")
    } else {
        let dts_info = format!(
            "posterior_mean={:.0}, posterior_std={:.0}",
            ts.thompson.posterior_mean, ts.thompson.posterior_std
        );
        format!("DTS+PID: {common_reason_suffix}, {dts_info}")
    };

    // Idempotency guard (py 7159-7186).
    if new_fee_ppm == raw_chain_fee && !channel_policy_change {
        cycle.pending_target_ppm = 0;
        cycle.last_revenue_rate = current_revenue_rate;
        cycle.last_fee_ppm = raw_chain_fee;
        cycle.last_broadcast_fee_ppm = new_fee_ppm;
        cycle.last_state = decision_reason.clone();
        cycle.trend_direction = new_direction;
        cycle.step_ppm = step_ppm;
        cycle.last_update = now;
        ts.last_revenue_rate = current_revenue_rate;
        ts.last_fee_ppm = raw_chain_fee;
        ts.last_broadcast_fee_ppm = new_fee_ppm;
        ts.last_state = decision_reason.clone();
        ts.last_update = now;
        trace.set("disposition", OValue::str("idempotent"));
        return AdjustResult {
            adjustment: None,
            trace: trace.finish(),
            governed: governed_trace,
        };
    }

    // reason_code (py 7188-7200).
    let fee_reason_code = match decision_reason.as_str() {
        "LOW_FEE_EXPLORATION" => FeeReasonCode::LowFeeExploration,
        "LOW_FEE_EXPLORATION_SUCCESS" => FeeReasonCode::LowFeeExplorationSuccess,
        "ZERO_FEE_PROBE" => FeeReasonCode::ZeroFeeProbe,
        "ZERO_FEE_PROBE_SUCCESS" => FeeReasonCode::ZeroFeeProbeSuccess,
        _ if is_congested => FeeReasonCode::Congestion,
        _ => FeeReasonCode::DtsPidSample,
    };

    // =====================================================================
    // Decision emit — dry-run set_channel_fee (py 7202-7215 → 7203 becomes
    // execution::decide_set_channel_fee; NO RPC side effects).
    // =====================================================================
    let decision = decide_set_channel_fee(
        &SetFeeRequest {
            channel_id: channel_id.to_string(),
            fee_ppm: new_fee_ppm,
            enforce_limits: true,
            effective_min_fee_ppm: Some(effective_min_fee_ppm),
            htlcmax_msat,
            base_fee_msat: target_base_fee_msat,
        },
        cfg,
        None,
    );
    if let Some(log) = &decision.clamp_log {
        trace.set("clamp_log", OValue::str(log.clone()));
    }
    let mut success = decision.success;
    if success && cfg.econ_governor_fees_enabled {
        if let Some(gdeps) = deps.governed {
            let (ok, code, gtrace) = execution::governed_authorize_fee_broadcast(
                gdeps,
                channel_id,
                decision.clamped_fee_ppm,
                Some(raw_chain_fee),
                &reason,
                Some(fee_reason_code.as_str()),
                now,
            );
            governed_trace = gtrace;
            if !ok {
                success = false;
                trace.set(
                    "governor_block",
                    OValue::str(format!("governor_block: {code}")),
                );
            }
        }
    }

    if success {
        // Read back the (clamped) applied fee (py 7217-7219).
        let new_fee_ppm = decision.clamped_fee_ppm;

        // L1: consume the upward-probe budget only when the broadcast fee
        // actually crossed the pre-stretch cap (py 7221-7233).
        if let Some(pre_cap) = upward_probe_pre_cap_ppm {
            if new_fee_ppm > pre_cap {
                dynamics::consume_upward_probe(&mut ts.thompson, now);
            }
        }

        cycle.pending_target_ppm = 0;
        cycle.last_revenue_rate = current_revenue_rate;
        cycle.last_fee_ppm = current_fee_ppm;
        cycle.last_broadcast_fee_ppm = new_fee_ppm;
        cycle.set_last_broadcast_at(now);
        cycle.last_state = decision_reason.clone();
        if new_direction != 0 && new_direction == prev_trend_direction {
            cycle.consecutive_same_direction += 1;
        } else {
            cycle.consecutive_same_direction = if new_direction != 0 { 1 } else { 0 };
        }
        cycle.trend_direction = new_direction;
        cycle.step_ppm = step_ppm;
        cycle.last_update = now;

        ts.last_revenue_rate = current_revenue_rate;
        ts.last_fee_ppm = current_fee_ppm;
        ts.last_broadcast_fee_ppm = new_fee_ppm;
        ts.set_last_broadcast_at(now);
        ts.last_state = decision_reason.clone();
        ts.last_update = now;

        let algorithm_values = OValue::obj(vec![
            (
                "current_revenue_rate".to_string(),
                OValue::Float(current_revenue_rate),
            ),
            (
                "previous_revenue_rate".to_string(),
                OValue::Float(previous_rate),
            ),
            ("rate_change".to_string(), OValue::Float(rate_change)),
            (
                "volume_since_sats".to_string(),
                OValue::Int(volume_since_sats),
            ),
            ("hours_elapsed".to_string(), OValue::Float(hours_elapsed)),
            ("direction".to_string(), OValue::Int(new_direction)),
            ("step_ppm".to_string(), OValue::Int(step_ppm)),
            (
                "consecutive_same_direction".to_string(),
                OValue::Int(cycle.consecutive_same_direction),
            ),
            (
                "volatility_reset".to_string(),
                OValue::Bool(volatility_reset),
            ),
            (
                "raw_dts_target_ppm".to_string(),
                opt_int(raw_dts_target_ppm),
            ),
            (
                "post_pid_target_ppm".to_string(),
                opt_int(post_pid_target_ppm),
            ),
            (
                "zero_flow_guard_reason".to_string(),
                match zero_flow_guard_reason {
                    Some(r) => OValue::str(r),
                    None => OValue::Null,
                },
            ),
            (
                "zero_flow_guard_target_ppm".to_string(),
                opt_int(zero_flow_guard_target_ppm),
            ),
            (
                "zero_revenue_streak".to_string(),
                opt_int(zero_revenue_streak),
            ),
            (
                "supported_fee_ceiling_ppm".to_string(),
                opt_int(supported_cap_ppm),
            ),
            (
                "bounded_target_ppm".to_string(),
                opt_int(bounded_target_ppm),
            ),
            (
                "blended_target_ppm".to_string(),
                opt_int(blended_target_ppm.or(bounded_target_ppm)),
            ),
            (
                "applied_target_ppm".to_string(),
                OValue::Int(applied_target_ppm.unwrap_or(new_fee_ppm)),
            ),
            (
                "target_blend_ratio".to_string(),
                OValue::Float(target_blend_ratio),
            ),
            ("bound_reason".to_string(), OValue::str(bound_reason)),
            (
                "delta_cap_reason".to_string(),
                OValue::str(delta_cap_reason),
            ),
            ("delta_cap_ppm".to_string(), OValue::Int(delta_cap_ppm)),
            (
                "delta_cap_applied".to_string(),
                OValue::Bool(delta_cap_applied),
            ),
            (
                "base_fee_policy_change".to_string(),
                OValue::Bool(base_fee_policy_change),
            ),
            (
                "current_base_fee_msat".to_string(),
                OValue::Int(current_base_fee_msat),
            ),
            (
                "target_base_fee_msat".to_string(),
                OValue::Int(target_base_fee_msat),
            ),
            (
                "htlcmax_policy_change".to_string(),
                OValue::Bool(htlcmax_policy_change),
            ),
            (
                "wake_damping_applied".to_string(),
                OValue::Bool(woke_from_sleep),
            ),
            ("wake_reason".to_string(), OValue::str(wake_reason.clone())),
            (
                "sparse_data_conservative".to_string(),
                OValue::Bool(sparse_data_conservative),
            ),
            (
                "exploration_mode".to_string(),
                OValue::str(exploration_mode),
            ),
            (
                "rebalance_cost_floor_ppm".to_string(),
                OValue::Int(rebalance_floor_ppm.unwrap_or(0)),
            ),
            (
                "rebalance_cost_nudge_ppm".to_string(),
                OValue::Int(rebalance_cost_ppm),
            ),
            ("floor_ppm".to_string(), OValue::Int(floor_ppm)),
            (
                "effective_min_fee_ppm".to_string(),
                OValue::Int(effective_min_fee_ppm),
            ),
            (
                "fee_profile".to_string(),
                OValue::str(fee_profile_name.to_string()),
            ),
            (
                "fee_profile_settings".to_string(),
                profile_settings_to_dict(profile),
            ),
            ("context_key".to_string(), OValue::str(context_key.clone())),
            ("time_bucket".to_string(), OValue::str(time_bucket.clone())),
            (
                "corridor_role".to_string(),
                OValue::str(corridor_role.clone()),
            ),
            (
                "context_observation_count".to_string(),
                OValue::Int(context_observation_count),
            ),
            (
                "contextual_sample_used".to_string(),
                OValue::Bool(contextual_sample_used),
            ),
            (
                "drain_multiplier".to_string(),
                OValue::Float(drain_multiplier),
            ),
            (
                "drain_discount_max_effective".to_string(),
                OValue::Float(effective_discount_max),
            ),
            (
                "node_receivable_ratio".to_string(),
                opt_float(ctx.node_receivable_ratio),
            ),
            (
                "node_drain_pressure".to_string(),
                opt_float(ctx.node_drain_pressure),
            ),
        ]);

        trace.set("disposition", OValue::str("broadcast"));
        trace.set("floor_ppm", OValue::Int(floor_ppm));
        trace.set("ceiling_ppm", OValue::Int(ceiling_ppm));
        trace.set("vegas_multiplier", OValue::Float(vegas_multiplier));
        let _ = original_step_ppm; // logged only (py 7268); kept for parity clarity

        return AdjustResult {
            adjustment: Some(FeeAdjustmentRec {
                channel_id: channel_id.to_string(),
                peer_id: peer_id.to_string(),
                old_fee_ppm: current_fee_ppm,
                new_fee_ppm,
                reason,
                algorithm_values,
                reason_code: fee_reason_code.as_str().to_string(),
            }),
            trace: trace.finish(),
            governed: governed_trace,
        };
    }

    // RPC failed / governor blocked (py 7333-7354): reset the observation
    // timer so the already-consumed window isn't double-counted.
    cycle.last_revenue_rate = current_revenue_rate;
    cycle.last_fee_ppm = current_fee_ppm;
    cycle.last_update = now;
    ts.last_revenue_rate = current_revenue_rate;
    ts.last_fee_ppm = current_fee_ppm;
    ts.last_update = now;
    trace.set("disposition", OValue::str("broadcast_refused"));
    AdjustResult {
        adjustment: None,
        trace: trace.finish(),
        governed: governed_trace,
    }
}

fn opt_int(v: Option<i64>) -> OValue {
    match v {
        Some(x) => OValue::Int(x),
        None => OValue::Null,
    }
}

fn opt_float(v: Option<f64>) -> OValue {
    match v {
        Some(x) => OValue::Float(x),
        None => OValue::Null,
    }
}

fn fmt_opt_int(v: Option<i64>) -> String {
    match v {
        Some(x) => x.to_string(),
        None => "None".to_string(),
    }
}

/// `FeeProfileSettings.to_dict` (py 2454-2467), key order frozen.
fn profile_settings_to_dict(p: &FeeProfileSettings) -> OValue {
    OValue::obj(vec![
        (
            "min_observation_hours".to_string(),
            OValue::Float(p.min_observation_hours),
        ),
        (
            "min_forwards_for_signal".to_string(),
            OValue::Int(p.min_forwards_for_signal),
        ),
        (
            "dts_discount_gamma".to_string(),
            OValue::Float(p.dts_discount_gamma),
        ),
        (
            "dts_sparse_discount_gamma".to_string(),
            OValue::Float(p.dts_sparse_discount_gamma),
        ),
        (
            "normal_target_blend_ratio".to_string(),
            OValue::Float(p.normal_target_blend_ratio),
        ),
        (
            "wake_target_blend_ratio".to_string(),
            OValue::Float(p.wake_target_blend_ratio),
        ),
        (
            "sparse_target_blend_ratio".to_string(),
            OValue::Float(p.sparse_target_blend_ratio),
        ),
        (
            "normal_cycle_max_delta_ratio".to_string(),
            OValue::Float(p.normal_cycle_max_delta_ratio),
        ),
        (
            "normal_cycle_min_delta_ppm".to_string(),
            OValue::Int(p.normal_cycle_min_delta_ppm),
        ),
        (
            "wake_cycle_max_delta_ratio".to_string(),
            OValue::Float(p.wake_cycle_max_delta_ratio),
        ),
        (
            "wake_cycle_min_delta_ppm".to_string(),
            OValue::Int(p.wake_cycle_min_delta_ppm),
        ),
    ])
}

// ---------------------------------------------------------------------------
// Gossip refresh (py 4923-5082)
// ---------------------------------------------------------------------------

/// `_should_force_gossip_refresh` (py 4923-4973).
fn should_force_gossip_refresh(
    cycle: &ChannelCycleState,
    evidence: &dyn FeeEvidence,
    channel_id: &str,
    now: i64,
) -> bool {
    if !ENABLE_GOSSIP_REFRESH {
        return false;
    }
    let last_broadcast_at = cycle.last_broadcast_at();
    if last_broadcast_at > 0 {
        let hours_since_broadcast = (now - last_broadcast_at) as f64 / 3600.0;
        if hours_since_broadcast < GOSSIP_REFRESH_MIN_BROADCAST_AGE_HOURS {
            return false;
        }
    } else {
        return false; // never broadcast — not via the refresh mechanism
    }
    if let Some(last_forward_ts) = evidence.last_forward_time(channel_id) {
        if last_forward_ts > 0 {
            let hours_since_forward = (now - last_forward_ts) as f64 / 3600.0;
            if hours_since_forward < GOSSIP_REFRESH_MIN_IDLE_HOURS {
                return false;
            }
        }
    }
    if cycle.last_gossip_refresh() > 0 {
        let hours_since_refresh = (now - cycle.last_gossip_refresh()) as f64 / 3600.0;
        if hours_since_refresh < GOSSIP_REFRESH_COOLDOWN_HOURS {
            return false;
        }
    }
    true
}

/// `_create_gossip_refresh_adjustment` (py 4975-5082): the +1 ppm nudge as
/// a dry-run decision, with `set_channel_fee`'s should_sync_state
/// bookkeeping (reason_code `gossip_refresh` is in the sync set, py
/// 7853-7892) plus the helper's own timer updates.
#[allow(clippy::too_many_arguments)]
fn create_gossip_refresh_adjustment(
    cycle: &mut ChannelCycleState,
    ts: &mut ChannelFeeState,
    cfg: &FeeCfgSnapshot,
    governed: Option<&GovernedDeps<'_>>,
    channel_id: &str,
    peer_id: &str,
    evidence: &dyn FeeEvidence,
    current_fee_ppm: i64,
    now: i64,
    governed_trace: &mut Option<GovernedTrace>,
) -> Option<FeeAdjustmentRec> {
    // Pick a nudge that survives clamping (py 5002-5014).
    let mut nudge_fee: Option<i64> = None;
    for cand in [
        current_fee_ppm + GOSSIP_REFRESH_NUDGE_PPM,
        current_fee_ppm - GOSSIP_REFRESH_NUDGE_PPM,
    ] {
        let clamped = cfg.min_fee_ppm.max(cfg.max_fee_ppm.min(cand));
        if clamped != current_fee_ppm {
            nudge_fee = Some(clamped);
            break;
        }
    }
    let nudge_fee = nudge_fee?;

    let last_broadcast_at = cycle.last_broadcast_at();
    let hours_since_broadcast = if last_broadcast_at > 0 {
        OValue::Float((now - last_broadcast_at) as f64 / 3600.0)
    } else {
        OValue::Int(999)
    };
    let hours_since_forward = match evidence.last_forward_time(channel_id) {
        Some(ts_fwd) if ts_fwd > 0 => OValue::Float((now - ts_fwd) as f64 / 3600.0),
        _ => OValue::Int(999),
    };

    // Execute (dry-run decision; py 5033-5042).
    let decision = decide_set_channel_fee(
        &SetFeeRequest {
            channel_id: channel_id.to_string(),
            fee_ppm: nudge_fee,
            enforce_limits: true,
            effective_min_fee_ppm: None,
            htlcmax_msat: None,
            base_fee_msat: cfg.base_fee_msat,
        },
        cfg,
        None,
    );
    let mut success = decision.success;
    if success && cfg.econ_governor_fees_enabled {
        if let Some(gdeps) = governed {
            let (ok, _code, gtrace) = execution::governed_authorize_fee_broadcast(
                gdeps,
                channel_id,
                decision.clamped_fee_ppm,
                Some(current_fee_ppm),
                "gossip_refresh",
                Some(FeeReasonCode::GossipRefresh.as_str()),
                now,
            );
            *governed_trace = gtrace;
            if !ok {
                success = false;
            }
        }
    }
    if !success {
        return None;
    }
    let nudge_fee = decision.clamped_fee_ppm;

    // set_channel_fee should_sync_state (py 7862-7892) + helper updates
    // (py 5044-5068).
    cycle.is_sleeping = false;
    cycle.sleep_until = 0;
    cycle.stable_cycles = 0;
    cycle.set_last_gossip_refresh(now);
    cycle.last_fee_ppm = nudge_fee;
    cycle.last_broadcast_fee_ppm = nudge_fee;
    cycle.set_last_broadcast_at(now);
    cycle.last_update = now;
    cycle.last_state = FeeReasonCode::GossipRefresh.as_str().to_string();
    ts.is_sleeping = false;
    ts.sleep_until = 0;
    ts.stable_cycles = 0;
    ts.set_last_gossip_refresh(now);
    ts.last_fee_ppm = nudge_fee;
    ts.last_broadcast_fee_ppm = nudge_fee;
    ts.set_last_broadcast_at(now);
    ts.last_update = now;
    ts.last_state = FeeReasonCode::GossipRefresh.as_str().to_string();

    Some(FeeAdjustmentRec {
        channel_id: channel_id.to_string(),
        peer_id: peer_id.to_string(),
        old_fee_ppm: current_fee_ppm,
        new_fee_ppm: nudge_fee,
        reason: "gossip_refresh".to_string(),
        algorithm_values: OValue::obj(vec![
            ("hours_since_broadcast".to_string(), hours_since_broadcast),
            ("hours_since_forward".to_string(), hours_since_forward),
            (
                "nudge_amount".to_string(),
                OValue::Int(GOSSIP_REFRESH_NUDGE_PPM),
            ),
        ]),
        reason_code: FeeReasonCode::GossipRefresh.as_str().to_string(),
    })
}

// ---------------------------------------------------------------------------
// Wake paths (py 4295-4411, 7356-7400)
// ---------------------------------------------------------------------------

/// `wake_all_sleeping_channels` (py 4295-4384) over the in-memory maps
/// (the DB-hydration arm is T9's store; a dry-run cycle holds every live
/// channel's state in memory after its first cycle).
pub fn wake_all_sleeping_channels(
    state: &mut ControllerState,
    profile: &FeeProfileSettings,
    now: i64,
) -> i64 {
    let mut woken = 0i64;
    let backdated = now - (profile.min_observation_hours * 3600.0) as i64 - 1;

    for cycle in state.cycle_states.values_mut() {
        let mut changed = false;
        if cycle.is_sleeping {
            cycle.is_sleeping = false;
            cycle.sleep_until = 0;
            cycle.stable_cycles = 0;
            changed = true;
        }
        if cycle.last_update > backdated {
            cycle.last_update = backdated;
            changed = true;
        }
        if changed {
            woken += 1;
        }
    }
    for ts in state.fee_states.values_mut() {
        let mut changed = false;
        if ts.is_sleeping {
            ts.is_sleeping = false;
            ts.sleep_until = 0;
            ts.stable_cycles = 0;
            changed = true;
        }
        if ts.last_update > backdated {
            ts.last_update = backdated;
            changed = true;
        }
        if changed {
            woken += 1;
        }
    }
    woken
}

/// `_maybe_wake_for_vegas_spike` (py 4386-4411): edge-triggered wake.
pub fn maybe_wake_for_vegas_spike(
    state: &mut ControllerState,
    profile: &FeeProfileSettings,
    now: i64,
) -> bool {
    let intensity = state.vegas.intensity;
    if state.vegas_wake_armed && intensity >= VEGAS_WAKE_INTENSITY_THRESHOLD {
        state.vegas_wake_armed = false;
        wake_all_sleeping_channels(state, profile, now);
        return true;
    }
    if !state.vegas_wake_armed && intensity < VEGAS_WAKE_REARM_INTENSITY {
        state.vegas_wake_armed = true;
    }
    false
}

/// `_handle_policy_change` (py 7356-7400): wake the peer's sleeping
/// channels so the next cycle applies the new policy.
pub fn handle_policy_change(
    state: &mut ControllerState,
    channel_states: &[ChannelStateRow],
    peer_id: &str,
) -> i64 {
    let channel_ids: Vec<&str> = channel_states
        .iter()
        .filter(|s| s.peer_id == peer_id && !s.channel_id.is_empty())
        .map(|s| s.channel_id.as_str())
        .collect();
    let mut woken = 0i64;
    for channel_id in channel_ids {
        if let Some(cycle) = state.cycle_states.get_mut(channel_id) {
            if cycle.is_sleeping {
                cycle.is_sleeping = false;
                cycle.sleep_until = 0;
                cycle.stable_cycles = 0;
                woken += 1;
            }
        }
        if let Some(ts) = state.fee_states.get_mut(channel_id) {
            if ts.is_sleeping {
                ts.is_sleeping = false;
                ts.sleep_until = 0;
                ts.stable_cycles = 0;
            }
        }
    }
    woken
}

// ---------------------------------------------------------------------------
// run_fee_cycle — py adjust_all_fees / _adjust_all_fees_inner (4413-4724)
// ---------------------------------------------------------------------------

/// The full dry-run fee cycle: paused gate, Vegas update + spike wake,
/// node-drain-bias aggregate, the per-channel loop, ONE state flush, and
/// the decision journal. Returns every journaled decision.
pub fn run_fee_cycle(state: &mut ControllerState, deps: &mut CycleDeps<'_>) -> Vec<FeeDecision> {
    let cfg = deps.cfg;
    let now = deps.now;
    let (profile_name, profile) = fee_profile(&cfg.fee_profile);
    let _ = profile_name;
    let mut decisions: Vec<FeeDecision> = Vec::new();
    let cycle_id = format!("fee-dryrun-{now}");

    if cfg.paused {
        state.set_summary("suppressed", "paused", Some("paused"), true);
        return decisions;
    }

    let channel_states = deps.evidence.channel_states();
    if channel_states.is_empty() {
        state.set_summary(
            "hold",
            "no_channel_state_data",
            Some("channel_state_data"),
            false,
        );
        return decisions;
    }

    let channels = deps.evidence.channels_info();
    let chain_costs = deps.evidence.chain_costs();

    // Vegas Reflex (py 4583-4597).
    if cfg.enable_vegas_reflex {
        if let Some(costs) = &chain_costs {
            let current_sat_vb = costs.sat_per_vbyte;
            let ma_sat_vb = deps.evidence.mempool_ma_24h();
            vegas::vegas_update(&mut state.vegas, current_sat_vb, ma_sat_vb, deps.rng, now);
            maybe_wake_for_vegas_spike(state, profile, now);
        }
    }

    // Node-drain-bias aggregate, ONCE per cycle (py 4599-4638).
    let mut node_receivable_ratio_value: Option<f64> = None;
    let mut node_drain_pressure_value: Option<f64> = None;
    let node_drain_bias_effective_cap: Option<f64> = {
        let mut pressure = 0.0;
        if cfg.node_drain_bias_enabled {
            let raw_channels = deps.evidence.node_channels();
            let ratio = drain::compute_node_receivable_ratio(&raw_channels);
            node_receivable_ratio_value = Some(ratio);
            pressure = drain::node_drain_pressure(
                ratio,
                cfg.receivable_ratio_target,
                cfg.receivable_ratio_floor,
            );
            node_drain_pressure_value = Some(pressure);
        }
        Some(drain::effective_drain_discount_max(
            cfg.drain_fee_discount_max,
            cfg.node_drain_bias_enabled,
            cfg.drain_fee_discount_max,
            pressure,
        ))
    };

    // Skip-reason tallies (py 4541-4553).
    let mut skip_reasons: BTreeMap<&'static str, i64> = BTreeMap::new();
    let mut adjustments: Vec<FeeAdjustmentRec> = Vec::new();
    let mut dirty: Vec<String> = Vec::new();

    for row in &channel_states {
        if row.channel_id.is_empty() || row.peer_id.is_empty() {
            continue;
        }
        let info = match channels.get(&row.channel_id) {
            Some(i) => i.clone(),
            None => continue,
        };
        let result = process_channel(
            state,
            deps,
            row,
            &info,
            chain_costs.as_ref(),
            node_drain_bias_effective_cap,
            node_receivable_ratio_value,
            node_drain_pressure_value,
        );
        if !dirty.contains(&row.channel_id) {
            dirty.push(row.channel_id.clone());
        }

        let (decision, adj) = match result.outcome {
            ChannelOutcome::Adjusted(adj) => {
                let d = FeeDecision {
                    channel_id: adj.channel_id.clone(),
                    peer_id: adj.peer_id.clone(),
                    old_fee_ppm: adj.old_fee_ppm,
                    new_fee_ppm: adj.new_fee_ppm,
                    reason: adj.reason.clone(),
                    reason_code: adj.reason_code.clone(),
                    algorithm_values: adj.algorithm_values.clone(),
                    trace: result.trace,
                    would_broadcast: true,
                    governed: result.governed,
                    cycle_id: cycle_id.clone(),
                    at: now,
                };
                (d, Some(*adj))
            }
            ChannelOutcome::Skipped(reason) => {
                *skip_reasons.entry(reason).or_insert(0) += 1;
                let fee = info.fee_proportional_millionths;
                let d = FeeDecision {
                    channel_id: row.channel_id.clone(),
                    peer_id: row.peer_id.clone(),
                    old_fee_ppm: fee,
                    new_fee_ppm: fee,
                    reason: format!("skip: {reason}"),
                    reason_code: skip_reason_code(reason).to_string(),
                    algorithm_values: OValue::Null,
                    trace: result.trace,
                    would_broadcast: false,
                    governed: result.governed,
                    cycle_id: cycle_id.clone(),
                    at: now,
                };
                (d, None)
            }
        };
        decisions.push(decision);
        if let Some(a) = adj {
            adjustments.push(a);
        }
    }

    // ONE batched state flush (py 4644-4661: `_cycle_batch_active` +
    // `_flush_pending_fee_strategy_rows`, last-write-wins by channel).
    if let Some(sink) = deps.state_sink {
        let rows: Vec<(String, ChannelCycleState, ChannelFeeState)> = dirty
            .iter()
            .map(|cid| {
                (
                    cid.clone(),
                    state.cycle_states.get(cid).cloned().unwrap_or_default(),
                    state.fee_states.get(cid).cloned().unwrap_or_default(),
                )
            })
            .collect();
        sink.flush_batch(&rows);
    }

    // Decision summary (py 4671-4722).
    if adjustments.is_empty() && !channel_states.is_empty() {
        let active_skips: Vec<(&str, i64)> = skip_reasons
            .iter()
            .filter(|(_, v)| **v > 0)
            .map(|(k, v)| (*k, *v))
            .collect();
        if !active_skips.is_empty() {
            let dominant_reason = active_skips
                .iter()
                .max_by_key(|(_, v)| *v)
                .map(|(k, _)| *k)
                .unwrap_or("fee_unchanged");
            let suppressed = matches!(
                dominant_reason,
                "policy_passive"
                    | "policy_static"
                    | "temporary_overlay"
                    | "sleeping"
                    | "waiting_time"
                    | "waiting_forwards"
                    | "gossip_hysteresis"
                    | "idempotent"
                    | "error"
            );
            state.set_summary(
                if suppressed { "suppressed" } else { "hold" },
                dominant_reason,
                Some(dominant_reason),
                suppressed,
            );
        } else {
            state.set_summary("hold", "fee_unchanged", Some("fee_unchanged"), false);
        }
    } else if let Some(last) = adjustments.last() {
        let action = if last.new_fee_ppm > last.old_fee_ppm {
            "raise"
        } else if last.new_fee_ppm < last.old_fee_ppm {
            "lower"
        } else {
            "hold"
        };
        let reason = last.reason.clone();
        let code = last.reason_code.clone();
        state.set_summary(action, &reason, Some(&code), false);
    }

    if let Some(journal) = deps.journal {
        // Best-effort: a journal IO failure must not abort the cycle
        // (mirrors Python's log-and-continue posture on bookkeeping).
        let _ = journal.append_all(&decisions);
    }

    decisions
}

/// Journal reason_code for scheduler skips (`skip_*` wire values from
/// `FeeReasonCode` where one exists; the loop-tally key otherwise).
fn skip_reason_code(reason: &str) -> &'static str {
    match reason {
        "sleeping" => FeeReasonCode::SkipSleeping.as_str(),
        "waiting_time" => FeeReasonCode::SkipWaitingTime.as_str(),
        "waiting_forwards" => FeeReasonCode::SkipWaitingForwards.as_str(),
        "fee_unchanged" | "idempotent" => FeeReasonCode::SkipFeeUnchanged.as_str(),
        "policy_passive" => FeeReasonCode::PolicyPassive.as_str(),
        "policy_static" => FeeReasonCode::PolicyStatic.as_str(),
        "gossip_hysteresis" => "skip_gossip_hysteresis",
        "alpha_guard" => "skip_alpha_guard",
        "temporary_overlay" => "skip_temporary_overlay",
        "error" => "skip_error",
        _ => "skip_unknown",
    }
}

// ---------------------------------------------------------------------------
// Direct unit tests: `channel_capacity_rank` / `channel_rebalance_cost_ppm`
// (Important-1 review finding — both were previously exercised by ZERO
// fixtures: no fixture carried an own-source gossip row, so the rank
// always hit its no-data default, and the one cost-history fixture
// (`floor_inversion`) activated the HARD floor, gating the soft nudge off
// at the source. `fixtures/fees/cycle` scenarios 17/18
// (`capacity_rank_own_source` / `rebal_cost_soft_nudge`) now close the
// end-to-end gap; these pin the pure functions directly.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn gossip(
        source: &str,
        active: bool,
        satoshis: Option<i64>,
        amount_msat: Option<i64>,
    ) -> GossipRow {
        GossipRow {
            source: source.to_string(),
            active,
            fee_per_millionth: 100,
            satoshis,
            amount_msat,
            last_update: 0,
            base_fee_msat: None,
        }
    }

    // -- channel_capacity_rank (py `_get_competitive_undercut_pct` 3506-3531) --

    #[test]
    fn capacity_rank_none_when_no_rows() {
        assert_eq!(channel_capacity_rank(&[], "us"), None);
    }

    #[test]
    fn capacity_rank_none_without_our_own_row() {
        let rows = vec![
            gossip("a", true, Some(1_000_000), None),
            gossip("b", true, Some(2_000_000), None),
        ];
        assert_eq!(channel_capacity_rank(&rows, "us"), None);
    }

    #[test]
    fn capacity_rank_none_without_active_competitors() {
        let rows = vec![
            gossip("us", true, Some(1_000_000), None),
            gossip("b", false, Some(2_000_000), None), // inactive: excluded
        ];
        assert_eq!(channel_capacity_rank(&rows, "us"), None);
    }

    #[test]
    fn capacity_rank_zero_capacity_row_is_skipped_boundary() {
        // py `if not cap or cap <= 0: continue` — exactly 0 is filtered
        // out, 1 (the smallest positive) survives.
        let rows = vec![
            gossip("us", true, Some(1_000_000), None),
            gossip("zero", true, Some(0), None),
            gossip("tiny", true, Some(1), None),
        ];
        assert_eq!(channel_capacity_rank(&rows, "us"), Some((0, 1)));
    }

    #[test]
    fn capacity_rank_own_row_counts_even_when_inactive() {
        // py: `if ch.get("source") == our_id: our_capacity = max(...)` has
        // NO active gate (only the `elif ch.get("active")` competitor arm
        // does) — our own row must count regardless of `active`.
        let rows = vec![
            gossip("us", false, Some(1_000_000), None),
            gossip("comp", true, Some(2_000_000), None),
        ];
        assert_eq!(channel_capacity_rank(&rows, "us"), Some((1, 1)));
    }

    #[test]
    fn capacity_rank_active_gating_excludes_inactive_competitors() {
        let rows = vec![
            gossip("us", true, Some(1_000_000), None),
            gossip("a", true, Some(2_000_000), None), // active, larger: counts
            gossip("b", false, Some(5_000_000), None), // inactive: excluded despite being larger
        ];
        assert_eq!(channel_capacity_rank(&rows, "us"), Some((1, 1)));
    }

    #[test]
    fn capacity_rank_uses_satoshis_over_amount_msat_default() {
        // py 3516: `ch.get("satoshis", base_to_sats_floor(ch.get("amount_msat", 0)))`.
        let rows = vec![
            gossip("us", true, None, Some(500_000)), // 500 sats via msat fallback
            gossip("a", true, None, Some(2_000_000_000)), // 2_000_000 sats
        ];
        assert_eq!(channel_capacity_rank(&rows, "us"), Some((1, 1)));
    }

    #[test]
    fn capacity_rank_strict_greater_than_boundary() {
        // `c > our_capacity` is STRICT: a competitor exactly equal to us
        // does not count as "larger".
        let rows = vec![
            gossip("us", true, Some(1_000_000), None),
            gossip("eq", true, Some(1_000_000), None), // equal: not larger
            gossip("gt", true, Some(1_000_001), None), // 1 sat over: larger
        ];
        assert_eq!(channel_capacity_rank(&rows, "us"), Some((1, 2)));
    }

    // -- channel_rebalance_cost_ppm (py `_get_channel_rebalance_cost_ppm` 3554-3594) --

    fn sample(timestamp: i64, cost_sats: i64, amount_sats: i64) -> RebalanceCostSample {
        RebalanceCostSample {
            cost_sats,
            amount_sats,
            timestamp,
        }
    }

    #[test]
    fn rebalance_cost_ppm_zero_for_sink_and_dormant() {
        // The "active-gating" carried from `_get_rebalance_cost_floor`:
        // sink/dormant channels don't pay outbound rebalance costs.
        let history = vec![sample(1000, 500, 100_000)];
        assert_eq!(channel_rebalance_cost_ppm("sink", &history, 100_000), 0);
        assert_eq!(channel_rebalance_cost_ppm("dormant", &history, 100_000), 0);
        assert_ne!(channel_rebalance_cost_ppm("balanced", &history, 100_000), 0);
    }

    #[test]
    fn rebalance_cost_ppm_window_boundary_inclusive() {
        let now = 100_000_000;
        let cutoff = now - REBALANCE_FLOOR_WINDOW_DAYS * 86400;
        let history = vec![
            sample(cutoff, 100, 10_000),    // exactly at cutoff: included (py `>=`)
            sample(cutoff - 1, 999_999, 1), // one second stale: excluded
        ];
        // Only the in-window sample counts: 100 * 1e6 / 10_000 = 10_000,
        // then capped at 5000 — if the stale sample leaked in, the ratio
        // (and thus the assertion) would differ.
        assert_eq!(channel_rebalance_cost_ppm("balanced", &history, now), 5000);
    }

    #[test]
    fn rebalance_cost_ppm_zero_volume_or_cost_is_zero() {
        assert_eq!(
            channel_rebalance_cost_ppm("balanced", &[sample(0, 0, 10_000)], 0),
            0,
            "zero cost -> 0"
        );
        assert_eq!(
            channel_rebalance_cost_ppm("balanced", &[sample(0, 100, 0)], 0),
            0,
            "zero volume -> 0"
        );
    }

    #[test]
    fn rebalance_cost_ppm_truncates_toward_zero_not_floor_division() {
        // 750 * 1_000_000 / 190_000 = 3947.368...; the soft nudge computes
        // this via `(total as f64 / total as f64) as i64` (truncating
        // cast) — a DIFFERENT code shape from the HARD floor's integer
        // `//` (`floors::rebalance_cost_floor`, which uses plain i64 `/`
        // on non-negative operands). Both land on 3947 here because
        // truncation == floor for non-negative operands, but this pins
        // the actual float-division code path rather than assuming it
        // matches the integer path by coincidence — a future edit that
        // swaps one division style for the other without checking both
        // fixture scenario 18 (`rebal_cost_soft_nudge`, cost_ppm=1421)
        // and this unit test would need to keep both green.
        let history = vec![sample(0, 750, 190_000)];
        let via_soft_nudge = channel_rebalance_cost_ppm("balanced", &history, 0);
        let via_integer_floor_div = (750_i64 * 1_000_000) / 190_000;
        assert_eq!(via_soft_nudge, 3947);
        assert_eq!(via_soft_nudge, via_integer_floor_div);
    }

    #[test]
    fn rebalance_cost_ppm_caps_at_5000() {
        // cost_ppm would be 100_000_000 uncapped; min(5000, ..) applies.
        let history = vec![sample(0, 100, 1)];
        assert_eq!(channel_rebalance_cost_ppm("balanced", &history, 0), 5000);
    }
}
