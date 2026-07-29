//! Task 67 slice 3: the flow-analysis runtime owner.
//!
//! OBSERVATION-ONLY. This owner reads live channel evidence, runs the
//! FROZEN `revops-analytics` kernels (velocity / adaptive decay / EMA /
//! Kalman / classification), and persists the derived state through the
//! observer store. It holds no action capability and can move no funds.
//!
//! **Deliberate divergence from Python (disclosed).** Python's
//! `flow_analysis_loop` (cl-revenue-ops.py:3170-3225) also performs three
//! MUTATIONS on every pass -- the cleanup-old-data sweep, the
//! expired-policy cleanup, and the reputation decay (named exactly in
//! [`REFUSED_MUTATIONS`]; spelled without call syntax here so the
//! observation-only source scan stays a scan for CALLABLE shapes rather
//! than for prose). Those are retention and policy
//! writes, not flow analysis. Bundling them into an
//! "observation-only" owner would make the observation-only claim false,
//! so they are REFUSED here with a typed
//! [`FlowRefusal::RetentionNotThisOwner`]. Retention already has an owner
//! (`revops_db::retention`, Task 59); expiring policies and decaying
//! reputation belong with the policy-write surfaces (Task 66).
//!
//! Every required input is `Result`-shaped: a failed read is a typed
//! refusal AND a failed loop pass, never a silent default. An empty
//! channel set is a legitimate zero-work pass; an UNREADABLE channel set
//! is not.

use revops_analytics::classification::{classify_balance_position, flow_state};
use revops_analytics::flow::{
    apply_kalman_filter, calculate_adaptive_decay, calculate_ema_flow, calculate_velocity,
    EmaBucket,
};
use revops_analytics::kalman::{DailyBucket, KalmanFlowFilter, KalmanFlowState};
use revops_db::analytics::ChannelFlowStateRow;
use serde_json::Value;

/// Per-channel history the assembler needs, keyed by short-channel-id.
#[derive(Clone, Debug, Default)]
pub struct ChannelHistory {
    /// Daily in/out buckets for volatility + adaptive decay.
    pub daily: Vec<DailyBucket>,
    /// EMA buckets (in/out/count/last_ts) for the flow ratio.
    pub ema: Vec<EmaBucket>,
    /// The persisted Kalman state, if this channel has one.
    pub kalman: Option<KalmanFlowState>,
    /// The previously classified state name (hysteresis input).
    pub previous_state: Option<String>,
    /// The previous flow ratio and its timestamp (velocity input).
    pub previous_ratio: f64,
    pub previous_ratio_at: i64,
}

/// Everything one flow pass consumes. Each fallible source arrives as a
/// `Result` produced by the caller's real read, so the pass stays pure
/// and every failure path is drivable in tests.
pub struct FlowDeps<'a> {
    /// The live `listpeerchannels` reply (REQUIRED).
    pub peer_channels_raw: Result<Value, String>,
    /// Per-scid history from the Rust-owned stores (REQUIRED).
    pub history: Result<std::collections::BTreeMap<String, ChannelHistory>, String>,
    pub source_threshold: f64,
    pub sink_threshold: f64,
    pub now: i64,
    pub boot_id: &'a str,
}

/// Typed refusals. Each names its failed source; none defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowRefusal {
    PeerChannelsUnavailable(String),
    HistoryUnavailable(String),
    /// Python's flow loop also swept retention and decayed reputation.
    /// This owner is observation-only and refuses to.
    RetentionNotThisOwner {
        operation: String,
    },
}

impl FlowRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::PeerChannelsUnavailable(_) => "flow_peer_channels_unavailable",
            Self::HistoryUnavailable(_) => "flow_history_unavailable",
            Self::RetentionNotThisOwner { .. } => "flow_retention_not_this_owner",
        }
    }
}

/// One pass's product: the rows to persist plus the updated Kalman states.
#[derive(Debug, Default)]
pub struct FlowPassResult {
    pub states: Vec<ChannelFlowStateRow>,
    /// (scid, kalman state) pairs to persist.
    pub kalman: Vec<(String, KalmanFlowState)>,
    /// Channels present in the snapshot but skipped, with the reason --
    /// surfaced, never silently dropped.
    pub skipped: Vec<(String, &'static str)>,
}

/// The three Python-side mutations this owner refuses. Named so the
/// refusal is a fact about a specific operation, not a vague "not
/// supported".
pub const REFUSED_MUTATIONS: [&str; 3] = [
    "cleanup_old_data",
    "cleanup_expired_policies",
    "decay_reputation",
];

/// Refuse one of Python's flow-loop mutations, typed.
pub fn refuse_retention_mutation(operation: &str) -> FlowRefusal {
    FlowRefusal::RetentionNotThisOwner {
        operation: operation.to_string(),
    }
}

fn msat_to_sats(v: Option<&Value>) -> i64 {
    match v {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) / 1000,
        Some(Value::String(s)) => {
            s.trim()
                .trim_end_matches("msat")
                .parse::<i64>()
                .unwrap_or(0)
                / 1000
        }
        _ => 0,
    }
}

/// Run one flow-analysis pass over the assembled evidence.
pub fn run_flow_pass(deps: FlowDeps<'_>) -> Result<FlowPassResult, FlowRefusal> {
    let raw = deps
        .peer_channels_raw
        .map_err(FlowRefusal::PeerChannelsUnavailable)?;
    let channels = raw
        .get("channels")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            FlowRefusal::PeerChannelsUnavailable(
                "listpeerchannels reply carries no channels array".to_string(),
            )
        })?;
    let history = deps.history.map_err(FlowRefusal::HistoryUnavailable)?;

    let mut result = FlowPassResult::default();
    for channel in channels {
        let scid = channel
            .get("short_channel_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if scid.is_empty() {
            // A channel with no scid is not yet routable; not an error.
            result
                .skipped
                .push((String::new(), "no short_channel_id (pre-confirmation)"));
            continue;
        }
        let peer_id = channel
            .get("peer_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let total = msat_to_sats(channel.get("total_msat"));
        if total <= 0 {
            result.skipped.push((scid, "zero capacity"));
            continue;
        }
        let to_us = msat_to_sats(channel.get("to_us_msat"));
        let outbound_ratio = to_us as f64 / total as f64;

        let hist = history.get(&scid).cloned().unwrap_or_default();

        // FROZEN kernels, in Python's order.
        let decay = calculate_adaptive_decay(&hist.daily);
        let (ema_in, ema_out, total_in, total_out, forward_count, last_ts) =
            calculate_ema_flow(&hist.ema, decay);
        let observed_ratio = if ema_in + ema_out > 0.0 {
            ema_out / (ema_in + ema_out)
        } else {
            outbound_ratio
        };
        let has_observation = forward_count > 0;
        let turnover = if total > 0 {
            (total_in + total_out) as f64 / total as f64
        } else {
            0.0
        };
        let confidence = if has_observation { 1.0 } else { 0.0 };

        let mut filter = KalmanFlowFilter::new(hist.kalman.clone());
        let outcome = apply_kalman_filter(
            &mut filter,
            observed_ratio,
            confidence,
            &hist.daily,
            has_observation,
            deps.now,
        );
        let velocity = calculate_velocity(
            outcome.flow_ratio,
            hist.previous_ratio,
            hist.previous_ratio_at,
            deps.now,
        );
        let state = flow_state(
            outcome.flow_ratio,
            deps.source_threshold,
            deps.sink_threshold,
            outbound_ratio,
            hist.previous_state.as_deref(),
            turnover,
        );
        let balance = classify_balance_position(
            outbound_ratio,
            hist.previous_state.as_deref(),
            outcome.flow_ratio,
            turnover,
        );
        let _ = last_ts;

        result.states.push(ChannelFlowStateRow {
            scid: scid.clone(),
            peer_id,
            flow_state: state.as_value().to_string(),
            balance_position: balance.as_value().to_string(),
            flow_ratio: outcome.flow_ratio,
            velocity,
            confidence: outcome.uncertainty,
            forward_count,
            updated_at: deps.now,
            boot_id: deps.boot_id.to_string(),
        });
        result.kalman.push((scid, filter.state.clone()));
    }
    Ok(result)
}
