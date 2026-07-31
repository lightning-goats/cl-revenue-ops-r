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

use revops_analytics::classification::{classify_balance_position, ChannelState};
use revops_analytics::flow::{
    apply_kalman_reclassification, calculate_adaptive_decay, calculate_ema_flow,
    calculate_velocity, update_temporal_profile, EmaBucket, HourlyHistogramBucket,
    ReclassificationInput, TemporalProfile,
};
use revops_analytics::kalman::{
    calculate_confidence, DailyBucket, KalmanFlowFilter, KalmanFlowState, NetFlowEntry,
};
use revops_db::analytics::ChannelFlowStateRow;
use serde_json::Value;

use crate::msat_evidence::validated_msat;

/// Per-channel history the assembler needs, keyed by short-channel-id.
#[derive(Clone, Debug, Default)]
pub struct ChannelHistory {
    /// Daily in/out buckets for volatility + adaptive decay.
    pub daily: Vec<DailyBucket>,
    /// EMA buckets (in/out/count/last_ts) for the flow ratio.
    pub ema: Vec<EmaBucket>,
    /// The persisted Kalman state, if this channel has one.
    pub kalman: Option<KalmanFlowState>,
    /// F71-R19: the RAW per-forward entries of the last 24h. These, not
    /// the EMA buckets, are what the Kalman filter observes.
    pub raw_entries: Vec<NetFlowEntry>,
    /// F71-R20: the DTS fee-controller's `posterior_variance` for this
    /// channel, already dug out of `v2_state_json` by the caller. `None`
    /// means the fee subsystem holds no state for this channel, which is
    /// py's own no-widening path -- it is NOT a stand-in for "we did not
    /// look".
    pub posterior_variance: Option<f64>,
    /// The previously classified state name (hysteresis input).
    pub previous_state: Option<String>,
    /// F71-R25: this channel's 24-bucket hour-of-day histogram (already
    /// per-day averaged by the store) and its persisted temporal profile.
    /// Absent buckets are a real "no forwards this window", which the
    /// frozen kernel reads as a zero histogram.
    pub hourly_histogram: Option<[HourlyHistogramBucket; 24]>,
    pub temporal_profile: Option<TemporalProfile>,
    /// F71-R25b: the dominant size-bucket label the FEE controller owns,
    /// dug out of `v2_state_json.size_buckets` by the caller. `None` is
    /// py's `except: pass` path -- size profiling unavailable, so the
    /// stored profile's existing label is kept rather than overwritten.
    pub dominant_bucket_override: Option<String>,
    /// F71-R21: the PREVIOUS cycle's persisted Kalman estimate, py
    /// `prev_state["kalman_flow_ratio"]` (flow_analysis.py:1468-1471).
    /// This is the balance classifier's veto input, and it is deliberately
    /// the PRIOR value -- py applies the fresh estimate only later, in
    /// `_apply_kalman_reclassification`. Feeding the fresh one back in
    /// here would make the veto circular.
    pub previous_kalman_ratio: f64,
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
    /// py `flow_window_days`, the divisor in `daily_volume`.
    pub flow_window_days: i64,
    /// py `htlc_congestion_threshold`.
    pub htlc_congestion_threshold: f64,
    pub now: i64,
    pub boot_id: &'a str,
}

/// Typed refusals. Each names its failed source; none defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowRefusal {
    PeerChannelsUnavailable(String),
    /// F71-R19: the read SUCCEEDED but a consumed row's money fields are
    /// not valid msat. `parse_msat` is permissive by design, so garbage
    /// would otherwise become a confident zero — a zero `spendable_msat`
    /// is a real, meaningful "we can send nothing", and it must not be
    /// reachable by corruption.
    MalformedPeerChannels(String),
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
            Self::MalformedPeerChannels(_) => "flow_peer_channels_malformed",
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
    /// F71-R25: (scid, updated temporal profile) pairs to persist, in the
    /// SAME atomic commit as the states and Kalman filters.
    pub temporal: Vec<(String, TemporalProfile)>,
    /// Channels present in the snapshot but skipped, with the reason --
    /// surfaced, never silently dropped.
    pub skipped: Vec<(String, &'static str)>,
    /// F71-R24: EVERY scid the snapshot carried, in ANY state -- analysed,
    /// skipped, transient, closing. This is the RETENTION set for store
    /// reconciliation, and it is deliberately not the set of analysed
    /// channels: R21 stops analysing anything that is not
    /// `CHANNELD_NORMAL`, so reconciling against the analysed set would
    /// purge the accumulated Kalman state of every channel merely passing
    /// through a transient state, which takes many observations to rebuild.
    pub observed_scids: std::collections::BTreeSet<String>,
}

/// The three Python-side mutations this owner refuses. Named so the
/// refusal is a fact about a specific operation, not a vague "not
/// supported".
/// py `TEMPORAL_HISTOGRAM_WINDOW_DAYS` (flow_analysis.py:288).
pub const TEMPORAL_HISTOGRAM_WINDOW_DAYS: i64 = 7;

/// The only channel state py's flow analyzer consumes
/// (flow_analysis.py:2119).
pub const CHANNELD_NORMAL: &str = "CHANNELD_NORMAL";

/// py `channel.get("max_htlcs", 483)` (flow_analysis.py:1458) -- CLN's
/// protocol ceiling on concurrent HTLCs, used when the channel does not
/// report its own limit.
pub const DEFAULT_MAX_HTLCS: f64 = 483.0;

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

/// A REQUIRED balance field, shape-validated then floored to sats.
///
/// F71-R20: absence is a REFUSAL, not py's `.get(field, 0) or 0`. This is
/// a deliberate, disclosed divergence. CLN always emits `spendable_msat`
/// and `receivable_msat` for a channel in a routable state, so absence
/// means the reply is not what we think it is — and defaulting to 0 does
/// not produce a harmless blank: it produces the confident claim "this
/// channel can send nothing", which flows straight into `outbound_ratio`,
/// the balance-position classifier, and the fee/rebalance surfaces
/// downstream. A measured zero and an unreadable field must not be the
/// same value.
fn required_balance_sats(channel: &Value, field: &str, scid: &str) -> Result<i64, FlowRefusal> {
    match channel.get(field) {
        None | Some(Value::Null) => Err(FlowRefusal::MalformedPeerChannels(format!(
            "channel {scid} has no {field}; a missing balance cannot be read as zero"
        ))),
        Some(raw) => validated_msat(raw, &format!("channel {scid} {field}"))
            .map(|msat| msat.div_euclid(1_000))
            .map_err(FlowRefusal::MalformedPeerChannels),
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
        // F71-R24: observed BEFORE any eligibility decision -- presence in
        // the snapshot is what earns retention, not eligibility to analyse.
        result.observed_scids.insert(scid.clone());
        // F71-R21: py analyses ONLY `CHANNELD_NORMAL` channels
        // (flow_analysis.py:2119). A channel that is opening, closing or
        // awaiting lock-in has no meaningful steady-state flow, and
        // classifying one would persist a state row py never writes --
        // which downstream fee and rebalance surfaces would then act on.
        let channel_state = channel.get("state").and_then(Value::as_str).unwrap_or("");
        if channel_state != CHANNELD_NORMAL {
            result.skipped.push((scid, "not CHANNELD_NORMAL"));
            continue;
        }
        // py `channel.get("peer_id", "")` -- tolerant on purpose. This is
        // a label on the row, not an input to any decision, so tightening
        // it here would DIVERGE from Python rather than harden anything.
        let peer_id = channel
            .get("peer_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        // py 1428-1445. `spendable_msat` already nets out pending HTLCs
        // and the channel reserve, which is why Python uses it for the
        // balance and NOT `to_us_msat`: `to_us` counts sats we cannot
        // actually send, so a channel pinned by its reserve looks
        // balanced when it can route nothing outbound.
        let spendable = required_balance_sats(channel, "spendable_msat", &scid)?;
        let receivable = required_balance_sats(channel, "receivable_msat", &scid)?;
        // py: capacity is total_msat when present and non-zero, else the
        // spendable+receivable approximation.
        let capacity = match channel.get("total_msat") {
            None | Some(Value::Null) => spendable + receivable,
            Some(raw) => {
                let total = validated_msat(raw, &format!("channel {scid} total_msat"))
                    .map(|msat| msat.div_euclid(1_000))
                    .map_err(FlowRefusal::MalformedPeerChannels)?;
                if total == 0 {
                    spendable + receivable
                } else {
                    total
                }
            }
        };
        if capacity <= 0 {
            result.skipped.push((scid, "zero capacity"));
            continue;
        }
        let our_balance = spendable;
        let outbound_ratio = our_balance as f64 / capacity as f64;

        let hist = history.get(&scid).cloned().unwrap_or_default();

        // FROZEN kernels, in Python's order.
        let decay = calculate_adaptive_decay(&hist.daily);
        let (ema_in, ema_out, total_in, total_out, forward_count, last_ts) =
            calculate_ema_flow(&hist.ema, decay);
        // py 1928-1930: the EMA ratio is NET flow over capacity, clamped.
        let ema_ratio = ((ema_out - ema_in) / capacity as f64).clamp(-1.0, 1.0);
        let has_flow_data = total_in > 0 || total_out > 0;
        // py 1934-1935.
        let daily_volume = (total_in + total_out) / deps.flow_window_days.max(1);
        let turnover = daily_volume as f64 / capacity as f64;
        // py 1457-1458 reads PRE-NORMALIZED `active_htlcs` (default 0) and
        // `max_htlcs` (default 483 -- CLN's protocol maximum, NOT 0), which
        // flow_analysis.py:2131-2137 derives from `max_accepted_htlcs` and
        // `len(htlcs)`. The 483 default matters: with a 0 default, a
        // channel reporting active HTLCs but no limit divides by zero and
        // reads as uncongested, when py would compute a real utilization.
        let active_htlcs = channel
            .get("active_htlcs")
            .and_then(Value::as_f64)
            .or_else(|| {
                channel
                    .get("htlcs")
                    .and_then(Value::as_array)
                    .map(|h| h.len() as f64)
            })
            .unwrap_or(0.0);
        let max_htlcs = channel
            .get("max_htlcs")
            .and_then(Value::as_f64)
            .or_else(|| channel.get("max_accepted_htlcs").and_then(Value::as_f64))
            .unwrap_or(DEFAULT_MAX_HTLCS);
        let htlc_utilization = if max_htlcs > 0.0 {
            active_htlcs / max_htlcs
        } else {
            0.0
        };
        let is_congested = htlc_utilization > deps.htlc_congestion_threshold;

        // py 1941-1960: the EMA-side classification, which the Kalman
        // reclassification may then override.
        let ema_state = if is_congested {
            ChannelState::Congested
        } else if has_flow_data {
            // F71-R21a: py thresholds on the EMA ratio but vetoes with the
            // PREVIOUS Kalman estimate -- two DIFFERENT signals
            // (flow_analysis.py:1948-1963). `classification::flow_state`
            // cannot express that: it passes the value it thresholded on
            // straight through as the veto argument, which is correct for
            // the Kalman reclassification path (where both ARE the fresh
            // kalman ratio) and wrong here. So the EMA branch is spelled
            // out rather than borrowed from that kernel.
            if ema_ratio > deps.source_threshold {
                ChannelState::Source
            } else if ema_ratio < deps.sink_threshold {
                ChannelState::Sink
            } else {
                // BALANCED zone: structural balance signal, vetoed by the
                // previous cycle's Kalman estimate, with real turnover.
                classify_balance_position(
                    outbound_ratio,
                    hist.previous_state.as_deref(),
                    hist.previous_kalman_ratio,
                    turnover,
                )
            }
        } else {
            classify_balance_position(
                outbound_ratio,
                hist.previous_state.as_deref(),
                hist.previous_kalman_ratio,
                0.0,
            )
        };
        let ema_confidence = calculate_confidence(forward_count, last_ts, deps.now);

        // F71-R19: the CANONICAL reclassification. This owner previously
        // hand-rolled a variant of it that fed the Kalman filter the
        // EMA-smoothed ratio, skipped the convergence gate, and always
        // overrode the state. `apply_kalman_reclassification` is the
        // frozen port of Python's own method -- raw 24h observation,
        // recency-weighted confidence, and an override that only applies
        // once the filter has actually converged.
        let mut filter = KalmanFlowFilter::new(hist.kalman.clone());
        let raw_entries = hist.raw_entries.clone();
        let outcome = apply_kalman_reclassification(
            &mut filter,
            &ReclassificationInput {
                capacity,
                our_balance,
                daily_volume,
                is_congested,
                daily_buckets: &hist.daily,
                raw_entries: &raw_entries,
                last_forward_ts: last_ts,
                previous_state: hist.previous_state.as_deref(),
                source_threshold: deps.source_threshold,
                sink_threshold: deps.sink_threshold,
                fallback_confidence: ema_confidence,
                // F71-R20: py READS this per channel
                // (`get_fee_strategy_state`) and widens the flow
                // thresholds by 50% when the DTS controller is still
                // exploring. Hardcoding `None` here claimed "no evidence"
                // while the fee store held it, which suppressed every
                // widening and biased classification toward SOURCE/SINK
                // exactly when the fee controller was least sure.
                posterior_variance: hist.posterior_variance,
                now: deps.now,
            },
        );
        // py mutates `metrics.state` only when the override actually ran.
        let state = outcome.state.unwrap_or(ema_state);

        // py `_calculate_velocity(flow_ratio, previous_ratio, ts)` runs on
        // the EMA ratio; the Kalman filter reports its OWN velocity as a
        // state variable. They are different quantities and both persist.
        let velocity = calculate_velocity(
            ema_ratio,
            hist.previous_ratio,
            hist.previous_ratio_at,
            deps.now,
        );
        // py passes the PREVIOUS cycle's Kalman estimate as the veto
        // input (flow_analysis.py:1963/1976), not the fresh one.
        let balance = classify_balance_position(
            outbound_ratio,
            hist.previous_state.as_deref(),
            hist.previous_kalman_ratio,
            turnover,
        );

        result.states.push(ChannelFlowStateRow {
            scid: scid.clone(),
            peer_id,
            flow_state: state.as_value().to_string(),
            balance_position: balance.as_value().to_string(),
            // F71-R20: EMA and Kalman are SEPARATE columns. Writing the
            // Kalman estimate into `flow_ratio` -- or, worse, the Kalman
            // UNCERTAINTY into `confidence`, its inverse -- silently
            // inverts what every downstream reader sees.
            flow_ratio: ema_ratio,
            velocity,
            confidence: ema_confidence,
            kalman_flow_ratio: outcome.kalman_flow_ratio,
            kalman_velocity: outcome.kalman_velocity,
            kalman_uncertainty: outcome.kalman_uncertainty,
            kalman_regime_change: outcome.kalman_regime_change,
            forward_count,
            updated_at: deps.now,
            boot_id: deps.boot_id.to_string(),
        });
        // F71-R25: the frozen temporal kernel, driven from the same
        // snapshot. py `_update_temporal_profile` (flow_analysis.py:1664).
        // `avg_daily_forwards` is the histogram's summed count divided by
        // the window -- py's F5 fix: the raw window TOTAL compared against
        // a per-day threshold let ~1.4 forwards/day graduate a channel.
        let histogram = hist
            .hourly_histogram
            .unwrap_or([HourlyHistogramBucket::default(); 24]);
        let avg_daily_forwards = (histogram.iter().map(|b| b.count).sum::<f64>()
            / TEMPORAL_HISTOGRAM_WINDOW_DAYS as f64) as i64;
        // py sets `existing.dominant_bucket` from the fee controller's
        // size profiling BEFORE calling the kernel, which then carries it
        // forward unchanged (flow_analysis.py:1709-1723).
        let mut existing = hist.temporal_profile.clone().unwrap_or_default();
        if let Some(label) = hist.dominant_bucket_override.clone() {
            existing.dominant_bucket = label;
        }
        let profile = update_temporal_profile(&existing, &histogram, avg_daily_forwards, deps.now);
        result.temporal.push((scid.clone(), profile));
        result.kalman.push((scid, filter.state.clone()));
    }
    Ok(result)
}
