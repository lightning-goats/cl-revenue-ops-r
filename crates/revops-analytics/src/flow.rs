//! Flow pipeline: `FlowMetrics`, Kalman-reclassification decision,
//! velocity/decay/EMA, `TemporalProfile` (port of `modules/flow_analysis.py`).
//!
//! Scope (Task 7, Wave 2 — needs T2 `classification` + T3 `kalman`, both
//! already on `main`): the PURE computational core of `FlowAnalyzer`.
//! Deferred out of this crate entirely, per the phase plan's "Deferred out
//! of this plan" section — `analyze_all_channels`'s RPC/DB orchestration
//! (`_get_channels`, `_get_daily_flow_from_db`, batched channel-state /
//! Kalman-state writes, the flow-result TTL cache, the stale-channel
//! reconciliation sweep) rides with Phase 3b wiring in `crates/revops`,
//! where it can be diffed against `revenue-r-analyze` output. Everything
//! below takes clock reads, config thresholds, and DB/RPC evidence as
//! explicit function parameters — no `self`, no `time.time()`, no config
//! object.
//!
//! # Clock injection
//!
//! Mirrors `kalman.rs`: every function that Python drives from
//! `time.time()` / `int(time.time())` takes that reading as an explicit
//! `now: i64` (Unix seconds) parameter instead.
//!
//! # Float summation parity
//!
//! `_calculate_ema_flow` accumulates via a manual Python `for` loop (`ema_in
//! += ...`) — naive sequential left-to-right float addition, NOT the
//! CPython 3.12+ Neumaier-compensated `sum()` builtin (that special case
//! only fires for `sum()` itself, never for a hand-rolled loop). A plain
//! Rust `for` loop with `+=` reproduces that term-for-term.
//! `_calculate_adaptive_decay`'s `mean_net`/`mean_volume` sums are of
//! Python `int` values (daily bucket sats), so a plain fold is exact
//! there — but its `variance` term sums `(x - mean_net) ** 2`, which is
//! `float`-valued (`mean_net` is itself a float from integer division), so
//! CPython's `sum()` on that genexpr *does* invoke Neumaier compensation.
//! Reuses `kalman::neumaier_sum` (now `pub(crate)`) rather than
//! duplicating it — same primitive, same reasoning as documented there.
//!
//! # Numpy-derived `TemporalProfile` fields (documented limitation)
//!
//! `_recompute_derived`'s `burstiness`/`diurnal_strength` go through
//! `np.mean`/`np.std`/`np.dot`, whose internal reduction order (pairwise
//! summation above a block-size threshold, SIMD-width-dependent below it)
//! is not part of numpy's documented public contract and is not, in
//! general, reproducible bit-for-bit by a hand-written Rust reduction —
//! unlike the hand-rolled Python loops elsewhere in this module, there is
//! no well-defined "the same arithmetic order" to transcribe. These two
//! fields (plus the `peak_hours`/`quiet_hours` index sets, which ARE exact
//! — pure sorting/comparison, no numpy reduction involved) are therefore
//! pinned in fixtures with a small epsilon tolerance rather than
//! `f64::to_bits` equality; every other float in this module (EMA,
//! velocity, decay, Kalman reclassification) stays bit-exact.
//!
//! # Volatility bucket-magnitude assumption (carried from the T3 review)
//!
//! `calculate_kalman_volatility`/`calculate_adaptive_decay` sum Python
//! `int` daily bucket sat values as `f64` and rely on that sum being
//! *exact* (no compensation needed) — true only because every addend is
//! an exact integer-valued `f64`, which in turn requires the summed
//! magnitude to stay under 2^53 (~9.007e15). A single day's `in`/`out`
//! bucket for one channel is bounded by that channel's capacity in sats;
//! BOLT#2's `funding_satoshis` is a 64-bit value but the network's actual
//! (wumbo-enabled) channels top out around low tens of BTC (~1e9 sats),
//! roughly six orders of magnitude below the 2^53 exactness boundary, and
//! `daily_buckets.len()` in these functions is always a small window (≤ a
//! few hundred days), so an accumulated running total staying below 2^53
//! is not a realistic concern for this deployment's data. This is a
//! documented assumption, not an enforced invariant: nothing in this
//! module asserts it, because the Python source doesn't either (same
//! `int -> float -> sum()` pattern, same implicit assumption).

use crate::classification::{self, ChannelState};
use crate::kalman::{
    self, calculate_confidence, calculate_kalman_volatility, compute_raw_kalman_observation,
    DailyBucket, KalmanFlowFilter, NetFlowEntry, KALMAN_CONVERGENCE_UNCERTAINTY,
    KALMAN_MIN_OBSERVATIONS, KALMAN_MIN_UPDATE_INTERVAL_SECS,
};
use revops_econ::pyfloat::py_round;

// =============================================================================
// Module-level constants (flow_analysis.py lines 65-78, 282-288)
// =============================================================================

/// Max flow_ratio change per hour (security bound) — the NON-Kalman
/// velocity bound (`_calculate_velocity`'s raw EMA-ratio velocity, not the
/// Kalman filter's tighter `KALMAN_MAX_VELOCITY`/`KALMAN_MIN_VELOCITY`).
pub const MAX_VELOCITY: f64 = 0.5;
pub const MIN_VELOCITY: f64 = -0.5;
/// Ignore velocity changes > 3 standard deviations.
pub const VELOCITY_OUTLIER_THRESHOLD: f64 = 3.0;

/// Default EMA decay factor.
pub const BASE_EMA_DECAY: f64 = 0.8;
/// Symmetric range around `BASE_EMA_DECAY`: fast=0.65, slow=0.95.
pub const DECAY_RANGE: f64 = 0.3;

pub const TEMPORAL_GRADUATION_DAYS: i64 = 7;
pub const TEMPORAL_MIN_DAILY_FORWARDS: i64 = 10;
pub const TEMPORAL_EMA_ALPHA: f64 = 0.3;

// =============================================================================
// FlowMetrics
// =============================================================================

/// Flow metrics for a single channel (port of the Python `FlowMetrics`
/// dataclass). All fields are public; constructed directly by callers
/// (Phase 3b wiring) rather than through a builder, matching the Python
/// dataclass's plain-field-assignment usage.
#[derive(Clone, Debug, PartialEq)]
pub struct FlowMetrics {
    pub channel_id: String,
    pub peer_id: String,
    pub sats_in: i64,
    pub sats_out: i64,
    pub capacity: i64,
    pub flow_ratio: f64,
    pub state: ChannelState,
    pub daily_volume: i64,
    pub is_congested: bool,
    pub confidence: f64,
    pub velocity: f64,
    pub flow_multiplier: f64,
    pub ema_decay: f64,
    pub forward_count: i64,
    pub kalman_flow_ratio: f64,
    pub kalman_velocity: f64,
    pub kalman_uncertainty: f64,
    pub kalman_regime_change: bool,
}

/// `FlowMetrics.to_dict()`'s value shape: every `kalman_*`/v2.0 float field
/// rounded via Python's banker's `round()` (`revops_econ::pyfloat::py_round`),
/// `state` as the wire `.value` (lowercase). Pinned against real Python
/// `to_dict()` output byte-for-byte in `tests/flow.rs`.
#[derive(Clone, Debug, PartialEq)]
pub struct FlowMetricsDict {
    pub channel_id: String,
    pub peer_id: String,
    pub sats_in: i64,
    pub sats_out: i64,
    pub capacity: i64,
    pub flow_ratio: f64,
    pub state: &'static str,
    pub daily_volume: i64,
    pub is_congested: bool,
    pub confidence: f64,
    pub velocity: f64,
    pub flow_multiplier: f64,
    pub ema_decay: f64,
    pub forward_count: i64,
    pub kalman_flow_ratio: f64,
    pub kalman_velocity: f64,
    pub kalman_uncertainty: f64,
    pub kalman_regime_change: bool,
}

impl FlowMetrics {
    /// Port of `FlowMetrics.to_dict()`.
    pub fn to_dict(&self) -> FlowMetricsDict {
        FlowMetricsDict {
            channel_id: self.channel_id.clone(),
            peer_id: self.peer_id.clone(),
            sats_in: self.sats_in,
            sats_out: self.sats_out,
            capacity: self.capacity,
            flow_ratio: py_round(self.flow_ratio, 4),
            state: self.state.as_value(),
            daily_volume: self.daily_volume,
            is_congested: self.is_congested,
            confidence: py_round(self.confidence, 3),
            velocity: py_round(self.velocity, 4),
            flow_multiplier: py_round(self.flow_multiplier, 3),
            ema_decay: py_round(self.ema_decay, 3),
            forward_count: self.forward_count,
            kalman_flow_ratio: py_round(self.kalman_flow_ratio, 4),
            kalman_velocity: py_round(self.kalman_velocity, 4),
            kalman_uncertainty: py_round(self.kalman_uncertainty, 4),
            kalman_regime_change: self.kalman_regime_change,
        }
    }
}

// =============================================================================
// _calculate_velocity
// =============================================================================

/// Rate of change of `flow_ratio` per hour, bounded to
/// `[MIN_VELOCITY, MAX_VELOCITY]` with outlier clamping.
#[allow(clippy::manual_clamp)] // NaN-propagation parity with Python's `max(lo, min(hi, x))` — see kalman.rs's module doc comment for the full rationale.
pub fn calculate_velocity(
    flow_ratio: f64,
    previous_ratio: f64,
    previous_timestamp: i64,
    now: i64,
) -> f64 {
    if previous_timestamp <= 0 {
        return 0.0;
    }

    let hours_elapsed = (now - previous_timestamp) as f64 / 3600.0;
    if hours_elapsed < 0.5 {
        return 0.0;
    }

    let mut raw_velocity = (flow_ratio - previous_ratio) / hours_elapsed;

    // B1 FIX: `abs(flow_ratio) + 0.01` is a floor to avoid a zero
    // threshold, not a shift of `flow_ratio` before taking its magnitude.
    let expected_max = VELOCITY_OUTLIER_THRESHOLD * (flow_ratio.abs() + 0.01);
    if raw_velocity.abs() > expected_max {
        raw_velocity = f64::max(-expected_max, f64::min(expected_max, raw_velocity));
    }

    f64::max(MIN_VELOCITY, f64::min(MAX_VELOCITY, raw_velocity))
}

// =============================================================================
// _calculate_adaptive_decay
// =============================================================================

/// Adaptive EMA decay factor from daily-bucket flow volatility, bounded to
/// `BASE_EMA_DECAY +/- DECAY_RANGE/2`.
pub fn calculate_adaptive_decay(daily_buckets: &[DailyBucket]) -> f64 {
    let min_decay = BASE_EMA_DECAY - DECAY_RANGE / 2.0;
    let max_decay = BASE_EMA_DECAY + DECAY_RANGE / 2.0;

    if daily_buckets.len() < 3 {
        return BASE_EMA_DECAY;
    }

    // net_flows / volumes: sums of Python `int` values -> exact as an f64
    // fold (see module doc comment), matching `kalman::calculate_kalman_volatility`.
    let net_flows: Vec<f64> = daily_buckets.iter().map(|b| b.out - b.in_).collect();
    let volumes: Vec<f64> = daily_buckets.iter().map(|b| b.out + b.in_).collect();

    let mean_volume = if !volumes.is_empty() {
        volumes.iter().sum::<f64>() / volumes.len() as f64
    } else {
        1.0
    };
    if mean_volume < 1000.0 {
        return BASE_EMA_DECAY;
    }

    // MA-7: sample variance (N-1 divisor).
    let mean_net = net_flows.iter().sum::<f64>() / net_flows.len() as f64;
    // `(x - mean_net) ** 2` is float-valued (mean_net is a float) -> CPython
    // 3.12+'s `sum()` Neumaier-compensates this genexpr.
    let variance = kalman::neumaier_sum(net_flows.iter().map(|x| (x - mean_net).powi(2)))
        / (net_flows.len() as f64 - 1.0);
    let std_dev = if variance > 0.0 { variance.sqrt() } else { 0.0 };

    let volatility = if mean_volume > 0.0 {
        std_dev / mean_volume
    } else {
        0.0
    };

    let decay = if volatility > 0.5 {
        min_decay
    } else if volatility < 0.1 {
        max_decay
    } else {
        max_decay - (volatility - 0.1) * ((max_decay - min_decay) / 0.4)
    };

    f64::max(min_decay, f64::min(max_decay, decay))
}

// =============================================================================
// _calculate_ema_flow
// =============================================================================

/// One day's flow bucket, as fed to `_calculate_ema_flow`
/// (`database.get_daily_flow_buckets()`'s row shape: `{'in':, 'out':,
/// 'count':, 'last_ts':}`). Index 0 = today, ascending age — callers (DB
/// layer, Phase 3b) are responsible for that ordering, exactly as the
/// Python docstring requires.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct EmaBucket {
    pub in_sats: i64,
    pub out_sats: i64,
    pub count: i64,
    pub last_ts: i64,
}

/// Exponential moving average of in/out flow over `daily_buckets`, weighted
/// `decay_factor ** age`. Returns `(ema_in, ema_out, total_in, total_out,
/// forward_count, last_forward_ts)`.
pub fn calculate_ema_flow(
    daily_buckets: &[EmaBucket],
    decay_factor: f64,
) -> (f64, f64, i64, i64, i64, i64) {
    if daily_buckets.is_empty() {
        return (0.0, 0.0, 0, 0, 0, 0);
    }

    let mut ema_in = 0.0_f64;
    let mut ema_out = 0.0_f64;
    let mut total_weight = 0.0_f64;
    let mut total_in = 0_i64;
    let mut total_out = 0_i64;
    let mut forward_count = 0_i64;
    let mut last_forward_ts = 0_i64;

    for (age, bucket) in daily_buckets.iter().enumerate() {
        // Python `decay_factor ** age`: float ** int dispatches to
        // `float.__pow__`, which converts the exponent to `double` and
        // calls C's `pow()` unconditionally (no integer fast path) — so
        // `powf`, never `powi` (repeated squaring would round differently).
        let weight = decay_factor.powf(age as f64);

        ema_in += bucket.in_sats as f64 * weight;
        ema_out += bucket.out_sats as f64 * weight;

        total_in += bucket.in_sats;
        total_out += bucket.out_sats;
        forward_count += bucket.count;

        if bucket.last_ts > last_forward_ts {
            last_forward_ts = bucket.last_ts;
        }

        total_weight += weight;
    }

    ema_in /= total_weight;
    ema_out /= total_weight;

    (
        ema_in,
        ema_out,
        total_in,
        total_out,
        forward_count,
        last_forward_ts,
    )
}

// =============================================================================
// KalmanStep + apply_kalman_filter (the pure 300s no-touch gate decision)
// =============================================================================

/// What happened to the Kalman filter this cycle — the pure decision the
/// Python 300s no-touch gate makes, so Phase 3b wiring can decide what to
/// persist without re-deriving the gate logic: `Untouched` means the
/// filter's `state_snapshot` was `None` in Python (nothing written);
/// `PredictOnly`/`Updated` both mean the state changed and must be
/// persisted (predict-only still advances `last_update` + covariance, so
/// dt-accounting on the next cycle stays correct even with no observation).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KalmanStep {
    /// A call arrived within `KALMAN_MIN_UPDATE_INTERVAL_SECS` of the
    /// filter's last consume — read but NOT mutated. Nothing to persist.
    Untouched,
    /// No raw observation this cycle (`has_observation=false`): predict-only
    /// ran, `last_update` was bumped directly. Must be persisted.
    PredictOnly,
    /// Predict + a real `update()` ran against observed data. Must be
    /// persisted.
    Updated,
}

/// Outcome of one `apply_kalman_filter` call — mirrors the Python
/// `_apply_kalman_filter` 5-tuple return plus the `KalmanStep` decision and
/// the NaN-recovery delta (for the caller's warn-log parity, if wanted).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct KalmanFilterOutcome {
    pub flow_ratio: f64,
    pub flow_velocity: f64,
    pub uncertainty: f64,
    pub regime_change: bool,
    pub observation_count: i64,
    pub step: KalmanStep,
    pub nan_recovery_delta: u32,
}

/// Pure port of `FlowAnalyzer._apply_kalman_filter`. `kf` is the caller's
/// in-memory filter for this channel (loaded/cached by Phase 3b wiring);
/// this function mutates it in place exactly as Python mutates
/// `self._kalman_filters[channel_id]` under `_kalman_lock` — the lock
/// itself is a wiring concern, out of scope here.
pub fn apply_kalman_filter(
    kf: &mut KalmanFlowFilter,
    observed_ratio: f64,
    confidence: f64,
    daily_buckets: &[DailyBucket],
    has_observation: bool,
    now: i64,
) -> KalmanFilterOutcome {
    let volatility = calculate_kalman_volatility(daily_buckets);

    let dt_hours = if kf.state.last_update > 0 {
        (now - kf.state.last_update) as f64 / 3600.0
    } else {
        24.0 // First run, assume 1 day.
    };
    // Cap dt to prevent explosion after long gaps (7 days = 168 hours).
    let dt_hours = f64::min(dt_hours, 168.0);

    let recently_updated =
        kf.state.last_update > 0 && (now - kf.state.last_update) < KALMAN_MIN_UPDATE_INTERVAL_SECS;

    if recently_updated {
        return KalmanFilterOutcome {
            regime_change: kf.is_regime_change(2.5),
            uncertainty: kf.get_uncertainty(),
            flow_ratio: kf.state.flow_ratio,
            flow_velocity: kf.state.flow_velocity,
            observation_count: kf.state.observation_count,
            step: KalmanStep::Untouched,
            nan_recovery_delta: 0,
        };
    }

    kf.predict(dt_hours, volatility);

    let step = if has_observation {
        kf.update(observed_ratio, confidence, now);
        KalmanStep::Updated
    } else {
        // Predict-only: still record that we ran so dt_hours stays
        // accurate next cycle. AUDIT FIX I-3: check for NaN after this
        // path too (update()'s own guard doesn't run here).
        kf.state.last_update = now;
        kf.reset_if_nan();
        KalmanStep::PredictOnly
    };

    let nan_recovery_delta = kf.take_nan_recovery_count();
    let regime_change = kf.is_regime_change(2.5);
    let uncertainty = kf.get_uncertainty();

    KalmanFilterOutcome {
        flow_ratio: kf.state.flow_ratio,
        flow_velocity: kf.state.flow_velocity,
        uncertainty,
        regime_change,
        observation_count: kf.state.observation_count,
        step,
        nan_recovery_delta,
    }
}

// =============================================================================
// apply_kalman_reclassification (pure-ified _apply_kalman_reclassification)
// =============================================================================

/// Evidence the analyzer (Phase 3b wiring) must gather and pass in — DB
/// reads, config lookups, and the DTS fee-strategy `posterior_variance`
/// JSON dig are all orchestration, not this pure decision.
pub struct ReclassificationInput<'a> {
    pub capacity: i64,
    pub our_balance: i64,
    pub daily_volume: i64,
    pub is_congested: bool,
    pub daily_buckets: &'a [DailyBucket],
    pub raw_entries: &'a [NetFlowEntry],
    pub last_forward_ts: i64,
    pub previous_state: Option<&'a str>,
    pub source_threshold: f64,
    pub sink_threshold: f64,
    /// `metrics.confidence` fallback used when the 24h raw-observation
    /// window has zero entries (Python: `if raw_count > 0 else
    /// metrics.confidence`).
    pub fallback_confidence: f64,
    /// The DTS fee-strategy state's `posterior_variance`, already dug out
    /// of `fee_strategy_state.v2_state_json` by the caller (nested-first,
    /// flat fallback, per Python's try/except). `None` reproduces every
    /// Python "no widening" case uniformly: a missing `fee_state` row, a
    /// missing/malformed `v2_state_json`, or any exception during the
    /// dig — all of those fall through to the untouched thresholds exactly
    /// like an explicit `variance <= 10000`.
    pub posterior_variance: Option<f64>,
    pub now: i64,
}

/// Outcome of `apply_kalman_reclassification`. `state` is `Some(..)` only
/// when Python's `metrics.state = ...` reassignment actually ran
/// (`not is_congested and kalman_converged`); `None` means the caller's
/// existing EMA-threshold classification (`_calculate_metrics`'s output)
/// must be left untouched.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReclassificationOutcome {
    pub kalman_flow_ratio: f64,
    pub kalman_velocity: f64,
    pub kalman_uncertainty: f64,
    pub kalman_regime_change: bool,
    pub step: KalmanStep,
    pub nan_recovery_delta: u32,
    pub state: Option<ChannelState>,
}

/// Pure port of `FlowAnalyzer._apply_kalman_reclassification`.
pub fn apply_kalman_reclassification(
    kf: &mut KalmanFlowFilter,
    input: &ReclassificationInput<'_>,
) -> ReclassificationOutcome {
    let (raw_observation, raw_count) =
        compute_raw_kalman_observation(input.capacity, input.raw_entries, input.now as f64);

    let kalman_confidence = if raw_count > 0 {
        calculate_confidence(raw_count as i64, input.last_forward_ts, input.now)
    } else {
        input.fallback_confidence
    };
    let has_observation = raw_count > 0;

    let filt = apply_kalman_filter(
        kf,
        raw_observation,
        kalman_confidence,
        input.daily_buckets,
        has_observation,
        input.now,
    );

    let kalman_converged = filt.uncertainty < KALMAN_CONVERGENCE_UNCERTAINTY
        && filt.observation_count >= KALMAN_MIN_OBSERVATIONS;

    let state = if !input.is_congested && kalman_converged {
        let mut source_thresh = input.source_threshold;
        let mut sink_thresh = input.sink_threshold;
        // DTS dampening: fee controller still exploratory -> widen flow
        // thresholds by 50%, biasing toward BALANCED.
        if let Some(variance) = input.posterior_variance {
            if variance > 10000.0 {
                source_thresh *= 1.5;
                sink_thresh *= 1.5;
            }
        }

        let outbound_ratio = if input.capacity > 0 {
            input.our_balance as f64 / input.capacity as f64
        } else {
            0.5
        };
        let turnover = if input.capacity > 0 {
            input.daily_volume as f64 / input.capacity as f64
        } else {
            0.0
        };

        Some(classification::flow_state(
            filt.flow_ratio,
            source_thresh,
            sink_thresh,
            outbound_ratio,
            input.previous_state,
            turnover,
        ))
    } else {
        None
    };

    ReclassificationOutcome {
        kalman_flow_ratio: filt.flow_ratio,
        kalman_velocity: filt.flow_velocity,
        kalman_uncertainty: filt.uncertainty,
        kalman_regime_change: filt.regime_change,
        step: filt.step,
        nan_recovery_delta: filt.nan_recovery_delta,
        state,
    }
}

// =============================================================================
// TemporalProfile
// =============================================================================

/// One hour-of-day bucket from the hourly forward histogram
/// (`_hourly_forward_histogram_sql`'s row shape), as fed to
/// `update_temporal_profile`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HourlyHistogramBucket {
    pub out_sats: f64,
    pub in_sats: f64,
    pub count: f64,
}

/// Per-channel hourly flow histogram for temporal pattern detection (port
/// of the Python `TemporalProfile` dataclass). Assumes numpy is present
/// (see module doc comment) — the Python `np is None` fallback branch
/// (defensive only; numpy is always installed in the real deployment) is
/// not reproduced.
#[derive(Clone, Debug, PartialEq)]
pub struct TemporalProfile {
    pub hourly_out: [f64; 24],
    pub hourly_in: [f64; 24],
    pub hourly_count: [f64; 24],
    pub peak_hours: Vec<i64>,
    pub quiet_hours: Vec<i64>,
    pub burstiness: f64,
    pub diurnal_strength: f64,
    pub dominant_bucket: String,
    pub observation_days: i64,
    pub last_observation_day: i64,
    pub last_updated: i64,
}

impl Default for TemporalProfile {
    fn default() -> Self {
        Self {
            hourly_out: [0.0; 24],
            hourly_in: [0.0; 24],
            hourly_count: [0.0; 24],
            peak_hours: Vec::new(),
            quiet_hours: Vec::new(),
            burstiness: 0.0,
            diurnal_strength: 0.0,
            dominant_bucket: "unknown".to_string(),
            observation_days: 0,
            last_observation_day: 0,
            last_updated: 0,
        }
    }
}

fn population_mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn population_std(values: &[f64], mean: f64) -> f64 {
    let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
    variance.sqrt()
}

impl TemporalProfile {
    /// `observation_days >= TEMPORAL_GRADUATION_DAYS`.
    pub fn graduated(&self) -> bool {
        self.observation_days >= TEMPORAL_GRADUATION_DAYS
    }

    /// Port of `TemporalProfile._recompute_derived`. `peak_hours`/
    /// `quiet_hours` are exact (pure sort/compare); `burstiness`/
    /// `diurnal_strength` reproduce numpy's mean/std/dot arithmetic with a
    /// straightforward Rust reduction — see the module doc comment on why
    /// these two are NOT guaranteed bit-exact against numpy's internal
    /// reduction order, and are fixture-pinned with an epsilon instead.
    fn recompute_derived(&mut self) {
        if self.hourly_out.iter().all(|&v| v == 0.0) {
            self.peak_hours.clear();
            self.quiet_hours.clear();
            self.burstiness = 0.0;
            self.diurnal_strength = 0.0;
            return;
        }

        let mean_val = population_mean(&self.hourly_out);
        self.burstiness = if mean_val > 0.0 {
            population_std(&self.hourly_out, mean_val) / mean_val
        } else {
            0.0
        };

        // Stable sort ascending by value (Python `sorted(enumerate(...),
        // key=lambda x: x[1])`) — Rust's `sort_by` is stable, matching
        // Python's Timsort tie behavior (original index order preserved).
        let mut indexed: Vec<(usize, f64)> = self.hourly_out.iter().copied().enumerate().collect();
        indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).expect("hourly_out must be finite"));

        let n_quartile = std::cmp::max(1, indexed.len() / 4);
        let mut quiet: Vec<i64> = indexed[..n_quartile]
            .iter()
            .map(|&(h, _)| h as i64)
            .collect();
        quiet.sort_unstable();
        let mut peak: Vec<i64> = indexed[indexed.len() - n_quartile..]
            .iter()
            .map(|&(h, _)| h as i64)
            .collect();
        peak.sort_unstable();
        self.quiet_hours = quiet;
        self.peak_hours = peak;

        let std_val = population_std(&self.hourly_out, mean_val);
        self.diurnal_strength = if self.hourly_out.len() == 24 && std_val > 0.0 {
            let normalized: Vec<f64> = self
                .hourly_out
                .iter()
                .map(|&v| (v - mean_val) / std_val)
                .collect();
            // np.roll(normalized, 12)[i] == normalized[(i - 12) mod 24];
            // for n=24 that's the same as normalized[(i + 12) mod 24].
            let dot: f64 = (0..24)
                .map(|i| normalized[i] * normalized[(i + 12) % 24])
                .sum();
            let autocorr_12 = dot / 24.0;
            f64::max(0.0, -autocorr_12)
        } else {
            0.0
        };
    }
}

/// Port of `update_temporal_profile`. `histogram` must have exactly 24
/// entries (one per hour-of-day), matching the Python contract (indexed
/// `histogram[h]` for `h in range(24)`).
pub fn update_temporal_profile(
    existing: &TemporalProfile,
    histogram: &[HourlyHistogramBucket; 24],
    daily_forwards: i64,
    now: i64,
) -> TemporalProfile {
    let mut updated = TemporalProfile::default();
    let is_first = existing.hourly_out.iter().all(|&v| v == 0.0);
    let alpha = TEMPORAL_EMA_ALPHA;

    for (h, bucket) in histogram.iter().enumerate() {
        let new_out = bucket.out_sats;
        let new_in = bucket.in_sats;
        let new_count = bucket.count;

        if is_first {
            updated.hourly_out[h] = new_out;
            updated.hourly_in[h] = new_in;
            updated.hourly_count[h] = new_count;
        } else {
            updated.hourly_out[h] = alpha * new_out + (1.0 - alpha) * existing.hourly_out[h];
            updated.hourly_in[h] = alpha * new_in + (1.0 - alpha) * existing.hourly_in[h];
            updated.hourly_count[h] = alpha * new_count + (1.0 - alpha) * existing.hourly_count[h];
        }
    }

    updated.dominant_bucket = existing.dominant_bucket.clone();
    updated.observation_days = existing.observation_days;
    updated.last_observation_day = existing.last_observation_day;

    let today = now.div_euclid(86400);
    if daily_forwards >= TEMPORAL_MIN_DAILY_FORWARDS && today != existing.last_observation_day {
        updated.observation_days += 1;
        updated.last_observation_day = today;
    }
    updated.last_updated = now;

    updated.recompute_derived();

    updated
}
