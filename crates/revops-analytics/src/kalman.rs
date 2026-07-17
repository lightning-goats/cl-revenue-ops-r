//! Kalman flow filter — bit-parity float math (port of the `KalmanFlowState`
//! / `KalmanFlowFilter` sections of `modules/flow_analysis.py`, plus the
//! module-level `_calculate_kalman_volatility`,
//! `_compute_raw_kalman_observation`, `_calculate_confidence`, and
//! `estimate_depletion_hours` helpers).
//!
//! Every constant and equation here is transcribed verbatim from the
//! Python, in the same arithmetic ORDER, per the Global Constraints in
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`: floats
//! stay bit-identical IEEE-754 doubles, so no algebraic simplification, no
//! `mul_add`, no re-association versus the source. This is a hand-rolled
//! 2x2 filter — no `nalgebra`.
//!
//! # Clock injection
//!
//! Python calls `int(time.time())` / `time.time()` INSIDE `update()`,
//! `_compute_raw_kalman_observation`, and `_calculate_confidence`. Every
//! Rust equivalent below takes the clock reading as an explicit parameter
//! instead, so callers (and the golden fixture replay in
//! `tests/kalman.rs`) can freeze it exactly like the Python fixture
//! generator did (`tools/port/gen_kalman_fixtures.py`, committed in the
//! cl_revenue_ops-port worktree, monkeypatches `flow_analysis.time.time`
//! for the same reason).
//!
//! # `to_dict` / `from_dict` representation
//!
//! Python's `KalmanFlowState.to_dict()` / `from_dict()` operate over a
//! plain `Dict[str, Any]` that can legitimately hold NaN/Inf as a *present*
//! value (Python's `json` module round-trips those with `allow_nan=True`).
//! `serde_json::Number` cannot represent NaN/Infinity at all
//! (`Number::from_f64` returns `None` for them), so a `serde_json::Value`
//! dict is the wrong carrier here — it would silently make the
//! present-vs-non-finite distinction unrepresentable. [`KalmanStateDict`]
//! is a plain struct of `Option<f64>` / `Option<i64>` fields instead: `None`
//! means "key absent" (falls back to the field's default, matching the I-7
//! `_safe()` fix's `is not None` check) and `Some(nan)` means "key present
//! with a non-finite value" (passes straight through unchanged — verified
//! against the real Python by direct execution: `_safe()` only ever
//! substitutes on a *missing* key, never on a non-finite *present* one).
//!
//! # `max(lo, min(hi, x))` instead of `x.clamp(lo, hi)`
//!
//! Every bound in this module is transcribed as Python's chained
//! `max(lo, min(hi, x))` (via `f64::max`/`f64::min`, IEEE-754 "propagate the
//! non-NaN operand" semantics), never `f64::clamp`. These are NOT
//! equivalent when `x` is NaN: `f64::min(hi, x)` returns `hi` (the non-NaN
//! operand), so the whole expression evaluates to `hi`; `x.clamp(lo, hi)`
//! instead returns `x` (NaN) unchanged. Python's own chained builtin
//! `max(lo, min(hi, x))` lands on `hi` too here (its `<`-based comparison
//! against a NaN candidate is always `False`, so the running candidate
//! never updates away from `hi`) — i.e. `f64::max`/`f64::min` is the
//! faithful transcription and `.clamp()` would be a silent behavioral
//! regression on the NaN path. `#[allow(clippy::manual_clamp)]` markers
//! below are intentional for this reason, not an oversight.

use revops_econ::pyfloat::py_pow;

// =============================================================================
// Constants (flow_analysis.py lines 69-129), transcribed with the exact
// same arithmetic expressions Python uses — do not fold the divisions.
// =============================================================================

pub const KALMAN_MAX_VELOCITY: f64 = 0.5 / 24.0;
pub const KALMAN_MIN_VELOCITY: f64 = -0.5 / 24.0;

pub const KALMAN_BASE_PROCESS_NOISE: f64 = 0.01 / 24.0;
pub const KALMAN_VELOCITY_PROCESS_NOISE: f64 = 0.005 / 24.0;
pub const KALMAN_MIN_PROCESS_NOISE: f64 = 0.001 / 24.0;
pub const KALMAN_MAX_PROCESS_NOISE: f64 = 0.1 / 24.0;

pub const KALMAN_BASE_MEASUREMENT_NOISE: f64 = 0.05;
pub const KALMAN_MIN_MEASUREMENT_NOISE: f64 = 0.01;
pub const KALMAN_MAX_MEASUREMENT_NOISE: f64 = 0.5;

pub const KALMAN_INITIAL_VARIANCE: f64 = 0.1;

pub const KALMAN_VOLATILITY_SCALING: f64 = 2.0;
pub const KALMAN_CONFIDENCE_SCALING: f64 = 0.8;

pub const KALMAN_CONVERGENCE_UNCERTAINTY: f64 = 0.25;
pub const KALMAN_MIN_OBSERVATIONS: i64 = 5;

/// The no-touch gate (T7 consumes it): a call arriving within this many
/// seconds of the filter's last consume must leave the filter untouched.
pub const KALMAN_MIN_UPDATE_INTERVAL_SECS: i64 = 300;

pub const MIN_CONFIDENCE: f64 = 0.1;
pub const MAX_CONFIDENCE: f64 = 1.0;
pub const MIN_FORWARDS_FOR_HIGH_CONFIDENCE: i64 = 20;
pub const CONFIDENCE_RECENCY_HALFLIFE_DAYS: f64 = 3.0;

pub const DEPLETION_MIN_DRAIN_SATS_PER_DAY: f64 = 1.0;

// =============================================================================
// KalmanFlowState
// =============================================================================

#[derive(Clone, Debug, PartialEq)]
pub struct KalmanFlowState {
    pub flow_ratio: f64,
    pub flow_velocity: f64,
    pub variance_ratio: f64,
    pub variance_velocity: f64,
    pub covariance: f64,
    pub last_update: i64,
    pub innovation_variance: f64,
    pub last_innovation: f64,
    pub observation_count: i64,
}

impl Default for KalmanFlowState {
    fn default() -> Self {
        Self {
            flow_ratio: 0.0,
            flow_velocity: 0.0,
            variance_ratio: KALMAN_INITIAL_VARIANCE,
            variance_velocity: KALMAN_INITIAL_VARIANCE,
            covariance: 0.0,
            last_update: 0,
            innovation_variance: 0.01,
            last_innovation: 0.0,
            observation_count: 0,
        }
    }
}

/// The `Dict[str, Any]` carrier for [`KalmanFlowState::to_dict`] /
/// [`KalmanFlowState::from_dict`] — see the module doc comment for why this
/// is a struct of `Option`s rather than `serde_json::Value`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KalmanStateDict {
    pub flow_ratio: Option<f64>,
    pub flow_velocity: Option<f64>,
    pub variance_ratio: Option<f64>,
    pub variance_velocity: Option<f64>,
    pub covariance: Option<f64>,
    pub last_update: Option<i64>,
    pub innovation_variance: Option<f64>,
    pub last_innovation: Option<f64>,
    pub observation_count: Option<i64>,
}

impl KalmanFlowState {
    pub fn to_dict(&self) -> KalmanStateDict {
        KalmanStateDict {
            flow_ratio: Some(self.flow_ratio),
            flow_velocity: Some(self.flow_velocity),
            variance_ratio: Some(self.variance_ratio),
            variance_velocity: Some(self.variance_velocity),
            covariance: Some(self.covariance),
            last_update: Some(self.last_update),
            innovation_variance: Some(self.innovation_variance),
            last_innovation: Some(self.last_innovation),
            observation_count: Some(self.observation_count),
        }
    }

    /// I-7 fix semantics: a MISSING key (`None` in the dict) falls back to
    /// the field's default; a PRESENT key — even a non-finite one — passes
    /// straight through unchanged. Confirmed against the real Python by
    /// direct execution (see module doc comment); do not "fix" this to also
    /// scrub non-finite present values, that is not what `_safe()` does.
    pub fn from_dict(d: &KalmanStateDict) -> Self {
        Self {
            flow_ratio: d.flow_ratio.unwrap_or(0.0),
            flow_velocity: d.flow_velocity.unwrap_or(0.0),
            variance_ratio: d.variance_ratio.unwrap_or(KALMAN_INITIAL_VARIANCE),
            variance_velocity: d.variance_velocity.unwrap_or(KALMAN_INITIAL_VARIANCE),
            covariance: d.covariance.unwrap_or(0.0),
            // Python: `int(d.get("last_update") or 0)` — an explicit `None`
            // or absent key both land on 0 (falsy-or-missing collapse).
            last_update: d.last_update.unwrap_or(0),
            innovation_variance: d.innovation_variance.unwrap_or(0.01),
            last_innovation: d.last_innovation.unwrap_or(0.0),
            observation_count: d.observation_count.unwrap_or(0),
        }
    }
}

// =============================================================================
// KalmanFlowFilter
// =============================================================================

pub struct KalmanFlowFilter {
    pub state: KalmanFlowState,
    nan_recovery_count: u32,
}

impl Default for KalmanFlowFilter {
    fn default() -> Self {
        Self::new(None)
    }
}

impl KalmanFlowFilter {
    pub fn new(state: Option<KalmanFlowState>) -> Self {
        Self {
            state: state.unwrap_or_default(),
            nan_recovery_count: 0,
        }
    }

    fn reset_state(&mut self) {
        self.nan_recovery_count += 1;
        self.state = KalmanFlowState::default();
    }

    fn has_nan(&self) -> bool {
        !(self.state.flow_ratio.is_finite()
            && self.state.flow_velocity.is_finite()
            && self.state.variance_ratio.is_finite()
            && self.state.variance_velocity.is_finite()
            && self.state.covariance.is_finite()
            && self.state.innovation_variance.is_finite()
            && self.state.last_innovation.is_finite())
    }

    fn ensure_positive_definite(&mut self) {
        self.state.variance_ratio = f64::max(1e-4, self.state.variance_ratio);
        self.state.variance_velocity = f64::max(1e-4, self.state.variance_velocity);
        let det = self.state.variance_ratio * self.state.variance_velocity
            - self.state.covariance.powi(2);
        if det <= 0.0 {
            let max_cov = (self.state.variance_ratio * self.state.variance_velocity).sqrt() * 0.9;
            self.state.covariance = f64::max(-max_cov, f64::min(max_cov, self.state.covariance));
        }
    }

    /// dt<=0 -> no-op; NaN anywhere in the incoming state -> reset to fresh
    /// defaults and return (checked BEFORE any arithmetic, unlike
    /// `update()`).
    #[allow(clippy::manual_clamp)] // see module doc comment: NaN-propagation parity with Python
    pub fn predict(&mut self, dt_hours: f64, volatility: f64) {
        if dt_hours <= 0.0 {
            return;
        }

        if self.has_nan() {
            self.reset_state();
            return;
        }

        self.state.flow_ratio += self.state.flow_velocity * dt_hours;

        self.state.flow_ratio = f64::max(-1.0, f64::min(1.0, self.state.flow_ratio));
        self.state.flow_velocity = f64::max(
            KALMAN_MIN_VELOCITY,
            f64::min(KALMAN_MAX_VELOCITY, self.state.flow_velocity),
        );

        let mut q_ratio = KALMAN_BASE_PROCESS_NOISE * volatility * KALMAN_VOLATILITY_SCALING;
        q_ratio = f64::max(
            KALMAN_MIN_PROCESS_NOISE,
            f64::min(KALMAN_MAX_PROCESS_NOISE, q_ratio),
        );

        let mut q_velocity = KALMAN_VELOCITY_PROCESS_NOISE * volatility;
        q_velocity = f64::max(
            KALMAN_MIN_PROCESS_NOISE / 10.0,
            f64::min(KALMAN_MAX_PROCESS_NOISE / 10.0, q_velocity),
        );

        let p00 = self.state.variance_ratio;
        let p01 = self.state.covariance;
        let p11 = self.state.variance_velocity;

        let new_p00 = p00
            + 2.0 * dt_hours * p01
            + dt_hours * dt_hours * p11
            + q_ratio * dt_hours
            + q_velocity * dt_hours * dt_hours * dt_hours / 3.0;
        let new_p01 = p01 + dt_hours * p11 + q_velocity * dt_hours * dt_hours / 2.0;
        let new_p11 = p11 + q_velocity * dt_hours;

        self.state.variance_ratio = new_p00;
        self.state.covariance = new_p01;
        self.state.variance_velocity = new_p11;

        self.ensure_positive_definite();
    }

    /// Non-finite observation -> return 0.0 WITHOUT touching state (B2
    /// fix). Otherwise runs the full Joseph-ish update; the NaN guard here
    /// is checked LAST (unlike `predict()`), so an already-poisoned
    /// incoming state propagates through the whole computation before
    /// being caught and reset.
    #[allow(clippy::manual_clamp)] // see module doc comment: NaN-propagation parity with Python
    pub fn update(&mut self, observed_ratio: f64, confidence: f64, now: i64) -> f64 {
        if !observed_ratio.is_finite() {
            return 0.0;
        }

        let mut r =
            KALMAN_BASE_MEASUREMENT_NOISE / f64::max(0.1, confidence * KALMAN_CONFIDENCE_SCALING);
        r = f64::max(
            KALMAN_MIN_MEASUREMENT_NOISE,
            f64::min(KALMAN_MAX_MEASUREMENT_NOISE, r),
        );

        let innovation = observed_ratio - self.state.flow_ratio;

        let mut s = self.state.variance_ratio + r;
        if s < 1e-10 {
            s = 1e-10;
        }

        let k0 = self.state.variance_ratio / s;
        let k1 = self.state.covariance / s;

        self.state.flow_ratio += k0 * innovation;
        self.state.flow_velocity += k1 * innovation;

        let p00 = self.state.variance_ratio;
        let p01 = self.state.covariance;
        let p11 = self.state.variance_velocity;

        // Joseph-ish form — NOT the textbook symmetric formula, transcribed
        // verbatim (flow_analysis.py lines 601-603).
        let new_p00 = (1.0 - k0) * p00 * (1.0 - k0) + k0 * k0 * r;
        let new_p01 = (1.0 - k0) * p01 - k1 * (1.0 - k0) * p00 + k0 * k1 * r;
        let new_p11 = p11 - k1 * p01 - k1 * (p01 - k1 * p00) + k1 * k1 * r;

        self.state.variance_ratio = new_p00;
        self.state.covariance = new_p01;
        self.state.variance_velocity = new_p11;

        self.ensure_positive_definite();

        // Bound state AFTER the positive-definite fix (order matters).
        self.state.flow_ratio = f64::max(-1.0, f64::min(1.0, self.state.flow_ratio));
        self.state.flow_velocity = f64::max(
            KALMAN_MIN_VELOCITY,
            f64::min(KALMAN_MAX_VELOCITY, self.state.flow_velocity),
        );

        self.state.last_innovation = innovation;
        self.state.innovation_variance = f64::max(
            0.001,
            0.9 * self.state.innovation_variance + 0.1 * innovation * innovation,
        );

        self.state.last_update = now;
        self.state.observation_count += 1;

        if self.has_nan() {
            self.reset_state();
            return 0.0;
        }

        innovation
    }

    pub fn get_uncertainty(&self) -> f64 {
        f64::max(0.0, self.state.variance_ratio).sqrt()
    }

    pub fn is_regime_change(&self, threshold: f64) -> bool {
        let expected_innovation_std = f64::max(0.001, self.state.innovation_variance).sqrt();
        self.state.last_innovation.abs() > threshold * expected_innovation_std
    }

    /// Read-and-reset the NaN-recovery counter (mirrors the Python
    /// wrapper's manual `n = kf._nan_recovery_count; kf._nan_recovery_count
    /// = 0` pattern in `_apply_kalman_filter`).
    pub fn take_nan_recovery_count(&mut self) -> u32 {
        std::mem::take(&mut self.nan_recovery_count)
    }

    /// Task 7 (`flow::apply_kalman_filter`) idiom: `if kf._has_nan():
    /// kf._reset_state()` — the AUDIT FIX I-3 guard run after a
    /// predict-only step that bypasses `update()`'s own after-the-fact NaN
    /// check (`_apply_kalman_filter`'s `has_observation=False` branch sets
    /// `kf.state.last_update` directly instead of calling `update()`).
    /// Returns `true` iff a reset occurred, incrementing the same counter
    /// `predict()`/`update()`'s internal guards use.
    pub fn reset_if_nan(&mut self) -> bool {
        if self.has_nan() {
            self.reset_state();
            true
        } else {
            false
        }
    }
}

// =============================================================================
// Module-level helpers (flow_analysis.py: FlowAnalyzer._calculate_kalman_volatility,
// FlowAnalyzer._compute_raw_kalman_observation, FlowAnalyzer._calculate_confidence,
// estimate_depletion_hours). Pure-ified: no `self`, no DB/clock reads inline.
// =============================================================================

/// One day's aggregated net flow, as fed to
/// `_calculate_kalman_volatility`. Values are floats to match Python's
/// unconditional float arithmetic on whatever numeric type the daily
/// aggregation produced.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DailyBucket {
    pub out: f64,
    pub in_: f64,
}

/// Volatility multiplier (0.5 to 2.0) for Kalman process-noise adaptation.
pub fn calculate_kalman_volatility(daily_buckets: &[DailyBucket]) -> f64 {
    if daily_buckets.len() < 3 {
        return 1.0;
    }

    let net_flows: Vec<f64> = daily_buckets.iter().map(|b| b.out - b.in_).collect();

    let changes: Vec<f64> = (1..net_flows.len())
        .map(|i| (net_flows[i] - net_flows[i - 1]).abs())
        .collect();

    let mean_change: f64 = changes.iter().sum::<f64>() / changes.len() as f64;
    let mean_flow: f64 = net_flows.iter().map(|nf| nf.abs()).sum::<f64>() / net_flows.len() as f64;

    if mean_flow < 1000.0 {
        return 0.5;
    }

    let cv = mean_change / f64::max(1.0, mean_flow);

    0.5 + f64::min(1.5, cv * 3.0)
}

/// A single net-flow ledger entry, as fed to
/// `_compute_raw_kalman_observation`. `timestamp` is a Unix-epoch-seconds
/// float (matches Python's `time.time()`-comparable entries); `net_msat`
/// stays integer msat, converted to sats via FLOAT division per entry
/// (never an integer sum then divide).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NetFlowEntry {
    pub timestamp: f64,
    pub net_msat: i64,
}

/// CPython 3.12+ special-cases `sum()` over an all-float stream to use
/// Neumaier (improved Kahan-Babuska) compensated summation instead of naive
/// left-to-right addition (gh-100425). This is NOT a cosmetic accuracy
/// improvement we can ignore for bit-parity purposes: it changes the
/// resulting bit pattern versus naive summation whenever the addends
/// aren't all exact integers (verified empirically — see the Task 3 report
/// — a real `raw_observation_cases` fixture case (`rand01`) differs from
/// naive `Iterator::sum()` by 2 ULPs; this function reproduces Python's
/// `sum(e.net_msat / 1000.0 for e in recent_entries)` exactly, 0 mismatches
/// across 4000 randomized cross-checks against live CPython). Do not
/// replace this with `.sum()` — that is a silent bit-parity regression.
///
/// `_calculate_kalman_volatility`'s sums, by contrast, sum Python `int`
/// values (the module's `Dict[str, int]` daily-bucket contract) — CPython
/// keeps an all-integer `sum()` as an exact arbitrary-precision integer, so
/// no compensation is needed there; a plain `f64` fold is exact as long as
/// every addend is itself an exact integer-valued float (true here, and
/// covered by `volatility_cases_match_python`).
///
/// `pub(crate)`: Task 7's `flow::calculate_adaptive_decay` needs the exact
/// same compensated-summation primitive for its own float-valued `sum()`
/// genexpr (the `(x - mean_net) ** 2` variance term) — same CPython 3.12+
/// rule, different call site. One implementation, two internal callers.
pub(crate) fn neumaier_sum(values: impl Iterator<Item = f64>) -> f64 {
    let mut s = 0.0_f64;
    let mut c = 0.0_f64;
    for x in values {
        let t = s + x;
        if s.abs() >= x.abs() {
            c += (s - t) + x;
        } else {
            c += (x - t) + s;
        }
        s = t;
    }
    s + c
}

/// 24-hour rolling window over `entries`; falls back to `(0.0, 0)` on no
/// data. Ratio clamped to `[-1, 1]`.
#[allow(clippy::manual_clamp)] // see module doc comment: NaN-propagation parity with Python
pub fn compute_raw_kalman_observation(
    capacity: i64,
    entries: &[NetFlowEntry],
    now: f64,
) -> (f64, usize) {
    if entries.is_empty() || capacity <= 0 {
        return (0.0, 0);
    }

    let recent: Vec<&NetFlowEntry> = entries
        .iter()
        .filter(|e| (now - e.timestamp) <= 86400.0)
        .collect();

    if recent.is_empty() {
        return (0.0, 0);
    }

    let net_sats_24h: f64 = neumaier_sum(recent.iter().map(|e| e.net_msat as f64 / 1000.0));
    let raw_ratio = net_sats_24h / capacity as f64;

    (f64::max(-1.0, f64::min(1.0, raw_ratio)), recent.len())
}

/// Observation confidence: `count_factor * recency_factor`, bounded to
/// `[MIN_CONFIDENCE, MAX_CONFIDENCE]`.
#[allow(clippy::manual_clamp)] // see module doc comment: NaN-propagation parity with Python
pub fn calculate_confidence(forward_count: i64, last_forward_ts: i64, now: i64) -> f64 {
    let count_factor = if forward_count >= MIN_FORWARDS_FOR_HIGH_CONFIDENCE {
        1.0
    } else {
        MIN_CONFIDENCE
            + (1.0 - MIN_CONFIDENCE)
                * (forward_count as f64 / MIN_FORWARDS_FOR_HIGH_CONFIDENCE as f64)
    };

    let recency_factor = if last_forward_ts > 0 {
        let days_since = (now - last_forward_ts) as f64 / 86400.0;
        let halflife = CONFIDENCE_RECENCY_HALFLIFE_DAYS;
        // `py_pow(0.5, _)`, not a bare `0.5f64.powf(_)`: LLVM rewrites a
        // constant-`0.5`-base `powf` into an `exp2` form under `-O2`+,
        // which diverges from CPython's `**` in the last bit for ~0.11%
        // of inputs — see `revops_econ::pyfloat::py_pow`'s doc comment.
        py_pow(0.5, days_since / halflife)
    } else {
        MIN_CONFIDENCE
    };

    let confidence = count_factor * recency_factor;

    f64::max(MIN_CONFIDENCE, f64::min(MAX_CONFIDENCE, confidence))
}

/// Hours until local liquidity depletes at the current drain rate. Unit
/// contract (flow_analysis.py lines 166-210): `kalman_ratio` is net flow
/// per DAY as a fraction of capacity; `kalman_velocity` is ratio change per
/// HOUR. Returns `None` on invalid inputs or when net drain is at/below
/// [`DEPLETION_MIN_DRAIN_SATS_PER_DAY`].
pub fn estimate_depletion_hours(
    local_sats: f64,
    capacity_sats: f64,
    kalman_ratio: f64,
    kalman_velocity: f64,
) -> Option<f64> {
    if !(local_sats.is_finite()
        && capacity_sats.is_finite()
        && kalman_ratio.is_finite()
        && kalman_velocity.is_finite())
    {
        return None;
    }
    if capacity_sats <= 0.0 || local_sats < 0.0 {
        return None;
    }

    let mut net_drain_sats_per_day = f64::max(0.0, kalman_ratio) * capacity_sats;
    net_drain_sats_per_day += kalman_velocity * 24.0 * capacity_sats / 2.0;
    net_drain_sats_per_day = f64::max(0.0, net_drain_sats_per_day);

    if net_drain_sats_per_day <= DEPLETION_MIN_DRAIN_SATS_PER_DAY {
        return None;
    }

    Some((local_sats / net_drain_sats_per_day) * 24.0)
}
