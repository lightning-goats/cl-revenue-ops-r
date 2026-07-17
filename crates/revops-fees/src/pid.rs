//! PI inventory controller (port of the PID controller state + update in
//! `modules/fee_controller.py`).
//!
//! Filled in by Phase 4 Task 3 (Wave 1).
//!
//! Mirrors `PIDState` (py 1964-2043) and `_PID_TARGET_RATIOS` (py
//! 1953-1961). Despite the class name, `kd`/`d_term` are dead weight kept
//! for blob compatibility only: `d_term` is hardcoded `0.0` in
//! `calculate_multiplier` (py 2015) — this is a P(I) controller, not PID,
//! in every code path that actually runs.
//!
//! Clock injection (Phase 4 Global Constraints): the Python method reads
//! `time.time()` internally (py 1983); the ported function takes `now: i64`
//! instead.

use crate::pyjson::OValue;

/// Target outbound-liquidity ratio per flow-classifier state (`fee_controller.py`
/// `_PID_TARGET_RATIOS`, py 1953-1961). Unknown/unlisted flow states — including
/// `"router"`, reserved vocabulary the classifier does not emit yet — fall back
/// to `0.5` via `.get(flow_state, 0.5)`.
const PID_TARGET_RATIOS: &[(&str, f64)] = &[
    ("source", 0.7),
    ("sink", 0.3),
    ("balanced", 0.5),
    ("balanced_active", 0.5),
    ("dormant", 0.5),
    ("congested", 0.5),
    ("unknown", 0.5),
];

/// `_PID_TARGET_RATIOS.get(flow_state, 0.5)`.
fn target_ratio(flow_state: &str) -> f64 {
    PID_TARGET_RATIOS
        .iter()
        .find(|(name, _)| *name == flow_state)
        .map(|(_, ratio)| *ratio)
        .unwrap_or(0.5)
}

/// PID controller state for channel balance management (`PIDState`, py
/// 1964-1975). Field defaults mirror the Python dataclass defaults exactly.
#[derive(Debug, Clone, PartialEq)]
pub struct PidState {
    pub kp: f64,
    pub ki: f64,
    pub kd: f64,
    pub ewma_error: f64,
    pub integral_error: f64,
    pub prev_ewma_error: f64,
    pub last_update_time: i64,
    pub integral_clamp: f64,
}

impl Default for PidState {
    fn default() -> Self {
        PidState {
            kp: 2.0,
            ki: 0.1,
            kd: 0.0,
            ewma_error: 0.0,
            integral_error: 0.0,
            prev_ewma_error: 0.0,
            last_update_time: 0,
            integral_clamp: 3.0,
        }
    }
}

/// `_EWMA_ALPHA` class constant (py 1975): not persisted, not overridable.
const EWMA_ALPHA: f64 = 0.3;

/// Port of `PIDState.calculate_multiplier` (py 1977-2020), verbatim:
///
/// - `dt = 0.0` on the first update (`last_update_time <= 0`); otherwise
///   `max((now - last_update_time) / 3600.0, 0.0)`.
/// - `target = _PID_TARGET_RATIOS.get(flow_state, 0.5)`.
/// - a non-finite `current_outbound_ratio` (NaN/±inf) is replaced by
///   `target` before computing the raw error.
/// - `ewma_error = ALPHA*raw_error + (1-ALPHA)*ewma_error`.
/// - `scale = 1 / log2(max(capacity_sats, 1) / 1_000_000 + 2)`.
/// - `integral_error += ewma_error * dt`, clamped to `±integral_clamp`,
///   **only when `dt > 0`** (a fresh/first-call state never touches the
///   integral term).
/// - `multiplier = clamp(1.5 ** (p_term + i_term + d_term), 0.5, 2.0)` with
///   `d_term` hardcoded `0.0` (py 2015).
///
/// `prev_ewma_error` and `last_update_time` are updated as a side effect,
/// matching the Python method mutating `self` in place.
pub fn calculate_multiplier(
    s: &mut PidState,
    current_outbound_ratio: f64,
    capacity_sats: i64,
    flow_state: &str,
    now: i64,
) -> f64 {
    let dt = if s.last_update_time <= 0 {
        0.0
    } else {
        ((now - s.last_update_time) as f64 / 3600.0).max(0.0)
    };
    s.last_update_time = now;

    let target = target_ratio(flow_state);

    let ratio = if current_outbound_ratio.is_finite() {
        current_outbound_ratio
    } else {
        target
    };
    let raw_error = target - ratio;

    s.ewma_error = EWMA_ALPHA * raw_error + (1.0 - EWMA_ALPHA) * s.ewma_error;

    let scale = 1.0 / ((capacity_sats.max(1) as f64) / 1_000_000.0 + 2.0).log2();
    let eff_kp = s.kp * scale;
    let eff_ki = s.ki * scale;
    let p_term = eff_kp * s.ewma_error;

    if dt > 0.0 {
        s.integral_error += s.ewma_error * dt;
        s.integral_error = s.integral_error.clamp(-s.integral_clamp, s.integral_clamp);
    }
    let i_term = eff_ki * s.integral_error;

    let d_term = 0.0;
    s.prev_ewma_error = s.ewma_error;

    let output = p_term + i_term + d_term;
    // Deliberately left as a bare `.powf(output)`, NOT migrated to
    // `revops_econ::pyfloat::py_pow`: this is a constant-`1.5`-base call,
    // and the workspace-wide float-hardening audit (T6 review adjudication)
    // empirically checked this exact shape against this toolchain and found
    // LLVM does NOT rewrite it (only `.powf(0.5)`, `.powf(2.0)`, and a
    // constant-`0.5`-base `powf(y)` are rewritten under `-O2`+). Migrating
    // sites that are not actually at risk would only add uniformity churn
    // that risks re-baking this crate's fixtures for zero parity benefit —
    // see `py_pow`'s doc comment for the full story.
    let multiplier = 1.5_f64.powf(output);
    multiplier.clamp(0.5, 2.0)
}

/// `PIDState.to_dict` (py 2022-2030) — exact key order.
pub fn pid_to_dict(s: &PidState) -> OValue {
    OValue::obj(vec![
        ("kp".to_string(), OValue::Float(s.kp)),
        ("ki".to_string(), OValue::Float(s.ki)),
        ("kd".to_string(), OValue::Float(s.kd)),
        ("ewma_error".to_string(), OValue::Float(s.ewma_error)),
        (
            "integral_error".to_string(),
            OValue::Float(s.integral_error),
        ),
        (
            "prev_ewma_error".to_string(),
            OValue::Float(s.prev_ewma_error),
        ),
        (
            "last_update_time".to_string(),
            OValue::Int(s.last_update_time),
        ),
        (
            "integral_clamp".to_string(),
            OValue::Float(s.integral_clamp),
        ),
    ])
}

/// `PIDState.from_dict` (py 2032-2043): every field is `float(d.get(key,
/// default))` / `int(d.get(key, default))`, i.e. an explicit cast — unlike
/// `GaussianThompsonState`'s number-passthrough fields, there is no
/// int/float ambiguity to preserve here. This port additionally falls back
/// to the default on a non-numeric value (mirroring Python's
/// `TypeError`/`ValueError` -> the call would raise; since callers of this
/// port never construct a blob that way, falling back is the closest safe
/// analogue rather than panicking).
pub fn pid_from_dict(d: &OValue) -> PidState {
    let default = PidState::default();
    PidState {
        kp: num_or(d, "kp", default.kp),
        ki: num_or(d, "ki", default.ki),
        kd: num_or(d, "kd", default.kd),
        ewma_error: num_or(d, "ewma_error", default.ewma_error),
        integral_error: num_or(d, "integral_error", default.integral_error),
        prev_ewma_error: num_or(d, "prev_ewma_error", default.prev_ewma_error),
        last_update_time: int_or(d, "last_update_time", default.last_update_time),
        integral_clamp: num_or(d, "integral_clamp", default.integral_clamp),
    }
}

fn num_or(d: &OValue, key: &str, default: f64) -> f64 {
    match d.get(key) {
        Some(v) => v
            .as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(default),
        None => default,
    }
}

fn int_or(d: &OValue, key: &str, default: i64) -> i64 {
    match d.get(key) {
        Some(v) => v
            .as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(default),
        None => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_ratio_defaults_unknown_flow_state_to_half() {
        assert_eq!(target_ratio("source"), 0.7);
        assert_eq!(target_ratio("sink"), 0.3);
        assert_eq!(target_ratio("router"), 0.5);
        assert_eq!(target_ratio("totally_unknown"), 0.5);
    }

    #[test]
    fn dict_round_trip_preserves_fields() {
        let s = PidState {
            kp: 2.5,
            ki: 0.2,
            kd: 0.0,
            ewma_error: 0.09,
            integral_error: 0.01,
            prev_ewma_error: 0.05,
            last_update_time: 1_752_400_000,
            integral_clamp: 3.0,
        };
        let d = pid_to_dict(&s);
        let s2 = pid_from_dict(&d);
        assert_eq!(s, s2);
    }
}
