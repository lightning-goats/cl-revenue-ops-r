//! Growth-budget math: `compute_growth_budget_status`, `_fleet_prior_status`
//! (port of `modules/growth_budget.py`).
//!
//! Task 4 (Wave 1) of
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`.
//!
//! Intentionally pure: no runtime state, no config mutation, no action
//! authorization. Callers remain responsible for the atomic reservation
//! rail that enforces the returned local ceiling.
//!
//! Note on typed vs. duck-typed inputs: Python's `growth_budget.py` runs
//! every numeric input (including nominally-`int`/`float`-typed function
//! parameters) through defensive `_safe_int`/`_fraction` coercion because
//! Python's type hints are not enforced at the call boundary. Rust's type
//! system already enforces `i64`/`f64`/`bool` at the [`GrowthBudgetInputs`]
//! boundary, so the only genuinely duck-typed value left is `fleet_prior`
//! (an externally-sourced, loosely-shaped blob — modeled here as
//! `serde_json::Value` to preserve the exact malformed-input behaviors the
//! Python source guards against, notably the `bool` rejection for
//! `beneficial_ratio`).

use revops_econ::pyfloat::py_round;
use serde_json::Value;

const MIN_PRIOR_SAMPLES: i64 = 3;
const MIN_BENEFICIAL_RATIO: f64 = 0.50;

// =============================================================================
// Duck-typed helpers (fleet_prior only — see module doc comment)
// =============================================================================

/// Python `bool(x)` truthiness over a JSON value.
fn py_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// `_safe_int`: bool is never a valid int (rejected -> default 0); numbers
/// truncate toward zero; numeric strings parse; anything else -> 0.
fn py_safe_int(v: Option<&Value>) -> i64 {
    match v {
        Some(Value::Bool(_)) | None => 0,
        Some(Value::Number(n)) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .unwrap_or(0),
        Some(Value::String(s)) => s.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

/// `_non_negative_int`: `max(0, _safe_int(value))`.
fn py_non_negative_int(v: Option<&Value>) -> i64 {
    py_safe_int(v).max(0)
}

/// Python `float(x)` conversion for the subset of JSON shapes that can
/// legitimately reach `beneficial_ratio` (bool is checked and rejected by
/// the caller BEFORE this is invoked, matching the Python
/// `isinstance(..., bool)` guard that runs before the `float()` call).
fn py_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

// =============================================================================
// FleetPriorStatus
// =============================================================================

/// Reasons a fleet prior was or wasn't used — frozen wire strings.
pub const REASON_MISSING: &str = "missing";
pub const REASON_UNUSABLE: &str = "unusable";
pub const REASON_INSUFFICIENT_SAMPLES: &str = "insufficient_samples";
pub const REASON_MALFORMED_RATIO: &str = "malformed_ratio";
pub const REASON_NON_POSITIVE_PRIOR: &str = "non_positive_prior";
pub const REASON_POSITIVE_PRIOR: &str = "positive_prior";

/// Port of `growth_budget._fleet_prior_status`'s return dict.
#[derive(Debug, Clone, PartialEq)]
pub struct FleetPriorStatus {
    pub present: bool,
    pub usable: bool,
    pub used: bool,
    pub reason: &'static str,
    pub sample_count: i64,
    pub beneficial_ratio: Option<f64>,
}

impl FleetPriorStatus {
    /// Insertion-order key/value pairs (Python dict literal order) for
    /// telemetry serializers that must preserve order.
    pub fn to_ordered_pairs(&self) -> Vec<(&'static str, Value)> {
        vec![
            ("present", Value::Bool(self.present)),
            ("usable", Value::Bool(self.usable)),
            ("used", Value::Bool(self.used)),
            ("reason", Value::String(self.reason.to_string())),
            ("sample_count", Value::from(self.sample_count)),
            (
                "beneficial_ratio",
                match self.beneficial_ratio {
                    Some(r) => serde_json::Number::from_f64(r)
                        .map(Value::Number)
                        .unwrap_or(Value::Null),
                    None => Value::Null,
                },
            ),
        ]
    }
}

/// Port of `growth_budget._fleet_prior_status`.
///
/// `fleet_prior` mirrors Python's `Optional[Dict[str, Any]]`: `None`, a
/// non-object, or an empty object are all "missing" (Python:
/// `isinstance(fleet_prior, dict) and fleet_prior` — an empty dict is
/// falsy too).
pub fn fleet_prior_status(fleet_prior: Option<&Value>) -> FleetPriorStatus {
    let mut status = FleetPriorStatus {
        present: false,
        usable: false,
        used: false,
        reason: REASON_MISSING,
        sample_count: 0,
        beneficial_ratio: None,
    };

    let obj = match fleet_prior.and_then(|v| v.as_object()) {
        Some(map) if !map.is_empty() => map,
        _ => return status,
    };

    status.present = true;
    status.reason = REASON_UNUSABLE;

    let usable = obj.get("usable").map(py_truthy).unwrap_or(false);
    if !usable {
        return status;
    }

    let sample_count = py_non_negative_int(obj.get("sample_count"));
    status.sample_count = sample_count;
    if sample_count < MIN_PRIOR_SAMPLES {
        status.reason = REASON_INSUFFICIENT_SAMPLES;
        return status;
    }

    let ratio_val = obj.get("beneficial_ratio");
    if matches!(ratio_val, Some(Value::Bool(_))) {
        status.reason = REASON_MALFORMED_RATIO;
        return status;
    }
    let ratio = match ratio_val.and_then(py_as_f64) {
        Some(r) if r.is_finite() => r,
        _ => {
            status.reason = REASON_MALFORMED_RATIO;
            return status;
        }
    };

    let ratio = ratio.clamp(0.0, 1.0);
    status.beneficial_ratio = Some(py_round(ratio, 4));
    status.usable = true;
    if ratio <= MIN_BENEFICIAL_RATIO {
        status.reason = REASON_NON_POSITIVE_PRIOR;
        return status;
    }

    status.used = true;
    status.reason = REASON_POSITIVE_PRIOR;
    status
}

// =============================================================================
// compute_growth_budget_status
// =============================================================================

/// Typed inputs to [`compute_growth_budget_status`] (a struct rather than
/// Python's long kwarg list — both to keep clippy's `too_many_arguments`
/// quiet and because Rust's type system already gives us the int/float/bool
/// discipline Python enforces defensively at runtime).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GrowthBudgetInputs {
    pub base_budget_sats: i64,
    pub net_profit_sats: i64,
    pub actual_spent_sats: i64,
    pub reserved_sats: i64,
    pub enabled: bool,
    pub earned_fraction: f64,
    pub growth_fraction: f64,
    pub growth_max_extra_sats: i64,
    pub hard_ceiling_sats: i64,
}

pub const MODE_FIXED: &str = "fixed";
pub const MODE_DYNAMIC_GROWTH: &str = "dynamic_growth";
pub const AUTHORITY_LOCAL: &str = "local";

/// Port of `growth_budget.compute_growth_budget_status`'s return dict.
#[derive(Debug, Clone, PartialEq)]
pub struct GrowthBudgetStatus {
    pub mode: &'static str,
    pub authority: &'static str,
    pub advisory_only: bool,
    pub fleet_prior_budget_authority: bool,
    pub base_budget_sats: i64,
    pub local_hard_ceiling_sats: i64,
    pub earned_credit_sats: i64,
    pub growth_credit_sats: i64,
    pub growth_credit_cap_sats: i64,
    pub effective_budget_sats: i64,
    pub actual_spent_sats: i64,
    pub reserved_sats: i64,
    pub remaining_sats: i64,
    pub capped_by_hard_ceiling: bool,
    pub fleet_prior: FleetPriorStatus,
}

impl GrowthBudgetStatus {
    /// Insertion-order key/value pairs (Python dict literal order) for
    /// telemetry serializers that must preserve order (key ORDER is part
    /// of the frozen contract here, shared with the Task 8 telemetry
    /// dumper).
    pub fn to_ordered_pairs(&self) -> Vec<(&'static str, Value)> {
        vec![
            ("mode", Value::String(self.mode.to_string())),
            ("authority", Value::String(self.authority.to_string())),
            ("advisory_only", Value::Bool(self.advisory_only)),
            (
                "fleet_prior_budget_authority",
                Value::Bool(self.fleet_prior_budget_authority),
            ),
            ("base_budget_sats", Value::from(self.base_budget_sats)),
            (
                "local_hard_ceiling_sats",
                Value::from(self.local_hard_ceiling_sats),
            ),
            ("earned_credit_sats", Value::from(self.earned_credit_sats)),
            ("growth_credit_sats", Value::from(self.growth_credit_sats)),
            (
                "growth_credit_cap_sats",
                Value::from(self.growth_credit_cap_sats),
            ),
            (
                "effective_budget_sats",
                Value::from(self.effective_budget_sats),
            ),
            ("actual_spent_sats", Value::from(self.actual_spent_sats)),
            ("reserved_sats", Value::from(self.reserved_sats)),
            ("remaining_sats", Value::from(self.remaining_sats)),
            (
                "capped_by_hard_ceiling",
                Value::Bool(self.capped_by_hard_ceiling),
            ),
            (
                "fleet_prior",
                Value::Object(
                    self.fleet_prior
                        .to_ordered_pairs()
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                ),
            ),
        ]
    }
}

/// Clamp a fraction into `[0.0, 1.0]`, treating non-finite as `0.0`.
/// Mirrors `growth_budget._fraction` for the (already-`f64`-typed) fraction
/// inputs — the Python bool-rejection branch of `_fraction` is unreachable
/// here because Rust's type system already prevents a `bool` from being
/// passed where an `f64` is expected.
fn clamp_fraction(value: f64) -> f64 {
    if !value.is_finite() {
        return 0.0;
    }
    value.clamp(0.0, 1.0)
}

/// Port of `growth_budget.compute_growth_budget_status`.
///
/// Fleet priors can only unlock a bounded growth credit. They never own
/// budget authority, bypass the local ceiling, or reduce the fixed base
/// floor.
pub fn compute_growth_budget_status(
    inputs: &GrowthBudgetInputs,
    fleet_prior: Option<&Value>,
) -> GrowthBudgetStatus {
    let base_budget = inputs.base_budget_sats.max(0);
    let actual_spent = inputs.actual_spent_sats.max(0);
    let reserved = inputs.reserved_sats.max(0);
    let hard_ceiling = base_budget.max(inputs.hard_ceiling_sats.max(0));
    let prior_status = fleet_prior_status(fleet_prior);

    if !inputs.enabled {
        let effective = base_budget;
        return GrowthBudgetStatus {
            mode: MODE_FIXED,
            authority: AUTHORITY_LOCAL,
            advisory_only: true,
            fleet_prior_budget_authority: false,
            base_budget_sats: base_budget,
            local_hard_ceiling_sats: hard_ceiling,
            earned_credit_sats: 0,
            growth_credit_sats: 0,
            growth_credit_cap_sats: inputs.growth_max_extra_sats.max(0),
            effective_budget_sats: effective,
            actual_spent_sats: actual_spent,
            reserved_sats: reserved,
            remaining_sats: (effective - actual_spent - reserved).max(0),
            capped_by_hard_ceiling: false,
            fleet_prior: prior_status,
        };
    }

    let positive_profit = inputs.net_profit_sats.max(0);
    let earned_credit =
        (positive_profit as f64 * clamp_fraction(inputs.earned_fraction)).floor() as i64;

    let mut growth_credit = 0i64;
    if prior_status.used {
        growth_credit =
            (positive_profit as f64 * clamp_fraction(inputs.growth_fraction)).floor() as i64;
        growth_credit = growth_credit.min(inputs.growth_max_extra_sats.max(0));
    }

    let uncapped = base_budget + earned_credit + growth_credit;
    let effective = base_budget.max(uncapped).min(hard_ceiling);

    GrowthBudgetStatus {
        mode: MODE_DYNAMIC_GROWTH,
        authority: AUTHORITY_LOCAL,
        advisory_only: true,
        fleet_prior_budget_authority: false,
        base_budget_sats: base_budget,
        local_hard_ceiling_sats: hard_ceiling,
        earned_credit_sats: earned_credit,
        growth_credit_sats: growth_credit,
        growth_credit_cap_sats: inputs.growth_max_extra_sats.max(0),
        effective_budget_sats: effective,
        actual_spent_sats: actual_spent,
        reserved_sats: reserved,
        remaining_sats: (effective - actual_spent - reserved).max(0),
        capped_by_hard_ceiling: effective < uncapped,
        fleet_prior: prior_status,
    }
}
