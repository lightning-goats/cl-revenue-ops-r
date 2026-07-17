//! Lossless (de)serialization of `GaussianThompsonState` to/from the
//! Python `v2_state_json` blob fields.
//!
//! Filled in by Phase 4 Tasks 3/9.
//!
//! Port of `GaussianThompsonState.to_dict`/`from_dict`
//! (`fee_controller.py:1721-1940`) as pure data transforms over
//! [`crate::pyjson::OValue`] (see that module's doc comment for why not
//! `serde_json::Value`).
//!
//! ## Known-keys / unknown-keys
//!
//! `to_dict()` always emits exactly the same 29 keys (26 stored fields +
//! `posterior_variance` and `weight_scheme`, which are derived, not
//! stored, fields). Any OTHER top-level key present on `from_dict`'s input
//! is preserved in [`super::GaussianThompsonState::extra`] (insertion
//! order) and re-emitted by `gts_to_dict` after the fixed 29 — Python's own
//! `from_dict`/`to_dict` has no such passthrough (unknown keys are simply
//! dropped), so this is intentionally MORE lossless than Python, which
//! matters for round-tripping a blob across a mixed Python/Rust rollout.
//!
//! `posterior_variance` and `weight_scheme` MUST be in the recognized-keys
//! set even though `from_dict` never assigns them to a struct field —
//! every real persisted blob has both (`to_dict` always writes them), so
//! treating them as "unknown" would duplicate them into `extra` and
//! re-emit them a second time after the fixed key block.

use super::{
    CtxPosterior, GaussianThompsonState, Observation, CONGESTION_OBS_FLAG, EXPLORATION_BOOST_MAX,
    EXPLORATION_BOOST_MIN, MAX_BIAS_NUDGES, MIN_STD, WEIGHT_SCHEME, ZERO_PROBE_FLAG,
    ZERO_REVENUE_WEIGHT_FACTOR,
};
use crate::mat3::{M3, V3};
use crate::pyjson::OValue;
use revops_econ::pyfloat::py_pow;

/// Every key `to_dict()` ever writes (`fee_controller.py:1721-1756`), in
/// its exact output order. Used both to build `gts_to_dict`'s output and to
/// recognize which top-level input keys are NOT unknown.
const KNOWN_KEYS: &[&str] = &[
    "prior_mean_fee",
    "prior_std_fee",
    "observations",
    "posterior_mean",
    "posterior_std",
    "posterior_variance",
    "posterior_coeffs",
    "posterior_precision",
    "noise_variance",
    "_prior_coeffs",
    "_prior_precision",
    "_last_fee_min",
    "_last_fee_max",
    "contextual_posteriors",
    "posterior_bias",
    "charged_fee_mean",
    "zero_revenue_streak",
    "zero_run_start_fee",
    "zero_run_start_ts",
    "positive_rate_ref",
    "positive_rate_ref_ts",
    "meaningful_gap_ema_hours",
    "last_meaningful_ts",
    "last_upward_probe_ts",
    "weight_scheme",
    "exploration_boost",
    "last_sampled_fee",
    "last_sample_time",
    "reseeded_at",
];

// ---------------------------------------------------------------------------
// to_dict
// ---------------------------------------------------------------------------

/// `GaussianThompsonState.to_dict` (py 1721-1756) — exact key order,
/// including the derived `posterior_variance` (`float(posterior_std) ** 2`
/// — via `py_pow(_, 2.0)`, NOT a bare `.powf(2.0)` or `x*x`: CPython's
/// `**` calls `pow()`, which disagrees with a direct multiply in the last
/// bit for ~0.08% of inputs, and LLVM rewrites a bare `.powf(2.0)` into
/// `x*x` under `-O2`+ — see `revops_econ::pyfloat::py_pow`'s doc comment
/// for the full story) and `weight_scheme` (always [`WEIGHT_SCHEME`] —
/// this port never persists a legacy-scheme state). Unknown top-level keys
/// captured by `gts_from_dict` are appended after all 29 known keys, in
/// the order they were first seen.
pub fn gts_to_dict(s: &GaussianThompsonState) -> OValue {
    let mut entries: Vec<(String, OValue)> = vec![
        (
            "prior_mean_fee".to_string(),
            num_value(s.prior_mean_fee, s.prior_mean_fee_is_int),
        ),
        (
            "prior_std_fee".to_string(),
            num_value(s.prior_std_fee, s.prior_std_fee_is_int),
        ),
        (
            "observations".to_string(),
            OValue::arr(s.observations.iter().map(observation_to_value).collect()),
        ),
        (
            "posterior_mean".to_string(),
            OValue::Float(s.posterior_mean),
        ),
        ("posterior_std".to_string(), OValue::Float(s.posterior_std)),
        (
            "posterior_variance".to_string(),
            OValue::Float(py_pow(s.posterior_std, 2.0)),
        ),
        (
            "posterior_coeffs".to_string(),
            v3_to_value(&s.posterior_coeffs),
        ),
        (
            "posterior_precision".to_string(),
            m3_to_value(&s.posterior_precision),
        ),
        (
            "noise_variance".to_string(),
            OValue::Float(s.noise_variance),
        ),
        ("_prior_coeffs".to_string(), v3_to_value(&s.prior_coeffs)),
        (
            "_prior_precision".to_string(),
            m3_to_value(&s.prior_precision),
        ),
        ("_last_fee_min".to_string(), OValue::Float(s.last_fee_min)),
        ("_last_fee_max".to_string(), OValue::Float(s.last_fee_max)),
        (
            "contextual_posteriors".to_string(),
            OValue::obj(
                s.contextual_posteriors
                    .iter()
                    .map(|(k, v)| (k.clone(), ctx_posterior_to_value(v)))
                    .collect(),
            ),
        ),
        (
            "posterior_bias".to_string(),
            OValue::arr(
                s.posterior_bias
                    .iter()
                    .map(|(fee, w, ts)| {
                        OValue::arr(vec![
                            OValue::Float(*fee),
                            OValue::Float(*w),
                            OValue::Int(*ts),
                        ])
                    })
                    .collect(),
            ),
        ),
        (
            "charged_fee_mean".to_string(),
            OValue::Float(s.charged_fee_mean),
        ),
        (
            "zero_revenue_streak".to_string(),
            OValue::Int(s.zero_revenue_streak),
        ),
        (
            "zero_run_start_fee".to_string(),
            OValue::Float(s.zero_run_start_fee),
        ),
        (
            "zero_run_start_ts".to_string(),
            OValue::Int(s.zero_run_start_ts),
        ),
        (
            "positive_rate_ref".to_string(),
            OValue::Float(s.positive_rate_ref),
        ),
        (
            "positive_rate_ref_ts".to_string(),
            OValue::Int(s.positive_rate_ref_ts),
        ),
        (
            "meaningful_gap_ema_hours".to_string(),
            OValue::Float(s.meaningful_gap_ema_hours),
        ),
        (
            "last_meaningful_ts".to_string(),
            OValue::Int(s.last_meaningful_ts),
        ),
        (
            "last_upward_probe_ts".to_string(),
            OValue::Int(s.last_upward_probe_ts),
        ),
        (
            "weight_scheme".to_string(),
            OValue::Str(WEIGHT_SCHEME.to_string()),
        ),
        (
            "exploration_boost".to_string(),
            OValue::Float(s.exploration_boost),
        ),
        (
            "last_sampled_fee".to_string(),
            OValue::Int(s.last_sampled_fee),
        ),
        (
            "last_sample_time".to_string(),
            OValue::Int(s.last_sample_time),
        ),
        ("reseeded_at".to_string(), OValue::Int(s.reseeded_at)),
    ];
    for (k, v) in &s.extra {
        entries.push((k.clone(), v.clone()));
    }
    OValue::obj(entries)
}

fn num_value(v: f64, is_int: bool) -> OValue {
    if is_int {
        OValue::Int(v as i64)
    } else {
        OValue::Float(v)
    }
}

fn v3_to_value(v: &V3) -> OValue {
    OValue::arr(v.iter().map(|x| OValue::Float(*x)).collect())
}

fn m3_to_value(m: &M3) -> OValue {
    OValue::arr(m.iter().map(v3_to_value).collect())
}

fn observation_to_value(o: &Observation) -> OValue {
    let mut items = vec![
        num_value(o.fee, o.fee_is_int),
        OValue::Float(o.revenue_rate),
        OValue::Float(o.weight),
        OValue::Int(o.ts),
        OValue::Str(o.time_bucket.clone()),
    ];
    if let Some(flag) = &o.flag {
        items.push(OValue::Str(flag.clone()));
    }
    items.extend(o.extra.iter().cloned());
    OValue::arr(items)
}

fn ctx_posterior_to_value(c: &CtxPosterior) -> OValue {
    OValue::arr(vec![
        OValue::Float(c.mean),
        OValue::Float(c.precision),
        OValue::Int(c.count),
        OValue::Int(c.last_update),
    ])
}

// ---------------------------------------------------------------------------
// from_dict
// ---------------------------------------------------------------------------

/// `GaussianThompsonState.from_dict` (py 1758-1940), verbatim: legacy
/// weight rescale when `weight_scheme != "exposure_v2"` (including when the
/// key is absent entirely — a pre-`weight_scheme` blob), 4-tuple
/// observations gain a `"normal"` time bucket, legacy 3-tuple contextual
/// posteriors convert to the 4-tuple layout, coefficient/precision-matrix
/// shape + positive-diagonal validation with default fallback, bias
/// entries validated and bounded to the last [`MAX_BIAS_NUDGES`], and every
/// scalar's Python `TypeError`/`ValueError` → default-fallback semantics
/// mirrored by a Rust try-parse.
pub fn gts_from_dict(d: &OValue) -> GaussianThompsonState {
    let mut state = GaussianThompsonState::default();

    // prior_mean_fee / prior_std_fee: passthrough, no cast (py 1762-1763) —
    // preserve the original int/float JSON typing for a byte-identical
    // round trip (see the struct doc comment on `GaussianThompsonState`).
    match d.get("prior_mean_fee") {
        Some(OValue::Int(i)) => {
            state.prior_mean_fee = *i as f64;
            state.prior_mean_fee_is_int = true;
        }
        Some(OValue::Float(f)) => {
            state.prior_mean_fee = *f;
            state.prior_mean_fee_is_int = false;
        }
        _ => {
            state.prior_mean_fee = 200.0;
            state.prior_mean_fee_is_int = true;
        }
    }
    match d.get("prior_std_fee") {
        Some(OValue::Int(i)) => {
            state.prior_std_fee = *i as f64;
            state.prior_std_fee_is_int = true;
        }
        Some(OValue::Float(f)) => {
            state.prior_std_fee = *f;
            state.prior_std_fee_is_int = false;
        }
        _ => {
            state.prior_std_fee = 100.0;
            state.prior_std_fee_is_int = true;
        }
    }

    // Legacy payloads (no weight_scheme marker, or a stale one) carry
    // outcome-scaled weights (py 1764-1770).
    let legacy_weights = d.get("weight_scheme").and_then(OValue::as_str) != Some(WEIGHT_SCHEME);

    state.observations = d
        .get("observations")
        .and_then(OValue::as_arr)
        .map(|arr| convert_observations(arr, legacy_weights))
        .unwrap_or_default();

    // posterior_mean / posterior_std: passthrough, no cast (py 1790-1791).
    state.posterior_mean = d
        .get("posterior_mean")
        .and_then(OValue::as_f64)
        .unwrap_or(200.0);
    state.posterior_std = d
        .get("posterior_std")
        .and_then(OValue::as_f64)
        .unwrap_or(100.0);

    // Contextual posteriors: legacy 3-tuple -> 4-tuple conversion (py
    // 1792-1805). Insertion order preserved (Python dict iteration order).
    state.contextual_posteriors = d
        .get("contextual_posteriors")
        .and_then(OValue::as_obj)
        .map(|entries| {
            entries
                .iter()
                .map(|(k, v)| (k.clone(), convert_ctx_posterior(v)))
                .collect()
        })
        .unwrap_or_default();

    // M1: posterior_coeffs shape + type validation, whole-array fallback.
    let default_coeffs: V3 = [0.0, 1.0, 0.0];
    state.posterior_coeffs = parse_v3_strict(d.get("posterior_coeffs")).unwrap_or(default_coeffs);

    // L5: posterior_precision shape + positive-diagonal validation.
    let default_prec: M3 = [[0.01, 0.0, 0.0], [0.0, 0.01, 0.0], [0.0, 0.0, 0.01]];
    state.posterior_precision = parse_m3_pd(d.get("posterior_precision")).unwrap_or(default_prec);

    // L6: noise_variance positive floor.
    state.noise_variance = d
        .get("noise_variance")
        .and_then(OValue::as_f64)
        .unwrap_or(1000.0)
        .max(10.0);

    // Fixed prior restore (falls back to defaults for old serialized
    // states).
    state.prior_coeffs = parse_v3_strict(d.get("_prior_coeffs")).unwrap_or(default_coeffs);
    state.prior_precision = parse_m3_pd(d.get("_prior_precision")).unwrap_or(default_prec);

    state.last_fee_min = try_float(d.get("_last_fee_min")).unwrap_or(0.0);
    state.last_fee_max = try_float(d.get("_last_fee_max")).unwrap_or(0.0);

    // Durable out-of-band nudges: validated, bounded to the last
    // MAX_BIAS_NUDGES (py 1847-1861).
    state.posterior_bias = d
        .get("posterior_bias")
        .and_then(OValue::as_arr)
        .map(convert_posterior_bias)
        .unwrap_or_default();

    // charged_fee_mean (py 1863-1869).
    state.charged_fee_mean = try_float(d.get("charged_fee_mean"))
        .filter(|v| v.is_finite() && *v >= 0.0)
        .unwrap_or(0.0);

    // Zero-revenue run tracking (py 1871-1885).
    state.zero_revenue_streak = try_int(d.get("zero_revenue_streak"))
        .map(|v| v.max(0))
        .unwrap_or(0);
    state.zero_run_start_fee = try_float(d.get("zero_run_start_fee"))
        .filter(|v| v.is_finite() && *v >= 0.0)
        .unwrap_or(0.0);
    state.zero_run_start_ts = try_int(d.get("zero_run_start_ts"))
        .map(|v| v.max(0))
        .unwrap_or(0);

    // Positive-rate reference (py 1887-1897).
    state.positive_rate_ref = try_float(d.get("positive_rate_ref"))
        .filter(|v| v.is_finite() && *v >= 0.0)
        .unwrap_or(0.0);
    state.positive_rate_ref_ts = try_int(d.get("positive_rate_ref_ts"))
        .map(|v| v.max(0))
        .unwrap_or(0);

    // Meaningful-revenue cadence (py 1899-1918).
    state.meaningful_gap_ema_hours = try_float(d.get("meaningful_gap_ema_hours"))
        .filter(|v| v.is_finite() && *v >= 0.0)
        .unwrap_or(0.0);
    state.last_meaningful_ts = try_int(d.get("last_meaningful_ts"))
        .map(|v| v.max(0))
        .unwrap_or(0);
    state.last_upward_probe_ts = try_int(d.get("last_upward_probe_ts"))
        .map(|v| v.max(0))
        .unwrap_or(0);

    // Retired exploration boost, clamped (py 1920-1930).
    let boost = try_float(d.get("exploration_boost"))
        .filter(|v| v.is_finite())
        .unwrap_or(1.0);
    state.exploration_boost = boost.clamp(EXPLORATION_BOOST_MIN, EXPLORATION_BOOST_MAX);

    // Retired prior re-seed marker (py 1932-1936).
    state.reseeded_at = try_int(d.get("reseeded_at")).map(|v| v.max(0)).unwrap_or(0);

    // last_sampled_fee / last_sample_time: literal passthrough, no cast at
    // all (py 1938-1939) — `d.get(key, 0)`.
    state.last_sampled_fee = d
        .get("last_sampled_fee")
        .and_then(OValue::as_i64)
        .unwrap_or(0);
    state.last_sample_time = d
        .get("last_sample_time")
        .and_then(OValue::as_i64)
        .unwrap_or(0);

    // Unknown top-level keys, preserved in first-seen order.
    if let Some(entries) = d.as_obj() {
        state.extra = entries
            .iter()
            .filter(|(k, _)| !KNOWN_KEYS.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
    }

    state
}

/// `float(t[1])`/`float(t[2])` legacy-weight rescale (py 1764-1789):
/// - 4-element tuples gain a `"normal"` time-bucket 5th element.
/// - if legacy (`weight_scheme` absent or stale), the weight (3rd element)
///   is rescaled to the exposure-only scheme: for a positive-rate window,
///   `w /= min(1, log1p(rate)/log1p(1000))` (only when the factor and `w`
///   are both `> 0`); for a zero-rate window, `w /= ZERO_REVENUE_WEIGHT_FACTOR`
///   unconditionally; then `w = min(1.0, w)`.
fn convert_observations(arr: &[OValue], legacy_weights: bool) -> Vec<Observation> {
    let mut out = Vec::new();
    for item in arr {
        let Some(elems) = item.as_arr() else {
            continue;
        };
        let mut t: Vec<OValue> = elems.to_vec();
        if t.len() == 4 {
            t.push(OValue::Str("normal".to_string()));
        }
        if legacy_weights && t.len() >= 4 {
            let rate = t[1].as_f64().unwrap_or(0.0);
            let mut w = t[2].as_f64().unwrap_or(0.0);
            if rate > 0.0 {
                let factor = (rate.ln_1p() / 1000.0_f64.ln_1p()).min(1.0);
                if factor > 0.0 && w > 0.0 {
                    w /= factor;
                }
            } else {
                w /= ZERO_REVENUE_WEIGHT_FACTOR;
            }
            w = w.min(1.0);
            t[2] = OValue::Float(w);
        }
        if t.len() < 4 {
            // Malformed (Python's tuple unpacking on `t[3]` etc. would
            // raise IndexError further down the real code paths) — skip
            // rather than construct a garbage Observation.
            continue;
        }
        let fee = t[0].as_f64().unwrap_or(0.0);
        let fee_is_int = matches!(t[0], OValue::Int(_));
        let revenue_rate = t[1].as_f64().unwrap_or(0.0);
        let weight = t[2].as_f64().unwrap_or(0.0);
        let ts = t[3].as_i64().unwrap_or(0);
        let time_bucket = t
            .get(4)
            .and_then(OValue::as_str)
            .unwrap_or("normal")
            .to_string();
        let flag = t.get(5).and_then(OValue::as_str).map(|s| s.to_string());
        let extra = if t.len() > 6 {
            t[6..].to_vec()
        } else {
            Vec::new()
        };
        out.push(Observation {
            fee,
            fee_is_int,
            revenue_rate,
            weight,
            ts,
            time_bucket,
            flag,
            extra,
        });
    }
    out
}

/// Legacy 3-tuple `(mean, std, count)` -> 4-tuple `(mean, precision, count,
/// last_update=0)` conversion (py 1796-1804): `precision = 1 /
/// max(std^2, MIN_STD^2)`. A non-3-element value is assumed to already be
/// the current 4-tuple layout and is taken as-is (py's `else` branch does
/// no validation at all here — `from_dict` trusts the shape).
fn convert_ctx_posterior(v: &OValue) -> CtxPosterior {
    let Some(arr) = v.as_arr() else {
        return CtxPosterior {
            mean: 0.0,
            precision: 1.0,
            count: 0,
            last_update: 0,
            was_legacy_3tuple: false,
        };
    };
    if arr.len() == 3 {
        let legacy_mean = arr[0].as_f64().unwrap_or(0.0);
        let legacy_std = arr[1].as_f64().unwrap_or(0.0);
        let legacy_count = arr[2].as_i64().unwrap_or(0);
        // `py_pow(_, 2.0)`, not a bare `.powf(2.0)`: see
        // `revops_econ::pyfloat::py_pow`'s doc comment (LLVM's `-O2`+
        // `powf(2.0)` -> `x*x` rewrite diverges from CPython's `**`).
        let legacy_precision = 1.0 / (py_pow(legacy_std, 2.0)).max(py_pow(MIN_STD, 2.0));
        CtxPosterior {
            mean: legacy_mean,
            precision: legacy_precision,
            count: legacy_count,
            last_update: 0,
            was_legacy_3tuple: true,
        }
    } else {
        CtxPosterior {
            mean: arr.first().and_then(OValue::as_f64).unwrap_or(0.0),
            precision: arr.get(1).and_then(OValue::as_f64).unwrap_or(0.0),
            count: arr.get(2).and_then(OValue::as_i64).unwrap_or(0),
            last_update: arr.get(3).and_then(OValue::as_i64).unwrap_or(0),
            was_legacy_3tuple: false,
        }
    }
}

fn parse_v3_strict(v: Option<&OValue>) -> Option<V3> {
    let arr = v?.as_arr()?;
    if arr.len() != 3 {
        return None;
    }
    let mut out = [0.0; 3];
    for (i, x) in arr.iter().enumerate() {
        out[i] = try_float(Some(x))?;
    }
    Some(out)
}

fn parse_m3_pd(v: Option<&OValue>) -> Option<M3> {
    let arr = v?.as_arr()?;
    if arr.len() != 3 {
        return None;
    }
    let mut out = [[0.0; 3]; 3];
    for (i, row) in arr.iter().enumerate() {
        let row_arr = row.as_arr()?;
        if row_arr.len() != 3 {
            return None;
        }
        for (j, x) in row_arr.iter().enumerate() {
            out[i][j] = x.as_f64()?;
        }
    }
    for (i, row) in out.iter().enumerate() {
        // `!(x > 0.0)` (not `x <= 0.0`) on purpose: mirrors Python's
        // `raw_prec[i][i] > 0` validation, where a NaN diagonal fails the
        // check (NaN comparisons are always false in both languages) —
        // `x <= 0.0` would wrongly treat a NaN diagonal as "valid".
        if row[i].is_nan() || row[i] <= 0.0 {
            return None;
        }
    }
    Some(out)
}

fn convert_posterior_bias(arr: &[OValue]) -> Vec<(f64, f64, i64)> {
    let start = arr.len().saturating_sub(MAX_BIAS_NUDGES);
    let mut out = Vec::new();
    for entry in &arr[start..] {
        let Some(elems) = entry.as_arr() else {
            continue;
        };
        if elems.len() < 3 {
            continue;
        }
        let (Some(target_fee), Some(weight), Some(ts)) = (
            try_float(Some(&elems[0])),
            try_float(Some(&elems[1])),
            try_int(Some(&elems[2])),
        ) else {
            continue;
        };
        if target_fee.is_finite() && weight.is_finite() && weight > 0.0 && target_fee >= 0.0 {
            out.push((target_fee, weight, ts));
        }
    }
    out
}

/// `float(x)` with Python's `TypeError`/`ValueError` -> `None`.
fn try_float(v: Option<&OValue>) -> Option<f64> {
    match v? {
        OValue::Int(i) => Some(*i as f64),
        OValue::Float(f) => Some(*f),
        OValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        OValue::Str(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// `int(x)` with Python's `TypeError`/`ValueError` -> `None`. Truncates a
/// finite float toward zero; a non-finite float or non-integer string
/// mirrors Python raising (`ValueError`/`OverflowError`).
fn try_int(v: Option<&OValue>) -> Option<i64> {
    match v? {
        OValue::Int(i) => Some(*i),
        OValue::Float(f) if f.is_finite() => Some(*f as i64),
        OValue::Bool(b) => Some(if *b { 1 } else { 0 }),
        OValue::Str(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// Wire flag constants re-exported for callers that build observations
/// directly (Task 2/7) without needing to import `super::` paths.
pub const CONGESTION_FLAG: &str = CONGESTION_OBS_FLAG;
pub const ZERO_PROBE_FLAG_STR: &str = ZERO_PROBE_FLAG;
