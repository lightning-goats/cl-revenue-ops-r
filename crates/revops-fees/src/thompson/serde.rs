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
            num_value(s.posterior_mean, s.posterior_mean_is_int),
        ),
        (
            "posterior_std".to_string(),
            num_value(s.posterior_std, s.posterior_std_is_int),
        ),
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

    // posterior_mean / posterior_std: passthrough, no cast (py 1790-1791),
    // same int/float-preserving contract as prior_mean_fee/prior_std_fee
    // above (Python's fallback default is a FLOAT literal --
    // `d.get("posterior_mean", 200.0)` -- so an absent key yields
    // `_is_int = false`, unlike prior_mean_fee's int-literal fallback).
    match d.get("posterior_mean") {
        Some(OValue::Int(i)) => {
            state.posterior_mean = *i as f64;
            state.posterior_mean_is_int = true;
        }
        Some(OValue::Float(f)) => {
            state.posterior_mean = *f;
            state.posterior_mean_is_int = false;
        }
        _ => {
            state.posterior_mean = 200.0;
            state.posterior_mean_is_int = false;
        }
    }
    match d.get("posterior_std") {
        Some(OValue::Int(i)) => {
            state.posterior_std = *i as f64;
            state.posterior_std_is_int = true;
        }
        Some(OValue::Float(f)) => {
            state.posterior_std = *f;
            state.posterior_std_is_int = false;
        }
        _ => {
            state.posterior_std = 100.0;
            state.posterior_std_is_int = false;
        }
    }

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
        // Audit lows F4/F5: Python passes the tuple through VERBATIM --
        // it never validates that element 5 (0-indexed) is a string, it
        // just happens to always be `CONGESTION_OBS_FLAG`/`ZERO_PROBE_FLAG`
        // in every real blob. If it's a string, treat it as the flag (as
        // before) and `extra` starts at index 6. If it is present but NOT
        // a string, there is no flag to consume -- `extra` must start AT
        // index 5 (not 6), or that element is silently dropped (a 6-tuple
        // shrinks to 5 elements on re-emit) and every element past it
        // shifts left by one (a >=7-tuple loses its 6th element).
        let flag = t.get(5).and_then(OValue::as_str).map(|s| s.to_string());
        let extra_start = if flag.is_some() { 6 } else { 5 };
        let extra = if t.len() > extra_start {
            t[extra_start..].to_vec()
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

// ---------------------------------------------------------------------------
// Fail-closed seed-import parity check (stateful-shadow Task 5 / revision
// plan Task R6)
// ---------------------------------------------------------------------------

/// One field where importing this dict through the Rust parity path would
/// DIVERGE from Python: either Python's own `from_dict` would RAISE where
/// the total Rust port silently falls back to a default, or (precision
/// matrices only) Python would silently keep a value the Rust port
/// silently replaces. Either way the SeedOnce import must refuse the whole
/// snapshot rather than start autonomous mode on state Python would not
/// have produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FromDictRaise {
    /// The offending `thompson_state` key.
    pub field: &'static str,
    pub detail: String,
}

/// Detect the classes of input where `GaussianThompsonState.from_dict`
/// (py 1784-1940) would raise -- the classes [`gts_from_dict`] deliberately
/// maps to defaults for the Python-authoritative dry-run window, which is
/// exactly wrong for a one-time seed that Rust then owns.
///
/// Mirrored raise sites, in Python's evaluation order:
/// - `observations`: iterating a non-list raises; `tuple(obs)` on a
///   non-iterable entry raises; the legacy-weight rescale's `float(t[1])` /
///   `float(t[2])` raise on non-numeric values.
/// - `contextual_posteriors`: `.items()` on a non-dict raises
///   `AttributeError`; `tuple(v)` on a non-iterable value raises; the
///   legacy 3-tuple conversion's `float(...)` raises on non-numeric
///   mean/std.
/// - `posterior_precision` / `_prior_precision`: the L5 guard expression
///   itself can raise (`len()` of a non-sized value; `raw_prec[i][i] > 0`
///   comparing a non-number, indexing a string row's char, or `KeyError`
///   on a dict) -- emulated including `all(...)`'s short-circuit order.
/// - `noise_variance`, `_last_fee_min`, `_last_fee_max`: bare `float(...)`
///   with no try/except.
/// - `posterior_bias`: `entry[0]` on a dict entry raises `KeyError`, which
///   the restore loop's `except (TypeError, ValueError, IndexError)` does
///   NOT catch.
///
/// Conservative bias: where Python's dynamic semantics are broader than a
/// faithful static check (e.g. numeric strings with underscores, or
/// iterable-but-not-list observation entries), this REFUSES -- a refusal
/// where Python would accept keeps the plugin fail-closed passive-observer;
/// the reverse direction (accepting where Python raises or diverges) is
/// the unsafe one and never happens.
pub fn gts_from_dict_would_raise(d: &OValue) -> Option<FromDictRaise> {
    // observations (py 1796-1815).
    if let Some(v) = d.get("observations") {
        let Some(arr) = v.as_arr() else {
            return Some(FromDictRaise {
                field: "observations",
                detail: "not a list: Python's iteration/tuple() would raise".to_string(),
            });
        };
        let legacy_weights = d.get("weight_scheme").and_then(OValue::as_str) != Some(WEIGHT_SCHEME);
        for (i, obs) in arr.iter().enumerate() {
            let Some(t) = obs.as_arr() else {
                return Some(FromDictRaise {
                    field: "observations",
                    detail: format!("entry {i} is not a list: tuple(obs) would raise"),
                });
            };
            if legacy_weights && t.len() >= 4 {
                for idx in [1usize, 2] {
                    if !python_floatable(&t[idx]) {
                        return Some(FromDictRaise {
                            field: "observations",
                            detail: format!(
                                "entry {i} element {idx}: legacy-weight rescale float() would raise"
                            ),
                        });
                    }
                }
            }
        }
    }

    // contextual_posteriors (py 1820-1832).
    if let Some(v) = d.get("contextual_posteriors") {
        let Some(entries) = v.as_obj() else {
            return Some(FromDictRaise {
                field: "contextual_posteriors",
                detail: "not a dict: .items() would raise AttributeError".to_string(),
            });
        };
        for (key, value) in entries {
            let Some(t) = value.as_arr() else {
                return Some(FromDictRaise {
                    field: "contextual_posteriors",
                    detail: format!("entry {key:?} is not a list: tuple(v) would raise"),
                });
            };
            if t.len() == 3 && (!python_floatable(&t[0]) || !python_floatable(&t[1])) {
                return Some(FromDictRaise {
                    field: "contextual_posteriors",
                    detail: format!(
                        "legacy 3-tuple entry {key:?}: float(mean)/float(std) would raise"
                    ),
                });
            }
        }
    }

    // posterior_precision / _prior_precision (py 1846-1852, 1866-1871).
    for field in ["posterior_precision", "_prior_precision"] {
        if let Some(v) = d.get(field) {
            if let Some(detail) = precision_guard_divergence(v) {
                return Some(FromDictRaise { field, detail });
            }
        }
    }

    // noise_variance / _last_fee_min / _last_fee_max: bare float() with no
    // try/except (py 1855, 1873-1874).
    for field in ["noise_variance", "_last_fee_min", "_last_fee_max"] {
        if let Some(v) = d.get(field) {
            if !python_floatable(v) {
                return Some(FromDictRaise {
                    field,
                    detail: "non-numeric value: bare float() would raise".to_string(),
                });
            }
        }
    }

    // posterior_bias (py 1877-1886): only the tail slice is iterated, and
    // only a dict entry raises (KeyError is absent from the except tuple).
    if let Some(arr) = d.get("posterior_bias").and_then(OValue::as_arr) {
        let start = arr.len().saturating_sub(MAX_BIAS_NUDGES);
        for (i, entry) in arr[start..].iter().enumerate() {
            if matches!(entry, OValue::Obj(_)) {
                return Some(FromDictRaise {
                    field: "posterior_bias",
                    detail: format!(
                        "entry {} is a dict: entry[0] raises KeyError (uncaught)",
                        start + i
                    ),
                });
            }
        }
    }

    None
}

/// Python truthiness for the precision-matrix guard.
fn python_truthy(v: &OValue) -> bool {
    match v {
        OValue::Null => false,
        OValue::Bool(b) => *b,
        OValue::Int(i) => *i != 0,
        OValue::Float(f) => *f != 0.0,
        OValue::Str(s) => !s.is_empty(),
        OValue::Arr(a) => !a.is_empty(),
        OValue::Obj(o) => !o.is_empty(),
    }
}

/// `float(x)` succeeds in Python: numbers, bools, and parseable numeric
/// strings. Rust's `str::parse::<f64>` is a conservative approximation of
/// Python's `float(str)` (e.g. Python additionally accepts `"1_0"`); a
/// string only Python can parse is REFUSED, never the reverse.
pub(crate) fn python_floatable(v: &OValue) -> bool {
    match v {
        OValue::Int(_) | OValue::Float(_) | OValue::Bool(_) => true,
        OValue::Str(s) => s.trim().parse::<f64>().is_ok(),
        _ => false,
    }
}

/// `int(x)` succeeds in Python. NARROWER than [`python_floatable`] in two
/// ways that matter for the seed scan: a string must be an INTEGER
/// literal (`int("1.5")` raises `ValueError` where `float("1.5")` does
/// not), and a non-finite float raises (`int(nan)` -> `ValueError`,
/// `int(inf)` -> `OverflowError`) where `float(nan)` is fine. A finite
/// float truncates rather than raising. Same conservative bias as
/// [`python_floatable`]: a string only Python can parse (e.g. `"1_0"`) is
/// REFUSED, never the reverse.
pub(crate) fn python_intable(v: &OValue) -> bool {
    match v {
        OValue::Int(_) | OValue::Bool(_) => true,
        OValue::Float(f) => f.is_finite(),
        OValue::Str(s) => s.trim().parse::<i64>().is_ok(),
        _ => false,
    }
}

/// Emulate the L5 guard
/// `raw_prec and len(raw_prec) == 3 and all(len(r) == 3 for r in raw_prec)
///  and all(raw_prec[i][i] > 0 for i in range(3))`
/// including `and`/`all` short-circuiting, returning `Some(detail)` when
/// Python would RAISE evaluating it (a guard that evaluates to `False`
/// falls back to the default matrix in both languages -- that is safe).
/// Additionally refuses a guard-TRUE matrix carrying non-numeric
/// OFF-diagonal elements: Python keeps them verbatim (`list(row)`), the
/// Rust parity path (`parse_m3_pd`) silently defaults the whole matrix --
/// a silent state divergence a Rust-owned seed must not import.
fn precision_guard_divergence(v: &OValue) -> Option<String> {
    if !python_truthy(v) {
        return None; // guard false at `raw_prec and ...` -> default -> safe
    }
    let arr = match v {
        OValue::Arr(a) => a,
        // len("...") works; every char then fails len(r)==3 -> guard false.
        OValue::Str(_) => return None,
        OValue::Obj(entries) => {
            // len(dict) then iteration over KEYS: any key of len != 3
            // short-circuits the guard to False; if every key has len 3,
            // `raw_prec[0]` raises KeyError (integer key on a JSON dict).
            if entries.len() != 3 {
                return None;
            }
            for (key, _) in entries {
                if key.chars().count() != 3 {
                    return None;
                }
            }
            return Some("dict value: raw_prec[0] raises KeyError".to_string());
        }
        // Truthy int/float/bool: len() raises TypeError.
        _ => return Some("len() of a non-sized value raises TypeError".to_string()),
    };
    if arr.len() != 3 {
        return None; // guard false -> default
    }
    // all(len(r) == 3 for r in raw_prec), short-circuit on first False.
    for row in arr {
        match row {
            OValue::Arr(a) => {
                if a.len() != 3 {
                    return None;
                }
            }
            OValue::Str(s) => {
                if s.chars().count() != 3 {
                    return None;
                }
            }
            OValue::Obj(o) => {
                if o.len() != 3 {
                    return None;
                }
            }
            _ => return Some("len() of a non-sized row raises TypeError".to_string()),
        }
    }
    // all(raw_prec[i][i] > 0 for i in range(3)), short-circuit on first
    // False; a raise anywhere before that False propagates.
    for (i, row) in arr.iter().enumerate() {
        match row {
            OValue::Arr(a) => match &a[i] {
                OValue::Int(x) => {
                    if *x <= 0 {
                        return None;
                    }
                }
                OValue::Float(f) => {
                    if f.is_nan() || *f <= 0.0 {
                        return None;
                    }
                }
                OValue::Bool(b) => {
                    if !*b {
                        return None;
                    }
                }
                _ => {
                    return Some(format!(
                        "diagonal [{i}][{i}] compares a non-number to 0: TypeError"
                    ))
                }
            },
            OValue::Str(_) => {
                return Some(format!(
                    "string row {i}: `char > 0` comparison raises TypeError"
                ))
            }
            OValue::Obj(_) => return Some(format!("dict row {i}: row[{i}] raises KeyError")),
            _ => unreachable!("non-sized rows already raised in the len() pass"),
        }
    }
    // Guard TRUE: Python copies rows verbatim; Rust's parse_m3_pd requires
    // all 9 elements numeric or defaults the matrix. Refuse the divergence.
    for (i, row) in arr.iter().enumerate() {
        let OValue::Arr(a) = row else {
            return Some(format!(
                "guard-true row {i} is not a list: Python list(row) keeps it, Rust defaults"
            ));
        };
        for (j, x) in a.iter().enumerate() {
            if !matches!(x, OValue::Int(_) | OValue::Float(_) | OValue::Bool(_)) {
                return Some(format!(
                    "element [{i}][{j}] is non-numeric: Python keeps it verbatim, the Rust \
                     parity path defaults the whole matrix (silent divergence)"
                ));
            }
        }
    }
    None
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

#[cfg(test)]
mod seed_parity_tests {
    use super::*;
    use crate::pyjson::parse;

    fn dict(json: &str) -> OValue {
        parse(json).expect("valid JSON fixture")
    }

    /// Python's `all(len(r) == 3 for r in raw_prec)` SHORT-CIRCUITS: a
    /// wrong-length row before an unsized row stops evaluation, so Python
    /// never raises -- the emulation must not refuse.
    #[test]
    fn precision_guard_short_circuit_before_unsized_row_is_safe() {
        let d = dict(r#"{"posterior_precision": [[1, 0, 0, 0], 5, [0, 0, 1]]}"#);
        assert_eq!(gts_from_dict_would_raise(&d), None);
    }

    /// ...but an unsized row REACHED by the generator (all earlier rows
    /// were len 3) raises `TypeError` in Python.
    #[test]
    fn precision_guard_unsized_row_after_len3_rows_raises() {
        let d = dict(r#"{"posterior_precision": [[1, 0, 0], 5, [0, 0, 1]]}"#);
        let raise = gts_from_dict_would_raise(&d).expect("must refuse");
        assert_eq!(raise.field, "posterior_precision");
    }

    /// The diagonal loop also short-circuits: a non-positive diagonal
    /// BEFORE a string row stops evaluation (guard False -> default), no
    /// raise.
    #[test]
    fn precision_guard_nonpositive_diag_short_circuits_before_string_row() {
        let d = dict(r#"{"posterior_precision": [[0, 0, 0], "abc", [0, 0, 1]]}"#);
        assert_eq!(gts_from_dict_would_raise(&d), None);
    }

    /// Truthy non-sized value: `len(raw_prec)` raises immediately.
    #[test]
    fn precision_guard_truthy_scalar_raises() {
        let d = dict(r#"{"posterior_precision": 7}"#);
        assert!(gts_from_dict_would_raise(&d).is_some());
    }

    /// A top-level STRING is sized and every char fails `len(r) == 3`:
    /// guard False, default, safe.
    #[test]
    fn precision_guard_top_level_string_is_safe() {
        let d = dict(r#"{"posterior_precision": "abc"}"#);
        assert_eq!(gts_from_dict_would_raise(&d), None);
    }

    /// A dict whose 3 keys all have length 3 reaches `raw_prec[0]` ->
    /// `KeyError` (uncaught); a dict with any other key shape
    /// short-circuits safely.
    #[test]
    fn precision_guard_dict_key_shapes() {
        let raising = dict(r#"{"posterior_precision": {"abc": 1, "def": 2, "ghi": 3}}"#);
        assert!(gts_from_dict_would_raise(&raising).is_some());
        let safe = dict(r#"{"posterior_precision": {"ab": 1, "def": 2, "ghi": 3}}"#);
        assert_eq!(gts_from_dict_would_raise(&safe), None);
    }

    /// Guard-TRUE matrix with a non-numeric OFF-diagonal: Python keeps it
    /// verbatim, `parse_m3_pd` defaults the matrix -- silent divergence,
    /// refused.
    #[test]
    fn precision_guard_true_with_string_offdiag_is_divergence() {
        let d = dict(r#"{"posterior_precision": [[1, "x", 0], [0, 1, 0], [0, 0, 1]]}"#);
        let raise = gts_from_dict_would_raise(&d).expect("must refuse divergence");
        assert!(raise.detail.contains("divergence"), "{raise:?}");
    }

    /// `posterior_bias` only refuses dict entries INSIDE the evaluated
    /// tail slice (`raw_bias[-MAX_BIAS_NUDGES:]`); an older dict entry is
    /// never touched by Python either.
    #[test]
    fn posterior_bias_dict_outside_tail_slice_is_safe() {
        let mut entries: Vec<String> = vec!["{\"k\": 1}".to_string()];
        entries.extend((0..MAX_BIAS_NUDGES).map(|_| "[100.0, 0.5, 1]".to_string()));
        let d = dict(&format!("{{\"posterior_bias\": [{}]}}", entries.join(", ")));
        assert_eq!(gts_from_dict_would_raise(&d), None);
        let d = dict(r#"{"posterior_bias": [[100.0, 0.5, 1], {"k": 1}]}"#);
        assert!(gts_from_dict_would_raise(&d).is_some());
    }

    /// Legacy-weight observations (`weight_scheme` stale/absent) with
    /// non-numeric rate/weight raise in the rescale's `float()`; under the
    /// CURRENT scheme the same tuple is never float()ed.
    #[test]
    fn observations_legacy_rescale_float_raises_only_when_legacy() {
        let legacy = dict(r#"{"observations": [[100, "x", 1.0, 5]]}"#);
        assert!(gts_from_dict_would_raise(&legacy).is_some());
        let current =
            dict(r#"{"weight_scheme": "exposure_v2", "observations": [[100, "x", 1.0, 5]]}"#);
        assert_eq!(gts_from_dict_would_raise(&current), None);
    }

    /// Non-iterable observation entries raise `tuple(obs)` regardless of
    /// scheme.
    #[test]
    fn observations_non_list_entry_raises() {
        let d = dict(r#"{"weight_scheme": "exposure_v2", "observations": [5]}"#);
        assert!(gts_from_dict_would_raise(&d).is_some());
    }

    /// Bare-float() scalars: `null` raises too (`float(None)` is a
    /// `TypeError`), absent keys are fine.
    #[test]
    fn bare_float_scalars_null_raises_absent_is_fine() {
        assert!(gts_from_dict_would_raise(&dict(r#"{"noise_variance": null}"#)).is_some());
        assert!(gts_from_dict_would_raise(&dict(r#"{"_last_fee_max": {}}"#)).is_some());
        assert_eq!(gts_from_dict_would_raise(&dict("{}")), None);
    }
}
