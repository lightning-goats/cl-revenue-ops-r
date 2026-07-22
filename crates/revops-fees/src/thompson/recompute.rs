//! Posterior recompute core + legacy Normal-Normal fallback + zero-regime
//! anchor + DTS discounting (port of `GaussianThompsonState`
//! `_recompute_posterior_core`/`_recompute_posterior_legacy`/
//! `apply_dts_discount`, `modules/fee_controller.py:1296-1719`), plus the
//! read-only helpers those paths need: `_positive_revenue_mass`,
//! `_earning_region_fee`, `_effective_positive_rate_ref` (py 851-900).
//!
//! # Struct ownership (Phase 4 Task 2 / Task 3 seam — RESOLVED at merge)
//!
//! While Tasks 2 and 3 developed in parallel, this module carried a
//! standalone `GtsCore` "pure numeric field bundle" so both branches
//! compiled independently. At the integration merge GtsCore was DROPPED
//! (not embedded): its field subset was name-and-type-identical to the
//! corresponding fields of [`GaussianThompsonState`] (owned by
//! `thompson/mod.rs`, Task 3), so every function here now takes
//! `&mut GaussianThompsonState` directly — exactly the signatures the
//! Phase 4 plan's Task 2 interface block declares. This matches Python,
//! where these are methods on `GaussianThompsonState` itself, and means
//! recompute simply never touches the serde/contextual/blob-compat fields.
//! The canonical [`Observation`] (with `fee_is_int` + `OValue` extras)
//! also lives in `thompson/mod.rs`; this module re-uses it.
//!
//! # THE discounting order-of-operations (read before touching anything here)
//!
//! 1. [`apply_dts_discount`] mutates three things IN PLACE, in this exact
//!    order: (a) Gaussian precision = `1/max(std^2, 1.0)`, `*= gamma`,
//!    floored at [`MIN_PRECISION`], `std = sqrt(1/precision)`; (b) every
//!    cell of `posterior_precision *= gamma`; (c) every stored
//!    observation's base weight `w := max(min(w, DISCOUNT_WEIGHT_FLOOR),
//!    w * gamma)` — never raises a weight already below the floor, never
//!    lets discounting push a weight below the floor either.
//! 2. The NEXT [`recompute_posterior_core`] rebuilds `Ln` from the FIXED
//!    prior (`prior_precision`/`prior_coeffs`) plus the (now decayed)
//!    observation weights and OVERWRITES `posterior_precision`/
//!    `posterior_std` — so steps (a)/(b) above only affect samples drawn
//!    between the discount and the next recompute, while (c) is the
//!    durable channel. The controller's per-channel cycle order is
//!    `update_posterior(...)` (ends in a recompute) -> `apply_dts_discount`
//!    -> `sample_fee_contextual(...)`: the sample draws from the
//!    discounted-in-place matrices. Do NOT "optimize" by recomputing after
//!    the discount — that erases (a)/(b) before they're ever sampled from.
//! 3. Inside [`recompute_posterior_core`] the weighted-obs collection order
//!    is the stored observation order; the regression accumulation is
//!    `for obs: for i in 0..3 { rhs[i] += wi*phi[i]*rev; for j in 0..3 {
//!    Ln[i][j] += wi*phi[i]*phi[j] } }` with `f = (fee - fee_min) *
//!    inv_range`, `wi = w * inv_sigma2`. Preserve exactly — reassociation
//!    flips the singularity branch on near-singular fits (Global
//!    Constraints: never reassociate Python arithmetic).
//! 4. Noise-variance update happens AFTER solving for `mu_n`:
//!    `new_sigma2 = ss / max(sw - 3.0, 1.0)`, `noise_variance =
//!    max(10.0, 0.7*new_sigma2 + 0.3*noise_variance)`.
//!
//! All `0.5 ** (age_hours / half_life)` decay math below goes through
//! [`revops_econ::pyfloat::py_pow`], not a bare `0.5f64.powf(...)`: LLVM
//! rewrites a constant-`0.5`-base `powf` into an `exp2` form under
//! `-O2`+, which diverges from CPython's `**` (`pow()`) in the last bit
//! for ~0.11% of inputs — see `py_pow`'s doc comment for the full story
//! (T6 review adjudication, workspace-wide float-hardening task).

use super::{GaussianThompsonState, CONGESTION_OBS_FLAG, MIN_STD, ZERO_PROBE_FLAG};
use crate::mat3::{invert3, matvec3};
use revops_econ::pyfloat::py_pow;

/// CPython 3.12+ `sum()` for floats: Neumaier (improved Kahan-Babuska)
/// compensated summation, NOT naive left-fold. This is a real, verified
/// behavioral change (CPython gh-100425, "Improve the accuracy of builtin
/// sum() for float inputs") — every Python `sum(...)` call over floats in
/// `fee_controller.py` uses this algorithm, and naive `Iterator::sum()`
/// diverges in the last 1-2 ULPs on large accumulations (empirically
/// confirmed against the live oracle: the 200-observation `charged_fee_mean`
/// fixture only matches with this algorithm). Explicit Python `+=`
/// accumulator loops (e.g. the regression's `rhs`/`Ln`/`ss`/`sw`
/// accumulation) are NOT `sum()` and must NOT go through this — only calls
/// that mirror a literal Python `sum(...)` builtin invocation do.
pub(crate) fn py_sum(iter: impl IntoIterator<Item = f64>) -> f64 {
    let mut s = 0.0f64;
    let mut c = 0.0f64;
    for x in iter {
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

// ---------------------------------------------------------------------------
// Constants — transcription of `GaussianThompsonState`'s class body
// (`fee_controller.py:258-383`) plus `MIN_PRECISION` (1663) and
// `DISCOUNT_WEIGHT_FLOOR` (1670). Every constant is load-bearing (Global
// Constraints: "Many constants encode named production incidents") — not
// tunable. `pub` (not `pub(crate)`) so the pinned-constants integration
// test (a separate crate) can see them.
//
// The constants ALSO needed by serde's from_dict (`MIN_STD`,
// `WEIGHT_SCHEME`, `ZERO_REVENUE_WEIGHT_FACTOR`, `MAX_BIAS_NUDGES`,
// `EXPLORATION_BOOST_MIN`/`MAX`, `CONGESTION_OBS_FLAG`, `ZERO_PROBE_FLAG`)
// have their single definition in `thompson/mod.rs` (they were duplicated
// here during parallel Task 2/3 development; collapsed at the merge).
// ---------------------------------------------------------------------------

/// Security: bounded memory per channel (py 258).
pub const MAX_OBSERVATIONS: i64 = 200;
/// 7-day half-life for observation decay (py 259).
pub const DECAY_HOURS: f64 = 168.0;
/// Minimum genuine observations before trusting the posterior (py 260).
pub const MIN_OBSERVATIONS: i64 = 5;
/// Trickle guard fraction of the positive-rate reference (py 284).
pub const TRICKLE_RESET_FRAC: f64 = 0.10;
/// Positive-rate EMA alpha (py 285).
pub const POSITIVE_RATE_EMA_ALPHA: f64 = 0.2;
/// Positive-rate reference half-life, hours (py 286).
pub const POSITIVE_RATE_REF_HALF_LIFE_HOURS: f64 = 168.0;
/// Meaningful-revenue cadence EMA alpha (py 292).
pub const MEANINGFUL_GAP_EMA_ALPHA: f64 = 0.3;
/// Bounded upward exploration stretch (py 303).
pub const UPWARD_PROBE_STRETCH: f64 = 1.25;
/// Upward-probe interval, hours (py 304).
pub const UPWARD_PROBE_INTERVAL_HOURS: f64 = 24.0;
/// Upward-probe minimum std (py 305).
pub const UPWARD_PROBE_MIN_STD: f64 = 60.0;
/// Supported-ceiling headroom multiplier (py 315).
pub const SUPPORTED_CEILING_HEADROOM: f64 = 1.25;
/// Supported-ceiling revenue-mass quantile (py 316).
pub const SUPPORTED_CEILING_MASS_QUANTILE: f64 = 0.90;
/// Minimum recency-decayed mass to count toward the ceiling (py 317).
pub const SUPPORTED_CEILING_MIN_WEIGHT: f64 = 1e-3;
/// Floor-escape headroom multiplier (py 325).
pub const SUPPORTED_CEILING_FLOOR_ESCAPE: f64 = 2.0;
/// SL-4 relative uncertainty floor fraction for the legacy path (py 343).
pub const REL_MIN_STD_FRAC: f64 = 0.04;
/// Consecutive zero-revenue windows before directional probing (py 344).
pub const ZERO_REVENUE_STREAK_THRESHOLD: i64 = 4;
/// Zero-probe pseudo-observation fee fraction (py 345).
pub const ZERO_PROBE_STEP_FRAC: f64 = 0.9;
/// Cap on cumulative downward zero-probe influence (py 346).
pub const ZERO_PROBE_FLOOR_FRAC: f64 = 0.3;
/// Minimum relative uncertainty when all revenue is zero (py 348).
pub const ZERO_REGIME_REL_STD: f64 = 0.15;
/// Consecutive zero windows after which the market is presumed to have
/// moved (anchor only on the current run's observations) (py 349).
pub const ZERO_REGIME_STREAK_OVERRIDE: i64 = 24;
/// Recency half-life for the zero-regime anchor mean, hours (py 353).
pub const ZERO_REGIME_ANCHOR_HALF_LIFE_HOURS: f64 = 24.0;
/// Slightly wider prior for secondary contexts (py 357).
pub const SECONDARY_EXPLORE_BOOST: f64 = 1.25;
/// Advisory nudge half-life, hours (py 359).
pub const BIAS_DECAY_HOURS: f64 = 24.0;
/// Below this decayed weight a nudge is pruned (py 360).
pub const BIAS_MIN_WEIGHT: f64 = 1e-3;
/// Relative tolerance for nudge-target dedupe (py 367).
pub const NUDGE_DEDUP_TOLERANCE: f64 = 0.05;
/// Max fraction a context can shift a sample (py 374).
pub const CTX_OFFSET_CAP_FRAC: f64 = 0.20;
/// Half-saturation observation count for context confidence (py 375).
pub const CTX_CONFIDENCE_COUNT: f64 = 10.0;
/// Per-update contextual precision decay (py 376).
pub const CTX_PRECISION_DECAY: f64 = 0.98;

/// Minimum posterior precision (max std ~= 200 ppm), py 1663.
pub const MIN_PRECISION: f64 = 0.000025;
/// Discount decay never pushes a stored weight below this floor and never
/// raises one already below it, py 1670.
pub const DISCOUNT_WEIGHT_FLOOR: f64 = 0.05;

// ---------------------------------------------------------------------------
// Read-only helpers (py 851-900).
// ---------------------------------------------------------------------------

/// `_effective_positive_rate_ref` (py 851-858): positive-rate reference
/// with 7-day half-life decay applied. `age_hours` IS clamped to >= 0 here
/// (py 855 `max(0.0, ...)`, unlike the core recompute's own age formula).
pub fn effective_positive_rate_ref(state: &GaussianThompsonState, now: i64) -> f64 {
    if state.positive_rate_ref <= 0.0 || state.positive_rate_ref_ts <= 0 {
        return 0.0;
    }
    let age_hours = ((now - state.positive_rate_ref_ts) as f64 / 3600.0).max(0.0);
    state.positive_rate_ref * py_pow(0.5, age_hours / POSITIVE_RATE_REF_HALF_LIFE_HOURS)
}

/// `_positive_revenue_mass` (py 860-892): `(fee, recency-decayed revenue
/// mass)` for genuine earning windows. Probe pseudo-observations carry
/// zero revenue (self-excluded); congestion-flagged windows are excluded
/// explicitly. Winsorizes: when >= 4 masses survive, caps any single
/// window's mass at 3x the median so one unreplicated whale window cannot
/// dominate the region statistics.
pub fn positive_revenue_mass(state: &GaussianThompsonState, now: i64) -> Vec<(f64, f64)> {
    let mut masses: Vec<(f64, f64)> = Vec::new();
    for obs in &state.observations {
        if obs.revenue_rate <= 0.0 {
            continue;
        }
        if obs.flag.as_deref() == Some(CONGESTION_OBS_FLAG) {
            continue;
        }
        // py 879: age_hours IS clamped to >= 0 here.
        let age_hours = ((now - obs.ts) as f64 / 3600.0).max(0.0);
        let decay = py_pow(0.5, age_hours / DECAY_HOURS);
        let mass = obs.revenue_rate * obs.weight * decay;
        if mass > SUPPORTED_CEILING_MIN_WEIGHT {
            masses.push((obs.fee, mass));
        }
    }
    if masses.len() >= 4 {
        let mut sorted_m: Vec<f64> = masses.iter().map(|(_, m)| *m).collect();
        sorted_m.sort_by(|a, b| a.partial_cmp(b).expect("mass is never NaN"));
        let median_m = sorted_m[sorted_m.len() / 2];
        let cap = 3.0 * median_m;
        for (_, m) in masses.iter_mut() {
            *m = m.min(cap);
        }
    }
    masses
}

/// `_earning_region_fee` (py 894-900): revenue-mass-weighted mean fee over
/// earning windows, or `None` if no positive mass exists.
pub fn earning_region_fee(state: &GaussianThompsonState, now: i64) -> Option<f64> {
    let masses = positive_revenue_mass(state, now);
    let total: f64 = py_sum(masses.iter().map(|(_, m)| *m));
    if total <= 0.0 {
        return None;
    }
    Some(py_sum(masses.iter().map(|(f, m)| f * m)) / total)
}

// ---------------------------------------------------------------------------
// Legacy Normal-Normal fallback (py 1578-1630).
// ---------------------------------------------------------------------------

/// `_recompute_posterior_legacy` (py 1578-1630): legacy Normal-Normal
/// conjugate posterior, used as a fallback for narrow fee ranges, singular
/// fits, or too few observations. When `weighted_obs` is `None`, rebuilds
/// the weighted list from `state.observations` using `now` (py 1593-1605,
/// `age_hours` NOT clamped, matching the core recompute's own formula).
pub fn recompute_posterior_legacy(
    state: &mut GaussianThompsonState,
    weighted_obs: Option<&[(f64, f64, f64)]>,
    now: i64,
) {
    let owned;
    let wobs: &[(f64, f64, f64)] = match weighted_obs {
        Some(w) => w,
        None => {
            if state.observations.is_empty() {
                state.posterior_mean = state.prior_mean_fee;
                state.posterior_std = state.prior_std_fee;
                return;
            }
            let mut v = Vec::with_capacity(state.observations.len());
            for obs in &state.observations {
                // py 1600: age_hours is NOT clamped to >= 0 here.
                let age_hours = (now - obs.ts) as f64 / 3600.0;
                let decay = py_pow(0.5, age_hours / DECAY_HOURS);
                let weight = obs.weight * decay;
                if weight < 1e-6 {
                    continue;
                }
                v.push((obs.fee, obs.revenue_rate, weight));
            }
            owned = v;
            &owned
        }
    };

    let total_weight: f64 = py_sum(wobs.iter().map(|(_, _, w)| *w));
    if total_weight > 0.1 {
        let weighted_sum: f64 = py_sum(wobs.iter().map(|(f, _, w)| f * w));
        let weighted_sq_sum: f64 = py_sum(wobs.iter().map(|(f, _, w)| f * f * w));
        let obs_mean = weighted_sum / total_weight;
        // `py_pow(_, 2.0)` for every VARIABLE squaring in this module:
        // CPython `x ** 2` calls libm pow(x, 2.0), which differs from a
        // multiply/`.powi(2)` in the last bit for ~0.086% of doubles
        // (2026-07-22 audit H1; pinned by posterior/pow_canary.json).
        // Constant squarings (MIN_STD * MIN_STD) are exact either way.
        let mut variance = (weighted_sq_sum / total_weight) - py_pow(obs_mean, 2.0);
        variance = variance.max(0.0);
        variance = variance.max(MIN_STD * MIN_STD);

        let prior_precision =
            1.0 / (MIN_STD * MIN_STD).max(py_pow(state.prior_std_fee, 2.0));
        let data_precision = total_weight / variance;
        let posterior_precision = prior_precision + data_precision;

        state.posterior_mean = (prior_precision * state.prior_mean_fee + data_precision * obs_mean)
            / posterior_precision;
        // SL-4: relative floor — see REL_MIN_STD_FRAC.
        state.posterior_std = MIN_STD
            .max(REL_MIN_STD_FRAC * state.posterior_mean.abs())
            .max(1.0 / posterior_precision.sqrt());
    } else {
        state.posterior_mean = state.prior_mean_fee;
        state.posterior_std = state.prior_std_fee;
    }
}

// ---------------------------------------------------------------------------
// Core recompute (py 1307-1576).
// ---------------------------------------------------------------------------

/// `_recompute_posterior_core` (py 1307-1576) verbatim: Bayesian polynomial
/// regression `R(F) = a*F^2 + b*F + c`, with the zero-revenue-regime
/// anchor and legacy Normal-Normal fallbacks. See the module doc comment
/// for the discounting order-of-operations this feeds.
pub fn recompute_posterior_core(state: &mut GaussianThompsonState, now: i64) {
    if state.observations.is_empty() {
        state.posterior_mean = state.prior_mean_fee;
        state.posterior_std = state.prior_std_fee;
        state.charged_fee_mean = 0.0;
        return;
    }

    // Collect weighted observations with time decay (py 1328-1351).
    // SL-3: zero-probe pseudo-observations are excluded from the fit and
    // the charged-fee reference, but stay in the anchor pool (their one
    // coherent role is the zero-regime anchor's downward gradient). The
    // weight cutoff (< 1e-6) excludes from BOTH — anchor_pool.append
    // happens after the cutoff and before the zero-probe skip.
    let mut weighted_obs: Vec<(f64, f64, f64)> = Vec::new(); // (fee, revenue, weight)
    let mut anchor_pool: Vec<(f64, f64, i64)> = Vec::new(); // (fee, weight, ts) incl. probes
    let mut fee_min = f64::INFINITY;
    let mut fee_max = f64::NEG_INFINITY;

    for obs in &state.observations {
        // py 1340: age_hours is NOT clamped to >= 0 here.
        let age_hours = (now - obs.ts) as f64 / 3600.0;
        let decay = py_pow(0.5, age_hours / DECAY_HOURS);
        let weight = obs.weight * decay;
        if weight < 1e-6 {
            continue;
        }
        anchor_pool.push((obs.fee, weight, obs.ts));
        if obs.flag.as_deref() == Some(ZERO_PROBE_FLAG) {
            continue;
        }
        weighted_obs.push((obs.fee, obs.revenue_rate, weight));
        fee_min = fee_min.min(obs.fee);
        fee_max = fee_max.max(obs.fee);
    }

    // Track the weighted mean of charged fees (py 1353-1356): only
    // overwritten when total_w > 0 — the stale value is retained otherwise.
    let total_w: f64 = py_sum(weighted_obs.iter().map(|(_, _, w)| *w));
    if total_w > 0.0 {
        let charged_sum: f64 = py_sum(weighted_obs.iter().map(|(f, _, w)| f * w));
        state.charged_fee_mean = charged_sum / total_w;
    }

    // Zero-revenue regime anchor (py 1358-1444).
    let zero_mass = py_sum(weighted_obs.iter().map(|(_, rev, w)| rev * w)) <= 1e-9;
    let streak_override =
        state.zero_revenue_streak >= ZERO_REGIME_STREAK_OVERRIDE && state.zero_run_start_ts > 0;
    let anchor_w: f64 = py_sum(anchor_pool.iter().map(|(_, w, _)| *w));

    if anchor_w > 0.0 && (zero_mass || streak_override) {
        let earning_anchor = earning_region_fee(state, now);
        if streak_override {
            if let Some(earning_anchor) = earning_anchor {
                let fees_pos: Vec<f64> = positive_revenue_mass(state, now)
                    .into_iter()
                    .map(|(f, _)| f)
                    .collect();
                let spread_std = if fees_pos.len() > 1 {
                    (fees_pos.iter().copied().fold(f64::NEG_INFINITY, f64::max)
                        - fees_pos.iter().copied().fold(f64::INFINITY, f64::min))
                        / 4.0
                } else {
                    0.0
                };
                let max_std = (1.0f64 / MIN_PRECISION).sqrt();
                state.posterior_mean = earning_anchor;
                state.posterior_std =
                    MIN_STD.max(max_std.min(spread_std.max(ZERO_REGIME_REL_STD * earning_anchor)));
                // Degenerate range => polynomial sampling disabled.
                state.last_fee_min = 0.0;
                state.last_fee_max = 0.0;
                return;
            }
        }

        // No earning history: recency-emphasised anchor over charged and
        // probed fees (py 1406-1444). The ZERO_REGIME_ANCHOR_HALF_LIFE_HOURS
        // half-life is used here (NOT the global DECAY_HOURS), and age_hours
        // IS clamped to >= 0 (py 1411 `max(0.0, ...)`).
        let anchor_all: Vec<(f64, f64, i64)> = anchor_pool
            .iter()
            .map(|&(f, w, ts)| {
                let age_hours = ((now - ts) as f64 / 3600.0).max(0.0);
                let decay = py_pow(0.5, age_hours / ZERO_REGIME_ANCHOR_HALF_LIFE_HOURS);
                (f, w * decay, ts)
            })
            .collect();
        let mut pairs: Vec<(f64, f64)> = if streak_override {
            anchor_all
                .iter()
                .filter(|&&(_, _, ts)| ts >= state.zero_run_start_ts)
                .map(|&(f, w, _)| (f, w))
                .collect()
        } else {
            Vec::new()
        };
        if pairs.is_empty() {
            pairs = anchor_all.iter().map(|&(f, w, _)| (f, w)).collect();
        }
        let pair_w: f64 = py_sum(pairs.iter().map(|(_, w)| *w));
        if pair_w > 0.0 {
            let anchor_mean = py_sum(pairs.iter().map(|(f, w)| f * w)) / pair_w;
            let fees: Vec<f64> = pairs.iter().map(|(f, _)| *f).collect();
            let spread_std = if fees.len() > 1 {
                (fees.iter().copied().fold(f64::NEG_INFINITY, f64::max)
                    - fees.iter().copied().fold(f64::INFINITY, f64::min))
                    / 4.0
            } else {
                0.0
            };
            let max_std = (1.0f64 / MIN_PRECISION).sqrt();
            state.posterior_mean = anchor_mean;
            state.posterior_std =
                MIN_STD.max(max_std.min(spread_std.max(ZERO_REGIME_REL_STD * anchor_mean)));
            // Degenerate range => _sample_from_polynomial_posterior returns None.
            state.last_fee_min = 0.0;
            state.last_fee_max = 0.0;
            return;
        }
        // pair_w <= 0: fall through to the normal fit path below (py has no
        // explicit else here — it simply doesn't return).
    }

    if weighted_obs.len() < 3 {
        // Need at least 3 points for a 3-parameter polynomial fit.
        recompute_posterior_legacy(state, Some(&weighted_obs), now);
        return;
    }

    let fee_range = fee_max - fee_min;
    if fee_range < 5.0 {
        // Too narrow to fit quadratic — use legacy Normal-Normal.
        recompute_posterior_legacy(state, Some(&weighted_obs), now);
        return;
    }

    // Normalize fees to [0, 1] for numerical stability (py 1457-1483).
    let inv_range = 1.0 / fee_range;
    let sigma2 = 10.0f64.max(state.noise_variance);
    let inv_sigma2 = 1.0 / sigma2;

    // Use the FIXED prior (not stored posterior) to avoid precision
    // accumulation.
    let l0 = state.prior_precision;
    let mu0 = state.prior_coeffs;
    let l0_mu0 = matvec3(&l0, &mu0);

    let mut ln = l0;
    let mut rhs = l0_mu0;

    for &(fee_raw, rev, w) in &weighted_obs {
        let f = (fee_raw - fee_min) * inv_range; // Normalize to [0,1]
        let phi = [f * f, f, 1.0];
        let wi = w * inv_sigma2;
        for i in 0..3 {
            rhs[i] += wi * phi[i] * rev;
            for j in 0..3 {
                ln[i][j] += wi * phi[i] * phi[j];
            }
        }
    }

    // Invert Lambda_n for posterior covariance.
    let sigma_n = match invert3(&ln) {
        Some(s) => s,
        None => {
            recompute_posterior_legacy(state, Some(&weighted_obs), now);
            return;
        }
    };

    // Posterior mean coefficients.
    let mu_n = matvec3(&sigma_n, &rhs);

    // Update noise variance from residuals (degrees-of-freedom corrected,
    // blended) — py 1494-1504, computed AFTER solving for mu_n.
    let mut ss = 0.0;
    let mut sw = 0.0;
    for &(fee_raw, rev, w) in &weighted_obs {
        let f = (fee_raw - fee_min) * inv_range;
        let pred = mu_n[0] * f * f + mu_n[1] * f + mu_n[2];
        ss += w * py_pow(rev - pred, 2.0);
        sw += w;
    }
    let new_sigma2 = ss / (sw - 3.0).max(1.0);
    state.noise_variance = 10.0f64.max(0.7 * new_sigma2 + 0.3 * state.noise_variance);

    // Store polynomial posterior and fee range for sampling.
    state.posterior_coeffs = mu_n;
    state.posterior_precision = ln;
    state.last_fee_min = fee_min;
    state.last_fee_max = fee_max;

    // Derive posterior_mean (optimal fee) and posterior_std from the
    // polynomial (py 1512-1576).
    let a = mu_n[0];
    let b = mu_n[1];
    if a < -1e-8 {
        // Concave: optimal at -b/(2a), un-normalize. Allow safe
        // extrapolation up to 50% beyond the tested range.
        let f_star = (-b / (2.0 * a)).clamp(-0.5, 1.5);
        state.posterior_mean = f_star * fee_range + fee_min;

        // Propagated uncertainty via delta method (py 1549-1560).
        let da = b / (2.0 * a * a); // d f*/d a
        let db = -1.0 / (2.0 * a); // d f*/d b
        let dc = 0.0;
        let grad = [da, db, dc];
        let mut var_fstar = 0.0;
        for i in 0..3 {
            for j in 0..3 {
                var_fstar += grad[i] * sigma_n[i][j] * grad[j];
            }
        }
        state.posterior_std = MIN_STD.max(var_fstar.max(0.0).sqrt() * fee_range);
    } else {
        // Non-concave: pick the best fee REGION by expected rate (LCB over
        // ~10% log-fee buckets), not the single best window (py 1520-1546).
        // Bucket iteration order must match Python dict insertion order
        // (first-seen key wins ties on `lcb > best_lcb`, strict >).
        let mut bucket_order: Vec<i64> = Vec::new();
        let mut buckets: std::collections::HashMap<i64, Vec<(f64, f64, f64)>> =
            std::collections::HashMap::new();
        for &(fee_raw, rev, w) in &weighted_obs {
            let key = (fee_raw.max(1.0).ln() / 1.1f64.ln()) as i64;
            buckets
                .entry(key)
                .or_insert_with(|| {
                    bucket_order.push(key);
                    Vec::new()
                })
                .push((fee_raw, rev, w));
        }
        let mut best_fee = fee_min;
        let mut best_lcb = f64::NEG_INFINITY;
        for key in &bucket_order {
            let entries = &buckets[key];
            let bw: f64 = py_sum(entries.iter().map(|(_, _, w)| *w));
            if bw <= 0.0 {
                continue;
            }
            let mean_rev: f64 = py_sum(entries.iter().map(|(_, r, w)| r * w)) / bw;
            let var: f64 =
                py_sum(entries.iter().map(|(_, r, w)| w * py_pow(r - mean_rev, 2.0))) / bw;
            let sq: f64 = py_sum(entries.iter().map(|(_, _, w)| w * w));
            let n_eff = if sq > 0.0 { bw * bw / sq } else { 1.0 };
            let lcb = mean_rev - var.max(0.0).sqrt() / n_eff.max(1.0).sqrt();
            if lcb > best_lcb {
                best_lcb = lcb;
                best_fee = py_sum(entries.iter().map(|(f, _, w)| f * w)) / bw;
            }
        }
        state.posterior_mean = best_fee;

        // Non-concave fallback std: observation spread, inflated as total
        // observation mass decays (py 1561-1576).
        let fees: Vec<f64> = weighted_obs.iter().map(|(f, _, _)| *f).collect();
        let spread_std = (fees.iter().copied().fold(f64::NEG_INFINITY, f64::max)
            - fees.iter().copied().fold(f64::INFINITY, f64::min))
            / 4.0;
        let inflation = (weighted_obs.len() as f64 / total_w.max(1e-6)).sqrt();
        let max_std = (1.0 / MIN_PRECISION).sqrt();
        state.posterior_std = MIN_STD.max(max_std.min(spread_std * inflation));
    }
}

/// `_recompute_posterior` (py 1296-1305): core rebuild, then re-apply
/// durable out-of-band nudges (`posterior_bias`) with decay so they are
/// not lost by the recompute.
pub fn recompute_posterior(state: &mut GaussianThompsonState, now: i64) {
    recompute_posterior_at_times(state, now, now);
}

/// Replay-aware form of [`recompute_posterior`] preserving Python's
/// separate `_recompute_posterior` and `_apply_posterior_bias` clock reads.
pub fn recompute_posterior_at_times(
    state: &mut GaussianThompsonState,
    recompute_now: i64,
    bias_now: i64,
) {
    recompute_posterior_core(state, recompute_now);
    apply_posterior_bias(state, bias_now);
}

/// `_blend_posterior_toward` (py 1226-1244): mean-only blend toward a
/// target, `weight/(1+weight)` of the distance. Never touches `posterior_std`.
/// `pub(crate)`: `dynamics::record_posterior_nudge` (Task 7) applies the
/// same immediate blend — single definition, never re-duplicated.
pub(crate) fn blend_posterior_toward(
    state: &mut GaussianThompsonState,
    target_fee: f64,
    weight: f64,
) {
    if weight <= 0.0 {
        return;
    }
    let frac = weight / (1.0 + weight);
    state.posterior_mean += (target_fee - state.posterior_mean) * frac;
}

/// `_apply_posterior_bias` (py 1275-1294): re-apply recorded nudges after a
/// posterior rebuild, with time decay; expired nudges (decayed weight below
/// [`BIAS_MIN_WEIGHT`]) are pruned.
fn apply_posterior_bias(state: &mut GaussianThompsonState, now: i64) {
    if state.posterior_bias.is_empty() {
        return;
    }
    let entries = state.posterior_bias.clone();
    let mut kept: Vec<(f64, f64, i64)> = Vec::new();
    for (target_fee, weight, ts) in entries {
        let age_hours = ((now - ts) as f64 / 3600.0).max(0.0);
        let decayed = weight * py_pow(0.5, age_hours / BIAS_DECAY_HOURS);
        if decayed < BIAS_MIN_WEIGHT {
            continue; // Expired — prune.
        }
        kept.push((target_fee, weight, ts));
        blend_posterior_toward(state, target_fee, decayed);
    }
    state.posterior_bias = kept;
}

// ---------------------------------------------------------------------------
// DTS discounting (py 1672-1719).
// ---------------------------------------------------------------------------

/// `apply_dts_discount` (py 1672-1719): widens the Gaussian posterior by
/// reducing precision, decays the polynomial posterior precision matrix,
/// and persistently discounts every stored observation's base weight. See
/// the module doc comment for the full order-of-operations contract this
/// feeds into. No-op when `gamma` is not strictly inside `(0.0, 1.0)`.
pub fn apply_dts_discount(state: &mut GaussianThompsonState, gamma: f64) {
    if !(0.0 < gamma && gamma < 1.0) {
        return;
    }

    // (a) Gaussian posterior: widen by reducing precision. `py_pow`, not a
    // multiply: see the audit-H1 comment in `recompute_posterior_legacy`.
    let mut precision = 1.0 / py_pow(state.posterior_std, 2.0).max(1.0);
    precision *= gamma;
    precision = precision.max(MIN_PRECISION);
    state.posterior_std = (1.0 / precision).sqrt();

    // (b) Polynomial posterior: decay precision matrix.
    for row in state.posterior_precision.iter_mut() {
        for cell in row.iter_mut() {
            *cell *= gamma;
        }
    }

    // (c) Persistent forgetting: decay each stored observation's base
    // weight so the NEXT posterior rebuild also reflects the discount.
    // Never decay below DISCOUNT_WEIGHT_FLOOR and never raise a weight
    // that is already below the floor.
    for obs in state.observations.iter_mut() {
        let base_weight = obs.weight;
        obs.weight = base_weight
            .min(DISCOUNT_WEIGHT_FLOOR)
            .max(base_weight * gamma);
    }
}
