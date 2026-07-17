//! Thompson sampling paths: polynomial-posterior draws (Cholesky) with the
//! diagonal and Gaussian fallbacks, exploration scaling, and the bounded
//! contextual offset (port of `sample_fee` / `sample_fee_contextual` /
//! `_sample_from_polynomial_posterior` / `get_exploitation_fee`,
//! `modules/fee_controller.py:530-723` + `1632-1634`; Phase 4 Task 7).
//!
//! # RNG stream discipline (THE contract — fixture-enforced)
//!
//! All draws go through the injected `&mut PyRandom`. `sample_fee`
//! consumes, exactly:
//! - sparse path (fewer than MIN_OBSERVATIONS real observations): ONE
//!   `gauss` (prior draw, std `max(10, prior_std*1.1) * boost`);
//! - polynomial path, `invert3` + `cholesky3` succeed: THREE `gauss(0,1)`
//!   (each scaled by `noise_scale` BEFORE `matvec3(L, z)`);
//! - polynomial path, Cholesky fails: THREE `gauss(0,1)` (diagonal
//!   approximation `sqrt(max(1e-6, Sigma[i][i]))`);
//! - polynomial returns non-concave (None AFTER its three draws) or
//!   declines early (range < 5 / singular precision, NO draws): ONE more
//!   `gauss` (Gaussian posterior draw).
//!
//! The bias shift applies on the prior path and the polynomial-success
//! path but NOT the Gaussian-fallback path (`posterior_mean` already
//! carries the nudges via `_apply_posterior_bias` after every recompute).
//! `last_sampled_fee`/`last_sample_time` are stamped with the injected
//! `now` (Python stamps `int(time.time())`).

use super::dynamics::{posterior_bias_shift, real_observation_count};
use super::recompute::{CTX_CONFIDENCE_COUNT, CTX_OFFSET_CAP_FRAC, MIN_OBSERVATIONS};
use super::{GaussianThompsonState, EXPLORATION_BOOST_MAX, EXPLORATION_BOOST_MIN, MIN_STD};
use crate::mat3::{cholesky3, invert3, matvec3, V3};
use crate::pyrand::PyRandom;

/// `int(max(floor, min(ceiling, sampled)))` — Python `int()` truncates
/// toward zero, which `as i64` matches for finite in-range values.
fn clamp_fee(floor: i64, ceiling: i64, sampled: f64) -> i64 {
    (floor as f64).max((ceiling as f64).min(sampled)) as i64
}

/// `sample_fee` (py 542-602): Thompson sample from the posterior, clamped
/// to `[floor, ceiling]`. `exploration_multiplier` of `None`, non-finite
/// or non-positive falls back to 1.0, then clamps to
/// `[EXPLORATION_BOOST_MIN, EXPLORATION_BOOST_MAX]`.
pub fn sample_fee(
    state: &mut GaussianThompsonState,
    floor: i64,
    ceiling: i64,
    exploration_multiplier: Option<f64>,
    rng: &mut PyRandom,
    now: i64,
) -> i64 {
    // Python: float(None) raises -> 1.0; then finite/positive guard.
    let mut boost = exploration_multiplier.unwrap_or(1.0);
    if !boost.is_finite() || boost <= 0.0 {
        boost = 1.0;
    }
    boost = EXPLORATION_BOOST_MIN.max(EXPLORATION_BOOST_MAX.min(boost));

    let sampled;
    if real_observation_count(state) < MIN_OBSERVATIONS {
        // Sparse: use prior with extra exploration (clamped to MIN_STD
        // like the normal path). The prior ignores posterior_mean, so
        // advisory nudges must be applied here.
        let explore_std = MIN_STD.max(state.prior_std_fee * 1.1) * boost;
        let mut s = rng.gauss(state.prior_mean_fee, explore_std);
        s += posterior_bias_shift(state, s, now);
        sampled = s;
    } else {
        // Try polynomial posterior sampling; fall back to Gaussian.
        if let Some(mut s) = sample_from_polynomial_posterior(state, floor, ceiling, boost, rng) {
            // Polynomial draws come from the regression coefficients and
            // ignore posterior_mean entirely — apply the durable nudge
            // shift here so advisory signals reach the sampled fee.
            s += posterior_bias_shift(state, s, now);
            let sampled_fee = clamp_fee(floor, ceiling, s);
            state.last_sampled_fee = sampled_fee;
            state.last_sample_time = now;
            return sampled_fee;
        }
        // Fallback: sample from Gaussian posterior. No extra bias shift:
        // posterior_mean already carries the nudges.
        let modulated_std = MIN_STD.max(state.posterior_std) * boost;
        sampled = rng.gauss(state.posterior_mean, modulated_std);
    }

    let sampled_fee = clamp_fee(floor, ceiling, sampled);
    state.last_sampled_fee = sampled_fee;
    state.last_sample_time = now;
    sampled_fee
}

/// `sample_fee_contextual` (py 604-669): base draw from the global
/// (polynomial-first) posterior, plus a confidence-weighted, bounded
/// additive offset from the context posterior. The offset is the context
/// mean's distance from the charged-fee reference, clamped to
/// `±CTX_OFFSET_CAP_FRAC * |base|`, scaled by `count / (count + 10)`.
/// Contexts with fewer than MIN_OBSERVATIONS observations (or a
/// non-finite mean, or no entry at all) pass the base through unchanged.
pub fn sample_fee_contextual(
    state: &mut GaussianThompsonState,
    context_key: &str,
    floor: i64,
    ceiling: i64,
    exploration_multiplier: Option<f64>,
    rng: &mut PyRandom,
    now: i64,
) -> i64 {
    // Python only forwards the kwarg when explicitly given; both spellings
    // reach the same None-tolerant handling in sample_fee.
    let base = sample_fee(state, floor, ceiling, exploration_multiplier, rng, now);

    let ctx = match state
        .contextual_posteriors
        .iter()
        .find(|(k, _)| k == context_key)
    {
        Some((_, ctx)) => *ctx,
        None => return base,
    };

    // Both the 4-tuple and legacy 3-tuple layouts expose (mean, count) —
    // the only fields this path reads (py 643-646).
    let ctx_mean = ctx.mean;
    let ctx_count = ctx.count;

    if ctx_count < MIN_OBSERVATIONS {
        return base;
    }
    if !ctx_mean.is_finite() {
        return base;
    }

    // Confidence saturates smoothly with observation count.
    let confidence = ctx_count as f64 / (ctx_count as f64 + CTX_CONFIDENCE_COUNT);

    let reference = if state.charged_fee_mean > 0.0 {
        state.charged_fee_mean
    } else {
        state.posterior_mean
    };
    let offset = ctx_mean - reference;
    let cap = CTX_OFFSET_CAP_FRAC * (base as f64).abs();
    let offset = (-cap).max(cap.min(offset)) * confidence;

    let sampled_fee = clamp_fee(floor, ceiling, base as f64 + offset);
    state.last_sampled_fee = sampled_fee;
    state.last_sample_time = now;
    sampled_fee
}

/// `_sample_from_polynomial_posterior` (py 671-723): draw
/// `beta ~ N(mu, Sigma)` from the Bayesian posterior over `[a, b, c]`,
/// then find the optimal fee from the sampled quadratic. Returns `None`
/// (fall back to Gaussian) on a degenerate fee range, a singular
/// precision matrix, or a non-concave sampled quadratic. See the module
/// doc for the exact draw counts per branch.
fn sample_from_polynomial_posterior(
    state: &GaussianThompsonState,
    _floor: i64,
    _ceiling: i64,
    noise_scale: f64,
    rng: &mut PyRandom,
) -> Option<f64> {
    // Use the fee range from the last recompute to match normalization.
    let fee_min = state.last_fee_min;
    let fee_max = state.last_fee_max;
    let fee_range = fee_max - fee_min;
    if fee_range < 5.0 {
        return None;
    }

    // Invert precision to covariance.
    let sigma = invert3(&state.posterior_precision)?;

    // Cholesky decompose for sampling.
    let beta_sampled: V3 = match cholesky3(&sigma) {
        None => {
            // Fallback: diagonal approximation. Draw order (z first, in
            // index order) and the noise_scale-on-draw shape are verbatim.
            let diag = [
                sigma[0][0].max(1e-6),
                sigma[1][1].max(1e-6),
                sigma[2][2].max(1e-6),
            ];
            let z = [
                rng.gauss(0.0, 1.0) * noise_scale,
                rng.gauss(0.0, 1.0) * noise_scale,
                rng.gauss(0.0, 1.0) * noise_scale,
            ];
            [
                state.posterior_coeffs[0] + z[0] * diag[0].sqrt(),
                state.posterior_coeffs[1] + z[1] * diag[1].sqrt(),
                state.posterior_coeffs[2] + z[2] * diag[2].sqrt(),
            ]
        }
        Some(l) => {
            let z = [
                rng.gauss(0.0, 1.0) * noise_scale,
                rng.gauss(0.0, 1.0) * noise_scale,
                rng.gauss(0.0, 1.0) * noise_scale,
            ];
            let lz = matvec3(&l, &z);
            [
                state.posterior_coeffs[0] + lz[0],
                state.posterior_coeffs[1] + lz[1],
                state.posterior_coeffs[2] + lz[2],
            ]
        }
    };

    let (a_s, b_s) = (beta_sampled[0], beta_sampled[1]);
    if a_s < -1e-8 {
        // Concave: optimal at -b/(2a) in normalized space, with slight
        // extrapolation allowed.
        let f_star_norm = -b_s / (2.0 * a_s);
        let f_star_norm = (-0.2f64).max(1.2f64.min(f_star_norm));
        Some(f_star_norm * fee_range + fee_min)
    } else {
        // Non-concave sample: fall back to Gaussian (draws already spent).
        None
    }
}

/// `get_exploitation_fee` (py 1632-1634): the current best estimate
/// (posterior mean) without exploration — `int()` truncation toward zero.
pub fn get_exploitation_fee(state: &GaussianThompsonState) -> i64 {
    state.posterior_mean as i64
}
