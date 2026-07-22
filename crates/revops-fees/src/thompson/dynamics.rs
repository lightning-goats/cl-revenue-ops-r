//! Thompson dynamics: observation updates, streak/reference bookkeeping,
//! zero-probe machinery, supported-fee ceiling, upward-probe budget,
//! contextual posteriors, durable posterior nudges, the Vegas adjustment,
//! and the pure failed-forward math (port of the update paths in
//! `modules/fee_controller.py`, Phase 4 Task 7).
//!
//! Every function that Python computed with `int(time.time())` takes
//! `now: i64` (Global Constraints: clock injection everywhere). All
//! constants are imported from `thompson/mod.rs` / `thompson/recompute.rs`
//! — single definitions, never re-duplicated (Task 2/3 integration-merge
//! obligation).

use super::recompute::{
    earning_region_fee, effective_positive_rate_ref, positive_revenue_mass, py_sum,
    recompute_posterior_at_times, BIAS_DECAY_HOURS, BIAS_MIN_WEIGHT, CTX_PRECISION_DECAY,
    DECAY_HOURS, MAX_OBSERVATIONS, MEANINGFUL_GAP_EMA_ALPHA, NUDGE_DEDUP_TOLERANCE,
    POSITIVE_RATE_EMA_ALPHA, SECONDARY_EXPLORE_BOOST, SUPPORTED_CEILING_FLOOR_ESCAPE,
    SUPPORTED_CEILING_HEADROOM, SUPPORTED_CEILING_MASS_QUANTILE, TRICKLE_RESET_FRAC,
    UPWARD_PROBE_INTERVAL_HOURS, UPWARD_PROBE_MIN_STD, UPWARD_PROBE_STRETCH, ZERO_PROBE_FLOOR_FRAC,
    ZERO_PROBE_STEP_FRAC, ZERO_REVENUE_STREAK_THRESHOLD,
};
use super::{
    CtxPosterior, GaussianThompsonState, Observation, CONGESTION_OBS_FLAG, MAX_BIAS_NUDGES,
    MIN_STD, ZERO_PROBE_FLAG,
};

// `blend_posterior_toward` stays defined in recompute.rs (it is the same
// blend `_apply_posterior_bias` re-applies after every rebuild); this
// module only calls it.
use super::recompute::blend_posterior_toward;
use revops_econ::pyfloat::py_pow;

/// `FeeController.FEE_RELEVANT_FAILCODES` (py 8501): the
/// WIRE_FEE_INSUFFICIENT family — the only forward-failure codes that are
/// evidence about OUR fee.
pub const FEE_RELEVANT_FAILCODES: &[i64] = &[0x1000 | 12];

/// `real_observation_count` (py 530-540): count of genuine market windows
/// (SL-3). Zero-probe pseudo-observations are fabricated points at fees
/// never charged and must not satisfy "enough data" gates;
/// congestion-flagged windows are real market windows and count.
pub fn real_observation_count(state: &GaussianThompsonState) -> i64 {
    state
        .observations
        .iter()
        .filter(|obs| obs.flag.as_deref() != Some(ZERO_PROBE_FLAG))
        .count() as i64
}

/// `update_posterior` (py 725-833) verbatim ORDER:
/// (1) input guards (bad hours -> 1.0; bad/negative rate -> 0.0; bad fee ->
/// return, NO state change); (2) `weight = min(1.0, hours/6.0)`;
/// (3) `meaningful = rate > 0 && rate >= TRICKLE_RESET_FRAC *
/// effective_positive_rate_ref(now)`; (4) streak bookkeeping (meaningful:
/// reset streak + EMA updates incl. positive_rate_ref seeding /
/// EMA-on-DECAYED-ref and gap-EMA; else: stamp zero_run_start on
/// streak == 0, streak += 1); (5) append obs (6-tuple with "congestion"
/// flag when congested); (6) probe injection when `!meaningful && streak >=
/// 4 && zero_run_start_fee > 0 && posterior_mean >= 0.3 * (earning anchor
/// OR zero_run_start_fee)`: `probe_fee = max(1, int(fee * 0.9))`, only if
/// `probe_fee < fee`, appended as ("zero_probe", rev 0.0, SAME
/// weight/ts/bucket); (7) prune to last 200; (8) recompute_posterior.
pub fn update_posterior(
    state: &mut GaussianThompsonState,
    fee: f64,
    revenue_rate: f64,
    hours: f64,
    time_bucket: &str,
    congested: bool,
    now: i64,
) {
    update_posterior_at_times(
        state,
        fee,
        revenue_rate,
        hours,
        time_bucket,
        congested,
        now,
        now,
        now,
    );
}

/// Replay-aware form of [`update_posterior`] preserving Python's distinct
/// update, recompute, and optional bias-application timestamps.
#[allow(clippy::too_many_arguments)]
pub fn update_posterior_at_times(
    state: &mut GaussianThompsonState,
    fee: f64,
    revenue_rate: f64,
    hours: f64,
    time_bucket: &str,
    congested: bool,
    now: i64,
    recompute_now: i64,
    bias_now: i64,
) {
    // (1) Guards against NaN/Inf inputs that would corrupt the posterior.
    let mut hours = hours;
    let mut revenue_rate = revenue_rate;
    if !hours.is_finite() || hours <= 0.0 {
        hours = 1.0;
    }
    if !revenue_rate.is_finite() || revenue_rate < 0.0 {
        revenue_rate = 0.0;
    }
    if !fee.is_finite() || fee < 0.0 {
        return; // Skip corrupt observation entirely.
    }

    // (2) Exposure-time-only weight (see WEIGHT_SCHEME).
    let weight = 1.0f64.min(hours / 6.0);

    // (3) Trickle guard.
    let r#ref = effective_positive_rate_ref(state, now);
    let meaningful = revenue_rate > 0.0 && revenue_rate >= TRICKLE_RESET_FRAC * r#ref;

    // (4) Streak bookkeeping.
    if meaningful {
        state.zero_revenue_streak = 0;
        state.zero_run_start_fee = 0.0;
        state.zero_run_start_ts = 0;
        if state.positive_rate_ref <= 0.0 {
            state.positive_rate_ref = revenue_rate;
        } else {
            // TRAP (py 776-779): the EMA blends against the DECAYED
            // reference `ref`, not the stored positive_rate_ref.
            state.positive_rate_ref =
                (1.0 - POSITIVE_RATE_EMA_ALPHA) * r#ref + POSITIVE_RATE_EMA_ALPHA * revenue_rate;
        }
        state.positive_rate_ref_ts = now;
        // Cadence tracking: EMA of gaps between meaningful windows.
        if state.last_meaningful_ts > 0 && now > state.last_meaningful_ts {
            let gap_hours = (now - state.last_meaningful_ts) as f64 / 3600.0;
            if state.meaningful_gap_ema_hours <= 0.0 {
                state.meaningful_gap_ema_hours = gap_hours;
            } else {
                state.meaningful_gap_ema_hours = (1.0 - MEANINGFUL_GAP_EMA_ALPHA)
                    * state.meaningful_gap_ema_hours
                    + MEANINGFUL_GAP_EMA_ALPHA * gap_hours;
            }
        }
        state.last_meaningful_ts = now;
    } else {
        if state.zero_revenue_streak == 0 {
            state.zero_run_start_fee = fee; // float(fee)
            state.zero_run_start_ts = now;
        }
        state.zero_revenue_streak += 1;
    }

    // (5) Add observation (5-tuple, or 6-tuple when congestion-flagged).
    if congested {
        state.observations.push(Observation::with_flag(
            fee,
            revenue_rate,
            weight,
            now,
            time_bucket,
            CONGESTION_OBS_FLAG,
        ));
    } else {
        state.observations.push(Observation::new(
            fee,
            revenue_rate,
            weight,
            now,
            time_bucket,
        ));
    }

    // (6) Directional zero-revenue probing. The descent floor is relative
    // to the EARNING anchor when one exists; only without any earning
    // history does it fall back to the zero-run start fee. NOTE: the gate
    // reads posterior_mean from the PREVIOUS recompute (stale by design —
    // the recompute happens at step (8)).
    if !meaningful
        && state.zero_revenue_streak >= ZERO_REVENUE_STREAK_THRESHOLD
        && state.zero_run_start_fee > 0.0
    {
        let earning_anchor = earning_region_fee(state, now);
        // Python truthiness: `earning_anchor if earning_anchor else
        // zero_run_start_fee` — None AND 0.0 both fall back.
        let floor_ref = match earning_anchor {
            Some(anchor) if anchor != 0.0 => anchor,
            _ => state.zero_run_start_fee,
        };
        if state.posterior_mean >= ZERO_PROBE_FLOOR_FRAC * floor_ref {
            // probe_fee = max(1, int(fee * 0.9)) — int() truncates.
            let probe_fee = 1i64.max((fee * ZERO_PROBE_STEP_FRAC) as i64);
            if (probe_fee as f64) < fee {
                state.observations.push(Observation::with_flag(
                    probe_fee as f64,
                    0.0,
                    weight,
                    now,
                    time_bucket,
                    ZERO_PROBE_FLAG,
                ));
            }
        }
    }

    // (7) Prune old observations (keep the LAST MAX_OBSERVATIONS).
    if state.observations.len() as i64 > MAX_OBSERVATIONS {
        let excess = state.observations.len() - MAX_OBSERVATIONS as usize;
        state.observations.drain(..excess);
    }

    // (8) Recompute posterior (core rebuild + bias re-apply).
    recompute_posterior_at_times(state, recompute_now, bias_now);
}

/// `is_meaningful_rate` (py 835-849): the same trickle classification
/// `update_posterior` uses for streaks (L8: the zero-flow guard's silence
/// test must agree with the streak's).
pub fn is_meaningful_rate(state: &GaussianThompsonState, revenue_rate: f64, now: i64) -> bool {
    // Python coerces via float() (non-numeric -> False); an f64 argument
    // always parses. NaN fails `rate > 0` exactly as in Python.
    let r#ref = effective_positive_rate_ref(state, now);
    revenue_rate > 0.0 && revenue_rate >= TRICKLE_RESET_FRAC * r#ref
}

/// `supported_fee_ceiling` (py 902-940): the fee below which
/// `SUPPORTED_CEILING_MASS_QUANTILE` of the recency-weighted positive
/// revenue mass lies, times the headroom factor — or `None` when there is
/// no earning history. When `floor_ppm` is given (positive) and the
/// quantile sits at/below it, the cap widens to `floor_ppm *
/// SUPPORTED_CEILING_FLOOR_ESCAPE` (floor-escape). A bound, never an
/// attractor.
pub fn supported_fee_ceiling(
    state: &GaussianThompsonState,
    now: i64,
    floor_ppm: Option<f64>,
) -> Option<f64> {
    let mut masses = positive_revenue_mass(state, now);
    let total = py_sum(masses.iter().map(|(_, m)| *m));
    if total <= 0.0 {
        return None;
    }
    // masses.sort(key=lambda fm: fm[0]) — stable sort by fee ONLY (ties
    // keep their winsorized-list order).
    masses.sort_by(|a, b| a.0.partial_cmp(&b.0).expect("fee is never NaN"));
    let threshold = total * SUPPORTED_CEILING_MASS_QUANTILE;
    let mut acc = 0.0;
    let mut quantile_fee = masses[masses.len() - 1].0;
    for &(fee, mass) in &masses {
        acc += mass;
        if acc >= threshold {
            quantile_fee = fee;
            break;
        }
    }
    let mut ceiling = quantile_fee * SUPPORTED_CEILING_HEADROOM;
    if let Some(floor_ppm) = floor_ppm {
        if floor_ppm > 0.0 && quantile_fee <= floor_ppm {
            ceiling = ceiling.max(floor_ppm * SUPPORTED_CEILING_FLOOR_ESCAPE);
        }
    }
    Some(ceiling)
}

/// `maybe_upward_probe_cap` (py 942-974): one bounded extra headroom step
/// above the supported ceiling. Guard ORDER is load-bearing (do not
/// reorder): cap-parse -> finite/positive -> streak -> mean -> std ->
/// interval. The budget is NOT consumed here (L1) — the controller calls
/// [`consume_upward_probe`] once the applied fee actually crosses the
/// pre-stretch cap.
pub fn maybe_upward_probe_cap(
    state: &GaussianThompsonState,
    now: i64,
    supported_cap: f64,
) -> Option<f64> {
    // Python's float(supported_cap) parse always succeeds for an f64.
    let cap = supported_cap;
    if !cap.is_finite() || cap <= 0.0 {
        return None;
    }
    if state.zero_revenue_streak != 0 {
        return None;
    }
    if state.posterior_mean <= cap {
        return None;
    }
    if state.posterior_std < UPWARD_PROBE_MIN_STD {
        return None;
    }
    if state.last_upward_probe_ts > 0
        && ((now - state.last_upward_probe_ts) as f64) < UPWARD_PROBE_INTERVAL_HOURS * 3600.0
    {
        return None;
    }
    Some(cap * UPWARD_PROBE_STRETCH)
}

/// `consume_upward_probe` (py 976-978): stamp the upward-probe budget —
/// the market test actually ran.
pub fn consume_upward_probe(state: &mut GaussianThompsonState, now: i64) {
    state.last_upward_probe_ts = now;
}

/// `_time_similarity` (py 980-1004): same bucket = 1.0, adjacent = 0.5,
/// opposite (low vs peak) = 0.2.
fn time_similarity(bucket1: &str, bucket2: &str) -> f64 {
    if bucket1 == bucket2 {
        return 1.0;
    }
    match (bucket1, bucket2) {
        ("low", "normal") | ("normal", "low") | ("normal", "peak") | ("peak", "normal") => 0.5,
        _ => 0.2,
    }
}

/// Find a context entry's index in the insertion-ordered map.
fn ctx_position(state: &GaussianThompsonState, context_key: &str) -> Option<usize> {
    state
        .contextual_posteriors
        .iter()
        .position(|(k, _)| k == context_key)
}

/// `update_contextual` (py 1006-1107): Normal-Normal conjugate update of a
/// context posterior, with hierarchical-prior init (role "S" widens by
/// SECONDARY_EXPLORE_BOOST), 7-day precision decay from last_update, the
/// CTX_PRECISION_DECAY per-update decay, minimum precision 1/200^2,
/// time/revenue/role observation weights, cross-pollination of adjacent
/// buckets (count NOT incremented), and the prune to the 104 most-used
/// contexts when > 130.
///
/// TRAP: the prune is `dict(sorted(items, key=count, reverse=True)[:104])`
/// — Python's sorted() is STABLE, so ties keep insertion order (which the
/// insertion-ordered `Vec` map preserves), and the surviving map is
/// REORDERED to count-desc. A BTreeMap (or unstable sort) changes which
/// contexts survive.
pub fn update_contextual(
    state: &mut GaussianThompsonState,
    context_key: &str,
    fee: f64,
    revenue_rate: f64,
    time_bucket: &str,
    now: i64,
) {
    if ctx_position(state, context_key).is_none() {
        // Initialize from global posterior as hierarchical prior.
        let parts: Vec<&str> = if context_key.contains(':') {
            context_key.split(':').collect()
        } else {
            Vec::new()
        };
        let role = if parts.len() >= 3 { parts[2] } else { "P" };
        let mut init_std = state.posterior_std;
        if role == "S" {
            init_std = state.posterior_std * SECONDARY_EXPLORE_BOOST;
        }
        // `py_pow(_, 2.0)` for every VARIABLE squaring here (py `** 2` is
        // libm pow, not a multiply — 2026-07-22 audit H1, pinned by the
        // pow_parity_canary_contextual update sequence). The constant
        // MIN_STD * MIN_STD square is exact either way.
        let init_precision = 1.0 / py_pow(init_std, 2.0).max(MIN_STD * MIN_STD);
        state.contextual_posteriors.push((
            context_key.to_string(),
            CtxPosterior {
                mean: state.posterior_mean,
                precision: init_precision,
                count: 0,
                last_update: now,
                was_legacy_3tuple: false,
            },
        ));
    }

    let idx = ctx_position(state, context_key).expect("just ensured present");
    let ctx = state.contextual_posteriors[idx].1;
    // Legacy 3-tuple handling (py 1049-1052): the load path already
    // converted with the SAME formula (precision = 1/max(std^2, MIN_STD^2),
    // last_update = 0), so the stored fields are numerically identical.
    let ctx_mean = ctx.mean;
    let mut ctx_precision = ctx.precision;
    let ctx_count = ctx.count;
    let ctx_last_update = ctx.last_update;

    // Time decay on accumulated precision (7-day half-life).
    if ctx_last_update > 0 {
        let age_hours = (now - ctx_last_update) as f64 / 3600.0;
        // `py_pow(0.5, _)`, not a bare `0.5f64.powf(_)`: see
        // `revops_econ::pyfloat::py_pow`'s doc comment (LLVM's `-O2`+
        // constant-`0.5`-base `powf` -> `exp2` rewrite diverges from
        // CPython's `**`).
        let decay = py_pow(0.5, age_hours / DECAY_HOURS);
        ctx_precision *= decay;
    }

    // Per-update precision decay (bounds accumulation; re-learnable).
    ctx_precision *= CTX_PRECISION_DECAY;

    // Minimum precision (max std ~= 200).
    ctx_precision = ctx_precision.max(1.0 / (200.0 * 200.0));

    // Observation weight: time-aware + revenue-based + role boost.
    let parts: Vec<&str> = if context_key.contains(':') {
        context_key.split(':').collect()
    } else {
        Vec::new()
    };
    let ctx_time = if parts.len() >= 2 { parts[1] } else { "normal" };
    let ctx_role = if parts.len() >= 3 { parts[2] } else { "P" };
    let time_weight = time_similarity(time_bucket, ctx_time);

    let revenue_weight = 1.0f64.min((revenue_rate + 1.0) / 100.0);
    let role_boost = if ctx_role == "S" { 1.3 } else { 1.0 };

    let obs_variance = (MIN_STD * MIN_STD).max(py_pow(state.posterior_std, 2.0));
    let obs_precision = (revenue_weight * time_weight * role_boost) / obs_variance;

    // Normal-Normal conjugate update.
    let new_precision = ctx_precision + obs_precision;
    let new_mean = (ctx_precision * ctx_mean + obs_precision * fee) / new_precision;
    let new_count = ctx_count + 1;

    state.contextual_posteriors[idx].1 = CtxPosterior {
        mean: new_mean,
        precision: new_precision,
        count: new_count,
        last_update: now,
        was_legacy_3tuple: false,
    };

    // Cross-pollinate related time buckets on a full-weight observation.
    if time_weight == 1.0 {
        update_related_time_contexts(state, context_key, fee, revenue_rate, time_bucket);
    }

    // Prune to prevent memory bloat: keep the 104 most-used, count-desc,
    // STABLE on ties; the surviving map takes the sorted order.
    if state.contextual_posteriors.len() > 130 {
        state
            .contextual_posteriors
            .sort_by_key(|entry| std::cmp::Reverse(entry.1.count)); // stable sort
        state.contextual_posteriors.truncate(104);
    }
}

/// `_update_related_time_contexts` (py 1109-1168): cross-pollinate ADJACENT
/// time contexts at 0.1x revenue-weight precision, WITHOUT incrementing
/// their count (or refreshing their last_update).
fn update_related_time_contexts(
    state: &mut GaussianThompsonState,
    context_key: &str,
    fee: f64,
    revenue_rate: f64,
    observed_time: &str,
) {
    let parts: Vec<&str> = context_key.split(':').collect();
    if parts.len() != 3 {
        return;
    }
    let (balance, role) = (parts[0], parts[2]);

    let adjacent: &[&str] = match observed_time {
        "low" => &["normal"],
        "normal" => &["low", "peak"],
        "peak" => &["normal"],
        _ => &[],
    };

    for adj_time in adjacent {
        let adj_key = format!("{balance}:{adj_time}:{role}");
        if let Some(idx) = ctx_position(state, &adj_key) {
            let adj = state.contextual_posteriors[idx].1;
            // Legacy 3-tuple values were converted on load with the same
            // formula py 1156-1158 applies here (precision from std,
            // last = 0), so the stored fields are numerically identical.
            let (adj_mean, adj_precision, adj_count, adj_last) =
                (adj.mean, adj.precision, adj.count, adj.last_update);

            let revenue_weight = 1.0f64.min((revenue_rate + 1.0) / 100.0);
            let obs_variance = (MIN_STD * MIN_STD).max(py_pow(state.posterior_std, 2.0));
            let cross_precision = 0.1 * revenue_weight / obs_variance;

            let new_precision = adj_precision + cross_precision;
            let new_mean = (adj_precision * adj_mean + cross_precision * fee) / new_precision;
            // Count NOT incremented; the write is a 4-tuple (a legacy
            // 3-tuple entry is upgraded in place).
            state.contextual_posteriors[idx].1 = CtxPosterior {
                mean: new_mean,
                precision: new_precision,
                count: adj_count,
                last_update: adj_last,
                was_legacy_3tuple: false,
            };
        }
    }
}

/// `record_posterior_nudge` (py 1170-1224): apply and durably record an
/// out-of-band posterior nudge. Dedup (M4): a target within
/// `NUDGE_DEDUP_TOLERANCE` of an existing entry REFRESHES it (max weight,
/// new ts) with NO immediate re-blend; otherwise append (capped to the
/// last `MAX_BIAS_NUDGES`) and blend the mean immediately.
pub fn record_posterior_nudge(
    state: &mut GaussianThompsonState,
    target_fee: f64,
    weight: f64,
    now: i64,
) {
    // Python's float() coercion always succeeds for f64 arguments; the
    // finite/positivity guards below are verbatim.
    if !(target_fee.is_finite() && weight.is_finite()) {
        return;
    }
    if weight <= 0.0 || target_fee < 0.0 {
        return;
    }

    for i in 0..state.posterior_bias.len() {
        let (existing_target, existing_weight, _ts) = state.posterior_bias[i];
        if (existing_target - target_fee).abs()
            <= NUDGE_DEDUP_TOLERANCE * existing_target.max(target_fee).max(1.0)
        {
            state.posterior_bias[i] = (target_fee, existing_weight.max(weight), now);
            return;
        }
    }

    state.posterior_bias.push((target_fee, weight, now));
    if state.posterior_bias.len() > MAX_BIAS_NUDGES {
        let excess = state.posterior_bias.len() - MAX_BIAS_NUDGES;
        state.posterior_bias.drain(..excess);
    }

    // Immediate effect on the current posterior.
    blend_posterior_toward(state, target_fee, weight);
}

/// `_posterior_bias_shift` (py 1246-1273): the additive shift the active
/// (time-decayed) nudges imply for a sample — the same `w/(1+w)` blend per
/// nudge, applied SEQUENTIALLY to the running value. Entries below
/// `BIAS_MIN_WEIGHT` are skipped (NOT pruned — pruning is
/// `_apply_posterior_bias`'s job).
pub fn posterior_bias_shift(state: &GaussianThompsonState, base: f64, now: i64) -> f64 {
    if state.posterior_bias.is_empty() {
        return 0.0;
    }
    let mut shifted = base;
    for &(target_fee, weight, ts) in &state.posterior_bias {
        let age_hours = ((now - ts) as f64 / 3600.0).max(0.0);
        // `py_pow(0.5, _)`, not a bare `0.5f64.powf(_)` — see
        // `revops_econ::pyfloat::py_pow`'s doc comment.
        let decayed = weight * py_pow(0.5, age_hours / BIAS_DECAY_HOURS);
        if decayed < BIAS_MIN_WEIGHT {
            continue;
        }
        shifted += (target_fee - shifted) * (decayed / (1.0 + decayed));
    }
    shifted - base
}

/// `apply_vegas_adjustment` (py 1636-1659): when Vegas raises the floor
/// beyond 1.2x, boost posterior uncertainty (capped at 2.0x) and, if the
/// new floor exceeds the mean, route it through the durable nudge channel
/// (weight 0.43 reproduces the old 0.3 blend fraction; the M4 dedupe keeps
/// sustained spikes from accumulating entries).
pub fn apply_vegas_adjustment(
    state: &mut GaussianThompsonState,
    vegas_multiplier: f64,
    new_floor: f64,
    now: i64,
) {
    if vegas_multiplier <= 1.2 {
        return;
    }
    let boost = vegas_multiplier.min(2.0);
    state.posterior_std = MIN_STD.max(state.posterior_std * boost);
    if new_floor > state.posterior_mean {
        record_posterior_nudge(state, new_floor, 0.43, now);
    }
}

/// The pure weight math of `record_failed_forward` (py 8598-8605): base
/// weight 0.1 (10% of a settled forward), boosted up to 3x on a log10
/// amount scale when the amount is positive.
pub fn failed_forward_nudge_weight(amount_sats: f64) -> f64 {
    let mut base_weight = 0.1;
    // Python gates on `amount_msat > 0`; amount_sats = amount_msat / 1000
    // is positive exactly when amount_msat is.
    if amount_sats > 0.0 {
        let amount_boost = 3.0f64.min(1.0 + amount_sats.max(1.0).log10() / 3.0);
        base_weight *= amount_boost;
    }
    base_weight
}

/// The implied nudge target of `record_failed_forward` (py 8596):
/// `int(current_fee_ppm * 0.8)` — a fee-insufficient failure pulls toward
/// 80% of the fee that was rejected.
pub fn failed_forward_implied_fee(current_fee_ppm: i64) -> i64 {
    (current_fee_ppm as f64 * 0.8) as i64
}

/// `is_fee_relevant_failure` (py 8503-8525): true when a forward failure
/// is evidence about OUR fee. A present failcode SHORT-CIRCUITS (membership
/// in [`FEE_RELEVANT_FAILCODES`] decides, even when a failreason is also
/// present); only a missing failcode falls through to the
/// `"FEE_INSUFFICIENT" in failreason.upper()` test. No usable signal at
/// all -> false (a misdirected systematic signal is worse than none).
pub fn is_fee_relevant_failure(failcode: Option<i64>, failreason: Option<&str>) -> bool {
    if let Some(code) = failcode {
        return FEE_RELEVANT_FAILCODES.contains(&code);
    }
    if let Some(reason) = failreason {
        if reason.to_uppercase().contains("FEE_INSUFFICIENT") {
            return true;
        }
    }
    false
}
