//! Frozen ADR-001 rail order: `cooldown(deadband(rate_limit(rails(raw_target))))`
//! with DTS+PID as the authoritative controller. This module ports the pure
//! rail stages: fee-step cap, target blend, damping (ADR-001 stage 2
//! rate_limit + stage 4 cooldown wake-variant), low-fee exploration target,
//! and the zero-flow ratchet guard — verbatim from
//! `modules/fee_controller.py:5124-5457`, plus the small helper curves
//! `_kalman_demand_factor` (2708-2731) and `_exploration_std_threshold`
//! (2732-2776).
//!
//! Rail ORDER and every constant here are load-bearing (Phase 4 Global
//! Constraints): clamp orderings encode named production incidents (the
//! 2026-06-12 fee runaway, the 2026-07-03 zero-flow floor-pinning fixes).
//! Comparisons are ported with the exact strict `>` / `>=` gates the
//! Python code uses — do not "simplify" a boundary.

use crate::profiles::{
    FeeProfileSettings, EXPLORATION_FEE_MULTIPLIER, EXPLORATION_HEADROOM_RATIO,
    EXPLORATION_MAX_DISCOUNT_RATIO, EXPLORATION_SPARSE_HEADROOM_RATIO,
};

// ---------------------------------------------------------------------------
// Constants transcribed verbatim from the `FeeController`/`GaussianThompsonState`
// class bodies (grep the class body in fee_controller.py for each name).
// ---------------------------------------------------------------------------

/// `UNROUTABLE_SPENDABLE_SATS` (py class body, Issue-SL-1 section).
pub const UNROUTABLE_SPENDABLE_SATS: i64 = 25_000;

/// `GOSSIP_GATE_SUPPRESSION_RATIO` (py class body, P2 fix section). Not
/// consumed by any function in this module — pinned here (same source
/// lines) for the Wave-3 cycle orchestrator that gates gossip broadcasts.
pub const GOSSIP_GATE_SUPPRESSION_RATIO: f64 = 0.05;

/// `ZERO_FLOW_GUARD_STREAK` (py class body, Issue #20 section).
pub const ZERO_FLOW_GUARD_STREAK: i64 = 8;
/// `ZERO_FLOW_DOWNSHIFT_STREAK`.
pub const ZERO_FLOW_DOWNSHIFT_STREAK: i64 = 24;
/// `ZERO_FLOW_DOWNSHIFT_RATIO`.
pub const ZERO_FLOW_DOWNSHIFT_RATIO: f64 = 0.85;
/// `ZERO_FLOW_DOWNSHIFT_INTERVAL_CYCLES`.
pub const ZERO_FLOW_DOWNSHIFT_INTERVAL_CYCLES: i64 = 12;
/// `ZERO_FLOW_ANCHOR_FLOOR_FRAC`.
pub const ZERO_FLOW_ANCHOR_FLOOR_FRAC: f64 = 0.5;
/// `ZERO_FLOW_GAP_GUARD_MULT`.
pub const ZERO_FLOW_GAP_GUARD_MULT: f64 = 2.0;
/// `ZERO_FLOW_GAP_DOWNSHIFT_MULT`.
pub const ZERO_FLOW_GAP_DOWNSHIFT_MULT: f64 = 4.0;
/// `ZERO_FLOW_GAP_CAP_HOURS`.
pub const ZERO_FLOW_GAP_CAP_HOURS: f64 = 168.0;
/// Also present in the class body but unrelated to the Issue #20 zero-flow
/// mechanics; kept alongside for the same "grep the class body" pinning
/// task without pulling in the whole ceiling-reduction subsystem (that is
/// Task 6/8 territory).
pub const ZERO_FLOW_DAYS_MODERATE: i64 = 3;
pub const ZERO_FLOW_DAYS_SEVERE: i64 = 7;
pub const ZERO_FLOW_FEE_THRESHOLD: i64 = 500;
pub const ZERO_FLOW_REDUCTION_MODERATE: f64 = 0.75;
pub const ZERO_FLOW_REDUCTION_SEVERE: f64 = 0.50;

/// `KALMAN_DEMAND_FACTOR_MIN` (py class body, P5 fix section).
pub const KALMAN_DEMAND_FACTOR_MIN: f64 = 1.0;
/// `KALMAN_DEMAND_FACTOR_MAX`.
pub const KALMAN_DEMAND_FACTOR_MAX: f64 = 2.0;

/// `UNDERCUT_EXPLORATION_STD_THRESHOLD` (py class body, Phase B.3 section).
pub const UNDERCUT_EXPLORATION_STD_THRESHOLD: f64 = 100.0;

/// `GaussianThompsonState.REL_MIN_STD_FRAC` (py line 343). Re-exported
/// from its single definition in `thompson::recompute` (the constant's
/// natural home — it is a `GaussianThompsonState` class constant). Task 5
/// carried a temporary duplicate for Wave-1 file disjointness; reconciled
/// at Task 7 per the T5 review note.
pub use crate::thompson::recompute::REL_MIN_STD_FRAC;

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DampingDiag {
    pub requested_delta_ppm: i64,
    pub max_delta_ppm: i64,
    pub cap_reason: &'static str,
    pub cap_applied: bool,
    pub wake_damping_applied: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlendDiag {
    pub blend_ratio: f64,
    pub blended_delta_ppm: i64,
    pub sparse_data_conservative: bool,
}

// ---------------------------------------------------------------------------
// _get_fee_step_cap (py 5124-5143)
// ---------------------------------------------------------------------------

/// Maximum allowed per-cycle fee move for the current mode
/// (`_get_fee_step_cap`, py 5124-5143).
///
/// `scaled_delta = int(math.ceil(max(current_fee_ppm, 1) * ratio))`; result
/// is `max(min_delta, scaled_delta)`.
pub fn fee_step_cap(
    current_fee_ppm: i64,
    woke_from_sleep: bool,
    profile: &FeeProfileSettings,
) -> i64 {
    let ratio = if woke_from_sleep {
        profile.wake_cycle_max_delta_ratio
    } else {
        profile.normal_cycle_max_delta_ratio
    };
    let min_delta = if woke_from_sleep {
        profile.wake_cycle_min_delta_ppm
    } else {
        profile.normal_cycle_min_delta_ppm
    };
    let scaled_delta = (current_fee_ppm.max(1) as f64 * ratio).ceil() as i64;
    min_delta.max(scaled_delta)
}

// ---------------------------------------------------------------------------
// _get_target_blend_ratio / _blend_fee_target (py 5163-5235)
// ---------------------------------------------------------------------------

/// Variance-continuous blend ratio (`_get_target_blend_ratio`, py
/// 5163-5206). `sparse` is accepted (and threaded into [`BlendDiag`] by
/// [`blend_fee_target`]) but — as in the Python source (see its docstring:
/// "prior design gated the confidence boost on `not
/// sparse_data_conservative`... new mapping drives the ratio directly from
/// posterior_std") — does NOT affect the ratio itself; it is a vestigial
/// parameter kept for call-site compatibility.
pub fn target_blend_ratio(
    woke: bool,
    sparse: bool,
    posterior_std: f64,
    profile_name: &str,
    profile: &FeeProfileSettings,
) -> f64 {
    let _ = sparse; // py: parameter accepted but unused in the ratio math.

    let mut ratio = if posterior_std >= 200.0 {
        profile.sparse_target_blend_ratio
    } else if posterior_std >= 100.0 {
        0.30
    } else if posterior_std >= 50.0 {
        0.45
    } else {
        0.60
    };

    if profile_name != "active" {
        ratio = ratio.min(profile.normal_target_blend_ratio);
    }
    if woke {
        ratio = ratio.min(profile.wake_target_blend_ratio);
    }
    ratio
}

/// Move part-way toward the bounded target before delta capping
/// (`_blend_fee_target`, py 5208-5235).
///
/// `blended_delta = int(round(requested_delta * blend_ratio))` (banker's
/// rounding, `f64::round_ties_even`) with the +-1 minimum-step rule when
/// `requested_delta != 0` but `blended_delta == 0`.
pub fn blend_fee_target(
    current: i64,
    bounded_target: i64,
    woke: bool,
    sparse: bool,
    posterior_std: f64,
    profile_name: &str,
    profile: &FeeProfileSettings,
) -> (i64, BlendDiag) {
    let blend_ratio = target_blend_ratio(woke, sparse, posterior_std, profile_name, profile);
    let requested_delta = bounded_target - current;
    let mut blended_delta = (requested_delta as f64 * blend_ratio).round_ties_even() as i64;

    if requested_delta != 0 && blended_delta == 0 {
        blended_delta = if requested_delta > 0 { 1 } else { -1 };
    }

    let blended_target = current + blended_delta;
    (
        blended_target,
        BlendDiag {
            blend_ratio,
            blended_delta_ppm: blended_delta,
            sparse_data_conservative: sparse,
        },
    )
}

// ---------------------------------------------------------------------------
// _apply_damped_fee_target (py 5280-5313) — ADR-001 stage 2 (rate_limit) +
// stage 4 (cooldown wake-variant). Conformance 09/10/11 replay this
// function directly.
// ---------------------------------------------------------------------------

pub fn apply_damped_fee_target(
    current: i64,
    target: i64,
    woke: bool,
    profile: &FeeProfileSettings,
) -> (i64, DampingDiag) {
    let requested_delta = target - current;
    let max_delta_ppm = fee_step_cap(current, woke, profile);
    let mut cap_reason = "none";
    let mut cap_applied = false;

    let applied_fee_ppm = if requested_delta.abs() > max_delta_ppm {
        cap_applied = true;
        cap_reason = if woke {
            "wake_cycle_delta_cap"
        } else {
            "normal_cycle_delta_cap"
        };
        current
            + if requested_delta > 0 {
                max_delta_ppm
            } else {
                -max_delta_ppm
            }
    } else {
        target
    };

    (
        applied_fee_ppm,
        DampingDiag {
            requested_delta_ppm: requested_delta,
            max_delta_ppm,
            cap_reason,
            cap_applied,
            wake_damping_applied: woke,
        },
    )
}

// ---------------------------------------------------------------------------
// _get_exploration_fee_target (py 5237-5278)
// ---------------------------------------------------------------------------

/// Bounded low-fee exploration target (`_get_exploration_fee_target`, py
/// 5237-5278). `effective_min_fee_ppm` is the class-aware config floor for
/// this channel (E-2); falls back to `cfg_min_fee_ppm` when absent.
pub fn exploration_fee_target(
    current_fee_ppm: i64,
    floor_ppm: i64,
    cfg_min_fee_ppm: i64,
    sparse: bool,
    effective_min_fee_ppm: Option<i64>,
) -> i64 {
    let config_floor = effective_min_fee_ppm.unwrap_or(cfg_min_fee_ppm);
    let exploration_floor = floor_ppm.max(config_floor);
    if current_fee_ppm <= exploration_floor {
        return exploration_floor;
    }

    let mut discount_ratio = EXPLORATION_MAX_DISCOUNT_RATIO;
    let mut headroom_ratio = EXPLORATION_HEADROOM_RATIO;
    if sparse {
        discount_ratio *= 0.5;
        headroom_ratio = EXPLORATION_SPARSE_HEADROOM_RATIO;
    }

    let floor_candidate = (exploration_floor as f64 * EXPLORATION_FEE_MULTIPLIER).ceil() as i64;
    let headroom = (current_fee_ppm - exploration_floor).max(0);
    let headroom_candidate =
        exploration_floor + (headroom as f64 * headroom_ratio).round_ties_even() as i64;
    let discounted_ceiling =
        (current_fee_ppm as f64 * (1.0 - discount_ratio)).round_ties_even() as i64;
    let candidate = floor_candidate.max(headroom_candidate);
    exploration_floor.max(candidate.min(discounted_ceiling))
}

// ---------------------------------------------------------------------------
// _zero_flow_streak_thresholds (py 5315-5343)
// ---------------------------------------------------------------------------

/// `(guard_streak, downshift_streak)` scaled to the channel's cadence
/// (`_zero_flow_streak_thresholds`, py 5315-5343).
pub fn zero_flow_streak_thresholds(gap_ema_hours: f64, cycle_hours: f64) -> (i64, i64) {
    let guard = ZERO_FLOW_GUARD_STREAK;
    let downshift = ZERO_FLOW_DOWNSHIFT_STREAK;

    if !gap_ema_hours.is_finite()
        || gap_ema_hours <= 0.0
        || !cycle_hours.is_finite()
        || cycle_hours <= 0.0
    {
        return (guard, downshift);
    }

    let gap = gap_ema_hours.min(ZERO_FLOW_GAP_CAP_HOURS);
    let gap_cycles = gap / cycle_hours;
    let guard = guard.max((gap_cycles * ZERO_FLOW_GAP_GUARD_MULT).ceil() as i64);
    let downshift = downshift.max((gap_cycles * ZERO_FLOW_GAP_DOWNSHIFT_MULT).ceil() as i64);
    (guard, downshift.max(guard))
}

// ---------------------------------------------------------------------------
// _apply_zero_flow_ratchet_guard (py 5345-5439)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default)]
pub struct ZeroFlowInputs {
    pub current_fee: i64,
    pub target_fee: i64,
    pub min_fee: i64,
    pub zero_revenue_streak: i64,
    pub forwards_since_update: i64,
    pub revenue_rate: f64,
    pub supported_fee_ceiling: Option<f64>,
    pub earning_anchor_ppm: Option<f64>,
    pub guard_streak: Option<i64>,
    pub downshift_streak: Option<i64>,
    pub rate_is_meaningful: Option<bool>,
}

/// Prevent stale DTS belief from raising fees during current silence
/// (`_apply_zero_flow_ratchet_guard`, py 5345-5439).
///
/// Tags frozen: `"zero_flow_ratchet_guard"` / `"zero_flow_downshift"` /
/// `"zero_flow_floor_override"`. Downshift fires only on interval
/// boundaries; the soft anchor floor and floor-override tag-honesty rules
/// are ported verbatim (py 5398-5439).
pub fn apply_zero_flow_ratchet_guard(i: &ZeroFlowInputs) -> (i64, Option<&'static str>) {
    let current = i.current_fee;
    let target = i.target_fee;
    let floor = i.min_fee.max(0);
    let streak = i.zero_revenue_streak.max(0);
    let mut forwards = i.forwards_since_update.max(0);
    let mut rate = i.revenue_rate;

    // L8: an economically-dead trickle counts as silence for the guard too.
    if i.rate_is_meaningful == Some(false) && rate > 0.0 {
        rate = 0.0;
        forwards = 0;
    }

    // Python: `int(guard_streak) if guard_streak else DEFAULT` — falsy
    // (None or 0) falls back to the default.
    let guard_thresh = i
        .guard_streak
        .filter(|&g| g != 0)
        .unwrap_or(ZERO_FLOW_GUARD_STREAK);
    let downshift_thresh = i
        .downshift_streak
        .filter(|&d| d != 0)
        .unwrap_or(ZERO_FLOW_DOWNSHIFT_STREAK);

    if rate != 0.0 || forwards != 0 || streak < guard_thresh {
        return (target, None);
    }

    let on_downshift_step = streak >= downshift_thresh
        && (streak - downshift_thresh) % ZERO_FLOW_DOWNSHIFT_INTERVAL_CYCLES == 0;

    if !on_downshift_step {
        let guarded = floor.max(target.min(current));
        // Guard-tag honesty: when the floor exceeds current, max(floor,..)
        // RAISES the fee ("hard floors win") — tag that distinctly.
        if guarded > current {
            return (guarded, Some("zero_flow_floor_override"));
        }
        return (guarded, Some("zero_flow_ratchet_guard"));
    }

    let mut downshift_cap = (current as f64 * ZERO_FLOW_DOWNSHIFT_RATIO).floor() as i64;
    let supported_cap = i.supported_fee_ceiling.unwrap_or(0.0);
    if supported_cap.is_finite() && supported_cap > 0.0 {
        downshift_cap = downshift_cap.min(supported_cap as i64);
    }

    // Soft decay floor at a fraction of the earning anchor: clamped to the
    // current fee so it can stop decay but never force a raise.
    let mut soft_floor = floor;
    let anchor = i.earning_anchor_ppm.unwrap_or(0.0);
    if anchor.is_finite() && anchor > 0.0 {
        let anchor_floor = current.min((anchor * ZERO_FLOW_ANCHOR_FLOOR_FRAC) as i64);
        soft_floor = soft_floor.max(anchor_floor);
    }

    let guarded = soft_floor.max(target.min(downshift_cap));
    if guarded > current {
        // Floor arm fired on the downshift branch: must not be tagged
        // "downshift".
        return (guarded, Some("zero_flow_floor_override"));
    }
    if guarded == current {
        // Anchor floor absorbed the whole step: a hold, not a downshift.
        return (guarded, Some("zero_flow_ratchet_guard"));
    }
    (guarded, Some("zero_flow_downshift"))
}

// ---------------------------------------------------------------------------
// _is_unroutable_zero_window (py 5441-5457)
// ---------------------------------------------------------------------------

/// True when a zero-revenue window is censored, not informative
/// (`_is_unroutable_zero_window`, py 5441-5457).
pub fn is_unroutable_zero_window(revenue_rate: f64, spendable_sats: f64) -> bool {
    revenue_rate <= 0.0 && spendable_sats < UNROUTABLE_SPENDABLE_SATS as f64
}

// ---------------------------------------------------------------------------
// _kalman_demand_factor (py 2708-2731)
// ---------------------------------------------------------------------------

/// Continuous, monotone demand-normalization factor (`_kalman_demand_factor`,
/// py 2708-2731). `clamp(expected_demand / 0.5, 1.0, 2.0)` — may only HALVE
/// an observation, never amplify one.
pub fn kalman_demand_factor(expected_demand: f64) -> f64 {
    let inner = (expected_demand / 0.5).min(KALMAN_DEMAND_FACTOR_MAX);
    inner.max(KALMAN_DEMAND_FACTOR_MIN)
}

// ---------------------------------------------------------------------------
// _exploration_std_threshold (py 2732-2776)
// ---------------------------------------------------------------------------

/// Explore-gate threshold composed with the SL-4 relative std floor
/// (`_exploration_std_threshold`, py 2732-2776). Callers must compare with
/// a STRICT `>` — a converged posterior sits exactly AT the relative
/// floor, and `>=` would re-create the absorbing state at the boundary.
pub fn exploration_std_threshold(current_fee_ppm: i64) -> f64 {
    let clamped_fee = current_fee_ppm.max(0);
    UNDERCUT_EXPLORATION_STD_THRESHOLD.max(REL_MIN_STD_FRAC * clamped_fee as f64)
}
