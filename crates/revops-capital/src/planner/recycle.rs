//! Port of `_apply_redeployment_ev_demotion` (py `modules/capacity_planner.py`
//! 1454-1483) and the pure selection core of `_evaluate_recycle_opportunities`
//! (py 2016-2108), both built on the already-ported
//! [`super::ev::calculate_redeployment_ev`] /
//! [`super::ev::calculate_recycle_ev`] / [`super::ev::calculate_open_ev`].
//!
//! `_evaluate_recycle_opportunities`'s eligibility filtering (py 2064-2079:
//! [`super::ev::is_recycle_eligible`] plus a `_close_protection_reason`
//! check) is NOT re-done here — [`find_best_recycle_pair`] takes an
//! ALREADY-ELIGIBLE loser list; the caller applies
//! [`super::ev::is_recycle_eligible`] and the protection check itself
//! (both are already pure, already-ported, single-call checks — re-wrapping
//! them here would only add indirection, not new ported behavior).

use super::ev::{
    calculate_open_ev, calculate_recycle_ev, calculate_redeployment_ev, OpenEvInputs,
    RedeploymentCandidate, RECYCLE_MIN_EV_SATS,
};
use super::losers::{Loser, LoserAction};

/// Port of `_apply_redeployment_ev_demotion` (py 1454-1483): prices every
/// `CLOSE` loser (that isn't a fire-sale or hard-bleeder — those bypass
/// pricing, py 1468-1470) against the best available winner redeployment;
/// demotes to `DEFIBRILLATE` when the redeployment EV isn't positive.
/// Mutates `losers` in place, matching Python's in-place dict mutation.
pub fn apply_redeployment_ev_demotion(losers: &mut [Loser], winners: &[RedeploymentCandidate]) {
    for loser in losers.iter_mut() {
        if loser.action != LoserAction::Close {
            continue;
        }
        if loser.is_fire_sale || loser.is_hard_bleeder {
            continue;
        }
        let (ev, _best_peer, _winner_ev) = calculate_redeployment_ev(
            loser.marginal_profit_30d_sats,
            loser.capacity,
            loser.estimated_closure_cost_sats,
            winners,
        );
        if ev <= 0.0 {
            loser.action = LoserAction::Defibrillate;
            loser.reason = format!("{} (NO PROFITABLE REDEPLOYMENT)", loser.reason);
        }
    }
}

/// One already-eligible loser (py's `loser` dict, the fields
/// `_calculate_recycle_ev` reads: `capacity`, `marginal_profit_30d_sats`,
/// plus `scid`/`peer_id` for reporting).
#[derive(Debug, Clone, Copy)]
pub struct EligibleLoser<'a> {
    pub scid: &'a str,
    pub peer_id: &'a str,
    pub capacity_sats: i64,
    pub marginal_profit_30d_sats: i64,
}

/// One recycle-candidate: an open-EV evidence template (py
/// `_calculate_open_ev(candidate["peer_id"], capacity, cfg)`, 2002) with
/// every field EXCEPT `channel_size_sats` already resolved — the loser's
/// capacity is substituted in per pairing, since Python recomputes open EV
/// fresh for every (candidate, loser) pair (2091-2092).
#[derive(Debug, Clone, Copy)]
pub struct RecycleCandidate<'a> {
    pub peer_id: &'a str,
    /// py's candidate `score`, used only to select the top-5 pool (2085).
    pub score: f64,
    pub open_ev_template: OpenEvInputs,
}

/// The chosen recycle pairing (py's `best_pair` dict, 2095-2099).
#[derive(Debug, Clone, PartialEq)]
pub struct RecyclePlan {
    pub loser_scid: String,
    pub loser_peer_id: String,
    pub candidate_peer_id: String,
    pub recycle_ev: f64,
}

/// Port of `_evaluate_recycle_opportunities`'s selection core (py
/// 2084-2108): rank candidates by score, take the top 5, and find the
/// (candidate, loser) pairing with the highest recycle EV strictly above
/// [`RECYCLE_MIN_EV_SATS`]. Ties keep the FIRST pairing found in
/// candidate-then-loser iteration order (py's `ev > best_ev` strict
/// inequality never replaces on a tie).
pub fn find_best_recycle_pair(
    eligible_losers: &[EligibleLoser],
    candidates: &[RecycleCandidate],
    close_cost_sats: i64,
) -> Option<RecyclePlan> {
    if eligible_losers.is_empty() || candidates.is_empty() {
        return None;
    }

    let mut sorted_candidates: Vec<&RecycleCandidate> = candidates.iter().collect();
    sorted_candidates.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    sorted_candidates.truncate(5);

    let mut best_ev = 0.0f64;
    let mut best: Option<RecyclePlan> = None;

    for candidate in &sorted_candidates {
        for loser in eligible_losers {
            let mut inputs = candidate.open_ev_template;
            inputs.channel_size_sats = loser.capacity_sats;
            let candidate_open_ev = calculate_open_ev(&inputs);
            let ev = calculate_recycle_ev(
                candidate_open_ev,
                loser.marginal_profit_30d_sats,
                close_cost_sats,
            );
            if ev > RECYCLE_MIN_EV_SATS && ev > best_ev {
                best_ev = ev;
                best = Some(RecyclePlan {
                    loser_scid: loser.scid.to_string(),
                    loser_peer_id: loser.peer_id.to_string(),
                    candidate_peer_id: candidate.peer_id.to_string(),
                    recycle_ev: ev,
                });
            }
        }
    }

    best
}
