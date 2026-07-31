//! `planner::recycle` composition tests. Both functions here build
//! directly on already-fixture-verified kernels
//! (`ev::calculate_redeployment_ev`, `ev::calculate_recycle_ev`,
//! `ev::calculate_open_ev` — see `tests/planner_ev.rs`); these tests are
//! hand-derived controls proving the COMPOSITION (mutation, ranking,
//! threshold, tie-breaking), matching this crate's existing precedent for
//! non-Python-fixtured functions (`ENTRYPOINTS.md`'s "Fixture generation").

use revops_capital::planner::ev::{OpenEvInputs, RedeploymentCandidate, RECYCLE_MIN_EV_SATS};
use revops_capital::planner::losers::{Loser, LoserAction};
use revops_capital::planner::recycle::{
    apply_redeployment_ev_demotion, find_best_recycle_pair, EligibleLoser, RecycleCandidate,
};

fn base_loser(action: LoserAction, is_fire_sale: bool, is_hard_bleeder: bool) -> Loser {
    Loser {
        scid: "700000x1x0".to_string(),
        peer_id: "loser1".to_string(),
        reason: "STAGNANT".to_string(),
        roi: -10.0,
        marginal_roi: -10.0,
        classification: "underwater".to_string(),
        capacity: 1_000_000,
        estimated_closure_cost_sats: 3000,
        rebal_difficulty: 0.0,
        opener: "local".to_string(),
        action,
        is_hard_bleeder,
        uptime_pct: None,
        regime_change: false,
        is_fire_sale,
        marginal_profit_30d_sats: -500,
        dead_capital_stage: None,
        close_protection: None,
    }
}

// --- apply_redeployment_ev_demotion -----------------------------------------

#[test]
fn positive_redeployment_ev_keeps_close() {
    let mut losers = vec![base_loser(LoserAction::Close, false, false)];
    let winners = vec![RedeploymentCandidate {
        peer_id: "winner1",
        open_ev_template: OpenEvInputs {
            channel_size_sats: 0,
            closed_channel_daily_net_est_sats: Some(1_000.0),
            observed_node_daily_ppm: Some(50.0),
            open_cost_sats: 1_000,
            close_cost_sats: 1_000,
            inbound_median_fee_ppm: None,
            min_annual_roi_pct: 1.0,
        },
    }];
    apply_redeployment_ev_demotion(&mut losers, &winners);
    assert_eq!(losers[0].action, LoserAction::Close);
}

#[test]
fn no_profitable_winner_demotes_to_defibrillate() {
    let mut losers = vec![base_loser(LoserAction::Close, false, false)];
    apply_redeployment_ev_demotion(&mut losers, &[]);
    assert_eq!(losers[0].action, LoserAction::Defibrillate);
    assert!(losers[0].reason.contains("NO PROFITABLE REDEPLOYMENT"));
}

/// Revert tripwire: fire-sale/hard-bleeder losers bypass EV pricing
/// entirely (py 1468-1470) — they must stay CLOSE even with zero winners.
#[test]
fn fire_sale_bypasses_redeployment_pricing() {
    let mut losers = vec![base_loser(LoserAction::Close, true, false)];
    apply_redeployment_ev_demotion(&mut losers, &[]);
    assert_eq!(
        losers[0].action,
        LoserAction::Close,
        "fire-sale losers must bypass EV pricing"
    );
}

#[test]
fn non_close_action_untouched() {
    let mut losers = vec![base_loser(LoserAction::Defibrillate, false, false)];
    let original_reason = losers[0].reason.clone();
    apply_redeployment_ev_demotion(&mut losers, &[]);
    assert_eq!(losers[0].action, LoserAction::Defibrillate);
    assert_eq!(losers[0].reason, original_reason);
}

// --- find_best_recycle_pair --------------------------------------------------

fn candidate<'a>(peer_id: &'a str, score: f64, observed_ppm: f64) -> RecycleCandidate<'a> {
    RecycleCandidate {
        peer_id,
        score,
        open_ev_template: OpenEvInputs {
            channel_size_sats: 0,
            closed_channel_daily_net_est_sats: None,
            observed_node_daily_ppm: Some(observed_ppm),
            open_cost_sats: 1000,
            close_cost_sats: 1000,
            inbound_median_fee_ppm: None,
            min_annual_roi_pct: 1.0,
        },
    }
}

#[test]
fn empty_inputs_yield_no_opportunity() {
    assert!(find_best_recycle_pair(&[], &[], 1000).is_none());
    let losers = vec![EligibleLoser {
        scid: "s",
        peer_id: "p",
        capacity_sats: 1_000_000,
        marginal_profit_30d_sats: -1000,
    }];
    assert!(find_best_recycle_pair(&losers, &[], 1000).is_none());
}

#[test]
fn below_minimum_ev_yields_no_opportunity() {
    let losers = vec![EligibleLoser {
        scid: "s",
        peer_id: "p",
        capacity_sats: 100_000,
        marginal_profit_30d_sats: 1000, // positive residual works against recycling
    }];
    let candidates = vec![candidate("cand1", 1.0, 5.0)];
    let result = find_best_recycle_pair(&losers, &candidates, 1000);
    assert!(
        result.is_none(),
        "weak EV must not clear RECYCLE_MIN_EV_SATS ({RECYCLE_MIN_EV_SATS})"
    );
}

#[test]
fn picks_the_highest_ev_pairing() {
    let losers = vec![
        EligibleLoser {
            scid: "s_low",
            peer_id: "p_low",
            capacity_sats: 2_000_000,
            marginal_profit_30d_sats: -500,
        },
        EligibleLoser {
            scid: "s_high",
            peer_id: "p_high",
            capacity_sats: 2_000_000,
            marginal_profit_30d_sats: -5000,
        },
    ];
    let candidates = vec![candidate("cand1", 1.0, 300.0)];
    let result =
        find_best_recycle_pair(&losers, &candidates, 1000).expect("expected an opportunity");
    assert_eq!(
        result.loser_peer_id, "p_high",
        "the more-bleeding loser has the higher recycle EV"
    );
}

/// Revert tripwire: only the top-5-by-score candidates are considered (py
/// `sorted_candidates[:5]`) — a 6th, higher-open-EV candidate must be
/// excluded when it ranks 6th by score.
#[test]
fn only_top_5_candidates_by_score_are_considered() {
    let losers = vec![EligibleLoser {
        scid: "s",
        peer_id: "p",
        capacity_sats: 2_000_000,
        marginal_profit_30d_sats: -2000,
    }];
    let mut candidates: Vec<RecycleCandidate> = (0..5)
        .map(|i| {
            candidate(
                Box::leak(format!("top{i}").into_boxed_str()),
                10.0 - i as f64,
                10.0,
            )
        })
        .collect();
    // A 6th candidate with a much higher observed ppm (-> much higher open
    // EV) but the LOWEST score, so it is excluded from the top-5 pool.
    candidates.push(candidate("excluded_by_rank", 0.01, 5000.0));
    let result = find_best_recycle_pair(&losers, &candidates, 1000);
    if let Some(plan) = result {
        assert_ne!(plan.candidate_peer_id, "excluded_by_rank");
    }
}

/// Review finding F71-R10: each winner's open EV must be recomputed
/// against THE LOSER BEING PRICED, not once for all losers.
///
/// Python's `_calculate_redeployment_ev` (py 2930-2966) calls
/// `_calculate_open_ev(winner["peer_id"], loser_capacity, cfg)` INSIDE the
/// per-loser call, and `calculate_open_ev` scales its forecast with
/// `channel_size_sats`. A single precomputed scalar per winner is therefore
/// structurally incapable of parity: with unequal loser capacities the
/// winner EV, the selected peer, and the close-vs-defibrillate verdict can
/// all differ.
///
/// Here the same winner is priced against a tiny loser and a large one. The
/// tiny loser's redeployment cannot cover its closure cost, so it demotes;
/// the large one's can, so it stays CLOSE. Under the old shape both losers
/// saw one identical EV and reached the SAME verdict, whichever it was.
#[test]
fn winner_ev_is_repriced_per_loser_capacity() {
    // forecast = min(90 * NEW_PEER_DISCOUNT, CEILING) = 45 ppm/day, so
    // EV(size) = size*0.002358 - 2000: negative for a 100k channel, about
    // +115_900 for a 50M one. That spread is the whole point -- it exists
    // ONLY because capacity is substituted per loser.
    let template = OpenEvInputs {
        // Substituted per loser -- the value here must not survive.
        channel_size_sats: 0,
        closed_channel_daily_net_est_sats: None,
        observed_node_daily_ppm: Some(90.0),
        open_cost_sats: 1_000,
        close_cost_sats: 1_000,
        inbound_median_fee_ppm: None,
        min_annual_roi_pct: 1.0,
    };
    let winners = vec![RedeploymentCandidate {
        peer_id: "winner1",
        open_ev_template: template,
    }];

    let mut tiny = base_loser(LoserAction::Close, false, false);
    tiny.scid = "700000x1x0".to_string();
    tiny.capacity = 100_000;
    tiny.estimated_closure_cost_sats = 50_000;
    tiny.marginal_profit_30d_sats = 0;

    let mut large = base_loser(LoserAction::Close, false, false);
    large.scid = "700000x2x0".to_string();
    large.capacity = 50_000_000;
    large.estimated_closure_cost_sats = 50_000;
    large.marginal_profit_30d_sats = 0;

    let mut losers = vec![tiny, large];
    apply_redeployment_ev_demotion(&mut losers, &winners);

    assert_eq!(
        losers[0].action,
        LoserAction::Defibrillate,
        "the tiny loser redeploys too little to cover closure: {}",
        losers[0].reason
    );
    assert_eq!(
        losers[1].action,
        LoserAction::Close,
        "the large loser redeploys enough to stay CLOSE: {}",
        losers[1].reason
    );
    assert_ne!(
        losers[0].action, losers[1].action,
        "unequal capacities MUST be able to reach different verdicts -- \
         identical verdicts here means the EV was not repriced"
    );
}
