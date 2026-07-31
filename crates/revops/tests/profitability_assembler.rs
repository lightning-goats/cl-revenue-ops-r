//! Task 67b slice 2: assemble `ChannelProfitability` per channel and run
//! the FROZEN `classify_channel`. Formulas are Python's exact ones
//! (modules/profitability_analyzer.py:795-880).

use std::collections::HashMap;

use revops::profitability_assembler::{
    assemble_channel_profitability, ChannelInput, ProfitabilityRefusal,
};
use revops::profitability_evidence::ChannelEvidence;
use revops_analytics::profitability::{DiagStats, ProfitabilityClass};
use revops_db::queries::{PerChannelCosts, PerChannelRevenue};

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;

fn revenue(fees: i64, volume: i64, count: i64) -> PerChannelRevenue {
    PerChannelRevenue {
        fees_earned_msat: fees,
        volume_routed_msat: volume,
        forward_count: count,
        sourced_volume_msat: 0,
        sourced_fee_contribution_msat: 0,
        sourced_forward_count: 0,
    }
}

fn costs(open: i64, rebal: i64, rebal_30d: i64, capacity: i64, opened_at: i64) -> PerChannelCosts {
    PerChannelCosts {
        peer_id: "02aa".into(),
        open_cost_sats: open,
        capacity_sats: capacity,
        opened_at,
        rebalance_cost_sats: rebal,
        rebalance_cost_30d_sats: rebal_30d,
        rebalance_cost_msat: 0,
        rebalance_cost_30d_msat: 0,
    }
}

fn input(all: PerChannelRevenue, w30: PerChannelRevenue, c: PerChannelCosts) -> ChannelInput {
    ChannelInput {
        scid: "700x1x0".into(),
        revenue_all_time: all,
        revenue_30d: w30,
        costs: c,
        opener: "local".into(),
        last_routed: Some(NOW - DAY),
        diag_attempt_count: 0,
        diag_last_success_time: 0,
        posterior_variance: None,
    }
}

/// Python's ROI: net_profit / total_cost, where net_profit uses TOTAL
/// CONTRIBUTION (direct + sourced) and total_cost is open + rebalance.
#[test]
fn roi_matches_pythons_formula() {
    let all = revenue(10_000_000, 100_000_000, 50); // 10_000 sats earned
    let c = costs(2_000, 500, 100, 5_000_000, NOW - 30 * DAY);
    let p =
        assemble_channel_profitability(input(all, revenue(0, 0, 0), c), NOW).expect("assembles");
    // total_cost = 2500; contribution = 10000; net = 7500; roi = 3.0 -> 300%
    assert_eq!(p.costs.total_cost_sats(), 2_500);
    assert_eq!(p.net_profit_sats, 7_500);
    assert!((p.roi_percent - 300.0).abs() < 1e-9, "{}", p.roi_percent);
}

/// The zero-cost branch is NOT a division guard -- Python gives a channel
/// with revenue and no recorded cost a synthetic ROI of 1.0 (free money),
/// and falls back to return-on-CAPACITY only when there is no
/// contribution at all.
#[test]
fn zero_cost_channels_use_pythons_synthetic_roi() {
    let c = costs(0, 0, 0, 5_000_000, NOW - 30 * DAY);
    // Earning, no cost -> exactly 100%, not infinity and not zero.
    let p = assemble_channel_profitability(
        input(
            revenue(1_000_000, 10_000_000, 5),
            revenue(0, 0, 0),
            c.clone(),
        ),
        NOW,
    )
    .expect("assembles");
    assert!((p.roi_percent - 100.0).abs() < 1e-9, "{}", p.roi_percent);

    // No cost AND no contribution -> return on capacity (0 here), never 100%.
    let p = assemble_channel_profitability(input(revenue(0, 0, 0), revenue(0, 0, 0), c), NOW)
        .expect("assembles");
    assert!((p.roi_percent - 0.0).abs() < 1e-9, "{}", p.roi_percent);
}

/// Marginal ROI is the 30-DAY window over ONGOING rebalance cost, with no
/// sunk open cost. Using the all-time figures would change the verdict.
#[test]
fn marginal_roi_uses_the_30d_window_not_all_time() {
    let all = revenue(10_000_000, 100_000_000, 50);
    let w30 = revenue(600_000, 5_000_000, 3); // 600 sats in the window
    let c = costs(50_000, 5_000, 300, 5_000_000, NOW - 90 * DAY);
    let p = assemble_channel_profitability(input(all, w30, c), NOW).expect("assembles");
    assert_eq!(p.rebalance_cost_30d_sats, 300);
    assert_eq!(
        p.marginal_profit_30d_sats, 300,
        "600 earned - 300 rebalance"
    );
    assert!(
        (p.marginal_roi() - 1.0).abs() < 1e-9,
        "{}",
        p.marginal_roi()
    );
    // All-time ROI is deeply negative here; marginal is positive. If the
    // assembler conflated them the channel would flip from winner to loser.
    assert!(p.roi_percent < 0.0, "{}", p.roi_percent);
}

/// The FROZEN classifier is actually consulted -- a long-idle underwater
/// channel must not come back PROFITABLE.
#[test]
fn the_frozen_classifier_is_consulted() {
    let c = costs(50_000, 10_000, 0, 5_000_000, NOW - 200 * DAY);
    let mut i = input(revenue(0, 0, 0), revenue(0, 0, 0), c);
    i.last_routed = Some(NOW - 120 * DAY);
    let p = assemble_channel_profitability(i, NOW).expect("assembles");
    assert_ne!(
        p.classification,
        ProfitabilityClass::Profitable,
        "an idle, cost-laden, zero-revenue channel is not profitable: {:?}",
        p.classification
    );
}

/// days_open comes from opened_at; a MISSING opened_at is a refusal, not
/// a channel that looks brand new (days_open=0 would make every staleness
/// branch read as "too early to judge").
#[test]
fn missing_open_timestamp_refuses_rather_than_faking_a_new_channel() {
    let c = costs(2_000, 0, 0, 5_000_000, 0);
    let err = assemble_channel_profitability(input(revenue(0, 0, 0), revenue(0, 0, 0), c), NOW)
        .expect_err("a channel with no open timestamp must refuse");
    assert_eq!(err.code(), "profitability_open_timestamp_missing");
    assert!(matches!(err, ProfitabilityRefusal::OpenTimestampMissing(_)));
}

/// The fleet assembler skips-with-reason rather than dropping silently,
/// so an operator can see WHY a channel is absent from winners/losers.
#[test]
fn fleet_assembly_reports_skips_with_reasons() {
    let mut rev = HashMap::new();
    rev.insert("700x1x0".to_string(), revenue(1_000_000, 10_000_000, 5));
    let mut cst = HashMap::new();
    cst.insert(
        "700x1x0".to_string(),
        costs(2_000, 0, 0, 5_000_000, NOW - 10 * DAY),
    );
    // A channel with costs but a missing open timestamp.
    cst.insert("800x1x0".to_string(), costs(1_000, 0, 0, 1_000_000, 0));

    let mut ev = HashMap::new();
    ev.insert("700x1x0".to_string(), evidence("local", None));
    ev.insert("800x1x0".to_string(), evidence("local", None));

    let out =
        revops::profitability_assembler::assemble_fleet(&rev, &HashMap::new(), &cst, &ev, NOW);
    assert_eq!(out.profitability.len(), 1);
    assert!(out.profitability.contains_key("700x1x0"));
    assert_eq!(out.skipped.len(), 1);
    assert_eq!(out.skipped[0].0, "800x1x0");
    assert!(
        out.skipped[0].1.contains("opened_at"),
        "the skip reason must name the missing column: {:?}",
        out.skipped
    );
}

fn evidence(opener: &str, last_routed: Option<i64>) -> ChannelEvidence {
    ChannelEvidence {
        last_routed,
        diag: DiagStats {
            attempt_count: 0,
            last_success_time: 0,
        },
        posterior_variance: None,
        opener: opener.to_string(),
    }
}

/// C71-25. The fleet assembler used to invent `opener: "local"`,
/// `last_routed: None` and zeroed diagnostics for every channel. Three of
/// those coincide with Python's no-row defaults, so no test ever saw them;
/// the fourth asserts this node paid for the open.
///
/// A channel with no evidence must now be SKIPPED with a reason, because
/// "we did not look" and "we looked and found nothing" are different facts
/// and only the second one is Python's.
#[test]
fn a_channel_without_evidence_is_skipped_rather_than_assembled_from_defaults() {
    let mut cst = HashMap::new();
    cst.insert(
        "700x1x0".to_string(),
        costs(2_000, 0, 0, 5_000_000, NOW - 10 * DAY),
    );

    let out = revops::profitability_assembler::assemble_fleet(
        &HashMap::new(),
        &HashMap::new(),
        &cst,
        &HashMap::new(),
        NOW,
    );
    assert!(
        out.profitability.is_empty(),
        "a channel with no evidence must not be classified from defaults"
    );
    assert_eq!(out.skipped.len(), 1);
    assert_eq!(out.skipped[0].0, "700x1x0");
    assert!(
        out.skipped[0].1.contains("evidence"),
        "the skip reason must say the evidence is missing: {:?}",
        out.skipped
    );
}

/// The damage the fabricated `last_routed: None` actually did: the
/// classifier substitutes `days_open` for `days_inactive` when there is no
/// routing time (py profitability_analyzer.py:2661-2663), so a mature
/// channel that routed yesterday was judged idle for its entire life.
#[test]
fn fleet_assembly_uses_the_evidence_last_routed_rather_than_reporting_never_routed() {
    let mut cst = HashMap::new();
    cst.insert(
        "700x1x0".to_string(),
        costs(2_000, 0, 0, 5_000_000, NOW - 400 * DAY),
    );
    let mut ev = HashMap::new();
    ev.insert("700x1x0".to_string(), evidence("remote", Some(NOW - DAY)));

    let out = revops::profitability_assembler::assemble_fleet(
        &HashMap::new(),
        &HashMap::new(),
        &cst,
        &ev,
        NOW,
    );
    let p = out.profitability.get("700x1x0").expect("assembles");
    assert_eq!(
        p.last_routed,
        Some(NOW - DAY),
        "the assembled channel must carry the evidence's routing time"
    );
    assert_eq!(
        p.opener, "remote",
        "the opener must come from the live snapshot, not a fabricated \"local\""
    );
}

// ---------------------------------------------------------------------
// C71-29: the fee multiplier the single-channel response used to gap-mark.
//
// py `get_fee_multiplier` (profitability_analyzer.py:979-1033) is a pure
// function of MARGINAL ROI -- deliberately not total ROI, so a channel is
// never punished with higher fees for an opening cost it has not yet
// recovered. Every input is already a field of the assembled channel, so
// the old `fee_multiplier: null` was an unwritten branch, not a missing
// source.
// ---------------------------------------------------------------------

use revops::profitability_assembler::fee_multiplier;

/// Build a channel with an exact marginal ROI: marginal profit over 30d
/// rebalance spend. Spend is >= 100 sats so py's F8 reliability gate is
/// satisfied and the ladder actually runs.
fn with_marginal_roi(
    profit_30d: i64,
    spend_30d: i64,
) -> revops_analytics::profitability::ChannelProfitability {
    let c = costs(1_000, spend_30d, spend_30d, 5_000_000, NOW - 100 * DAY);
    let mut i = input(revenue(0, 0, 0), revenue(0, 0, 0), c);
    i.last_routed = Some(NOW - DAY);
    let mut p = assemble_channel_profitability(i, NOW).expect("assembles");
    // Set the marginal numerator directly: the ladder is what is under
    // test, not the 30d revenue arithmetic that feeds it.
    p.marginal_profit_30d_sats = profit_30d;
    p.rebalance_cost_30d_sats = spend_30d;
    p
}

#[test]
fn the_fee_multiplier_ladder_matches_pythons_marginal_roi_bands() {
    // > 20%: operationally strong, keep fees competitive.
    assert_eq!(fee_multiplier(&with_marginal_roi(300, 1_000)), 0.95);
    // >= 0: covering ongoing costs, no change.
    assert_eq!(fee_multiplier(&with_marginal_roi(200, 1_000)), 1.0);
    assert_eq!(fee_multiplier(&with_marginal_roi(0, 1_000)), 1.0);
    // -20%..0: modest increase.
    assert_eq!(fee_multiplier(&with_marginal_roi(-200, 1_000)), 1.05);
    // -50%..-20%: larger increase.
    assert_eq!(fee_multiplier(&with_marginal_roi(-500, 1_000)), 1.10);
    // < -50%: try to recover.
    assert_eq!(fee_multiplier(&with_marginal_roi(-600, 1_000)), 1.15);
}

#[test]
fn the_ladder_boundaries_are_pythons_exact_comparisons() {
    // py uses `> 0.20` and `>= 0`, `>= -0.20`, `>= -0.50`. Exactly 20%
    // is NOT the competitive band; exactly -20% and -50% ARE the gentler
    // ones. Flipping any comparison changes a real channel's fee.
    assert_eq!(
        fee_multiplier(&with_marginal_roi(200, 1_000)),
        1.0,
        "exactly +20% is not > 20%"
    );
    assert_eq!(
        fee_multiplier(&with_marginal_roi(-200, 1_000)),
        1.05,
        "exactly -20% stays modest"
    );
    assert_eq!(
        fee_multiplier(&with_marginal_roi(-500, 1_000)),
        1.10,
        "exactly -50% stays 1.10"
    );
}

#[test]
fn a_thinly_evidenced_marginal_roi_is_neutral_rather_than_a_fee_driver() {
    // py audit F8: under 100 sats of 30d rebalance spend the ratio swings
    // on a handful of sats. A 99-sat spend with a catastrophic ratio must
    // not raise fees.
    let thin = with_marginal_roi(-900, 99);
    assert!(
        thin.marginal_roi() < -0.5,
        "precondition: the raw ratio is severe"
    );
    assert_eq!(
        fee_multiplier(&thin),
        1.0,
        "a 99-sat spend must not drive a fee change"
    );
    // One sat more of evidence and the rule engages.
    assert_eq!(fee_multiplier(&with_marginal_roi(-900, 100)), 1.15);
}

#[test]
fn a_zombie_in_severe_loss_is_left_alone_rather_than_repriced() {
    // py: at < -50% marginal ROI a ZOMBIE returns 1.0 -- it is flagged for
    // closure, not re-priced. Returning 1.15 would keep raising fees on a
    // channel nobody routes through.
    let mut zombie = with_marginal_roi(-900, 1_000);
    zombie.classification = ProfitabilityClass::Zombie;
    assert_eq!(fee_multiplier(&zombie), 1.0);

    // The same zombie in a SHALLOWER loss still takes the ordinary band --
    // the zombie branch is reached only after the -50% test.
    let mut shallow = with_marginal_roi(-300, 1_000);
    shallow.classification = ProfitabilityClass::Zombie;
    assert_eq!(fee_multiplier(&shallow), 1.10);
}

#[test]
fn the_multiplier_uses_marginal_not_total_roi() {
    // The sunk-cost guard, stated as behaviour: a channel drowning in
    // OPENING cost but covering its ongoing spend must keep competitive
    // fees. Using total ROI here would raise fees on exactly the channels
    // that are working.
    let c = costs(500_000, 1_000, 1_000, 5_000_000, NOW - 100 * DAY);
    let mut i = input(revenue(0, 0, 0), revenue(0, 0, 0), c);
    i.last_routed = Some(NOW - DAY);
    let mut p = assemble_channel_profitability(i, NOW).expect("assembles");
    p.marginal_profit_30d_sats = 300;
    p.rebalance_cost_30d_sats = 1_000;

    assert!(p.roi_percent < -50.0, "precondition: total ROI is dire");
    assert_eq!(
        fee_multiplier(&p),
        0.95,
        "marginal ROI is +30%: the channel is operationally healthy"
    );
}
