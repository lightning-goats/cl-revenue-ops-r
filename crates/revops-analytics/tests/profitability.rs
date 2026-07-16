//! Golden + unit tests for `revops_analytics::profitability` (Task 4,
//! Wave 1, of
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`).
//!
//! Fixtures vendored byte-for-byte from
//! `~/bin/cl_revenue_ops-port/tests/golden/fixtures/profitability/` (the
//! `classify_*` and `marginal_roi_*` files; `role30d_*` is Task 2's).
//! `FROZEN_NOW` matches `tests/golden/test_golden_profitability.py`.

use revops_analytics::profitability::{
    classify_channel, days_since_routed, ChannelCosts, ChannelProfitability, ChannelRevenue,
    ChannelRole, ClassifyEvidence, DiagStats, ProfitabilityClass,
};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

const FROZEN_NOW: i64 = 1_752_400_000;

fn fixture(name: &str) -> Value {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("fixtures/golden/profitability");
    path.push(format!("{name}.json"));
    let raw = fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

fn expect_classification(v: &Value) -> ProfitabilityClass {
    match v["classification"].as_str().unwrap() {
        "PROFITABLE" => ProfitabilityClass::Profitable,
        "BREAK_EVEN" => ProfitabilityClass::BreakEven,
        "UNDERWATER" => ProfitabilityClass::Underwater,
        "STAGNANT_CANDIDATE" => ProfitabilityClass::StagnantCandidate,
        "ZOMBIE" => ProfitabilityClass::Zombie,
        other => panic!("unknown classification wire value {other}"),
    }
}

fn no_evidence(now: i64) -> ClassifyEvidence<'static> {
    ClassifyEvidence {
        now,
        diag_stats: None,
        posterior_variance: None,
        contribution_30d_msat: None,
    }
}

// =============================================================================
// classify_channel goldens
// =============================================================================

#[test]
fn classify_young_profitable_golden() {
    let fx = fixture("classify_young_profitable");
    let inputs = &fx["inputs"];
    let result = classify_channel(
        inputs["roi"].as_f64().unwrap(),
        inputs["net_profit"].as_i64().unwrap(),
        inputs["last_routed"].as_i64(),
        inputs["days_open"].as_i64().unwrap(),
        inputs["forward_count"].as_i64().unwrap(),
        &no_evidence(FROZEN_NOW),
    );
    assert_eq!(result, expect_classification(&fx));
    assert_eq!(result.as_name(), "PROFITABLE");
}

#[test]
fn classify_old_loser_stagnant_golden() {
    let fx = fixture("classify_old_loser_stagnant");
    let inputs = &fx["inputs"];
    // _analyzer() default diag stats: attempt_count 0, last_success_time
    // None -> the zombie branch never fires (attempt_count < 2), so the
    // Rust evidence is "no diag stats" (equivalent to attempt_count 0).
    let result = classify_channel(
        inputs["roi"].as_f64().unwrap(),
        inputs["net_profit"].as_i64().unwrap(),
        inputs["last_routed"].as_i64(),
        inputs["days_open"].as_i64().unwrap(),
        inputs["forward_count"].as_i64().unwrap(),
        &no_evidence(FROZEN_NOW),
    );
    assert_eq!(result, expect_classification(&fx));
    assert_eq!(result.as_name(), "STAGNANT_CANDIDATE");
}

#[test]
fn classify_never_routed_mature_golden() {
    let fx = fixture("classify_never_routed_mature");
    let inputs = &fx["inputs"];
    assert!(inputs["last_routed"].is_null());
    let result = classify_channel(
        inputs["roi"].as_f64().unwrap(),
        inputs["net_profit"].as_i64().unwrap(),
        None,
        inputs["days_open"].as_i64().unwrap(),
        inputs["forward_count"].as_i64().unwrap(),
        &no_evidence(FROZEN_NOW),
    );
    assert_eq!(result, expect_classification(&fx));
    assert_eq!(result.as_name(), "BREAK_EVEN");
}

#[test]
fn classify_breakeven_active_golden() {
    let fx = fixture("classify_breakeven_active");
    let inputs = &fx["inputs"];
    let result = classify_channel(
        inputs["roi"].as_f64().unwrap(),
        inputs["net_profit"].as_i64().unwrap(),
        inputs["last_routed"].as_i64(),
        inputs["days_open"].as_i64().unwrap(),
        inputs["forward_count"].as_i64().unwrap(),
        &no_evidence(FROZEN_NOW),
    );
    assert_eq!(result, expect_classification(&fx));
    assert_eq!(result.as_name(), "BREAK_EVEN");
}

#[test]
fn classify_zombie_after_failed_defib_golden() {
    let fx = fixture("classify_zombie_after_failed_defib");
    // Evidence transcribed from
    // test_golden_classify_zombie_after_failed_defib: attempts=2,
    // success=None (-> 0), roi=-0.40, last_routed = FROZEN_NOW - 86_400*30,
    // days_open=200, forward_count=5.
    let diag = DiagStats {
        attempt_count: 2,
        last_success_time: 0,
    };
    let ev = ClassifyEvidence {
        now: FROZEN_NOW,
        diag_stats: Some(&diag),
        posterior_variance: None,
        contribution_30d_msat: None,
    };
    let result = classify_channel(-0.40, -8000, Some(FROZEN_NOW - 86_400 * 30), 200, 5, &ev);
    assert_eq!(result, expect_classification(&fx));
    assert_eq!(result.as_name(), "ZOMBIE");
}

// Zombie via the OTHER sub-branch: a successful diagnostic more than 48h
// ago that still hasn't produced a fresh forward.
#[test]
fn classify_zombie_after_stale_diagnostic_success() {
    let diag = DiagStats {
        attempt_count: 2,
        last_success_time: FROZEN_NOW - 3600 * 49, // 49h ago
    };
    let ev = ClassifyEvidence {
        now: FROZEN_NOW,
        diag_stats: Some(&diag),
        posterior_variance: None,
        contribution_30d_msat: None,
    };
    // last_routed None (never routed) satisfies `!last_routed`.
    let result = classify_channel(-0.40, -8000, None, 200, 5, &ev);
    assert_eq!(result, ProfitabilityClass::Zombie);
}

#[test]
fn classify_not_zombie_when_diagnostic_succeeded_recently() {
    // Same shape as the stale-success case, but only 10h since success:
    // hours_since_diag_success (10) is not > 48, so no ZOMBIE — falls
    // through to STAGNANT_CANDIDATE (roi < -0.10, but days_inactive
    // depends on last_routed=None -> days_open=200 >= 7).
    let diag = DiagStats {
        attempt_count: 2,
        last_success_time: FROZEN_NOW - 3600 * 10,
    };
    let ev = ClassifyEvidence {
        now: FROZEN_NOW,
        diag_stats: Some(&diag),
        posterior_variance: None,
        contribution_30d_msat: None,
    };
    let result = classify_channel(-0.40, -8000, None, 200, 5, &ev);
    assert_eq!(result, ProfitabilityClass::StagnantCandidate);
}

#[test]
fn classify_diagnostic_failed_but_routed_recently_skips_zombie() {
    // attempt_count >= 2, no success ever, but days_inactive < 7 ->
    // "Skipping ZOMBIE classification" (Python debug-log branch): falls
    // through since roi < -0.10 but days_inactive(0) < 7 skips STAGNANT,
    // and roi < underwater_thresh -> UNDERWATER.
    let diag = DiagStats {
        attempt_count: 2,
        last_success_time: 0,
    };
    let ev = ClassifyEvidence {
        now: FROZEN_NOW,
        diag_stats: Some(&diag),
        posterior_variance: None,
        contribution_30d_msat: None,
    };
    let result = classify_channel(-0.40, -8000, Some(FROZEN_NOW - 3600), 200, 5, &ev);
    assert_eq!(result, ProfitabilityClass::Underwater);
}

#[test]
fn classify_last_routed_zero_is_treated_as_never_routed() {
    // Python `if last_routed:` treats 0 as falsy -> same result as
    // last_routed=None with the same days_open.
    let a = classify_channel(0.0, -500, Some(0), 120, 0, &no_evidence(FROZEN_NOW));
    let b = classify_channel(0.0, -500, None, 120, 0, &no_evidence(FROZEN_NOW));
    assert_eq!(a, b);
    assert_eq!(a, ProfitabilityClass::BreakEven);
}

#[test]
fn classify_dts_widening_narrows_underwater_and_widens_profitable() {
    // posterior_variance < 2500 -> profitable_thresh 0.05, underwater
    // thresh -0.15. roi = 0.06 is PROFITABLE under widened (but would be
    // BREAK_EVEN under the un-widened 0.10 threshold).
    let ev = ClassifyEvidence {
        now: FROZEN_NOW,
        diag_stats: None,
        posterior_variance: Some(100.0),
        contribution_30d_msat: None,
    };
    let widened = classify_channel(0.06, 100, Some(FROZEN_NOW - 3600), 90, 500, &ev);
    assert_eq!(widened, ProfitabilityClass::Profitable);

    let unwidened = classify_channel(
        0.06,
        100,
        Some(FROZEN_NOW - 3600),
        90,
        500,
        &no_evidence(FROZEN_NOW),
    );
    assert_eq!(unwidened, ProfitabilityClass::BreakEven);

    // roi = -0.12 is UNDERWATER un-widened but BREAK_EVEN once widened
    // (threshold -0.15), and NOT caught by the days_inactive>=7 stagnant
    // branch here (days_inactive = 0).
    let widened_neg = classify_channel(-0.12, -100, Some(FROZEN_NOW - 3600), 90, 500, &ev);
    assert_eq!(widened_neg, ProfitabilityClass::BreakEven);
    let unwidened_neg = classify_channel(
        -0.12,
        -100,
        Some(FROZEN_NOW - 3600),
        90,
        500,
        &no_evidence(FROZEN_NOW),
    );
    assert_eq!(unwidened_neg, ProfitabilityClass::Underwater);
}

#[test]
fn classify_f3_corpse_branch_reclassifies_stale_profitable_history() {
    // Lifetime ROI is strongly positive (never decays), but the channel has
    // earned nothing in 30d, has been inactive 30+ days, and is mature
    // (>60 days open): audit F3 says this is dead capital, not PROFITABLE.
    let ev = ClassifyEvidence {
        now: FROZEN_NOW,
        diag_stats: None,
        posterior_variance: None,
        contribution_30d_msat: Some(0),
    };
    let result = classify_channel(0.50, 50_000, Some(FROZEN_NOW - 86_400 * 40), 400, 900, &ev);
    assert_eq!(result, ProfitabilityClass::StagnantCandidate);
}

#[test]
fn classify_f3_corpse_branch_disabled_when_no_window_data() {
    // Same evidence, but contribution_30d_msat: None (no windowed data
    // fetched) -> F3 branch never fires; falls through to lifetime PROFITABLE.
    let result = classify_channel(
        0.50,
        50_000,
        Some(FROZEN_NOW - 86_400 * 40),
        400,
        900,
        &no_evidence(FROZEN_NOW),
    );
    assert_eq!(result, ProfitabilityClass::Profitable);
}

// =============================================================================
// marginal_roi goldens
// =============================================================================

fn prof_with_marginal(profit_30d: i64, cost_30d: i64) -> ChannelProfitability {
    let costs = ChannelCosts {
        channel_id: "111x222x0".to_string(),
        peer_id: format!("02{}", "a".repeat(64)),
        open_cost_sats: 500,
        rebalance_cost_sats: 1000,
        effective_rebalance_cost_sats: 0,
    };
    let revenue = ChannelRevenue {
        channel_id: "111x222x0".to_string(),
        fees_earned_msat: 2_000_000,
        volume_routed_msat: 1_000_000_000,
        forward_count: 100,
        sourced_volume_msat: 0,
        sourced_fee_contribution_msat: 0,
        sourced_forward_count: 0,
    };
    let net_profit_sats = 2000 - costs.total_cost_sats();
    ChannelProfitability {
        channel_id: "111x222x0".to_string(),
        peer_id: format!("02{}", "a".repeat(64)),
        capacity_sats: 2_000_000,
        costs,
        revenue,
        net_profit_sats,
        roi_percent: 10.0,
        classification: ProfitabilityClass::Profitable,
        cost_per_sat_routed: 0.001,
        fee_per_sat_routed: 0.002,
        days_open: 30,
        last_routed: None,
        marginal_profit_30d_sats: profit_30d,
        rebalance_cost_30d_sats: cost_30d,
        opener: "local".to_string(),
        contribution_30d_msat: 0,
        fees_earned_30d_msat: 0,
        sourced_fee_30d_msat: 0,
        forward_count_30d: 0,
        sourced_forward_count_30d: 0,
        window_30d_available: false,
    }
}

fn assert_marginal_roi_golden(fixture_name: &str, profit_30d: i64, cost_30d: i64) {
    let fx = fixture(fixture_name);
    let expected = fx["marginal_roi"].as_f64().unwrap();
    let prof = prof_with_marginal(profit_30d, cost_30d);
    assert_eq!(
        prof.marginal_roi().to_bits(),
        expected.to_bits(),
        "fixture {fixture_name}: expected {expected} bits {:x}, got {} bits {:x}",
        expected.to_bits(),
        prof.marginal_roi(),
        prof.marginal_roi().to_bits(),
    );
}

#[test]
fn marginal_roi_profit_over_cost_golden() {
    assert_marginal_roi_golden("marginal_roi_profit_over_cost", 500, 200);
}

#[test]
fn marginal_roi_negative_profit_golden() {
    assert_marginal_roi_golden("marginal_roi_negative_profit", -300, 600);
    // Corpus s05 anchor: -300/600 = -0.5 exactly.
    let prof = prof_with_marginal(-300, 600);
    assert_eq!(prof.marginal_roi(), -0.5);
}

#[test]
fn marginal_roi_zero_cost_positive_profit_golden() {
    assert_marginal_roi_golden("marginal_roi_zero_cost_positive_profit", 500, 0);
}

#[test]
fn marginal_roi_zero_cost_zero_profit_golden() {
    assert_marginal_roi_golden("marginal_roi_zero_cost_zero_profit", 0, 0);
}

#[test]
fn marginal_roi_percent_is_marginal_roi_times_100() {
    let prof = prof_with_marginal(500, 200);
    assert_eq!(prof.marginal_roi_percent(), prof.marginal_roi() * 100.0);
    assert_eq!(prof.marginal_roi_percent(), 250.0);
}

#[test]
fn marginal_roi_reliable_gates_on_100_sats_of_30d_rebalance_spend() {
    assert!(!prof_with_marginal(500, 99).marginal_roi_reliable());
    assert!(prof_with_marginal(500, 100).marginal_roi_reliable());
}

#[test]
fn is_operationally_profitable_matches_marginal_roi_sign() {
    assert!(prof_with_marginal(1, 200).is_operationally_profitable());
    assert!(prof_with_marginal(0, 200).is_operationally_profitable());
    assert!(!prof_with_marginal(-1, 200).is_operationally_profitable());
}

// =============================================================================
// sat-property boundary table
// =============================================================================

#[test]
fn fees_earned_sats_ceil_boundary_table() {
    let cases: [(i64, i64); 5] = [(0, 0), (1, 1), (999, 1), (1000, 1), (1001, 2)];
    for (msat, expected_sats) in cases {
        let rev = ChannelRevenue {
            channel_id: "x".to_string(),
            fees_earned_msat: msat,
            volume_routed_msat: 0,
            forward_count: 0,
            sourced_volume_msat: 0,
            sourced_fee_contribution_msat: msat,
            sourced_forward_count: 0,
        };
        assert_eq!(rev.fees_earned_sats(), expected_sats, "msat={msat}");
        assert_eq!(
            rev.sourced_fee_contribution_sats(),
            expected_sats,
            "msat={msat}"
        );
    }
}

#[test]
fn volume_routed_sats_floor_boundary_table() {
    let cases: [(i64, i64); 5] = [(0, 0), (1, 0), (999, 0), (1000, 1), (1001, 1)];
    for (msat, expected_sats) in cases {
        let rev = ChannelRevenue {
            channel_id: "x".to_string(),
            fees_earned_msat: 0,
            volume_routed_msat: msat,
            forward_count: 0,
            sourced_volume_msat: msat,
            sourced_fee_contribution_msat: 0,
            sourced_forward_count: 0,
        };
        assert_eq!(rev.volume_routed_sats(), expected_sats, "msat={msat}");
        assert_eq!(rev.sourced_volume_sats(), expected_sats, "msat={msat}");
    }
}

#[test]
fn negative_or_zero_fee_msat_never_reports_a_fee() {
    let rev = ChannelRevenue {
        channel_id: "x".to_string(),
        fees_earned_msat: -500,
        volume_routed_msat: 0,
        forward_count: 0,
        sourced_volume_msat: 0,
        sourced_fee_contribution_msat: -1,
        sourced_forward_count: 0,
    };
    assert_eq!(rev.fees_earned_sats(), 0);
    assert_eq!(rev.sourced_fee_contribution_sats(), 0);
}

#[test]
fn total_contribution_msat_is_max_not_sum() {
    let rev = ChannelRevenue {
        channel_id: "x".to_string(),
        fees_earned_msat: 5000,
        volume_routed_msat: 0,
        forward_count: 10,
        sourced_volume_msat: 0,
        sourced_fee_contribution_msat: 9000,
        sourced_forward_count: 3,
    };
    assert_eq!(rev.total_contribution_msat(), 9000);
    assert_eq!(rev.total_contribution_sats(), 9);
    assert_eq!(rev.total_forward_count(), 13);
}

// =============================================================================
// channel_role / role_30d
// =============================================================================

fn base_prof() -> ChannelProfitability {
    prof_with_marginal(0, 0)
}

#[test]
fn channel_role_dormant_under_10_total_forwards() {
    let mut prof = base_prof();
    prof.revenue.forward_count = 5;
    prof.revenue.sourced_forward_count = 4;
    assert_eq!(prof.channel_role(), ChannelRole::Dormant);
}

#[test]
fn channel_role_gateways_at_70_percent_boundary() {
    let mut prof = base_prof();
    // 71 inbound / 100 total -> INBOUND_GATEWAY (>0.70 strict).
    prof.revenue.forward_count = 29;
    prof.revenue.sourced_forward_count = 71;
    assert_eq!(prof.channel_role(), ChannelRole::InboundGateway);

    // Exactly 70/100 -> NOT gateway (strict >), falls to Balanced.
    prof.revenue.forward_count = 30;
    prof.revenue.sourced_forward_count = 70;
    assert_eq!(prof.channel_role(), ChannelRole::Balanced);

    prof.revenue.forward_count = 71;
    prof.revenue.sourced_forward_count = 29;
    assert_eq!(prof.channel_role(), ChannelRole::OutboundGateway);
}

#[test]
fn role_30d_falls_back_to_lifetime_when_window_unavailable() {
    let mut prof = base_prof();
    prof.revenue.forward_count = 71;
    prof.revenue.sourced_forward_count = 29;
    prof.window_30d_available = false;
    prof.forward_count_30d = 0;
    prof.sourced_forward_count_30d = 0;
    assert_eq!(prof.role_30d(), prof.channel_role());
    assert_eq!(prof.role_30d(), ChannelRole::OutboundGateway);
}

#[test]
fn role_30d_decays_stale_gateway_to_dormant() {
    let mut prof = base_prof();
    // Lifetime says OUTBOUND_GATEWAY...
    prof.revenue.forward_count = 71;
    prof.revenue.sourced_forward_count = 29;
    // ...but the trailing 30d window shows near-zero activity.
    prof.window_30d_available = true;
    prof.forward_count_30d = 1;
    prof.sourced_forward_count_30d = 1;
    assert_eq!(prof.channel_role(), ChannelRole::OutboundGateway);
    assert_eq!(prof.role_30d(), ChannelRole::Dormant);
}

#[test]
fn total_forward_count_30d_sums_both_directions() {
    let mut prof = base_prof();
    prof.forward_count_30d = 7;
    prof.sourced_forward_count_30d = 5;
    assert_eq!(prof.total_forward_count_30d(), 12);
}

// =============================================================================
// ChannelCosts
// =============================================================================

#[test]
fn total_cost_sats_is_open_plus_rebalance() {
    let costs = ChannelCosts {
        channel_id: "x".to_string(),
        peer_id: "y".to_string(),
        open_cost_sats: 500,
        rebalance_cost_sats: 1500,
        effective_rebalance_cost_sats: 0,
    };
    assert_eq!(costs.total_cost_sats(), 2000);
}

// =============================================================================
// _days_since_routed
// =============================================================================

#[test]
fn days_since_routed_uses_days_open_when_never_routed() {
    let mut prof = base_prof();
    prof.days_open = 42;
    prof.last_routed = None;
    assert_eq!(days_since_routed(FROZEN_NOW, &prof), 42);

    // 0 is treated as never-routed too.
    prof.last_routed = Some(0);
    assert_eq!(days_since_routed(FROZEN_NOW, &prof), 42);
}

#[test]
fn days_since_routed_computes_from_last_routed_when_present() {
    let mut prof = base_prof();
    prof.days_open = 42;
    prof.last_routed = Some(FROZEN_NOW - 86_400 * 3);
    assert_eq!(days_since_routed(FROZEN_NOW, &prof), 3);
}

// =============================================================================
// ProfitabilityClass wire strings
// =============================================================================

#[test]
fn profitability_class_wire_strings() {
    assert_eq!(ProfitabilityClass::Profitable.as_value(), "profitable");
    assert_eq!(ProfitabilityClass::Profitable.as_name(), "PROFITABLE");
    assert_eq!(ProfitabilityClass::BreakEven.as_value(), "break_even");
    assert_eq!(ProfitabilityClass::BreakEven.as_name(), "BREAK_EVEN");
    assert_eq!(ProfitabilityClass::Underwater.as_value(), "underwater");
    assert_eq!(ProfitabilityClass::Underwater.as_name(), "UNDERWATER");
    assert_eq!(
        ProfitabilityClass::StagnantCandidate.as_value(),
        "stagnant_candidate"
    );
    assert_eq!(
        ProfitabilityClass::StagnantCandidate.as_name(),
        "STAGNANT_CANDIDATE"
    );
    assert_eq!(ProfitabilityClass::Zombie.as_value(), "zombie");
    assert_eq!(ProfitabilityClass::Zombie.as_name(), "ZOMBIE");
}
