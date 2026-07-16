//! Tests for `revops_analytics::growth` (Task 4, Wave 1, of
//! `docs/superpowers/plans/2026-07-17-phase3-analytics-budget.md`).
//!
//! `growth_budget.py` has no golden fixture corpus (Mirrors note: "already
//! pure" — no `tests/golden` entry for it in the Python port); these tests
//! transcribe the module's documented behaviors and the checklist's
//! itemized scenarios directly.

use revops_analytics::growth::{
    compute_growth_budget_status, fleet_prior_status, GrowthBudgetInputs,
};
use serde_json::json;

fn base_inputs() -> GrowthBudgetInputs {
    GrowthBudgetInputs {
        base_budget_sats: 10_000,
        net_profit_sats: 0,
        actual_spent_sats: 0,
        reserved_sats: 0,
        enabled: true,
        earned_fraction: 0.0,
        growth_fraction: 0.0,
        growth_max_extra_sats: 0,
        hard_ceiling_sats: 10_000,
    }
}

// =============================================================================
// disabled ("fixed") shape
// =============================================================================

#[test]
fn disabled_returns_fixed_shape_at_base_budget() {
    let mut inputs = base_inputs();
    inputs.enabled = false;
    inputs.actual_spent_sats = 2_000;
    inputs.reserved_sats = 1_000;
    let status = compute_growth_budget_status(&inputs, None);

    assert_eq!(status.mode, "fixed");
    assert_eq!(status.authority, "local");
    assert!(status.advisory_only);
    assert!(!status.fleet_prior_budget_authority);
    assert_eq!(status.base_budget_sats, 10_000);
    assert_eq!(status.local_hard_ceiling_sats, 10_000);
    assert_eq!(status.earned_credit_sats, 0);
    assert_eq!(status.growth_credit_sats, 0);
    assert_eq!(status.effective_budget_sats, 10_000);
    assert_eq!(status.actual_spent_sats, 2_000);
    assert_eq!(status.reserved_sats, 1_000);
    assert_eq!(status.remaining_sats, 7_000);
    assert!(!status.capped_by_hard_ceiling);
    assert_eq!(status.fleet_prior.reason, "missing");
}

#[test]
fn disabled_still_computes_fleet_prior_status() {
    // Python computes `_fleet_prior_status` BEFORE the `if not enabled`
    // branch — the disabled shape still carries a real fleet_prior status,
    // not a stub.
    let mut inputs = base_inputs();
    inputs.enabled = false;
    let fleet_prior = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 0.9});
    let status = compute_growth_budget_status(&inputs, Some(&fleet_prior));
    assert_eq!(status.fleet_prior.reason, "positive_prior");
    assert!(status.fleet_prior.used);
    // But `used` never leaks into the disabled shape's credits.
    assert_eq!(status.growth_credit_sats, 0);
    assert_eq!(status.effective_budget_sats, 10_000);
}

#[test]
fn disabled_remaining_never_goes_negative() {
    let mut inputs = base_inputs();
    inputs.enabled = false;
    inputs.actual_spent_sats = 999_999;
    let status = compute_growth_budget_status(&inputs, None);
    assert_eq!(status.remaining_sats, 0);
}

// =============================================================================
// enabled ("dynamic_growth") shape
// =============================================================================

#[test]
fn enabled_without_fleet_prior_only_applies_earned_credit() {
    let mut inputs = base_inputs();
    inputs.net_profit_sats = 1_000;
    inputs.earned_fraction = 0.5;
    inputs.growth_fraction = 0.5;
    inputs.growth_max_extra_sats = 5_000;
    inputs.hard_ceiling_sats = 50_000;
    let status = compute_growth_budget_status(&inputs, None);

    assert_eq!(status.mode, "dynamic_growth");
    assert_eq!(status.earned_credit_sats, 500); // floor(1000 * 0.5)
    assert_eq!(status.growth_credit_sats, 0); // no usable prior -> no growth credit
    assert_eq!(status.effective_budget_sats, 10_500);
    assert!(!status.capped_by_hard_ceiling);
}

#[test]
fn enabled_with_usable_prior_applies_growth_credit_too() {
    let mut inputs = base_inputs();
    inputs.net_profit_sats = 1_000;
    inputs.earned_fraction = 0.5;
    inputs.growth_fraction = 0.3;
    inputs.growth_max_extra_sats = 5_000;
    inputs.hard_ceiling_sats = 50_000;
    let fleet_prior = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 0.75});
    let status = compute_growth_budget_status(&inputs, Some(&fleet_prior));

    assert_eq!(status.earned_credit_sats, 500);
    assert_eq!(status.growth_credit_sats, 300); // floor(1000 * 0.3)
    assert_eq!(status.effective_budget_sats, 10_800);
    assert!(status.fleet_prior.used);
}

#[test]
fn growth_credit_never_exceeds_its_cap() {
    let mut inputs = base_inputs();
    inputs.net_profit_sats = 100_000;
    inputs.growth_fraction = 1.0;
    inputs.growth_max_extra_sats = 42;
    inputs.hard_ceiling_sats = 1_000_000;
    let fleet_prior = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 0.9});
    let status = compute_growth_budget_status(&inputs, Some(&fleet_prior));
    assert_eq!(status.growth_credit_sats, 42);
    assert_eq!(status.growth_credit_cap_sats, 42);
}

#[test]
fn hard_ceiling_caps_effective_budget_and_sets_the_flag() {
    let mut inputs = base_inputs();
    inputs.net_profit_sats = 1_000_000;
    inputs.earned_fraction = 1.0;
    inputs.hard_ceiling_sats = 10_050; // just above base_budget
    let status = compute_growth_budget_status(&inputs, None);

    assert_eq!(status.local_hard_ceiling_sats, 10_050);
    assert_eq!(status.effective_budget_sats, 10_050);
    assert!(status.capped_by_hard_ceiling);
}

#[test]
fn hard_ceiling_never_drops_below_base_budget() {
    let mut inputs = base_inputs();
    inputs.hard_ceiling_sats = 5; // below base_budget (10_000)
    let status = compute_growth_budget_status(&inputs, None);
    assert_eq!(status.local_hard_ceiling_sats, 10_000);
}

#[test]
fn negative_net_profit_yields_zero_credits_not_negative() {
    let mut inputs = base_inputs();
    inputs.net_profit_sats = -50_000;
    inputs.earned_fraction = 0.5;
    inputs.growth_fraction = 0.5;
    let fleet_prior = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 0.9});
    let status = compute_growth_budget_status(&inputs, Some(&fleet_prior));
    assert_eq!(status.earned_credit_sats, 0);
    assert_eq!(status.growth_credit_sats, 0);
    assert_eq!(status.effective_budget_sats, 10_000);
}

#[test]
fn remaining_sats_subtracts_spent_and_reserved_floored_at_zero() {
    let mut inputs = base_inputs();
    inputs.actual_spent_sats = 6_000;
    inputs.reserved_sats = 3_000;
    let status = compute_growth_budget_status(&inputs, None);
    assert_eq!(status.remaining_sats, 1_000);

    inputs.reserved_sats = 5_000;
    let status = compute_growth_budget_status(&inputs, None);
    assert_eq!(status.remaining_sats, 0);
}

// =============================================================================
// fleet_prior_status gates
// =============================================================================

#[test]
fn fleet_prior_missing_when_none_or_empty() {
    assert_eq!(fleet_prior_status(None).reason, "missing");
    let empty = json!({});
    assert_eq!(fleet_prior_status(Some(&empty)).reason, "missing");
    let not_an_object = json!([1, 2, 3]);
    assert_eq!(fleet_prior_status(Some(&not_an_object)).reason, "missing");
}

#[test]
fn fleet_prior_unusable_when_usable_flag_false() {
    let fp = json!({"usable": false, "sample_count": 10, "beneficial_ratio": 0.9});
    let status = fleet_prior_status(Some(&fp));
    assert_eq!(status.reason, "unusable");
    assert!(status.present);
    assert!(!status.usable);
}

#[test]
fn fleet_prior_insufficient_samples_below_three() {
    let fp = json!({"usable": true, "sample_count": 2, "beneficial_ratio": 0.9});
    let status = fleet_prior_status(Some(&fp));
    assert_eq!(status.reason, "insufficient_samples");
    assert_eq!(status.sample_count, 2);

    let fp3 = json!({"usable": true, "sample_count": 3, "beneficial_ratio": 0.9});
    let status3 = fleet_prior_status(Some(&fp3));
    assert_ne!(status3.reason, "insufficient_samples");
}

#[test]
fn fleet_prior_malformed_ratio_when_ratio_is_a_bool() {
    let fp = json!({"usable": true, "sample_count": 10, "beneficial_ratio": true});
    let status = fleet_prior_status(Some(&fp));
    assert_eq!(status.reason, "malformed_ratio");
    assert_eq!(status.beneficial_ratio, None);
}

#[test]
fn fleet_prior_malformed_ratio_when_ratio_is_unparseable() {
    let fp = json!({"usable": true, "sample_count": 10, "beneficial_ratio": "not-a-number"});
    let status = fleet_prior_status(Some(&fp));
    assert_eq!(status.reason, "malformed_ratio");
}

#[test]
fn fleet_prior_malformed_ratio_when_non_finite() {
    // JSON has no NaN/Infinity literal, but a numeric-looking string could
    // still slip through as non-finite in other languages; here we cover
    // the missing-key case which also fails the float() parse (None ->
    // TypeError in Python).
    let fp = json!({"usable": true, "sample_count": 10});
    let status = fleet_prior_status(Some(&fp));
    assert_eq!(status.reason, "malformed_ratio");
}

#[test]
fn fleet_prior_non_positive_when_ratio_at_or_below_half() {
    let fp = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 0.5});
    let status = fleet_prior_status(Some(&fp));
    assert_eq!(status.reason, "non_positive_prior");
    assert!(!status.used);
    assert_eq!(status.beneficial_ratio, Some(0.5));
}

#[test]
fn fleet_prior_positive_when_ratio_above_half() {
    let fp = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 0.5001});
    let status = fleet_prior_status(Some(&fp));
    assert_eq!(status.reason, "positive_prior");
    assert!(status.used);
    assert!(status.usable);
}

#[test]
fn fleet_prior_ratio_is_rounded_to_4_places_and_clamped() {
    let fp = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 1.5});
    let status = fleet_prior_status(Some(&fp));
    assert_eq!(status.beneficial_ratio, Some(1.0)); // clamped before rounding

    let fp2 = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 0.123456789});
    let status2 = fleet_prior_status(Some(&fp2));
    assert_eq!(status2.beneficial_ratio, Some(0.1235));
}

#[test]
fn fleet_prior_ordered_pairs_preserve_key_order() {
    let fp = json!({"usable": true, "sample_count": 10, "beneficial_ratio": 0.9});
    let status = fleet_prior_status(Some(&fp));
    let pairs = status.to_ordered_pairs();
    let keys: Vec<&str> = pairs.iter().map(|(k, _)| *k).collect();
    assert_eq!(
        keys,
        vec![
            "present",
            "usable",
            "used",
            "reason",
            "sample_count",
            "beneficial_ratio"
        ]
    );
}

#[test]
fn growth_budget_status_ordered_pairs_preserve_key_order() {
    let inputs = base_inputs();
    let status = compute_growth_budget_status(&inputs, None);
    let pairs = status.to_ordered_pairs();
    let keys: Vec<&str> = pairs.iter().map(|(k, _)| *k).collect();
    assert_eq!(
        keys,
        vec![
            "mode",
            "authority",
            "advisory_only",
            "fleet_prior_budget_authority",
            "base_budget_sats",
            "local_hard_ceiling_sats",
            "earned_credit_sats",
            "growth_credit_sats",
            "growth_credit_cap_sats",
            "effective_budget_sats",
            "actual_spent_sats",
            "reserved_sats",
            "remaining_sats",
            "capped_by_hard_ceiling",
            "fleet_prior",
        ]
    );
}
