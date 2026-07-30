//! Task 67c slice 3: open-EV, dual-fund and redeployment evidence.
//!
//! The traps here are all ABSENT-vs-ZERO or wrong-constant traps that
//! silently bias the planner AGAINST ever opening a channel -- which looks
//! identical to "the planner ran and found nothing worth doing".

use revops::open_ev_evidence::{
    chain_costs, observed_node_daily_ppm, ChainCostSources, ProfitabilitySample,
    LEGACY_CLOSE_COST_SATS, LEGACY_OPEN_COST_SATS,
};
use serde_json::json;

/// The node's observed daily ppm is the MEDIAN of realized
/// (fees_earned/days_open)/capacity across channels.
#[test]
fn observed_daily_ppm_is_the_median_realized_rate() {
    // capacity 1_000_000 sats, 10 days open, 100 sats earned
    //   => (100/10)/1_000_000 * 1e6 = 10 ppm/day
    let samples = vec![
        ProfitabilitySample {
            capacity_sats: 1_000_000,
            days_open: 10,
            fees_earned_msat: 100_000,
        },
        ProfitabilitySample {
            capacity_sats: 1_000_000,
            days_open: 10,
            fees_earned_msat: 200_000,
        },
        ProfitabilitySample {
            capacity_sats: 1_000_000,
            days_open: 10,
            fees_earned_msat: 300_000,
        },
    ];
    assert_eq!(observed_node_daily_ppm(&samples), Some(20.0));
}

/// A brand-new node with no usable channel data yields None, NOT zero.
/// `Some(0.0)` would forecast zero revenue for every candidate, making
/// every EV deeply negative, so the planner would never open a channel
/// again -- and would report success while doing it.
#[test]
fn no_usable_history_is_none_not_zero() {
    assert_eq!(observed_node_daily_ppm(&[]), None);
    // Channels that exist but cannot produce a rate are SKIPPED, not
    // counted as zero-rate samples (py 2852's continue).
    let unusable = vec![
        ProfitabilitySample {
            capacity_sats: 0,
            days_open: 10,
            fees_earned_msat: 100_000,
        },
        ProfitabilitySample {
            capacity_sats: 1_000_000,
            days_open: 0,
            fees_earned_msat: 100_000,
        },
    ];
    assert_eq!(observed_node_daily_ppm(&unusable), None);
}

/// A zero-earning channel that IS measurable counts as a real 0.0 sample.
/// Skipping it would bias the median upward and overstate every forecast.
#[test]
fn a_measurable_zero_earning_channel_is_a_real_sample() {
    let samples = vec![
        ProfitabilitySample {
            capacity_sats: 1_000_000,
            days_open: 10,
            fees_earned_msat: 0,
        },
        ProfitabilitySample {
            capacity_sats: 1_000_000,
            days_open: 10,
            fees_earned_msat: 200_000,
        },
        ProfitabilitySample {
            capacity_sats: 1_000_000,
            days_open: 10,
            fees_earned_msat: 400_000,
        },
    ];
    assert_eq!(observed_node_daily_ppm(&samples), Some(20.0));
}

/// Open and close are priced with DIFFERENT feerates and DIFFERENT tx
/// sizes. Pricing a close like an open is a regression of a specific
/// audit fix (py E-4.6, 2983-2988): the planner executes MUTUAL closes, so
/// mutual_close is the target rate, and a close is ~200 vbytes to an
/// open's ~140.
#[test]
fn open_and_close_use_their_own_feerates_and_sizes() {
    let feerates = json!({
        "perkb": {"opening": 10_000, "mutual_close": 2_000, "unilateral_close": 50_000}
    });
    let c = chain_costs(&ChainCostSources {
        feerates: Ok(feerates),
    });
    // opening: 10000/1000 = 10 sat/vB * 140 vbytes
    assert_eq!(c.open_cost_sats, 1_400);
    // mutual close: 2000/1000 = 2 sat/vB * 200 vbytes
    assert_eq!(c.close_cost_sats, 400);
}

/// When only unilateral_close is published it is used -- it is the HIGHER
/// commitment-tx rate, so the bias is conservative (overstating close cost
/// argues against opening). Silently falling back to the OPENING rate
/// would be the unsafe direction.
#[test]
fn unilateral_close_is_the_conservative_fallback() {
    let feerates = json!({"perkb": {"opening": 10_000, "unilateral_close": 50_000}});
    let c = chain_costs(&ChainCostSources {
        feerates: Ok(feerates),
    });
    assert_eq!(c.close_cost_sats, 10_000, "50 sat/vB * 200 vbytes");
}

/// A feerate RPC failure falls back to the legacy static defaults rather
/// than refusing. This is one of the port's few deliberate NON-refusals:
/// py catches the exception, and refusing here would stop the whole
/// planner cycle over a transient RPC hiccup.
#[test]
fn feerate_failure_falls_back_to_legacy_defaults() {
    let c = chain_costs(&ChainCostSources {
        feerates: Err("rpc down".into()),
    });
    assert_eq!(c.open_cost_sats, LEGACY_OPEN_COST_SATS);
    assert_eq!(c.close_cost_sats, LEGACY_CLOSE_COST_SATS);
    assert!(c.used_fallback, "the fallback must be visible, not silent");

    // A reply that parses but carries no usable rate falls back the same way.
    let c = chain_costs(&ChainCostSources {
        feerates: Ok(json!({"perkb": {}})),
    });
    assert_eq!(c.open_cost_sats, LEGACY_OPEN_COST_SATS);
    assert_eq!(c.close_cost_sats, LEGACY_CLOSE_COST_SATS);
}
