//! Task 71 slice A: the P&L / ROC / TLV evidence producers that
//! profitability, econ-snapshot and dashboard all read.
//!
//! Python returns fabricated ZEROS when `listfunds` fails during TLV (py
//! 1904-1907: warn, fall through, report zeros). Task 71 forbids that, so
//! TLV refuses typed instead -- a deliberate, disclosed divergence. A
//! zeroed TLV is indistinguishable from a node that genuinely holds
//! nothing, and TLV is the net-worth figure the dashboard reports.

use revops::econ_evidence::{
    calculate_roc, pnl_summary, total_liquidating_value, EconRefusal, PnlSources, TlvSources,
};
use serde_json::json;

fn pnl(gross_msat: i64, rebal: i64, closure: i64) -> PnlSources {
    PnlSources {
        window_days: 30,
        gross_revenue_msat: gross_msat,
        rebalance_cost_sats: rebal,
        closure_cost_sats: closure,
        volume_sats: 1_000_000,
        forward_count: 42,
    }
}

/// The basic arithmetic, including the msat->sats CEIL at the boundary.
#[test]
fn pnl_computes_net_and_margin() {
    // 10_000_500 msat ceils to 10_001 sats.
    let s = pnl_summary(pnl(10_000_500, 1_000, 500));
    assert_eq!(s.gross_revenue_sats, 10_001);
    assert_eq!(s.opex_sats, 1_500);
    assert_eq!(s.net_profit_sats, 8_501);
    assert_eq!(s.operating_margin_pct, 85.0);
    assert_eq!(s.volume_sats, 1_000_000);
    assert_eq!(s.forward_count, 42);
}

/// Zero revenue does NOT mean zero margin. Python distinguishes an idle
/// node (no revenue, no costs -> 0%) from a bleeding one (no revenue but
/// real costs -> -100%). Collapsing both to 0.0 would make a node burning
/// sats on rebalances look merely idle on the dashboard.
#[test]
fn zero_revenue_margin_distinguishes_idle_from_bleeding() {
    let idle = pnl_summary(pnl(0, 0, 0));
    assert_eq!(idle.operating_margin_pct, 0.0, "idle node");

    let bleeding = pnl_summary(pnl(0, 5_000, 0));
    assert_eq!(
        bleeding.operating_margin_pct, -100.0,
        "costs but no revenue"
    );
}

/// `window_days < 1` is clamped to 1 rather than dividing by zero during
/// annualization (py 1834's explicit BUG FIX).
#[test]
fn window_days_below_one_is_clamped() {
    let mut s = pnl(1_000_000, 0, 0);
    s.window_days = 0;
    assert_eq!(pnl_summary(s).window_days, 1);

    let roc = calculate_roc(pnl_summary(pnl(1_000_000, 0, 0)), 10_000_000, 0);
    assert_eq!(roc.window_days, 1);
    assert!(
        roc.annualized_roc_pct.is_finite(),
        "must not divide by zero"
    );
}

/// ROC annualizes over the window and rounds exactly as Python does
/// (roc 4dp, annualized 2dp).
#[test]
fn roc_annualizes_and_rounds() {
    // net 1_000 sats on 1_000_000 capacity over 30d = 0.1% for the window.
    let roc = calculate_roc(pnl_summary(pnl(1_000_000, 0, 0)), 1_000_000, 30);
    assert_eq!(roc.total_capacity_sats, 1_000_000);
    assert_eq!(roc.net_profit_sats, 1_000);
    assert_eq!(roc.roc_pct, 0.1);
    // 0.1 * (365/30) = 1.2166... -> 1.22
    assert_eq!(roc.annualized_roc_pct, 1.22);
}

/// Zero capacity yields a genuine 0.0, not a divide. A node with no
/// channels really has no return on deployed capital -- this zero is
/// measured, unlike TLV's refusal below.
#[test]
fn zero_capacity_roc_is_a_real_zero() {
    let roc = calculate_roc(pnl_summary(pnl(1_000_000, 0, 0)), 0, 30);
    assert_eq!(roc.roc_pct, 0.0);
    assert_eq!(roc.annualized_roc_pct, 0.0);
}

/// TLV counts CONFIRMED outputs and CHANNELD_NORMAL channels only, and is
/// onchain + LOCAL balance -- remote balance is reported separately and
/// must never enter the total. TLV answers "what is ours if we closed
/// today", so including remote would overstate the node's net worth by
/// the counterparties' money.
#[test]
fn tlv_sums_confirmed_outputs_and_local_balances() {
    let funds = json!({
        "outputs": [
            {"status": "confirmed", "amount_msat": 5_000_000i64},
            {"status": "unconfirmed", "amount_msat": 9_000_000_000i64},
        ],
        "channels": [
            {"state": "CHANNELD_NORMAL", "our_amount_msat": 600_000_000i64,
             "amount_msat": 1_000_000_000i64},
            {"state": "CHANNELD_AWAITING_LOCKIN", "our_amount_msat": 700_000_000i64,
             "amount_msat": 900_000_000i64},
        ]
    });
    let tlv = total_liquidating_value(TlvSources {
        listfunds: Ok(funds),
    })
    .expect("reads");
    assert_eq!(tlv.onchain_sats, 5_000, "unconfirmed output excluded");
    assert_eq!(
        tlv.local_balance_sats, 600_000,
        "non-NORMAL channel excluded"
    );
    assert_eq!(tlv.remote_balance_sats, 400_000);
    assert_eq!(tlv.channel_count, 1);
    assert_eq!(
        tlv.tlv_sats, 605_000,
        "onchain + local only; remote must not be counted"
    );
}

/// DELIBERATE DIVERGENCE, disclosed: Python logs a warning and returns
/// ZEROS when listfunds fails. This refuses instead. A zeroed TLV is
/// indistinguishable from a node that holds nothing, and TLV is the
/// headline net-worth number -- reporting zero net worth because an RPC
/// timed out is a false statement about the operator's money.
#[test]
fn tlv_refuses_rather_than_fabricating_zeros() {
    let err = total_liquidating_value(TlvSources {
        listfunds: Err("listfunds rpc timeout".into()),
    })
    .expect_err("must refuse");
    assert_eq!(err.code(), "econ_listfunds_unavailable");
    assert!(matches!(err, EconRefusal::ListfundsUnavailable(_)));
}
