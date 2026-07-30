//! Task 71 slice A: the ROC / TLV evidence producers that profitability,
//! econ-snapshot and dashboard read.
//!
//! P&L is NOT here. `revops_db::queries::pnl_summary` is the canonical
//! Python-parity authority (already consumed by `rpc_dashboard` and
//! `rpc_health`) and is covered by `revops-db/tests/queries.rs`. Review
//! finding F71-R2 caught a duplicate implementation in this module; the
//! structural guard at the bottom of this file keeps it from coming back.
//!
//! Python returns fabricated ZEROS when `listfunds` fails during TLV (py
//! 1904-1907: warn, fall through, report zeros). Task 71 forbids that, so
//! TLV refuses typed instead -- a deliberate, disclosed divergence. A
//! zeroed TLV is indistinguishable from a node that genuinely holds
//! nothing, and TLV is the net-worth figure the dashboard reports.

use revops::econ_evidence::{calculate_roc, total_liquidating_value, EconRefusal, TlvSources};
use revops_db::queries::PnlSummary;
use serde_json::json;

/// A canonical P&L result, as `revops_db::queries::pnl_summary` would
/// return it. ROC reads only `net_profit_sats`.
fn canonical_pnl(net_profit_sats: i64) -> PnlSummary {
    PnlSummary {
        window_days: 30,
        gross_revenue_sats: net_profit_sats.max(0),
        opex_sats: 0,
        rebalance_cost_sats: 0,
        closure_cost_sats: 0,
        net_profit_sats,
        operating_margin_pct: 0.0,
        volume_sats: 1_000_000,
        forward_count: 42,
    }
}

/// `window_days < 1` is clamped to 1 rather than dividing by zero during
/// annualization (py 1830's explicit BUG FIX).
#[test]
fn window_days_below_one_is_clamped() {
    let roc = calculate_roc(canonical_pnl(1_000), 10_000_000, 0);
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
    // 1_000 sats net on 1_000_000 capacity = 0.1% for the window.
    let roc = calculate_roc(canonical_pnl(1_000), 1_000_000, 30);
    assert_eq!(roc.total_capacity_sats, 1_000_000);
    assert_eq!(roc.net_profit_sats, 1_000);
    assert_eq!(roc.roc_pct, 0.1);
    // 0.1 * (365/30) = 1.2166... -> 1.22
    assert_eq!(roc.annualized_roc_pct, 1.22);
}

/// Zero capacity yields a genuine 0.0, not a divide. A node with no
/// channels really has no return on deployed capital -- this zero is
/// MEASURED, unlike the refusals below.
#[test]
fn zero_capacity_roc_is_a_real_zero() {
    let roc = calculate_roc(canonical_pnl(1_000), 0, 30);
    assert_eq!(roc.roc_pct, 0.0);
    assert_eq!(roc.annualized_roc_pct, 0.0);
}

/// TLV counts CONFIRMED outputs and CHANNELD_NORMAL channels only, and is
/// onchain + LOCAL balance -- remote balance is reported separately and
/// must never enter the total. TLV answers "what is ours if we closed
/// today", so including remote would overstate net worth by the
/// counterparties' money.
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

/// A genuinely empty node is a MEASURED zero, not a refusal. Empty arrays
/// are real evidence: this node holds nothing.
#[test]
fn a_genuinely_empty_node_is_a_measured_zero() {
    let funds = json!({"outputs": [], "channels": []});
    let tlv = total_liquidating_value(TlvSources {
        listfunds: Ok(funds),
    })
    .expect("reads");
    assert_eq!(tlv.tlv_sats, 0);
    assert_eq!(tlv.channel_count, 0);
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

/// Review finding F71-R3: a SUCCESSFUL but malformed payload must refuse
/// too. Optional traversal plus parse-to-zero defaults would turn every one
/// of these into a success-shaped zero -- the same false statement about
/// the operator's money that the RPC-failure case makes, only harder to
/// notice, because nothing failed.
#[test]
fn malformed_listfunds_payloads_refuse() {
    let cases = [
        // Required arrays missing entirely.
        json!({"channels": []}),
        json!({"outputs": []}),
        // Required array present but the wrong type.
        json!({"outputs": {}, "channels": []}),
        // A confirmed output with no usable amount.
        json!({"outputs": [{"status": "confirmed"}], "channels": []}),
        // A live channel missing a required balance field.
        json!({"outputs": [],
               "channels": [{"state": "CHANNELD_NORMAL", "amount_msat": 1_000i64}]}),
        // Impossible balance: our share exceeds the channel total.
        json!({"outputs": [],
               "channels": [{"state": "CHANNELD_NORMAL",
                             "our_amount_msat": 2_000i64, "amount_msat": 1_000i64}]}),
    ];
    for payload in cases {
        let err = total_liquidating_value(TlvSources {
            listfunds: Ok(payload.clone()),
        })
        .unwrap_err();
        assert_eq!(err.code(), "econ_listfunds_malformed", "{payload:?}");
        assert!(matches!(err, EconRefusal::MalformedListfunds(_)));
    }
}

/// Review finding F71-R6: presence is not validity. `parse_msat` is
/// deliberately permissive -- it returns 0 for null, bools, arrays,
/// objects and unparseable strings -- so a presence-only check followed by
/// canonical parsing still fabricates a zero at this money boundary.
/// Required amount fields must be VALID CLN msat representations.
#[test]
fn present_but_invalid_amounts_refuse() {
    let invalid = [
        json!("garbage"),
        json!({}),
        json!([]),
        json!(null),
        json!(true),
        json!("12x34"),
        json!("msat"),
    ];
    for bad in invalid {
        let funds = json!({
            "outputs": [{"status": "confirmed", "amount_msat": bad.clone()}],
            "channels": []
        });
        let err = total_liquidating_value(TlvSources {
            listfunds: Ok(funds),
        })
        .unwrap_err();
        assert_eq!(err.code(), "econ_listfunds_malformed", "{bad:?}");

        let funds = json!({
            "outputs": [],
            "channels": [{"state": "CHANNELD_NORMAL",
                          "our_amount_msat": bad.clone(), "amount_msat": 1_000i64}]
        });
        let err = total_liquidating_value(TlvSources {
            listfunds: Ok(funds),
        })
        .unwrap_err();
        assert_eq!(err.code(), "econ_listfunds_malformed", "{bad:?}");
    }
}

/// A VALID zero is still a measured zero. The refusal above must key on
/// the value being unusable, not on it being zero -- an empty channel and
/// a corrupt one are different facts.
#[test]
fn a_valid_zero_amount_is_accepted() {
    for good in [json!(0), json!("0"), json!("0msat")] {
        let funds = json!({
            "outputs": [{"status": "confirmed", "amount_msat": good.clone()}],
            "channels": [{"state": "CHANNELD_NORMAL",
                          "our_amount_msat": good.clone(), "amount_msat": good.clone()}]
        });
        let tlv = total_liquidating_value(TlvSources {
            listfunds: Ok(funds),
        })
        .unwrap_or_else(|e| panic!("valid zero {good:?} must be accepted, got {e:?}"));
        assert_eq!(tlv.tlv_sats, 0);
        assert_eq!(tlv.channel_count, 1, "the channel is real, just empty");
    }
}

/// The canonical string form with the msat suffix parses to a real value.
#[test]
fn suffixed_string_amounts_parse() {
    let funds = json!({
        "outputs": [{"status": "confirmed", "amount_msat": "5000msat"}],
        "channels": []
    });
    let tlv = total_liquidating_value(TlvSources {
        listfunds: Ok(funds),
    })
    .expect("valid suffixed string");
    assert_eq!(tlv.onchain_sats, 5);
}

/// An UNCONFIRMED output with a malformed amount is not an error -- it is
/// excluded before its amount is ever read, so it cannot poison the total.
/// Only the fields actually depended on are required.
#[test]
fn malformed_but_ignored_rows_do_not_refuse() {
    let funds = json!({
        "outputs": [{"status": "unconfirmed"}, {"status": "confirmed", "amount_msat": 1_000i64}],
        "channels": [{"state": "ONCHAIN"}]
    });
    let tlv = total_liquidating_value(TlvSources {
        listfunds: Ok(funds),
    })
    .expect("reads");
    assert_eq!(tlv.onchain_sats, 1);
    assert_eq!(tlv.channel_count, 0);
}

/// Structural guard for review finding F71-R2: exactly ONE P&L arithmetic
/// authority exists. `econ_evidence` must not regrow its own margin
/// computation -- two implementations of the same financial contract drift,
/// and the duplicate also bypassed the intended store routing.
#[test]
fn pnl_arithmetic_has_exactly_one_authority() {
    let src = include_str!("../src/econ_evidence.rs");
    assert!(
        !src.contains("operating_margin_pct ="),
        "econ_evidence must not compute operating margin; \
         revops_db::queries::pnl_summary is the sole authority"
    );
    assert!(
        !src.contains("pub fn pnl_summary"),
        "econ_evidence must not declare a second pnl_summary"
    );
}
