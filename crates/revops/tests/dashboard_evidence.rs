//! C71-28 (RED): the four `_phase1b_gaps` the dashboard has carried since
//! Phase 1b -- `financial_health.tlv_sats`,
//! `financial_health.annualized_roc_pct`, `warnings`, `bleeder_count`.
//!
//! Each has an exact Python source, and each currently answers with a
//! placeholder that Python never emits:
//!
//! - `tlv_sats: null`          -- py `get_tlv()` over `listfunds`
//! - `annualized_roc_pct: null`-- py `calculate_roc(window_days)`
//! - `warnings: []`            -- py's bleeder warning lines. THIS is the
//!   dangerous one: an empty array is a well-formed Python answer meaning
//!   "no channel is bleeding", so a node quietly losing money reports the
//!   same thing as a healthy one.
//! - `bleeder_count: null`

use revops::dashboard_evidence::{
    annualized_roc_pct, bleeder_warnings, total_capacity_sats, total_liquidating_value,
};
use revops_db::queries::{PerChannelCosts, PerChannelRevenue};
use serde_json::json;
use std::collections::HashMap;

// ---------------------------------------------------------------------
// TLV -- py `get_tlv` (profitability_analyzer.py:1867-1915)
// ---------------------------------------------------------------------

#[test]
fn tlv_is_confirmed_onchain_plus_local_channel_balance() {
    // py: confirmed outputs only, and CHANNELD_NORMAL channels only.
    let funds = json!({
        "outputs": [
            {"status": "confirmed", "amount_msat": 5_000_000},
            {"status": "unconfirmed", "amount_msat": 9_000_000},
        ],
        "channels": [
            {"state": "CHANNELD_NORMAL", "our_amount_msat": 2_000_000, "amount_msat": 6_000_000},
            {"state": "ONCHAIN", "our_amount_msat": 7_000_000, "amount_msat": 8_000_000},
        ],
    });
    let tlv = total_liquidating_value(&funds).expect("listfunds is well-formed");
    assert_eq!(tlv.onchain_sats, 5_000);
    assert_eq!(tlv.local_balance_sats, 2_000);
    assert_eq!(tlv.tlv_sats, 7_000);
    assert_eq!(
        tlv.channel_count, 1,
        "only CHANNELD_NORMAL channels are counted"
    );
}

#[test]
fn tlv_floors_partial_sats_and_reads_msat_strings() {
    // py `base_to_sats_floor` on a balance: a partial sat is not yours yet.
    let funds = json!({
        "outputs": [{"status": "confirmed", "amount_msat": "1999msat"}],
        "channels": [{"state": "CHANNELD_NORMAL", "our_amount_msat": "2999msat",
                      "amount_msat": "5000msat"}],
    });
    let tlv = total_liquidating_value(&funds).expect("well-formed");
    assert_eq!(tlv.onchain_sats, 1);
    assert_eq!(tlv.local_balance_sats, 2);
    assert_eq!(tlv.tlv_sats, 3);
}

#[test]
fn a_listfunds_reply_with_no_arrays_refuses_rather_than_reporting_zero_net_worth() {
    // DISCLOSED DIVERGENCE. py catches RpcError and "returns zeros"
    // (profitability_analyzer.py:1904-1907), so an unreachable node reports
    // a node worth 0 sats -- indistinguishable from a genuinely empty one.
    // TLV is the operator's net worth; zero is never a safe placeholder.
    assert!(total_liquidating_value(&json!({"error": "boom"})).is_err());
    assert!(total_liquidating_value(&json!(null)).is_err());
}

#[test]
fn a_genuinely_empty_node_reports_zero_tlv_and_that_is_a_real_answer() {
    let tlv = total_liquidating_value(&json!({"outputs": [], "channels": []}))
        .expect("empty arrays are present, so this was consulted");
    assert_eq!(tlv.tlv_sats, 0);
    assert_eq!(tlv.channel_count, 0);
}

// ---------------------------------------------------------------------
// Capacity + ROC -- py `_get_all_channels` / `calculate_roc`
// ---------------------------------------------------------------------

#[test]
fn capacity_counts_only_normal_channels_and_falls_back_to_spendable_plus_receivable() {
    // py: `total_msat` first; if it floors to 0, `spendable + receivable`.
    let channels = vec![
        json!({"short_channel_id": "700x1x0", "state": "CHANNELD_NORMAL",
               "total_msat": 5_000_000}),
        json!({"short_channel_id": "800x1x0", "state": "CHANNELD_NORMAL",
               "total_msat": 0, "spendable_msat": 1_000_000, "receivable_msat": 2_000_000}),
        json!({"short_channel_id": "900x1x0", "state": "CHANNELD_AWAITING_LOCKIN",
               "total_msat": 9_000_000}),
    ];
    assert_eq!(total_capacity_sats(&channels), 8_000);
}

#[test]
fn a_channel_with_no_scid_is_not_capacity() {
    // py skips a channel whose normalized SCID is empty -- it is not yet a
    // channel the analyzer can name.
    let channels = vec![json!({"state": "CHANNELD_NORMAL", "total_msat": 5_000_000})];
    assert_eq!(total_capacity_sats(&channels), 0);
}

#[test]
fn roc_annualizes_the_window_return_on_deployed_capital() {
    // py: roc_pct = (net/capacity)*100; annualized = roc_pct * (365/window).
    // 1_000 sats profit on 100_000 sats over 30 days = 1% -> 12.17% annual.
    assert_eq!(annualized_roc_pct(1_000, 100_000, 30), 12.17);
}

#[test]
fn roc_is_zero_when_no_capacity_is_deployed() {
    // py's explicit else-branch, not a divide guard: with nothing deployed
    // there is no return to annualize.
    assert_eq!(annualized_roc_pct(5_000, 0, 30), 0.0);
}

#[test]
fn roc_treats_a_sub_one_day_window_as_one_day() {
    // py `if window_days < 1: window_days = 1`, guarding the annualization
    // divide.
    assert_eq!(annualized_roc_pct(1_000, 100_000, 0), 365.0);
}

#[test]
fn a_losing_node_reports_a_negative_roc_rather_than_clamping_to_zero() {
    assert!(annualized_roc_pct(-1_000, 100_000, 30) < 0.0);
}

// ---------------------------------------------------------------------
// Bleeders + warnings -- py `identify_bleeders` (profitability_analyzer
// .py:1500-1594) over `get_channel_full_pnl` (database.py:3252-3292)
// ---------------------------------------------------------------------

fn revenue(direct_msat: i64, sourced_msat: i64) -> PerChannelRevenue {
    PerChannelRevenue {
        fees_earned_msat: direct_msat,
        volume_routed_msat: 0,
        forward_count: 0,
        sourced_volume_msat: 0,
        sourced_fee_contribution_msat: sourced_msat,
        sourced_forward_count: 0,
    }
}

fn costs(rebalance_msat: i64) -> PerChannelCosts {
    PerChannelCosts {
        peer_id: "02aa".into(),
        open_cost_sats: 0,
        capacity_sats: 1_000_000,
        opened_at: 1,
        rebalance_cost_sats: 0,
        rebalance_cost_30d_sats: 0,
        rebalance_cost_msat: rebalance_msat,
        rebalance_cost_30d_msat: rebalance_msat,
    }
}

fn active(scids: &[&str]) -> Vec<serde_json::Value> {
    scids
        .iter()
        .map(|s| {
            json!({"short_channel_id": s, "state": "CHANNELD_NORMAL",
                        "total_msat": 1_000_000_000})
        })
        .collect()
}

#[test]
fn a_channel_that_spent_more_than_it_earned_is_a_bleeder() {
    let mut rev = HashMap::new();
    rev.insert("700x1x0".to_string(), revenue(1_000_000, 0)); // 1000 sats
    let mut cst = HashMap::new();
    cst.insert("700x1x0".to_string(), costs(3_000_000)); // 3000 sats

    let out = bleeder_warnings(&active(&["700x1x0"]), &rev, &cst);
    assert_eq!(out.bleeder_count, 1);
    assert_eq!(
        out.warnings,
        vec![
            "Channel 700x1x0 is bleeding: Spent 3000 sats rebalancing, earned 1000 sats."
                .to_string()
        ],
        "the warning line is Python's exact wording; operators grep it"
    );
}

#[test]
fn contribution_is_the_max_role_not_the_sum_of_both_roles() {
    // py `get_channel_full_pnl`: `max(revenue_msat, sourced_fee_msat)` --
    // "credit the channel for its most valuable role", NOT the per-channel
    // valuation's direct+sourced sum. Summing here would hide a bleeder
    // whose two roles each fall short of its rebalance spend.
    let mut rev = HashMap::new();
    rev.insert("700x1x0".to_string(), revenue(1_000_000, 1_000_000));
    let mut cst = HashMap::new();
    cst.insert("700x1x0".to_string(), costs(1_500_000));

    let out = bleeder_warnings(&active(&["700x1x0"]), &rev, &cst);
    assert_eq!(
        out.bleeder_count, 1,
        "max(1000, 1000) - 1500 = -500: a bleeder. Summing to 2000 would \
         report this channel as healthy."
    );
}

#[test]
fn a_channel_that_never_routed_but_was_paid_to_fill_is_still_a_bleeder() {
    // py audit F7: no activity filter. A pure bleeder -- paid to fill,
    // never routed -- is exactly the channel an operator must see.
    let rev = HashMap::new();
    let mut cst = HashMap::new();
    cst.insert("700x1x0".to_string(), costs(2_000_000));

    let out = bleeder_warnings(&active(&["700x1x0"]), &rev, &cst);
    assert_eq!(out.bleeder_count, 1);
    assert!(out.warnings[0].contains("earned 0 sats"));
}

#[test]
fn a_profitable_channel_is_not_a_bleeder() {
    let mut rev = HashMap::new();
    rev.insert("700x1x0".to_string(), revenue(5_000_000, 0));
    let mut cst = HashMap::new();
    cst.insert("700x1x0".to_string(), costs(1_000_000));

    let out = bleeder_warnings(&active(&["700x1x0"]), &rev, &cst);
    assert_eq!(out.bleeder_count, 0);
    assert!(out.warnings.is_empty());
}

#[test]
fn only_active_channels_are_examined() {
    // py iterates `_get_all_channels()` -- CHANNELD_NORMAL only. A closed
    // channel's historic loss is not a live warning.
    let mut rev = HashMap::new();
    rev.insert("700x1x0".to_string(), revenue(0, 0));
    let mut cst = HashMap::new();
    cst.insert("700x1x0".to_string(), costs(9_000_000));

    // The channel IS in the listpeerchannels reply -- it is closing, not
    // absent. An empty list would leave the state filter untested, since
    // the loop would iterate nothing either way.
    let closing = vec![
        json!({"short_channel_id": "700x1x0", "state": "CHANNELD_SHUTTING_DOWN",
                              "total_msat": 1_000_000_000}),
    ];
    let out = bleeder_warnings(&closing, &rev, &cst);
    assert_eq!(out.bleeder_count, 0, "the channel is not open any more");
    assert!(out.warnings.is_empty());
}

#[test]
fn a_sub_sat_loss_is_not_a_bleeder_because_python_rounds_toward_zero() {
    // net_pnl_sats = base_delta_to_sats_toward_zero(net_pnl_msat), and the
    // gate is `net_pnl_sats < 0`. A 500 msat loss rounds to 0 and is not a
    // bleeder -- flooring instead would manufacture warnings on every
    // channel with a sub-sat rounding loss.
    let mut rev = HashMap::new();
    rev.insert("700x1x0".to_string(), revenue(1_000_000, 0));
    let mut cst = HashMap::new();
    cst.insert("700x1x0".to_string(), costs(1_000_500));

    let out = bleeder_warnings(&active(&["700x1x0"]), &rev, &cst);
    assert_eq!(out.bleeder_count, 0);
}

#[test]
fn bleeders_are_ordered_worst_first() {
    let mut rev = HashMap::new();
    rev.insert("700x1x0".to_string(), revenue(0, 0));
    rev.insert("800x1x0".to_string(), revenue(0, 0));
    let mut cst = HashMap::new();
    cst.insert("700x1x0".to_string(), costs(1_000_000));
    cst.insert("800x1x0".to_string(), costs(9_000_000));

    let out = bleeder_warnings(&active(&["700x1x0", "800x1x0"]), &rev, &cst);
    assert_eq!(out.bleeder_count, 2);
    assert!(
        out.warnings[0].contains("800x1x0"),
        "py sorts by net_pnl_sats ascending -- the worst bleeder leads: {:?}",
        out.warnings
    );
}

#[test]
fn both_scid_spellings_reach_the_same_channels_pnl() {
    // The snapshot folds onto the `x` spelling; a `:`-spelled channel from
    // listpeerchannels must still find its own costs, or it silently reads
    // as a channel with no spend at all.
    let rev = HashMap::new();
    let mut cst = HashMap::new();
    cst.insert("700x1x0".to_string(), costs(2_000_000));

    let channels = vec![
        json!({"short_channel_id": "700:1:0", "state": "CHANNELD_NORMAL",
                               "total_msat": 1_000_000_000}),
    ];
    let out = bleeder_warnings(&channels, &rev, &cst);
    assert_eq!(out.bleeder_count, 1);
    assert!(
        out.warnings[0].contains("700x1x0"),
        "the warning names the normalized scid: {:?}",
        out.warnings
    );
}

// ---------------------------------------------------------------------
// C71-28: the RPC caller's composition. `main.rs` is a binary no test can
// import, which is where the previous placeholder wiring survived.
// ---------------------------------------------------------------------

fn dashboard_handler() -> String {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"))
        .expect("main.rs is readable");
    let after = source
        .split_once("&dashboard_name,")
        .expect("the dashboard RPC must be registered")
        .1;
    after
        .split_once(".rpcmethod(")
        .map(|(handler, _)| handler.to_string())
        .unwrap_or_else(|| after.to_string())
}

#[test]
fn the_dashboard_consults_every_source_its_four_fields_need() {
    let handler = dashboard_handler();
    for source in [
        "listfunds",
        "fetch_channel_snapshot",
        "profitability_snapshot",
        "pnl_summary",
    ] {
        assert!(
            handler.contains(source),
            "the dashboard must consult `{source}`; without it a formerly \
             gapped field would be answered from a placeholder again"
        );
    }
    assert_eq!(
        handler.matches("fetch_channel_snapshot").count(),
        1,
        "one channel snapshot per call: capacity (for ROC) and the bleeder \
         set must describe the same node state"
    );
}

#[test]
fn the_dashboard_never_answers_a_failed_source_with_a_placeholder() {
    let handler = dashboard_handler();
    let code: String = handler
        .lines()
        .map(|line| line.split("//").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join(" ");
    for code_name in [
        "dashboard_funds_unavailable",
        "dashboard_channels_unavailable",
        "dashboard_snapshot_unavailable",
    ] {
        assert!(
            code.contains(code_name),
            "every source needs its own refusal: `{code_name}` is missing"
        );
    }
    assert!(
        !code.contains("_phase1b_gaps"),
        "the gap marker is retired on this surface"
    );
}

// ---------------------------------------------------------------------
// C71-35: main.rs must DELEGATE the econ assembly, not inline it.
//
// The behavioural matrix for every gate lives in `tests/econ_producer.rs`
// and runs against real stores and a fake CLN socket. The only thing left
// to check from source is that `main.rs` still routes there -- a handler
// that drifted back to inline logic would silently lose all of that
// coverage, because `main.rs` is a binary no test can import.
// ---------------------------------------------------------------------

#[test]
fn the_econ_rpc_delegates_to_the_testable_producer() {
    let source = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/main.rs"))
        .expect("main.rs is readable");
    let after = source
        .split_once("&econ_snapshot_name,")
        .expect("the econ-snapshot RPC must be registered")
        .1;
    let handler = after
        .split_once(".rpcmethod(")
        .map(|(handler, _)| handler.to_string())
        .unwrap_or_else(|| after.to_string());
    let code: String = handler
        .lines()
        .map(|line| line.split("//").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join(" ");

    assert!(
        code.contains("econ_producer::econ_snapshot_response"),
        "the handler must call the producer that the behavioural tests drive"
    );
    assert!(
        !code.contains("build_econ_snapshot_not_wired"),
        "the surface is wired; the unported marker would hide a real answer"
    );
    // No inline assembly: the gates belong to the producer, where they are
    // executable.
    for inlined in [
        "SnapshotAssembly::Ready",
        "gather_profitability",
        "budget_status",
    ] {
        assert!(
            !code.contains(inlined),
            "`{inlined}` belongs in the producer, not in the untestable binary"
        );
    }
}
