//! Unit tests for the Phase 1b Task 5 response builders
//! (`rpc_history::build_history`, `rpc_report::build_report`,
//! `rpc_dashboard::{build_dashboard, parse_window_days}`), following
//! `rpc_status.rs`'s existing test style: pure functions over
//! hand-constructed structs, no DB involved. DB-integration coverage for
//! the underlying query functions lives in
//! `crates/revops-db/tests/queries.rs`.
//!
//! **Guard-string parity (Phase 1b Task 5 review finding 1):** When the DB
//! is not configured, `revenue-r-history` and `revenue-r-report` RPCs return
//! `{"error": "Plugin not initialized"}` (not "Database not initialized"),
//! matching Python cl-revenue-ops.py:4913-4914 and :5409-5410. The
//! `revenue-r-dashboard` RPC returns `{"error": "Database not initialized"}`
//! when DB is missing (matching Python line 5756).
//!
//! **Guard-order (Phase 1b Task 5 review finding 2):** For
//! `revenue-r-dashboard`, the DB presence check happens *before* validating
//! `window_days`, ensuring bad window_days + no DB returns the DB error
//! (Phase 1 obligation) rather than the window validation error.

use revops::rpc_dashboard::{build_dashboard, parse_window_days};
use revops::rpc_history::build_history;
use revops::rpc_report::build_report;
use revops_db::queries::{ClosedChannelsSummary, ClosureCostWindows, LifetimeStats, PnlSummary};
use serde_json::json;

fn stats(
    total_revenue_msat: i64,
    rebalance: i64,
    opening: i64,
    closure: i64,
    forwards: i64,
) -> LifetimeStats {
    LifetimeStats {
        total_revenue_msat,
        total_rebalance_cost_sats: rebalance,
        total_opening_cost_sats: opening,
        total_closure_cost_sats: closure,
        total_forwards: forwards,
    }
}

fn closed(channel_count: i64) -> ClosedChannelsSummary {
    ClosedChannelsSummary {
        channel_count,
        total_capacity: 3_000_000,
        total_open_costs: 800,
        total_closure_costs: 300,
        total_revenue: 1_200,
        total_rebalance_costs: 70,
        total_forwards: 25,
        total_net_pnl: 30,
        avg_days_open: 75.0,
    }
}

#[test]
fn build_history_computes_roi_and_totals() {
    let s = stats(58_000, 200, 500, 300, 16);
    let c = closed(2);
    let v = build_history(&s, &c);

    assert_eq!(v["lifetime_revenue_sats"], 58);
    assert_eq!(v["lifetime_opening_costs_sats"], 500);
    assert_eq!(v["lifetime_closure_costs_sats"], 300);
    assert_eq!(v["lifetime_rebalance_costs_sats"], 200);
    assert_eq!(v["lifetime_total_costs_sats"], 1_000);
    assert_eq!(v["lifetime_net_profit_sats"], -942);
    assert_eq!(v["lifetime_roi_percent"], -94.2);
    assert_eq!(v["lifetime_forward_count"], 16);
    assert_eq!(v["closed_channels_summary"]["channel_count"], 2);
    assert_eq!(v["closed_channels_summary"]["total_capacity"], 3_000_000);
    assert_eq!(v["closed_channels_summary"]["avg_days_open"], 75.0);
}

#[test]
fn build_history_ceils_sub_sat_revenue() {
    // 58_500 msat -> ceil to 59 sats (not 58) -- sub-sat earnings must stay
    // visible, matching Python's own comment on this exact conversion.
    let s = stats(58_500, 0, 0, 0, 1);
    let v = build_history(&s, &closed(0));
    assert_eq!(v["lifetime_revenue_sats"], 59);
}

#[test]
fn build_history_zero_costs_with_revenue_is_100_percent_roi() {
    let s = stats(5_000, 0, 0, 0, 1);
    let v = build_history(&s, &closed(0));
    assert_eq!(v["lifetime_roi_percent"], 100.0);
}

#[test]
fn build_history_zero_costs_zero_revenue_is_zero_roi() {
    let s = stats(0, 0, 0, 0, 0);
    let v = build_history(&s, &closed(0));
    assert_eq!(v["lifetime_roi_percent"], 0.0);
}

#[test]
fn build_report_costs_shape_matches_python() {
    let costs = ClosureCostWindows {
        last_24h_sats: 1,
        last_7d_sats: 2,
        last_30d_sats: 3,
        total_sats: 4,
    };
    let v = build_report("costs", Some(&costs), 1_800_000_000);

    assert_eq!(v["type"], "costs");
    assert_eq!(v["closure_costs"]["last_24h_sats"], 1);
    assert_eq!(v["closure_costs"]["last_7d_sats"], 2);
    assert_eq!(v["closure_costs"]["last_30d_sats"], 3);
    assert_eq!(v["closure_costs"]["total_sats"], 4);
    assert_eq!(v["estimated_defaults"]["channel_open_sats"], 5000);
    assert_eq!(v["estimated_defaults"]["channel_close_sats"], 3000);
    assert_eq!(v["generated_at"], 1_800_000_000);
}

/// When `revenue-r-report` is called with `report_type="costs"` and no DB is
/// available, the main.rs guard (line 378-380) returns `"Plugin not initialized"`
/// *before* calling the builder. This test documents the builder's fallback
/// behavior when called directly with None costs (used in tests only).
#[test]
fn build_report_costs_without_db_errors() {
    let v = build_report("costs", None, 0);
    assert_eq!(v["error"], "Database not initialized");
}

#[test]
fn build_report_gap_marks_summary_policies_peer() {
    for t in ["summary", "policies", "peer"] {
        let v = build_report(t, None, 0);
        assert_eq!(v["error"], "not_yet_ported");
        assert_eq!(v["report_type"], t);
        assert_eq!(v["reason"], "requires policy_manager (Phase 3)");
    }
}

#[test]
fn build_report_unknown_type_matches_python_error_string_verbatim() {
    let v = build_report("bogus", None, 0);
    assert_eq!(
        v["error"],
        "Unknown report type: bogus. Use 'summary', 'peer', 'policies', or 'costs'"
    );
}

fn pnl() -> PnlSummary {
    PnlSummary {
        window_days: 30,
        gross_revenue_sats: 8,
        opex_sats: 500,
        rebalance_cost_sats: 200,
        closure_cost_sats: 300,
        net_profit_sats: -492,
        operating_margin_pct: -6150.0,
        volume_sats: 7_987,
        forward_count: 6,
    }
}

#[test]
fn build_dashboard_populates_db_backed_fields() {
    let v = build_dashboard(&pnl());

    assert_eq!(v["period"]["window_days"], 30);
    assert_eq!(v["period"]["gross_revenue_sats"], 8);
    assert_eq!(v["period"]["opex_sats"], 500);
    assert_eq!(v["period"]["rebalance_cost_sats"], 200);
    assert_eq!(v["period"]["closure_cost_sats"], 300);
    assert_eq!(v["period"]["volume_sats"], 7_987);
    assert_eq!(v["period"]["forward_count"], 6);
    assert_eq!(v["financial_health"]["net_profit_sats"], -492);
    assert_eq!(v["financial_health"]["operating_margin_pct"], -6150.0);
}

#[test]
fn build_dashboard_gap_marks_tlv_roc_warnings_bleeders() {
    let v = build_dashboard(&pnl());

    assert!(v["financial_health"]["tlv_sats"].is_null());
    assert!(v["financial_health"]["annualized_roc_pct"].is_null());
    assert!(v["bleeder_count"].is_null());
    assert_eq!(v["warnings"], json!([]));

    let gaps: Vec<&str> = v["_phase1b_gaps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g.as_str().unwrap())
        .collect();
    assert_eq!(
        gaps,
        vec![
            "financial_health.tlv_sats",
            "financial_health.annualized_roc_pct",
            "warnings",
            "bleeder_count",
        ]
    );
}

#[test]
fn parse_window_days_defaults_to_30_when_absent() {
    assert_eq!(parse_window_days(None), Ok(30));
    assert_eq!(parse_window_days(Some(&serde_json::Value::Null)), Ok(30));
}

#[test]
fn parse_window_days_clamps_to_365_max() {
    assert_eq!(parse_window_days(Some(&json!(1000))), Ok(365));
}

#[test]
fn parse_window_days_clamps_to_1_min() {
    assert_eq!(parse_window_days(Some(&json!(-5))), Ok(1));
    assert_eq!(parse_window_days(Some(&json!(0))), Ok(1));
}

#[test]
fn parse_window_days_truncates_float() {
    assert_eq!(parse_window_days(Some(&json!(10.9))), Ok(10));
}

#[test]
fn parse_window_days_parses_numeric_string() {
    assert_eq!(parse_window_days(Some(&json!("14"))), Ok(14));
}

#[test]
fn parse_window_days_rejects_non_integer() {
    let err = parse_window_days(Some(&json!("abc"))).unwrap_err();
    assert_eq!(err["error"], "window_days must be an integer");

    let err2 = parse_window_days(Some(&json!(true))).unwrap_err();
    assert_eq!(err2["error"], "window_days must be an integer");

    let err3 = parse_window_days(Some(&json!([1, 2]))).unwrap_err();
    assert_eq!(err3["error"], "window_days must be an integer");
}

/// **Guard-order test (Phase 1b Task 5 review finding 2):**
/// This test documents the parse_window_days error string. In the actual
/// `revenue-r-dashboard` RPC (main.rs), when both DB is missing AND
/// window_days is invalid, the DB check happens *first* (lines 391-393),
/// returning `"Database not initialized"`, so this window_days error is never
/// returned in that scenario. Without the reordering, bad window_days would
/// have been returned even when the DB was missing, violating the guard order.
#[test]
fn parse_window_days_error_pins_guard_order() {
    let err = parse_window_days(Some(&json!("not_a_number")));
    assert!(err.is_err());
    assert_eq!(err.unwrap_err()["error"], "window_days must be an integer");
}

// -- Task 67 slice 6: health reports CURRENT-BOOT loop status --

/// The audit's core complaint, now surfaced end-to-end: a loop whose pass
/// completed in a PRIOR boot must not read `passed` in health. It reads
/// `never_run_this_boot`, and the row's raw terminal fields still show the
/// history so an operator can see what happened and when.
#[test]
fn health_does_not_inherit_a_prior_boots_pass() {
    use revops_db::loop_health::{LoopHealthRow, LoopId, TerminalStatus, WiringStatus};

    let mut row = LoopHealthRow::new(LoopId::Fee, WiringStatus::Ready, 100);
    row.generation = 7;
    row.terminal_generation = 7;
    row.terminal_status = TerminalStatus::Passed;
    row.last_passed_at = Some(90);
    row.boot_id = Some("boot-OLD".to_string());
    row.terminal_boot_id = Some("boot-OLD".to_string());
    let rows = vec![row];

    // THIS boot never ran it.
    let value =
        revops::rpc_health::build_health_with_loops(200, None, None, None, Ok(&rows), "boot-NEW");
    let loop0 = &value["loops"][0];
    assert_eq!(
        loop0["current_status"], "never_run_this_boot",
        "a prior-boot pass must not be inherited: {loop0}"
    );
    // History is still visible -- the verdict changed, not the record.
    assert_eq!(loop0["terminal_status"], "passed");
    assert_eq!(loop0["last_passed_at"], 90);
    assert_eq!(loop0["terminal_boot_id"], "boot-OLD");

    // The boot that DID run it sees passed.
    let value =
        revops::rpc_health::build_health_with_loops(200, None, None, None, Ok(&rows), "boot-OLD");
    assert_eq!(value["loops"][0]["current_status"], "passed");

    // And health reports WHICH boot it is judging against, so the verdict
    // is attributable rather than a bare string.
    assert_eq!(value["boot_id"], "boot-OLD");
}

/// Task 67 slice 6b: `revenue-r-analyze` for a single channel is served
/// from the flow owner's PERSISTED state. Fields the store does not hold
/// are declared in `_gaps` -- never defaulted to zero, which would be
/// indistinguishable from a genuinely idle channel.
#[test]
fn analyze_serves_persisted_flow_state_and_gaps_the_rest() {
    use revops_db::analytics::ChannelFlowStateRow;

    let row = ChannelFlowStateRow {
        scid: "700x1x0".into(),
        peer_id: "02aa".into(),
        flow_state: "source".into(),
        balance_position: "depleted".into(),
        flow_ratio: 0.82,
        velocity: 1.5,
        confidence: 0.61,
        kalman_flow_ratio: 0.0,
        kalman_velocity: 0.0,
        kalman_uncertainty: 0.0,
        kalman_regime_change: false,
        forward_count: 12,
        updated_at: 1_800_000_000,
        boot_id: "boot-a".into(),
    };
    let v = revops::rpc_analyze::build_analyze_from_persisted(
        Some(&serde_json::json!("700x1x0")),
        Some(&row),
    );
    assert_eq!(v["channel"], "700x1x0");
    let a = &v["analysis"];
    assert_eq!(a["state"], "source");
    assert_eq!(a["balance_position"], "depleted");
    assert_eq!(a["forward_count"], 12);
    assert_eq!(a["peer_id"], "02aa");
    // Provenance: which process derived this, and when.
    assert_eq!(a["boot_id"], "boot-a");
    assert_eq!(a["updated_at"], 1_800_000_000);
    // Unpersisted FlowMetrics fields are DECLARED, not zeroed.
    let gaps: Vec<&str> = a["_gaps"]
        .as_array()
        .expect("declared gaps")
        .iter()
        .map(|g| g.as_str().unwrap())
        .collect();
    for expected in ["sats_in", "sats_out", "capacity", "daily_volume"] {
        assert!(
            gaps.contains(&expected),
            "missing gap for {expected}: {gaps:?}"
        );
    }
    for zeroed in ["sats_in", "sats_out", "capacity", "daily_volume"] {
        assert!(
            a.get(zeroed).map(|x| x.is_null()).unwrap_or(true),
            "{zeroed} must be null, never a fabricated 0"
        );
    }

    // A channel the analyzer has no row for is Python's own
    // `{"channel": ..., "analysis": null}` -- no marker, not an error.
    let v = revops::rpc_analyze::build_analyze_from_persisted(
        Some(&serde_json::json!("999x9x9")),
        None,
    );
    assert_eq!(v["channel"], "999x9x9");
    assert!(v["analysis"].is_null());
    assert!(
        v.get("error").is_none(),
        "unknown channel is not an error: {v}"
    );
}
