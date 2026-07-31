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

fn evidence() -> revops::rpc_dashboard::DashboardEvidence {
    revops::rpc_dashboard::DashboardEvidence {
        tlv_sats: 7_000,
        annualized_roc_pct: 12.17,
        warnings: vec![
            "Channel 700x1x0 is bleeding: Spent 3000 sats rebalancing, earned 1000 sats."
                .to_string(),
        ],
        bleeder_count: 1,
    }
}

#[test]
fn build_dashboard_populates_db_backed_fields() {
    let v = build_dashboard(&pnl(), &evidence());

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

/// C71-28: the four fields this test used to pin as GAPS are now served.
///
/// The replacement matters most for `warnings`. `tlv_sats: null` and
/// `bleeder_count: null` are visibly absent, but `warnings: []` is a
/// well-formed Python answer meaning "no channel is bleeding" -- so a node
/// losing money on every channel reported exactly what a healthy one
/// reports. That is the only one of the four a caller could act on while
/// being wrong.
#[test]
fn build_dashboard_serves_tlv_roc_warnings_and_bleeders() {
    let v = build_dashboard(&pnl(), &evidence());

    assert_eq!(v["financial_health"]["tlv_sats"], json!(7_000));
    assert_eq!(v["financial_health"]["annualized_roc_pct"], json!(12.17));
    assert_eq!(v["bleeder_count"], json!(1));
    assert_eq!(
        v["warnings"],
        json!(["Channel 700x1x0 is bleeding: Spent 3000 sats rebalancing, earned 1000 sats."])
    );
    assert!(
        v.get("_phase1b_gaps").is_none(),
        "no Phase-1b gaps remain on this surface: {v:?}"
    );
}

/// An empty warnings list must still be reachable -- a healthy node really
/// does have nothing to report, and that is not the same fact as the old
/// placeholder.
#[test]
fn a_healthy_node_reports_no_warnings_and_still_carries_no_gap_marker() {
    let v = build_dashboard(&pnl(), &revops::rpc_dashboard::DashboardEvidence::default());
    assert_eq!(v["warnings"], json!([]));
    assert_eq!(v["bleeder_count"], json!(0));
    assert_eq!(v["financial_health"]["tlv_sats"], json!(0));
    assert!(v.get("_phase1b_gaps").is_none());
}

#[test]
fn parse_window_days_defaults_to_30_only_when_the_parameter_is_omitted() {
    // C71-30. Python binds the signature default of 30 only when the
    // parameter is NEVER PASSED. This test used to assert that an explicit
    // `null` also produced 30 -- see the test below for why that is wrong.
    assert_eq!(parse_window_days(None), Ok(30));
}

/// An EXPLICIT null is not an omitted parameter.
///
/// Python's default binds at call time; an explicit `None` reaches
/// `int(None)`, which raises `TypeError` and returns the error dict.
/// Mapping it to 30 silently gave a caller a 30-day window when it had
/// asked for something the server could not understand -- a wrong answer
/// with no error, which is worse than either correct outcome.
#[test]
fn parse_window_days_rejects_an_explicit_null() {
    let err = parse_window_days(Some(&serde_json::Value::Null)).unwrap_err();
    assert_eq!(err["error"], "window_days must be an integer");
}

/// `bool` is an `int` subclass in Python, so `int(True) == 1` and
/// `int(False) == 0` -- and `max(1, ...)` then makes BOTH of them 1.
///
/// This port previously rejected booleans, on the stated grounds that no
/// real caller passes one. That is a divergence either way; matching
/// Python is the objective, and the asymmetry (false also yielding 1, via
/// the clamp rather than the cast) is exactly the kind of detail a
/// reimplementation gets wrong.
#[test]
fn parse_window_days_treats_booleans_as_pythons_int_subclass() {
    assert_eq!(parse_window_days(Some(&json!(true))), Ok(1));
    assert_eq!(
        parse_window_days(Some(&json!(false))),
        Ok(1),
        "int(False) is 0, and the min-clamp lifts it to 1"
    );
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

    let err3 = parse_window_days(Some(&json!([1, 2]))).unwrap_err();
    assert_eq!(err3["error"], "window_days must be an integer");

    let err4 = parse_window_days(Some(&json!({"a": 1}))).unwrap_err();
    assert_eq!(err4["error"], "window_days must be an integer");

    // Python's `int()` accepts surrounding whitespace but not a decimal
    // string: `int(" 45 ")` is 45, `int("45.9")` raises.
    assert_eq!(parse_window_days(Some(&json!(" 45 "))), Ok(45));
    assert!(parse_window_days(Some(&json!("45.9"))).is_err());
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

#[test]
fn profile_preview_freezes_validated_active_profile_at_startup() {
    use revops::rpc_profile_preview::startup_active_profile;

    assert_eq!(startup_active_profile(Ok(None)), Ok("custom".to_string()));
    assert_eq!(
        startup_active_profile(Ok(Some(" BALANCED ".to_string()))),
        Ok("balanced".to_string())
    );
    assert_eq!(
        startup_active_profile(Ok(Some("not-a-profile".to_string()))),
        Ok("custom".to_string()),
        "Python skips an invalid persisted enum and keeps the default"
    );
    assert_eq!(
        startup_active_profile(Err("config override read failed".to_string())),
        Err("config override read failed".to_string()),
        "a failed startup read must not fabricate an active profile"
    );
}

#[test]
fn profile_preview_applies_startup_bundle_below_explicit_overrides() {
    let mut current = serde_json::Map::from_iter([
        ("daily_budget_sats".to_string(), serde_json::json!(5000)),
        ("weekly_budget_sats".to_string(), serde_json::json!(35000)),
    ]);
    let explicit = std::collections::BTreeSet::from(["daily_budget_sats".to_string()]);

    revops::rpc_profile_preview::apply_active_profile(&mut current, "balanced", &explicit);

    assert_eq!(current["daily_budget_sats"], serde_json::json!(5000));
    assert_eq!(current["weekly_budget_sats"], serde_json::json!(56000));
    assert_eq!(current["growth_budget_enabled"], serde_json::json!(true));
}
#[test]
fn fee_authority_status_matches_pythons_fixed_startup_states() {
    use revops::rpc_fee_authority_status::build_fee_authority_status;

    assert_eq!(
        build_fee_authority_status(true, 0, 1_700_000_000, 1_700_000_005, "initial"),
        serde_json::json!({
            "schema": "revenue_ops_fee_authority/v1",
            "enabled": true,
            "generation": 0,
            "transitioned_at": 1_700_000_000,
            "observed_at": 1_700_000_005,
            "reason": "initial",
        })
    );
    assert_eq!(
        build_fee_authority_status(false, 1, 1_700_000_000, 1_700_000_005, "init"),
        serde_json::json!({
            "schema": "revenue_ops_fee_authority/v1",
            "enabled": false,
            "generation": 1,
            "transitioned_at": 1_700_000_000,
            "observed_at": 1_700_000_005,
            "reason": "init",
        })
    );
}

#[test]
fn fee_cycle_denial_matches_python_execution_gate_shape() {
    use revops::rpc_fee_authority_status::FeeAuthorityStatusSnapshot;

    let disabled = FeeAuthorityStatusSnapshot::from_startup_mode(false, 1_700_000_000);
    assert_eq!(
        disabled.fee_cycle_denial_response(),
        Some(serde_json::json!({
            "ok": false,
            "adjusted_channels": 0,
            "fee_debug": {},
            "status": "blocked",
            "reason": "fee_authority_disabled",
            "operation": "revenue-fee-cycle",
            "generation": 1,
            "transitioned_at": 1_700_000_000,
        }))
    );

    let enabled = FeeAuthorityStatusSnapshot::from_startup_mode(true, 1_700_000_000);
    assert_eq!(enabled.fee_cycle_denial_response(), None);
}
