//! `planner::losers` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kind `"identify_losers"`,
//! generated from the REAL `CapacityPlanner._identify_losers` with
//! `_close_protection_reason` / `_check_defib_allowed` monkeypatched to
//! injected values, and `_capital_efficiency` left unset so the
//! dead-capital branch (tested separately in `planner_dead_capital.rs`) is
//! never taken — isolating this module's new fire-sale/stagnant/regime-change
//! logic).

use revops_capital::planner::losers::{
    identify_losers, LoserAction, LoserChannelEvidence, LoserFlowEvidence,
};
use revops_capital::planner::winners::RebalanceSuccessStats;
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/capital/planner/kernels.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn scenarios() -> Vec<Value> {
    fixture()["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "identify_losers")
        .cloned()
        .collect()
}

fn action_from_str(s: &str) -> LoserAction {
    match s {
        "CLOSE" => LoserAction::Close,
        "DEFIBRILLATE" => LoserAction::Defibrillate,
        "FEE_REDUCE" => LoserAction::FeeReduce,
        other => panic!("unknown action {other}"),
    }
}

fn build_channels(case: &Value) -> Vec<LoserChannelEvidence> {
    let success = case["input"]["success_data"]
        .as_object()
        .map(|s| RebalanceSuccessStats {
            success_rate: s["success_rate"].as_f64().unwrap(),
            total: s["total"].as_i64().unwrap(),
        });
    let close_protection = case["input"]["close_protection"]
        .as_str()
        .map(|s| s.to_string());
    let defib_allowed = case["input"]["defib_allowed"].as_array().unwrap();
    let defib_ok = defib_allowed[0].as_bool().unwrap();
    let defib_reason = defib_allowed[1].as_str().unwrap();
    let defib_policy_blocked = !defib_ok && defib_reason.contains("rebalance_mode=");
    let attempt_count = case["input"]["diag_stats"]["attempt_count"]
        .as_i64()
        .unwrap_or(0);
    let is_hard_bleeder = case["input"]["bleeders_hard"].as_bool().unwrap_or(false);

    case["input"]["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ch| {
            let prof = &ch["prof"];
            let flow = if ch["flow"].is_null() {
                None
            } else {
                Some(LoserFlowEvidence {
                    flow_ratio: ch["flow"]["flow_ratio"].as_f64().unwrap(),
                    capacity: ch["flow"]["capacity"].as_i64().unwrap(),
                    daily_volume: ch["flow"]["daily_volume"].as_f64().unwrap(),
                    kalman_regime_change: ch["flow"]["kalman_regime_change"].as_bool().unwrap(),
                })
            };
            LoserChannelEvidence {
                scid: ch["scid"].as_str().unwrap().to_string(),
                peer_id: prof["peer_id"].as_str().unwrap().to_string(),
                capacity_sats: prof["capacity_sats"].as_i64().unwrap(),
                roi_percent: prof["roi_percent"].as_f64().unwrap(),
                marginal_roi_percent: prof["marginal_roi_percent"].as_f64().unwrap(),
                marginal_profit_30d_sats: prof["marginal_profit_30d_sats"].as_i64().unwrap(),
                classification: prof["classification"].as_str().unwrap().to_string(),
                days_open: prof["days_open"].as_i64().unwrap(),
                opener: prof["opener"].as_str().unwrap().to_string(),
                flow,
                rebalance_success: success,
                diagnostic_attempt_count: attempt_count,
                is_hard_bleeder,
                defib_policy_blocked,
                close_protection_reason: close_protection.clone(),
                uptime_pct: None,
                estimated_closure_cost_sats: 3000,
                dead_capital: None,
            }
        })
        .collect()
}

#[test]
fn identify_losers_matches_python() {
    let cases = scenarios();
    assert_eq!(cases.len(), 14, "expected 14 identify_losers scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let channels = build_channels(case);
        let actual = identify_losers(&channels);
        let expected = case["output"].as_array().unwrap();

        assert_eq!(actual.len(), expected.len(), "{name}: loser count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.scid, e["scid"].as_str().unwrap(), "{name}: scid");
            assert_eq!(a.peer_id, e["peer_id"].as_str().unwrap(), "{name}: peer_id");
            assert_eq!(a.reason, e["reason"].as_str().unwrap(), "{name}: reason");
            assert_eq!(a.roi, e["roi"].as_f64().unwrap(), "{name}: roi");
            assert_eq!(
                a.marginal_roi,
                e["marginal_roi"].as_f64().unwrap(),
                "{name}: marginal_roi"
            );
            assert_eq!(
                a.classification,
                e["classification"].as_str().unwrap(),
                "{name}: classification"
            );
            assert_eq!(
                a.capacity,
                e["capacity"].as_i64().unwrap(),
                "{name}: capacity"
            );
            assert_eq!(
                a.rebal_difficulty,
                e["rebal_difficulty"].as_f64().unwrap(),
                "{name}: rebal_difficulty"
            );
            assert_eq!(a.opener, e["opener"].as_str().unwrap(), "{name}: opener");
            assert_eq!(
                a.action,
                action_from_str(e["action"].as_str().unwrap()),
                "{name}: action"
            );
            assert_eq!(
                a.is_hard_bleeder,
                e["is_hard_bleeder"].as_bool().unwrap(),
                "{name}: is_hard_bleeder"
            );
            assert_eq!(
                a.regime_change,
                e["regime_change"].as_bool().unwrap(),
                "{name}: regime_change"
            );
            assert_eq!(
                a.is_fire_sale,
                e["is_fire_sale"].as_bool().unwrap(),
                "{name}: is_fire_sale"
            );
            assert_eq!(
                a.marginal_profit_30d_sats,
                e["marginal_profit_30d_sats"].as_i64().unwrap(),
                "{name}: marginal_profit_30d_sats"
            );
        }
    }
}

/// Control: the protective closure gate (`close_protection_reason`) skips
/// the WHOLE channel, even one that would otherwise be an obvious
/// fire-sale close — py 997-998.
#[test]
fn close_protection_skips_whole_channel() {
    let ev = LoserChannelEvidence {
        scid: "700000:1:0".to_string(),
        peer_id: "peerX".to_string(),
        capacity_sats: 500_000,
        roi_percent: -70.0,
        marginal_roi_percent: -60.0,
        marginal_profit_30d_sats: -200,
        classification: "zombie".to_string(),
        days_open: 100,
        opener: "local".to_string(),
        flow: Some(LoserFlowEvidence {
            flow_ratio: 0.0,
            capacity: 500_000,
            daily_volume: 1000.0,
            kalman_regime_change: false,
        }),
        rebalance_success: None,
        diagnostic_attempt_count: 2,
        is_hard_bleeder: false,
        defib_policy_blocked: false,
        close_protection_reason: Some("inbound_gateway_protected".to_string()),
        uptime_pct: None,
        estimated_closure_cost_sats: 3000,
        dead_capital: None,
    };
    assert!(identify_losers(&[ev]).is_empty());
}
