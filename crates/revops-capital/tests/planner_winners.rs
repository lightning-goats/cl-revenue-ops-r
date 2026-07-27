//! `planner::winners` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kind `"identify_winners"`,
//! generated from the REAL `CapacityPlanner._identify_winners`).

use revops_capital::planner::winners::{
    identify_winners, RebalanceSuccessStats, WinnerCandidateEvidence, WinnerFlowEvidence,
};
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
        .filter(|s| s["kind"] == "identify_winners")
        .cloned()
        .collect()
}

fn opt_f64(v: &Value) -> Option<f64> {
    v.as_f64()
}

fn build_channels(case: &Value) -> Vec<WinnerCandidateEvidence> {
    let success = case["input"]["success_data"]
        .as_object()
        .map(|s| RebalanceSuccessStats {
            success_rate: s["success_rate"].as_f64().unwrap(),
            total: s["total"].as_i64().unwrap(),
        });
    let dts_mean = case["input"]["fee_strategy_state"]
        .as_object()
        .and_then(|s| s.get("v2_state_json"))
        .and_then(|s| s.as_str())
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .and_then(|v| {
            v.get("fee_state")
                .and_then(|fs| fs.get("thompson_state"))
                .and_then(|ts| ts.get("posterior_mean"))
                .and_then(|m| m.as_f64())
        });

    case["input"]["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ch| {
            let prof = &ch["prof"];
            let flow = if ch["flow"].is_null() {
                None
            } else {
                Some(WinnerFlowEvidence {
                    daily_volume: ch["flow"]["daily_volume"].as_f64().unwrap(),
                    flow_ratio: ch["flow"]["flow_ratio"].as_f64().unwrap(),
                    kalman_velocity: ch["flow"]["kalman_velocity"].as_f64().unwrap(),
                    is_congested: ch["flow"]["is_congested"].as_bool().unwrap(),
                })
            };
            WinnerCandidateEvidence {
                scid: ch["scid"].as_str().unwrap().to_string(),
                peer_id: prof["peer_id"].as_str().unwrap().to_string(),
                capacity_sats: prof["capacity_sats"].as_i64().unwrap(),
                marginal_roi_percent: prof["marginal_roi_percent"].as_f64().unwrap(),
                flow,
                rebalance_success: success,
                sourced_fee_contribution_sats: prof
                    .get("sourced_fee_contribution_sats")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0),
                channel_role: prof
                    .get("channel_role")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                dts_posterior_mean: dts_mean,
            }
        })
        .collect()
}

#[test]
fn identify_winners_matches_python() {
    let cases = scenarios();
    assert_eq!(cases.len(), 12, "expected 12 identify_winners scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let channels = build_channels(case);
        let actual = identify_winners(&channels);
        let expected = case["output"].as_array().unwrap();

        assert_eq!(actual.len(), expected.len(), "{name}: winner count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.scid, e["scid"].as_str().unwrap(), "{name}: scid");
            assert_eq!(a.peer_id, e["peer_id"].as_str().unwrap(), "{name}: peer_id");
            assert_eq!(a.roi, e["roi"].as_f64().unwrap(), "{name}: roi");
            assert_eq!(
                a.flow_ratio,
                e["flow_ratio"].as_f64().unwrap(),
                "{name}: flow_ratio"
            );
            assert_eq!(
                a.turnover,
                e["turnover"].as_f64().unwrap(),
                "{name}: turnover"
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
            assert_eq!(
                a.velocity_urgency,
                e["velocity_urgency"].as_bool().unwrap(),
                "{name}: velocity_urgency"
            );
            assert_eq!(
                a.congestion_urgent,
                e["congestion_urgent"].as_bool().unwrap(),
                "{name}: congestion_urgent"
            );
            assert_eq!(
                a.sourced_fee_contribution_sats,
                e["sourced_fee_contribution_sats"].as_i64().unwrap(),
                "{name}: sourced_fee_contribution_sats"
            );
            assert_eq!(
                a.channel_role,
                e["channel_role"].as_str().map(|s| s.to_string()),
                "{name}: channel_role"
            );
            assert_eq!(
                a.dts_posterior_mean,
                opt_f64(&e["dts_posterior_mean"]),
                "{name}: dts_posterior_mean"
            );
        }
    }
}

/// Control: a channel with no flow metrics is never classified as a
/// winner, regardless of how strong its ROI is — matches Python's
/// `if not flow_metrics: continue` (py 820-822).
#[test]
fn missing_flow_metrics_excludes_channel() {
    let channels = vec![WinnerCandidateEvidence {
        scid: "700000:1:0".to_string(),
        peer_id: "peerX".to_string(),
        capacity_sats: 1_000_000,
        marginal_roi_percent: 90.0,
        flow: None,
        rebalance_success: None,
        sourced_fee_contribution_sats: 0,
        channel_role: None,
        dts_posterior_mean: None,
    }];
    assert!(identify_winners(&channels).is_empty());
}
