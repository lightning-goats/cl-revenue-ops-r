//! `planner::gates` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kinds
//! `"failed_open_backoff_reason"` and `"peer_exposure_cap_reason"`,
//! generated from the REAL `CapacityPlanner` gate methods).

use revops_capital::planner::gates::{
    failed_open_backoff_reason, peer_exposure_cap_reason, PeerChannelCapacity, PlannerActionRecord,
};
use serde_json::Value;
use std::path::PathBuf;

const PEER_ID: &str = "peer_abcdefabcdefabcdef";

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/capital/planner/kernels.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn scenarios(kind: &str) -> Vec<Value> {
    let fx = fixture();
    fx["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == kind)
        .cloned()
        .collect()
}

#[test]
fn failed_open_backoff_reason_matches_python() {
    let cases = scenarios("failed_open_backoff_reason");
    assert_eq!(
        cases.len(),
        7,
        "expected 7 failed_open_backoff_reason scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let actions_json = case["input"]["actions"].as_array().unwrap();
        let now = case["input"]["now"].as_i64().unwrap();

        let action_strs: Vec<(String, String, i64)> = actions_json
            .iter()
            .map(|a| {
                (
                    a["action_type"].as_str().unwrap().to_string(),
                    a["status"].as_str().unwrap().to_string(),
                    a["created_at"].as_i64().unwrap(),
                )
            })
            .collect();
        let actions: Vec<PlannerActionRecord> = action_strs
            .iter()
            .map(|(t, s, c)| PlannerActionRecord {
                action_type: t.as_str(),
                status: s.as_str(),
                created_at: *c,
            })
            .collect();

        let actual = failed_open_backoff_reason(PEER_ID, &actions, now);
        match case["output"].as_str() {
            Some(expected) => assert_eq!(actual.as_deref(), Some(expected), "{name}"),
            None => assert_eq!(actual, None, "{name}"),
        }
    }
}

/// Control: a completed open must reset the failure streak — this is the
/// exact behavior distinguishing `success_resets_streak` from
/// `many_failures_capped_at_168h` in the fixture; without the `break` on
/// `completed`/`dry_run`, the completed-then-failed sequence would report
/// 2 failures instead of 1 (still blocked, but with the wrong retry math).
#[test]
fn completed_open_resets_failure_streak() {
    let now = 2_000_000_000;
    let actions = [
        PlannerActionRecord {
            action_type: "open",
            status: "failed",
            created_at: now - 100,
        },
        PlannerActionRecord {
            action_type: "open",
            status: "completed",
            created_at: now - 200,
        },
        // An older failure BEFORE the reset must never be counted.
        PlannerActionRecord {
            action_type: "open",
            status: "failed",
            created_at: now - 300,
        },
    ];
    let reason = failed_open_backoff_reason(PEER_ID, &actions, now).unwrap();
    assert!(reason.contains("1 recent failure"), "reason was: {reason}");
}

#[test]
fn peer_exposure_cap_reason_matches_python() {
    let cases = scenarios("peer_exposure_cap_reason");
    assert_eq!(
        cases.len(),
        6,
        "expected 6 peer_exposure_cap_reason scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let max_channel_sats = case["input"]["max_channel_sats"].as_i64().unwrap();
        let channels_json = case["input"]["channels"].as_array().unwrap();

        let channel_data: Vec<(String, String, i64)> = channels_json
            .iter()
            .map(|c| {
                (
                    c["peer_id"].as_str().unwrap().to_string(),
                    c["state"].as_str().unwrap().to_string(),
                    c["total_msat"].as_i64().unwrap(),
                )
            })
            .collect();
        let channels: Vec<PeerChannelCapacity> = channel_data
            .iter()
            .map(|(p, s, t)| PeerChannelCapacity {
                peer_id: p.as_str(),
                state: s.as_str(),
                total_msat: *t,
            })
            .collect();

        let actual = peer_exposure_cap_reason(PEER_ID, max_channel_sats, &channels);
        match case["output"].as_str() {
            Some(expected) => assert_eq!(actual.as_deref(), Some(expected), "{name}"),
            None => assert_eq!(actual, None, "{name}"),
        }
    }
}

/// Control: exposure from a DIFFERENT peer's channels must never count
/// toward this peer's cap — proves the peer_id filter is applied, not just
/// a raw capacity sum.
#[test]
fn other_peers_capacity_does_not_count() {
    let channels = [PeerChannelCapacity {
        peer_id: "totally_different_peer",
        state: "CHANNELD_NORMAL",
        total_msat: 100_000_000_000,
    }];
    assert_eq!(
        peer_exposure_cap_reason(PEER_ID, 1_000_000, &channels),
        None
    );
}
