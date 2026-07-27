//! `planner::candidate_score` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kind `"score_candidate"`,
//! generated from the REAL `CapacityPlanner._score_candidate`).

use revops_capital::planner::candidate_score::{
    score_candidate, CandidateEnrichmentEvidence, ClosedChannelProfitSummary, DemandFlowRole,
    PeerReputation,
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
        .filter(|s| s["kind"] == "score_candidate")
        .cloned()
        .collect()
}

fn build_evidence(input: &Value) -> CandidateEnrichmentEvidence {
    let reputation = input["reputation"].as_object().map(|r| PeerReputation {
        successes: r["successes"].as_i64().unwrap(),
        failures: r["failures"].as_i64().unwrap(),
    });
    let closed_channel_profit =
        input["closed_summary"]
            .as_object()
            .map(|c| ClosedChannelProfitSummary {
                marginal_roi_proxy: c["marginal_roi_proxy"].as_f64().unwrap(),
            });
    let uptime_pct = input["uptime"].as_f64();
    let has_clearnet_address = input["node_addresses"]
        .as_array()
        .map(|addrs| {
            addrs
                .iter()
                .any(|a| matches!(a["type"].as_str(), Some("ipv4") | Some("ipv6")))
        })
        .unwrap_or(false);
    let inbound_median_fee_ppm = input["inbound_fee_data"]["median_fee_ppm"].as_f64();
    let dest_channel_capacities_sats: Vec<i64> = input["dest_channels"]
        .as_array()
        .map(|chans| {
            chans
                .iter()
                .filter(|c| c["active"].as_bool().unwrap_or(false))
                .map(|c| c["amount_msat"].as_i64().unwrap() / 1000)
                .filter(|&c| c > 0)
                .collect()
        })
        .unwrap_or_default();
    let is_sink_adjacent = input["sink_adjacent"].as_bool().unwrap_or(false);
    let demand_flow_role = input["demand_flow_role"].as_str().map(|s| match s {
        "sink" => DemandFlowRole::Sink,
        "source" => DemandFlowRole::Source,
        "unknown" => DemandFlowRole::Unknown,
        _ => DemandFlowRole::Other,
    });

    CandidateEnrichmentEvidence {
        reputation,
        closed_channel_profit,
        uptime_pct,
        has_clearnet_address,
        inbound_median_fee_ppm,
        dest_channel_capacities_sats,
        is_sink_adjacent,
        demand_flow_role,
    }
}

#[test]
fn score_candidate_matches_python() {
    let cases = scenarios();
    assert_eq!(cases.len(), 17, "expected 17 score_candidate scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let base_score = case["input"]["base_score"].as_f64().unwrap();
        let evidence = build_evidence(&case["input"]);
        let actual = score_candidate(base_score, &evidence);
        let expected = case["output"].as_f64().unwrap();
        assert!(
            (actual - expected).abs() < 1e-9,
            "{name}: expected {expected}, got {actual}"
        );
    }
}

/// Control: every enrichment multiplier is genuinely applied — a fully
/// "loaded" evidence set must differ from the bare base score.
#[test]
fn enrichment_actually_changes_score() {
    let evidence = CandidateEnrichmentEvidence {
        reputation: Some(PeerReputation {
            successes: 0,
            failures: 9,
        }),
        ..Default::default()
    };
    let scored = score_candidate(1.0, &evidence);
    assert!(
        scored < 1.0,
        "poor reputation must reduce the score, got {scored}"
    );
}
