//! `planner::sizing` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kind `"size_channel"`,
//! generated from the REAL `CapacityPlanner._size_channel`).

use revops_capital::planner::sizing::{size_channel, SizingCandidate, SizingConfig};
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
        .filter(|s| s["kind"] == "size_channel")
        .cloned()
        .collect()
}

fn parse_candidate(v: &Value) -> SizingCandidate<'_> {
    SizingCandidate {
        peer_id: v["peer_id"].as_str().unwrap(),
        score: v["score"].as_f64().unwrap(),
    }
}

#[test]
fn size_channel_matches_python() {
    let cases = scenarios();
    assert_eq!(cases.len(), 6, "expected 6 size_channel scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let candidate = parse_candidate(&case["input"]["candidate"]);
        let all: Vec<SizingCandidate> = case["input"]["all_candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(parse_candidate)
            .collect();
        let available = case["input"]["available_sats"].as_i64().unwrap();
        let cfg = SizingConfig {
            planner_min_channel_sats: case["input"]["min_ch"].as_i64().unwrap(),
            planner_max_channel_sats: case["input"]["max_ch"].as_i64().unwrap(),
        };
        let dest_caps: Vec<i64> = case["input"]["dest_channels"]
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

        let actual = size_channel(&candidate, &all, available, &cfg, &dest_caps);
        let expected = case["output"].as_i64().unwrap();
        assert_eq!(actual, expected, "{name}");
    }
}

/// Revert tripwire: the "never more than 50% of available" cap must
/// actually apply.
#[test]
fn size_channel_never_exceeds_half_available() {
    let candidate = SizingCandidate {
        peer_id: "p1",
        score: 1.0,
    };
    let all = vec![SizingCandidate {
        peer_id: "p1",
        score: 1.0,
    }];
    let cfg = SizingConfig {
        planner_min_channel_sats: 100_000,
        planner_max_channel_sats: 50_000_000,
    };
    let size = size_channel(&candidate, &all, 10_000_000, &cfg, &[]);
    assert!(
        size <= 5_000_000,
        "expected <= half of available, got {size}"
    );
}
