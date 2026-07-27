//! `planner::portfolio_gate` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kind `"portfolio_gate"`,
//! generated from the REAL `CapacityPlanner._check_portfolio_balance_gate`
//! by `tools/port/gen_capital_planner_fixtures.py`).

use revops_capital::planner::portfolio_gate::{check_portfolio_balance_gate, ChannelBalance};
use serde_json::Value;
use std::path::PathBuf;

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
fn matches_python_across_all_scenarios() {
    let cases = scenarios("portfolio_gate");
    assert_eq!(
        cases.len(),
        8,
        "expected 8 portfolio_gate fixture scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let channels: Vec<ChannelBalance> = case["input"]["channels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| ChannelBalance {
                state: c["state"].as_str().unwrap().to_string(),
                to_us_msat: c["to_us_msat"].as_i64().unwrap(),
                total_msat: c["total_msat"].as_i64().unwrap(),
            })
            .collect();

        let expected = case["output"].as_str().unwrap();
        let actual = check_portfolio_balance_gate(&channels);
        assert_eq!(actual.as_str(), expected, "scenario {name}");
    }
}

/// Control: healthy and blocked must be genuinely different outcomes for
/// different inputs — this would pass vacuously if the gate always
/// returned "healthy".
#[test]
fn healthy_and_blocked_are_distinguishable() {
    let healthy = check_portfolio_balance_gate(&[ChannelBalance {
        state: "CHANNELD_NORMAL".to_string(),
        to_us_msat: 100_000_000,
        total_msat: 1_000_000_000,
    }]);
    let blocked = check_portfolio_balance_gate(&[ChannelBalance {
        state: "CHANNELD_NORMAL".to_string(),
        to_us_msat: 999_000_000,
        total_msat: 1_000_000_000,
    }]);
    assert_ne!(healthy.as_str(), blocked.as_str());
    assert_eq!(healthy.as_str(), "healthy");
    assert_eq!(blocked.as_str(), "blocked");
}
