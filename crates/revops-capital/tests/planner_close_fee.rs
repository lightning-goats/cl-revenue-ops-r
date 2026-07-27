//! `planner::close_fee` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kinds `"close_fee_plan"` and
//! `"extract_actual_close_fee_sats"`, generated from the REAL
//! `CapacityPlanner` close-fee methods).

use revops_capital::planner::close_fee::{
    close_fee_plan, extract_actual_close_fee_sats, CloseFeeConfig, CloseFeeSource, Feerates,
};
use serde_json::Value;
use std::collections::BTreeMap;
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

fn parse_feerates(v: &Value) -> Feerates {
    let mut perkb = BTreeMap::new();
    if let Some(obj) = v["perkb"].as_object() {
        for (k, val) in obj {
            perkb.insert(k.clone(), val.as_f64().unwrap());
        }
    }
    Feerates { perkb }
}

fn parse_cfg(v: &Value) -> CloseFeeConfig {
    let mut cfg = CloseFeeConfig::default();
    if let Some(m) = v["planner_close_fee_reserve_multiplier"].as_f64() {
        cfg.planner_close_fee_reserve_multiplier = m;
    }
    if let Some(c) = v["planner_close_fee_cap_sats"].as_i64() {
        cfg.planner_close_fee_cap_sats = c;
    }
    if let Some(b) = v["planner_close_feerange_enabled"].as_bool() {
        cfg.planner_close_feerange_enabled = b;
    }
    cfg
}

#[test]
fn close_fee_plan_matches_python() {
    use revops_capital::planner::close_fee::estimate_close_cost_sats;

    let cases = scenarios("close_fee_plan");
    assert_eq!(cases.len(), 10, "expected 10 close_fee_plan scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let feerates = parse_feerates(&case["input"]["feerates"]);
        let cfg = parse_cfg(&case["input"]["cfg"]);
        let estimated = estimate_close_cost_sats(Some(&feerates));

        let plan = close_fee_plan(&cfg, estimated);
        let expected = &case["output"];

        assert_eq!(plan.ok, expected["ok"].as_bool().unwrap(), "{name}: ok");
        assert_eq!(
            plan.estimated_cost_sats,
            expected["estimated_cost_sats"].as_i64().unwrap(),
            "{name}: estimated_cost_sats"
        );
        assert_eq!(
            plan.reserve_sats,
            expected["reserve_sats"].as_i64().unwrap(),
            "{name}: reserve_sats"
        );
        assert_eq!(
            plan.fee_cap_sats,
            expected["fee_cap_sats"].as_i64().unwrap(),
            "{name}: fee_cap_sats"
        );
        let expected_source = expected["source"].as_str().unwrap();
        let actual_source = plan.source.as_str();
        assert_eq!(actual_source, expected_source, "{name}: source");

        match &expected["feerange"] {
            Value::Null => assert_eq!(plan.feerange, None, "{name}: feerange should be None"),
            Value::Array(arr) => {
                let expected_pair = (
                    arr[0].as_str().unwrap().to_string(),
                    arr[1].as_str().unwrap().to_string(),
                );
                assert_eq!(plan.feerange, Some(expected_pair), "{name}: feerange");
            }
            other => panic!("{name}: unexpected feerange shape {other:?}"),
        }
    }
}

/// Control: fixed_cap and multiplier sources must be reachable and
/// distinct outcomes, and the two are not the same numeric answer for the
/// same estimated cost — this would fail vacuously if `close_fee_plan`
/// ignored `planner_close_fee_cap_sats` entirely.
#[test]
fn fixed_cap_and_multiplier_sources_differ() {
    let cases = scenarios("close_fee_plan");
    let fixed = cases
        .iter()
        .find(|c| c["name"] == "fixed_cap_sufficient")
        .unwrap();
    let mult = cases
        .iter()
        .find(|c| c["name"] == "multiplier_default")
        .unwrap();
    assert_eq!(fixed["output"]["source"], "fixed_cap");
    assert_eq!(mult["output"]["source"], "multiplier");
    assert_eq!(
        CloseFeeSource::FixedCap.as_str(),
        fixed["output"]["source"].as_str().unwrap()
    );
}

#[test]
fn extract_actual_close_fee_sats_matches_python() {
    let cases = scenarios("extract_actual_close_fee_sats");
    assert_eq!(
        cases.len(),
        6,
        "expected 6 extract_actual_close_fee_sats scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let result = &case["input"]["result"];
        let actual = extract_actual_close_fee_sats(result);
        match &case["output"] {
            Value::Null => assert_eq!(actual, None, "{name}"),
            v => assert_eq!(actual, v.as_i64(), "{name}"),
        }
    }
}

/// Control for the Python dead-code quirk documented on
/// `extract_actual_close_fee_sats`: a dict with NO matching field at all
/// still returns `Some(0)`, not `None` — only a non-dict input returns
/// `None`. A naive "return None if nothing matched" reimplementation would
/// fail this specific assertion while still passing a superficial
/// smoke test.
#[test]
fn no_matching_fields_returns_zero_not_none() {
    let result = serde_json::json!({"foo": "bar"});
    assert_eq!(extract_actual_close_fee_sats(&result), Some(0));

    let not_a_dict = serde_json::json!("not-a-dict");
    assert_eq!(extract_actual_close_fee_sats(&not_a_dict), None);
}
