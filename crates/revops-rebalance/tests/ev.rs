//! Sats-EV gate / per-attempt ceiling / fee-escalation golden parity,
//! pinned by `fixtures/rebalance/ev.json` (generated from the REAL
//! `modules/rebalance_engine_v2._build_score_decomposition` /
//! `_per_attempt_fee_ceiling` and `modules/rebalancer.EVRebalancer.
//! _apply_fee_escalation` by `tools/port/gen_rebalance_fixtures.py ev` in
//! the port worktree, branch `phase5-t6-gen`).
//!
//! `final_score_sats` is compared via `revops_econ::pyfloat::py_repr`
//! string equality (Global Constraints: byte-parity discipline), never
//! epsilon.

use revops_econ::pyfloat::py_repr;
use revops_rebalance::ev::{fee_escalation, per_attempt_ceiling, sats_ev_gate, EvInputs};
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rebalance/ev.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

#[test]
fn sats_ev_gate_replays_python_fixture_byte_identically() {
    let fx = fixture();
    let cases = fx["gate_cases"].as_array().expect("gate_cases array");
    assert!(
        cases.len() >= 500,
        "expected >= 500 gate cases, got {}",
        cases.len()
    );

    for case in cases {
        let case_id = case["case_id"].as_str().unwrap();
        let inputs = &case["inputs"];
        let ev_inputs = EvInputs {
            probability_ppm: inputs["probability_ppm"].as_i64().unwrap(),
            dest_attempts: inputs["dest_attempts"].as_i64().unwrap(),
            dest_success_rate: inputs["dest_success_rate"].as_f64().unwrap(),
            efv_sats: inputs["efv_sats"].as_f64().unwrap(),
            fee_sats: inputs["fee_sats"].as_i64().unwrap(),
            source_opportunity_sats: inputs["source_opportunity_sats"].as_f64().unwrap(),
            activity_penalty_sats: inputs["activity_penalty_sats"].as_f64().unwrap(),
            hold_margin_sats: inputs["hold_margin_sats"].as_f64().unwrap(),
        };

        let verdict = sats_ev_gate(&ev_inputs);
        let expected = &case["expected"];
        let expected_pass = expected["pass"].as_bool().unwrap();
        let expected_final_score = expected["final_score_sats"].as_f64().unwrap();
        let expected_reject_reason = expected["reject_reason"].as_str();

        assert_eq!(verdict.pass, expected_pass, "case {case_id}: pass mismatch");
        assert_eq!(
            py_repr(verdict.final_score_sats),
            py_repr(expected_final_score),
            "case {case_id}: final_score_sats mismatch (py_repr)"
        );
        assert_eq!(
            verdict.reject_reason, expected_reject_reason,
            "case {case_id}: reject_reason mismatch"
        );
    }
}

#[test]
fn zero_cost_route_always_passes_regardless_of_score() {
    let fx = fixture();
    let cases = fx["gate_cases"].as_array().unwrap();
    let case = cases
        .iter()
        .find(|c| c["case_id"] == "zero_cost_route_always_passes")
        .expect("zero_cost_route_always_passes case present");
    let inputs = &case["inputs"];
    assert_eq!(inputs["fee_sats"].as_i64().unwrap(), 0);
    let ev_inputs = EvInputs {
        probability_ppm: inputs["probability_ppm"].as_i64().unwrap(),
        dest_attempts: inputs["dest_attempts"].as_i64().unwrap(),
        dest_success_rate: inputs["dest_success_rate"].as_f64().unwrap(),
        efv_sats: inputs["efv_sats"].as_f64().unwrap(),
        fee_sats: 0,
        source_opportunity_sats: inputs["source_opportunity_sats"].as_f64().unwrap(),
        activity_penalty_sats: inputs["activity_penalty_sats"].as_f64().unwrap(),
        hold_margin_sats: 1_000_000.0,
    };
    let verdict = sats_ev_gate(&ev_inputs);
    assert!(verdict.pass, "zero-cost route must always pass");
    assert_eq!(verdict.reject_reason, None);
}

#[test]
fn hold_margin_exact_tie_passes_strict_inequality() {
    let fx = fixture();
    let cases = fx["gate_cases"].as_array().unwrap();
    let tie = cases
        .iter()
        .find(|c| c["case_id"] == "hold_margin_exact_tie_passes")
        .expect("tie case present");
    assert!(tie["expected"]["pass"].as_bool().unwrap());
    let just_above = cases
        .iter()
        .find(|c| c["case_id"] == "hold_margin_just_above_rejects")
        .expect("just-above case present");
    assert!(!just_above["expected"]["pass"].as_bool().unwrap());
}

#[test]
fn per_attempt_ceiling_replays_python_fixture() {
    let fx = fixture();
    let cases = fx["per_attempt_ceiling_cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let case_id = case["case_id"].as_str().unwrap();
        let budget = case["prob_adjusted_budget_sats"].as_i64().unwrap();
        let amount = case["amount_sats"].as_i64().unwrap();
        let ppm = case["pair_fee_cap_ppm"].as_i64().unwrap();
        let expected = case["expected"].as_i64().unwrap();
        assert_eq!(
            per_attempt_ceiling(budget, amount, ppm),
            expected,
            "case {case_id}"
        );
    }
}

#[test]
fn fee_escalation_replays_python_fixture() {
    let fx = fixture();
    let cases = fx["fee_escalation_cases"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let case_id = case["case_id"].as_str().unwrap();
        let last = case["last_attempted_sats"].as_i64().unwrap();
        let ev_max = case["ev_max_sats"].as_i64().unwrap();
        let expected = case["expected"].as_i64().unwrap();
        assert_eq!(fee_escalation(last, ev_max), expected, "case {case_id}");
    }
}

#[test]
fn constants_match_frozen_contract() {
    use revops_rebalance::ev::{
        EXPECTED_UTILIZATION, FAILURE_COST_RATE, SOURCE_UTILIZATION_DISCOUNT,
        UNVALIDATED_ADVERTISED_FEE_DISCOUNT,
    };
    let fx = fixture();
    let c = &fx["constants"];
    assert_eq!(
        EXPECTED_UTILIZATION,
        c["expected_utilization"].as_f64().unwrap()
    );
    assert_eq!(
        SOURCE_UTILIZATION_DISCOUNT,
        c["source_utilization_discount"].as_f64().unwrap()
    );
    assert_eq!(FAILURE_COST_RATE, c["failure_cost_rate"].as_f64().unwrap());
    assert_eq!(
        UNVALIDATED_ADVERTISED_FEE_DISCOUNT,
        c["unvalidated_advertised_fee_discount"].as_f64().unwrap()
    );
}
