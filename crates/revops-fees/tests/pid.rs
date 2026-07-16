//! `PidState::calculate_multiplier` parity, pinned by
//! `fixtures/fees/pid/sequences.json` (generated from the REAL
//! `PIDState.calculate_multiplier` — `fee_controller.py:1977-2020` — by
//! `tools/port/gen_fees_fixtures.py pid` in the port worktree).
//!
//! Each sequence replays several calls against a single persistent
//! `PidState`, pinning `(multiplier, ewma_error, integral_error)` after
//! EVERY step as `py_repr` strings. Sequence 12 is exactly
//! `tests/conformance/scenarios/12-dts-pid-components/case.json`'s PID
//! case (fresh-state, dt=0) — the generator itself cross-checks
//! `round(multiplier, 12) == 1.026338203439`.

use revops_econ::pyfloat::py_repr;
use revops_fees::pid::{calculate_multiplier, PidState};
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fees/pid/sequences.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn parse_f(v: &Value) -> f64 {
    v.as_str().expect("repr string").parse().expect("parse f64")
}

#[test]
fn all_sequences_present() {
    let fx = fixture();
    let sequences = fx["sequences"].as_array().expect("sequences array");
    assert_eq!(sequences.len(), 12, "brief mandates 12 sequences");
}

#[test]
fn sequences_match_python_step_by_step() {
    let fx = fixture();
    let sequences = fx["sequences"].as_array().expect("sequences array");

    for seq in sequences {
        let name = seq["name"].as_str().expect("name");
        let steps = seq["steps"].as_array().expect("steps array");
        let mut state = PidState::default();

        for (i, step) in steps.iter().enumerate() {
            let now = step["now"].as_i64().expect("now");
            let ratio = parse_f(&step["ratio"]);
            let capacity = step["capacity_sats"].as_i64().expect("capacity_sats");
            let flow_state = step["flow_state"].as_str().expect("flow_state");

            let multiplier = calculate_multiplier(&mut state, ratio, capacity, flow_state, now);

            let expected_multiplier = step["multiplier"].as_str().unwrap();
            let expected_ewma = step["ewma_error"].as_str().unwrap();
            let expected_integral = step["integral_error"].as_str().unwrap();

            assert_eq!(
                py_repr(multiplier),
                expected_multiplier,
                "{name} step {i}: multiplier"
            );
            assert_eq!(
                py_repr(state.ewma_error),
                expected_ewma,
                "{name} step {i}: ewma_error"
            );
            assert_eq!(
                py_repr(state.integral_error),
                expected_integral,
                "{name} step {i}: integral_error"
            );
        }
    }
}

#[test]
fn conformance_scenario_12_matches_recorded_case() {
    // tests/conformance/scenarios/12-dts-pid-components/case.json:
    // round(pid_multiplier, 12) == 1.026338203439, round(pid_ewma_error, 12) == 0.09
    let fx = fixture();
    let sequences = fx["sequences"].as_array().unwrap();
    let last = sequences
        .iter()
        .find(|s| s["name"] == "conformance_scenario_12_fresh_state")
        .expect("scenario 12 present");
    let step = &last["steps"].as_array().unwrap()[0];

    let mut state = PidState::default();
    let now = step["now"].as_i64().unwrap();
    let ratio = parse_f(&step["ratio"]);
    let capacity = step["capacity_sats"].as_i64().unwrap();
    let flow_state = step["flow_state"].as_str().unwrap();
    let multiplier = calculate_multiplier(&mut state, ratio, capacity, flow_state, now);

    assert_eq!((multiplier * 1e12).round() / 1e12, 1.026338203439);
    assert_eq!((state.ewma_error * 1e12).round() / 1e12, 0.09);
}

#[test]
fn nan_ratio_is_replaced_by_target_before_error_computation() {
    // From the "nan_ratio_guard" fixture sequence: a NaN
    // current_outbound_ratio must behave exactly as if the ratio equalled
    // the flow state's target ratio (0.5 for "balanced").
    let mut state = PidState::default();
    let with_target = calculate_multiplier(&mut state, 0.5, 500_000_000, "balanced", 1_752_400_000);

    let mut state2 = PidState::default();
    let with_nan = calculate_multiplier(
        &mut state2,
        f64::NAN,
        500_000_000,
        "balanced",
        1_752_400_000,
    );

    assert_eq!(with_nan, with_target);
    assert_eq!(state.ewma_error, state2.ewma_error);
}

#[test]
fn first_call_never_touches_integral_even_with_nonzero_ewma_seed() {
    // dt == 0 on the very first call regardless of last_update_time being
    // freshly defaulted (<=0) — the integral term must stay exactly 0.0.
    let mut state = PidState::default();
    calculate_multiplier(&mut state, 0.9, 1, "source", 1_752_400_000);
    assert_eq!(state.integral_error, 0.0);
}

#[test]
fn multiplier_is_always_clamped_to_half_and_two() {
    let fx = fixture();
    let sequences = fx["sequences"].as_array().unwrap();
    for seq in sequences {
        for step in seq["steps"].as_array().unwrap() {
            let m: f64 = step["multiplier"].as_str().unwrap().parse().unwrap();
            assert!(
                (0.5..=2.0).contains(&m),
                "multiplier {m} out of clamp range"
            );
        }
    }
}
