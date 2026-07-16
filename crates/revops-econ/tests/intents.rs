//! Integration (public-API, black-box) tests for `revops_econ::intents` +
//! `revops_econ::ev`, exercised via the crate's public surface only.
//!
//! Golden values below are transcribed verbatim from the Python conformance
//! corpus and independently re-verified against the live Python reference
//! implementation (`~/bin/cl_revenue_ops-port`, `modules/econ_intents.py`):
//!
//! - `tests/conformance/scenarios/31-duplicate-idempotency-key/case.json`
//! - `tests/conformance/scenarios/37-clock-seed-determinism/case.json`
//! - `tests/conformance/scenarios/34-expired-intent/case.json`
//!
//! All three pin the SAME underlying intent (`_env()` generator defaults):
//! `REBALANCE`, target `"111x222x0"`, amount 400_000 sats (400_000_000
//! msat), snapshot `"snap-1"`, bucket `"rebalance"`, `created_at`
//! `1_752_400_000`, `expires_at` `+600` seconds.

use revops_econ::ev::{benefit_msat_from_sats, confidence_micro, expected_value_msat};
use revops_econ::intents::{
    compute_idempotency_key, from_wire, is_expired, make_intent, to_wire, Explanation, IntentFields,
};
use revops_econ::types::{Micro, Msat, SignedMsat, UnixTime};

/// `idempotency_key` from both corpus files (31 and 37).
const GOLDEN_KEY: &str = "1119af2f103eb34445d30decf78b97f16d11267a8fe66e6aa83b340abdbe33f4";
/// `intent_id` from both corpus files (31 and 37): `"int-" + key[:16]`.
const GOLDEN_INTENT_ID: &str = "int-1119af2f103eb344";

fn golden_fields() -> IntentFields {
    let created_at = UnixTime::new(1_752_400_000).unwrap();
    IntentFields {
        intent_type: "REBALANCE".to_string(),
        snapshot_id: "snap-1".to_string(),
        created_at,
        expires_at: created_at.plus_seconds(600).unwrap(),
        target: "111x222x0".to_string(),
        amount_msat: Some(Msat::new(400_000_000).unwrap()),
        expected_benefit_msat: SignedMsat(0),
        max_cost_msat: Msat::new(3_000_000).unwrap(),
        capital_committed_msat: Msat::new(400_000_000).unwrap(),
        confidence_micro: Micro::new(0).unwrap(),
        reason_codes: vec![],
        explanation: Explanation {
            kind: "conformance".to_string(),
            components: vec![("case".to_string(), serde_json::json!(1))],
        },
        preconditions: vec![],
        priority: 50,
        budget_bucket: "rebalance".to_string(),
        origin_policy: "conformance".to_string(),
        reversible: false,
    }
}

#[test]
fn compute_idempotency_key_matches_corpus_scenarios_31_and_37() {
    let key = compute_idempotency_key(
        "REBALANCE",
        "111x222x0",
        Some(400_000_000),
        "snap-1",
        "rebalance",
    );
    assert_eq!(key, GOLDEN_KEY);
}

#[test]
fn make_intent_matches_corpus_scenarios_31_and_37() {
    let env = make_intent(golden_fields()).unwrap();
    assert_eq!(env.idempotency_key, GOLDEN_KEY);
    assert_eq!(env.intent_id.as_str(), GOLDEN_INTENT_ID);
}

/// Workstream J (corpus s37, "clock-seed-determinism"): identical (fields,
/// clock) -> identical ids, no hidden entropy in the authoritative path.
#[test]
fn make_intent_deterministic_across_independent_builds() {
    let a = make_intent(golden_fields()).unwrap();
    let b = make_intent(golden_fields()).unwrap();
    assert_eq!(a.intent_id, b.intent_id);
    assert_eq!(a.idempotency_key, b.idempotency_key);
}

/// Corpus s34 (`34-expired-intent`): expiry boundary is inclusive at
/// `expires_at`; probes `expires_at - 1, expires_at, expires_at + 1` ->
/// `[false, true, true]`.
#[test]
fn is_expired_inclusive_boundary_matches_corpus_s34() {
    let env = make_intent(golden_fields()).unwrap();
    let probes = [1_752_400_599i64, 1_752_400_600, 1_752_400_601];
    let expected = [false, true, true];
    for (t, want) in probes.iter().zip(expected) {
        assert_eq!(is_expired(&env, UnixTime::new(*t).unwrap()), want);
    }
}

#[test]
fn wire_round_trip_preserves_the_envelope() {
    let env = make_intent(golden_fields()).unwrap();
    let wire = to_wire(&env);
    let round_tripped = from_wire(&wire).unwrap();
    assert_eq!(env, round_tripped);
}

#[test]
fn wire_schema_mismatch_is_rejected() {
    let env = make_intent(golden_fields()).unwrap();
    let mut wire = to_wire(&env);
    wire["schema_version"] = serde_json::json!(99);
    assert!(from_wire(&wire).is_err());
}

/// THE pinned rounding-split divergence (Global Constraints /
/// task-3-brief.md): `confidence_micro` (banker's, `ev.rs`) and
/// `Micro::from_float_clamped` (half-up-by-truncation, `types.rs`) must
/// disagree on a genuine f64 tie. `0.1234565 * 1e6 == 123456.5` exactly
/// (verified `python3 -c "print(repr(0.1234565*1e6))"` => `123456.5`, not
/// IEEE-754 noise near a `.5` boundary). Python's own `round()` (banker's)
/// gives 123456 (verified against `modules/econ_ev.py::confidence_micro`);
/// Python's `int(x*1e6+0.5)` (half-up) gives 123457.
#[test]
fn confidence_micro_and_from_float_clamped_diverge_on_a_pinned_tie() {
    let input = 0.1234565;
    let banker = confidence_micro(Some(input)).value();
    let half_up = Micro::from_float_clamped(input).unwrap().value();
    assert_eq!(
        banker, 123_456,
        "confidence_micro must use banker's rounding"
    );
    assert_eq!(
        half_up, 123_457,
        "Micro::from_float_clamped must use half-up-by-truncation"
    );
    assert_ne!(
        banker, half_up,
        "the two rounding rules must NOT agree on this pinned tie"
    );
}

#[test]
fn expected_value_msat_public_api_subtracts_costs() {
    let ev = expected_value_msat(10_000, 1_000, 2_000, 500).unwrap();
    assert_eq!(ev.0, 6_500);
}

#[test]
fn benefit_msat_from_sats_public_api_scales_and_defaults_conservatively() {
    assert_eq!(
        benefit_msat_from_sats(Some(400_000.0)).unwrap().0,
        400_000_000
    );
    assert_eq!(benefit_msat_from_sats(None).unwrap().0, 0);
    assert_eq!(benefit_msat_from_sats(Some(f64::NAN)).unwrap().0, 0);
}
