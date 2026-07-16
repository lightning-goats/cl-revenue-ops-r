//! `GaussianThompsonState::to_dict`/`from_dict` lossless round-trip parity,
//! pinned by `fixtures/fees/state_dict/cases.json` (generated from the
//! REAL `GaussianThompsonState.to_dict`/`from_dict` —
//! `fee_controller.py:1721-1940` — by `tools/port/gen_fees_fixtures.py
//! state_dict` in the port worktree).
//!
//! Every case pins `dumps_python(gts_to_dict(gts_from_dict(parse(blob))))`
//! byte-identical against a Python-computed `expected` string. For cases
//! involving an unknown top-level key that Python's own `to_dict()` drops,
//! `expected` is the CONTRACT truth (Python's known-fields output with the
//! unknown key manually re-appended by the generator, matching this port's
//! own choice to place unknown keys after all known keys) — see that
//! generator's `_state_dict_case_with_unknown` for the exact rule.

use revops_econ::pyfloat::py_repr;
use revops_fees::pyjson::{dumps_python, parse};
use revops_fees::thompson::serde::{gts_from_dict, gts_to_dict};
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fees/state_dict/cases.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn case<'a>(fx: &'a Value, name: &str) -> &'a Value {
    fx["cases"]
        .as_array()
        .expect("cases array")
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("case {name} not found"))
}

/// The core round-trip assertion (test contract (a)): parse the blob,
/// from_dict, to_dict, dumps_python — byte-identical to Python's own
/// `json.dumps(from_dict(d).to_dict())` (or the unknown-key contract truth
/// for cases where that applies).
fn assert_round_trip(c: &Value) {
    let name = c["name"].as_str().unwrap();
    let blob = c["blob"].as_str().expect("blob is a JSON string");
    let expected = c["expected"].as_str().expect("expected is a JSON string");

    let parsed = parse(blob).unwrap_or_else(|e| panic!("{name}: parse blob: {e}"));
    let state = gts_from_dict(&parsed);
    let out = dumps_python(&gts_to_dict(&state));

    assert_eq!(out, expected, "{name}: round trip byte mismatch");
}

#[test]
fn all_cases_round_trip_byte_identical() {
    let fx = fixture();
    for c in fx["cases"].as_array().unwrap() {
        assert_round_trip(c);
    }
}

#[test]
fn current_format_blob_round_trips() {
    let fx = fixture();
    assert_round_trip(case(&fx, "current_format_roundtrip"));
}

#[test]
fn legacy_weight_rescale_matches_pinned_reprs() {
    let fx = fixture();
    let c = case(&fx, "legacy_weight_rescale_absent_marker");
    assert_round_trip(c);

    let blob = c["blob"].as_str().unwrap();
    let expected_weights: Vec<&str> = c["rescaled_weights"]
        .as_array()
        .expect("rescaled_weights array")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();

    let parsed = parse(blob).unwrap();
    let state = gts_from_dict(&parsed);
    let actual_weights: Vec<String> = state
        .observations
        .iter()
        .map(|o| py_repr(o.weight))
        .collect();

    assert_eq!(actual_weights, expected_weights);
}

#[test]
fn legacy_weight_rescale_stale_marker_also_triggers() {
    let fx = fixture();
    assert_round_trip(case(&fx, "legacy_weight_rescale_stale_marker"));
}

#[test]
fn legacy_ctx_3tuple_converts_alongside_current_4tuple() {
    let fx = fixture();
    let c = case(&fx, "legacy_ctx_3tuple_alongside_current_4tuple");
    assert_round_trip(c);

    let blob = c["blob"].as_str().unwrap();
    let parsed = parse(blob).unwrap();
    let state = gts_from_dict(&parsed);

    let expected = c["converted_ctx"]
        .as_object()
        .expect("converted_ctx object");
    assert_eq!(state.contextual_posteriors.len(), expected.len());
    for (key, ctx) in &state.contextual_posteriors {
        let exp = expected
            .get(key)
            .unwrap_or_else(|| panic!("missing key {key}"));
        let exp = exp.as_array().unwrap();
        assert_eq!(py_repr(ctx.mean), exp[0].as_str().unwrap(), "{key} mean");
        assert_eq!(
            py_repr(ctx.precision),
            exp[1].as_str().unwrap(),
            "{key} precision"
        );
        assert_eq!(ctx.count, exp[2].as_i64().unwrap(), "{key} count");
        assert_eq!(
            ctx.last_update,
            exp[3].as_i64().unwrap(),
            "{key} last_update"
        );
    }

    // The 3-tuple entry converted; the already-4-tuple entry passed
    // through as-is.
    let three_tuple = state
        .contextual_posteriors
        .iter()
        .find(|(k, _)| k == "low:normal:P")
        .unwrap();
    assert!(three_tuple.1.was_legacy_3tuple);
    let four_tuple = state
        .contextual_posteriors
        .iter()
        .find(|(k, _)| k == "mid:peak:S")
        .unwrap();
    assert!(!four_tuple.1.was_legacy_3tuple);
}

#[test]
fn unknown_top_level_key_survives_round_trip() {
    let fx = fixture();
    let c = case(&fx, "unknown_top_level_key_survives_roundtrip");
    assert_round_trip(c);

    let blob = c["blob"].as_str().unwrap();
    let parsed = parse(blob).unwrap();
    let state = gts_from_dict(&parsed);

    assert_eq!(state.extra.len(), 1);
    assert_eq!(state.extra[0].0, "future_field");
}

#[test]
fn six_tuple_congestion_and_zero_probe_flags_preserved() {
    let fx = fixture();
    let c = case(&fx, "six_tuple_congestion_and_zero_probe");
    assert_round_trip(c);

    let blob = c["blob"].as_str().unwrap();
    let parsed = parse(blob).unwrap();
    let state = gts_from_dict(&parsed);

    assert_eq!(state.observations.len(), 2);
    assert_eq!(state.observations[0].flag.as_deref(), Some("congestion"));
    assert_eq!(state.observations[1].flag.as_deref(), Some("zero_probe"));
}

#[test]
fn non_ascii_unknown_value_escapes_correctly() {
    let fx = fixture();
    assert_round_trip(case(&fx, "non_ascii_unknown_value"));
}

/// A legacy thompson_aimd_v1-era blob can carry an observation fee as a
/// JSON float (`250.0`); `from_dict`/`to_dict` never cast observation[0]
/// (same non-cast contract as `prior_mean_fee`/`prior_std_fee`), so a
/// float-typed fee must re-emit as `250.0` and an int-typed fee in the same
/// list must independently re-emit as `250` — byte-identical to Python.
#[test]
fn observation_fee_int_float_typing_preserved_independently() {
    let fx = fixture();
    let c = case(&fx, "float_and_int_observation_fee_roundtrip");
    assert_round_trip(c);

    let blob = c["blob"].as_str().unwrap();
    let parsed = parse(blob).unwrap();
    let state = gts_from_dict(&parsed);

    assert_eq!(state.observations.len(), 2);
    assert!(
        !state.observations[0].fee_is_int,
        "first fee was JSON float"
    );
    assert!(state.observations[1].fee_is_int, "second fee was JSON int");
    assert_eq!(state.observations[0].fee, 250.0);
    assert_eq!(state.observations[1].fee, 250.0);
}
