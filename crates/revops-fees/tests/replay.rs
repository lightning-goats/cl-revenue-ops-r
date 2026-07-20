use revops_core::canonical::canonical_json;
use revops_fees::replay::{decode_tagged_float, replay_fee_capture};
use revops_fees::replay_wire::{parse_fee_capture, FeeCycleReplayV0, WireValue};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const COMPLETE_ADJUSTMENT: &[u8] =
    include_bytes!("../../../fixtures/fees/replay/complete_adjustment.v0.json");
const COMPLETE_SKIP: &[u8] = include_bytes!("../../../fixtures/fees/replay/complete_skip.v0.json");

fn fixture_value() -> Value {
    serde_json::from_slice(COMPLETE_ADJUSTMENT).expect("Python fixture is JSON")
}

fn reseal(value: &mut Value) {
    value
        .as_object_mut()
        .expect("fixture object")
        .remove("payload_sha256");
    let canonical = canonical_json(value).expect("canonical mutation");
    value["payload_sha256"] = json!(hex::encode(Sha256::digest(canonical.as_bytes())));
}

fn parse_mutated(mut value: Value) -> FeeCycleReplayV0 {
    reseal(&mut value);
    parse_fee_capture(&serde_json::to_vec(&value).expect("fixture bytes"))
        .expect("mutated fixture remains a valid envelope")
}

fn assert_path_error(capture: &FeeCycleReplayV0, family: &str, path: &str) -> String {
    let error = replay_fee_capture(capture).expect_err("corruption must fail closed");
    let rendered = error.to_string();
    for needle in [family, "expected ordinal", "expected", "actual", path] {
        assert!(
            rendered.contains(needle),
            "{needle:?} missing from {rendered}"
        );
    }
    rendered
}

#[test]
fn strict_complete_adjustment_replays_exactly() {
    let capture = parse_fee_capture(COMPLETE_ADJUSTMENT).expect("sealed Python fixture");
    let replay = replay_fee_capture(&capture).expect("strict offline replay");

    assert_eq!(
        replay.ordered_outcomes,
        capture.expected["ordered_outcomes"]
    );
    assert_eq!(
        replay.post_channel_state,
        capture.expected["post_channel_state"]
    );
    let WireValue::Object(post_global) = &capture.expected["post_global"] else {
        panic!("post_global object");
    };
    assert_eq!(
        replay.decision_summary,
        post_global
            .get("decision_summary")
            .expect("summary")
            .clone()
    );
    assert_eq!(replay.execution, capture.observations["execution"]);
    assert_eq!(replay.consumed.evidence, 8);
    assert_eq!(replay.consumed.clock, 5);
    assert_eq!(replay.consumed.entropy, 1);
    assert_eq!(replay.consumed.governor, 1);
    assert_eq!(replay.consumed.execution, 1);
    assert_eq!(replay.consumed.state_flush, 1);
    assert_eq!(replay.decisions.len(), 1);
    let WireValue::Object(decision) = &replay.decisions[0] else {
        panic!("decision object");
    };
    assert_eq!(
        decision.get("trace"),
        Some(&WireValue::Object(
            [
                (
                    "disposition".to_string(),
                    WireValue::String("policy_static".to_string())
                ),
                ("would_broadcast".to_string(), WireValue::Bool(true)),
            ]
            .into_iter()
            .collect()
        ))
    );
}

#[test]
fn strict_complete_passive_skip_replays_exactly() {
    let capture = parse_fee_capture(COMPLETE_SKIP).expect("sealed Python skip fixture");
    let replay = replay_fee_capture(&capture).expect("strict offline skip replay");

    assert_eq!(
        replay.ordered_outcomes,
        capture.expected["ordered_outcomes"]
    );
    assert_eq!(
        replay.post_channel_state,
        capture.expected["post_channel_state"]
    );
    assert_eq!(replay.consumed.evidence, 7);
    assert_eq!(replay.consumed.clock, 2);
    assert_eq!(replay.consumed.entropy, 1);
    assert_eq!(replay.consumed.governor, 0);
    assert_eq!(replay.consumed.execution, 0);
    assert_eq!(replay.consumed.state_flush, 1);
    assert_eq!(replay.decisions.len(), 1);
}

#[test]
fn strict_clock_rejects_label_reorder_and_missing_entry_with_path() {
    let mut wrong = fixture_value();
    wrong["observations"]["clock"][0]["label"] = json!("vegas.update");
    let capture = parse_mutated(wrong);
    let rendered = assert_path_error(&capture, "clock", "$.observations.clock[0]");
    assert!(rendered.contains("cycle.started_at"));
    assert!(rendered.contains("vegas.update"));

    let mut missing = fixture_value();
    missing["observations"]["clock"]
        .as_array_mut()
        .expect("clock array")
        .remove(4);
    missing["completeness"]["clock_entries"] = json!(4);
    let capture = parse_mutated(missing);
    assert_path_error(&capture, "clock", "$.observations.clock[4]");
}

#[test]
fn strict_evidence_rejects_wrong_arguments_ordinal_duplicate_and_error_entries() {
    let mut wrong_args = fixture_value();
    wrong_args["observations"]["evidence"][6]["args"] = json!(["other"]);
    let capture = parse_mutated(wrong_args);
    assert_path_error(&capture, "evidence", "$.observations.evidence[6].args");

    let mut duplicate = fixture_value();
    duplicate["observations"]["evidence"][1]["ordinal"] = json!(0);
    let capture = parse_mutated(duplicate);
    assert_path_error(&capture, "evidence", "$.observations.evidence[1].ordinal");

    let mut fallback = fixture_value();
    let entry = fallback["observations"]["evidence"][4]
        .as_object_mut()
        .expect("evidence object");
    entry.remove("result");
    entry.insert(
        "error".to_string(),
        json!({"category": "RuntimeError", "message": "db down"}),
    );
    let capture = parse_mutated(fallback);
    assert_path_error(&capture, "evidence", "$.observations.evidence[4].error");
}

#[test]
fn strict_entropy_rejects_op_label_args_and_reconstructs_exact_bits() {
    assert_eq!(
        decode_tagged_float(
            &WireValue::TaggedFloat("0.32383276483316237".to_string()),
            "$.observations.entropy[0].result"
        )
        .expect("tagged float")
        .to_bits(),
        0.32383276483316237_f64.to_bits()
    );

    for (field, value, path) in [
        ("op", json!("gauss"), "$.observations.entropy[0].op"),
        ("label", json!("other"), "$.observations.entropy[0].label"),
        ("args", json!([0, 1]), "$.observations.entropy[0].args"),
    ] {
        let mut wrong = fixture_value();
        wrong["observations"]["entropy"][0][field] = value;
        let capture = parse_mutated(wrong);
        assert_path_error(&capture, "entropy", path);
    }
}

#[test]
fn strict_governor_rejects_request_and_result_drift() {
    let mut wrong_request = fixture_value();
    wrong_request["observations"]["governor"][0]["request"]["fee_ppm"] = json!(251);
    let capture = parse_mutated(wrong_request);
    assert_path_error(&capture, "governor", "$.observations.governor[0].request");

    let mut wrong_result = fixture_value();
    wrong_result["observations"]["governor"][0]["result"]["authorized"] = json!(false);
    let capture = parse_mutated(wrong_result);
    let error = replay_fee_capture(&capture).expect_err("authorization changes outcome");
    assert!(
        error.to_string().contains("$.expected.ordered_outcomes"),
        "{error}"
    );
}

#[test]
fn strict_execution_result_drives_terminal_outcome_and_request_is_exact() {
    let mut wrong_request = fixture_value();
    wrong_request["observations"]["execution"][0]["request"]["reason"] = json!("other");
    let capture = parse_mutated(wrong_request);
    assert_path_error(&capture, "execution", "$.observations.execution[0].request");

    let mut failed = fixture_value();
    failed["observations"]["execution"][0]["result"]["success"] = json!(false);
    let capture = parse_mutated(failed);
    let error = replay_fee_capture(&capture).expect_err("execution failure changes outcome");
    assert!(
        error.to_string().contains("$.expected.ordered_outcomes"),
        "{error}"
    );
}

#[test]
fn strict_rejects_unconsumed_extra_transcript_entries() {
    for family in ["clock", "entropy", "evidence", "governor", "execution"] {
        let mut wrong = fixture_value();
        let new_len = {
            let entries = wrong["observations"][family]
                .as_array_mut()
                .expect("family array");
            let mut extra = entries.last().expect("nonempty family").clone();
            extra["ordinal"] = json!(entries.len());
            entries.push(extra);
            entries.len()
        };
        match family {
            "clock" => wrong["completeness"]["clock_entries"] = json!(new_len),
            "entropy" => wrong["completeness"]["entropy_entries"] = json!(new_len),
            "evidence" => wrong["completeness"]["evidence_entries"] = json!(new_len),
            _ => {}
        }
        let capture = parse_mutated(wrong);
        assert_path_error(
            &capture,
            family,
            &format!("$.observations.{family}[{}]", new_len - 1),
        );
    }
}

#[test]
fn pre_state_rejects_unknown_missing_duplicate_and_order_drift() {
    let mut unknown = fixture_value();
    unknown["pre_state"]["global"]["unexpected"] = json!(true);
    let error = replay_fee_capture(&parse_mutated(unknown)).expect_err("unknown state");
    assert!(error.to_string().contains("$.pre_state.global.unexpected"));

    let mut missing = fixture_value();
    missing["pre_state"]["ordered_channels"][0]["cycle_state"]
        .as_object_mut()
        .expect("cycle state")
        .remove("last_fee_ppm");
    let error = replay_fee_capture(&parse_mutated(missing)).expect_err("missing state");
    assert!(error
        .to_string()
        .contains("$.pre_state.ordered_channels[0].cycle_state.last_fee_ppm"));

    let mut duplicate = fixture_value();
    let channel = duplicate["pre_state"]["ordered_channels"][0].clone();
    duplicate["pre_state"]["ordered_channels"]
        .as_array_mut()
        .expect("channels")
        .push(channel);
    let outcome = duplicate["expected"]["ordered_outcomes"][0].clone();
    duplicate["expected"]["ordered_outcomes"]
        .as_array_mut()
        .expect("outcomes")
        .push(outcome);
    duplicate["completeness"]["evaluated_channels"] = json!(2);
    duplicate["completeness"]["terminal_outcomes"] = json!(2);
    let error = replay_fee_capture(&parse_mutated(duplicate)).expect_err("duplicate channel");
    assert!(error.to_string().contains("duplicate channel_id"));

    let mut order = fixture_value();
    order["pre_state"]["ordered_channels"][0]["channel_state"]["channel_id"] = json!("999x1x0");
    let error = replay_fee_capture(&parse_mutated(order)).expect_err("channel order mismatch");
    assert!(error
        .to_string()
        .contains("$.pre_state.ordered_channels[0]"));
}

#[test]
fn malformed_capture_returns_error_without_panicking() {
    let mut malformed = parse_fee_capture(COMPLETE_ADJUSTMENT).expect("fixture");
    malformed
        .pre_state
        .remove("global")
        .expect("fixture has global");
    let error = replay_fee_capture(&malformed).expect_err("missing global");
    assert!(error.to_string().contains("$.pre_state.global"));
}

#[test]
fn replay_module_has_no_production_io_dependencies() {
    let source = include_str!("../src/replay.rs");
    for forbidden in [
        "cln_rpc",
        "rusqlite",
        "Journal",
        "EconLedger",
        "std::net",
        "std::process",
        "Command::",
    ] {
        assert!(!source.contains(forbidden), "{forbidden} in replay.rs");
    }
}
