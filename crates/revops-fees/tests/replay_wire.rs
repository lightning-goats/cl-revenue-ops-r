use revops_core::canonical::canonical_json;
use revops_fees::replay_wire::{
    parse_fee_capture, validate_capture_manifest, FeeCaptureManifestV0, ReplayWireError, WireValue,
    MAX_REPLAY_ENVELOPE_BYTES, REPLAY_SCHEMA_NAME, REPLAY_SCHEMA_VERSION,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const COMPLETE_SKIP: &[u8] = include_bytes!("../../../fixtures/fees/replay/complete_skip.v0.json");

fn fixture_value() -> Value {
    serde_json::from_slice(COMPLETE_SKIP).expect("Python fixture must be JSON")
}

fn reseal(value: &mut Value) {
    let object = value.as_object_mut().expect("fixture must be an object");
    object.remove("payload_sha256");
    let canonical = canonical_json(value).expect("mutated body must be canonical");
    let digest = hex::encode(Sha256::digest(canonical.as_bytes()));
    value
        .as_object_mut()
        .expect("fixture must remain an object")
        .insert("payload_sha256".to_string(), Value::String(digest));
}

fn parse_value(
    value: &Value,
) -> Result<revops_fees::replay_wire::FeeCycleReplayV0, ReplayWireError> {
    parse_fee_capture(
        &serde_json::to_vec(value).expect("test mutation must remain serializable JSON"),
    )
}

fn valid_manifest_value() -> Value {
    json!({
        "schema_name": "fee_cycle_capture_manifest",
        "schema_version": 0,
        "capture_run_id": "run-a",
        "state": "closed",
        "queue_drained": true,
        "started_at": "2026-07-19T00:00:00+00:00",
        "updated_at": "2026-07-19T01:00:00+00:00",
        "attempted": 1,
        "completed": 1,
        "failed": 0,
        "dropped": 0,
        "last_attempted_seq": 1,
        "last_completed_seq": 1,
        "retained_sequence_range": {"first": 1, "last": 1},
        "writer_health": "healthy",
        "last_error_category": null,
        "attempts": [{
            "capture_seq": 1,
            "cycle_id": "run-a:00000001",
            "status": "completed",
            "eligible": true,
            "filename": "run-a-00000001-run-a:00000001.json",
            "bytes": COMPLETE_SKIP.len()
        }]
    })
}

fn manifest_from(value: Value) -> FeeCaptureManifestV0 {
    serde_json::from_value(value).expect("test manifest must match the public wire type")
}

#[test]
fn parses_real_python_sealed_complete_skip_fixture() {
    let capture = parse_fee_capture(COMPLETE_SKIP).expect("real sealed fixture must parse");

    assert_eq!(capture.schema_name, REPLAY_SCHEMA_NAME);
    assert_eq!(capture.schema_version, REPLAY_SCHEMA_VERSION);
    assert_eq!(capture.started_at, "2026-07-19T00:00:00+00:00");
    assert_eq!(
        capture.configuration.get("depletion_threshold"),
        Some(&WireValue::TaggedFloat("0.9500000000000001".to_string()))
    );
}

#[test]
fn rejects_wrong_schema_identity_even_when_resealed() {
    for (field, wrong) in [
        ("schema_name", json!("other")),
        ("schema_version", json!(1)),
    ] {
        let mut value = fixture_value();
        value[field] = wrong;
        reseal(&mut value);

        let error = parse_value(&value).expect_err("wrong schema identity must fail closed");
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[test]
fn rejects_schema_scalar_boundaries_even_when_resealed() {
    for (field, wrong) in [
        ("capture_seq", json!(0)),
        ("capture_run_id", json!("")),
        ("cycle_id", json!("")),
        ("started_at", json!("")),
    ] {
        let mut value = fixture_value();
        value[field] = wrong;
        reseal(&mut value);

        let error = parse_value(&value).expect_err("schema scalar boundary must fail closed");
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[test]
fn rejects_unknown_top_level_field_even_when_resealed() {
    let mut value = fixture_value();
    value["unexpected"] = json!("field");
    reseal(&mut value);

    let error = parse_value(&value).expect_err("unknown top-level field must be rejected");
    assert!(error.to_string().contains("unknown field"), "{error}");
}

#[test]
fn rejects_oversize_bytes_before_json_or_replay_construction() {
    let bytes = vec![b'!'; MAX_REPLAY_ENVELOPE_BYTES + 1];

    let error = parse_fee_capture(&bytes).expect_err("oversize input must fail");
    assert!(matches!(error, ReplayWireError::EnvelopeTooLarge { .. }));
}

#[test]
fn rejects_missing_invalid_and_tampered_payload_digest() {
    let mut missing = fixture_value();
    missing
        .as_object_mut()
        .expect("fixture object")
        .remove("payload_sha256");
    assert!(parse_value(&missing)
        .expect_err("missing digest must fail")
        .to_string()
        .contains("payload_sha256"));

    let mut invalid = fixture_value();
    invalid["payload_sha256"] = json!("not-a-sha256");
    assert!(parse_value(&invalid)
        .expect_err("invalid digest must fail")
        .to_string()
        .contains("digest"));

    let mut tampered = fixture_value();
    tampered["cycle_id"] = json!("tampered");
    assert!(parse_value(&tampered)
        .expect_err("tampering must fail")
        .to_string()
        .contains("digest"));
}

#[test]
fn rejects_untagged_json_float_before_digest_verification() {
    let mut value = fixture_value();
    value["configuration"]["depletion_threshold"] = json!(0.5);

    let error = parse_value(&value).expect_err("untagged float must fail closed");
    assert!(error.to_string().contains("untagged JSON float"), "{error}");
}

#[test]
fn rejects_non_finite_noncanonical_and_extra_key_tagged_floats() {
    for rendered in ["nan", "inf", "-inf", "NaN", "1"] {
        let mut value = fixture_value();
        value["configuration"]["depletion_threshold"] = json!({"__f__": rendered});
        let error = parse_value(&value).expect_err("invalid tagged float must fail closed");
        assert!(error.to_string().contains("tagged float"), "{error}");
    }

    let mut extra = fixture_value();
    extra["configuration"]["depletion_threshold"] = json!({"__f__": "0.5", "unexpected": true});
    let error = parse_value(&extra).expect_err("tagged-float extra keys must fail closed");
    assert!(error.to_string().contains("tagged float"), "{error}");
}

#[test]
fn rejects_incomplete_capture_even_when_resealed() {
    let mut value = fixture_value();
    value["completeness"]["complete"] = json!(false);
    reseal(&mut value);

    let error = parse_value(&value).expect_err("incomplete capture must fail closed");
    assert!(error.to_string().contains("complete"), "{error}");
}

#[test]
fn rejects_evaluated_and_terminal_count_mismatches_even_when_resealed() {
    for field in ["evaluated_channels", "terminal_outcomes"] {
        let mut value = fixture_value();
        value["completeness"][field] = json!(2);
        reseal(&mut value);

        let error = parse_value(&value).expect_err("count mismatch must fail closed");
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[test]
fn validates_closed_drained_zero_loss_manifest() {
    let capture = parse_fee_capture(COMPLETE_SKIP).expect("fixture must parse");
    let manifest = manifest_from(valid_manifest_value());

    validate_capture_manifest(&manifest, &[capture]).expect("valid manifest must pass");
}

#[test]
fn accepts_lossless_manifest_with_degraded_health_metadata() {
    let capture = parse_fee_capture(COMPLETE_SKIP).expect("fixture must parse");

    for (writer_health, last_error_category) in [
        ("degraded", Value::Null),
        ("healthy", json!("OSError")),
        ("degraded", json!("OSError")),
    ] {
        let mut value = valid_manifest_value();
        value["writer_health"] = json!(writer_health);
        value["last_error_category"] = last_error_category;
        let manifest = manifest_from(value);

        validate_capture_manifest(&manifest, std::slice::from_ref(&capture))
            .expect("health metadata must not override zero-loss manifest invariants");
    }
}

#[test]
fn rejects_manifest_that_is_not_closed_drained_and_lossless() {
    let capture = parse_fee_capture(COMPLETE_SKIP).expect("fixture must parse");

    for (field, wrong) in [
        ("state", json!("active")),
        ("queue_drained", json!(false)),
        ("failed", json!(1)),
        ("dropped", json!(1)),
    ] {
        let mut value = valid_manifest_value();
        value[field] = wrong;
        let manifest = manifest_from(value);
        let error = validate_capture_manifest(&manifest, std::slice::from_ref(&capture))
            .expect_err("unsafe manifest must fail closed");
        assert!(error.to_string().contains(field), "{error}");
    }
}

#[test]
fn rejects_manifest_capture_from_another_run() {
    let mut capture = parse_fee_capture(COMPLETE_SKIP).expect("fixture must parse");
    capture.capture_run_id = "run-b".to_string();
    let manifest = manifest_from(valid_manifest_value());

    let error = validate_capture_manifest(&manifest, &[capture])
        .expect_err("mixed run IDs must fail closed");
    assert!(error.to_string().contains("capture_run_id"), "{error}");
}

#[test]
fn rejects_nonconsecutive_manifest_capture_sequence() {
    let first = parse_fee_capture(COMPLETE_SKIP).expect("fixture must parse");
    let mut third = first.clone();
    third.capture_seq = 3;
    third.cycle_id = "run-a:00000003".to_string();
    let mut value = valid_manifest_value();
    value["attempted"] = json!(3);
    value["completed"] = json!(3);
    value["last_attempted_seq"] = json!(3);
    value["last_completed_seq"] = json!(3);
    value["retained_sequence_range"] = json!({"first": 1, "last": 3});
    value["attempts"] = json!([
        {
            "capture_seq": 1,
            "cycle_id": "run-a:00000001",
            "status": "completed",
            "eligible": true,
            "filename": "run-a-00000001-run-a:00000001.json",
            "bytes": COMPLETE_SKIP.len()
        },
        {
            "capture_seq": 3,
            "cycle_id": "run-a:00000003",
            "status": "completed",
            "eligible": true,
            "filename": "run-a-00000003-run-a:00000003.json",
            "bytes": COMPLETE_SKIP.len()
        }
    ]);
    let manifest = manifest_from(value);

    let error = validate_capture_manifest(&manifest, &[first, third])
        .expect_err("sequence gaps must fail closed");
    assert!(error.to_string().contains("consecutive"), "{error}");
}
