use revops_core::canonical::canonical_json;
use revops_fees::replay_wire::{
    parse_fee_capture, validate_capture_manifest, FeeCaptureManifestV0, ReplayWireError, WireValue,
    MAX_REPLAY_ENVELOPE_BYTES, REPLAY_SCHEMA_NAME, REPLAY_SCHEMA_VERSION,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const COMPLETE_ADJUSTMENT: &[u8] =
    include_bytes!("../../../fixtures/fees/replay/complete_adjustment.v0.json");
const COMPLETE_SKIP: &[u8] = include_bytes!("../../../fixtures/fees/replay/complete_skip.v0.json");
const EFFECTIVE_FALLBACK: &[u8] =
    include_bytes!("../../../fixtures/fees/replay/effective_fallback.v0.json");
const FAILURE_RECOVERY: &[u8] =
    include_bytes!("../../../fixtures/fees/replay/failure_recovery.v0.json");

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
    let capture = fixture_value();
    let capture_run_id = capture["capture_run_id"].as_str().expect("fixture run id");
    let cycle_id = capture["cycle_id"].as_str().expect("fixture cycle id");
    let started_at = capture["started_at"].as_str().expect("fixture start");
    json!({
        "schema_name": "fee_cycle_capture_manifest",
        "schema_version": 0,
        "capture_run_id": capture_run_id,
        "state": "closed",
        "queue_drained": true,
        "started_at": started_at,
        "updated_at": started_at,
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
            "cycle_id": cycle_id,
            "status": "completed",
            "eligible": true,
            "filename": format!("{capture_run_id}-00000001-{cycle_id}.json"),
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
    assert_eq!(
        capture.producer.get("started_at"),
        Some(&WireValue::String(capture.started_at.clone()))
    );
    assert_eq!(
        capture.configuration.get("capex_exploration_rate"),
        Some(&WireValue::TaggedFloat("0.1".to_string()))
    );
}

#[test]
fn parses_u64_max_from_live_cln_evidence_without_losing_integer_identity() {
    let mut value = fixture_value();
    value["observations"]["evidence"][0]["result"]["123x456x0"]
        ["our_max_htlc_value_in_flight_msat"] = json!(u64::MAX);
    reseal(&mut value);

    let capture = parse_value(&value).expect("live CLN u64 evidence must parse");
    let evidence = match capture.observations.get("evidence") {
        Some(WireValue::Array(entries)) => entries,
        other => panic!("evidence must be an array, got {other:?}"),
    };
    let result = match &evidence[0] {
        WireValue::Object(entry) => entry.get("result").expect("evidence result"),
        other => panic!("evidence entry must be an object, got {other:?}"),
    };
    let channel = match result {
        WireValue::Object(result) => result.get("123x456x0").expect("channel row"),
        other => panic!("evidence result must be an object, got {other:?}"),
    };
    let value = match channel {
        WireValue::Object(channel) => channel
            .get("our_max_htlc_value_in_flight_msat")
            .expect("u64 field"),
        other => panic!("channel row must be an object, got {other:?}"),
    };
    assert_eq!(value, &WireValue::Unsigned(u64::MAX));
}

#[test]
fn all_replay_fixtures_are_exact_python_writer_artifact_bytes() {
    for (name, bytes, writer_sha256) in [
        (
            "complete_adjustment.v0.json",
            COMPLETE_ADJUSTMENT,
            "52fbd5e3b7c44bb361fea1c700e9ca9c6ede375413c64a62eac58fa61955f75c",
        ),
        (
            "complete_skip.v0.json",
            COMPLETE_SKIP,
            "e3a119958c037bce18056ad42bfd8582e93cb4ed51b1a72a2bcb155c1c1e46f4",
        ),
        (
            "effective_fallback.v0.json",
            EFFECTIVE_FALLBACK,
            "ba9bd50d3bf8acb504a9b8e72a965b63ede847adc759df06fff6b105673824dc",
        ),
        (
            "failure_recovery.v0.json",
            FAILURE_RECOVERY,
            "c68e195387a981c0f1aeeda7af4ade98d3e49ede633f939e95c703c074f17555",
        ),
    ] {
        let value: Value =
            serde_json::from_slice(bytes).unwrap_or_else(|error| panic!("{name}: {error}"));
        let canonical = canonical_json(&value)
            .unwrap_or_else(|error| panic!("{name}: canonical encoding failed: {error}"));
        assert_eq!(
            bytes,
            canonical.as_bytes(),
            "{name} must exactly match Python production _json_bytes output"
        );
        assert_eq!(
            hex::encode(Sha256::digest(bytes)),
            writer_sha256,
            "{name} must exactly match the committed Python writer artifact"
        );
        assert_eq!(
            value["producer"]["python_commit"], "76c352885f2d23569e76da38b13e2d6111e64799",
            "{name} must identify the committed Python producer"
        );
    }
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
    value["configuration"]["capex_exploration_rate"] = json!(0.5);

    let error = parse_value(&value).expect_err("untagged float must fail closed");
    assert!(error.to_string().contains("untagged JSON float"), "{error}");
}

#[test]
fn rejects_non_finite_noncanonical_and_extra_key_tagged_floats() {
    for rendered in ["nan", "inf", "-inf", "NaN", "1"] {
        let mut value = fixture_value();
        value["configuration"]["capex_exploration_rate"] = json!({"__f__": rendered});
        let error = parse_value(&value).expect_err("invalid tagged float must fail closed");
        assert!(error.to_string().contains("tagged float"), "{error}");
    }

    let mut extra = fixture_value();
    extra["configuration"]["capex_exploration_rate"] = json!({"__f__": "0.5", "unexpected": true});
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
fn rejects_evaluated_terminal_and_trace_count_mismatches_even_when_resealed() {
    for field in [
        "evaluated_channels",
        "terminal_outcomes",
        "decision_trace_entries",
    ] {
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
    capture.capture_run_id = "other-run".to_string();
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
    third.cycle_id = format!("{}:00000003", first.capture_run_id);
    let capture_run_id = first.capture_run_id.clone();
    let first_cycle_id = first.cycle_id.clone();
    let third_cycle_id = third.cycle_id.clone();
    let mut value = valid_manifest_value();
    value["attempted"] = json!(3);
    value["completed"] = json!(3);
    value["last_attempted_seq"] = json!(3);
    value["last_completed_seq"] = json!(3);
    value["retained_sequence_range"] = json!({"first": 1, "last": 3});
    value["attempts"] = json!([
        {
            "capture_seq": 1,
            "cycle_id": first_cycle_id,
            "status": "completed",
            "eligible": true,
            "filename": format!("{capture_run_id}-00000001-{}.json", first.cycle_id),
            "bytes": COMPLETE_SKIP.len()
        },
        {
            "capture_seq": 3,
            "cycle_id": third_cycle_id,
            "status": "completed",
            "eligible": true,
            "filename": format!("{capture_run_id}-00000003-{}.json", third.cycle_id),
            "bytes": COMPLETE_SKIP.len()
        }
    ]);
    let manifest = manifest_from(value);

    let error = validate_capture_manifest(&manifest, &[first, third])
        .expect_err("sequence gaps must fail closed");
    assert!(error.to_string().contains("consecutive"), "{error}");
}
