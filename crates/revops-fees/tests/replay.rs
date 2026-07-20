use revops_core::canonical::canonical_json;
use revops_fees::replay::{decode_tagged_float, replay_fee_capture, ReplayError};
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
    assert_eq!(replay.governor, capture.observations["governor"]);
    assert_eq!(
        replay.state_flush, capture.expected["post_channel_state"],
        "the captured MemoryStateSink rows are the exact persisted output"
    );
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
fn enabled_node_drain_reports_kernel_derived_ratio_pressure_and_effective_cap() {
    let mut enabled = fixture_value();
    enabled["configuration"]["node_drain_bias_enabled"] = json!(true);
    enabled["configuration"]["drain_fee_discount_max"] = json!({"__f__": "0.3"});

    let evidence = enabled["observations"]["evidence"]
        .as_array_mut()
        .expect("evidence transcript");
    evidence.insert(
        6,
        json!({
            "ordinal": 6,
            "op": "node_channels",
            "args": [],
            "result": [
                {
                    "state": "CHANNELD_NORMAL",
                    "to_us_msat": 750,
                    "total_msat": 1000
                },
                {
                    "state": "CHANNELD_NORMAL",
                    "to_us_msat": 150,
                    "total_msat": 1000
                },
                {
                    "state": "ONCHAIN",
                    "to_us_msat": 1000,
                    "total_msat": 1000
                }
            ]
        }),
    );
    for (ordinal, entry) in evidence.iter_mut().enumerate().skip(7) {
        entry["ordinal"] = json!(ordinal);
    }
    enabled["completeness"]["evidence_entries"] = json!(9);
    enabled["expected"]["post_global"]["drain_values"] = json!({
        "node_receivable_ratio": {"__f__": "0.55"},
        "node_drain_pressure": {"__f__": "0.0"},
        "effective_drain_discount_max": {"__f__": "0.3"}
    });

    let capture = parse_mutated(enabled);
    let replay = replay_fee_capture(&capture).expect("enabled node drain replay");
    let WireValue::Object(post_global) = replay.post_global else {
        panic!("post_global object");
    };
    let WireValue::Object(expected_post_global) = &capture.expected["post_global"] else {
        panic!("expected post_global object");
    };
    assert_eq!(
        post_global.get("drain_values"),
        expected_post_global.get("drain_values")
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
    assert_eq!(replay.governor, capture.observations["governor"]);
    let WireValue::Object(skip_decision) = &replay.decisions[0] else {
        panic!("skip decision object");
    };
    assert_eq!(skip_decision.get("governed"), Some(&WireValue::Null));
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
fn strict_governor_result_rejects_partial_and_extra_shapes() {
    let mut missing = fixture_value();
    missing["observations"]["governor"][0]["result"]
        .as_object_mut()
        .expect("governor result")
        .remove("reason");
    assert_path_error(
        &parse_mutated(missing),
        "governor",
        "$.observations.governor[0].result.reason",
    );

    let mut extra = fixture_value();
    extra["observations"]["governor"][0]["result"]["surprise"] = json!(true);
    assert_path_error(
        &parse_mutated(extra),
        "governor",
        "$.observations.governor[0].result.surprise",
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
fn strict_observation_family_set_rejects_unknown_and_missing_families() {
    let mut wrong = fixture_value();
    wrong["observations"]["surprise"] = json!([]);
    let error = replay_fee_capture(&parse_mutated(wrong))
        .expect_err("unknown observation family must fail closed");
    assert!(
        error.to_string().contains("$.observations.surprise"),
        "{error}"
    );

    for family in ["evidence", "clock", "entropy", "governor", "execution"] {
        let mut missing = parse_fee_capture(COMPLETE_ADJUSTMENT).expect("fixture");
        missing.observations.remove(family);
        let error = replay_fee_capture(&missing).expect_err("missing family must fail closed");
        assert!(
            error
                .to_string()
                .contains(&format!("$.observations.{family}")),
            "{family}: {error}"
        );
    }
}

#[test]
fn strict_clock_and_entropy_reject_error_metadata_even_with_valid_values() {
    for (family, field, path) in [
        ("clock", "error", "$.observations.clock[0].error"),
        (
            "clock",
            "fallback_error",
            "$.observations.clock[0].fallback_error",
        ),
        ("entropy", "error", "$.observations.entropy[0].error"),
        (
            "entropy",
            "fallback_error",
            "$.observations.entropy[0].fallback_error",
        ),
    ] {
        let mut wrong = fixture_value();
        wrong["observations"][family][0][field] =
            json!({"category": "RuntimeError", "message": "fallback used"});
        let capture = parse_mutated(wrong);
        assert_path_error(&capture, family, path);
    }
}

#[test]
fn strict_execution_result_validates_full_shape_and_values() {
    for (field, replacement, path) in [
        (
            "channel_id",
            json!("other-channel"),
            "$.observations.execution[0].result.channel_id",
        ),
        (
            "old_fee_ppm",
            json!(999),
            "$.observations.execution[0].result.old_fee_ppm",
        ),
        (
            "base_fee_msat",
            json!(999),
            "$.observations.execution[0].result.base_fee_msat",
        ),
    ] {
        let mut wrong = fixture_value();
        wrong["observations"]["execution"][0]["result"][field] = replacement;
        let capture = parse_mutated(wrong);
        assert_path_error(&capture, "execution", path);
    }

    let mut extra = fixture_value();
    extra["observations"]["execution"][0]["result"]["surprise"] = json!(true);
    assert_path_error(
        &parse_mutated(extra),
        "execution",
        "$.observations.execution[0].result.surprise",
    );

    let mut missing = fixture_value();
    missing["observations"]["execution"][0]["result"]
        .as_object_mut()
        .expect("execution result")
        .remove("base_fee_msat");
    assert_path_error(
        &parse_mutated(missing),
        "execution",
        "$.observations.execution[0].result.base_fee_msat",
    );
}

#[test]
fn strict_policy_requires_exact_fields_types_enums_and_identity() {
    let policy_index = 7;
    for (field, replacement, path) in [
        (
            "rebalance_mode",
            json!("bogus"),
            "$.observations.evidence[7].result.rebalance_mode",
        ),
        (
            "updated_at",
            json!("bad"),
            "$.observations.evidence[7].result.updated_at",
        ),
        (
            "tags",
            json!("not-an-array"),
            "$.observations.evidence[7].result.tags",
        ),
        (
            "peer_id",
            json!("other-peer"),
            "$.observations.evidence[7].result.peer_id",
        ),
    ] {
        let mut wrong = fixture_value();
        wrong["observations"]["evidence"][policy_index]["result"][field] = replacement;
        assert_path_error(&parse_mutated(wrong), "evidence", path);
    }

    let mut missing = fixture_value();
    missing["observations"]["evidence"][policy_index]["result"]
        .as_object_mut()
        .expect("policy result")
        .remove("updated_at");
    assert_path_error(
        &parse_mutated(missing),
        "evidence",
        "$.observations.evidence[7].result.updated_at",
    );

    let mut extra = fixture_value();
    extra["observations"]["evidence"][policy_index]["result"]["surprise"] = json!(true);
    assert_path_error(
        &parse_mutated(extra),
        "evidence",
        "$.observations.evidence[7].result.surprise",
    );
}

#[test]
fn transcript_shape_failures_preserve_structured_context() {
    for (family, mutate, path) in [
        ("clock", "value", "$.observations.clock[0].value"),
        ("entropy", "result", "$.observations.entropy[0].result"),
    ] {
        let mut wrong = fixture_value();
        wrong["observations"][family][0][mutate] = json!("wrong-type");
        let error = replay_fee_capture(&parse_mutated(wrong))
            .expect_err("transcript value type mismatch must fail closed");
        assert!(
            matches!(error, ReplayError::Transcript { .. }),
            "{family} mismatch lost structured transcript context: {error}"
        );
        assert!(error.to_string().contains(path), "{error}");
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
fn pre_state_rejects_corrupted_nested_pid_and_thompson_shapes_at_source_path() {
    let corruptions = [
        (
            "pid.kp type",
            "$.pre_state.ordered_channels[0].fee_state.pid.kp",
            vec!["fee_state", "pid", "kp"],
            json!("bad"),
        ),
        (
            "posterior coeff length",
            "$.pre_state.ordered_channels[0].fee_state.thompson.posterior_coeffs",
            vec!["fee_state", "thompson", "posterior_coeffs"],
            json!([{"__f__": "0.0"}, {"__f__": "1.0"}]),
        ),
        (
            "posterior precision shape",
            "$.pre_state.ordered_channels[0].fee_state.thompson.posterior_precision[1]",
            vec!["fee_state", "thompson", "posterior_precision"],
            json!([
                [{"__f__": "1.0"}, {"__f__": "0.0"}, {"__f__": "0.0"}],
                [{"__f__": "0.0"}],
                [{"__f__": "0.0"}, {"__f__": "0.0"}, {"__f__": "1.0"}]
            ]),
        ),
        (
            "observation tuple type",
            "$.pre_state.ordered_channels[0].fee_state.thompson.observations[0]",
            vec!["fee_state", "thompson", "observations"],
            json!(["not-a-tuple"]),
        ),
        (
            "contextual posterior tuple shape",
            "$.pre_state.ordered_channels[0].fee_state.thompson.contextual_posteriors.ctx",
            vec!["fee_state", "thompson", "contextual_posteriors"],
            json!({"ctx": [{"__f__": "1.0"}, {"__f__": "2.0"}]}),
        ),
        (
            "posterior bias tuple shape",
            "$.pre_state.ordered_channels[0].fee_state.thompson.posterior_bias[0]",
            vec!["fee_state", "thompson", "posterior_bias"],
            json!([[{"__f__": "1.0"}, {"__f__": "2.0"}]]),
        ),
    ];

    for (name, expected_path, keys, replacement) in corruptions {
        let mut wrong = fixture_value();
        let mut cursor = &mut wrong["pre_state"]["ordered_channels"][0];
        for key in keys {
            cursor = &mut cursor[key];
        }
        *cursor = replacement;
        let error = replay_fee_capture(&parse_mutated(wrong))
            .expect_err("nested replay state corruption must fail closed");
        assert!(
            error.to_string().contains(expected_path),
            "{name}: expected {expected_path}, got {error}"
        );
    }
}

#[test]
fn pre_state_rejects_invalid_thompson_semantic_domains_at_exact_paths() {
    let thompson_path = "$.pre_state.ordered_channels[0].fee_state.thompson";
    let scalar_corruptions = [
        ("prior_mean_fee", json!(-1)),
        ("prior_std_fee", json!(0)),
        ("posterior_std", json!({"__f__": "0.0"})),
        ("noise_variance", json!({"__f__": "-1.0"})),
        ("charged_fee_mean", json!({"__f__": "-1.0"})),
        ("zero_revenue_streak", json!(-1)),
        ("zero_run_start_fee", json!({"__f__": "-1.0"})),
        ("zero_run_start_ts", json!(-1)),
        ("positive_rate_ref", json!({"__f__": "-1.0"})),
        ("positive_rate_ref_ts", json!(-1)),
        ("meaningful_gap_ema_hours", json!({"__f__": "-1.0"})),
        ("last_meaningful_ts", json!(-1)),
        ("last_upward_probe_ts", json!(-1)),
        ("exploration_boost", json!({"__f__": "0.749999"})),
        ("exploration_boost", json!({"__f__": "2.000001"})),
        ("last_sampled_fee", json!(-1)),
        ("last_sample_time", json!(-1)),
        ("reseeded_at", json!(-1)),
    ];

    for (field, replacement) in scalar_corruptions {
        let mut wrong = fixture_value();
        wrong["pre_state"]["ordered_channels"][0]["fee_state"]["thompson"][field] = replacement;
        let error = replay_fee_capture(&parse_mutated(wrong))
            .expect_err("invalid Thompson scalar must fail closed");
        let expected_path = format!("{thompson_path}.{field}");
        assert!(
            error.to_string().contains(&expected_path),
            "{field}: expected {expected_path}, got {error}"
        );
    }

    let contextual_corruptions = [(1, json!({"__f__": "0.0"})), (2, json!(-1)), (3, json!(-1))];
    for (index, replacement) in contextual_corruptions {
        let mut wrong = fixture_value();
        wrong["pre_state"]["ordered_channels"][0]["fee_state"]["thompson"]
            ["contextual_posteriors"] = json!({"ctx": [{"__f__": "1.0"}, {"__f__": "2.0"}, 0, 0]});
        wrong["pre_state"]["ordered_channels"][0]["fee_state"]["thompson"]
            ["contextual_posteriors"]["ctx"][index] = replacement;
        let error = replay_fee_capture(&parse_mutated(wrong))
            .expect_err("invalid contextual posterior scalar must fail closed");
        let expected_path = format!("{thompson_path}.contextual_posteriors.ctx[{index}]");
        assert!(
            error.to_string().contains(&expected_path),
            "contextual index {index}: expected {expected_path}, got {error}"
        );
    }

    let bias_corruptions = [
        (0, json!({"__f__": "-1.0"})),
        (1, json!({"__f__": "0.0"})),
        (2, json!(-1)),
    ];
    for (index, replacement) in bias_corruptions {
        let mut wrong = fixture_value();
        wrong["pre_state"]["ordered_channels"][0]["fee_state"]["thompson"]["posterior_bias"] =
            json!([[{"__f__": "1.0"}, {"__f__": "1.0"}, 0]]);
        wrong["pre_state"]["ordered_channels"][0]["fee_state"]["thompson"]["posterior_bias"][0]
            [index] = replacement;
        let error = replay_fee_capture(&parse_mutated(wrong))
            .expect_err("invalid posterior bias scalar must fail closed");
        let expected_path = format!("{thompson_path}.posterior_bias[0][{index}]");
        assert!(
            error.to_string().contains(&expected_path),
            "posterior bias index {index}: expected {expected_path}, got {error}"
        );
    }

    let mut oversized_bias = fixture_value();
    oversized_bias["pre_state"]["ordered_channels"][0]["fee_state"]["thompson"]["posterior_bias"] =
        Value::Array(
            (0..51)
                .map(|_| json!([{"__f__": "1.0"}, {"__f__": "1.0"}, 0]))
                .collect(),
        );
    let error = replay_fee_capture(&parse_mutated(oversized_bias))
        .expect_err("oversized posterior bias must fail closed");
    assert!(
        error
            .to_string()
            .contains(&format!("{thompson_path}.posterior_bias")),
        "oversized posterior bias: {error}"
    );
}

#[test]
fn pre_state_preserves_valid_thompson_semantic_edges_exactly() {
    for boost in ["0.75", "2.0"] {
        let mut edge = fixture_value();
        let thompson = &mut edge["pre_state"]["ordered_channels"][0]["fee_state"]["thompson"];
        thompson["prior_mean_fee"] = json!(0);
        thompson["prior_std_fee"] = json!(1);
        thompson["posterior_std"] = json!({"__f__": "1e-06"});
        thompson["noise_variance"] = json!({"__f__": "1e-06"});
        thompson["contextual_posteriors"] =
            json!({"edge": [{"__f__": "-1.0"}, {"__f__": "1e-06"}, 0, 0]});
        thompson["posterior_bias"] = json!([[{"__f__": "0.0"}, {"__f__": "1e-06"}, 0]]);
        thompson["exploration_boost"] = json!({"__f__": boost});
        edge["expected"]["post_channel_state"][0]["fee_state"]["thompson"] = thompson.clone();

        let capture = parse_mutated(edge);
        replay_fee_capture(&capture).expect("valid semantic edges replay exactly");
    }
}

#[test]
fn malformed_transcript_matrix_is_structured_and_never_panics() {
    let cases = [
        (
            "initial evidence result",
            "evidence",
            0,
            "channels_info",
            "$.observations.evidence[0].result",
        ),
        (
            "in-kernel evidence result",
            "evidence",
            3,
            "channel_states",
            "$.observations.evidence[3].result",
        ),
        (
            "clock value",
            "clock",
            0,
            "cycle.started_at",
            "$.observations.clock[0].value",
        ),
        (
            "entropy result",
            "entropy",
            0,
            "vegas.boost",
            "$.observations.entropy[0].result",
        ),
        (
            "governor result",
            "governor",
            0,
            "authorize",
            "$.observations.governor[0].result.authorized",
        ),
        (
            "execution result",
            "execution",
            0,
            "execute",
            "$.observations.execution[0].result.success",
        ),
    ];

    for (name, family, ordinal, label_or_op, path) in cases {
        let mut wrong = fixture_value();
        match name {
            "initial evidence result" => {
                wrong["observations"]["evidence"][0]["result"] = json!("bad")
            }
            "in-kernel evidence result" => {
                wrong["observations"]["evidence"][3]["result"] = json!("bad")
            }
            "clock value" => wrong["observations"]["clock"][0]["value"] = json!("bad"),
            "entropy result" => wrong["observations"]["entropy"][0]["result"] = json!("bad"),
            "governor result" => {
                wrong["observations"]["governor"][0]["result"]["authorized"] = json!("bad")
            }
            "execution result" => {
                wrong["observations"]["execution"][0]["result"]["success"] = json!("bad")
            }
            _ => unreachable!("known malformed case"),
        }
        let capture = parse_mutated(wrong);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            replay_fee_capture(&capture)
        }));
        assert!(outcome.is_ok(), "{name} panicked");
        let error = outcome
            .expect("checked no panic")
            .expect_err("malformed transcript must fail closed");
        assert!(
            matches!(error, ReplayError::Transcript { .. }),
            "{name} lost structured context: {error}"
        );
        let rendered = error.to_string();
        for needle in [
            family,
            &format!("expected ordinal {ordinal}"),
            label_or_op,
            "actual",
            path,
        ] {
            assert!(
                rendered.contains(needle),
                "{name}: {needle:?} missing from {rendered}"
            );
        }
    }
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
