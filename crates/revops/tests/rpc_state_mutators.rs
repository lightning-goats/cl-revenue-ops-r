use revops::rpc_params::{decode_params, load_rpc_contract, method_spec, ParamBinding};
use revops::rpc_state_mutators::{
    ban_plan, ban_success, completed_spend_response, completed_write_response,
    deprecated_policy_write_gate, ignore_plan, ignore_success, invalid_peer_id_error,
    parse_spend_release_params, parse_spend_release_stale_params, parse_spend_reserve_params,
    parse_spend_settle_params, policy_write_override, spend_release_response,
    spend_release_stale_response, spend_reserve_rejection, spend_reserve_response,
    spend_settle_response, unban_plan, unban_success, unignore_success,
};
use revops::state_writer::StateWriteAck;
use revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode};
use revops_db::state_writer::SpendReleaseBatch;
use serde_json::{json, Map, Value};

const PEER: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn configured_policy(tags: &[&str]) -> PeerPolicy {
    PeerPolicy {
        peer_id: PEER.to_string(),
        strategy: FeeStrategy::Static,
        rebalance_mode: RebalanceMode::SourceOnly,
        fee_ppm_target: Some(321),
        tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
        updated_at: 1_800_000_000,
        fee_multiplier_min: Some(0.75),
        fee_multiplier_max: Some(2.25),
        expires_at: Some(1_900_000_000),
    }
}

#[test]
fn deprecated_policy_write_override_matches_python_string_gate() {
    let accepted = [
        ("internal", json!(true)),
        ("internal", json!(" YES ")),
        ("admin", json!("on")),
        ("admin", json!(1)),
    ];
    for (key, value) in accepted {
        let params = Map::from_iter([(key.to_string(), value)]);
        assert!(policy_write_override(&params), "{params:?}");
    }

    for value in [
        json!(false),
        json!(null),
        json!("false"),
        json!(0),
        json!(1.0),
        json!([]),
    ] {
        let params = Map::from_iter([("internal".to_string(), value)]);
        assert!(!policy_write_override(&params), "{params:?}");
    }
}

#[test]
fn deprecated_aliases_keep_the_exact_python_operator_lockdown() {
    let empty = Map::new();
    assert_eq!(
        deprecated_policy_write_gate("ignore", &empty),
        Some(json!({
            "error": "revenue-ignore is deprecated for normal operator use. Use revenue-policy list/get/find/changes for diagnostics."
        }))
    );
    assert_eq!(
        deprecated_policy_write_gate("unignore", &empty),
        Some(json!({
            "error": "revenue-unignore is deprecated for normal operator use. Use revenue-policy list/get/find/changes for diagnostics."
        }))
    );

    let internal = Map::from_iter([("internal".to_string(), json!("true"))]);
    assert_eq!(deprecated_policy_write_gate("ignore", &internal), None);
    assert_eq!(deprecated_policy_write_gate("unignore", &internal), None);
}

#[test]
fn invalid_peer_id_error_is_the_frozen_python_response() {
    assert_eq!(
        invalid_peer_id_error(),
        json!({"error": "Invalid peer_id format: expected 66-character hex pubkey"})
    );
}

#[test]
fn ignore_replaces_tags_but_preserves_unspecified_policy_fields() {
    let existing = configured_policy(&["no_close", "whale"]);
    let write = ignore_plan(&existing, "manual");
    assert_eq!(write.peer_id, PEER);
    assert_eq!(write.strategy, "passive");
    assert_eq!(write.rebalance_mode, "disabled");
    assert_eq!(write.fee_ppm_target, Some(321));
    assert_eq!(write.tags.as_deref(), Some(r#"["ignored","manual"]"#));
    assert_eq!(write.fee_multiplier_min, Some(0.75));
    assert_eq!(write.fee_multiplier_max, Some(2.25));
    assert_eq!(write.expires_at, Some(1_900_000_000));

    let already_named = ignore_plan(&existing, "ignored");
    assert_eq!(already_named.tags.as_deref(), Some(r#"["ignored"]"#));
}

#[test]
fn ban_and_unban_preserve_other_tags_and_policy_scalars() {
    let existing = configured_policy(&["no_close", "whale"]);
    let banned = ban_plan(&existing);
    assert_eq!(banned.strategy, "passive");
    assert_eq!(banned.rebalance_mode, "disabled");
    assert_eq!(
        banned.tags.as_deref(),
        Some(r#"["no_close","whale","banned"]"#)
    );
    assert_eq!(banned.fee_ppm_target, Some(321));
    assert_eq!(banned.fee_multiplier_min, Some(0.75));
    assert_eq!(banned.fee_multiplier_max, Some(2.25));
    assert_eq!(banned.expires_at, None, "Python bans clear expiry");

    let mut banned_policy = existing;
    banned_policy.tags = vec!["no_close".into(), "banned".into(), "whale".into()];
    let unbanned = unban_plan(&banned_policy);
    assert_eq!(unbanned.strategy, "dynamic");
    assert_eq!(unbanned.rebalance_mode, "enabled");
    assert_eq!(unbanned.tags.as_deref(), Some(r#"["no_close","whale"]"#));
    assert_eq!(unbanned.fee_ppm_target, Some(321));
    assert_eq!(unbanned.fee_multiplier_min, Some(0.75));
    assert_eq!(unbanned.fee_multiplier_max, Some(2.25));
    assert_eq!(unbanned.expires_at, Some(1_900_000_000));
}

#[test]
fn applied_mutators_return_exact_python_success_shapes() {
    assert_eq!(
        ignore_success(PEER, "manual"),
        json!({
            "status": "success",
            "action": "ignore",
            "peer_id": PEER,
            "reason": "manual",
            "message": format!("Peer {PEER} set to passive strategy with rebalancing disabled."),
            "warning": "DEPRECATED: Use 'revenue-policy set' instead."
        })
    );
    assert_eq!(
        unignore_success(PEER),
        json!({
            "status": "success",
            "action": "unignore",
            "peer_id": PEER,
            "message": format!("Peer {PEER} reverted to default policy (dynamic strategy, rebalancing enabled)."),
            "warning": "DEPRECATED: Use 'revenue-policy delete' instead."
        })
    );
    assert_eq!(
        ban_success(PEER, "operator", &["no_close".into(), "banned".into()]),
        json!({
            "status": "success",
            "action": "ban",
            "peer_id": PEER,
            "reason": "operator",
            "tags": ["no_close", "banned"],
            "message": "Peer banned: no channel opens, no LN+ swaps, no fee/rebalance management. Existing channels and in-flight swaps are untouched."
        })
    );
    assert_eq!(
        unban_success(PEER, &["no_close".into()]),
        json!({"status": "success", "action": "unban", "peer_id": PEER, "tags": ["no_close"]})
    );
}

#[test]
fn only_completed_durable_ack_can_return_success() {
    let success = completed_write_response(StateWriteAck::Applied(()), |_| json!({"ok": true}));
    assert_eq!(success, json!({"ok": true}));

    let failures: [(StateWriteAck<()>, &str); 5] = [
        (StateWriteAck::AlreadyTerminal, "already_terminal"),
        (StateWriteAck::Denied("invalid transition".into()), "denied"),
        (
            StateWriteAck::NotAdmitted("queue full".into()),
            "not_admitted",
        ),
        (
            StateWriteAck::AdmittedOutcomeUnknown("receipt expired".into()),
            "admitted_outcome_unknown",
        ),
        (
            StateWriteAck::StorageFailure("sqlite rollback".into()),
            "storage_failure",
        ),
    ];
    for (ack, expected_code) in failures {
        let value: Value = completed_write_response(ack, |_| json!({"ok": true}));
        assert_eq!(value["status"], "error");
        assert_eq!(value["error"]["code"], expected_code);
        assert_ne!(value, json!({"ok": true}));
    }
}

fn decoded_spend_params(name: &str, raw: Value) -> Map<String, Value> {
    let spec = method_spec(&load_rpc_contract(), name);
    decode_params(&spec, &raw, ParamBinding::PositionalOrNamed).unwrap()
}

#[test]
fn spend_mutator_params_bind_positionally_with_python_handler_coercions() {
    let reserve = parse_spend_reserve_params(&decoded_spend_params(
        "revenue-spend-reserve",
        json!(["r-1", " Rebalance ", "25", "sub", null, 123, "{\"z\": 2}"]),
    ))
    .unwrap();
    assert_eq!(reserve.request.reservation_id, "r-1");
    assert_eq!(reserve.request.category, " Rebalance ");
    assert_eq!(reserve.request.amount_sats, 25);
    assert_eq!(reserve.request.subcategory.as_deref(), Some("sub"));
    assert_eq!(reserve.request.reference_id, None);
    assert_eq!(reserve.request.channel_id.as_deref(), Some("123"));
    assert_eq!(reserve.request.metadata, Some(json!({"z": 2})));

    let release =
        parse_spend_release_params(&decoded_spend_params("revenue-spend-release", json!([7])));
    assert_eq!(release, "7");

    let stale = parse_spend_release_stale_params(&decoded_spend_params(
        "revenue-spend-release-stale",
        json!(["0", " FOO ", 0]),
    ))
    .unwrap();
    assert_eq!(stale.max_age_seconds, 1);
    assert_eq!(stale.category.as_deref(), Some("foo"));
    assert_eq!(stale.limit, 1);

    let settle = parse_spend_settle_params(&decoded_spend_params(
        "revenue-spend-settle",
        json!(["r-1", "12", "", "false"]),
    ))
    .unwrap();
    assert_eq!(settle.reservation_id, "r-1");
    assert_eq!(settle.actual_spent_sats, Some(12));
    assert_eq!(settle.source, None, "empty source uses Python fallback");
    assert!(settle.record_event, "bool(false-string) is true in Python");
}

#[test]
fn spend_reserve_validation_and_metadata_fallback_match_python() {
    let zero = decoded_spend_params(
        "revenue-spend-reserve",
        json!({"reservation_id": "r", "category": "misc", "amount_sats": 0}),
    );
    assert_eq!(
        parse_spend_reserve_params(&zero).unwrap_err(),
        json!({"error": "amount_sats must be > 0"})
    );

    let bad_int = decoded_spend_params(
        "revenue-spend-reserve",
        json!({"reservation_id": "r", "category": "misc", "amount_sats": null}),
    );
    assert!(parse_spend_reserve_params(&bad_int).unwrap_err()["error"]
        .as_str()
        .unwrap()
        .contains("int() argument"));

    let raw = decoded_spend_params(
        "revenue-spend-reserve",
        json!({
            "reservation_id": "r",
            "category": "misc",
            "amount_sats": 1,
            "metadata_json": "not-json"
        }),
    );
    assert_eq!(
        parse_spend_reserve_params(&raw).unwrap().request.metadata,
        Some(json!({"raw": "not-json"}))
    );
}

#[test]
fn spend_mutator_responses_are_exact_and_only_follow_completed_results() {
    let args = parse_spend_reserve_params(&decoded_spend_params(
        "revenue-spend-reserve",
        json!(["r-1", "misc", 25]),
    ))
    .unwrap();
    let before = json!({"remaining_sats": 100, "effective_budget_sats": 100});
    let after = json!({"remaining_sats": 75, "effective_budget_sats": 100});

    assert_eq!(
        spend_reserve_rejection(25, 10, &before),
        json!({
            "status": "rejected",
            "reason": "insufficient_unified_budget",
            "requested_sats": 25,
            "remaining_sats": 10,
            "budget": before,
        })
    );
    assert_eq!(
        spend_reserve_response(false, &args, &before, &after),
        json!({"status": "error", "error": "Failed to reserve spend"})
    );
    assert_eq!(
        spend_reserve_response(true, &args, &before, &after),
        json!({
            "status": "success",
            "reservation_id": "r-1",
            "category": "misc",
            "amount_sats": 25,
            "budget_before": before,
            "budget_after_estimate": after,
        })
    );
    assert_eq!(
        spend_release_response("r-1", true),
        json!({"status": "success", "reservation_id": "r-1"})
    );
    assert_eq!(
        spend_settle_response("r-1", false),
        json!({"status": "not_found", "reservation_id": "r-1"})
    );
    let released = SpendReleaseBatch {
        released_count: 2,
        released_sats: 75,
        reservation_ids: vec!["a".into(), "b".into()],
    };
    assert_eq!(
        spend_release_stale_response(&released, &after),
        json!({
            "status": "success",
            "released_count": 2,
            "released_sats": 75,
            "reservation_ids": ["a", "b"],
            "budget_after": after,
        })
    );

    let response = completed_spend_response::<()>(
        StateWriteAck::NotAdmitted("queue full".into()),
        |_| json!({"status": "success"}),
    );
    assert_eq!(response, json!({"error": "queue full"}));
    assert_ne!(response["status"], "success");
}
