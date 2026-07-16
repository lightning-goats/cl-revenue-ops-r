use cln_plugin::options;
use revops::config_types::{classify_runtime_key, load, typed_value, FieldType};
use revops::rpc_status::{build_config_response, option_value_to_json};

#[test]
fn config_response_known_key_with_value() {
    let v = build_config_response(
        "observer",
        true,
        Some(&options::Value::Boolean(true)),
        None,
        0,
    );
    assert_eq!(v["key"], "observer");
    assert_eq!(v["value"], true);
}

#[test]
fn config_response_known_key_without_value() {
    // A registered option that CLN never set and that has no default
    // (a `*_no_default` variant) resolves to `null`, not an error.
    let v = build_config_response("some-optional", true, None, None, 0);
    assert_eq!(v["key"], "some-optional");
    assert!(v["value"].is_null());
}

#[test]
fn unknown_key_error_is_capital_u() {
    let v = build_config_response("nope", false, None, None, 0);
    assert_eq!(v["error"], "Unknown config key: nope"); // was lowercase in Phase 1a
    assert!(v.get("key").is_none());
}

#[test]
fn known_key_shape_has_version_and_classification() {
    let v = build_config_response(
        "daily-budget-sats",
        true,
        Some(&options::Value::Integer(5000)),
        Some(FieldType::Int),
        3,
    );
    assert_eq!(v["key"], "daily-budget-sats");
    assert_eq!(v["value"], 5000);
    assert_eq!(v["version"], 3);
    // daily_budget_sats IS in PUBLIC_RUNTIME_KEYS -- confirmed against the
    // fixture (fixtures/config_types.json).
    assert_eq!(v["classification"], "public");
}

#[test]
fn classify_matches_python_fixture() {
    let table = load();
    for key in &table.public_keys {
        assert_eq!(classify_runtime_key(key), "public", "key={key}");
    }
    for key in &table.deprecated_keys {
        assert_eq!(classify_runtime_key(key), "deprecated", "key={key}");
    }
    assert_eq!(
        classify_runtime_key("definitely_not_a_real_key_xyz"),
        "internal"
    );
}

#[test]
fn typed_value_parses_float_backed_by_string_option() {
    // hot_channel_protection_min_velocity is a `float` Config field backed
    // by a `string`-typed CLN option (per fixtures/config_types.json).
    let v = typed_value(
        "hot-channel-protection-min-velocity",
        &options::Value::String("0.25".to_string()),
    );
    assert_eq!(v, serde_json::json!(0.25));
}

#[test]
fn typed_value_passes_through_int_and_bool_natively() {
    assert_eq!(
        typed_value("daily-budget-sats", &options::Value::Integer(7)),
        serde_json::json!(7)
    );
    assert_eq!(
        typed_value("observer", &options::Value::Boolean(true)),
        serde_json::json!(true)
    );
}

#[test]
fn option_value_to_json_converts_scalars() {
    assert_eq!(
        option_value_to_json(Some(&options::Value::String("x".into()))),
        serde_json::json!("x")
    );
    assert_eq!(
        option_value_to_json(Some(&options::Value::Integer(7))),
        serde_json::json!(7)
    );
    assert_eq!(
        option_value_to_json(Some(&options::Value::Boolean(false))),
        serde_json::json!(false)
    );
    assert!(option_value_to_json(None).is_null());
}
