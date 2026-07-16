use cln_plugin::options;
use revops::rpc_status::{build_config_response, option_value_to_json};

#[test]
fn config_response_known_key_with_value() {
    let v = build_config_response("observer", true, Some(&options::Value::Boolean(true)));
    assert_eq!(v["key"], "observer");
    assert_eq!(v["value"], true);
}

#[test]
fn config_response_known_key_without_value() {
    // A registered option that CLN never set and that has no default
    // (a `*_no_default` variant) resolves to `null`, not an error.
    let v = build_config_response("some-optional", true, None);
    assert_eq!(v["key"], "some-optional");
    assert!(v["value"].is_null());
}

#[test]
fn config_response_unknown_key_is_stable_error_string() {
    let v = build_config_response("nope", false, None);
    assert_eq!(v["error"], "unknown config key: nope");
    assert!(v.get("key").is_none());
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
