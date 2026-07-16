//! Pure response builders for the `revenue-r-status` and `revenue-r-config`
//! RPCs. These take already-resolved inputs (gathered by `main.rs` from the
//! running `Plugin<S>` / `ConfiguredPlugin<S, I, O>`) so the response shape
//! itself can be unit-tested without spinning up a plugin process over
//! stdio.

use cln_plugin::options;
use serde_json::{json, Value};

/// Inputs for [`build_status`], gathered once at plugin init (the DB probe
/// is a one-shot read at init; see the module docs on `main.rs`'s `State`
/// for why the `Connection` itself isn't retained).
pub struct StatusInputs {
    pub version: String,
    pub observer: bool,
    pub db_path: Option<String>,
    pub db_tables: Option<usize>,
}

/// Build the `revenue-r-status` response body.
pub fn build_status(i: &StatusInputs) -> Value {
    json!({
        "status": "running",
        "version": i.version,
        "mode": if i.observer { "observer" } else { "enforcing" },
        "db": {
            "path": i.db_path,
            "tables": i.db_tables,
        },
    })
}

/// Convert a resolved cln-plugin option value into JSON. `None` becomes
/// `null` — this happens for a registered-but-valueless option (a
/// `*_no_default` variant) that CLN never set, not an unknown key (see
/// [`build_config_response`] for that case).
pub fn option_value_to_json(value: Option<&options::Value>) -> Value {
    match value {
        Some(v) => serde_json::to_value(v).unwrap_or(Value::Null),
        None => Value::Null,
    }
}

/// Build the `revenue-r-config` response body.
///
/// `known` reflects whether `key` resolved to one of this plugin's
/// registered option names at all; `value` is that option's current
/// resolved value (already defaulted by cln-plugin's init handshake when
/// CLN didn't set it explicitly — see `ConfiguredPlugin::option_str`/
/// `Plugin::option_str`). An unknown key produces the stable
/// `{"error": "unknown config key: <key>"}` shape the diff harness matches;
/// a known key always produces `{"key": ..., "value": ...}`, with `value`
/// possibly `null`.
pub fn build_config_response(key: &str, known: bool, value: Option<&options::Value>) -> Value {
    if !known {
        return json!({"error": format!("unknown config key: {key}")});
    }
    json!({"key": key, "value": option_value_to_json(value)})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_shape() {
        let v = build_status(&StatusInputs {
            version: "0.1.0".into(),
            observer: true,
            db_path: Some("/tmp/x.db".into()),
            db_tables: Some(35),
        });
        assert_eq!(v["status"], "running");
        assert_eq!(v["version"], "0.1.0");
        assert_eq!(v["mode"], "observer");
        assert_eq!(v["db"]["tables"], 35);
    }

    #[test]
    fn status_enforcing_mode_when_not_observer() {
        let v = build_status(&StatusInputs {
            version: "0.1.0".into(),
            observer: false,
            db_path: None,
            db_tables: None,
        });
        assert_eq!(v["mode"], "enforcing");
        assert!(v["db"]["path"].is_null());
        assert!(v["db"]["tables"].is_null());
    }

    #[test]
    fn config_response_known_key_with_value() {
        let v = build_config_response("observer", true, Some(&options::Value::Boolean(true)));
        assert_eq!(v["key"], "observer");
        assert_eq!(v["value"], true);
    }

    #[test]
    fn config_response_known_key_without_value_is_null() {
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
    fn option_value_to_json_converts_variants() {
        assert_eq!(
            option_value_to_json(Some(&options::Value::String("x".into()))),
            json!("x")
        );
        assert_eq!(
            option_value_to_json(Some(&options::Value::Integer(7))),
            json!(7)
        );
        assert_eq!(option_value_to_json(None), Value::Null);
    }
}
