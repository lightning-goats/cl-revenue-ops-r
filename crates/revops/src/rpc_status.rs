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

// Unit-test coverage for `build_status`, `build_config_response`, and
// `option_value_to_json` lives in the integration tests
// `crates/revops/tests/status.rs` and `crates/revops/tests/config.rs` --
// those duplicated this module's former inline `#[cfg(test)]` bodies
// verbatim, so the inline copies were removed.
