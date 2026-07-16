//! Pure response builders for the `revenue-r-status` and `revenue-r-config`
//! RPCs. These take already-resolved inputs (gathered by `main.rs` from the
//! running `Plugin<S>` / `ConfiguredPlugin<S, I, O>`) so the response shape
//! itself can be unit-tested without spinning up a plugin process over
//! stdio.

use crate::config_types::{self, FieldType};
use cln_plugin::options;
use serde_json::{json, Value};

/// Inputs for [`build_status`]. `db_tables` is resolved live, at call time,
/// via the persistent DB actor (`revops_db::actor::DbHandle::table_count`)
/// rather than snapshotted once at init -- see `main.rs`'s `State`, which
/// holds a `DbHandle` instead of a one-shot `Option<usize>` count.
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
/// `Plugin::option_str`); `field_type` is the corresponding `Config` field's
/// declared Python type, if `key` maps onto one (see
/// [`crate::config_types::field_type_for`]) — used to render the value in
/// Python's native scalar shape rather than cln-plugin's raw option
/// representation. An unknown key produces the stable capital-U
/// `{"error": "Unknown config key: <key>"}` shape (byte-parity with
/// `cl-revenue-ops.py`'s `revenue-config get`, cl-revenue-ops.py:5670); a
/// known key produces `{"key", "value", "version", "classification"}` --
/// Python's exact shape (cl-revenue-ops.py:5671-5679) MINUS the `warning`
/// key, which only applies to non-public keys and isn't reproduced here (a
/// documented, not silent, gap — see the plan's Task 3 gap note).
///
/// `version` is always a Phase 1b placeholder: Python's live per-key
/// `version` is the DB-persisted `config._version`, incremented on writes
/// through `revenue-config set`, and Phase 1b has no DB-backed
/// override-write path yet. Every known-key response therefore lists
/// `"version"` in a `_phase1b_gaps` array so the diff harness's `--strict`
/// mode skips that one key instead of flagging a false mismatch.
pub fn build_config_response(
    key: &str,
    known: bool,
    value: Option<&options::Value>,
    field_type: Option<FieldType>,
    version: i64,
) -> Value {
    if !known {
        return json!({"error": format!("Unknown config key: {key}")});
    }
    let typed_value = value.map_or(Value::Null, |v| config_types::convert_value(field_type, v));
    json!({
        "key": key,
        "value": typed_value,
        "version": version,
        "classification": config_types::classify_runtime_key(key),
        "_phase1b_gaps": ["version"],
    })
}

// Unit-test coverage for `build_status`, `build_config_response`, and
// `option_value_to_json` lives in the integration tests
// `crates/revops/tests/status.rs` and `crates/revops/tests/config.rs` --
// those duplicated this module's former inline `#[cfg(test)]` bodies
// verbatim, so the inline copies were removed.
