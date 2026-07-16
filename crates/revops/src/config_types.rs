//! Typed `Config` field metadata for `revenue-r-config`, AST-extracted from
//! Python's `Config` dataclass (`modules/config.py:494-836`) into
//! `fixtures/config_types.json` by `tools/port/gen_config_types_fixture.py`
//! (port worktree, branch `port`).
//!
//! The fixture is keyed by the Python field name (underscored, e.g.
//! `daily_budget_sats`), NOT the hyphenated CLN option suffix (e.g.
//! `daily-budget-sats`) -- the two-namespace split the diff harness's
//! `diff_config.py` docstring already documents. Lookups in this module
//! accept either spelling: a direct match on the field name first, then a
//! hyphen-to-underscore fallback so callers can pass the RPC `key` (an
//! option suffix) directly.

use cln_plugin::options;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

/// One `Config` dataclass field's declared Python type annotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Int,
    Float,
    Bool,
    String,
}

impl FieldType {
    fn from_fixture_str(s: &str) -> Option<Self> {
        match s {
            "int" => Some(FieldType::Int),
            "float" => Some(FieldType::Float),
            "bool" => Some(FieldType::Bool),
            "string" => Some(FieldType::String),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawTable {
    fields: HashMap<String, String>,
    public_keys: Vec<String>,
    deprecated_keys: Vec<String>,
}

/// Typed `Config` field metadata plus the public/deprecated runtime-key
/// classification sets, loaded from the embedded fixture.
pub struct ConfigTypes {
    pub fields: HashMap<String, FieldType>,
    pub public_keys: Vec<String>,
    pub deprecated_keys: Vec<String>,
}

/// Load the embedded `fixtures/config_types.json` table.
pub fn load() -> ConfigTypes {
    let raw: RawTable = serde_json::from_str(include_str!("../../../fixtures/config_types.json"))
        .expect("embedded config_types.json is valid");
    let fields = raw
        .fields
        .into_iter()
        .filter_map(|(name, ty)| FieldType::from_fixture_str(&ty).map(|ft| (name, ft)))
        .collect();
    ConfigTypes {
        fields,
        public_keys: raw.public_keys,
        deprecated_keys: raw.deprecated_keys,
    }
}

/// Resolve a `FieldType` for `key`, trying it verbatim first (the Python
/// field name, e.g. `daily_budget_sats`) and then with hyphens converted to
/// underscores (the CLN option suffix, e.g. `daily-budget-sats`). Returns
/// `None` for keys that don't correspond to a typed `Config` field at all
/// (some registered options are Rust-only or don't map 1:1 onto a `Config`
/// dataclass field).
pub fn field_type_for(key: &str) -> Option<FieldType> {
    let table = load();
    if let Some(ft) = table.fields.get(key) {
        return Some(*ft);
    }
    let underscored = key.replace('-', "_");
    table.fields.get(&underscored).copied()
}

/// Port of `Config.classify_runtime_key` (modules/config.py:898-905):
/// `"public"` if `key` is in `PUBLIC_RUNTIME_KEYS`, `"deprecated"` if in
/// `DEPRECATED_RUNTIME_KEYS`, else `"internal"`. Like [`field_type_for`],
/// accepts `key` verbatim (the Python field name) or hyphenated (the CLN
/// option suffix).
pub fn classify_runtime_key(key: &str) -> &'static str {
    let table = load();
    let underscored = key.replace('-', "_");
    let matches = |k: &str| k == key || k == underscored;
    if table.public_keys.iter().any(|k| matches(k)) {
        "public"
    } else if table.deprecated_keys.iter().any(|k| matches(k)) {
        "deprecated"
    } else {
        "internal"
    }
}

/// Convert a resolved `cln-plugin` option value into the JSON scalar shape
/// Python would emit for a `Config` field of the given (already-resolved)
/// type. `Int`/`Bool`/`String` fields pass their native `cln-plugin` value
/// straight through (their CLN option type already matches); a `Float`
/// field backed by a `string`-typed CLN option gets parsed to `f64` --
/// several `Config` float fields are declared as CLN `string` options
/// (confirmed per-field from the fixture, not assumed). `field_type: None`
/// (no typed metadata for this key) also passes the raw value through
/// unconverted.
pub fn convert_value(field_type: Option<FieldType>, raw: &options::Value) -> Value {
    if field_type == Some(FieldType::Float) {
        if let options::Value::String(s) = raw {
            if let Ok(f) = s.parse::<f64>() {
                return serde_json::json!(f);
            }
        }
    }
    serde_json::to_value(raw).unwrap_or(Value::Null)
}

/// Convenience wrapper: resolve `field`'s `FieldType` (see
/// [`field_type_for`]) and convert `raw` accordingly.
pub fn typed_value(field: &str, raw: &options::Value) -> Value {
    convert_value(field_type_for(field), raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_embedded_table() {
        let table = load();
        assert!(!table.fields.is_empty());
        assert!(!table.public_keys.is_empty());
        assert!(!table.deprecated_keys.is_empty());
    }

    #[test]
    fn field_type_for_accepts_hyphenated_suffix() {
        assert_eq!(field_type_for("daily_budget_sats"), Some(FieldType::Int));
        assert_eq!(field_type_for("daily-budget-sats"), Some(FieldType::Int));
    }

    #[test]
    fn field_type_for_unknown_key_is_none() {
        assert_eq!(field_type_for("not-a-real-config-field"), None);
    }

    #[test]
    fn convert_value_passes_through_when_no_field_type() {
        assert_eq!(
            convert_value(None, &options::Value::Integer(9)),
            serde_json::json!(9)
        );
    }
}
