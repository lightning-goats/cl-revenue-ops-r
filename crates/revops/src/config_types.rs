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
    /// `CONFIG_FIELD_RANGES` (modules/config.py:354-480, 96 entries as of
    /// this writing): field name -> `[min, max]`, both numbers (int or
    /// float in Python; `f64` here covers both without loss for the
    /// comparison this table exists for).
    #[serde(default)]
    ranges: HashMap<String, (f64, f64)>,
    /// `STRING_ENUM_VALID_VALUES` (modules/config.py:483-490, 5 entries):
    /// field name -> valid value list.
    #[serde(default)]
    enums: HashMap<String, Vec<String>>,
}

/// Typed `Config` field metadata plus the public/deprecated runtime-key
/// classification sets, loaded from the embedded fixture.
pub struct ConfigTypes {
    pub fields: HashMap<String, FieldType>,
    pub public_keys: Vec<String>,
    pub deprecated_keys: Vec<String>,
    /// `CONFIG_FIELD_RANGES` -- see [`field_range`].
    pub ranges: HashMap<String, (f64, f64)>,
    /// `STRING_ENUM_VALID_VALUES` -- see [`field_enum`].
    pub enums: HashMap<String, Vec<String>>,
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
        ranges: raw.ranges,
        enums: raw.enums,
    }
}

/// `CONFIG_FIELD_RANGES[field]` (`(min, max)`), or `None` if `field` has no
/// range constraint. `field` must already be the Python `Config` field name
/// (snake_case) -- unlike [`field_type_for`]/[`classify_runtime_key`], no
/// hyphen-to-underscore fallback is applied here since every caller in this
/// module already resolves to the field name first (via
/// `config_resolve::db_override_key`).
pub fn field_range(field: &str) -> Option<(f64, f64)> {
    load().ranges.get(field).copied()
}

/// `STRING_ENUM_VALID_VALUES[field]`, or `None` if `field` has no enum
/// constraint. Same field-name-only lookup contract as [`field_range`].
pub fn field_enum(field: &str) -> Option<Vec<String>> {
    load().enums.get(field).cloned()
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
/// type.
///
/// Cross-referencing `fixtures/config_types.json` against
/// `fixtures/options.json` shows 62 of the 90 `Int`/`Bool` `Config` fields
/// are registered as CLN `string`-typed options (their Python default is a
/// string like `"5000"` or `"true"`, parsed by hand in
/// `cl-revenue-ops.py`'s `_safe_int`/`_safe_int_opt` calls and the assorted
/// inline `.lower() == 'true'` / `.lower() in (...)` checks around
/// `config_kwargs`). At runtime those arrive here as
/// `options::Value::String("5000")`, not `Value::Integer`/`Value::Boolean`
/// -- so `Int`/`Bool` fields need the same "parse the string" treatment
/// `Float` already got, or they'd pass through this function as JSON
/// strings where Python returns ints/bools.
///
/// `Int` fields: `s.parse::<i64>()`, mirroring Python's `int(options[key])`
/// (`_safe_int`/`_safe_int_opt` in `cl-revenue-ops.py`) -- both reject
/// non-integer strings (e.g. `"5000.0"`) the same way.
///
/// `Bool` fields: Python's own bool-parsing is *not* consistent across the
/// `Config` field spread -- `cl-revenue-ops.py`'s per-field startup
/// conversions vary between `.lower() == 'true'` (14 fields) and
/// `.lower() in ('true', '1', 'yes')` (9 fields), but the one generic,
/// field-type-driven bool parser in the codebase --
/// `modules/config.py`'s `Config._apply_override`/`update_runtime`, which
/// (like this function) looks up a field's declared type and converts
/// generically rather than per-field -- accepts `('true', '1', 'yes',
/// 'on')` case-insensitively as truthy, anything else as false. Since this
/// function is the direct Rust analogue of that generic, type-driven
/// conversion (not of the ad hoc per-field startup expressions), it
/// mirrors `_apply_override`'s tolerance: `true`/`1`/`yes`/`on`
/// (case-insensitive) are truthy, everything else (including unparseable
/// junk) is false -- matching `_apply_override`'s permissive fallthrough
/// rather than `update_runtime`'s stricter reject-on-typo behavior, since
/// this is a read path (`revenue-r-config get`), not a validated write.
///
/// `Float` fields backed by a `string`-typed CLN option get parsed to
/// `f64` -- several `Config` float fields are declared as CLN `string`
/// options (confirmed per-field from the fixture, not assumed).
///
/// Native-typed values (an `Int` field backed by a CLN `int` option, a
/// `Bool` field backed by a CLN `bool` option, any `String` field) pass
/// through unconverted, as does `field_type: None` (no typed metadata for
/// this key).
pub fn convert_value(field_type: Option<FieldType>, raw: &options::Value) -> Value {
    match field_type {
        Some(FieldType::Float) => {
            if let options::Value::String(s) = raw {
                if let Ok(f) = s.parse::<f64>() {
                    return serde_json::json!(f);
                }
            }
        }
        Some(FieldType::Int) => {
            if let options::Value::String(s) = raw {
                if let Ok(i) = s.parse::<i64>() {
                    return serde_json::json!(i);
                }
            }
        }
        Some(FieldType::Bool) => {
            if let options::Value::String(s) = raw {
                let truthy = matches!(s.to_lowercase().as_str(), "true" | "1" | "yes" | "on");
                return serde_json::json!(truthy);
            }
        }
        _ => {}
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
        assert!(!table.ranges.is_empty());
        assert!(!table.enums.is_empty());
    }

    /// `CONFIG_FIELD_RANGES['min_fee_ppm']` (modules/config.py:355):
    /// `(5, 100000)`, the CRITICAL-02 economic-viability floor.
    #[test]
    fn field_range_loads_min_fee_ppm() {
        assert_eq!(field_range("min_fee_ppm"), Some((5.0, 100000.0)));
    }

    #[test]
    fn field_range_unknown_field_is_none() {
        assert_eq!(field_range("not-a-real-config-field"), None);
    }

    /// `STRING_ENUM_VALID_VALUES['fee_profile']` (modules/config.py:485).
    #[test]
    fn field_enum_loads_fee_profile() {
        assert_eq!(
            field_enum("fee_profile"),
            Some(vec!["active".to_string(), "conservative".to_string()])
        );
    }

    #[test]
    fn field_enum_unknown_field_is_none() {
        assert_eq!(field_enum("not-a-real-config-field"), None);
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

    /// `flow_interval` (fixture: `"int"`) is registered as a `string`-typed
    /// CLN option (`revenue-ops-flow-interval`, default `"3600"` in
    /// `fixtures/options.json`) -- this is the actual runtime shape
    /// `cln-plugin` hands back for it: `options::Value::String("5000")`,
    /// not `Value::Integer`. Confirms the fix for CRITICAL 1: a
    /// string-backed `Int` field must parse to a JSON number, matching
    /// Python's `_safe_int('revenue-ops-flow-interval')`.
    #[test]
    fn convert_value_parses_string_backed_int_field() {
        assert_eq!(field_type_for("flow_interval"), Some(FieldType::Int));
        assert_eq!(
            convert_value(
                Some(FieldType::Int),
                &options::Value::String("5000".to_string())
            ),
            serde_json::json!(5000)
        );
    }

    /// Unparseable string for an `Int` field (e.g. corrupted config) falls
    /// back to passing the raw value through as JSON, rather than
    /// panicking -- matches this function's existing no-field-type
    /// fallback behavior.
    #[test]
    fn convert_value_int_field_falls_back_on_unparseable_string() {
        assert_eq!(
            convert_value(
                Some(FieldType::Int),
                &options::Value::String("not-a-number".to_string())
            ),
            serde_json::json!("not-a-number")
        );
    }

    /// A real string-backed bool field: `hot_channel_protection_enabled`
    /// (fixture: `"bool"`) is registered as CLN option
    /// `revenue-ops-hot-channel-protection-enabled`, a `string`-typed
    /// option with default `"true"` per `fixtures/options.json`. Runtime
    /// shape is `options::Value::String("true")`; must convert to JSON
    /// `true`, not the string `"true"`.
    #[test]
    fn convert_value_parses_string_backed_bool_field() {
        assert_eq!(
            field_type_for("hot_channel_protection_enabled"),
            Some(FieldType::Bool)
        );
        assert_eq!(
            convert_value(
                Some(FieldType::Bool),
                &options::Value::String("true".to_string())
            ),
            serde_json::json!(true)
        );
        assert_eq!(
            convert_value(
                Some(FieldType::Bool),
                &options::Value::String("false".to_string())
            ),
            serde_json::json!(false)
        );
    }

    /// Bool tolerance mirrors `modules/config.py`'s generic,
    /// field-type-driven parser (`Config._apply_override` /
    /// `update_runtime`): `true`/`1`/`yes`/`on` (case-insensitive) are
    /// truthy; everything else -- including "false" spellings and
    /// unparseable junk -- is false.
    #[test]
    fn convert_value_bool_field_accepts_python_truthy_spellings() {
        for truthy in ["true", "TRUE", "True", "1", "yes", "YES", "on", "On"] {
            assert_eq!(
                convert_value(
                    Some(FieldType::Bool),
                    &options::Value::String(truthy.to_string())
                ),
                serde_json::json!(true),
                "expected {truthy:?} to parse truthy"
            );
        }
        for falsy in ["false", "FALSE", "0", "no", "off", "garbage", ""] {
            assert_eq!(
                convert_value(
                    Some(FieldType::Bool),
                    &options::Value::String(falsy.to_string())
                ),
                serde_json::json!(false),
                "expected {falsy:?} to parse falsy"
            );
        }
    }

    /// A `Bool` field already backed by a native CLN `bool` option (the 28
    /// of 90 that aren't string-typed) passes through unconverted -- no
    /// double-parsing of an already-correct `Value::Boolean`.
    #[test]
    fn convert_value_native_bool_passes_through() {
        assert_eq!(
            convert_value(Some(FieldType::Bool), &options::Value::Boolean(true)),
            serde_json::json!(true)
        );
    }

    /// A `Float` field's existing conversion is unaffected by the
    /// Int/Bool fix -- regression guard.
    #[test]
    fn convert_value_still_parses_string_backed_float_field() {
        assert_eq!(
            convert_value(
                Some(FieldType::Float),
                &options::Value::String("0.20".to_string())
            ),
            serde_json::json!(0.20)
        );
    }
}
