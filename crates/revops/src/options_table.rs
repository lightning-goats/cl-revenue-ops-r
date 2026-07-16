//! The full Python `plugin.add_option(...)` surface, AST-extracted from
//! `cl-revenue-ops.py` into `fixtures/options.json` by
//! `tools/port/gen_options_table.py` (port worktree, branch `port`).
//!
//! `main.rs` walks [`load`] and registers every entry with the cln-plugin
//! builder under the shadow-name mapping (see [`shadow_name`]), so the Rust
//! port advertises the entire Python option surface without colliding with
//! the Python plugin's own option names when both run side by side.

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct OptDef {
    pub name: String,
    pub opt_type: String,
    pub default: serde_json::Value,
    pub description: String,
    pub dynamic: bool,
}

pub fn load() -> Vec<OptDef> {
    serde_json::from_str(include_str!("../../../fixtures/options.json"))
        .expect("embedded options.json is valid")
}

/// `revenue-ops-foo` -> shadow suffix `foo` (see design spec collision rule).
pub fn shadow_name(canonical: &str) -> String {
    let suffix = canonical.strip_prefix("revenue-ops-").unwrap_or(canonical);
    format!("revops-r-{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_embedded_table() {
        let opts = load();
        assert!(!opts.is_empty());
    }

    #[test]
    fn shadow_name_strips_canonical_prefix() {
        assert_eq!(shadow_name("revenue-ops-db-path"), "revops-r-db-path");
        assert_eq!(shadow_name("something-else"), "revops-r-something-else");
    }

    /// Guard test: `main.rs`'s `config_name_map()` and `register_python_options()`
    /// both derive the shadow/canonical suffix from a fixture entry's `name`
    /// by stripping a literal `"revenue-ops-"` prefix (`strip_prefix` /
    /// `removeprefix`, falling back to the whole name unchanged if the
    /// prefix is absent). If a future fixture regeneration ever emits a
    /// name that doesn't start with `"revenue-ops-"`, that fallback would
    /// silently succeed with the wrong suffix instead of failing loudly --
    /// this test catches that drift at fixture-load time instead.
    #[test]
    fn every_fixture_option_name_has_canonical_prefix() {
        let opts = load();
        assert!(!opts.is_empty());
        for opt in &opts {
            assert!(
                opt.name.starts_with("revenue-ops-"),
                "fixture option {:?} does not start with 'revenue-ops-'",
                opt.name
            );
        }
    }

    /// Guard test: all embedded fixture entries must have defaults that parse
    /// for their declared opt_type. Null defaults are allowed (they register
    /// as valueless options). This catches fixture drift at test time.
    #[test]
    fn fixture_defaults_parse_for_declared_type() {
        let opts = load();
        for opt in &opts {
            let parse_result = match opt.opt_type.as_str() {
                "int" => crate::as_int_default(&opt.default).is_some(),
                "bool" => crate::as_bool_default(&opt.default).is_some(),
                _ => crate::as_string_default(&opt.default).is_some(),
            };
            // All current entries have either:
            // - null default (parse_result = true, as no-default is correct)
            // - non-null default that successfully parses (parse_result = true)
            // If an entry has a non-null default that doesn't parse, this fails.
            let is_null = opt.default.is_null();
            assert!(
                is_null || parse_result,
                "option {} (type: {}) has non-null default that fails to parse: {:?}",
                opt.name,
                opt.opt_type,
                opt.default
            );
        }
    }
}
