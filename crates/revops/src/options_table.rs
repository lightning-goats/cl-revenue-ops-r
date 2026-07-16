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
}
