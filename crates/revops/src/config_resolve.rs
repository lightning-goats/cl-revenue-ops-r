//! Effective-value resolution for `revenue-r-config`, mirroring Python's
//! exact 3-layer precedence at `Config.load_overrides`/`Config._apply_override`
//! (`modules/config.py:907-914,1015-1047`) and its call sites in
//! `cl-revenue-ops.py` (`Config(**config_kwargs)` constructed at line 2605,
//! `config.load_overrides(database)` called on top at line 2764):
//!
//!   (a) DB override      -- a row in the production DB's `config_overrides`
//!                            table (`revops_db::queries::config_override`),
//!                            keyed by the Python `Config` field name
//!                            (snake_case). Applied LAST in Python (inside
//!                            `load_overrides`, via `setattr`), so it wins
//!                            over everything else.
//!   (b) listconfigs value -- lightningd's live resolved value for the
//!                            PYTHON option name (`revenue-ops-<suffix>`),
//!                            fetched ONCE at init via a single
//!                            `listconfigs` RPC call and cached in `State`.
//!                            This is what `Config(**config_kwargs)` was
//!                            constructed from in Python -- it already
//!                            reflects either the operator's config-file
//!                            value or the option's own registered default
//!                            (lightningd resolves that before the plugin
//!                            ever sees `options`).
//!   (c) fixture default   -- this Rust plugin's OWN registered (shadow- or
//!                            canonical-named) option value. Before this
//!                            module existed, this was the ONLY layer
//!                            `revenue-r-config` ever consulted -- the bug
//!                            this module fixes: the operator's
//!                            `revenue-ops-*` config lines set the PYTHON
//!                            plugin's options, never this plugin's
//!                            shadow-named `revops-r-*` options, so (c)
//!                            alone always reported the Rust fixture
//!                            default instead of the operator's real value.
//!
//! **No divergence from Python's own precedence was found.** Python never
//! re-applies the option value after `load_overrides` runs, and
//! `load_overrides` is the only place a DB override is applied -- so the
//! order really is exactly (a) > (b) > (c), matching this module's design
//! one-for-one. (`db_path`/`dry_run` are `IMMUTABLE_CONFIG_KEYS`, meaning
//! Python's `load_overrides` explicitly skips applying a DB override to
//! them even if one exists in the table -- this module mirrors that by
//! never attempting (a)/(b) lookups for `db-path` at all, since that key
//! isn't even a Python `Config` field reachable via `revenue-r-config`
//! here; see [`python_option_name`]'s `observer`/`db-path`/
//! `observer-db-path` exclusions.)

use cln_plugin::options;
use cln_rpc::ClnRpc;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;

/// `revenue-r-config` keys with NO Python option counterpart at all --
/// `observer`/`db-path`/`observer-db-path` are this Rust plugin's own
/// concepts (shadow-DB-probe wiring), never registered by
/// `cl-revenue-ops.py`. DB-override and listconfigs lookups are both
/// meaningless for these three; only the fixture-default layer (c)
/// applies, exactly as before this module existed.
const SELF_ONLY_KEYS: [&str; 3] = ["observer", "db-path", "observer-db-path"];

/// The Python plugin's registered CLN option name for `key` (a
/// `revenue-r-config` suffix, e.g. `min-fee-ppm`), or `None` for the three
/// Rust-only keys in [`SELF_ONLY_KEYS`].
pub fn python_option_name(key: &str) -> Option<String> {
    if SELF_ONLY_KEYS.contains(&key) {
        None
    } else {
        Some(format!("revenue-ops-{key}"))
    }
}

/// The `config_overrides.key` value for `key` (a `revenue-r-config`
/// suffix): the Python `Config` dataclass's snake_case field name, e.g.
/// `min-fee-ppm` -> `min_fee_ppm`. Matches `diff_config.py`'s own
/// `py_key = suffix.replace("-", "_")` transform exactly (same fixture,
/// same two-namespace split documented there).
pub fn db_override_key(key: &str) -> String {
    key.replace('-', "_")
}

/// Resolve which of the three layers wins, given each already fetched:
/// `db_override` (a) beats `python_value` (b) beats `fixture_value` (c).
/// Pure and total -- `None` only when all three are `None` (an unset,
/// no-default option with no DB override and no listconfigs entry, the
/// same "no value at all" case the pre-existing fixture-only path already
/// handled).
pub fn resolve_option_value(
    db_override: Option<options::Value>,
    python_value: Option<options::Value>,
    fixture_value: Option<options::Value>,
) -> Option<options::Value> {
    db_override.or(python_value).or(fixture_value)
}

/// Fetch every `revenue-ops-*` entry from a single `listconfigs` RPC call
/// (no filter arg -- lists everything lightningd knows about), keyed by
/// the FULL Python option name (e.g. `revenue-ops-min-fee-ppm`) with its
/// resolved value reconstructed as an `options::Value`.
///
/// Never errors to the caller: any RPC failure (socket missing, lightningd
/// not ready, malformed response) is logged to stderr and this returns an
/// empty map, which degrades resolution to (c) fixture-default only --
/// the exact pre-existing Phase 1a/1b behavior. A `listconfigs` outage at
/// init must never block plugin startup or take `revenue-r-config` down.
pub async fn fetch_python_option_values(socket_path: &Path) -> HashMap<String, options::Value> {
    match call_listconfigs(socket_path).await {
        Ok(body) => parse_listconfigs_response(&body),
        Err(e) => {
            eprintln!(
                "revops: listconfigs unavailable ({e}); revenue-r-config falls back to \
                 fixture defaults for the Python option layer"
            );
            HashMap::new()
        }
    }
}

async fn call_listconfigs(socket_path: &Path) -> anyhow::Result<Value> {
    let mut rpc = ClnRpc::new(socket_path).await.map_err(|e| {
        anyhow::anyhow!(
            "connect lightning-rpc socket {}: {e}",
            socket_path.display()
        )
    })?;
    rpc.call_raw::<Value, Value>("listconfigs", &json!({}))
        .await
        .map_err(|e| anyhow::anyhow!("listconfigs RPC error: {e}"))
}

/// Pure parse: extract `revenue-ops-*` entries from a raw `listconfigs`
/// response body into `name -> options::Value`. Confirmed against a live
/// node (lnnode): every `revenue-ops-*` entry reports exactly one of
/// `value_str`/`value_int`/`value_bool` (118 of 119 registered Python
/// options are CLN `string`-typed and report `value_str`; the remaining
/// one, `reservation-timeout-hours`, is CLN `int`-typed and reports
/// `value_int`) -- this function handles all three shapes generically so
/// it isn't tied to today's exact type split.
pub fn parse_listconfigs_response(body: &Value) -> HashMap<String, options::Value> {
    let mut out = HashMap::new();
    let Some(configs) = body.get("configs").and_then(|c| c.as_object()) else {
        return out;
    };
    for (name, entry) in configs {
        if !name.starts_with("revenue-ops-") {
            continue;
        }
        if let Some(v) = extract_value(entry) {
            out.insert(name.clone(), v);
        }
    }
    out
}

/// One `listconfigs` config entry -> `options::Value`, picking whichever
/// `value_*` field CLN reported. `None` for an entry with none of the
/// three (e.g. a valueless/unset option) -- callers treat that the same
/// as "no listconfigs entry at all" (layer (b) doesn't apply).
fn extract_value(entry: &Value) -> Option<options::Value> {
    if let Some(s) = entry.get("value_str").and_then(|v| v.as_str()) {
        return Some(options::Value::String(s.to_string()));
    }
    if let Some(i) = entry.get("value_int").and_then(|v| v.as_i64()) {
        return Some(options::Value::Integer(i));
    }
    if let Some(b) = entry.get("value_bool").and_then(|v| v.as_bool()) {
        return Some(options::Value::Boolean(b));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_status::option_value_to_json;
    use serde_json::json;

    fn json_of(v: Option<options::Value>) -> Value {
        option_value_to_json(v.as_ref())
    }

    #[test]
    fn python_option_name_maps_suffix_to_full_name() {
        assert_eq!(
            python_option_name("min-fee-ppm"),
            Some("revenue-ops-min-fee-ppm".to_string())
        );
    }

    #[test]
    fn python_option_name_excludes_self_only_keys() {
        assert_eq!(python_option_name("observer"), None);
        assert_eq!(python_option_name("db-path"), None);
        assert_eq!(python_option_name("observer-db-path"), None);
    }

    #[test]
    fn db_override_key_converts_hyphens_to_underscores() {
        assert_eq!(db_override_key("daily-budget-sats"), "daily_budget_sats");
        assert_eq!(db_override_key("min_fee_ppm"), "min_fee_ppm");
    }

    /// (a) DB override wins over everything, even when (b)/(c) both have a
    /// value too -- the live-evidence bug this module fixes (operator set
    /// `daily-budget-sats` via `revenue-config set`, DB override 1000 vs.
    /// listconfigs 5000 vs. fixture default 5000).
    #[test]
    fn db_override_wins_over_python_and_fixture() {
        let resolved = resolve_option_value(
            Some(options::Value::String("1000".to_string())),
            Some(options::Value::String("5000".to_string())),
            Some(options::Value::String("5000".to_string())),
        );
        assert_eq!(json_of(resolved), json!("1000"));
    }

    /// (b) listconfigs value wins over (c) fixture default when there is
    /// no DB override -- the live-evidence bug's primary case (operator
    /// set `min-fee-ppm 40` in lightningd's config for the PYTHON plugin;
    /// no `revenue-config set` override exists at all).
    #[test]
    fn python_value_wins_over_fixture_when_no_db_override() {
        let resolved = resolve_option_value(
            None,
            Some(options::Value::String("40".to_string())),
            Some(options::Value::String("10".to_string())),
        );
        assert_eq!(json_of(resolved), json!("40"));
    }

    /// (c) fixture default is the final fallback when neither (a) nor (b)
    /// has anything -- e.g. `listconfigs` was unavailable at init, or the
    /// key isn't a real Python option at all (an `OPTION_ONLY_KEYS`-style
    /// gap) so listconfigs never reported it.
    #[test]
    fn fixture_default_is_final_fallback() {
        let resolved = resolve_option_value(None, None, Some(options::Value::Integer(2000)));
        assert_eq!(json_of(resolved), json!(2000));
    }

    #[test]
    fn all_three_absent_resolves_to_none() {
        assert!(resolve_option_value(None, None, None).is_none());
    }

    #[test]
    fn parse_listconfigs_response_filters_to_revenue_ops_prefix_only() {
        let body = json!({
            "configs": {
                "revenue-ops-min-fee-ppm": {"value_str": "40", "source": "/config:1"},
                "bind-addr": {"value_str": "127.0.0.1", "source": "default"},
            }
        });
        let map = parse_listconfigs_response(&body);
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("revenue-ops-min-fee-ppm"));
        assert!(!map.contains_key("bind-addr"));
    }

    /// Live-node evidence (lnnode, 2026-07-16): all 119 `revenue-ops-*`
    /// entries report `value_str`, e.g.
    /// `{"value_str": "50", "source": "/data/lightningd/config:35", ...}`
    /// for `revenue-ops-min-fee-ppm`.
    #[test]
    fn parse_listconfigs_response_reads_value_str() {
        let body = json!({
            "configs": {
                "revenue-ops-min-fee-ppm": {
                    "value_str": "50",
                    "source": "/data/lightningd/config:35",
                    "plugin": "/data/lightningd/plugins/cl_revenue_ops/cl-revenue-ops.py"
                }
            }
        });
        let map = parse_listconfigs_response(&body);
        assert_eq!(
            json_of(map.get("revenue-ops-min-fee-ppm").cloned()),
            json!("50")
        );
    }

    /// Live-node evidence: the one CLN `int`-typed option
    /// (`reservation-timeout-hours`) reports `value_int` instead.
    #[test]
    fn parse_listconfigs_response_reads_value_int() {
        let body = json!({
            "configs": {
                "revenue-ops-reservation-timeout-hours": {
                    "value_int": 4,
                    "source": "default"
                }
            }
        });
        let map = parse_listconfigs_response(&body);
        assert_eq!(
            json_of(map.get("revenue-ops-reservation-timeout-hours").cloned()),
            json!(4)
        );
    }

    #[test]
    fn parse_listconfigs_response_reads_value_bool() {
        let body = json!({
            "configs": {
                "revenue-ops-some-flag": {"value_bool": true, "source": "default"}
            }
        });
        let map = parse_listconfigs_response(&body);
        assert_eq!(
            json_of(map.get("revenue-ops-some-flag").cloned()),
            json!(true)
        );
    }

    #[test]
    fn parse_listconfigs_response_missing_configs_key_is_empty_map() {
        let map = parse_listconfigs_response(&json!({"unexpected": "shape"}));
        assert!(map.is_empty());
    }

    #[test]
    fn parse_listconfigs_response_entry_with_no_value_field_is_skipped() {
        let body = json!({
            "configs": {
                "revenue-ops-unset-option": {"source": "default"}
            }
        });
        let map = parse_listconfigs_response(&body);
        assert!(map.is_empty());
    }
}
