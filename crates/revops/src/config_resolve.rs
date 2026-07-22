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

use crate::config_types::{self, FieldType};
use cln_plugin::options;
use cln_rpc::ClnRpc;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;

/// `listconfigs`' RPC timeout, matching hydration's own house pattern
/// (`crates/revops/src/hydration.rs`'s `RPC_TIMEOUT_SECONDS`, itself
/// `Config.rpc_timeout_seconds`'s default, modules/config.py:734).
const LISTCONFIGS_TIMEOUT_SECONDS: u64 = 15;

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

/// The four `revenue-r-config` suffixes whose registered CLN option name
/// (layer (b), used verbatim by [`python_option_name`]) does NOT match the
/// `Config` dataclass field name (layer (a)'s `config_overrides.key`) under
/// the naive `suffix.replace("-", "_")` transform -- the exact "Case 2" set
/// `tools/diff-harness/diff_config.py`'s `OPTION_ONLY_KEYS` comment
/// documents (both landed there as SKIPPED before this fix, since neither
/// harness nor this module had the remap). Verified directly against
/// `cl-revenue-ops.py`:
///   - `vegas-reflex` -> `enable_vegas_reflex` (line 2545)
///   - `vegas-decay` -> `vegas_decay_rate` (line 2546)
///   - `planner-max-fee-rate` -> `planner_max_fee_rate_sat_vb` (line 2560)
///   - `boltz-structural-budget-sats` -> `boltz_structural_budget_sats_per_day`
///     (line 2431)
///
/// `fixtures/config_types.json` (AST-extracted straight from the `Config`
/// dataclass) carries only field-name -> type, not this suffix -> field-name
/// mapping, so it can't be derived from the fixture; this is the "small
/// cited map" fallback.
const FIELD_NAME_OVERRIDES: [(&str, &str); 4] = [
    ("vegas-reflex", "enable_vegas_reflex"),
    ("vegas-decay", "vegas_decay_rate"),
    ("planner-max-fee-rate", "planner_max_fee_rate_sat_vb"),
    (
        "boltz-structural-budget-sats",
        "boltz_structural_budget_sats_per_day",
    ),
];

/// The `config_overrides.key` value for `key` (a `revenue-r-config`
/// suffix): the Python `Config` dataclass's snake_case field name, e.g.
/// `min-fee-ppm` -> `min_fee_ppm`. Matches `diff_config.py`'s own
/// `py_key = suffix.replace("-", "_")` transform for every key EXCEPT the
/// four in [`FIELD_NAME_OVERRIDES`], which need the explicit remap instead.
pub fn db_override_key(key: &str) -> String {
    for (suffix, field) in FIELD_NAME_OVERRIDES {
        if key == suffix {
            return field.to_string();
        }
    }
    key.replace('-', "_")
}

/// `IMMUTABLE_CONFIG_KEYS` (modules/config.py:22-25): `Config.load_overrides`
/// never applies a DB override to these fields even when a row exists, "to
/// stop `dry_run` from being overridden to hide actions" (the file's own
/// comment). `db_path` doesn't need an entry here -- it's already excluded
/// structurally, since the Rust `db-path` key is [`SELF_ONLY_KEYS`] (this
/// plugin's OWN db-path option, not reachable to Python's `db_path` field at
/// all; see that const's doc comment) -- but `dry-run` maps to a REAL Python
/// option (`revenue-ops-dry-run` -> `Config.dry_run`,
/// cl-revenue-ops.py:1417,2539) that DOES flow through layers (a)/(b)/(c)
/// here, so it needs an explicit skip of layer (a) to mirror Python's
/// safety rule.
const IMMUTABLE_KEYS: [&str; 1] = ["dry-run"];

/// Whether `key` (a `revenue-r-config` suffix) must never have a DB override
/// applied, mirroring `IMMUTABLE_CONFIG_KEYS`. See [`IMMUTABLE_KEYS`].
pub fn is_immutable_key(key: &str) -> bool {
    IMMUTABLE_KEYS.contains(&key)
}

/// Mirror of `Config._apply_override`'s type-conversion and validation gate
/// (modules/config.py:1015-1047), given `field` (the Python `Config` field
/// name, i.e. [`db_override_key`]'s output -- NOT the `revenue-r-config`
/// suffix) and `raw` (the override's stored string value).
///
/// Returns `Some(raw)` unchanged if the override is admissible (downstream
/// `config_types::convert_value` re-parses the same string the same way, so
/// handing back the untouched raw value is equivalent to Python keeping its
/// freshly `setattr`-assigned typed value), `None` if it must be skipped --
/// callers must then fall through to layer (b)/(c), exactly as Python's
/// `_apply_override` leaves the pre-existing (option-layer) value in place
/// on any of these same three failure modes:
///
///   - unparseable for the field's declared type (`int(value)`/`float(value)`
///     raise `ValueError`) -- `int`/`float` fields only; `bool`/`str` fields
///     can't fail to parse (Python's bool conversion is a permissive
///     `.lower() in (...)` membership test, never a raise).
///   - a non-finite float (`NaN`/`Infinity`) -- `math.isfinite` check,
///     AUDIT FIX C-2.
///   - out of [`config_types::field_range`] (`CONFIG_FIELD_RANGES`), or not
///     a recognized [`config_types::field_enum`] value
///     (`STRING_ENUM_VALID_VALUES`, case-insensitive).
///
/// `field_type_for(field)` defaulting to `FieldType::String` when `field`
/// has no typed metadata mirrors Python's own default:
/// `CONFIG_FIELD_TYPES.get(key, str)`.
pub fn validate_override(field: &str, raw: &str) -> Option<String> {
    let field_type = config_types::field_type_for(field).unwrap_or(FieldType::String);
    match field_type {
        FieldType::Bool => Some(raw.to_string()),
        FieldType::Int => {
            let parsed: i64 = raw.parse().ok()?;
            if !in_range(field, parsed as f64) {
                return None;
            }
            Some(raw.to_string())
        }
        FieldType::Float => {
            let parsed: f64 = raw.parse().ok()?;
            if !parsed.is_finite() {
                return None;
            }
            if !in_range(field, parsed) {
                return None;
            }
            Some(raw.to_string())
        }
        FieldType::String => match config_types::field_enum(field) {
            Some(valid) => {
                let lower = raw.to_lowercase();
                if valid.iter().any(|v| v == raw || v.to_lowercase() == lower) {
                    // Python lowercases the typed value once it clears the
                    // enum check (`_apply_override`: `typed_value =
                    // typed_value.lower()`).
                    Some(lower)
                } else {
                    None
                }
            }
            None => Some(raw.to_string()),
        },
    }
}

fn in_range(field: &str, value: f64) -> bool {
    match config_types::field_range(field) {
        Some((min, max)) => value >= min && value <= max,
        None => true,
    }
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
    match try_fetch_python_option_values(socket_path).await {
        Ok(map) => map,
        Err(e) => {
            eprintln!(
                "revops: listconfigs unavailable ({e}); revenue-r-config falls back to \
                 fixture defaults for the Python option layer"
            );
            HashMap::new()
        }
    }
}

/// Error-surfacing variant of [`fetch_python_option_values`], for callers
/// that must distinguish "fetched an empty/updated map" from "fetch
/// failed" (the [`PythonOptionCache`] refresh path keeps its previous
/// snapshot on failure rather than blanking it).
async fn try_fetch_python_option_values(
    socket_path: &Path,
) -> anyhow::Result<HashMap<String, options::Value>> {
    let body = call_listconfigs(socket_path, LISTCONFIGS_TIMEOUT_SECONDS).await?;
    Ok(parse_listconfigs_response(&body))
}

/// Shared, refreshable `listconfigs` snapshot (2026-07-22 audit M3).
///
/// Python re-reads `listconfigs` at the top of every boltz/planner cycle
/// (`_refresh_dynamic_config`, cl-revenue-ops.py:6597-6685) and updates
/// the live `config` object, so a `setconfig` on a `dynamic:true` option
/// takes effect without a plugin restart — and an init-time outage heals
/// on the next cycle. The one-shot init fetch this plugin previously
/// cached gave neither: the fee controller would run a whole window on
/// fixture defaults after a cold-start socket race, and dynamic options
/// silently stopped being dynamic. This cache is shared by every
/// consumer (`revenue-r-config` resolution and the fee scheduler's
/// per-cycle cfg resolution) and refreshed before each dispatched fee
/// cycle; a failed refresh KEEPS the last good snapshot.
#[derive(Clone, Default)]
pub struct PythonOptionCache {
    inner: std::sync::Arc<std::sync::RwLock<HashMap<String, options::Value>>>,
}

impl PythonOptionCache {
    /// An empty cache (no fetch attempted yet): resolution degrades to
    /// (a) DB override > (c) fixture default until the first successful
    /// refresh.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Clone of the current snapshot (short lock, no await inside).
    pub fn snapshot(&self) -> HashMap<String, options::Value> {
        self.inner.read().expect("option cache lock poisoned").clone()
    }

    /// Apply a fetch outcome: success replaces the snapshot wholesale
    /// (lightningd's resolved values are authoritative for layer (b));
    /// failure keeps the previous snapshot untouched. Returns whether the
    /// fetch succeeded. Split from [`Self::refresh`] so the keep-on-
    /// failure contract is unit-testable without a socket.
    pub fn apply_fetch(
        &self,
        fetched: Result<HashMap<String, options::Value>, String>,
    ) -> bool {
        match fetched {
            Ok(map) => {
                *self.inner.write().expect("option cache lock poisoned") = map;
                true
            }
            Err(e) => {
                eprintln!(
                    "revops: listconfigs refresh failed ({e}); keeping the previous \
                     Python option snapshot"
                );
                false
            }
        }
    }

    /// Re-fetch `listconfigs` and apply per [`Self::apply_fetch`].
    pub async fn refresh(&self, socket_path: &Path) -> bool {
        let fetched = try_fetch_python_option_values(socket_path)
            .await
            .map_err(|e| format!("{e:#}"));
        self.apply_fetch(fetched)
    }
}

/// Wrapped in `revops_rpc::call_with_timeout`, matching hydration's house
/// pattern for every lightningd RPC call this plugin makes
/// (`crates/revops/src/hydration.rs`'s `fetch_settled_forwards`) -- a
/// hung/slow `listconfigs` (e.g. lightningd under heavy load at plugin
/// init) must degrade to the empty-map fallback within a bounded time,
/// never block plugin startup indefinitely.
///
/// `timeout_secs` is a parameter (rather than reading
/// `LISTCONFIGS_TIMEOUT_SECONDS` directly) purely so this function's own
/// `#[cfg(test)]` seam can inject a 0-second budget and assert the actual
/// timeout wiring fires deterministically, without a test that spends 15
/// real seconds waiting -- the only production caller,
/// [`fetch_python_option_values`], always passes the real
/// `LISTCONFIGS_TIMEOUT_SECONDS` (15s).
async fn call_listconfigs(socket_path: &Path, timeout_secs: u64) -> anyhow::Result<Value> {
    revops_rpc::call_with_timeout("listconfigs", timeout_secs, async {
        let mut rpc = ClnRpc::new(socket_path).await.map_err(|e| {
            anyhow::anyhow!(
                "connect lightning-rpc socket {}: {e}",
                socket_path.display()
            )
        })?;
        rpc.call_raw::<Value, Value>("listconfigs", &json!({}))
            .await
            .map_err(|e| anyhow::anyhow!("listconfigs RPC error: {e}"))
    })
    .await
    .map_err(anyhow::Error::from)
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

    /// CRITICAL 2: the four suffixes whose `Config` field name doesn't match
    /// the naive `suffix.replace("-", "_")` transform -- verified against
    /// `cl-revenue-ops.py:2431,2545,2546,2560`.
    #[test]
    fn db_override_key_applies_field_name_overrides() {
        assert_eq!(db_override_key("vegas-reflex"), "enable_vegas_reflex");
        assert_eq!(db_override_key("vegas-decay"), "vegas_decay_rate");
        assert_eq!(
            db_override_key("planner-max-fee-rate"),
            "planner_max_fee_rate_sat_vb"
        );
        assert_eq!(
            db_override_key("boltz-structural-budget-sats"),
            "boltz_structural_budget_sats_per_day"
        );
    }

    /// IMPORTANT 4 / `IMMUTABLE_CONFIG_KEYS` (modules/config.py:22-25):
    /// `dry-run` is flagged immutable; every other key (including a
    /// similarly-named but distinct key) is not.
    #[test]
    fn is_immutable_key_flags_only_dry_run() {
        assert!(is_immutable_key("dry-run"));
        assert!(!is_immutable_key("planner-dry-run"));
        assert!(!is_immutable_key("min-fee-ppm"));
    }

    // -- validate_override: CRITICAL 1 (Config._apply_override port) --

    /// An unparseable int override is skipped (falls through), matching
    /// Python's `int(value)` raising `ValueError` inside `_apply_override`'s
    /// try/except.
    #[test]
    fn validate_override_rejects_unparseable_int() {
        assert_eq!(validate_override("daily_budget_sats", "not-a-number"), None);
    }

    /// An out-of-range int override is skipped -- `daily_budget_sats`'s
    /// `CONFIG_FIELD_RANGES` entry is `(0, 10_000_000)`.
    #[test]
    fn validate_override_rejects_out_of_range_int() {
        assert_eq!(validate_override("daily_budget_sats", "50000000"), None);
        assert_eq!(
            validate_override("daily_budget_sats", "5000"),
            Some("5000".to_string())
        );
    }

    /// An out-of-range float override is skipped -- `min_fee_ppm`'s range is
    /// `(5, 100000)` (CRITICAL-02 economic-viability floor).
    #[test]
    fn validate_override_rejects_out_of_range_via_min_fee_ppm() {
        assert_eq!(validate_override("min_fee_ppm", "1"), None);
        assert_eq!(
            validate_override("min_fee_ppm", "40"),
            Some("40".to_string())
        );
    }

    /// A non-finite float override (NaN/Infinity) is skipped -- AUDIT FIX
    /// C-2 in `_apply_override`.
    #[test]
    fn validate_override_rejects_non_finite_float() {
        assert_eq!(validate_override("vegas_decay_rate", "nan"), None);
        assert_eq!(validate_override("vegas_decay_rate", "inf"), None);
        assert_eq!(
            validate_override("vegas_decay_rate", "0.5"),
            Some("0.5".to_string())
        );
    }

    /// An unparseable float override is skipped the same way.
    #[test]
    fn validate_override_rejects_unparseable_float() {
        assert_eq!(validate_override("vegas_decay_rate", "not-a-float"), None);
    }

    /// An invalid enum override (`fee_profile` only accepts
    /// `STRING_ENUM_VALID_VALUES["fee_profile"]`) is skipped.
    #[test]
    fn validate_override_rejects_invalid_enum() {
        assert_eq!(validate_override("fee_profile", "aggressive"), None);
        assert_eq!(
            validate_override("fee_profile", "active"),
            Some("active".to_string())
        );
        // Case-insensitive match is admissible, mirroring
        // `_apply_override`'s `.lower()` comparison.
        assert_eq!(
            validate_override("fee_profile", "ACTIVE"),
            Some("active".to_string())
        );
    }

    /// A bool override is never rejected for parse failure (Python's bool
    /// conversion is a permissive membership test, not a raise) and bool
    /// fields have no range/enum entries to fail either.
    #[test]
    fn validate_override_bool_field_always_passes_through() {
        assert_eq!(
            validate_override("enable_vegas_reflex", "true"),
            Some("true".to_string())
        );
        assert_eq!(
            validate_override("enable_vegas_reflex", "garbage"),
            Some("garbage".to_string())
        );
    }

    /// A field with no typed metadata at all defaults to `String` (Python's
    /// `CONFIG_FIELD_TYPES.get(key, str)`) and passes through unless it also
    /// has an enum constraint.
    #[test]
    fn validate_override_unknown_field_defaults_to_string() {
        assert_eq!(
            validate_override("not-a-real-config-field", "anything"),
            Some("anything".to_string())
        );
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

    /// IMPORTANT 3: `call_listconfigs` is wrapped in
    /// `revops_rpc::call_with_timeout` -- a connection that accepts but
    /// never replies must time out rather than hang forever. Injects a
    /// 0-second budget (same trick as
    /// `revops-rpc/tests/timeout.rs::timeout_error_string_matches_python`)
    /// so this asserts the real timeout wiring fires without the test
    /// suite waiting out the production 15s budget.
    #[tokio::test]
    async fn call_listconfigs_times_out_on_a_hanging_response() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lightning-rpc");
        // Bind but deliberately never `accept()`/write a reply -- the
        // client's connect() succeeds (AF_UNIX doesn't require an accept()
        // to complete a local connect), then `call_raw` hangs forever
        // waiting for a response that never comes. Held alive for the
        // test's duration so the socket isn't torn down mid-await.
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let held = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            drop(listener);
        });

        let err = call_listconfigs(&path, 0).await.unwrap_err();
        assert_eq!(err.to_string(), "RPC timeout after 0s on listconfigs");

        held.abort();
    }
}
