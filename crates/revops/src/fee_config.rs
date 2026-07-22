//! Per-cycle `FeeCfgSnapshot` resolver (Phase 4b Task 1).
//!
//! `revops_fees::cycle::FeeCfgSnapshot` is a frozen 22-field contract whose
//! `Default` mirrors Python's `Config` dataclass defaults (drift-guard
//! tested in `revops-fees`' own `tests/cycle.rs`). This module resolves a
//! LIVE snapshot of those 22 fields every fee cycle, using the exact
//! 3-layer precedence `revenue-r-config`'s handler already implements
//! (`crate::config_resolve`, `main.rs`'s `revenue-r-config` rpcmethod):
//!
//!   (a) DB override   -- `revops_db::queries::config_override` +
//!                         `config_resolve::validate_override`. Applied
//!                         LAST in Python (`Config.load_overrides`), so it
//!                         wins over everything else.
//!   (b) listconfigs    -- the init-cached `python_option_values` map
//!                         (`State`'s single `listconfigs` snapshot,
//!                         `config_resolve::fetch_python_option_values`).
//!   (c) struct default -- `FeeCfgSnapshot::default()`'s field value. This
//!                         layer is NOT the Rust plugin's own registered
//!                         shadow option (unlike `revenue-r-config`'s
//!                         layer (c)) -- `resolve_fee_cfg` has no `Plugin`
//!                         handle, only `db`/`python_option_values`, so the
//!                         frozen, Python-verified struct default IS the
//!                         fixture-default layer here.
//!
//! **DB-override-only keys** (`paused`, `authority_level`,
//! `econ_governor_fees_enabled` -- ledger note "17 PUBLIC_RUNTIME_KEYS", no
//! CLN option exists for any of the three) skip layer (b) ENTIRELY: (a) ->
//! (c), never consulting `python_option_values` even if a (fake/stale) map
//! entry happens to exist for one of them.
//!
//! `enable_dynamic_htlcmax` is the one field that is NOT converted through
//! `config_types::typed_value`'s type-driven coercion: it keeps whatever
//! RAW resolved value each layer produced (a genuine bool vs. a string like
//! `"false"`, which Python's own narrow truthiness check
//! (`admission::is_enabled`) treats differently -- a non-empty string is
//! not automatically false). Coercing it here would destroy that
//! distinction before it ever reaches `admission::HtlcmaxCfg`.
//!
//! The scheduler (T6) calls [`resolve_fee_cfg`] at the top of EVERY cycle
//! -- per-cycle resolution is what makes a runtime `revenue-config set`
//! change on the Python side (which lands in `config_overrides`, layer (a))
//! visible to the Rust controller without a restart.

use crate::config_resolve::{self};
use crate::config_types;
use crate::rpc_status::option_value_to_json;
use cln_plugin::options::Value as OptValue;
use revops_db::actor::DbHandle;
use revops_db::queries;
use revops_fees::cycle::FeeCfgSnapshot;
use std::collections::HashMap;

/// `revenue-r-config` suffixes with NO Python option counterpart at all --
/// layer (b) (`python_option_values`) must never be consulted for these,
/// even if the caller's map happens to contain a matching entry. See the
/// module doc comment.
const DB_OVERRIDE_ONLY_SUFFIXES: [&str; 3] =
    ["paused", "authority-level", "econ-governor-fees-enabled"];

fn is_db_override_only(suffix: &str) -> bool {
    DB_OVERRIDE_ONLY_SUFFIXES.contains(&suffix)
}

/// Layer (a): the raw, already-validated DB override string for `suffix`
/// (a `revenue-r-config` suffix, e.g. `"max-fee-ppm"`), or `None` if no row
/// exists, the row fails `validate_override`, `suffix` is
/// `config_resolve::is_immutable_key` (mirrors `revenue-r-config`'s own
/// skip, though none of `FeeCfgSnapshot`'s 22 fields are immutable today),
/// or there is no DB handle at all (e.g. tests, or a plugin that hasn't
/// finished init).
async fn db_layer(db: Option<&DbHandle>, suffix: &str) -> Option<String> {
    if config_resolve::is_immutable_key(suffix) {
        return None;
    }
    let handle = db?;
    let field = config_resolve::db_override_key(suffix);
    let raw = match queries::config_override(handle, &field).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("revops: fee_config db_override query failed for {field}: {e}");
            None
        }
    }?;
    config_resolve::validate_override(&field, &raw)
}

/// Layer (b): the cached `listconfigs` value for `suffix`, or `None` for a
/// [`DB_OVERRIDE_ONLY_SUFFIXES`] key (skip entirely -- no CLN option to
/// look up), a `config_resolve::SELF_ONLY_KEYS`-style key (none of
/// `FeeCfgSnapshot`'s fields are, but `python_option_name` still handles
/// it), or a suffix with nothing in the map (the common case for
/// `high-liquidity-threshold`/`base-fee-msat`, which Python never
/// registers as a CLN option at all -- `listconfigs` never reports them,
/// so this naturally falls through to (c) without special-casing).
fn python_layer(
    suffix: &str,
    python_option_values: &HashMap<String, OptValue>,
) -> Option<OptValue> {
    if is_db_override_only(suffix) {
        return None;
    }
    let name = config_resolve::python_option_name(suffix)?;
    python_option_values.get(&name).cloned()
}

/// (a) or (b), whichever resolves first -- `None` means "fall through to
/// the struct default" (layer (c), handled by each typed `resolve_*`
/// helper below).
async fn resolve_raw(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
    suffix: &str,
) -> Option<OptValue> {
    match db_layer(db, suffix).await {
        Some(raw) => Some(OptValue::String(raw)),
        None => python_layer(suffix, python_option_values),
    }
}

async fn resolve_int(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
    suffix: &str,
    default: i64,
) -> i64 {
    let field = config_resolve::db_override_key(suffix);
    match resolve_raw(db, python_option_values, suffix).await {
        Some(raw) => config_types::typed_value(&field, &raw)
            .as_i64()
            .unwrap_or(default),
        None => default,
    }
}

async fn resolve_float(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
    suffix: &str,
    default: f64,
) -> f64 {
    let field = config_resolve::db_override_key(suffix);
    match resolve_raw(db, python_option_values, suffix).await {
        Some(raw) => config_types::typed_value(&field, &raw)
            .as_f64()
            .unwrap_or(default),
        None => default,
    }
}

async fn resolve_bool(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
    suffix: &str,
    default: bool,
) -> bool {
    let field = config_resolve::db_override_key(suffix);
    // Layer-aware casts (2026-07-22 audit M2): a DB override row goes
    // through `_apply_override`'s generic tolerant parser
    // (`config_types::typed_value`), but a listconfigs value goes through
    // the field's PYTHON STARTUP cast -- the two disagree on '1'/'yes'/
    // 'on' for strict fields, and e.g. vegas-reflex=1 flipping the wrong
    // way desyncs the shared per-cycle RNG stream from the oracle.
    if let Some(raw) = db_layer(db, suffix).await {
        return config_types::typed_value(&field, &OptValue::String(raw))
            .as_bool()
            .unwrap_or(default);
    }
    match python_layer(suffix, python_option_values) {
        Some(OptValue::String(s)) => config_types::python_startup_bool(&field, &s),
        Some(OptValue::Boolean(b)) => b,
        Some(_) | None => default,
    }
}

async fn resolve_string(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
    suffix: &str,
    default: String,
) -> String {
    let field = config_resolve::db_override_key(suffix);
    match resolve_raw(db, python_option_values, suffix).await {
        Some(raw) => config_types::typed_value(&field, &raw)
            .as_str()
            .map(str::to_string)
            .unwrap_or(default),
        None => default,
    }
}

async fn resolve_string_opt(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
    suffix: &str,
    default: Option<String>,
) -> Option<String> {
    match resolve_raw(db, python_option_values, suffix).await {
        Some(raw) => {
            let field = config_resolve::db_override_key(suffix);
            config_types::typed_value(&field, &raw)
                .as_str()
                .map(str::to_string)
                .or(default)
        }
        None => default,
    }
}

/// `enable_dynamic_htlcmax` only: resolves like every other field, but
/// converts the winning raw `OptValue` straight to JSON via
/// `option_value_to_json` (a `String`/`Integer`/`Boolean` -> matching JSON
/// scalar, no field-type-driven coercion) rather than
/// `config_types::typed_value`, which would collapse a DB override string
/// like `"false"` into `Value::Bool(false)` and destroy the raw/typed
/// distinction `admission::is_enabled` depends on.
async fn resolve_raw_json(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
    suffix: &str,
    default: serde_json::Value,
) -> serde_json::Value {
    match resolve_raw(db, python_option_values, suffix).await {
        Some(raw) => option_value_to_json(Some(&raw)),
        None => default,
    }
}

/// Resolve a live `FeeCfgSnapshot` for the current cycle. See the module
/// doc comment for the 3-layer precedence and the DB-override-only /
/// raw-passthrough exceptions.
pub async fn resolve_fee_cfg(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
) -> FeeCfgSnapshot {
    let default = FeeCfgSnapshot::default();
    FeeCfgSnapshot {
        min_fee_ppm: resolve_int(db, python_option_values, "min-fee-ppm", default.min_fee_ppm)
            .await,
        max_fee_ppm: resolve_int(db, python_option_values, "max-fee-ppm", default.max_fee_ppm)
            .await,
        min_fee_ppm_saturated: resolve_int(
            db,
            python_option_values,
            "min-fee-ppm-saturated",
            default.min_fee_ppm_saturated,
        )
        .await,
        fee_interval: resolve_int(
            db,
            python_option_values,
            "fee-interval",
            default.fee_interval,
        )
        .await,
        flow_interval: resolve_int(
            db,
            python_option_values,
            "flow-interval",
            default.flow_interval,
        )
        .await,
        htlc_congestion_threshold: resolve_float(
            db,
            python_option_values,
            "htlc-congestion-threshold",
            default.htlc_congestion_threshold,
        )
        .await,
        market_fee_mode: resolve_string(
            db,
            python_option_values,
            "market-fee-mode",
            default.market_fee_mode.clone(),
        )
        .await,
        drain_fee_discount_max: resolve_float(
            db,
            python_option_values,
            "drain-fee-discount-max",
            default.drain_fee_discount_max,
        )
        .await,
        high_liquidity_threshold: resolve_float(
            db,
            python_option_values,
            "high-liquidity-threshold",
            default.high_liquidity_threshold,
        )
        .await,
        fee_profile: resolve_string(
            db,
            python_option_values,
            "fee-profile",
            default.fee_profile.clone(),
        )
        .await,
        base_fee_msat: resolve_int(
            db,
            python_option_values,
            "base-fee-msat",
            default.base_fee_msat,
        )
        .await,
        enable_vegas_reflex: resolve_bool(
            db,
            python_option_values,
            "vegas-reflex",
            default.enable_vegas_reflex,
        )
        .await,
        enable_dynamic_htlcmax: resolve_raw_json(
            db,
            python_option_values,
            "enable-dynamic-htlcmax",
            default.enable_dynamic_htlcmax.clone(),
        )
        .await,
        htlcmax_source_pct: resolve_float(
            db,
            python_option_values,
            "htlcmax-source-pct",
            default.htlcmax_source_pct,
        )
        .await,
        htlcmax_sink_pct: resolve_float(
            db,
            python_option_values,
            "htlcmax-sink-pct",
            default.htlcmax_sink_pct,
        )
        .await,
        htlcmax_balanced_pct: resolve_float(
            db,
            python_option_values,
            "htlcmax-balanced-pct",
            default.htlcmax_balanced_pct,
        )
        .await,
        paused: resolve_bool(db, python_option_values, "paused", default.paused).await,
        node_drain_bias_enabled: resolve_bool(
            db,
            python_option_values,
            "node-drain-bias-enabled",
            default.node_drain_bias_enabled,
        )
        .await,
        node_drain_bias_max: resolve_float(
            db,
            python_option_values,
            "node-drain-bias-max",
            default.node_drain_bias_max,
        )
        .await,
        receivable_ratio_target: resolve_float(
            db,
            python_option_values,
            "receivable-ratio-target",
            default.receivable_ratio_target,
        )
        .await,
        receivable_ratio_floor: resolve_float(
            db,
            python_option_values,
            "receivable-ratio-floor",
            default.receivable_ratio_floor,
        )
        .await,
        econ_governor_fees_enabled: resolve_bool(
            db,
            python_option_values,
            "econ-governor-fees-enabled",
            default.econ_governor_fees_enabled,
        )
        .await,
        authority_level: resolve_string_opt(
            db,
            python_option_values,
            "authority-level",
            default.authority_level.clone(),
        )
        .await,
    }
}

/// Per-cycle resolution of the `neighbor_median_min_competitors` key
/// through the SAME 3-layer precedence as every `FeeCfgSnapshot` field
/// ((a) DB override -> (b) cached `listconfigs` value -> (c) the Python
/// `Config` default of `2`), returned as a typed `serde_json::Value` (the
/// fixture types the field `int`, so a raw override/option string like
/// `"3"` comes back as JSON `3`). NOT routed through `FeeCfgSnapshot` --
/// the struct is a frozen 22-field contract (see [`resolve_min_competitors`]);
/// the T6 scheduler resolves this each cycle and validates the result
/// through [`resolve_min_competitors`] before threading it into the
/// cycle's `CycleDeps::min_competitors`.
pub async fn resolve_neighbor_median_min_competitors(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
) -> serde_json::Value {
    let suffix = "neighbor-median-min-competitors";
    match resolve_raw(db, python_option_values, suffix).await {
        Some(raw) => {
            let field = config_resolve::db_override_key(suffix);
            config_types::typed_value(&field, &raw)
        }
        // Python `Config.neighbor_median_min_competitors` default
        // (config.py, mirrored by fixtures/options.json's default "2").
        None => serde_json::json!(2),
    }
}

/// Validates [`resolve_neighbor_median_min_competitors`]'s output into the
/// usable competitor-count threshold `market::neighbor_fee_median`/
/// `neighbor_fee_percentile` now take as an explicit parameter (Phase 4b
/// Task 8a -- replaces the old Task 8 verify==3 gate, since production
/// runs with the DB-resolved value `2`, not the baked `3`; changing
/// production to `3` is out of scope, a phase-global constraint).
///
/// `Ok(n)` for any JSON integer `n > 0`. `Err` (never silently defaulted --
/// the T6 scheduler's fail-closed rule for the dry-run window) when the
/// resolved value is missing, non-integer (including a numeric STRING --
/// `resolve_neighbor_median_min_competitors` already runs the value
/// through `config_types::typed_value`, so a well-formed override/option
/// always comes back as a genuine JSON number; a JSON string surviving to
/// here means the typed conversion itself failed), or non-positive. This
/// is intentionally stricter than Python's own inline `int(getattr(cfg,
/// ..., 3) or 3)` (which would coerce a falsy/negative reading rather than
/// refuse) -- the dry-run window prefers a loud skip over silently running
/// with a nonsensical competitor-count gate.
pub fn resolve_min_competitors(resolved: &serde_json::Value) -> Result<usize, String> {
    match resolved.as_i64() {
        Some(n) if n > 0 => Ok(n as usize),
        Some(n) => Err(format!(
            "neighbor_median_min_competitors resolved to non-positive value {n}"
        )),
        None => Err(format!(
            "neighbor_median_min_competitors resolved to a non-integer value: {resolved}"
        )),
    }
}
