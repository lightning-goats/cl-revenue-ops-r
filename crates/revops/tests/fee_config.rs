//! Integration tests for `revops::fee_config::resolve_fee_cfg` -- the
//! per-cycle `FeeCfgSnapshot` resolver (Phase 4b Task 1). Exercises the same
//! 3-layer precedence `revenue-r-config` already implements
//! (`revops::config_resolve`): (a) DB override (`config_overrides` table) >
//! (b) cached `listconfigs` Python option value > (c) `FeeCfgSnapshot::default()`
//! field -- with the three DB-override-only keys (`paused`, `authority_level`,
//! `econ_governor_fees_enabled`) skipping layer (b) entirely (no CLN option
//! exists for them).
//!
//! Uses the same fixture-DB-copy + seeded `config_overrides` row pattern as
//! `crates/revops/tests/config_resolve.rs`'s
//! `db_override_key_resolves_seeded_override_for_a_renamed_field` test.

use revops::config_resolve::db_override_key;
use revops_fees::cycle::FeeCfgSnapshot;
use std::collections::HashMap;

/// Copies `fixtures/fixture.db` into a fresh tempdir and inserts one
/// `config_overrides` row keyed by the Python `Config` field name for
/// `revenue_r_config_suffix` (via `db_override_key`), returning a live
/// read-only `DbHandle` plus the `TempDir` guard (keep it alive for the
/// handle's lifetime).
async fn fixture_db_with_override(
    revenue_r_config_suffix: &str,
    raw_value: &str,
) -> (revops_db::actor::DbHandle, tempfile::TempDir) {
    let fixture_db =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seeded.db");
    std::fs::copy(&fixture_db, &path).unwrap();
    let field = db_override_key(revenue_r_config_suffix);
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO config_overrides (key, value, version, updated_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![field, raw_value, 1i64, 1_800_000_000i64],
        )
        .unwrap();
    }
    let handle = revops_db::actor::spawn_read_only(&path).await.unwrap();
    (handle, dir)
}

#[tokio::test]
async fn resolve_fee_cfg_defaults_when_no_db_no_python() {
    let cfg = revops::fee_config::resolve_fee_cfg(None, &HashMap::new()).await;
    assert_eq!(cfg, FeeCfgSnapshot::default());
}

#[tokio::test]
async fn resolve_fee_cfg_db_override_beats_listconfigs() {
    let (handle, _tmp) = fixture_db_with_override("max-fee-ppm", "1500").await;
    let mut py = HashMap::new();
    py.insert(
        "revenue-ops-max-fee-ppm".to_string(),
        cln_plugin::options::Value::Integer(1234),
    );
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &py).await;
    assert_eq!(cfg.max_fee_ppm, 1500);
}

#[tokio::test]
async fn resolve_fee_cfg_listconfigs_beats_default_when_no_override() {
    let mut py = HashMap::new();
    py.insert(
        "revenue-ops-max-fee-ppm".to_string(),
        cln_plugin::options::Value::String("1234".to_string()),
    );
    let cfg = revops::fee_config::resolve_fee_cfg(None, &py).await;
    assert_eq!(cfg.max_fee_ppm, 1234);
}

#[tokio::test]
async fn resolve_fee_cfg_db_override_only_keys_skip_listconfigs() {
    // `paused` has NO CLN option: a (fake) listconfigs value must be
    // ignored even if present in the map.
    let (handle, _tmp) = fixture_db_with_override("paused", "true").await;
    let mut py = HashMap::new();
    py.insert(
        "revenue-ops-paused".to_string(),
        cln_plugin::options::Value::Boolean(false),
    );
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &py).await;
    assert!(cfg.paused);
}

#[tokio::test]
async fn resolve_fee_cfg_authority_level_db_override_only_default_is_capital() {
    let cfg = revops::fee_config::resolve_fee_cfg(None, &HashMap::new()).await;
    assert_eq!(cfg.authority_level, Some("capital".to_string()));
}

#[tokio::test]
async fn resolve_fee_cfg_authority_level_db_override_applies() {
    let (handle, _tmp) = fixture_db_with_override("authority-level", "observe").await;
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
    assert_eq!(cfg.authority_level, Some("observe".to_string()));
}

#[tokio::test]
async fn resolve_fee_cfg_econ_governor_fees_enabled_db_override_only() {
    let (handle, _tmp) = fixture_db_with_override("econ-governor-fees-enabled", "true").await;
    let mut py = HashMap::new();
    py.insert(
        "revenue-ops-econ-governor-fees-enabled".to_string(),
        cln_plugin::options::Value::Boolean(false),
    );
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &py).await;
    assert!(cfg.econ_governor_fees_enabled);
}

/// `enable_dynamic_htlcmax` must keep the RAW resolved value (admission's
/// narrow truthiness distinguishes a genuine bool from a string) -- a DB
/// override string "false" is NOT coerced to `Value::Bool(false)`.
#[tokio::test]
async fn resolve_fee_cfg_enable_dynamic_htlcmax_keeps_raw_string() {
    let (handle, _tmp) = fixture_db_with_override("enable-dynamic-htlcmax", "false").await;
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
    assert_eq!(
        cfg.enable_dynamic_htlcmax,
        serde_json::Value::String("false".to_string())
    );
}

#[tokio::test]
async fn resolve_fee_cfg_enable_dynamic_htlcmax_default_is_bool_false() {
    let cfg = revops::fee_config::resolve_fee_cfg(None, &HashMap::new()).await;
    assert_eq!(cfg.enable_dynamic_htlcmax, serde_json::Value::Bool(false));
}

#[test]
fn neighbor_median_min_competitors_ok_true_only_for_3() {
    use revops::fee_config::neighbor_median_min_competitors_ok;
    assert!(neighbor_median_min_competitors_ok(&serde_json::json!(3)));
    assert!(!neighbor_median_min_competitors_ok(&serde_json::json!(4)));
    assert!(!neighbor_median_min_competitors_ok(&serde_json::json!("3")));
    assert!(!neighbor_median_min_competitors_ok(
        &serde_json::Value::Null
    ));
}

/// Table test: every one of the 22 `FeeCfgSnapshot` fields is plumbed
/// through a DB override -- walks (revenue-r-config suffix, override raw
/// value, assertion) so no field is silently unplumbed. Each row seeds its
/// own fixture DB (one row per DB, matching `config_overrides`' one-row-
/// per-key shape) and checks the resolved snapshot reflects the override.
#[tokio::test]
async fn all_22_fields_are_plumbed() {
    async fn assert_int_field(
        suffix: &str,
        raw: &str,
        expected: i64,
        get: impl Fn(&FeeCfgSnapshot) -> i64,
    ) {
        let (handle, _tmp) = fixture_db_with_override(suffix, raw).await;
        let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
        assert_eq!(get(&cfg), expected, "field for suffix {suffix}");
    }
    async fn assert_float_field(
        suffix: &str,
        raw: &str,
        expected: f64,
        get: impl Fn(&FeeCfgSnapshot) -> f64,
    ) {
        let (handle, _tmp) = fixture_db_with_override(suffix, raw).await;
        let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
        assert_eq!(get(&cfg), expected, "field for suffix {suffix}");
    }
    async fn assert_bool_field(
        suffix: &str,
        raw: &str,
        expected: bool,
        get: impl Fn(&FeeCfgSnapshot) -> bool,
    ) {
        let (handle, _tmp) = fixture_db_with_override(suffix, raw).await;
        let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
        assert_eq!(get(&cfg), expected, "field for suffix {suffix}");
    }
    async fn assert_string_field(
        suffix: &str,
        raw: &str,
        expected: &str,
        get: impl Fn(&FeeCfgSnapshot) -> String,
    ) {
        let (handle, _tmp) = fixture_db_with_override(suffix, raw).await;
        let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
        assert_eq!(get(&cfg), expected, "field for suffix {suffix}");
    }

    assert_int_field("min-fee-ppm", "40", 40, |c| c.min_fee_ppm).await;
    assert_int_field("max-fee-ppm", "3000", 3000, |c| c.max_fee_ppm).await;
    assert_int_field("min-fee-ppm-saturated", "5", 5, |c| c.min_fee_ppm_saturated).await;
    assert_int_field("fee-interval", "900", 900, |c| c.fee_interval).await;
    assert_int_field("flow-interval", "1800", 1800, |c| c.flow_interval).await;
    assert_float_field("htlc-congestion-threshold", "0.5", 0.5, |c| {
        c.htlc_congestion_threshold
    })
    .await;
    assert_string_field("market-fee-mode", "premium", "premium", |c| {
        c.market_fee_mode.clone()
    })
    .await;
    assert_float_field("drain-fee-discount-max", "0.25", 0.25, |c| {
        c.drain_fee_discount_max
    })
    .await;
    assert_float_field("high-liquidity-threshold", "0.9", 0.9, |c| {
        c.high_liquidity_threshold
    })
    .await;
    assert_string_field("fee-profile", "conservative", "conservative", |c| {
        c.fee_profile.clone()
    })
    .await;
    assert_int_field("base-fee-msat", "500", 500, |c| c.base_fee_msat).await;
    assert_bool_field("vegas-reflex", "false", false, |c| c.enable_vegas_reflex).await;
    // enable_dynamic_htlcmax covered separately above (raw passthrough).
    assert_float_field("htlcmax-source-pct", "0.6", 0.6, |c| c.htlcmax_source_pct).await;
    assert_float_field("htlcmax-sink-pct", "0.4", 0.4, |c| c.htlcmax_sink_pct).await;
    assert_float_field("htlcmax-balanced-pct", "0.55", 0.55, |c| {
        c.htlcmax_balanced_pct
    })
    .await;
    assert_bool_field("paused", "true", true, |c| c.paused).await;
    assert_bool_field("node-drain-bias-enabled", "true", true, |c| {
        c.node_drain_bias_enabled
    })
    .await;
    assert_float_field("receivable-ratio-target", "0.45", 0.45, |c| {
        c.receivable_ratio_target
    })
    .await;
    assert_float_field("receivable-ratio-floor", "0.15", 0.15, |c| {
        c.receivable_ratio_floor
    })
    .await;
    assert_bool_field("econ-governor-fees-enabled", "true", true, |c| {
        c.econ_governor_fees_enabled
    })
    .await;

    // authority_level: Option<String>, covered separately above too, but
    // included here for the "every field" table completeness.
    let (handle, _tmp) = fixture_db_with_override("authority-level", "assist").await;
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
    assert_eq!(cfg.authority_level, Some("assist".to_string()));
}

// ---------------------------------------------------------------------------
// T6 consumer: per-cycle neighbor_median_min_competitors resolution
// (verify==3, not plumbed -- the scheduler fails closed on anything != 3)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn neighbor_min_competitors_defaults_to_2_and_fails_the_gate() {
    // Python `Config.neighbor_median_min_competitors` defaults to 2
    // (fixtures/options.json default "2") -- an unconfigured node must
    // therefore FAIL the ==3 gate, never silently pass it.
    let v =
        revops::fee_config::resolve_neighbor_median_min_competitors(None, &HashMap::new()).await;
    assert_eq!(v, serde_json::json!(2));
    assert!(!revops::fee_config::neighbor_median_min_competitors_ok(&v));
}

#[tokio::test]
async fn neighbor_min_competitors_db_override_resolves_typed_and_passes_gate() {
    let (handle, _tmp) = fixture_db_with_override("neighbor-median-min-competitors", "3").await;
    let v =
        revops::fee_config::resolve_neighbor_median_min_competitors(Some(&handle), &HashMap::new())
            .await;
    // The raw DB string "3" must come back TYPED (the fixture types the
    // field int), so the strict ==json!(3) gate can pass.
    assert_eq!(v, serde_json::json!(3));
    assert!(revops::fee_config::neighbor_median_min_competitors_ok(&v));
}

#[tokio::test]
async fn neighbor_min_competitors_listconfigs_layer_resolves_typed() {
    let mut py = HashMap::new();
    py.insert(
        "revenue-ops-neighbor-median-min-competitors".to_string(),
        cln_plugin::options::Value::String("3".to_string()),
    );
    let v = revops::fee_config::resolve_neighbor_median_min_competitors(None, &py).await;
    assert_eq!(v, serde_json::json!(3));
    assert!(revops::fee_config::neighbor_median_min_competitors_ok(&v));
}
