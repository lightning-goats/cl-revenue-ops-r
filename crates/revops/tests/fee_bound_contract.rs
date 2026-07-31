//! Tasks 74/75 corrected contract: the fee-bound invariant, proven through
//! the REAL `resolve_fee_cfg` path rather than against helper functions.
//!
//! Both layers are reachable here without mocking anything: `db = None`
//! makes layer (a) fall through immediately, so a `listconfigs` map alone
//! exercises the STARTUP-option path, and a seeded `config_overrides` row
//! exercises the PERSISTED path. Testing the composed rules in isolation
//! would prove only that the helpers agree with themselves.
//!
//! ## The contract, read from Python main at fc4c76b
//!
//! Verified at source in `/home/sat/bin/cl_revenue_ops/.worktrees/
//! override-fee-floor`, not from a summary -- the default checkout is on
//! `task24-finalize-bool` and still carries the OLD downward repair.
//!
//! Canonical ranges (`CONFIG_FIELD_RANGES`): `min_fee_ppm` (5, 100000),
//! `max_fee_ppm` (1, 100000). Defaults: 10 and 2000.
//!
//! STARTUP (`_validate_startup_config_options`, `cl-revenue-ops.py:558-566`):
//!   1. `_validate_numeric_config_options` CLAMPS into the canonical range
//!      (`max(lo, min(num, hi))`), warning on clamp.
//!   2. `_enforce_fee_bound_invariant` raises max to min when crossed.
//!
//! PERSISTED (`config.py` post-load repair):
//!   1. Each override is validated INDIVIDUALLY; an out-of-range row is
//!      SKIPPED with a warning, NOT clamped.
//!   2. Then, if min > max, `self.max_fee_ppm = self.min_fee_ppm`.
//!
//! ## Two things this file exists to stop
//!
//! THE DIRECTION. The repair raises the CEILING; it never lowers the floor
//! and never swaps. In Python's own words: lowering `min_fee_ppm` to a
//! persisted `max_fee_ppm` of 1-4 is individually in range but drags the
//! floor under its own `CONFIG_FIELD_RANGES` minimum (CRITICAL-02), and
//! the two bounds have DIFFERENT lower limits (5 vs 1) so no downward
//! repair can hold both invariants.
//!
//! THE ASYMMETRY. Startup CLAMPS, persisted SKIPS. A single shared rule
//! would be wrong for exactly one of the two layers.

use cln_plugin::options::Value as OptValue;
use revops::config_resolve::db_override_key;
use revops_fees::cycle::FeeCfgSnapshot;
use std::collections::HashMap;

/// A `listconfigs` map -- layer (b), the CLN STARTUP options.
fn startup_options(pairs: &[(&str, i64)]) -> HashMap<String, OptValue> {
    pairs
        .iter()
        .map(|(suffix, value)| (format!("revenue-ops-{suffix}"), OptValue::Integer(*value)))
        .collect()
}

/// Push a `listconfigs` map through the REAL cache gate, exactly as
/// production does: `apply_fetch` is where every startup value enters and
/// where the clamp lives.
fn cached(opts: HashMap<String, OptValue>) -> HashMap<String, OptValue> {
    let cache = revops::config_resolve::PythonOptionCache::empty();
    assert!(cache.apply_fetch(Ok(opts)), "a successful fetch applies");
    cache.snapshot()
}

/// Resolve with the startup layer only (`db = None` -> layer (a) falls
/// through immediately).
async fn startup_bounds(min_fee_ppm: i64, max_fee_ppm: i64) -> (i64, i64) {
    let opts = cached(startup_options(&[
        ("min-fee-ppm", min_fee_ppm),
        ("max-fee-ppm", max_fee_ppm),
    ]));
    let cfg = revops::fee_config::resolve_fee_cfg(None, &opts).await;
    (cfg.min_fee_ppm, cfg.max_fee_ppm)
}

/// Copies `fixtures/fixture.db` and seeds one `config_overrides` row per
/// entry -- layer (a), the PERSISTED overrides.
async fn fixture_db_with_overrides(
    overrides: &[(&str, &str)],
) -> (revops_db::actor::DbHandle, tempfile::TempDir) {
    let fixture_db =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seeded.db");
    std::fs::copy(&fixture_db, &path).unwrap();
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        for (suffix, raw) in overrides {
            conn.execute(
                "INSERT INTO config_overrides (key, value, version, updated_at) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![db_override_key(suffix), *raw, 1i64, 1_800_000_000i64],
            )
            .unwrap();
        }
    }
    let handle = revops_db::actor::spawn_read_only(&path).await.unwrap();
    (handle, dir)
}

/// Resolve with the persisted layer only (no `listconfigs` entries).
async fn persisted_bounds(overrides: &[(&str, &str)]) -> (i64, i64) {
    let (handle, _tmp) = fixture_db_with_overrides(overrides).await;
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
    (cfg.min_fee_ppm, cfg.max_fee_ppm)
}

// =====================================================================
// the defaults both layers fall back to
// =====================================================================

/// Pinned because every persisted case below depends on the default floor
/// being 10 -- if it drifted, those cases would still pass while proving
/// something else.
#[tokio::test]
async fn the_rust_defaults_match_pythons() {
    let default = FeeCfgSnapshot::default();
    assert_eq!(
        (default.min_fee_ppm, default.max_fee_ppm),
        (10, 2_000),
        "py config.py:596/605"
    );
}

// =====================================================================
// Task 74: startup options -- clamp, then raise the ceiling
// =====================================================================

/// min 3 is below the canonical floor of 5, so it clamps UP to 5; max 2 is
/// in its own range and stays 2; the pair is now crossed, so max rises to
/// 5. A downward repair would give (2, 2) -- a floor under CRITICAL-02.
#[tokio::test]
async fn a_startup_min_below_the_floor_clamps_up_then_raises_the_ceiling() {
    assert_eq!(startup_bounds(3, 2).await, (5, 5));
}

/// Both zero: each clamps to its own floor, then max rises to 5.
///
/// NOTE this composed result does NOT by itself prove the per-field floors
/// -- a single shared floor of 5 would produce the same (5, 5), because the
/// repair raises max to 5 regardless. The mutation harness caught that.
/// [`each_fee_field_clamps_to_its_own_declared_floor`] below pins the part
/// this one cannot see.
#[tokio::test]
async fn startup_zeros_clamp_to_each_fields_own_floor_then_repair_upward() {
    assert_eq!(startup_bounds(0, 0).await, (5, 5));
}

/// The two bounds have DIFFERENT declared floors -- min_fee_ppm (5,
/// 100000) and max_fee_ppm (1, 100000) -- and that difference is the whole
/// reason the crossed repair must go upward. It is invisible in any
/// resolved snapshot, because a max clamped to 1 is immediately raised
/// again by the repair, so it is pinned directly on the clamp.
#[test]
fn each_fee_field_clamps_to_its_own_declared_floor() {
    use revops::config_resolve::clamp_startup_int;
    assert_eq!(clamp_startup_int("min_fee_ppm", 0), 5);
    assert_eq!(clamp_startup_int("max_fee_ppm", 0), 1);
    assert_eq!(clamp_startup_int("min_fee_ppm", 200_000), 100_000);
    assert_eq!(clamp_startup_int("max_fee_ppm", 200_000), 100_000);
}

/// A field with no declared range is left exactly as given.
#[test]
fn a_field_without_a_declared_range_is_not_clamped() {
    use revops::config_resolve::clamp_startup_int;
    assert_eq!(clamp_startup_int("no_such_field_has_a_range", -42), -42);
}

/// min above the ceiling clamps DOWN to 100000 and drags max up with it.
#[tokio::test]
async fn a_startup_min_above_the_ceiling_clamps_down_then_raises_the_ceiling() {
    assert_eq!(startup_bounds(200_000, 50).await, (100_000, 100_000));
}

/// The repair must not fire on healthy input.
#[tokio::test]
async fn healthy_startup_bounds_are_untouched() {
    assert_eq!(startup_bounds(10, 2_000).await, (10, 2_000));
}

/// Never lowers the floor, never swaps -- across every crossed in-range
/// input.
#[tokio::test]
async fn a_startup_repair_never_lowers_the_floor_or_swaps_the_pair() {
    for (min, max) in [(10, 1), (500, 499), (100_000, 1), (6, 5)] {
        let (out_min, out_max) = startup_bounds(min, max).await;
        assert_eq!(out_min, min, "the floor moved for ({min}, {max})");
        assert_eq!(out_max, min, "the ceiling must rise to the floor");
        assert!(out_min <= out_max);
    }
}

// =====================================================================
// Task 75: persisted overrides -- validate individually, then repair
// =====================================================================

/// The case the whole corrected contract exists for. A persisted
/// `max_fee_ppm` of 1 is INDIVIDUALLY VALID (its range starts at 1), so it
/// is applied, leaving min at its default of 10. The repair then raises
/// max back to 10 -- where the old downward repair produced a 1 PPM floor.
#[tokio::test]
async fn a_persisted_max_below_the_default_floor_raises_back_to_the_floor() {
    for persisted_max in ["1", "4", "9"] {
        assert_eq!(
            persisted_bounds(&[("max-fee-ppm", persisted_max)]).await,
            (10, 10),
            "persisted max_fee_ppm={persisted_max} must not drag the floor down"
        );
    }
}

/// Both persisted and both individually in range; only the cross-field
/// invariant is violated.
#[tokio::test]
async fn a_persisted_crossed_pair_repairs_upward() {
    assert_eq!(
        persisted_bounds(&[("min-fee-ppm", "100000"), ("max-fee-ppm", "1")]).await,
        (100_000, 100_000)
    );
}

/// An out-of-range persisted row is SKIPPED, not clamped -- the startup
/// layer's rule must not leak here. The row falls through to the default.
#[tokio::test]
async fn an_out_of_range_persisted_override_is_skipped_not_clamped() {
    assert_eq!(
        persisted_bounds(&[("min-fee-ppm", "3")]).await,
        (10, 2_000),
        "min_fee_ppm=3 is below its range: python skips the row, leaving the default"
    );
    assert_eq!(
        persisted_bounds(&[("min-fee-ppm", "200000")]).await,
        (10, 2_000),
        "a clamp here would be the startup rule leaking into the persisted layer"
    );
}

#[tokio::test]
async fn healthy_persisted_bounds_are_untouched() {
    assert_eq!(
        persisted_bounds(&[("min-fee-ppm", "50"), ("max-fee-ppm", "3000")]).await,
        (50, 3_000)
    );
}

// =====================================================================
// the two layers disagree on purpose
// =====================================================================

/// Stated as one test because it is one decision: the SAME out-of-range
/// value is clamped as a startup option and skipped as a persisted
/// override. A port that handled both alike would be wrong about exactly
/// one of them.
#[tokio::test]
async fn the_startup_and_persisted_layers_treat_out_of_range_oppositely() {
    assert_eq!(
        startup_bounds(3, 2_000).await.0,
        5,
        "startup clamps 3 up to the canonical floor"
    );
    assert_eq!(
        persisted_bounds(&[("min-fee-ppm", "3")]).await.0,
        10,
        "persisted skips 3 entirely, leaving the default"
    );
}

/// Layer precedence is unchanged by the clamp: a valid DB override still
/// beats a `listconfigs` value, and the override is NOT clamped on its way
/// through.
#[tokio::test]
async fn a_persisted_override_still_beats_a_startup_option() {
    let (handle, _tmp) = fixture_db_with_overrides(&[("max-fee-ppm", "1500")]).await;
    let opts = cached(startup_options(&[("max-fee-ppm", 1_234)]));
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &opts).await;
    assert_eq!(cfg.max_fee_ppm, 1_500);
}

// =====================================================================
// the clamp is not fee-bound-specific
// =====================================================================

/// Python's `_INIT_NUMERIC_RANGES` is `dict(CONFIG_FIELD_RANGES)` and
/// clamps EVERY numeric startup option, floats included -- so the layer
/// rule belongs to the resolver, not to the fee pair.
///
/// `drain_fee_discount_max` (range `[0.0, 0.5]`) is used because it is not
/// half of any crossed-pair repair, so it isolates the clamp from the
/// floor/target and low/high threshold repairs.
#[tokio::test]
async fn a_float_startup_option_is_clamped_into_its_range() {
    let mut opts = HashMap::new();
    opts.insert(
        "revenue-ops-drain-fee-discount-max".to_string(),
        OptValue::String("5.0".to_string()),
    );
    let cfg = revops::fee_config::resolve_fee_cfg(None, &cached(opts)).await;
    assert_eq!(cfg.drain_fee_discount_max, 0.5);
}

/// ...and the persisted layer still SKIPS rather than clamps for floats
/// too, leaving the default standing.
#[tokio::test]
async fn a_float_persisted_override_out_of_range_is_skipped_not_clamped() {
    let (handle, _tmp) = fixture_db_with_overrides(&[("drain-fee-discount-max", "5.0")]).await;
    let cfg = revops::fee_config::resolve_fee_cfg(Some(&handle), &HashMap::new()).await;
    assert_eq!(
        cfg.drain_fee_discount_max,
        FeeCfgSnapshot::default().drain_fee_discount_max,
        "an out-of-range persisted float must fall through to the default"
    );
}

// =====================================================================
// the clamp covers EVERY numeric field, not just the fee pair
// =====================================================================

/// The scope ruling on task 74: Python's `_INIT_NUMERIC_RANGES` is
/// `dict(CONFIG_FIELD_RANGES)`, deliberately covering all 96 numeric
/// fields "so startup cannot silently omit a newly governed field". A
/// clamp that only reached the ~18 fee-cycle resolver sites would leave
/// the other numeric fields exposed on the operator-facing config surface.
///
/// Asserted on the CACHE SNAPSHOT rather than on any one consumer, because
/// that snapshot is what every consumer reads.
#[test]
fn the_startup_clamp_covers_numeric_fields_beyond_the_fee_pair() {
    let mut opts = HashMap::new();
    // int, not a fee bound, and not a FeeCfgSnapshot field
    opts.insert(
        "revenue-ops-planner-max-closes-per-cycle".to_string(),
        OptValue::Integer(1_000_000),
    );
    // float, not a fee bound
    opts.insert(
        "revenue-ops-growth-budget-earned-fraction".to_string(),
        OptValue::String("9.5".to_string()),
    );
    let snap = cached(opts);

    let planner = snap
        .get("revenue-ops-planner-max-closes-per-cycle")
        .expect("present");
    let fraction = snap
        .get("revenue-ops-growth-budget-earned-fraction")
        .expect("present");

    let (_, planner_hi) = revops::config_types::field_range("planner_max_closes_per_cycle")
        .expect("planner_max_closes_per_cycle declares a range");
    assert_eq!(
        revops::config_resolve::option_value_to_string(planner).and_then(|s| s.parse::<i64>().ok()),
        Some(planner_hi as i64),
        "an out-of-range non-fee int must be clamped at the cache gate"
    );
    assert_eq!(
        revops::config_resolve::option_value_to_string(fraction)
            .and_then(|s| s.parse::<f64>().ok()),
        Some(1.0),
        "growth_budget_earned_fraction is declared (0.0, 1.0)"
    );
}

/// An in-band value passes through the gate untouched, and a field with no
/// declared range is never rewritten.
#[test]
fn the_cache_gate_leaves_in_band_and_unranged_values_alone() {
    let mut opts = HashMap::new();
    opts.insert("revenue-ops-min-fee-ppm".to_string(), OptValue::Integer(50));
    opts.insert(
        "revenue-ops-not-a-real-option".to_string(),
        OptValue::Integer(-42),
    );
    let snap = cached(opts);
    assert_eq!(
        revops::config_resolve::option_value_to_string(
            snap.get("revenue-ops-min-fee-ppm").unwrap()
        )
        .and_then(|s| s.parse::<i64>().ok()),
        Some(50)
    );
    assert_eq!(
        revops::config_resolve::option_value_to_string(
            snap.get("revenue-ops-not-a-real-option").unwrap()
        )
        .and_then(|s| s.parse::<i64>().ok()),
        Some(-42)
    );
}

/// Task 75's parametrisation: every crossed pair ends ORDERED and IN BAND,
/// and the two already-ordered pairs are no-ops.
#[tokio::test]
async fn task75_edge_cases_all_end_ordered_and_in_band() {
    for (min, max) in [(5, 1), (900, 100), (100_000, 1), (10, 1), (10, 4)] {
        let (out_min, out_max) = startup_bounds(min, max).await;
        assert!(
            out_min <= out_max,
            "({min}, {max}) -> ({out_min}, {out_max})"
        );
        assert!(
            (5..=100_000).contains(&out_min),
            "min out of band: {out_min}"
        );
        assert!(
            (1..=100_000).contains(&out_max),
            "max out of band: {out_max}"
        );
    }
    for (min, max) in [(10, 2_000), (5, 5)] {
        assert_eq!(startup_bounds(min, max).await, (min, max), "no-op expected");
    }
}

// =====================================================================
// the plugin's OWN startup option, through the production helpers
// =====================================================================
//
// The cached `listconfigs` map is not the only way a startup value reaches
// a consumer. `resolved_config_json` and the config RPC also read this
// plugin's OWN registered option, which is operator-supplied. Two ways that
// value escapes the cache entirely:
//
//   - SHADOW NAMING: this plugin registers `revenue-r-*`, so its own option
//     never appears in the cached `revenue-ops-*` map.
//   - LISTCONFIGS OUTAGE: the cache is empty, but the supplied option still
//     carries a value.
//
// These tests drive `resolve_startup_option_value` and
// `parse_clamped_startup_i64` -- the SAME functions main.rs calls -- rather
// than re-assembling the rule locally, so a mutation in either is caught
// here.

fn as_i64(value: &OptValue) -> Option<i64> {
    revops::config_resolve::option_value_to_string(value).and_then(|s| s.trim().parse::<i64>().ok())
}

/// Layer (c), the own option, IS clamped -- and the clamp keys off the
/// `Config` field name, never the CLN option name, which is why a
/// shadow-named `revenue-r-*` option is covered despite never entering the
/// cached map.
#[test]
fn the_own_option_layer_is_clamped_by_the_production_resolver() {
    let resolved = revops::config_resolve::resolve_startup_option_value(
        "min_fee_ppm",
        None,
        None,
        Some(OptValue::Integer(0)),
    )
    .expect("the own option resolves");
    assert_eq!(as_i64(&resolved), Some(5));
}

/// A `listconfigs` outage empties the cache; the own option still resolves
/// and is still clamped.
#[test]
fn a_listconfigs_outage_still_clamps_the_own_option() {
    let cache = revops::config_resolve::PythonOptionCache::empty();
    assert!(!cache.apply_fetch(Err("listconfigs unavailable".to_string())));
    assert!(
        cache.snapshot().is_empty(),
        "outage leaves no cached values"
    );

    let cached = cache.snapshot().get("revenue-ops-min-fee-ppm").cloned();
    let resolved = revops::config_resolve::resolve_startup_option_value(
        "min_fee_ppm",
        None,
        cached,
        Some(OptValue::Integer(200_000)),
    )
    .expect("the own option resolves during an outage");
    assert_eq!(as_i64(&resolved), Some(100_000));
}

/// Layer (a) is NOT clamped: a persisted override has already been
/// range-checked by `validate_override`, which SKIPS an out-of-range row.
/// If the resolver clamped it, a skipped row would become an applied one.
#[test]
fn the_persisted_layer_is_not_clamped_by_the_production_resolver() {
    let resolved = revops::config_resolve::resolve_startup_option_value(
        "min_fee_ppm",
        // 3 is BELOW min_fee_ppm's floor of 5, so a clamp would rewrite it to
        // 5 and be indistinguishable from an applied override. Production
        // rejects such a row upstream in `validate_override`; this pins the
        // resolver's own contract, that it never clamps layer (a).
        Some(OptValue::Integer(3)),
        Some(OptValue::Integer(50)),
        Some(OptValue::Integer(0)),
    )
    .expect("the db override wins");
    assert_eq!(
        as_i64(&resolved),
        Some(3),
        "layer (a) must win and pass through UNCLAMPED"
    );
}

/// Precedence is unchanged by the clamp.
#[test]
fn the_production_resolver_keeps_db_over_cached_over_own() {
    let r = |db, cached, own| {
        revops::config_resolve::resolve_startup_option_value("min_fee_ppm", db, cached, own)
            .as_ref()
            .and_then(as_i64)
    };
    assert_eq!(
        r(
            Some(OptValue::Integer(11)),
            Some(OptValue::Integer(22)),
            Some(OptValue::Integer(33))
        ),
        Some(11)
    );
    assert_eq!(
        r(
            None,
            Some(OptValue::Integer(22)),
            Some(OptValue::Integer(33))
        ),
        Some(22)
    );
    assert_eq!(r(None, None, Some(OptValue::Integer(33))), Some(33));
    assert_eq!(r(None, None, None), None);
}

/// Item 213, driven through the helper main.rs actually calls.
/// `flow_window_days` is declared `[1, 365]` with a fallback of 7, so an
/// out-of-range 999 must resolve to 365. The boundary is deliberately not
/// the fallback: a regression to `as_str` drops the clamped value onto 7,
/// which this catches.
#[test]
fn the_production_parse_helper_survives_a_clamped_integer() {
    let got = revops::config_resolve::parse_clamped_startup_i64(
        "flow_window_days",
        Some(OptValue::Integer(999)),
        7,
    );
    assert_eq!(
        got, 365,
        "999 must clamp to the ceiling, not fall back to 7"
    );
    assert_ne!(got, 7, "the fallback must not mask the clamp");

    assert_eq!(
        revops::config_resolve::parse_clamped_startup_i64(
            "flow_window_days",
            Some(OptValue::Integer(0)),
            7
        ),
        1
    );
    assert_eq!(
        revops::config_resolve::parse_clamped_startup_i64("flow_window_days", None, 7),
        7,
        "an absent option uses the caller's default"
    );
    assert_eq!(
        revops::config_resolve::parse_clamped_startup_i64(
            "flow_window_days",
            Some(OptValue::String("30".into())),
            7
        ),
        30,
        "a String-valued option parses too"
    );
}

// =====================================================================
// EXHAUSTIVE: every declared numeric range, through the real gate
// =====================================================================

/// The 19 ranged `Config` fields that are NOT operator startup options.
///
/// Python's `_validate_numeric_config_options` iterates all 96 of
/// `CONFIG_FIELD_RANGES` but only clamps keys PRESENT in `kwargs`, so these
/// are never reached at startup -- they are reachable only as persisted
/// overrides, where the contract is SKIP, not clamp. Pinned as an exact set
/// so a field silently gaining or losing a startup option is caught.
const RANGED_WITHOUT_STARTUP_OPTION: [&str; 19] = [
    "base_fee_msat",
    "capex_bootstrap_bps",
    "capex_bootstrap_max_sats",
    "capex_exploration_rate",
    "capex_global_envelope_sats",
    "capex_grace_days",
    "capex_probability_budget_bonus",
    "capex_reinvestment_rate",
    "capex_tactical_rate",
    "estimated_open_cost_sats",
    "high_liquidity_threshold",
    "inbound_fee_estimate_ppm",
    "low_liquidity_threshold",
    "max_concurrent_jobs",
    "rebalance_cooldown_hours",
    "rebalance_max_amount",
    "sink_threshold",
    "source_threshold",
    "thompson_prior_std_fee",
];

/// The startup option names lightningd actually registers.
fn registered_option_names() -> std::collections::BTreeSet<String> {
    let raw = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/options.json"),
    )
    .expect("read fixtures/options.json");
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&raw).expect("valid options fixture");
    parsed
        .iter()
        .filter_map(|o| o.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect()
}

/// The canonical Python option name for a `Config` field -- the inverse of
/// `config_resolve::db_override_key`, including its four irregular remaps.
fn canonical_option_name(field: &str) -> String {
    const IRREGULAR: [(&str, &str); 4] = [
        ("enable_vegas_reflex", "vegas-reflex"),
        ("vegas_decay_rate", "vegas-decay"),
        ("planner_max_fee_rate_sat_vb", "planner-max-fee-rate"),
        (
            "boltz_structural_budget_sats_per_day",
            "boltz-structural-budget-sats",
        ),
    ];
    for (f, suffix) in IRREGULAR {
        if field == f {
            return format!("revenue-ops-{suffix}");
        }
    }
    format!("revenue-ops-{}", field.replace('_', "-"))
}

/// Item 215's reachability gate, corrected by item 216's fixture audit.
///
/// Sampling two fields would let the other 94 range entries be deleted, or
/// an irregular name remap be broken, without any test noticing. So this
/// walks EVERY numeric ranged field, synthesizes a below-floor and an
/// above-ceiling value, and pushes each through the real
/// `PythonOptionCache::apply_fetch` gate under the field's exact canonical
/// option name.
///
/// Crucially it checks each name against the REGISTERED option set rather
/// than against a name transform: an earlier version of this test round-
/// tripped the transform only, which meant it invented option names for the
/// 19 fields that have none and "proved" they clamp. Those 19 are asserted
/// as an exact set instead.
#[test]
fn every_declared_numeric_range_is_clamped_at_the_startup_gate() {
    let types = revops::config_types::load();
    let registered = registered_option_names();

    let mut ranged: Vec<(String, (f64, f64))> = types
        .ranges
        .iter()
        .filter(|(field, _)| {
            matches!(
                revops::config_types::field_type_for(field),
                Some(revops::config_types::FieldType::Int)
                    | Some(revops::config_types::FieldType::Float)
            )
        })
        .map(|(f, r)| (f.clone(), *r))
        .collect();
    ranged.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        ranged.len(),
        96,
        "expected 96 numeric ranged fields; the fixture changed"
    );

    let mut without_option = Vec::new();
    let mut proven = 0usize;

    for (field, (lo, hi)) in &ranged {
        let option_name = canonical_option_name(field);
        if !registered.contains(&option_name) {
            without_option.push(field.clone());
            continue;
        }
        let is_int = revops::config_types::field_type_for(field)
            == Some(revops::config_types::FieldType::Int);

        for (probe, expected) in [(lo - 1.0, *lo), (hi + 1.0, *hi)] {
            let supplied = if is_int {
                OptValue::Integer(probe as i64)
            } else {
                OptValue::String(probe.to_string())
            };
            let mut opts = HashMap::new();
            opts.insert(option_name.clone(), supplied);
            let snap = cached(opts);
            let got = revops::config_resolve::option_value_to_string(
                snap.get(&option_name)
                    .expect("the option survives the gate"),
            )
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or_else(|| panic!("{field}: value unreadable after the gate"));

            let want = if is_int { expected.trunc() } else { expected };
            assert!(
                (got - want).abs() < 1e-9,
                "{field}: probe {probe} should clamp to {want}, got {got}"
            );
        }
        proven += 1;
    }

    without_option.sort();
    assert_eq!(
        without_option,
        RANGED_WITHOUT_STARTUP_OPTION
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "the set of ranged fields lacking a startup option changed"
    );
    assert_eq!(
        proven,
        96 - RANGED_WITHOUT_STARTUP_OPTION.len(),
        "every ranged field with a real startup option must be proven clamped"
    );
    assert_eq!(proven, 77, "77 real startup options carry a declared range");
}
