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

/// Resolve with the startup layer only (`db = None` -> layer (a) falls
/// through immediately).
async fn startup_bounds(min_fee_ppm: i64, max_fee_ppm: i64) -> (i64, i64) {
    let opts = startup_options(&[("min-fee-ppm", min_fee_ppm), ("max-fee-ppm", max_fee_ppm)]);
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
    let opts = startup_options(&[("max-fee-ppm", 1_234)]);
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
    let cfg = revops::fee_config::resolve_fee_cfg(None, &opts).await;
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
