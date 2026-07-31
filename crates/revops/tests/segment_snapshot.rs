//! R68-8 (RED): stamping `observer_member_id` into the frozen segstore's
//! exported snapshot, and reading the observation count the withhold
//! decision needs.
//!
//! ## Why this exists
//!
//! Python's `SegmentObservationStore.export_snapshot`
//! (`modules/segment_observations.py:137-160`) takes a keyword-only
//! `observer_member_id` and stamps it into the returned dict. The frozen
//! Rust kernel's interface is `export_snapshot(&self, now) -> OValue` with
//! NO such parameter -- a deliberate, documented scope decision
//! (`segstore.rs:11-23`): "the engine (T7), which knows its own member id,
//! is expected to merge it in when assembling the datastore write".
//!
//! So the merge must ADD that key. It must never expect the kernel to have
//! produced it, and it must never re-derive any of the kernel's own work
//! (validation, TTL prune, sort, truncation) -- this slice consumes
//! `export_snapshot`'s output and does exactly one thing to it.
//!
//! ## The contract that makes position load-bearing
//!
//! Python builds the snapshot as a dict LITERAL
//! (`segment_observations.py:154-160`):
//!
//! ```text
//! {
//!     "generated_at": now_ts,
//!     "ttl_seconds": self.ttl_seconds,
//!     "schema_version": self.SCHEMA_VERSION,
//!     "observer_member_id": str(observer_member_id or "").strip(),
//!     "segment_observations": valid,
//! }
//! ```
//!
//! `json.dumps` walks a dict in insertion order and this blob is written
//! VERBATIM to the datastore (`rebalance_engine_v2.py:3180-3186`
//! `json.dumps(snapshot)`), so the stamp's POSITION is part of the wire
//! contract, not a cosmetic detail. It sits fourth -- between
//! `schema_version` and `segment_observations`. Appending it would produce
//! a byte-different blob for every publish.
//!
//! The frozen kernel emits the other four keys in exactly that relative
//! order minus the stamp (`segstore.rs:312-320`), so the merge inserts
//! rather than reorders.
//!
//! ## NOT a defect: the guard reads raw, the stamp writes trimmed
//!
//! `_push_segment_observation_snapshot` (`rebalance_engine_v2.py:3167-3172`)
//! guards on `if store is None or not observer_member_id` -- Python
//! truthiness on the UNTRIMMED string -- while `export_snapshot` stamps
//! `str(observer_member_id or "").strip()`. A whitespace-only id is
//! therefore truthy, passes the guard, and is stamped as `""`.
//!
//! That asymmetry is reachable only if `_get_our_id()` returns whitespace,
//! which it cannot, so under operator policy item 178 it is NOT a proven
//! defect and is ported exactly rather than "fixed". R68-7's
//! `segment_publish_is_allowed` already guards on the raw string, which
//! matches; the trim belongs here, at the stamp.

use revops::datastore_producers::{
    snapshot_observation_count, stamp_observer_member_id, SnapshotRefusal, SNAPSHOT_KEY_ORDER,
};
use revops_fees::pyjson::OValue;
use revops_rebalance::segstore::SegmentObservationStore;

/// The exact four-key object the frozen kernel emits (`segstore.rs:312-320`),
/// with one observation so the array is non-empty.
fn kernel_snapshot() -> OValue {
    OValue::obj(vec![
        ("generated_at".to_string(), OValue::Int(1050)),
        ("ttl_seconds".to_string(), OValue::Int(500)),
        ("schema_version".to_string(), OValue::Int(1)),
        (
            "segment_observations".to_string(),
            OValue::arr(vec![OValue::obj(vec![
                ("observation_id".to_string(), OValue::str("obs-1040-6")),
                ("short_channel_id".to_string(), OValue::str("555x5x0")),
                ("direction".to_string(), OValue::Int(0)),
            ])]),
        ),
    ])
}

fn keys(v: &OValue) -> Vec<String> {
    v.as_obj()
        .expect("an object")
        .iter()
        .map(|(k, _)| k.clone())
        .collect()
}

fn get<'a>(v: &'a OValue, key: &str) -> &'a OValue {
    v.as_obj()
        .expect("an object")
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, val)| val)
        .unwrap_or_else(|| panic!("key {key} present"))
}

// =====================================================================
// the stamp lands in Python's position
// =====================================================================

#[test]
fn the_stamped_snapshot_has_pythons_exact_five_key_order() {
    let stamped = stamp_observer_member_id(kernel_snapshot(), "02aabb").expect("a valid snapshot");
    assert_eq!(keys(&stamped), SNAPSHOT_KEY_ORDER.to_vec());
}

/// The mutation this is here to kill: appending the stamp instead of
/// inserting it. Both produce a dict with the same five keys and the same
/// values -- only the serialized bytes differ, and those bytes are what
/// the datastore stores.
#[test]
fn the_stamp_is_inserted_before_the_observations_never_appended() {
    let stamped = stamp_observer_member_id(kernel_snapshot(), "02aabb").expect("a valid snapshot");
    let k = keys(&stamped);
    let stamp_at = k
        .iter()
        .position(|key| key == "observer_member_id")
        .expect("stamped");
    let observations_at = k
        .iter()
        .position(|key| key == "segment_observations")
        .expect("the kernel's array survives");

    assert_eq!(stamp_at, 3, "python's literal puts the stamp fourth");
    assert!(
        stamp_at < observations_at,
        "stamp must precede the observations array, got {k:?}"
    );
}

/// A serialized check, because the byte string is the actual artifact the
/// datastore holds -- key order is invisible to a field-by-field
/// comparison but decides these bytes.
#[test]
fn the_serialized_blob_carries_the_stamp_in_pythons_position() {
    let snapshot = OValue::obj(vec![
        ("generated_at".to_string(), OValue::Int(1050)),
        ("ttl_seconds".to_string(), OValue::Int(500)),
        ("schema_version".to_string(), OValue::Int(1)),
        (
            "segment_observations".to_string(),
            OValue::arr(vec![OValue::Int(7)]),
        ),
    ]);
    let stamped = stamp_observer_member_id(snapshot, "02aabb").expect("a valid snapshot");

    assert_eq!(
        revops_fees::pyjson::dumps_python(&stamped),
        r#"{"generated_at": 1050, "ttl_seconds": 500, "schema_version": 1, "observer_member_id": "02aabb", "segment_observations": [7]}"#
    );
}

// =====================================================================
// what the stamp writes
// =====================================================================

/// py `str(observer_member_id or "").strip()`.
#[test]
fn the_stamped_member_id_is_trimmed() {
    let stamped =
        stamp_observer_member_id(kernel_snapshot(), "  02aabb\n").expect("a valid snapshot");
    assert_eq!(get(&stamped, "observer_member_id"), &OValue::str("02aabb"));
}

/// The guard/stamp asymmetry described in the module docs: a
/// whitespace-only id is truthy to Python's guard but strips to empty.
/// Ported exactly, not repaired.
#[test]
fn a_whitespace_only_member_id_stamps_as_empty_matching_pythons_strip() {
    let stamped = stamp_observer_member_id(kernel_snapshot(), "   ").expect("a valid snapshot");
    assert_eq!(get(&stamped, "observer_member_id"), &OValue::str(""));
}

/// The merge does exactly one thing. Everything the frozen kernel decided
/// -- validation, TTL prune, sort order, truncation -- passes through
/// untouched.
#[test]
fn the_frozen_kernels_four_keys_survive_the_stamp_unchanged() {
    let before = kernel_snapshot();
    let stamped = stamp_observer_member_id(before.clone(), "02aabb").expect("a valid snapshot");

    for key in [
        "generated_at",
        "ttl_seconds",
        "schema_version",
        "segment_observations",
    ] {
        assert_eq!(get(&stamped, key), get(&before, key), "{key} was altered");
    }
}

// =====================================================================
// typed refusals -- no .ok()-to-null conversion (Task 68's rule)
// =====================================================================

/// Stamping twice would silently produce a dict with two
/// `observer_member_id` keys, which `json.dumps` happily emits and which no
/// consumer can interpret. The kernel never emits the key, so its presence
/// means someone already stamped.
#[test]
fn a_snapshot_that_already_carries_an_observer_id_is_refused() {
    let already = OValue::obj(vec![
        ("generated_at".to_string(), OValue::Int(1050)),
        ("ttl_seconds".to_string(), OValue::Int(500)),
        ("schema_version".to_string(), OValue::Int(1)),
        (
            "observer_member_id".to_string(),
            OValue::str("someone-else"),
        ),
        (
            "segment_observations".to_string(),
            OValue::arr(vec![OValue::Int(1)]),
        ),
    ]);
    assert_eq!(
        stamp_observer_member_id(already, "02aabb"),
        Err(SnapshotRefusal::AlreadyStamped)
    );
}

#[test]
fn a_non_object_snapshot_is_refused() {
    assert_eq!(
        stamp_observer_member_id(OValue::arr(vec![]), "02aabb"),
        Err(SnapshotRefusal::NotAnObject)
    );
}

/// Without the array there is nothing to attribute, and the withhold
/// decision downstream has no count to read.
#[test]
fn a_snapshot_without_the_observations_key_is_refused() {
    let missing = OValue::obj(vec![
        ("generated_at".to_string(), OValue::Int(1050)),
        ("ttl_seconds".to_string(), OValue::Int(500)),
        ("schema_version".to_string(), OValue::Int(1)),
    ]);
    assert_eq!(
        stamp_observer_member_id(missing, "02aabb"),
        Err(SnapshotRefusal::MissingObservations)
    );
}

#[test]
fn observations_that_are_not_an_array_are_refused() {
    let wrong = OValue::obj(vec![
        ("generated_at".to_string(), OValue::Int(1050)),
        ("ttl_seconds".to_string(), OValue::Int(500)),
        ("schema_version".to_string(), OValue::Int(1)),
        ("segment_observations".to_string(), OValue::Int(3)),
    ]);
    assert_eq!(
        stamp_observer_member_id(wrong, "02aabb"),
        Err(SnapshotRefusal::ObservationsNotAnArray)
    );
}

#[test]
fn every_snapshot_refusal_carries_a_distinct_actionable_code() {
    let codes = [
        SnapshotRefusal::NotAnObject.code(),
        SnapshotRefusal::AlreadyStamped.code(),
        SnapshotRefusal::MissingObservations.code(),
        SnapshotRefusal::ObservationsNotAnArray.code(),
    ];
    let unique: std::collections::BTreeSet<_> = codes.iter().collect();
    assert_eq!(
        unique.len(),
        codes.len(),
        "codes must not collide: {codes:?}"
    );
    for code in codes {
        assert!(code.starts_with("segment_snapshot_"), "{code}");
    }
}

// =====================================================================
// the count the withhold decision reads
// =====================================================================

/// py `if not snapshot.get("segment_observations"): return False` -- R68-7's
/// `segment_publish_is_allowed` takes a count, and this is where that count
/// comes from.
#[test]
fn the_observation_count_is_the_length_of_the_frozen_kernels_array() {
    assert_eq!(
        snapshot_observation_count(&kernel_snapshot()),
        Ok(1),
        "one observation in the fixture snapshot"
    );

    let empty = OValue::obj(vec![(
        "segment_observations".to_string(),
        OValue::arr(vec![]),
    )]);
    assert_eq!(snapshot_observation_count(&empty), Ok(0));
}

#[test]
fn an_uncountable_snapshot_is_refused_rather_than_counted_as_zero() {
    assert_eq!(
        snapshot_observation_count(&OValue::Int(3)),
        Err(SnapshotRefusal::NotAnObject)
    );

    let wrong = OValue::obj(vec![("segment_observations".to_string(), OValue::Int(3))]);
    assert_eq!(
        snapshot_observation_count(&wrong),
        Err(SnapshotRefusal::ObservationsNotAnArray)
    );

    let missing = OValue::obj(vec![("generated_at".to_string(), OValue::Int(1))]);
    assert_eq!(
        snapshot_observation_count(&missing),
        Err(SnapshotRefusal::MissingObservations)
    );
}

// =====================================================================
// against the real frozen kernel, not a hand-built shape
// =====================================================================

/// The hand-built `kernel_snapshot()` above encodes my belief about what
/// the kernel emits. This test removes that assumption by driving the real
/// store, so the merge is proven against the thing it will actually
/// consume in production.
#[test]
fn a_real_frozen_store_export_stamps_into_the_python_key_order() {
    let store = SegmentObservationStore::new(500, 4);
    store.record("111x1x0", 0, 10_000, "liquidity", 0.9, 1000);
    store.record("222x2x1", 1, 60_000, "fee", 1.0, 1010);

    let exported = store.export_snapshot(1050);
    assert!(
        !exported
            .as_obj()
            .expect("the kernel returns an object")
            .iter()
            .any(|(k, _)| k == "observer_member_id"),
        "the frozen kernel must NOT stamp -- if it starts to, this merge double-stamps"
    );
    assert_eq!(
        snapshot_observation_count(&exported),
        Ok(2),
        "both records survive validation at now=1050 with ttl=500"
    );

    let stamped = stamp_observer_member_id(exported, "test-node").expect("a valid snapshot");
    assert_eq!(keys(&stamped), SNAPSHOT_KEY_ORDER.to_vec());
    assert_eq!(
        get(&stamped, "observer_member_id"),
        &OValue::str("test-node")
    );
}

/// Cross-check the key order against the RECORDED PYTHON OUTPUT rather
/// than against my reading of the source: the fixture is real
/// `export_snapshot` output, and its five keys must appear in
/// [`SNAPSHOT_KEY_ORDER`] order in the raw file text.
#[test]
fn the_key_order_matches_the_recorded_python_fixture() {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/rebalance/segstore.json");
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read fixture: {e}"));

    let snapshot_at = raw
        .find(r#""snapshot""#)
        .expect("fixture has an export step");
    let body = &raw[snapshot_at..];

    let mut previous = 0usize;
    for key in SNAPSHOT_KEY_ORDER {
        let at = body
            .find(&format!(r#""{key}""#))
            .unwrap_or_else(|| panic!("fixture snapshot contains {key}"));
        assert!(
            at > previous,
            "{key} is out of python's literal order in the recorded fixture"
        );
        previous = at;
    }
}
