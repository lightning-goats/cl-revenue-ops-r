//! R68-8 (RED): the segment-observation publish decision, end to end.
//!
//! Ties together the three pieces this producer needs and proves them
//! against real Python output rather than against my reading of it:
//!
//! 1. the frozen `revops_rebalance::segstore` kernel's `export_snapshot`,
//! 2. R68-8's `observer_member_id` stamp (`tests/segment_snapshot.rs`),
//! 3. R68-7's withhold rules, and the frozen
//!    `revops_analytics::telemetry::datastore_envelope`.
//!
//! ## The conversion, and why `PyVal` grew a list variant
//!
//! `export_snapshot` returns an `OValue` (`revops_fees::pyjson`) while the
//! envelope and [`crate::datastore_producers::publish`] speak `PyDict`
//! (`revops_analytics::telemetry`). Those are two independent
//! Python-parity value models in this workspace, and the segment payload
//! is the first one that has to cross between them.
//!
//! It could not, as typed: `PyVal` was `Bool|Int|Float|Str|Dict` with NO
//! array variant, and `segment_observations` is irreducibly a list of
//! observation objects. The variant was added additively under the team
//! ruling recorded in working-set item 197 -- every pre-existing telemetry
//! golden is untouched and still asserts the same bytes, and the new list
//! goldens in `revops-analytics/tests/telemetry.rs` were taken from real
//! CPython `json.dumps` output.
//!
//! `OValue::Null` has no `PyVal` counterpart and is REFUSED rather than
//! dropped or coerced: silently omitting a null would publish a payload
//! that differs from the snapshot the kernel actually produced.

use revops::datastore_producers::{segment_decision, PublishDecision, SnapshotRefusal, Withheld};
use revops_analytics::telemetry::{datastore_envelope, python_dumps, PyVal};
use revops_fees::pyjson::OValue;
use revops_rebalance::segstore::SegmentObservationStore;

/// Replays `fixtures/rebalance/segstore.json`'s recorded sequence exactly
/// (`init: ttl_seconds=500, max_observations=4`, then its eight `record`
/// steps) so the export below is the same one Python recorded.
fn fixture_store() -> SegmentObservationStore {
    let store = SegmentObservationStore::new(500, 4);
    store.record("111x1x0", 0, 10_000, "liquidity", 0.9, 1000);
    store.record("zero-amount", 0, 0, "liquidity", 0.5, 1005);
    store.record("neg-amount", 0, -5, "liquidity", 0.5, 1006);
    store.record("222x2x1", 1, 60_000, "fee", 1.5, 1010);
    store.record("333x3x0", 2, 200_000, "timeout", -0.3, 1020);
    store.record("444x4x1", 1, 5_000_000, "not_a_real_class", 2.0, 1030);
    store.record("", 0, 80_000, "liquidity", 0.4, 1040);
    store.record("555x5x0", 0, 80_000, "liquidity", 0.4, 1040);
    store
}

fn published(decision: PublishDecision) -> revops_analytics::telemetry::PyDict {
    match decision {
        PublishDecision::Publish(payload) => payload,
        PublishDecision::Withhold(reason) => panic!("expected a publish, withheld: {reason:?}"),
    }
}

// =====================================================================
// the whole path, against real recorded Python output
// =====================================================================

/// The strongest test in this slice: the real frozen store exports, R68-8
/// stamps, the value model converts, the frozen envelope encodes -- and the
/// resulting bytes equal `json.dumps` of the snapshot PYTHON recorded in
/// `fixtures/rebalance/segstore.json` (its first `export` step, `now=1050`,
/// `observer_member_id="test-node"`), with the envelope's `timestamp`
/// appended.
///
/// Nothing in this expectation was written by hand: it is CPython's output
/// for the fixture's own object.
#[test]
fn the_published_blob_is_byte_identical_to_pythons_recorded_snapshot() {
    let snapshot = fixture_store().export_snapshot(1050);
    let payload = published(segment_decision(snapshot, "test-node").expect("a valid snapshot"));

    assert_eq!(
        datastore_envelope(payload, 1234, 60_000).expect("within the size cap"),
        r#"{"generated_at": 1050, "ttl_seconds": 500, "schema_version": 1, "observer_member_id": "test-node", "segment_observations": [{"observation_id": "obs-1040-6", "short_channel_id": "555x5x0", "direction": 0, "amount_bucket_sats": 50000, "outcome": "failure", "failure_class": "liquidity", "confidence": 0.4, "observed_at": 1040, "source_channel_id": "", "dest_channel_id": "", "route_policy": "", "router_kind": "", "correlation_id": ""}, {"observation_id": "obs-1030-4", "short_channel_id": "444x4x1", "direction": 1, "amount_bucket_sats": 5000000, "outcome": "failure", "failure_class": "unknown", "confidence": 1.0, "observed_at": 1030, "source_channel_id": "", "dest_channel_id": "", "route_policy": "", "router_kind": "", "correlation_id": ""}], "timestamp": 1234}"#
    );
}

// =====================================================================
// withholding (R68-7's rules, now reached through a real snapshot)
// =====================================================================

/// py `if not snapshot.get("segment_observations"): return False`
/// (`modules/rebalance_engine_v2.py:3174-3175`). A store that has recorded
/// nothing exports a well-formed snapshot with an EMPTY array -- publishing
/// it would overwrite a still-useful previous snapshot with nothing.
#[test]
fn an_empty_observation_set_withholds_the_publish() {
    let snapshot = SegmentObservationStore::new(500, 4).export_snapshot(1050);
    assert_eq!(
        segment_decision(snapshot, "test-node"),
        Ok(PublishDecision::Withhold(Withheld::NoObservations))
    );
}

/// Every observation TTL-expired: the snapshot is still well formed, the
/// array is empty again, and the same withhold applies.
#[test]
fn observations_aged_past_the_ttl_withhold_the_publish() {
    let snapshot = fixture_store().export_snapshot(9_999);
    assert_eq!(
        segment_decision(snapshot, "test-node"),
        Ok(PublishDecision::Withhold(Withheld::NoObservations))
    );
}

/// py `if store is None or not observer_member_id: return False`.
#[test]
fn an_unnamed_observer_withholds_the_publish() {
    let snapshot = fixture_store().export_snapshot(1050);
    assert_eq!(
        segment_decision(snapshot, ""),
        Ok(PublishDecision::Withhold(Withheld::NoObserverIdentity))
    );
}

/// Both guards fail at once; the identity is reported, because it is the
/// one the operator must fix.
#[test]
fn an_unnamed_observer_with_no_observations_reports_the_identity_first() {
    let snapshot = SegmentObservationStore::new(500, 4).export_snapshot(1050);
    assert_eq!(
        segment_decision(snapshot, ""),
        Ok(PublishDecision::Withhold(Withheld::NoObserverIdentity))
    );
}

/// The guard reads the RAW string (Python truthiness) while the stamp
/// trims -- so a whitespace-only id publishes, carrying an empty
/// attribution. Ported exactly; see `tests/segment_snapshot.rs`.
#[test]
fn a_whitespace_only_observer_publishes_with_an_empty_attribution() {
    let snapshot = fixture_store().export_snapshot(1050);
    let payload = published(segment_decision(snapshot, "   ").expect("a valid snapshot"));

    assert!(
        python_dumps(&PyVal::Dict(payload)).contains(r#""observer_member_id": """#),
        "python's guard is truthy on whitespace but its stamp strips"
    );
}

// =====================================================================
// withhold is not refusal
// =====================================================================

/// A malformed snapshot is REFUSED, not withheld. Both end in "nothing was
/// published", but a withhold is the system working as designed and a
/// refusal is a bug an operator needs to see -- collapsing them would hide
/// the second inside the first forever.
#[test]
fn a_malformed_snapshot_is_refused_rather_than_quietly_withheld() {
    assert_eq!(
        segment_decision(OValue::Int(3), "test-node"),
        Err(SnapshotRefusal::NotAnObject)
    );

    let missing = OValue::obj(vec![("generated_at".to_string(), OValue::Int(1050))]);
    assert_eq!(
        segment_decision(missing, "test-node"),
        Err(SnapshotRefusal::MissingObservations)
    );
}

/// `PyVal` has no null. Dropping the key or coercing it to `""` would
/// publish a payload that differs from the snapshot the kernel produced,
/// so the conversion refuses instead.
#[test]
fn a_null_in_the_snapshot_is_refused_rather_than_dropped_or_coerced() {
    let with_null = OValue::obj(vec![
        ("generated_at".to_string(), OValue::Null),
        (
            "segment_observations".to_string(),
            OValue::arr(vec![OValue::Int(1)]),
        ),
    ]);
    assert_eq!(
        segment_decision(with_null, "test-node"),
        Err(SnapshotRefusal::UnsupportedNull)
    );
}

/// A null nested inside an observation must be refused too -- the
/// conversion has to walk the whole tree, not just the top level.
#[test]
fn a_null_nested_inside_an_observation_is_also_refused() {
    let nested = OValue::obj(vec![
        ("generated_at".to_string(), OValue::Int(1050)),
        (
            "segment_observations".to_string(),
            OValue::arr(vec![OValue::obj(vec![(
                "confidence".to_string(),
                OValue::Null,
            )])]),
        ),
    ]);
    assert_eq!(
        segment_decision(nested, "test-node"),
        Err(SnapshotRefusal::UnsupportedNull)
    );
}

#[test]
fn the_null_refusal_carries_its_own_actionable_code() {
    assert_eq!(
        SnapshotRefusal::UnsupportedNull.code(),
        "segment_snapshot_unsupported_null"
    );
}

// =====================================================================
// the conversion preserves what the kernel decided
// =====================================================================

/// Order, nesting and value types all survive the crossing between the two
/// value models. Asserted on the serialized form, since that is what the
/// datastore stores.
#[test]
fn the_conversion_preserves_order_nesting_and_value_types() {
    let snapshot = OValue::obj(vec![
        ("generated_at".to_string(), OValue::Int(1050)),
        ("ttl_seconds".to_string(), OValue::Int(500)),
        ("schema_version".to_string(), OValue::Int(1)),
        (
            "segment_observations".to_string(),
            OValue::arr(vec![OValue::obj(vec![
                ("confidence".to_string(), OValue::Float(0.4)),
                ("outcome".to_string(), OValue::str("failure")),
                ("direction".to_string(), OValue::Int(0)),
                ("nested".to_string(), OValue::arr(vec![OValue::Bool(true)])),
            ])]),
        ),
    ]);
    let payload = published(segment_decision(snapshot, "02aabb").expect("a valid snapshot"));

    assert_eq!(
        python_dumps(&PyVal::Dict(payload)),
        r#"{"generated_at": 1050, "ttl_seconds": 500, "schema_version": 1, "observer_member_id": "02aabb", "segment_observations": [{"confidence": 0.4, "outcome": "failure", "direction": 0, "nested": [true]}]}"#
    );
}

/// The size cap is the frozen envelope's, applied to the real encoded
/// bytes -- this producer's payload is the one most likely to reach it,
/// since it grows with the observation count.
#[test]
fn an_oversized_snapshot_is_refused_by_the_frozen_envelope() {
    let store = SegmentObservationStore::new(100_000, 5_000);
    for i in 0..3_000 {
        store.record(&format!("{i}x{i}x0"), 0, 80_000, "liquidity", 0.4, 1_000);
    }
    let payload = published(segment_decision(store.export_snapshot(1_050), "test-node").unwrap());

    assert!(
        datastore_envelope(payload, 1234, 60_000).is_err(),
        "3000 observations must exceed the 60000-byte cap"
    );
}
