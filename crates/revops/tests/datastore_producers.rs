//! R68-6 (RED): datastore producers with authoritative-store ownership
//! and readback.
//!
//! Task 68 names three producers, and Python writes all three the same
//! fire-and-forget way. `DataService.datastore_push`
//! (`modules/data_service.py:445`) is documented as *"Fire-and-forget:
//! logs failures, never raises. Returns True on success, False on
//! failure."* -- one `bool` collapsing five distinct outcomes (not a
//! dict, reserved `error` key, over the size cap, RPC raised, RPC
//! returned), and `True` means only that `rpc.datastore(...)` did not
//! throw. Nothing ever reads the key back, so "the RPC returned" stands
//! in for "the datastore holds this value".
//!
//! The loss compounds at the call sites:
//!
//! - `["revenue", "status"]` (`cl-revenue-ops.py:3717`) and
//!   `["revenue", "fee-bounds"]` (`cl-revenue-ops.py:3722`) discard the
//!   returned bool entirely, inside a block that ends
//!   `except Exception: pass  # Datastore push is best-effort`.
//! - `["revenue", "segment-observations"]`
//!   (`modules/rebalance_engine_v2.py:3179`) at least returns the bool,
//!   but its caller cannot tell a refused envelope from a failed write.
//!
//! That is precisely the `.ok()`-to-null conversion Task 68 forbids. The
//! generated inventory agrees this is unported: the `datastore` external
//! boundary carries `rust_adapter: null, rust_transport: "missing"`.
//!
//! Scope: ownership, the publish path, and readback. The ENVELOPE rules
//! (timestamp injection, the `error` key, the size cap) are already the
//! frozen `revops_analytics::telemetry::datastore_envelope` kernel and are
//! consumed here, never re-derived. Per-producer PAYLOAD assembly -- the
//! fee-bounds `(min + max) // 2` arithmetic, the status dict, the
//! segment-observation snapshot merge -- is deliberately the next slice.

use std::collections::HashMap;
use std::sync::Mutex;

use revops::datastore_producers::{
    publish, DatastoreTransport, Producer, PublishRefusal, DATASTORE_MAX_BYTES,
};
use revops::lifecycle::Owner;
use revops_analytics::telemetry::{PyDict, PyVal};

const NOW: i64 = 1_800_000_000;

/// Records every call, so a test can prove a refusal happened BEFORE the
/// write rather than after it.
#[derive(Default)]
struct FakeStore {
    stored: Mutex<HashMap<String, String>>,
    writes: Mutex<Vec<String>>,
    reads: Mutex<Vec<String>>,
    write_error: Option<String>,
    read_error: Option<String>,
    /// Accept the write, acknowledge it, and store nothing -- the exact
    /// shape a fire-and-forget push cannot see.
    swallow_write: bool,
}

fn joined(key: &[&str]) -> String {
    key.join("/")
}

impl FakeStore {
    fn new() -> Self {
        Self::default()
    }

    /// Pre-seed a key with bytes this publish did not write.
    fn holding(self, key: &[&str], value: &str) -> Self {
        self.stored
            .lock()
            .unwrap()
            .insert(joined(key), value.to_string());
        self
    }

    fn writes(&self) -> Vec<String> {
        self.writes.lock().unwrap().clone()
    }
    fn reads(&self) -> Vec<String> {
        self.reads.lock().unwrap().clone()
    }
    fn value(&self, key: &[&str]) -> Option<String> {
        self.stored.lock().unwrap().get(&joined(key)).cloned()
    }
}

impl DatastoreTransport for FakeStore {
    fn write(&self, key: &[&str], encoded: &str) -> Result<(), String> {
        self.writes.lock().unwrap().push(joined(key));
        if let Some(detail) = &self.write_error {
            return Err(detail.clone());
        }
        if !self.swallow_write {
            self.stored
                .lock()
                .unwrap()
                .insert(joined(key), encoded.to_string());
        }
        Ok(())
    }

    fn read(&self, key: &[&str]) -> Result<Option<String>, String> {
        self.reads.lock().unwrap().push(joined(key));
        if let Some(detail) = &self.read_error {
            return Err(detail.clone());
        }
        Ok(self.stored.lock().unwrap().get(&joined(key)).cloned())
    }
}

fn payload() -> PyDict {
    let mut p = PyDict::new();
    p.push("min_fee_ppm", PyVal::Int(50));
    p
}

// =====================================================================
// the roster and its authoritative owners
// =====================================================================

/// Exactly the three keys Python pushes for these producers. A fourth
/// would be a Rust-only datastore path; a missing one is a producer that
/// silently stopped publishing.
#[test]
fn the_producer_roster_is_exactly_the_three_keys_python_pushes() {
    let keys: Vec<Vec<&str>> = Producer::ALL.iter().map(|p| p.key().to_vec()).collect();
    assert_eq!(
        keys,
        vec![
            vec!["revenue", "status"],
            vec!["revenue", "fee-bounds"],
            vec!["revenue", "segment-observations"],
        ]
    );
}

/// "Explicit authoritative-store ownership": each key has exactly one
/// owner allowed to write it, derived from where Python pushes it. Status
/// and fee-bounds are pushed from the fee-adjustment path
/// (`cl-revenue-ops.py:3717`/`:3722`); segment-observations from the
/// rebalance engine (`rebalance_engine_v2.py:3179`).
#[test]
fn every_producer_declares_one_authoritative_owner() {
    assert_eq!(Producer::Status.owner(), Owner::FeeScheduler);
    assert_eq!(Producer::FeeBounds.owner(), Owner::FeeScheduler);
    assert_eq!(Producer::Rebalance.owner(), Owner::Rebalance);
}

/// Ownership is enforced BEFORE the write, not audited after it. A
/// refusal that still wrote has already corrupted the key it was meant
/// to protect.
#[test]
fn a_producer_key_is_written_by_its_owner_and_no_one_else() {
    let store = FakeStore::new();
    let refusal = publish(
        &store,
        Producer::FeeBounds,
        Owner::Rebalance,
        payload(),
        NOW,
    )
    .expect_err("only the fee scheduler owns fee-bounds");

    assert!(matches!(refusal, PublishRefusal::NotOwner { .. }));
    assert!(
        store.writes().is_empty(),
        "a non-owner must be refused before the write: {:?}",
        store.writes()
    );
}

/// Ownership is checked FIRST, before the payload is even encoded. A
/// non-owner told "your payload is too large" would shrink it and retry
/// a write it was never allowed to make; the actionable refusal is that
/// it does not own the key at all.
#[test]
fn a_non_owner_is_refused_before_its_payload_is_even_encoded() {
    let store = FakeStore::new();
    let mut big = PyDict::new();
    big.push("blob", PyVal::Str("x".repeat(DATASTORE_MAX_BYTES + 1)));

    let refusal = publish(&store, Producer::Status, Owner::Boltz, big, NOW)
        .expect_err("a non-owner with an oversized payload is still a non-owner");

    assert!(
        matches!(refusal, PublishRefusal::NotOwner { .. }),
        "ownership outranks the envelope: {refusal:?}"
    );
    assert!(store.writes().is_empty());
}

// =====================================================================
// the happy path
// =====================================================================

#[test]
fn a_clean_publish_stores_the_encoded_payload_under_its_key() {
    let store = FakeStore::new();
    let published = publish(
        &store,
        Producer::Status,
        Owner::FeeScheduler,
        payload(),
        NOW,
    )
    .expect("a stored and verified write is a publish");

    assert_eq!(published.producer, Producer::Status);
    let stored = store
        .value(&Producer::Status.key())
        .expect("the key holds the payload");
    assert_eq!(published.bytes, stored.len());
    assert!(
        stored.contains("min_fee_ppm"),
        "the payload must survive the envelope: {stored}"
    );
}

/// The envelope is the frozen analytics kernel's job, consumed here
/// rather than re-derived: an absent `timestamp` is injected.
#[test]
fn the_stored_payload_carries_the_python_envelope() {
    let store = FakeStore::new();
    publish(
        &store,
        Producer::Status,
        Owner::FeeScheduler,
        payload(),
        NOW,
    )
    .expect("clean publish");

    let stored = store.value(&Producer::Status.key()).unwrap();
    assert!(
        stored.contains("timestamp") && stored.contains(&NOW.to_string()),
        "datastore_envelope must inject the timestamp: {stored}"
    );
}

/// The readback must read the key that was WRITTEN. Verifying a
/// different key would confirm nothing at all.
#[test]
fn the_readback_reads_back_the_same_key_it_wrote() {
    let store = FakeStore::new();
    publish(
        &store,
        Producer::Rebalance,
        Owner::Rebalance,
        payload(),
        NOW,
    )
    .expect("clean publish");

    let expected = joined(&Producer::Rebalance.key());
    assert_eq!(store.writes(), vec![expected.clone()]);
    assert_eq!(
        store.reads(),
        vec![expected],
        "the verification must target the key that was written"
    );
}

// =====================================================================
// the envelope refusals (delegated, but must not be swallowed)
// =====================================================================

/// Python logs a warning and returns False. Truncating to fit, or
/// reporting success, would publish a payload no consumer can parse.
#[test]
fn an_oversized_payload_is_refused_rather_than_truncated() {
    let store = FakeStore::new();
    let mut big = PyDict::new();
    big.push("blob", PyVal::Str("x".repeat(DATASTORE_MAX_BYTES + 1)));

    let refusal = publish(&store, Producer::Status, Owner::FeeScheduler, big, NOW)
        .expect_err("an oversized payload is not publishable");

    assert!(matches!(refusal, PublishRefusal::Envelope { .. }));
    assert!(
        store.writes().is_empty(),
        "nothing may be written once the envelope is refused"
    );
}

#[test]
fn a_payload_carrying_a_reserved_error_key_is_refused() {
    let store = FakeStore::new();
    let mut bad = PyDict::new();
    bad.push("error", PyVal::Str("upstream failed".to_string()));

    let refusal = publish(&store, Producer::Status, Owner::FeeScheduler, bad, NOW)
        .expect_err("an error response is not a status snapshot");

    assert!(matches!(refusal, PublishRefusal::Envelope { .. }));
    assert!(store.writes().is_empty());
}

// =====================================================================
// readback: the gap this slice exists to close
// =====================================================================

#[test]
fn a_failed_write_is_not_a_publish() {
    let store = FakeStore {
        write_error: Some("connection refused".to_string()),
        ..FakeStore::new()
    };
    let refusal = publish(
        &store,
        Producer::Status,
        Owner::FeeScheduler,
        payload(),
        NOW,
    )
    .expect_err("a failed write is not a publish");

    assert!(matches!(refusal, PublishRefusal::WriteFailed { .. }));
}

/// THE failure Python cannot see. `rpc.datastore(...)` returned without
/// raising, so `datastore_push` returns `True` and the caller records a
/// successful publish -- while the key holds nothing. Every downstream
/// consumer then reads a stale value, or none, and the producer looks
/// healthy the entire time.
#[test]
fn a_write_that_was_acknowledged_but_stored_nothing_is_not_a_publish() {
    let store = FakeStore {
        swallow_write: true,
        ..FakeStore::new()
    };
    let refusal = publish(
        &store,
        Producer::Status,
        Owner::FeeScheduler,
        payload(),
        NOW,
    )
    .expect_err("an acknowledged write that stored nothing is not a publish");

    assert!(
        matches!(refusal, PublishRefusal::ReadbackMissing { .. }),
        "{refusal:?}"
    );
    assert_eq!(
        store.writes().len(),
        1,
        "the write WAS attempted and acknowledged"
    );
}

/// The key exists but holds bytes this publish did not write -- a
/// half-applied write, or another writer racing the same key. Presence
/// is not correctness.
#[test]
fn a_key_holding_bytes_this_publish_did_not_write_is_not_a_publish() {
    let store = FakeStore {
        swallow_write: true,
        ..FakeStore::new()
    }
    .holding(&Producer::FeeBounds.key(), r#"{"min_fee_ppm": 999}"#);

    let refusal = publish(
        &store,
        Producer::FeeBounds,
        Owner::FeeScheduler,
        payload(),
        NOW,
    )
    .expect_err("someone else's bytes are not this publish");

    assert!(
        matches!(refusal, PublishRefusal::ReadbackMismatch { .. }),
        "{refusal:?}"
    );
}

/// A readback that could not be performed is NOT proof of success --
/// the same rule R68-1 applied to an unstattable production path. The
/// write may well have landed; this publish simply cannot say so, and
/// "we could not check" must never be reported as "verified".
#[test]
fn a_readback_that_could_not_be_read_is_not_proof_of_success() {
    let store = FakeStore {
        read_error: Some("datastore unavailable".to_string()),
        ..FakeStore::new()
    };
    let refusal = publish(
        &store,
        Producer::Status,
        Owner::FeeScheduler,
        payload(),
        NOW,
    )
    .expect_err("an unverifiable write is not a verified one");

    assert!(
        matches!(refusal, PublishRefusal::ReadbackUnreadable { .. }),
        "{refusal:?}"
    );
    assert_eq!(store.writes().len(), 1, "the write itself did happen");
}

// =====================================================================
// refusal vocabulary
// =====================================================================

#[test]
fn every_refusal_carries_a_distinct_actionable_code() {
    let codes = [
        PublishRefusal::NotOwner {
            producer: Producer::Status,
            attempted_by: Owner::Boltz,
        }
        .code(),
        PublishRefusal::Envelope {
            producer: Producer::Status,
            detail: "too large".to_string(),
        }
        .code(),
        PublishRefusal::WriteFailed {
            producer: Producer::Status,
            detail: "boom".to_string(),
        }
        .code(),
        PublishRefusal::ReadbackUnreadable {
            producer: Producer::Status,
            detail: "boom".to_string(),
        }
        .code(),
        PublishRefusal::ReadbackMissing {
            producer: Producer::Status,
        }
        .code(),
        PublishRefusal::ReadbackMismatch {
            producer: Producer::Status,
        }
        .code(),
    ];
    let unique: std::collections::BTreeSet<_> = codes.iter().collect();
    assert_eq!(
        unique.len(),
        codes.len(),
        "codes must not collide: {codes:?}"
    );
    for code in codes {
        assert!(code.starts_with("datastore_"), "{code}");
    }
}

/// A code alone tells an operator what went wrong but not which producer
/// stopped publishing.
#[test]
fn every_refusal_names_the_producer_it_belongs_to() {
    for producer in Producer::ALL {
        assert_eq!(
            PublishRefusal::ReadbackMissing { producer }.producer(),
            producer
        );
        assert_eq!(
            PublishRefusal::NotOwner {
                producer,
                attempted_by: Owner::Boltz,
            }
            .producer(),
            producer
        );
    }
}

#[test]
fn the_size_cap_matches_the_python_constant() {
    // `_DATASTORE_MAX_BYTES = 60000` (modules/data_service.py:443), a
    // safety margin under CLN's 65KB limit. Const-asserted, so widening
    // it past the node's real ceiling fails the BUILD.
    const { assert!(DATASTORE_MAX_BYTES == 60_000) };
}
