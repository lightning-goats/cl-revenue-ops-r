//! `SegmentObservationStore` (port of `modules/segment_observations.py`,
//! `~/bin/cl_revenue_ops-port`, branch `port`).
//!
//! Thread-safe, in-memory, TTL-bounded ring of local failure-derived
//! route-segment observations. The engine (T7) calls [`SegmentObservationStore::record`]
//! from the executor's failure path and periodically calls
//! [`SegmentObservationStore::export_snapshot`] to build the blob it writes
//! verbatim to CLN datastore key `["revenue", "segment-observations"]`
//! ([`DATASTORE_KEY`]).
//!
//! ## Scope decision: `observer_member_id`
//!
//! Python's `export_snapshot(self, *, observer_member_id: str, now=None)`
//! stamps a caller-supplied `observer_member_id` into the returned dict.
//! The Task 3 interface frozen by the plan is `pub fn export_snapshot(&self,
//! now) -> OValue` — no `observer_member_id` parameter. This is a
//! deliberate, documented scope decision (not an oversight): the fixture
//! (`fixtures/rebalance/segstore.json`) records the real Python output
//! including `observer_member_id`, but this task's snapshot omits that key
//! entirely; the engine (T7), which knows its own member id, is expected to
//! merge it in when assembling the datastore write. Tests below assert
//! field-by-field against the fixture's `snapshot` object rather than whole
//! -object equality, explicitly skipping `observer_member_id`.
//!
//! ## Bucket function
//!
//! [`bucket_amount_sats`] is a port of `SegmentObservationStore
//! .bucket_amount_sats` (`segment_observations.py:41-54`): amounts `<= 0`
//! bucket to `0` (rejected — [`SegmentObservationStore::record`] returns
//! `None` and does NOT consume an `observation_id` sequence number for
//! these); `1..=49_999` bucket to `50_000`; otherwise the largest entry of
//! [`BUCKETS`] not exceeding `amount_sats` (buckets `>= 10_000_000` all
//! bucket to `10_000_000`, the table's ceiling).
//!
//! ## Validation / TTL-prune / sort (export time only)
//!
//! Python's `_valid_observation` (called only from `export_snapshot`, never
//! from `record_failure`) is the ONLY place raw stored fields are
//! normalized: `short_channel_id`/`source_channel_id`/etc. are trimmed,
//! `failure_class` collapses to `"unknown"` if empty or not one of
//! `{liquidity, fee, timeout, unknown}`, `confidence` clamps to `[0, 1]`,
//! and entries fail validation (dropped from the snapshot) if
//! `short_channel_id` is empty, `direction` is not `0`/`1`, `amount_bucket_sats
//! <= 0`, or `observed_at <= 0 || now - observed_at > ttl_seconds`. Crucially,
//! `record()`'s stored entry keeps the RAW (un-trimmed, un-clamped,
//! un-normalized) values — only `export_snapshot` normalizes, and it does so
//! on a *copy* used for the returned snapshot, while ALSO permanently
//! replacing the internal ring with the filtered (now-normalized) survivors
//! (`segment_observations.py:141-153`) — a dropped-then-gone entry does not
//! reappear on a later export even if a later export's `now` would not by
//! itself have TTL-expired it.

use revops_fees::pyjson::OValue;
use std::collections::VecDeque;
use std::sync::Mutex;

/// `segment_observations.py:14` (`DATASTORE_KEY`).
pub const DATASTORE_KEY: [&str; 2] = ["revenue", "segment-observations"];
/// `segment_observations.py:15` (`SCHEMA_VERSION`).
pub const SCHEMA_VERSION: i64 = 1;
/// `segment_observations.py:16` (`DEFAULT_TTL_SECONDS`).
pub const DEFAULT_TTL_SECONDS: i64 = 900;
/// `segment_observations.py:17` (`DEFAULT_MAX_OBSERVATIONS`).
pub const DEFAULT_MAX_OBSERVATIONS: i64 = 200;

/// `segment_observations.py:18-27` (`BUCKETS`).
const BUCKETS: [i64; 8] = [
    50_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
];

const VALID_FAILURE_CLASSES: [&str; 4] = ["liquidity", "fee", "timeout", "unknown"];

/// Port of `SegmentObservationStore.bucket_amount_sats`
/// (`segment_observations.py:41-54`, a classmethod in Python). EXACT
/// semantics: `amount_sats <= 0` -> `0`; else the largest entry of
/// [`BUCKETS`] that does not exceed `amount_sats` (so `1..=49_999` bucket to
/// `50_000`, and anything `>= 10_000_000` buckets to `10_000_000`).
pub fn bucket_amount_sats(amount_sats: i64) -> i64 {
    if amount_sats <= 0 {
        return 0;
    }
    let mut bucket = BUCKETS[0];
    for candidate in BUCKETS {
        if amount_sats < candidate {
            break;
        }
        bucket = candidate;
    }
    bucket
}

/// One stored segment observation. Mirrors the dict shape
/// `record_failure`/`_valid_observation` produce in Python. Returned raw
/// (un-normalized) by [`SegmentObservationStore::record`]; returned
/// normalized (trimmed/clamped, see module docs) inside
/// [`SegmentObservationStore::export_snapshot`]'s `segment_observations`
/// array.
///
/// `source_channel_id`/`dest_channel_id`/`route_policy`/`router_kind`/
/// `correlation_id` are part of Python's normalized dict shape but are not
/// settable via this task's frozen six-parameter `record` signature, so
/// they are always `""` for observations created here (documented scope
/// decision, same as the `observer_member_id` one above — a future task
/// may widen `record` if the engine needs to populate them).
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentObservation {
    pub observation_id: String,
    pub short_channel_id: String,
    pub direction: u8,
    pub amount_bucket_sats: i64,
    pub outcome: &'static str,
    pub failure_class: String,
    pub confidence: f64,
    pub observed_at: i64,
    pub source_channel_id: String,
    pub dest_channel_id: String,
    pub route_policy: String,
    pub router_kind: String,
    pub correlation_id: String,
}

fn observation_to_ovalue(o: &SegmentObservation) -> OValue {
    OValue::obj(vec![
        ("observation_id".to_string(), OValue::str(&o.observation_id)),
        (
            "short_channel_id".to_string(),
            OValue::str(&o.short_channel_id),
        ),
        ("direction".to_string(), OValue::Int(o.direction as i64)),
        (
            "amount_bucket_sats".to_string(),
            OValue::Int(o.amount_bucket_sats),
        ),
        ("outcome".to_string(), OValue::str(o.outcome)),
        ("failure_class".to_string(), OValue::str(&o.failure_class)),
        ("confidence".to_string(), OValue::Float(o.confidence)),
        ("observed_at".to_string(), OValue::Int(o.observed_at)),
        (
            "source_channel_id".to_string(),
            OValue::str(&o.source_channel_id),
        ),
        (
            "dest_channel_id".to_string(),
            OValue::str(&o.dest_channel_id),
        ),
        ("route_policy".to_string(), OValue::str(&o.route_policy)),
        ("router_kind".to_string(), OValue::str(&o.router_kind)),
        ("correlation_id".to_string(), OValue::str(&o.correlation_id)),
    ])
}

struct Inner {
    observations: VecDeque<SegmentObservation>,
    next_observation_id: u64,
}

/// Port of `SegmentObservationStore` (`segment_observations.py:10-153`).
/// Thread-safe (single internal `Mutex`); `Mutex<VecDeque<..>>` per the T3
/// interface (max [`DEFAULT_MAX_OBSERVATIONS`], TTL [`DEFAULT_TTL_SECONDS`]
/// by default, both overridable via [`SegmentObservationStore::new`]).
pub struct SegmentObservationStore {
    ttl_seconds: i64,
    max_observations: usize,
    inner: Mutex<Inner>,
}

impl SegmentObservationStore {
    /// Port of `SegmentObservationStore.__init__`
    /// (`segment_observations.py:29-36`): `ttl_seconds` clamps to a floor
    /// of 60, `max_observations` clamps to a floor of 1.
    pub fn new(ttl_seconds: i64, max_observations: i64) -> Self {
        Self {
            ttl_seconds: ttl_seconds.max(60),
            max_observations: max_observations.max(1) as usize,
            inner: Mutex::new(Inner {
                observations: VecDeque::new(),
                next_observation_id: 1,
            }),
        }
    }

    /// Default-tuned store: [`DEFAULT_TTL_SECONDS`] /
    /// [`DEFAULT_MAX_OBSERVATIONS`] (Python's `__init__` defaults).
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_TTL_SECONDS, DEFAULT_MAX_OBSERVATIONS)
    }

    /// Port of `record_failure` (`segment_observations.py:97-129`), narrowed
    /// to this task's frozen six-parameter interface (see module docs on
    /// the omitted optional Python kwargs). Returns `None` without
    /// consuming an `observation_id` sequence number when
    /// `bucket_amount_sats(amount_sats) <= 0` (`segment_observations.py:110
    /// -112`: the bucket check happens BEFORE the lock is taken and the
    /// counter incremented).
    pub fn record(
        &self,
        short_channel_id: &str,
        direction: u8,
        amount_sats: i64,
        failure_class: &str,
        confidence: f64,
        now: i64,
    ) -> Option<SegmentObservation> {
        let bucket = bucket_amount_sats(amount_sats);
        if bucket <= 0 {
            return None;
        }

        let mut inner = self.inner.lock().expect("segstore mutex poisoned");
        let seq = inner.next_observation_id;
        inner.next_observation_id += 1;

        let entry = SegmentObservation {
            observation_id: format!("obs-{now}-{seq}"),
            short_channel_id: short_channel_id.to_string(),
            direction,
            amount_bucket_sats: bucket,
            outcome: "failure",
            failure_class: failure_class.to_string(),
            confidence,
            observed_at: now,
            source_channel_id: String::new(),
            dest_channel_id: String::new(),
            route_policy: String::new(),
            router_kind: String::new(),
            correlation_id: String::new(),
        };

        inner.observations.push_back(entry.clone());
        while inner.observations.len() > self.max_observations {
            inner.observations.pop_front();
        }
        Some(entry)
    }

    /// Port of `_valid_observation` (`segment_observations.py:56-89`),
    /// applied only at export time. Returns `None` for entries that fail
    /// validation (dropped from the snapshot and from the internal ring).
    fn valid_observation(
        &self,
        entry: &SegmentObservation,
        now: i64,
    ) -> Option<SegmentObservation> {
        let short_channel_id = entry.short_channel_id.trim().to_string();
        if short_channel_id.is_empty() || (entry.direction != 0 && entry.direction != 1) {
            return None;
        }
        if entry.amount_bucket_sats <= 0 {
            return None;
        }
        if entry.observed_at <= 0 || (now - entry.observed_at) > self.ttl_seconds {
            return None;
        }

        let trimmed_class = entry.failure_class.trim();
        let failure_class = if trimmed_class.is_empty() {
            "unknown".to_string()
        } else if VALID_FAILURE_CLASSES.contains(&trimmed_class) {
            trimmed_class.to_string()
        } else {
            "unknown".to_string()
        };

        let observation_id = entry.observation_id.trim().to_string();
        if observation_id.is_empty() {
            return None;
        }

        Some(SegmentObservation {
            observation_id,
            short_channel_id,
            direction: entry.direction,
            amount_bucket_sats: entry.amount_bucket_sats,
            outcome: "failure",
            failure_class,
            confidence: entry.confidence.clamp(0.0, 1.0),
            observed_at: entry.observed_at,
            source_channel_id: entry.source_channel_id.trim().to_string(),
            dest_channel_id: entry.dest_channel_id.trim().to_string(),
            route_policy: entry.route_policy.trim().to_string(),
            router_kind: entry.router_kind.trim().to_string(),
            correlation_id: entry.correlation_id.trim().to_string(),
        })
    }

    /// Port of `export_snapshot` (`segment_observations.py:131-153`),
    /// narrowed to this task's frozen `(&self, now) -> OValue` interface
    /// (`observer_member_id` omitted — see module docs). Validates + prunes
    /// (TTL, malformed direction/scid/bucket), sorts by `observed_at`
    /// descending (stable — ties keep insertion order, matching Python's
    /// stable `list.sort(..., reverse=True)`), permanently replaces the
    /// internal ring with the tail (last `max_observations`) of that sorted
    /// list (`segment_observations.py:145`: `valid[-self.max_observations:]`
    /// — a no-op in practice since `record` already caps ring size, kept
    /// for exact parity), and returns the FULL (untruncated) sorted list in
    /// `segment_observations`.
    pub fn export_snapshot(&self, now: i64) -> OValue {
        let mut inner = self.inner.lock().expect("segstore mutex poisoned");

        let mut valid: Vec<SegmentObservation> = inner
            .observations
            .iter()
            .filter_map(|e| self.valid_observation(e, now))
            .collect();
        valid.sort_by_key(|o| std::cmp::Reverse(o.observed_at));

        let full = valid.clone();
        let start = valid.len().saturating_sub(self.max_observations);
        let capped = valid.split_off(start);
        inner.observations = capped.into();

        OValue::obj(vec![
            ("generated_at".to_string(), OValue::Int(now)),
            ("ttl_seconds".to_string(), OValue::Int(self.ttl_seconds)),
            ("schema_version".to_string(), OValue::Int(SCHEMA_VERSION)),
            (
                "segment_observations".to_string(),
                OValue::arr(full.iter().map(observation_to_ovalue).collect()),
            ),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn fixture() -> serde_json::Value {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/rebalance/segstore.json");
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        serde_json::from_str(&raw).expect("valid JSON")
    }

    /// Round-trips an [`OValue`] through the Python-parity writer so it can
    /// be compared against the `serde_json`-parsed fixture with plain
    /// `assert_eq!` (object key order doesn't matter for equality either
    /// way, since `serde_json::Value::Object` is itself unordered).
    fn to_json(v: &OValue) -> serde_json::Value {
        serde_json::from_str(&revops_fees::pyjson::dumps_python(v)).expect("valid JSON out")
    }

    fn observation_to_json(o: &SegmentObservation) -> serde_json::Value {
        to_json(&observation_to_ovalue(o))
    }

    #[test]
    fn bucket_amount_sats_matches_python_fixture_boundaries() {
        let fx = fixture();
        let cases = fx["bucket_boundaries"].as_array().expect("array");
        assert!(cases.len() > 20, "expect a healthy boundary spread");
        for case in cases {
            let amount = case["amount_sats"].as_i64().expect("amount_sats");
            let expected = case["bucket"].as_i64().expect("bucket");
            assert_eq!(
                bucket_amount_sats(amount),
                expected,
                "bucket_amount_sats({amount})"
            );
        }
    }

    #[test]
    fn bucket_amount_sats_explicit_contract_points() {
        assert_eq!(bucket_amount_sats(-1), 0);
        assert_eq!(bucket_amount_sats(0), 0);
        assert_eq!(bucket_amount_sats(1), 50_000);
        assert_eq!(bucket_amount_sats(49_999), 50_000);
        assert_eq!(bucket_amount_sats(50_000), 50_000);
        assert_eq!(bucket_amount_sats(50_001), 50_000);
        assert_eq!(bucket_amount_sats(10_000_000), 10_000_000);
        assert_eq!(bucket_amount_sats(10_000_000 + 1), 10_000_000);
        assert_eq!(bucket_amount_sats(999_999_999), 10_000_000);
    }

    #[test]
    fn record_and_export_sequence_matches_python_fixture() {
        let fx = fixture();
        let seq = &fx["sequence"];
        let ttl_seconds = seq["init"]["ttl_seconds"].as_i64().unwrap();
        let max_observations = seq["init"]["max_observations"].as_i64().unwrap();
        let store = SegmentObservationStore::new(ttl_seconds, max_observations);

        // `steps` is a SINGLE chronologically-ordered list (not separate
        // records/exports arrays): export_snapshot mutates the internal
        // ring (permanently prunes it), so record/export calls MUST replay
        // in exactly this interleaved order.
        let steps = seq["steps"].as_array().expect("steps array");
        for step in steps {
            match step["op"].as_str().unwrap() {
                "record" => {
                    let args = &step["args"];
                    let scid = args["short_channel_id"].as_str().unwrap();
                    let direction = args["direction"].as_i64().unwrap() as u8;
                    let amount_sats = args["amount_sats"].as_i64().unwrap();
                    let failure_class = args["failure_class"].as_str().unwrap();
                    let confidence = args["confidence"].as_f64().unwrap();
                    let observed_at = args["observed_at"].as_i64().unwrap();

                    let got = store.record(
                        scid,
                        direction,
                        amount_sats,
                        failure_class,
                        confidence,
                        observed_at,
                    );

                    match (&got, &step["result"]) {
                        (None, serde_json::Value::Null) => {}
                        (Some(obs), expected) => {
                            assert_eq!(
                                &observation_to_json(obs),
                                expected,
                                "record({scid}, {direction}, {amount_sats}) result"
                            );
                        }
                        (None, expected) => panic!("expected {expected:?}, got None"),
                    }
                }
                "export" => {
                    let now = step["now"].as_i64().unwrap();
                    let snapshot = to_json(&store.export_snapshot(now));
                    let expected = &step["snapshot"];

                    assert_eq!(snapshot["generated_at"], expected["generated_at"]);
                    assert_eq!(snapshot["ttl_seconds"], expected["ttl_seconds"]);
                    assert_eq!(snapshot["schema_version"], expected["schema_version"]);
                    assert_eq!(
                        snapshot["segment_observations"], expected["segment_observations"],
                        "export_snapshot({now}) segment_observations"
                    );
                    // observer_member_id is intentionally NOT part of this
                    // task's narrowed export_snapshot interface (module
                    // docs) — the fixture carries it (real Python output),
                    // the Rust snapshot does not, and that asymmetry is
                    // the point of this assertion.
                    assert!(snapshot.get("observer_member_id").is_none());
                    assert!(expected.get("observer_member_id").is_some());
                }
                other => panic!("unknown step op {other:?}"),
            }
        }
    }

    #[test]
    fn new_clamps_ttl_and_max_observations_floors() {
        let store = SegmentObservationStore::new(1, 0);
        // ttl floor of 60: an observation 61s old should already be pruned
        // (observed_at=1, not 0, so this exercises the TTL floor rather
        // than the separate observed_at <= 0 rejection).
        store.record("100x1x0", 0, 100_000, "liquidity", 0.5, 1);
        let snap = to_json(&store.export_snapshot(62));
        assert_eq!(
            snap["segment_observations"].as_array().unwrap().len(),
            0,
            "ttl floor of 60 should have pruned a 61s-old observation"
        );

        // max_observations floor of 1: two records, only the newest survives.
        let store2 = SegmentObservationStore::new(500, 0);
        store2.record("a", 0, 100_000, "liquidity", 0.5, 100);
        store2.record("b", 0, 100_000, "liquidity", 0.5, 200);
        let snap2 = to_json(&store2.export_snapshot(200));
        let obs = snap2["segment_observations"].as_array().unwrap();
        assert_eq!(obs.len(), 1, "max_observations floor of 1");
        assert_eq!(obs[0]["short_channel_id"], "b");
    }

    #[test]
    fn observation_id_sequence_not_consumed_by_rejected_records() {
        let store = SegmentObservationStore::with_defaults();
        let first = store
            .record("scid-a", 0, 100_000, "liquidity", 0.5, 1000)
            .expect("valid record");
        assert_eq!(first.observation_id, "obs-1000-1");

        // Rejected: amount_sats <= 0 bucket to 0, no sequence consumed.
        assert!(store
            .record("scid-b", 0, 0, "liquidity", 0.5, 1001)
            .is_none());
        assert!(store
            .record("scid-c", 0, -5, "liquidity", 0.5, 1002)
            .is_none());

        let second = store
            .record("scid-d", 0, 100_000, "liquidity", 0.5, 1003)
            .expect("valid record");
        assert_eq!(second.observation_id, "obs-1003-2");
    }
}
