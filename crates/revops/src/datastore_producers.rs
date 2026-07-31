//! R68-6: datastore producers with authoritative-store ownership and
//! readback.
//!
//! Python writes all three of Task 68's datastore producers through
//! `DataService.datastore_push` (`modules/data_service.py:445`), whose
//! own docstring reads *"Fire-and-forget: logs failures, never raises.
//! Returns True on success, False on failure."* That single `bool`
//! collapses five distinct outcomes -- payload not a dict, reserved
//! `error` key, over the size cap, the RPC raised, the RPC returned --
//! and `True` means only that `rpc.datastore(...)` did not throw.
//!
//! Nothing reads the key back. "The RPC returned" therefore stands in
//! for "the datastore holds this value", and the two are not the same
//! claim: an acknowledged write that stored nothing looks identical to a
//! successful one, forever, while every downstream consumer reads a
//! stale value or none at all.
//!
//! The loss compounds at the call sites: `["revenue", "status"]`
//! (`cl-revenue-ops.py:3717`) and `["revenue", "fee-bounds"]` (`:3722`)
//! discard the returned bool entirely, inside a block that ends
//! `except Exception: pass  # Datastore push is best-effort`.
//!
//! This module replaces that with an owner check, a write, a readback,
//! and a typed outcome. The ENVELOPE rules (timestamp injection, the
//! reserved `error` key, the size cap) are the frozen
//! [`revops_analytics::telemetry::datastore_envelope`] kernel and are
//! consumed here, never re-derived.

use revops_analytics::telemetry::{datastore_envelope, PyDict, PyVal};
use revops_fees::pyjson::OValue;

use crate::lifecycle::Owner;

/// py `_DATASTORE_MAX_BYTES = 60000` (`modules/data_service.py:443`), a
/// safety margin under CLN's 65KB datastore limit.
pub const DATASTORE_MAX_BYTES: usize = 60_000;

/// The three datastore producers Task 68 names.
///
/// A fixed roster rather than "whatever key a caller passes": an
/// arbitrary key would be a Rust-only datastore path with no Python
/// counterpart, and a producer that quietly stopped publishing would
/// leave no evidence that it ever should have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Producer {
    /// `["revenue", "status"]` -- `cl-revenue-ops.py:3717`.
    Status,
    /// `["revenue", "fee-bounds"]` -- `cl-revenue-ops.py:3722`.
    FeeBounds,
    /// `["revenue", "segment-observations"]` --
    /// `modules/rebalance_engine_v2.py:3179`.
    Rebalance,
}

impl Producer {
    pub const ALL: [Producer; 3] = [Producer::Status, Producer::FeeBounds, Producer::Rebalance];

    pub fn key(self) -> [&'static str; 2] {
        match self {
            Self::Status => ["revenue", "status"],
            Self::FeeBounds => ["revenue", "fee-bounds"],
            Self::Rebalance => ["revenue", "segment-observations"],
        }
    }

    /// The single owner allowed to write this key.
    ///
    /// Derived from which Python code path pushes it: status and
    /// fee-bounds are pushed from the fee-adjustment path, and
    /// segment-observations from the rebalance engine.
    pub fn owner(self) -> Owner {
        match self {
            Self::Status | Self::FeeBounds => Owner::FeeScheduler,
            Self::Rebalance => Owner::Rebalance,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::FeeBounds => "fee_bounds",
            Self::Rebalance => "segment_observations",
        }
    }
}

// =====================================================================
// R68-7: payload assembly and the publish/withhold decision
// =====================================================================

/// Why a producer published nothing this cycle.
///
/// "Do not publish" is a real outcome, distinct from both a silent skip
/// and a publish full of defaults. Python expresses it by returning
/// early; here it is declared, so a producer that goes quiet leaves
/// evidence of WHY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Withheld {
    /// py `cfg_snap = config.snapshot() if config else None` then
    /// `if cfg_snap and data_service:` (`cl-revenue-ops.py:3720-3721`).
    NoConfigSnapshot,
    /// py `if store is None or not observer_member_id: return False`
    /// (`modules/rebalance_engine_v2.py:3169-3171`).
    NoObserverIdentity,
    /// py `if not snapshot.get("segment_observations"): return False`
    /// (`modules/rebalance_engine_v2.py:3174-3175`).
    NoObservations,
}

impl Withheld {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NoConfigSnapshot => "datastore_withheld_no_config_snapshot",
            Self::NoObserverIdentity => "datastore_withheld_no_observer_identity",
            Self::NoObservations => "datastore_withheld_no_observations",
        }
    }
}

/// Publish this cycle, or decline with a reason.
#[derive(Debug, Clone, PartialEq)]
pub enum PublishDecision {
    Publish(PyDict),
    Withhold(Withheld),
}

/// py `{"min_fee_ppm": ..., "max_fee_ppm": ..., "mid_fee_ppm":
/// (min + max) // 2}` (`cl-revenue-ops.py:3723-3725`).
///
/// `div_euclid` rather than `/`: Python's `//` floors toward negative
/// infinity while Rust's `/` truncates toward zero. The two agree over
/// the declared policy range for these fields
/// (`CONFIG_FIELD_RANGES['min_fee_ppm'] = (5, 100000)`,
/// `modules/config.py:355`), so this is the correct operator rather than
/// a claim that out-of-range bounds are supported -- see the note in
/// `tests/datastore_payloads.rs` and Task 74.
pub fn fee_bounds_payload(min_fee_ppm: i64, max_fee_ppm: i64) -> PyDict {
    let mut payload = PyDict::new();
    payload.push("min_fee_ppm", PyVal::Int(min_fee_ppm));
    payload.push("max_fee_ppm", PyVal::Int(max_fee_ppm));
    payload.push(
        "mid_fee_ppm",
        PyVal::Int(min_fee_ppm.saturating_add(max_fee_ppm).div_euclid(2)),
    );
    payload
}

/// Fee-bounds publishes only with a config snapshot in hand.
///
/// Publishing zeros instead would be indistinguishable, to every
/// external consumer, from an operator who really set the floor to zero.
pub fn fee_bounds_decision(snapshot: Option<(i64, i64)>) -> PublishDecision {
    match snapshot {
        Some((min_fee_ppm, max_fee_ppm)) => {
            PublishDecision::Publish(fee_bounds_payload(min_fee_ppm, max_fee_ppm))
        }
        None => PublishDecision::Withhold(Withheld::NoConfigSnapshot),
    }
}

/// py `{"operator_controls": {"values": ...}, "fee_decision": ...}`
/// (`cl-revenue-ops.py:3706-3714`).
///
/// The opposite absence rule to fee-bounds, and deliberately so: an
/// absent config yields an EMPTY `values` dict rather than withholding,
/// because an operator watching this key needs to see that the plugin is
/// alive and holding no config -- something silence cannot express.
pub fn status_payload(operator_controls: Option<PyDict>, fee_decision: Option<PyDict>) -> PyDict {
    let mut controls_wrapper = PyDict::new();
    controls_wrapper.push("values", PyVal::Dict(operator_controls.unwrap_or_default()));

    let mut payload = PyDict::new();
    payload.push("operator_controls", PyVal::Dict(controls_wrapper));
    payload.push(
        "fee_decision",
        PyVal::Dict(fee_decision.unwrap_or_default()),
    );
    payload
}

/// Both of the rebalance engine's guards, in Python's order.
///
/// The identity is checked first: an unnamed observer keeps withholding
/// no matter how many observations arrive, so it is the one the operator
/// must fix.
pub fn segment_publish_is_allowed(
    observer_member_id: &str,
    observation_count: usize,
) -> Result<(), Withheld> {
    if observer_member_id.is_empty() {
        return Err(Withheld::NoObserverIdentity);
    }
    if observation_count == 0 {
        return Err(Withheld::NoObservations);
    }
    Ok(())
}

/// The CLN datastore, as this module needs it.
///
/// A trait so the producers are fake-proven: the real transport is
/// `rpc.datastore(mode="create-or-replace")` plus `listdatastore`, which
/// the generated inventory still marks `rust_transport: "missing"`.
///
/// `read` distinguishes "the key is absent" (`Ok(None)`) from "the
/// datastore could not be asked" (`Err`). Collapsing those is how a
/// failed verification becomes a successful publish.
pub trait DatastoreTransport {
    fn write(&self, key: &[&str], encoded: &str) -> Result<(), String>;
    fn read(&self, key: &[&str]) -> Result<Option<String>, String>;
}

/// A write that was stored AND verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    pub producer: Producer,
    pub bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishRefusal {
    /// Someone other than the key's authoritative owner tried to write
    /// it. Refused BEFORE the write -- a refusal that still wrote has
    /// already corrupted the key it was meant to protect.
    NotOwner {
        producer: Producer,
        attempted_by: Owner,
    },
    /// The frozen envelope kernel rejected the payload.
    Envelope {
        producer: Producer,
        detail: String,
    },
    WriteFailed {
        producer: Producer,
        detail: String,
    },
    /// The write was acknowledged and the readback could not be
    /// performed. NOT proof of success: the write may well have landed,
    /// but this publish cannot say so, and "we could not check" must
    /// never be reported as "verified" (the rule R68-1 applied to an
    /// unstattable production path).
    ReadbackUnreadable {
        producer: Producer,
        detail: String,
    },
    /// Acknowledged, and the key holds nothing. The failure Python
    /// reports as success.
    ReadbackMissing {
        producer: Producer,
    },
    /// The key exists and holds bytes this publish did not write -- a
    /// half-applied write, or another writer racing the key. Presence is
    /// not correctness.
    ReadbackMismatch {
        producer: Producer,
    },
}

impl PublishRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotOwner { .. } => "datastore_not_owner",
            Self::Envelope { .. } => "datastore_envelope_rejected",
            Self::WriteFailed { .. } => "datastore_write_failed",
            Self::ReadbackUnreadable { .. } => "datastore_readback_unreadable",
            Self::ReadbackMissing { .. } => "datastore_readback_missing",
            Self::ReadbackMismatch { .. } => "datastore_readback_mismatch",
        }
    }

    /// Which producer stopped publishing. A code alone tells an operator
    /// what went wrong but not where to look.
    pub fn producer(&self) -> Producer {
        match self {
            Self::NotOwner { producer, .. }
            | Self::Envelope { producer, .. }
            | Self::WriteFailed { producer, .. }
            | Self::ReadbackUnreadable { producer, .. }
            | Self::ReadbackMissing { producer }
            | Self::ReadbackMismatch { producer } => *producer,
        }
    }
}

/// Publish one producer's payload, and verify it landed.
///
/// The order is the contract: ownership is checked before the envelope
/// is built, the envelope before anything is written, and the readback
/// after the write is acknowledged. Only a write that was stored AND
/// read back byte-for-byte is a [`Published`].
pub fn publish<T: DatastoreTransport + ?Sized>(
    transport: &T,
    producer: Producer,
    writer: Owner,
    payload: PyDict,
    now: i64,
) -> Result<Published, PublishRefusal> {
    if writer != producer.owner() {
        return Err(PublishRefusal::NotOwner {
            producer,
            attempted_by: writer,
        });
    }

    let encoded = datastore_envelope(payload, now, DATASTORE_MAX_BYTES).map_err(|error| {
        PublishRefusal::Envelope {
            producer,
            detail: error.to_string(),
        }
    })?;

    let key = producer.key();
    transport
        .write(&key, &encoded)
        .map_err(|detail| PublishRefusal::WriteFailed { producer, detail })?;

    // The acknowledgement is not the evidence. Read the key BACK -- the
    // same key that was written -- and compare.
    match transport.read(&key) {
        Err(detail) => Err(PublishRefusal::ReadbackUnreadable { producer, detail }),
        Ok(None) => Err(PublishRefusal::ReadbackMissing { producer }),
        Ok(Some(found)) if found != encoded => Err(PublishRefusal::ReadbackMismatch { producer }),
        Ok(Some(_)) => Ok(Published {
            producer,
            bytes: encoded.len(),
        }),
    }
}

// =====================================================================
// R68-8: stamping the segment-observation snapshot
// =====================================================================

/// Python's five-key snapshot literal, in order
/// (`modules/segment_observations.py:154-160`).
///
/// The order is a WIRE contract, not documentation: `json.dumps` walks a
/// dict in insertion order and `rebalance_engine_v2.py:3180-3186` writes
/// the result verbatim, so a stamp appended at the end produces a
/// byte-different blob from the one Python publishes.
pub const SNAPSHOT_KEY_ORDER: [&str; 5] = [
    "generated_at",
    "ttl_seconds",
    "schema_version",
    "observer_member_id",
    "segment_observations",
];

/// The key the frozen kernel deliberately omits and this module supplies.
const OBSERVER_MEMBER_ID: &str = "observer_member_id";
/// The kernel's observation array.
const SEGMENT_OBSERVATIONS: &str = "segment_observations";

/// Every typed way a snapshot is unusable. Task 68's rule -- required
/// reads return typed outcomes -- applies here: an unreadable snapshot is
/// refused, never silently treated as an empty one, because "no
/// observations" and "I could not tell how many observations" lead to the
/// same silent non-publish while meaning opposite things.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotRefusal {
    /// The export was not a JSON object at all.
    NotAnObject,
    /// An `observer_member_id` was already present. The frozen kernel
    /// never emits one, so this is a second stamp -- and stamping twice
    /// yields a dict with two identical keys, which `json.dumps` emits
    /// happily and no consumer can interpret.
    AlreadyStamped,
    /// No `segment_observations` key: nothing to attribute, and no count
    /// for the withhold decision to read.
    MissingObservations,
    /// `segment_observations` was present but not an array.
    ObservationsNotAnArray,
    /// The snapshot carried a JSON null, which the telemetry value model
    /// has no representation for. Refused rather than dropped or coerced:
    /// either would publish a payload that differs from the snapshot the
    /// kernel actually produced.
    UnsupportedNull,
}

impl SnapshotRefusal {
    /// Stable machine-matchable code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotAnObject => "segment_snapshot_not_an_object",
            Self::AlreadyStamped => "segment_snapshot_already_stamped",
            Self::MissingObservations => "segment_snapshot_missing_observations",
            Self::ObservationsNotAnArray => "segment_snapshot_observations_not_an_array",
            Self::UnsupportedNull => "segment_snapshot_unsupported_null",
        }
    }
}

/// Validate that `snapshot` is an object carrying an array of
/// observations, returning both its entries and that array.
///
/// Returns the array rather than re-finding it in each caller: a second
/// lookup would need its own "not an array" arm that the first lookup has
/// already made unreachable, and an arm no input can reach is one no test
/// can falsify.
type SnapshotParts<'a> = (&'a [(String, OValue)], &'a [OValue]);

fn snapshot_parts(snapshot: &OValue) -> Result<SnapshotParts<'_>, SnapshotRefusal> {
    let entries = snapshot.as_obj().ok_or(SnapshotRefusal::NotAnObject)?;
    let observations = entries
        .iter()
        .find(|(key, _)| key == SEGMENT_OBSERVATIONS)
        .map(|(_, value)| value)
        .ok_or(SnapshotRefusal::MissingObservations)?
        .as_arr()
        .ok_or(SnapshotRefusal::ObservationsNotAnArray)?;
    Ok((entries, observations))
}

/// Merge `observer_member_id` into the frozen segstore's export.
///
/// Port of the stamp Python performs inline inside `export_snapshot`
/// (`modules/segment_observations.py:158`,
/// `str(observer_member_id or "").strip()`) but which the frozen
/// [`revops_rebalance::segstore::SegmentObservationStore::export_snapshot`]
/// deliberately omits, leaving it to the engine that knows its own member
/// id (`segstore.rs:11-23`).
///
/// This does exactly one thing. Validation, TTL pruning, sort order and
/// truncation are the kernel's decisions and pass through untouched --
/// consumed, never re-derived.
///
/// The stamp is INSERTED immediately before `segment_observations`, which
/// is Python's fourth position, rather than appended; see
/// [`SNAPSHOT_KEY_ORDER`] for why that is load-bearing. Anchoring on the
/// observations key rather than on the literal index 3 means a kernel that
/// grew another leading field would still stamp in the Python-correct
/// place.
///
/// The value is trimmed. The *guard* on whether to publish at all reads
/// the raw string ([`segment_publish_is_allowed`]), matching Python's
/// untrimmed truthiness test at `rebalance_engine_v2.py:3170` -- so a
/// whitespace-only id passes the guard and stamps as `""`, exactly as
/// Python does.
pub fn stamp_observer_member_id(
    snapshot: OValue,
    observer_member_id: &str,
) -> Result<OValue, SnapshotRefusal> {
    Ok(OValue::Obj(stamped_entries(&snapshot, observer_member_id)?))
}

/// The stamp itself, returning entries rather than a rebuilt [`OValue`] so
/// [`segment_decision`] can convert them directly instead of re-unwrapping
/// an object it just built -- a re-unwrap would need a failure arm that no
/// input could reach.
fn stamped_entries(
    snapshot: &OValue,
    observer_member_id: &str,
) -> Result<Vec<(String, OValue)>, SnapshotRefusal> {
    let (entries, _) = snapshot_parts(snapshot)?;
    if entries.iter().any(|(key, _)| key == OBSERVER_MEMBER_ID) {
        return Err(SnapshotRefusal::AlreadyStamped);
    }

    let mut stamped: Vec<(String, OValue)> = Vec::with_capacity(entries.len() + 1);
    for (key, value) in entries {
        if key == SEGMENT_OBSERVATIONS {
            stamped.push((
                OBSERVER_MEMBER_ID.to_string(),
                OValue::str(observer_member_id.trim()),
            ));
        }
        stamped.push((key.clone(), value.clone()));
    }
    Ok(stamped)
}

/// How many observations the snapshot carries, for R68-7's
/// [`segment_publish_is_allowed`].
///
/// py `if not snapshot.get("segment_observations"): return False`. A
/// snapshot that cannot be counted is REFUSED rather than reported as
/// zero: both outcomes withhold the publish, but only one of them is a
/// bug worth an operator's attention.
pub fn snapshot_observation_count(snapshot: &OValue) -> Result<usize, SnapshotRefusal> {
    let (_, observations) = snapshot_parts(snapshot)?;
    Ok(observations.len())
}

/// Cross the two Python-parity value models: `revops_fees::pyjson::OValue`
/// (what the frozen segstore returns) to
/// `revops_analytics::telemetry::PyVal` (what the frozen envelope and
/// [`publish`] speak).
///
/// Both preserve insertion order and both distinguish int from float, so
/// this is a structural mapping with one gap: `PyVal` has no null, and
/// `OValue::Null` is therefore refused rather than dropped or coerced.
fn ovalue_to_pyval(value: &OValue) -> Result<PyVal, SnapshotRefusal> {
    Ok(match value {
        OValue::Null => return Err(SnapshotRefusal::UnsupportedNull),
        OValue::Bool(b) => PyVal::Bool(*b),
        OValue::Int(n) => PyVal::Int(*n),
        OValue::Float(f) => PyVal::Float(*f),
        OValue::Str(s) => PyVal::Str(s.clone()),
        OValue::Arr(items) => PyVal::List(
            items
                .iter()
                .map(ovalue_to_pyval)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        OValue::Obj(entries) => PyVal::Dict(ovalue_entries_to_pydict(entries)?),
    })
}

fn ovalue_entries_to_pydict(entries: &[(String, OValue)]) -> Result<PyDict, SnapshotRefusal> {
    let mut dict = PyDict::new();
    for (key, value) in entries {
        dict.push(key.clone(), ovalue_to_pyval(value)?);
    }
    Ok(dict)
}

/// The `["revenue", "segment-observations"]` producer's whole decision:
/// take the frozen kernel's export, apply Python's two guards, stamp the
/// observer's identity, and hand back a payload the frozen envelope can
/// encode.
///
/// Port of `_push_segment_observation_snapshot`
/// (`modules/rebalance_engine_v2.py:3167-3186`), with its ordering kept:
/// the identity guard reads the RAW member id (Python truthiness) and runs
/// before the observation-count guard, so an unnamed observer is reported
/// even when it also has nothing to say.
///
/// `Withhold` and `Err` are deliberately different outcomes. Both publish
/// nothing, but a withhold is this producer working as designed while a
/// refusal is a malformed snapshot an operator needs to see -- Python
/// returns `False` for both and loses that distinction.
pub fn segment_decision(
    snapshot: OValue,
    observer_member_id: &str,
) -> Result<PublishDecision, SnapshotRefusal> {
    let (_, observations) = snapshot_parts(&snapshot)?;
    if let Err(withheld) = segment_publish_is_allowed(observer_member_id, observations.len()) {
        return Ok(PublishDecision::Withhold(withheld));
    }

    let stamped = stamped_entries(&snapshot, observer_member_id)?;
    Ok(PublishDecision::Publish(ovalue_entries_to_pydict(
        &stamped,
    )?))
}
