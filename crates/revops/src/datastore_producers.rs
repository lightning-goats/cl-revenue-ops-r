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
