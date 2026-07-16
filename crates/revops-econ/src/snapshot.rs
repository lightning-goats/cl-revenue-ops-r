//! Canonical economic snapshot (port of `modules/econ_snapshot.py`).
//!
//! The schema (`schemas/economic_snapshot.v0.schema.json` in the Python
//! port) is the normative contract; these types are one implementation of
//! it (J1 rule). Immutable after construction; channels are sorted by
//! `channel_id` (J3 stable ordering). Missing profitability evidence
//! yields ZERO-valued economics with ZERO confidence — never invented
//! cost, never silent authorization (invariant 7); callers decide how low
//! confidence gates their decisions.

use crate::types::{ChannelId, EconError, EconResult, Micro, Msat, PeerId, SignedMsat, UnixTime};

/// The ONE canonical serializer: field order in `to_wire` is irrelevant
/// because this sorts keys and rejects float-typed numbers fail-closed.
pub use revops_core::canonical::{canonical_json, CanonicalError};

pub const SCHEMA_NAME: &str = "economic_snapshot";
pub const SCHEMA_VERSION: i64 = 0;

/// Union of today's two classification vocabularies (profitability
/// `ChannelRole` + flow `ChannelState`); wire-frozen, keep byte-identical
/// to the JSON schema `enum`.
pub const ROLES: [&str; 8] = [
    "SOURCE",
    "SINK",
    "ROUTER",
    "BALANCED",
    "INBOUND_GATEWAY",
    "OUTBOUND_GATEWAY",
    "DORMANT",
    "UNKNOWN",
];

/// Wire-frozen lifecycle vocabulary, keep byte-identical to the JSON
/// schema `enum`.
pub const LIFECYCLES: [&str; 8] = [
    "CANDIDATE",
    "OPENING",
    "EVALUATING",
    "PRODUCTIVE",
    "PROTECTED",
    "UNDERPERFORMING",
    "RECYCLING",
    "CLOSING",
];

/// A reason a channel's economics are shielded from otherwise-applicable
/// decisions (e.g. an LN+ contract). `reason`/`owner` must be non-empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protection {
    pub reason: String,
    pub owner: String,
    pub expires_at: Option<UnixTime>,
}

impl Protection {
    pub fn new(
        reason: impl Into<String>,
        owner: impl Into<String>,
        expires_at: Option<UnixTime>,
    ) -> EconResult<Self> {
        let reason = reason.into();
        let owner = owner.into();
        if reason.is_empty() || owner.is_empty() {
            return Err(EconError {
                msg: format!(
                    "Protection requires reason and owner: reason={reason:?} owner={owner:?}"
                ),
            });
        }
        Ok(Self {
            reason,
            owner,
            expires_at,
        })
    }
}

fn protection_wire(p: &Protection) -> serde_json::Value {
    serde_json::json!({
        "reason": p.reason,
        "owner": p.owner,
        "expires_at": p.expires_at.map(UnixTime::value),
    })
}

/// Per-channel canonical economics. All 20 fields, exact names from
/// `econ_snapshot.py` lines 66-86. Invariants (role/lifecycle in the wire
/// vocabularies, non-negative forward counts) are enforced by the
/// constructors (`build_channel_snapshot`), not by the struct itself —
/// fields are public so callers/tests can inspect them directly, mirroring
/// the Python dataclass's public attributes.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelSnapshot {
    pub channel_id: ChannelId,
    pub peer_id: PeerId,
    pub capacity_msat: Msat,
    pub local_msat: Msat,
    pub remote_msat: Msat,
    pub spendable_msat: Msat,
    pub receivable_msat: Msat,
    pub exit_revenue_msat: Msat,
    pub sourced_value_msat: Msat,
    pub rebalance_cost_msat: Msat,
    pub capital_cost_msat: Msat,
    pub net_value_msat: SignedMsat,
    pub exit_volume_msat: Msat,
    pub sourced_volume_msat: Msat,
    pub forward_count: i64,
    pub sourced_forward_count: i64,
    pub role: String,
    pub lifecycle: String,
    pub protections: Vec<Protection>,
    pub confidence_micro: Micro,
}

fn channel_wire(c: &ChannelSnapshot) -> serde_json::Value {
    serde_json::json!({
        "channel_id": c.channel_id.as_str(),
        "peer_id": c.peer_id.as_str(),
        "capacity_msat": c.capacity_msat.value(),
        "local_msat": c.local_msat.value(),
        "remote_msat": c.remote_msat.value(),
        "spendable_msat": c.spendable_msat.value(),
        "receivable_msat": c.receivable_msat.value(),
        "exit_revenue_msat": c.exit_revenue_msat.value(),
        "sourced_value_msat": c.sourced_value_msat.value(),
        "rebalance_cost_msat": c.rebalance_cost_msat.value(),
        "capital_cost_msat": c.capital_cost_msat.value(),
        "net_value_msat": c.net_value_msat.0,
        "exit_volume_msat": c.exit_volume_msat.value(),
        "sourced_volume_msat": c.sourced_volume_msat.value(),
        "forward_count": c.forward_count,
        "sourced_forward_count": c.sourced_forward_count,
        "role": c.role,
        "lifecycle": c.lifecycle,
        "protections": c.protections.iter().map(protection_wire).collect::<Vec<_>>(),
        "confidence_micro": c.confidence_micro.value(),
    })
}

/// Daily rebalance/open-cost budget accounting for the node as a whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetState {
    pub cap_msat: Msat,
    pub reserved_msat: Msat,
    pub spent_msat: Msat,
}

/// Node-level economic state. 6 fields (5 `Msat` + `daily_budget`) plus
/// two opaque JSON arrays passed through verbatim (their shapes are not
/// yet part of the governed contract).
#[derive(Debug, Clone, PartialEq)]
pub struct NodeState {
    pub total_local_msat: Msat,
    pub total_remote_msat: Msat,
    pub receivable_objective_msat: Msat,
    pub onchain_confirmed_msat: Msat,
    pub reserved_msat: Msat,
    pub daily_budget: BudgetState,
    pub pending_operations: Vec<serde_json::Value>,
    pub external_obligations: Vec<serde_json::Value>,
}

fn node_wire(n: &NodeState) -> serde_json::Value {
    serde_json::json!({
        "total_local_msat": n.total_local_msat.value(),
        "total_remote_msat": n.total_remote_msat.value(),
        "receivable_objective_msat": n.receivable_objective_msat.value(),
        "onchain_confirmed_msat": n.onchain_confirmed_msat.value(),
        "reserved_msat": n.reserved_msat.value(),
        "daily_budget": {
            "cap_msat": n.daily_budget.cap_msat.value(),
            "reserved_msat": n.daily_budget.reserved_msat.value(),
            "spent_msat": n.daily_budget.spent_msat.value(),
        },
        "pending_operations": n.pending_operations,
        "external_obligations": n.external_obligations,
    })
}

/// Canonical immutable per-cycle economic snapshot. Construction validates
/// `snapshot_id`/`evidence_window_seconds` and sorts `channels` by
/// `channel_id` (J3 stable ordering) so wire output is deterministic
/// regardless of caller-supplied order.
#[derive(Debug, Clone, PartialEq)]
pub struct EconomicSnapshot {
    pub snapshot_id: String,
    pub observed_at: UnixTime,
    pub evidence_window_seconds: i64,
    pub node: NodeState,
    pub channels: Vec<ChannelSnapshot>,
}

impl EconomicSnapshot {
    /// Sorts channels by channel_id at construction (J3) and validates.
    pub fn new(
        snapshot_id: impl Into<String>,
        observed_at: UnixTime,
        evidence_window_seconds: i64,
        node: NodeState,
        channels: Vec<ChannelSnapshot>,
    ) -> EconResult<Self> {
        let snapshot_id = snapshot_id.into();
        if snapshot_id.is_empty() {
            return Err(EconError {
                msg: "snapshot_id required".to_string(),
            });
        }
        if evidence_window_seconds < 0 {
            return Err(EconError {
                msg: format!("evidence_window_seconds: {evidence_window_seconds}"),
            });
        }
        let mut channels = channels;
        channels.sort_by(|a, b| a.channel_id.cmp(&b.channel_id));
        Ok(Self {
            snapshot_id,
            observed_at,
            evidence_window_seconds,
            node,
            channels,
        })
    }

    /// Plain JSON-safe value matching `economic_snapshot.v0.schema.json`.
    /// Field order is irrelevant — pass through `canonical_json` for a
    /// deterministic, sorted-key byte string.
    pub fn to_wire(&self) -> serde_json::Value {
        serde_json::json!({
            "schema_name": SCHEMA_NAME,
            "schema_version": SCHEMA_VERSION,
            "snapshot_id": self.snapshot_id,
            "observed_at": self.observed_at.value(),
            "evidence_window_seconds": self.evidence_window_seconds,
            "node": node_wire(&self.node),
            "channels": self.channels.iter().map(channel_wire).collect::<Vec<_>>(),
        })
    }
}

/// Duck-typed `prof` (Python's `ChannelProfitability`) made explicit: the
/// flattened union of `prof.revenue.*`, `prof.costs.*`, and top-level
/// `prof.*` fields that `build_channel_snapshot` actually reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfEvidence {
    pub fees_earned_msat: i64,
    pub sourced_fee_contribution_msat: i64,
    pub rebalance_cost_sats: i64,
    pub open_cost_sats: i64,
    pub net_profit_sats: i64,
    pub volume_routed_msat: i64,
    pub forward_count: i64,
    pub sourced_forward_count_30d: i64,
}

fn json_i64(v: &serde_json::Value, key: &str) -> EconResult<i64> {
    v.get(key)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| EconError {
            msg: format!("channel.{key} missing or not an integer"),
        })
}

fn json_i64_or_zero(v: &serde_json::Value, key: &str) -> i64 {
    v.get(key).and_then(serde_json::Value::as_i64).unwrap_or(0)
}

fn json_str(v: &serde_json::Value, key: &str) -> EconResult<String> {
    v.get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| EconError {
            msg: format!("channel.{key} missing or not a string"),
        })
}

/// `sats * 1000` as a `SignedMsat`, checked: `net_profit_sats` is a signed
/// P&L figure that can be large in either direction, so the `*1000` scale
/// to msat must not silently wrap on overflow.
fn signed_msat_from_sats(sats: i64) -> EconResult<SignedMsat> {
    let scaled = sats as i128 * 1000;
    if scaled < i64::MIN as i128 || scaled > i64::MAX as i128 {
        return Err(EconError {
            msg: format!("SignedMsat overflow: {sats} sats * 1000"),
        });
    }
    Ok(SignedMsat(scaled as i64))
}

/// Pure mapper from today's data sources to the canonical model.
///
/// `channel`: a listpeerchannels-shaped normalized JSON object with
/// integer msat fields (`short_channel_id`, `peer_id`, `total_msat`,
/// `to_us_msat`, `spendable_msat`, `receivable_msat`).
///
/// `prof`: `None` means zero economics, zero confidence — missing
/// evidence is never free or safe (invariant 7).
///
/// Note: `sourced_volume_msat` is always `Msat(0)` regardless of `prof`,
/// mirroring a Python quirk (`econ_snapshot.py` hardcodes it) — this is
/// intentional byte-parity, not a bug to "fix" in the port.
#[allow(clippy::too_many_arguments)]
pub fn build_channel_snapshot(
    channel: &serde_json::Value,
    prof: Option<&ProfEvidence>,
    flow_confidence: Option<f64>,
    role: &str,
    lifecycle: &str,
    protections: Vec<Protection>,
) -> EconResult<ChannelSnapshot> {
    if !ROLES.contains(&role) {
        return Err(EconError {
            msg: format!("unknown role: {role:?}"),
        });
    }
    if !LIFECYCLES.contains(&lifecycle) {
        return Err(EconError {
            msg: format!("unknown lifecycle: {lifecycle:?}"),
        });
    }

    let capacity = Msat::new(json_i64(channel, "total_msat")?)?;
    let local = Msat::new(json_i64(channel, "to_us_msat")?)?;
    let remote = capacity.sub(local)?;

    let (
        exit_revenue,
        sourced_value,
        rebalance_cost,
        capital_cost,
        net_value,
        exit_volume,
        forward_count,
        sourced_forward_count,
    ) = match prof {
        Some(p) => (
            Msat::new(p.fees_earned_msat)?,
            Msat::new(p.sourced_fee_contribution_msat)?,
            Msat::from_sats(p.rebalance_cost_sats)?,
            Msat::from_sats(p.open_cost_sats)?,
            signed_msat_from_sats(p.net_profit_sats)?,
            Msat::new(p.volume_routed_msat)?,
            p.forward_count,
            p.sourced_forward_count_30d,
        ),
        None => (
            Msat::new(0)?,
            Msat::new(0)?,
            Msat::new(0)?,
            Msat::new(0)?,
            SignedMsat(0),
            Msat::new(0)?,
            0,
            0,
        ),
    };

    if forward_count < 0 || sourced_forward_count < 0 {
        return Err(EconError {
            msg: format!(
                "forward counts must be non-negative ints: forward_count={forward_count} sourced_forward_count={sourced_forward_count}"
            ),
        });
    }

    let confidence = match flow_confidence {
        None => Micro::new(0)?,
        Some(f) => Micro::from_float_clamped(f)?,
    };

    Ok(ChannelSnapshot {
        channel_id: ChannelId::new(json_str(channel, "short_channel_id")?)?,
        peer_id: PeerId::new(json_str(channel, "peer_id")?)?,
        capacity_msat: capacity,
        local_msat: local,
        remote_msat: remote,
        spendable_msat: Msat::new(json_i64_or_zero(channel, "spendable_msat"))?,
        receivable_msat: Msat::new(json_i64_or_zero(channel, "receivable_msat"))?,
        exit_revenue_msat: exit_revenue,
        sourced_value_msat: sourced_value,
        rebalance_cost_msat: rebalance_cost,
        capital_cost_msat: capital_cost,
        net_value_msat: net_value,
        exit_volume_msat: exit_volume,
        sourced_volume_msat: Msat::new(0)?,
        forward_count,
        sourced_forward_count,
        role: role.to_string(),
        lifecycle: lifecycle.to_string(),
        protections,
        confidence_micro: confidence,
    })
}
