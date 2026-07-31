//! Exact policy-mutator planning and response contracts for canonical RPCs.

use revops_analytics::policy::PeerPolicy;
use revops_db::state_writer::PeerPolicyWrite;
use serde_json::{json, Map, Value};

use crate::state_writer::StateWriteAck;

const BANNED_TAG: &str = "banned";

fn override_truthy(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        None | Some(Value::Null) => false,
        Some(Value::String(value)) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Some(value) => matches!(
            value.to_string().trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
    }
}

/// Python `_policy_write_override`. This is deliberately narrower than
/// generic Python truthiness: the string `"false"` is denied.
pub fn policy_write_override(params: &Map<String, Value>) -> bool {
    override_truthy(params.get("internal")) || override_truthy(params.get("admin"))
}

/// The deprecated ignore aliases retain Python's operator lockdown verbatim.
pub fn deprecated_policy_write_gate(action: &str, params: &Map<String, Value>) -> Option<Value> {
    if policy_write_override(params) {
        return None;
    }
    let method = match action {
        "ignore" => "revenue-ignore",
        "unignore" => "revenue-unignore",
        other => other,
    };
    Some(json!({
        "error": format!(
            "{method} is deprecated for normal operator use. Use revenue-policy list/get/find/changes for diagnostics."
        )
    }))
}

pub fn invalid_peer_id_error() -> Value {
    json!({"error": "Invalid peer_id format: expected 66-character hex pubkey"})
}

fn write_from_policy(
    existing: &PeerPolicy,
    strategy: &str,
    rebalance_mode: &str,
    tags: Vec<String>,
    expires_at: Option<i64>,
) -> PeerPolicyWrite {
    PeerPolicyWrite {
        peer_id: existing.peer_id.clone(),
        strategy: strategy.to_string(),
        rebalance_mode: rebalance_mode.to_string(),
        fee_ppm_target: existing.fee_ppm_target,
        tags: Some(Value::Array(tags.into_iter().map(Value::String).collect()).to_string()),
        fee_multiplier_min: existing.fee_multiplier_min,
        fee_multiplier_max: existing.fee_multiplier_max,
        expires_at,
    }
}

/// Python `set_policy(... passive, disabled, tags=[ignored, reason])`.
pub fn ignore_plan(existing: &PeerPolicy, reason: &str) -> PeerPolicyWrite {
    let tags = if reason == "ignored" {
        vec!["ignored".to_string()]
    } else {
        vec!["ignored".to_string(), reason.to_string()]
    };
    write_from_policy(existing, "passive", "disabled", tags, existing.expires_at)
}

/// Python `ban_peer`: preserve tags, add `banned`, and clear expiry.
pub fn ban_plan(existing: &PeerPolicy) -> PeerPolicyWrite {
    let mut tags = existing.tags.clone();
    if !tags.iter().any(|tag| tag == BANNED_TAG) {
        tags.push(BANNED_TAG.to_string());
    }
    write_from_policy(existing, "passive", "disabled", tags, None)
}

/// Python `unban_peer`: remove `banned` while preserving other fields.
pub fn unban_plan(existing: &PeerPolicy) -> PeerPolicyWrite {
    let tags = existing
        .tags
        .iter()
        .filter(|tag| tag.as_str() != BANNED_TAG)
        .cloned()
        .collect();
    write_from_policy(existing, "dynamic", "enabled", tags, existing.expires_at)
}

pub fn ignore_success(peer_id: &str, reason: &str) -> Value {
    json!({
        "status": "success",
        "action": "ignore",
        "peer_id": peer_id,
        "reason": reason,
        "message": format!("Peer {peer_id} set to passive strategy with rebalancing disabled."),
        "warning": "DEPRECATED: Use 'revenue-policy set' instead.",
    })
}

pub fn unignore_success(peer_id: &str) -> Value {
    json!({
        "status": "success",
        "action": "unignore",
        "peer_id": peer_id,
        "message": format!(
            "Peer {peer_id} reverted to default policy (dynamic strategy, rebalancing enabled)."
        ),
        "warning": "DEPRECATED: Use 'revenue-policy delete' instead.",
    })
}

pub fn ban_success(peer_id: &str, reason: &str, tags: &[String]) -> Value {
    json!({
        "status": "success",
        "action": "ban",
        "peer_id": peer_id,
        "reason": reason,
        "tags": tags,
        "message": "Peer banned: no channel opens, no LN+ swaps, no fee/rebalance management. Existing channels and in-flight swaps are untouched.",
    })
}

pub fn unban_success(peer_id: &str, tags: &[String]) -> Value {
    json!({
        "status": "success",
        "action": "unban",
        "peer_id": peer_id,
        "tags": tags,
    })
}

fn write_error(code: &str, message: &str) -> Value {
    json!({
        "status": "error",
        "error": {"code": code, "message": message},
    })
}

/// Invoke `success` only after a durable completed write reply.
pub fn completed_write_response<T>(
    ack: StateWriteAck<T>,
    success: impl FnOnce(T) -> Value,
) -> Value {
    match ack {
        StateWriteAck::Applied(value) => success(value),
        StateWriteAck::AlreadyTerminal => write_error(
            "already_terminal",
            "state already terminal; no mutation applied",
        ),
        StateWriteAck::Denied(detail) => write_error("denied", &detail),
        StateWriteAck::NotAdmitted(detail) => write_error("not_admitted", &detail),
        StateWriteAck::AdmittedOutcomeUnknown(detail) => {
            write_error("admitted_outcome_unknown", &detail)
        }
        StateWriteAck::StorageFailure(detail) => write_error("storage_failure", &detail),
    }
}
