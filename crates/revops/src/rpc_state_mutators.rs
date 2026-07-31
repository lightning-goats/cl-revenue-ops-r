//! Exact policy-mutator planning and response contracts for canonical RPCs.

use revops_analytics::policy::PeerPolicy;
use revops_db::{
    budget::ReserveRequest,
    state_writer::{PeerPolicyWrite, SpendReleaseBatch},
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

use crate::{
    rpc_params::{is_truthy_py, python_int},
    state_writer::StateWriteAck,
};

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

/// Decoded handler arguments for Python's generic spend reservation RPC.
#[derive(Debug, Clone)]
pub struct SpendReserveParams {
    pub request: ReserveRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendReleaseStaleParams {
    pub max_age_seconds: i64,
    pub category: Option<String>,
    pub limit: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendSettleParams {
    pub reservation_id: String,
    pub actual_spent_sats: Option<i64>,
    pub source: Option<String>,
    pub record_event: bool,
}

fn python_str(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => "None".to_string(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn optional_python_str(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(value) => Some(python_str(value)),
    }
}

fn in_band_error(message: impl Into<String>) -> Value {
    json!({"error": message.into()})
}

/// Parse Python handler-defined values after the generated positional/named
/// binder has applied defaults.
pub fn parse_spend_reserve_params(
    params: &Map<String, Value>,
) -> Result<SpendReserveParams, Value> {
    let amount =
        python_int(params.get("amount_sats").unwrap_or(&Value::Null)).map_err(in_band_error)?;
    if amount <= 0 {
        return Err(in_band_error("amount_sats must be > 0"));
    }

    let metadata = match params.get("metadata_json") {
        None | Some(Value::Null) => None,
        Some(value) if !is_truthy_py(value) => None,
        Some(Value::String(raw)) => {
            Some(serde_json::from_str(raw).unwrap_or_else(|_| json!({"raw": raw})))
        }
        Some(raw) => Some(json!({"raw": raw})),
    };

    Ok(SpendReserveParams {
        request: ReserveRequest {
            reservation_id: python_str(params.get("reservation_id").unwrap_or(&Value::Null)),
            amount_sats: amount,
            category: python_str(params.get("category").unwrap_or(&Value::Null)),
            subcategory: optional_python_str(params.get("subcategory")),
            reference_id: optional_python_str(params.get("reference_id")),
            channel_id: optional_python_str(params.get("channel_id")),
            metadata,
            ..ReserveRequest::default()
        },
    })
}

pub fn parse_spend_release_params(params: &Map<String, Value>) -> String {
    python_str(params.get("reservation_id").unwrap_or(&Value::Null))
}

pub fn parse_spend_release_stale_params(
    params: &Map<String, Value>,
) -> Result<SpendReleaseStaleParams, Value> {
    let max_age_seconds = python_int(
        params
            .get("max_age_seconds")
            .unwrap_or(&Value::Number(3600.into())),
    )
    .map_err(in_band_error)?
    .max(1);
    let limit = python_int(params.get("limit").unwrap_or(&Value::Number(100.into())))
        .map_err(in_band_error)?
        .max(1);
    let category = params
        .get("category")
        .filter(|value| is_truthy_py(value))
        .map(python_str)
        .map(|value| value.trim().to_ascii_lowercase());

    Ok(SpendReleaseStaleParams {
        max_age_seconds,
        category,
        limit,
    })
}

pub fn parse_spend_settle_params(params: &Map<String, Value>) -> Result<SpendSettleParams, Value> {
    let actual_spent_sats = match params.get("actual_spent_sats") {
        None | Some(Value::Null) => None,
        Some(value) => Some(python_int(value).map_err(in_band_error)?),
    };
    let source = params
        .get("source")
        .filter(|value| is_truthy_py(value))
        .map(python_str);
    let record_event = params
        .get("record_event")
        .map(is_truthy_py)
        .unwrap_or(false);

    Ok(SpendSettleParams {
        reservation_id: python_str(params.get("reservation_id").unwrap_or(&Value::Null)),
        actual_spent_sats,
        source,
        record_event,
    })
}

pub fn spend_reserve_rejection(requested_sats: i64, remaining_sats: i64, budget: &Value) -> Value {
    json!({
        "status": "rejected",
        "reason": "insufficient_unified_budget",
        "requested_sats": requested_sats,
        "remaining_sats": remaining_sats,
        "budget": budget,
    })
}

pub fn spend_reserve_response(
    granted: bool,
    params: &SpendReserveParams,
    budget_before: &Value,
    budget_after: &Value,
) -> Value {
    if !granted {
        return json!({"status": "error", "error": "Failed to reserve spend"});
    }
    json!({
        "status": "success",
        "reservation_id": params.request.reservation_id,
        "category": params.request.category,
        "amount_sats": params.request.amount_sats,
        "budget_before": budget_before,
        "budget_after_estimate": budget_after,
    })
}

pub fn spend_release_response(reservation_id: &str, released: bool) -> Value {
    json!({
        "status": if released { "success" } else { "not_found" },
        "reservation_id": reservation_id,
    })
}

pub fn spend_release_stale_response(released: &SpendReleaseBatch, budget_after: &Value) -> Value {
    json!({
        "status": "success",
        "released_count": released.released_count,
        "released_sats": released.released_sats,
        "reservation_ids": released.reservation_ids,
        "budget_after": budget_after,
    })
}

pub fn spend_settle_response(reservation_id: &str, settled: bool) -> Value {
    spend_release_response(reservation_id, settled)
}

/// Generic spend RPCs use Python's in-band exception shape. Only an applied,
/// completed state-writer result is allowed to invoke the success builder.
pub fn completed_spend_response<T>(
    ack: StateWriteAck<T>,
    success: impl FnOnce(T) -> Value,
) -> Value {
    match ack {
        StateWriteAck::Applied(value) => success(value),
        StateWriteAck::AlreadyTerminal => {
            in_band_error("state already terminal; no mutation applied")
        }
        StateWriteAck::Denied(detail)
        | StateWriteAck::NotAdmitted(detail)
        | StateWriteAck::AdmittedOutcomeUnknown(detail)
        | StateWriteAck::StorageFailure(detail) => in_band_error(detail),
    }
}

fn profile_bundle(entries: &[(&str, Value)]) -> Map<String, Value> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

/// Python `PROFILE_BUNDLES`; authority controls and safety invariants never belong here.
pub fn profile_bundles() -> BTreeMap<String, Map<String, Value>> {
    BTreeMap::from([
        ("custom".into(), Map::new()),
        (
            "preserve".into(),
            profile_bundle(&[
                ("daily_budget_sats", json!(2000)),
                ("weekly_budget_sats", json!(14000)),
                ("rebalance_hold_margin", json!(5.0)),
                ("growth_budget_enabled", json!(false)),
                ("growth_budget_earned_fraction", json!(0.1)),
                ("growth_budget_experiment_fraction", json!(0.05)),
                ("growth_budget_max_extra_sats", json!(1000)),
                ("planner_min_annual_roi_pct", json!(5.0)),
                ("planner_max_opens_per_cycle", json!(0)),
                ("planner_max_closes_per_cycle", json!(0)),
                ("lnplus_swap_preference_margin", json!(0.5)),
            ]),
        ),
        (
            "conservative".into(),
            profile_bundle(&[
                ("daily_budget_sats", json!(5000)),
                ("weekly_budget_sats", json!(35000)),
                ("rebalance_hold_margin", json!(0.0)),
                ("growth_budget_enabled", json!(false)),
                ("growth_budget_earned_fraction", json!(0.25)),
                ("growth_budget_experiment_fraction", json!(0.1)),
                ("growth_budget_max_extra_sats", json!(2000)),
                ("planner_min_annual_roi_pct", json!(1.0)),
                ("planner_max_opens_per_cycle", json!(1)),
                ("planner_max_closes_per_cycle", json!(0)),
                ("lnplus_swap_preference_margin", json!(0.2)),
            ]),
        ),
        (
            "balanced".into(),
            profile_bundle(&[
                ("daily_budget_sats", json!(8000)),
                ("weekly_budget_sats", json!(56000)),
                ("rebalance_hold_margin", json!(0.0)),
                ("growth_budget_enabled", json!(true)),
                ("growth_budget_earned_fraction", json!(0.25)),
                ("growth_budget_experiment_fraction", json!(0.1)),
                ("growth_budget_max_extra_sats", json!(2000)),
                ("planner_min_annual_roi_pct", json!(1.0)),
                ("planner_max_opens_per_cycle", json!(1)),
                ("planner_max_closes_per_cycle", json!(1)),
                ("lnplus_swap_preference_margin", json!(0.2)),
            ]),
        ),
        (
            "growth".into(),
            profile_bundle(&[
                ("daily_budget_sats", json!(12000)),
                ("weekly_budget_sats", json!(84000)),
                ("rebalance_hold_margin", json!(0.0)),
                ("growth_budget_enabled", json!(true)),
                ("growth_budget_earned_fraction", json!(0.4)),
                ("growth_budget_experiment_fraction", json!(0.2)),
                ("growth_budget_max_extra_sats", json!(5000)),
                ("planner_min_annual_roi_pct", json!(0.5)),
                ("planner_max_opens_per_cycle", json!(2)),
                ("planner_max_closes_per_cycle", json!(1)),
                ("lnplus_swap_preference_margin", json!(0.1)),
            ]),
        ),
    ])
}

fn python_profile_name(profile: &Value) -> String {
    if is_truthy_py(profile) {
        python_str(profile).trim().to_ascii_lowercase()
    } else {
        String::new()
    }
}

fn python_repr(value: &Value) -> String {
    match value {
        Value::String(value) => {
            let slash = char::from_u32(92).expect("valid slash");
            let quote = char::from_u32(39).expect("valid quote");
            let doubled_slash = format!("{slash}{slash}");
            let escaped_quote = format!("{slash}{quote}");
            let escaped = value
                .replace(slash, &doubled_slash)
                .replace(quote, &escaped_quote);
            format!("{quote}{escaped}{quote}")
        }
        other => python_str(other),
    }
}

fn python_numeric(value: &Value) -> Option<f64> {
    match value {
        Value::Bool(true) => Some(1.0),
        Value::Bool(false) => Some(0.0),
        Value::Number(value) => value.as_f64(),
        _ => None,
    }
}

fn python_equal(left: &Value, right: &Value) -> bool {
    match (python_numeric(left), python_numeric(right)) {
        (Some(left), Some(right)) => left == right,
        _ => left == right,
    }
}

/// Read-only Python `preview_profile`; all per-key arrays are ordered by key.
pub fn preview_profile(
    current_values: &Map<String, Value>,
    profile: &Value,
    explicit_keys: &BTreeSet<String>,
) -> Value {
    let name = python_profile_name(profile);
    let bundles = profile_bundles();
    let Some(bundle) = bundles.get(&name) else {
        return json!({
            "error": format!("unknown profile: {}", python_repr(profile)),
            "valid_profiles": ["preserve", "conservative", "balanced", "growth", "custom"],
        });
    };

    let mut changes = Vec::new();
    let mut blocked = Vec::new();
    let mut unchanged = Vec::new();
    let mut merged = current_values.clone();
    let mut keys = bundle.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        let profile_value = &bundle[key];
        let current = current_values.get(key).cloned().unwrap_or(Value::Null);
        let mut entry = json!({
            "key": key,
            "current": current,
            "profile_value": profile_value,
        });
        if explicit_keys.contains(key) {
            entry["blocked_by"] = json!("explicit_override");
            blocked.push(entry);
        } else if python_equal(&current, profile_value) {
            unchanged.push(entry);
        } else {
            merged.insert(key.clone(), profile_value.clone());
            changes.push(entry);
        }
    }

    let mut contradictions = Vec::new();
    if let (Some(daily), Some(weekly)) = (
        merged.get("daily_budget_sats"),
        merged.get("weekly_budget_sats"),
    ) {
        if matches!(
            (python_numeric(daily), python_numeric(weekly)),
            (Some(d), Some(w)) if d > w
        ) {
            contradictions.push(format!(
                "daily_budget_sats ({}) > weekly_budget_sats ({}) in the merged result; the weekly cap binds first",
                python_str(daily),
                python_str(weekly)
            ));
        }
    }

    json!({
        "profile": name,
        "would_change": changes,
        "blocked_by_explicit_override": blocked,
        "already_equal": unchanged,
        "contradiction_precheck": contradictions,
        "activation": format!(
            "takes effect at plugin restart after `revenue-config set risk_profile {name}`"
        ),
    })
}

pub fn preview_all(
    current_values: &Map<String, Value>,
    explicit_keys: &BTreeSet<String>,
) -> Map<String, Value> {
    ["preserve", "conservative", "balanced", "growth"]
        .into_iter()
        .map(|name| {
            (
                name.to_string(),
                preview_profile(current_values, &json!(name), explicit_keys),
            )
        })
        .collect()
}

/// Assemble Python `revenue_profile_preview` after config and overrides are read.
pub fn build_profile_preview(
    current_values: &Map<String, Value>,
    active_profile: &str,
    overrides: &Map<String, Value>,
    requested_profile: Option<&Value>,
) -> Value {
    let bundle_keys = profile_bundles()
        .values()
        .flat_map(|bundle| bundle.keys().cloned())
        .collect::<BTreeSet<_>>();
    let explicit = overrides
        .keys()
        .filter(|key| key.as_str() != "risk_profile")
        .cloned()
        .collect::<BTreeSet<_>>();
    let explicit_bundle_keys = explicit
        .intersection(&bundle_keys)
        .cloned()
        .collect::<Vec<_>>();
    let persisted_value = overrides
        .get("risk_profile")
        .filter(|value| is_truthy_py(value))
        .cloned()
        .unwrap_or_else(|| json!(active_profile));
    let persisted = python_str(&persisted_value).trim().to_ascii_lowercase();

    let mut response = Map::from_iter([
        ("active_profile".into(), json!(active_profile)),
        ("persisted_profile".into(), json!(persisted)),
        ("pending_restart".into(), json!(persisted != active_profile)),
        ("explicit_override_keys".into(), json!(explicit_bundle_keys)),
    ]);
    if let Some(profile) = requested_profile.filter(|value| !value.is_null()) {
        response.insert(
            "preview".into(),
            preview_profile(current_values, profile, &explicit),
        );
    } else {
        response.insert(
            "comparison".into(),
            Value::Object(preview_all(current_values, &explicit)),
        );
    }
    Value::Object(response)
}
