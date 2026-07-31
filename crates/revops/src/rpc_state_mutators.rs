//! Exact policy-mutator planning and response contracts for canonical RPCs.

use revops_analytics::policy::{is_valid_peer_id, PeerPolicy};
use revops_db::{
    actor::DbHandle,
    budget::{ClearStats, ReserveRequest},
    queries::ChannelStateIdentity,
    state_writer::{PeerPolicyWrite, PolicyDelete, SpendReleaseBatch},
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::{
    fee_scheduler::{CycleMsg, SchedulerIngress},
    rpc_params::{is_truthy_py, python_int},
    state_writer::{CoreMutators, StateWriteAck},
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

pub fn ignore_success(peer_id: &str, reason: impl Into<Value>) -> Value {
    let reason = reason.into();
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

pub fn ban_success(peer_id: &str, reason: impl Into<Value>, tags: &[String]) -> Value {
    let reason = reason.into();
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

pub fn clear_reservations_response(cleared: &ClearStats, budget_available: i64) -> Value {
    json!({
        "status": "success",
        "cleared_count": cleared.cleared_count,
        "released_sats": cleared.released_sats,
        "budget_available": budget_available.max(0),
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

const MAX_POLICY_CHANGES_PER_MINUTE: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreStateMutationAction {
    Ignore,
    Unignore,
    Ban,
    Unban,
    ClearReservations,
    SpendRelease,
    SpendReleaseStale,
    SpendReserve,
    SpendSettle,
}

/// Async provider of the unified total-cost budget snapshot — the same
/// shape `rpc_total_cost_budget::total_cost_budget_response` answers
/// (Python `_total_cost_budget_status()`). The reserve gate and the
/// budget-embedding responses consume it; Task 69's authority assembly
/// supplies the live implementation over the real config/boltz/DB stack.
pub type BudgetStatusProvider =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Value> + Send>> + Send + Sync>;

pub struct CoreStateMutationOwner {
    core: CoreMutators,
    reader: DbHandle,
    fee_ingress: SchedulerIngress,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    daily_budget_sats: Arc<dyn Fn() -> i64 + Send + Sync>,
    budget_status: BudgetStatusProvider,
    rate_limit: Mutex<HashMap<String, Vec<i64>>>,
}

impl CoreStateMutationOwner {
    pub fn assemble(
        core: CoreMutators,
        reader: DbHandle,
        fee_ingress: SchedulerIngress,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        daily_budget_sats: Arc<dyn Fn() -> i64 + Send + Sync>,
        budget_status: BudgetStatusProvider,
    ) -> Self {
        Self {
            core,
            reader,
            fee_ingress,
            clock,
            daily_budget_sats,
            budget_status,
            rate_limit: Mutex::new(HashMap::new()),
        }
    }

    pub async fn handle(
        &self,
        action: CoreStateMutationAction,
        params: &Map<String, Value>,
    ) -> Value {
        match action {
            CoreStateMutationAction::Ignore => self.ignore(params).await,
            CoreStateMutationAction::Unignore => self.unignore(params).await,
            CoreStateMutationAction::Ban => self.ban(params).await,
            CoreStateMutationAction::Unban => self.unban(params).await,
            CoreStateMutationAction::ClearReservations => self.clear_reservations(params).await,
            CoreStateMutationAction::SpendRelease => self.spend_release(params).await,
            CoreStateMutationAction::SpendReleaseStale => self.spend_release_stale(params).await,
            CoreStateMutationAction::SpendReserve => self.spend_reserve(params).await,
            CoreStateMutationAction::SpendSettle => self.spend_settle(params).await,
        }
    }

    fn peer_id(params: &Map<String, Value>) -> Result<String, Value> {
        let peer_id = params
            .get("peer_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !is_valid_peer_id(peer_id) {
            return Err(invalid_peer_id_error());
        }
        Ok(peer_id.to_string())
    }

    async fn existing_policy(&self, peer_id: &str, now: i64) -> Result<PeerPolicy, Value> {
        revops_db::queries::policy_for_peer(&self.reader, peer_id, now)
            .await
            .map_err(|error| write_error("read_failed", &format!("{error:#}")))
    }

    fn rate_limit_allows(&self, peer_id: &str, now: i64) -> bool {
        let window_start = now - 60;
        let mut all = self.rate_limit.lock().expect("policy rate limit poisoned");
        let timestamps = all.entry(peer_id.to_string()).or_default();
        timestamps.retain(|timestamp| *timestamp > window_start);
        timestamps.len() < MAX_POLICY_CHANGES_PER_MINUTE
    }

    fn record_completed_change(&self, peer_id: &str, now: i64) {
        let window_start = now - 60;
        let mut all = self.rate_limit.lock().expect("policy rate limit poisoned");
        let timestamps = all.entry(peer_id.to_string()).or_default();
        timestamps.retain(|timestamp| *timestamp > window_start);
        timestamps.push(now);
    }

    fn rate_limit_error(peer_id: &str) -> Value {
        json!({
            "status": "error",
            "error": format!(
                "Rate limited: max {} changes/minute for {}...",
                MAX_POLICY_CHANGES_PER_MINUTE,
                &peer_id[..12]
            ),
        })
    }

    async fn wake_after_commit(&self, peer_id: &str) {
        if self
            .fee_ingress
            .send(CycleMsg::PolicyChanged {
                peer_id: peer_id.to_string(),
            })
            .await
            .is_err()
        {
            eprintln!(
                "revops: committed peer policy for {peer_id}, but fee owner is unavailable; the next cycle still rehydrates policy state"
            );
        }
    }

    async fn complete_upsert(
        &self,
        peer_id: &str,
        now: i64,
        write: PeerPolicyWrite,
        success: Value,
    ) -> Value {
        let ack = self.core.upsert_peer_policy(write, now).await;
        let applied = matches!(&ack, StateWriteAck::Applied(()));
        let response = completed_write_response(ack, |_| success);
        if applied {
            self.record_completed_change(peer_id, now);
            self.wake_after_commit(peer_id).await;
        }
        response
    }

    pub async fn ignore(&self, params: &Map<String, Value>) -> Value {
        if let Some(denial) = deprecated_policy_write_gate("ignore", params) {
            return denial;
        }
        let peer_id = match Self::peer_id(params) {
            Ok(peer_id) => peer_id,
            Err(error) => return error,
        };
        let now = (self.clock)();
        let existing = match self.existing_policy(&peer_id, now).await {
            Ok(existing) => existing,
            Err(error) => return error,
        };
        if !self.rate_limit_allows(&peer_id, now) {
            return Self::rate_limit_error(&peer_id);
        }
        let reason = params
            .get("reason")
            .cloned()
            .unwrap_or_else(|| json!("manual"));
        let write = ignore_plan(&existing, &python_str(&reason));
        let success = ignore_success(&peer_id, reason);
        self.complete_upsert(&peer_id, now, write, success).await
    }

    pub async fn unignore(&self, params: &Map<String, Value>) -> Value {
        if let Some(denial) = deprecated_policy_write_gate("unignore", params) {
            return denial;
        }
        let peer_id = match Self::peer_id(params) {
            Ok(peer_id) => peer_id,
            Err(error) => return error,
        };
        let ack = self.core.delete_peer_policy(peer_id.clone()).await;
        let changed = matches!(&ack, StateWriteAck::Applied(PolicyDelete::Deleted));
        let response = completed_write_response(ack, |_| unignore_success(&peer_id));
        if changed {
            self.wake_after_commit(&peer_id).await;
        }
        response
    }

    pub async fn ban(&self, params: &Map<String, Value>) -> Value {
        let peer_id = match Self::peer_id(params) {
            Ok(peer_id) => peer_id,
            Err(error) => return error,
        };
        let now = (self.clock)();
        let existing = match self.existing_policy(&peer_id, now).await {
            Ok(existing) => existing,
            Err(error) => return error,
        };
        if !self.rate_limit_allows(&peer_id, now) {
            return Self::rate_limit_error(&peer_id);
        }
        let reason = params
            .get("reason")
            .cloned()
            .unwrap_or_else(|| json!("operator"));
        let mut tags = existing.tags.clone();
        if !tags.iter().any(|tag| tag == BANNED_TAG) {
            tags.push(BANNED_TAG.to_string());
        }
        let write = ban_plan(&existing);
        let success = ban_success(&peer_id, reason, &tags);
        self.complete_upsert(&peer_id, now, write, success).await
    }

    pub async fn unban(&self, params: &Map<String, Value>) -> Value {
        let peer_id = match Self::peer_id(params) {
            Ok(peer_id) => peer_id,
            Err(error) => return error,
        };
        let now = (self.clock)();
        let existing = match self.existing_policy(&peer_id, now).await {
            Ok(existing) => existing,
            Err(error) => return error,
        };
        if !self.rate_limit_allows(&peer_id, now) {
            return Self::rate_limit_error(&peer_id);
        }
        let tags = existing
            .tags
            .iter()
            .filter(|tag| tag.as_str() != BANNED_TAG)
            .cloned()
            .collect::<Vec<_>>();
        let write = unban_plan(&existing);
        let success = unban_success(&peer_id, &tags);
        self.complete_upsert(&peer_id, now, write, success).await
    }

    pub async fn clear_reservations(&self, _params: &Map<String, Value>) -> Value {
        let cleared = match self.core.clear_all_budget_reservations().await {
            StateWriteAck::Applied(cleared) => cleared,
            other => return completed_spend_response(other, |_| unreachable!("applied handled")),
        };
        let now = (self.clock)();
        let spent = match self
            .reader
            .budget_status(now.saturating_sub(24 * 3600))
            .await
        {
            Ok(status) => status.spent_sats,
            Err(error) => return in_band_error(format!("{error:#}")),
        };
        let daily_budget = (self.daily_budget_sats)();
        let available = match daily_budget.checked_sub(spent) {
            Some(available) => available,
            None => {
                return in_band_error(format!(
                    "budget_available overflows i64: daily budget {daily_budget} - spent {spent}"
                ))
            }
        };
        clear_reservations_response(&cleared, available)
    }

    pub async fn spend_release(&self, params: &Map<String, Value>) -> Value {
        let reservation_id = parse_spend_release_params(params);
        let ack = self
            .core
            .release_spend_reservation(reservation_id.clone())
            .await;
        completed_spend_response(ack, |released| {
            spend_release_response(&reservation_id, released)
        })
    }

    /// Port of `revenue_spend_reserve` (cl-revenue-ops.py:7802-7870).
    /// Order is Python's: amount sanity, then the FRIENDLY unified-budget
    /// gate (a failed budget read returns verbatim; over-remaining answers
    /// the rejection dict), then the write. The AUTHORITATIVE cross-
    /// category rail is the `effective_budget_sats`/`since_timestamp` pair
    /// carried into `reserve_spend`'s BEGIN IMMEDIATE (P2-011) — Python's
    /// `_spend_reserve_lock` only serializes the friendly gate, so two
    /// racing pre-checks here (no lock) are caught by the same in-tx rail
    /// Python relies on. `budget_after_estimate` is fetched only on a
    /// granted write, like Python's lazy success-dict construction.
    pub async fn spend_reserve(&self, params: &Map<String, Value>) -> Value {
        let parsed = match parse_spend_reserve_params(params) {
            Ok(parsed) => parsed,
            Err(error) => return error,
        };

        let budget = (self.budget_status)().await;
        if budget.get("error").is_some() {
            return budget;
        }
        let budget_i64 = |key: &str, default: i64| -> i64 {
            budget.get(key).and_then(Value::as_i64).unwrap_or(default)
        };
        let remaining = budget_i64("remaining_sats", 0);
        if parsed.request.amount_sats > remaining {
            return spend_reserve_rejection(parsed.request.amount_sats, remaining, &budget);
        }
        let now = (self.clock)();
        let mut request = parsed.request.clone();
        request.effective_budget_sats = Some(budget_i64("effective_budget_sats", 0));
        request.since_timestamp = Some(now - budget_i64("window_hours", 24).max(1) * 3600);

        match self.core.reserve_spend(request, now).await {
            StateWriteAck::Applied((true, _remaining_after)) => {
                let budget_after = (self.budget_status)().await;
                spend_reserve_response(true, &parsed, &budget, &budget_after)
            }
            StateWriteAck::Applied((false, _)) => {
                spend_reserve_response(false, &parsed, &budget, &Value::Null)
            }
            other => completed_spend_response(other, |_: (bool, i64)| {
                unreachable!("applied arms handled above")
            }),
        }
    }

    /// Port of `revenue_cleanup_closed`'s success path
    /// (cl-revenue-ops.py:6394-6490) over already-fetched CLN blobs. Reads
    /// come off `self.reader`; each channel's archive+purge is ONE sealed
    /// write. Per-channel failures collect into `errors` and the sweep
    /// continues, exactly like Python's per-channel try/except.
    pub async fn cleanup_closed(
        &self,
        evidence: crate::rpc_cleanup_closed::CleanupClosedEvidence,
    ) -> Value {
        use crate::rpc_cleanup_closed as pure;
        let tracked = match revops_db::queries::all_channel_states(&self.reader).await {
            Ok(tracked) => tracked,
            // py outer except: the base result with the error recorded.
            Err(error) => return pure::cleanup_result(0, 0, &[], &[error.to_string()]),
        };
        if tracked.is_empty() {
            return pure::no_tracked_channels();
        }
        let open = match &evidence.peer_channels {
            Ok(blob) => pure::open_scids(blob),
            Err(error) => {
                return pure::cleanup_result(
                    0,
                    0,
                    &[],
                    &[format!("Failed to get open channels: {error}")],
                )
            }
        };
        let closed: Vec<&ChannelStateIdentity> = tracked
            .iter()
            .filter(|state| !open.contains(&state.channel_id))
            .collect();
        if closed.is_empty() {
            return pure::no_closed_channels();
        }
        let closed_info = evidence
            .closed_list
            .as_ref()
            .map(pure::closed_info_by_scid)
            .unwrap_or_default();

        let mut archived = 0i64;
        let mut channels = Vec::new();
        let mut errors = Vec::new();
        for state in closed {
            match self
                .archive_one_closed(state, &closed_info, evidence.block_height, evidence.now)
                .await
            {
                Ok(()) => {
                    archived += 1;
                    channels.push(state.channel_id.clone());
                }
                Err(error) => {
                    errors.push(format!("Error processing {}: {error}", state.channel_id))
                }
            }
        }
        // py increments archived and cleaned together per success.
        pure::cleanup_result(archived, archived, &channels, &errors)
    }

    /// One channel's evidence gathering + archive write, port of
    /// `_archive_closed_channel` (cl-revenue-ops.py:7561-7679).
    async fn archive_one_closed(
        &self,
        state: &ChannelStateIdentity,
        closed_info: &BTreeMap<String, Map<String, Value>>,
        block_height: i64,
        now: i64,
    ) -> Result<(), String> {
        use crate::rpc_cleanup_closed as pure;
        let channel_id = state.channel_id.as_str();
        let empty = Map::new();
        let ch_info = closed_info.get(channel_id).unwrap_or(&empty);
        let close_type = pure::close_type_from_info(ch_info);

        let cost = revops_db::queries::channel_cost_row(&self.reader, channel_id)
            .await
            .map_err(|error| error.to_string())?;
        let open_cost_sats = cost.as_ref().map(|row| row.open_cost_sats).unwrap_or(0);
        let opened_at = pure::repair_opened_at(
            channel_id,
            cost.as_ref().map(|row| row.opened_at),
            block_height,
            now,
        );
        let closure_cost_sats =
            revops_db::queries::channel_closure_cost_total(&self.reader, channel_id)
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or(0);
        // py: window_days=3650 ("10 years = all time").
        let pnl = revops_db::queries::channel_pnl(&self.reader, channel_id, 3650, now)
            .await
            .map_err(|error| error.to_string())?;

        let mut closer = pure::determine_closer(&close_type).to_string();
        let capacity_sats =
            revops_core::msat::parse_msat(ch_info.get("total_msat").unwrap_or(&Value::Null)) / 1000;
        // py peer precedence: tracked state, then listclosedchannels, then
        // the writer's 'unknown' fallback.
        let peer_id = if !state.peer_id.is_empty() {
            state.peer_id.clone()
        } else {
            ch_info
                .get("peer_id")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string()
        };
        if closer == "unknown" {
            if let Some(info_closer) = ch_info.get("closer").and_then(Value::as_str) {
                if !info_closer.is_empty() {
                    closer = info_closer.to_string();
                }
            }
        }

        let archive = revops_db::state_writer::ClosedChannelArchive {
            channel_id: channel_id.to_string(),
            peer_id,
            capacity_sats,
            opened_at,
            closed_at: now,
            close_type,
            open_cost_sats,
            closure_cost_sats,
            // py: `pnl.get('revenue_msat', 0) // 1000`.
            total_revenue_sats: pnl.revenue_msat.div_euclid(1000),
            total_rebalance_cost_sats: pnl.rebalance_cost_sats,
            forward_count: pnl.forward_count,
            funding_txid: None,
            closing_txid: ch_info
                .get("closing_txid")
                .and_then(Value::as_str)
                .map(str::to_string),
            closer,
        };
        match self.core.archive_closed_channel(archive).await {
            StateWriteAck::Applied(()) => Ok(()),
            StateWriteAck::AlreadyTerminal => Err("state already terminal".to_string()),
            StateWriteAck::Denied(detail)
            | StateWriteAck::NotAdmitted(detail)
            | StateWriteAck::AdmittedOutcomeUnknown(detail)
            | StateWriteAck::StorageFailure(detail) => Err(detail),
        }
    }

    /// Port of `revenue_spend_release_stale` (cl-revenue-ops.py:7884-7908):
    /// the safe recovery sweep for orphaned reservations — the same
    /// operation `_compute_total_cost_budget_status` runs best-effort at
    /// the top of every budget read in Python, which slice 1's read path
    /// deliberately deferred to THIS mutator.
    pub async fn spend_release_stale(&self, params: &Map<String, Value>) -> Value {
        let parsed = match parse_spend_release_stale_params(params) {
            Ok(parsed) => parsed,
            Err(error) => return error,
        };
        let ack = self
            .core
            .release_stale_spend_reservations(
                parsed.category,
                parsed.max_age_seconds,
                parsed.limit,
                (self.clock)(),
            )
            .await;
        match ack {
            StateWriteAck::Applied(released) => {
                let budget_after = (self.budget_status)().await;
                spend_release_stale_response(&released, &budget_after)
            }
            other => completed_spend_response(other, |_: SpendReleaseBatch| {
                unreachable!("applied arm handled above")
            }),
        }
    }

    pub async fn spend_settle(&self, params: &Map<String, Value>) -> Value {
        let parsed = match parse_spend_settle_params(params) {
            Ok(parsed) => parsed,
            Err(error) => return error,
        };
        let reservation_id = parsed.reservation_id.clone();
        let ack = self
            .core
            .settle_spend_reservation(
                parsed.reservation_id,
                parsed.actual_spent_sats,
                parsed.source,
                parsed.record_event,
                (self.clock)(),
            )
            .await;
        completed_spend_response(ack, |settled| {
            spend_settle_response(&reservation_id, settled)
        })
    }
}

#[cfg(test)]
mod core_mutation_owner_tests {
    use super::CoreStateMutationOwner;
    use crate::fee_scheduler::{CycleMsg, SchedulerIngress};
    use crate::state_writer::{CoreMutators, CoreStateLiveCapability, ProductionStateWriter};
    use revops_db::state_writer::spawn_state_writer;
    use rusqlite::Connection;
    use serde_json::{json, Map, Value};
    use std::path::PathBuf;
    use std::sync::Arc;

    const NOW: i64 = 1_800_000_000;
    const PEER: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// A healthy unified-budget snapshot, the shape
    /// `total_cost_budget_response` answers with. The provider seam lets
    /// each test choose the budget the gate sees without a live boltz/
    /// config stack.
    fn budget_value(remaining: i64, effective: i64) -> Value {
        json!({
            "source": "total_cost_budget",
            "window_hours": 24,
            "remaining_sats": remaining,
            "effective_budget_sats": effective,
        })
    }

    fn budget_provider(value: Value) -> super::BudgetStatusProvider {
        Arc::new(move || {
            let value = value.clone();
            Box::pin(async move { value })
        })
    }

    async fn fixture() -> (
        tempfile::TempDir,
        PathBuf,
        CoreStateMutationOwner,
        tokio::sync::mpsc::Receiver<CycleMsg>,
    ) {
        fixture_with_budget(budget_provider(budget_value(5_000, 5_000))).await
    }

    async fn fixture_with_budget(
        budget: super::BudgetStatusProvider,
    ) -> (
        tempfile::TempDir,
        PathBuf,
        CoreStateMutationOwner,
        tokio::sync::mpsc::Receiver<CycleMsg>,
    ) {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("production.db");
        std::fs::copy(source, &path).expect("copy fixture");
        let conn = Connection::open(&path).expect("open fixture");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("enable WAL");
        drop(conn);

        let reader = revops_db::actor::spawn_read_only(&path)
            .await
            .expect("spawn read actor");
        let writer = ProductionStateWriter::assemble(
            spawn_state_writer(&path).await.expect("spawn state writer"),
        );
        let live = CoreStateLiveCapability::for_tests();
        let core = CoreMutators::assemble(writer, live);
        let (ingress, receiver) = SchedulerIngress::bounded_channel(16);
        let owner = CoreStateMutationOwner::assemble(
            core,
            reader,
            ingress,
            Arc::new(|| NOW),
            Arc::new(|| 100),
            budget,
        );
        (dir, path, owner, receiver)
    }

    fn params(entries: &[(&str, Value)]) -> Map<String, Value> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    #[tokio::test]
    async fn ignore_returns_only_after_commit_and_then_wakes_the_fee_owner() {
        let (_dir, path, owner, mut receiver) = fixture().await;
        let response = owner
            .ignore(&params(&[
                ("peer_id", json!(PEER)),
                ("reason", json!(7)),
                ("internal", json!(true)),
            ]))
            .await;
        assert_eq!(response["status"], "success");
        assert_eq!(response["reason"], json!(7));

        let conn = Connection::open(path).expect("read committed policy");
        let row: (String, String, String) = conn
            .query_row(
                "SELECT strategy, rebalance_mode, tags FROM peer_policies WHERE peer_id = ?1",
                [PEER],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("policy row committed before response");
        assert_eq!(
            row,
            (
                "passive".into(),
                "disabled".into(),
                "[\"ignored\",\"7\"]".into()
            )
        );
        assert!(matches!(
            receiver.try_recv(),
            Ok(CycleMsg::PolicyChanged { peer_id }) if peer_id == PEER
        ));
    }

    #[tokio::test]
    async fn unignore_of_an_absent_policy_succeeds_without_a_false_change_wake() {
        let (_dir, _path, owner, mut receiver) = fixture().await;
        let response = owner
            .unignore(&params(&[
                ("peer_id", json!(PEER)),
                ("internal", json!("yes")),
            ]))
            .await;
        assert_eq!(response["status"], "success");
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn ban_and_unban_preserve_tags_and_wake_only_after_each_commit() {
        let (_dir, path, owner, mut receiver) = fixture().await;
        let conn = Connection::open(&path).expect("seed policy");
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                PEER,
                "static",
                "source_only",
                r#"["no_close","whale"]"#,
                NOW - 1,
                NOW + 1000,
            ],
        )
        .expect("seed policy");
        drop(conn);

        let banned = owner
            .ban(&params(&[
                ("peer_id", json!(PEER)),
                ("reason", json!("operator")),
            ]))
            .await;
        assert_eq!(banned["tags"], json!(["no_close", "whale", "banned"]));
        assert!(matches!(
            receiver.try_recv(),
            Ok(CycleMsg::PolicyChanged { .. })
        ));

        let unbanned = owner.unban(&params(&[("peer_id", json!(PEER))])).await;
        assert_eq!(unbanned["tags"], json!(["no_close", "whale"]));
        assert!(matches!(
            receiver.try_recv(),
            Ok(CycleMsg::PolicyChanged { .. })
        ));

        let conn = Connection::open(path).expect("read final policy");
        let row: (String, String, String, Option<i64>) = conn
            .query_row(
                "SELECT strategy, rebalance_mode, tags, expires_at FROM peer_policies WHERE peer_id = ?1",
                [PEER],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("read unbanned row");
        assert_eq!(
            row,
            (
                "dynamic".into(),
                "enabled".into(),
                "[\"no_close\",\"whale\"]".into(),
                None
            )
        );
    }

    #[tokio::test]
    async fn policy_rate_limit_counts_only_ten_completed_writes_per_peer() {
        let (_dir, _path, owner, _receiver) = fixture().await;
        for index in 0..10 {
            let response = owner
                .ban(&params(&[
                    ("peer_id", json!(PEER)),
                    ("reason", json!(format!("attempt-{index}"))),
                ]))
                .await;
            assert_eq!(
                response["status"], "success",
                "attempt {index}: {response:?}"
            );
        }
        assert_eq!(
            owner
                .ban(&params(&[
                    ("peer_id", json!(PEER)),
                    ("reason", json!("eleventh")),
                ]))
                .await,
            json!({
                "status": "error",
                "error": format!("Rate limited: max 10 changes/minute for {}...", &PEER[..12]),
            })
        );
    }

    #[tokio::test]
    async fn failed_policy_writes_consume_neither_rate_budget_nor_a_wake() {
        let (_dir, path, owner, mut receiver) = fixture().await;
        let conn = Connection::open(&path).expect("open fixture for failure injection");
        conn.execute_batch(
            "CREATE TRIGGER reject_peer_policy_insert
             BEFORE INSERT ON peer_policies
             BEGIN
                 SELECT RAISE(ABORT, 'injected peer policy failure');
             END;",
        )
        .expect("install failure trigger");

        for index in 0..10 {
            let response = owner
                .ban(&params(&[
                    ("peer_id", json!(PEER)),
                    ("reason", json!(format!("failed-{index}"))),
                ]))
                .await;
            assert_eq!(
                response["error"]["code"], "storage_failure",
                "attempt {index}: {response:?}"
            );
        }
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        conn.execute_batch("DROP TRIGGER reject_peer_policy_insert;")
            .expect("remove failure trigger");
        let recovered = owner
            .ban(&params(&[
                ("peer_id", json!(PEER)),
                ("reason", json!("recovered")),
            ]))
            .await;
        assert_eq!(recovered["status"], "success", "{recovered:?}");
        assert!(matches!(
            receiver.try_recv(),
            Ok(CycleMsg::PolicyChanged { peer_id }) if peer_id == PEER
        ));
    }

    fn seed_spend_reservation(path: &std::path::Path, reservation_id: &str, amount_sats: i64) {
        let conn = Connection::open(path).expect("open fixture to seed reservation");
        conn.execute(
            "INSERT INTO spend_reservations
             (reservation_id, category, reserved_sats, reserved_at, status)
             VALUES (?1, 'misc', ?2, ?3, 'active')",
            rusqlite::params![reservation_id, amount_sats, NOW - 1],
        )
        .expect("seed spend reservation");
    }

    #[tokio::test]
    async fn clear_reservations_commits_legacy_rows_then_reports_spent_only_budget() {
        let (_dir, path, owner, _receiver) = fixture().await;
        let conn = Connection::open(&path).expect("seed clear-reservations fixture");
        conn.execute(
            "INSERT INTO budget_reservations
             (reservation_id, reserved_sats, reserved_at, job_channel_id, status)
             VALUES ('legacy-a', 25, ?1, '1x1x0', 'active'),
                    ('legacy-b', 30, ?1, '2x2x0', 'active'),
                    ('legacy-done', 99, ?1, '3x3x0', 'released')",
            [NOW - 1],
        )
        .expect("seed legacy reservations");
        conn.execute(
            "INSERT INTO spend_reservations
             (reservation_id, category, reserved_sats, reserved_at, status)
             VALUES ('generic-active', 'rebalance', 77, ?1, 'active')",
            [NOW - 1],
        )
        .expect("seed generic reservation that Python does not clear");
        conn.execute(
            "INSERT INTO rebalance_costs
             (timestamp, channel_id, peer_id, cost_sats, amount_sats)
             VALUES (?1, '4x4x0', '02aa', 40, 1000)",
            [NOW - 1],
        )
        .expect("seed daily spend");
        drop(conn);

        let first = owner.clear_reservations(&Map::new()).await;
        assert_eq!(
            first,
            json!({
                "status": "success",
                "cleared_count": 2,
                "released_sats": 55,
                "budget_available": 60,
            })
        );

        let conn = Connection::open(&path).expect("read committed clear");
        let active_legacy: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM budget_reservations WHERE status = 'active'",
                [],
                |row| row.get(0),
            )
            .expect("count active legacy rows");
        let generic_status: String = conn
            .query_row(
                "SELECT status FROM spend_reservations WHERE reservation_id = 'generic-active'",
                [],
                |row| row.get(0),
            )
            .expect("read generic row");
        assert_eq!(active_legacy, 0);
        assert_eq!(
            generic_status, "active",
            "Python clears only the legacy table"
        );
        drop(conn);

        assert_eq!(
            owner.clear_reservations(&Map::new()).await,
            json!({
                "status": "success",
                "cleared_count": 0,
                "released_sats": 0,
                "budget_available": 60,
            })
        );
    }

    #[tokio::test]
    async fn spend_release_returns_only_after_commit_and_repeated_release_is_not_found() {
        let (_dir, path, owner, _receiver) = fixture().await;
        seed_spend_reservation(&path, "release-me", 25);

        let first = owner
            .spend_release(&params(&[("reservation_id", json!("release-me"))]))
            .await;
        assert_eq!(
            first,
            json!({"status": "success", "reservation_id": "release-me"})
        );
        let status: String = Connection::open(&path)
            .expect("open committed reservation")
            .query_row(
                "SELECT status FROM spend_reservations WHERE reservation_id = ?1",
                ["release-me"],
                |row| row.get(0),
            )
            .expect("read committed release");
        assert_eq!(status, "released");

        let second = owner
            .spend_release(&params(&[("reservation_id", json!("release-me"))]))
            .await;
        assert_eq!(
            second,
            json!({"status": "not_found", "reservation_id": "release-me"})
        );
    }

    #[tokio::test]
    async fn spend_settle_commits_status_and_event_atomically_then_is_terminal() {
        let (_dir, path, owner, _receiver) = fixture().await;
        seed_spend_reservation(&path, "settle-me", 40);

        let first = owner
            .spend_settle(&params(&[
                ("reservation_id", json!("settle-me")),
                ("actual_spent_sats", json!(33)),
                ("source", json!("rpc-test")),
                ("record_event", json!(true)),
            ]))
            .await;
        assert_eq!(
            first,
            json!({"status": "success", "reservation_id": "settle-me"})
        );

        let conn = Connection::open(&path).expect("read committed settlement");
        let status: String = conn
            .query_row(
                "SELECT status FROM spend_reservations WHERE reservation_id = ?1",
                ["settle-me"],
                |row| row.get(0),
            )
            .expect("read settled reservation");
        assert_eq!(status, "spent");
        let event: (i64, String) = conn
            .query_row(
                "SELECT amount_sats, source FROM spend_events WHERE event_id = ?1",
                ["resv:settle-me"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read atomic settlement event");
        assert_eq!(event, (33, "rpc-test".to_string()));
        drop(conn);

        let second = owner
            .spend_settle(&params(&[
                ("reservation_id", json!("settle-me")),
                ("actual_spent_sats", json!(33)),
                ("source", json!("rpc-test")),
                ("record_event", json!(true)),
            ]))
            .await;
        assert_eq!(
            second,
            json!({"status": "not_found", "reservation_id": "settle-me"})
        );
        let event_count: i64 = Connection::open(path)
            .expect("open event count")
            .query_row(
                "SELECT COUNT(*) FROM spend_events WHERE event_id = ?1",
                ["resv:settle-me"],
                |row| row.get(0),
            )
            .expect("count settlement events");
        assert_eq!(event_count, 1);
    }

    /// Task 66 slice 2, `revenue-spend-reserve` (py cl-revenue-ops.py:
    /// 7802-7870): grant path commits the row, and the response embeds the
    /// gate's budget as `budget_before` plus a post-write
    /// `budget_after_estimate`.
    #[tokio::test]
    async fn spend_reserve_commits_row_and_embeds_both_budgets() {
        let (_dir, path, owner, _receiver) = fixture().await;
        let response = owner
            .spend_reserve(&params(&[
                ("reservation_id", json!("resv-1")),
                ("category", json!("channel_open")),
                ("amount_sats", json!(500)),
                ("subcategory", json!("expand")),
            ]))
            .await;
        assert_eq!(
            response,
            json!({
                "status": "success",
                "reservation_id": "resv-1",
                "category": "channel_open",
                "amount_sats": 500,
                "budget_before": budget_value(5_000, 5_000),
                "budget_after_estimate": budget_value(5_000, 5_000),
            })
        );

        let conn = Connection::open(&path).expect("read committed reservation");
        let row: (String, i64, String) = conn
            .query_row(
                "SELECT category, reserved_sats, status FROM spend_reservations \
                 WHERE reservation_id = 'resv-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("reservation committed before response");
        assert_eq!(row, ("channel_open".to_string(), 500, "active".to_string()));
    }

    /// The friendly pre-gate: amount above the unified remaining answers
    /// Python's exact rejection dict (py 7833-7840) and writes NOTHING.
    #[tokio::test]
    async fn spend_reserve_rejects_over_remaining_without_writing() {
        let (_dir, path, owner, _receiver) =
            fixture_with_budget(budget_provider(budget_value(100, 5_000))).await;
        let response = owner
            .spend_reserve(&params(&[
                ("reservation_id", json!("resv-over")),
                ("category", json!("misc")),
                ("amount_sats", json!(500)),
            ]))
            .await;
        assert_eq!(
            response,
            json!({
                "status": "rejected",
                "reason": "insufficient_unified_budget",
                "requested_sats": 500,
                "remaining_sats": 100,
                "budget": budget_value(100, 5_000),
            })
        );
        let count: i64 = Connection::open(&path)
            .expect("open")
            .query_row("SELECT COUNT(*) FROM spend_reservations", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 0, "a rejected reserve must not write");
    }

    /// The AUTHORITATIVE in-transaction rail: even when the friendly gate's
    /// remaining is generous (stale/lying), the effective budget carried
    /// into `reserve_spend`'s BEGIN IMMEDIATE refuses the grant (py's
    /// P2-011 design: the pre-check is only a friendly early rejection).
    #[tokio::test]
    async fn spend_reserve_authoritative_rail_refuses_despite_generous_gate() {
        let (_dir, path, owner, _receiver) =
            fixture_with_budget(budget_provider(budget_value(1_000_000, 100))).await;
        let response = owner
            .spend_reserve(&params(&[
                ("reservation_id", json!("resv-rail")),
                ("category", json!("misc")),
                ("amount_sats", json!(500)),
            ]))
            .await;
        assert_eq!(
            response,
            json!({"status": "error", "error": "Failed to reserve spend"})
        );
        let count: i64 = Connection::open(&path)
            .expect("open")
            .query_row(
                "SELECT COUNT(*) FROM spend_reservations WHERE status = 'active'",
                [],
                |row| row.get(0),
            )
            .expect("count");
        assert_eq!(count, 0, "the in-tx rail must refuse the insert");
    }

    /// Python: `if "error" in budget: return budget` (py 7828-7830) — a
    /// failed budget read is returned verbatim, and nothing is written.
    #[tokio::test]
    async fn spend_reserve_propagates_a_budget_error_verbatim() {
        let (_dir, _path, owner, _receiver) =
            fixture_with_budget(budget_provider(json!({"error": "Plugin not initialized"}))).await;
        let response = owner
            .spend_reserve(&params(&[
                ("reservation_id", json!("resv-err")),
                ("category", json!("misc")),
                ("amount_sats", json!(500)),
            ]))
            .await;
        assert_eq!(response, json!({"error": "Plugin not initialized"}));
    }

    /// py 7818-7819: non-positive amounts are refused before any budget
    /// read or write.
    #[tokio::test]
    async fn spend_reserve_refuses_non_positive_amount() {
        let (_dir, _path, owner, _receiver) = fixture().await;
        let response = owner
            .spend_reserve(&params(&[
                ("reservation_id", json!("resv-zero")),
                ("category", json!("misc")),
                ("amount_sats", json!(0)),
            ]))
            .await;
        assert_eq!(response, json!({"error": "amount_sats must be > 0"}));
    }

    /// Task 66 slice 2, `revenue-spend-release-stale` (py 7884-7908): only
    /// active rows older than max_age_seconds release, the category filter
    /// applies lowercased, and the response embeds a post-write
    /// `budget_after`. This is the same operation slice 1 deferred out of
    /// the total-cost-budget read path.
    #[tokio::test]
    async fn spend_release_stale_releases_only_old_matching_rows() {
        let (_dir, path, owner, _receiver) = fixture().await;
        let conn = Connection::open(&path).expect("seed stale rows");
        conn.execute(
            "INSERT INTO spend_reservations \
             (reservation_id, category, reserved_sats, reserved_at, status) VALUES \
             ('old-rebalance', 'rebalance', 40, ?1, 'active'), \
             ('old-open', 'channel_open', 70, ?1, 'active'), \
             ('fresh-rebalance', 'rebalance', 25, ?2, 'active'), \
             ('old-released', 'rebalance', 99, ?1, 'released')",
            rusqlite::params![NOW - 7200, NOW - 100],
        )
        .expect("seed reservations");
        drop(conn);

        let response = owner
            .spend_release_stale(&params(&[
                ("max_age_seconds", json!(3600)),
                ("category", json!("Rebalance")),
            ]))
            .await;
        assert_eq!(
            response,
            json!({
                "status": "success",
                "released_count": 1,
                "released_sats": 40,
                "reservation_ids": ["old-rebalance"],
                "budget_after": budget_value(5_000, 5_000),
            })
        );

        let conn = Connection::open(&path).expect("verify statuses");
        let status = |id: &str| -> String {
            conn.query_row(
                "SELECT status FROM spend_reservations WHERE reservation_id = ?1",
                [id],
                |row| row.get(0),
            )
            .expect("row status")
        };
        assert_eq!(status("old-rebalance"), "released");
        assert_eq!(status("old-open"), "active", "category filter must hold");
        assert_eq!(status("fresh-rebalance"), "active", "age filter must hold");
    }

    /// Without a category the sweep crosses categories, still respecting
    /// the age filter; defaults are max_age_seconds=3600, limit=100.
    #[tokio::test]
    async fn spend_release_stale_without_category_sweeps_all_old_actives() {
        let (_dir, path, owner, _receiver) = fixture().await;
        let conn = Connection::open(&path).expect("seed stale rows");
        conn.execute(
            "INSERT INTO spend_reservations \
             (reservation_id, category, reserved_sats, reserved_at, status) VALUES \
             ('old-a', 'rebalance', 40, ?1, 'active'), \
             ('old-b', 'channel_open', 70, ?1, 'active'), \
             ('fresh', 'misc', 25, ?2, 'active')",
            rusqlite::params![NOW - 7200, NOW - 100],
        )
        .expect("seed reservations");
        drop(conn);

        let response = owner.spend_release_stale(&Map::new()).await;
        assert_eq!(response["status"], "success");
        assert_eq!(response["released_count"], 2);
        assert_eq!(response["released_sats"], 110);
    }

    fn cleanup_evidence(
        peer_channels: Result<Value, String>,
        closed_list: Option<Value>,
    ) -> crate::rpc_cleanup_closed::CleanupClosedEvidence {
        crate::rpc_cleanup_closed::CleanupClosedEvidence {
            peer_channels,
            closed_list,
            block_height: 0,
            now: NOW,
        }
    }

    /// Task 66 slice 3, `revenue-cleanup-closed` (py cl-revenue-ops.py:
    /// 6359-6490 + `_archive_closed_channel` :7561-7679): the tracked-but-
    /// no-longer-open channel is archived with its full hand-derived P&L
    /// and purged from tracking; the still-open channel is untouched.
    #[tokio::test]
    async fn cleanup_closed_archives_hand_derived_pnl_and_purges_tracking() {
        let (_dir, path, owner, _receiver) = fixture().await;
        let conn = Connection::open(&path).expect("seed cleanup fixture");
        conn.execute_batch(&format!(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, sats_out, capacity, updated_at) VALUES
               ('111x1x0', '{peer_a}', 'source', 0.2, 10, 90, 1000000, {now}),
               ('222x2x0', '{peer_b}', 'sink', 0.9, 90, 10, 1000000, {now});
             INSERT INTO channel_costs (channel_id, peer_id, open_cost_sats, capacity_sats, opened_at) VALUES
               ('111x1x0', '{peer_a}', 500, 1000000, {opened});
             INSERT INTO channel_closure_costs
               (channel_id, peer_id, close_type, closure_fee_sats, htlc_sweep_fee_sats, penalty_fee_sats, total_closure_cost_sats, closed_at, resolution_complete)
               VALUES ('111x1x0', '{peer_a}', 'mutual', 300, 0, 0, 300, {now}, 1);
             INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time) VALUES
               ('9x9x0', '111x1x0', 1000000, 997500, 2500, {recent}, {recent}),
               ('9x9x0', '111x1x0', 1000000, 999000, 1000, {old_forward}, {old_forward});
             INSERT INTO rebalance_costs (channel_id, peer_id, cost_sats, cost_msat, amount_sats, timestamp) VALUES
               ('111x1x0', '{peer_a}', 200, 200000, 50000, {recent});",
            peer_a = "a".repeat(66),
            peer_b = "b".repeat(66),
            now = NOW,
            opened = NOW - 10 * 86400,
            recent = NOW - 3600,
            old_forward = NOW - 100 * 86400,
        ))
        .expect("seed rows");
        drop(conn);

        let response = owner
            .cleanup_closed(cleanup_evidence(
                Ok(json!({"channels": [{"short_channel_id": "222x2x0"}]})),
                Some(json!({"closedchannels": [{
                    "short_channel_id": "111x1x0",
                    "close_cause": "user initiated MUTUAL close",
                    "closer": "local",
                    "total_msat": 5_000_000_000i64,
                    "closing_txid": "ctxid-1",
                }]})),
            ))
            .await;
        assert_eq!(
            response,
            json!({
                "archived": 1,
                "cleaned": 1,
                "channels": ["111x1x0"],
                "errors": [],
            })
        );

        let conn = Connection::open(&path).expect("verify archive");
        let row: (String, i64, i64, i64, String, String, Option<String>) = conn
            .query_row(
                "SELECT peer_id, capacity_sats, net_pnl_sats, total_revenue_sats, close_type, closer, closing_txid \
                 FROM closed_channels WHERE channel_id = '111x1x0'",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                },
            )
            .expect("archived row");
        // revenue = 2500 (recent) + 1000 (100 days old -- INSIDE the
        // all-time 3650d window, outside any 30d default) = 3500 msat
        // // 1000 = 3; net = 3 - (500 + 300 + 200) = -997; capacity from
        // listclosedchannels total_msat / 1000; the tracked peer wins over
        // the blob; cause "MUTUAL" beats closer "local".
        assert_eq!(
            row,
            (
                "a".repeat(66),
                5_000_000,
                -997,
                3,
                "mutual".to_string(),
                "mutual".to_string(),
                Some("ctxid-1".to_string())
            )
        );
        let remaining: Vec<String> = conn
            .prepare("SELECT channel_id FROM channel_states")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            remaining,
            vec!["222x2x0".to_string()],
            "only the closed channel purges"
        );
    }

    /// The three guard arms: nothing tracked, a failed listpeerchannels
    /// read (short-circuits — a partial open-set would misclassify every
    /// open channel as closed), and everything still open.
    #[tokio::test]
    async fn cleanup_closed_guard_arms_match_python() {
        let (_dir, path, owner, _receiver) = fixture().await;
        assert_eq!(
            owner
                .cleanup_closed(cleanup_evidence(Ok(json!({"channels": []})), None))
                .await,
            json!({
                "message": "No tracked channels found",
                "archived": 0, "cleaned": 0, "channels": [], "errors": [],
            })
        );

        let conn = Connection::open(&path).expect("seed one tracked");
        conn.execute(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, sats_out, capacity, updated_at) \
             VALUES ('111x1x0', ?1, 'source', 0.2, 10, 90, 1000000, ?2)",
            rusqlite::params!["a".repeat(66), NOW],
        )
        .expect("seed");
        drop(conn);

        let failed = owner
            .cleanup_closed(cleanup_evidence(Err("socket gone".to_string()), None))
            .await;
        assert_eq!(
            failed,
            json!({
                "archived": 0, "cleaned": 0, "channels": [],
                "errors": ["Failed to get open channels: socket gone"],
            })
        );

        let all_open = owner
            .cleanup_closed(cleanup_evidence(
                Ok(json!({"channels": [{"short_channel_id": "111:1:0"}]})),
                None,
            ))
            .await;
        assert_eq!(
            all_open,
            json!({
                "message": "No closed channels found to clean up",
                "archived": 0, "cleaned": 0, "channels": [], "errors": [],
            }),
            "the legacy-colon spelling still counts as open"
        );
    }

    /// The closer-override arm (py 7639-7641): when the close TYPE stays
    /// unknown but listclosedchannels carries a truthy closer, that closer
    /// is recorded — here "mutual", which maps through no close_type arm.
    #[tokio::test]
    async fn cleanup_closed_takes_blob_closer_when_type_is_unknown() {
        let (_dir, path, owner, _receiver) = fixture().await;
        let conn = Connection::open(&path).expect("seed");
        conn.execute(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, sats_out, capacity, updated_at) \
             VALUES ('444x4x0', ?1, 'source', 0.2, 10, 90, 1000000, ?2)",
            rusqlite::params!["d".repeat(66), NOW],
        )
        .expect("seed");
        drop(conn);

        let response = owner
            .cleanup_closed(cleanup_evidence(
                Ok(json!({"channels": []})),
                Some(json!({"closedchannels": [{
                    "short_channel_id": "444x4x0",
                    "closer": "mutual",
                }]})),
            ))
            .await;
        assert_eq!(response["archived"], 1, "{response:?}");

        let conn = Connection::open(&path).expect("verify");
        let row: (String, String) = conn
            .query_row(
                "SELECT close_type, closer FROM closed_channels WHERE channel_id = '444x4x0'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .expect("row");
        assert_eq!(row, ("unknown".to_string(), "mutual".to_string()));
    }

    /// Absent from listclosedchannels entirely: the archive still lands,
    /// fully unknown close metadata, zero capacity — never fabricated.
    #[tokio::test]
    async fn cleanup_closed_without_closure_info_archives_honest_unknowns() {
        let (_dir, path, owner, _receiver) = fixture().await;
        let conn = Connection::open(&path).expect("seed");
        conn.execute(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, sats_out, capacity, updated_at) \
             VALUES ('333x3x0', ?1, 'source', 0.2, 10, 90, 1000000, ?2)",
            rusqlite::params!["c".repeat(66), NOW],
        )
        .expect("seed");
        drop(conn);

        let response = owner
            .cleanup_closed(cleanup_evidence(Ok(json!({"channels": []})), None))
            .await;
        assert_eq!(response["archived"], 1, "{response:?}");

        let conn = Connection::open(&path).expect("verify");
        let row: (i64, String, String, i64) = conn
            .query_row(
                "SELECT capacity_sats, close_type, closer, total_revenue_sats \
                 FROM closed_channels WHERE channel_id = '333x3x0'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .expect("row");
        assert_eq!(row, (0, "unknown".to_string(), "unknown".to_string(), 0));
    }

    /// The `handle()` dispatch edge for the two new actions — the other
    /// tests call the methods directly, so a cross-wired dispatch arm
    /// would otherwise survive every one of them. Each action is probed
    /// with params whose response shape is DISTINCTIVE to its method.
    #[tokio::test]
    async fn handle_dispatches_the_two_new_spend_actions() {
        let (_dir, _path, owner, _receiver) = fixture().await;

        // Only spend_reserve refuses non-positive amounts with this arm.
        let reserve = owner
            .handle(
                super::CoreStateMutationAction::SpendReserve,
                &params(&[
                    ("reservation_id", json!("d-1")),
                    ("category", json!("misc")),
                    ("amount_sats", json!(0)),
                ]),
            )
            .await;
        assert_eq!(reserve, json!({"error": "amount_sats must be > 0"}));

        // Only spend_release_stale answers a released_count sweep summary
        // on empty params (spend_reserve would refuse the missing amount).
        let stale = owner
            .handle(
                super::CoreStateMutationAction::SpendReleaseStale,
                &Map::new(),
            )
            .await;
        assert_eq!(stale["status"], "success");
        assert_eq!(stale["released_count"], 0);
    }
}
