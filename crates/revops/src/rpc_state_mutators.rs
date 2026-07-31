//! Exact policy-mutator planning and response contracts for canonical RPCs.

use revops_analytics::policy::{is_valid_peer_id, PeerPolicy};
use revops_db::{
    actor::DbHandle,
    budget::{ClearStats, ReserveRequest},
    state_writer::{PeerPolicyWrite, PolicyDelete, SpendReleaseBatch},
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    SpendSettle,
}

pub struct CoreStateMutationOwner {
    core: CoreMutators,
    reader: DbHandle,
    fee_ingress: SchedulerIngress,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    daily_budget_sats: Arc<dyn Fn() -> i64 + Send + Sync>,
    rate_limit: Mutex<HashMap<String, Vec<i64>>>,
}

impl CoreStateMutationOwner {
    pub fn assemble(
        core: CoreMutators,
        reader: DbHandle,
        fee_ingress: SchedulerIngress,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
        daily_budget_sats: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Self {
        Self {
            core,
            reader,
            fee_ingress,
            clock,
            daily_budget_sats,
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

    async fn fixture() -> (
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
}
