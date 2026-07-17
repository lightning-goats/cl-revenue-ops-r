//! Per-channel `v2_state_json` persistence with LOSSLESS legacy-blob
//! round-trip (read-modify-write of the Python state envelope).
//!
//! Port of `fee_controller.py`'s persistence layer (Phase 4 Task 9):
//! `_load_persisted_fee_strategy_row` (3639-3656), `_extract_fee_state_payload`
//! (3659-3689), `_extract_cycle_state_payload` (3692-3715),
//! `_serialize_cycle_state_payload` (3718-3741), `_build_merged_fee_strategy_row`
//! (3743-3873), `_build_fee_strategy_row_kwargs`/`_serialize_fee_strategy_row`/
//! `_persist_fee_strategy_row`/`_flush_pending_fee_strategy_rows` (3963-4058),
//! `ChannelFeeState.to_v2_dict`/`from_v2_dict` (2138-2223), the
//! explicit-shared-fields tracking (2105-2136, 2294-2325), and
//! `_get_cycle_state`/`_save_cycle_state` (8313-8411).
//!
//! ## The lossless contract
//!
//! Every level parses known fields into types and keeps EVERYTHING else
//! (unknown keys, order, int-vs-float number types) in the
//! [`crate::pyjson::OValue`] tree it was parsed from. Two DIFFERENT
//! round-trip contracts are pinned by the test suites, and they are NOT
//! the same claim:
//!
//! 1. **Raw structural fidelity**: `dumps_python(parse(blob))` is
//!    content-identical (and, for a blob Python itself would re-emit
//!    unchanged, byte-identical) to `blob` — this holds unconditionally
//!    for ANY blob because [`crate::pyjson::OValue`] is a lossless
//!    order-preserving tree; nothing in this module needs to do anything
//!    special to satisfy it (see [`V2StateEnvelope::raw`]).
//! 2. **Merged-write fidelity**: [`build_merged_row`] reproduces
//!    `_build_merged_fee_strategy_row`'s output byte-for-byte. This is a
//!    NARROWER shape than (1): the real Python function only re-emits
//!    `algorithm_version`/`fee_state`/`cycle_state`/the 3 flat shared
//!    scalars, so unrelated top-level keys on the ORIGINAL blob (junk that
//!    predates the current nested format, or a future field some other
//!    writer added at the wrong level) are intentionally dropped by the
//!    real merge — this is Python's actual behavior, not a lossiness bug
//!    in this port. Keys inside `fee_state` (and inside `thompson_state`,
//!    via [`crate::thompson::GaussianThompsonState::extra`]) DO survive,
//!    because the nested payload is copied wholesale.

use std::collections::HashSet;

use crate::pid::{self, PidState};
use crate::pyjson::OValue;
use crate::thompson::serde::{gts_from_dict, gts_to_dict};
use crate::thompson::GaussianThompsonState;

/// `FeeController.ABS_MAX_FEE_PPM` (py 2918): hard absolute ceiling used to
/// sanitize a poisoned/out-of-range persisted `pending_target_ppm` /
/// `congestion_entry_fee_ppm` on load.
pub const ABS_MAX_FEE_PPM: i64 = 100_000;

// ---------------------------------------------------------------------------
// FeeStrategyRow: the flat `fee_strategy_state` row (production schema).
// ---------------------------------------------------------------------------

/// One `fee_strategy_state` row (read-only production schema, see
/// `fixtures/schema.sql` and `modules/database.py:655-666` +
/// migration `ALTER TABLE`s at 1099-1164). `v2_state_json` is the raw
/// stored string — `parse_v2_blob` is the only place that parses it.
#[derive(Debug, Clone, PartialEq)]
pub struct FeeStrategyRow {
    pub channel_id: String,
    pub last_revenue_rate: f64,
    pub last_fee_ppm: i64,
    pub trend_direction: i64,
    pub step_ppm: i64,
    pub consecutive_same_direction: i64,
    pub last_update: i64,
    pub last_broadcast_fee_ppm: i64,
    pub is_sleeping: bool,
    pub sleep_until: i64,
    pub stable_cycles: i64,
    pub forward_count_since_update: i64,
    pub last_volume_sats: i64,
    pub last_state: String,
    pub v2_state_json: String,
}

impl Default for FeeStrategyRow {
    fn default() -> Self {
        FeeStrategyRow {
            channel_id: String::new(),
            last_revenue_rate: 0.0,
            last_fee_ppm: 0,
            trend_direction: 1,
            step_ppm: 50,
            consecutive_same_direction: 0,
            last_update: 0,
            last_broadcast_fee_ppm: 0,
            is_sleeping: false,
            sleep_until: 0,
            stable_cycles: 0,
            forward_count_since_update: 0,
            last_volume_sats: 0,
            last_state: "balanced".to_string(),
            v2_state_json: "{}".to_string(),
        }
    }
}

/// Read-only fetch of every `fee_strategy_state` row, exactly the columns
/// the controller's own load path reads (`_load_persisted_fee_strategy_row`
/// plus the legacy-scalar columns `_extract_cycle_state_payload` reads via
/// `db_state`). Panics on a query/row-mapping error: this is diagnostic
/// tooling over a read-only snapshot copy, not a production write path —
/// see `tests/production_blobs.rs` for the only caller.
pub fn read_fee_strategy_rows(conn: &rusqlite::Connection) -> Vec<FeeStrategyRow> {
    let mut stmt = conn
        .prepare(
            "SELECT channel_id, last_revenue_rate, last_fee_ppm, trend_direction, step_ppm,
                    consecutive_same_direction, last_update, last_broadcast_fee_ppm,
                    is_sleeping, sleep_until, stable_cycles, forward_count_since_update,
                    last_volume_sats, last_state, v2_state_json
             FROM fee_strategy_state",
        )
        .expect("prepare fee_strategy_state select");
    let rows = stmt
        .query_map([], |r| {
            Ok(FeeStrategyRow {
                channel_id: r.get(0)?,
                last_revenue_rate: r.get(1)?,
                last_fee_ppm: r.get(2)?,
                trend_direction: r.get(3)?,
                step_ppm: r.get(4)?,
                consecutive_same_direction: r.get(5)?,
                last_update: r.get(6)?,
                last_broadcast_fee_ppm: r.get(7)?,
                is_sleeping: r.get::<_, i64>(8)? != 0,
                sleep_until: r.get(9)?,
                stable_cycles: r.get(10)?,
                forward_count_since_update: r.get(11)?,
                last_volume_sats: r.get(12)?,
                last_state: r
                    .get::<_, Option<String>>(13)?
                    .unwrap_or_else(|| "balanced".to_string()),
                v2_state_json: r
                    .get::<_, Option<String>>(14)?
                    .unwrap_or_else(|| "{}".to_string()),
            })
        })
        .expect("query fee_strategy_state");
    rows.filter_map(Result::ok).collect()
}

// ---------------------------------------------------------------------------
// OValue dict helpers matching Python `dict` mutation semantics exactly:
// `setdefault` never moves an existing key; `update` updates an existing
// key's value IN PLACE (same position) and appends genuinely new keys at
// the end, in the order they're encountered. Both are load-bearing for
// byte-identical key ordering against the real `json.dumps` output.
// ---------------------------------------------------------------------------

fn obj_setdefault(
    entries: &mut Vec<(String, OValue)>,
    key: &str,
    default: impl FnOnce() -> OValue,
) {
    if !entries.iter().any(|(k, _)| k == key) {
        entries.push((key.to_string(), default()));
    }
}

fn obj_update(entries: &mut Vec<(String, OValue)>, updates: &[(String, OValue)]) {
    for (k, v) in updates {
        if let Some(existing) = entries.iter_mut().find(|(ek, _)| ek == k) {
            existing.1 = v.clone();
        } else {
            entries.push((k.clone(), v.clone()));
        }
    }
}

/// Update (or insert) a single key in place — the one-key special case of
/// [`obj_update`], used for the 3 canonical shared-scalar overwrites in
/// [`build_merged_row`].
fn obj_set(entries: &mut Vec<(String, OValue)>, key: &str, value: OValue) {
    if let Some(existing) = entries.iter_mut().find(|(k, _)| k == key) {
        existing.1 = value;
    } else {
        entries.push((key.to_string(), value));
    }
}

fn truthy(v: &OValue) -> bool {
    match v {
        OValue::Null => false,
        OValue::Bool(b) => *b,
        OValue::Int(i) => *i != 0,
        OValue::Float(f) => *f != 0.0,
        OValue::Str(s) => !s.is_empty(),
        OValue::Arr(a) => !a.is_empty(),
        OValue::Obj(o) => !o.is_empty(),
    }
}

// ---------------------------------------------------------------------------
// SharedScalars / FeeStatePayload / CycleStatePayload / V2StateEnvelope
// ---------------------------------------------------------------------------

/// The 3 canonical flat scalars every merged row carries (`_shared_fields`
/// ClassVar on both `ChannelFeeState` and `ChannelCycleState`, py 2105-2107 /
/// 2294-2296): `last_gossip_refresh`, `last_broadcast_at`,
/// `dynamic_htlcmin_baseline_msat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SharedField {
    LastGossipRefresh,
    LastBroadcastAt,
    DynamicHtlcminBaselineMsat,
}

impl SharedField {
    fn key(self) -> &'static str {
        match self {
            SharedField::LastGossipRefresh => "last_gossip_refresh",
            SharedField::LastBroadcastAt => "last_broadcast_at",
            SharedField::DynamicHtlcminBaselineMsat => "dynamic_htlcmin_baseline_msat",
        }
    }
}

/// The memoized `_persisted_shared_fields[channel_id]` snapshot
/// (`_load_persisted_fee_strategy_row`, py 3648-3654).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct SharedScalars {
    pub last_gossip_refresh: i64,
    pub last_broadcast_at: i64,
    pub dynamic_htlcmin_baseline_msat: Option<i64>,
}

/// The DTS/PID portion of a persisted `v2_state_json` payload
/// (`_extract_fee_state_payload`'s return value, py 3658-3689, OR a fresh
/// `ChannelFeeState::to_v2_dict()` shape). Wraps the raw dict so unknown
/// keys the real payload carries (Python's own unknown-key passthrough via
/// `dict(nested_payload)`) survive untouched.
#[derive(Debug, Clone, PartialEq)]
pub struct FeeStatePayload(pub OValue);

impl FeeStatePayload {
    pub fn get(&self, key: &str) -> Option<&OValue> {
        self.0.get(key)
    }

    pub fn as_ovalue(&self) -> &OValue {
        &self.0
    }
}

/// The cycle-tracking portion of a persisted `v2_state_json` payload
/// (`_extract_cycle_state_payload`'s return value, py 3692-3715, OR a
/// fresh `_serialize_cycle_state_payload` shape).
#[derive(Debug, Clone, PartialEq)]
pub struct CycleStatePayload(pub OValue);

impl CycleStatePayload {
    pub fn get(&self, key: &str) -> Option<&OValue> {
        self.0.get(key)
    }

    pub fn as_ovalue(&self) -> &OValue {
        &self.0
    }
}

/// LOSSLESS envelope over a parsed `v2_state_json` blob. `raw` is the full
/// parsed tree (source of truth for the structural round-trip contract);
/// `fee_state`/`cycle_state`/`shared` are the SAME resolution the real
/// Python persistence layer performs on load, exposed as typed views for
/// [`load_fee_state`]/[`load_cycle_state`]/[`build_merged_row`].
#[derive(Debug, Clone, PartialEq)]
pub struct V2StateEnvelope {
    pub algorithm_version: String,
    pub fee_state: FeeStatePayload,
    pub cycle_state: CycleStatePayload,
    pub shared: SharedScalars,
    pub raw: OValue,
}

/// `_load_persisted_fee_strategy_row` + `_extract_fee_state_payload` +
/// `_extract_cycle_state_payload` (py 3639-3715): parse `blob` (a JSON
/// parse failure, or an empty string, mirrors Python's
/// `json.loads(...) if v2_json_str else {}` / `except JSONDecodeError:
/// v2_data = {}` fallback to an empty object — NOT a panic), then build
/// the 3 resolved views. `row` stands in for `db_state` (the flat legacy
/// scalar columns from the SAME `fee_strategy_state` row `blob` came
/// from).
pub fn parse_v2_blob(blob: &str, row: &FeeStrategyRow) -> V2StateEnvelope {
    let raw = if blob.is_empty() {
        OValue::obj(vec![])
    } else {
        crate::pyjson::parse(blob).unwrap_or_else(|_| OValue::obj(vec![]))
    };
    let shared = extract_shared_scalars(&raw, row);
    let fee_state = extract_fee_state_payload(&raw, row);
    let cycle_state = extract_cycle_state_payload(&raw, row);
    let algorithm_version = fee_state
        .get("algorithm_version")
        .and_then(OValue::as_str)
        .unwrap_or("dts_pid_v1")
        .to_string();
    V2StateEnvelope {
        algorithm_version,
        fee_state,
        cycle_state,
        shared,
        raw,
    }
}

fn extract_shared_scalars(raw: &OValue, row: &FeeStrategyRow) -> SharedScalars {
    SharedScalars {
        last_gossip_refresh: raw
            .get("last_gossip_refresh")
            .and_then(OValue::as_i64)
            .unwrap_or(0),
        last_broadcast_at: raw
            .get("last_broadcast_at")
            .and_then(OValue::as_i64)
            .unwrap_or(row.last_update),
        dynamic_htlcmin_baseline_msat: raw
            .get("dynamic_htlcmin_baseline_msat")
            .and_then(OValue::as_i64),
    }
}

/// `_extract_fee_state_payload` (py 3658-3689), verbatim.
fn extract_fee_state_payload(raw: &OValue, row: &FeeStrategyRow) -> FeeStatePayload {
    let legacy_broadcast_at = row.last_update;
    let mut entries: Vec<(String, OValue)> = match raw.get("fee_state") {
        Some(OValue::Obj(e)) => e.clone(),
        _ => {
            const FLAT_KEYS: &[&str] = &[
                "algorithm_version",
                "thompson_state",
                "last_vegas_multiplier",
                "last_gossip_refresh",
                "last_broadcast_at",
                "pid_state",
                "dynamic_htlcmin_baseline_msat",
            ];
            FLAT_KEYS
                .iter()
                .filter_map(|k| raw.get(k).map(|v| (k.to_string(), v.clone())))
                .collect()
        }
    };
    obj_setdefault(&mut entries, "algorithm_version", || {
        OValue::str("dts_pid_v1")
    });
    obj_setdefault(&mut entries, "last_vegas_multiplier", || OValue::Float(1.0));
    obj_setdefault(&mut entries, "last_gossip_refresh", || {
        raw.get("last_gossip_refresh")
            .cloned()
            .unwrap_or(OValue::Int(0))
    });
    obj_setdefault(&mut entries, "last_broadcast_at", || {
        raw.get("last_broadcast_at")
            .cloned()
            .unwrap_or(OValue::Int(legacy_broadcast_at))
    });
    obj_setdefault(&mut entries, "dynamic_htlcmin_baseline_msat", || {
        raw.get("dynamic_htlcmin_baseline_msat")
            .cloned()
            .unwrap_or(OValue::Null)
    });
    FeeStatePayload(OValue::Obj(entries))
}

/// `_extract_cycle_state_payload` (py 3692-3715), verbatim — the base-dict
/// key order (13 flat-row keys, then `last_gossip_refresh`/
/// `last_broadcast_at`/`dynamic_htlcmin_baseline_msat`) followed by
/// `payload.update(nested_payload)`'s in-place-update-else-append
/// semantics is load-bearing for byte-identical re-emission.
fn extract_cycle_state_payload(raw: &OValue, row: &FeeStrategyRow) -> CycleStatePayload {
    let mut entries: Vec<(String, OValue)> = vec![
        (
            "last_revenue_rate".to_string(),
            OValue::Float(row.last_revenue_rate),
        ),
        ("last_fee_ppm".to_string(), OValue::Int(row.last_fee_ppm)),
        (
            "trend_direction".to_string(),
            OValue::Int(row.trend_direction),
        ),
        ("step_ppm".to_string(), OValue::Int(row.step_ppm)),
        ("last_update".to_string(), OValue::Int(row.last_update)),
        (
            "consecutive_same_direction".to_string(),
            OValue::Int(row.consecutive_same_direction),
        ),
        ("is_sleeping".to_string(), OValue::Bool(row.is_sleeping)),
        ("sleep_until".to_string(), OValue::Int(row.sleep_until)),
        ("stable_cycles".to_string(), OValue::Int(row.stable_cycles)),
        (
            "last_broadcast_fee_ppm".to_string(),
            OValue::Int(row.last_broadcast_fee_ppm),
        ),
        (
            "last_state".to_string(),
            OValue::str(row.last_state.clone()),
        ),
        (
            "forward_count_since_update".to_string(),
            OValue::Int(row.forward_count_since_update),
        ),
        (
            "last_volume_sats".to_string(),
            OValue::Int(row.last_volume_sats),
        ),
        (
            "last_gossip_refresh".to_string(),
            raw.get("last_gossip_refresh")
                .cloned()
                .unwrap_or(OValue::Int(0)),
        ),
        (
            "last_broadcast_at".to_string(),
            raw.get("last_broadcast_at")
                .cloned()
                .unwrap_or(OValue::Int(row.last_update)),
        ),
        (
            "dynamic_htlcmin_baseline_msat".to_string(),
            raw.get("dynamic_htlcmin_baseline_msat")
                .cloned()
                .unwrap_or(OValue::Null),
        ),
    ];
    if let Some(OValue::Obj(nested)) = raw.get("cycle_state") {
        obj_update(&mut entries, nested);
    }
    CycleStatePayload(OValue::Obj(entries))
}

// ---------------------------------------------------------------------------
// ChannelFeeState / ChannelCycleState
// ---------------------------------------------------------------------------

/// Per-channel DTS+PID fee state (`ChannelFeeState`, py 2053-2136). The 3
/// shared scalars are private: mutate them only through
/// `set_last_gossip_refresh`/`set_last_broadcast_at`/
/// `set_dynamic_htlcmin_baseline_msat`, which mirror Python's
/// `__setattr__` hook (py 2118-2124) by recording the assignment in
/// `explicit_shared` regardless of whether the new value differs from the
/// field's current one — a real caller-initiated `state.last_gossip_refresh
/// = 0` still counts as "explicit", which is why the merge-matrix fixture
/// tests an `explicit_zero` case distinct from `untouched_default`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelFeeState {
    pub thompson: GaussianThompsonState,
    pub last_revenue_rate: f64,
    pub last_fee_ppm: i64,
    pub last_broadcast_fee_ppm: i64,
    pub last_update: i64,
    last_broadcast_at: i64,
    pub last_state: String,
    pub is_sleeping: bool,
    pub sleep_until: i64,
    pub stable_cycles: i64,
    pub forward_count_since_update: i64,
    pub last_volume_sats: i64,
    pub algorithm_version: String,
    last_gossip_refresh: i64,
    pub last_vegas_multiplier: f64,
    pub pid: PidState,
    pub last_fee_profile: String,
    pub last_context_key: String,
    pub last_time_bucket: String,
    pub last_corridor_role: String,
    pub last_contextual_sample_used: bool,
    dynamic_htlcmin_baseline_msat: Option<i64>,
    explicit_shared: HashSet<SharedField>,
}

impl Default for ChannelFeeState {
    fn default() -> Self {
        ChannelFeeState {
            thompson: GaussianThompsonState::default(),
            last_revenue_rate: 0.0,
            last_fee_ppm: 0,
            last_broadcast_fee_ppm: 0,
            last_update: 0,
            last_broadcast_at: 0,
            last_state: "balanced".to_string(),
            is_sleeping: false,
            sleep_until: 0,
            stable_cycles: 0,
            forward_count_since_update: 0,
            last_volume_sats: 0,
            algorithm_version: "dts_pid_v1".to_string(),
            last_gossip_refresh: 0,
            last_vegas_multiplier: 1.0,
            pid: PidState::default(),
            last_fee_profile: "active".to_string(),
            last_context_key: String::new(),
            last_time_bucket: "normal".to_string(),
            last_corridor_role: "P".to_string(),
            last_contextual_sample_used: false,
            dynamic_htlcmin_baseline_msat: None,
            explicit_shared: HashSet::new(),
        }
    }
}

impl ChannelFeeState {
    pub fn last_gossip_refresh(&self) -> i64 {
        self.last_gossip_refresh
    }

    pub fn last_broadcast_at(&self) -> i64 {
        self.last_broadcast_at
    }

    pub fn dynamic_htlcmin_baseline_msat(&self) -> Option<i64> {
        self.dynamic_htlcmin_baseline_msat
    }

    pub fn set_last_gossip_refresh(&mut self, v: i64) {
        self.last_gossip_refresh = v;
        self.explicit_shared.insert(SharedField::LastGossipRefresh);
    }

    pub fn set_last_broadcast_at(&mut self, v: i64) {
        self.last_broadcast_at = v;
        self.explicit_shared.insert(SharedField::LastBroadcastAt);
    }

    pub fn set_dynamic_htlcmin_baseline_msat(&mut self, v: Option<i64>) {
        self.dynamic_htlcmin_baseline_msat = v;
        self.explicit_shared
            .insert(SharedField::DynamicHtlcminBaselineMsat);
    }

    pub fn explicit_shared_fields(&self) -> &HashSet<SharedField> {
        &self.explicit_shared
    }

    pub fn clear_explicit_shared_fields(&mut self) {
        self.explicit_shared.clear();
    }
}

/// Per-channel cycle-tracking state (`ChannelCycleState`, py 2227-2325).
/// Same shared-field privacy/tracking discipline as [`ChannelFeeState`].
#[derive(Debug, Clone, PartialEq)]
pub struct ChannelCycleState {
    pub last_revenue_rate: f64,
    pub last_fee_ppm: i64,
    pub trend_direction: i64,
    pub step_ppm: i64,
    pub last_update: i64,
    last_broadcast_at: i64,
    pub consecutive_same_direction: i64,
    pub is_sleeping: bool,
    pub sleep_until: i64,
    pub stable_cycles: i64,
    pub last_broadcast_fee_ppm: i64,
    pub last_state: String,
    pub forward_count_since_update: i64,
    pub last_volume_sats: i64,
    pub congestion_active: bool,
    pub congestion_quiet_cycles: i64,
    pub congestion_entry_fee_ppm: i64,
    pub pending_target_ppm: i64,
    last_gossip_refresh: i64,
    dynamic_htlcmin_baseline_msat: Option<i64>,
    explicit_shared: HashSet<SharedField>,
}

impl Default for ChannelCycleState {
    fn default() -> Self {
        ChannelCycleState {
            last_revenue_rate: 0.0,
            last_fee_ppm: 0,
            trend_direction: 1,
            step_ppm: 50,
            last_update: 0,
            last_broadcast_at: 0,
            consecutive_same_direction: 0,
            is_sleeping: false,
            sleep_until: 0,
            stable_cycles: 0,
            last_broadcast_fee_ppm: 0,
            last_state: "balanced".to_string(),
            forward_count_since_update: 0,
            last_volume_sats: 0,
            congestion_active: false,
            congestion_quiet_cycles: 0,
            congestion_entry_fee_ppm: 0,
            pending_target_ppm: 0,
            last_gossip_refresh: 0,
            dynamic_htlcmin_baseline_msat: None,
            explicit_shared: HashSet::new(),
        }
    }
}

impl ChannelCycleState {
    pub fn last_gossip_refresh(&self) -> i64 {
        self.last_gossip_refresh
    }

    pub fn last_broadcast_at(&self) -> i64 {
        self.last_broadcast_at
    }

    pub fn dynamic_htlcmin_baseline_msat(&self) -> Option<i64> {
        self.dynamic_htlcmin_baseline_msat
    }

    pub fn set_last_gossip_refresh(&mut self, v: i64) {
        self.last_gossip_refresh = v;
        self.explicit_shared.insert(SharedField::LastGossipRefresh);
    }

    pub fn set_last_broadcast_at(&mut self, v: i64) {
        self.last_broadcast_at = v;
        self.explicit_shared.insert(SharedField::LastBroadcastAt);
    }

    pub fn set_dynamic_htlcmin_baseline_msat(&mut self, v: Option<i64>) {
        self.dynamic_htlcmin_baseline_msat = v;
        self.explicit_shared
            .insert(SharedField::DynamicHtlcminBaselineMsat);
    }

    pub fn explicit_shared_fields(&self) -> &HashSet<SharedField> {
        &self.explicit_shared
    }

    pub fn clear_explicit_shared_fields(&mut self) {
        self.explicit_shared.clear();
    }
}

// ---------------------------------------------------------------------------
// to_v2_dict / from_v2_dict (ChannelFeeState) and the cycle-state
// equivalents.
// ---------------------------------------------------------------------------

/// `ChannelFeeState.to_v2_dict` (py 2138-2163) — exact key order.
pub fn fee_state_to_v2_dict(s: &ChannelFeeState) -> OValue {
    OValue::obj(vec![
        (
            "algorithm_version".to_string(),
            OValue::str(s.algorithm_version.clone()),
        ),
        ("thompson_state".to_string(), gts_to_dict(&s.thompson)),
        (
            "last_vegas_multiplier".to_string(),
            OValue::Float(s.last_vegas_multiplier),
        ),
        (
            "last_gossip_refresh".to_string(),
            OValue::Int(s.last_gossip_refresh),
        ),
        (
            "last_broadcast_at".to_string(),
            OValue::Int(s.last_broadcast_at),
        ),
        ("pid_state".to_string(), pid::pid_to_dict(&s.pid)),
        (
            "last_fee_profile".to_string(),
            OValue::str(s.last_fee_profile.clone()),
        ),
        (
            "last_context_key".to_string(),
            OValue::str(s.last_context_key.clone()),
        ),
        (
            "last_time_bucket".to_string(),
            OValue::str(s.last_time_bucket.clone()),
        ),
        (
            "last_corridor_role".to_string(),
            OValue::str(s.last_corridor_role.clone()),
        ),
        (
            "last_contextual_sample_used".to_string(),
            OValue::Bool(s.last_contextual_sample_used),
        ),
        (
            "dynamic_htlcmin_baseline_msat".to_string(),
            s.dynamic_htlcmin_baseline_msat
                .map(OValue::Int)
                .unwrap_or(OValue::Null),
        ),
    ])
}

const FEE_STATE_KNOWN_VERSIONS: &[&str] = &["thompson_aimd_v1", "dts_pid_v1"];

/// `ChannelFeeState.from_v2_dict` (py 2164-2223), verbatim, PLUS the
/// controller-level unknown-version migration stamp
/// (`_get_channel_fee_state_locked`, py 3927-3936: an unrecognized
/// `algorithm_version` gets overwritten to `"dts_pid_v1"` after load — this
/// is NOT part of `from_v2_dict` itself in Python, but every real call site
/// applies it immediately after, so [`load_fee_state`] folds it in.
/// `legacy_state` (Python's optional argument) is always supplied here as
/// `row` — a real `db_state` dict is `{"channel_id": ...}` at the very
/// least, which is Python-truthy, so the "legacy fields from main table"
/// block (py 2213-2222) always applies in production; this port applies it
/// unconditionally to match.
pub fn load_fee_state(env: &V2StateEnvelope, row: &FeeStrategyRow) -> ChannelFeeState {
    let d = env.fee_state.as_ovalue();
    let empty = OValue::obj(vec![]);
    let mut state = ChannelFeeState::default();

    let orig_version = d.get("algorithm_version").and_then(OValue::as_str);
    state.thompson = gts_from_dict(d.get("thompson_state").unwrap_or(&empty));
    state.algorithm_version = orig_version
        .map(|s| s.to_string())
        .unwrap_or_else(|| "migrated".to_string());
    state.last_vegas_multiplier = d
        .get("last_vegas_multiplier")
        .and_then(OValue::as_f64)
        .unwrap_or(1.0);
    state.last_gossip_refresh = d
        .get("last_gossip_refresh")
        .and_then(OValue::as_i64)
        .unwrap_or(0);
    let legacy_broadcast_at = row.last_update;
    state.last_broadcast_at = d
        .get("last_broadcast_at")
        .and_then(OValue::as_i64)
        .unwrap_or(legacy_broadcast_at);
    state.pid = pid::pid_from_dict(d.get("pid_state").unwrap_or(&empty));

    state.last_fee_profile = d
        .get("last_fee_profile")
        .and_then(OValue::as_str)
        .unwrap_or("active")
        .to_string();
    state.last_context_key = d
        .get("last_context_key")
        .and_then(OValue::as_str)
        .unwrap_or("")
        .to_string();
    state.last_time_bucket = d
        .get("last_time_bucket")
        .and_then(OValue::as_str)
        .unwrap_or("normal")
        .to_string();
    state.last_corridor_role = d
        .get("last_corridor_role")
        .and_then(OValue::as_str)
        .unwrap_or("P")
        .to_string();
    state.last_contextual_sample_used = d
        .get("last_contextual_sample_used")
        .map(truthy)
        .unwrap_or(false);
    state.dynamic_htlcmin_baseline_msat = d
        .get("dynamic_htlcmin_baseline_msat")
        .and_then(OValue::as_i64);

    // Legacy fields from the main table (`row`, always Python-truthy in
    // production — see doc comment above).
    state.last_revenue_rate = row.last_revenue_rate;
    state.last_fee_ppm = row.last_fee_ppm;
    state.last_broadcast_fee_ppm = row.last_broadcast_fee_ppm;
    state.last_update = row.last_update;
    state.last_state = row.last_state.clone();
    state.is_sleeping = row.is_sleeping;
    state.sleep_until = row.sleep_until;
    state.stable_cycles = row.stable_cycles;
    state.forward_count_since_update = row.forward_count_since_update;
    state.last_volume_sats = row.last_volume_sats;

    if !orig_version
        .map(|v| FEE_STATE_KNOWN_VERSIONS.contains(&v))
        .unwrap_or(false)
    {
        state.algorithm_version = "dts_pid_v1".to_string();
    }

    state.clear_explicit_shared_fields();
    state
}

/// `_serialize_cycle_state_payload` (py 3718-3741) — exact key order.
pub fn serialize_cycle_state_payload(s: &ChannelCycleState) -> OValue {
    OValue::obj(vec![
        (
            "last_revenue_rate".to_string(),
            OValue::Float(s.last_revenue_rate),
        ),
        ("last_fee_ppm".to_string(), OValue::Int(s.last_fee_ppm)),
        (
            "trend_direction".to_string(),
            OValue::Int(s.trend_direction),
        ),
        ("step_ppm".to_string(), OValue::Int(s.step_ppm)),
        ("last_update".to_string(), OValue::Int(s.last_update)),
        (
            "last_broadcast_at".to_string(),
            OValue::Int(s.last_broadcast_at),
        ),
        (
            "consecutive_same_direction".to_string(),
            OValue::Int(s.consecutive_same_direction),
        ),
        ("is_sleeping".to_string(), OValue::Bool(s.is_sleeping)),
        ("sleep_until".to_string(), OValue::Int(s.sleep_until)),
        ("stable_cycles".to_string(), OValue::Int(s.stable_cycles)),
        (
            "last_broadcast_fee_ppm".to_string(),
            OValue::Int(s.last_broadcast_fee_ppm),
        ),
        ("last_state".to_string(), OValue::str(s.last_state.clone())),
        (
            "forward_count_since_update".to_string(),
            OValue::Int(s.forward_count_since_update),
        ),
        (
            "last_volume_sats".to_string(),
            OValue::Int(s.last_volume_sats),
        ),
        (
            "congestion_active".to_string(),
            OValue::Bool(s.congestion_active),
        ),
        (
            "congestion_quiet_cycles".to_string(),
            OValue::Int(s.congestion_quiet_cycles),
        ),
        (
            "congestion_entry_fee_ppm".to_string(),
            OValue::Int(s.congestion_entry_fee_ppm),
        ),
        (
            "pending_target_ppm".to_string(),
            OValue::Int(s.pending_target_ppm),
        ),
        (
            "last_gossip_refresh".to_string(),
            OValue::Int(s.last_gossip_refresh),
        ),
        (
            "dynamic_htlcmin_baseline_msat".to_string(),
            s.dynamic_htlcmin_baseline_msat
                .map(OValue::Int)
                .unwrap_or(OValue::Null),
        ),
    ])
}

/// The uncached load path of `_get_cycle_state` (py 8313-8391, MINUS the
/// desync-correction blocks, which need an `actual_fee_ppm` argument this
/// function's signature doesn't take — see the interface doc comment on
/// [`load_cycle_state`]).
pub fn load_cycle_state(env: &V2StateEnvelope, _row: &FeeStrategyRow) -> ChannelCycleState {
    let cd = env.cycle_state.as_ovalue();
    let get_i64 = |k: &str, default: i64| cd.get(k).and_then(OValue::as_i64).unwrap_or(default);
    let get_f64 = |k: &str, default: f64| cd.get(k).and_then(OValue::as_f64).unwrap_or(default);
    let get_str = |k: &str, default: &str| {
        cd.get(k)
            .and_then(OValue::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| default.to_string())
    };
    let get_bool = |k: &str, default: bool| cd.get(k).map(truthy).unwrap_or(default);

    // P2 sanitization (py 8344-8351): non-numeric -> 0, clamp to
    // [0, ABS_MAX_FEE_PPM].
    let pending_target_ppm = get_i64("pending_target_ppm", 0).clamp(0, ABS_MAX_FEE_PPM);
    // `_safe_entry_fee` (py 8352-8357): same clamp shape.
    let congestion_entry_fee_ppm = get_i64("congestion_entry_fee_ppm", 0).clamp(0, ABS_MAX_FEE_PPM);

    let mut state = ChannelCycleState {
        last_revenue_rate: get_f64("last_revenue_rate", 0.0),
        last_fee_ppm: get_i64("last_fee_ppm", 0),
        trend_direction: get_i64("trend_direction", 1),
        step_ppm: get_i64("step_ppm", 50),
        last_update: get_i64("last_update", 0),
        last_broadcast_at: get_i64("last_broadcast_at", get_i64("last_update", 0)),
        consecutive_same_direction: get_i64("consecutive_same_direction", 0),
        is_sleeping: get_bool("is_sleeping", false),
        sleep_until: get_i64("sleep_until", 0),
        stable_cycles: get_i64("stable_cycles", 0),
        last_broadcast_fee_ppm: get_i64("last_broadcast_fee_ppm", 0),
        last_state: get_str("last_state", "balanced"),
        forward_count_since_update: get_i64("forward_count_since_update", 0),
        last_volume_sats: get_i64("last_volume_sats", 0),
        congestion_active: get_bool("congestion_active", false),
        congestion_quiet_cycles: get_i64("congestion_quiet_cycles", 0),
        congestion_entry_fee_ppm,
        pending_target_ppm,
        last_gossip_refresh: get_i64("last_gossip_refresh", 0),
        dynamic_htlcmin_baseline_msat: cd
            .get("dynamic_htlcmin_baseline_msat")
            .and_then(OValue::as_i64),
        explicit_shared: HashSet::new(),
    };
    state.clear_explicit_shared_fields();
    state
}

// ---------------------------------------------------------------------------
// build_merged_row: `_build_merged_fee_strategy_row` (py 3743-3873).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CallerPreference {
    Fee,
    Cycle,
}

/// `_build_merged_fee_strategy_row(channel_id, cycle_state=cycle,
/// fee_state=fee)` (py 3743-3873): merges the caller's update(s) with the
/// persisted counterpart state so save order never destroys the other
/// side of the controller. `persisted` must be the envelope parsed from
/// the SAME channel's currently-persisted row (`parse_v2_blob`'s output) —
/// this function does not re-read or re-parse anything itself, matching
/// the "PERF: warm in-memory states skip the full-row re-read" shortcut in
/// spirit (the re-read already happened, once, in `parse_v2_blob`).
pub fn build_merged_row(
    channel_id: &str,
    cycle: Option<&ChannelCycleState>,
    fee: Option<&ChannelFeeState>,
    persisted: &V2StateEnvelope,
) -> (FeeStrategyRow, OValue) {
    let mut cycle_payload: Vec<(String, OValue)> = match cycle {
        Some(cs) => match serialize_cycle_state_payload(cs) {
            OValue::Obj(e) => e,
            _ => unreachable!(),
        },
        None => match persisted.cycle_state.as_ovalue() {
            OValue::Obj(e) => e.clone(),
            _ => vec![],
        },
    };
    let mut fee_payload: Vec<(String, OValue)> = match fee {
        Some(fs) => match fee_state_to_v2_dict(fs) {
            OValue::Obj(e) => e,
            _ => unreachable!(),
        },
        None => match persisted.fee_state.as_ovalue() {
            OValue::Obj(e) => e.clone(),
            _ => vec![],
        },
    };

    // Caller-preference rule (py 3809-3816): fee-caller wins ONLY when
    // fee_state is given WITHOUT cycle_state; every other combination
    // (cycle-only, or both given) prefers cycle_state, and fee_state's own
    // explicit-shared tracking is then IGNORED entirely.
    let (preference, explicit_shared_fields) = match (cycle, fee) {
        (None, Some(fs)) => (CallerPreference::Fee, fs.explicit_shared_fields().clone()),
        (cycle, _) => (
            CallerPreference::Cycle,
            cycle
                .map(|c| c.explicit_shared_fields().clone())
                .unwrap_or_default(),
        ),
    };

    let resolve = |field: SharedField, persisted_value: OValue| -> OValue {
        let primary = match preference {
            CallerPreference::Fee => &fee_payload,
            CallerPreference::Cycle => &cycle_payload,
        };
        if explicit_shared_fields.contains(&field) {
            if let Some((_, v)) = primary.iter().find(|(k, _)| k == field.key()) {
                return v.clone();
            }
        }
        persisted_value
    };

    let persisted_gossip = OValue::Int(persisted.shared.last_gossip_refresh);
    let persisted_broadcast = OValue::Int(persisted.shared.last_broadcast_at);
    let persisted_htlcmin = persisted
        .shared
        .dynamic_htlcmin_baseline_msat
        .map(OValue::Int)
        .unwrap_or(OValue::Null);

    let canonical_gossip = resolve(SharedField::LastGossipRefresh, persisted_gossip);
    let canonical_broadcast = resolve(SharedField::LastBroadcastAt, persisted_broadcast);
    let canonical_htlcmin = resolve(SharedField::DynamicHtlcminBaselineMsat, persisted_htlcmin);

    obj_set(
        &mut cycle_payload,
        "last_gossip_refresh",
        canonical_gossip.clone(),
    );
    obj_set(
        &mut cycle_payload,
        "last_broadcast_at",
        canonical_broadcast.clone(),
    );
    obj_set(
        &mut cycle_payload,
        "dynamic_htlcmin_baseline_msat",
        canonical_htlcmin.clone(),
    );
    obj_set(
        &mut fee_payload,
        "last_gossip_refresh",
        canonical_gossip.clone(),
    );
    obj_set(
        &mut fee_payload,
        "last_broadcast_at",
        canonical_broadcast.clone(),
    );
    obj_set(
        &mut fee_payload,
        "dynamic_htlcmin_baseline_msat",
        canonical_htlcmin.clone(),
    );

    let algorithm_version = fee_payload
        .iter()
        .find(|(k, _)| k == "algorithm_version")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| OValue::str("dts_pid_v1"));

    let merged_v2 = OValue::obj(vec![
        ("algorithm_version".to_string(), algorithm_version),
        ("fee_state".to_string(), OValue::Obj(fee_payload.clone())),
        (
            "cycle_state".to_string(),
            OValue::Obj(cycle_payload.clone()),
        ),
        ("last_gossip_refresh".to_string(), canonical_gossip),
        ("last_broadcast_at".to_string(), canonical_broadcast),
        (
            "dynamic_htlcmin_baseline_msat".to_string(),
            canonical_htlcmin,
        ),
    ]);

    let cp = OValue::Obj(cycle_payload);
    let row_fields = FeeStrategyRow {
        channel_id: channel_id.to_string(),
        last_revenue_rate: cp
            .get("last_revenue_rate")
            .and_then(OValue::as_f64)
            .unwrap_or(0.0),
        last_fee_ppm: cp.get("last_fee_ppm").and_then(OValue::as_i64).unwrap_or(0),
        trend_direction: cp
            .get("trend_direction")
            .and_then(OValue::as_i64)
            .unwrap_or(1),
        step_ppm: cp.get("step_ppm").and_then(OValue::as_i64).unwrap_or(50),
        consecutive_same_direction: cp
            .get("consecutive_same_direction")
            .and_then(OValue::as_i64)
            .unwrap_or(0),
        last_broadcast_fee_ppm: cp
            .get("last_broadcast_fee_ppm")
            .and_then(OValue::as_i64)
            .unwrap_or(0),
        last_state: cp
            .get("last_state")
            .and_then(OValue::as_str)
            .unwrap_or("unknown")
            .to_string(),
        is_sleeping: cp.get("is_sleeping").map(truthy).unwrap_or(false),
        sleep_until: cp.get("sleep_until").and_then(OValue::as_i64).unwrap_or(0),
        stable_cycles: cp
            .get("stable_cycles")
            .and_then(OValue::as_i64)
            .unwrap_or(0),
        forward_count_since_update: cp
            .get("forward_count_since_update")
            .and_then(OValue::as_i64)
            .unwrap_or(0),
        last_volume_sats: cp
            .get("last_volume_sats")
            .and_then(OValue::as_i64)
            .unwrap_or(0),
        last_update: cp.get("last_update").and_then(OValue::as_i64).unwrap_or(0),
        v2_state_json: String::new(),
    };

    (row_fields, merged_v2)
}

// ---------------------------------------------------------------------------
// PendingRows: `_pending_fee_strategy_rows` + `_flush_pending_fee_strategy_rows`
// (py 3963-4058) — last-write-wins per channel, serialized EXACTLY ONCE at
// flush.
// ---------------------------------------------------------------------------

/// Rows enqueued during a batched cycle, keyed by channel_id (last write
/// per channel wins; first-enqueue POSITION is preserved on a re-write of
/// the same channel, matching Python `dict` reassignment semantics — this
/// only affects flush ORDER, never content). `v2_state_json` is kept as an
/// unserialized [`OValue`] tree until [`PendingRows::flush`] — the `dumps`
/// call happens exactly once per channel, no matter how many times a
/// channel is re-enqueued within one cycle.
#[derive(Debug, Default)]
pub struct PendingRows {
    entries: Vec<(String, FeeStrategyRow, OValue)>,
}

impl PendingRows {
    pub fn new() -> Self {
        PendingRows {
            entries: Vec::new(),
        }
    }

    /// `_persist_fee_strategy_row` when `_cycle_batch_active` (py 3989-3991):
    /// `self._pending_fee_strategy_rows[channel_id] = row_kwargs`.
    pub fn enqueue(&mut self, channel_id: &str, row: FeeStrategyRow, v2: OValue) {
        if let Some(existing) = self.entries.iter_mut().find(|(id, _, _)| id == channel_id) {
            existing.1 = row;
            existing.2 = v2;
        } else {
            self.entries.push((channel_id.to_string(), row, v2));
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `_flush_pending_fee_strategy_rows` (py 3994-4022), the serialization
    /// half: `_serialize_fee_strategy_row` (py 3999-4006) runs over every
    /// pending row exactly once (`json.dumps`, i.e.
    /// [`crate::pyjson::dumps_python`]), and the pending map is cleared
    /// first (`pending = self._pending_fee_strategy_rows; ...;
    /// self._pending_fee_strategy_rows = {}`) so a re-entrant enqueue
    /// during the write itself would start a fresh batch — this port
    /// mirrors that by draining `self.entries` up front via `mem::take`.
    /// The actual DB write (batch-or-per-row, with the batch-write
    /// exception fallback) is the caller's concern — this type owns only
    /// the enqueue/serialize-once contract, not I/O.
    pub fn flush(&mut self) -> Vec<FeeStrategyRow> {
        let drained = std::mem::take(&mut self.entries);
        drained
            .into_iter()
            .map(|(_, mut row, v2)| {
                row.v2_state_json = crate::pyjson::dumps_python(&v2);
                row
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(channel_id: &str) -> FeeStrategyRow {
        FeeStrategyRow {
            channel_id: channel_id.to_string(),
            ..FeeStrategyRow::default()
        }
    }

    #[test]
    fn parse_v2_blob_empty_string_falls_back_to_empty_object() {
        let r = row("c1");
        let env = parse_v2_blob("", &r);
        assert_eq!(env.raw, OValue::obj(vec![]));
        assert_eq!(env.algorithm_version, "dts_pid_v1");
    }

    #[test]
    fn parse_v2_blob_invalid_json_falls_back_to_empty_object() {
        let r = row("c1");
        let env = parse_v2_blob("{not json", &r);
        assert_eq!(env.raw, OValue::obj(vec![]));
    }

    #[test]
    fn obj_setdefault_does_not_move_existing_key() {
        let mut entries = vec![
            ("a".to_string(), OValue::Int(1)),
            ("b".to_string(), OValue::Int(2)),
        ];
        obj_setdefault(&mut entries, "a", || OValue::Int(999));
        obj_setdefault(&mut entries, "c", || OValue::Int(3));
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), OValue::Int(1)),
                ("b".to_string(), OValue::Int(2)),
                ("c".to_string(), OValue::Int(3)),
            ]
        );
    }

    #[test]
    fn obj_update_updates_in_place_and_appends_new_keys() {
        let mut entries = vec![
            ("a".to_string(), OValue::Int(1)),
            ("b".to_string(), OValue::Int(2)),
        ];
        let updates = vec![
            ("b".to_string(), OValue::Int(99)),
            ("c".to_string(), OValue::Int(3)),
        ];
        obj_update(&mut entries, &updates);
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), OValue::Int(1)),
                ("b".to_string(), OValue::Int(99)),
                ("c".to_string(), OValue::Int(3)),
            ]
        );
    }

    #[test]
    fn explicit_shared_field_tracking_mirrors_python_setattr_hook() {
        let mut cs = ChannelCycleState::default();
        assert!(cs.explicit_shared_fields().is_empty());
        cs.set_last_gossip_refresh(0); // explicit, even though value == default
        assert!(cs
            .explicit_shared_fields()
            .contains(&SharedField::LastGossipRefresh));
        cs.clear_explicit_shared_fields();
        assert!(cs.explicit_shared_fields().is_empty());
    }

    #[test]
    fn pending_rows_last_write_wins_and_serializes_once() {
        let mut pending = PendingRows::new();
        pending.enqueue(
            "chan1",
            row("chan1"),
            OValue::obj(vec![("v".to_string(), OValue::Int(1))]),
        );
        pending.enqueue(
            "chan1",
            row("chan1"),
            OValue::obj(vec![("v".to_string(), OValue::Int(2))]),
        );
        pending.enqueue(
            "chan2",
            row("chan2"),
            OValue::obj(vec![("v".to_string(), OValue::Int(3))]),
        );
        assert_eq!(pending.len(), 2);
        let flushed = pending.flush();
        assert!(pending.is_empty());
        assert_eq!(flushed.len(), 2);
        assert_eq!(flushed[0].channel_id, "chan1");
        assert_eq!(flushed[0].v2_state_json, "{\"v\": 2}");
        assert_eq!(flushed[1].channel_id, "chan2");
        assert_eq!(flushed[1].v2_state_json, "{\"v\": 3}");
    }

    #[test]
    fn build_merged_row_with_no_callers_reproduces_persisted_payloads() {
        let r = row("c1");
        let blob = r#"{"algorithm_version": "dts_pid_v1", "fee_state": {"algorithm_version": "dts_pid_v1"}, "cycle_state": {}, "last_gossip_refresh": 42, "last_broadcast_at": 7, "dynamic_htlcmin_baseline_msat": 900}"#;
        let env = parse_v2_blob(blob, &r);
        let (_row_fields, merged) = build_merged_row("c1", None, None, &env);
        assert_eq!(merged.get("last_gossip_refresh"), Some(&OValue::Int(42)));
        assert_eq!(merged.get("last_broadcast_at"), Some(&OValue::Int(7)));
        assert_eq!(
            merged.get("dynamic_htlcmin_baseline_msat"),
            Some(&OValue::Int(900))
        );
    }
}
