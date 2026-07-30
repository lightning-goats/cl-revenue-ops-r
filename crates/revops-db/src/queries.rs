//! DB-backed read-query functions for Phase 1b's read-RPC subset
//! (`revenue-r-history`, `-report`, `-dashboard`). Each function is a
//! statement-for-statement port of its `database.py`/
//! `profitability_analyzer.py` namesake in `~/bin/cl_revenue_ops-port`
//! (branch `port`) -- see each function's doc comment for the exact source
//! lines. All queries run read-only, through the persistent actor
//! (`crate::actor::DbHandle`), never crossing a task boundary.
//!
//! These are pure DB aggregates -- no live `listfunds`/`listpeerchannels`
//! RPC calls, no `policy_manager`/`fee_controller` state. That is this
//! phase's own scope boundary (see the plan's per-RPC gap table): fields
//! that need something beyond plain SQL are built by the `rpc_*` response
//! builders in `crates/revops`, not here, as an explicit `_phase1b_gaps`
//! entry.

use crate::actor::DbHandle;
use anyhow::{Context, Result};
use revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode};
use revops_core::msat::{base_to_sats_ceil, base_to_sats_floor, py_round2};
use rusqlite::{types::Value as SqlValue, Row};
use std::collections::BTreeMap;
use std::collections::HashMap;

/// Port of `Database.get_config_override` (modules/database.py:7316-7322):
/// `SELECT value FROM config_overrides WHERE key = ?`. `key` is the Python
/// `Config` dataclass field name (snake_case, e.g. `min_fee_ppm`), NOT the
/// CLN option suffix (`min-fee-ppm`) -- `config_overrides.key` is written
/// by `Database.set_config_override` keyed exactly the same way
/// `Config.load_overrides` reads it back (`hasattr(self, key)`,
/// modules/config.py:912). Returns `None` when no override row exists for
/// `key` -- the common case, never an error (see
/// [`crate::actor::DbHandle::query_optional_string`]).
pub async fn config_override(handle: &DbHandle, key: &str) -> Result<Option<String>> {
    handle
        .query_optional_string(
            "SELECT value FROM config_overrides WHERE key = ?1",
            vec![SqlValue::Text(key.to_string())],
        )
        .await
}

/// Lossy `SqlValue` -> `String`, defaulting to `default` on `NULL` and
/// coercing any other storage class via its natural text representation
/// (SQLite's own dynamic typing already allows a column to hold any class
/// regardless of declared affinity, so a mistyped cell here is not
/// exceptional -- just another value to represent as text).
fn sql_text_or(value: SqlValue, default: &str) -> String {
    match value {
        SqlValue::Null => default.to_string(),
        SqlValue::Text(s) => s,
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Real(f) => f.to_string(),
        SqlValue::Blob(b) => String::from_utf8_lossy(&b).into_owned(),
    }
}

/// Lossy `SqlValue` -> `Option<i64>`: `NULL` and anything that fails to
/// parse as an integer both become `None` (never an error, never a
/// dropped row) -- the same "python-int-timestamp-cell" leniency
/// `spend_ledger_aggregates` already uses for coverage timestamps, reused
/// here for policy scalar columns.
fn sql_opt_i64(value: SqlValue) -> Option<i64> {
    match value {
        SqlValue::Null => None,
        SqlValue::Integer(i) => Some(i),
        SqlValue::Real(f) if f.is_finite() => {
            let truncated = f.trunc();
            if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                Some(truncated as i64)
            } else {
                None
            }
        }
        SqlValue::Real(_) => None,
        SqlValue::Text(s) => s.trim().parse::<i64>().ok(),
        SqlValue::Blob(b) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok()),
    }
}

/// Lossy `SqlValue` -> `Option<f64>`, same "default rather than drop"
/// convention as [`sql_opt_i64`].
fn sql_opt_f64(value: SqlValue) -> Option<f64> {
    match value {
        SqlValue::Null => None,
        SqlValue::Real(f) => Some(f),
        SqlValue::Integer(i) => Some(i as f64),
        SqlValue::Text(s) => s.trim().parse::<f64>().ok(),
        SqlValue::Blob(b) => std::str::from_utf8(&b)
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok()),
    }
}

/// Round-2 correction, CRITICAL: decode the `tags` JSON column at the
/// ELEMENT level, not as a single typed `Vec<String>` parse. Python's
/// `json.loads('["banned", 7]')` succeeds and returns the list
/// `["banned", 7]` UNCHANGED -- `"banned" in tags` is still `True` with the
/// non-string `7` sitting right next to it, because Python lists are
/// heterogeneous and `in` never cares about a sibling element's type. The
/// PRE-round-2 decode instead parsed the WHOLE column as `Vec<String>` via
/// serde's typed array deserializer, which fails outright the moment ANY
/// element isn't a JSON string; `.unwrap_or_default()` then replaced the
/// ENTIRE array with `[]` -- silently erasing the valid `"banned"` tag
/// alongside its one malformed sibling and vanishing the peer from
/// `revenue-r-list-banned`. That is the F10 defect one layer down: F10's
/// row-level fix (below) stops a malformed SCALAR column from dropping the
/// row; this stops a malformed TAGS-ARRAY ELEMENT from dropping the tag.
///
/// **This is a DELIBERATE FAIL-SAFE DIVERGENCE from Python, not a
/// Python-exact port.** Python's `tags` stays a heterogeneous list --
/// the raw `7` stays IN the list, it just never matches a string tag test.
/// Rust's `PeerPolicy::tags` is typed `Vec<String>`, so there is no
/// equivalent slot to keep a non-string member in. Dropping ONLY the
/// non-string element (never the whole array, never the row) preserves
/// every membership/reason-lookup result Python's `"tag" in tags`/`next(t
/// for t in tags if ...)` could ever produce for a STRING tag -- the only
/// kind any real writer (`revenue-ban`, `-hot-channel-protection-peers`,
/// operator tooling) ever puts in this column -- while failing SAFE
/// (a numeric/object/array element is silently invisible to every tag
/// test, exactly as if it were never recorded) instead of failing OPEN
/// (the pre-round-2 behavior of erasing the whole tag set, including any
/// real `"banned"` membership, over one malformed sibling).
fn decode_tags_json(raw: &str) -> Vec<String> {
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(serde_json::Value::Array(items)) => items
            .into_iter()
            .filter_map(|item| match item {
                serde_json::Value::String(s) => Some(s),
                // Non-string element (number, bool, null, array, object):
                // dropped INDIVIDUALLY -- never propagated to `.ok()`/
                // `unwrap_or_default()` on the whole array. See this
                // function's doc comment for why this is a fail-safe
                // divergence from Python's own (heterogeneous-list-
                // preserving) `json.loads` behavior, not a byte-exact port
                // of it.
                _ => None,
            })
            .collect(),
        // Malformed JSON entirely, or valid JSON that isn't an array
        // (`json.loads` on a non-string Python value raises `TypeError`,
        // caught the same way as a JSON decode error in Python) -- both
        // default to empty, never drop the row.
        _ => Vec::new(),
    }
}

/// Task 50 correction round, F10 (fix, per the supervisor's scope
/// update): port of `PolicyManager._row_to_policy`
/// (policy_manager.py:395-422). Python NEVER validates or drops a row over
/// a malformed/NULL scalar column -- `row['peer_id']`/`row['updated_at']`/
/// etc. are returned exactly as SQLite stored them, unchecked; only the
/// tags-JSON decode and the two enum conversions have explicit
/// try/except-with-default handling. The OLD Rust decode used
/// `row.get::<_, T>(i)?` with a STRICT target type per column and
/// `.ok()`-dropped the WHOLE row on the first conversion failure --
/// security-relevant, because a banned peer with one malformed cell
/// (e.g. `expires_at` holding non-numeric text) would silently vanish
/// from `revenue-r-list-banned`/`-list-ignored`/`-policy list`.
///
/// This version decodes every column via the lossy `SqlValue`-based
/// helpers above, so it CANNOT fail on a malformed/NULL scalar -- the row
/// is always kept, with per-column defaults standing in for whatever
/// didn't parse: `peer_id`/`strategy`/`rebalance_mode` default to `""`
/// (which then falls through the existing enum-default handling below for
/// the latter two); `fee_ppm_target`/`fee_multiplier_min/max`/`expires_at`
/// default to `None`; `updated_at` defaults to `0`; malformed/non-array
/// tags JSON defaults to `[]` (see [`decode_tags_json`] for the
/// element-level policy when the JSON parses but individual elements
/// don't). `expires_at: None` specifically is the FAIL-SAFE reading for a
/// security-relevant field -- a policy row with a garbage expiry stays
/// visible (never expires) rather than silently reading as already-expired
/// and being filtered out. **These per-column scalar defaults are also a
/// deliberate fail-safe divergence, not a Python-exact port**: Python's
/// `_row_to_policy` (policy_manager.py:384-439) does NOT generally coerce
/// malformed scalar column types -- it returns `row['peer_id']`,
/// `row['fee_ppm_target']`, `row['updated_at']`, and the present v2
/// columns directly, whatever SQLite handed back, and only wraps the tags
/// JSON decode and the two enum conversions in try/except-with-default.
/// Coercing the scalar columns too is a Rust-side strengthening (never
/// drop the row, never propagate a type panic) chosen to satisfy the
/// no-silent-drop requirement, not a claim that Python does the same
/// coercion.
fn decode_policy_row(row: &Row) -> rusqlite::Result<PeerPolicy> {
    let peer_id = sql_text_or(row.get::<_, SqlValue>(0)?, "");
    let strategy = sql_text_or(row.get::<_, SqlValue>(1)?, "");
    let rebalance_mode = sql_text_or(row.get::<_, SqlValue>(2)?, "");
    let fee_ppm_target = sql_opt_i64(row.get::<_, SqlValue>(3)?);
    let tags = match row.get::<_, SqlValue>(4)? {
        SqlValue::Text(raw) => decode_tags_json(&raw),
        // NULL, or a non-text storage class (`json.loads` on a non-string
        // Python value raises `TypeError`, caught the same way as a JSON
        // decode error) -- both default to empty, never drop the row.
        _ => Vec::new(),
    };
    let updated_at = sql_opt_i64(row.get::<_, SqlValue>(5)?).unwrap_or(0);
    let fee_multiplier_min = sql_opt_f64(row.get::<_, SqlValue>(6)?);
    let fee_multiplier_max = sql_opt_f64(row.get::<_, SqlValue>(7)?);
    let expires_at = sql_opt_i64(row.get::<_, SqlValue>(8)?);

    Ok(PeerPolicy {
        peer_id,
        strategy: FeeStrategy::from_value(&strategy).unwrap_or(FeeStrategy::Dynamic),
        rebalance_mode: RebalanceMode::from_value(&rebalance_mode)
            .unwrap_or(RebalanceMode::Enabled),
        fee_ppm_target,
        tags,
        updated_at,
        fee_multiplier_min,
        fee_multiplier_max,
        expires_at,
    })
}

async fn query_policy_rows(
    handle: &DbHandle,
    sql: &'static str,
    params: Vec<SqlValue>,
) -> Result<Vec<PeerPolicy>> {
    // Task 50 correction round, F10: `decode_policy_row` can no longer
    // fail on a malformed/NULL scalar column (see its doc comment), so
    // there is no longer a per-row `.ok()`-drop here -- a row is only
    // ever absent from the result because the SQL genuinely returned
    // fewer rows, never because one field didn't parse. Should
    // `decode_policy_row` ever legitimately error (e.g. a future column
    // added without a lenient accessor), that now propagates as a real
    // `Err` for the WHOLE call -- a loud in-band failure, never a silent
    // drop, per the supervisor's fallback ruling.
    handle.query_rows(sql, params, decode_policy_row).await
}

/// All active explicit peer policies in newest-first update order. Row decoding
/// mirrors `PolicyManager._row_to_policy`: malformed/non-list tags become
/// empty and unknown enum values degrade to the safe defaults.
pub async fn all_policies(handle: &DbHandle, now: i64) -> Result<Vec<PeerPolicy>> {
    let policies = query_policy_rows(
        handle,
        concat!(
            "SELECT peer_id, strategy, rebalance_mode, fee_ppm_target, tags, ",
            "updated_at, fee_multiplier_min, fee_multiplier_max, expires_at ",
            "FROM peer_policies ORDER BY updated_at DESC"
        ),
        vec![],
    )
    .await?;
    Ok(policies
        .into_iter()
        .filter(|policy| !policy.is_expired(now))
        .collect())
}

/// One active explicit policy row, or the same default policy Python returns
/// when no row exists or the row is expired. Database/actor failures still
/// propagate as errors.
pub async fn policy_for_peer(handle: &DbHandle, peer_id: &str, now: i64) -> Result<PeerPolicy> {
    let mut rows = query_policy_rows(
        handle,
        concat!(
            "SELECT peer_id, strategy, rebalance_mode, fee_ppm_target, tags, ",
            "updated_at, fee_multiplier_min, fee_multiplier_max, expires_at ",
            "FROM peer_policies WHERE peer_id = ?1"
        ),
        vec![SqlValue::Text(peer_id.to_string())],
    )
    .await?;
    Ok(match rows.pop() {
        Some(policy) if !policy.is_expired(now) => policy,
        _ => PeerPolicy::default_for(peer_id),
    })
}

/// Active policies carrying an exact tag. Filtering decoded tag arrays in
/// Rust avoids false positives from SQL substring matching of JSON text.
pub async fn policies_by_tag(handle: &DbHandle, tag: &str, now: i64) -> Result<Vec<PeerPolicy>> {
    Ok(all_policies(handle, now)
        .await?
        .into_iter()
        .filter(|policy| policy.has_tag(tag))
        .collect())
}

/// Active policies changed strictly after `since`, newest first.
pub async fn policy_changes_since(
    handle: &DbHandle,
    since: i64,
    now: i64,
) -> Result<Vec<PeerPolicy>> {
    let policies = query_policy_rows(
        handle,
        concat!(
            "SELECT peer_id, strategy, rebalance_mode, fee_ppm_target, tags, ",
            "updated_at, fee_multiplier_min, fee_multiplier_max, expires_at ",
            "FROM peer_policies WHERE updated_at > ?1 ORDER BY updated_at DESC"
        ),
        vec![SqlValue::Integer(since)],
    )
    .await?;
    Ok(policies
        .into_iter()
        .filter(|policy| !policy.is_expired(now))
        .collect())
}

/// Raw maximum `updated_at`, including expired rows, or zero for an empty
/// table. This matches `Database.get_last_policy_change_timestamp`.
pub async fn last_policy_change_timestamp(handle: &DbHandle) -> Result<i64> {
    handle
        .query_i64(
            "SELECT COALESCE(MAX(updated_at), 0) FROM peer_policies",
            vec![],
        )
        .await
}

/// One row of Python's `hot_channel_protection_overrides` table.
#[derive(Debug, Clone, PartialEq)]
pub struct HotChannelProtectionOverridePeer {
    pub peer_id: String,
    pub added_at: i64,
    pub note: Option<String>,
    pub min_depletion_trigger_pct: Option<f64>,
}

/// Port of `Database.list_hot_channel_protection_override_peers`.
pub async fn hot_channel_protection_override_peers(
    handle: &DbHandle,
) -> Result<Vec<HotChannelProtectionOverridePeer>> {
    handle
        .query_rows(
            concat!(
                "SELECT peer_id, added_at, note, min_depletion_trigger_pct ",
                "FROM hot_channel_protection_overrides ORDER BY added_at ASC"
            ),
            vec![],
            |row| {
                Ok(HotChannelProtectionOverridePeer {
                    peer_id: row.get(0)?,
                    added_at: row.get(1)?,
                    note: row.get(2)?,
                    min_depletion_trigger_pct: row.get(3)?,
                })
            },
        )
        .await
}

/// Windowed generic spend-ledger aggregates and measured evidence coverage.
#[derive(Debug, Clone, PartialEq)]
pub struct SpendLedgerAggregates {
    pub spent_24h_sats: i64,
    pub reserved_24h_sats: i64,
    pub spent_by_category: BTreeMap<String, i64>,
    pub reserved_by_category: BTreeMap<String, i64>,
    pub event_count_by_category: BTreeMap<String, i64>,
    pub active_reservation_count_by_category: BTreeMap<String, i64>,
    pub covered_hours: Option<f64>,
    pub coverage_status: String,
}

impl Default for SpendLedgerAggregates {
    fn default() -> Self {
        Self {
            spent_24h_sats: 0,
            reserved_24h_sats: 0,
            spent_by_category: BTreeMap::new(),
            reserved_by_category: BTreeMap::new(),
            event_count_by_category: BTreeMap::new(),
            active_reservation_count_by_category: BTreeMap::new(),
            covered_hours: None,
            coverage_status: "unknown".to_string(),
        }
    }
}

async fn category_totals(
    handle: &DbHandle,
    sql: &'static str,
    cutoff: i64,
) -> Result<BTreeMap<String, i64>> {
    let rows = handle
        .query_rows(sql, vec![SqlValue::Integer(cutoff)], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .await?;
    Ok(rows.into_iter().collect())
}

fn checked_spend_window(window_hours: i64, now: i64) -> Result<(i64, i64, i64)> {
    let window_hours = window_hours.max(1);
    let window_seconds = window_hours
        .checked_mul(3600)
        .context("window_hours exceeds the supported timestamp range")?;
    let cutoff = now
        .checked_sub(window_seconds)
        .context("window_hours produces an unsupported cutoff")?;
    Ok((window_hours, window_seconds, cutoff))
}

fn python_int_timestamp_cell(row: &Row) -> rusqlite::Result<Option<i64>> {
    let value = row.get::<_, SqlValue>(0)?;
    Ok(match value {
        SqlValue::Null => None,
        SqlValue::Integer(value) => Some(value),
        SqlValue::Real(value) if value.is_finite() => {
            let truncated = value.trunc();
            if truncated >= i64::MIN as f64 && truncated <= i64::MAX as f64 {
                Some(truncated as i64)
            } else {
                None
            }
        }
        SqlValue::Text(value) => value.trim().parse::<i64>().ok(),
        SqlValue::Blob(value) => std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok()),
        SqlValue::Real(_) => None,
    })
}

fn spend_coverage(
    event_earliest: Option<i64>,
    reservation_earliest: Option<i64>,
    now: i64,
    window_hours: i64,
    window_seconds: i64,
) -> (Option<f64>, String) {
    let earliest = [event_earliest, reservation_earliest]
        .into_iter()
        .flatten()
        .filter(|timestamp| *timestamp > 0)
        .min();
    let Some(earliest) = earliest else {
        return (None, "unknown".to_string());
    };
    if earliest > now {
        return (None, "unknown".to_string());
    }
    let span_seconds = now - earliest;
    if span_seconds >= window_seconds {
        return (Some(window_hours as f64), "complete".to_string());
    }
    (
        Some(py_round2(span_seconds as f64 / 3600.0)),
        "partial".to_string(),
    )
}

/// Port of `Database.get_spend_ledger_summary`'s aggregate and coverage
/// reads. `now` is injected so cutoff and coverage share one clock sample.
pub async fn spend_ledger_aggregates(
    handle: &DbHandle,
    window_hours: i64,
    now: i64,
) -> Result<SpendLedgerAggregates> {
    let (window_hours, window_seconds, cutoff) = checked_spend_window(window_hours, now)?;
    let spent_24h_sats = handle
        .query_i64(
            "SELECT COALESCE(SUM(amount_sats), 0) FROM spend_events WHERE timestamp >= ?1",
            vec![SqlValue::Integer(cutoff)],
        )
        .await?;
    let reserved_24h_sats = handle
        .query_i64(
            concat!(
                "SELECT COALESCE(SUM(reserved_sats), 0) FROM spend_reservations ",
                "WHERE status = 'active' AND reserved_at >= ?1"
            ),
            vec![SqlValue::Integer(cutoff)],
        )
        .await?;
    let spent_by_category = category_totals(
        handle,
        concat!(
            "SELECT category, COALESCE(SUM(amount_sats), 0) FROM spend_events ",
            "WHERE timestamp >= ?1 GROUP BY category"
        ),
        cutoff,
    )
    .await?;
    let reserved_by_category = category_totals(
        handle,
        concat!(
            "SELECT category, COALESCE(SUM(reserved_sats), 0) FROM spend_reservations ",
            "WHERE status = 'active' AND reserved_at >= ?1 GROUP BY category"
        ),
        cutoff,
    )
    .await?;
    let event_count_by_category = category_totals(
        handle,
        concat!(
            "SELECT category, COUNT(*) FROM spend_events ",
            "WHERE timestamp >= ?1 GROUP BY category"
        ),
        cutoff,
    )
    .await?;
    let active_reservation_count_by_category = category_totals(
        handle,
        concat!(
            "SELECT category, COUNT(*) FROM spend_reservations ",
            "WHERE status = 'active' AND reserved_at >= ?1 GROUP BY category"
        ),
        cutoff,
    )
    .await?;
    let event_earliest = handle
        .query_row(
            "SELECT MIN(timestamp) FROM spend_events",
            vec![],
            python_int_timestamp_cell,
        )
        .await?;
    let reservation_earliest = handle
        .query_row(
            "SELECT MIN(reserved_at) FROM spend_reservations",
            vec![],
            python_int_timestamp_cell,
        )
        .await?;
    let (covered_hours, coverage_status) = spend_coverage(
        event_earliest,
        reservation_earliest,
        now,
        window_hours,
        window_seconds,
    );

    Ok(SpendLedgerAggregates {
        spent_24h_sats,
        reserved_24h_sats,
        spent_by_category,
        reserved_by_category,
        event_count_by_category,
        active_reservation_count_by_category,
        covered_hours,
        coverage_status,
    })
}

/// One active row from Python's `spend_reservations` table.
#[derive(Debug, Clone, PartialEq)]
pub struct ActiveReservation {
    pub reservation_id: String,
    pub category: String,
    pub subcategory: Option<String>,
    pub reserved_sats: i64,
    pub reserved_at: i64,
    pub reference_id: Option<String>,
    pub channel_id: Option<String>,
    pub status: String,
    pub metadata_json: Option<String>,
}

/// Active reservations inside the requested window, oldest first. Both the
/// window and row limit use Python's minimum-one clamps.
pub async fn active_spend_reservations(
    handle: &DbHandle,
    window_hours: i64,
    limit: i64,
    now: i64,
) -> Result<Vec<ActiveReservation>> {
    let (_, _, cutoff) = checked_spend_window(window_hours, now)?;
    handle
        .query_rows(
            concat!(
                "SELECT reservation_id, category, subcategory, reserved_sats, reserved_at, ",
                "reference_id, channel_id, status, metadata_json FROM spend_reservations ",
                "WHERE status = 'active' AND reserved_at >= ?1 ",
                "ORDER BY reserved_at ASC LIMIT ?2"
            ),
            vec![SqlValue::Integer(cutoff), SqlValue::Integer(limit.max(1))],
            |row| {
                Ok(ActiveReservation {
                    reservation_id: row.get(0)?,
                    category: row.get(1)?,
                    subcategory: row.get(2)?,
                    reserved_sats: row.get(3)?,
                    reserved_at: row.get(4)?,
                    reference_id: row.get(5)?,
                    channel_id: row.get(6)?,
                    status: row.get(7)?,
                    metadata_json: row.get(8)?,
                })
            },
        )
        .await
}

/// One row from Python's `planner_candidates` table.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannerCandidateRow {
    pub peer_id: String,
    pub score: f64,
    pub source: String,
    pub last_evaluated: i64,
    pub capacity_recommendation_sats: Option<i64>,
    pub connect_successes: i64,
    pub connect_failures: i64,
    pub metadata_json: Option<String>,
}

impl PlannerCandidateRow {
    /// Python returns `dict(sqlite3.Row)`, including the raw
    /// `metadata_json` string rather than parsing it.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "peer_id": self.peer_id,
            "score": self.score,
            "source": self.source,
            "last_evaluated": self.last_evaluated,
            "capacity_recommendation_sats": self.capacity_recommendation_sats,
            "connect_successes": self.connect_successes,
            "connect_failures": self.connect_failures,
            "metadata_json": self.metadata_json,
        })
    }
}

fn decode_planner_candidate(row: &Row) -> rusqlite::Result<PlannerCandidateRow> {
    Ok(PlannerCandidateRow {
        peer_id: row.get(0)?,
        score: row.get(1)?,
        source: row.get(2)?,
        last_evaluated: row.get(3)?,
        capacity_recommendation_sats: row.get(4)?,
        connect_successes: row.get::<_, Option<i64>>(5)?.unwrap_or(0),
        connect_failures: row.get::<_, Option<i64>>(6)?.unwrap_or(0),
        metadata_json: row.get(7)?,
    })
}

/// Port of `Database.get_planner_candidates`: inclusive score floor,
/// optional truthy source filter, score-descending order, then LIMIT.
pub async fn planner_candidates(
    handle: &DbHandle,
    min_score: f64,
    source: Option<&str>,
    limit: i64,
) -> Result<Vec<PlannerCandidateRow>> {
    match source.filter(|value| !value.is_empty()) {
        Some(source) => {
            handle
                .query_rows(
                    concat!(
                        "SELECT peer_id, score, source, last_evaluated, ",
                        "capacity_recommendation_sats, connect_successes, ",
                        "connect_failures, metadata_json FROM planner_candidates ",
                        "WHERE score >= ?1 AND source = ?2 ",
                        "ORDER BY score DESC LIMIT ?3"
                    ),
                    vec![
                        SqlValue::Real(min_score),
                        SqlValue::Text(source.to_string()),
                        SqlValue::Integer(limit.max(1)),
                    ],
                    decode_planner_candidate,
                )
                .await
        }
        None => {
            handle
                .query_rows(
                    concat!(
                        "SELECT peer_id, score, source, last_evaluated, ",
                        "capacity_recommendation_sats, connect_successes, ",
                        "connect_failures, metadata_json FROM planner_candidates ",
                        "WHERE score >= ?1 ORDER BY score DESC LIMIT ?2"
                    ),
                    vec![SqlValue::Real(min_score), SqlValue::Integer(limit.max(1))],
                    decode_planner_candidate,
                )
                .await
        }
    }
}

/// One row from Python's `planner_actions` table.
#[derive(Debug, Clone, PartialEq)]
pub struct PlannerActionRow {
    pub id: i64,
    pub action_type: String,
    pub peer_id: String,
    pub channel_id: Option<String>,
    pub amount_sats: Option<i64>,
    pub estimated_cost_sats: Option<i64>,
    pub actual_cost_sats: Option<i64>,
    pub status: String,
    pub created_at: i64,
    pub completed_at: Option<i64>,
    pub reason: Option<String>,
    pub metadata_json: Option<String>,
}

impl PlannerActionRow {
    /// Python returns the SQLite row as a dictionary without decoding its
    /// optional metadata JSON string.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "action_type": self.action_type,
            "peer_id": self.peer_id,
            "channel_id": self.channel_id,
            "amount_sats": self.amount_sats,
            "estimated_cost_sats": self.estimated_cost_sats,
            "actual_cost_sats": self.actual_cost_sats,
            "status": self.status,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "reason": self.reason,
            "metadata_json": self.metadata_json,
        })
    }
}

fn decode_planner_action(row: &Row) -> rusqlite::Result<PlannerActionRow> {
    Ok(PlannerActionRow {
        id: row.get(0)?,
        action_type: row.get(1)?,
        peer_id: row.get(2)?,
        channel_id: row.get(3)?,
        amount_sats: row.get(4)?,
        estimated_cost_sats: row.get(5)?,
        actual_cost_sats: row.get(6)?,
        status: row.get(7)?,
        created_at: row.get(8)?,
        completed_at: row.get(9)?,
        reason: row.get(10)?,
        metadata_json: row.get(11)?,
    })
}

/// Port of `Database.get_planner_actions`: optional truthy status
/// filter, newest-created first, then LIMIT.
pub async fn planner_actions(
    handle: &DbHandle,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<PlannerActionRow>> {
    match status.filter(|value| !value.is_empty()) {
        Some(status) => {
            handle
                .query_rows(
                    concat!(
                        "SELECT id, action_type, peer_id, channel_id, amount_sats, ",
                        "estimated_cost_sats, actual_cost_sats, status, created_at, ",
                        "completed_at, reason, metadata_json FROM planner_actions ",
                        "WHERE status = ?1 ORDER BY created_at DESC LIMIT ?2"
                    ),
                    vec![
                        SqlValue::Text(status.to_string()),
                        SqlValue::Integer(limit.max(1)),
                    ],
                    decode_planner_action,
                )
                .await
        }
        None => {
            handle
                .query_rows(
                    concat!(
                        "SELECT id, action_type, peer_id, channel_id, amount_sats, ",
                        "estimated_cost_sats, actual_cost_sats, status, created_at, ",
                        "completed_at, reason, metadata_json FROM planner_actions ",
                        "ORDER BY created_at DESC LIMIT ?1"
                    ),
                    vec![SqlValue::Integer(limit.max(1))],
                    decode_planner_action,
                )
                .await
        }
    }
}

/// Port of `Database.get_lifetime_stats` (modules/database.py:6018-6087).
///
/// Deliberately SEVEN separate statements, mirroring Python's own
/// non-atomic composition (the Python method itself never wraps these in
/// one transaction either) -- EXCEPT the two `lifetime_aggregates` pruned
/// columns, which Python reads in one fetchone (database.py:6043-6048)
/// and are therefore one combined statement here too (audit low #12: the
/// split read could tear against the prune job's transaction). See
/// [`DbHandle::query_row`] for the contrasting fully-atomic case
/// (`closed_channels_summary`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifetimeStats {
    pub total_revenue_msat: i64,
    pub total_rebalance_cost_sats: i64,
    pub total_opening_cost_sats: i64,
    pub total_closure_cost_sats: i64,
    pub total_forwards: i64,
}

/// `now` is the caller's current Unix time (seconds) -- passed in rather
/// than read internally so the "exclude today" boundary-day fix
/// (`today_start = (now // 86400) * 86400`, matching Python's own comment
/// at database.py:6035-6036) is deterministic and unit-testable.
pub async fn lifetime_stats(handle: &DbHandle, now: i64) -> Result<LifetimeStats> {
    let today_start = (now / 86400) * 86400;

    // ONE statement for both pruned columns, matching Python's single
    // fetchone (database.py:6043-6048) — audit low #12: two separate
    // round-trips could tear against the Python prune job's transaction
    // (a pre-prune revenue paired with a post-prune count, a combination
    // Python can never report). The other seven reads below legitimately
    // stay separate: Python's own composition is non-atomic across THEM.
    let (pruned_revenue_msat, pruned_forward_count) = handle
        .query_row(
            "SELECT COALESCE((SELECT pruned_revenue_msat FROM lifetime_aggregates WHERE id = 1), 0), \
             COALESCE((SELECT pruned_forward_count FROM lifetime_aggregates WHERE id = 1), 0)",
            vec![],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .await?;
    // Current revenue from forwards table (msat) -- unconditional sum, no
    // date filter (matches Python: this table only retains recent raw rows
    // by construction of the pruning job, not by this query).
    let current_revenue_msat = handle
        .query_i64("SELECT COALESCE(SUM(fee_msat), 0) FROM forwards", vec![])
        .await?;
    // Rolled-up revenue, excluding today to avoid double-counting with
    // `forwards` on the boundary day.
    let rollup_revenue_msat = handle
        .query_i64(
            "SELECT COALESCE(SUM(total_fee_msat), 0) FROM daily_forwarding_stats WHERE date < ?1",
            vec![SqlValue::Integer(today_start)],
        )
        .await?;
    let total_revenue_msat = pruned_revenue_msat + rollup_revenue_msat + current_revenue_msat;

    let total_rebalance_cost_sats = handle
        .query_i64(
            "SELECT COALESCE(SUM(cost_sats), 0) FROM rebalance_costs",
            vec![],
        )
        .await?;
    let total_opening_cost_sats = handle
        .query_i64(
            "SELECT COALESCE(SUM(open_cost_sats), 0) FROM channel_costs",
            vec![],
        )
        .await?;
    let total_closure_cost_sats = handle
        .query_i64(
            "SELECT COALESCE(SUM(total_closure_cost_sats), 0) FROM channel_closure_costs",
            vec![],
        )
        .await?;

    let current_forwards = handle
        .query_i64("SELECT COUNT(*) FROM forwards", vec![])
        .await?;
    let rollup_forwards = handle
        .query_i64(
            "SELECT COALESCE(SUM(forward_count), 0) FROM daily_forwarding_stats WHERE date < ?1",
            vec![SqlValue::Integer(today_start)],
        )
        .await?;
    let total_forwards = pruned_forward_count + rollup_forwards + current_forwards;

    Ok(LifetimeStats {
        total_revenue_msat,
        total_rebalance_cost_sats,
        total_opening_cost_sats,
        total_closure_cost_sats,
        total_forwards,
    })
}

/// Port of `Database.get_closed_channels_summary` (database.py:6495-6526):
/// one 9-column atomic `SELECT`, ported as ONE `query_row` call (not nine
/// `query_i64` calls) to preserve that atomicity under a concurrently
/// written production DB -- see [`DbHandle::query_row`]'s doc comment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClosedChannelsSummary {
    pub channel_count: i64,
    pub total_capacity: i64,
    pub total_open_costs: i64,
    pub total_closure_costs: i64,
    pub total_revenue: i64,
    pub total_rebalance_costs: i64,
    pub total_forwards: i64,
    pub total_net_pnl: i64,
    pub avg_days_open: f64,
}

const CLOSED_CHANNELS_SUMMARY_SQL: &str = "SELECT
        COUNT(*) as channel_count,
        COALESCE(SUM(capacity_sats), 0) as total_capacity,
        COALESCE(SUM(open_cost_sats), 0) as total_open_costs,
        COALESCE(SUM(closure_cost_sats), 0) as total_closure_costs,
        COALESCE(SUM(total_revenue_sats), 0) as total_revenue,
        COALESCE(SUM(total_rebalance_cost_sats), 0) as total_rebalance_costs,
        COALESCE(SUM(forward_count), 0) as total_forwards,
        COALESCE(SUM(net_pnl_sats), 0) as total_net_pnl,
        COALESCE(AVG(days_open), 0) as avg_days_open
    FROM closed_channels";

pub async fn closed_channels_summary(handle: &DbHandle) -> Result<ClosedChannelsSummary> {
    handle
        .query_row(CLOSED_CHANNELS_SUMMARY_SQL, vec![], |r| {
            Ok(ClosedChannelsSummary {
                channel_count: r.get(0)?,
                total_capacity: r.get(1)?,
                total_open_costs: r.get(2)?,
                total_closure_costs: r.get(3)?,
                total_revenue: r.get(4)?,
                total_rebalance_costs: r.get(5)?,
                total_forwards: r.get(6)?,
                total_net_pnl: r.get(7)?,
                avg_days_open: r.get(8)?,
            })
        })
        .await
}

/// Port of `Database.get_closure_costs_since` (database.py:6353-6369).
pub async fn closure_costs_since(handle: &DbHandle, since_timestamp: i64) -> Result<i64> {
    handle
        .query_i64(
            "SELECT COALESCE(SUM(total_closure_cost_sats), 0) FROM channel_closure_costs WHERE closed_at >= ?1",
            vec![SqlValue::Integer(since_timestamp)],
        )
        .await
}

/// Port of `Database.get_total_closure_costs` (database.py:6319-6332).
pub async fn total_closure_costs(handle: &DbHandle) -> Result<i64> {
    handle
        .query_i64(
            "SELECT COALESCE(SUM(total_closure_cost_sats), 0) FROM channel_closure_costs",
            vec![],
        )
        .await
}

/// 24h/7d/30d/total closure-cost windows, port of `revenue-report costs`'s
/// composition in `cl-revenue-ops.py` (`get_closure_costs_since` x3 +
/// `get_total_closure_costs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosureCostWindows {
    pub last_24h_sats: i64,
    pub last_7d_sats: i64,
    pub last_30d_sats: i64,
    pub total_sats: i64,
}

pub async fn closure_costs_windows(handle: &DbHandle, now: i64) -> Result<ClosureCostWindows> {
    let last_24h_sats = closure_costs_since(handle, now - 86400).await?;
    let last_7d_sats = closure_costs_since(handle, now - 7 * 86400).await?;
    let last_30d_sats = closure_costs_since(handle, now - 30 * 86400).await?;
    let total_sats = total_closure_costs(handle).await?;
    Ok(ClosureCostWindows {
        last_24h_sats,
        last_7d_sats,
        last_30d_sats,
        total_sats,
    })
}

/// Port of `Database.get_total_routing_revenue` (database.py:2921-2960).
/// Returns msat (conversion to sats happens at the reporting boundary, same
/// as Python).
pub async fn total_routing_revenue_msat(
    handle: &DbHandle,
    since_timestamp: i64,
    now: i64,
) -> Result<i64> {
    let since_day = (since_timestamp / 86400) * 86400;
    let today_start = (now / 86400) * 86400;
    handle
        .query_i64(
            "SELECT
                (SELECT COALESCE(SUM(fee_msat), 0) FROM forwards WHERE timestamp >= ?1) +
                (SELECT COALESCE(SUM(total_fee_msat), 0) FROM daily_forwarding_stats WHERE date >= ?2 AND date < ?3)",
            vec![
                SqlValue::Integer(since_timestamp),
                SqlValue::Integer(since_day),
                SqlValue::Integer(today_start),
            ],
        )
        .await
}

/// Port of `Database.get_total_volume_since` (database.py:5601-5626).
/// Converts msat -> sats via floor (never overstate spendable volume),
/// matching Python's own `base_to_sats_floor` call at the DB layer.
pub async fn total_volume_sats_since(
    handle: &DbHandle,
    since_timestamp: i64,
    now: i64,
) -> Result<i64> {
    let since_day = (since_timestamp / 86400) * 86400;
    let today_start = (now / 86400) * 86400;
    let total_volume_msat = handle
        .query_i64(
            "SELECT
                (SELECT COALESCE(SUM(out_msat), 0) FROM forwards WHERE timestamp >= ?1) +
                (SELECT COALESCE(SUM(total_out_msat), 0) FROM daily_forwarding_stats WHERE date >= ?2 AND date < ?3)",
            vec![
                SqlValue::Integer(since_timestamp),
                SqlValue::Integer(since_day),
                SqlValue::Integer(today_start),
            ],
        )
        .await?;
    Ok(base_to_sats_floor(total_volume_msat.max(0) as u64) as i64)
}

/// Port of `Database.get_total_forward_count_since` (database.py:5652-5671).
pub async fn total_forward_count_since(
    handle: &DbHandle,
    since_timestamp: i64,
    now: i64,
) -> Result<i64> {
    let since_day = (since_timestamp / 86400) * 86400;
    let today_start = (now / 86400) * 86400;
    handle
        .query_i64(
            "SELECT
                (SELECT COUNT(*) FROM forwards WHERE timestamp >= ?1) +
                (SELECT COALESCE(SUM(forward_count), 0) FROM daily_forwarding_stats WHERE date >= ?2 AND date < ?3)",
            vec![
                SqlValue::Integer(since_timestamp),
                SqlValue::Integer(since_day),
                SqlValue::Integer(today_start),
            ],
        )
        .await
}

/// Port of `Database.get_total_rebalance_fees` (database.py:2814-2839).
/// Schema here already carries `rebalance_costs.cost_msat` (see
/// `fixtures/schema.sql`), so Python's `sqlite3.OperationalError` legacy
/// fallback (for a pre-migration schema without that column) has no
/// equivalent path in this port -- there's nothing to fall back from.
/// Converts msat -> sats via ceil, matching Python's own
/// `base_to_sats_ceil` call at the DB layer.
pub async fn total_rebalance_fees_since(handle: &DbHandle, since_timestamp: i64) -> Result<i64> {
    let total_fees_msat = handle
        .query_i64(
            "SELECT COALESCE(SUM(COALESCE(cost_msat, cost_sats * 1000)), 0) FROM rebalance_costs WHERE timestamp >= ?1",
            vec![SqlValue::Integer(since_timestamp)],
        )
        .await?;
    Ok(base_to_sats_ceil(total_fees_msat.max(0) as u64) as i64)
}

/// Port of `ProfitabilityAnalyzer.get_pnl_summary`
/// (profitability_analyzer.py:1441-1498), composed entirely from the
/// `database.py`-ported functions above -- no live RPC, no
/// `policy_manager`. `window_days` is clamped to a minimum of 1 here
/// (Python's own internal clamp: "BUG FIX: Validate window_days..."); the
/// separate upper clamp (`min(window_days, 365)`) lives in the
/// `revenue-dashboard` RPC handler itself in Python (cl-revenue-ops.py),
/// not in `get_pnl_summary` -- ported the same way, in the RPC-layer
/// `parse_window_days` (`crates/revops/src/rpc_dashboard.rs`), not here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PnlSummary {
    pub window_days: i64,
    pub gross_revenue_sats: i64,
    pub opex_sats: i64,
    pub rebalance_cost_sats: i64,
    pub closure_cost_sats: i64,
    pub net_profit_sats: i64,
    pub operating_margin_pct: f64,
    pub volume_sats: i64,
    pub forward_count: i64,
}

pub async fn pnl_summary(handle: &DbHandle, window_days_in: i64, now: i64) -> Result<PnlSummary> {
    let window_days = window_days_in.max(1);
    let since_timestamp = now - window_days * 86400;

    let gross_revenue_msat = total_routing_revenue_msat(handle, since_timestamp, now).await?;
    let gross_revenue_sats = if gross_revenue_msat > 0 {
        base_to_sats_ceil(gross_revenue_msat as u64) as i64
    } else {
        0
    };
    let volume_sats = total_volume_sats_since(handle, since_timestamp, now).await?;
    let forward_count = total_forward_count_since(handle, since_timestamp, now).await?;
    let rebalance_cost_sats = total_rebalance_fees_since(handle, since_timestamp).await?;
    let closure_cost_sats = closure_costs_since(handle, since_timestamp).await?;
    let opex_sats = rebalance_cost_sats + closure_cost_sats;
    let net_profit_sats = gross_revenue_sats - opex_sats;
    let operating_margin_pct = if gross_revenue_sats > 0 {
        py_round2((net_profit_sats as f64 / gross_revenue_sats as f64) * 100.0)
    } else if opex_sats == 0 {
        0.0
    } else {
        -100.0
    };

    Ok(PnlSummary {
        window_days,
        gross_revenue_sats,
        opex_sats,
        rebalance_cost_sats,
        closure_cost_sats,
        net_profit_sats,
        operating_margin_pct,
        volume_sats,
        forward_count,
    })
}

// ---------------------------------------------------------------------------
// Task 67b: per-channel profitability inputs
// ---------------------------------------------------------------------------

/// Per-channel revenue, msat-native, split by ATTRIBUTION SIDE.
///
/// The EXIT channel earns the fee (`fees_earned_msat`). The `sourced_*`
/// fields are ENTRY-side attribution, used for protection and valuation
/// only — summing them into fleet revenue double-counts every forward,
/// because each forward appears on exactly one channel's earned side and
/// exactly one channel's sourced side.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PerChannelRevenue {
    pub fees_earned_msat: i64,
    pub volume_routed_msat: i64,
    pub forward_count: i64,
    pub sourced_volume_msat: i64,
    pub sourced_fee_contribution_msat: i64,
    pub sourced_forward_count: i64,
}

/// Per-channel costs. The 30-day rebalance figure is kept SEPARATE from
/// the all-time one: marginal ROI is defined over ongoing rebalance cost
/// with no sunk open cost, so conflating them silently changes every
/// winner/loser verdict.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PerChannelCosts {
    pub peer_id: String,
    pub open_cost_sats: i64,
    pub capacity_sats: i64,
    pub opened_at: i64,
    pub rebalance_cost_sats: i64,
    pub rebalance_cost_30d_sats: i64,
}

/// Aggregate `forwards` per channel, from `since` (inclusive; 0 = all).
pub async fn per_channel_revenue(
    handle: &DbHandle,
    since: i64,
) -> Result<HashMap<String, PerChannelRevenue>> {
    let mut out: HashMap<String, PerChannelRevenue> = HashMap::new();

    // EXIT side: this channel earned the fee.
    let earned = handle
        .query_rows(
            "SELECT out_channel, COALESCE(SUM(fee_msat),0), COALESCE(SUM(out_msat),0), COUNT(*)
             FROM forwards
             WHERE out_channel IS NOT NULL AND timestamp >= ?1
             GROUP BY out_channel",
            vec![SqlValue::Integer(since)],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .await?;
    for (scid, fees, volume, count) in earned {
        let e = out.entry(scid).or_default();
        e.fees_earned_msat = fees;
        e.volume_routed_msat = volume;
        e.forward_count = count;
    }

    // ENTRY side: attribution only, NEVER summed into fleet revenue.
    let sourced = handle
        .query_rows(
            "SELECT in_channel, COALESCE(SUM(fee_msat),0), COALESCE(SUM(in_msat),0), COUNT(*)
             FROM forwards
             WHERE in_channel IS NOT NULL AND timestamp >= ?1
             GROUP BY in_channel",
            vec![SqlValue::Integer(since)],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .await?;
    for (scid, fees, volume, count) in sourced {
        let e = out.entry(scid).or_default();
        e.sourced_fee_contribution_msat = fees;
        e.sourced_volume_msat = volume;
        e.sourced_forward_count = count;
    }
    Ok(out)
}

/// Per-channel costs. `window_since` bounds the 30-day rebalance figure.
pub async fn per_channel_costs(
    handle: &DbHandle,
    window_since: i64,
) -> Result<HashMap<String, PerChannelCosts>> {
    let mut out: HashMap<String, PerChannelCosts> = HashMap::new();

    let opens = handle
        .query_rows(
            "SELECT channel_id, COALESCE(peer_id,''), COALESCE(open_cost_sats,0),
                    COALESCE(capacity_sats,0), COALESCE(opened_at,0)
             FROM channel_costs",
            vec![],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .await?;
    for (scid, peer_id, open_cost, capacity, opened_at) in opens {
        let e = out.entry(scid).or_default();
        e.peer_id = peer_id;
        e.open_cost_sats = open_cost;
        e.capacity_sats = capacity;
        e.opened_at = opened_at;
    }

    let rebals = handle
        .query_rows(
            "SELECT channel_id, COALESCE(peer_id,''), COALESCE(SUM(cost_sats),0),
                    COALESCE(SUM(CASE WHEN timestamp >= ?1 THEN cost_sats ELSE 0 END),0)
             FROM rebalance_costs
             WHERE channel_id IS NOT NULL
             GROUP BY channel_id",
            vec![SqlValue::Integer(window_since)],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )
        .await?;
    for (scid, peer_id, total, windowed) in rebals {
        let e = out.entry(scid).or_default();
        if e.peer_id.is_empty() {
            e.peer_id = peer_id;
        }
        e.rebalance_cost_sats = total;
        e.rebalance_cost_30d_sats = windowed;
    }
    Ok(out)
}
