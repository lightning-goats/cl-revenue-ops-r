//! Task 67: durable stores for the analytics runtime owners.
//!
//! The pure kernels live in `revops-analytics` (flow/kalman/temporal/
//! profitability) and are frozen. This module owns only the PERSISTENCE
//! those kernels' inputs and outputs need, in the same
//! canonical-column-refusal style as `loop_health`: an unshipped table is
//! never migrated, because a migration would have to invent values for
//! columns no prior writer produced.
//!
//! Two retention classes, deliberately different:
//!
//! - **Current state** (`rust_channel_flow_states`, `rust_kalman_state`,
//!   `rust_temporal_profiles`): one row per short-channel-id, upsert
//!   replaces. Bounded by the channel count, so no sweep is needed.
//! - **Time series** (`rust_financial_snapshots`): append-only, one row
//!   per snapshot (Python's cadence is daily). Classified Class E
//!   (never-prune), NOT windowed: it is financial history and the ROC/TLV
//!   trend basis, ~365 rows/year, and a silently truncated series would
//!   change what the trend surfaces mean.
//!
//! `rust_channel_flow_states` carries `boot_id` so an operator can tell
//! WHICH process last wrote a channel's state -- the same provenance
//! discipline Task 67 applied to loop health.

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection};

/// One channel's derived flow state (the kernel's `FlowMetrics`
/// projection that downstream surfaces actually read).
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelFlowStateRow {
    pub scid: String,
    pub peer_id: String,
    pub flow_state: String,
    pub balance_position: String,
    pub flow_ratio: f64,
    pub velocity: f64,
    pub confidence: f64,
    pub forward_count: i64,
    pub updated_at: i64,
    /// The process that wrote this row.
    pub boot_id: String,
}

/// One financial snapshot (py `financial_snapshots`,
/// database.py:1030 / `_take_financial_snapshot`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinancialSnapshotRow {
    pub taken_at: i64,
    pub local_balance_sats: i64,
    pub remote_balance_sats: i64,
    pub onchain_sats: i64,
    pub capacity_sats: i64,
    pub revenue_accumulated_sats: i64,
    pub rebalance_cost_accumulated_sats: i64,
    pub channel_count: i64,
}

const FLOW_STATE_COLUMNS: [&str; 10] = [
    "scid",
    "peer_id",
    "flow_state",
    "balance_position",
    "flow_ratio",
    "velocity",
    "confidence",
    "forward_count",
    "updated_at",
    "boot_id",
];

const SNAPSHOT_COLUMNS: [&str; 9] = [
    "id",
    "taken_at",
    "local_balance_sats",
    "remote_balance_sats",
    "onchain_sats",
    "capacity_sats",
    "revenue_accumulated_sats",
    "rebalance_cost_accumulated_sats",
    "channel_count",
];

pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS rust_channel_flow_states (
            scid TEXT PRIMARY KEY,
            peer_id TEXT NOT NULL,
            flow_state TEXT NOT NULL,
            balance_position TEXT NOT NULL,
            flow_ratio REAL NOT NULL,
            velocity REAL NOT NULL,
            confidence REAL NOT NULL,
            forward_count INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            boot_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rust_kalman_state (
            scid TEXT PRIMARY KEY,
            state_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rust_temporal_profiles (
            scid TEXT PRIMARY KEY,
            profile_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rust_financial_snapshots (
            id INTEGER PRIMARY KEY,
            taken_at INTEGER NOT NULL,
            local_balance_sats INTEGER NOT NULL,
            remote_balance_sats INTEGER NOT NULL,
            onchain_sats INTEGER NOT NULL,
            capacity_sats INTEGER NOT NULL,
            revenue_accumulated_sats INTEGER NOT NULL,
            rebalance_cost_accumulated_sats INTEGER NOT NULL,
            channel_count INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_rust_financial_snapshots_taken_at
            ON rust_financial_snapshots(taken_at);",
    )
    .context("init analytics schema")?;
    assert_canonical(conn, "rust_channel_flow_states", &FLOW_STATE_COLUMNS)?;
    assert_canonical(conn, "rust_financial_snapshots", &SNAPSHOT_COLUMNS)?;
    Ok(())
}

/// Same refusal posture as `loop_health::init_schema`: a noncanonical
/// shape is an error, never a silent migration -- there is no honest
/// value to backfill for a column no prior writer produced.
fn assert_canonical(conn: &Connection, table: &str, expected: &[&str]) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if names.iter().map(String::as_str).collect::<Vec<_>>() != expected {
        bail!(
            "noncanonical {table} schema: expected {expected:?}, found {names:?}; this \
             unshipped table cannot be migrated without fabricating analytics evidence"
        );
    }
    Ok(())
}

/// One day's aggregated flow for one channel. Index 0 in the vector is
/// the most recent 24h, index 1 the 24h before that, and so on -- the
/// order the frozen EMA kernel requires.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlowDailyBucket {
    pub in_sats: i64,
    pub out_sats: i64,
    pub count: i64,
    pub last_ts: i64,
}

/// Port of py `database.get_daily_flow_buckets` (database.py:5008) over
/// the Rust plugin's OWN `ingested_forwards` table.
///
/// Task 71 / F71-R16: the flow-analysis pass's daily/EMA buckets had no
/// Rust producer at all, so `FlowDeps::history` could only ever be
/// hand-built in a test. Without this, a wired pass would hand every
/// channel an EMPTY bucket list -- which the frozen kernels read as
/// "no observations", collapsing the whole fleet to `confidence = 0.0`
/// and the balance-ratio fallback. That is a fabricated-empty-evidence
/// failure, not a missing feature.
///
/// Three py behaviours that look incidental and are not:
///
/// * Bins are ROLLING age-in-days from `now`
///   (`CAST((now - timestamp) / 86400 AS INTEGER)`), not calendar days.
/// * A channel seen even once gets its FULL `window_days` vector, zero-
///   padded. The decay and volatility kernels short-circuit on
///   `len() < 3`, so the padding changes their output.
/// * A channel with no forwards in the window is ABSENT from the map --
///   never a zero-filled vector. Absent means "no evidence"; a zero
///   bucket means "measured no flow".
pub fn daily_flow_buckets(
    conn: &Connection,
    now: i64,
    window_days: i64,
) -> Result<std::collections::BTreeMap<String, Vec<FlowDailyBucket>>> {
    let mut out: std::collections::BTreeMap<String, Vec<FlowDailyBucket>> =
        std::collections::BTreeMap::new();
    if window_days <= 0 {
        return Ok(out);
    }
    let start_time = now - window_days * 86_400;

    // Each forward contributes to TWO channels: `in_channel` receives, and
    // `out_channel` sends. py UNIONs the two projections then regroups, so
    // a channel that is both sides of the same forward sums both.
    let mut stmt = conn
        .prepare(
            "WITH flow AS (
                 SELECT in_channel AS channel_id,
                        CAST((?1 - timestamp) / 86400 AS INTEGER) AS age_days,
                        SUM(in_msat) AS in_msat, 0 AS out_msat,
                        COUNT(*) AS cnt, MAX(timestamp) AS last_ts
                 FROM ingested_forwards WHERE timestamp >= ?2
                 GROUP BY in_channel, age_days
                 UNION ALL
                 SELECT out_channel AS channel_id,
                        CAST((?1 - timestamp) / 86400 AS INTEGER) AS age_days,
                        0 AS in_msat, SUM(out_msat) AS out_msat,
                        COUNT(*) AS cnt, MAX(timestamp) AS last_ts
                 FROM ingested_forwards WHERE timestamp >= ?2
                 GROUP BY out_channel, age_days
             )
             SELECT channel_id, age_days,
                    SUM(in_msat), SUM(out_msat), SUM(cnt), MAX(last_ts)
             FROM flow
             WHERE age_days >= 0 AND age_days < ?3
             GROUP BY channel_id, age_days",
        )
        .context("prepare daily flow bucket query")?;

    let rows = stmt
        .query_map(params![now, start_time, window_days], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("read daily flow buckets")?;

    for (channel_id, age_days, in_msat, out_msat, count, last_ts) in rows {
        if channel_id.is_empty() {
            continue;
        }
        let Ok(index) = usize::try_from(age_days) else {
            continue;
        };
        if age_days >= window_days {
            continue;
        }
        let buckets = out
            .entry(channel_id)
            .or_insert_with(|| vec![FlowDailyBucket::default(); window_days as usize]);
        let bucket = &mut buckets[index];
        // py `base_to_sats_floor` per bucket per direction.
        bucket.in_sats += in_msat.div_euclid(1_000);
        bucket.out_sats += out_msat.div_euclid(1_000);
        bucket.count += count;
        bucket.last_ts = bucket.last_ts.max(last_ts);
    }
    Ok(out)
}

/// Upsert one channel's flow state (current-state semantics).
pub fn upsert_channel_flow_state(conn: &Connection, row: &ChannelFlowStateRow) -> Result<()> {
    conn.execute(
        "INSERT INTO rust_channel_flow_states
             (scid, peer_id, flow_state, balance_position, flow_ratio, velocity,
              confidence, forward_count, updated_at, boot_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
         ON CONFLICT(scid) DO UPDATE SET
             peer_id = excluded.peer_id,
             flow_state = excluded.flow_state,
             balance_position = excluded.balance_position,
             flow_ratio = excluded.flow_ratio,
             velocity = excluded.velocity,
             confidence = excluded.confidence,
             forward_count = excluded.forward_count,
             updated_at = excluded.updated_at,
             boot_id = excluded.boot_id",
        params![
            row.scid,
            row.peer_id,
            row.flow_state,
            row.balance_position,
            row.flow_ratio,
            row.velocity,
            row.confidence,
            row.forward_count,
            row.updated_at,
            row.boot_id
        ],
    )
    .context("upsert channel flow state")?;
    Ok(())
}

/// Persist ONE complete flow pass — every derived state row and every
/// updated Kalman state — in a single transaction.
///
/// Task 71 / F71-R18. The per-row alternative (N `upsert_channel_flow_state`
/// plus N `upsert_kalman_state` calls) is not merely slower, it is
/// undetectably wrong on partial failure. `rust_channel_flow_states`
/// carries a `boot_id`, so a half-written state set is at least
/// *visible*; `rust_kalman_state` carries NO boot_id and no provenance at
/// all, so a half-written filter set is indistinguishable from a complete
/// one. The next pass would then resume some channels from this boot's
/// filter and others from a previous boot's, silently and permanently.
///
/// All-or-nothing removes the question: either the whole pass is durable
/// or none of it is, and the failed pass fails the loop generation.
pub fn persist_flow_pass(
    conn: &mut Connection,
    states: &[ChannelFlowStateRow],
    kalman: &[(String, serde_json::Value)],
    updated_at: i64,
) -> Result<usize> {
    let tx = conn.transaction().context("open flow pass transaction")?;
    for row in states {
        upsert_channel_flow_state(&tx, row)
            .with_context(|| format!("flow pass: persist state for {}", row.scid))?;
    }
    for (scid, state) in kalman {
        upsert_kalman_state(&tx, scid, state, updated_at)
            .with_context(|| format!("flow pass: persist kalman state for {scid}"))?;
    }
    tx.commit().context("commit flow pass transaction")?;
    Ok(states.len())
}

/// All channel flow states, scid-ordered.
pub fn channel_flow_states(conn: &Connection) -> Result<Vec<ChannelFlowStateRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT scid, peer_id, flow_state, balance_position, flow_ratio, velocity,
                    confidence, forward_count, updated_at, boot_id
             FROM rust_channel_flow_states ORDER BY scid ASC",
        )
        .context("prepare channel flow states")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ChannelFlowStateRow {
                scid: row.get(0)?,
                peer_id: row.get(1)?,
                flow_state: row.get(2)?,
                balance_position: row.get(3)?,
                flow_ratio: row.get(4)?,
                velocity: row.get(5)?,
                confidence: row.get(6)?,
                forward_count: row.get(7)?,
                updated_at: row.get(8)?,
                boot_id: row.get(9)?,
            })
        })
        .context("query channel flow states")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("read channel flow states")?;
    Ok(rows)
}

/// One channel's flow state, if any.
pub fn channel_flow_state(conn: &Connection, scid: &str) -> Result<Option<ChannelFlowStateRow>> {
    Ok(channel_flow_states(conn)?
        .into_iter()
        .find(|row| row.scid == scid))
}

fn upsert_json(
    conn: &Connection,
    table: &str,
    column: &str,
    scid: &str,
    payload: &serde_json::Value,
    updated_at: i64,
) -> Result<()> {
    let text = serde_json::to_string(payload).context("serialize analytics payload")?;
    conn.execute(
        &format!(
            "INSERT INTO {table} (scid, {column}, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(scid) DO UPDATE SET
                 {column} = excluded.{column}, updated_at = excluded.updated_at"
        ),
        params![scid, text, updated_at],
    )
    .with_context(|| format!("upsert {table}"))?;
    Ok(())
}

fn list_json(
    conn: &Connection,
    table: &str,
    column: &str,
) -> Result<Vec<(String, serde_json::Value, i64)>> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT scid, {column}, updated_at FROM {table} ORDER BY scid ASC"
        ))
        .with_context(|| format!("prepare {table}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .with_context(|| format!("query {table}"))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("read {table}"))?;
    rows.into_iter()
        .map(|(scid, text, updated_at)| {
            Ok((
                scid,
                serde_json::from_str(&text).context("parse analytics payload")?,
                updated_at,
            ))
        })
        .collect()
}

pub fn upsert_kalman_state(
    conn: &Connection,
    scid: &str,
    state: &serde_json::Value,
    updated_at: i64,
) -> Result<()> {
    upsert_json(
        conn,
        "rust_kalman_state",
        "state_json",
        scid,
        state,
        updated_at,
    )
}

pub fn kalman_states(conn: &Connection) -> Result<Vec<(String, serde_json::Value, i64)>> {
    list_json(conn, "rust_kalman_state", "state_json")
}

pub fn upsert_temporal_profile(
    conn: &Connection,
    scid: &str,
    profile: &serde_json::Value,
    updated_at: i64,
) -> Result<()> {
    upsert_json(
        conn,
        "rust_temporal_profiles",
        "profile_json",
        scid,
        profile,
        updated_at,
    )
}

pub fn temporal_profiles(conn: &Connection) -> Result<Vec<(String, serde_json::Value, i64)>> {
    list_json(conn, "rust_temporal_profiles", "profile_json")
}

/// Append one financial snapshot (time-series semantics: never upserted,
/// so a re-run at the same instant is two honest observations).
pub fn insert_financial_snapshot(conn: &Connection, row: &FinancialSnapshotRow) -> Result<i64> {
    conn.execute(
        "INSERT INTO rust_financial_snapshots
             (taken_at, local_balance_sats, remote_balance_sats, onchain_sats,
              capacity_sats, revenue_accumulated_sats,
              rebalance_cost_accumulated_sats, channel_count)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            row.taken_at,
            row.local_balance_sats,
            row.remote_balance_sats,
            row.onchain_sats,
            row.capacity_sats,
            row.revenue_accumulated_sats,
            row.rebalance_cost_accumulated_sats,
            row.channel_count
        ],
    )
    .context("insert financial snapshot")?;
    Ok(conn.last_insert_rowid())
}

/// The most recent snapshots, newest first.
pub fn financial_snapshots(conn: &Connection, limit: i64) -> Result<Vec<FinancialSnapshotRow>> {
    let mut stmt = conn
        .prepare(
            "SELECT taken_at, local_balance_sats, remote_balance_sats, onchain_sats,
                    capacity_sats, revenue_accumulated_sats,
                    rebalance_cost_accumulated_sats, channel_count
             FROM rust_financial_snapshots ORDER BY taken_at DESC, id DESC LIMIT ?1",
        )
        .context("prepare financial snapshots")?;
    let rows = stmt
        .query_map([limit], |row| {
            Ok(FinancialSnapshotRow {
                taken_at: row.get(0)?,
                local_balance_sats: row.get(1)?,
                remote_balance_sats: row.get(2)?,
                onchain_sats: row.get(3)?,
                capacity_sats: row.get(4)?,
                revenue_accumulated_sats: row.get(5)?,
                rebalance_cost_accumulated_sats: row.get(6)?,
                channel_count: row.get(7)?,
            })
        })
        .context("query financial snapshots")?
        .collect::<rusqlite::Result<Vec<_>>>()
        .context("read financial snapshots")?;
    Ok(rows)
}
