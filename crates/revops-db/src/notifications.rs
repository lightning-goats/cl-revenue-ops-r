//! The Rust plugin's OWN writable sqlite file (never the production DB —
//! see the phase1b plan's Global Constraints). Holds a narrower schema
//! than production: dedup-insert of settled forwards, plus a
//! peer-connection-event log and a channel-closure-event log, deliberately
//! WITHOUT production's bookkeeper-cost enrichment (that's Phase 3/6
//! territory, tracked as a gap, not built here).
//!
//! `compute_forward_hydration_start` is a direct, statement-for-statement
//! port of `_compute_forward_hydration_start` (cl-revenue-ops.py:602-625),
//! parity-tested against `fixtures/hydration.json`
//! (`tools/port/gen_hydration_fixtures.py` in the `cl_revenue_ops-port`
//! worktree).

use anyhow::Result;
use rusqlite::{params, Connection};

/// One settled forward, as extracted from CLN's `forward_event`
/// notification. Field names/order mirror production's `forwards` table
/// dedup shape (`fixtures/schema.sql` lines 56-70) exactly, since the
/// `UNIQUE INDEX` below reuses that same seven-column key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardRow {
    pub in_channel: String,
    pub out_channel: String,
    pub in_msat: i64,
    pub out_msat: i64,
    pub fee_msat: i64,
    /// Matches production's `timestamp` column, which stores what CLN
    /// calls `received_time`.
    pub timestamp: i64,
    pub resolved_time: i64,
}

/// Idempotent `CREATE TABLE IF NOT EXISTS` for the observer's own db.
pub fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE IF NOT EXISTS ingested_forwards (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             in_channel TEXT NOT NULL,
             out_channel TEXT NOT NULL,
             in_msat INTEGER NOT NULL,
             out_msat INTEGER NOT NULL,
             fee_msat INTEGER NOT NULL,
             timestamp INTEGER NOT NULL,
             resolved_time INTEGER DEFAULT 0
         );
         CREATE UNIQUE INDEX IF NOT EXISTS idx_ingested_forwards_unique
             ON ingested_forwards(in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time);
         CREATE TABLE IF NOT EXISTS peer_connection_events (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             peer_id TEXT NOT NULL,
             event_type TEXT NOT NULL,
             ts INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS channel_closure_events (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             scid TEXT NOT NULL,
             cause TEXT NOT NULL,
             ts INTEGER NOT NULL
         );",
    )?;
    Ok(())
}

/// `INSERT OR IGNORE` into `ingested_forwards`. Returns `true` if a new row
/// was actually inserted, `false` if it was a dedup no-op (exact-match on
/// the seven-column unique index) -- the correctness backstop that makes
/// startup hydration and a live `forward_event` racing on the same forward
/// safe under any interleaving.
pub fn insert_forward_ignore_dup(conn: &Connection, f: &ForwardRow) -> Result<bool> {
    let changed = conn.execute(
        "INSERT OR IGNORE INTO ingested_forwards
             (in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            f.in_channel,
            f.out_channel,
            f.in_msat,
            f.out_msat,
            f.fee_msat,
            f.timestamp,
            f.resolved_time
        ],
    )?;
    Ok(changed == 1)
}

/// `SELECT MAX(timestamp) FROM ingested_forwards` -- `None` on an empty
/// table (drives the "empty table -> full warm start" branch of
/// [`compute_forward_hydration_start`]).
pub fn last_forward_ts(conn: &Connection) -> Result<Option<i64>> {
    Ok(
        conn.query_row("SELECT MAX(timestamp) FROM ingested_forwards", [], |r| {
            r.get(0)
        })?,
    )
}

/// Record a peer connect/disconnect event (dedup-insert concern only --
/// mirrors `database.record_connection_event`, cl-revenue-ops.py:6837/6880
/// call sites). No uniqueness constraint: repeated connects/disconnects
/// are legitimate distinct events, unlike a forward's exact-duplicate key.
pub fn insert_peer_connection_event(
    conn: &Connection,
    peer_id: &str,
    event_type: &str,
    ts: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO peer_connection_events (peer_id, event_type, ts) VALUES (?1, ?2, ?3)",
        params![peer_id, event_type, ts],
    )?;
    Ok(())
}

/// Record a channel-closure event (SCID + cause + timestamp only --
/// deliberately narrower than production's full closure-accounting schema,
/// per the plan's Task 2 interface note).
pub fn insert_channel_closure_event(
    conn: &Connection,
    scid: &str,
    cause: &str,
    ts: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO channel_closure_events (scid, cause, ts) VALUES (?1, ?2, ?3)",
        params![scid, cause, ts],
    )?;
    Ok(())
}

/// `FORWARD_HYDRATION_EVENT_JITTER_SECONDS` (cl-revenue-ops.py:311): a gap
/// this small between "now" and the last stored forward is normal
/// notification jitter around a restart, not a real backfill need.
const FORWARD_HYDRATION_EVENT_JITTER_SECONDS: i64 = 300;

/// Direct port of `_compute_forward_hydration_start`
/// (cl-revenue-ops.py:602-625): compute a bounded startup backfill start
/// time for the forwards table.
///
/// - Empty table (`last_forward_ts` is `None`) -> full warm start:
///   `now - max(flow_window_days, 14) * 86400`.
/// - Non-empty table, gap `<= FORWARD_HYDRATION_EVENT_JITTER_SECONDS` ->
///   `None` (no hydration needed -- the live notification stream already
///   covers this restart).
/// - Otherwise -> bounded overlap backfill:
///   `max(last_forward_ts - 86400, now - max(flow_window_days + 1, 15) * 86400)`.
pub fn compute_forward_hydration_start(
    last_forward_ts: Option<i64>,
    flow_window_days: i64,
    now: i64,
) -> Option<i64> {
    let Some(last) = last_forward_ts else {
        return Some(now - flow_window_days.max(14) * 86400);
    };
    let gap = (now - last).max(0);
    if gap <= FORWARD_HYDRATION_EVENT_JITTER_SECONDS {
        return None;
    }
    let floor = now - (flow_window_days + 1).max(15) * 86400;
    let overlap_start = last - 86400;
    Some(overlap_start.max(floor))
}
