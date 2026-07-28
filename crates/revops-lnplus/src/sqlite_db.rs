//! `SqliteLnPlusDb` — a [`crate::ports::LnPlusDb`] implementation over `rusqlite`,
//! matching `database.py`'s `lnplus_*` methods and the `lnplus_swaps` /
//! `lnplus_peers` schema exactly (`database.py:1359-1409`).
//!
//! Per the wiring task's explicit instruction this lives INSIDE
//! `revops-lnplus`, not `revops-db` — `ENTRYPOINTS.md` §2 originally
//! blocked a production `LnPlusDb` on adding these tables to `revops-db`;
//! the task that produced this file supersedes that blocker by building the
//! schema and every query here instead.
//!
//! The unified budget rail (`reserve_spend` / `release_spend_reservation` /
//! `mark_spend_reservation_spent`) is a DIFFERENT, already-ported subsystem
//! ([`revops_db::budget::BudgetDb`], over `budget_reservations` /
//! `spend_reservations` / `spend_events` — none of which are
//! `lnplus_swaps`/`lnplus_peers`). Rather than re-implement that rail a
//! second time in this crate (schema drift risk, and a flat violation of
//! "don't duplicate the reviewed thing"), [`SqliteLnPlusDb`] COMPOSES with
//! it: it opens its own `revops_db::budget::BudgetDb` at the same sqlite
//! path and delegates those three methods verbatim. This only USES
//! `revops-db` as a dependency — nothing in `revops-db` is modified, per
//! the task's hard rule.
//!
//! # Breaker persistence wire format
//!
//! `ENTRYPOINTS.md` §2 flags this explicitly: Python only ever stored a
//! plain string in `config_overrides['_lnplus_breaker']`
//! (`breaker::BREAKER_KEY`). This port's [`crate::breaker::BreakerCause`] is
//! structured, so [`SqliteLnPlusDb::get_breaker`]/`set_breaker` encode it as
//! JSON in the SAME key. **This means a Python writer and this Rust reader
//! (or vice versa) sharing one physical database would not understand each
//! other's breaker row.** That is a non-issue as long as this crate only
//! ever runs against its own database file (the project-wide "Rust doesn't
//! hold write authority over the production db until cutover" constraint —
//! see `revops_db::budget`'s module doc). A malformed/foreign-format value
//! is treated as "no breaker tripped" (fail-open on READ only — never
//! silently un-trips a Rust-tripped breaker, since Rust only ever writes
//! its own JSON shape) rather than a panic.

use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension};

use crate::breaker::{BreakerCause, BreakerState};
use crate::db_types::{PeerRow, SwapPatch, SwapRow};
use crate::ports::{
    AttemptIntent, AttemptKind, AttemptResolution, AttemptRow, AttemptState, BeginAttemptAck,
    CasOutcome, CompoundOutcome, InsertOutcome, LnPlusDb, Logger, PlannerActionRequest, PortError,
    PortResult, ReserveSpendRequest, ResolveAck, TerminalizeSpec, TripAck,
};
use crate::types::{Metadata, Rating};

/// House lock-wait budget, matching `revops-db`'s `BUSY_TIMEOUT_MS` /
/// `BudgetDb::open`'s default (both 5000ms) so every connection this crate
/// opens behaves the same way under contention.
pub const BUSY_TIMEOUT_MS: u64 = 5000;

/// Exact Python DDL (`database.py:1359-1409`) for the two tables named in
/// the task spec, plus the two supporting tables [`LnPlusDb`] also needs
/// (`config_overrides` — breaker + backfill-flag persistence,
/// `database.py:872-878`; `planner_actions` — breadcrumb rows,
/// `database.py:1331-1346`). All four are `CREATE TABLE IF NOT EXISTS` /
/// `CREATE INDEX IF NOT EXISTS`, safe to run every time a connection opens
/// and safe to run against a database another process (or `revops-db`'s
/// `BudgetDb`) is also writing to under WAL.
const DDL: &str = "
CREATE TABLE IF NOT EXISTS config_overrides (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS planner_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    channel_id TEXT,
    amount_sats INTEGER,
    estimated_cost_sats INTEGER,
    actual_cost_sats INTEGER,
    status TEXT NOT NULL DEFAULT 'planned',
    created_at INTEGER NOT NULL,
    completed_at INTEGER,
    reason TEXT,
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_planner_actions_status ON planner_actions(status);
CREATE INDEX IF NOT EXISTS idx_planner_actions_peer_time ON planner_actions(peer_id, created_at);
CREATE TABLE IF NOT EXISTS lnplus_swaps (
    swap_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    capacity_sats INTEGER NOT NULL,
    duration_months INTEGER NOT NULL,
    ends_at INTEGER,
    outbound_peer TEXT,
    incoming_peer TEXT,
    our_identifier TEXT,
    applied_at INTEGER NOT NULL,
    opened_at INTEGER,
    completed_at INTEGER,
    channel_funding_txid TEXT,
    deadline_at INTEGER,
    planner_action_id INTEGER,
    outcome TEXT,
    metadata_json TEXT,
    tag_added INTEGER,
    incoming_tag_added INTEGER
);
CREATE INDEX IF NOT EXISTS idx_lnplus_swaps_status ON lnplus_swaps(status);
CREATE TABLE IF NOT EXISTS lnplus_peers (
    pubkey TEXT PRIMARY KEY,
    swaps_count INTEGER NOT NULL DEFAULT 0,
    ratings_given_positive INTEGER NOT NULL DEFAULT 0,
    ratings_given_negative INTEGER NOT NULL DEFAULT 0,
    defections INTEGER NOT NULL DEFAULT 0,
    last_swap_at INTEGER
);
CREATE TABLE IF NOT EXISTS lnplus_attempts (
    attempt_id TEXT PRIMARY KEY,
    swap_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    reservation_id TEXT,
    peer_id TEXT,
    amount_sats INTEGER,
    detail TEXT,
    created_at INTEGER NOT NULL,
    resolved_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_lnplus_attempts_inflight
    ON lnplus_attempts(swap_id, kind)
    WHERE state IN ('intent', 'outcome_unknown');
CREATE INDEX IF NOT EXISTS idx_lnplus_attempts_state ON lnplus_attempts(state);
";

/// `database.py`'s `_lnplus_breaker` key (`crate::breaker::BREAKER_KEY`) and
/// the backfill-done flag (`crate::reconcile::BACKFILL_FLAG`) both live in
/// `config_overrides` — this module only re-exports the constant already
/// defined in `breaker.rs`/`reconcile.rs` rather than redeclaring it.
use crate::breaker::BREAKER_KEY;

/// Idempotent schema init — safe to call on every process start
/// (`CREATE TABLE`/`CREATE INDEX ... IF NOT EXISTS` throughout, matching
/// `database.py`'s own additive-migration convention, see its `initialize`
/// doc comment).
pub fn ensure_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(DDL)
}

/// Combined error surface for [`SqliteLnPlusDb::open`]: either the sqlite
/// connection this crate owns failed, or the composed
/// `revops_db::budget::BudgetDb` (a different connection, same file) did.
#[derive(Debug)]
pub enum OpenError {
    Sqlite(rusqlite::Error),
    Budget(revops_db::budget::BudgetError),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Sqlite(e) => write!(f, "sqlite error: {e}"),
            OpenError::Budget(e) => write!(f, "budget rail error: {e}"),
        }
    }
}
impl std::error::Error for OpenError {}
impl From<rusqlite::Error> for OpenError {
    fn from(e: rusqlite::Error) -> Self {
        OpenError::Sqlite(e)
    }
}
impl From<revops_db::budget::BudgetError> for OpenError {
    fn from(e: revops_db::budget::BudgetError) -> Self {
        OpenError::Budget(e)
    }
}

/// `rusqlite`-backed [`LnPlusDb`] over exactly ONE `Connection` (Task 61
/// 4A architecture gate): LN+ lifecycle state AND the unified budget rail
/// share this single connection, so a compound of terminal transition +
/// reservation settle/release + receipt can be one `BEGIN IMMEDIATE`
/// transaction. Budget-rail logic is NOT duplicated here — the rail's
/// transaction-composable kernels (`revops_db::budget::reserve_spend_in_tx`
/// / `mark_spent_in_tx` / `release_spend_reservation_on`) run on this
/// store's connection, with this store owning the boundary.
///
/// `Mutex`-wrapped (rather than `RefCell`) so the type stays usable from
/// behind an `Arc` if the plugin ever calls the evaluator/watcher passes
/// from more than one OS thread.
pub struct SqliteLnPlusDb {
    conn: Mutex<Connection>,
    logger: Box<dyn Logger + Send + Sync>,
}

impl SqliteLnPlusDb {
    /// Opens (creating if needed) the lnplus tables AND the budget-rail
    /// tables at `path`, on one connection. `path` must be this crate's
    /// own database file — never lnnode's production `revenue_ops.db`
    /// (see `revops_db::budget`'s module doc for why: the Rust plugin
    /// does not hold production write authority pre-cutover).
    pub fn open(path: &Path, logger: Box<dyn Logger + Send + Sync>) -> Result<Self, OpenError> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS))?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        ensure_schema(&conn)?;
        revops_db::budget::ensure_rail_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            logger,
        })
    }

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn warn(&self, msg: &str) {
        self.logger.log(
            crate::ports::LogLevel::Warn,
            &format!("LNPLUS sqlite: {msg}"),
        );
    }

    /// Task 61 4A: a persistence failure is logged AND returned — the log
    /// line is diagnostics, the `Err` is the acknowledgement the caller
    /// acts on. Never one without the other.
    fn ack_err(&self, what: &str, e: impl std::fmt::Display) -> PortError {
        self.warn(&format!("{what} failed: {e}"));
        PortError::new(format!("{what}: {e}"))
    }
}

/// The dynamic `SET` clause both [`LnPlusDb::cas_swap`] and the compound
/// share: `(sql fragments, boxed params)` for exactly the fields `patch`
/// sets. Returns `None` when the patch is empty (nothing to write).
fn patch_set_clause(patch: &SwapPatch) -> Option<(String, Vec<Box<dyn rusqlite::ToSql>>)> {
    let mut sets: Vec<&str> = Vec::new();
    let mut values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    macro_rules! set_field {
        ($col:literal, $val:expr) => {
            sets.push(concat!($col, " = ?"));
            values.push(Box::new($val));
        };
    }
    if let Some(v) = &patch.status {
        set_field!("status", v.clone());
    }
    if let Some(v) = &patch.outbound_peer {
        set_field!("outbound_peer", v.clone());
    }
    if let Some(v) = &patch.incoming_peer {
        set_field!("incoming_peer", v.clone());
    }
    if let Some(v) = &patch.our_identifier {
        set_field!("our_identifier", v.clone());
    }
    if let Some(v) = patch.opened_at {
        set_field!("opened_at", v);
    }
    if let Some(v) = patch.ends_at {
        set_field!("ends_at", v);
    }
    if let Some(v) = patch.deadline_at {
        set_field!("deadline_at", v);
    }
    if let Some(v) = &patch.channel_funding_txid {
        set_field!("channel_funding_txid", v.clone());
    }
    if let Some(v) = &patch.outcome {
        set_field!("outcome", v.clone());
    }
    if let Some(v) = patch.tag_added {
        set_field!("tag_added", v as i64);
    }
    if let Some(v) = patch.incoming_tag_added {
        set_field!("incoming_tag_added", v as i64);
    }
    if sets.is_empty() {
        return None;
    }
    Some((sets.join(", "), values))
}

/// CAS core shared by [`LnPlusDb::cas_swap`] and the compound: runs the
/// guarded UPDATE on `conn` (a connection OR an open transaction) and
/// classifies the result. `require_null_funding_txid` adds the
/// deadline-miss veto to the guard.
fn cas_swap_on(
    conn: &rusqlite::Connection,
    swap_id: &str,
    expected_statuses: &[&str],
    require_null_funding_txid: bool,
    patch: &SwapPatch,
) -> rusqlite::Result<CasOutcome> {
    let Some((set_clause, mut values)) = patch_set_clause(patch) else {
        // An empty patch from an expected status is a vacuous Applied —
        // but only if the guard actually holds; check it explicitly.
        let actual: Option<String> = conn
            .query_row(
                "SELECT status FROM lnplus_swaps WHERE swap_id = ?1",
                [swap_id],
                |r| r.get(0),
            )
            .optional()?;
        return Ok(match actual {
            Some(ref s) if expected_statuses.contains(&s.as_str()) => CasOutcome::Applied,
            other => CasOutcome::Conflict { actual: other },
        });
    };
    let marks = expected_statuses
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let txid_guard = if require_null_funding_txid {
        " AND channel_funding_txid IS NULL"
    } else {
        ""
    };
    let sql = format!(
        "UPDATE lnplus_swaps SET {set_clause} \
         WHERE swap_id = ? AND status IN ({marks}){txid_guard}"
    );
    values.push(Box::new(swap_id.to_string()));
    for s in expected_statuses {
        values.push(Box::new(s.to_string()));
    }
    let params: Vec<&dyn rusqlite::ToSql> = values.iter().map(|b| b.as_ref()).collect();
    let changed = conn.execute(&sql, params.as_slice())?;
    if changed > 0 {
        return Ok(CasOutcome::Applied);
    }
    let actual: Option<String> = conn
        .query_row(
            "SELECT status FROM lnplus_swaps WHERE swap_id = ?1",
            [swap_id],
            |r| r.get(0),
        )
        .optional()?;
    Ok(CasOutcome::Conflict { actual })
}

/// `set_config_override`'s core (M-13 v2 version ordering), runnable on a
/// connection or an open transaction.
fn set_config_override_on(
    conn: &rusqlite::Connection,
    key: &str,
    value: &str,
    now: i64,
) -> rusqlite::Result<()> {
    let current_max: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM config_overrides",
        [],
        |r| r.get(0),
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO config_overrides (key, value, version, updated_at) \
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![key, value, current_max + 1, now],
    )?;
    Ok(())
}

/// Fail-closed breaker read on a connection or an open transaction: a
/// present-but-undecodable value is an ERROR (corruption evidence in a
/// Rust-only store), never silently "untripped".
fn get_breaker_on(conn: &rusqlite::Connection) -> Result<Option<BreakerState>, String> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM config_overrides WHERE key = ?1",
            [BREAKER_KEY],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| format!("breaker read: {e}"))?;
    match raw {
        None => Ok(None),
        Some(raw) => match decode_breaker(&raw) {
            Some(state) => Ok(Some(state)),
            None => Err(format!(
                "breaker value at {BREAKER_KEY:?} is not this crate's JSON shape — refusing to \
                 treat an undecodable persisted breaker as untripped (fail closed)"
            )),
        },
    }
}

fn row_to_swap(row: &rusqlite::Row) -> rusqlite::Result<SwapRow> {
    let metadata_json: Option<String> = row.get("metadata_json")?;
    let metadata: Option<Metadata> = metadata_json
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok());
    let tag_added: Option<i64> = row.get("tag_added")?;
    let incoming_tag_added: Option<i64> = row.get("incoming_tag_added")?;
    Ok(SwapRow {
        swap_id: row.get("swap_id")?,
        status: row.get("status")?,
        capacity_sats: row.get("capacity_sats")?,
        duration_months: row.get("duration_months")?,
        outbound_peer: row.get("outbound_peer")?,
        incoming_peer: row.get("incoming_peer")?,
        our_identifier: row.get("our_identifier")?,
        applied_at: row.get("applied_at")?,
        opened_at: row.get("opened_at")?,
        ends_at: row.get("ends_at")?,
        deadline_at: row.get("deadline_at")?,
        channel_funding_txid: row.get("channel_funding_txid")?,
        outcome: row.get("outcome")?,
        tag_added: tag_added.map(|v| v != 0),
        incoming_tag_added: incoming_tag_added.map(|v| v != 0),
        planner_action_id: row.get("planner_action_id")?,
        metadata,
    })
}

const SWAP_COLUMNS: &str = "swap_id, status, capacity_sats, duration_months, outbound_peer, \
     incoming_peer, our_identifier, applied_at, opened_at, ends_at, deadline_at, \
     channel_funding_txid, outcome, tag_added, incoming_tag_added, metadata_json, \
     planner_action_id";

impl LnPlusDb for SqliteLnPlusDb {
    // -- swap ledger --------------------------------------------------

    /// Task 61 4A replacement for `lnplus_record_swap`'s `INSERT OR
    /// REPLACE`: a plain `INSERT` persisting EVERY row field, with the
    /// conflict typed as [`InsertOutcome::AlreadyExists`] instead of a
    /// silent overwrite.
    fn insert_swap_new(&self, row: &SwapRow) -> PortResult<InsertOutcome> {
        let metadata_json = row
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m).unwrap_or_default());
        let conn = self.conn.lock().unwrap();
        let result = conn.execute(
            "INSERT INTO lnplus_swaps \
             (swap_id, status, capacity_sats, duration_months, outbound_peer, \
              incoming_peer, our_identifier, applied_at, opened_at, ends_at, deadline_at, \
              channel_funding_txid, outcome, tag_added, incoming_tag_added, \
              planner_action_id, metadata_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            rusqlite::params![
                row.swap_id,
                row.status,
                row.capacity_sats,
                row.duration_months,
                row.outbound_peer,
                row.incoming_peer,
                row.our_identifier,
                row.applied_at,
                row.opened_at,
                row.ends_at,
                row.deadline_at,
                row.channel_funding_txid,
                row.outcome,
                row.tag_added.map(|v| v as i64),
                row.incoming_tag_added.map(|v| v as i64),
                row.planner_action_id,
                metadata_json,
            ],
        );
        match result {
            Ok(_) => Ok(InsertOutcome::Inserted),
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Ok(InsertOutcome::AlreadyExists)
            }
            Err(e) => Err(self.ack_err(&format!("insert_swap_new({})", row.swap_id), e)),
        }
    }

    /// Task 61 4A replacement for `lnplus_update_swap`'s blind UPDATE:
    /// the patch applies only from an expected status (CAS on the
    /// lifecycle column), acknowledged with a typed outcome.
    fn cas_swap(
        &self,
        swap_id: &str,
        expected_statuses: &[&str],
        patch: &SwapPatch,
    ) -> PortResult<CasOutcome> {
        let conn = self.conn.lock().unwrap();
        cas_swap_on(&conn, swap_id, expected_statuses, false, patch)
            .map_err(|e| self.ack_err(&format!("cas_swap({swap_id})"), e))
    }

    fn get_swap(&self, swap_id: &str) -> Option<SwapRow> {
        let conn = self.conn.lock().unwrap();
        let sql = format!("SELECT {SWAP_COLUMNS} FROM lnplus_swaps WHERE swap_id = ?1");
        conn.query_row(&sql, [swap_id], row_to_swap)
            .optional()
            .unwrap_or_else(|e| {
                self.warn(&format!("get_swap({swap_id}) failed: {e}"));
                None
            })
    }

    fn get_swaps_by_status(&self, statuses: &[&str]) -> Vec<SwapRow> {
        if statuses.is_empty() {
            return Vec::new();
        }
        let marks = statuses.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT {SWAP_COLUMNS} FROM lnplus_swaps WHERE status IN ({marks}) ORDER BY applied_at"
        );
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                self.warn(&format!("get_swaps_by_status prepare failed: {e}"));
                return Vec::new();
            }
        };
        let params: Vec<&dyn rusqlite::ToSql> =
            statuses.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
        let rows = stmt.query_map(params.as_slice(), row_to_swap);
        match rows {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e) => {
                self.warn(&format!("get_swaps_by_status query failed: {e}"));
                Vec::new()
            }
        }
    }

    /// `lnplus_prune_terminal` (`database.py:7656-7671`), same cutoff logic:
    /// a row qualifies only when `applied_at` is older than the cutoff AND
    /// (`ends_at IS NULL` or also older than the cutoff).
    fn prune_terminal(&self, older_than_days: i64, now: i64) -> PortResult<usize> {
        let cutoff = now - older_than_days.max(0) * 86_400;
        let statuses = crate::db_types::TERMINAL_STATUSES;
        let marks = statuses.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "DELETE FROM lnplus_swaps WHERE status IN ({marks}) \
             AND applied_at < ? AND (ends_at IS NULL OR ends_at < ?)"
        );
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = statuses
            .iter()
            .map(|s| Box::new(s.to_string()) as Box<dyn rusqlite::ToSql>)
            .collect();
        params.push(Box::new(cutoff));
        params.push(Box::new(cutoff));
        let conn = self.conn.lock().unwrap();
        let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|b| b.as_ref()).collect();
        conn.execute(&sql, refs.as_slice())
            .map_err(|e| self.ack_err("prune_terminal", e))
    }

    // -- peer reputation ------------------------------------------------

    fn get_peer(&self, pubkey: &str) -> Option<PeerRow> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT pubkey, swaps_count, defections, ratings_given_positive, \
             ratings_given_negative, last_swap_at FROM lnplus_peers WHERE pubkey = ?1",
            [pubkey],
            |row| {
                Ok(PeerRow {
                    pubkey: row.get(0)?,
                    swaps_count: row.get(1)?,
                    defections: row.get(2)?,
                    ratings_given_positive: row.get(3)?,
                    ratings_given_negative: row.get(4)?,
                    last_swap_at: row.get(5)?,
                })
            },
        )
        .optional()
        .unwrap_or_else(|e| {
            self.warn(&format!("get_peer({pubkey}) failed: {e}"));
            None
        })
    }

    /// `lnplus_bump_peer` (`database.py:7631-7646`) — `INSERT ... ON
    /// CONFLICT DO UPDATE`, same upsert shape.
    fn bump_peer(&self, pubkey: &str, defection: bool, rating: Option<Rating>) -> PortResult<()> {
        let now = Self::now();
        let pos = matches!(rating, Some(Rating::Positive)) as i64;
        let neg = matches!(rating, Some(Rating::Negative)) as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO lnplus_peers (pubkey, swaps_count, defections, \
                 ratings_given_positive, ratings_given_negative, last_swap_at) \
             VALUES (?1, 1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(pubkey) DO UPDATE SET \
                 swaps_count = swaps_count + 1, \
                 defections = defections + excluded.defections, \
                 ratings_given_positive = ratings_given_positive + excluded.ratings_given_positive, \
                 ratings_given_negative = ratings_given_negative + excluded.ratings_given_negative, \
                 last_swap_at = excluded.last_swap_at",
            rusqlite::params![pubkey, defection as i64, pos, neg, now],
        )
        .map(|_| ())
        .map_err(|e| self.ack_err(&format!("bump_peer({pubkey})"), e))
    }

    // -- backfill flag (config_overrides) ------------------------------

    fn get_config_override(&self, key: &str) -> Option<String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM config_overrides WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()
        .unwrap_or_else(|e| {
            self.warn(&format!("get_config_override({key}) failed: {e}"));
            None
        })
    }

    /// `set_config_override` (`database.py:7324-7362`) — `BEGIN IMMEDIATE`,
    /// version computed BEFORE the `INSERT OR REPLACE` (M-13 v2 fix: an
    /// `INSERT OR REPLACE` deletes the conflicting row first, so reading
    /// `MAX(version)` after it would see the wrong max if this key held it).
    /// Task 61 4A: any failure — begin, write, or COMMIT — is acknowledged
    /// as `Err`, with the transaction rolled back (COMMIT inside the
    /// guarded result, the Task 42 boundary rule).
    fn set_config_override(&self, key: &str, value: &str) -> PortResult<()> {
        let now = Self::now();
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| self.ack_err(&format!("set_config_override({key}) begin"), e))?;
        let result: rusqlite::Result<()> = (|| {
            set_config_override_on(&tx, key, value, now)?;
            tx.commit()?;
            Ok(())
        })();
        result.map_err(|e| self.ack_err(&format!("set_config_override({key})"), e))
    }

    fn delete_config_override(&self, key: &str) -> PortResult<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM config_overrides WHERE key = ?1", [key])
            .map(|_| ())
            .map_err(|e| self.ack_err(&format!("delete_config_override({key})"), e))
    }

    // -- circuit breaker --------------------------------------------------

    /// See this module's doc comment: JSON-encoded, own wire format (not
    /// Python's plain-string one). Task 61 4A: fails CLOSED — an
    /// undecodable persisted value is an `Err`, never "untripped".
    fn get_breaker(&self) -> PortResult<Option<BreakerState>> {
        let conn = self.conn.lock().unwrap();
        get_breaker_on(&conn).map_err(|e| self.ack_err("get_breaker", e))
    }

    fn set_breaker(&self, state: &BreakerState) -> PortResult<()> {
        self.set_config_override(BREAKER_KEY, &encode_breaker(state))
    }

    fn clear_breaker(&self) -> PortResult<()> {
        self.delete_config_override(BREAKER_KEY)
    }

    /// Task 61 4A: the atomic compound. One `BEGIN IMMEDIATE` transaction
    /// covers the guarded row CAS AND the breaker advance; COMMIT sits
    /// inside the guarded result so any failure — including COMMIT itself
    /// — rolls both halves back together and returns `Err`.
    fn terminalize_and_trip(
        &self,
        spec: &TerminalizeSpec<'_>,
        patch: &SwapPatch,
        cause: BreakerCause,
        now: i64,
    ) -> PortResult<CompoundOutcome> {
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| {
                self.ack_err(&format!("terminalize_and_trip({}) begin", spec.swap_id), e)
            })?;
        let result: Result<CompoundOutcome, String> = (|| {
            let cas = cas_swap_on(
                &tx,
                spec.swap_id,
                spec.expected_statuses,
                spec.require_null_funding_txid,
                patch,
            )
            .map_err(|e| format!("row cas: {e}"))?;
            match cas {
                CasOutcome::Conflict { actual } => {
                    // Guard did not hold — nothing may land, including the
                    // breaker half. COMMIT the (empty) transaction inside
                    // the guard so an error here still rolls back.
                    tx.commit().map_err(|e| format!("commit: {e}"))?;
                    Ok(CompoundOutcome::Conflict { actual })
                }
                CasOutcome::Applied => {
                    let breaker = match get_breaker_on(&tx)? {
                        Some(_) => TripAck::AlreadyTripped, // B10: first cause untouched
                        None => {
                            let state = BreakerState {
                                tripped_at: now,
                                cause,
                            };
                            set_config_override_on(&tx, BREAKER_KEY, &encode_breaker(&state), now)
                                .map_err(|e| format!("breaker write: {e}"))?;
                            TripAck::NewTrip
                        }
                    };
                    tx.commit().map_err(|e| format!("commit: {e}"))?;
                    Ok(CompoundOutcome::Terminalized { breaker })
                }
            }
        })();
        // A failed transaction is rolled back by `tx`'s Drop; the Err is
        // the acknowledgement.
        result.map_err(|e| self.ack_err(&format!("terminalize_and_trip({})", spec.swap_id), e))
    }

    // -- planner-action breadcrumbs --------------------------------------

    // -- attempt/reservation identity (Task 61 4B) -----------------------

    fn begin_attempt(&self, intent: &AttemptIntent) -> PortResult<BeginAttemptAck> {
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| self.ack_err("begin_attempt begin", e))?;
        let result: Result<BeginAttemptAck, String> = (|| {
            // The partial unique index enforces this too; checking first
            // yields the typed Blocked with the existing identity instead
            // of a constraint error.
            let existing: Option<(String, String)> = tx
                .query_row(
                    "SELECT attempt_id, state FROM lnplus_attempts \
                     WHERE swap_id = ?1 AND kind = ?2 \
                       AND state IN ('intent', 'outcome_unknown')",
                    rusqlite::params![intent.swap_id, intent.kind.as_str()],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(|e| format!("inflight lookup: {e}"))?;
            if let Some((existing_attempt_id, state)) = existing {
                let state = AttemptState::parse(&state)
                    .ok_or_else(|| format!("undecodable attempt state {state:?}"))?;
                tx.commit().map_err(|e| format!("commit: {e}"))?;
                return Ok(BeginAttemptAck::Blocked {
                    existing_attempt_id,
                    state,
                });
            }
            tx.execute(
                "INSERT INTO lnplus_attempts \
                 (attempt_id, swap_id, kind, state, reservation_id, peer_id, \
                  amount_sats, created_at) \
                 VALUES (?1, ?2, ?3, 'intent', ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    intent.attempt_id,
                    intent.swap_id,
                    intent.kind.as_str(),
                    intent.reservation_id,
                    intent.peer_id,
                    intent.amount_sats,
                    intent.created_at,
                ],
            )
            .map_err(|e| format!("intent insert: {e}"))?;
            tx.commit().map_err(|e| format!("commit: {e}"))?;
            Ok(BeginAttemptAck::Started)
        })();
        result.map_err(|e| self.ack_err(&format!("begin_attempt({})", intent.attempt_id), e))
    }

    /// Task 61 4B compound resolutions: each is ONE `BEGIN IMMEDIATE`
    /// transaction; the attempt-state CAS is the exactly-once guard, and
    /// the row/settle/receipt/release writes join that same transaction —
    /// a failure on ANY half rolls the whole resolution back.
    fn resolve_attempt(
        &self,
        attempt_id: &str,
        resolution: &AttemptResolution,
        now: i64,
    ) -> PortResult<ResolveAck> {
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| self.ack_err("resolve_attempt begin", e))?;
        let result: Result<ResolveAck, String> = (|| {
            let Some(attempt) =
                get_attempt_on(&tx, attempt_id).map_err(|e| format!("attempt read: {e}"))?
            else {
                tx.commit().map_err(|e| format!("commit: {e}"))?;
                return Ok(ResolveAck::UnknownAttempt);
            };
            // Exactly-once guard: only non-terminal states may resolve.
            // Quarantining is additionally restricted to Intent (an
            // already-unknown attempt is not "re-quarantined").
            let (new_state, detail): (AttemptState, Option<&str>) = match resolution {
                AttemptResolution::NotSubmitted { detail } => {
                    (AttemptState::NotSubmitted, Some(detail))
                }
                AttemptResolution::CommittedApply => (AttemptState::Committed, None),
                AttemptResolution::CommittedFund { .. } => (AttemptState::Committed, None),
                AttemptResolution::OutcomeUnknown { detail } => {
                    (AttemptState::OutcomeUnknown, Some(detail))
                }
            };
            let allowed_from = match resolution {
                AttemptResolution::OutcomeUnknown { .. } => vec![AttemptState::Intent],
                _ => vec![AttemptState::Intent, AttemptState::OutcomeUnknown],
            };
            if !allowed_from.contains(&attempt.state) {
                tx.commit().map_err(|e| format!("commit: {e}"))?;
                return Ok(ResolveAck::AlreadyResolved {
                    state: attempt.state,
                });
            }

            // The compound halves, all joining this one transaction.
            match resolution {
                AttemptResolution::NotSubmitted { .. } => {
                    if let Some(rid) = &attempt.reservation_id {
                        revops_db::budget::release_spend_reservation_on(&tx, rid)
                            .map_err(|e| format!("release: {e}"))?;
                    }
                }
                AttemptResolution::CommittedFund {
                    txid,
                    actual_cost_sats,
                } => {
                    let cas = cas_swap_on(
                        &tx,
                        &attempt.swap_id,
                        &["applied", "opening"],
                        false,
                        &SwapPatch::default()
                            .channel_funding_txid(txid.clone())
                            .opened_at(now),
                    )
                    .map_err(|e| format!("row cas: {e}"))?;
                    if let CasOutcome::Conflict { actual } = cas {
                        // A funded channel with no in-flight row is a
                        // serious inconsistency — fail closed, attempt
                        // stays reconcilable, operator-visible.
                        return Err(format!(
                            "swap {} moved (now {actual:?}) under a committed fund attempt",
                            attempt.swap_id
                        ));
                    }
                    if let Some(rid) = &attempt.reservation_id {
                        match revops_db::budget::mark_spent_in_tx(
                            &tx,
                            rid,
                            *actual_cost_sats,
                            Some("lnplus_swaps"),
                            true,
                            now,
                        )
                        .map_err(|e| format!("settle: {e}"))?
                        {
                            revops_db::budget::MarkSpentTx::Applied(true) => {}
                            revops_db::budget::MarkSpentTx::Applied(false) => {
                                return Err(format!(
                                    "reservation {rid} was not active under a committed fund \
                                     attempt — refusing a receipt-less settle"
                                ));
                            }
                            revops_db::budget::MarkSpentTx::EventRejected => {
                                return Err(format!(
                                    "settlement event for {rid} rejected — rolling the whole \
                                     resolution back"
                                ));
                            }
                        }
                    }
                }
                AttemptResolution::CommittedApply | AttemptResolution::OutcomeUnknown { .. } => {}
            }

            let changed = tx
                .execute(
                    "UPDATE lnplus_attempts \
                     SET state = ?1, detail = COALESCE(?2, detail), resolved_at = ?3 \
                     WHERE attempt_id = ?4",
                    rusqlite::params![new_state.as_str(), detail, now, attempt_id],
                )
                .map_err(|e| format!("attempt update: {e}"))?;
            if changed == 0 {
                return Err("attempt row vanished mid-resolution".to_string());
            }
            tx.commit().map_err(|e| format!("commit: {e}"))?;
            Ok(ResolveAck::Resolved)
        })();
        result.map_err(|e| self.ack_err(&format!("resolve_attempt({attempt_id})"), e))
    }

    fn get_attempt(&self, attempt_id: &str) -> PortResult<Option<AttemptRow>> {
        let conn = self.conn.lock().unwrap();
        get_attempt_on(&conn, attempt_id).map_err(|e| self.ack_err("get_attempt", e))
    }

    fn unknown_attempts(&self) -> PortResult<Vec<AttemptRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT attempt_id, swap_id, kind, state, reservation_id, peer_id, \
                 amount_sats, detail, created_at, resolved_at \
                 FROM lnplus_attempts WHERE state = 'outcome_unknown' ORDER BY created_at",
            )
            .map_err(|e| self.ack_err("unknown_attempts prepare", e))?;
        let rows = stmt
            .query_map([], row_to_attempt)
            .map_err(|e| self.ack_err("unknown_attempts query", e))?;
        let mut out = Vec::new();
        for row in rows {
            let row = row.map_err(|e| self.ack_err("unknown_attempts row", e))?;
            out.push(row.map_err(PortError::new)?);
        }
        Ok(out)
    }

    fn quarantine_stale_intents(&self, detail: &str, _now: i64) -> PortResult<usize> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE lnplus_attempts \
             SET state = 'outcome_unknown', detail = ?1, resolved_at = NULL \
             WHERE state = 'intent'",
            rusqlite::params![detail],
        )
        .map_err(|e| self.ack_err("quarantine_stale_intents", e))
    }

    /// `record_planner_action` (`database.py:7454-7468`).
    fn record_planner_action(&self, req: &PlannerActionRequest) -> PortResult<i64> {
        let now = Self::now();
        let metadata_json = req
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m).unwrap_or_default());
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO planner_actions \
             (action_type, peer_id, amount_sats, estimated_cost_sats, status, created_at, reason, metadata_json) \
             VALUES (?1, ?2, ?3, ?4, 'planned', ?5, ?6, ?7)",
            rusqlite::params![
                req.action_type,
                req.peer_id,
                req.amount_sats,
                req.estimated_cost_sats,
                now,
                req.reason,
                metadata_json,
            ],
        )
        .map(|_| conn.last_insert_rowid())
        .map_err(|e| self.ack_err("record_planner_action", e))
    }

    /// `update_planner_action` (`database.py:7470-7494`): sets `status`
    /// (always, this port's callers always pass one), and stamps
    /// `completed_at = now` whenever the new status is `completed`/`failed`
    /// (Python's `elif status in (...)：` fallback branch — this port never
    /// takes the explicit-`completed_at`-argument path Python also supports,
    /// since [`LnPlusDb::update_planner_action`] has no such parameter).
    fn update_planner_action(&self, action_id: i64, status: &str) -> PortResult<()> {
        let conn = self.conn.lock().unwrap();
        let result = if matches!(status, "completed" | "failed") {
            let now = Self::now();
            conn.execute(
                "UPDATE planner_actions SET status = ?1, completed_at = ?2 WHERE id = ?3",
                rusqlite::params![status, now, action_id],
            )
        } else {
            conn.execute(
                "UPDATE planner_actions SET status = ?1 WHERE id = ?2",
                rusqlite::params![status, action_id],
            )
        };
        result
            .map(|_| ())
            .map_err(|e| self.ack_err(&format!("update_planner_action({action_id})"), e))
    }

    fn trip_breaker_if_untripped(&self, state: &BreakerState) -> PortResult<TripAck> {
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| self.ack_err("trip_breaker_if_untripped begin", e))?;
        let result: Result<TripAck, String> = (|| {
            let ack = match get_breaker_on(&tx)? {
                Some(_) => TripAck::AlreadyTripped, // B10: first cause untouched
                None => {
                    set_config_override_on(&tx, BREAKER_KEY, &encode_breaker(state), Self::now())
                        .map_err(|e| format!("breaker write: {e}"))?;
                    TripAck::NewTrip
                }
            };
            tx.commit().map_err(|e| format!("commit: {e}"))?;
            Ok(ack)
        })();
        result.map_err(|e| self.ack_err("trip_breaker_if_untripped", e))
    }

    fn clear_breaker_if_cause(&self, expected: &BreakerCause) -> PortResult<bool> {
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| self.ack_err("clear_breaker_if_cause begin", e))?;
        let result: Result<bool, String> = (|| {
            let cleared = match get_breaker_on(&tx)? {
                Some(state) if &state.cause == expected => {
                    tx.execute("DELETE FROM config_overrides WHERE key = ?1", [BREAKER_KEY])
                        .map_err(|e| format!("breaker delete: {e}"))?;
                    true
                }
                _ => false,
            };
            tx.commit().map_err(|e| format!("commit: {e}"))?;
            Ok(cleared)
        })();
        result.map_err(|e| self.ack_err("clear_breaker_if_cause", e))
    }

    // -- unified budget rail ----------------------------------------------
    // Runs `revops_db::budget`'s transaction-composable kernels on THIS
    // store's single connection (never a second connection) — see this
    // module's doc comment. Not a duplicate implementation: the guard/sum/
    // insert/settle logic is single-sourced in revops-db.

    fn reserve_spend(&self, req: &ReserveSpendRequest) -> PortResult<bool> {
        let now = Self::now();
        let metadata = if req.metadata.is_empty() {
            None
        } else {
            let mut map = serde_json::Map::new();
            for (k, v) in &req.metadata {
                map.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            Some(serde_json::Value::Object(map))
        };
        let budget_req = revops_db::budget::ReserveRequest {
            reservation_id: req.reservation_id.clone(),
            amount_sats: req.amount_sats,
            category: req.category.to_string(),
            subcategory: Some(req.subcategory.to_string()),
            reference_id: None,
            channel_id: None,
            metadata,
            effective_budget_sats: req.effective_budget_sats,
            since_timestamp: req.since_timestamp,
            weekly_budget_limit: None,
            weekly_since_timestamp: None,
        };
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| self.ack_err("reserve_spend begin", e))?;
        let result: Result<bool, String> = (|| {
            let (granted, _remaining) =
                revops_db::budget::reserve_spend_in_tx(&tx, &budget_req, now)
                    .map_err(|e| format!("reserve: {e}"))?;
            tx.commit().map_err(|e| format!("commit: {e}"))?;
            Ok(granted)
        })();
        result.map_err(|e| self.ack_err("reserve_spend", e))
    }

    fn release_spend_reservation(&self, reservation_id: &str) -> PortResult<()> {
        let conn = self.conn.lock().unwrap();
        revops_db::budget::release_spend_reservation_on(&conn, reservation_id)
            .map(|_| ())
            .map_err(|e| self.ack_err("release_spend_reservation", e))
    }

    fn mark_spend_reservation_spent(
        &self,
        reservation_id: &str,
        actual_spent_sats: i64,
        source: &str,
    ) -> PortResult<bool> {
        let now = Self::now();
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| self.ack_err("mark_spend_reservation_spent begin", e))?;
        let result: Result<bool, String> = (|| {
            match revops_db::budget::mark_spent_in_tx(
                &tx,
                reservation_id,
                Some(actual_spent_sats),
                Some(source),
                true,
                now,
            )
            .map_err(|e| format!("settle: {e}"))?
            {
                revops_db::budget::MarkSpentTx::Applied(changed) => {
                    tx.commit().map_err(|e| format!("commit: {e}"))?;
                    Ok(changed)
                }
                revops_db::budget::MarkSpentTx::EventRejected => {
                    // tx drops -> rollback: the 'spent' flip is undone.
                    Ok(false)
                }
            }
        })();
        result.map_err(|e| self.ack_err("mark_spend_reservation_spent", e))
    }
}

// -- attempt rows (Task 61 4B) ---------------------------------------------

/// Inner `Result<AttemptRow, String>` so an undecodable kind/state is a
/// fail-closed error, never a silently skipped row.
#[allow(clippy::type_complexity)]
fn row_to_attempt(row: &rusqlite::Row) -> rusqlite::Result<Result<AttemptRow, String>> {
    let kind_raw: String = row.get("kind")?;
    let state_raw: String = row.get("state")?;
    let (Some(kind), Some(state)) = (
        AttemptKind::parse(&kind_raw),
        AttemptState::parse(&state_raw),
    ) else {
        return Ok(Err(format!(
            "undecodable attempt kind/state ({kind_raw:?}/{state_raw:?}) — refusing to guess"
        )));
    };
    Ok(Ok(AttemptRow {
        attempt_id: row.get("attempt_id")?,
        swap_id: row.get("swap_id")?,
        kind,
        state,
        reservation_id: row.get("reservation_id")?,
        peer_id: row.get("peer_id")?,
        amount_sats: row.get("amount_sats")?,
        detail: row.get("detail")?,
        created_at: row.get("created_at")?,
        resolved_at: row.get("resolved_at")?,
    }))
}

/// Attempt read on a connection or an open transaction; fail-closed on
/// undecodable rows.
fn get_attempt_on(
    conn: &rusqlite::Connection,
    attempt_id: &str,
) -> Result<Option<AttemptRow>, String> {
    let row = conn
        .query_row(
            "SELECT attempt_id, swap_id, kind, state, reservation_id, peer_id, \
             amount_sats, detail, created_at, resolved_at \
             FROM lnplus_attempts WHERE attempt_id = ?1",
            [attempt_id],
            row_to_attempt,
        )
        .optional()
        .map_err(|e| format!("attempt query: {e}"))?;
    match row {
        None => Ok(None),
        Some(Ok(attempt)) => Ok(Some(attempt)),
        Some(Err(e)) => Err(e),
    }
}

// -- breaker wire format ---------------------------------------------------

fn encode_breaker(state: &BreakerState) -> String {
    let cause = match &state.cause {
        BreakerCause::OpeningGhostNoLocalRecord { swap_id } => {
            serde_json::json!({"kind": "OpeningGhostNoLocalRecord", "swap_id": swap_id})
        }
        BreakerCause::PendingGhostNoLocalRecord { swap_id } => {
            serde_json::json!({"kind": "PendingGhostNoLocalRecord", "swap_id": swap_id})
        }
        BreakerCause::LocalRowDivergentFromRemote { swap_id, detail } => {
            serde_json::json!({"kind": "LocalRowDivergentFromRemote", "swap_id": swap_id, "detail": detail})
        }
        BreakerCause::MissedOpenDeadline { swap_id } => {
            serde_json::json!({"kind": "MissedOpenDeadline", "swap_id": swap_id})
        }
        BreakerCause::AmbiguousFundedChannelDivergence { swap_id, detail } => {
            serde_json::json!({"kind": "AmbiguousFundedChannelDivergence", "swap_id": swap_id, "detail": detail})
        }
        BreakerCause::LnPlusOutage { detail } => {
            serde_json::json!({"kind": "LnPlusOutage", "detail": detail})
        }
        BreakerCause::OperatorAbandonedSwap { swap_id } => {
            serde_json::json!({"kind": "OperatorAbandonedSwap", "swap_id": swap_id})
        }
    };
    serde_json::json!({"tripped_at": state.tripped_at, "cause": cause}).to_string()
}

fn decode_breaker(raw: &str) -> Option<BreakerState> {
    let v: serde_json::Value = serde_json::from_str(raw).ok()?;
    let tripped_at = v.get("tripped_at")?.as_i64()?;
    let cause_v = v.get("cause")?;
    let kind = cause_v.get("kind")?.as_str()?;
    let swap_id = || {
        cause_v
            .get("swap_id")
            .and_then(|s| s.as_str())
            .map(str::to_string)
    };
    let detail = || {
        cause_v
            .get("detail")
            .and_then(|s| s.as_str())
            .map(str::to_string)
    };
    let cause = match kind {
        "OpeningGhostNoLocalRecord" => BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: swap_id()?,
        },
        "PendingGhostNoLocalRecord" => BreakerCause::PendingGhostNoLocalRecord {
            swap_id: swap_id()?,
        },
        "LocalRowDivergentFromRemote" => BreakerCause::LocalRowDivergentFromRemote {
            swap_id: swap_id()?,
            detail: detail()?,
        },
        "MissedOpenDeadline" => BreakerCause::MissedOpenDeadline {
            swap_id: swap_id()?,
        },
        "AmbiguousFundedChannelDivergence" => BreakerCause::AmbiguousFundedChannelDivergence {
            swap_id: swap_id()?,
            detail: detail()?,
        },
        "LnPlusOutage" => BreakerCause::LnPlusOutage { detail: detail()? },
        "OperatorAbandonedSwap" => BreakerCause::OperatorAbandonedSwap {
            swap_id: swap_id()?,
        },
        _ => return None,
    };
    Some(BreakerState { tripped_at, cause })
}
