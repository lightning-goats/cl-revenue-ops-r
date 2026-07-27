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
    LnPlusDb, Logger, PlannerActionRequest, PortError, PortResult, ReserveSpendRequest,
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

/// `rusqlite`-backed [`LnPlusDb`]. `Mutex`-wrapped connections (rather than
/// `RefCell`) so the type stays usable from behind an `Arc` if the plugin
/// ever calls the evaluator/watcher passes from more than one OS thread —
/// every method here still runs to completion holding the lock, so there is
/// no cross-call atomicity beyond that (matching Python's single-connection,
/// GIL-adjacent behavior; true cross-statement transactions are used only
/// where `database.py` itself uses `BEGIN IMMEDIATE`, i.e. inside
/// `revops_db::budget::BudgetDb`).
pub struct SqliteLnPlusDb {
    conn: Mutex<Connection>,
    budget: Mutex<revops_db::budget::BudgetDb>,
    logger: Box<dyn Logger>,
}

impl SqliteLnPlusDb {
    /// Opens (creating if needed) the lnplus tables at `path`, plus a
    /// composed `BudgetDb` at the SAME path for the three budget-rail
    /// methods. `path` must be this crate's own database file — never
    /// lnnode's production `revenue_ops.db` (see `revops_db::budget`'s
    /// module doc for why: the Rust plugin does not hold production write
    /// authority pre-cutover).
    pub fn open(path: &Path, logger: Box<dyn Logger>) -> Result<Self, OpenError> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS))?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        ensure_schema(&conn)?;
        let budget = revops_db::budget::BudgetDb::open(path)?;
        Ok(Self {
            conn: Mutex::new(conn),
            budget: Mutex::new(budget),
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

    /// `lnplus_record_swap` (`database.py:7571-7584`) — `INSERT OR REPLACE`.
    fn record_swap(&self, row: &SwapRow) {
        let metadata_json = row
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m).unwrap_or_default());
        let conn = self.conn.lock().unwrap();
        let result = conn.execute(
            "INSERT OR REPLACE INTO lnplus_swaps \
             (swap_id, status, capacity_sats, duration_months, outbound_peer, \
              incoming_peer, our_identifier, applied_at, planner_action_id, metadata_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                row.swap_id,
                row.status,
                row.capacity_sats,
                row.duration_months,
                row.outbound_peer,
                row.incoming_peer,
                row.our_identifier,
                row.applied_at,
                row.planner_action_id,
                metadata_json,
            ],
        );
        if let Err(e) = result {
            self.warn(&format!("record_swap({}) failed: {e}", row.swap_id));
        }
    }

    /// `lnplus_update_swap` (`database.py:7586-7595`) — dynamic `SET`
    /// clause over exactly the columns [`SwapPatch`] sets (a fixed subset of
    /// Python's `_LNPLUS_UPDATABLE_FIELDS`; `completed_at` is in Python's
    /// updatable set but has no `SwapPatch` field since nothing in
    /// `lnplus_swaps.py` ever writes it — see this module's doc comment).
    fn update_swap(&self, swap_id: &str, patch: &SwapPatch) {
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
            return; // py: `if not fields: return`
        }
        let sql = format!(
            "UPDATE lnplus_swaps SET {} WHERE swap_id = ?",
            sets.join(", ")
        );
        values.push(Box::new(swap_id.to_string()));
        let conn = self.conn.lock().unwrap();
        let params: Vec<&dyn rusqlite::ToSql> = values.iter().map(|b| b.as_ref()).collect();
        if let Err(e) = conn.execute(&sql, params.as_slice()) {
            self.warn(&format!("update_swap({swap_id}) failed: {e}"));
        }
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
    fn prune_terminal(&self, older_than_days: i64, now: i64) -> usize {
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
        match conn.execute(&sql, refs.as_slice()) {
            Ok(n) => n,
            Err(e) => {
                self.warn(&format!("prune_terminal failed: {e}"));
                0
            }
        }
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
    fn bump_peer(&self, pubkey: &str, defection: bool, rating: Option<Rating>) {
        let now = Self::now();
        let pos = matches!(rating, Some(Rating::Positive)) as i64;
        let neg = matches!(rating, Some(Rating::Negative)) as i64;
        let conn = self.conn.lock().unwrap();
        let result = conn.execute(
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
        );
        if let Err(e) = result {
            self.warn(&format!("bump_peer({pubkey}) failed: {e}"));
        }
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
    fn set_config_override(&self, key: &str, value: &str) {
        let now = Self::now();
        let mut conn_guard = self.conn.lock().unwrap();
        let tx =
            match conn_guard.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate) {
                Ok(tx) => tx,
                Err(e) => {
                    self.warn(&format!("set_config_override({key}) begin failed: {e}"));
                    return;
                }
            };
        let result: rusqlite::Result<()> = (|| {
            let current_max: i64 = tx.query_row(
                "SELECT COALESCE(MAX(version), 0) FROM config_overrides",
                [],
                |r| r.get(0),
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO config_overrides (key, value, version, updated_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![key, value, current_max + 1, now],
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                if let Err(e) = tx.commit() {
                    self.warn(&format!("set_config_override({key}) commit failed: {e}"));
                }
            }
            Err(e) => {
                self.warn(&format!("set_config_override({key}) failed: {e}"));
                // `tx` drops here -> automatic rollback (its `Drop` impl).
            }
        }
    }

    fn delete_config_override(&self, key: &str) {
        let conn = self.conn.lock().unwrap();
        if let Err(e) = conn.execute("DELETE FROM config_overrides WHERE key = ?1", [key]) {
            self.warn(&format!("delete_config_override({key}) failed: {e}"));
        }
    }

    // -- circuit breaker --------------------------------------------------

    /// See this module's doc comment: JSON-encoded, own wire format (not
    /// Python's plain-string one).
    fn get_breaker(&self) -> Option<BreakerState> {
        let raw = self.get_config_override(BREAKER_KEY)?;
        match decode_breaker(&raw) {
            Some(state) => Some(state),
            None => {
                self.warn(&format!(
                    "get_breaker: value at {BREAKER_KEY:?} is not this crate's JSON shape \
                     (foreign writer?) — treating as untripped rather than guessing"
                ));
                None
            }
        }
    }

    fn set_breaker(&self, state: &BreakerState) {
        self.set_config_override(BREAKER_KEY, &encode_breaker(state));
    }

    fn clear_breaker(&self) {
        self.delete_config_override(BREAKER_KEY);
    }

    // -- planner-action breadcrumbs --------------------------------------

    /// `record_planner_action` (`database.py:7454-7468`).
    fn record_planner_action(&self, req: &PlannerActionRequest) -> i64 {
        let now = Self::now();
        let metadata_json = req
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m).unwrap_or_default());
        let conn = self.conn.lock().unwrap();
        let result = conn.execute(
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
        );
        match result {
            Ok(_) => conn.last_insert_rowid(),
            Err(e) => {
                self.warn(&format!("record_planner_action failed: {e}"));
                0
            }
        }
    }

    /// `update_planner_action` (`database.py:7470-7494`): sets `status`
    /// (always, this port's callers always pass one), and stamps
    /// `completed_at = now` whenever the new status is `completed`/`failed`
    /// (Python's `elif status in (...)：` fallback branch — this port never
    /// takes the explicit-`completed_at`-argument path Python also supports,
    /// since [`LnPlusDb::update_planner_action`] has no such parameter).
    fn update_planner_action(&self, action_id: i64, status: &str) {
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
        if let Err(e) = result {
            self.warn(&format!("update_planner_action({action_id}) failed: {e}"));
        }
    }

    // -- unified budget rail ----------------------------------------------
    // Delegates to the composed `revops_db::budget::BudgetDb` — see this
    // module's doc comment for why that is NOT a duplicate implementation.

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
        let mut budget = self.budget.lock().unwrap();
        budget
            .reserve_spend(budget_req, now)
            .map(|(granted, _remaining)| granted)
            .map_err(|e| PortError::new(format!("reserve_spend: {e}")))
    }

    fn release_spend_reservation(&self, reservation_id: &str) -> PortResult<()> {
        let mut budget = self.budget.lock().unwrap();
        budget
            .release_spend_reservation(reservation_id)
            .map(|_| ())
            .map_err(|e| PortError::new(format!("release_spend_reservation: {e}")))
    }

    fn mark_spend_reservation_spent(
        &self,
        reservation_id: &str,
        actual_spent_sats: i64,
        source: &str,
    ) -> PortResult<bool> {
        let now = Self::now();
        let mut budget = self.budget.lock().unwrap();
        budget
            .mark_spend_reservation_spent(
                reservation_id,
                Some(actual_spent_sats),
                Some(source),
                true,
                now,
            )
            .map_err(|e| PortError::new(format!("mark_spend_reservation_spent: {e}")))
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
        _ => return None,
    };
    Some(BreakerState { tripped_at, cause })
}
