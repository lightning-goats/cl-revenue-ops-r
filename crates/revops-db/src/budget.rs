//! THE BUDGET RAIL — reserve/spend/release lifecycle over the production
//! schema's `budget_reservations` + `spend_reservations` + `spend_events`
//! tables (port of `modules/database.py`'s `_reserve_budget_atomic` /
//! `reserve_spend` / release / settle / sweep family, v2.18.1).
//!
//! # PRODUCTION-WRITE CONSTRAINT (read this twice)
//!
//! This module writes the PRODUCTION SCHEMA SHAPE — but it NEVER writes the
//! production database. Operator ruling (design spec constraint 2): the Rust
//! plugin never writes lnnode's `revenue_ops.db` and never holds action
//! authority until an explicit per-subsystem flag cutover. Therefore, in this
//! phase and until cutover:
//!
//! - [`BudgetDb::open`] takes an EXPLICIT path and has no production-path
//!   default anywhere. Every write path operates ONLY on (a) the plugin's
//!   OWN parallel DB file (the `owner.rs` pattern — plugin-created, never
//!   the production path), or (b) throwaway COPIES of fixtures in tests.
//! - Nothing here is wired to live RPC authority. Wiring + cutover rehearsal
//!   on a DB copy is a later, separately-gated step.
//! - Test hygiene: every test constructs its DB via `tempfile::TempDir` +
//!   this module's own DDL (the exact Python schema shape) or a fixture copy.
//!
//! # Money-safety invariants (mirrored verbatim from Python)
//!
//! - **P4-017 committed-total shape:** active reservations are
//!   currently-HELD budget and sum from BOTH tables with NO time filter;
//!   only committed costs (`rebalance_costs`) and committed spend events
//!   (`spend_events`) are windowed on the budget period.
//! - **Terminal guard:** re-reserving a `'spent'`/`'released'`
//!   reservation_id is refused with COMMIT (not ROLLBACK) — a terminal rid
//!   is never resurrected to `'active'` (double count). Re-reserving an
//!   *active* rid REPLACES its amount, so only the delta gates.
//! - **P2-003:** the settle path (`SELECT` → `UPDATE 'spent'` → settlement
//!   event `INSERT`) is ONE `BEGIN IMMEDIATE` transaction; a failed event
//!   write rolls the `'spent'` flip back — a reservation is NEVER left
//!   `'spent'` without its event (fail toward HOLDING budget).
//! - **P2-008:** a spend-event write is retried on BUSY/LOCKED and then
//!   surfaces the error — never a silent lost write (a lost event
//!   under-counts the budget in the OVERSPEND direction).
//! - **DD1/P1-003:** `BEGIN IMMEDIATE` takes the single WAL writer lock up
//!   front, so every budget check is serialized against every other reserve
//!   path — the categories can never jointly overshoot.

use rusqlite::{Connection, ErrorCode, OptionalExtension};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::time::Duration;

/// Python `Database.MAX_AMOUNT_SATS` (10 BTC): `_sanitize_amount` clamps
/// magnitudes above this, preserving sign.
pub const MAX_AMOUNT_SATS: i64 = 10_000_000_000;

/// Python `Database._COMMITTED_ONCHAIN_SPEND_CATEGORIES` (P4-021/P4-022):
/// categories whose reservation represents a COMMITTED on-chain spend. The
/// blind stale sweep must never release them.
pub const COMMITTED_ONCHAIN_SPEND_CATEGORIES: [&str; 3] =
    ["channel_open", "channel_close", "boltz"];

/// Errors from the budget rail.
#[derive(Debug)]
pub enum BudgetError {
    /// Underlying sqlite failure (including P2-008 retry exhaustion).
    Sqlite(rusqlite::Error),
    /// Metadata could not be serialized byte-compatibly (floats or a
    /// non-object value — Python callers pass a `Dict` of ints/strings).
    Metadata(String),
    /// Invalid caller input on a path whose Rust signature has no `bool`
    /// channel for the Python `return False` (see [`BudgetDb::record_spend_event`]).
    InvalidInput(String),
}

impl fmt::Display for BudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BudgetError::Sqlite(e) => write!(f, "budget sqlite error: {e}"),
            BudgetError::Metadata(m) => write!(f, "budget metadata error: {m}"),
            BudgetError::InvalidInput(m) => write!(f, "budget invalid input: {m}"),
        }
    }
}

impl std::error::Error for BudgetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BudgetError::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for BudgetError {
    fn from(e: rusqlite::Error) -> Self {
        BudgetError::Sqlite(e)
    }
}

type Result<T> = std::result::Result<T, BudgetError>;

/// Arguments to [`BudgetDb::reserve_spend`], mirroring the Python keyword
/// signature. `effective_budget_sats: None` means best-effort (caller
/// enforces budget); `since_timestamp: None` defaults to `now - 24h`.
#[derive(Debug, Clone, Default)]
pub struct ReserveRequest {
    pub reservation_id: String,
    pub amount_sats: i64,
    pub category: String,
    pub subcategory: Option<String>,
    pub reference_id: Option<String>,
    pub channel_id: Option<String>,
    /// JSON object; serialized as Python `json.dumps(metadata, sort_keys=True)`
    /// with default separators. `None` or `{}` (falsy in Python) store NULL.
    pub metadata: Option<Value>,
    pub effective_budget_sats: Option<i64>,
    pub since_timestamp: Option<i64>,
    pub weekly_budget_limit: Option<i64>,
    pub weekly_since_timestamp: Option<i64>,
}

/// A generic spend event (`record_spend_event`). `timestamp` is the injected
/// clock value — Python's `timestamp or time.time()` fallback has no
/// ambient-clock analog here; callers must supply the real time.
#[derive(Debug, Clone, Default)]
pub struct SpendEvent {
    pub event_id: String,
    pub category: String,
    pub amount_sats: i64,
    pub subcategory: Option<String>,
    pub timestamp: i64,
    pub reference_id: Option<String>,
    pub channel_id: Option<String>,
    pub source: Option<String>,
    pub metadata: Option<Value>,
}

/// One row of [`BudgetDb::get_spend_reservation_states`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationState {
    pub status: String,
    pub reserved_sats: i64,
}

/// Result of [`BudgetDb::clear_all_reservations`] (Issue #33).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClearStats {
    pub cleared_count: i64,
    pub released_sats: i64,
}

/// Exact Python DDL (`modules/database.py` initialize, lines 973–1030) for
/// the three rail tables + indexes, plus a minimal `rebalance_costs` (the
/// committed sums read it; production already has the full table — this is
/// `CREATE IF NOT EXISTS` only and never alters an existing table).
const DDL: &str = "
CREATE TABLE IF NOT EXISTS budget_reservations (
    reservation_id TEXT PRIMARY KEY,
    reserved_sats INTEGER NOT NULL,
    reserved_at INTEGER NOT NULL,
    job_channel_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active'
);
CREATE INDEX IF NOT EXISTS idx_budget_reservations_status ON budget_reservations(status, reserved_at);
CREATE TABLE IF NOT EXISTS spend_reservations (
    reservation_id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    subcategory TEXT,
    reserved_sats INTEGER NOT NULL,
    reserved_at INTEGER NOT NULL,
    reference_id TEXT,
    channel_id TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_spend_reservations_status ON spend_reservations(status, reserved_at);
CREATE INDEX IF NOT EXISTS idx_spend_reservations_category ON spend_reservations(category, reserved_at);
CREATE TABLE IF NOT EXISTS spend_events (
    event_id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    subcategory TEXT,
    amount_sats INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    reference_id TEXT,
    channel_id TEXT,
    source TEXT,
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_spend_events_time ON spend_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_spend_events_category ON spend_events(category, timestamp);
CREATE INDEX IF NOT EXISTS idx_spend_events_time_channel ON spend_events(timestamp, channel_id, amount_sats);
CREATE TABLE IF NOT EXISTS rebalance_costs (
    cost_sats INTEGER,
    timestamp INTEGER
);
";

/// Single-owner budget-rail handle over one writable `rusqlite::Connection`.
/// The async actor wrap is Phase 3b wiring; until then the connection never
/// crosses a task boundary.
pub struct BudgetDb {
    conn: Connection,
}

impl BudgetDb {
    /// Open (creating if needed) the rail database at an EXPLICIT path with
    /// the house `busy_timeout = 5000` and the exact Python DDL.
    ///
    /// SHADOW CONSTRAINT: callers must pass the plugin's OWN file or a test
    /// tempdir/fixture copy — never lnnode's production `revenue_ops.db`.
    /// There is deliberately no default path and no config lookup here.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_busy_timeout(path, 5000)
    }

    /// Test seam: like [`BudgetDb::open`] but with a caller-chosen busy
    /// timeout so lock-contention tests (P2-008 retry exhaustion) do not
    /// take 3 × 5s. Production code paths use [`BudgetDb::open`].
    pub fn open_with_busy_timeout(path: &Path, busy_timeout_ms: u64) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_millis(busy_timeout_ms))?;
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch(DDL)?;
        Ok(Self { conn })
    }

    // -----------------------------------------------------------------
    // reserve
    // -----------------------------------------------------------------

    /// Create a generic spend reservation (Python `reserve_spend`).
    ///
    /// Returns `(granted, remaining)`. `remaining` follows Python's
    /// `_return_remaining=True` contract: after-grant headroom
    /// (`min(daily, weekly) - amount`) on success, the violated limit's
    /// remaining on a budget refusal, and `0` on sanitize/terminal refusals
    /// and best-effort (no-budget) grants.
    ///
    /// ONE `BEGIN IMMEDIATE` wraps guard + sums + insert (P2-003); sanitize
    /// rejects happen BEFORE the transaction, exactly as in Python.
    pub fn reserve_spend(&mut self, req: ReserveRequest, now: i64) -> Result<(bool, i64)> {
        let amount = sanitize_amount(req.amount_sats);
        if amount <= 0 {
            return Ok((false, 0));
        }
        let rid = req.reservation_id.trim().to_string();
        if rid.is_empty() {
            return Ok((false, 0));
        }
        let cat = req.category.trim().to_lowercase();
        if cat.is_empty() {
            return Ok((false, 0));
        }
        // Python computes meta_json before BEGIN IMMEDIATE; a metadata error
        // therefore never leaves a transaction open.
        let meta_json = metadata_json(req.metadata.as_ref())?;
        let budget_since = req.since_timestamp.unwrap_or(now - 24 * 3600);

        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = reserve_spend_locked(
            &self.conn,
            &req,
            &rid,
            &cat,
            amount,
            meta_json.as_deref(),
            budget_since,
            now,
        );
        if result.is_err() {
            // Mirror Python's except: ROLLBACK (best-effort), then surface.
            // (Python logged and returned (False, 0); the Rust surface keeps
            // the error — either way no reservation exists: fail toward
            // refusing the spend.)
            rollback_quietly(&self.conn);
        }
        result
    }

    /// Python `release_spend_reservation`: single autocommit
    /// `UPDATE … SET status='released' WHERE … status='active'`.
    pub fn release_spend_reservation(&mut self, reservation_id: &str) -> Result<bool> {
        let n = self.conn.execute(
            "UPDATE spend_reservations SET status = 'released' \
             WHERE reservation_id = ?1 AND status = 'active'",
            [reservation_id],
        )?;
        Ok(n > 0)
    }

    /// Python `mark_spend_reservation_spent` (P2-003 atomic settle).
    ///
    /// `SELECT` row → `UPDATE 'spent' WHERE 'active'` → (when `record_event`)
    /// settlement event `resv:{rid}` with `source` defaulting to
    /// `"reservation_settlement"`, ALL in one `BEGIN IMMEDIATE`. If the event
    /// write fails, the `'spent'` flip is rolled back and the error (or
    /// `false` for a sanitize reject) is returned — a reservation is NEVER
    /// left `'spent'` without its event.
    pub fn mark_spend_reservation_spent(
        &mut self,
        reservation_id: &str,
        actual_spent_sats: Option<i64>,
        source: Option<&str>,
        record_event: bool,
        now: i64,
    ) -> Result<bool> {
        let rid = reservation_id.trim().to_string();
        if rid.is_empty() {
            return Ok(false);
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = mark_spent_locked(
            &self.conn,
            &rid,
            actual_spent_sats,
            source,
            record_event,
            now,
        );
        if result.is_err() {
            rollback_quietly(&self.conn);
        }
        result
    }

    /// Python `record_spend_event` (P2-008): `INSERT OR REPLACE`, retried 3x
    /// on `SQLITE_BUSY`/`SQLITE_LOCKED` with 50/100/150 ms sleeps, then `Err`
    /// — never a silent lost write. Empty ids / non-positive amounts are
    /// `Err(InvalidInput)` (Python returned `False`; this signature has no
    /// bool channel — the settle path uses the internal bool-shaped helper
    /// so its behavior matches Python exactly).
    pub fn record_spend_event(&mut self, ev: SpendEvent) -> Result<()> {
        match record_spend_event_on(&self.conn, &ev)? {
            true => Ok(()),
            false => Err(BudgetError::InvalidInput(
                "spend event requires non-empty event_id/category and amount > 0".into(),
            )),
        }
    }

    /// Python `get_spend_reservation_states` (read-only reconciliation map).
    /// `None` returns all rows capped at 10000; `Some(ids)` filters (entries
    /// that are empty after trimming are dropped, but surviving ids are
    /// passed through UNtrimmed, as in Python).
    pub fn get_spend_reservation_states(
        &self,
        reservation_ids: Option<&[String]>,
    ) -> Result<BTreeMap<String, ReservationState>> {
        let mut out = BTreeMap::new();
        let mut collect = |row: &rusqlite::Row<'_>| -> rusqlite::Result<()> {
            let rid: String = row.get(0)?;
            let status: String = row.get(1)?;
            let reserved_sats: Option<i64> = row.get(2)?;
            out.insert(
                rid,
                ReservationState {
                    status,
                    reserved_sats: reserved_sats.unwrap_or(0),
                },
            );
            Ok(())
        };
        match reservation_ids {
            Some(ids) => {
                let ids: Vec<&String> = ids.iter().filter(|r| !r.trim().is_empty()).collect();
                if ids.is_empty() {
                    return Ok(out);
                }
                let qmarks = vec!["?"; ids.len()].join(",");
                let sql = format!(
                    "SELECT reservation_id, status, reserved_sats FROM spend_reservations \
                     WHERE reservation_id IN ({qmarks}) ORDER BY reservation_id"
                );
                let mut stmt = self.conn.prepare(&sql)?;
                let mut rows = stmt.query(rusqlite::params_from_iter(ids))?;
                while let Some(row) = rows.next()? {
                    collect(row)?;
                }
            }
            None => {
                let mut stmt = self.conn.prepare(
                    "SELECT reservation_id, status, reserved_sats FROM spend_reservations \
                     ORDER BY reservation_id LIMIT 10000",
                )?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    collect(row)?;
                }
            }
        }
        Ok(out)
    }

    /// Python `get_category_spend_sats`: windowed sum of `spend_events` for a
    /// category (normalized identically to `record_spend_event`), optionally
    /// filtered on subcategory.
    pub fn get_category_spend_sats(
        &self,
        category: &str,
        subcategory: Option<&str>,
        since_timestamp: i64,
    ) -> Result<i64> {
        let cat = category.trim().to_lowercase();
        let total = match subcategory {
            None => self.conn.query_row(
                "SELECT COALESCE(SUM(amount_sats), 0) FROM spend_events \
                 WHERE category = ?1 AND timestamp >= ?2",
                rusqlite::params![cat, since_timestamp],
                |r| r.get(0),
            )?,
            Some(sub) => self.conn.query_row(
                "SELECT COALESCE(SUM(amount_sats), 0) FROM spend_events \
                 WHERE category = ?1 AND subcategory = ?2 AND timestamp >= ?3",
                rusqlite::params![cat, sub, since_timestamp],
                |r| r.get(0),
            )?,
        };
        Ok(total)
    }

    // -----------------------------------------------------------------
    // sweeps
    // -----------------------------------------------------------------

    /// Python `cleanup_stale_reservations` (P4-015): release active
    /// reservations older than the cutoff in BOTH the legacy
    /// `budget_reservations` table and the unified table's
    /// `category='rebalance'` rows — EXCEPT reservations whose id matches a
    /// `rebalance_history` row parked as `'pending_settlement'` (an in-flight
    /// HTLC holds its budget). Mirrors Python's error surface exactly: the
    /// legacy UPDATE propagates a missing `rebalance_history`, while the
    /// unified UPDATE tolerates sqlite failures (Python's
    /// `except sqlite3.OperationalError: pass` for minimal/partial schemas).
    pub fn cleanup_stale_reservations(&mut self, max_age_seconds: i64, now: i64) -> Result<i64> {
        let cutoff = now - max_age_seconds;
        let mut count = self.conn.execute(
            "UPDATE budget_reservations SET status = 'released' \
             WHERE status = 'active' AND reserved_at < ?1 \
               AND reservation_id NOT IN ( \
                   SELECT CAST(id AS TEXT) FROM rebalance_history \
                   WHERE status = 'pending_settlement' \
               )",
            [cutoff],
        )? as i64;
        match self.conn.execute(
            "UPDATE spend_reservations SET status = 'released' \
             WHERE status = 'active' AND category = 'rebalance' \
               AND reserved_at < ?1 \
               AND reservation_id NOT IN ( \
                   SELECT CAST(id AS TEXT) FROM rebalance_history \
                   WHERE status = 'pending_settlement' \
               )",
            [cutoff],
        ) {
            Ok(n) => count += n as i64,
            Err(rusqlite::Error::SqliteFailure(..)) => {} // tolerated (Python)
            Err(e) => return Err(e.into()),
        }
        Ok(count)
    }

    /// Python `cleanup_stale_spend_reservations` (P4-021): the blind
    /// (no-category) sweep skips the committed on-chain spend categories;
    /// an explicit category sweep (operator recovery) reaches everything.
    pub fn cleanup_stale_spend_reservations(
        &mut self,
        max_age_seconds: i64,
        category: Option<&str>,
        now: i64,
    ) -> Result<i64> {
        let cutoff = now - max_age_seconds;
        let n = match category {
            Some(cat) => self.conn.execute(
                "UPDATE spend_reservations SET status = 'released' \
                 WHERE status = 'active' AND reserved_at < ?1 AND category = ?2",
                rusqlite::params![cutoff, cat.trim().to_lowercase()],
            )?,
            None => {
                let [c1, c2, c3] = COMMITTED_ONCHAIN_SPEND_CATEGORIES;
                self.conn.execute(
                    "UPDATE spend_reservations SET status = 'released' \
                     WHERE status = 'active' AND reserved_at < ?1 \
                       AND category NOT IN (?2, ?3, ?4)",
                    rusqlite::params![cutoff, c1, c2, c3],
                )?
            }
        };
        Ok(n as i64)
    }

    /// Python `count_stale_reservations` (Issue #24): count without
    /// releasing — legacy `budget_reservations` only, as in Python.
    pub fn count_stale_reservations(&self, max_age_seconds: i64, now: i64) -> Result<i64> {
        let cutoff = now - max_age_seconds;
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM budget_reservations \
             WHERE status = 'active' AND reserved_at < ?1",
            [cutoff],
            |r| r.get(0),
        )?)
    }

    /// Python `clear_all_reservations` (Issue #33, audit fix I-1): read the
    /// stats and release ALL active rows in one `BEGIN IMMEDIATE` (no TOCTOU
    /// with a concurrent reserve). Touches ONLY `budget_reservations`,
    /// exactly as Python does.
    pub fn clear_all_reservations(&mut self) -> Result<ClearStats> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| {
            let (cleared_count, released_sats): (i64, i64) = self.conn.query_row(
                "SELECT COUNT(*), COALESCE(SUM(reserved_sats), 0) \
                 FROM budget_reservations WHERE status = 'active'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            self.conn.execute(
                "UPDATE budget_reservations SET status = 'released' WHERE status = 'active'",
                [],
            )?;
            self.conn.execute_batch("COMMIT")?;
            Ok(ClearStats {
                cleared_count,
                released_sats,
            })
        })();
        if result.is_err() {
            rollback_quietly(&self.conn);
        }
        result
    }

    // -----------------------------------------------------------------
    // Phase 2J compatibility wrappers (rebalance callers)
    // -----------------------------------------------------------------

    /// Python `reserve_budget`: compatibility wrapper over the generic spend
    /// ledger — new reservations land in `spend_reservations`
    /// (`category='rebalance'`); the legacy table is transition-read-only.
    /// Python swallowed exceptions into `(False, 0)` after logging; here the
    /// error propagates — in both surfaces no reservation exists (fail
    /// toward refusing the spend).
    #[allow(clippy::too_many_arguments)] // mirrors the Python signature
    pub fn reserve_budget(
        &mut self,
        reservation_id: &str,
        amount_sats: i64,
        channel_id: &str,
        budget_limit: i64,
        since_timestamp: i64,
        weekly_budget_limit: Option<i64>,
        weekly_since_timestamp: Option<i64>,
        now: i64,
    ) -> Result<(bool, i64)> {
        self.reserve_spend(
            ReserveRequest {
                reservation_id: reservation_id.to_string(),
                amount_sats,
                category: "rebalance".to_string(),
                channel_id: Some(channel_id.to_string()),
                effective_budget_sats: Some(budget_limit),
                since_timestamp: Some(since_timestamp),
                weekly_budget_limit,
                weekly_since_timestamp,
                ..ReserveRequest::default()
            },
            now,
        )
    }

    /// Python `release_budget_reservation` (Phase 2J dual-path): unified
    /// ledger first, then the legacy-table fallback for rows created before
    /// unification (drains to zero, removed in Phase 5).
    pub fn release_budget_reservation(&mut self, reservation_id: &str) -> Result<bool> {
        if self.release_spend_reservation(reservation_id)? {
            return Ok(true);
        }
        let n = self.conn.execute(
            "UPDATE budget_reservations SET status = 'released' \
             WHERE reservation_id = ?1 AND status = 'active'",
            [reservation_id],
        )?;
        Ok(n > 0)
    }

    /// Python `mark_budget_spent` (Phase 2J dual-path): unified settle with
    /// `record_event=false` — actual rebalance costs stay in
    /// `rebalance_costs`, so nothing double-counts — then the legacy-table
    /// fallback. (`actual_spent` is logging-only in Python; with no event
    /// recorded, no clock is needed either.)
    pub fn mark_budget_spent(&mut self, reservation_id: &str, actual_spent: i64) -> Result<bool> {
        if self.mark_spend_reservation_spent(reservation_id, Some(actual_spent), None, false, 0)? {
            return Ok(true);
        }
        let n = self.conn.execute(
            "UPDATE budget_reservations SET status = 'spent' \
             WHERE reservation_id = ?1 AND status = 'active'",
            [reservation_id],
        )?;
        Ok(n > 0)
    }
}

// ---------------------------------------------------------------------------
// locked bodies (run inside an already-open BEGIN IMMEDIATE)
// ---------------------------------------------------------------------------

/// Body of `reserve_spend` inside the writer lock. Owns COMMIT/ROLLBACK on
/// its success paths; an `Err` return leaves the transaction open for the
/// caller's best-effort ROLLBACK (mirroring Python's `except` block).
#[allow(clippy::too_many_arguments)]
fn reserve_spend_locked(
    conn: &Connection,
    req: &ReserveRequest,
    rid: &str,
    cat: &str,
    amount: i64,
    meta_json: Option<&str>,
    budget_since: i64,
    now: i64,
) -> Result<(bool, i64)> {
    // Terminal guard: INSERT OR REPLACE would otherwise resurrect a terminal
    // ('spent'/'released') reservation_id back to 'active', double counting
    // it against the budget. Refusal COMMITs (not ROLLBACK).
    let existing: Option<String> = conn
        .query_row(
            "SELECT status FROM spend_reservations WHERE reservation_id = ?1",
            [rid],
            |r| r.get(0),
        )
        .optional()?;
    let mut existing_active_sats = 0i64;
    if let Some(status) = existing {
        if status == "spent" || status == "released" {
            conn.execute_batch("COMMIT")?;
            return Ok((false, 0));
        }
        if status == "active" {
            // Re-reserving an active id REPLACES its amount, so only the
            // delta counts against the budget (avoid double-counting).
            existing_active_sats = conn
                .query_row(
                    "SELECT reserved_sats FROM spend_reservations WHERE reservation_id = ?1",
                    [rid],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(0);
        }
    }

    let mut daily_remaining = 0i64;
    let mut weekly_remaining: Option<i64> = None;
    let enforce_budget = req.effective_budget_sats.is_some();
    if let Some(effective_budget_sats) = req.effective_budget_sats {
        // THE P4-017 SHAPE: active holds from BOTH tables with NO time
        // filter; committed costs/events windowed on the budget period.
        let gen_reserved = sum_i64(
            conn,
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM spend_reservations \
             WHERE status = 'active'",
            &[],
        )?;
        let reb_reserved = sum_i64(
            conn,
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM budget_reservations \
             WHERE status = 'active'",
            &[],
        )?;
        let reb_committed = sum_i64(
            conn,
            "SELECT COALESCE(SUM(cost_sats), 0) FROM rebalance_costs WHERE timestamp >= ?1",
            &[&budget_since],
        )?;
        let gen_committed = sum_i64(
            conn,
            "SELECT COALESCE(SUM(amount_sats), 0) FROM spend_events WHERE timestamp >= ?1",
            &[&budget_since],
        )?;
        // Exclude this id's current active amount (it is being replaced).
        let already =
            gen_reserved + reb_reserved + reb_committed + gen_committed - existing_active_sats;
        daily_remaining = effective_budget_sats - already;
        if amount > daily_remaining {
            conn.execute_batch("ROLLBACK")?;
            return Ok((false, daily_remaining));
        }

        // Optional weekly cap, byte-matched to _reserve_budget_atomic's
        // weekly sum shape: window filter on committed costs/events; active
        // holds counted in full (the SAME unfiltered sums — P4-017).
        if let (Some(weekly_budget_limit), Some(weekly_since)) =
            (req.weekly_budget_limit, req.weekly_since_timestamp)
        {
            let weekly_spent = sum_i64(
                conn,
                "SELECT COALESCE(SUM(cost_sats), 0) FROM rebalance_costs WHERE timestamp >= ?1",
                &[&weekly_since],
            )?;
            let weekly_gen_spent = sum_i64(
                conn,
                "SELECT COALESCE(SUM(amount_sats), 0) FROM spend_events WHERE timestamp >= ?1",
                &[&weekly_since],
            )?;
            let weekly_already = weekly_spent + reb_reserved + gen_reserved + weekly_gen_spent
                - existing_active_sats;
            let remaining = weekly_budget_limit - weekly_already;
            if amount > remaining {
                conn.execute_batch("ROLLBACK")?;
                return Ok((false, remaining));
            }
            weekly_remaining = Some(remaining);
        }
    }

    conn.execute(
        "INSERT OR REPLACE INTO spend_reservations \
         (reservation_id, category, subcategory, reserved_sats, reserved_at, \
          reference_id, channel_id, status, metadata_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', ?8)",
        rusqlite::params![
            rid,
            cat,
            req.subcategory,
            amount,
            now,
            req.reference_id,
            req.channel_id,
            meta_json
        ],
    )?;
    conn.execute_batch("COMMIT")?;

    if !enforce_budget {
        return Ok((true, 0));
    }
    let mut after = daily_remaining - amount;
    if let Some(weekly) = weekly_remaining {
        after = after.min(weekly - amount);
    }
    Ok((true, after))
}

/// Body of `mark_spend_reservation_spent` inside the writer lock (P2-003).
fn mark_spent_locked(
    conn: &Connection,
    rid: &str,
    actual_spent_sats: Option<i64>,
    source: Option<&str>,
    record_event: bool,
    now: i64,
) -> Result<bool> {
    struct ResvRow {
        category: String,
        subcategory: Option<String>,
        reserved_sats: i64,
        reference_id: Option<String>,
        channel_id: Option<String>,
    }
    let row: Option<ResvRow> = conn
        .query_row(
            "SELECT category, subcategory, reserved_sats, reference_id, channel_id \
             FROM spend_reservations WHERE reservation_id = ?1",
            [rid],
            |r| {
                Ok(ResvRow {
                    category: r.get(0)?,
                    subcategory: r.get(1)?,
                    reserved_sats: r.get(2)?,
                    reference_id: r.get(3)?,
                    channel_id: r.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(row) = row else {
        conn.execute_batch("COMMIT")?;
        return Ok(false);
    };
    let changed = conn.execute(
        "UPDATE spend_reservations SET status = 'spent' \
         WHERE reservation_id = ?1 AND status = 'active'",
        [rid],
    )? > 0;
    if changed && record_event {
        let amount = sanitize_amount(actual_spent_sats.unwrap_or(row.reserved_sats));
        let ev = SpendEvent {
            event_id: format!("resv:{rid}"),
            category: row.category,
            amount_sats: amount,
            subcategory: row.subcategory,
            timestamp: now,
            reference_id: row.reference_id,
            channel_id: row.channel_id,
            source: Some(source.unwrap_or("reservation_settlement").to_string()),
            metadata: Some(serde_json::json!({ "reservation_id": rid })),
        };
        // Same connection: the INSERT joins this transaction. If it fails we
        // roll the 'spent' flip back — do not leave the reservation 'spent'
        // without its event (fail toward HOLDING budget).
        if !record_spend_event_on(conn, &ev)? {
            conn.execute_batch("ROLLBACK")?;
            return Ok(false);
        }
    }
    conn.execute_batch("COMMIT")?;
    Ok(changed)
}

/// Internal `record_spend_event` (P2-008) with Python's exact bool shape:
/// `Ok(false)` = sanitize reject (Python `return False`), `Ok(true)` =
/// persisted, `Err` = retry exhaustion or a non-transient failure (Python
/// `raise` — the caller must not report a spend against a lost event).
fn record_spend_event_on(conn: &Connection, ev: &SpendEvent) -> Result<bool> {
    let eid = ev.event_id.trim().to_string();
    let cat = ev.category.trim().to_lowercase();
    if eid.is_empty() || cat.is_empty() {
        return Ok(false);
    }
    let amount = sanitize_amount(ev.amount_sats);
    // P4-003: a non-positive amount would SUM() into committed spend and
    // lower it in the overspend-permitting direction. Reject before writing.
    if amount <= 0 {
        return Ok(false);
    }
    let meta_json = metadata_json(ev.metadata.as_ref())?;
    let mut last_err: Option<rusqlite::Error> = None;
    for attempt in 0u64..3 {
        match conn.execute(
            "INSERT OR REPLACE INTO spend_events \
             (event_id, category, subcategory, amount_sats, timestamp, \
              reference_id, channel_id, source, metadata_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                eid,
                cat,
                ev.subcategory,
                amount,
                ev.timestamp,
                ev.reference_id,
                ev.channel_id,
                ev.source,
                meta_json
            ],
        ) {
            Ok(_) => return Ok(true),
            Err(e) if is_busy_or_locked(&e) => {
                last_err = Some(e);
                std::thread::sleep(Duration::from_millis(50 * (attempt + 1)));
            }
            // Non-transient (e.g. schema error): do not silently lose the
            // write — surface it to the caller immediately.
            Err(e) => return Err(e.into()),
        }
    }
    Err(BudgetError::Sqlite(last_err.expect(
        "retry loop exits early unless a busy/locked error was recorded",
    )))
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Python `Database._sanitize_amount`: clamp |amount| to `MAX_AMOUNT_SATS`,
/// preserving sign. (The float/NaN branches have no i64 analog.)
fn sanitize_amount(amount_sats: i64) -> i64 {
    if amount_sats.unsigned_abs() > MAX_AMOUNT_SATS as u64 {
        if amount_sats >= 0 {
            MAX_AMOUNT_SATS
        } else {
            -MAX_AMOUNT_SATS
        }
    } else {
        amount_sats
    }
}

/// Python `json.dumps(metadata or {}, sort_keys=True) if metadata else None`:
/// `None` and `{}` (falsy) store NULL; a non-empty object is serialized with
/// sorted keys and DEFAULT separators (`", "`, `": "`), `ensure_ascii=True`.
/// Floats are rejected (metadata is caller data; the rail is integer-only).
fn metadata_json(metadata: Option<&Value>) -> Result<Option<String>> {
    match metadata {
        None => Ok(None),
        Some(Value::Object(map)) if map.is_empty() => Ok(None),
        Some(v @ Value::Object(_)) => Ok(Some(python_dumps_sorted(v)?)),
        Some(other) => Err(BudgetError::Metadata(format!(
            "metadata must be a JSON object, got: {other}"
        ))),
    }
}

/// Local re-implementation of Python `json.dumps(x, sort_keys=True)` with
/// default separators and `ensure_ascii=True` — same semantics as
/// `revops_econ::ledger::python_dumps_default` (re-implemented here per the
/// phase plan: revops-db must not depend on revops-econ). Byte-compat
/// contract for `spend_reservations.metadata_json` / `spend_events.metadata_json`.
fn python_dumps_sorted(v: &Value) -> Result<String> {
    let mut out = String::new();
    write_value(v, &mut out)?;
    Ok(out)
}

fn write_value(v: &Value, out: &mut String) -> Result<()> {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => {
            if n.is_f64() {
                return Err(BudgetError::Metadata(format!(
                    "budget metadata forbids non-integer numbers: {n}"
                )));
            }
            out.push_str(&n.to_string());
        }
        Value::String(s) => write_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_value(item, out)?;
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_string(k, out);
                out.push_str(": ");
                write_value(&map[*k], out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) >= 0x7F => write_unicode_escape(c, out),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// `\uXXXX` (UTF-16 surrogate pair for astral codepoints), matching Python's
/// `ensure_ascii=True` encoder.
fn write_unicode_escape(c: char, out: &mut String) {
    let cp = c as u32;
    if cp > 0xFFFF {
        let v = cp - 0x10000;
        let high = 0xD800 + (v >> 10);
        let low = 0xDC00 + (v & 0x3FF);
        out.push_str(&format!("\\u{high:04x}\\u{low:04x}"));
    } else {
        out.push_str(&format!("\\u{cp:04x}"));
    }
}

fn sum_i64(conn: &Connection, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Result<i64> {
    Ok(conn.query_row(sql, params, |r| r.get(0))?)
}

fn is_busy_or_locked(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(err, _)
            if err.code == ErrorCode::DatabaseBusy || err.code == ErrorCode::DatabaseLocked
    )
}

/// Best-effort ROLLBACK for error paths (Python's nested
/// `try: conn.execute("ROLLBACK") except: pass`).
fn rollback_quietly(conn: &Connection) {
    let _ = conn.execute_batch("ROLLBACK");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Golden strings for the local serializer, generated with
    /// `python3 -c "import json; print(json.dumps(<value>, sort_keys=True))"`.
    #[test]
    fn python_dumps_sorted_matches_python_golden_strings() {
        let cases: Vec<(Value, &str)> = vec![
            (json!({}), "{}"),
            (json!({"reserved_msat": 400000}), r#"{"reserved_msat": 400000}"#),
            (
                json!({"b": 1, "a": 2, "c": {"z": 1, "y": 2}}),
                r#"{"a": 2, "b": 1, "c": {"y": 2, "z": 1}}"#,
            ),
            (
                json!({"empty_list": [], "neg": -5, "nested": [1, 2, {"x": "y"}]}),
                r#"{"empty_list": [], "neg": -5, "nested": [1, 2, {"x": "y"}]}"#,
            ),
            (
                // ensure_ascii=True: non-ASCII escapes, surrogate pair for 😀.
                json!({"a": "héllo", "b": "日本語", "c": "😀"}),
                "{\"a\": \"h\\u00e9llo\", \"b\": \"\\u65e5\\u672c\\u8a9e\", \"c\": \"\\ud83d\\ude00\"}",
            ),
            (
                json!({"kind": "ledger_stale_reservation", "terminal": true}),
                r#"{"kind": "ledger_stale_reservation", "terminal": true}"#,
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(python_dumps_sorted(&value).unwrap(), expected);
        }
        assert!(python_dumps_sorted(&json!({"f": 0.5})).is_err());
    }

    #[test]
    fn sanitize_amount_clamps_magnitude_preserving_sign() {
        assert_eq!(sanitize_amount(5), 5);
        assert_eq!(sanitize_amount(-5), -5);
        assert_eq!(sanitize_amount(MAX_AMOUNT_SATS), MAX_AMOUNT_SATS);
        assert_eq!(sanitize_amount(MAX_AMOUNT_SATS + 1), MAX_AMOUNT_SATS);
        assert_eq!(sanitize_amount(-(MAX_AMOUNT_SATS + 1)), -MAX_AMOUNT_SATS);
        assert_eq!(sanitize_amount(i64::MIN), -MAX_AMOUNT_SATS);
    }

    #[test]
    fn metadata_json_nullness_matches_python_falsiness() {
        assert_eq!(metadata_json(None).unwrap(), None);
        assert_eq!(metadata_json(Some(&json!({}))).unwrap(), None);
        assert_eq!(
            metadata_json(Some(&json!({"a": 1}))).unwrap().as_deref(),
            Some(r#"{"a": 1}"#)
        );
        assert!(metadata_json(Some(&json!([1, 2]))).is_err());
        assert!(metadata_json(Some(&json!("s"))).is_err());
    }
}
