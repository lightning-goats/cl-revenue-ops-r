//! C71-31: py `Database.get_budget_status(since)` (database.py:4686-4746),
//! the `budget` block of `_assemble_econ_snapshot`.
//!
//! Three SELECTs under ONE transaction, and the transaction is Python's own
//! requirement, not a stylistic choice: its audit fix C-1 says so in the
//! source -- "without this, a rebalance completing between the two reads
//! could move sats from reserved to spent, skewing the total". That is the
//! same torn-read failure C71-21 closed for the profitability snapshot, and
//! it is worse here because the two halves move in opposite directions: a
//! settle between the reads double-counts the same sats as BOTH spent and
//! reserved, inflating committed budget and throttling spend that was
//! actually available.
//!
//! **The lock is NOT exact production parity, deliberately.** Python opens
//! `BEGIN IMMEDIATE` on a WRITABLE production connection, taking the WAL
//! writer reservation up front. This port reads through
//! `spawn_read_only`, so it can only take a deferred READ transaction.
//! That still pins all three SELECTs to one WAL snapshot -- which is the
//! property the torn-read failure needs -- but it does NOT exclude a
//! concurrent Python writer the way Python's own call does, and it cannot:
//! acquiring a writer reservation on the production database is precisely
//! what the shadow window forbids. Single-snapshot behaviour is pinned
//! here; writable production-owner transaction/lock semantics belong to
//! the Task 69 cutover review.
//!
//! Reserved has TWO sources that are SUMMED, not chosen between: the legacy
//! `budget_reservations` table and the unified `spend_reservations` ledger
//! (`category = 'rebalance'`). Python reads both because the unification is
//! mid-migration and rows exist on both sides. Taking only one silently
//! under-reports committed budget.

use anyhow::{Context, Result};
use rusqlite::Connection;

/// py's `{'spent', 'reserved', 'total_committed'}` return shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BudgetStatus {
    pub spent_sats: i64,
    pub reserved_sats: i64,
    pub total_committed_sats: i64,
}

/// Read the windowed budget position under a single snapshot.
///
/// Runs inside the read actor's own turn (see
/// [`crate::actor::DbHandle::budget_status`]) -- callers never receive a
/// connection.
pub(crate) fn read_budget_status(conn: &Connection, since: i64) -> Result<BudgetStatus> {
    let tx = conn
        .unchecked_transaction()
        .context("open budget status read transaction")?;

    // py: SUM(cost_sats) -- the sats column, deliberately. This is the
    // FLEET spend total, not the per-channel cost whose msat-native form
    // C71-28 needed; Python reads them with different queries and this one
    // has no `cost_msat` COALESCE.
    let spent_sats: i64 = tx
        .query_row(
            "SELECT COALESCE(SUM(cost_sats), 0) FROM rebalance_costs WHERE timestamp >= ?1",
            [since],
            |row| row.get(0),
        )
        .context("read budget spent")?;

    let legacy_reserved: i64 = tx
        .query_row(
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM budget_reservations
              WHERE status = 'active' AND reserved_at >= ?1",
            [since],
            |row| row.get(0),
        )
        .context("read legacy budget reservations")?;

    // py wraps this one in `except sqlite3.OperationalError` because
    // "minimal/partial schemas (tests, tooling) may lack the generic ledger
    // table". An ABSENT TABLE is a schema fact; any other failure still
    // refuses, so a real read error cannot masquerade as zero reservations.
    let unified_reserved: i64 = match tx.query_row(
        "SELECT COALESCE(SUM(reserved_sats), 0) FROM spend_reservations
          WHERE status = 'active' AND category = 'rebalance' AND reserved_at >= ?1",
        [since],
        |row| row.get::<_, i64>(0),
    ) {
        Ok(value) => value,
        Err(rusqlite::Error::SqliteFailure(_, Some(ref message)))
            if message.contains("no such table") =>
        {
            0
        }
        Err(error) => {
            return Err(anyhow::Error::from(error)).context("read unified spend reservations")
        }
    };

    tx.finish().context("finish budget status read")?;

    // C71-33: checked. Each SQL `SUM` is individually an i64, so two
    // legally-representable component totals can still overflow their Rust
    // sum -- which panics in debug and WRAPS in release, and a wrapped
    // committed-budget figure is a negative number that frees the entire
    // daily cap. Python's integers do neither, so refusing is the only
    // behaviour that is not silently wrong.
    let reserved_sats = legacy_reserved
        .checked_add(unified_reserved)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "reserved budget overflows i64: legacy {legacy_reserved} + unified \
             {unified_reserved}"
            )
        })?;
    let total_committed_sats = spent_sats.checked_add(reserved_sats).ok_or_else(|| {
        anyhow::anyhow!(
            "committed budget overflows i64: spent {spent_sats} + reserved \
             {reserved_sats}"
        )
    })?;
    Ok(BudgetStatus {
        spent_sats,
        reserved_sats,
        total_committed_sats,
    })
}

/// Typed handle-level read: one actor turn, one transaction, one `.await`.
pub async fn budget_status(handle: &crate::actor::DbHandle, since: i64) -> Result<BudgetStatus> {
    handle.budget_status(since).await
}
