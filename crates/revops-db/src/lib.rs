#![forbid(unsafe_code)]
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

pub mod actor;
pub mod notifications;
pub mod owner;

/// Open the production sqlite database read-only. Never creates the file;
/// errors if it does not already exist.
pub fn open_read_only(path: &Path) -> Result<Connection> {
    ensure!(path.exists(), "database not found: {}", path.display());
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open read-only {}", path.display()))?;
    conn.busy_timeout(std::time::Duration::from_millis(5000))?;
    Ok(conn)
}

/// List all table names in the database, sorted.
///
/// Returns `Err` rather than panicking on a `prepare`/`query` failure (e.g.
/// `SQLITE_BUSY` under contention at init) so the caller can take the same
/// graceful `disable()` path used for an `open_read_only` failure, instead
/// of crashing plugin startup.
pub fn table_names(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .context("prepare table_names query")?;
    let names = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .context("run table_names query")?
        .filter_map(|r| r.ok())
        .collect();
    Ok(names)
}

/// Dual-column msat/sat convention from the Python sats->msat migration:
/// prefer the msat column, fall back to sats*1000, 0 when both absent.
pub fn coalesce_msat(msat: Option<i64>, sats: Option<i64>) -> i64 {
    msat.unwrap_or_else(|| sats.map(|s| s * 1000).unwrap_or(0))
}
