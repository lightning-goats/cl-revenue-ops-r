//! Persistent read-only DB actor: a single-owner task that holds the
//! `rusqlite::Connection` (which is `!Sync`) and services requests off an
//! `mpsc` channel. Replaces Phase 1a's Task 8 probe-and-drop
//! (`open_read_only` -> use once -> `drop`) with a long-lived connection
//! that coexists safely with the Python plugin's writer under WAL: the
//! connection never crosses a task boundary, only request/reply messages
//! do (mirrors the design spec's "single-owner actor tasks (mpsc) where
//! Python held one lock across a whole cycle").
//!
//! There is no explicit shutdown message: once every [`DbHandle`] clone is
//! dropped, the `mpsc::Sender` side closes, `blocking_recv` returns `None`,
//! and the owning task exits cleanly on its own.

use anyhow::{Context, Result};
use rusqlite::{types::Value as SqlValue, Connection, OptionalExtension, Row};
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

enum Command {
    TableCount(oneshot::Sender<Result<usize>>),
    /// `sql` must be `'static` (a query literal, never operator input) --
    /// callers build query STRINGS at compile time; only bind PARAMETERS
    /// are dynamic. This keeps the actor a closed, auditable surface.
    QueryI64 {
        sql: &'static str,
        params: Vec<SqlValue>,
        reply: oneshot::Sender<Result<i64>>,
    },
    /// Single-nullable-string-column query -- for lookups where "no row"
    /// is the normal, common case (e.g. `config_overrides` has no row for
    /// most keys, since most settings are never overridden) and must
    /// resolve to `Ok(None)`, not an `Err` indistinguishable from a real
    /// SQL/connection failure. `query_i64`/`query_row` both propagate
    /// `rusqlite`'s `QueryReturnedNoRows` as an `Err` (via
    /// `Connection::query_row`), which is correct for their own callers
    /// (every existing caller's query always expects exactly one row, via
    /// `COALESCE(...)`/aggregate SQL) but wrong for this one.
    QueryOptionalString {
        sql: &'static str,
        params: Vec<SqlValue>,
        reply: oneshot::Sender<Result<Option<String>>>,
    },
    /// Type-erased job for [`DbHandle::query_row`]. Rust enums can't carry
    /// a generic variant, so the job closure captures its own (typed)
    /// oneshot reply sender and does the query + reply-send internally;
    /// the actor loop just calls it with the connection. This is the
    /// `QueryRow<T>` extension point Task 1's doc comment flagged ("extend
    /// with a `QueryRow<T>` variant if a later task needs more than one
    /// column") -- Task 5 is that later task (`get_closed_channels_summary`
    /// is a 9-column aggregate).
    ///
    /// `+ Sync` (in addition to `+ Send`) keeps `Command` itself `Sync`, so
    /// `mpsc::error::SendError<Command>` stays usable with
    /// `anyhow::Context` the same way it already was for the other two
    /// variants -- dropping it would silently break `.context("actor
    /// gone")` on every variant, not just this one.
    Exec(Box<dyn FnOnce(&Connection) + Send + Sync>),
}

/// Cheap, `Clone`-able handle to the actor task. Cloning just clones the
/// `mpsc::Sender`; the underlying `Connection` stays pinned to the one
/// owning task.
#[derive(Clone, Debug)]
pub struct DbHandle {
    tx: mpsc::Sender<Command>,
}

impl DbHandle {
    /// Number of tables in the database (`sqlite_master` count), resolved
    /// live at call time rather than snapshotted once at init -- so a
    /// caller (e.g. `revenue-r-status`) always reports the DB's current
    /// state.
    pub async fn table_count(&self) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::TableCount(reply))
            .await
            .context("actor gone")?;
        rx.await.context("actor dropped reply")?
    }

    /// Single-i64-column query -- covers every aggregate this phase's read
    /// RPCs need (`SUM(...)`, `COUNT(*)`). Extend with a `QueryRow<T>`
    /// variant if a later task needs more than one column; keep this one
    /// narrow rather than over-generalizing ahead of a real second caller
    /// shape.
    pub async fn query_i64(&self, sql: &'static str, params: Vec<SqlValue>) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::QueryI64 { sql, params, reply })
            .await
            .context("actor gone")?;
        rx.await.context("actor dropped reply")?
    }

    /// Single-nullable-string-column query. `Ok(None)` means "zero rows
    /// matched" (the normal case for a lookup like `config_overrides` on an
    /// un-overridden key) -- see [`Command::QueryOptionalString`]'s doc
    /// comment for why this needs its own primitive rather than reusing
    /// `query_i64`/`query_row`.
    pub async fn query_optional_string(
        &self,
        sql: &'static str,
        params: Vec<SqlValue>,
    ) -> Result<Option<String>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::QueryOptionalString { sql, params, reply })
            .await
            .context("actor gone")?;
        rx.await.context("actor dropped reply")?
    }

    /// Single-row, multi-column query with a caller-supplied row mapper --
    /// the general form `query_i64` deliberately stayed narrower than (see
    /// its doc comment). Needed for aggregates that select more than one
    /// column in a single statement, e.g. `get_closed_channels_summary`'s
    /// 9-column `SELECT`.
    ///
    /// Running that as ONE statement (rather than nine separate
    /// `query_i64` calls, one per column) matters beyond convenience: it
    /// mirrors Python's own single-`.execute()` atomicity. Production's DB
    /// can be concurrently written by the Python plugin under WAL; nine
    /// independent round trips could each land on a different WAL
    /// snapshot if a write commits in between, producing a combination of
    /// values Python's one atomic read could never have produced. A single
    /// `query_row` call takes one snapshot for every column.
    pub async fn query_row<T, F>(
        &self,
        sql: &'static str,
        params: Vec<SqlValue>,
        map: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: Fn(&Row) -> rusqlite::Result<T> + Send + Sync + 'static,
    {
        let (reply, rx) = oneshot::channel::<Result<T>>();
        let job: Box<dyn FnOnce(&Connection) + Send + Sync> = Box::new(move |conn: &Connection| {
            let param_refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
            let result = conn
                .query_row(sql, param_refs.as_slice(), |r| map(r))
                .context("query_row");
            let _ = reply.send(result);
        });
        self.tx
            .send(Command::Exec(job))
            .await
            .context("actor gone")?;
        rx.await.context("actor dropped reply")?
    }
}

/// Open the database read-only and spawn the single-owner actor task that
/// owns the resulting `Connection` for the rest of the plugin's lifetime.
///
/// Errors (missing file, `PRAGMA`/open failure, or a schema-listing
/// failure) propagate synchronously to the caller before any task is
/// spawned -- same fail path `main.rs` already uses for a bad `db-path`
/// (`configured.disable(...)`), just with the connection's ownership
/// moving into the actor task on success instead of being dropped after
/// one probe.
pub async fn spawn_read_only(path: &Path) -> Result<DbHandle> {
    // Open (and validate) on the CALLER's task first so a bad path fails
    // plugin init synchronously, exactly like Phase 1a's probe-drop did --
    // only the *ownership* of the connection moves into the actor task.
    let conn = crate::open_read_only(path)?;
    // Probe `table_names` synchronously here, before the actor task is
    // spawned and before we return `Ok`. A file can open fine (SQLite's
    // open is lazy -- it doesn't read the header until the first query)
    // and still fail to list tables (corrupt `sqlite_master`, a lock we
    // can't get past `busy_timeout`, etc). Deferring that check to
    // request time -- as `DbHandle::table_count` used to be the only
    // caller of `table_names` -- let a schema-listing failure slip past
    // `main.rs`'s open-failure handling entirely (the `.ok()` at the
    // `revenue-r-status` call site swallowed it), silently dropping the
    // default-vs-explicit leniency split commit 126f391 added for this
    // exact class of failure. Probing here instead means a schema-listing
    // failure returns `Err` from `spawn_read_only` itself, so it goes
    // through the *same* default-path-miss/explicit-path-miss branches in
    // `main.rs` that an open failure already does.
    crate::table_names(&conn).context("initial table_names probe")?;
    let (tx, mut rx) = mpsc::channel::<Command>(32);
    tokio::task::spawn_blocking(move || {
        // rusqlite::Connection is !Sync; owning it inside one blocking
        // task and only ever touching it from this thread is what makes
        // the single-owner-actor pattern sound here.
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                Command::TableCount(reply) => {
                    let result = crate::table_names(&conn).map(|v| v.len());
                    let _ = reply.send(result);
                }
                Command::QueryI64 { sql, params, reply } => {
                    let result = run_query_i64(&conn, sql, &params);
                    let _ = reply.send(result);
                }
                Command::QueryOptionalString { sql, params, reply } => {
                    let result = run_query_optional_string(&conn, sql, &params);
                    let _ = reply.send(result);
                }
                Command::Exec(job) => job(&conn),
            }
        }
    });
    Ok(DbHandle { tx })
}

fn run_query_i64(conn: &Connection, sql: &str, params: &[SqlValue]) -> Result<i64> {
    let param_refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    conn.query_row(sql, param_refs.as_slice(), |r: &Row| r.get(0))
        .context("query_i64")
}

fn run_query_optional_string(
    conn: &Connection,
    sql: &str,
    params: &[SqlValue],
) -> Result<Option<String>> {
    let param_refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    conn.query_row(sql, param_refs.as_slice(), |r: &Row| r.get::<_, String>(0))
        .optional()
        .context("query_optional_string")
}
