# Phase 1b: Persistent Observer, Notification Ingestion, Typed Config, Read RPCs — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close out Phase 1 (design spec `docs/superpowers/specs/2026-07-16-rust-port-design.md`) so the expedited Jul-19 gate — "field-for-field RPC parity and ingestion parity vs Python on lnnode via the diff harness" — is checkable. Phase 1a shipped a loadable plugin, money-math/canonical-JSON/SCID foundations, a probe-and-drop read-only DB open, the full 119-option shadow surface, and skeleton `revenue-r-status`/`revenue-r-config` RPCs. Phase 1b turns the DB probe into a persistent connection, ingests the four Python notifications into the Rust plugin's own database (never production), fixes the two known Phase 1a parity debts (config error string, canonical db-path default), ships the read-RPC subset the diff harness needs (`revenue-r-history`, `-report`, `-dashboard`), and writes the deploy runbook.

**Carried obligations (progress ledger, `.superpowers/sdd/progress.md` line 15):** typed `revenue-r-config` w/ "Unknown config key" capital-U parity, canonical db-path default from fixture, WAL concurrent-writer + cold-start tests, notification ingestion + hydration, persistent DB actor. All five are covered below (Tasks 1–4).

**Architecture:** Builds on the existing workspace (`crates/revops` bin, `revops-core`, `revops-db`, `revops-rpc`, `revops-econ` libs). No new crates — `revops-db` gains an `actor` module (single-owner mpsc task, replacing Task 8's open-count-drop) and a `notifications` module (the Rust plugin's own writable sqlite file); `crates/revops` gains `rpc_history.rs`, `rpc_report.rs`, `rpc_dashboard.rs` (pure response builders, same pattern as the existing `rpc_status.rs`), a `config_types.rs` typed-value module, and a `notify.rs` subscription-handler module.

**Tech Stack:** unchanged from Phase 1a (Rust stable, cln-plugin 0.7.0, tokio, rusqlite system-linked, serde_json). This phase adds `cln-rpc` (or a hand-rolled unix-socket JSON-RPC client if the crate's `getmanifest`-time constraints make it awkward inside `cln-plugin`'s init closure — see Task 2 self-review) for the one live call Phase 1b genuinely needs: paged `listforwards` for startup hydration.

## Global Constraints

- Rust plugin NEVER writes the production DB. Every new write path in this phase targets the Rust plugin's own db file (new option, see Task 2). If Task 2's own-db path is unset, notification ingestion is a no-op (log at debug, drop the event) — never silently fall through to the production connection.
- Error strings, JSON shapes, and rounding must match Python byte-for-byte where a fixture exists; where Phase 1b cannot compute a Python field because its source module isn't ported yet, the field is `null` and its key is listed in a `_phase1b_gaps` array — never a fabricated or silently-zero value. This is the plan's explicit "no silent stubs" contract; the diff harness treats gapped keys as skip-list entries (extending `SKIP_KEYS` in `tools/diff-harness/diff_config.py`'s sibling scripts), not parity failures.
- `rusqlite` stays system-linked (no `bundled` feature) on every connection this phase adds, including the new own-db writer.
- All new modules: `#![forbid(unsafe_code)]` at crate root (already in place), clippy warnings deny in CI.
- Fixture generators live in the Python `port` worktree (`~/bin/cl_revenue_ops-port` @ branch `port`) under `tools/port/`, committed there; generated fixtures copied into `fixtures/` here.
- Python source of truth: `~/bin/cl_revenue_ops-port/cl-revenue-ops.py` + `modules/database.py`, `modules/config.py`, `modules/profitability_analyzer.py` @ branch `port` (v2.18.1).
- Do not commit. Per the calling controller's instructions, this plan file is written but not committed — the controller commits.

---

### Task 1: Persistent DB actor (single-owner mpsc task) + WAL/cold-start integration tests

**Parallel-safety:** Foundational — must land before Tasks 2 and 5 (both consume the actor). Safe to run alone; touches `revops-db` (new files) and `crates/revops/src/main.rs` (replaces the Task 8 probe-drop block). No other task should start against the same `main.rs` region until this merges.

**Files:**
- Create: `crates/revops-db/src/actor.rs`
- Create: `crates/revops-db/tests/actor_wal.rs`
- Modify: `crates/revops-db/src/lib.rs` (export `actor`)
- Modify: `crates/revops/src/main.rs` (replace the open-count-drop block with actor spawn; `State` gains `db: Option<revops_db::actor::DbHandle>` instead of `db_tables: Option<usize>` computed once)
- Modify: `crates/revops/src/rpc_status.rs` (`StatusInputs.db_tables` becomes async-resolved via the handle at call time instead of an init-time snapshot — see Step 4)

**Interfaces:**
- `revops_db::actor::DbHandle` — `Clone`, cheap (wraps an `mpsc::Sender<Command>`). `async fn table_count(&self) -> anyhow::Result<usize>`; `async fn query_row<T>(&self, sql: &'static str, params: Vec<rusqlite::types::Value>, map: fn(&rusqlite::Row) -> rusqlite::Result<T>) -> anyhow::Result<T>` (generic enough for Task 5's read queries; `T: Send + 'static`).
- `revops_db::actor::spawn_read_only(path: &Path) -> anyhow::Result<DbHandle>` — opens via the existing `open_read_only`, then spawns a `tokio::task` owning the `Connection` (which is `!Sync`, hence the actor: the connection never crosses a task boundary, only messages do) and services `Command`s off an `mpsc::channel(32)` until every `DbHandle` clone drops (channel closes, task exits cleanly — no explicit shutdown message needed for a read-only actor).
- Errors from `spawn_read_only` (missing file, `PRAGMA` failure) propagate synchronously to the caller — same fail path `main.rs` already uses (`configured.disable(...)`).

- [ ] **Step 1: Failing cold-start test**

```rust
// crates/revops-db/tests/actor_wal.rs
use revops_db::actor::spawn_read_only;
use std::path::Path;

#[tokio::test]
async fn cold_start_before_writer_fails_gracefully() {
    // No file has ever been created at this path -- mirrors an operator
    // pointing revops-r-db-path at a DB the Python plugin hasn't
    // initialized yet (or a typo'd path). Must be a clean Err, never a
    // panic that would crash plugin init.
    let err = spawn_read_only(Path::new("/nonexistent/cl-revops-phase1b/nope.db"))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not found") || err.to_string().contains("database"));
}
```

- [ ] **Step 2: Failing concurrent-writer test**

```rust
// same file, crates/revops-db/tests/actor_wal.rs
use rusqlite::Connection;
use std::time::Duration;

fn init_wal_db(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; \
         CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);",
    )
    .unwrap();
}

#[tokio::test]
async fn reader_sees_only_committed_data_while_writer_holds_open_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.db");
    init_wal_db(&path);

    // Writer opens its own connection (simulating the Python plugin's
    // writer thread) and holds an uncommitted BEGIN IMMEDIATE across the
    // whole test body.
    let mut writer = Connection::open(&path).unwrap();
    writer.busy_timeout(Duration::from_millis(5000)).unwrap();
    let tx = writer.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate).unwrap();
    tx.execute("INSERT INTO t (v) VALUES ('uncommitted')", []).unwrap();

    // Reader (our actor) attaches WHILE the writer transaction is still
    // open and uncommitted.
    let handle = spawn_read_only(&path).await.expect("reader attaches under WAL");
    let count_before: i64 = handle
        .query_row("SELECT COUNT(*) FROM t", vec![], |r| r.get(0))
        .await
        .unwrap();
    // WAL snapshot isolation: the reader must NOT see the writer's
    // uncommitted row -- this is the property that makes read-only
    // coexistence with Python's writer safe.
    assert_eq!(count_before, 0, "reader saw an uncommitted write");

    tx.commit().unwrap();
    drop(writer);

    let count_after: i64 = handle
        .query_row("SELECT COUNT(*) FROM t", vec![], |r| r.get(0))
        .await
        .unwrap();
    assert_eq!(count_after, 1, "reader didn't pick up the committed write");
}
```

Add `tempfile` as a dev-dependency of `revops-db`.

- [ ] **Step 3: Run, verify both fail** (module doesn't exist yet) — `cargo test -p revops-db --test actor_wal`.

- [ ] **Step 4: Implement the actor**

```rust
// crates/revops-db/src/actor.rs
use anyhow::{Context, Result};
use rusqlite::{types::Value as SqlValue, Connection, Row};
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

type BoxedMap<T> = Box<dyn Fn(&Row) -> rusqlite::Result<T> + Send>;

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
}

#[derive(Clone)]
pub struct DbHandle {
    tx: mpsc::Sender<Command>,
}

impl DbHandle {
    pub async fn table_count(&self) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Command::TableCount(reply)).await.context("actor gone")?;
        rx.await.context("actor dropped reply")?
    }

    /// Single-i64-column query -- covers every aggregate in Task 5
    /// (`SUM(...)`, `COUNT(*)`), which is all this phase's read RPCs need.
    /// Extend with a `QueryRow<T>` variant if a later task needs more than
    /// one column; keep this one narrow rather than over-generalizing
    /// ahead of a real second caller shape.
    pub async fn query_i64(&self, sql: &'static str, params: Vec<SqlValue>) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::QueryI64 { sql, params, reply })
            .await
            .context("actor gone")?;
        rx.await.context("actor dropped reply")?
    }
}

pub async fn spawn_read_only(path: &Path) -> Result<DbHandle> {
    // Open (and validate) on the CALLER's task first so a bad path fails
    // plugin init synchronously, exactly like Phase 1a's probe-drop did --
    // only the *ownership* of the connection moves into the actor task.
    let conn = crate::open_read_only(path)?;
    let (tx, mut rx) = mpsc::channel::<Command>(32);
    tokio::task::spawn_blocking(move || {
        // rusqlite::Connection is !Sync; owning it inside one blocking
        // task and only ever touching it from this thread is what makes
        // the single-owner-actor pattern sound here (mirrors the design
        // spec's "single-owner actor tasks (mpsc) where Python held one
        // lock across a whole cycle").
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
            }
        }
    });
    Ok(DbHandle { tx })
}

fn run_query_i64(conn: &Connection, sql: &str, params: &[SqlValue]) -> Result<i64> {
    let param_refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    conn.query_row(sql, param_refs.as_slice(), |r| r.get(0))
        .context("query_i64")
}
```

(If `spawn_blocking` + a `blocking_recv` loop doesn't fit the crate's async story cleanly, a plain `std::thread::spawn` with a `std::sync::mpsc` channel bridged via `tokio::task::spawn_blocking` on the CALLER side per-request is an acceptable fallback — the contract this task pins is single-owner-never-crosses-a-task, not the exact tokio primitive.)

- [ ] **Step 5: Run, verify pass** — `cargo test -p revops-db --test actor_wal`.

- [ ] **Step 6: Wire into `main.rs`**, replacing the Task 8 block (`match db_path_setting { Some(raw) => { ... open_read_only ... drop(conn) ... } }`) with:

```rust
let db: Option<revops_db::actor::DbHandle> = match &db_path_setting {
    Some(raw) => match revops_db::actor::spawn_read_only(&PathBuf::from(raw)).await {
        Ok(handle) => Some(handle),
        Err(e) => {
            configured
                .disable(&format!("{db_path_name} set but DB actor spawn failed: {e}"))
                .await?;
            return Ok(());
        }
    },
    None => None,
};
```

`State.db_tables: Option<usize>` becomes `State.db: Option<revops_db::actor::DbHandle>`; `revenue-r-status` resolves `db_tables` by calling `db.table_count().await` at request time instead of reading an init-time field. Update `StatusInputs` and its one call site accordingly; `crates/revops/tests/status.rs`'s `build_status` unit test is unaffected (it already takes `db_tables: Option<usize>` as a plain input — only the caller in `main.rs` changes how it's obtained).

- [ ] **Step 7: fmt + clippy + full suite; commit** (controller commits per plan header — do NOT run `git commit` from inside this task).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`.

---

### Task 2: Notification ingestion into the Rust plugin's own DB + startup hydration

**Parallel-safety:** Depends on Task 1 (reuses `revops_db::actor` — this task adds a second, read-write actor variant for the plugin's own db). Touches `revops-db` (new `notifications.rs`) and `crates/revops/src/main.rs` (adds 4 `.subscribe(...)` registrations + a new `observer-db-path` option). Can run in parallel with Task 5 once Task 1 has merged, but both land subscriptions/RPCs into `main.rs` — coordinate on rebase order, don't block on it (files touched are disjoint at the function level).

**Files:**
- Create: `tools/port/gen_hydration_fixtures.py` (port worktree)
- Create: `fixtures/hydration.json`
- Create: `crates/revops-db/src/notifications.rs`
- Create: `crates/revops-db/tests/notifications.rs`
- Modify: `crates/revops-db/src/actor.rs` (add `spawn_read_write` alongside `spawn_read_only`, or a small sibling `owner.rs` — see Step 4)
- Modify: `crates/revops-db/src/lib.rs`
- Create: `crates/revops/src/notify.rs`
- Modify: `crates/revops/src/main.rs` (register `observer-db-path` option, 4 subscriptions, startup hydration call)

**Interfaces:**
- `revops_db::notifications::init_schema(conn: &Connection) -> Result<()>` — idempotent `CREATE TABLE IF NOT EXISTS` for the plugin's own db: an `ingested_forwards` table mirroring production's dedup shape exactly (`in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time` + the same `UNIQUE INDEX` on those seven columns, per `fixtures/schema.sql` lines 56–70) plus `peer_connection_events (peer_id, event_type, ts)` and `channel_closure_events (scid, cause, ts)` — deliberately narrower than production's full closure-accounting schema; Phase 1b only needs dedup + a hydration anchor, not the bookkeeper-cost enrichment (that's Phase 3/6 territory, tracked as a gap, not built here).
- `revops_db::notifications::insert_forward_ignore_dup(conn: &Connection, f: &ForwardRow) -> Result<bool>` — `INSERT OR IGNORE`, returns whether a row was actually inserted (for tests).
- `revops_db::notifications::last_forward_ts(conn: &Connection) -> Result<Option<i64>>` — `SELECT MAX(timestamp) FROM ingested_forwards`.
- `revops_db::notifications::compute_forward_hydration_start(last_forward_ts: Option<i64>, flow_window_days: i64, now: i64) -> Option<i64>` — direct port of `_compute_forward_hydration_start` (cl-revenue-ops.py:602-625): empty table → `now - max(flow_window_days, 14) * 86400`; non-empty with `gap <= 300` (`FORWARD_HYDRATION_EVENT_JITTER_SECONDS`) → `None`; else `max(last_forward_ts - 86400, now - max(flow_window_days + 1, 15) * 86400)`.
- `crates/revops/src/notify.rs::on_forward_event(handle: &DbHandle, event: &serde_json::Value) -> ()` — infallible (matches Python's top-level `try/except` — a Rust panic in a subscription handler must not propagate; wrap the body in `std::panic::catch_unwind` or keep every fallible step as a `Result` swallowed to a `tracing`/`plugin.log` warning, never a `?` that unwinds through the notification callback).

- [ ] **Step 1: Fixture generator against Python truth**

```python
# tools/port/gen_hydration_fixtures.py  (run from ~/bin/cl_revenue_ops-port)
import json, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import importlib.util
spec = importlib.util.spec_from_file_location("revops_main", "cl-revenue-ops.py")
# _compute_forward_hydration_start has no external deps beyond `time` --
# safe to exec the module for just this one function via AST extraction
# instead of a full plugin import (which needs pyln stubs). Simplest robust
# path: re-implement the call by importing the whole file with pyln mocked,
# matching gen_schema_fixture.sh's approach.
from unittest.mock import MagicMock
sys.modules.setdefault("pyln", MagicMock())
sys.modules.setdefault("pyln.client", MagicMock())
import cl_revenue_ops  # if the file isn't importable as-is, exec() it directly instead
fn = cl_revenue_ops._compute_forward_hydration_start

NOW = 1_800_000_000
CASES = [
    {"last_forward_ts": None, "flow_window_days": 7, "now": NOW},
    {"last_forward_ts": None, "flow_window_days": 30, "now": NOW},
    {"last_forward_ts": NOW - 100, "flow_window_days": 7, "now": NOW},       # within jitter -> None
    {"last_forward_ts": NOW - 300, "flow_window_days": 7, "now": NOW},       # exactly at boundary -> None
    {"last_forward_ts": NOW - 301, "flow_window_days": 7, "now": NOW},       # just over -> backfill
    {"last_forward_ts": NOW - 10 * 86400, "flow_window_days": 7, "now": NOW},
    {"last_forward_ts": NOW - 100 * 86400, "flow_window_days": 7, "now": NOW},  # floor clamps it
]
out = [{**c, "result": fn(c["last_forward_ts"], c["flow_window_days"], c["now"])} for c in CASES]
json.dump(out, open(sys.argv[1], "w"), indent=1)
```

Run it, commit the generator to `port`, copy `fixtures/hydration.json` here.

(**Before finalizing**: if importing `cl-revenue-ops.py` as a module fails for reasons beyond pyln stubbing, fall back to `ast`-extracting just the `_compute_forward_hydration_start` function body and `exec`-ing it in isolation with a stub `time.time` — the fixture's job is pinning input/output pairs, not proving the whole file imports.)

- [ ] **Step 2: Failing parity test**

```rust
// crates/revops-db/tests/notifications.rs (hydration half)
use revops_db::notifications::compute_forward_hydration_start;

#[test]
fn hydration_start_matches_python() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/hydration.json")).unwrap();
    for c in cases.as_array().unwrap() {
        let last = c["last_forward_ts"].as_i64();
        let flow_window_days = c["flow_window_days"].as_i64().unwrap();
        let now = c["now"].as_i64().unwrap();
        let expected = c["result"].as_i64();
        assert_eq!(
            compute_forward_hydration_start(last, flow_window_days, now),
            expected,
            "case={c:?}"
        );
    }
}

// dedup half
use revops_db::notifications::{init_schema, insert_forward_ignore_dup, last_forward_ts, ForwardRow};
use rusqlite::Connection;

fn sample() -> ForwardRow {
    ForwardRow {
        in_channel: "1x1x0".into(), out_channel: "2x2x0".into(),
        in_msat: 100_000, out_msat: 99_000, fee_msat: 1_000,
        timestamp: 1_800_000_000, resolved_time: 1_800_000_005,
    }
}

#[test]
fn dedup_ignores_exact_duplicate_insert() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    assert!(insert_forward_ignore_dup(&conn, &sample()).unwrap(), "first insert");
    assert!(!insert_forward_ignore_dup(&conn, &sample()).unwrap(), "dup must be ignored");
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM ingested_forwards", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn hydration_and_live_insert_race_safely() {
    // Simulates the exact scenario the design doc calls out: startup
    // hydration and a live forward_event for the SAME forward can both
    // attempt an insert. Both must succeed at the DB layer (no error),
    // and the row count must still be 1.
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    let row = sample();
    insert_forward_ignore_dup(&conn, &row).unwrap();
    insert_forward_ignore_dup(&conn, &row).unwrap(); // "hydration" reinserting what "live" already wrote
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM ingested_forwards", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 1);
    assert_eq!(last_forward_ts(&conn).unwrap(), Some(1_800_000_000));
}
```

- [ ] **Step 3: Run, verify FAIL** (module missing).

- [ ] **Step 4: Implement `notifications.rs`**

```rust
// crates/revops-db/src/notifications.rs
use anyhow::Result;
use rusqlite::{params, Connection};

pub struct ForwardRow {
    pub in_channel: String,
    pub out_channel: String,
    pub in_msat: i64,
    pub out_msat: i64,
    pub fee_msat: i64,
    pub timestamp: i64,       // received_time, matching production's column name
    pub resolved_time: i64,
}

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

/// Returns true if a new row was inserted, false if it was a dedup no-op.
pub fn insert_forward_ignore_dup(conn: &Connection, f: &ForwardRow) -> Result<bool> {
    let changed = conn.execute(
        "INSERT OR IGNORE INTO ingested_forwards
             (in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![f.in_channel, f.out_channel, f.in_msat, f.out_msat, f.fee_msat, f.timestamp, f.resolved_time],
    )?;
    Ok(changed == 1)
}

pub fn last_forward_ts(conn: &Connection) -> Result<Option<i64>> {
    Ok(conn.query_row("SELECT MAX(timestamp) FROM ingested_forwards", [], |r| r.get(0))?)
}

const FORWARD_HYDRATION_EVENT_JITTER_SECONDS: i64 = 300;

/// Direct port of cl-revenue-ops.py:602-625 `_compute_forward_hydration_start`.
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
```

- [ ] **Step 5: Run, verify PASS.**

- [ ] **Step 6: Own-db actor + subscriptions wiring in `main.rs`.**

Add a new option (own db, no Python analog — no collision risk, so it does not need the shadow/canonical dance beyond the existing `opt_name()` helper for naming consistency):

```rust
let observer_db_name = opt_name("observer-db-path");
let observer_db_opt = DefaultStringConfigOption::new_str_with_default(
    &observer_db_name,
    "~/.lightning/revops-r-observer.db",
    "Path to the Rust plugin's OWN sqlite file (read-write). Never the production DB.",
);
```

At init (after `configured.start`-equivalent point where the DB path resolves), open/spawn a **read-write** actor over this path (add `revops_db::actor::spawn_read_write` — same shape as `spawn_read_only` but via `Connection::open` instead of `open_read_only`, running `notifications::init_schema` once before the command loop starts), release stale connections aren't a concern here (WAL, single writer = us). Register the four subscriptions:

```rust
.subscribe("forward_event", {
    let observer_db = observer_db.clone();
    move |plugin, v| { let observer_db = observer_db.clone(); async move {
        notify::on_forward_event(&observer_db, &v).await;
        Ok(serde_json::json!("continue"))
    }}
})
.subscribe("connect", { /* notify::on_connect, same shape */ })
.subscribe("disconnect", { /* notify::on_disconnect */ })
.subscribe("channel_state_changed", { /* notify::on_channel_state_changed */ })
```

(Adjust to `cln-plugin` 0.7.0's actual `subscribe` signature — Phase 1a's Task 1 self-review already flagged builder-API drift as an expected friction point; the tests are the contract.)

`notify.rs` handlers each: parse the minimal fields Task 2's own schema needs (`in_channel`/`out_channel`/`*_msat`/`received_time`/`resolved_time` for forward_event; `id`/`peer_id` for connect/disconnect per the three-method fallback at cl-revenue-ops.py:6822-6835; `short_channel_id` or `channel_id` + `cause` for channel_state_changed), and are individually wrapped so a parse failure logs and returns rather than panics — mirror the Python top-level `try/except` per handler, not one guard around all four.

Startup hydration: after the own-db actor is up, call `notifications::last_forward_ts` (through the actor), compute `compute_forward_hydration_start(last, flow_window_days_from_config, now)`, and if `Some(start)`, page `listforwards(status="settled", index="created", start=0, limit=1000)` via the live `cln-rpc` client (new: `crates/revops/src/hydration.rs`, thin — reuses the paging/fallback contract at cl-revenue-ops.py:632-670: any RPC error or missing `created_index` aborts hydration for this boot with a warn log, dedup at the DB layer remains the correctness backstop, so a partial/aborted hydration is safe, never wrong) inserting each row via `insert_forward_ignore_dup`.

- [ ] **Step 7: fmt + clippy + full suite.**

**Self-review flag:** this task is the first place Phase 1b needs a genuine live `cln-rpc` call (`listforwards`) rather than DB-only reads. If wiring a full `cln-rpc` client inside `cln-plugin`'s init proves awkward (the crate may expect RPC calls only after `.start()` returns a running `Plugin`), split hydration into its own post-start `tokio::spawn`ed task rather than blocking `main`'s init path — either is acceptable; what's NOT acceptable is silently skipping hydration on difficulty. If genuinely blocked, land ingestion (Steps 1–6 minus hydration) and file the hydration gap explicitly in this plan's Self-Review Notes, not as a silent omission.

---

### Task 3: Typed `revenue-r-config` — capital-U error string, typed values, version/classification fields

**Parallel-safety:** Independent of Tasks 1/2/5. Touches `crates/revops/src/rpc_status.rs`, a new `crates/revops/src/config_types.rs`, `crates/revops/tests/config.rs`, and a small, disjoint region of `main.rs` (the `revenue-r-config` rpcmethod closure body). Safe to run in parallel with Task 4 (different functions in the same files — rebase, don't block).

**Files:**
- Create: `tools/port/gen_config_types_fixture.py` (port worktree)
- Create: `fixtures/config_types.json`
- Create: `crates/revops/src/config_types.rs`
- Modify: `crates/revops/src/rpc_status.rs` (`build_config_response` signature changes)
- Modify: `crates/revops/tests/config.rs`
- Modify: `crates/revops/src/main.rs` (config rpcmethod closure)

**Interfaces:**
- `config_types::load() -> HashMap<String, FieldType>` where `FieldType = Int | Float | Bool | String` — parsed from embedded `fixtures/config_types.json`, one entry per `Config` dataclass field (`modules/config.py:494-836`), keyed by the Python field name (underscored, e.g. `daily_budget_sats`), NOT the hyphenated option suffix (the diff harness's existing `diff_config.py` docstring already documents this exact two-namespace split — Task 3 reuses that mapping convention).
- `config_types::typed_value(field: &str, raw: &options::Value) -> serde_json::Value` — converts the `cln-plugin` resolved value (already a `String`/`i64`/`bool` per its own `opt_type`) into the JSON scalar shape Python would emit: for `FieldType::Int`/`Bool`, pass through if the option's own native type already matches; for a `FieldType::Float` Python field backed by a `string`-typed CLN option (several config fields are floats parsed from string options — confirm per-field from the fixture, not assumed), parse the string to `f64`. Fields with `FieldType::String` compare as-is.
- `rpc_status::build_config_response(key: &str, known: bool, value: Option<&options::Value>, field_type: Option<FieldType>, version: i64) -> Value` — gains `field_type` and `version` params. Known-key shape becomes `{"key", "value", "version", "classification"}` (Python's exact shape at cl-revenue-ops.py:5671-5679, MINUS the `warning` key — see gap note below). Unknown-key shape becomes `{"error": "Unknown config key: {key}"}` (capital U, exact substring match).
- `rpc_status::classify_runtime_key(key: &str) -> &'static str` — port of `Config.classify_runtime_key` (modules/config.py:898-905): `"public"` if in the public-runtime-keys fixture set, `"deprecated"` if in a deprecated-keys fixture set (both extracted alongside `config_types.json` — see Step 1), else `"internal"`.

**Gap note (explicit, not silent):** Python's `revenue-config get <key>` on a non-public key still returns the value plus a `warning` key (`_not_public_error`, cl-revenue-ops.py:5677-5678); a live per-key **version** in Python is the DB-persisted `config._version`, incremented on writes through `revenue-config set` — Phase 1b's Rust side has no DB-backed override-write path yet (that's Phase 1b's option surface, read-only), so `version` here is a constant `0` until Task-2-and-later write support lands. Document this as a `_phase1b_gaps: ["version"]` entry in the response when `version` is the constant placeholder, so the diff harness's `--strict` mode (already scaffolded per its docstring) knows to skip that one key rather than flag a false mismatch.

- [ ] **Step 1: Fixture generator (AST field-type + public/deprecated key extraction)**

```python
# tools/port/gen_config_types_fixture.py  (run from ~/bin/cl_revenue_ops-port)
import ast, json, sys

tree = ast.parse(open("modules/config.py").read())
class_node = next(n for n in ast.walk(tree) if isinstance(n, ast.ClassDef) and n.name == "Config")

TYPE_MAP = {"int": "int", "float": "float", "bool": "bool", "str": "string"}
fields = {}
for stmt in class_node.body:
    if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
        ann = stmt.annotation
        name = ann.id if isinstance(ann, ast.Name) else None
        if name in TYPE_MAP:
            fields[stmt.target.id] = TYPE_MAP[name]

import modules.config as cfgmod
public_keys = sorted(cfgmod.PUBLIC_RUNTIME_KEYS)
deprecated_keys = sorted(getattr(cfgmod, "DEPRECATED_RUNTIME_KEYS", []))

json.dump(
    {"fields": fields, "public_keys": public_keys, "deprecated_keys": deprecated_keys},
    open(sys.argv[1], "w"), indent=1,
)
print(f"{len(fields)} typed fields, {len(public_keys)} public, {len(deprecated_keys)} deprecated -> {sys.argv[1]}")
```

Run, commit generator to `port`, copy `fixtures/config_types.json` here.

- [ ] **Step 2: Failing tests**

```rust
// crates/revops/tests/config.rs — extend existing file
use revops::config_types::{classify_runtime_key, load, typed_value, FieldType};

#[test]
fn unknown_key_error_is_capital_u() {
    let v = build_config_response("nope", false, None, None, 0);
    assert_eq!(v["error"], "Unknown config key: nope"); // was lowercase in Phase 1a
}

#[test]
fn known_key_shape_has_version_and_classification() {
    let v = build_config_response("daily-budget-sats", true, Some(&options::Value::Integer(5000)), Some(FieldType::Int), 3);
    assert_eq!(v["key"], "daily-budget-sats");
    assert_eq!(v["value"], 5000);
    assert_eq!(v["version"], 3);
    assert_eq!(v["classification"], "public"); // daily_budget_sats IS in PUBLIC_RUNTIME_KEYS -- confirm against the fixture, adjust if not
}

#[test]
fn classify_matches_python_fixture() {
    let table = load();
    for key in &table.public_keys {
        assert_eq!(classify_runtime_key(key), "public", "key={key}");
    }
    for key in &table.deprecated_keys {
        assert_eq!(classify_runtime_key(key), "deprecated", "key={key}");
    }
    assert_eq!(classify_runtime_key("definitely_not_a_real_key_xyz"), "internal");
}
```

- [ ] **Step 3: Run → FAIL. Step 4: Implement `config_types.rs`, update `rpc_status.rs`, wire `main.rs`'s config closure to pass `field_type`/`version` through.**

- [ ] **Step 5: Run full suite → PASS; fmt/clippy clean.**

---

### Task 4: Canonical-mode db-path default from `fixtures/options.json`

**Parallel-safety:** Small, independent. Touches `crates/revops/tests/manifest.rs` (the deliberately-pinned assertion) and `crates/revops/src/main.rs` (the `db_path_opt` construction). Can run fully in parallel with Tasks 1–3, 5, 6.

**Files:**
- Modify: `crates/revops/tests/manifest.rs` (flip the pinned assertion, lines 159-168)
- Modify: `crates/revops/src/main.rs` (`db_path_opt` default becomes mode-dependent)

**Interfaces:** No new public interface — this is a targeted default-value fix per the design spec's db-path ruling (`docs/superpowers/specs/2026-07-16-rust-port-design.md` lines 78-87): in **shadow** mode the observer default stays `""` (no accidental DB probe when both plugins are loaded); in **canonical** mode (`REVOPS_CANONICAL_NAMES=1`, i.e. Python unloaded) the default becomes Python's own default, `~/.lightning/revenue_ops.db`, read from `fixtures/options.json`'s `revenue-ops-db-path` entry — because canonical mode means this Rust plugin IS the only plugin, and an operator relying on the option's default must still get DB access.

- [ ] **Step 1: Flip the pinned test** (it currently asserts `Some("")` in canonical mode and says in its own comment "1b must change this"):

```rust
// crates/revops/tests/manifest.rs — replace the final block of
// manifest_canonical_mode_advertises_revenue_ops_names with:
    let table_default = table
        .as_array().unwrap()
        .iter()
        .find(|o| o["name"] == "revenue-ops-db-path")
        .expect("fixture has revenue-ops-db-path")["default"]
        .as_str().unwrap().to_string();
    assert_eq!(
        db_path_opt["default"].as_str(),
        Some(table_default.as_str()),
        "canonical-mode db-path default must equal Python's fixture default: {db_path_opt:?}"
    );

// and add a companion shadow-mode assertion (new test) pinning "" stays
// the shadow default, so a future change can't accidentally flip BOTH:
#[test]
fn manifest_shadow_mode_db_path_default_stays_empty() {
    let result = manifest_with(false);
    let opts = result["options"].as_array().unwrap();
    let db_path_opt = opts.iter().find(|o| o["name"].as_str() == Some("revops-r-db-path")).unwrap();
    assert_eq!(db_path_opt["default"].as_str(), Some(""), "shadow default must stay opt-in-empty");
}
```

- [ ] **Step 2: Run → FAIL** (current code hardcodes `""` regardless of mode).

- [ ] **Step 3: Implement** — in `main.rs`, make the db-path option's default mode-dependent:

```rust
let db_path_default: String = if canonical_names() {
    options_table::load()
        .into_iter()
        .find(|o| o.name == "revenue-ops-db-path")
        .and_then(|o| as_string_default(&o.default))
        .unwrap_or_default()
} else {
    String::new()
};
let db_path_opt = DefaultStringConfigOption::new_str_with_default(
    &db_path_name,
    &db_path_default,
    "Path to the revops sqlite database, opened read-only at init (empty = disabled)",
);
```

- [ ] **Step 4: Run full suite → PASS; fmt/clippy clean.**

**Self-review note:** this task deliberately does NOT change shadow-mode behavior — coexistence safety (Rust never touches the DB unless an operator opts in while Python is still the writer) is unaffected. Only the canonical (post-cutover, Python-unloaded) default changes, per the spec ruling.

---

### Task 5: Read RPC subset — `revenue-r-history`, `revenue-r-report`, `revenue-r-dashboard`

**Parallel-safety:** Depends on Task 1 (uses `revops_db::actor::DbHandle` for live queries against the production DB). Independent of Tasks 2/3/4 at the file level (new files `rpc_history.rs`/`rpc_report.rs`/`rpc_dashboard.rs`, new `revops-db` query functions, new `main.rs` rpcmethod registrations in a disjoint block from Task 2's subscriptions and Task 3's config closure). Can run in parallel with Task 2 once Task 1 merges.

**Investigated (read `modules/profitability_analyzer.py` + `modules/database.py` on the port worktree) — per-handler DB-backed vs. gap split:**

| RPC | Python source | Phase 1b status |
|---|---|---|
| `revenue-history` | `profitability_analyzer.get_lifetime_report()` → `database.get_lifetime_stats()` + `database.get_closed_channels_summary()` | **Fully portable.** Both are plain SQL aggregates over `forwards`, `daily_forwarding_stats`, `rebalance_costs`, `channel_costs`, `channel_closure_costs`, `lifetime_aggregates`, `closed_channels` — no other module involved. Byte-parity target. |
| `revenue-report costs` | `database.get_closure_costs_since()` ×3 windows + `database.get_total_closure_costs()` | **Fully portable** (plain SQL). |
| `revenue-report summary` / `policies` / `peer` | `policy_manager.get_all_policies()` / `.get_policy()`, `profitability_analyzer.get_profitability_by_peer()` | **Gap.** `policy_manager` is Phase 3 scope (governed econ / policy layer, per the design spec's phase list). Phase 1b returns an explicit not-yet-ported error (see contract below), NOT a fabricated policy count. The existing Python "unknown report type" contract (`{"error": "Unknown report type: {t}..."}`) is preserved verbatim for truly-unknown types. |
| `revenue-dashboard` `period.*` and `financial_health.net_profit_sats`/`operating_margin_pct` | `profitability_analyzer.get_pnl_summary()` → `database.get_total_routing_revenue/_total_volume_since/_total_forward_count_since/_total_rebalance_fees/_get_closure_costs_since` | **Fully portable** (plain SQL, same pattern as `revenue-history`). |
| `revenue-dashboard` `financial_health.tlv_sats`/`annualized_roc_pct`, `warnings`/`bleeder_count` | `profitability_analyzer.get_tlv()` (needs live `listfunds` + `listpeerchannels` on-chain/local-balance sums), `.calculate_roc()` (needs live channel capacity sum), `.identify_bleeders()` (needs live channel list + per-channel sourced/direct P&L attribution) | **Gap.** None of these are DB-only: `get_tlv`/`calculate_roc` need a live RPC call this phase deliberately doesn't wire (Task 5 is DB-only per its own scope — a live `cln-rpc` client for `listfunds`/`listpeerchannels` is Task 2's hydration-only carve-out, not generalized here), and `identify_bleeders` additionally needs `profitability_analyzer`'s sourced-fee-contribution logic (Phase 3). Returned as JSON `null` with keys listed in `_phase1b_gaps`. |

**Files:**
- Create: `tools/port/gen_read_rpc_fixtures.py` (port worktree)
- Create: `fixtures/read_rpc.json`, `fixtures/read_rpc_fixture.db` (small populated DB, committed binary — same pattern as Phase 1a's `fixtures/fixture.db`)
- Modify: `crates/revops-db/src/lib.rs` (new query functions)
- Create: `crates/revops/src/rpc_history.rs`, `crates/revops/src/rpc_report.rs`, `crates/revops/src/rpc_dashboard.rs`
- Modify: `crates/revops/src/main.rs` (register `revenue-r-history`, `-report`, `-dashboard`)
- Create: `crates/revops/tests/read_rpcs.rs`

**Interfaces:**
- `revops_db::lifetime_stats(handle: &DbHandle) -> Result<LifetimeStats>` where `LifetimeStats { total_revenue_msat, total_rebalance_cost_sats, total_opening_cost_sats, total_closure_cost_sats, total_forwards }` — the exact SQL from `modules/database.py:6018-6087` (`get_lifetime_stats`), ported statement-for-statement (pruned aggregates + daily rollup + current-day forwards, excluding today per the boundary-day fix comment).
- `revops_db::closed_channels_summary(handle: &DbHandle) -> Result<ClosedChannelsSummary>` — port of `get_closed_channels_summary` (database.py:6495-6526), one `SELECT ... FROM closed_channels` aggregate.
- `revops_db::pnl_summary(handle: &DbHandle, window_days: i64) -> Result<PnlSummary>` — port of `get_pnl_summary` (profitability_analyzer.py:1441-1498) composed from `revops_db::total_routing_revenue_msat`, `total_volume_sats_since`, `total_forward_count_since`, `total_rebalance_fees_since`, `closure_costs_since` — each a direct SQL port of its database.py namesake (see exact statements below).
- `revops_db::closure_costs_windows(handle: &DbHandle, now: i64) -> Result<ClosureCostWindows>` — 24h/7d/30d/total, port of `get_closure_costs_since`/`get_total_closure_costs` (database.py:6319-6390).
- `rpc_history::build_history(stats: &LifetimeStats, closed: &ClosedChannelsSummary) -> Value` — pure builder; `lifetime_revenue_sats = revops_core::msat::base_to_sats_ceil(stats.total_revenue_msat)` (reuses Phase 1a's Task 2 rounding helper — ceiling, matching the Python comment "matches every other revenue report in this module"); totals, ROI percent (mirror Python's `round(x, 2)` — Rust: format to 2 decimals via `(x * 100.0).round() / 100.0`, confirm against fixture, banker's-rounding edge cases are out of scope for this integer-heavy handler but flag if the fixture reveals a `.5` boundary case).
- `rpc_report::build_report(report_type: &str, costs: Option<&ClosureCostWindows>) -> Value` — `"costs"` → full Python shape; `"summary"|"policies"|"peer"` → `{"error": "not_yet_ported", "report_type": t, "reason": "requires policy_manager (Phase 3)"}`; anything else → `{"error": format!("Unknown report type: {t}. Use 'summary', 'peer', 'policies', or 'costs'")}` (Python's exact string, cl-revenue-ops.py:5526).
- `rpc_dashboard::build_dashboard(window_days: i64, pnl: &PnlSummary) -> Value` — full `period.*` block populated; `financial_health.net_profit_sats`/`operating_margin_pct` populated from `pnl`; `financial_health.tlv_sats`/`annualized_roc_pct` → `null`; `warnings` → `[]`; `bleeder_count` → `null`; top-level `_phase1b_gaps: ["financial_health.tlv_sats", "financial_health.annualized_roc_pct", "warnings", "bleeder_count"]`.

- [ ] **Step 1: Fixture generator** — populate a small fixture DB (extend Phase 1a's `gen_schema_fixture.sh` pattern: init via `Database(...).initialize()`, then insert synthetic rows into `forwards`, `rebalance_costs`, `channel_costs`, `channel_closure_costs`, `closed_channels`, `daily_forwarding_stats`, `lifetime_aggregates`), then call `database.get_lifetime_stats()`, `.get_closed_channels_summary()`, and `profitability_analyzer.get_pnl_summary(30)` (constructed directly against the same DB, no live RPC needed since `get_pnl_summary`'s only inputs are `self.database.*` calls) against it, dumping expected results to `fixtures/read_rpc.json` alongside the populated `fixtures/read_rpc_fixture.db`.

```python
# tools/port/gen_read_rpc_fixtures.py  (run from ~/bin/cl_revenue_ops-port)
import json, sys, os, time
from unittest.mock import MagicMock
sys.modules.setdefault("pyln", MagicMock()); sys.modules.setdefault("pyln.client", MagicMock())
sys.path.insert(0, os.getcwd())
from modules.database import Database
from modules.profitability_analyzer import ProfitabilityAnalyzer

OUT_DIR = sys.argv[1]
db_path = f"{OUT_DIR}/fixtures/read_rpc_fixture.db"
db = Database(db_path, MagicMock())
db.initialize()
conn = db._get_connection()
now = 1_800_000_000
conn.execute("INSERT INTO forwards (in_channel,out_channel,in_msat,out_msat,fee_msat,timestamp,resolved_time) VALUES (?,?,?,?,?,?,?)",
             ("1x1x0","2x2x0",1_000_000,999_000,1_000, now - 3600, now - 3595))
conn.execute("INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at) VALUES (?,?,?,?,?)",
             ("2x2x0","0"*66,500,1_000_000, now - 90*86400))
conn.execute("INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,timestamp) VALUES (?,?,?,?)" if False else
             "INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats) VALUES (?,?,?)", ("2x2x0","0"*66,200))
conn.commit()

analyzer = ProfitabilityAnalyzer(MagicMock(), db, MagicMock())
out = {
    "lifetime_stats": db.get_lifetime_stats(),
    "closed_channels_summary": db.get_closed_channels_summary(),
    "pnl_summary_30d": analyzer.get_pnl_summary(30),
}
json.dump(out, open(f"{OUT_DIR}/fixtures/read_rpc.json", "w"), indent=1)
print("wrote read_rpc fixtures")
```

**(Before finalizing: run this against the actual `rebalance_costs`/`channel_costs` schemas in `fixtures/schema.sql` — the exact column list above is a best-effort reconstruction from the earlier `.schema` dump in this plan's research, not re-verified column-by-column against every table; correct the INSERT statements to match whatever `sqlite3 fixtures/read_rpc_fixture.db .schema` actually reports before trusting the generator's output.)**

- [ ] **Step 2: Failing tests** — `crates/revops/tests/read_rpcs.rs`, following the `fixtures/fixture.db`-path pattern from Phase 1a Task 5, opens `read_rpc_fixture.db` read-only via `revops_db::actor::spawn_read_only`, calls the new query functions, and asserts against `fixtures/read_rpc.json`; separately unit-tests `build_history`/`build_report`/`build_dashboard` as pure functions over hand-constructed `LifetimeStats`/etc. (no DB needed for the builder-shape tests, matching `rpc_status.rs`'s existing test style).

- [ ] **Step 3: Run → FAIL. Step 4: Implement** the query functions (statement-for-statement SQL ports — copy the exact `SELECT` text from `database.py`/`profitability_analyzer.py` at the line numbers cited above, substituting `?`-params 1:1) and the three builders.

- [ ] **Step 5: Register the three RPCs in `main.rs`**, each handler: fetch its state's `db: Option<DbHandle>` (from Task 1), `None` → `{"error": "Database not initialized"}` (matches Python's guard shape at cl-revenue-ops.py:5755-5756 / 4913-4914), `Some(handle)` → run the query functions and hand off to the builder.

- [ ] **Step 6: Run full suite → PASS; fmt/clippy clean.**

---

### Task 6: Deployment runbook — `docs/runbooks/observer-deploy.md`

**Parallel-safety:** Fully independent (docs-only file). Run any time, in parallel with everything.

**Files:**
- Create: `docs/runbooks/observer-deploy.md`

**Content outline** (write in full prose + command blocks, not just this outline):

1. **Build.** Either build directly on lnnode (`cargo build --release -p revops`, matching the toolchain pin in `rust-toolchain.toml`) or cross-compile and `scp` the `target/release/revops` binary over — lnnode is the only test node (design spec constraint 2), so whichever is faster for the operator's setup is fine; note the binary must be executable and owned appropriately for CLN's plugin directory conventions.
2. **Shadow-mode plugin start.** `lightning-cli plugin start /path/to/revops` (or `plugin-dir` + restart, per the operator's existing CLN config style) with:
   - `REVOPS_CANONICAL_NAMES` **unset** (shadow mode — Python plugin stays loaded and authoritative).
   - `revops-r-db-path=/path/to/revenue_ops.db` (production DB, opened read-only per Task 1's actor — confirm the exact production path via `lightning-cli revenue-config get db_path` on the Python side first).
   - `revops-r-observer-db-path=~/.lightning/revops-r-observer.db` (Task 2's own writable file — a fresh path, never the production one; note in the runbook that this file's schema is Rust-only, per Task 2's narrower `ingested_forwards`/`peer_connection_events`/`channel_closure_events` tables, not a production schema clone).
   - Confirm with `lightning-cli revenue-r-status` (`db.tables` should report the production table count) and `lightning-cli revenue-r-ping`.
3. **Diff-harness invocations.** `tools/diff-harness/diff_config.py --node lnnode` (config parity, already `--strict`-capable per Task 3's typed values — document that `--strict` should be the DEFAULT invocation now that Task 3 lands typed values, dropping Phase 1a's string-normalization fallback); a new `tools/diff-harness/diff_read_rpcs.py` (write this script as part of Task 6, following `diff_config.py`'s ssh+`lightning-cli`+JSON pattern) comparing `revenue-history`/`revenue-r-history`, `revenue-report costs`/`revenue-r-report report_type=costs`, and `revenue-dashboard`/`revenue-r-dashboard` field-by-field, SKIPPING the `_phase1b_gaps`-listed keys (read that array off the Rust response itself rather than hand-maintaining a skip list, so the skip set never drifts from what the Rust side actually declares as a gap).
4. **What closes the Jul-19 comparison window.** Explicit exit checklist: (a) `diff_config.py --strict` clean over the full 119-key surface; (b) `diff_read_rpcs.py` clean modulo declared `_phase1b_gaps`; (c) notification-ingestion parity — forward count in `ingested_forwards` matches production `forwards` table row count over a shared observation window (spot-check via `sqlite3` on both files, not a diff-harness script, since the schemas intentionally differ); (d) the WAL concurrent-writer and cold-start integration tests (Task 1) green in CI; (e) manual confirmation the Python plugin's behavior is completely unaffected (its own `revenue-status`/fee/rebalance cycles keep running normally with the Rust plugin loaded alongside it) for at least one full cycle of each of Python's 8 loops.
5. **Rollback.** `lightning-cli plugin stop revops` (or the binary's basename as CLN registered it) — zero production impact by construction (Task 1's Global Constraint: Rust never writes the production DB; stopping it just stops a read-only observer + its own separate db file). No Python-side action needed; Python was never paused. Note explicitly: unlike a fee/rebalance cutover, THERE IS NO REVERSIBILITY CONCERN here because Rust never held write authority over anything Python depends on.

---

## Self-Review Notes

- **Spec coverage:** all six requested tasks are covered; the five carried Phase 1b obligations from the progress ledger (typed config w/ capital-U string, canonical db-path default, WAL/cold-start tests, notification ingestion + hydration, persistent DB actor) map onto Tasks 1–4 exactly.
- **Biggest risk identified while reading `cl-revenue-ops.py`'s notification/hydration code:** the `forward_event` handler's fee-controller nudge path (lines 6704-6755) does something Phase 1b must NOT replicate yet — it mutates `fee_controller._channel_fee_states` under a bounded-timeout lock shared with the (unported) fee-adjustment cycle. Phase 1b's own `notify::on_forward_event` only needs the dedup-insert half (lines 6757-6797: `record_forward`/`record_forward_and_reputation`), but a careless read of the handler could tempt an implementer into porting the failed-forward DTS-nudge logic too, since it lives in the same function. That logic depends on `GaussianThompsonState` (Phase 4 scope) and the `_state_lock` serialization discipline the design spec calls out as a top registry risk ("Threading translation... cycle-spanning locks → actor tasks") — building even a stub version of it now, ahead of the real fee controller, risks baking in a lock/actor shape that doesn't match Phase 4's eventual real requirements. Task 2 as written explicitly scopes `on_forward_event` to dedup-insert only and does not touch fee state; this must stay true during implementation even though the Python source has the two concerns interleaved in one function body.
- **Second-order risk:** `_compute_forward_hydration_start`'s `flow_window_days` input comes from `config.flow_window_days` (Task 3 gives typed config read access, but Phase 1b's config surface is still read-only/no-DB-override-writes) — hydration must read the LIVE option value at hydration time (post-init, once options are resolved), not a hardcoded default, or a lnnode operator running with a non-default `flow_interval`/`flow_window_days` gets silently wrong backfill bounds. Task 2's wiring step should pull this from the same `State`/option-resolution path Task 3 establishes, not a fresh hardcoded `7`.
- **Fixture-generator risk carried from Phase 1a's pattern:** three of this plan's four new generators (`gen_hydration_fixtures.py`, `gen_config_types_fixture.py`, `gen_read_rpc_fixtures.py`) import or exec parts of `cl-revenue-ops.py`/`modules/*.py` with `pyln` mocked — Phase 1a's Task 5 generator (`gen_schema_fixture.sh`) already proved this pattern works for `Database`; the two NEW modules this plan imports (`ProfitabilityAnalyzer`, and `cl-revenue-ops.py`'s own hydration function) haven't been proven importable this way yet — each generator step says explicitly what to do if the import fails (AST-extract instead of import), so this is a flagged uncertainty with a resolution path, not a silent assumption.
- **Plan file written, not committed** per the task instructions — the calling controller commits.
