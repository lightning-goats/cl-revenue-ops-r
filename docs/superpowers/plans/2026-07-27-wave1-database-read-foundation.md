# Wave 1 Database Read Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Supply the typed read-only database evidence needed by the compiled policy, hot-channel, and spend-ledger RPC builders, and independently prove that the newly landed LN+ SQLite store is safe only on a Rust-owned database path.

**Architecture:** Extend the existing single-owner `DbHandle` actor with one generic multi-row query primitive, then keep Python-parity SQL and row decoding in `revops_db::queries`. Shared DB row types live in `revops-db` and are re-exported by the response builders so the later `main.rs` wiring has one contract. `SqliteLnPlusDb` remains crate-local; this lane adds temp-file integration evidence and documentation, not a second LN+ schema.

**Tech Stack:** Rust 2021, Tokio `mpsc`/`oneshot`, `rusqlite` 0.32, `anyhow`, `serde_json`, existing `revops-analytics` policy types, `tempfile`.

## Global Constraints

- Python remains the sole mutation authority until the coordinated whole-plugin cutover.
- The production Python database is read-only from Rust; no write-capable handle may be introduced for these queries.
- The LN+ store is opened only on the Rust-owned observer database; tests use fresh `tempfile::TempDir` paths and never a live database.
- Do not modify or register RPCs in `main.rs` in this lane.
- Do not duplicate `lnplus_swaps`, `lnplus_peers`, breaker, planner-action, or budget-rail storage in `revops-db`.
- No network, live CLN RPC, LN+, Boltz, or production database contact.
- Start every production increment with a targeted test observed failing for the intended reason.
- End with focused tests, workspace tests, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `git diff --check`, exact file inventory, and independent review.

## File map

- `crates/revops-db/src/actor.rs`: generic multi-row execution on the existing read-only connection owner.
- `crates/revops-db/src/queries.rs`: policy decoding, policy/hot-channel/spend reads, and shared typed rows.
- `crates/revops-db/tests/actor_wal.rs`: actor ordering/error propagation tripwires.
- `crates/revops-db/tests/queries.rs`: Python-pinned query semantics against copied fixture databases.
- `crates/revops-db/Cargo.toml`: internal-only dependency on `revops-analytics`; no new external crate.
- `crates/revops/src/rpc_hot_channel_protection_peers.rs`: re-export the DB-owned row type.
- `crates/revops/src/rpc_spend_ledger.rs`: re-export DB-owned ledger types and render measured coverage.
- `crates/revops-lnplus/tests/sqlite_db.rs`: Rust-owned temp-file concurrency, rollback, restart, and foreign-breaker tests.
- `crates/revops-lnplus/src/sqlite_db.rs`: documentation or a narrow test-support accessor only if a failing boundary test proves it necessary.
- `crates/revops-lnplus/ENTRYPOINTS.md`: state the Rust-owned-path restriction and migration boundary.

---

### Task 1: Add a typed multi-row actor primitive

**Files:**
- Modify: `crates/revops-db/src/actor.rs`
- Modify: `crates/revops-db/tests/actor_wal.rs`

**Interfaces:**
- Consumes: `DbHandle::query_row<T, F>(&self, &'static str, Vec<SqlValue>, F) -> Result<T>` and the actor's type-erased `Command::Exec` job.
- Produces: `DbHandle::query_rows<T, F>(&self, &'static str, Vec<SqlValue>, F) -> Result<Vec<T>>` where `T: Send + 'static` and `F: Fn(&Row) -> rusqlite::Result<T> + Send + Sync + 'static`.

- [ ] **Step 1: Add failing actor tests**

Add a Tokio test that creates `t(seq INTEGER, value TEXT)`, opens `spawn_read_only`, and calls the missing API:

```rust
let rows = handle
    .query_rows(
        "SELECT value FROM t WHERE seq >= ?1 ORDER BY seq ASC",
        vec![rusqlite::types::Value::Integer(2)],
        |row| row.get::<_, String>(0),
    )
    .await
    .unwrap();
assert_eq!(rows, vec!["two".to_string(), "three".to_string()]);
```

Add a second test whose mapper requests an `i64` from a TEXT column and asserts the returned error contains `query_rows`; a row decode error must not truncate into a successful prefix.

- [ ] **Step 2: Observe the red test**

Run:

```bash
cargo test -p revops-db --test actor_wal query_rows -- --nocapture
```

Expected: compile failure because `DbHandle::query_rows` does not exist. Save the command and diagnostic in the Task 48 report.

- [ ] **Step 3: Implement the minimal actor method**

Implement through `Command::Exec`, keeping the connection on its owner task:

```rust
pub async fn query_rows<T, F>(
    &self,
    sql: &'static str,
    params: Vec<SqlValue>,
    map: F,
) -> Result<Vec<T>>
where
    T: Send + 'static,
    F: Fn(&Row) -> rusqlite::Result<T> + Send + Sync + 'static,
{
    let (reply, rx) = oneshot::channel::<Result<Vec<T>>>();
    let job: Box<dyn FnOnce(&Connection) + Send + Sync> = Box::new(move |conn| {
        let param_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
        let result = (|| -> Result<Vec<T>> {
            let mut stmt = conn.prepare(sql).context("query_rows prepare")?;
            let mapped = stmt
                .query_map(param_refs.as_slice(), |row| map(row))
                .context("query_rows execute")?;
            mapped
                .collect::<rusqlite::Result<Vec<T>>>()
                .context("query_rows decode")
        })();
        let _ = reply.send(result);
    });
    self.tx.send(Command::Exec(job)).await.context("actor gone")?;
    rx.await.context("actor dropped reply")?
}
```

- [ ] **Step 4: Run the focused actor tests**

Run:

```bash
cargo test -p revops-db --test actor_wal query_rows -- --nocapture
```

Expected: both new tests pass.

- [ ] **Step 5: Commit the actor checkpoint**

```bash
git add crates/revops-db/src/actor.rs crates/revops-db/tests/actor_wal.rs
git commit -m "feat(db): add typed multi-row reads"
```

---

### Task 2: Add Python-parity policy reads

**Files:**
- Modify: `crates/revops-db/Cargo.toml`
- Modify: `crates/revops-db/src/queries.rs`
- Modify: `crates/revops-db/tests/queries.rs`

**Interfaces:**
- Consumes: `revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode}` and Task 1's `query_rows`.
- Produces:

```rust
pub async fn all_policies(handle: &DbHandle) -> Result<Vec<PeerPolicy>>;
pub async fn policy_for_peer(handle: &DbHandle, peer_id: &str) -> Result<PeerPolicy>;
pub async fn policies_by_tag(handle: &DbHandle, tag: &str) -> Result<Vec<PeerPolicy>>;
pub async fn policy_changes_since(handle: &DbHandle, since: i64) -> Result<Vec<PeerPolicy>>;
pub async fn last_policy_change_timestamp(handle: &DbHandle) -> Result<i64>;
```

The row mapper treats malformed `tags` JSON or a non-array JSON value as `[]`, invalid strategy as `Dynamic`, and invalid rebalance mode as `Enabled`, matching `_row_to_policy`. SQL ordering is `updated_at DESC`; `policy_for_peer` returns `PeerPolicy::default_for(peer_id)` when absent. Expiry filtering remains in the existing response builders, which receive `now` and already own presentation-time expiry semantics.

- [ ] **Step 1: Add failing policy-query tests**

Seed four policies into a copied fixture:

```text
peer a: updated_at=400, tags=["vip","banned"], dynamic/enabled
peer b: updated_at=300, tags=["vip"], static/source_only
peer c: updated_at=200, tags={"not":"an array"}, invalid strategy/mode
peer d: updated_at=100, tags="not-json", passive/disabled
```

Assert descending list order; exact typed mapping; invalid enum fallback; both corrupt tag shapes become empty; absent `policy_for_peer` is a default with the requested peer ID; `policies_by_tag("vip")` returns only a then b; changes use strict `updated_at > since`; and empty/non-empty MAX timestamps return 0/400.

- [ ] **Step 2: Observe the red tests**

Run:

```bash
cargo test -p revops-db --test queries policy_ -- --nocapture
```

Expected: compile failure because the five query functions are absent. Save the red evidence.

- [ ] **Step 3: Add the internal crate dependency and row decoder**

Add only:

```toml
revops-analytics = { path = "../revops-analytics" }
```

Create one private row decoder in `queries.rs`. Decode tags with `serde_json::from_str::<Vec<String>>`, falling back to an empty vector. Map unknown enum strings with `unwrap_or(FeeStrategy::Dynamic)` and `unwrap_or(RebalanceMode::Enabled)`.

- [ ] **Step 4: Implement the five read functions**

Use an explicit nine-column select, never `SELECT *`:

```sql
SELECT peer_id, strategy, rebalance_mode, fee_ppm_target, tags,
       updated_at, fee_multiplier_min, fee_multiplier_max, expires_at
FROM peer_policies
```

Append `ORDER BY updated_at DESC`, `WHERE peer_id = ?1`, or `WHERE updated_at > ?1 ORDER BY updated_at DESC` as appropriate. Implement `policies_by_tag` by calling `all_policies` and filtering `PeerPolicy::has_tag` in Rust; do not substring-match JSON in SQL. Implement the absent-peer default without collapsing SQL/actor errors into absence.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cargo test -p revops-db --test queries policy_ -- --nocapture
```

Expected: all new policy tests pass.

- [ ] **Step 6: Commit the policy checkpoint**

```bash
git add crates/revops-db/Cargo.toml crates/revops-db/src/queries.rs crates/revops-db/tests/queries.rs Cargo.lock
git commit -m "feat(db): read peer policies"
```

---

### Task 3: Add hot-channel and spend-ledger reads with honest coverage

**Files:**
- Modify: `crates/revops-db/src/queries.rs`
- Modify: `crates/revops-db/tests/queries.rs`
- Modify: `crates/revops/src/rpc_hot_channel_protection_peers.rs`
- Modify: `crates/revops/src/rpc_spend_ledger.rs`

**Interfaces:**
- Consumes: Task 1's `query_rows`, existing fixture tables, and `revops_core::msat::py_round2`.
- Produces:

```rust
pub struct HotChannelProtectionOverridePeer {
    pub peer_id: String,
    pub added_at: i64,
    pub note: String,
    pub min_depletion_trigger_pct: Option<f64>,
}

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

pub async fn hot_channel_protection_override_peers(
    handle: &DbHandle,
) -> Result<Vec<HotChannelProtectionOverridePeer>>;
pub async fn spend_ledger_aggregates(
    handle: &DbHandle,
    window_hours: i64,
    now: i64,
) -> Result<SpendLedgerAggregates>;
pub async fn active_spend_reservations(
    handle: &DbHandle,
    window_hours: i64,
    limit: i64,
    now: i64,
) -> Result<Vec<ActiveReservation>>;
```

The RPC modules `pub use` these DB-owned types so current callers and tests keep their names. `build_spend_ledger` renders `covered_hours` into both `coverage_hours` and `covered_hours`, renders `coverage_status`, and removes those fields from `_gaps` because an evidence-backed `unknown` is a real answer.

- [ ] **Step 1: Add failing query and builder tests**

Seed in-window and out-of-window event/reservation rows across two categories. Assert:

- the cutoff is `now - max(1, window_hours) * 3600` and inclusive (`>=`);
- only active, in-window reservations contribute;
- category totals and counts are exact and use deterministic `BTreeMap` ordering;
- active rows are ordered `reserved_at ASC` and `limit` is clamped to at least 1;
- hot-channel rows are ordered `added_at ASC` and preserve NULL depletion values;
- no evidence gives `covered_hours=None`, `coverage_status="unknown"`;
- evidence older than the window gives the clamped window and `complete`;
- partial evidence uses Python two-decimal rounding;
- a future-only timestamp is `unknown`;
- the response builder emits measured coverage and no longer gap-lists the three coverage fields.

- [ ] **Step 2: Observe the red tests**

Run:

```bash
cargo test -p revops-db --test queries spend_ -- --nocapture
cargo test -p revops-db --test queries hot_channel_ -- --nocapture
cargo test -p revops rpc_spend_ledger -- --nocapture
```

Expected: query-function compile failures and the builder coverage assertion failure. Save both red diagnostics.

- [ ] **Step 3: Implement DB-owned row types and reads**

Use the Python SQL literally for totals, grouping, counts, and active rows. Calculate coverage from the minimum positive timestamp across `spend_events.timestamp` and `spend_reservations.reserved_at`; `None` or a minimum later than `now` is `unknown`, a span at least the requested window is `complete`, otherwise `partial` with `py_round2(span_seconds as f64 / 3600.0)`.

- [ ] **Step 4: Re-export shared types and render coverage**

Replace the duplicate type declarations in both RPC modules with `pub use revops_db::queries::{...}`. Preserve all existing JSON field names and reservation age clamping. The builder's `_gaps` must be an empty array after coverage is wired; do not invent coverage when the DB query reports `unknown`.

- [ ] **Step 5: Run focused crate tests**

Run:

```bash
cargo test -p revops-db --test queries -- --nocapture
cargo test -p revops rpc_hot_channel_protection_peers -- --nocapture
cargo test -p revops rpc_spend_ledger -- --nocapture
```

Expected: all focused tests pass.

- [ ] **Step 6: Commit the evidence-query checkpoint**

```bash
git add crates/revops-db/src/queries.rs crates/revops-db/tests/queries.rs crates/revops/src/rpc_hot_channel_protection_peers.rs crates/revops/src/rpc_spend_ledger.rs
git commit -m "feat(db): read policy-adjacent spend evidence"
```

---

### Task 4: Verify the LN+ SQLite ownership and concurrency boundary

**Files:**
- Modify: `crates/revops-lnplus/tests/sqlite_db.rs`
- Modify only if required by a red test: `crates/revops-lnplus/src/sqlite_db.rs`
- Modify: `crates/revops-lnplus/ENTRYPOINTS.md`

**Interfaces:**
- Consumes: `SqliteLnPlusDb::open`, the `LnPlusDb` trait, `BudgetDb` composition, structured `BreakerState`, and the existing 5000 ms busy timeout.
- Produces: test evidence that one Rust-owned file supports the LN+ connection plus its composed budget connection without partial state, silent corruption, or restart loss. No production API is required unless the test cannot observe the invariant through existing methods.

- [ ] **Step 1: Add failing boundary tests**

Add temp-file-only tests for:

1. two independently opened `SqliteLnPlusDb` instances see committed swap, planner-action, and budget state after reopen;
2. an explicit raw SQLite transaction that inserts a swap and then rolls back leaves no row after restart;
3. a raw `BEGIN IMMEDIATE` write lock causes an LN+ write to wait for the configured busy period and then fail without a partial row or planner action;
4. releasing the raw lock before the timeout permits the queued write and leaves one complete row;
5. closing and reopening preserves structured breaker first-cause state exactly;
6. Python-shaped plain-text and malformed JSON breaker values remain isolated as foreign encodings, produce no panic, and are not rewritten merely by reading.

Use an explicit short test-only timeout only if production code already exposes injection cleanly; otherwise assert the existing 5000 ms behavior with a generous monotonic bound and keep that one test ignored only if CI timing is demonstrably unstable. Do not weaken the production timeout to speed a test.

- [ ] **Step 2: Observe red evidence before any production change**

Run:

```bash
cargo test -p revops-lnplus --test sqlite_db boundary_ -- --nocapture
```

Expected: at least the lock/atomicity test fails against the landed implementation or cannot observe an error because write failures are currently swallowed. Save the exact result; if every new test is already green, record that the review found no production defect and do not manufacture one.

- [ ] **Step 3: Make only evidence-required production changes**

If a write error is unobservable, introduce the narrowest result-returning internal operation while keeping the `LnPlusDb` trait behavior stable. If transaction or cross-connection safety fails, wrap the smallest state transition in a transaction or align both connections' timeout/journal configuration. Do not move schema into `revops-db`, do not touch a production path, and do not add a second source of truth.

- [ ] **Step 4: Document the ownership boundary**

State in `ENTRYPOINTS.md`:

```text
SqliteLnPlusDb is a write-capable store and MUST be opened only on the
Rust-owned observer database. It must never be pointed at the Python
production revenue_ops.db while Python is authoritative. The shared
_lnplus_breaker key has incompatible Python plain-text and Rust structured
JSON encodings; cross-owner migration requires an explicit one-time format
conversion and may not occur implicitly at read time.
```

- [ ] **Step 5: Run the LN+ boundary suite**

Run:

```bash
cargo test -p revops-lnplus --test sqlite_db -- --nocapture
```

Expected: all tests pass, no live path is touched, and the lock test proves either bounded success after release or bounded failure without partial state.

- [ ] **Step 6: Commit the boundary-review checkpoint**

```bash
git add crates/revops-lnplus/tests/sqlite_db.rs crates/revops-lnplus/src/sqlite_db.rs crates/revops-lnplus/ENTRYPOINTS.md
git commit -m "test(lnplus): prove SQLite ownership boundary"
```

Omit `src/sqlite_db.rs` from `git add` if the tests establish safety without a production change.

---

### Task 5: Run the Tier-1 implementation gate and prepare independent review

**Files:**
- Modify: `.superpowers/sdd/progress.md` (git-ignored durable session ledger)
- Create: `/home/sat/agent-tasks/task-48-report.md` (outside the repository; report only)

**Interfaces:**
- Consumes: Tasks 1-4 commits.
- Produces: a clean actual-diff review package and Task 48 implementation evidence; only the Rust verifier may pass `review`.

- [ ] **Step 1: Run all gates from the lane worktree**

```bash
cargo test -p revops-db
cargo test -p revops-lnplus
cargo test -p revops
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
git diff --check main...HEAD
git status --short
git diff --name-status main...HEAD
```

Expected: every command passes; status contains no uncommitted implementation files; changed files match this plan plus the plan/design documents.

- [ ] **Step 2: Revert-discriminate the actor and one query caller**

Temporarily remove the `query_rows` collection call and separately bypass one policy/spend query invocation; run the targeted tests and observe each fail for the expected behavioral reason. Restore the committed code and rerun the focused tests green. Do not commit the temporary reversions.

- [ ] **Step 3: Write the implementation report**

Record: base and head commits; every observed-red command/diagnostic; exact changed files; focused/workspace counts; fmt/clippy/diff results; whether LN+ review required production changes; and any remaining C2 gaps. Do not include credentials, live DB contents, node identifiers, or private operational values.

- [ ] **Step 4: Mark only owner criteria and notify the verifier**

Pass Task 48 `impl` and `boundary` with evidence. Do not touch `review`. Then send only `hexmem task 48 is ready for review` to the Rust pane using the canonical three-call tmux protocol.
