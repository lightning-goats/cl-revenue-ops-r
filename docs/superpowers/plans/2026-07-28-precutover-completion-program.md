# Whole-Plugin Pre-Cutover Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish every Rust whole-plugin port, runtime-reachability, fake-transport, persistence, and safety item that can be implemented and verified without a live soak or the coordinated authority cutover.

**Architecture:** Keep the approved five-wave design and its capability separation. Each production-equivalent subsystem gets a single-owner runtime facade with observer-only construction, a separately gated action adapter, persistent health evidence, and fake-boundary tests. The plugin entrypoint composes those facades; the whole-plugin arm can enable action adapters only after every subsystem reports ready.

**Tech Stack:** Rust 2021 workspace, `cln-plugin`/`cln-rpc`, Tokio, rusqlite, serde JSON, local fake JSON-RPC sockets, local fake HTTP/process transports, Cargo tests, Hexmem tier-1 review contracts.

## Global Constraints

- Python remains the sole mutation authority until one coordinated whole-plugin cutover.
- Shadow construction must not contain a mutation-capable adapter.
- Rust never writes the Python production database; Rust-owned state uses the observer database.
- Every money-moving submission is preceded by governed authorization and a durable reservation.
- Ambiguous post-submission outcomes retain the reservation, enter quarantine, and are never automatically retried.
- Missing or stale evidence, persistence failure, unresolved quarantine, or incomplete reconciliation denies action.
- External-boundary tests use only local fake sockets, servers, and executables; they never contact a live node, LN+, or Boltz.
- A decision, state, execution, authority, or timer change resets the later design soak. This plan does not deploy a new candidate.
- Every code task is RED-first, includes a revert-discriminating caller/registration tripwire, and receives independent review.
- `docs/port/PARITY-CHECKLIST.md` is updated in the checkpoint that changes reachability or evidence state.

## Programme Boundary

Included: Wave 1 leftovers, Wave 2 reachability, Wave 3 fake-boundary execution, Task 41, Task 42, the staged implementation and fake tests of Task 51, whole-plugin readiness/arm construction, retention, reconciliation, and all 69 Python-equivalent RPC registrations.

Excluded until separately gated: Task 45 and later soak observations, deployment of Task 51 lifecycle automation, a live LN+/Boltz/CLN call, disabling Python mutation paths, consuming a production whole-plugin arm, and the actual authority handoff or rollback drill.

---

### Task 1: Planner Read RPC Quartet — Hexmem Task 56

**Files:**
- Modify: `crates/revops-db/src/queries.rs`
- Modify: `crates/revops-db/tests/queries.rs`
- Modify: `crates/revops/src/main.rs`
- Modify: `crates/revops/tests/manifest.rs`
- Modify: `docs/port/PARITY-CHECKLIST.md`

**Interfaces:**
- Produces: `queries::PlannerCandidateRow`, `queries::planner_candidates(&DbHandle, f64, Option<&str>, i64)`, `queries::PlannerActionRow`, and `queries::planner_actions(&DbHandle, i64)`.
- Consumes: existing `rpc_planner_candidate_sources`, `rpc_planner_candidates`, `rpc_planner_history`, and `rpc_planner_status` response builders.
- Guarantees: Python SQL ordering/filter/limit semantics, JSON metadata pass-through, read-only production DB access, explicit refusal of nonempty positional parameters, and semantic equality of `[]` and `{}` no-argument calls.

- [ ] **Step 1: Add failing DB-query tests**

```rust
#[tokio::test]
async fn planner_candidates_match_python_filter_order_limit_and_source() {
    // Seed scores 7.0, 3.0, and -2.0 from two sources; assert score-descending
    // order, inclusive min_score, optional source filtering, and LIMIT.
}

#[tokio::test]
async fn planner_actions_match_python_newest_first_limit_and_null_shape() {
    // Seed two actions with nullable fields and JSON metadata; assert id-descending
    // order, exact nullable columns, metadata text, and LIMIT.
}
```

- [ ] **Step 2: Run the DB tests and observe RED**

Run: `cargo test -p revops-db planner_ -- --nocapture`

Expected: compilation failure because the typed planner query interfaces do not exist.

- [ ] **Step 3: Implement typed read-only planner queries**

```rust
pub async fn planner_candidates(
    handle: &DbHandle,
    min_score: f64,
    source: Option<&str>,
    limit: i64,
) -> Result<Vec<PlannerCandidateRow>>;

pub async fn planner_actions(
    handle: &DbHandle,
    limit: i64,
) -> Result<Vec<PlannerActionRow>>;
```

Use the exact Python projections and `ORDER BY score DESC LIMIT ?` / `ORDER BY created_at DESC LIMIT ?` queries. Decode nullable cells without dropping a whole row, and preserve `metadata_json` as parsed JSON at the RPC boundary.

- [ ] **Step 4: Verify DB tests GREEN**

Run: `cargo test -p revops-db planner_ -- --nocapture`

Expected: all planner query tests pass with zero failures.

- [ ] **Step 5: Add failing manifest and caller-tripwire tests**

```rust
const PLANNER_READ_METHODS: &[&str] = &[
    "revenue-r-planner-candidate-sources",
    "revenue-r-planner-candidates",
    "revenue-r-planner-history",
    "revenue-r-planner-status",
];

#[test]
fn manifest_registers_planner_read_quartet_in_both_naming_modes() {}

#[test]
fn planner_read_rpcs_round_trip_distinctive_database_rows() {}

#[test]
fn planner_read_rpcs_refuse_nonempty_positional_params() {}
```

- [ ] **Step 6: Run manifest tests and observe RED**

Run: `cargo test -p revops --test manifest planner_read_ -- --nocapture`

Expected: the four method names are absent or method-not-found.

- [ ] **Step 7: Register the four handlers with real queries**

```rust
.rpcmethod(&planner_candidates_name, "capacity planner candidate pool", |p, v| async move {
    // reject nonempty positional params, parse limit, query real rows, build response
})
```

Repeat the same pattern for candidate sources, history, and status. Do not return success-shaped empty data when the DB query fails; return an in-band error matching the existing registered-handler convention.

- [ ] **Step 8: Verify focused RPC tests GREEN and mutation-test the tripwire**

Run: `cargo test -p revops --test manifest planner_read_ -- --nocapture`

Then temporarily remove one query call, confirm the distinctive-row test fails, restore the code byte-for-byte, and rerun it green.

- [ ] **Step 9: Update the parity checklist and run task gates**

Run:

```bash
cargo test -p revops-db
cargo test -p revops --test manifest
cargo test --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Record total Rust RPCs and Python-equivalent coverage from the manifest, not from arithmetic in prose.

- [ ] **Step 10: Commit and request independent review**

```bash
git add crates/revops-db/src/queries.rs crates/revops-db/tests/queries.rs \
  crates/revops/src/main.rs crates/revops/tests/manifest.rs \
  docs/port/PARITY-CHECKLIST.md
git commit -m "feat(rpc): wire planner read surfaces"
```

Owner marks only Task 56 `impl`; Python verifier owns `review`.

---

### Task 2: Provenance and SeedOnce Corrections — Hexmem Tasks 41 and 42

**Files:** `crates/revops/build.rs`, `crates/revops/tests/manifest.rs`, `crates/revops/src/fee_scheduler.rs`, `crates/revops/src/fee_runway.rs`, `crates/revops/tests/fee_scheduler.rs`.

- [ ] Implement Task 41's Git-ref/index rerun tracking with a docs-only HEAD-advance regression.
- [ ] Implement Task 42's first-cycle Rust mempool evidence and commit-coupled seed provenance semantics.
- [ ] Independently mutation-test both corrections and merge only after their existing Hexmem review criteria pass.

### Task 3: Observer Runtime Framework and Persistent Loop Health

**Files:** create `crates/revops/src/runtime.rs`, `crates/revops/src/loop_health.rs`; modify `crates/revops/src/main.rs`, `crates/revops/src/fee_runway.rs`, and `crates/revops/tests/manifest.rs`.

- [ ] RED-test that removing any required subsystem spawn or health write fails.
- [ ] Add bounded single-flight owners for fee, rebalance, planner, LN+, and Boltz.
- [ ] Persist last-start, last-pass, last-error, dropped/coalesced work, and generation per loop.
- [ ] Prove observer mode cannot construct any action adapter.

### Task 4: LN+ Runtime, Operator Surface, and Fake Boundaries

**Files:** create `crates/revops/src/lnplus_runtime.rs`; modify `crates/revops/src/main.rs`, `crates/revops-lnplus/src/http.rs`, `crates/revops-lnplus/src/ports.rs`, and LN+ integration tests.

- [ ] Wire the Rust-owned `SqliteLnPlusDb`, evaluator/watcher owners, status, breaker-clear, abandon, and backfill RPCs.
- [ ] Add concrete HTTP and CLN-signmessage adapters exercised only against local fake servers/sockets.
- [ ] Prove structured terminal/idempotent outcomes, first-cause breaker preservation, governed reservation, ambiguous quarantine, and exactly-once settlement.

### Task 5: Rebalance Runtime, RPCs, and Fake Payment Transport

**Files:** create `crates/revops/src/rebalance_runtime.rs`; modify `crates/revops-rebalance/src/executor.rs`, `crates/revops-rebalance/src/ports.rs`, `crates/revops/src/main.rs`, and integration tests.

- [ ] Register cycle/debug/manual rebalance RPCs and spawn the observer owner.
- [ ] Exercise `sendpay`/`waitsendpay` through a fake CLN socket, including rejection, timeout, disconnect-after-submit, and reconciliation.
- [ ] Prove reservation-before-submit and exactly-once settle/release behavior.

### Task 6: CapacityPlanner Runtime and Fake Channel Mutation Transport

**Files:** create `crates/revops/src/planner_runtime.rs`; modify `crates/revops-capital`, `crates/revops/src/main.rs`, and planner integration tests.

- [ ] Wire planner status/report/execute/history/candidate RPCs and the observer planning loop.
- [ ] Add governed `fundchannel`, `close`, and defibrillation adapters against fake CLN sockets.
- [ ] Prove no action on missing evidence, budget denial, unknown outcome, or persistence failure.

### Task 7: Boltz Runtime, Full RPC Surface, and Governed Autocycle

**Files:** create `crates/revops/src/boltz_runtime.rs`; modify `crates/revops-boltz`, `crates/revops/src/main.rs`, and fake-executable integration tests.

- [ ] Register all Boltz status/history/budget/wallet/refund/claim/chainswap/withdraw/deposit/backup/recommendation/cycle RPCs.
- [ ] Spawn observer-only balance, auto-cycle, and expansion-treasury owners.
- [ ] Gate process execution behind governed reservations and quarantine ambiguous create/claim/refund outcomes.

### Task 8: Remaining Core RPC and Configuration Parity

**Files:** create focused `rpc_*.rs` builders as needed; modify `crates/revops/src/main.rs`, `crates/revops-db/src/queries.rs`, and manifest tests.

- [ ] Register the remaining policy writes, config get/set/delete, fee debug/cycle/wake/set, ignore/ban operations, econ cycle/reconcile, cleanup, reservation, and profile-preview methods.
- [ ] Implement one port-wide positional-parameter policy and test every method against the 69-name source inventory.
- [ ] Replace every `not_yet_ported` response with real evidence or a deliberate cutover-time refusal whose contract cannot collide with a Python success.

### Task 9: Retention, Store Budget, and Authority-Fetch Hardening

**Files:** modify `crates/revops/src/fee_runway.rs`, `crates/revops-db`, `crates/revops/src/python_authority.rs`, and their tests.

- [ ] Add bounded pruning for all eight runway tables with transaction and restart tests.
- [ ] Reconcile store-operation timeout floors with SQLite busy timeout.
- [ ] Prove authorization performs two genuinely independent authority fetches.
- [ ] Close arm re-mint/reuse paths without making a restart reacquire authority.

### Task 10: Staged Fail-Closed Restart Controller — Hexmem Task 51

**Files:** create host lifecycle controller/service files and fake-command tests; do not install or deploy them in this plan.

- [ ] Document and review exact green preconditions before implementation.
- [ ] Implement pinned checksum/source/flags/database/authority verification and bounded backoff.
- [ ] Prove fake restart, every refusal gate, and restart-storm handling.
- [ ] Leave Task 51's live `deploy` criterion untouched until Task 45 closes and the operator acknowledges a fresh window.

### Task 11: Whole-Plugin Arm and Preflight Readiness

**Files:** extend `crates/revops/src/cutover_arm.rs`, `crates/revops/src/main.rs`, and rehearsal tests.

- [ ] Bind the arm to the exact complete subsystem set, node, commit, binary hash, release, mode, times, and nonce.
- [ ] Validate every loop, budget, reconciliation, quarantine, persistence, and Python-authority-off precondition before atomic consumption.
- [ ] Exercise all denial and rollback-order paths in copied-state fake rehearsals.

### Task 12: Final Code-Complete Audit and Checklist Freeze

**Files:** modify `docs/port/PARITY-CHECKLIST.md`; create a generated 69-RPC/loop/action inventory under `docs/port/`.

- [ ] Recompute compiled, reachable, effective, transport-proven, and promotion-ready counts from source/tests.
- [ ] Require 69/69 Python-equivalent RPC registration and all production-equivalent observer loop spawns.
- [ ] Require fake-boundary coverage for every external mutation type and zero unauthorized action construction in observer mode.
- [ ] Mark only soak and actual cutover rows open; publish the reviewed, green code-complete commit without deploying it.

