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

- [x] **Step 1: Add failing DB-query tests**

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

- [x] **Step 2: Run the DB tests and observe RED**

Run: `cargo test -p revops-db planner_ -- --nocapture`

Expected: compilation failure because the typed planner query interfaces do not exist.

- [x] **Step 3: Implement typed read-only planner queries**

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

Use the exact Python projections and `ORDER BY score DESC LIMIT ?` / `ORDER BY created_at DESC LIMIT ?` queries. Decode nullable cells without dropping a whole row, and preserve `metadata_json` as the raw JSON string Python's `dict(sqlite3.Row)` returns.

- [x] **Step 4: Verify DB tests GREEN**

Run: `cargo test -p revops-db planner_ -- --nocapture`

Expected: all planner query tests pass with zero failures.

- [x] **Step 5: Add failing manifest and caller-tripwire tests**

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

- [x] **Step 6: Run manifest tests and observe RED**

Run: `cargo test -p revops --test manifest planner_read_ -- --nocapture`

Expected: the four method names are absent or method-not-found.

- [x] **Step 7: Register the four handlers with real queries**

```rust
.rpcmethod(&planner_candidates_name, "capacity planner candidate pool", |p, v| async move {
    // reject nonempty positional params, parse limit, query real rows, build response
})
```

Repeat the same pattern for candidate sources, history, and status. Do not return success-shaped empty data when the DB query fails; return an in-band error matching the existing registered-handler convention.

- [x] **Step 8: Verify focused RPC tests GREEN and mutation-test the tripwire**

Run: `cargo test -p revops --test manifest planner_read_ -- --nocapture`

Then temporarily remove one query call, confirm the distinctive-row test fails, restore the code byte-for-byte, and rerun it green.

- [x] **Step 9: Update the parity checklist and run task gates**

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

- [x] **Step 10: Commit and request independent review**

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

- [x] Implement Task 41's Git-ref/index rerun tracking with a docs-only HEAD-advance regression (reviewed and merged at `7688e40`).
- [ ] Correct the three supervisor-review blockers at `0ca9742`: verified
      bound success provenance, pre-hydration A3 generation denial, and
      rollback/reusability after a real transaction-boundary failure.
- [ ] Re-run independent Python review at the correction SHA. The initial PASS
      was superseded to FAIL after F1–F3 were reproduced first-hand; do not
      publish or unblock Task 61 from the `0ca9742` checkpoint.

### Task 3: Observer Runtime Framework and Persistent Loop Health — Hexmem Task 57

**Design:** `docs/superpowers/specs/2026-07-28-observer-runtime-loop-health-design.md`.

**Files:** create `crates/revops/src/runtime.rs`,
`crates/revops/src/loop_health.rs`, and
`crates/revops-db/src/loop_health.rs`; modify the observer DB schema/owner,
`crates/revops/src/main.rs`, `fee_scheduler.rs`, `rpc_health.rs`, and their
integration/action-surface tests.

- [x] RED-test that removing any required subsystem spawn or health write fails.
- [x] Add the bounded single-flight framework for all five loop identities and
      production-spawn only real passes. Do not use success-shaped no-op owners;
      rebalance/planner/LN+/Boltz remain `not_wired` until Tasks 4–7.
- [x] Persist last-start, last-pass, last-error, dropped/coalesced work, and generation per loop.
- [x] Make fee health depend on a real owner completion acknowledgement, not dispatch.
- [x] Split `AuthorityRuntime::Observer` from `AuthorityRuntime::Live` and prove
      observer mode cannot accept, hold, or construct any action adapter.

Task 57 passed all three Hexmem criteria at reviewed `49a940a` and is merged
into canonical main through `a598239`.

### Task 4: LN+ Runtime, Operator Surface, and Fake Boundaries — Hexmem Task 61

**Files:** create `crates/revops/src/lnplus_runtime.rs`; modify
`crates/revops/src/{main,runtime,lib}.rs`, the LN+ lifecycle/store types,
`crates/revops-db` owner/schema/tests, manifest/action-surface tests, Cargo
manifests/lock, loop health/request types, and the parity checklist.

- [ ] Make every lifecycle write fallible and acknowledged, replace overwrite-prone
      swaps with CAS transitions, and make compound terminal/breaker changes atomic.
- [ ] Persist a stable attempt/reservation identity and distinguish not-submitted,
      committed, and outcome-unknown; unknown retains the reservation, enters
      quarantine, reconciles after restart, and cannot be retried automatically.
- [ ] Wire one typed evaluator/watcher owner and the exact four Python-equivalent
      status, breaker-clear, abandon, and backfill RPCs through durable owner acks.
- [ ] Add concrete HTTP and CLN signer/chain adapters exercised only against local
      TCP/Unix fakes, including exact request, timeout/reset phase, cap, malformed,
      recovery, and no-resubmit cases.
- [ ] Keep every action adapter structurally absent from `ObserverRuntime`; dry-run
      observation must not create failed success-shaped intents.

Task 61 follows Task 42 because both change the manifest. It lands before Task 59
so the whole-plugin authority bracket can include LN+ readiness and reuse the same
admission/outcome-unknown vocabulary.

### Task 5: Rebalance Runtime, RPCs, and Fake Payment Transport — Hexmem Task 60

**Files:** create `crates/revops/src/rebalance_runtime.rs`; modify `crates/revops-rebalance/src/executor.rs`, `crates/revops-rebalance/src/ports.rs`, `crates/revops/src/main.rs`, and integration tests.

- [ ] Register cycle/debug/manual rebalance RPCs and spawn the observer owner.
- [ ] Exercise `sendpay`/`waitsendpay` through a fake CLN socket, including rejection, timeout, disconnect-after-submit, and reconciliation.
- [ ] Replace string-based failure inference with typed clean-before-write,
      rejected, success, and outcome-unknown-after-submit results.
- [ ] Prove durable intent/reservation before submission, persistent quarantine with
      no retry after ambiguity, and transactional exactly-once receipt plus
      settle/release behavior across restart.
- [ ] Require manual/force actions to retain durable reservation, hard cap, intent,
      and quarantine invariants even when soft policy is bypassed.

Task 60 follows Tasks 42 and 59 so it reuses the canonical store admission,
timeout, authority-bracket, and nonce semantics.

### Task 6: CapacityPlanner Runtime and Fake Channel Mutation Transport — Hexmem Task 62

**Files:** create `crates/revops/src/planner_runtime.rs`; modify `crates/revops-capital`, `crates/revops/src/main.rs`, and planner integration tests.

- [ ] Wire planner status/report/execute/history/candidate RPCs and the observer planning loop.
- [ ] Add governed `fundchannel`, `close`, and defibrillation adapters against fake CLN sockets.
- [ ] Define the Rust-owned planner store and rewire read surfaces explicitly so
      owner writes cannot be hidden behind production read-only DB queries.
- [ ] Require a positive resolved budget plus durable intent/action/reservation
      before submission; revalidate fresh authority, policy, conflict, budget, and
      action evidence at execution time.
- [ ] Prove no action on missing/stale evidence, budget denial, unknown outcome,
      or persistence failure; unknown retains reservation/quarantine and cannot retry.

Task 62 follows Tasks 42, 59, 61, and 60. Defibrillation reuses Task 60's governed
rebalance facade and channel mutations reuse Task 61's shared outcome semantics.

### Task 7: Boltz Runtime, Full RPC Surface, and Governed Autocycle — Hexmem Task 63

**Files:** create `crates/revops/src/boltz_runtime.rs`; modify the Boltz
process/command/error/driver/budget/journal/execution modules,
`revops-capital` reservation semantics, `revops-db` owner/schema/store,
runtime/main/health, manifest/action-surface tests, fake executables, and the
parity checklist.

- [ ] Add typed durable attempt, reservation, terminal receipt, quarantine,
      journal, ignore, cooldown, auto-cycle, and reconciliation state.
- [ ] Split an allowlisted, bounded, secret-redacting query transport from a
      live-only non-forgeable action capability; arbitrary argv and the public
      `ExecutionMode::Armed` value must not authorize execution.
- [ ] Require fresh governor/conflict/structural-envelope/budget evidence plus a
      durable attempt and shared-budget reservation before process spawn.
- [ ] Use one serialized Boltz actor for manual, scheduled, balance, treasury,
      cooldown, and reconciliation work; an unknown outcome suspends it, retains
      reservation/quarantine, forbids retry, and cannot report a loop pass.
- [ ] Register all 22 Python-equivalent Boltz RPCs and prove them end-to-end
      through test-owned executables, including malformed/timeout/restart cases.

Task 63 follows Tasks 42, 61, 59, 60, and 62. Its planner-backed balance and
treasury modes may not use fabricated evidence.

### Task 8: Remaining Core RPC and Configuration Parity — Hexmem Tasks 64–66

**Files:** create focused `rpc_*.rs` builders as needed; modify `crates/revops/src/main.rs`, `crates/revops-db/src/queries.rs`, and manifest tests.

- [ ] **Task 64 (independent):** generate and check in the exact 69-RPC,
      121-option, eight-loop/shutdown, and external-mutation inventories with
      Python source provenance; add one 69-entry parameter registry/common
      decoder; restore the two missing fee-authority/replay-capture options.
- [ ] **Task 65:** add the one serialized, typed live state-writer capability
      for config, policy/tags/bans, hot-channel overrides, cleanup, and unified
      budget administration. Keep it absent from `ObserverRuntime`.
- [ ] **Task 66:** register the remaining fee/econ/recovery/core methods through
      result-bearing owners and close every canonical success-shaped partial
      response. No Python-equivalent RPC counts effective while returning
      Rust-only vocabulary, null gaps, or `not_yet_ported`.
- [ ] Require the generated positional/named policy and caller-removal tripwire
      at all 69 methods; exact set equality replaces count-only assertions.

### Task 8.5: Missing Whole-Plugin Runtime Owners — Hexmem Tasks 67 and 68

- [ ] **Task 67:** port flow-analysis, startup-snapshot, and
      financial-snapshot owners plus profitability/analyze evidence; expand
      loop health to eight business/startup identities and bind every pass to
      the current boot/session, commit, binary, and configuration.
- [ ] **Task 68:** port datastore status/fee-bounds/rebalance producers, all
      four subscriptions, hydration/deferred cursors, retention health, and
      clean shutdown/drain. Require strict store routing, device/inode alias
      denial, and typed required reads/writes.

### Task 9: Retention, Store Budget, and Authority-Fetch Hardening — Hexmem Tasks 58 and 59

**Reviewed design:** Task 58 revision 3 at `ddeb14c` (design branch; review
passed 2/2). **Implementation:** Hexmem Task 59. Its stored dependency is Task
42; the operational merge order additionally requires Task 61 first because
Task 59 must bind the now-present LN+ readiness state into the authority bracket.

**Files:** add `crates/revops-db/src/retention.rs`; modify
`crates/revops-db/src/{fee_runway,owner}.rs`,
`crates/revops/src/{fee_state,fee_scheduler,fee_execution,python_authority,cutover_arm,main}.rs`,
the rehearsal binary, runbook, and tests.

- [x] Complete and independently review the fail-closed design checkpoint.
- [ ] Sweep only Class-W evidence with a fair global batch cap; preserve
      append-only identity/audit rows, current state, quarantine, A3 replay
      identity, and deferred notification/hydration cursors exactly.
- [ ] Reconcile store-operation timeout floors with SQLite busy timeout and
      distinguish not-admitted, admitted-outcome-unknown, query-saturated, and
      query-response-timeout states without treating backlog as owner death.
- [ ] Prove authorization performs two endpoint-bound, genuinely independent
      authority fetches; consume one bracket into one fresh authorization and
      one batch, refusing stale open brackets and stale dispatch.
- [ ] Close arm re-mint/reuse paths with async DB-first nonce consumption and a
      non-resettable one-resolution production guard, without making restart
      reacquire authority.
- [ ] Execute the reviewed RED/mutation matrix and merge/push Task 59 only
      after its independent review passes; no live contact or deployment.

### Task 10: Staged Fail-Closed Restart Controller — Hexmem Task 51

**Files:** create host lifecycle controller/service files and fake-command tests; do not install or deploy them in this plan.

- [ ] Document and review exact green preconditions before implementation.
- [ ] Implement pinned checksum/source/flags/database/authority verification and bounded backoff.
- [ ] Prove fake restart, every refusal gate, and restart-storm handling.
- [ ] Leave Task 51's live `deploy` criterion untouched until Task 45 closes and the operator acknowledges a fresh window.

Implementation also waits for Tasks 64, 66, 67, 68, and reviewed Task 69; the
current nullable status and five-loop registry are not valid preflight input.

### Task 11: Whole-Plugin Arm and Preflight Readiness — Hexmem Task 69 Design

**Files:** extend `crates/revops/src/cutover_arm.rs`, `crates/revops/src/main.rs`, and rehearsal tests.

- [ ] First design and independently review a global epoch-bound positive
      Python-authority-off proof that survives the canonical RPC namespace
      transition; missing Python RPCs are never proof.
- [ ] Define a v2 arm bound to the exact complete subsystem/inventory digest,
      node, clean commit, binary hash, release, mode, store identities, times,
      and nonce. Supersets and subsets both deny.
- [ ] Parse without mutation; validate and revalidate strict evidence; only
      then burn the durable nonce and atomically consume the arm into one
      non-cloneable whole-plugin capability and exact sealed runtime set.
- [ ] Bind every action to store-verified governor/reservation capabilities
      and its exact canonical request digest, not caller-provided booleans or
      nonempty strings.
- [ ] Specify rollback that destroys Rust mutation capability first, preserves
      ambiguous outcomes/reservations, proves mutation surfaces absent, then
      restores Python; a consumed arm is never reconstructed.

### Task 12: Final Code-Complete Audit and Checklist Freeze

**Files:** modify `docs/port/PARITY-CHECKLIST.md`; create a generated 69-RPC/loop/action inventory under `docs/port/`.

- [ ] Land Task 64's deterministic generated inventory early and keep it red
      until each owning task supplies real evidence.
- [ ] Recompute compiled, reachable, effective, transport-proven,
      review-passed, soak-required, and promotion-ready states from source/tests.
- [ ] Require 69/69 Python-equivalent RPC registration and all production-equivalent observer loop spawns.
- [ ] Require fake-boundary coverage for every external mutation type and zero unauthorized action construction in observer mode.
- [ ] Embed the final inventory digest in the release manifest and arm schema;
      prohibit count-only, source-presence, placeholder-refusal, nullable-health,
      and manual-checklist shortcuts.
- [ ] Mark only soak and actual cutover rows open; publish the reviewed, green code-complete commit without deploying it.
