# Task 57 F1-F8 Corrections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct all F1-F8 review findings while preserving Python as sole mutation authority and leaving the four deferred subsystem loops honestly `not_wired`.

**Architecture:** Durable loop state gains a suspension dimension that terminal writes cannot clear, plus canonical-schema and single-terminal CAS enforcement. The fee owner moves every producer onto one fixed-capacity Tokio ingress, observer construction becomes token- and concrete-pass-set-bound, and cadence is prepared idle then activated only after plugin start.

**Tech Stack:** Rust, Tokio bounded MPSC/oneshot, rusqlite, cln-plugin, Cargo integration/unit/doctests.

## Global Constraints

- Work only in `/home/sat/bin/cl-revenue-ops-r/.worktrees/codex-precutover-completion` on `codex/precutover-completion`.
- Add correction commits on top of `d66779d`; do not rewrite the clean checkpoint history.
- RED-first for every production behavior and preserve the observed failure text in the report.
- Python remains sole mutation authority. No merge, push, deploy, live contact, shadow restart, arm change, or Hexmem criterion update.
- Production-spawn only the real fee pass; rebalance, planner, LN+, and Boltz remain `not_wired` without no-op owners.
- Run focused suites, mutation proofs, workspace all-targets, fmt, strict Clippy, and diff checks before completion.

---

### Task 1: Durable suspension, canonical schema, and single-terminal CAS (F1, F6, F8)

**Files:**
- Modify: `crates/revops-db/src/loop_health.rs`
- Modify: `crates/revops-db/src/owner.rs`
- Modify: `crates/revops-db/tests/loop_health.rs`
- Modify: `crates/revops/src/loop_health.rs`
- Modify: `crates/revops/src/rpc_health.rs`
- Modify: `crates/revops/tests/runtime.rs`

**Interfaces:**
- Produces `RuntimeStatus::{Active,Suspended}` and `LoopHealthRow::{runtime_status,last_suspended_at,last_suspension_reason}`.
- Produces `suspend_loop(id, at, reason)` through DB and observer actor layers.
- Extends `LoopHealthPersistence` with `suspend(...)` and `is_available()`.

- [ ] **Step 1: Write DB RED tests**

Add tests that require canonical runtime columns, reject a partial legacy table without ALTER/backfill, preserve suspension across reopen, reactivate current-boot ready while retaining suspension history, keep suspension after a later finish, and refuse finish-then-fail plus fail-then-finish for one generation.

- [ ] **Step 2: Verify DB RED**

Run: `cargo test -p revops-db --test loop_health -- --nocapture`

Expected: compile failures for absent runtime fields and `suspend_loop`, followed by behavioral failure if scaffolding alone is present.

- [ ] **Step 3: Write runtime and RPC RED tests**

Extend the memory store with scripted transient suspension failures and availability. Add tests for backpressure failure during an in-flight pass, begin failure with no pass, retry-until-marker recovery, actor-unavailable termination, terminal-after-suspension precedence, and `current_status == "suspended"`.

- [ ] **Step 4: Verify runtime/RPC RED**

Run: `cargo test -p revops --test runtime -- --nocapture`

Run: `cargo test -p revops --lib rpc_health::tests -- --nocapture`

Expected: missing persistence/runtime suspension API or status incorrectly remains `passed`/`error`.

- [ ] **Step 5: Implement the minimal durable contract**

Use canonical schema columns:

```rust
pub enum RuntimeStatus { Active, Suspended }

pub fn suspend_loop(
    conn: &Connection,
    id: LoopId,
    at: i64,
    reason: &str,
) -> Result<()>;
```

Registration sets exact current wiring and reactivates only `ready`, retaining historical suspension fields. Begin requires ready/active. Terminal SQL includes `AND terminal_generation < generation` and never changes runtime status. Schema initialization compares the canonical column set and rejects partial tables.

Local suspension happens once, then `persist_suspension` retries with bounded delay until success or `store.is_available()` is false. Use the same helper for begin, terminal, and backpressure failures.

- [ ] **Step 6: Verify GREEN**

Run the three focused commands from Steps 2 and 4. Expected: all pass.

- [ ] **Step 7: Mutation proof**

Temporarily remove the suspension store call and observe the in-flight suspension test RED. Restore, temporarily move suspension precedence below terminal status and observe the RPC test RED. Restore exactly and rerun focused GREEN.

- [ ] **Step 8: Commit**

Commit message: `fix(runtime): make loop suspension durable`

---

### Task 2: Fixed-capacity fee-owner ingress (F2)

**Files:**
- Modify: `crates/revops/src/fee_scheduler.rs`
- Modify: `crates/revops/src/main.rs`
- Modify: `crates/revops/tests/fee_scheduler.rs`
- Modify: `crates/revops/tests/action_surface.rs`

**Interfaces:**
- Produces `SchedulerIngress`, a private-sender wrapper around `tokio::sync::mpsc::Sender<CycleMsg>`.
- `SchedulerIngress::send` is async for plugin/RPC/prefetch producers.
- `SchedulerIngress::blocking_send` is module-private for dedicated A3 callbacks.
- Owner uses `Receiver::blocking_recv`; wake sender/receiver are bounded and private.

- [ ] **Step 1: Write bounded-ingress RED tests**

Add saturation tests that retain an unstarted owner body, fill exactly `OWNER_QUEUE_CAPACITY`, and prove the next async notification/RPC/cycle send remains pending until the owner drains. Add an A3 callback test proving `blocking_send` backpressures and is delivered exactly once in FIFO order. Add closed-owner tests proving RPC/cycle sends return explicit error and outstanding ACK senders close.

- [ ] **Step 2: Write structural RED test**

Assert the scheduler source contains neither `mpsc::channel::<CycleMsg>()` nor `unbounded_channel`, that `SchedulerHandle` exposes no unbounded wake sender, and that callback paths use `blocking_send` rather than ignored sends.

- [ ] **Step 3: Verify RED**

Run: `cargo test -p revops --test fee_scheduler bounded_owner_ingress -- --nocapture`

Run: `cargo test -p revops --test action_surface scheduler -- --nocapture`

Expected: current std unbounded owner channel/unbounded wake path is detected; saturation cannot backpressure.

- [ ] **Step 4: Implement bounded ingress**

Use:

```rust
pub const OWNER_QUEUE_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct SchedulerIngress {
    tx: tokio::sync::mpsc::Sender<CycleMsg>,
}

impl SchedulerIngress {
    pub async fn send(&self, msg: CycleMsg) -> Result<(), OwnerClosed>;
    fn blocking_send(&self, msg: CycleMsg) -> Result<(), OwnerClosed>;
}
```

Replace the owner loop with `blocking_recv`. Await sends from notifications, A3 preparation tasks, RPCs, fee pass, and trigger loop. Route every store callback through `blocking_send` with loud closed-owner handling. Remove public `wake_tx`; keep a bounded private wake channel.

- [ ] **Step 5: Verify GREEN**

Run the focused commands from Step 3 plus `cargo test -p revops --test fee_scheduler -- --nocapture`.

- [ ] **Step 6: Mutation proof**

Temporarily replace bounded owner construction with unbounded construction or bypass one callback send; confirm the structural or saturation test RED, restore, and rerun GREEN.

- [ ] **Step 7: Commit**

Commit message: `fix(fees): bound every owner ingress path`

---

### Task 3: Structural observer construction and post-start cadence (F3, F5)

**Files:**
- Modify: `crates/revops/src/fee_mode.rs`
- Modify: `crates/revops/src/runtime.rs`
- Modify: `crates/revops/src/fee_scheduler.rs`
- Modify: `crates/revops/src/main.rs`
- Modify: `crates/revops/tests/runtime.rs`
- Modify: `crates/revops/tests/action_surface.rs`

**Interfaces:**
- Produces non-forgeable `ObserverModeToken` from validated passive/autonomous mode only.
- Produces private-field `ObserverPassSet` with `empty()` and `with_fee(Arc<FeeObserverPass>)`.
- Produces validated authority selection that invokes a live factory only for live mode.
- Replaces immediate cadence spawn with `FeeCadenceActivation::activate(self)`.

- [ ] **Step 1: Write structural RED proofs**

Add compile-fail doctests for `ObserverModeToken` struct literals and arbitrary `ObserverPassSet` fields/trait-object injection. Add tests that validated passive and autonomous modes each select observer construction while a live factory closure panics if invoked. Keep source scans as defense in depth.

- [ ] **Step 2: Write paused-time cadence RED test**

Construct an idle activation with a one-second interval. Advance paused time before activation and assert zero loop generations/requests. Activate, advance once, and assert the first request appears.

- [ ] **Step 3: Verify RED**

Run: `cargo test -p revops --doc runtime -- --nocapture`

Run: `cargo test -p revops --test runtime -- --nocapture`

Run: `cargo test -p revops --test action_surface -- --nocapture`

Expected: public arbitrary pass map remains accepted; token/pass set absent; cadence runs immediately.

- [ ] **Step 4: Implement structural APIs and startup ordering**

The public runtime constructor accepts exactly `(ObserverModeToken, store, ObserverPassSet)`. Keep generic fake assembly in a private function used by unit tests only. Main derives the token before moving the validated mode, builds the owner/runtime and an idle activation, calls `configured.start(state).await`, then consumes activation. Dropping activation on start error does nothing.

- [ ] **Step 5: Verify GREEN and mutate**

Run Step 3 commands. Temporarily invoke the live factory in one observer branch and observe the panic-factory test RED. Temporarily activate cadence during construction and observe paused-time RED. Restore and rerun GREEN.

- [ ] **Step 6: Commit**

Commit message: `fix(runtime): seal observer construction and activation`

---

### Task 4: Complete ACK matrix and ordering proof (F4)

**Files:**
- Modify: `crates/revops/src/fee_scheduler.rs` only if tests expose a defect.
- Modify: `crates/revops/tests/fee_scheduler.rs`

**Interfaces:**
- Uses `run_or_defer_cycle_with_ack` and the bounded ingress from Task 2.
- Preserves `cycle_completion`: only `CycleOutcome::Ran` maps to `Ok(())`.

- [ ] **Step 1: Write RED-first ACK matrix tests**

Drive a pending A3 occurrence, enqueue two prepared cycles, and assert the first ACK is explicit superseded `Err`, the second stays pending, then receives the actual terminal result after the matching A3 callback. Add separate skipped/error outcome assertions and owner-loss cases for queued and deferred receivers.

- [ ] **Step 2: Verify RED or missing-proof state**

Run: `cargo test -p revops --test fee_scheduler acknowledgement -- --nocapture`

If existing code passes a behavior immediately, strengthen the test with the required ordering/owner-loss observation until it detects removal of the handoff.

- [ ] **Step 3: Apply minimal fix if needed and verify GREEN**

Do not refactor ACK code unless a RED test exposes a defect. Rerun the focused command and the full fee-scheduler test target.

- [ ] **Step 4: Mutation proofs**

Remove each handoff in turn: prior deferred `Err`, newest deferred sender store, terminal sender, and owner-held sender drop/closure. Each corresponding test must RED. Restore exactly and rerun GREEN.

- [ ] **Step 5: Commit**

Commit message: `test(fees): prove deferred cycle acknowledgements`

---

### Task 5: Checklist, report, and full gates

**Files:**
- Modify: `docs/port/PARITY-CHECKLIST.md`
- Modify: `.superpowers/sdd/task-3-report.md`

**Interfaces:**
- Report maps F1-F8 to exact code, tests, RED evidence, mutation evidence, and commit SHAs.

- [ ] **Step 1: Update documentation honestly**

Record durable suspension, bounded all-producer fee ingress, token/pass-set boundary, ACK matrix, post-start activation, canonical schema, current-boot wiring amendment, and single-terminal CAS. Do not mark later loops reachable/effective.

- [ ] **Step 2: Run focused gates**

Run DB loop-health, runtime, health RPC, action-surface, fee-scheduler ACK/saturation, manifest health, and doctest targets. Record pass counts and exit codes.

- [ ] **Step 3: Run fresh full gates**

Run:

```text
cargo test --workspace --all-targets
cargo test -p revops --doc
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

- [ ] **Step 4: Update and commit the report**

Force-add the ignored report only. Commit message: `docs(sdd): report Task 57 corrections`.

- [ ] **Step 5: Verify clean checkpoint**

Run `git status --short --branch` and `git log --oneline d66779d..HEAD`. Notify the supervisor that the branch is ready for independent re-review. Do not push or merge.
