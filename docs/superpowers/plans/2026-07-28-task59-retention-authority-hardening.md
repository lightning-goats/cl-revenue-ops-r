# Task 59 Retention, Timeout, Authority, and Arm-Reuse Hardening Implementation Plan

> **For Codex:** Execute this plan incrementally with RED-first tests, focused gates after each slice, and a clean checkpoint commit before moving to the next independent invariant.

**Goal:** Implement the reviewed Task 58 revision-3 hardening contract on canonical Task 61 base `9c99d7c2bebfd829e92fae24eef6a7e47e0e4d29` without changing live authority scope or Task 61's observational loop-health model.

**Architecture:** Keep every SQLite mutation behind the existing single-owner actor. Add a classified, bounded retention subsystem and durable arm-nonce ledger there; expose two-phase admission/receipt APIs so callers can distinguish refused work from admitted work whose outcome becomes unknown. Keep Python-authority proof endpoint-bound and linear by moving the second read inside a consuming bracket close. Resolve startup mode through a private repeatable kernel while the public production wrapper consumes a process-global, non-resettable token exactly once.

**Tech Stack:** Rust 2021, Tokio bounded MPSC/oneshot, rusqlite transactions, serde/serde_json, existing CLN JSON-RPC proxy, cargo test/fmt/clippy.

---

## Slice 1: Retention classification and bounded sweep

**Files:**
- Create: `crates/revops-db/src/retention.rs`
- Modify: `crates/revops-db/src/lib.rs`
- Modify: `crates/revops-db/src/fee_runway.rs`
- Modify: `crates/revops-db/src/owner.rs`
- Modify: `crates/revops/src/fee_state.rs`
- Modify: `crates/revops/src/fee_scheduler.rs`
- Test: `crates/revops-db/tests/retention.rs`
- Test: `crates/revops/src/fee_scheduler.rs`

1. Add RED tests for the exact table classification, never-prune preservation, per-table batch bounds, global eight-batch cap, fair cursor continuation, keep-last runway snapshots, and owner-thread-only dispatch.
2. Run the focused tests and capture the expected failures before implementation.
3. Implement constants and a complete schema inventory lint, classifying every application table exactly once and allowlisting only SQLite internals.
4. Implement one-transaction-per-batch deletes in fixed table order, one batch per table per round, a global batch cap, and a persisted in-memory cursor returned to the owner for the next sweep.
5. Add an owner command plus `RunwayStateStore` dispatch seam. Schedule a sweep only after a successful scheduled cycle commit, off the cycle owner; expose a never-reset failure counter in fee debug and never block/refuse a successful fee cycle because retention failed.
6. Run focused DB and scheduler tests, self-review the diff, and commit the slice.

## Slice 2: Store/query two-phase admission and terminal result persistence

**Files:**
- Modify: `crates/revops-db/src/owner.rs`
- Modify: `crates/revops/src/fee_execution.rs`
- Modify: `crates/revops/src/fee_scheduler.rs`
- Modify: `crates/revops/src/main.rs`
- Test: `crates/revops-db/tests/owner.rs`
- Test: `crates/revops/src/fee_execution.rs`
- Test: `crates/revops/src/fee_scheduler.rs`

1. Add RED tests for `try_send` full/closed admission refusal, admitted receipt timeout/outcome-unknown, the seven-second store floor without changing wire timeout, query admission refusal, query response timeout, and terminal result-write failure for success/rejected/clean-failure/ambiguous outcomes.
2. Run focused tests and record the RED evidence.
3. Add typed store admission and receipt APIs. Clamp only store work to `BUSY_TIMEOUT + 2s`; classify pre-admission failure as `store_admission_refused` and post-admission expiry as `store_intent_outcome_unknown`.
4. Add Query-only `SchedulerIngress::try_send_query`; use bounded response waiting in diagnostic RPCs with stable `owner_queue_saturated` and `owner_response_timeout` codes. Keep effectful scheduler messages on async backpressure.
5. Make every terminal broadcast-result write fallible. On persistence failure, poison execution, attempt quarantine, and return `ResultPersistenceUnknown { request_id, rpc_outcome, detail }`, preserving the Task 61 admitted/acknowledged/outcome-unknown vocabulary.
6. Add pending-intent age to diagnostics, run focused tests, self-review, and commit the slice.

## Slice 3: Endpoint-bound Python authority bracket and dispatch freshness

**Files:**
- Modify: `crates/revops/src/python_authority.rs`
- Modify: `crates/revops/src/fee_execution.rs`
- Modify: `crates/revops/src/bin/rehearse_fee_cutover.rs`
- Test: `crates/revops/src/python_authority.rs`
- Test: `crates/revops/src/fee_execution.rs`
- Test: `crates/revops/tests/action_surface.rs`

1. Add RED tests for stale-open refusal before the second fetch, single-use close, endpoint/client binding, advancing observation, live-authorization dispatch staleness, and compile-fail pins against forging/reuse.
2. Run focused tests and capture the expected compile/runtime failures.
3. Make `PythonAuthorityOff` fields private with narrow accessors. Add a non-clone `OpenBracket` that owns the originating client, first reading, and open timestamp; `PythonAuthorityClient::open_bracket(self, ...)` consumes the client.
4. Implement crate-private consuming `close(self, ...)`, checking bracket age and first-reading freshness before issuing exactly one second endpoint read, then enforcing the stable advancing epoch.
5. Make authorization consume the bracket and close it immediately before minting. Add a private `minted_at` and reject authorization older than 30 seconds at dispatch.
6. Update offline rehearsal/tests, run focused gates plus doctests, self-review, and commit the slice.

## Slice 4: DB-first arm nonce consumption and one-resolution guard

**Files:**
- Modify: `crates/revops-db/src/fee_runway.rs`
- Modify: `crates/revops-db/src/owner.rs`
- Modify: `crates/revops/src/cutover_arm.rs`
- Modify: `crates/revops/src/main.rs`
- Test: `crates/revops-db/tests/owner.rs`
- Test: `crates/revops/src/cutover_arm.rs`
- Test: `crates/revops/src/main.rs`
- Test: `crates/revops/tests/action_surface.rs`

1. Add RED tests for nonce uniqueness, DB-first ordering, DB failure preserving the arm file, duplicate nonce refusal even under a different path, current-thread Tokio operation, kernel mode-matrix repeatability, and exactly-one public resolution.
2. Run focused tests and capture RED evidence.
3. Add append-only `rust_consumed_arm_nonces` DDL and owner command/API. Map primary-key collision to `ReusedNonce` and every other insert failure to `ConsumeFailed`.
4. Split arm handling into pure `validate()` returning a private non-clone validated capability and `consume_validated()` performing the atomic rename.
5. Add private async `resolve_startup_mode_kernel`: state gate, arm validation, awaited durable nonce insert, filesystem consume, then fee-mode validation. Add a private non-clone startup-resolution token guarded by a static `AtomicBool::swap` with no reset/test constructor. The public async wrapper consumes that token; production has one call site.
6. Move matrix tests to the private kernel and keep exactly one test for the global guard. Run focused tests, action-surface pins, self-review, and commit the slice.

## Slice 5: Runbook, mutations, full verification, and handoff

**Files:**
- Modify: the Task 58-specified operator runbook section(s)
- Create: `/home/sat/agent-tasks/task-59-implementation-report.md`

1. Update retention/runbook language: no automatic `VACUUM`; any operator-manual `VACUUM` requires the plugin stopped. List never-prune evidence and state DB-path plus ledger restore/backup policy.
2. Execute the reviewed R/T/F/A mutation matrix. For signature-only compile-shape pins, use the documented compile-failure substitution and disclose it in the report.
3. Run focused tests again, then `cargo test --workspace --all-targets`, workspace doctests, release tests/build, `cargo fmt --all -- --check`, and `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
4. Inspect `git diff --check`, full diff, status, and commit history. Request an implementation code review and address only technically verified findings.
5. Write the implementation report with exact commits, RED/GREEN evidence, mutation outcomes, test counts, safety-boundary confirmation, and remaining review ownership.
6. After owner verification, mark only Task 59 `impl` PASS in Hexmem. Leave `review` untouched, make the final clean checkpoint commit, and notify the supervisor with the exact SHA and no-push/no-merge statement.
