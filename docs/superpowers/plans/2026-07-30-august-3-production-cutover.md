# August 3 Production Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver Python-contract parity and transfer whole-plugin production authority to the Rust plugin by 2026-08-03 without split authority or weakened fail-closed gates.

**Architecture:** Complete and review analytics evidence first, then close the exact canonical RPC surface, lifecycle ownership, and the v2 all-or-none authority handoff. Integrate small commits into `main`, serialize Cargo gates, rehearse the exact release artifact on local fakes, and require a fresh operator-approved production preflight before consuming the single-use arm.

**Tech Stack:** Rust 2021, `cln-plugin`, Tokio, `rusqlite`, Serde JSON, generated Python inventory fixtures, local fake CLN sockets, Cargo test/Clippy/fmt.

## Global Constraints

- Production authority target: 2026-08-03.
- Canonical parity source: `fixtures/port/plugin_inventory.json` and `fixtures/port/rpc_params.json`, pinned to Python commit `e579de8df523f174283fc2aa21f395c8ef006ac6`.
- Exactly 69 Python RPCs, 121 Python options, eight loop identities, four notification subscriptions, and the recorded shutdown contract must be effective.
- Preserve one all-or-none whole-plugin live capability; never stage subsystem authority.
- Canonical mutators return durable completion, not queue admission.
- Missing, malformed, stale, or unavailable evidence denies or refuses; it never authorizes action.
- No live CLN/LN+/Boltz contact, production database writes, deploy, Python shutdown, or arm consumption until Tasks 1-8 pass and Task 9 explicitly enters its production steps.
- Rust worker owns its active files; Codex uses an isolated worktree for non-overlapping implementation.
- Cargo tests, Clippy, fmt, mutation runs, and integration commits are serialized.
- Task 67 review is performed only by Codex.
- Unrelated repositories, host maintenance, scheduled checks, refactors, and features are deferred.

---

### Task 1: Finish Task 71 and perform the reserved Task 67 review

**Files:**
- Modify: `crates/revops-db/src/profitability_history.rs`
- Modify: `crates/revops-db/src/actor.rs`
- Modify: `crates/revops/src/profitability_evidence.rs`
- Modify: `crates/revops/src/profitability_assembler.rs`
- Modify: `crates/revops/src/main.rs`
- Test: `crates/revops-db/tests/profitability_history.rs`
- Test: `crates/revops/src/profitability_evidence_tests.rs`
- Test: `crates/revops/tests/manifest.rs`

**Interfaces:**
- Consumes: `DbHandle`, `ObserverHandle`, bounded CLN query transport, frozen `ChannelProfitability` classifier.
- Produces: one typed production-database profitability snapshot; typed posterior and opener evidence; result-bearing profitability, dashboard, and econ inputs; a clean Task 71 commit ready for Codex review.

- [ ] **Step 1: Finish the red-first profitability producer**

The producer must have this shape; names may follow the existing module, but the source boundaries may not change:

```rust
pub struct ProfitabilitySources<'a> {
    pub production_db: &'a revops_db::actor::DbHandle,
    pub observer: &'a revops_db::owner::ObserverHandle,
    pub channels: &'a [serde_json::Value],
    pub now: i64,
}

pub async fn gather_profitability(
    sources: ProfitabilitySources<'_>,
) -> Result<FleetProfitability, ProfitabilityEvidenceRefusal>;
```

`gather_profitability` performs one production-database await, one observer-store await, and consumes one already-fetched bounded channel snapshot. It never calls the existing split `per_channel_revenue`, `per_channel_costs`, or `channel_history` APIs.

- [ ] **Step 2: Run the focused evidence and mutation gates**

Run:

```bash
cargo test -p revops-db --test profitability_history
cargo test -p revops --lib profitability_evidence
cargo test -p revops --test manifest revenue_r_profitability
```

Expected: all focused tests pass. The mutation matrix must kill removal of the inbound daily rollup, SCID alias folding, the single transaction, posterior parse refusal, missing opener refusal, and any restored fabricated default.

- [ ] **Step 3: Finish dashboard and econ-snapshot evidence routing**

Replace only the Task 71 analytics placeholders. `revenue-dashboard` and `revenue-econ-snapshot` must consume the same result-bearing profitability/economic snapshot, not independently refetch or fabricate values.

- [ ] **Step 4: Run the Task 71 checkpoint gates**

Run from the Task 71 worktree:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Expected: all commands exit zero. Record suite and test counts from this exact worktree.

- [ ] **Step 5: Commit the Task 71 checkpoint**

```bash
git add crates/revops-db/src/actor.rs crates/revops-db/src/profitability_history.rs crates/revops-db/tests/profitability_history.rs crates/revops/src/lib.rs crates/revops/src/profitability_evidence.rs crates/revops/src/profitability_evidence_tests.rs crates/revops/src/profitability_assembler.rs crates/revops/src/main.rs crates/revops/tests/manifest.rs
git commit -m "feat(analytics): complete result-bearing profitability evidence"
```

- [ ] **Step 6: Codex performs Task 67 review**

Codex independently re-derives intended-store routing, current-boot loop readiness, no stale prior-boot pass, no fabricated required input, observer capability isolation, and mutation kills. Only Codex calls `hexmem_task_verify` for Task 67's `review` criterion.

---

### Task 2: Add an exact canonical RPC reachability gate

**Files:**
- Modify: `crates/revops/tests/manifest.rs`
- Modify: `crates/revops/tests/rpc_param_contract.rs`
- Modify: `crates/revops/src/main.rs`

**Interfaces:**
- Consumes: `load_rpc_contract()` and the initialized plugin manifest.
- Produces: exact-set assertions for 69 canonical names and a per-method parameter-decoder tripwire.

- [ ] **Step 1: Write the failing exact-set test**

Add a test that sorts the canonical contract names and initialized canonical plugin RPC names, then asserts equality:

```rust
#[test]
fn canonical_mode_registers_exactly_the_python_rpc_set() {
    let expected = revops::rpc_params::load_rpc_contract()
        .methods
        .into_iter()
        .map(|method| method.name)
        .collect::<std::collections::BTreeSet<_>>();
    let actual = canonical_manifest_rpc_names();
    assert_eq!(actual, expected);
}
```

Also assert that Rust-only names are absent in canonical mode and remain available only under the `revenue-r-*` shadow namespace.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p revops --test manifest canonical_mode_registers_exactly_the_python_rpc_set -- --exact
```

Expected: FAIL listing the 19 absent canonical names.

- [ ] **Step 3: Add a shared method lookup and decoder wrapper**

Add to `rpc_params.rs`:

```rust
pub fn method_spec(contract: &RpcParameterContract, name: &str) -> RpcMethodSpec {
    contract.methods.iter().find(|method| method.name == name)
        .unwrap_or_else(|| panic!("missing embedded RPC contract for {name}"))
        .clone()
}
```

Every canonical registration captures its `RpcMethodSpec` and calls `decode_params(..., ParamBinding::PositionalOrNamed)` before owner logic.

- [ ] **Step 4: Commit the RED gate separately**

```bash
git add crates/revops/tests/manifest.rs crates/revops/tests/rpc_param_contract.rs crates/revops/src/rpc_params.rs
git commit -m "test(rpc): require exact Python method and parameter set"
```

---

### Task 3: Wire canonical fee and read-only core RPCs

**Files:**
- Create: `crates/revops/src/rpc_fee_authority_status.rs`
- Create: `crates/revops/src/rpc_profile_preview.rs`
- Modify: `crates/revops/src/main.rs`
- Modify: `crates/revops/src/lib.rs`
- Modify: `crates/revops/src/rpc_capex_status.rs`
- Modify: `crates/revops/src/rpc_total_cost_budget.rs`
- Test: `crates/revops/tests/manifest.rs`
- Test: `crates/revops/tests/read_rpcs.rs`

**Interfaces:**
- Consumes: current Rust authority state, fee cycle owner, `FeeTrigger::WakeAll`, typed capex/economic evidence, exact parameter specs.
- Produces: effective `revenue-fee-authority-status`, `revenue-fee-cycle`, `revenue-wake-all`, `revenue-profile-preview`, `revenue-capex-status`, and `revenue-total-cost-budget`.

- [ ] **Step 1: Write response-contract tests from Python fixtures**

For each method, test named and positional calls, unavailable-source refusal, legitimate empty data, and success. `revenue-wake-all` and `revenue-fee-cycle` must await a typed owner reply that includes a completed cycle or wake count.

- [ ] **Step 2: Implement typed completion replies**

Use this boundary rather than returning after `send`:

```rust
pub struct FeeCycleCompletion {
    pub generation: i64,
    pub completed_at: i64,
    pub result: Result<serde_json::Value, String>,
}

pub async fn run_now(&self, reason: String) -> anyhow::Result<FeeCycleCompletion>;
pub async fn wake_all(&self) -> anyhow::Result<FeeCycleCompletion>;
```

- [ ] **Step 3: Remove nullable evidence from capex and total-cost-budget**

Replace optional required inputs with one typed `Result<...>` producer. Missing production evidence returns the Python error branch; it never emits a success object with `_phase1b_gaps`.

- [ ] **Step 4: Run focused tests and commit**

```bash
cargo test -p revops --test read_rpcs
cargo test -p revops --test manifest fee_
git add crates/revops/src crates/revops/tests/read_rpcs.rs crates/revops/tests/manifest.rs
git commit -m "feat(rpc): wire canonical fee and core read methods"
```

---

### Task 4: Wire canonical state-writer mutators

**Files:**
- Create: `crates/revops/src/rpc_state_mutators.rs`
- Modify: `crates/revops/src/state_writer.rs`
- Modify: `crates/revops-db/src/state_writer.rs`
- Modify: `crates/revops/src/main.rs`
- Test: `crates/revops/tests/action_surface.rs`
- Test: `crates/revops-db/tests/state_writer.rs`

**Interfaces:**
- Consumes: `ProductionStateWriter`, Python-equivalent policy/tag semantics, exact parameter contracts.
- Produces: completed `ban`, `unban`, `ignore`, `unignore`, `cleanup-closed`, `clear-reservations`, `spend-reserve`, `spend-release`, `spend-release-stale`, and `spend-settle` responses.

- [ ] **Step 1: Write owner-completion and observer-denial tests**

Each test must prove: observer runtime cannot construct the handler dependency; live fake writer commits the intended row transition; response arrives after commit; actor loss, queue full, transaction failure, and unknown outcome return distinct errors.

- [ ] **Step 2: Add a non-cloneable live mutator bundle**

```rust
pub struct CoreStateLiveCapability {
    _seal: (),
}

pub struct CoreMutators {
    writer: ProductionStateWriter,
    _live: CoreStateLiveCapability,
}
```

Do not expose `StateWriterHandle` or a generic SQL closure through the RPC layer.

- [ ] **Step 3: Implement exact Python response builders**

Response builders accept typed completion receipts. They do not inspect database connections or convert queue admission into success.

- [ ] **Step 4: Mutation-test every false-success seam**

Kill mutations that return before reply, swallow transaction errors, default missing policy rows, clear reservations after partial failure, or retry unknown outcomes.

- [ ] **Step 5: Run focused tests and commit**

```bash
cargo test -p revops-db --test state_writer
cargo test -p revops --test action_surface state_
git add crates/revops-db/src/state_writer.rs crates/revops-db/tests/state_writer.rs crates/revops/src/state_writer.rs crates/revops/src/rpc_state_mutators.rs crates/revops/src/main.rs crates/revops/tests/action_surface.rs
git commit -m "feat(rpc): wire completed core state mutations"
```

---

### Task 5: Complete econ ownership and live-only set-fee

**Files:**
- Create: `crates/revops/src/econ_owner.rs`
- Modify: `crates/revops/src/rpc_econ_reconcile.rs`
- Modify: `crates/revops/src/main.rs`
- Modify: `crates/revops/src/fee_execution.rs`
- Test: `crates/revops/tests/action_surface.rs`
- Test: `crates/revops/tests/read_rpcs.rs`

**Interfaces:**
- Consumes: econ ledger owner, production database snapshot, governor/reservations, whole-plugin live capability.
- Produces: completed `revenue-econ-reconcile`, `revenue-econ-cycle`, and `revenue-set-fee`.

- [ ] **Step 1: Add dry-run and apply-path RED tests**

Dry-run reconcile may run in observer mode and returns no `applied` key. Apply and cycle require the live capability and durable intents. `set-fee` requires one governed intent, one durable reservation, one setchannel result, and a terminal receipt.

- [ ] **Step 2: Implement one serialized econ owner**

```rust
pub enum EconCommand {
    Reconcile { apply: bool, reply: tokio::sync::oneshot::Sender<anyhow::Result<serde_json::Value>> },
    Cycle { reply: tokio::sync::oneshot::Sender<anyhow::Result<serde_json::Value>> },
}
```

The actor handles one command at a time. Apply failures and unknown outcomes retain reservations and quarantine.

- [ ] **Step 3: Route set-fee through the existing governed fee executor**

No direct `RpcBroadcaster::set_channel` call is permitted from the RPC closure.

- [ ] **Step 4: Run focused tests and commit**

```bash
cargo test -p revops --test action_surface econ_
cargo test -p revops --test action_surface set_fee
git add crates/revops/src/econ_owner.rs crates/revops/src/rpc_econ_reconcile.rs crates/revops/src/fee_execution.rs crates/revops/src/main.rs crates/revops/tests/action_surface.rs
git commit -m "feat(rpc): add governed econ and set-fee owners"
```

---

### Task 6: Close every canonical partial response

**Files:**
- Modify: `crates/revops/src/rpc_status.rs`
- Modify: `crates/revops/src/rpc_report.rs`
- Modify: `crates/revops/src/rpc_dashboard.rs`
- Modify: `crates/revops/src/rpc_health.rs`
- Modify: `crates/revops/src/rpc_profitability.rs`
- Modify: `crates/revops/src/rpc_analyze.rs`
- Modify: `crates/revops/src/rpc_econ_snapshot.rs`
- Modify: `crates/revops/src/rpc_capacity_report.rs`
- Modify: `crates/revops/src/rpc_policy.rs`
- Modify: `crates/revops/src/rpc_hot_channel_protection_peers.rs`
- Test: `crates/revops/tests/manifest.rs`
- Test: `crates/revops/tests/read_rpcs.rs`

**Interfaces:**
- Consumes: completed Task 71 evidence and Tasks 3-5 owners.
- Produces: Python-equivalent success/error shapes with no placeholder markers or required null gaps.

- [ ] **Step 1: Add a canonical placeholder scan test**

Invoke every read-only canonical method with a valid local fixture and reject these markers in any success response: `not_yet_ported`, `_phase1b_gaps`, `_not_wired`, `ANALYTICS_GAP`, and required `null` fields listed by the Python response contract.

- [ ] **Step 2: Replace builders one surface at a time**

Each builder consumes a typed evidence struct. If required evidence is unavailable, return the exact Python error branch instead of a partial success object.

- [ ] **Step 3: Mutation-test placeholder regressions**

For each formerly partial method, temporarily restore its old placeholder path and prove the manifest test fails.

- [ ] **Step 4: Run exact-set and workspace gates, then commit**

```bash
cargo test -p revops --test manifest
cargo test -p revops --test read_rpcs
git add crates/revops/src/rpc_*.rs crates/revops/tests/manifest.rs crates/revops/tests/read_rpcs.rs
git commit -m "feat(rpc): close canonical response-contract gaps"
```

---

### Task 7: Complete lifecycle, datastore, subscription, and drain parity

**Files:**
- Modify: `crates/revops-db/src/notifications.rs`
- Modify: `crates/revops-db/src/owner.rs`
- Modify: `crates/revops/src/notify.rs`
- Create: `crates/revops/src/lifecycle.rs`
- Modify: `crates/revops/src/main.rs`
- Test: `crates/revops-db/tests/notifications.rs`
- Test: `crates/revops-db/tests/owner.rs`
- Create: `crates/revops/tests/lifecycle.rs`
- Modify: `crates/revops/tests/manifest.rs`

**Interfaces:**
- Consumes: exact four-subscription inventory, current-boot identity, strict store identities, all runtime owner handles.
- Produces: current-boot startup receipt, restart-safe cursors, datastore producer receipts, and a complete bounded shutdown receipt.

- [ ] **Step 1: Write RED lifecycle tests**

Tests cover all four subscriptions, cursor hydration, duplicate delivery, crash-before-commit, restart resume, retention health, device/inode alias denial, missing schema, owner panic, bounded drain timeout, and successful full join.

- [ ] **Step 2: Implement a lifecycle owner**

```rust
pub struct ShutdownReceipt {
    pub stopped_intake: bool,
    pub drained_owners: Vec<String>,
    pub joined_owners: Vec<String>,
    pub persisted_cursors: Vec<String>,
    pub completed_at: i64,
}

pub async fn shutdown(self) -> Result<ShutdownReceipt, LifecycleRefusal>;
```

Shutdown order is: stop new intake, persist notification cursors, drain action owners, drain observer owners, flush stores, join every owner, then return the receipt.

- [ ] **Step 3: Bind startup and health to the exact release identity**

No prior-boot receipt, loop generation, cursor, or datastore status can satisfy current-boot readiness.

- [ ] **Step 4: Run focused and workspace gates, then commit**

```bash
cargo test -p revops-db --test notifications
cargo test -p revops-db --test owner
cargo test -p revops --test lifecycle
cargo test -p revops --test manifest
git add crates/revops-db/src crates/revops-db/tests crates/revops/src/lifecycle.rs crates/revops/src/notify.rs crates/revops/src/main.rs crates/revops/tests/lifecycle.rs crates/revops/tests/manifest.rs
git commit -m "feat(runtime): complete lifecycle and shutdown ownership"
```

---

### Task 8: Implement the v2 whole-plugin authority handoff

**Files:**
- Modify: `crates/revops/src/cutover_arm.rs`
- Modify: `crates/revops/src/python_authority.rs`
- Modify: `crates/revops/src/runtime.rs`
- Create: `crates/revops/src/whole_plugin_authority.rs`
- Modify: `crates/revops/src/main.rs`
- Modify: `crates/revops/src/bin/rehearse_fee_cutover.rs`
- Test: `crates/revops/tests/cutover_arm.rs`
- Test: `crates/revops/tests/python_authority.rs`
- Test: `crates/revops/tests/action_surface.rs`
- Test: `crates/revops/tests/fee_cutover_rehearsal.rs`

**Interfaces:**
- Consumes: exact release/inventory/store identity, positive epoch-bound Python authority-off proof, current-boot lifecycle receipt, fresh nonce, strict preflight snapshot.
- Produces: one non-cloneable `WholePluginLiveCapability`, postflight receipt, and rollback receipt.

- [ ] **Step 1: Write the v2 denial matrix before implementation**

Tests deny: missing authority RPC, absence-as-proof, stale epoch, wrong source commit, wrong binary hash, wrong inventory digest, wrong configuration digest, database path alias by device/inode, stale loop receipt, incomplete owner set, consumed nonce, consume-before-preflight, state drift between parse and consume, partial capability construction, and rollback that restores Python before destroying Rust authority.

- [ ] **Step 2: Define the sealed capability**

```rust
pub struct WholePluginLiveCapability {
    fee: FeeLiveCapability,
    rebalance: RebalanceLiveCapability,
    capital: CapitalLiveCapability,
    lnplus: LnPlusLiveCapability,
    boltz: BoltzLiveCapability,
    core: CoreStateLiveCapability,
    _not_clone: std::marker::PhantomData<*mut ()>,
}
```

The constructor is private and returns the full struct or an error without leaking any child capability.

- [ ] **Step 3: Implement parse, preflight, reverify, consume, construct, postflight**

Arm bytes are parsed without mutation. All evidence is fetched and verified. The same evidence is fetched again immediately before durable nonce consumption. Construction happens only after successful consumption and returns all-or-none.

- [ ] **Step 4: Implement rollback destruction order**

Drop/revoke the whole Rust capability; drain and join Rust action owners; preserve ambiguous reservations/quarantine; verify Rust mutation absence; restore Python authority; verify Python authority epoch and canonical namespace.

- [ ] **Step 5: Run the handoff mutation matrix and commit**

```bash
cargo test -p revops --test cutover_arm
cargo test -p revops --test python_authority
cargo test -p revops --test action_surface
cargo test -p revops --test fee_cutover_rehearsal
git add crates/revops/src/cutover_arm.rs crates/revops/src/python_authority.rs crates/revops/src/runtime.rs crates/revops/src/whole_plugin_authority.rs crates/revops/src/main.rs crates/revops/src/bin/rehearse_fee_cutover.rs crates/revops/tests
git commit -m "feat(cutover): add whole-plugin v2 authority handoff"
```

---

### Task 9: Integrate, rehearse, and execute production cutover

**Files:**
- Modify: `docs/port/PARITY-CHECKLIST.md`
- Create: `docs/cutover/2026-08-02-rehearsal-evidence.md`
- Create: `docs/cutover/2026-08-03-production-cutover-evidence.md`

**Interfaces:**
- Consumes: exact integrated release commit and binary, all prior receipts, operator approval.
- Produces: green rehearsal evidence, production preflight/postflight evidence, and tested rollback evidence.

- [ ] **Step 1: Integrate all reviewed commits into `main`**

Require a clean worktree and inspect every merge diff. Do not resolve conflicts by discarding either side; re-run focused tests for every conflicted module.

- [ ] **Step 2: Run fresh release gates**

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Also run exact inventory, placeholder scan, mutation matrices, and local-fake action-surface tests. Record the exact commit, binary SHA-256, suite counts, and command exits.

- [ ] **Step 3: Run the August 2 local-fake rehearsal**

Use `rehearse_fee_cutover` extended to whole-plugin scope. Prove a successful transition and every denial/rollback case against temporary stores and fake sockets. Write the evidence document with the exact artifact identity.

- [ ] **Step 4: Prepare the production rollback before deployment**

Stage the prior Python configuration and binary, the Rust capability-destruction command, service/plugin stop/start commands, store backups, and fresh-arm recovery procedure. Verify the rollback artifacts without changing authority.

- [ ] **Step 5: Run the fresh production preflight and request operator approval**

Read-only preflight must be green on the installed artifact. Present exact RPC/option/loop/subscription equality, authority-off proof plan, store identities, current-boot receipts, nonce, and rollback readiness. Do not consume the arm before the operator approves this evidence.

- [ ] **Step 6: Execute the authority transfer as observable stages**

The operator-approved sequence is: quiesce Python mutation intake; drain Python; verify positive epoch-bound Python authority off; transition canonical namespace; consume the fresh arm; construct Rust whole-plugin capability; start Rust owners; verify postflight. Check rollback conditions after every stage.

- [ ] **Step 7: Verify production and close the release**

Verify exact canonical RPCs, eight current-boot loops, four subscriptions, datastore producers, zero quarantine/persistence failures, and one Rust whole-plugin authority holder. Record evidence and independently verify before marking Tasks 66, 68, 69, and the release complete.

## Plan Self-Review

- Spec coverage: Tasks 1-9 cover analytics, exact RPC names and parameters, canonical responses, lifecycle/datastore ownership, all-or-none handoff, rehearsal, rollback, and production evidence.
- Placeholder scan: the plan contains no unresolved placeholder work; every task has exact files, interfaces, commands, outcomes, and a commit boundary.
- Type consistency: the result-bearing owner replies feed RPC builders; lifecycle receipt feeds handoff preflight; whole-plugin capability feeds mutator construction; the exact artifact identity flows from integration through rehearsal and production.
