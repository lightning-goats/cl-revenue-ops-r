# Python Fee-Authority Handoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the Python plugin one default-on, queryable fee-authority gate that blocks every scheduled, triggered, and manual fee mutation path during the eventual Rust cutover while leaving all non-fee subsystems running.

**Architecture:** A small thread-safe `FeeAuthorityGate` is the sole source of Python fee authority. The CLN dynamic option updates it, a read-only RPC exposes its generation and transition time, and both the plugin entry points and the central fee-controller execution boundary deny work when it is disabled. Default `true` preserves present behavior; shadow tests use transient configuration, while manual cutover persists the disabled value.

**Tech Stack:** Python 3, pytest, pyln-client dynamic plugin options, Core Lightning `setconfig`, existing `FeeController` and fee replay capture.

## Global Constraints

- Work from `docs/superpowers/specs/2026-07-20-rust-fee-cutover-runway-design.md`.
- Work in an isolated worktree created from current `/home/sat/bin/cl_revenue_ops` `main`; do not edit or clean another worktree.
- Read `/home/sat/bin/cl_revenue_ops/AGENTS.md` and every file it requires before changing Python code.
- Use `superpowers:test-driven-development` for every behavior change: add the named failing test, observe the expected failure, then implement the minimum change.
- Use `superpowers:systematic-debugging` for unexpected failures and `superpowers:verification-before-completion` before every commit, publish, or deployment claim.
- The switch is internal fee-authority state, not a `PUBLIC_RUNTIME_KEYS` database override.
- The switch defaults to enabled. Deploying this release must not change live behavior.
- Disabling fee authority must not unload the Python plugin or disable capture, analytics, reporting, rebalancing, planner recommendations, LN+, capex, profitability, or read-only RPCs.
- When disabled, Python must not mutate fee-controller state in response to scheduled, failed-forward, policy-change, wake, manual-cycle, direct fee-setting, or dynamic `htlcmax` paths.
- No test may invoke a live action RPC. All `setchannel` assertions use fakes.
- Do not add Sling, Hive, Mycelium, fleet coordination, or another authority source.
- Commit each green logical unit separately. Do not deploy until the full Python suite passes.

---

### Task 1: Establish the isolated Python baseline

**Files:**

- Read: `/home/sat/bin/cl_revenue_ops/AGENTS.md`
- Read: `/home/sat/bin/cl_revenue_ops/README.md`
- Read all additional files required by `AGENTS.md`
- Inspect: `/home/sat/bin/cl_revenue_ops/modules/config.py`
- Inspect: `/home/sat/bin/cl_revenue_ops/modules/fee_controller.py`
- Inspect: `/home/sat/bin/cl_revenue_ops/cl-revenue-ops.py`
- Inspect: `/home/sat/bin/cl_revenue_ops/tests/test_operator_surface.py`
- Inspect: `/home/sat/bin/cl_revenue_ops/tests/test_fee_setting_execution.py`
- Inspect: `/home/sat/bin/cl_revenue_ops/tests/test_forward_hot_path.py`

- [ ] **Step 1: Create an isolated worktree and prove its base.**

  ```bash
  cd /home/sat/bin/cl_revenue_ops
  git status --short --branch
  git fetch origin
  git worktree add .worktrees/rust-fee-authority-handoff -b codex/rust-fee-authority-handoff origin/main
  git -C .worktrees/rust-fee-authority-handoff status --short --branch
  git -C .worktrees/rust-fee-authority-handoff rev-parse HEAD
  ```

  Expected: the source worktree has no unreviewed overlap; the new worktree is clean and points at the current `origin/main`. Stop if the base is not the intended production source.

- [ ] **Step 2: Read the repository contract and inventory every fee mutation path.**

  ```bash
  PY_WT=/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff
  sed -n '1,$p' "$PY_WT/AGENTS.md"
  sed -n '1,$p' "$PY_WT/README.md"
  sed -n '1,$p' "$PY_WT/modules/lnplus_swaps.py"
  sed -n '1,$p' "$PY_WT/modules/rebalance_engine_v2.py"
  sed -n '1,$p' "$PY_WT/modules/capacity_planner.py"
  sed -n '1,$p' "$PY_WT/modules/capex_budget.py"
  sed -n '1,$p' "$PY_WT/modules/capital_efficiency.py"
  sed -n '1,$p' "$PY_WT/modules/profitability_analyzer.py"
  rg -n 'run_fee_adjustment|fee_adjustment_loop|set_channel_fee|record_failed_forward|_handle_policy_change|revenue-fee-cycle|revenue-wake-all|revenue-set-fee|set_channel\(' "$PY_WT"
  ```

  Expected: the inventory contains the scheduled loop, three manual RPCs, failed-forward and policy/wake paths, and the central `FeeController.set_channel_fee` boundary. Confirm unrelated subsystems do not call the new gate.

- [ ] **Step 3: Run the focused baseline before editing.**

  ```bash
  cd "$PY_WT"
  .venv/bin/pytest -q \
    tests/test_operator_surface.py \
    tests/test_fee_setting_execution.py \
    tests/test_forward_hot_path.py \
    tests/test_fee_cycle_capture_config.py
  ```

  Expected: exit 0. Record the collected count in the implementation log; any baseline failure is diagnosed before feature work.

---

### Task 2: Add the thread-safe authority model

**Files:**

- Create: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/modules/fee_authority.py`
- Create: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/tests/test_fee_authority_handoff.py`

- [ ] **Step 1: Write failing model tests.**

  Test default enabled state, idempotent writes, generation increments only on an actual transition, monotonic transition timestamps from an injected clock, stable reasons, and concurrent reads during a transition.

  ```python
  def test_gate_defaults_enabled_and_transitions_once():
      clock = iter([1000, 1001, 1002]).__next__
      gate = FeeAuthorityGate(enabled=True, now_fn=clock)
      assert gate.snapshot().enabled is True
      off = gate.set_enabled(False, reason="setconfig")
      assert (off.enabled, off.generation, off.transitioned_at) == (False, 1, 1001)
      again = gate.set_enabled(False, reason="setconfig")
      assert again == off
  ```

  ```bash
  .venv/bin/pytest -q tests/test_fee_authority_handoff.py
  ```

  Expected before implementation: collection fails because `modules.fee_authority` does not exist.

- [ ] **Step 2: Implement the minimum locked gate.**

  Use immutable snapshots and keep time injectable:

  ```python
  @dataclass(frozen=True)
  class FeeAuthorityStatus:
      enabled: bool
      generation: int
      transitioned_at: int
      reason: str

  class FeeAuthorityGate:
      def __init__(self, enabled: bool = True, now_fn: Callable[[], float] = time.time): ...
      def snapshot(self) -> FeeAuthorityStatus: ...
      def set_enabled(self, enabled: bool, reason: str) -> FeeAuthorityStatus: ...
      def deny_reason(self, operation: str) -> dict[str, object] | None: ...
  ```

  `deny_reason` returns `None` when enabled and otherwise returns the stable machine-readable fields `status=blocked`, `reason=fee_authority_disabled`, `operation`, `generation`, and `transitioned_at`.

- [ ] **Step 3: Prove the model and commit it.**

  ```bash
  .venv/bin/pytest -q tests/test_fee_authority_handoff.py
  git diff --check
  git add modules/fee_authority.py tests/test_fee_authority_handoff.py
  git commit -m "feat(fees): add Python authority gate"
  ```

  Expected: all authority-model tests pass; no plugin behavior has changed yet.

---

### Task 3: Expose dynamic configuration and positive status

**Files:**

- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/modules/config.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/cl-revenue-ops.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/tests/test_operator_surface.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/tests/test_fee_authority_handoff.py`

- [ ] **Step 1: Add failing option and status-RPC tests.**

  Require:

  - option `revenue-ops-fee-authority-enabled` is boolean, dynamic, and defaults to `true`;
  - `Config.fee_authority_enabled` and `ConfigSnapshot.fee_authority_enabled` default to `True`;
  - init parses the option and initializes the gate;
  - the on-change callback accepts CLN boolean spellings and rejects invalid input without changing the prior state;
  - RPC `revenue-fee-authority-status` returns schema `revenue_ops_fee_authority/v1`, effective boolean, generation, transition timestamp, observed timestamp, and reason;
  - the new key is absent from `PUBLIC_RUNTIME_KEYS`.

  ```bash
  .venv/bin/pytest -q \
    tests/test_operator_surface.py \
    tests/test_fee_authority_handoff.py
  ```

  Expected before implementation: failures for the missing option, config fields, callback, and RPC.

- [ ] **Step 2: Add the config fields and plugin option.**

  Follow the existing replay-capture option pattern but keep authority separate:

  ```python
  plugin.add_option(
      "revenue-ops-fee-authority-enabled",
      True,
      "Permit Python fee evaluation and setchannel authority",
      opt_type="bool",
      dynamic=True,
      on_change=_on_fee_authority_change,
  )
  ```

  The callback parses first, then updates the `Config` field and the shared gate in one locked operation. It returns a stable readback string including the resulting generation.

- [ ] **Step 3: Add the read-only status RPC.**

  Return only current in-process state; do not infer from plugin liveness or configuration files:

  ```python
  @plugin.method("revenue-fee-authority-status")
  def revenue_fee_authority_status(plugin):
      status = fee_authority_gate.snapshot()
      return {
          "schema": "revenue_ops_fee_authority/v1",
          "enabled": status.enabled,
          "generation": status.generation,
          "transitioned_at": status.transitioned_at,
          "observed_at": int(time.time()),
          "reason": status.reason,
      }
  ```

- [ ] **Step 4: Verify configuration behavior and commit.**

  ```bash
  .venv/bin/pytest -q \
    tests/test_operator_surface.py \
    tests/test_fee_authority_handoff.py \
    tests/test_fee_cycle_capture_config.py
  git diff --check
  git add modules/config.py cl-revenue-ops.py \
    tests/test_operator_surface.py tests/test_fee_authority_handoff.py
  git commit -m "feat(fees): expose authority control and status"
  ```

  Expected: default-on behavior, valid dynamic transitions, stable status schema, and existing capture configuration all pass.

---

### Task 4: Gate scheduled, manual, and central execution paths

**Files:**

- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/cl-revenue-ops.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/modules/fee_controller.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/tests/test_fee_authority_handoff.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/tests/test_fee_setting_execution.py`

- [ ] **Step 1: Add failing denial tests for every direct path.**

  With authority disabled, assert:

  - the scheduled loop does not invoke `run_fee_adjustment`;
  - `run_fee_adjustment` returns a stable blocked result without calling `adjust_all_fees`;
  - `revenue-fee-cycle`, `revenue-wake-all`, and `revenue-set-fee` return blocked results;
  - `FeeController.set_channel_fee` performs no state mutation, governor request, or `data_service.set_channel` call;
  - dynamic `htlcmax` shares that same central denial;
  - re-enabling authority restores the existing path.

  ```bash
  .venv/bin/pytest -q \
    tests/test_fee_authority_handoff.py \
    tests/test_fee_setting_execution.py
  ```

  Expected before implementation: the fake controller and fake data service record forbidden calls.

- [ ] **Step 2: Add a shared plugin-level denial helper.**

  ```python
  def _fee_authority_denial(operation: str) -> dict[str, object] | None:
      return fee_authority_gate.deny_reason(operation)
  ```

  Check it before entering scheduled/manual fee work. Preserve each RPC's established outer response shape while including stable `reason=fee_authority_disabled` and the authority generation.

- [ ] **Step 3: Enforce the gate at the central controller boundary.**

  Inject the gate into `FeeController` with a default-enabled instance for backward-compatible direct construction. Check it at the first line of `set_channel_fee`, before waking strategy state or constructing governor/RPC requests. Do not rely only on RPC wrappers.

- [ ] **Step 4: Verify no action or state mutation and commit.**

  ```bash
  .venv/bin/pytest -q \
    tests/test_fee_authority_handoff.py \
    tests/test_fee_setting_execution.py \
    tests/test_dts_pid.py
  git diff --check
  git add cl-revenue-ops.py modules/fee_controller.py \
    tests/test_fee_authority_handoff.py tests/test_fee_setting_execution.py
  git commit -m "feat(fees): gate Python fee execution paths"
  ```

  Expected: all denial tests pass and the existing enabled-path fee tests remain unchanged.

---

### Task 5: Freeze triggered fee-state mutation after handoff

**Files:**

- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/cl-revenue-ops.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/modules/fee_controller.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/tests/test_forward_hot_path.py`
- Modify: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/tests/test_fee_authority_handoff.py`

- [ ] **Step 1: Add failing trigger-denial tests.**

  With authority disabled, assert failed/local-failed forwards do not call `record_failed_forward`, policy-change events do not wake controller state, and other fee wake paths do not update fee strategy state. Successful/settled forward accounting and non-fee analytics must still run.

  ```bash
  .venv/bin/pytest -q \
    tests/test_forward_hot_path.py \
    tests/test_fee_authority_handoff.py
  ```

  Expected before implementation: fee-state fakes record mutations while authority is disabled.

- [ ] **Step 2: Gate trigger entry points and defensive controller methods.**

  Check authority before failed-forward and policy/wake mutation in the plugin, and again inside `FeeController.record_failed_forward` and `_handle_policy_change`. This defense-in-depth prevents a future indirect caller from bypassing the plugin wrapper.

- [ ] **Step 3: Prove non-fee behavior remains live.**

  Add assertions that capture/status/read paths and normal forward accounting still return their established values with fee authority disabled. Do not gate the entire event subscriber.

- [ ] **Step 4: Run focused tests and commit.**

  ```bash
  .venv/bin/pytest -q \
    tests/test_forward_hot_path.py \
    tests/test_fee_authority_handoff.py \
    tests/test_loop_heartbeat_surface.py \
    tests/test_plugin_audit_regressions.py
  git diff --check
  git add cl-revenue-ops.py modules/fee_controller.py \
    tests/test_forward_hot_path.py tests/test_fee_authority_handoff.py
  git commit -m "fix(fees): freeze triggered state without authority"
  ```

  Expected: fee triggers are inert while unrelated subscriber and read behavior remains available.

---

### Task 6: Prove compatibility, publish, and stage default-on deployment

**Files:**

- Modify if needed: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/README.md`
- Create: `/home/sat/bin/cl_revenue_ops/.worktrees/rust-fee-authority-handoff/docs/runbooks/python-fee-authority-handoff.md`

- [ ] **Step 1: Document the exact operational contract.**

  Include these commands and their semantics:

  ```bash
  lightning-cli revenue-fee-authority-status
  lightning-cli setconfig \
    config=revenue-ops-fee-authority-enabled val=false transient=true
  lightning-cli setconfig \
    config=revenue-ops-fee-authority-enabled val=true transient=true
  ```

  State explicitly that the live cutover uses `transient=false`, verifies the returned source is `config.setconfig`, and is never performed by a timer. Do not put a live cutover command in an automated script.

- [ ] **Step 2: Run source-surface and full regression gates.**

  ```bash
  rg -n 'fee_authority|fee-authority' modules cl-revenue-ops.py tests docs
  rg -n 'sling|hive|mycelium|fleet' modules/fee_authority.py docs/runbooks/python-fee-authority-handoff.md
  .venv/bin/pytest -q \
    tests/test_fee_authority_handoff.py \
    tests/test_operator_surface.py \
    tests/test_fee_setting_execution.py \
    tests/test_forward_hot_path.py \
    tests/test_fee_cycle_capture.py \
    tests/test_fee_cycle_capture_config.py \
    tests/test_fee_cycle_capture_integration.py \
    tests/test_dts_pid.py \
    tests/test_architecture_guard.py
  .venv/bin/pytest -q
  ```

  Expected: all tests pass; the architecture search has no forbidden dependency; no test reaches a live CLN socket.

- [ ] **Step 3: Commit documentation and verify the branch.**

  ```bash
  git diff --check
  git add README.md docs/runbooks/python-fee-authority-handoff.md
  git commit -m "docs: document Python fee handoff"
  git status --short --branch
  git log --oneline origin/main..HEAD
  ```

  Expected: clean worktree and only the intended authority commits.

- [ ] **Step 4: Obtain review, publish, and stage rollback.**

  Follow `superpowers:requesting-code-review`, resolve findings with tests, rerun the full suite, then:

  ```bash
  git push -u origin codex/rust-fee-authority-handoff
  ```

  Stage the reviewed Python source and a checksummed rollback artifact through the established `lnnode` deployment process. Do not disable authority during deployment.

- [ ] **Step 5: Verify live default-on behavior.**

  After the approved plugin-only restart:

  ```bash
  ssh lnnode 'lightning-cli revenue-fee-authority-status'
  ssh lnnode 'lightning-cli listconfigs | jq ".configs[\"revenue-ops-fee-authority-enabled\"]"'
  ssh lnnode 'lightning-cli getinfo | jq -r .id'
  ```

  Expected: authority is `enabled=true`; source and effective value match the deployed default/config; the plugin remains healthy. Verify Rust remains observer/dry-run/no-broadcast and that no unrelated subsystem was restarted.

- [ ] **Step 6: Perform a transient handoff rehearsal only if separately approved.**

  Disable with `transient=true`, verify every mutation path is blocked through fakes or an observational live check, then immediately restore `true` with `transient=true` and verify the generation advances. Never leave the live node disabled after this rehearsal.

## Completion Evidence

Record the Python source commit, deployment checksum, full test count, focused denial-test count, authority status before/after deployment, any transient rehearsal generations, plugin restart scope, and rollback checksum in the Rust runway evidence directory. Completion of this plan does not authorize Rust broadcasts or fee cutover.
