# Shadow Parity and Deployment Closeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce exact offline fee-cycle parity evidence from naturally captured Python authority cycles, repair the shadow comparison harnesses, and activate a checksummed Rust release that remains structurally observer-only and dry-run-only.

**Architecture:** The Python plugin remains the sole live fee authority and emits default-off, atomic, versioned replay envelopes. Rust gains a strict offline replay boundary around the existing `run_fee_cycle` kernel by replacing concrete clock, entropy, and governor dependencies with traits that have production and transcript implementations. Live deployment is a separate attested-artifact workflow; no replay component can construct CLN RPC, SQLite, journal, ledger, or action-capable objects.

**Tech Stack:** Python 3/pytest/pyln-client; Rust 1.97/Cargo/serde/serde_json/sha2; CLN JSON-RPC for plugin lifecycle and read-only verification; SQLite CLI for read-only comparisons; SSH/SCP/SHA-256 for deployment provenance.

## Global Constraints

- Work from the approved design:
  `docs/superpowers/specs/2026-07-19-shadow-parity-deployment-closeout-design.md`.
- Also treat the Python worktree design
  `docs/superpowers/specs/2026-07-18-fee-cycle-replay-capture-design.md`
  as binding for schema, completeness, retention, and window thresholds.
- Preserve the dirty Python capture worktree in place. Never reset, clean, or
  overwrite its three modified files.
- Before implementation, invoke `superpowers:using-git-worktrees`; keep Rust
  feature work isolated from `main` until its focused tests pass.
- For every defect or behavior change, invoke
  `superpowers:test-driven-development` and observe a failing test before the
  implementation change.
- On every failure or unexpected live result, invoke
  `superpowers:systematic-debugging`; do not patch the symptom.
- Before any completion, commit, push, or deployment claim, invoke
  `superpowers:verification-before-completion`.
- Python remains the only process with fee execution authority. Rust stays
  `observer=true` and `fee-dryrun=true`.
- Do not add or invoke `setchannel`, `revenue-fee-cycle`,
  `revenue-wake-all`, `revenue-set-fee`, payment, rebalance, channel,
  planner, Boltz, or on-chain action RPCs.
- Do not add Sling, Hive, Mycelium, fleet coordination, or a coordinator.
- Rust must continue to open
  `/data/lightningd/.lightning/revenue_ops.db` read-only and may write only
  its observer database and dry-run artifacts.
- No parity claim may use `tools/diff-harness/diff_fee_decisions.py`; it is
  retained only as a post-cycle diagnostic.
- A live deployment is accepted only when the local, staged, installed, and
  running Rust artifact checksums are identical.
- Keep one implementation task in progress at a time and commit after every
  green logical unit.

---

### Task 1: Close out and commit the Python atomic capture

**Files:**

- Read completely:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/AGENTS.md`
- Read completely:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/README.md`
- Read completely:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/modules/lnplus_swaps.py`
- Read completely:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/modules/rebalance_engine_v2.py`
- Read completely:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/modules/rebalance_planner_v2.py`
- Read completely:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/modules/capex_budget.py`
- Read completely:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/modules/profitability_analyzer.py`
- Review/modify:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/modules/fee_controller.py`
- Review/modify:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/modules/fee_cycle_capture.py`
- Review/modify:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/tests/test_fee_cycle_capture_integration.py`
- Modify:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/schemas/fee_cycle_replay.v0.schema.json`
- Verify:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/tests/test_fee_cycle_capture.py`
- Verify:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/tests/test_fee_cycle_capture_config.py`
- Modify:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/tests/test_fee_cycle_replay_wire.py`

- [ ] **Step 1: Establish the protected baseline and read the required material.**

  ```bash
  PY_WT=/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python
  git -C "$PY_WT" status --short --branch
  git -C "$PY_WT" diff --check
  sed -n '1,$p' "$PY_WT/AGENTS.md"
  sed -n '1,$p' "$PY_WT/README.md"
  sed -n '1,$p' "$PY_WT/modules/lnplus_swaps.py"
  sed -n '1,$p' "$PY_WT/modules/rebalance_engine_v2.py"
  sed -n '1,$p' "$PY_WT/modules/rebalance_planner_v2.py"
  sed -n '1,$p' "$PY_WT/modules/capex_budget.py"
  sed -n '1,$p' "$PY_WT/modules/profitability_analyzer.py"
  rg -l 'revenue-(status|history|report|dashboard|health|config)' "$PY_WT/tests" \
    | sort \
    | xargs -r -n1 sed -n '1,$p'
  ```

  Expected: branch `codex/fee-cycle-replay-capture-python`; only the known
  capture files are dirty; `diff --check` is clean. Stop if unrelated dirty
  files appear.

- [ ] **Step 2: Re-run the focused capture baseline before editing.**

  ```bash
  cd "$PY_WT"
  .venv/bin/pytest -q \
    tests/test_fee_cycle_capture.py \
    tests/test_fee_cycle_capture_config.py \
    tests/test_fee_cycle_replay_wire.py \
    tests/test_fee_cycle_capture_integration.py
  ```

  Expected: exit 0, currently 66 tests passing. A different count is
  acceptable only when the collected test names explain it; no failure is
  acceptable.

- [ ] **Step 3: Review the dirty implementation against the pinned contracts.**

  ```bash
  git diff -- modules/fee_controller.py modules/fee_cycle_capture.py \
    tests/test_fee_cycle_capture_integration.py
  rg -n 'time\.time|random\.random|random\.gauss' modules/fee_controller.py
  rg -n 'decision_now|decision_random|decision_gauss' \
    modules/fee_controller.py tests/test_fee_cycle_capture_integration.py
  ```

  Confirm all 28 clock sites, four Gaussian call sites, the Vegas random
  site, the 19 `FeeEvidence` operations, governor request/result, execution
  request/result, pre-state, one terminal outcome per ordered channel, and
  post-state are covered by stable labels and ordinals. Confirm capture code
  never re-runs evidence I/O.

- [ ] **Step 4: Add a failing schema-conformance test for real session output.**

  The approved design and `FeeCycleCaptureSession.to_body()` include
  top-level `started_at`, but the current closed JSON schema neither declares
  nor requires that field. Add a test that constructs a real session body,
  seals it, and validates it with Draft 2020-12 `jsonschema`. Assert
  `started_at` is required and is a non-empty string.

  ```bash
  .venv/bin/pytest -q \
    tests/test_fee_cycle_replay_wire.py::test_real_session_envelope_matches_closed_schema
  ```

  Expected before the schema fix: failure because `started_at` is an
  additional property.

- [ ] **Step 5: Align schema version 0 with the approved envelope.**

  Add `started_at` to `required` and `properties` in
  `schemas/fee_cycle_replay.v0.schema.json`. Do not relax
  `additionalProperties: false`.

  ```bash
  .venv/bin/pytest -q tests/test_fee_cycle_replay_wire.py
  ```

  Expected: all wire tests pass, including real session schema validation.

- [ ] **Step 6: Prove the capture contract tests detect drift.**

  Run the existing negative mutation tests:

  ```bash
  .venv/bin/pytest -q \
    tests/test_fee_cycle_capture_integration.py::test_decision_call_inventory_rejects_label_expression_and_count_drift \
    tests/test_fee_cycle_capture_integration.py::test_recording_failure_never_changes_delegated_result \
    tests/test_fee_cycle_capture_integration.py::test_disabled_capture_preserves_seeded_result_and_state
  ```

  Expected: all pass. If review finds an uncovered path, first add the
  smallest named regression test beside these tests, run it to observe the
  expected failure, and only then patch the capture implementation.

- [ ] **Step 7: Verify the authority path and no-action invariant.**

  ```bash
  .venv/bin/pytest -q \
    tests/test_fee_cycle_capture_integration.py::test_full_cycle_capture_records_pre_state_outcome_and_post_state \
    tests/test_fee_cycle_capture_integration.py::test_capture_records_pre_state_before_sleep_mutation \
    tests/test_fee_cycle_capture_integration.py::test_policy_and_overlay_skips_are_explicit_terminal_outcomes \
    tests/test_fee_cycle_capture_integration.py::test_each_major_dynamic_skip_has_one_terminal_outcome \
    tests/test_fee_cycle_capture_integration.py::test_dry_run_records_execution_without_calling_setchannel \
    tests/test_fee_cycle_capture_integration.py::test_governor_and_execution_request_result_transcripts
  rg -n 'setchannel|revenue-fee-cycle|revenue-wake-all|revenue-set-fee' \
    modules/fee_cycle_capture.py tests/test_fee_cycle_capture*.py
  ```

  Expected: tests pass. Any textual match must be a negative assertion or
  documentation, never a call.

- [ ] **Step 8: Run the broader Python regression gates.**

  ```bash
  .venv/bin/pytest -q \
    tests/test_fee_controller.py \
    tests/test_dts_pid.py \
    tests/test_architecture_guard.py \
    tests/test_fee_cycle_capture.py \
    tests/test_fee_cycle_capture_config.py \
    tests/test_fee_cycle_replay_wire.py \
    tests/test_fee_cycle_capture_integration.py
  .venv/bin/pytest -q
  ```

  Expected: both commands exit 0 with no skips newly attributable to this
  change and no live RPCs.

- [ ] **Step 9: Commit only the intended Python capture files.**

  ```bash
  git diff --check
  git status --short
  git add modules/fee_controller.py modules/fee_cycle_capture.py \
    schemas/fee_cycle_replay.v0.schema.json \
    tests/test_fee_cycle_capture_integration.py \
    tests/test_fee_cycle_replay_wire.py
  git diff --cached --check
  git commit -m "feat(fees): complete atomic replay capture"
  git status --short --branch
  ```

  Expected: the capture worktree is clean after the commit.

---

### Task 2: Repair and pin the shadow comparison harnesses

**Files:**

- Modify: `tools/diff-harness/diff_read_rpcs.py`
- Modify: `tools/diff-harness/diff_config.py`
- Modify: `tools/diff-harness/diff_fee_decisions.py`

- [ ] **Step 1: Add failing self-tests for the read-RPC database default.**

  Introduce:

  ```python
  DEFAULT_OBSERVER_DB = "/data/lightningd/.lightning/revops-r-observer.db"
  ```

  First add a self-test assertion that parses default arguments and requires
  `args.observer_db == DEFAULT_OBSERVER_DB` and `not args.observer_db.startswith("~")`.
  Run:

  ```bash
  python3 tools/diff-harness/diff_read_rpcs.py --self-test
  ```

  Expected before changing the parser default: failure mentioning the old
  `~/.lightning/revops-r-observer.db`.

- [ ] **Step 2: Make the absolute observer database the parser default.**

  Refactor argument construction into `build_parser()` so self-test and
  `main()` use the same parser. Keep `--observer-db` override support.

  ```bash
  python3 tools/diff-harness/diff_read_rpcs.py --self-test
  ```

  Expected: `[self-test] ALL PASS`.

- [ ] **Step 3: Add failing tests for the four Python field remaps.**

  In `diff_config.py`, add:

  ```python
  PYTHON_FIELD_MAP = {
      "vegas-reflex": "enable_vegas_reflex",
      "vegas-decay": "vegas_decay_rate",
      "planner-max-fee-rate": "planner_max_fee_rate_sat_vb",
      "boltz-structural-budget-sats": "boltz_structural_budget_sats_per_day",
  }
  ```

  Add stubbed self-tests that assert the Python CLI receives each mapped field
  and the Rust CLI still receives the original suffix. Run:

  ```bash
  python3 tools/diff-harness/diff_config.py --self-test
  ```

  Expected before wiring the map: failure on the first mapped key.

- [ ] **Step 4: Split remapped fields from constructor-only options.**

  Rename `OPTION_ONLY_KEYS` to `CONSTRUCTOR_ONLY_KEYS` and leave exactly these
  twelve entries:

  ```text
  boltz-enabled
  boltz-cli-path
  boltz-datadir
  boltz-use-sudo
  boltz-sudo-user
  boltz-timeout-seconds
  boltz-daily-budget-sats
  boltz-enforce-budget
  boltz-btc-wallet
  boltz-lbtc-wallet
  boltz-routing-fee-limit-ppm
  boltz-max-withdraw-sats
  ```

  Change `diff_key()` to resolve
  `PYTHON_FIELD_MAP.get(suffix, suffix.replace("-", "_"))`. Add a
  `diff_constructor_option()` path that normalizes the live
  `listconfigs revenue-ops-<suffix>` value against `revenue-r-config`, with
  explicit tests for bool, int, string, null, malformed response, and
  transport failure.

  ```bash
  python3 tools/diff-harness/diff_config.py --self-test
  ```

  Expected: `[self-test] ALL PASS`; the report says 12 option-surface checks,
  not 16 skipped keys.

- [ ] **Step 5: Mark the old fee journal comparison diagnostic-only.**

  Add a top-level warning to the module docstring and live output:

  ```text
  DIAGNOSTIC ONLY: post-cycle Python state is not a valid fee-parity input.
  Use replay_fee_capture against complete fee_cycle_replay v0 envelopes for
  the fee parity gate.
  ```

  Do not change its comparison or exit-code semantics.

  ```bash
  python3 tools/diff-harness/diff_fee_decisions.py --self-test
  ```

  Expected: existing self-tests pass and the diagnostic warning is present.

- [ ] **Step 6: Run all harness self-tests and commit.**

  ```bash
  python3 tools/diff-harness/diff_config.py --self-test
  python3 tools/diff-harness/diff_read_rpcs.py --self-test
  python3 tools/diff-harness/diff_fee_decisions.py --self-test
  git diff --check
  git add tools/diff-harness/diff_config.py \
    tools/diff-harness/diff_read_rpcs.py \
    tools/diff-harness/diff_fee_decisions.py
  git commit -m "fix(harness): compare effective shadow inputs"
  ```

---

### Task 3: Add the strict Rust replay wire and manifest validator

**Files:**

- Modify: `crates/revops-fees/Cargo.toml`
- Modify: `crates/revops-fees/src/lib.rs`
- Create: `crates/revops-fees/src/replay_wire.rs`
- Create: `crates/revops-fees/tests/replay_wire.rs`
- Create: `fixtures/fees/replay/complete_skip.v0.json`
- Copy/reference:
  `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python/schemas/fee_cycle_replay.v0.schema.json`

- [ ] **Step 1: Add a real sealed Python fixture.**

  Use `modules.fee_cycle_replay_wire.seal_envelope()` from the Python
  worktree to create one minimal complete, skipped-channel envelope. Copy the
  resulting bytes unchanged to
  `fixtures/fees/replay/complete_skip.v0.json`; never hand-edit the digest.
  The fixture must include the required top-level `started_at`.

- [ ] **Step 2: Write failing wire tests.**

  Test:

  - exact `schema_name == "fee_cycle_replay"` and `schema_version == 0`;
  - unknown top-level fields rejected;
  - body larger than 32 MiB rejected before full replay construction;
  - missing/invalid/tampered `payload_sha256` rejected;
  - tagged floats parse from `{"__f__":"<CPython repr>"}`;
  - untagged JSON floats, non-finite strings, and extra tagged-float keys
    rejected;
  - `completeness.complete` must be true;
  - evaluated-channel and terminal-outcome counts agree;
  - manifest sequence is one run ID, strictly consecutive, closed, drained,
    with zero failed/dropped captures.

  ```bash
  cargo test -p revops-fees --test replay_wire
  ```

  Expected: compile failure because `replay_wire` does not exist.

- [ ] **Step 3: Implement the versioned wire types and digest check.**

  Use `#[serde(deny_unknown_fields)]` on closed structures. Preserve flexible
  payload sections as recursively validated `WireValue`, not unchecked
  `serde_json::Value`. Remove `payload_sha256`, canonicalize the remaining
  integer/string/tagged-float body with
  `revops_core::canonical::canonical_json`, and verify SHA-256 in constant
  time-equivalent byte comparison.

  Add `sha2.workspace = true` and `hex.workspace = true` to
  `crates/revops-fees/Cargo.toml`.

  Public API:

  ```rust
  pub const REPLAY_SCHEMA_NAME: &str = "fee_cycle_replay";
  pub const REPLAY_SCHEMA_VERSION: i64 = 0;
  pub const MAX_REPLAY_ENVELOPE_BYTES: usize = 32 * 1024 * 1024;

  pub fn parse_fee_capture(bytes: &[u8])
      -> Result<FeeCycleReplayV0, ReplayWireError>;

  pub fn validate_capture_manifest(
      manifest: &FeeCaptureManifestV0,
      captures: &[FeeCycleReplayV0],
  ) -> Result<(), ReplayWireError>;
  ```

- [ ] **Step 4: Run focused tests and commit.**

  ```bash
  cargo fmt --all -- --check
  cargo test -p revops-fees --test replay_wire
  cargo clippy -p revops-fees --all-targets -- -D warnings
  git add crates/revops-fees/Cargo.toml crates/revops-fees/src/lib.rs \
    crates/revops-fees/src/replay_wire.rs \
    crates/revops-fees/tests/replay_wire.rs \
    fixtures/fees/replay/complete_skip.v0.json
  git commit -m "feat(replay): validate fee capture wire"
  ```

---

### Task 4: Replace hard-wired decision context with production-safe traits

**Files:**

- Modify: `crates/revops-fees/src/pyrand.rs`
- Modify: `crates/revops-fees/src/vegas.rs`
- Modify: `crates/revops-fees/src/thompson/sampling.rs`
- Modify: `crates/revops-fees/src/execution.rs`
- Modify: `crates/revops-fees/src/cycle.rs`
- Modify: `crates/revops/src/fee_scheduler.rs`
- Modify: existing tests under `crates/revops-fees/tests/`
- Create: `crates/revops-fees/tests/decision_context.rs`

- [ ] **Step 1: Write failing entropy abstraction tests.**

  Define the intended seam in tests:

  ```rust
  pub trait DecisionEntropy {
      fn random(&mut self, label: &str) -> Result<f64, DecisionInputError>;
      fn gauss(
          &mut self,
          label: &str,
          mu: f64,
          sigma: f64,
      ) -> Result<f64, DecisionInputError>;
  }
  ```

  Require the production `PyRandom` adapter to preserve every existing pinned
  sequence and ignore labels only after validating they are non-empty.

  ```bash
  cargo test -p revops-fees --test decision_context
  ```

  Expected: compile failure because the trait is absent.

- [ ] **Step 2: Refactor entropy users without changing production draws.**

  Implement `DecisionEntropy for PyRandom`. Change Vegas and Thompson
  sampling to call the exact Python semantic labels:

  - `vegas.boost`
  - `thompson.prior`
  - `thompson.posterior`
  - `thompson.polynomial.coefficient.0`
  - `thompson.polynomial.coefficient.1`
  - `thompson.polynomial.coefficient.2`

  Preserve short-circuit order and Gaussian-cache behavior. Propagate
  transcript errors fail-closed through `run_fee_cycle`; do not substitute a
  random value.

  ```bash
  cargo test -p revops-fees --test pyrand --test vegas \
    --test thompson_sampling --test decision_context
  ```

  Expected: all prior bit-for-bit fixtures and new interface tests pass.

- [ ] **Step 3: Write failing clock transcript tests.**

  Define:

  ```rust
  pub trait DecisionClock {
      fn now(&mut self, label: &str) -> Result<i64, DecisionInputError>;
  }
  ```

  Add a fixed production clock whose value is captured once by the scheduler
  but can be consumed at every semantically labeled call site. Add tests that
  pin label and branch order against the Python capture inventory, including
  `cycle.started_at`, `cycle.channel.evaluate`, `pid.calculate`,
  `vegas.update`, `governor.authorize`, `fee.apply`, and
  `fee.state_sync`.

  Expected before implementation: compile failure for `DecisionClock`.

- [ ] **Step 4: Thread the clock through the kernel.**

  Replace `CycleDeps.now: i64` with `CycleDeps.clock: &mut dyn DecisionClock`.
  Read timestamps at the same semantic boundaries as Python. The production
  scheduler still calls `now_unix()` exactly once per cycle and constructs
  `FixedDecisionClock`; this preserves the current one-clock-per-cycle live
  invariant while making transcript order observable offline.

  ```bash
  cargo test -p revops-fees --test cycle --test decision_context
  cargo test -p revops --test fee_scheduler
  ```

  Expected: all pass; scheduler tests still prove one wall-clock read per
  cycle.

- [ ] **Step 5: Write failing governor abstraction tests.**

  Define:

  ```rust
  pub trait FeeAuthorizer {
      fn authorize(
          &self,
          request: &FeeAuthorizationRequest,
      ) -> Result<FeeAuthorizationResult, DecisionInputError>;
  }
  ```

  Test that the production adapter returns the existing `GovernedTrace` and
  ledger behavior, while a scripted adapter needs no `EconLedger` or
  `ActiveIntentRegistry`.

- [ ] **Step 6: Refactor governor calls behind `FeeAuthorizer`.**

  Keep `GovernedDeps` as production plumbing and implement a
  `GovernedFeeAuthorizer` wrapper around
  `governed_authorize_fee_broadcast`. Change `CycleDeps.governed` to
  `authorizer: Option<&dyn FeeAuthorizer>`. Preserve all current fail-closed
  behavior and reason strings.

  ```bash
  cargo test -p revops-fees --test cycle
  cargo test -p revops-fees
  cargo test -p revops --test fee_scheduler
  ```

  Expected: all pass; no journal or ledger is required by the scripted
  adapter.

- [ ] **Step 7: Commit the decision-context refactor.**

  ```bash
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test -p revops-fees
  cargo test -p revops --test fee_scheduler
  git add crates/revops-fees/src crates/revops-fees/tests \
    crates/revops/src/fee_scheduler.rs crates/revops/tests/fee_scheduler.rs
  git commit -m "refactor(fees): inject replayable decision context"
  ```

---

### Task 5: Implement strict transcript adapters, state import, and exact comparison

**Files:**

- Modify: `crates/revops-fees/src/lib.rs`
- Create: `crates/revops-fees/src/replay.rs`
- Create: `crates/revops-fees/tests/replay.rs`
- Create: `fixtures/fees/replay/complete_adjustment.v0.json`
- Modify for direct state import: `crates/revops-fees/src/state_store.rs`
- Modify for exact output conversion: `crates/revops-fees/src/journal.rs`

- [ ] **Step 1: Add a representative complete Python adjustment fixture.**

  Generate it with the Python capture manager from a seeded test cycle that
  exercises evidence, at least one entropy draw, governor authorization,
  execution dry-run, state flush, and an adjusted terminal outcome. Seal it
  with Python and copy the bytes unchanged to
  `fixtures/fees/replay/complete_adjustment.v0.json`.

- [ ] **Step 2: Write failing strict-cursor tests.**

  Cover:

  - evidence operation, arguments, ordinal, and result;
  - clock label, ordinal, and value;
  - entropy op, label, arguments, ordinal, and result;
  - governor request/result;
  - execution request/result;
  - extra, missing, duplicated, reordered, or unconsumed entries;
  - exception/fallback entries;
  - exact tagged-float bit reconstruction.

  Every mismatch must include family, expected ordinal, expected label/op,
  actual label/op, and JSON path.

  ```bash
  cargo test -p revops-fees --test replay strict_
  ```

  Expected: compile failure because replay adapters are absent.

- [ ] **Step 3: Implement typed transcript cursors.**

  Implement `TranscriptEvidence`, `TranscriptClock`, `TranscriptEntropy`,
  `TranscriptAuthorizer`, `TranscriptExecution`, and `MemoryStateSink`.
  Each adapter consumes one entry in order and errors on mismatch. No adapter
  has a fallback to live I/O or a default value.

- [ ] **Step 4: Write failing pre-state import tests.**

  Import captured:

  - global Vegas state;
  - ordered channel cycle state;
  - ordered channel Thompson/PID state;
  - skip-gate prior epoch;
  - summaries and counters required by `ControllerState`.

  Test that unknown state fields, missing required fields, duplicate channel
  IDs, and a state channel order different from `ordered_channels` fail
  closed.

- [ ] **Step 5: Implement direct `ControllerState` construction.**

  Add a replay-only constructor that accepts typed captured state. Do not
  hydrate from SQLite and do not query `FeeEvidence` to reconstruct state.
  Keep production hydration unchanged.

- [ ] **Step 6: Write the failing end-to-end replay test.**

  Required public API:

  ```rust
  pub fn replay_fee_capture(
      capture: &FeeCycleReplayV0,
  ) -> Result<FeeReplayResultV0, ReplayError>;
  ```

  Assert the sealed adjustment fixture produces exact:

  - ordered terminal outcomes;
  - `FeeDecision` trace objects;
  - decision summary;
  - governed/execution transcript;
  - post-state;
  - complete consumption of all six transcript families.

  Also assert malformed fixtures return structured errors and never panic.

- [ ] **Step 7: Implement replay around the existing pure kernel.**

  Construct only in-memory adapters; pass `journal=None`; use
  `MemoryStateSink`; call `run_fee_cycle` once; canonicalize actual and
  expected wire values and return path-addressed mismatches. Add a compile-time
  dependency guard test that `replay.rs` contains no `cln_rpc`, `rusqlite`,
  `Journal`, `EconLedger`, `std::net`, or process-spawn imports.

  ```bash
  cargo test -p revops-fees --test replay
  cargo test -p revops-fees
  ```

  Expected: both fixtures replay exactly and every corruption case fails
  closed.

- [ ] **Step 8: Commit replay adapters and comparator.**

  ```bash
  cargo fmt --all -- --check
  cargo clippy -p revops-fees --all-targets -- -D warnings
  cargo test -p revops-fees
  git add crates/revops-fees/src crates/revops-fees/tests \
    fixtures/fees/replay/complete_adjustment.v0.json
  git commit -m "feat(replay): run strict offline fee parity"
  ```

---

### Task 6: Add the offline replay CLI and closed-window runner

**Files:**

- Create: `crates/revops/src/bin/replay_fee_capture.rs`
- Create: `crates/revops/tests/replay_cli.rs`
- Create: `tools/replay_fee_capture_window.py`
- Create: `tools/tests/test_replay_fee_capture_window.py`
- Modify: `crates/revops/Cargo.toml` only if an existing workspace dependency
  must be exposed to the CLI

- [ ] **Step 1: Write failing CLI contract tests.**

  Require:

  ```text
  replay_fee_capture --capture <file>
  replay_fee_capture --manifest <file> --capture-dir <dir>
  ```

  Reject all unknown flags, especially `--node`, `--rpc-file`,
  `--lightning-dir`, and `--db`. Exit codes:

  - `0`: all captures exact;
  - `1`: parity mismatch;
  - `2`: malformed/incomplete input or I/O error.

  Output one JSON object with commit, run ID, capture count, evaluated channel
  count, adjustment count, mismatch count, and per-file result.

  ```bash
  cargo test -p revops --test replay_cli
  ```

  Expected: failure because the binary does not exist.

- [ ] **Step 2: Implement the Rust CLI with manual strict argument parsing.**

  Do not add a broad CLI framework. Read only the explicit local paths,
  delegate validation and replay to `revops-fees`, and never construct the
  plugin, scheduler, RPC actor, database actor, journal, or ledger.

- [ ] **Step 3: Add the window runner tests.**

  `tools/replay_fee_capture_window.py` must:

  - read one closed manifest;
  - require `queue_drained=true`, `failed=0`, and `dropped=0`;
  - require consecutive retained sequences;
  - require at least six complete cycles, 100 evaluations, and 10 actual
    adjustments;
  - invoke the explicit local replay binary once for the selected window;
  - emit a machine-readable JSON verdict;
  - never call SSH or Lightning RPC.

  ```bash
  pytest -q tools/tests/test_replay_fee_capture_window.py
  ```

  Expected before implementation: import/file failure.

- [ ] **Step 4: Implement and verify both runners.**

  ```bash
  cargo test -p revops --test replay_cli
  pytest -q tools/tests/test_replay_fee_capture_window.py
  cargo run -q -p revops --bin replay_fee_capture -- \
    --capture fixtures/fees/replay/complete_adjustment.v0.json
  ```

  Expected: exit 0 and JSON with `mismatch_count: 0`.

- [ ] **Step 5: Run workspace gates and commit.**

  ```bash
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  cargo test --workspace --release
  pytest -q tools/tests/test_replay_fee_capture_window.py
  git add crates/revops/src/bin/replay_fee_capture.rs \
    crates/revops/tests/replay_cli.rs crates/revops/Cargo.toml \
    tools/replay_fee_capture_window.py \
    tools/tests/test_replay_fee_capture_window.py
  git commit -m "feat(replay): add offline capture window runner"
  ```

---

### Task 7: Integrate and publish the reviewed source commits

**Files:**

- Python branch:
  `codex/fee-cycle-replay-capture-python`
- Rust implementation branch created for this plan
- Python `main` worktree:
  `/home/sat/bin/cl_revenue_ops`
- Rust `main` worktree:
  `/home/sat/bin/cl-revenue-ops-r`

- [ ] **Step 1: Invoke `superpowers:requesting-code-review`.**

  Review both diffs against the approved design. Resolve findings with TDD and
  re-run the focused gates before integration.

- [ ] **Step 2: Verify both branches are clean and based on known commits.**

  ```bash
  git -C /home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python \
    status --short --branch
  git status --short --branch
  git log -1 --format='%H %s'
  ```

  Expected: no working-tree changes.

- [ ] **Step 3: Integrate Python without rewriting the capture history.**

  ```bash
  git -C /home/sat/bin/cl_revenue_ops fetch origin
  git -C /home/sat/bin/cl_revenue_ops switch main
  git -C /home/sat/bin/cl_revenue_ops merge --no-ff \
    codex/fee-cycle-replay-capture-python \
    -m "merge: atomic fee-cycle replay capture"
  cd /home/sat/bin/cl_revenue_ops
  .venv/bin/pytest -q \
    tests/test_fee_cycle_capture.py \
    tests/test_fee_cycle_capture_config.py \
    tests/test_fee_cycle_replay_wire.py \
    tests/test_fee_cycle_capture_integration.py
  .venv/bin/pytest -q
  ```

  Expected: clean merge and full green suite. Do not force-push.

- [ ] **Step 4: Integrate Rust and rerun the final source gates.**

  Use `superpowers:finishing-a-development-branch`; merge the reviewed Rust
  implementation branch into `main`, then:

  ```bash
  cd /home/sat/bin/cl-revenue-ops-r
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  cargo test --workspace --release
  python3 tools/diff-harness/diff_config.py --self-test
  python3 tools/diff-harness/diff_read_rpcs.py --self-test
  python3 tools/diff-harness/diff_fee_decisions.py --self-test
  git status --short --branch
  ```

  Expected: all gates pass and both `main` worktrees are clean.

- [ ] **Step 5: Push both exact reviewed histories.**

  ```bash
  git -C /home/sat/bin/cl_revenue_ops push origin main
  git -C /home/sat/bin/cl-revenue-ops-r push origin main
  git -C /home/sat/bin/cl_revenue_ops rev-parse HEAD origin/main
  git -C /home/sat/bin/cl-revenue-ops-r rev-parse HEAD origin/main
  ```

  Expected: each pair of commit IDs is identical.

---

### Task 8: Deploy Python capture instrumentation default-off

**Files/paths:**

- Local source: `/home/sat/bin/cl_revenue_ops`
- Remote checkout: `/data/lightningd/plugins/cl_revenue_ops`
- Remote plugin:
  `/data/lightningd/plugins/cl_revenue_ops/cl-revenue-ops.py`
- Remote capture output:
  `/data/lightningd/.lightning/revenue_ops_fee_replay/`

- [ ] **Step 1: Record the live baseline and prove capture is off.**

  ```bash
  ssh lnnode 'cd /data/lightningd/plugins/cl_revenue_ops &&
    git status --short --branch &&
    git rev-parse HEAD &&
    lightning-cli revenue-status &&
    lightning-cli revenue-health'
  ```

  Expected: clean remote checkout and healthy Python authority. Save the
  returned commit as `PY_PREV`.

- [ ] **Step 2: Fast-forward the clean remote checkout to the reviewed commit.**

  ```bash
  ssh lnnode 'cd /data/lightningd/plugins/cl_revenue_ops &&
    git fetch origin &&
    git merge --ff-only origin/main &&
    git status --short --branch &&
    git rev-parse HEAD'
  ```

  Expected: remote HEAD equals local Python `origin/main`. Stop before plugin
  restart on any dirty state or non-fast-forward.

- [ ] **Step 3: Restart only Python and verify default-off behavior.**

  ```bash
  ssh lnnode 'lightning-cli plugin stop \
    /data/lightningd/plugins/cl_revenue_ops/cl-revenue-ops.py &&
    lightning-cli plugin start \
    /data/lightningd/plugins/cl_revenue_ops/cl-revenue-ops.py'
  ssh lnnode 'lightning-cli revenue-status &&
    lightning-cli revenue-health &&
    lightning-cli revenue-config get fee_replay_capture_enabled'
  ```

  Expected: Python is active and healthy; capture value is `false`; no active
  capture manifest exists.

- [ ] **Step 4: Verify rollback before enabling capture.**

  If health fails, disable capture if possible, stop the plugin, switch the
  clean remote checkout to detached `PY_PREV`, and restart the same absolute
  plugin path. Rust remains an observer throughout.

---

### Task 9: Build, attest, and activate the exact Rust release

**Files/paths:**

- Local binary: `target/release/revops`
- Remote deploy dir: `/home/lightningd/revops-r-deploy`
- Installed binary: `/home/lightningd/revops-r-deploy/revops`
- Production DB: `/data/lightningd/.lightning/revenue_ops.db`
- Observer DB: `/data/lightningd/.lightning/revops-r-observer.db`
- Journal dir: `/data/lightningd/.lightning`

- [ ] **Step 1: Invoke `superpowers:verification-before-completion` and build from a clean commit.**

  ```bash
  cd /home/sat/bin/cl-revenue-ops-r
  git status --porcelain
  RUST_COMMIT=$(git rev-parse HEAD)
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  cargo test --workspace --release
  cargo build --release -p revops
  ```

  Expected: empty `git status`, all gates green.

- [ ] **Step 2: Record the local artifact identity and compatibility.**

  ```bash
  LOCAL_SHA=$(sha256sum target/release/revops | awk '{print $1}')
  stat -c '%s %n' target/release/revops
  file target/release/revops
  ldd target/release/revops
  printf '%s %s\n' "$RUST_COMMIT" "$LOCAL_SHA"
  ```

  Expected: x86-64 ELF; all dependencies resolve; `libsqlite3.so.0` is
  dynamically linked; no `revops-rebalance` or action binary is linked.

- [ ] **Step 3: Stage the exact bytes and verify before stopping Rust.**

  ```bash
  scp target/release/revops \
    "lnnode:/home/lightningd/revops-r-deploy/revops.new.$LOCAL_SHA"
  ssh lnnode "chmod +x \
    /home/lightningd/revops-r-deploy/revops.new.$LOCAL_SHA &&
    sha256sum /home/lightningd/revops-r-deploy/revops.new.$LOCAL_SHA &&
    file /home/lightningd/revops-r-deploy/revops.new.$LOCAL_SHA &&
    ldd /home/lightningd/revops-r-deploy/revops.new.$LOCAL_SHA"
  ```

  Expected: staged SHA equals `LOCAL_SHA`; dependencies resolve. Any failure
  leaves the current observer running.

- [ ] **Step 4: Preserve rollback bytes and start a fresh journal epoch.**

  ```bash
  ssh lnnode 'OLD_SHA=$(sha256sum \
    /home/lightningd/revops-r-deploy/revops | awk "{print \$1}") &&
    cp -p /home/lightningd/revops-r-deploy/revops \
    /home/lightningd/revops-r-deploy/revops.rollback.$OLD_SHA'
  ssh lnnode 'lightning-cli plugin stop \
    /home/lightningd/revops-r-deploy/revops'
  ssh lnnode 'if test -f \
    /data/lightningd/.lightning/fee_dryrun_journal.jsonl; then
    mv /data/lightningd/.lightning/fee_dryrun_journal.jsonl \
    /data/lightningd/.lightning/fee_dryrun_journal.pre-closeout.$(date -u +%Y%m%dT%H%M%SZ).jsonl;
    fi'
  ssh lnnode "mv \
    /home/lightningd/revops-r-deploy/revops.new.$LOCAL_SHA \
    /home/lightningd/revops-r-deploy/revops"
  ```

- [ ] **Step 5: Start with every shadow option explicit.**

  ```bash
  ssh lnnode 'lightning-cli -k plugin subcommand=start \
    plugin=/home/lightningd/revops-r-deploy/revops \
    revops-r-observer=true \
    revops-r-fee-dryrun=true \
    revops-r-db-path=/data/lightningd/.lightning/revenue_ops.db \
    revops-r-observer-db-path=/data/lightningd/.lightning/revops-r-observer.db \
    revops-r-journal-dir=/data/lightningd/.lightning'
  ```

  Expected: plugin start succeeds. On failure, atomically restore the
  checksum-addressed rollback binary and restart it with these same explicit
  options.

- [ ] **Step 6: Prove installed and running artifact identity.**

  ```bash
  ssh lnnode 'sha256sum /home/lightningd/revops-r-deploy/revops'
  ssh lnnode 'lightning-cli revenue-r-status &&
    lightning-cli plugin list'
  ```

  Expected: installed SHA equals `LOCAL_SHA`; both Python and Rust are active;
  Rust reports observer and fee dry-run true.

- [ ] **Step 7: Prove production DB access is read-only and no action path exists.**

  Inspect the running Rust PID's `/proc/<pid>/fd` symlink for
  `revenue_ops.db`, then its matching `fdinfo`. Decode access mode from the
  low two flag bits and require `O_RDONLY`. Confirm its only writable
  database descriptor is `revops-r-observer.db`.

  ```bash
  strings target/release/revops | rg 'setchannel|revenue-set-fee'
  cargo test -p revops no_setchannel_symbol_in_crate
  ```

  Expected: no actionable Rust fee RPC symbol; the safety test passes.

---

### Task 10: Collect a bounded natural capture window and replay it

**Files/paths:**

- Remote output:
  `/data/lightningd/.lightning/revenue_ops_fee_replay/`
- Local evidence dir:
  `/tmp/revenue_ops_fee_replay-2026-07-19/`
- Local replay binary:
  `target/release/replay_fee_capture`

- [ ] **Step 1: Enable only the dynamic observational capture.**

  ```bash
  ssh lnnode 'lightning-cli setconfig \
    revenue-ops-fee-replay-capture-enabled true &&
    lightning-cli revenue-config get fee_replay_capture_enabled'
  ```

  Expected: readback `true`. Do not invoke a fee cycle; wait for normal
  scheduling.

- [ ] **Step 2: Monitor the manifest without triggering work.**

  Poll no more often than once per minute using read-only SSH/file reads.
  Continue until one run satisfies all:

  - at least six completed complete cycles;
  - at least 100 evaluated channels;
  - at least 10 adjusted terminal outcomes;
  - `failed == 0`;
  - `dropped == 0`;
  - consecutive attempted/completed sequence numbers.

  If `failed` or `dropped` becomes nonzero, disable capture immediately and
  diagnose before continuing.

- [ ] **Step 3: Disable and drain capture.**

  ```bash
  ssh lnnode 'lightning-cli setconfig \
    revenue-ops-fee-replay-capture-enabled false &&
    lightning-cli revenue-config get fee_replay_capture_enabled'
  ```

  Wait read-only until the selected manifest says `state == "closed"` and
  `queue_drained == true`.

- [ ] **Step 4: Copy the frozen run locally without modifying remote evidence.**

  ```bash
  mkdir -p /tmp/revenue_ops_fee_replay-2026-07-19
  scp 'lnnode:/data/lightningd/.lightning/revenue_ops_fee_replay/*.json' \
    /tmp/revenue_ops_fee_replay-2026-07-19/
  sha256sum /tmp/revenue_ops_fee_replay-2026-07-19/*.json \
    | sort
  ```

- [ ] **Step 5: Run the closed-window parity gate.**

  ```bash
  cargo build --release -p revops --bin replay_fee_capture
  python3 tools/replay_fee_capture_window.py \
    --manifest /tmp/revenue_ops_fee_replay-2026-07-19/manifest-*.v0.json \
    --capture-dir /tmp/revenue_ops_fee_replay-2026-07-19 \
    --replay-bin target/release/replay_fee_capture
  ```

  Expected: exit 0, all thresholds met, every envelope consumed completely,
  `mismatch_count == 0`.

---

### Task 11: Resolve any exact replay mismatch at its first boundary

**Files:**

- Modify only the file implicated by the first exact mismatch
- Add the smallest regression test beside that component

- [ ] **Step 1: Stop on the first mismatch and classify it.**

  Classify as exactly one of:

  ```text
  schema
  state import
  evidence
  clock
  entropy
  governor
  execution
  decision
  trace
  post-state
  ```

  Do not consult the post-cycle live journal as a cause.

- [ ] **Step 2: Add one failing minimized test.**

  Copy only the smallest necessary envelope fragment into a unit test. Run the
  single test and observe the same path-addressed failure.

- [ ] **Step 3: Implement one fix and re-run from narrow to broad.**

  ```bash
  cargo test -p revops-fees --test replay
  cargo test -p revops-fees
  cargo test --workspace
  cargo test --workspace --release
  ```

  Repeat Task 10 Step 5. Never weaken strict ordering, completeness, or exact
  comparison.

- [ ] **Step 4: If Rust source changed, repeat Tasks 7 and 9.**

  A replay fix creates a new source commit and artifact. Re-push, rebuild,
  checksum, stage, preserve rollback, activate, and verify the new exact
  binary. Archive the interim journal and start a new final journal epoch.

---

### Task 12: Run the final live shadow gates and record the closeout

**Files:**

- Create: `docs/audit/2026-07-19-shadow-parity-deployment-closeout.md`
- Modify: none outside the evidence report

- [ ] **Step 1: Run valid live comparison gates.**

  ```bash
  python3 tools/diff-harness/diff_config.py --node lnnode
  python3 tools/diff-harness/diff_read_rpcs.py --node lnnode --since \
    "$(date -u -d '7 days ago' +%s)"
  ```

  Expected:

  - all comparable configuration fields exact, including four remaps;
  - all 12 constructor-only option surfaces exact;
  - implemented read-RPC fields exact;
  - ingestion counts and seven-column dedup keys exact over the shared
    window.

- [ ] **Step 2: Re-check live safety and health.**

  Verify:

  - Python plugin active and healthy;
  - Rust plugin active;
  - Rust `observer=true`;
  - Rust `fee-dryrun=true`;
  - capture readback `false`;
  - selected manifest closed and drained;
  - Rust production DB descriptor read-only;
  - fresh journal rows remain simulation records only; `would_broadcast=true`
    may describe a predicted adjustment but must never correspond to an
    invoked action RPC;
  - no action RPC was used in validation.

- [ ] **Step 3: Write the evidence report.**

  Record:

  - Python and Rust commits;
  - local/staged/installed Rust SHA-256, size, file type, and dependencies;
  - previous rollback SHA;
  - live option values and plugin paths;
  - manifest run ID, sequences, cycle/evaluation/adjustment/failure/drop
    counts;
  - replay result and mismatch count;
  - config/read-RPC/ingestion results;
  - explicit limitations: parity is proven only for the selected window and
    does not authorize cutover.

- [ ] **Step 4: Verify the report and commit it.**

  ```bash
  git diff --check
  rg -n 'TO''DO|TB''D|FIX''ME' \
    docs/audit/2026-07-19-shadow-parity-deployment-closeout.md
  git add docs/audit/2026-07-19-shadow-parity-deployment-closeout.md
  git commit -m "docs: record shadow parity deployment closeout"
  git push origin main
  ```

  Expected: placeholder scan returns no matches; push succeeds.

- [ ] **Step 5: Record the verified durable outcome in private Hexmem.**

  Store only durable decisions and verified outcome facts: exact commits and
  artifact checksum, observer/dry-run safety posture, capture run ID and
  thresholds, zero replay mismatches, and capture disabled. Do not store raw
  envelope contents, credentials, keys, or transient logs.
