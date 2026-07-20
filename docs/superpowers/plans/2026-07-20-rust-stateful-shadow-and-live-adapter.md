# Rust Stateful Shadow and Dormant Live-Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the complete Rust fee-authority candidate autonomously in live shadow with restart-persistent Rust-owned state, decision-relevant recorders, production triggers, governor/ledger evidence, and exact outbound request construction, while compiling a real `setchannel` adapter that cannot be selected without a one-session cutover arm and positive Python handoff.

**Architecture:** The pure fee kernel produces typed, serialized `setchannel` intents into a capability-free recording executor. A transactional Rust-owned state store persists controller state, intents, ledger/governor evidence, and quarantine before any live dispatcher can act. Startup validates one of two complete modes: autonomous shadow has no mutation handle; live fee authority consumes a release-bound arm and constructs a broadcaster that revalidates Python authority and all batch gates before calling CLN.

**Tech Stack:** Rust 1.97, Cargo workspace, serde/serde_json, rusqlite through the existing single-owner actor, cln-rpc, sha2, Tokio, tempfile, fake Unix-socket CLN integration tests.

## Global Constraints

- Work from `docs/superpowers/specs/2026-07-20-rust-fee-cutover-runway-design.md` and only after the Python handoff plan has a reviewed interface.
- Invoke `superpowers:using-git-worktrees` and implement in an isolated Rust worktree based on the reviewed branch tip.
- Use `superpowers:test-driven-development` for every behavior change and `superpowers:systematic-debugging` for unexpected failures.
- Invoke `superpowers:verification-before-completion` before each commit, publish, build, staging, deployment, or completion claim.
- The Python production database is read-only from Rust in every mode. All new state is stored only in the configured Rust observer database.
- Live shadow must remain `observer=true`, `fee-dryrun=true`, `fee-broadcast=false`, and `fee-stateful-shadow=true`.
- The shadow construction graph must not own or receive `ClnFeeBroadcaster`, `CutoverArm`, or another mutation-capable object.
- The real adapter may invoke only `setchannel`. It must never expose generic action RPC dispatch.
- No live test or shadow deployment creates an arm or invokes `setchannel`, payment, rebalance, channel, planner, Boltz, LN+, or on-chain action RPCs.
- State flush, audit-intent flush, governor, ledger, authority-readback, provenance, and quarantine are hard live gates. Missing or stale evidence denies action.
- An ambiguous post-submission transport outcome is persisted and quarantines all subsequent broadcasts; it is never retried automatically.
- A process restart cannot reacquire live authority. A new, exact-binary arm is required.
- Preserve strict offline replay as the oracle lane. Autonomous shadow divergence is reported separately and does not rewrite replay expectations.
- Commit each green logical unit separately.

---

### Task 1: Establish the isolated Rust baseline and contracts

**Files:**

- Read: `Cargo.toml`
- Read: `crates/revops-fees/src/execution.rs`
- Read: `crates/revops-fees/src/cycle.rs`
- Read: `crates/revops-fees/src/replay.rs`
- Read: `crates/revops/src/fee_scheduler.rs`
- Read: `crates/revops/src/fee_state.rs`
- Read: `crates/revops/src/fee_evidence.rs`
- Read: `crates/revops/src/main.rs`
- Read: `crates/revops-db/src/notifications.rs`
- Read: `crates/revops-db/src/owner.rs`
- Read corresponding tests under `crates/*/tests/`

- [ ] **Step 1: Create the isolated worktree.**

  ```bash
  cd /home/sat/bin/cl-revenue-ops-r
  git status --short --branch
  git fetch origin
  git worktree add .worktrees/stateful-shadow-cutover \
    -b codex/stateful-shadow-cutover HEAD
  git -C .worktrees/stateful-shadow-cutover status --short --branch
  ```

  Expected: the source worktree is clean or contains only the already-reviewed plan commits; the feature worktree is clean.

- [ ] **Step 2: Run the current workspace baseline and pin source surfaces.**

  ```bash
  cd /home/sat/bin/cl-revenue-ops-r/.worktrees/stateful-shadow-cutover
  cargo fmt --all -- --check
  cargo test --workspace
  cargo clippy --workspace --all-targets -- -D warnings
  rg -n 'setchannel|FeeExecutor|StateSink|SeedOnce|mempool_ma_24h|PolicyChanged' crates
  ```

  Expected: all existing gates pass and `setchannel` is absent from non-test Rust source before this plan.

- [ ] **Step 3: Record the mode matrix as a test table before implementation.**

  The only accepted modes are:

  | Mode | observer | fee-dryrun | fee-broadcast | fee-stateful-shadow | arm |
  | --- | --- | --- | --- | --- | --- |
  | passive observer | true | false | false | false | absent |
  | autonomous fee shadow | true | true | false | true | absent |
  | live fee authority | false | false | true | false | valid and consumed |

  Every other combination fails initialization with a stable message. Add this table to a failing unit test in `crates/revops/src/fee_mode.rs`/`crates/revops/tests/fee_mode.rs` during Task 8.

---

### Task 2: Make exact `setchannel` intent construction typed and pure

**Files:**

- Modify: `crates/revops-fees/src/execution.rs`
- Modify: `crates/revops-fees/src/cycle.rs`
- Create: `crates/revops-fees/tests/execution.rs`
- Modify: `crates/revops-fees/tests/cycle.rs`
- Modify: `crates/revops-fees/src/replay.rs`
- Modify: `crates/revops-fees/tests/replay.rs`

- [ ] **Step 1: Add failing serialization and validation tests.**

  Cover base fee, ppm, optional `htlcmin`, dynamic optional `htlcmax`, exact channel id, overflow/negative rejection, and omission rather than `null` for absent optional fields. Pin exact JSON:

  ```rust
  assert_eq!(
      request.to_params().unwrap(),
      serde_json::json!({
          "id": "123x4x5",
          "feebase": 0,
          "feeppm": 321,
          "htlcmin": 1000,
          "htlcmax": 4_000_000
      })
  );
  ```

  ```bash
  cargo test -p revops-fees --test execution setchannel
  ```

  Expected before implementation: failures because there is no typed RPC request and `SetFeeRequest` does not carry `htlcmin_msat`.

- [ ] **Step 2: Add the pure wire type.**

  Keep decision inputs distinct from validated RPC intent:

  ```rust
  #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
  pub struct SetChannelRequest {
      pub id: String,
      pub feebase: u64,
      pub feeppm: u32,
      pub htlcmin: Option<u64>,
      pub htlcmax: Option<u64>,
  }

  impl SetChannelRequest {
      pub fn try_from_execution(request: &FeeExecutionRequest) -> Result<Self, DecisionInputError>;
      pub fn to_params(&self) -> serde_json::Value;
  }
  ```

  Extend the in-kernel request with `htlcmin_msat`. The serializer owns field names and optional-field omission; no scheduler or broadcaster hand-builds JSON.

- [ ] **Step 3: Make the existing executor expose prepared intents without RPC capability.**

  Add `PreparedFeeAction { request, decision, old_fee_ppm, expected_base_fee_msat }` and a thread-safe `RecordingFeeExecutor` that delegates pure clamping then records only successful would-broadcast intents. It contains no socket path or RPC type.

- [ ] **Step 4: Verify replay remains exact and commit.**

  ```bash
  cargo test -p revops-fees --test execution
  cargo test -p revops-fees --test cycle
  cargo test -p revops-fees --test replay
  cargo test -p revops-fees
  cargo fmt --all -- --check
  git diff --check
  git add crates/revops-fees
  git commit -m "feat(fees): build typed setchannel intents"
  ```

  Expected: pure and replay suites pass with byte-exact existing decisions and exact new wire payloads.

---

### Task 3: Make end-of-cycle persistence fail closed

**Files:**

- Modify: `crates/revops-fees/src/cycle.rs`
- Modify: `crates/revops-fees/src/replay.rs`
- Modify: `crates/revops-fees/tests/cycle.rs`
- Modify: `crates/revops-fees/tests/replay.rs`
- Modify: `crates/revops/src/fee_state.rs`
- Modify: `crates/revops/tests/fee_state.rs`

- [ ] **Step 1: Add a failing state-sink error propagation test.**

  ```rust
  impl StateSink for FailingSink {
      fn flush_batch(&self, _: &[(String, ChannelCycleState, ChannelFeeState)])
          -> Result<(), DecisionInputError>
      {
          Err(DecisionInputError::new("injected state flush failure"))
      }
  }
  ```

  Assert `run_fee_cycle` returns the injected error and does not report a completed cycle.

  ```bash
  cargo test -p revops-fees --test cycle state_sink_failure
  ```

  Expected before implementation: the trait signature cannot compile because it returns `()`.

- [ ] **Step 2: Change `StateSink` to a fallible boundary.**

  ```rust
  pub trait StateSink {
      fn flush_batch(
          &self,
          rows: &[(String, ChannelCycleState, ChannelFeeState)],
      ) -> Result<(), DecisionInputError>;
  }
  ```

  Propagate the error from the kernel. Update `MemoryStateSink`, replay sinks, counting sinks, and `JournalStateSink`; no implementation may log and continue.

- [ ] **Step 3: Test atomic-file failure behavior for the transitional journal sink.**

  Assert a write/rename failure leaves the prior complete artifact intact and returns an error. This protects shadow evidence until the SQLite store replaces it.

- [ ] **Step 4: Verify and commit.**

  ```bash
  cargo test -p revops-fees
  cargo test -p revops --test fee_state
  cargo fmt --all -- --check
  git diff --check
  git add crates/revops-fees crates/revops/src/fee_state.rs crates/revops/tests/fee_state.rs
  git commit -m "fix(fees): fail cycles on state persistence errors"
  ```

---

### Task 4: Add transactional Rust-owned fee state and audit schema

**Files:**

- Modify: `crates/revops-db/src/notifications.rs`
- Modify: `crates/revops-db/src/owner.rs`
- Modify: `crates/revops-db/tests/notifications.rs`
- Modify: `crates/revops-db/tests/owner.rs`
- Create: `crates/revops-db/src/fee_runway.rs`

- [ ] **Step 1: Add failing migration and transaction tests.**

  Require idempotent creation of these versioned Rust-only tables:

  - `rust_fee_state` and `rust_fee_state_generation`;
  - `rust_fee_cycles` and `rust_fee_requests`;
  - `rust_mempool_fee_history`;
  - `rust_fee_trigger_events`;
  - `rust_fee_ledger`;
  - `rust_execution_quarantine`;
  - `rust_runway_snapshots`.

  Assert foreign keys and uniqueness prevent duplicate request identities, and an injected request-row failure rolls back state, cycle, ledger, and requests together.

  ```bash
  cargo test -p revops-db --test notifications rust_fee_schema
  cargo test -p revops-db --test owner fee_cycle_transaction
  ```

  Expected before implementation: missing tables and owner commands.

- [ ] **Step 2: Define typed actor inputs and snapshots.**

  ```rust
  pub struct FeeCycleCommit {
      pub cycle_id: String,
      pub started_at: i64,
      pub completed_at: i64,
      pub source_commit: String,
      pub binary_sha256: String,
      pub state_rows: Vec<FeeStateRow>,
      pub requests: Vec<PreparedFeeActionRow>,
      pub governor: Vec<GovernorAuditRow>,
      pub ledger: Vec<LedgerAuditRow>,
  }

  pub struct FeeStateSnapshot {
      pub generation: u64,
      pub rows: Vec<FeeStateRow>,
  }
  ```

  Add owner commands for atomic commit, load latest state, record/query mempool samples, triggers, mutation count, and quarantine. The connection remains inside the existing single-owner task.

- [ ] **Step 3: Add a bounded blocking bridge for the scheduler thread.**

  The scheduler is a standard thread. Provide explicit `blocking_*` methods using `blocking_send` plus `oneshot::Receiver::blocking_recv`, with timeouts enforced by the caller. Do not open a second SQLite connection in the scheduler.

- [ ] **Step 4: Verify migrations, rollback, concurrency, and commit.**

  ```bash
  cargo test -p revops-db
  cargo clippy -p revops-db --all-targets -- -D warnings
  cargo fmt --all -- --check
  git diff --check
  git add crates/revops-db
  git commit -m "feat(db): persist Rust fee runway state"
  ```

  Expected: repeated initialization is safe; transaction failure leaves the previous generation and request count unchanged.

---

### Task 5: Implement restart-persistent `SeedOnce` autonomous shadow

**Files:**

- Modify: `crates/revops/src/fee_state.rs`
- Modify: `crates/revops/src/fee_scheduler.rs`
- Modify: `crates/revops/tests/fee_state.rs`
- Modify: `crates/revops/tests/fee_scheduler.rs`

- [ ] **Step 1: Add failing cold-start and restart tests.**

  Cover:

  - an empty Rust database seeds once from the read-only Python snapshot;
  - the first successful Rust commit records generation 1;
  - a restarted scheduler loads generation 1 from Rust, even if Python state changed;
  - corrupt/missing Rust state after a recorded generation fails closed rather than reseeding;
  - `RehydratePerCycle` remains available only for strict replay/legacy dry-run tests.

  ```bash
  cargo test -p revops --test fee_state seed_once
  cargo test -p revops --test fee_scheduler restart
  ```

  Expected before implementation: restart rehydrates from Python or loses autonomous state.

- [ ] **Step 2: Add explicit hydration sources.**

  ```rust
  pub enum HydrationSource {
      PythonSeed,
      RustGeneration(u64),
  }

  pub fn rehydrate_from_rows(
      state: &mut ControllerState,
      rows: &[FeeStateRow],
  ) -> Result<(), DecisionInputError>;
  ```

  `SeedOnce` first queries Rust state. It consults Python only when Rust reports no prior generation. Once a Rust generation exists, Python is never an autonomous-state source again.

- [ ] **Step 3: Commit state and intents atomically after each successful cycle.**

  Construct a recording executor for the cycle, run the kernel, drain its prepared intents, and commit state plus the full audit batch. A commit error produces `CycleOutcome::PersistenceFailed`, increments a red error counter, and prevents the generation from advancing.

- [ ] **Step 4: Add restart marker and divergence evidence.**

  Persist process identity, prior generation, hydration source, and startup timestamp. Expose the latest generation and restart marker to status/report code without reading Python state.

- [ ] **Step 5: Verify and commit.**

  ```bash
  cargo test -p revops --test fee_state
  cargo test -p revops --test fee_scheduler
  cargo test -p revops-db
  cargo fmt --all -- --check
  git diff --check
  git add crates/revops/src/fee_state.rs crates/revops/src/fee_scheduler.rs \
    crates/revops/tests/fee_state.rs crates/revops/tests/fee_scheduler.rs
  git commit -m "feat(fees): persist autonomous SeedOnce state"
  ```

---

### Task 6: Record Rust mempool evidence and production triggers

**Files:**

- Modify: `crates/revops/src/fee_evidence.rs`
- Modify: `crates/revops/src/fee_scheduler.rs`
- Modify: `crates/revops/src/main.rs`
- Create: `crates/revops/src/fee_triggers.rs`
- Create: `crates/revops/tests/fee_triggers.rs`
- Modify: `crates/revops/tests/fee_scheduler.rs`
- Modify: `crates/revops/tests/fee_evidence.rs`

- [ ] **Step 1: Add failing mempool recorder tests.**

  Assert Rust samples are written to `rust_mempool_fee_history`, the 24-hour moving average uses only fresh Rust-owned rows in autonomous mode, old rows are pruned transactionally, and missing/stale samples deny a decision that needs Vegas evidence.

- [ ] **Step 2: Implement the recorder and evidence switch.**

  Record the same decision-relevant sample cadence as Python through the observer actor. `FeeEvidence::mempool_ma_24h` uses Python production rows only in strict replay/compatibility mode and Rust rows in autonomous shadow/live mode.

- [ ] **Step 3: Add failing trigger coalescing tests.**

  Cover fixed interval, failed-forward nudge, policy change, wake-all, Vegas spike, bounded queue saturation, coalescing keys, receipt timestamps, and explicit drop counters. A dropped trigger is a persisted red event.

  ```rust
  pub enum FeeTrigger {
      FixedInterval,
      FailedForward { channel_id: String },
      PolicyChanged { channel_id: String },
      WakeAll,
      VegasSpike,
  }
  ```

- [ ] **Step 4: Wire subscribers to one bounded queue.**

  Subscriber handlers may enqueue and record only; they do not run a cycle inline. The scheduler drains/coalesces receipts, persists why a trigger did or did not run, and preserves the existing fixed-interval cadence.

- [ ] **Step 5: Verify and commit.**

  ```bash
  cargo test -p revops --test fee_evidence
  cargo test -p revops --test fee_triggers
  cargo test -p revops --test fee_scheduler
  cargo test -p revops-db
  cargo fmt --all -- --check
  git diff --check
  git add crates/revops/src crates/revops/tests crates/revops-db
  git commit -m "feat(fees): record mempool evidence and triggers"
  ```

---

### Task 7: Add release-bound cutover-arm validation and consumption

**Files:**

- Create: `crates/revops/src/cutover_arm.rs`
- Create: `crates/revops/tests/cutover_arm.rs`
- Create: `crates/revops/build.rs`
- Modify: `crates/revops/Cargo.toml`
- Modify: workspace `Cargo.toml`

- [ ] **Step 1: Add failing parser/provenance tests.**

  Pin schema `revops_fee_cutover_arm/v1` and test correct arm, malformed JSON, wrong schema/version/node/subsystem/commit/hash, early/expired times, empty/reused nonce, non-regular file, symlink, mode other than `0600`, and atomic consumption.

  ```rust
  pub struct CutoverArm {
      pub schema: String,
      pub node_id: String,
      pub subsystem: String,
      pub source_commit: String,
      pub binary_sha256: String,
      pub not_before: i64,
      pub expires_at: i64,
      pub nonce: String,
  }
  ```

  ```bash
  cargo test -p revops --test cutover_arm
  ```

  Expected before implementation: the module and release provenance are absent.

- [ ] **Step 2: Embed source provenance and hash the running executable.**

  `build.rs` exports the exact git commit through `REVOPS_SOURCE_COMMIT`; startup hashes `std::env::current_exe()` with SHA-256. Dirty/unavailable provenance is rejected for live mode but remains reportable in local tests.

- [ ] **Step 3: Validate then atomically consume.**

  Open without following symlinks, verify owner and mode, read once, validate every field against current node/time/binary, then rename into a consumed directory on the same filesystem and fsync the parent. Return a non-serializable `LiveSessionArm` capability. A restart cannot reconstruct it from the consumed file.

- [ ] **Step 4: Verify and commit.**

  ```bash
  cargo test -p revops --test cutover_arm
  cargo clippy -p revops --all-targets -- -D warnings
  cargo fmt --all -- --check
  git diff --check
  git add Cargo.toml crates/revops/Cargo.toml crates/revops/build.rs \
    crates/revops/src/cutover_arm.rs crates/revops/tests/cutover_arm.rs
  git commit -m "feat(cutover): validate release-bound fee arms"
  ```

---

### Task 8: Validate operating mode and Python authority

**Files:**

- Create: `crates/revops/src/fee_mode.rs`
- Create: `crates/revops/src/python_authority.rs`
- Create: `crates/revops/tests/fee_mode.rs`
- Create: `crates/revops/tests/python_authority.rs`
- Modify: `crates/revops/src/lib.rs`

- [ ] **Step 1: Add failing mode-matrix tests.**

  Encode the Task 1 table. Assert all partial/conflicting combinations return stable errors and shadow mode returns a type that cannot contain a live broadcaster.

  ```rust
  pub enum ValidatedFeeMode {
      PassiveObserver,
      AutonomousShadow(ShadowMode),
      LiveAuthority(LiveMode),
  }
  ```

- [ ] **Step 2: Add failing Python-status validation tests.**

  Validate exact schema, `enabled=false`, nonnegative generation/timestamps, bounded observation age, stable transition epoch across the batch acquisition, and RPC method presence. Missing/malformed/stale/enabled responses are denials.

  ```rust
  pub struct PythonAuthorityOff {
      pub generation: u64,
      pub transitioned_at: i64,
      pub observed_at: i64,
  }
  ```

- [ ] **Step 3: Implement a narrow read-only authority client.**

  It calls only `revenue-fee-authority-status` through the existing timeout wrapper. It cannot call `setconfig` or any action RPC. The live batch authorizer requests a fresh token immediately before dispatch.

- [ ] **Step 4: Verify and commit.**

  ```bash
  cargo test -p revops --test fee_mode
  cargo test -p revops --test python_authority
  cargo fmt --all -- --check
  git diff --check
  git add crates/revops/src/fee_mode.rs crates/revops/src/python_authority.rs \
    crates/revops/src/lib.rs crates/revops/tests/fee_mode.rs \
    crates/revops/tests/python_authority.rs
  git commit -m "feat(cutover): validate fee mode and Python handoff"
  ```

---

### Task 9: Implement the guarded CLN broadcaster and quarantine

**Files:**

- Create: `crates/revops/src/fee_execution.rs`
- Create: `crates/revops/tests/fee_execution.rs`
- Modify: `crates/revops/src/fee_scheduler.rs`
- Modify: `crates/revops-db/src/owner.rs`
- Modify: `crates/revops-db/tests/owner.rs`
- Modify: `crates/revops/tests/fee_scheduler.rs`

- [ ] **Step 1: Build a fake CLN Unix-socket test server.**

  The fake records method/params and injects success, explicit rejection, timeout before write, disconnect after full request receipt, and malformed response. It never points at the live Lightning socket.

- [ ] **Step 2: Add failing capability and exact-call tests.**

  Assert:

  - autonomous shadow creates zero fake-RPC connections and persists `would_broadcast` rows;
  - valid live mode sends exactly one `setchannel` call with the typed payload;
  - Python-authoritative, state, governor, ledger, quarantine, and stale-evidence denials send zero calls;
  - explicit CLN rejection is a reconciled failure row;
  - disconnect/timeout after submission creates persistent quarantine and blocks the next batch;
  - restart restores quarantine before any arm is accepted.

- [ ] **Step 3: Implement a narrow broadcaster.**

  ```rust
  pub struct ClnFeeBroadcaster {
      socket_path: PathBuf,
      _session: LiveSessionArm,
  }

  impl ClnFeeBroadcaster {
      async fn broadcast_batch(
          &self,
          authorization: LiveBatchAuthorization,
          requests: &[PersistedFeeRequest],
      ) -> Result<BatchReceipt, BroadcastError>;
  }
  ```

  The only action call site is:

  ```rust
  rpc.call_raw::<serde_json::Value, serde_json::Value>(
      "setchannel",
      &request.to_params(),
  ).await
  ```

  Persist intent before submission and result afterward. Classify transport state conservatively; any uncertainty after bytes may have been accepted is ambiguous.

- [ ] **Step 4: Require a fresh batch authorization.**

  `LiveBatchAuthorization` binds candidate SHA, state generation, Python authority generation, governor decision, ledger reservation, and quarantine-empty observation. It is private, non-cloneable, and consumed by `broadcast_batch`.

- [ ] **Step 5: Verify fake-RPC behavior and commit.**

  ```bash
  cargo test -p revops --test fee_execution
  cargo test -p revops --test fee_scheduler
  cargo test -p revops-db --test owner quarantine
  cargo fmt --all -- --check
  git diff --check
  git add crates/revops/src/fee_execution.rs crates/revops/src/fee_scheduler.rs \
    crates/revops/tests/fee_execution.rs crates/revops/tests/fee_scheduler.rs \
    crates/revops-db/src/owner.rs crates/revops-db/tests/owner.rs
  git commit -m "feat(cutover): add guarded CLN fee broadcaster"
  ```

---

### Task 10: Wire plugin options, status, and structural action allowlist

**Files:**

- Modify: `crates/revops/src/main.rs`
- Modify: `crates/revops/src/lib.rs`
- Modify: `crates/revops/tests/manifest.rs`
- Modify: `crates/revops/tests/fee_scheduler.rs`
- Create: `crates/revops/tests/action_surface.rs`

- [ ] **Step 1: Add failing manifest and init tests.**

  Add options:

  - `revops-r-fee-stateful-shadow`, boolean, default `false`;
  - `revops-r-fee-broadcast`, boolean, default `false`;
  - `revops-r-cutover-arm-path`, string, default empty.

  Assert the full mode matrix, observer-DB collision guard, mandatory writable Rust state, arm absence in shadow, and arm consumption in live mode.

- [ ] **Step 2: Construct capability-separated scheduler modes at startup.**

  Autonomous shadow uses `SeedOnce`, fixed interval plus trigger queue, Rust mempool evidence, recording executor, governor/ledger auditing, and no broadcaster. Live mode can be constructed only from `LiveMode`, consumed arm, and healthy persisted state.

- [ ] **Step 3: Expose a read-only runway status RPC.**

  Add `revops-fee-runway-status` returning schema/version, mode, candidate commit/hash, state generation, hydration source, last cycle, trigger queue/drop counters, mempool freshness, governor/ledger health, quarantine, prepared-request count, and mutation-call count. It performs no mutations.

- [ ] **Step 4: Replace the obsolete no-symbol test with a strict allowlist.**

  `crates/revops/tests/action_surface.rs` recursively scans non-test Rust sources and permits the string `setchannel` only in `crates/revops/src/fee_execution.rs` and the typed serializer documentation/test fixtures. It also asserts shadow constructors do not mention `ClnFeeBroadcaster`.

- [ ] **Step 5: Verify and commit.**

  ```bash
  cargo test -p revops --test manifest
  cargo test -p revops --test fee_scheduler
  cargo test -p revops --test action_surface
  cargo test -p revops --test fee_execution
  cargo fmt --all -- --check
  git diff --check
  git add crates/revops/src crates/revops/tests
  git commit -m "feat(plugin): wire stateful fee shadow modes"
  ```

---

### Task 11: Build the copied-state fake-RPC rehearsal harness

**Files:**

- Create: `crates/revops/src/bin/rehearse_fee_cutover.rs`
- Create: `crates/revops/tests/fee_cutover_rehearsal.rs`
- Create: `fixtures/cutover/README.md`

- [ ] **Step 1: Add a failing end-to-end rehearsal test.**

  Run against temporary copies of Python and Rust SQLite databases plus the fake Unix socket. Cover valid once-only activation, Python-still-authoritative, early/expired/wrong-node/wrong-commit/wrong-hash arms, state flush failure, governor denial, ledger failure, explicit rejection, ambiguous result, restart quarantine, reconciliation, and ordered rollback.

- [ ] **Step 2: Implement a non-live harness binary.**

  The binary refuses the configured live Lightning path, requires copied database paths under an explicit temporary rehearsal root, creates a synthetic fake-node arm, runs the exact release code paths, and emits versioned JSON evidence.

- [ ] **Step 3: Prove no live socket or production writable DB is reachable.**

  ```bash
  cargo test -p revops --test fee_cutover_rehearsal
  cargo run -p revops --bin rehearse_fee_cutover -- --help
  rg -n '/data/lightningd|lightning-rpc' crates/revops/src/bin/rehearse_fee_cutover.rs
  ```

  Expected: tests pass; any production-looking path appears only in an explicit rejection assertion.

- [ ] **Step 4: Commit the harness.**

  ```bash
  git diff --check
  git add crates/revops/src/bin/rehearse_fee_cutover.rs \
    crates/revops/tests/fee_cutover_rehearsal.rs fixtures/cutover/README.md
  git commit -m "test(cutover): rehearse fee handoff and rollback"
  ```

---

### Task 12: Full verification, review, release build, and shadow-only staging

**Files:**

- Modify: `README.md`
- Create: `docs/runbooks/rust-fee-cutover.md`
- Modify: `Cargo.lock`

- [ ] **Step 1: Document exact mode and manual handoff contracts.**

  Pin shadow options, live gates, arm schema, one-session behavior, Python status expectations, quarantine reconciliation, and rollback order. The runbook must state that timers cannot create arms or alter authority.

- [ ] **Step 2: Run the complete local verification matrix.**

  ```bash
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  cargo test --workspace --release
  cargo test -p revops --test fee_cutover_rehearsal --release
  rg -n 'setchannel' crates --glob '*.rs'
  rg -n 'call_raw' crates/revops/src --glob '*.rs'
  git diff --check
  ```

  Expected: all gates pass; the only action method is the allowlisted guarded adapter; read-only RPC calls remain separately identifiable.

- [ ] **Step 3: Obtain review and resolve every safety finding with a regression test.**

  Follow `superpowers:requesting-code-review`. Re-run the complete matrix after fixes. Then commit documentation/lockfile:

  ```bash
  git add README.md docs/runbooks/rust-fee-cutover.md Cargo.lock
  git commit -m "docs: publish Rust fee cutover runbook"
  git status --short --branch
  ```

- [ ] **Step 4: Publish and build exact release artifacts.**

  ```bash
  git push -u origin codex/stateful-shadow-cutover
  cargo build --workspace --release --locked
  sha256sum target/release/cl-revenue-ops-r \
    target/release/replay_fee_capture \
    target/release/rehearse_fee_cutover
  file target/release/cl-revenue-ops-r
  ldd target/release/cl-revenue-ops-r
  git rev-parse HEAD
  ```

  Record source commit, hashes, size, file type, and dynamic libraries. A post-build source change invalidates these artifacts.

- [ ] **Step 5: Stage and deploy only autonomous shadow mode.**

  Use the established checksummed atomic plugin replacement procedure. The deployed options must be exactly:

  ```text
  observer=true
  fee-dryrun=true
  fee-stateful-shadow=true
  fee-broadcast=false
  cutover-arm-path=
  ```

  Do not create an arm. Do not disable Python authority. Restart only the Rust dynamic plugin.

- [ ] **Step 6: Verify the live shadow safety boundary.**

  Verify installed/running checksum, `revops-fee-runway-status`, Python authority `enabled=true`, state generation advancement, Rust-owned mempool freshness, trigger receipts, governor/ledger audit rows, zero quarantine, zero mutation calls, and read-only production DB descriptors. Restart the Rust plugin once and prove it resumes from the Rust generation rather than reseeding from Python.

## Completion Evidence

Record the reviewed source commit, release hashes, test counts, fake-RPC request transcript, invalid-gate matrix, restart generation before/after, live option readback, Python authority readback, Rust production-DB descriptor modes, mutation count, and rollback hashes. Completion of this plan leaves Rust in autonomous shadow and does not authorize cutover.
