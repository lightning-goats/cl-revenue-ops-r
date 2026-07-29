# Task 65 Canonical Live State-Writer Rail Implementation Plan

> **For the implementer (solo mode):** RED-first per slice, focused gates,
> checkpoint commit per slice; review = Slice 5 mutations + operator
> sign-off.

**Goal:** One serialized, typed, fail-closed production-state writer
capability for config overrides, policy/tag state, hot-channel protection
overrides, budget reservation administration, and versioned publication —
structurally unreachable from observer mode, proven only on temp DBs.

**Architecture:** A new single-owner WRITABLE actor over a caller-supplied
production-schema DB path (`revops-db/src/state_writer.rs`) — the write
sibling of `actor::spawn_read_only`, opening EXISTING files only and
verifying the Python-owned schema before accepting a single command. A
typed capability front (`revops/src/state_writer.rs`) exposes the write
operations with the six-way ack vocabulary and the ordering rails
(version-inside-transaction before publication; policy commit before any
wake callback). Production wires NOTHING to it (zero construction sites,
action-surface pinned) until Task 69's whole-plugin authority consumes it.

**Tech Stack:** Rust 2021, rusqlite (BEGIN IMMEDIATE transactions), the
Task 59 two-phase admission vocabulary, temp-DB fixtures reproducing the
exact Python DDL (database.py:842-990).

## Global Constraints

- Repo-only: no live/production DB mutation ever — every test opens a
  temp file carrying the Python schema; the runtime constructs no writer.
- Do NOT touch the existing read-only production actor
  (`revops_db::actor`) or the observer db owner; the writer is a third,
  separate actor. No ad-hoc connections anywhere else.
- Python owns the DDL. The writer CREATEs nothing; it refuses (typed) a
  missing file, a missing table, or a missing required column
  (fail-closed identity check at open).
- Batches are bounded to 100 rows and transactional (the task text's
  bound); a batch over the bound is refused whole, never truncated.
- Ack vocabulary everywhere: `Applied` / `AlreadyTerminal` / `Denied` /
  admission `NotAdmitted` / `AdmittedOutcomeUnknown` / `StorageFailure`
  — reusing `StoreReceipt`/`StoreAdmissionRefused` for the two-phase
  admission half.
- Ordering rails: config version computed INSIDE `BEGIN IMMEDIATE`
  (M-13 v2, database.py:7324-7362) and returned as the publication
  value; policy/tag/hot-channel commits complete BEFORE any wake seam
  fires; a failed config READ surfaces as an error, never as
  "no override".
- Terminal non-resurrection: budget transitions guard on
  `status = 'active'` exactly like Python (release/spend), and a second
  transition acks `AlreadyTerminal`, never re-applies.

---

### Slice 1: The writable production-schema actor (revops-db)

**Files:**
- Create: `crates/revops-db/src/state_writer.rs`
- Modify: `crates/revops-db/src/lib.rs`
- Test: `crates/revops-db/tests/state_writer.rs`

**Interfaces produced:**
- `pub fn spawn_state_writer(path: &Path) -> Result<StateWriterHandle>`:
  refuses a missing file (`open with SQLITE_OPEN_READWRITE`, no CREATE);
  verifies `config_overrides(key,value,version,updated_at)`,
  `peer_policies(peer_id,strategy,rebalance_mode,fee_ppm_target,tags,updated_at)`,
  `hot_channel_protection_overrides(peer_id,added_at,note,min_depletion_trigger_pct)`,
  `budget_reservations(reservation_id,reserved_sats,reserved_at,job_channel_id,status)`
  exist with those columns (PRAGMA table_info), else a typed
  `StateWriterOpenError::{MissingFile,SchemaMismatch{table,detail}}`.
- Handle ops (async + two-phase try variants where marked ⚡):
  - ⚡`set_config_override(key, value) -> Result<AppliedVersion>` —
    BEGIN IMMEDIATE, `MAX(version)+1` computed inside, INSERT OR
    REPLACE, returns the in-transaction version (py parity verbatim).
  - ⚡`delete_config_override(key) -> Result<ConfigDelete>` — DELETE +
    version bump row-less publication: bump is recorded by returning
    `AlreadyAbsent` vs `Deleted{version}` where version = MAX+1 written
    onto a `__config_version__` sentinel row? NO SENTINEL: Python's
    poller detects deletes by the key vanishing; deletion here is
    `DELETE FROM config_overrides WHERE key=?`, ack
    `Deleted`/`AlreadyAbsent`, durable by txn commit.
  - `upsert_peer_policy(PeerPolicyWrite { peer_id, strategy,
    rebalance_mode, fee_ppm_target: Option<i64>, tags: Option<String> })
    -> Result<PolicyAck>` — INSERT OR REPLACE with `updated_at=now`;
    `PolicyAck::Applied`.
  - `set_hot_channel_override(peer_id, note: Option<String>,
    min_depletion_trigger_pct: Option<f64>) -> Result<()>` and
    `remove_hot_channel_override(peer_id) -> Result<bool>` (false =
    absent).
  - `release_budget_reservation(reservation_id) ->
    Result<BudgetTransition>` and
    `mark_budget_spent(reservation_id, actual_spent: i64) ->
    Result<BudgetTransition>` where `BudgetTransition::{Applied,
    AlreadyTerminal}` — the UPDATE guards `AND status = 'active'`
    (py:3748/:3772), zero rows = AlreadyTerminal (or Denied(NotFound)
    when no row exists at all — distinguished by a follow-up SELECT in
    the SAME transaction).
  - `cleanup_stale_reservations(max_age_seconds) -> Result<i64>` —
    py:3802 semantics (release aged actives; pending_settlement
    carve-out copied from the Python WHERE clause at implementation
    time).
  - `apply_policy_batch(Vec<PeerPolicyWrite>) -> Result<BatchAck>` —
    len > 100 refused whole (`BatchAck::DeniedOverBound{len}`), else ONE
    transaction, all-or-nothing, `BatchAck::Applied{count}`.

- [ ] RED: open-refusal tests (missing file; a temp db WITHOUT
  `peer_policies` → SchemaMismatch naming the table; a db missing the
  `tags` column → SchemaMismatch naming the column).
- [ ] RED: config version parity test — two writes to DIFFERENT keys get
  strictly increasing versions; INSERT OR REPLACE of the max-version key
  does not regress the next version (the M-13 v2 trap, asserted by
  writing key A(v1), key B(v2), key B(v3) then key A(v4)).
- [ ] RED: budget transition tests — active→released Applied;
  released→spent AlreadyTerminal; missing id Denied; stale cleanup
  releases only aged actives.
- [ ] RED: batch tests — 101 policies refused whole (table unchanged);
  a mid-batch constraint failure rolls back ALL rows.
- [ ] GREEN: implement actor + ops. Focused gates; commit slice.

### Slice 2: The typed capability front + admission acks (revops)

**Files:**
- Create: `crates/revops/src/state_writer.rs`
- Modify: `crates/revops/src/lib.rs`
- Test: `crates/revops/tests/state_writer.rs`

**Interfaces produced:**
- `pub enum StateWriteAck<T> { Applied(T), AlreadyTerminal, Denied(String),
  NotAdmitted(String), AdmittedOutcomeUnknown(String),
  StorageFailure(String) }` with `pub fn code(&self) -> &'static str`
  (`applied`/`already_terminal`/`denied`/`not_admitted`/
  `admitted_outcome_unknown`/`storage_failure`).
- `pub struct ProductionStateWriter` — !Clone, holds the
  `StateWriterHandle`; every op returns `StateWriteAck<_>`: admission
  refusal → NotAdmitted (provably nothing enqueued), receipt expiry →
  AdmittedOutcomeUnknown (the write may land; caller must re-read, never
  retry blindly), actor error → StorageFailure, transition guard →
  AlreadyTerminal, validation → Denied.
- Constructor `ProductionStateWriter::assemble(handle) -> Self` is `pub`
  but has ZERO production call sites (action-surface pinned; the real
  authority-gated construction is Task 69's `WholePluginLiveCapability`
  — DISCLOSED as the capability seam this task prepares but cannot
  finish). `revops::runtime::ObserverRuntime` gains nothing: a source
  scan asserts `runtime.rs` and `lnplus_runtime.rs` never name
  `ProductionStateWriter` (extends the existing capability-absence
  scans in `tests/action_surface.rs`).
- Ordering rails:
  - `set_config_override` ack carries the committed version; a
    `publish: FnOnce(version)` seam runs STRICTLY after Applied (test
    pins: publish never observed on any non-Applied ack; version seen by
    publish equals the committed one).
  - `upsert_peer_policy_then_wake(write, wake: FnOnce())` — wake fires
    strictly after commit; on any non-Applied ack the wake NEVER fires.

- [ ] RED: ack mapping tests over a real temp-schema actor (wedge with a
  BEGIN IMMEDIATE holder for AdmittedOutcomeUnknown via a tight receipt
  budget; fill the queue for NotAdmitted; poison a table for
  StorageFailure; double-transition for AlreadyTerminal).
- [ ] RED: ordering tests (publish-after-commit only; wake-after-commit
  only, both pinned with recording closures).
- [ ] RED: action-surface — zero `ProductionStateWriter` references in
  `runtime.rs`/`lnplus_runtime.rs`/`main.rs` production text; zero
  `assemble(` call sites outside tests.
- [ ] GREEN; focused gates; commit slice.

### Slice 3: Read-failure-never-no-override (config_resolve hardening)

**Files:**
- Modify: `crates/revops/src/config_resolve.rs` (layer (a) read path)
- Test: existing config tests + one new RED

**Contract:** a FAILED `config_overrides` read (missing table, io error)
must surface as a typed resolution error to `revenue-r-config` — never
fall through to layer (b)/(c) as if no override existed. (Inspect the
current `.ok()`-shape at implementation time; if it already errors, the
RED test simply pins it and this slice is a no-op commit with the pin.)

- [ ] RED: sabotage `config_overrides` (DROP via raw conn) → the resolve
  path returns an error naming the layer, not a silent default.
- [ ] GREEN (or pin-only); commit slice.

### Slice 4: Observer refusal surfaces

**Files:**
- Modify: `crates/revops/src/main.rs` (only where mutating names already
  exist), `crates/revops/tests/action_surface.rs`
- Test: `crates/revops/tests/state_writer.rs` additions

**Contract:** any ALREADY-REGISTERED Rust RPC arm that a Python operator
would use to mutate state (survey at implementation: `revenue-r-config`'s
set arm is the known candidate) must return the STABLE typed refusal
`{"error": {"code": "state_writer_authority_absent", ...}}` instead of a
fake success or a silent no-op, while read arms stay untouched. NO new
RPC names (that is Task 66's surface).

- [ ] RED: drive the existing set-arm(s); assert the stable refusal
  code; assert read arms unchanged.
- [ ] GREEN; commit slice.

### Slice 5: Mutations, battery, report

- [ ] Mutations (apply → pinned test red → revert; log kept): W1 version
  computed OUTSIDE the transaction (re-read after commit) → version
  parity test; W2 drop the `status='active'` guard → AlreadyTerminal
  test; W3 batch bound off-by-one (accept 101) → bound test; W4
  mid-batch failure commits the prefix → rollback test; W5 publish fires
  on StorageFailure → ordering test; W6 wake before commit → ordering
  test; W7 schema check skips a table → open-refusal test; W8 observer
  set-arm fakes success → refusal test; W9 `ProductionStateWriter` named
  in runtime.rs → capability scan; W10 read-failure falls through to
  default → slice-3 pin.
- [ ] Full battery: workspace debug+release, doctests, fmt, clippy
  --all-features -D warnings, diff check.
- [ ] `/home/sat/agent-tasks/task-65-implementation-report.md`, mark
  `impl`; `review` = operator sign-off.
