# Fee-runway retention, store-timeout, authority-fetch, and arm-reuse hardening — design checkpoint (revision 3)

- Task: hexmem 58 (pre-cutover programme Task 9). Design only: no
  production code is edited by this checkpoint, no live node was
  contacted, no production DB was read.
- Revision 2 executed the round-1 correction contract
  `/home/sat/agent-tasks/task-58-review-findings.md` (F1–F14; Appendix
  A). Revision 3 executes the round-2 contract
  `/home/sat/agent-tasks/task-58-review-round2-findings.md`
  (R2-F1–R2-F4; Appendix B).
- Base: canonical main `a598239` (post-Task-57 merge of reviewed
  `49a940a`); this branch is rebased onto it, with revision 1 preserved
  as `8864fcd` for the correction diff. `file:line` references marked
  `[57]` were re-derived against `a598239`; unmarked references are in
  files Task 57 does not touch, verified unchanged from `7688e40`.
- Owner: rust (Hex). Verifier: codex. Verifier alone marks `review`.
- Related specs: `2026-07-20-rust-fee-cutover-runway-design.md`; the
  2026-07-26 stateful-shadow revision plan; Task 57's observer-runtime
  design (loop health, bounded owner framework); Task 44's A3
  architecture (off-owner store dispatch, identity-bound results,
  generation CAS).

## 0. Scope and non-goals

Four hardening areas, specified to implementation precision:

- **R** — retention for the fee-runway tables: bounded sweeps for the
  windowed evidence tables, an explicit append-only/growth contract for
  the audit tables (per F14, this design does NOT claim the sweep
  bounds every table).
- **T** — reconciling outer store-operation deadlines with SQLite's
  `busy_timeout` and the single-owner queue, with honest UNKNOWN
  semantics for anything that expires after admission.
- **F** — making the live batch authorizer's two Python-authority reads
  structurally independent, single-use, endpoint-bound, and
  dispatch-fresh.
- **A** — closing cutover-arm re-mint/reuse paths within the stated
  threat boundary (F12), while preserving "restart never reacquires
  authority".

Non-goals: no change to decision algorithms, shadow/live mode
semantics, the A3 pending state machine, or the engagement gate; no
schema removal; no change to the production (Python) database
(read-only `O_RDONLY` stays); no notification/hydration pruning (F8 —
deferred to a separate cursor-preserving task); no operator sweep RPC.

## 1. Current-state inventory

### 1.1 Connection and store architecture

- Observer DB open: `crates/revops-db/src/owner.rs:794`
  (`open_observer_db`): `busy_timeout` = `BUSY_TIMEOUT_MS` = **5000 ms**
  (`crates/revops-db/src/lib.rs:19`), WAL verified loudly.
- Single-owner actor (`spawn_read_write`): one connection, one
  `spawn_blocking` task, commands on a **capacity-64** tokio mpsc
  channel, executed **serially**. A queued command is not cancellable —
  an outer timeout abandons the reply, never the work.
- The fee-scheduler owner thread's inbox is a **BOUNDED Tokio MPSC,
  capacity `OWNER_QUEUE_CAPACITY = 64`** (fee_scheduler.rs:222), behind
  the private-sender `SchedulerIngress` boundary (:236-260 — async
  `send().await` backpressure is the only public production send API,
  compile_fail-pinned; sync callback/owner threads use `blocking_send`;
  constructed at `spawn_with_thread_spawner`, :3905). The `std_mpsc`
  alias (:152) serves only the per-Query REPLY channel, not the inbox.
  (Corrected per R2-F1 — revision 2 misstated this as an unbounded
  `std::sync::mpsc`.) Task 57's `LoopHandle::request` admission
  (`Enqueued`/`Coalesced`/`Dropped`, :3848-3855) additionally gates
  loop-generated messages before a send exists.
- Task 57's loop-health store writes (`begin`/`pass`/`fail` upserts,
  crates/revops/src/loop_health.rs:300) run on the ASYNC side through
  the same store actor — they add small single-row upserts to the store
  actor's queue, not work to the scheduler owner thread's serial chain.

### 1.2 Table inventory, writers, readers, growth

All DDL: `crates/revops-db/src/fee_runway.rs:89-266`; observer
notification tables: `crates/revops-db/src/notifications.rs`; Task 57's
loop-health table: classified in §2.1 `[57 ✓]`.

| table | writer(s) | growth | readers that constrain retention |
|---|---|---|---|
| `rust_fee_state_generation` | `commit_fee_cycle` only | 1 row, overwritten | hydration; A3 CAS (`cycle_exists_with_generation` :766); live authorize (`current_state_generation` :829) |
| `rust_fee_state` | `commit_fee_cycle` only | 1 row/channel, overwritten | hydration (`load_latest_state` :703) |
| `rust_fee_cycles` | `commit_fee_cycle(_guarded)` | 1/committed cycle — **append-only identity, never swept (F1)**: ≈75/day ≈ 27 k rows/yr ≈ low-MB/yr | **`cycle_exists` (:749) is A3's durable stable-event replay guard** — an A3 cycle can have zero requests, zero ledger rows, zero attempts, and only a SET-NULL trigger receipt, and its row is STILL what prevents the same event key from executing twice across restarts |
| `rust_fee_requests` | `commit_fee_cycle` (FK CASCADE) | 1/would-broadcast prepared action | `mutation_count` = `COUNT(*)` all time (:1029) — never touched by any sweep (F1/F2: parents are never deleted, so the CASCADE is unreachable) |
| `rust_fee_shadow_outcomes` | `commit_fee_cycle` (FK CASCADE) | 1/(cycle,channel) — dominant grower (~3–3.5 k/day measured) | engagement gate (windowed); **swept DIRECTLY by `cycle_ts` (F1), never via parent cascade** |
| `rust_mempool_fee_history` | `record_mempool_sample_pruned` | already bounded at insert (`retain_since = now − 86 400`, fee_evidence.rs:85) | 24 h MA; SeedOnce evidence — unchanged |
| `rust_mempool_ma_comparison` | `record_mempool_ma_comparison` | 1/cycle | daily rollup (windowed) — swept by `at` |
| `rust_fee_trigger_events` | owner thread + A3 off-owner dispatch | 1/trigger receipt | A1/A2 evidence (windowed) — swept by `received_at`; FK to cycles is `ON DELETE SET NULL` and, with F1, never fires |
| `rust_fee_ledger` | `commit_fee_cycle` (FK CASCADE) | rows only for governed/ledgered cycles | audit — never touched by any sweep |
| `rust_execution_quarantine` | `insert_quarantine`; restart reconciliation (:1410) | incident-bounded | `active_quarantine` gates the live path — never swept |
| `rust_runway_snapshots` | no production writer at base (timer is future work) | 1/tick once wired | `latest_runway_snapshot` — keep-last N |
| `rust_fee_seed_events`, `rust_fee_restart_markers` | scheduler startup | ~1/deploy, 1/restart | resume-not-reseed rails — never swept |
| `rust_broadcast_attempts` | live path only | 1/live dispatch attempt | `broadcast_attempt_count` all-time rail; `outcome IS NULL` drives restart quarantine — never swept |
| `rust_consumed_arm_nonces` (new, §5) | arm consumption | 1/consumed arm | replay deny-list — never swept |
| `rust_loop_health` `[57 ✓]` | Task 57 loop-health CAS (own module, `crates/revops-db/src/loop_health.rs:173`, with a canonical-columns self-check that refuses noncanonical schemas) | **one row per `loop_name` (PRIMARY KEY), upsert/CAS-overwritten — bounded by construction** | health surface (`ListLoopHealth`) — Class C |
| (adjacent) `ingested_forwards`, `peer_connection_events`, `channel_closure_events` | notification/hydration | forwards unbounded (largest absolute grower) | `last_forward_ts` = hydration resume cursor — **Class D: classified, NOT pruned by this task (F8)** |

### 1.3 Existing outer deadlines on store operations (base facts)

| call site | deadline | finding |
|---|---|---|
| `LiveBatchAuthorization::authorize` store reads (fee_execution.rs:294-341) | `AUTHORIZE_STORE_BUDGET` = 7 s (:115) | pattern kept; claim narrowed by F9 (§3.2) |
| `broadcast_batch` intent/result/quarantine writes | `store_budget()` = operator `timeout_seconds`, unclamped (:731) | T1 (§3.2) |
| `record_result` (:735-760) | budgeted, **error swallowed to stderr; batch continues** | **F4 (§3.4)** |
| RPC bridge: `handle.tx.send(Query).await` (main.rs:1249, :1318 — **admission itself is an unbounded await on a full queue**) then `spawn_blocking(reply_rx.recv())` (main.rs:1259, :1330) | none on either phase | T2 (§3.3) |
| Owner-thread blocking store calls | none (deliberate) | §3.5 |
| A3 dispatches | none; identity/CAS-bound results | §3.6 |
| Async notification writes | none | out of scope; Task 57 loop health (§3.7) |

## 2. Area R — retention

### 2.1 Classification (four classes; lint-enforced)

**Class E — append-only audit/identity. Never swept. NOT bounded by
this design (F14) — growth and maintenance boundary stated in §2.5:**

- `rust_fee_cycles` — **(F1)** the A3 replay-idempotency ledger.
  `cycle_exists` answers "has this stable event key already committed"
  across restarts; deleting any row re-opens replay for that key
  (re-execution consumes RNG entropy and can re-install state). The
  cycle row IS the identity, not evidence about it. Append-only.
- `rust_fee_requests`, `rust_fee_ledger` — mutation-adjacent audit;
  with parents never deleted, the CASCADEs are structurally
  unreachable (closes F2's hazard at the root).
- `rust_broadcast_attempts`, `rust_execution_quarantine`,
  `rust_fee_seed_events`, `rust_fee_restart_markers`,
  `rust_consumed_arm_nonces` — as revision 1, unchanged rationale.

**Class C — current-state, bounded by construction:**
`rust_fee_state_generation`, `rust_fee_state`, and `[57 ✓]`
`rust_loop_health` (loop_health.rs:173: `loop_name` PRIMARY KEY, all
columns upsert/CAS-overwritten per loop identity; row count = number of
loop identities — classified here explicitly per the review's required
cleanup).

**Class D — classified, deliberately NOT pruned by this task (F8):**
`ingested_forwards`, `peer_connection_events`,
`channel_closure_events`. Pruning them safely requires a durable resume
cursor that survives a longer-than-window outage (today the cursor IS
`MAX(timestamp)` — after an outage longer than any window, every row is
past the horizon and a sweep could delete the cursor itself). That
cursor-preserving design is a separate task; until it exists these
tables only grow. The classification lint still requires them to be
named so the deferral is visible, not silent.

**Class W — windowed evidence (the sweep's only targets):**

| table | key | window |
|---|---|---|
| `rust_fee_shadow_outcomes` | `cycle_ts` (indexed, DDL :145) | `RUNWAY_EVIDENCE_RETENTION_SECONDS` |
| `rust_fee_trigger_events` | `received_at` (indexed, :177) | same |
| `rust_mempool_ma_comparison` | `at` (indexed, :164) | same |
| `rust_runway_snapshots` | keep-last `SNAPSHOT_KEEP_LAST` by `id` | — |
| `rust_mempool_fee_history` | unchanged — bounded at insert; not double-managed | — |

No sweep statement names `rust_fee_cycles`, `rust_fee_requests`,
`rust_fee_ledger`, or any other Class C/D/E table — enforced by test
R0 (§9).

### 2.2 Preservation invariants (each a RED-first test)

- **I1 — quarantine evidence.** No quarantine row is ever deleted.
  (The revision-1 "keep the referenced cycle" clause is now subsumed:
  cycles are never deleted at all.)
- **I2 — mutation audit is monotone.** `mutation_count`,
  `broadcast_attempt_count`, and the full row sets of
  `rust_fee_requests`/`rust_fee_ledger`/`rust_broadcast_attempts` are
  identical before and after any sweep. Enforced structurally (no
  statement targets them or their parents) AND regression-tested with
  seeded actual child rows: a cycle carrying real request+ledger rows,
  older than every horizon, survives repeated sweeps byte-identical
  (F2: the test asserts on the child ROW SET, not `request_count`).
- **I3 — A3 replay guard (F1).** Seed an old committed A3 cycle with
  `request_count = 0`, no ledger/attempt rows, and a SET-NULL trigger
  receipt; run sweeps past every horizon; `cycle_exists(cycle_id)` must
  still answer true, and driving the same `event_key` through the A3
  pending machine must refuse as a duplicate (the full replay path,
  not just the SQL).
- **I4 — seed/restart provenance.** Byte-identical across sweeps.
- **I5 — unresolved work.** No `outcome IS NULL` attempt row is
  touched; the sweep never calls `record_broadcast_attempt_result`.
- **I6 — read surfaces stay answerable.** Status counters,
  `latest_*` reads, `active_quarantine`, `current_state_generation`,
  and both A3 CAS reads answer identically immediately before/after a
  sweep.
- **I7 — gate windows.** Window ≥ 4× the 72 h design soak window.
- **I8 — hydration resume cursor.** Trivially preserved: Class D is
  not pruned (F8). The invariant stays listed so the future
  forwards-retention task inherits it as a named obligation.

### 2.3 Constants

```rust
// crates/revops-db/src/retention.rs (new)
pub const RUNWAY_EVIDENCE_RETENTION_SECONDS: i64 = 30 * 86_400; // 30 d (kept per review)
pub const SNAPSHOT_KEEP_LAST: u64 = 90;
pub const RETENTION_BATCH_ROWS: usize = 500;
pub const RETENTION_MAX_BATCHES_PER_SWEEP: usize = 8;           // GLOBAL bound (F11)
```

(`RETAIN_MIN_CYCLES` and `FORWARDS_RETENTION_SECONDS` are deleted from
the design: the former is moot — cycles are never swept (F1) — and the
latter belongs to the deferred Class-D task (F8).)

### 2.4 Sweep mechanics (F11: one global bound, fair deterministic cursor)

- `Command::RunRetentionSweep { now, reply }` →
  `fee_runway::run_retention_sweep(conn, now, cursor) ->
  Result<RetentionReport>`.
- Deterministic round-robin: fixed table order
  `[shadow_outcomes, trigger_events, ma_comparison, snapshots]`; each
  round deletes at most ONE batch (`RETENTION_BATCH_ROWS`, id-batched
  `DELETE ... WHERE id IN (SELECT id ... WHERE <key> < :horizon ORDER
  BY <key> ASC LIMIT :batch)`) per table; rounds repeat until
  `RETENTION_MAX_BATCHES_PER_SWEEP` **total** batches are consumed or a
  full round deletes zero rows. Worst-case actor occupancy per sweep is
  therefore exactly `RETENTION_MAX_BATCHES_PER_SWEEP` bounded
  transactions (~4 k rows), regardless of table count.
- Fairness across sweeps: the sweep carries a start-table cursor
  (owner-side, in-memory), advanced to the table after the one that
  received the sweep's last batch; a restart resets it to the head —
  acceptable, because within a single sweep every table already gets a
  batch before any table gets two (starvation requires the cap to be
  exhausted mid-round repeatedly from the same start point, which the
  cursor prevents across sweeps and the round-robin prevents within
  one).
- Each batch is its own `BEGIN IMMEDIATE`/`COMMIT` with
  rollback-on-error (crash-idempotent, §7).
- Scheduling: enqueued off-owner (`dispatch_run_retention_sweep`, Task
  44's `spawn_store_dispatch` pattern) after each successful scheduled
  cycle commit; never on the A3 path; never while A3 state is pending.
  Failure increments `retention_failures` (surfaced in
  `revops-fee-debug`) and never affects a cycle.
- No auto-VACUUM (unchanged; runbook documents manual VACUUM with the
  plugin stopped).

### 2.5 Append-only growth contract (F14)

The automated sweep bounds Class W only. Class E grows without bound by
design, at these measured/derived rates:

| table | rate | 5-year projection |
|---|---|---|
| `rust_fee_cycles` | ≈75/day | ≈137 k rows, ≈10–15 MB |
| `rust_fee_requests` + `rust_fee_ledger` | 0 in dry-run shadow; in live mode ≈ rows-per-actual-fee-change (single digits/day at Python's observed change rate) | ≪ cycles |
| `rust_broadcast_attempts` | live dispatches only | ≪ cycles |
| seed/restart/quarantine/nonces | per deploy/restart/incident/arm | trivial |

Maintenance boundary: none of this is pruned automatically, ever. When
(years out) size matters, the procedure is operator-manual and offline:
stop the plugin, archive with `sqlite3 .dump` (or file copy), then a
deliberate, reviewed archival migration — a future task with its own
replay-guard analysis, because `rust_fee_cycles` archival interacts
with F1 (the A3 replay guard would need an archived-identity check or
an explicit generation floor). This design does not authorize that
deletion; it only names where the boundary sits.

### 2.6 Schema/migration

- `rust_consumed_arm_nonces` DDL (§5.3) — the only DDL addition.
  (Revision 2's `idx_rust_fee_cycles_completed` is REMOVED per R2-F4:
  no current query filters or orders `rust_fee_cycles` by
  `completed_at` — the only cycle query is identity lookup by
  `cycle_id` — and no retention statement touches the table. A later
  task may add it alongside the concrete query and query-plan evidence
  that needs it.)
- No destructive changes; old binaries ignore the addition.

### 2.7 Classification lint

`retention_classifies_every_table`: FULL schema init — all three
module inits (`notifications::init_schema`, which chains
`fee_runway`'s DDL, plus `loop_health::init_schema`, which is a
separate module init at loop_health.rs:172 `[57 ✓]`) — then read
`sqlite_master` and assert every table appears in exactly one of
`EXCLUDED_TABLES` / `CURRENT_STATE_TABLES` / `DEFERRED_TABLES` /
`WINDOWED_TABLES`. SQLite-internal names (`sqlite_sequence`,
`sqlite_stat*`) are matched by an explicit `SQLITE_INTERNAL` allowlist
— deliberately classified, not silently skipped (review cleanup). Task
57's `rust_loop_health` is classified Class C in §2.1; any future
unclassified table reds this test.

## 3. Area T — store-timeout reconciliation

### 3.1 The ordering and semantics rules (normative, narrowed per F9)

For a wait `W` wrapping a store operation:

1. **Floor rule (narrowed claim).** If `W` exists, `W ≥ BUSY_TIMEOUT_MS
   + 2 s` (`STORE_BUDGET_FLOOR` = 7 s). This guarantees only that a
   single legitimate SQLite lock wait on an OTHERWISE IDLE actor is
   never cut short. It does NOT bound end-to-end latency: the budget
   also spans channel admission, up to 63 queued commands each possibly
   paying their own lock waits, and the reply hop. No constant can make
   expiry prove a wedge (F9).
2. **Admission/receipt contract (F9).** Live-path writes distinguish
   three outcomes:
   - **Not admitted** — `try_send` returned Full/Closed: provably not
     enqueued, no side effect possible ⇒ a clean, reportable non-write
     (deny reason `store_admission_refused`).
   - **Admitted, reply within budget** — the actual result.
   - **Admitted, budget expired** — **UNKNOWN**: the command is queued
     and uncancellable and may still execute ⇒ conservative fail-closed
     handling (deny + poison/quarantine per §3.4); never reported as "no
     write happened".
3. Reads may treat expiry as a section-local read failure (idempotent,
   side-effect-free) — never as proof the owner is dead (F13).

### 3.2 T1 — clamp `store_budget()` (claim per §3.1.1 only)

`store_budget()` (fee_execution.rs:731) becomes
`Duration::from_secs(timeout_seconds).max(STORE_BUDGET_FLOOR)`. The
wire half keeps the operator's raw `timeout_seconds`. The wedged-store
test (tests/fee_execution.rs:1106) is rewritten: the wedge becomes an
unresponsive actor (Task 44 `WedgedStore` pattern) rather than a 60 s
busy-wait raced by a 1 s budget, and a NEW companion test holds a real
2 s lock and proves the clamped budget survives it (the pre-fix red:
today's 1 s budget denies the batch and leaves an orphan intent).
Additional test per F9: a healthy actor with queued work ahead of the
write — expiry (if forced via a slow queued command) classifies as
UNKNOWN, not clean failure.

### 3.3 T2 — bound the RPC bridge (derivation re-done post-57, F13)

Two independently-bounded phases replace today's two unbounded waits
(main.rs:1249/:1318 admission, :1259/:1330 response) — corrected per
R2-F1: revision 2 bounded only the response, leaving admission an
unbounded `send().await` against the full bounded queue.

- **Phase 1 — admission is refused, never awaited.** A new
  `SchedulerIngress::try_send_query(query: FeeDebugQuery, reply:
  std_mpsc::Sender<Value>) -> Result<(), QueryAdmissionRefused>`
  constructs `CycleMsg::Query` internally and `try_send`s it. A full
  queue returns the typed section-local error `owner_queue_saturated`
  IMMEDIATELY — the RPC never parks on admission, and a refused send
  provably enqueued nothing (Tokio `try_send` either transfers the
  message or returns it — no later enqueue is possible, closing the
  cancellation-safety question the review raised). The method carries
  ONLY `FeeDebugQuery` messages by construction, so Task 57's pinned
  "async backpressure is the only public production send API" property
  survives verbatim for every effectful message type — the try-path
  structurally cannot carry anything but a diagnostic read.
- **Phase 2 — response.** `reply_rx.recv_timeout(
  RPC_BRIDGE_RECV_TIMEOUT)`; expiry returns the DISTINCT typed error
  `owner_response_timeout`.
- **Semantics (both phases):** each is a **section-local read
  failure** — retryable, loud, and NOT evidence the owner died
  (`owner_queue_saturated` = the owner is busy admitting real work;
  `owner_response_timeout` = an admitted Query sat behind a legitimate
  store-contended backlog, or the owner is genuinely stuck — the error
  text says both readings and points at loop health). Nothing anywhere
  may treat either as a trip condition.
- **Derivation over the ACTUAL bounded composition (R2-F1):** an
  admitted Query can sit behind up to **63** legitimate messages. The
  expensive owner handlers are `RunPrepared` (evidence + mempool
  insert+prune + MA write + guarded commit + receipts ≈ 5×BUSY + work)
  and `handle_failed_forward` (fee_scheduler.rs:2475 — synchronous
  idempotency read + guarded nudge commit + receipt on the owner
  thread, ≈ 3×BUSY + work each); trigger/A3-result messages are
  cheap-to-moderate. A fully store-contended 63-deep backlog of heavy
  handlers is therefore **minutes-scale in the worst legitimate case —
  no constant both covers it and stays useful as a diagnostic bound.**
  The design consequently does NOT claim the response timeout bounds
  legitimate backlog. `RPC_BRIDGE_RECV_TIMEOUT = 30 s` is a
  DIAGNOSTIC FRESHNESS bound: it covers the typical worst (a Query
  behind one heavy handler chain under full lock contention, ≈ 25 s)
  and converts everything slower into the retryable
  `owner_response_timeout` — honestly labeled as "busy or backlogged,
  retry" rather than failure, per the semantics above.
- Tests (replacing revision 2's single T2, per R2-F1):
  - T2a `saturated_queue_refuses_query_admission_typed`: fill the
    bounded inbox to capacity with legitimate messages; the RPC returns
    `owner_queue_saturated` immediately (no hang); mutation: revert
    admission to `.send().await` — the test times out/red.
  - T2b `admitted_query_answers_behind_max_legitimate_backlog`: enqueue
    a maximum bounded backlog that includes at least one store-blocking
    heavy handler (`FailedForward` with a lock-holding store) ahead of
    an admitted Query; the Query eventually answers (no deadlock) and a
    deliberately-small test budget demonstrates the typed
    `owner_response_timeout` on the way; mutation: revert
    `recv_timeout` to `recv()` — red via harness timeout.


### 3.4 F4 — terminal result-write failure poisons the batch

`record_result` (fee_execution.rs:735) becomes fallible and its callers
act on it. New contract, for EVERY terminal outcome (success, rejected,
clean-failure — not just ambiguous):

- On result-write budget expiry or error: **stop the batch
  immediately** (no further requests dispatched), **poison the
  broadcaster** (the existing prior-persistence-failure refusal
  mechanism, fee_execution.rs' "can no longer trust its own
  store-backed quarantine check" state), **attempt a quarantine
  insert** (budgeted; its own failure keeps the poison — never assumed
  successful), and **return a typed non-success**
  `BroadcastError::ResultPersistenceUnknown { request_id, rpc_outcome,
  detail }` that names the RPC outcome the process observed but could
  not durably record.
- Rationale: the RPC outcome without its durable record is exactly the
  state restart reconciliation quarantines; discovering it at the NEXT
  restart (today's behavior — stderr log, batch continues) lets an
  arbitrary number of further mutations happen first.
- Tests (each RED against the current swallow): wedged result-write
  after (a) RPC success, (b) explicit rejection, (c) clean failure —
  each must stop the batch, poison, and return the typed error;
  mutations restore the log-and-continue body and each test must re-red
  independently.

### 3.5 Owner-thread blocking calls: deliberately NO outer timeout

Unchanged from revision 1 (§3.4 there): an owner-thread timeout cannot
cancel the queued command and would recreate scheduled-commit ambiguity
that F7 (Task 44) killed for A3. Visibility is Task 57 loop health +
§3.3's typed bridge error.

### 3.6 A3 pending-age visibility

Unchanged: `A3_PENDING_AGE_WARN_SECONDS = 60`,
`oldest_pending_age_seconds` in fee-debug, log on threshold crossing,
no cancellation.

### 3.7 Notification-path writes

Out of scope; Task 57 loop health is the visibility surface (dependency
recorded). Class D retention deferral (F8) keeps this path entirely
untouched by Task 58.

### 3.8 Attribution (F10 — claim removed)

Revision 1 claimed a caller-side deny string lets restart
reconciliation attribute an orphan intent to budget expiry. Withdrawn:
the string was never persisted, and an ordered persisted marker is
extra live-path machinery for diagnostic (not safety) value. The
contract is now: an unresolved intent row reconciles with the existing
generic reason ("no result was recorded before the prior process
exited", fee_runway.rs:1427) and the conservative quarantine stands.
The caller-side deny string (`store_intent_outcome_unknown`) remains in
the RETURNED error only, documented as non-durable.

## 4. Area F — independent, single-use, endpoint-bound authority bracketing

### 4.1 Base facts

As revision 1 (§4.1–4.2): `validate_stable_epoch` (python_authority.rs:
333) enforces epoch identity + strictly advancing `observed_at`;
`PythonAuthorityOff` fields are `pub` (forgeable); `authorize`
(fee_execution.rs:294) takes both readings as injected parameters.

### 4.2 Design (revised for F3 + F5)

```rust
// python_authority.rs
pub struct PythonAuthorityOff { /* fields now PRIVATE + accessors */ }

/// Fetch #1 proof. !Clone. Holds the ORIGINATING client by value —
/// close cannot be pointed at a different endpoint (F5).
pub struct OpenBracket { client: PythonAuthorityClient,
                         first: PythonAuthorityOff,
                         opened_at: std::time::Instant }
impl PythonAuthorityClient {
    pub async fn open_bracket(self, now, max_age)
        -> Result<OpenBracket, PythonAuthorityDenyReason>;
}
impl OpenBracket {
    /// Fetch #2 happens HERE, against self.client — no client
    /// parameter exists (F5). Consumes self.
    pub(crate) async fn close(self, now, max_age)
        -> Result<BracketedAuthorityOff, PythonAuthorityDenyReason>;
}
/// Two-real-fetches proof. !Clone, no public constructor, fields
/// private, consumed by value exactly once (F3).
pub struct BracketedAuthorityOff { /* private */ }
```

- **F5 (freshness + placement):** `authorize` takes the `OpenBracket`
  by value and calls `close()` INTERNALLY, as its last gate before
  minting — the second fetch is inside the authorization path,
  immediately before the single-use authorization exists. There is no
  public `close`, so no caller can hold a closed bracket around.
- **Stale OPEN bracket refusal (R2-F2 — distinct from the minted
  deadline below):** `close()` FIRST checks
  `self.opened_at.elapsed() > OPEN_BRACKET_MAX_LIFETIME` (proposed
  **30 s**, monotonic) and, independently, revalidates the FIRST
  reading's wall-clock age against authorization-time `now` via the
  same `max_age_seconds` bound `validate_status` used at open. Either
  failure returns the NEW typed deny
  `PythonAuthorityDenyReason::StaleOpenBracket { age_seconds,
  max_seconds }` (code `python_authority_stale_open_bracket` — a new
  variant, honoring the never-reword-existing-codes contract) and
  mints nothing. **Call-count contract (explicit per the review):
  fetch #2 is SKIPPED for an already-stale open bracket** — a refused
  stale bracket performs exactly ONE total fetch (the open), a
  successful bracket exactly TWO; tests assert the fake server's call
  count in both cases. A retained two-fetch proof can therefore never
  span more than `OPEN_BRACKET_MAX_LIFETIME` of wall clock before the
  second fetch, and the batch it authorizes is additionally bounded by
  the dispatch deadline below.
- **Dispatch deadline (F5):** `LiveBatchAuthorization` gains a private
  `minted_at: Instant`; `broadcast_batch` refuses with a typed
  `BroadcastError::AuthorizationStale` when
  `minted_at.elapsed() > AUTHORIZATION_DISPATCH_FRESHNESS` (proposed
  **30 s**; rationale: an order of magnitude above the authorize→
  dispatch hop in the same async task, an order of magnitude below the
  Python-side `max_age` staleness bound, so the wall-clock window in
  which Python could re-enable behind a parked authorization is capped
  by construction).
- **F3 (single use):** `authorize(..., bracket: OpenBracket, ...)`
  consumes the bracket by value — one two-fetch proof mints at most one
  `LiveBatchAuthorization`; `broadcast_batch(authorization: LiveBatch
  Authorization, ...)` already consumes the authorization by value
  (verified at :576) — one authorization, one batch. Both reuses are
  compile errors, pinned by `compile_fail` doctests (use-after-move on
  each), alongside the two forgery `compile_fail` pins
  (`PythonAuthorityOff { .. }`, `BracketedAuthorityOff { .. }`).
- **Same-second observations (F14 cleanup):** two honest fetches inside
  Python's 1 s `observed_at` resolution deny as
  `NonAdvancingObservation`. Pinned semantics: **no automatic
  tight-loop retry anywhere** — the attempt is denied and the next
  attempt belongs to a later operator/cycle invocation. (A retry loop
  would turn a frozen Python clock into a spin.)
- Fixture migration: tests build readings via
  `validate_status(&json!({...}), now, max_age)` and brackets via a
  fake socket server behind `PythonAuthorityClient` (rehearsal binary
  already spawns fake CLN servers; reuse that harness).

## 5. Area A — cutover-arm reuse/re-mint closure

### 5.1–5.2 Base facts and gaps

As revision 1: atomic `RENAME_NOREPLACE` consumption + fsync;
`LiveSessionArm` non-forgeable/non-persistable; gaps G1 (single
filesystem replay ledger), G2 (nothing pins one resolution per
process), G3 (retention/cleanup interplay unstated).

### 5.3 Durable nonce deny-ledger — now async-safe (F6)

Consumption splits into three explicitly-ordered steps, orchestrated
from async startup:

```rust
// cutover_arm.rs — pure, offline, NO ledger param (unchanged testability)
pub fn validate(arm_path, identity) -> Result<ValidatedArm, CutoverArmDenyReason>;
//  ^ everything through field validation; NO filesystem mutation.
//    ValidatedArm: private fields, not Clone/Serialize, single use.
pub fn consume_validated(arm: ValidatedArm, consumed_dir)
    -> Result<LiveSessionArm, CutoverArmDenyReason>;   // rename + fsync

// main.rs — async orchestration (resolve_startup_mode becomes async):
let validated = cutover_arm::validate(arm_path, &identity)?;
store.insert_consumed_arm_nonce(validated.nonce(), ...).await   // DB-first
     .map_err(/* PK conflict => ReusedNonce; other => ConsumeFailed */)?;
let arm = cutover_arm::consume_validated(validated, consumed_dir)?;
```

- **F6:** no blocking bridge anywhere on this path — the ledger insert
  is the plain async `ObserverHandle` method through the actor;
  `resolve_startup_mode` becomes `async fn` (its only caller is async
  startup). The revision-1 `ConsumedNonceLedger` trait + synchronous
  `blocking_send` bridge is DELETED from the design (it panics under a
  current-thread runtime). cutover_arm.rs stays entirely DB-free and
  offline-testable: the pure `validate`/`consume_validated` split
  needs no fake ledger at all, and the orchestration is tested at the
  `resolve_startup_mode` level against an in-memory observer DB.
- **Regression (F6):** `#[tokio::test(flavor = "current_thread")]`
  driving the full async consumption path — red today would be a panic
  ("cannot block the current thread from within a runtime") under the
  revision-1 shape; green proves no blocking bridge remains.
- DB-first ordering and crash windows unchanged (§7): nonce burned in
  DB + rename unperformed ⇒ next attempt denies `ReusedNonce`; operator
  mints a fresh arm. Deny-list only — no read path grants anything;
  restart-never-reacquires is untouched.

### 5.4 One resolution per process (F7)

The source scan is demoted to defense-in-depth. The proof becomes a
**pure kernel + guarded production wrapper** (reshaped per R2-F3 so the
existing startup-mode test matrix stays deterministic without any
reset seam):

```rust
// PRIVATE pure kernel: the current resolve_startup_mode body, verbatim.
// No global state, no token — the existing multi-case unit-test matrix
// in main.rs migrates here UNCHANGED and remains order-independent.
async fn resolve_startup_mode_kernel(inputs: StartupModeInputs<'_>)
    -> Result<ValidatedFeeMode, StartupModeDenyReason>;

/// Minted at most once per process (static AtomicBool::swap; private
/// field; no Clone; NO reset API of any kind, cfg(test) included).
pub struct StartupResolutionToken { _private: () }
impl StartupResolutionToken { pub fn take() -> Option<Self>; }

/// The ONLY public resolution path: consumes the token by value and
/// delegates to the kernel.
pub async fn resolve_startup_mode(token: StartupResolutionToken,
                                  inputs: StartupModeInputs<'_>)
    -> Result<ValidatedFeeMode, StartupModeDenyReason>;
```

- **Test-seam boundary (R2-F3):** the mode MATRIX tests exercise the
  private kernel directly (same file's unit-test module — no
  visibility widening needed) and never touch the global guard, so
  they stay order-independent. Exactly ONE test touches
  `StartupResolutionToken::take()`: the same-process double-invocation
  test, which takes the token, resolves once, proves the second
  `take()` returns `None`, and asserts the wrapper's typed
  `AlreadyResolved` refusal path. One consumer of the process-global
  per test binary = deterministic under any test order.
- **Structural sealing:** there is NO test-only factory, reset, or
  `cfg(test)` constructor — nothing to leak into production or
  integration tests. The kernel is private to `main.rs`'s module, so
  external/integration code cannot bypass the wrapper; a
  `compile_fail` doctest pins token non-constructibility, and the
  action-surface scan asserts exactly one production call site of
  `resolve_startup_mode` and zero non-test references to the kernel.
- A second in-process resolution cannot obtain a token: fail-closed
  typed `StartupModeDenyReason::AlreadyResolved`, regardless of fresh
  nonces (the round-1 F7 red: today two calls with two fresh arms both
  succeed).
- The source scan (`validate`/`consume_validated` single production
  caller) stays as a secondary tripwire.

### 5.5 Threat and recovery boundary (F12 — stated, not solved)

What the dual ledger DOES close: loss, wipe, or repointing of EITHER
ledger alone — the surviving ledger still denies every burned nonce.
Tests: (a) consumed_dir deleted/repointed, DB intact ⇒ `ReusedNonce`;
(b) observer DB replaced fresh, consumed_dir intact ⇒ `ReusedNonce`
(filesystem `EEXIST` path).

What no purely local design can close: a COORDINATED rollback — both
ledgers restored from the same pre-consumption snapshot, or
`observer-db-path` AND `consumed-arm-dir` repointed together at fresh
locations. A process cannot distinguish that from a genuine first
boot. **This is an explicit residual trust boundary, not a solved
property.** Mitigations (runbook §2.1 additions, required by this
design):

- Pin `revops-r-db-path`, `revops-r-observer-db-path`, and the
  consumed-arm dir as part of the candidate's recorded start-arg
  identity (they already are for the DB paths — extend to the arm
  dir), so any repointing is a visible start-arg diff at the next
  check-in, not a silent config drift.
- Operator recovery procedure after ANY restore/repoint touching
  either ledger: treat every previously-minted nonce as burned; mint a
  fresh arm (fresh nonce, fresh `not_before`/`expires_at`) after the
  restore; record the event. Backup/restore of the observer DB must
  include `rust_consumed_arm_nonces` by construction (it is in the same
  file — stated so a future selective-restore tool doesn't exclude it).

## 6. Exact files/interfaces touched by the implementation task

| file | change |
|---|---|
| `crates/revops-db/src/retention.rs` (new) | constants, 4-class table classification consts + `SQLITE_INTERNAL` allowlist, `RetentionReport` |
| `crates/revops-db/src/fee_runway.rs` | `run_retention_sweep` (round-robin cursor, global batch cap); `rust_consumed_arm_nonces` DDL + `insert_consumed_nonce` (index removed per R2-F4) |
| `crates/revops-db/src/owner.rs` | `Command::RunRetentionSweep`, `Command::InsertConsumedArmNonce` + siblings |
| `crates/revops/src/fee_state.rs` | trait + ObserverHandle `dispatch_run_retention_sweep` |
| `crates/revops/src/fee_scheduler.rs` | sweep enqueue after scheduled commit; `retention_failures`, `oldest_pending_age_seconds`; pending clock stamp; `SchedulerIngress::try_send_query` (Query-only try-path preserving the async-backpressure pin for effectful messages) |
| `crates/revops/src/fee_execution.rs` | `STORE_BUDGET_FLOOR` clamp; try_send admission contract; fallible `record_result` + `ResultPersistenceUnknown` + poison-on-result-failure; `authorize(bracket: OpenBracket, ...)` by value; `AuthorizationStale` dispatch deadline |
| `crates/revops/src/python_authority.rs` | private fields; `OpenBracket` (client-owning, `opened_at` lifetime-checked) / `BracketedAuthorityOff`; `pub(crate) close`; `StaleOpenBracket` variant; `OPEN_BRACKET_MAX_LIFETIME`; 4 compile_fail pins |
| `crates/revops/src/cutover_arm.rs` | `validate`/`consume_validated` split; `ValidatedArm` |
| `crates/revops/src/main.rs` | private `resolve_startup_mode_kernel` + guarded public wrapper; `StartupResolutionToken` (no reset seam); bridge sites switch to `try_send_query` + `recv_timeout` (main.rs:1249/:1259, :1318/:1330) |
| `crates/revops/src/bin/rehearse_fee_cutover.rs` | migrate to validate/consume split + bracket harness |
| `docs/runbooks/rust-fee-cutover.md` | VACUUM policy; never-prune list; F12 ledger-identity binding + restore recovery procedure |

## 7. Crash/restart matrix

| crash point | state after restart | why it's safe |
|---|---|---|
| mid retention batch | txn rolled back; earlier batches kept | per-batch invariants; idempotent re-run; cursor reset only affects fairness |
| after sweep, before report | rows gone, no log | observability only |
| write admitted, budget expired, process crashed, write landed | orphan `outcome IS NULL` intent | restart reconciliation quarantines (generic reason — F10) |
| result-write failed, poison set, crash before quarantine insert landed | unresolved intent | same reconciliation path; poison was belt, reconciliation is suspenders |
| after nonce INSERT, before rename | nonce burned, arm file present | `ReusedNonce` on retry; fresh arm required — fail-closed |
| after rename, before fsync return | consumed on disk or not; nonce in DB | DB ledger covers the one crash window the fs contract had |
| restart with live session lost | no authority (capability non-persistable; ledger denies, never grants) | unchanged invariant |
| coordinated dual-ledger restore | replay possible | OUT OF SCOPE by stated boundary (F12); operator procedure applies |

## 8. Fail-closed error semantics (new/changed, all typed)

- `RetentionReport { deleted: BTreeMap<&'static str, u64>, truncated }`;
  failure → `retention_failures` + log, never affects a cycle.
- Bridge: `owner_queue_saturated` (admission refused, nothing
  enqueued — R2-F1) and `owner_response_timeout` (admitted, response
  expired — busy/backlogged reading stated in the error text) —
  DISTINCT typed JSON errors, each a section-local read failure (F13),
  never a trip condition.
- `PythonAuthorityDenyReason::StaleOpenBracket { age_seconds,
  max_seconds }` (R2-F2 — new variant, added not reworded).
- `store_admission_refused` (not enqueued — clean) vs
  `store_intent_outcome_unknown` (admitted, expired — UNKNOWN,
  non-durable string, F10) — distinct deny reasons.
- `BroadcastError::ResultPersistenceUnknown { .. }` (F4) and
  `BroadcastError::AuthorizationStale` (F5).
- `StartupModeDenyReason::AlreadyResolved` (F7).
- Nonce PK conflict → `ReusedNonce`; other ledger insert failure →
  `ConsumeFailed(detail)` (arm file untouched, operator-retryable).
- `PythonAuthorityDenyReason` codes unchanged (stable-code contract).

## 9. RED-first test + mutation matrix (revised)

| # | test | red against | mutation that must re-red it |
|---|---|---|---|
| R0 | `sweep_statements_touch_only_windowed_tables` (source/statement scan) | n/a (structural pin) | add a DELETE naming a non-W table |
| R1 | `sweep_preserves_all_quarantine_rows` | guard-last construction | target quarantine in sweep |
| R2 | `sweep_preserves_actual_request_and_ledger_child_rows` (seeded real children, asserts row SETS — F2) | sweep that deletes old cycles (revision-1 shape) | re-introduce cycle deletion |
| R3 | `old_zero_request_a3_cycle_still_refuses_replay_after_sweeps` (full A3 event-key path — F1/I3) | revision-1 predicate | delete cycles past horizon |
| R4 | `sweep_batches_bounded_globally_and_fair_across_tables` (backlog in EVERY W table, repeated sweeps drain all — F11) | per-table-uncapped or first-table-greedy variant | remove global cap / remove round-robin |
| R5 | `retention_classifies_every_table_including_sqlite_internal` | unclassified table (incl. `sqlite_sequence`) | add unclassified DDL |
| R6 | `sweep_failure_counts_loudly_never_blocks_cycle` | inline blocking sweep | make dispatch synchronous |
| T1a | `store_budget_never_undercuts_a_real_lock_wait` (2 s held lock, clamped budget survives) | current 1 s-budget behavior | remove `.max(STORE_BUDGET_FLOOR)` |
| T1b | `unresponsive_actor_denies_within_clamped_budget` (WedgedStore rewrite of tests/fee_execution.rs:1106) | — | — |
| T1c | `admission_full_is_clean_deny_post_admission_expiry_is_unknown` (F9) | single-outcome current shape | collapse the two deny reasons |
| T2a | `saturated_queue_refuses_query_admission_typed` (R2-F1) | current `.send().await` admission (hangs on full queue — harness-timeout red) | revert `try_send_query` to `.send().await` |
| T2b | `admitted_query_answers_behind_max_legitimate_backlog` (63-deep incl. a store-blocking `FailedForward` handler; typed `owner_response_timeout` demonstrated with a small test budget) (R2-F1/F13) | unbounded `recv()` (harness-timeout red) | revert `recv_timeout` to `recv()` |
| T4 | `a3_pending_age_visible_after_threshold` | no age surface | drop counter update |
| F4a-c | wedged result-write after success / rejection / clean-failure ⇒ batch stops, poison, `ResultPersistenceUnknown` | current log-and-continue `record_result` | restore swallow body (each independently) |
| F1c | compile_fail: reuse a moved `OpenBracket` / `LiveBatchAuthorization`; forge `PythonAuthorityOff`/`BracketedAuthorityOff` (F3) | fields currently pub / params currently by-ref | re-widen / re-borrow |
| F5a | `close_targets_the_opening_endpoint_only` (structural: no client param — asserted via API shape test + doc) | revision-1 `close(client)` shape | re-add client param |
| F5b | `stale_authorization_refused_at_dispatch` (mock clock past freshness) | no deadline today | remove `AuthorizationStale` check |
| F5d | `stale_open_bracket_refused_before_second_fetch` (mock clock past `OPEN_BRACKET_MAX_LIFETIME`; fake server asserts exactly ONE fetch) (R2-F2) | no open-bracket lifetime today | remove the `opened_at` check (independently of F5b's) |
| F5c | `second_fetch_happens_inside_authorize` (fake server call-count == 2, second strictly during authorize) | injected-readings shape | make close reuse `first` |
| A1 | `wiped_consumed_dir_does_not_permit_replay` (DB denies) | filesystem-only ledger | skip DB insert |
| A1b | `fresh_observer_db_does_not_permit_replay` (dir denies — F12 single-loss pair) | — | skip rename |
| A2 | `nonce_insert_before_rename_survives_crash_between` | rename-first order | swap order |
| A5 | `same_process_second_resolution_refuses` — the ONE test touching the process-global guard; matrix tests exercise the private kernel and never share it (R2-F3) | current unguarded resolution (red today: both succeed with fresh arms) | bypass token take |
| A5b | action-surface scan: one production `resolve_startup_mode` call site, zero non-test kernel references, no token constructor/reset anywhere (R2-F3) | n/a (structural pin) | add a reset/factory seam |
| A6 | `current_thread_runtime_consumption_no_panic` (F6) | revision-1 blocking bridge (panic) | reintroduce blocking_send bridge |
| F14 | `same_second_second_read_denies_without_retry` (fake server asserts exactly 2 calls, no loop) | n/a (pins no-retry) | add retry loop |
| gates | full workspace debug+release, clippy, fmt, T8b byte-guards | — | — |

Red-first honesty note unchanged: where a compile-shape change forces
implementation-first (F1c signatures), mutation verification
substitutes, disclosed in the implementation report.

## 10. Integration sequencing (corrected per review)

1. **Strict after-Task-57 sequencing is REQUIRED and now satisfied**
   (review cleanup: Task 57 modifies both `main.rs` and
   `fee_scheduler.rs`, plus owner.rs and the observer schema). This
   revision is committed on the branch rebased onto canonical
   `a598239`; every `[57 ✓]` reference and the T2 constant were
   derived against that merged source.
2. The classification lint (R5) is the schema interlock:
   `rust_loop_health` is classified Class C here; any further Task-57
   table reds R5 until classified.
3. Areas F and A touch live-path files Task 57 does not own; no
   ordering constraint beyond (1); one coherent checkpoint.
4. Task 42 (SeedOnce coherence): no interaction —
   `rust_mempool_fee_history` bounds untouched.
5. Deployment: repo-only until a fresh operator-acknowledged soak
   window; any deployment containing this is a new candidate with a
   fresh clock.

## 11. Open questions for the reviewer (non-blocking defaults)

1. `AUTHORIZATION_DISPATCH_FRESHNESS = 30 s` and
   `OPEN_BRACKET_MAX_LIFETIME = 30 s` — acceptable ceilings?
2. `RPC_BRIDGE_RECV_TIMEOUT = 30 s` as a diagnostic-freshness bound
   (explicitly NOT a legitimate-backlog bound, per §3.3) — confirm the
   framing.

## Appendix A — F1–F14 correction mapping

| finding | disposition | sections | tests |
|---|---|---|---|
| F1 cycles pruning kills A3 replay guard | `rust_fee_cycles` → Class E append-only, never swept; `rust_fee_shadow_outcomes` swept directly by `cycle_ts` | §2.1, §1.2 | R3 (full A3 replay), R0, R2 |
| F2 denormalized `request_count` trust | moot by F1 (no parent deletion anywhere); defense: R0 structural scan + R2 asserts actual child ROW SETS | §2.1, §2.2 I2 | R0, R2 |
| F3 reusable bracket | `authorize` consumes `OpenBracket` by value → one proof, one authorization; `broadcast_batch` consumes authorization by value → one batch; compile_fail pins | §4.2 | F1c |
| F4 result-write swallow | fallible `record_result`; stop batch + poison + attempted quarantine + `ResultPersistenceUnknown` for ALL terminal outcomes | §3.4 | F4a-c |
| F5 bracket provenance/freshness | `OpenBracket` owns its client (no client param at close); `close()` is `pub(crate)`, called only inside `authorize` immediately before minting; `minted_at` dispatch deadline in `broadcast_batch` | §4.2 | F5a-c |
| F6 blocking ledger bridge panics | trait+blocking bridge deleted; pure validate / async ledger insert / consume_validated split; async `resolve_startup_mode`; current-thread regression | §5.3 | A6 |
| F7 one call site ≠ one invocation | linear `StartupResolutionToken` (AtomicBool, by-value, no Clone) + typed `AlreadyResolved`; source scan demoted to tripwire | §5.4 | A5 + compile_fail |
| F8 forwards pruning vs resume cursor | Class D: classified, NOT pruned by Task 58; separate cursor-preserving task; constants removed | §2.1, §0 | R5 (classification), I8 named for successor task |
| F9 floor doesn't bound queue delay | claim narrowed to single-lock-wait-on-idle-actor; try_send admission/receipt contract distinguishing not-enqueued vs admitted-unknown | §3.1, §3.2 | T1a-c |
| F10 unpersisted attribution | claim withdrawn; generic conservative quarantine documented; deny string documented non-durable | §3.8 | (crash row in §7) |
| F11 ambiguous batch bound | one GLOBAL cap + fixed-order round-robin + cross-sweep cursor; worst-case occupancy stated | §2.4 | R4 |
| F12 dual-ledger rollback | explicit residual trust boundary; single-loss test pair; runbook binds ledger identity into start-arg identity + restore recovery procedure (fresh arm, nonces treated burned) | §5.5, §7 | A1, A1b |
| F13 20 s not queue-proven | superseded by R2-F1's two-phase redesign (Appendix B): admission refused via `try_send_query`, response bounded as a diagnostic-freshness timeout, derivation over the actual bounded 64-capacity ingress | §3.3 | T2a, T2b |
| F14 "bounded every table" overstated | four-class model with explicit append-only growth contract + manual archival boundary; same-second no-retry pinned | §2.5, §4.2 | F14 |

## Appendix B — R2-F1–R2-F4 correction mapping (revision 3)

| finding | disposition | sections | tests |
|---|---|---|---|
| R2-F1 wrong queue + unbounded admission | §1.1 queue facts corrected (bounded Tokio MPSC cap 64 behind `SchedulerIngress`; `std_mpsc` = reply channel only); two-phase bridge: `try_send_query` admission refusal (`owner_queue_saturated`, provably nothing enqueued, Query-only so the async-backpressure pin survives for effectful messages) + `recv_timeout` response (`owner_response_timeout`); derivation redone over 63-deep bounded backlog incl. `handle_failed_forward`'s synchronous store work; 30 s explicitly a diagnostic-freshness bound, NOT a legitimate-backlog bound; neither error proves owner death | §1.1, §1.3, §3.3, §8 | T2a, T2b (+ independent mutations) |
| R2-F2 stale retained OpenBracket | `close()` checks `opened_at` against `OPEN_BRACKET_MAX_LIFETIME` (30 s, monotonic) AND revalidates the first reading's age with authorization-time `now`; new `StaleOpenBracket` deny variant; fetch #2 SKIPPED when stale (call-count contract: 1 fetch refused / 2 fetches success); distinct from the minted-authorization dispatch deadline | §4.2, §8 | F5d (+ F5b stays independent) |
| R2-F3 token has no safe test seam | pure private `resolve_startup_mode_kernel` (existing matrix migrates verbatim, order-independent, no global) + guarded public wrapper owning the once-only token; NO reset/factory/cfg(test) constructor exists; exactly one test touches the global guard; compile_fail + action-surface pins | §5.4 | A5, A5b, compile_fail |
| R2-F4 unproven index | `idx_rust_fee_cycles_completed` removed; DDL additions reduced to `rust_consumed_arm_nonces`; future task must bring query-plan evidence | §2.6, §6 | — |

Round-1 cleanups: §10.1 (strict after-57 sequencing — corrected), §2.1/§2.7
(`rust_loop_health` Class C, `sqlite_sequence` allowlist), §9 (matrix
reworked per above), §2.3 (30 d kept; forwards constants removed; no
operator sweep RPC), line references re-derived against `a598239`
(`[57 ✓]` tags).
