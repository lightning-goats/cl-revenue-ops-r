# Fee-runway retention, store-timeout, authority-fetch, and arm-reuse hardening — design checkpoint

- Task: hexmem 58 (pre-cutover programme Task 9). Design only: no
  production code is edited by this checkpoint, no live node was
  contacted, no production DB was read.
- Base: canonical main `7688e40`. Every `file:line` below is against that
  commit.
- Owner: rust (Hex). Verifier: codex.
- Related specs: `2026-07-20-rust-fee-cutover-runway-design.md` (runway),
  the 2026-07-26 stateful-shadow revision plan, Task 57's observer-runtime
  design (loop health / bounded owners), Task 44's A3 off-owner store
  architecture (fee_scheduler.rs — pending map, identity-bound results,
  generation CAS).

## 0. Scope and non-goals

Four hardening areas over the Rust fee runway, specified to
implementation precision:

- **R** — bounded retention for every fee-runway table.
- **T** — reconciling every outer store-operation deadline with SQLite's
  own `busy_timeout` so an outer expiry can never race the database's
  bounded wait into an ambiguous caller outcome.
- **F** — making the live batch authorizer's two Python-authority reads
  *structurally* independent fetches, not caller-injected evidence.
- **A** — closing cutover-arm re-mint/reuse paths while preserving
  "restart never reacquires authority".

Non-goals: no change to decision algorithms, shadow/live mode semantics,
the A3 pending state machine, or the engagement gate; no schema
*removal*; no change to the production (Python) database, which stays
read-only `O_RDONLY` from this plugin.

## 1. Current-state inventory (re-derived from source at 7688e40)

### 1.1 Connection and store architecture

- Observer DB opened by `crates/revops-db/src/owner.rs:794`
  (`open_observer_db`): `busy_timeout` = `BUSY_TIMEOUT_MS` = **5000 ms**
  (`crates/revops-db/src/lib.rs:19`), WAL verified loudly
  (`require_wal_mode`, owner.rs:820).
- Single-owner actor (`spawn_read_write`, owner.rs:837): one
  `rusqlite::Connection` owned by one `spawn_blocking` task; commands
  arrive on a **capacity-64** `tokio::mpsc` channel and execute
  **serially**. Neither `send`/`blocking_send` nor the oneshot replies
  carry a deadline (owner.rs:207-221 documents this as deliberate: the
  bridge is bounded, the *policy* belongs to callers).
- A queued command is **not cancellable**: an outer timeout abandons the
  reply, never the work. The command still executes when the actor gets
  to it. This fact drives every rule in §3.

### 1.2 Table inventory, writers, readers, growth

All DDL: `crates/revops-db/src/fee_runway.rs:89-266`; observer
notification tables: `crates/revops-db/src/notifications.rs`.

| table | writer(s) | growth | readers that constrain retention |
|---|---|---|---|
| `rust_fee_state_generation` | `commit_fee_cycle` only | 1 row, overwritten | hydration; A3 CAS (`cycle_exists_with_generation` :766); live authorize (`current_state_generation` :829) |
| `rust_fee_state` | `commit_fee_cycle` only | 1 row/channel, overwritten | hydration (`load_latest_state` :703) |
| `rust_fee_cycles` | `commit_fee_cycle(_guarded)` | **1/committed cycle, unbounded** | A3 idempotency (`cycle_exists`); engagement gate (external, windowed); `request_count` column feeds audit |
| `rust_fee_requests` | `commit_fee_cycle` (FK CASCADE on cycles) | 1/would-broadcast prepared action | **`mutation_count` = `COUNT(*)` over ALL TIME** (fee_runway.rs:1029) — the runway-status "prepared-request" rail |
| `rust_fee_shadow_outcomes` | `commit_fee_cycle` (FK CASCADE) | **1/(cycle,channel), unbounded — dominant grower** | engagement gate STARVATION/RATE/FLAPPER metrics (windowed `--since/--until`) |
| `rust_mempool_fee_history` | `record_mempool_sample_pruned` (fee_scheduler.rs:1869-1870) | **already bounded**: insert+prune in one `BEGIN IMMEDIATE`, `retain_since = now − MEMPOOL_MA_WINDOW_SECONDS` (86 400 s, fee_evidence.rs:85) | 24h MA (`query_mempool_samples_since` fee_scheduler.rs:1646); SeedOnce first-cycle evidence |
| `rust_mempool_ma_comparison` | `record_mempool_ma_comparison` (fee_scheduler.rs:1909) | 1/cycle, unbounded | daily rollup / diff harness (windowed) |
| `rust_fee_trigger_events` | owner thread `record_trigger_event` (fee_scheduler.rs:2214) + off-owner `dispatch_record_trigger_event` (A3 receipts, :3450) | 1/trigger receipt, unbounded; rate ≈ trigger volume | A1/A2 new-surface evidence (soak check-ins read `detail` for `failed_forward`/`policy_changed`) — windowed |
| `rust_fee_ledger` | `commit_fee_cycle` (FK CASCADE) | rows only for cycles with governed/ledgered actions | mutation-adjacent audit (governor/ledger trail) |
| `rust_execution_quarantine` | `insert_quarantine` :1056; `reconcile_quarantine_on_restart` :1410 | rare | `active_quarantine` (:1076, `cleared_at IS NULL`) gates the ENTIRE live path |
| `rust_runway_snapshots` | **no production call site at 7688e40** (grep: only tests; the runway timer that will write it is future work) | 1/timer tick once wired | `latest_runway_snapshot` :1471 |
| `rust_fee_seed_events` | `record_seed_event` :1106 (fee_scheduler.rs:1986) | ~1/deploy | resume-not-reseed proofs (`seed_events == 1` soak rail); `latest_seed_event` feeds mode validation |
| `rust_fee_restart_markers` | `record_restart_marker` :1158 (fee_scheduler.rs:2028) | 1/process start | resume provenance (soak rail: `prior_generation`/`rust_generation`) |
| `rust_broadcast_attempts` | live path only (`insert_broadcast_attempt` :1308, result :1331) | 1/live dispatch attempt | `broadcast_attempt_count` (**all-time rail, soak asserts 0**); `unresolved_broadcast_attempts` :1354 + `reconcile_quarantine_on_restart` |
| (adjacent) `ingested_forwards`, `peer_connection_events`, `channel_closure_events` | notification/hydration path | forwards: 1/settled forward, **unbounded, largest absolute grower in the file** | `last_forward_ts` (hydration resume marker = `MAX(timestamp)`) |

Measured growth (task-45 soak evidence, 45-channel node, no production
DB touched for this design): 62 committed cycles in ~20 h of runtime
(≈ 75/day); shadow outcomes ≈ one per (cycle × non-sleeping channel) ≈
3–3.5 k/day; would-broadcast outcomes 188 across 35 gate-scored cycles;
`rust_fee_requests` stayed at 0 all soak (dry-run shadow commits carry
an empty requests vec — consistent with `mutation_call_count 0`).
Steady-state bulk ≈ 1.2–1.5 M rows/year in
`rust_fee_shadow_outcomes` + `rust_fee_trigger_events` if never pruned.
Everything else is either overwritten in place, rare, or already
bounded.

### 1.3 Existing outer deadlines on store operations

| call site | deadline | vs 5 s busy_timeout | verdict |
|---|---|---|---|
| `LiveBatchAuthorization::authorize` store reads (fee_execution.rs:294-341) | `AUTHORIZE_STORE_BUDGET` = `BUSY_TIMEOUT_MS + 2000` = 7 s (fee_execution.rs:115) | above | correct — pinned pattern to generalize |
| `broadcast_batch` intent/result/quarantine writes | `store_budget()` = **operator's `timeout_seconds`, unclamped** (fee_execution.rs:731) | **can be below** — the wedged-store test (tests/fee_execution.rs:1106-1148) runs it at 1 s under a 5 s busy wait and asserts the budget fires first | **defect T1** (§3.2) |
| RPC bridge waits (`revops-fee-debug` etc.): `spawn_blocking(move || reply_rx.recv())` (main.rs:1261, :1324) | **none** | unbounded | **defect T2** (§3.3) |
| Owner-thread blocking store calls (scheduled commit fee_scheduler.rs:1800, evidence :1870-1909, seed/marker :1986/:2028, trigger :2214, `cycle_exists` :2398) | none (deliberate) | n/a | keep — §3.4 rationale |
| A3 dispatches (`dispatch_*`, fee_scheduler.rs:2902/:3364/:3450) | none; owner stays responsive by design (Task 44 F5), results identity+generation bound (F7) | n/a | keep + add pending-age visibility (§3.5) |
| Async notification writes (`insert_forward` notify.rs:103, hydration.rs:179) | none | unbounded stall possible | defer to Task 57 loop health (§3.6) |

## 2. Area R — bounded retention

### 2.1 Classification

Every table gets exactly one class. The classes are enforced by a lint
test (§2.6) so any FUTURE table (Task 57's loop-health tables included)
must be classified before it can merge.

**Class E — excluded from retention forever (append-only audit/identity):**

- `rust_fee_seed_events`, `rust_fee_restart_markers` — deploy/restart
  provenance; ~1 row per deploy/restart, negligible volume, and the
  resume-not-reseed soak rail depends on their history.
- `rust_broadcast_attempts` — the live mutation audit trail.
  `broadcast_attempt_count` is an all-time rail; deleting any row
  silently shrinks it. Unresolved rows (`outcome IS NULL`) additionally
  drive restart quarantine. Never deleted by any sweep.
- `rust_execution_quarantine` — active rows (`cleared_at IS NULL`)
  gate live execution; cleared rows are incident history. Volume is
  incident-bounded. Never deleted.
- `rust_fee_requests`, `rust_fee_ledger` — mutation-adjacent audit.
  Not deleted directly, and §2.2's invariant I2 keeps their parent
  cycles out of the sweep, so the FK CASCADE can never touch them.
- The consumed-arm nonce ledger (new, §5.3) — a deny list; deletion
  would re-enable replay.

**Class C — current-state, bounded by construction (no sweep needed):**

- `rust_fee_state_generation` (1 row), `rust_fee_state` (1
  row/channel) — `INSERT ... ON CONFLICT DO UPDATE` overwrites.

**Class W — windowed evidence (the sweep's actual targets):**

| table | window | plus |
|---|---|---|
| `rust_fee_cycles` (+ CASCADE to `rust_fee_shadow_outcomes` only, per I2) | `RUNWAY_EVIDENCE_RETENTION_SECONDS` | keep-last `RETAIN_MIN_CYCLES` regardless of age |
| `rust_fee_trigger_events` | same window | `cycle_id` FK is `ON DELETE SET NULL` — rows never block cycle deletion, and their own sweep is independent |
| `rust_mempool_ma_comparison` | same window | — |
| `rust_runway_snapshots` | keep-last `SNAPSHOT_KEEP_LAST` rows | writer currently unwired; policy specified now so the timer task inherits it |
| `rust_mempool_fee_history` | unchanged — already bounded at insert (fee_scheduler.rs:1869) | not double-managed by the sweep |
| (adjacent, recommended, same mechanism) `ingested_forwards` | `FORWARDS_RETENTION_SECONDS` | `MAX(timestamp)` row trivially inside any window, so the hydration resume marker is safe by construction |
| (adjacent) `peer_connection_events`, `channel_closure_events` | `FORWARDS_RETENTION_SECONDS` | — |

### 2.2 Preservation invariants (each becomes a RED-first test)

- **I1 — quarantine evidence.** No sweep deletes any
  `rust_execution_quarantine` row, and no cycle row referenced by any
  quarantine row's `cycle_id` is deleted (even past the window) while
  that quarantine row exists. (The FK is `SET NULL`; the invariant is
  stronger than the FK because a nulled reference is evidence loss.)
- **I2 — mutation audit is monotone.** `mutation_count` and the row
  sets of `rust_fee_requests` / `rust_fee_ledger` /
  `rust_broadcast_attempts` are IDENTICAL before and after any sweep.
  Enforced structurally: a cycle is sweep-eligible only if
  `request_count = 0` AND it has no `rust_fee_ledger` child AND its
  `cycle_id` appears in no `rust_broadcast_attempts` row. In dry-run
  shadow every cycle qualifies (measured: all 62 soak cycles had
  `request_count = 0`); any cycle that ever carried a prepared action
  or governed decision is retained forever.
- **I3 — latest committed generation/state.** The row with
  `MAX(completed_at)` (and, independently, the highest `generation`)
  in `rust_fee_cycles` is never deleted, regardless of age —
  keep-last `RETAIN_MIN_CYCLES` subsumes this; the invariant is tested
  directly anyway (a wall-clock-stalled node must not sweep its only
  recent evidence).
- **I4 — seed/restart provenance.** Class E: byte-identical before and
  after every sweep.
- **I5 — unresolved work.** No row with `outcome IS NULL` in
  `rust_broadcast_attempts` is deleted (subsumed by Class E), and no
  sweep runs `record_broadcast_attempt_result` — reconciliation stays
  the exclusive job of `reconcile_quarantine_on_restart`.
- **I6 — status/rehearsal read surfaces stay answerable.**
  `revops-fee-runway-status` counters, `latest_runway_snapshot`,
  `latest_seed_event`, `latest_restart_marker`, `active_quarantine`,
  `current_state_generation`, and the A3 CAS reads return the same
  answers immediately after a sweep as immediately before it (sweep
  targets are strictly historical rows outside every one of those
  queries).
- **I7 — gate windows.** The retention window is ≥ 4× the 72 h design
  soak window, so `engagement_gate.py` can always re-derive the
  freshest soak and compare against several prior ones.
- **I8 — hydration resume marker.** `last_forward_ts` is unchanged by
  any sweep (window ≥ any plausible ingest gap; the max-timestamp row
  is definitionally the newest).

### 2.3 Constants (proposed; rationale is the review surface)

```rust
// crates/revops-db/src/retention.rs (new)
pub const RUNWAY_EVIDENCE_RETENTION_SECONDS: i64 = 30 * 86_400; // 30 d
pub const RETAIN_MIN_CYCLES: u64 = 64;
pub const SNAPSHOT_KEEP_LAST: u64 = 90;
pub const FORWARDS_RETENTION_SECONDS: i64 = 90 * 86_400;        // 90 d
pub const RETENTION_BATCH_ROWS: usize = 500;
pub const RETENTION_MAX_BATCHES_PER_SWEEP: usize = 8;
```

- 30 d: ≥ 10× the waived 24 h window and 10× the longest observed
  incident-diagnosis lookback so far; caps the two bulk tables at
  ~100–110 k rows steady state (single-digit MB) — bounded without ever
  being the thing an investigation runs out of. 14 d was considered and
  rejected: A1/A2 absence investigations this month compared against
  soaks ~3 weeks apart.
- `RETAIN_MIN_CYCLES = 64`: ≳ one full soak day of cycles (measured 62)
  survives even total wall-clock weirdness (I3).
- 90 d forwards: forwards are the raw evidence under fee-decision
  autopsies; at a few hundred/day this is ≈ 30–45 k rows — still small,
  and revenue analysis wants seasons, not weeks.
- Batching: 500 rows/`BEGIN IMMEDIATE` bounds each write txn well under
  one `busy_timeout`; 8 batches/sweep bounds a sweep's total actor
  occupancy (≤ ~4 k rows) at roughly one day of backlog per cycle, so
  a long-dormant DB drains in a few cycles without ever starving a
  pending commit.

### 2.4 Sweep mechanics

- New store command `Command::RunRetentionSweep { now, reply }` →
  `fee_runway::run_retention_sweep(conn, now) -> Result<RetentionReport>`
  where `RetentionReport` carries per-table deleted counts and a
  `truncated: bool` (hit the batch cap; more remains).
- Eligibility predicate for cycles (one statement, id-batched):

```sql
DELETE FROM rust_fee_cycles WHERE cycle_id IN (
  SELECT c.cycle_id FROM rust_fee_cycles c
  WHERE c.completed_at < :horizon
    AND c.request_count = 0                                   -- I2
    AND NOT EXISTS (SELECT 1 FROM rust_fee_ledger l
                    WHERE l.cycle_id = c.cycle_id)            -- I2
    AND NOT EXISTS (SELECT 1 FROM rust_broadcast_attempts b
                    WHERE b.cycle_id = c.cycle_id)            -- I2
    AND NOT EXISTS (SELECT 1 FROM rust_execution_quarantine q
                    WHERE q.cycle_id = c.cycle_id)            -- I1
    AND c.cycle_id NOT IN (SELECT cycle_id FROM rust_fee_cycles
                           ORDER BY completed_at DESC
                           LIMIT :retain_min)                 -- I3
  ORDER BY c.completed_at ASC
  LIMIT :batch
);
```

  (`IN (SELECT ... LIMIT)` deliberately, not `DELETE ... LIMIT` — the
  bundled SQLite is not guaranteed to be compiled with
  `SQLITE_ENABLE_UPDATE_DELETE_LIMIT`.) `rust_fee_shadow_outcomes`
  rows go via the existing `ON DELETE CASCADE`; `rust_fee_trigger_events`
  and `rust_mempool_ma_comparison` get their own analogous
  timestamp-batched deletes; snapshots by `id NOT IN (last N)`.
- Each batch is its own `BEGIN IMMEDIATE`/`COMMIT` with the same
  rollback-on-error shape as `commit_fee_cycle` — a mid-sweep crash
  loses nothing and re-runs idempotently (§7).
- **Scheduling**: the fee-scheduler owner enqueues one sweep via a NEW
  `dispatch_run_retention_sweep` (off-owner, Task 44's
  `spawn_store_dispatch` pattern, callback → `CycleMsg`) after each
  successful *scheduled* cycle commit. Never on the A3 path, never
  during pending A3 state (`run_or_defer_cycle` untouched). A sweep
  result is logged with per-table counts; failure increments a new
  `retention_failures` counter surfaced in `revops-fee-debug`'s runway
  counters and NEVER fails, defers, or gates a cycle (fail-open for the
  sweep is fail-CLOSED for evidence: rows are only ever deleted after a
  fully successful pass).
- **No auto-VACUUM.** File size plateaus at the high-water mark;
  `PRAGMA incremental_vacuum` stays off; a manual `VACUUM` remains an
  operator action with the plugin stopped (documented in the runbook,
  §8). Rationale: VACUUM takes an exclusive lock the actor must never
  wait on mid-soak.

### 2.5 Schema/migration

- One idempotent DDL addition (backward + forward compatible; old
  binaries simply ignore it):
  `CREATE INDEX IF NOT EXISTS idx_rust_fee_cycles_completed ON
  rust_fee_cycles(completed_at);` — the sweep's scan key.
  `rust_fee_trigger_events(received_at)` and
  `rust_mempool_ma_comparison(at)` are already indexed (DDL :177, :164).
- No column or table changes for Area R. A downgraded binary sees fewer
  historical rows — identical to a freshly-seeded DB, which every
  reader already handles.

### 2.6 Classification lint (future-proofing, Task 57 interplay)

New test `retention_classifies_every_table`: opens a fresh observer DB,
runs full schema init, reads `sqlite_master`, and asserts every table
name appears in exactly one of `EXCLUDED_TABLES` / `CURRENT_STATE_TABLES`
/ `WINDOWED_TABLES` (const slices in `retention.rs`). When Task 57's
loop-health tables land, this test goes red until they are explicitly
classified — unclassified growth can never merge silently.

## 3. Area T — store-timeout reconciliation

### 3.1 The ordering rule (normative)

For every wait `W` wrapping a store operation whose statement(s) can
block on SQLite's lock wait `B` (= `BUSY_TIMEOUT_MS`, charged per lock
acquisition — a txn can pay it more than once):

1. If `W` exists, then `W ≥ B + M` with margin `M = 2 s`
   (`STORE_BUDGET_FLOOR = BUSY_TIMEOUT_MS + 2_000 ms = 7 s`, the
   already-pinned `AUTHORIZE_STORE_BUDGET` value, promoted from a local
   constant to the shared floor).
2. An expired `W` means **UNKNOWN, not failure**: the command is queued
   and uncancellable (§1.1), so the operation may still complete. The
   caller must fail closed AND must never record or report "the write
   did not happen".
3. Reads may treat expiry as a denial (they are idempotent and
   side-effect-free); writes must route expiry into the ambiguity
   machinery (quarantine / reconciliation), never into a
   clean-failure branch.

### 3.2 Fix T1 — clamp `store_budget()`

`ClnFeeBroadcaster::store_budget()` (fee_execution.rs:731) becomes
`Duration::from_secs(timeout_seconds).max(STORE_BUDGET_FLOOR)`. The RPC
half (`attempt_send`, :795-806) keeps the operator's raw
`timeout_seconds` — the "one operator-visible number" property changes
to "one operator number for the WIRE, one pinned floor for the STORE",
documented on the method.

Why this matters concretely (the current failure schedule): operator
sets `timeout_seconds = 1`; a legitimate 2 s lock wait (an engagement
gate read, an operator `sqlite3` session) expires the intent-write
budget → batch denied → the queued insert lands 1 s later anyway → an
orphan `outcome IS NULL` intent for a request that provably sent zero
bytes → next restart, `reconcile_quarantine_on_restart` (fee_runway.rs:
1410) marks it ambiguous and **quarantines the whole live path**. A
self-inflicted quarantine from a healthy 2 s lock wait is exactly the
"ambiguous caller outcome" this task exists to remove. The existing
wedged-store test (tests/fee_execution.rs:1106) is REWRITTEN, not
deleted: the wedge becomes a genuinely unresponsive actor (Task 44's
`WedgedStore` pattern) instead of a 60 s busy-wait raced by a 1 s
budget, and its elapsed-time assertion moves to
`STORE_BUDGET_FLOOR + 1 s`.

Additionally, the intent-write expiry path gets a distinct deny string
(`store_intent_outcome_unknown`) so a later reconciliation quarantine
can be attributed to a budget expiry rather than a crash.

### 3.3 Fix T2 — bound the RPC bridge

Both `spawn_blocking(move || reply_rx.recv())` sites (main.rs:1261,
:1324) become `reply_rx.recv_timeout(RPC_BRIDGE_RECV_TIMEOUT)` with

```rust
pub const RPC_BRIDGE_RECV_TIMEOUT: Duration = Duration::from_secs(20);
```

Derivation: the owner thread's longest legitimate serial chain before
answering a `Query` is one scheduled commit (`BEGIN IMMEDIATE` ≤ 5 s
busy + work) queued behind one mempool insert+prune txn (≤ 5 s busy) +
margin ⇒ 2 B + 10 s = 20 s. Expiry returns a typed JSON error
(`{"error": "fee-cycle owner did not answer within 20s — see
revops-fee-debug counters and plugin log"}`) instead of hanging the
lightningd RPC forever. Rule 2 applies trivially — `Query` is a read.

### 3.4 Owner-thread blocking calls: deliberately NO outer timeout

The scheduler owner's `blocking_*` calls (§1.3 row 4) stay undeadlined,
and this is now a PINNED decision rather than an accident: an outer
timeout on the owner thread cannot cancel the queued command; the only
things it could do are (a) abandon a commit whose outcome it then
doesn't know — recreating at the scheduled-commit level the exact
ambiguity F7's CAS was built to kill at the A3 level, or (b) crash the
loop. A stuck store must instead become *visible*: Task 57's loop-health
begin/pass/fail wraps the cycle loop, so a wedged blocking call shows as
a failing/stale fee-cycle loop-health row (plus §3.3 keeps operators
able to ask). Sequencing note for the reviewer: if this lands before
Task 57's impl, visibility is the existing log + T2's typed error; the
loop-health integration is listed as a follow-through item in §9.

### 3.5 A3 dispatch pending-age visibility

A3 callbacks are deadline-free by design (results are identity- and
generation-bound; late arrivals are conflicts, not installs — Task 44
F7). The gap is *silent* pendings: a store that never answers leaves
`pending_initial_fees` occupied and `deferred_cycle` parked with only
counters moving. Add `A3_PENDING_AGE_WARN_SECONDS = 60`: the owner
stamps each pending entry with its dispatch clock; `fee-debug`'s
`initial_fee` block gains `oldest_pending_age_seconds`, and the owner
logs once per threshold crossing (no cancellation, no state change —
visibility only).

### 3.6 Notification-path writes

`insert_forward`/peer/closure awaits (notify.rs:103, hydration.rs:179)
stay unbudgeted in THIS design: they are ordered evidence ingestion
(dropping is evidence loss, A1 depends on forwards), the channel bound
(64) provides backpressure, and Task 57's notification-loop health is
the correct visibility surface. Explicitly out of scope here to avoid
double-designing Task 57's owner framework; recorded as a dependency.

## 4. Area F — genuinely independent Python-authority fetches

### 4.1 What already holds (python_authority.rs)

`validate_stable_epoch` (:333) requires identical
`generation`/`transitioned_at` AND strictly advancing `observed_at`;
`PythonAuthorityOff` is deliberately not `Copy` (:64-71);
`fetch_validated_status` (:392) is the intended per-read primitive.

### 4.2 The gaps

- `PythonAuthorityOff`'s fields are `pub` (:72-77): any call site can
  mint `PythonAuthorityOff { observed_at: x + 1, .. }` and satisfy
  bracketing without any second RPC ever happening.
- `LiveBatchAuthorization::authorize` (fee_execution.rs:294) takes both
  readings as caller-supplied parameters — the authorizer *checks*
  consistency but cannot *prove* provenance. "Rechecking cached/shared
  evidence" is exactly one bad caller away.
- `observed_at` is Python-reported at 1 s resolution: two honest
  fetches inside the same second deny as `NonAdvancingObservation`.
  Fail-closed, but worth pinning as intended (retry-later, never
  loosen).

### 4.3 Design: a bracket capability with no injectable readings

New in `python_authority.rs`:

```rust
/// Proof that TWO live fetches of revenue-fee-authority-status happened
/// in this process, in order, around a batch acquisition. No public
/// constructor; readings are not extractable for reuse.
pub struct BracketedAuthorityOff { first: PythonAuthorityOff,
                                   second: PythonAuthorityOff }

pub struct OpenBracket { first: PythonAuthorityOff /* non-Clone */ }

impl PythonAuthorityClient {
    /// Fetch #1. `OpenBracket` is !Clone and consumed by close().
    pub async fn open_bracket(&self, now: i64, max_age: i64)
        -> Result<OpenBracket, PythonAuthorityDenyReason>;
}
impl OpenBracket {
    /// Fetch #2 happens INSIDE this method — the caller cannot supply
    /// it — then validate_stable_epoch(first, second) decides.
    pub async fn close(self, client: &PythonAuthorityClient,
                       now: i64, max_age: i64)
        -> Result<BracketedAuthorityOff, PythonAuthorityDenyReason>;
}
```

- `PythonAuthorityOff` fields become private with read-only accessors;
  the ONLY constructors are `validate_status` (module-internal field
  init) — fixture-building tests migrate to
  `validate_status(&json!({...}), now, max_age)`, which they already
  exercise.
- `LiveBatchAuthorization::authorize` signature changes: the two
  `&PythonAuthorityOff` parameters are replaced by one
  `&BracketedAuthorityOff`. The epoch/advancement validation stays
  inside `close()` (single place), and `authorize` reads
  `bracket.second_generation()` for
  `python_authority_generation`.
- Two `compile_fail` doctests pin the capability (mirroring
  `LiveSessionArm`'s :248-258 pattern): (1) constructing
  `BracketedAuthorityOff { .. }` outside the module; (2) constructing
  `PythonAuthorityOff { .. }` outside the module.
- Independence in time is structural (`close()` runs strictly after
  `open_bracket()` returned, with the batch assembly between them);
  independence in *evidence* keeps the existing runtime checks
  (advancing `observed_at`, stable epoch) because Python's endpoint —
  not Rust's call order — is the authority on what was observed.
- Rehearsal binary (`rehearse_fee_cutover.rs`) migrates to drive the
  bracket against its fake RPC server, preserving its deny-matrix
  coverage.

## 5. Area A — cutover-arm reuse/re-mint closure

### 5.1 What already holds (cutover_arm.rs)

Atomic one-time consumption via `RENAME_NOREPLACE` into
`consumed_dir/<nonce>` + dir fsync (:480-519); same-nonce replay ⇒
`ReusedNonce` without touching the first consumption's evidence;
`LiveSessionArm` non-serializable, private fields, compile_fail-pinned
(:248); a failure AFTER the rename still counts as consumed (fail-closed
direction, :477-479); arms present in non-live modes are consumed to
prove misconfiguration (main.rs:525-533); restart cannot reacquire
authority because the file was renamed away and the capability cannot be
persisted.

### 5.2 The gaps

- **G1 — the replay ledger is one directory.** `consumed_dir` is the
  only record a nonce was ever burned. Point `consumed-arm-dir` at a
  different path (config change between restarts), lose the dir (tmpfs,
  restore-from-backup, disk-full cleanup — this month's incident made
  that concrete), or wipe it, and every historical nonce is mintable
  again.
- **G2 — nothing pins "one resolution per process".**
  `resolve_startup_mode` (main.rs:535) is called once at startup today,
  but no test or type pins that a future RPC (e.g. a "reload mode"
  convenience) can't call `validate_and_consume` again mid-session.
- **G3 — retention/cleanup interplay is unstated.** Nothing today says
  the sweep (Area R) or any operator cleanup may not touch
  `consumed_dir`.

### 5.3 Design: a durable nonce deny-ledger + a single-resolution pin

- New Class-E table (idempotent DDL addition to fee_runway.rs):

```sql
CREATE TABLE IF NOT EXISTS rust_consumed_arm_nonces (
    nonce TEXT PRIMARY KEY,
    consumed_at INTEGER NOT NULL,
    source_commit TEXT NOT NULL,
    binary_sha256 TEXT NOT NULL,
    arm_expires_at INTEGER NOT NULL
);
```

- Consumption order becomes: validate fields → **INSERT the nonce row
  (plain INSERT; `SQLITE_CONSTRAINT_PRIMARYKEY` ⇒ `ReusedNonce`)** →
  `RENAME_NOREPLACE` → dir fsync. Both ledgers must say "never seen"
  for consumption to proceed; either one refusing denies. Crash
  windows (§7): after INSERT / before rename ⇒ nonce burned in DB, file
  still present ⇒ next attempt denies `ReusedNonce` — an operator mints
  a FRESH nonce; fail-closed, never replay. The DB row is a **deny
  list**: no code path reads it to *grant* anything, so
  restart-never-reacquires is untouched (the capability still only
  exists via `validate_and_consume` in-process).
- Interface: `validate_and_consume` gains a
  `nonce_ledger: &dyn ConsumedNonceLedger` parameter (a one-method
  trait implemented by `ObserverHandle` synchronously through the
  actor, and by an in-memory fake for the module's offline tests —
  keeping cutover_arm itself DB-free and fully offline-testable, which
  is its pinned design property). `resolve_startup_mode` threads the
  real ledger through `StartupModeInputs`. The ledger insert is a store
  write on the startup path with no outer deadline (startup blocks are
  visible and acceptable; §3 rules apply only to steady-state paths).
- **G2 pin**: `resolve_startup_mode` remains the only production caller
  of `validate_and_consume` (rehearsal binary excepted), enforced by a
  source-scan test in `tests/cutover_arm.rs` (the same technique as the
  removed action-call-site scan the codebase already used); and the
  resolved `ValidatedFeeMode` stays immutable in `State` (already
  structural — no setter exists; add the assertion to the scan test's
  doc so a future setter is a visible violation).
- **G3 pin**: `consumed_dir` and `rust_consumed_arm_nonces` are named
  in `EXCLUDED_TABLES`/runbook §8 as never-pruned; the retention lint
  (§2.6) covers the table; the runbook change covers the directory.
- Expired-but-unconsumed arms: still denied by `Expired` and left on
  disk untouched (operator's artifact, operator's cleanup) — unchanged,
  now stated.

## 6. Exact files/interfaces touched by the implementation task

| file | change |
|---|---|
| `crates/revops-db/src/retention.rs` (new) | constants, table classification consts, `RetentionReport` |
| `crates/revops-db/src/fee_runway.rs` | `run_retention_sweep`; `rust_consumed_arm_nonces` DDL + `insert_consumed_nonce`; `idx_rust_fee_cycles_completed` |
| `crates/revops-db/src/owner.rs` | `Command::RunRetentionSweep` + `Command::InsertConsumedArmNonce` + async/blocking siblings |
| `crates/revops/src/fee_state.rs` | trait: `dispatch_run_retention_sweep`; ObserverHandle impl via `spawn_store_dispatch` |
| `crates/revops/src/fee_scheduler.rs` | enqueue sweep after scheduled commit; `retention_failures` + `oldest_pending_age_seconds` counters; pending-entry clock stamp |
| `crates/revops/src/fee_execution.rs` | `STORE_BUDGET_FLOOR`; `store_budget()` clamp; `store_intent_outcome_unknown` deny string; `authorize(...)` takes `&BracketedAuthorityOff` |
| `crates/revops/src/python_authority.rs` | private fields + accessors; `OpenBracket`/`BracketedAuthorityOff`; compile_fail doctests |
| `crates/revops/src/cutover_arm.rs` | `ConsumedNonceLedger` trait param; DB-first consumption order |
| `crates/revops/src/main.rs` | `RPC_BRIDGE_RECV_TIMEOUT` + `recv_timeout` at :1261/:1324; thread nonce ledger through `StartupModeInputs` |
| `crates/revops/src/bin/rehearse_fee_cutover.rs` | bracket + ledger migration |
| `docs/runbooks/rust-fee-cutover.md` | §8 additions: VACUUM policy, consumed_dir/never-prune list |

## 7. Crash/restart matrix

| crash point | state after restart | why it's safe |
|---|---|---|
| mid retention batch | txn rolled back; earlier batches kept | each batch independently satisfies I1–I8; sweep is idempotent |
| after sweep, before report/log | rows gone, no log line | report is observability only; counters recomputed from tables |
| outer budget expired, queued write landed post-crash | intent row `outcome IS NULL` | existing `reconcile_quarantine_on_restart` path — now attributable via the T1 deny string |
| after nonce INSERT, before arm rename | nonce burned, arm file present | next consumption denies `ReusedNonce`; operator mints fresh arm — fail-closed, no replay |
| after rename, before fsync return | consumed on disk (rename durable or not); nonce in DB regardless | DB ledger makes the pre-existing "fsync failed ⇒ still consumed" contract durable across the one crash window it had |
| restart with live session lost | no authority: capability non-persistable, arm renamed away, DB ledger only denies | unchanged invariant, now doubly enforced |

## 8. Fail-closed error semantics (new/changed, all typed)

- `RetentionReport { deleted: BTreeMap<&'static str, u64>, truncated }`;
  sweep failure → `retention_failures` counter + log; never affects a
  cycle, never leaves a partial batch.
- RPC bridge expiry → structured JSON error naming the timeout; never a
  hang.
- `store_budget` expiry on intent write →
  `BroadcastError::Persistence("store_intent_outcome_unknown: ...")`;
  batch denied before any RPC byte, outcome ambiguity owned by
  reconciliation.
- Bracket failure → existing `PythonAuthorityDenyReason` codes
  unchanged (stable-code contract respected: no variant reworded; the
  new construction path reuses them).
- Nonce-ledger insert failure that is NOT a PK conflict →
  `ConsumeFailed(detail)` (deny; arm file untouched, retryable by the
  operator).

## 9. RED-first test + mutation matrix

Every behavior lands test-first (observed red on the pre-change tree),
per repo discipline; mutations verify each guard is load-bearing with
sha256-exact restores.

| # | test (new unless noted) | red against | mutation that must re-red it |
|---|---|---|---|
| R1 | `sweep_preserves_active_quarantine_and_its_cycle` | current tree (no sweep exists → write sweep first red-style against a stub returning 0? No: red = predicate without the `q.cycle_id` clause) | drop the I1 `NOT EXISTS` clause |
| R2 | `sweep_never_changes_mutation_or_broadcast_counts` | predicate without `request_count = 0` | drop any of the three I2 clauses (3 mutants) |
| R3 | `sweep_keeps_last_n_cycles_on_a_stalled_clock` | predicate without keep-last subquery | set `RETAIN_MIN_CYCLES = 0` |
| R4 | `sweep_batches_are_bounded_and_idempotent` | unbatched DELETE | remove `LIMIT :batch` |
| R5 | `retention_classifies_every_table` | any unclassified table | add a table to DDL without classifying |
| R6 | `sweep_failure_counts_loudly_and_never_blocks_the_cycle` | sweep wired inline/blocking | make dispatch synchronous on owner |
| T1 | `store_budget_never_undercuts_sqlite_busy_wait` (rewrites tests/fee_execution.rs:1106) | current `store_budget()` (1 s < 5 s reproduces today's race) | remove the `.max(STORE_BUDGET_FLOOR)` clamp |
| T2 | `fee_debug_rpc_answers_with_typed_error_when_owner_is_wedged` | current unbounded `recv()` (test hangs → use harness timeout as red) | revert `recv_timeout` to `recv` |
| T3 | `intent_budget_expiry_reports_unknown_not_clean_failure` | current deny string | reclassify expiry as clean failure |
| T4 | `a3_pending_age_is_visible_after_warn_threshold` | no age surface | drop the counter update |
| F1 | `authorize_requires_a_bracket_not_two_readings` (compile-level: signature change) + runtime `bracket_close_performs_a_real_second_fetch` (fake RPC server counts calls) | current injected-parameters shape | make `close()` reuse `first` |
| F2 | compile_fail: forge `BracketedAuthorityOff`/`PythonAuthorityOff` | fields currently `pub` | re-widen field visibility |
| F3 | `same_second_second_fetch_denies_non_advancing` (exists as unit for `validate_stable_epoch` — extend through bracket) | n/a (already green at unit level) | drop the `observed_at` strict check in `close()` |
| A1 | `wiped_consumed_dir_does_not_permit_nonce_replay` | current filesystem-only ledger (reproduces G1 exactly) | skip the DB insert |
| A2 | `nonce_insert_before_rename_survives_crash_between` (fault-injecting ledger fake) | rename-first ordering | swap the order back |
| A3 | `validate_and_consume_has_one_production_caller` (source scan) | n/a (green today; pins G2) | add a second caller |
| A4 | `db_ledger_is_a_deny_list_never_a_grant` (no read path returns authority; source scan for readers) | n/a (structural pin) | add a grant-side read |
| gates | full workspace debug+release, clippy, fmt, T8b byte-guards | — | — |

Red-first honesty note: R1–R4's "red" is achieved by writing each
invariant test against a first-cut sweep that deliberately lacks the
guard under test (guard-last construction), the same discipline Task 44
used; where compile-shape changes force implementation-first (F1
signature), mutation verification substitutes, disclosed in the
implementation report.

## 10. Integration sequencing (Task 57 and neighbors)

1. **Order**: implement AFTER Task 57's impl merges to canonical main
   (its owner-framework and loop-health tables change owner.rs and the
   observer schema; landing retention first would force Task 57 to
   rebase across the sweep AND leave its new tables unclassified).
   The classification lint (R5) is the designed interlock: Task 57's
   tables go red at *this* task's implementation until classified —
   coordination is a compile-time/test-time fact, not a memo.
2. Areas T2/T4 (bridge timeout, pending-age) touch only
   main.rs/fee_scheduler.rs surfaces Task 57 doesn't own — they can
   land in the same implementation task regardless.
3. Area F and A touch live-path files (fee_execution.rs,
   cutover_arm.rs, python_authority.rs) that neither Task 57 nor Task
   51 (restart supervisor — node-side) edits; no ordering constraint,
   reviewed as one unit with R/T for a single coherent checkpoint.
4. Task 42 (SeedOnce first-cycle coherence) reads
   `rust_mempool_fee_history` semantics; Area R deliberately does NOT
   touch that table's bounds, so no interaction.
5. Deployment: all of this is repo-only until a fresh soak window is
   operator-acknowledged (any deployment containing this + A3/Task 53
   is a NEW candidate with a fresh clock, per the standing task-45
   boundary).

## 11. Open questions for the reviewer (explicitly non-blocking defaults)

1. 30 d vs 60 d for `RUNWAY_EVIDENCE_RETENTION_SECONDS` — default 30 d
   stands unless codex wants deeper lookback for the whole-plugin
   waves.
2. Should `ingested_forwards` retention land in the same
   implementation task (recommended: yes, same mechanism, one review)
   or be split to keep the fee-runway checkpoint minimal?
3. `RETENTION_MAX_BATCHES_PER_SWEEP = 8`: with a years-dormant DB the
   drain takes many cycles; an operator-invoked
   `revops-r-retention-sweep` RPC (bounded, same code path) could be
   added later — deliberately out of scope here.
