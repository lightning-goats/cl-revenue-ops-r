# Task 60 Rebalance Runtime Wiring Implementation Plan

> **For the implementer (solo mode):** Execute incrementally with RED-first
> tests, focused gates after each slice, and a clean checkpoint commit per
> slice. Super-agent review is suspended (operator directive 2026-07-29);
> the review criterion is satisfied by the mutation matrix in Slice 6 plus
> operator sign-off.

**Goal:** Wire `revops-rebalance` into the plugin runtime with typed
submission outcomes, durable Rust-owned reservations/receipts, and the three
Python-equivalent operator RPCs — with zero live-payment capability.

**Architecture:** All durable state goes through the existing single-owner
observer-db actor (new Class-E tables, retention-classified). Submission
outcomes are a four-way typed vocabulary mirroring Task 59's fee rails:
provably-not-sent, rejected-with-proof, success, and
outcome-unknown-after-submit (which retains its reservation, quarantines,
and can never auto-resubmit). Concrete `PaymentRpc`/`ReconcileRpc` adapters
speak CLN JSON-RPC over a socket path and are proven only against local
Unix-socket fakes; `PaymentMode::Live` remains constructible only by the
future cutover task (source-scan pinned today — untouched).

**Tech Stack:** Rust 2021, rusqlite transactions, Tokio, existing
`cln_rpc`-style raw socket calls, `FakeClnServer` test pattern from
`tests/fee_execution.rs`.

## Global Constraints

- Repo-only: no live CLN contact, no deployment, no arm consumption, no
  Python shutdown, no production DB writes (`revenue_ops.db` stays behind
  the read-only actor).
- `PaymentMode::Live` gains no new construction site (existing source-scan
  test must stay green verbatim).
- Reuse Task 59 vocabulary: two-phase store admission
  (`StoreReceipt`/`StoreAdmissionRefused`), `STORE_BUDGET_FLOOR`, typed
  outcome-unknown semantics. No new blocking bridges on async paths.
- Every new observer-db table is classified in `retention.rs` in the same
  commit that adds its DDL (the R5 lint reds otherwise).
- `force` on the manual RPC bypasses SOFT policy only (cooldown, budget
  advisory); it never bypasses the hard amount cap
  (`rebalance_max_amount`), durable reservation, intent recording, rate
  limiting, or quarantine refusal (py `cl-revenue-ops.py:4867-4880`).
- RPC names use the established `rpc_name()` prefix (`revenue-r-*`) with
  Python-equivalent response contracts; exact-name registration belongs to
  the Task 66/69 cutover surface, never here (Python still owns
  `revenue-rebalance*` on the node).

---

### Slice 1: Durable rebalance rails in revops-db (+ EXCLUDED membership pin)

**Files:**
- Modify: `crates/revops-db/src/fee_runway.rs` (DDL + row types + fns; the
  observer schema lives here today)
- Modify: `crates/revops-db/src/retention.rs` (classify new tables)
- Modify: `crates/revops-db/src/owner.rs` (commands + handle methods)
- Test: `crates/revops-db/tests/retention.rs`, `crates/revops-db/tests/owner.rs`

**Interfaces produced:**
- `rust_rebalance_attempts` (Class E append-only): `id INTEGER PK`,
  `request_id TEXT NOT NULL UNIQUE`, `source_channel TEXT NOT NULL`,
  `dest_channel TEXT NOT NULL`, `amount_sats INTEGER NOT NULL`,
  `max_fee_sats INTEGER NOT NULL`, `payment_hash TEXT`, `trigger TEXT NOT
  NULL CHECK (trigger IN ('cycle','manual','manual_force'))`,
  `submitted_at INTEGER NOT NULL`, `outcome TEXT CHECK (outcome IN
  ('success','rejected','clean_failure_before_write','outcome_unknown'))`,
  `outcome_detail TEXT`, `fee_paid_sats INTEGER`, `completed_at INTEGER`.
- `rust_rebalance_reservations` (Class E, status-flipped like quarantine):
  `id INTEGER PK`, `attempt_request_id TEXT NOT NULL`, `reserved_sats
  INTEGER NOT NULL`, `reserved_at INTEGER NOT NULL`, `status TEXT NOT NULL
  CHECK (status IN ('active','settled','released','quarantined'))`,
  `settled_fee_sats INTEGER`, `resolved_at INTEGER`.
- `fee_runway` fns: `insert_rebalance_attempt(conn, &RebalanceAttemptIntent)
  -> Result<i64>` (inserts attempt + `active` reservation in ONE
  transaction), `settle_rebalance_attempt(conn, request_id, &RebalanceSettle)
  -> Result<()>` (terminal outcome + reservation flip in ONE transaction;
  refuses a second terminal write), `unresolved_rebalance_attempts(conn)`,
  `active_rebalance_reserved_sats(conn, since)`.
- Owner: `Command::InsertRebalanceAttempt` / `SettleRebalanceAttempt` /
  `UnresolvedRebalanceAttempts` / `ActiveRebalanceReservedSats`, async +
  blocking siblings, plus two-phase
  `try_insert_rebalance_attempt(...) -> Result<StoreReceipt<i64>,
  StoreAdmissionRefused>` for the submit path.

- [ ] RED: retention classification test extends
  `retention_classifies_every_table_including_sqlite_internal` (fails on
  unclassified new DDL) and adds
  `never_prune_membership_is_pinned_exactly` asserting `EXCLUDED_TABLES`
  equals the exact expected set including both new tables (the facts:1720
  follow-up from the task-59 review).
- [ ] RED: owner round-trip test — intent insert returns id, duplicate
  `request_id` is a clean actor-reported error, settle flips attempt and
  reservation atomically, second settle refuses, unresolved list shows the
  pending attempt, reserved-sats sums only `active` rows.
- [ ] GREEN: DDL, retention classes, fee_runway fns, owner commands.
- [ ] Focused gates (`revops-db` suite), fmt, clippy; commit slice.

### Slice 2: Typed submission outcome rail (no-resubmit discipline)

**Files:**
- Create: `crates/revops/src/rebalance_execution.rs`
- Modify: `crates/revops/src/lib.rs`
- Test: in-file unit tests + `crates/revops/tests/rebalance_execution.rs`

**Interfaces produced:**
- `pub enum RebalanceSubmitOutcome { CleanFailureBeforeWrite { detail },
  Rejected { detail }, Success { fee_sats, fee_msat, hops, parts },
  OutcomeUnknownAfterSubmit { payment_hash, detail } }`.
- `pub fn classify_execution(result: &ExecutionResult) ->
  RebalanceSubmitOutcome`: `success=true` → `Success`;
  `payment_pending=true` (waitsendpay code 200 / CLN pending) →
  `OutcomeUnknownAfterSubmit`; error strings proven pre-write
  (`DRYRUN_GATE_SENDPAY_DISABLED`, invoice/route-build failures,
  `NATIVE_INVOICE_ERROR_PREFIX`, sendpay immediate refusal) →
  `CleanFailureBeforeWrite`; explicit CLN failure after write with terminal
  proof (waitsendpay definite failure codes) → `Rejected`; anything
  ambiguous defaults to `OutcomeUnknownAfterSubmit` (fail-closed).
- `pub struct RebalanceSettlement` — the owner-side terminal action per
  outcome: settle/release/quarantine mapping. Unknown retains the
  reservation (`quarantined`), records `outcome_unknown`, and NEVER
  produces a resubmit instruction (the type has no retry variant —
  structural pin).

- [ ] RED: classification table-driven test over scripted
  `ExecutionResult`s (each of the four arms + the ambiguous default) —
  fails on the missing module.
- [ ] RED: `unknown_outcome_retains_reservation_and_never_resubmits` —
  drives settle-through-owner with an unknown outcome, asserts reservation
  status `quarantined`, reserved sats still counted, and a source-scan-style
  assertion that `rebalance_execution.rs` contains no loop over submit.
- [ ] GREEN: implement module; wire settlement through Slice 1 owner API.
- [ ] Focused gates; commit slice.

### Slice 3: Concrete PaymentRpc + ReconcileRpc adapters (socket fakes only)

**Files:**
- Create: `crates/revops/src/rebalance_adapters.rs`
- Modify: `crates/revops/src/lib.rs`
- Test: `crates/revops/tests/rebalance_adapters.rs` (FakeClnServer pattern
  copied from `tests/fee_execution.rs` — behaviors: Success(Value),
  Rejected{code,message}, DisconnectAfterReceipt, HangForever)

**Interfaces produced:**
- `pub struct ClnPaymentRpc { socket_path: PathBuf, timeout_seconds: u64 }`
  implementing `revops_rebalance::executor::PaymentRpc` (getinfo_id,
  invoice, sendpay, waitsendpay(timeout), delpay, delinvoice) — one fresh
  connection per call, `revops_rpc::call_with_timeout` wrapper, error
  mapping to `RpcFailure` preserving CLN code/message so
  `FailureKind`/classification sees real codes; waitsendpay timeout maps to
  the pending shape (`payment_pending`), never to a clean failure.
- `pub struct ClnReconcileRpc { socket_path, timeout_seconds }` with
  `listsendpays(payment_hash) -> Result<Value, RpcFailure>`: the restart
  reconciliation read for unresolved attempts (complete/failed/pending
  disambiguation).
- Both are plain structs with no mode field: the DryRun/Live gate stays in
  `NativeRouteExecutor::mode`, untouched.

- [ ] RED: adapter tests against the fake socket — sendpay param shape
  (route/payment_hash/bolt11/payment_secret), waitsendpay timeout → pending
  mapping, JSON-RPC error → RpcFailure with code, disconnect → transport
  failure, invoice expiry parameter passed verbatim.
- [ ] RED: source-scan test — `PaymentMode::Live` construction sites still
  zero outside tests (existing pin re-asserted against the new module by
  path inclusion).
- [ ] GREEN: implement adapters.
- [ ] Focused gates; commit slice.

### Slice 4: Rebalance owner — evidence revalidation + exactly-once settle

**Files:**
- Create: `crates/revops/src/rebalance_owner.rs`
- Modify: `crates/revops/src/lib.rs`, `crates/revops/src/main.rs` (spawn
  under `LoopId::Rebalance` registration; observer-mode stays structurally
  free of it)
- Test: `crates/revops/tests/rebalance_owner.rs`

**Interfaces produced:**
- `pub struct RebalanceOwner` — ONE serialized owner (bounded mpsc, Task 57
  ingress discipline) handling `RebalanceMsg::{RunCycle{reply},
  Manual{params, reply}, Debug{query, reply}, ReconcileOnStart{reply}}`.
- Flow per submission: (1) revalidate FRESH evidence (funds/peer channels
  via `FacadeRpc`) — stale/missing evidence refuses typed
  (`evidence_unavailable`), fail-closed; (2) durable intent + active
  reservation via two-phase `try_insert_rebalance_attempt` (admission
  refusal = clean typed non-write `store_admission_refused`; receipt expiry
  = `store_intent_outcome_unknown`, submission NEVER proceeds); (3) execute
  via `CandidateExecutor` (DryRun rails today); (4) classify via Slice 2;
  (5) settle exactly once through the owner transaction. A settle-write
  failure surfaces `ResultPersistenceUnknown`-shaped typed error and
  suspends the rebalance owner (no further submissions until restart) —
  the Task 59 F4 posture.
- `ReconcileOnStart`: every `outcome IS NULL` attempt is looked up via
  `ReconcileRpc`; definite success/failure settles it; still-pending or
  lookup failure quarantines the reservation (never releases silently).
- `force` handling: skips cooldown/budget-advisory checks ONLY; hard cap,
  reservation, intent, rate limit, quarantine refusal identical to
  non-force (py parity).

- [ ] RED: owner tests with scripted CandidateExecutor + fake store —
  intent-before-execute ordering (a refused store admission executes
  NOTHING — zero CandidateExecutor calls), exactly-once settle (second
  terminal refuses), unknown quarantines + suspends nothing, settle-failure
  suspends further submissions, reconcile-on-start settles a definite
  outcome and quarantines a pending one, force bypasses only soft gates.
- [x] GREEN: implement owner. EXECUTED DEVIATIONS (disclosed): (a) no
  `LoopId::Rebalance` variant -- expanding the loop registry from five to
  eight is explicitly Task 67's scope; the owner registers no loop-health
  row today. (b) `deps.engine` is `Option<...>`: production main.rs wires
  `None` pre-cutover (full engine assembly needs production-equivalent
  Facade/RebalanceStore impls that belong to the cutover surface), so the
  RPCs keep Python's exact "Rebalancer not initialized" arm while every
  owner rail is proven against scripted engines. (c) cycle executions are
  recorded as post-hoc atomic attempt+terminal rows -- the frozen kernel
  executes internally and cannot be intercepted for pre-submit intents
  without breaking parity; the cutover task that injects live payment
  capability must move the intent write ahead of the wire (module doc).
- [x] Focused gates; commit slice.

### Slice 5: The three operator RPCs (Python-equivalent contracts)

**Files:**
- Create: `crates/revops/src/rpc_rebalance_ops.rs`
- Modify: `crates/revops/src/main.rs` (three `rpc_name()` registrations),
  `crates/revops/tests/action_surface.rs` (reachability pins)
- Test: `crates/revops/tests/rpc_rebalance_ops.rs`

**Contracts (from `cl-revenue-ops.py`):**
- `revenue-r-rebalance-cycle [max_candidates=20]` (py:3802): runs one owner
  cycle, returns `{"status":"success","rebalance_decision":...,
  "last_cycle":...}` or `{"error":...}` — never a fake success when the
  owner refused.
- `revenue-r-rebalance-debug [channel_id] [peer_id] [summary_only]
  [include_hot_markers] [max_candidates]` (py:3896): filter coercion parity
  (non-int `max_candidates` → 0, never an exception out of a diagnostic;
  `include_hot_markers` forced false under `summary_only`), capital
  controls/thresholds/channel buckets shape.
- `revenue-r-rebalance from_channel to_channel amount_sats [max_fee_sats]
  [force=false]` (py:4826): usage error string verbatim; rate limit BOTH
  force values; SCID regex `^\d+[x:]\d+[x:]\d+$` with the exact error
  format; `amount_sats >= 1` int coercion errors verbatim; hard cap
  rejection shape (`requested_sats`/`max_amount_sats`) "rejected even under
  force"; `max_fee_sats` non-negative int-or-null; success/error envelope
  (`status` + flattened result minus its own `status`).
- All three go through the owner ingress with `query_owner_bounded`-style
  bounded waits (typed `owner_queue_saturated`/`owner_response_timeout`),
  and return completion results, not queue admission.

- [ ] RED: byte-shape tests per RPC (usage/validation error strings
  verbatim against the Python source), reachability pin in action_surface
  (three names registered exactly once, no exact-Python-name registration
  anywhere), saturation/timeout typed errors surface.
- [ ] GREEN: implement handlers + registrations.
- [ ] Focused gates; commit slice.

### Slice 6: Mutations, full verification, report, operator sign-off

- [ ] Mutation matrix (each applied, pinned test red, reverted; log kept):
  M1 drop reservation insert from the intent txn → Slice 1 atomicity test;
  M2 allow second terminal settle → exactly-once test; M3 classify pending
  as CleanFailure → classification + unknown-retention tests; M4 add a
  resubmit loop on unknown → no-resubmit pin; M5 execute before store
  admission → ordering test; M6 force bypasses the hard cap → force test;
  M7 reconcile releases a pending reservation → reconcile test; M8
  unclassified table / EXCLUDED membership drift → retention pins; M9
  Live-mode construction in adapters → source-scan; M10 skip evidence
  revalidation → evidence test.
- [ ] `cargo test --workspace --all-targets` (debug + release), doctests,
  `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`,
  `git diff --check`, clean tree.
- [ ] Write `/home/sat/agent-tasks/task-60-implementation-report.md`
  (commits, RED/GREEN evidence, mutation outcomes, safety boundary, solo-
  mode note) and mark task 60 `impl`; `review` waits for operator sign-off.
