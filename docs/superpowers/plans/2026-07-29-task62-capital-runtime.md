# Task 62 Capacity Planner + Capital Execution Runtime Plan

> **For the implementer (solo mode):** RED-first per slice, focused gates,
> checkpoint commit per slice; review = Slice 6 mutations + operator
> sign-off.

**Goal:** Wire the pure `plan_cycle` kernel into the plugin runtime with a
real evidence assembler, an owner-backed planner loop, durable
intent/reservation/action records before any external submit, typed
submission outcomes with quarantine-no-retry, and the planner RPC family —
with zero live mutation capability (fundchannel/close reachable only
through fakes; the live adapter construction is authority-gated like every
other action surface).

**Architecture:** Three rails already exist and are reused verbatim:
Task 60's durable-ledger shape (Class-E intent+reservation tables in the
observer db with two-phase admission), Task 60's four-way outcome
discipline (unknown quarantines, structurally no resubmit), and Task 65's
capability boundary (live adapters constructible only under the future
Task 69 authority; observer mode refuses typed). New here: the evidence
assembler (production read-only actor + RPC prefetch + observer reads →
`CycleEvidence`), the serialized planner owner, and the
`GovernorFacade`/`BudgetDb`/`ActiveIntentRegistry` boundaries the task
names.

**Tech Stack:** Rust 2021; existing seams `revops_capital::planner::
{plan_cycle, CycleEvidence, CyclePlan}` (kernel-tested, untouched);
Python reference `modules/capacity_planner.py` (`execute_cycle:363`,
`_execute_open:3062` — budget reserve BEFORE fundchannel,
`_execute_close:3767`, `_execute_defibrillation:3666`,
`_rpc_fundchannel:3054`).

## Global Constraints

- Repo-only: no live CLN contact, no deploy, no production DB writes; all
  transports proven on blocking Unix-socket fakes (the task-60 adapter
  pattern); temp DBs everywhere.
- The pure planner kernel is FROZEN (parity-reviewed); the runtime feeds
  and consumes it, never edits it.
- Positive budget evidence is MANDATORY: a missing, unreadable, or stale
  budget/spend input fail-closes the cycle typed — never "assume
  headroom" (the Task 8/11 audit's nullable-evidence complaint).
- Durable order per submission: intent + ACTIVE reservation + action
  record committed BEFORE any external submit; fresh action evidence
  revalidated at execution time; unknown outcomes retain the reservation
  (quarantined) with structurally no retry.
- New observer-db tables are retention-classified in the same commit
  (Class E) and the `never_prune_membership_is_pinned_exactly` pin is
  extended — deliberately, both sides.
- Live mutation adapters (`fundchannel`/`close`) follow the
  `PaymentMode::Live` discipline: the capability type exists, production
  constructs none of it, and a workspace source scan pins that.

---

### Slice 1: Durable capital rails (revops-db)

**Files:** modify `fee_runway.rs` (DDL+fns), `retention.rs`, `owner.rs`;
tests `tests/retention.rs`, `tests/owner.rs`.

- `rust_capital_intents` (Class E): `id PK`, `request_id TEXT NOT NULL
  UNIQUE`, `kind TEXT NOT NULL CHECK (kind IN ('open','close','defib'))`,
  `peer_id TEXT NOT NULL`, `channel_id TEXT`, `amount_sats INTEGER NOT
  NULL`, `reason TEXT`, `submitted_at INTEGER NOT NULL`, `outcome TEXT
  CHECK (outcome IN ('clean_refusal','rejected','success',
  'outcome_unknown'))`, `outcome_detail TEXT`, `txid TEXT`,
  `completed_at INTEGER`.
- `rust_capital_reservations` (Class E): same shape as the rebalance
  reservations (`attempt_request_id`, `reserved_sats`, `reserved_at`,
  `status CHECK ('active','settled','released','quarantined')`,
  `settled_sats`, `resolved_at`).
- fns mirroring task 60 slice 1 exactly: `insert_capital_intent`
  (intent + active reservation, ONE txn, UNIQUE dedup),
  `settle_capital_intent` (outcome + reservation flip, ONE txn,
  `outcome IS NULL` exactly-once guard), `unresolved_capital_intents`,
  `active_capital_reserved_sats(since)`.
- Owner commands + async handles + two-phase `try_insert_capital_intent`.

- [ ] RED: membership pin gains both tables (fails on missing DDL);
  owner round-trip (atomic insert+reserve, dedup, exactly-once settle,
  quarantine keeps counting); then GREEN; gates; commit.

### Slice 2: Boundaries — GovernorFacade, BudgetDb, ActiveIntentRegistry

**Files:** create `crates/revops/src/capital_boundaries.rs`; modify
`lib.rs`; test `crates/revops/tests/capital_boundaries.rs`.

- `pub trait GovernorFacade: Send + Sync { fn authorize(&self, kind:
  CapitalKind, amount_sats: i64) -> GovernorVerdict }` with
  `GovernorVerdict::{Authorized{reason_code}, Denied{reason_code}}` —
  the governor consult before any submit (production impl arrives with
  authority assembly; tests script it).
- `pub trait BudgetDb: Send + Sync { fn positive_budget_evidence(&self,
  now: i64) -> Result<BudgetEvidence, String> }` where `BudgetEvidence
  { available_sats: i64, window_reserved_sats: i64, observed_at: i64 }`
  and the CALLER enforces: `Err` → typed fail-closed refusal
  (`capital_budget_evidence_unavailable`); `observed_at` older than
  `BUDGET_EVIDENCE_MAX_AGE_SECONDS = 60` → stale refusal
  (`capital_budget_evidence_stale`); `available_sats <= 0` → exhausted
  refusal. A temp-DB-backed impl reads the production-schema
  `budget_reservations`/`spend_reservations` through the READ-ONLY actor
  plus the observer-side `active_capital_reserved_sats`.
- `pub struct ActiveIntentRegistry` — in-process duplicate guard seeded
  from `unresolved_capital_intents` at startup: `begin(request_id | peer
  +kind)` refuses while an identical intent is in flight or unresolved;
  `resolve(request_id)` releases. (The DB UNIQUE is the durable rail;
  the registry is the pre-submit fast path that also covers
  cross-restart unresolved rows.)
- `pub enum CapitalSubmitOutcome { CleanRefusal{detail}, Rejected{detail},
  Success{txid: Option<String>}, OutcomeUnknown{detail} }` +
  `settlement_for` mapping (success→settled, rejected/clean→released,
  unknown→quarantined) — the settlement layer is execution-free
  (task-60 source-scan discipline, same forbidden-callable list adapted:
  `fundchannel`, `.close(`, `execute_cycle`).

- [ ] RED: budget-evidence gate table (missing/err/stale/non-positive
  all refuse typed, fresh-positive passes); registry duplicate/unresolved
  refusals incl. seeded-from-store; outcome mapping + execution-free
  scan. GREEN; gates; commit.

### Slice 3: Evidence assembler (read-only) + capacity/status RPC reality

**Files:** create `crates/revops/src/capital_evidence.rs`; modify
`main.rs` (capacity-report + planner-status arms), `lib.rs`; tests
`crates/revops/tests/capital_evidence.rs`.

- `pub async fn assemble_cycle_evidence(deps: &EvidenceDeps) ->
  Result<CycleEvidence, EvidenceRefusal>` — fills the kernel's
  `CycleEvidence` from: production READ-ONLY actor (channel states,
  policies, planner history/backoff), one RPC prefetch
  (listpeerchannels for `peer_channels`/balances — the fee-evidence
  prefetch pattern), config snapshot (planner_enabled, limits), and
  `now`. EVERY required input is `Result`-shaped: any failed read is a
  typed `EvidenceRefusal` naming the source — no `.ok()`-to-default on
  required evidence (nullable-evidence audit complaint). Discovery/
  enrichment fields fill from the same production tables the Python
  planner reads; fields whose Python source is a live external service
  keep their EXPLICIT empty-with-reason shape (documented per field in
  the module, not silently defaulted).
- `revenue-r-capacity-report`: when the planner runtime is absent
  (observer mode today) keep the byte-exact Python refusal (existing
  pin); when assembly succeeds in tests, `build_capacity_report` over
  assembled evidence.
- `revenue-r-planner-status`: Python-parity shape over the assembler's
  refusal/success (py:4596) — status reports evidence health honestly,
  never a fake-ready.

- [ ] RED: assembler refusal table (each required source sabotaged →
  its named refusal); a full temp fixture (production-schema DB + fake
  RPC) assembles a CycleEvidence the FROZEN kernel accepts
  (plan_cycle runs, no panic, deterministic skip/plan). GREEN; gates;
  commit.

### Slice 4: Transport adapters + execution classification (fakes only)

**Files:** create `crates/revops/src/capital_adapters.rs`; modify
`lib.rs`; tests `crates/revops/tests/capital_adapters.rs`.

- `pub trait FundchannelRpc: Send + Sync { fn fundchannel(&self, peer_id:
  &str, amount_sats: i64, request_amt: Option<i64>, compact_lease:
  Option<String>) -> Result<Value, RpcFailure>; }` and `pub trait
  CloseRpc { fn close(&self, channel_id: &str, unilateral_timeout_secs:
  Option<i64>) -> Result<Value, RpcFailure>; }` — blocking socket impls
  (`ClnFundchannelRpc`/`ClnCloseRpc`) with the task-60 error-encoding
  conventions (JSON error dicts; "rpc timeout" only for deadline expiry;
  transport ≠ timeout). Defibrillation's execution primitive is NOT an
  on-chain action: py `_execute_defibrillation:3666` drives probe/lure
  state — implemented as a typed `DefibrillationAction` consuming the
  Task 65 writer primitives where it writes (verified against the py
  body at implementation; if it proves rebalance-shaped, it reuses the
  task-60 owner seam instead — decided from source, disclosed either
  way).
- `classify_capital_submit(kind, result) -> CapitalSubmitOutcome`:
  connect-refused/validation refusal pre-write → CleanRefusal; explicit
  CLN error post-submit with terminal proof → Rejected; success with
  txid → Success; deadline expiry or ambiguous shape → OutcomeUnknown
  (fail-closed default — an on-chain fundchannel whose reply was lost
  MAY have broadcast).
- A capability wrapper `CapitalActionAdapters` holding both traits,
  constructed nowhere in production (source scan; the Task 69 authority
  consumes it later).

- [ ] RED: fake-socket wire-shape tests (fundchannel params incl.
  request_amt/compact_lease passthrough; close params), classification
  table incl. the ambiguous default, scan pins. GREEN; gates; commit.

### Slice 5: The planner owner + planner RPC family

**Files:** create `crates/revops/src/capital_owner.rs`; modify
`main.rs`, `lib.rs`, `tests/action_surface.rs`, `tests/manifest.rs`
(count guard widened deliberately); tests
`crates/revops/tests/capital_owner.rs`, `tests/rpc_capital.rs`.

- `CapitalOwner` (task-60 owner pattern: one OS thread, bounded ingress,
  suspension on settle-persistence failure): per planned action —
  governor consult → positive budget evidence (fresh) → registry begin →
  durable intent+reservation (two-phase) → fresh action-evidence
  revalidation (balances re-read; a changed/missing target refuses
  typed) → execute via adapters (absent in production → the typed
  `capital_adapters_not_assembled` refusal) → classify → settle exactly
  once → registry resolve. Reconcile-on-start settles definite outcomes
  from `listclosedchannels`/`listfunds` lookups and QUARANTINES the
  rest.
- RPC family on `rpc_name()`: `planner-status`, `planner-candidates`,
  `planner-candidate-sources`, `planner-history` (read-shaped, Python
  response contracts), `planner-execute` (owner cycle; Python
  uninitialized arm `{"error": "Capacity planner not initialized"}`
  verbatim while adapters are unassembled). Manifest guard 31 → +5,
  names asserted; action_surface pins registrations through rpc_name
  only.

- [ ] RED: owner rail tests (ordering: refused evidence/budget/registry
  → zero adapter calls; intent-write failure → no execute; unknown →
  quarantine + budget still held; settle failure → suspend; reconcile
  definite/quarantine split), RPC byte-parity arms. GREEN; gates;
  commit.

### Slice 6: Mutations, battery, report

- [ ] Mutations (apply → red → revert, logged): C1 budget evidence
  `Err`→`Ok(0-available)` treated as pass; C2 stale evidence accepted
  (drop the age check); C3 intent txn loses the reservation insert; C4
  second settle allowed; C5 execute before intent; C6 unknown releases
  the reservation; C7 registry allows a duplicate in-flight intent; C8
  evidence assembler defaults a failed required read; C9 adapters named
  in runtime.rs / constructed in production; C10 fresh-evidence
  revalidation skipped.
- [ ] Full battery (workspace debug+release, doctests, fmt, clippy
  --all-features, diff check); report
  `/home/sat/agent-tasks/task-62-implementation-report.md`; mark `impl`;
  `review` = operator sign-off.
