# Task 67: Analytics Runtime Owners + Current-Boot Eight-Loop Health

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans.
> Solo mode: tier-1 review = self-verification (RED-first + mutation matrix
> + full gates) + operator sign-off.

**Goal:** Port the three missing runtime owners (flow-analysis,
startup-snapshot, financial-snapshot), expand the loop registry from five to
the exact eight business/startup loops, and bind loop health to the CURRENT
BOOT so a fresh process can never inherit a prior boot's pass. Wire the
analytics RPC surfaces to real Rust-owned evidence with typed failure
instead of `not_yet_ported` markers. Observation-only throughout: no action
capability, no policy writes.

**Architecture:** The pure kernels are already frozen in `revops-analytics`
(flow/kalman/profitability/classification/demand_flow). This task builds the
ASSEMBLERS that feed them and the STORES that persist their outputs, plus
three observer passes registered in `ObserverRuntime`.

## The audit's core defect (what slice 1 fixes)

`rust_loop_health` is a durable one-row-per-loop table with no session
binding. `reconcile_incomplete_on_restart` only downgrades rows that were
*in flight* at the crash; a row that COMPLETED before a restart keeps
`terminal_status='passed'` and its prior-boot `last_passed_at`. So
`revenue-r-health` reports `passed` on a process that has never run a single
pass. Python's own heartbeats are in-memory and therefore inherently
current-boot; the durable Rust table regressed on that property.

## Global constraints

- The eight loop names are Python's exact thread labels
  (cl-revenue-ops.py:3588-3600): `flow-analysis`, `fee-adjustment`,
  `rebalance-check`, `startup-snapshot`, `financial-snapshot`,
  `boltz-auto-cycle`, `capacity-planner`, `lnplus-watcher`.
- `rust_loop_health`'s canonical-column refusal is DELIBERATE (it refuses to
  migrate rather than fabricate terminal evidence). Adding the boot column
  edits `CANONICAL_COLUMNS`; it must NOT be softened into an ALTER TABLE
  path. Existing dev DBs failing startup is the correct outcome.
- Observation-only: the new owners take no action capability. Python's
  flow-analysis loop ALSO does `cleanup_old_data`, `cleanup_expired_policies`
  and `decay_reputation` — those are mutations and are explicitly REFUSED
  here (retention already has an owner in `revops-db/src/retention.rs`).
  Disclosed, not smuggled.
- `revenue-r-analyze` with no `channel_id` stays refused: in Python it
  triggers a mutating fleet sweep, and an RPC that writes is not a read RPC.
- New tables are `rust_`-prefixed with the same canonical-column refusal.
- Analytics writes go through `ObserverHandle` (the serialized actor), NEVER
  through Task 65's `StateWriterHandle` (scoped to policy/config state).

## Slices

### Slice 1: Boot identity + eight loops + current-boot health

`revops-db/src/loop_health.rs`: `LoopId` gains `FlowAnalysis`
("flow-analysis"), `StartupSnapshot` ("startup-snapshot"),
`FinancialSnapshot` ("financial-snapshot"); `REQUIRED_LOOPS: [LoopId; 8]`;
the hardcoded `ORDER BY CASE` extended. `CANONICAL_COLUMNS` gains
`boot_id TEXT` (set on every `begin_loop_pass`) and `terminal_boot_id TEXT`
(set on finish/fail). New `BootIdentity { boot_id, process_id,
source_commit, binary_sha256, started_at }` minted once per process
(reusing `fee_scheduler::source_commit()`/`binary_sha256()`) and persisted
to a new `rust_boot_sessions` table — one shared record, so fee restart
markers and loop health cannot drift.

Health predicate becomes: a loop is `passed` ONLY when
`terminal_status='passed' AND terminal_generation = generation AND
terminal_boot_id = <this boot>`. A prior-boot pass reads `never_run_this_boot`
— a distinct honest state, never `passed` and never `error`.

- [ ] RED: `prior_boot_pass_is_not_inherited` (write a passed row with a
  foreign boot id, assert the current-boot status is NOT passed);
  `eight_loops_are_registered_exactly`; canonical-column pin updated.
  GREEN; gates; commit.

### Slice 2: Analytics durable stores

`revops-db/src/analytics.rs` + owner commands: `rust_channel_states`
(scid PK, flow state/role, balance position, updated_at, boot_id),
`rust_kalman_state` (scid PK, state json, updated_at),
`rust_temporal_profiles` (scid PK, profile json, updated_at),
`rust_financial_snapshots` (id, taken_at, local/remote/onchain/capacity
sats, revenue_accumulated, rebalance_cost_accumulated, channel_count).
Retention classification: snapshots are Class W (windowed, they are a time
series); the three current-state tables are Class C (bounded by scid
upsert). Pinned in the retention membership tests.

- [ ] RED: round-trip + upsert-replaces + retention-class pins. GREEN;
  gates; commit.

### Slice 3: Flow-analysis owner (observation-only)

`revops/src/flow_owner.rs`: assembles `listpeerchannels` + forwards +
persisted kalman/temporal state, runs the FROZEN
`revops_analytics::flow`/`kalman` kernels, persists channel states/kalman/
temporal profiles through the observer store. Cadence: Python's
`max(60, flow_interval)` with the 30s startup stagger; jitter is NOT ported
(deterministic scheduling; disclosed). Every required read is `Result`-shaped
— a failed read is a typed refusal and a FAILED loop pass, never a default.
The three Python mutations are refused with a typed
`flow_retention_not_this_owner` refusal naming `retention.rs`.

- [ ] RED: assembly refuses typed on each failed source; a healthy pass
  persists and marks the loop passed for THIS boot; the mutation refusal.
  GREEN; gates; commit.

### Slice 4: Startup-snapshot owner (one-shot)

`revops/src/startup_snapshot_owner.rs`: one-shot after a 60s interruptible
delay; reads peers, records a connection event for each connected peer
without history in the last hour (`insert_peer_connection_event` already
exists). Records its loop pass AFTER the work succeeds — Python records the
heartbeat BEFORE and never fails it; that bug is NOT ported (disclosed).

- [ ] RED: one-shot completes once and is `passed` this boot; a failed peer
  read marks the loop FAILED (not passed); no double-run. GREEN; gates;
  commit.

### Slice 5: Financial-snapshot owner

`revops/src/financial_snapshot_owner.rs`: 300s startup delay, one immediate
snapshot, then 86400s cadence. Assembles TLV (listfunds + listpeerchannels)
and lifetime stats, writes `rust_financial_snapshots`. During the first 300s
the loop honestly reads `never_run_this_boot`, never `error` — pinned.

- [ ] RED: snapshot round-trip; pre-first-run health state; typed refusal
  on unreadable TLV inputs. GREEN; gates; commit.

### Slice 6: Analytics RPC surfaces

Replace the `not_yet_ported` hardwiring with real evidence paths:
`rpc_analyze` (single channel from persisted flow state; no-channel_id stays
refused), `rpc_profitability` (real channel + summary builders, reachable),
`rpc_econ_snapshot`, `rpc_dashboard` (`tlv_sats`/`annualized_roc_pct` from
the snapshot store; remaining gaps stay typed), `rpc_health` (eight loops,
current-boot statuses). Every surface: typed failure, never a null gap or a
false success. Manifest/read-RPC tests updated (loops.len() 5 → 8).

- [ ] RED: each surface's refusal + healthy arm. GREEN; gates; commit.

### Slice 7: Mutations, battery, report

Mutation matrix A1–A10: A1 prior-boot pass inherited (drop the boot
predicate); A2 terminal_generation check dropped; A3 a failed flow source
defaults instead of refusing; A4 flow owner performs the Python retention
mutations; A5 startup-snapshot records its pass before the work; A6
financial-snapshot pre-first-run reads `error`; A7 REQUIRED_LOOPS back to 5;
A8 analyze fleet-sweep arm becomes reachable; A9 analytics writes routed
through the state writer; A10 canonical-column refusal softened to an
ALTER TABLE path. Full battery (debug+release, doctests, clippy
--all-features, fmt, diff check); report
`/home/sat/agent-tasks/task-67-implementation-report.md`.

## Explicitly OUT of scope (disclosed)

- **Filling Task 62's eleven `EvidenceGap` fields.** Those feed the capital
  planner kernel, and supplying them also makes Task 63's deliberately-dead
  Boltz execution branch (`boltz_owner.rs:868-883`, documented "kept total,
  unreachable until Task 67") LIVE. Turning a dead money-moving path live is
  a behavior change that deserves its own task and its own review, not a
  drive-by inside an analytics port. This task builds the owners and stores
  those gaps will read FROM; a follow-up task connects them.
- Jitter on loop cadences (deterministic scheduling is preferred and
  testable; Python's ±10-20% jitter exists to desynchronize threads, which
  the single-owner design does not need).
