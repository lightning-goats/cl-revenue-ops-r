# Task 63: Boltz Runtime — Serialized Governed Execution + Quarantine

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans.
> Solo mode: tier-1 review = self-verification (RED-first + mutation matrix
> + full gates) + operator sign-off.

**Goal:** Wire Boltz into the Rust plugin: durable attempt/reservation
lifecycle, hardened allowlisted transport, non-forgeable action capability,
governed serialized owner, exact 22 Python-equivalent RPCs, fake-executable
end-to-end proof. No live CLI/CLN contact, no arm consumption, no
production DB writes.

**Architecture:** Mirror the task-60/62 owner pattern. The `revops-boltz`
kernels stay pure; task 63 hardens the ONE I/O boundary (`process.rs`),
adds the durable rails in `revops-db`, and composes everything in a
serialized owner in `revops`. Ambiguity (`CreateOutcome::Unknown`,
`ManualActionOutcome::Unverified`) becomes durable QUARANTINE: reservation
held, pair/channel blocked, structurally no resubmit.

**Audit baseline:** c68fddd. Survey (2026-07-29) confirms: no argv
allowlist, unbounded output, direct-child-only kill, argv leaked into
Timeout errors (pinned by `process_fake_executable.rs` test 8 — that pin
inverts in slice 2), `ExecutionMode::Armed` publicly constructible,
`ProcessBoltzCli.config` pub+mutable, zero durable pre-spawn state, no
pending-swap gate, `cli.rs:36` byte-slice char-boundary panic, duplicated
response builders (crate `rpc.rs` vs revops `rpc_boltz_*.rs`).

## Global constraints

- Python parity strings verbatim:
  - `{"error": "Boltz CLI integration not initialized"}` (manager-None arm,
    all manager-backed RPCs)
  - `"Boltz CLI integration disabled (set revenue-ops-boltz-enabled=true)"`
    (already in `CliError::Disabled`)
  - usage short-circuits (fire even with boltz dead):
    status/refund/claim usage strings byte-exact (see rpc_boltz_status.rs)
  - auto-cycle disabled shapes: `{'status':'disabled','reason':'boltz
    integration disabled','trigger':...}` / `'boltz auto-cycle disabled by
    config'`
- Manifest guard widens 32 → 54 (22 Boltz RPCs), deliberately, with names
  asserted.
- The backup mnemonic NEVER enters logs, errors, durable payloads, health,
  snapshots, or hexmem. No Debug/Display on the secret type.
- Unknown outcomes: never a loop pass, reservation retained (quarantined),
  no auto-resubmit path exists structurally.
- Zero production construction of the action capability/transport until
  Task 69 (source-scan pinned).

## Slices

### Slice 1: Durable rails (7A) — revops-db

`rust_boltz_attempts` (request_id UNIQUE; kind CHECK IN
('loop_in','loop_out','chainswap','refund','claim','withdraw');
channel_id NULL; amount_sats; estimated_fee_sats; argv_digest (sha256 of
redacted argv); outcome NULL CHECK IN
('not_submitted','rejected','committed','outcome_unknown');
swap_id NULL; outcome_detail; submitted_at; completed_at) +
`rust_boltz_reservations` (attempt_request_id, reserved_sats, reserved_at,
status CHECK active/settled/released/quarantined, settled_sats,
resolved_at). Same exactly-once settle discipline as capital
(`settle_boltz_attempt` bails "already terminal"). Durable operational
state: `rust_boltz_cooldowns` (channel_id PK, last_action_ts — fixes
restart-resets-cooldowns), `rust_boltz_ignores` (swap_id PK, note,
added_at), `rust_boltz_journal` (swap_id PK, record_json, recorded_at,
source — the JSON file's replacement; prune kernel stays
`revops_boltz::journal`). Queries: unresolved/quarantined attempts,
active+quarantined reserved sats, cooldown get/upsert, ignores CRUD,
journal upsert/list. Class E (never-prune): attempts, reservations,
ignores. Journal prunes by its own 180d/200 kernel, NOT by retention
sweeps (excluded there too; pruning goes through the owner explicitly).
Actor commands + async/blocking handles + `try_insert_boltz_attempt`
two-phase. RED: revops-db/tests/owner.rs round-trips + retention pin.

### Slice 2: Transport hardening (7B) — revops-boltz

`process.rs`:
- Query allowlist AT THE TRANSPORT: `ProcessBoltzCli` becomes query-only —
  first arg after the base must be in
  `["wallet" (list only: second token "list"), "listswaps", "swapinfo",
  "quote"/"fees" (quote argv family), "stats", "getpairs"]`-shaped
  allowlist (derive exact verbs from argv.rs builders); everything else →
  `CliError::ExitFailure`-shaped typed refusal
  (`boltz_query_transport_refused`) WITHOUT spawning. Fund-moving verbs
  (createswap, createreverseswap, createchainswap, refundswap, claimswaps,
  wallet send, swapmnemonic) are structurally unreachable through it.
- New `ArmedBoltzCli` (same file): full-argv transport, constructible ONLY
  from the slice-3 capability (private field). It is the only path to
  fund-moving verbs.
- Output bounds: cap each stream at 256 KiB; over-limit → truncate and
  append `"[truncated N bytes]"`; reader threads count and drain.
- Process-tree kill: `pre_exec(setsid)` (unix), on timeout
  `killpg(-pid)` then `kill` fallback, then `wait`. Test with a fake that
  spawns a child (`sh -c 'sleep 30 & wait'`) and assert the grandchild
  dies.
- stdin → `Stdio::null()`.
- Redaction: `CliError::Timeout.command` becomes the REDACTED form:
  subcommand + arg COUNT only (`"createswap (7 args redacted)"`) — flip
  test 8's assertion from "contains 100000" to "does NOT contain 100000".
  ExitFailure/NotFound messages capped at 300 CHARS (char-safe) and
  scrubbed of any line containing "mnemonic" (case-insensitive).
- Fix `cli.rs:36` byte-slice panic (`char_indices` boundary).
- `BoltzCliProcessConfig.enabled` private; constructor-only.
RED: extend `process_fake_executable.rs` (grandchild kill, output cap,
redacted timeout, allowlist refusal without spawn — assert the fake never
ran via a side-effect file) + `cli.rs` unit for the char boundary.

### Slice 3: Capability + typed outcomes (7C) — revops

`crates/revops/src/boltz_boundaries.rs`:
- `BoltzActionCapability` — private-field struct, NOT Clone/Copy, no
  public constructor; `pub(crate) fn assemble_for_tests` under cfg(test) +
  a doc note that Task 69 mints the production one. Holds the
  `ArmedBoltzCli` and the withdraw hard cap (`max_withdraw_sats` lives IN
  the capability, not the call — hazard 2).
- `BoltzSubmitOutcome { NotSubmitted{detail}, RejectedWithProof{detail},
  Committed{swap_id: Option<String>}, OutcomeUnknownAfterSubmit{detail} }`
  + `settlement_for_boltz` mapping (committed→settled,
  rejected/not_submitted→released, unknown→QUARANTINED) — mirror
  capital_boundaries; module execution-free (source scan: no "createswap",
  no "ExecutionMode", no ".run(").
- Classifier `classify_boltz_create(CreateOutcome<Value>)` and
  `classify_boltz_manual(ManualActionOutcome)`
  (Unverified → OutcomeUnknownAfterSubmit — an exit-0 refund is NOT proof).
- `MnemonicSecret` (no Debug/Display/Serialize; explicit
  `into_rpc_value()` consuming self — the single sanctioned egress).
RED: tests/boltz_boundaries.rs (mapping table, capability
non-constructibility via source scan, secret type has no Debug — compile
pin via trait-bound test).

### Slice 4: Governed submit rail (7D) — revops

`crates/revops/src/boltz_execution.rs`: the per-action rail as pure-ish
functions the owner calls (owner thread in slice 5): governor consult
(reuse `GovernorFacade`) → shared budget evidence (BudgetDb-style trait
over spend/reservation reads incl. Boltz active+quarantined holds) →
pending-swap gate (durable unresolved+quarantined attempts UNION a
listswaps-derived pending count — both must be zero unless
allow_concurrent) → structural envelope (fail-closed: unreadable spend →
refuse) → cooldown (durable table) → durable attempt+reservation
(two-phase, BEFORE spawn) → execute via capability → classify → settle
exactly-once transactionally (spend receipt row and reservation flip in
ONE txn — the P4-019 loud-write) → suspension on persistence failure.
RED: tests drive ordering with counting fakes (zero spawn on every
refusal; intent row exists before the fake transport is hit — fake
asserts the row via a shared handle).

### Slice 5: Serialized owner (7E) — revops

`crates/revops/src/boltz_owner.rs`: one OS thread, bounded ingress:
Manual(loop-in/out/chainswap/refund/claim/withdraw), BalanceCycle,
TreasuryCycle, AutoCycleRunNow{force,dry_run}, Reconcile, Debug, plus
read acks (status/history/budget assembled off-thread from the store +
query transport). Auto-cycle: single-flight, mode select via
`autocycle::select_boltz_auto_cycle_mode`, durable
consecutive-error state, cooldown pre-claim/restore via the durable
table (`cooldown_after_attempt` kernel). Reconcile-on-start: unresolved
attempts → `swapinfo`/`listswaps` through the QUERY transport (positive
visibility settles committed; absence/lookup failure quarantines;
Unverified refund/claim quarantine until a terminal swap status is
observed). Deps: `adapters: Option<BoltzActionCapability>` (None
pre-cutover), query: `Arc<dyn BoltzCli>` (ProcessBoltzCli, production-
constructible), governor Option, store, clock. RED: rail/refusal/
quarantine/suspension/reconcile tests mirroring capital_owner.rs.

### Slice 6: 22 RPCs (7F) — revops

`rpc_boltz_ops.rs` (+ reconcile the duplicate builders: keep the
`revops_boltz::rpc` crate builders as the single source; the three
existing `rpc_boltz_*.rs` delegate or fold in; kill the hand-duplicated
`swap_created_ts`). All 22 names through `rpc_name("boltz-…")`.
Pre-cutover production: owner spawned with `adapters: None`, query
transport DISABLED config default → manager-backed RPCs return the
verbatim uninitialized arm; usage short-circuits fire first for
status/refund/claim; auto-cycle status/run-now return their Python
disabled shapes. backup/backup-verify: refuse pre-cutover (uninitialized
arm); mnemonic path only via `MnemonicSecret::into_rpc_value()` behind
the capability. Manifest 32→54 + names; action_surface: rpc_name-once
pins; scans: no ArmedBoltzCli/BoltzActionCapability/createswap literals
in production files (runtime.rs/lnplus_runtime.rs/main.rs).
RED: rpc tests + manifest e2e (call revenue-r-boltz-quote etc. after
init, assert exact arm) + count guard trip.

### Slice 7: E2E fake-executable proof (7G) + mutations + battery

E2E: full owner + REAL `ProcessBoltzCli`/`ArmedBoltzCli` (test-minted
capability) against fake executables: happy loop-out (attempt row →
spawn → committed → settled + journal + spend receipt), timeout →
quarantine (reservation held, pending gate blocks next, reconcile via
scripted swapinfo settles or keeps quarantine), refund Unverified →
quarantine. Mutation matrix B1–B12: B1 allowlist dropped (query transport
runs createswap); B2 output cap dropped; B3 tree-kill → child-kill only;
B4 timeout command unredacted; B5 attempt write moved after spawn; B6
unknown releases reservation; B7 pending-swap gate dropped; B8 second
settle allowed; B9 capability constructed in production (scan); B10
structural envelope fail-open; B11 cooldown not durable (restart resets —
kill via store-backed test); B12 Unverified classified as Committed.
Full battery (debug+release, doctests, clippy --all-features -D warnings,
fmt), report `/home/sat/agent-tasks/task-63-implementation-report.md`,
hexmem impl PASS, operator sign-off.

## Non-goals (disclosed)

- No live boltzcli/CLN contact anywhere; all proofs via fake executables.
- Governor/budget production impls and capability minting: Task 69.
- Treasury/balance recommendation ANALYTICS inputs (winners/losers-style
  evidence) stay honest-empty where their Python analyzers are Task 67
  scope; the cycle paths gate/skip exactly like the kernels dictate.
- Python's in-memory-only cooldown parity is deliberately IMPROVED
  (durable table) — divergence disclosed, matching the task contract's
  "durable … cooldown … state".
