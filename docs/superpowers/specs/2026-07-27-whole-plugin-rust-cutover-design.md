# Whole-Plugin Rust Port and Cutover Design

**Status:** Operator-approved design, written from the 2026-07-27 Option 1
decision.

**Goal:** Replace the Python `cl_revenue_ops` plugin with the Rust plugin as a
whole. Construction and verification are staged by subsystem, but mutation
authority changes in one coordinated whole-plugin cutover after every required
path is wired and green.

## Direction and scope

The former fee-only cutover plans are implementation evidence, not the current
programme boundary. The final authority handoff includes:

- fee decisions and `setchannel` execution;
- governed economics, budget reservation, settlement, and reconciliation;
- profitability, policy, configuration, and operator RPCs;
- rebalance planning and payment execution;
- capacity planning, channel opens, closes, and defibrillation;
- Boltz management and autonomous cycles;
- LN+ evaluation, lifecycle watching, opening, withdrawal, rating, and
  reconciliation;
- all background loops, notification producers, persistence, retention, and
  status surfaces needed by those features.

Read-only Rust surfaces may be deployed before cutover. No Rust subsystem may
receive mutation authority early merely because it is individually complete.
The existing fee candidate remains useful shadow evidence but is not a separate
production cutover without a new operator decision.

## Non-negotiable invariants

1. Python remains the sole mutation authority until the coordinated cutover.
2. Rust shadow processes have no mutation-capable construction path.
3. The Python production database remains read-only from Rust. Rust-owned state
   lives in the Rust observer database until the final migration design names a
   different owner explicitly.
4. Every money-moving operation is authorized by the governed-economics and
   budget rails before submission.
5. Ambiguous post-submission outcomes are quarantined and reconciled. They are
   never classified as clean failures and never retried automatically.
6. LN+ terminal/idempotent HTTP outcomes use structured status-and-error
   matching. Free-text substring matching cannot advance lifecycle state.
7. A restart cannot reacquire live authority. Cutover consumes a short-lived,
   exact-binary, exact-node arm.
8. A missing dependency, stale evidence source, failed persistence write, or
   incomplete reconciliation denies action.
9. The 24-hour fee soak is an explicit operator waiver for that candidate. It
   does not revise the 72-hour design gate for a decision, state, execution,
   authority, or timer change.
10. No module is complete merely because its crate tests pass.

## Honest progress model

Programme reporting uses reachable entry points, not ported lines of code.
Each feature is tracked through five independently evidenced states:

1. **Compiled:** its module is declared and built by the workspace.
2. **Reachable:** its RPC is registered, notification producer is connected, or
   loop is spawned by the plugin entrypoint.
3. **Effective:** the reachable caller invokes the intended decision or state
   kernel and a revert-discriminating test fails when that invocation is
   removed.
4. **Transport-proven:** external calls run against a sandboxed fake boundary
   with exact request, timeout, ambiguity, and recovery assertions.
5. **Promotion-ready:** independent review and required shadow/runway evidence
   pass with zero unauthorized mutation calls.

The supervisor reports these counts separately:

- declared and compiled modules;
- registered operator RPC methods out of the Python surface;
- spawned production-equivalent loops;
- reachable live call sites by action type;
- promotion-ready subsystems.

The parity checklist is not allowed to mark a row complete from source presence
or LOC alone.

## Programme decomposition

### Wave 1: Complete the shared foundations

Two non-overlapping lanes run in parallel.

**CapacityPlanner lane**

- Port the five candidate-discovery strategies.
- Port winner and loser classification as typed evidence-to-decision kernels.
- Port candidate enrichment and scoring.
- Port the `execute_cycle` orchestration without live mutation capability.
- Generate expectations by running the unmodified Python implementation.
- Add revert-discriminating tests at each real caller.

This lane owns `revops-capital`. It produces the planner evidence and plan types
needed by LN+, Boltz, planner RPCs, and capacity reports.

**Database and policy lane**

- Add typed read queries for policy-manager list/get/find/change/tag data.
- Add hot-channel protection override reads.
- Add spend-event and spend-reservation aggregate reads.
- Independently review and adopt the newly landed
  `revops_lnplus::sqlite_db::SqliteLnPlusDb`; do not duplicate its LN+ swap,
  peer, breaker, planner-action, or budget-rail storage in `revops-db`.
- Pin the LN+ store to a Rust-owned database path and test cross-connection
  lock, busy-timeout, transaction, and restart behavior before plugin wiring.
- Pin any shared Python/Rust wire representation with migration and round-trip
  tests before a caller depends on it.

This lane owns `revops-db` and the integration review of the crate-local LN+
store. It does not register RPCs or implement planners, so it can merge
independently of the CapacityPlanner lane. The crate-local LN+ schema remains
where it landed unless review finds a concrete safety or ownership violation;
moving it merely for architectural symmetry is out of scope.

### Wave 2: Make existing code reachable

- Register the compiled read-only RPC builders in `main.rs` after their required
  database reads exist.
- Replace declared-gap fields only when the real evidence source is wired.
- Add a manifest-level parity test proving every intended RPC builder is both
  declared and registered.
- Add observer-only CapacityPlanner, LN+, Boltz, and rebalance loop owners.
- Give each loop persistent health, last-pass, error, and dropped-work evidence.
- Prove a removed spawn call or registration makes a targeted test red.

Read-only registration may merge ahead of loop completion. Loop code remains
incapable of constructing action adapters.

### Wave 3: Prove external boundaries without live effects

- Exercise `ProcessBoltzCli::run()` with a sandboxed fake executable, including
  stdout, stderr, nonzero exit, timeout, child termination, and an
  ambiguous-create outcome.
- Exercise LN+ HTTPS and `signmessage` integration with a local fake server and
  fake CLN RPC socket.
- Exercise rebalance `sendpay`/`waitsendpay`, planner `fundchannel`/`close`, and
  fee `setchannel` request construction through fake RPC sockets.
- Verify budget reservation happens before submission and every determinate
  exit settles or releases it exactly once.
- Verify unknown outcomes retain reservations or quarantine as required and
  cannot be retried by a later loop.
- Persist journal and reconciliation evidence atomically.

No test in this wave contacts a live node, LN+, or Boltz service.

### Wave 4: Whole-plugin shadow and runway

- Deploy the runway automation, durable reports directory, and host lifecycle
  configuration required by the existing design.
- Run all production-equivalent loops with action adapters structurally absent.
- Compare Rust decisions, state transitions, budgets, and summaries against
  Python for the same frozen evidence windows.
- Require one binary identity throughout each evaluated window.
- Reset the design soak clock after any decision, state, execution, authority,
  or timer-gate change.
- Treat mutation calls, quarantine, stale provenance, non-advancing state,
  false-clean summaries, or unexplained missing loop passes as RED.

### Wave 5: Coordinated whole-plugin authority handoff

The release artifact contains one whole-plugin authority arm binding:

- node identity;
- source commit and binary hash;
- release identifier;
- exact subsystem set;
- mode;
- not-before and expiry times;
- one-time nonce.

Startup validates all subsystem state, reconciliation, budgets, loop readiness,
and Python-authority readback before consuming the arm. If any subsystem fails,
none receives mutation authority. The handoff sequence is:

1. capture final Python and Rust state/evidence snapshots;
2. stop or disable every Python mutation path;
3. independently confirm Python authority is off;
4. start the exact Rust release with the whole-plugin arm;
5. validate all gates and consume the arm atomically;
6. confirm Rust owns every expected RPC, loop, and mutation surface;
7. monitor the bounded post-cutover window with an immediate rollback trigger.

Rollback stops Rust mutation authority first, preserves all ambiguous-operation
evidence, reconciles external state, and only then restores Python authority.
The same arm cannot be reused after rollback or restart.

## Test and review gates

Every implementation task follows red-green-refactor and ends with:

- focused tests, including a test observed failing before production code;
- a revert-discriminating caller or registration tripwire;
- relevant Python-oracle fixtures;
- crate and workspace tests;
- `cargo fmt --all -- --check`;
- `cargo clippy --workspace --all-targets -- -D warnings`;
- `git diff --check` and an exact changed-file inventory;
- independent review by an agent other than the owner.

Tier-1 work includes all state, execution, database schema, scheduler, authority,
and production-equivalent changes. The owner marks implementation criteria only;
the verifier alone marks review.

## Agent topology and capacity planning

The supervisor is both project manager and an implementation owner. Work is
divided by file ownership so parallel agents do not collide:

- Rust worker: CapacityPlanner and later subsystem loop orchestration.
- Codex supervisor: `revops-db` read-query foundation and independent review of
  the landed LN+ SQLite boundary, followed by an independently assigned
  integration lane.
- Independent verifier: re-derives parity and safety claims before merge.

Current measured worker tasks consume about 310k tokens each, with a 242k-387k
range and 17-35 minute wall time. Before dispatch, the supervisor queries each
worker's current usage and remaining capacity. Tasks are sized as one reviewable
subsystem deliverable, not as broad multi-crate requests. Workers proactively
notify the supervisor when implementation or review is complete.

Task 46 estimated 9-11 subsystem-sized implementation tasks at `0eba911`,
excluding independent verification. The LN+ wiring task landed afterward, so
the live estimate is 8-10 tasks until the planner and database lanes produce a
more exact dependency inventory. Three or four isolated worktrees may run in
parallel only when their changed-file sets do not overlap.

## Integration and publication

- Each lane commits a green logical checkpoint on its own branch.
- The verifier reviews the actual diff and reruns the relevant gates.
- The supervisor merges only reviewed commits and reruns the combined workspace
  gates.
- The parity checklist is updated in the same checkpoint that makes an entry
  point reachable.
- Releases and pushes use the `santyr` identity and occur only from a clean,
  reviewed branch with public-safety checks green.
- The public Hexmem publication freeze does not block pushes to this existing
  Rust repository, but no Rust artifact may contain private operational data.

## Baseline snapshot

At the start of this design pass, `main` was `0eba911`, the workspace had 1,986
passing tests, 10 of 69 Python RPCs were registered in Rust, 27 RPC builders
compiled, and CapacityPlanner had 2,031 Rust source lines versus 3,977 Python
source lines. These are snapshot measurements, not permanent assertions; future
status reports must recompute them. Before written-spec review, Rust advanced
`main` to `69a0a1c` with LN+ transport traits, a crate-local SQLite store, gated
evaluator/watcher pass drivers, and sandboxed transport tests. It still did not
register the LN+ RPCs or spawn those loops from the plugin entrypoint.
