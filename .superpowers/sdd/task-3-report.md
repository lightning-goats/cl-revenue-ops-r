# Task 3 Report: Observer Runtime Framework and Persistent Loop Health

Task 57 was first implemented in `d7f227c` and reported in `d66779d`.
The review correction contract was then completed on
`codex/precutover-completion` in these committed checkpoints:

- `1f6ab3b` — amend the approved design for the correction contract.
- `e115d1d` — record the RED-first correction plan.
- `e02ac7b` — make suspension durable and correct F1, F6, and F8.
- `18ab9af` — bound every scheduler ingress path and correct F2.
- `3f8cb7e` — seal observer construction and post-start activation for F3/F5.
- `471d24f` — complete the deferred/superseded acknowledgement proof for F4.

## Scope and safety posture

- Python remains the sole mutation authority.
- No live CLN call, deployment, arming, authority transition, shadow restart,
  merge, or push was performed.
- Observer construction cannot receive an action adapter. A private
  `ObserverMode` is derived only from a validated passive or autonomous-shadow
  mode, and the production `ObserverPassSet` has private fields and accepts only
  concrete vetted observer passes.
- Only the fee observer pass is wired. Rebalance, planner, LN+, and Boltz remain
  durably `not_wired`; no success-shaped no-op owner was introduced.
- No Hexmem implementation or review criterion was marked. Independent
  owner/supervisor verification remains required before `impl`; Python alone
  owns the later `review` criterion.

## Review findings F1-F8

### F1 — durable suspension

`rust_loop_health` now persists `active`/`suspended`, suspension time, and a
bounded reason. Suspension retries until durable or until the store actor is
provably closed, takes precedence in the health RPC, and cannot be masked by a
late terminal pass. Current-boot ready registration explicitly reactivates a
wired loop while retaining suspension/error history.

Tests cover transient backpressure failure with an in-flight pass, suspension
without a running pass, retry/recovery, actor loss, restart persistence, and
late terminal completion.

### F2 — bounded scheduler ingress

The fee owner uses a fixed-capacity Tokio MPSC. Async producers await
`send`, the plain owner thread uses `blocking_recv`, and dedicated A3 callback
threads use `blocking_send`. The wake channel is bounded and coalescing; no
public sender bypasses the bounded surface. RPC closure is explicit, while
notifications and A3 callbacks backpressure and A3 delivery remains FIFO and
exactly once.

Tests saturate notification, RPC, cycle-ACK, wake, and A3 producer classes,
exercise owner loss, and structurally reject unbounded owner/wake ingress.

### F3 — structural observer/action isolation

`ObserverMode` is non-forgeable outside the validated mode conversion.
`ObserverPassSet` exposes only concrete vetted constructors; the generic fake
seam and `ObserverPass` trait are crate-private. `AuthorityPlan` invokes the
action factory only for live mode.

Compile-fail doctests reject forged observer tokens and arbitrary pass-set
construction. Panic-factory tests prove both observer modes leave the action
factory untouched, with a live exact-once positive control. Source scans remain
as defense in depth. These guarantees do not claim that the four deferred
subsystems are wired or vetted.

### F4 — completion acknowledgement matrix

Tests prove that an older deferred cycle receives an explicit superseded
`Err`, the newest receiver stays pending and then receives the real terminal
result, skipped and persistence-failed cycles never report success, and owner
loss closes a still-outstanding receiver.

### F5 — post-start cadence activation

Fee cadence construction is inert. `main` retains an activation handle and
activates only after `configured.start(state).await` succeeds. A paused-time
test observes generation zero before activation and generation one afterward.

### F6 — honest schema handling

Because the table has not shipped, the fabricated partial-schema ALTER path was
removed. Startup requires the exact canonical 15-column schema and rejects an
unsupported partial legacy table rather than manufacturing interrupted
generations.

### F7 — current-boot wiring design amendment

The approved design now distinguishes current-boot wiring from historical
terminal/error evidence. Exact current registration may change stale
`ready` to `not_wired`; historical completion and error state is retained.
This supersedes the earlier sticky-ready rule and preserves the stale-ready
regression test.

### F8 — single-terminal CAS

Terminal updates require `terminal_generation < generation`; a generation can
accept exactly one terminal result. Tests reject same-generation pass-to-error
and error-to-pass flips.

## Independent re-review corrections

The reopened independent review found four remaining capability-boundary gaps;
all are corrected in this checkpoint:

- Store-dispatch thread launch failure is now an explicit `Result` and never
  invokes a completion callback inline. The owner handles launch failure
  terminally in place, clears pending state, and counts persistence failure.
- Passive mode now carries a sealed `PassiveMode` capability which external
  callers cannot construct. Runtime startup also rejects a passive token paired
  with a fee pass before registration, reconciliation, or any store write.
- `SchedulerIngress::bounded_channel` is crate-private. The public test seam
  exposes only `A3ResultReceiver`, whose API can return only
  `InitialFeeStoreResult`, rather than a raw `Receiver<CycleMsg>`.
- The action-surface guard now parses the concrete public runtime start
  signature and requires both `mode: ObserverMode` and
  `passes: ObserverPassSet`, avoiding a vacuous whole-file substring check.

## RED and mutation evidence

Every mutation below was temporary, observed RED, and restored before the final
green runs.

- F1: removing the suspension write timed out the recovery test; removing RPC
  suspension precedence returned `passed` where `suspended` was required.
- F2: changing async owner admission to `try_send` broke saturation
  backpressure; changing A3 `blocking_send` to `try_send` broke FIFO/exactly-once
  delivery.
- F3/F5: eagerly requesting before activation advanced generation from 0 to 1;
  suppressing the live action-factory call tripped the live exact-once control.
- F4: dropping the old superseded handoff, newest deferred terminal handoff, or
  immediate terminal handoff produced `Closed` instead of the contracted
  result; mapping every terminal outcome to `Ok` made the skip/error test fail.
- Original lifecycle mutations remain covered: removing begin, finish, fail,
  coalesced, or dropped persistence writes fails the corresponding focused
  runtime assertion.
- Dispatch launch failure: making the injected spawner run the callback inline
  deadlocked the saturated-queue regression test; suppressing each owner's
  inline launch-error handler failed its terminal/no-pending-leak assertion.
- Passive sealing and raw receiver isolation: removing the passive-plus-fee
  rejection accepted the invalid pair and wrote state; widening
  `bounded_channel` made both the compile-fail doctest and action-surface guard
  fail.
- Public signature guard: renaming either concrete `start` parameter made the
  structural assertion fail.

## Focused GREEN evidence

- `cargo test -p revops-db --test loop_health` — 5 passed.
- `cargo test -p revops --lib runtime_tests` — 10 passed.
- `cargo test -p revops --test fee_scheduler` — 93 passed.
- `cargo test -p revops --test action_surface` — 8 passed.
- `cargo test -p revops --lib rpc_health::tests` — 11 passed.
- `cargo test -p revops --test manifest health` — 3 passed.
- `cargo test -p revops --doc` — 10 passed, including the compile-fail proofs.
- The real bounded owner queue was filled before injected dispatch launch
  failure; the result callback was never invoked inline and the test completed
  without deadlock.

## Full verification gates

- `cargo test --workspace --all-targets --quiet` — exit 0; every workspace
  target passed.
- `cargo fmt --all -- --check` — exit 0.
- `cargo clippy --workspace --all-targets -- -D warnings` — exit 0.
- `git diff --check` — exit 0.
- The isolated correction worktree passed every code gate before this report
  update; final cleanliness is checked after the report commit.

## Remaining limits

- Rebalance, planner, LN+, and Boltz are intentionally `not_wired` until Tasks
  4-7 add concrete vetted observer types.
- If loop-health persistence is unavailable, the runtime cannot promise a new
  durable marker; it retries until the actor is provably unavailable, returns
  failure, and never reports clean admission or healthy status.
- This correction checkpoint has not been deployed or exercised against a live
  node.
