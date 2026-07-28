# Task 3 Report: Observer Runtime Framework and Persistent Loop Health

Hexmem Task 57 was implemented on `codex/precutover-completion` in implementation commit
`d7f227c7b87dc7cecfe3495f8dbbf90946d29153`.

## Scope and safety posture

- Python remains the sole mutation authority.
- No live CLN call, deployment, arming, authority transition, or soak reset was performed.
- `AuthorityRuntime::Observer` cannot hold or construct an action adapter. Only
  `AuthorityRuntime::Live` contains `ClnFeeBroadcaster`.
- The production observer path spawns only the real fee pass. Rebalance, planner,
  LN+, and Boltz are durably registered as `not_wired`; there are no success-shaped
  no-op owners.
- Loop-health state is persisted only in the Rust-owned observer database.

## Implementation

- Added a migration-safe `rust_loop_health` store with the exact five loop
  identities, current-boot wiring registration, generation-CAS begin/terminal
  writes, bounded error history, durable backpressure counters, and restart
  reconciliation based on generation rather than timestamp ordering.
- Added a bounded single-flight runtime: one active request, eight distinct pending
  request keys, duplicate coalescing, ninth-key dropping, fail-closed persistence,
  panic capture, and suspension after unrecordable lifecycle writes.
- Added structural `AuthorityRuntime::{Observer, Live}` separation. Observer runtime
  stores loop handles only; the live runtime alone stores the broadcaster.
- Added a real fee completion acknowledgement. `RunPrepared` now acknowledges only
  an actual `CycleOutcome::Ran`; disconnected, skipped, deferred/superseded, and
  error outcomes fail the pass.
- Routed the production autonomous fee cadence through the bounded loop owner with
  the legacy fee owner in external-trigger-only mode. No other subsystem pass is
  inserted in this checkpoint.
- Added health RPC loop inventory with wiring, generation, terminal generation and
  status, timestamps/history, error, coalesced, and dropped fields. A loop-store
  read failure is section-local and does not fabricate healthy rows.
- Updated the port checklist to record the real fee-only checkpoint and four
  intentionally deferred loop owners.

## RED evidence

The tests were written and observed failing before implementation.

1. `cargo test -p revops-db --test loop_health`
   - RED: unresolved `revops_db::loop_health` and missing observer actor methods.
2. `cargo test -p revops --test runtime`
   - RED: unresolved `revops::{loop_health, runtime}` and DB loop-health module.
3. `cargo test -p revops --test fee_scheduler run_prepared_acknowledges_only_after_real_owner_outcome --no-run`
   - RED: missing `spawn_owner_for_runtime`; `CycleMsg::RunPrepared` had no
     completion acknowledgement.
4. `cargo test -p revops --lib rpc_health::tests::durable_loop_rows_replace_only_the_loops_gap --no-run`
   - RED: missing `build_health_with_loops`.
5. Supervisor regressions were also observed RED before correction:
   - same-second restart reconciliation missed an unmatched generation;
   - current-boot registration retained stale `ready` instead of `not_wired`;
   - begin-persistence failure did not account for the abandoned request;
   - same-second active health was incorrectly reported as passed;
   - terminal kind was not represented independently from timestamps.

## Mutation safety proofs

Each mutation was temporary, its focused test was observed RED, and the source was
restored before the final green run.

- Removed the begin write; `owner_is_single_flight_coalesces_duplicates_and_drops_ninth_pending_key`
  failed because generation remained `0` instead of `1`.
- Removed the finish write; the same test failed because finish-write count was `0`
  instead of `9`.
- Removed the fail write; `error_panic_and_later_generation_are_distinguished`
  failed because `last_error` was absent.
- Removed the coalesced-counter write; the single-flight test failed with
  coalesced total `0` instead of `1`.
- Removed the dropped-counter write; the single-flight test failed with dropped
  total `0` instead of `1`.
- Restoration proof: `cargo test -p revops --test runtime -- --nocapture` passed all
  6 runtime tests.

These mutations demonstrate that required lifecycle and backpressure writes are
asserted behavior, not incidental code coverage.

## Focused GREEN evidence

- `cargo test -p revops-db --test loop_health` — 3 passed.
- `cargo test -p revops --test runtime` — 6 passed.
- `cargo test -p revops --test fee_scheduler run_prepared_acknowledges_only_after_real_owner_outcome` — 1 passed.
- `cargo test -p revops --test action_surface` — 5 passed.
- `cargo test -p revops --lib rpc_health::tests` — 10 passed.
- `cargo test -p revops --test manifest health` — 3 passed.

The action-surface tests prove that the observer-side source region contains none
of `ClnFeeBroadcaster`, `PaymentMode::Live`, or `ExecutionMode::Armed`, and that the
production pass registry contains exactly one insertion: fee.

## Full verification gates

- `cargo test --workspace --all-targets` — exit 0; every workspace target passed.
- `cargo fmt --all -- --check` — exit 0.
- `cargo clippy --workspace --all-targets -- -D warnings` — exit 0.
- `git diff --check` — exit 0.
- `git diff --cached --check` before the implementation commit — exit 0.

## Remaining limits and concerns

- Rebalance, planner, LN+, and Boltz are intentionally `not_wired` until Tasks 4–7.
  This is an explicit checkpoint boundary, not a hidden healthy state.
- If the loop-health persistence service itself is unavailable, the owner cannot
  guarantee a durable abandoned/backpressure counter. It returns an error, suspends
  the loop, and logs the secondary persistence failure rather than reporting clean
  admission or success. An already persisted unmatched begin remains available for
  restart reconciliation.
- This work has not been deployed or exercised against a live node.
