# Failed-Forward Atomic Receipt Implementation Plan

**Goal:** Make A1 failed-forward nudges obey bounded-trigger backpressure and persist an effect-claiming receipt atomically with the exact durable posterior state.

**Safety boundary:** Rust-owned observer state only. No CLN socket, live node, network, Python DB write, fee broadcast, or A3 semantic change. docs/port/PARITY-CHECKLIST.md remains Task 52's scope.

## Design

- Offer FeeTrigger::FailedForward before any guard/effect/state work.
- Dropped occurrences return before evaluation. A Coalesced occurrence returns with zero effect once the pending channel has committed a nudge; if the earlier occurrence was skipped or failed, one later occurrence may still become the pending channel's first committed nudge.
- Derive a stable event key from the signal's channel, amount, failure identity, and event timestamp.
- Treat an already-committed event key as a duplicate with zero effect.
- Stage the target channel's cloned ChannelFeeState; do not mutate owner state yet.
- For an applied nudge, commit one FeeCycleCommit containing the serialized state row and its effect receipt in the same transaction.
- Install staged state and advance the in-memory rate-limit timestamp only after that commit succeeds.
- On missing store, missing paired cycle state, or persistence failure, install nothing and never emit an APPLIED receipt.

## TDD sequence

- [x] Add red tests for saturated-queue byte identity, coalesced/replayed zero duplication, atomic persistence failure, and ordinary exactly-once success.
- [x] Run the focused scheduler tests and capture the pre-fix failures.
- [x] Implement the minimum offer-first, stage-then-commit correction.
- [x] Re-run focused tests green; mutation-test offer ordering, with the initial red commit-failure test proving the install-before-commit defect.
- [x] Run fee scheduler, action-surface, workspace debug/release, fmt, clippy with warnings denied, and diff-check gates.
- [x] Prepare one scoped checkpoint and its independent-review handoff; the commit and criterion write follow this plan artifact.
