# Task 53 - Failed-Forward Backpressure and Atomic Receipt Report

## Scope and checkpoint

- Hexmem task: 53
- Owner: Codex
- Independent verifier: Python
- Base: independently reviewed Task 44 checkpoint dca2098
- Scope: A1 failed-forward handling only
- Excluded: A3 behavior, docs/port/PARITY-CHECKLIST.md, live CLN/node/network contact
- Production source hash after mutation restoration: 3be93ec7fcc77b91c5bb8f716a567c4a121b7aac1b5a7617c41e71577000feae

## Defect

CycleOwner::handle_failed_forward previously called apply_failure_nudge before
TriggerQueue::offer. A full bounded queue therefore returned Dropped only after
the owner posterior and rate-limit timestamp had already changed. The APPLIED
receipt was then written through the separate record_trigger_event path, so a
receipt failure could leave an unreceipted effect and a state persistence
failure could not roll the effect back.

## Implemented boundary

1. Offer FeeTrigger::FailedForward before any nudge guard, entropy, or state work.
2. Dropped returns with a truthful no-effect receipt.
3. Once one pending same-channel occurrence commits a nudge, later Coalesced
   occurrences remain zero-effect until the scheduled cycle drains the trigger.
   If the earlier pending occurrence was skipped or its commit failed, a later
   occurrence may still become that pending channel's first committed nudge.
4. Derive a stable SHA-256 event key from length-prefixed channel, amount,
   failure identity, and event timestamp fields. An already committed key is a
   duplicate with zero effect across owner restart.
5. Evaluate the nudge against a cloned ChannelFeeState.
6. Persist the cloned state row and the APPLIED trigger receipt together in one
   FeeCycleCommit transaction. Only a successful commit installs the clone,
   advances last_failure_nudge_ts, records the pending-channel latch, and adopts
   the returned generation.
7. Missing store, failed idempotency read, missing paired cycle state, or failed
   commit installs no state and emits no APPLIED receipt. A post-rollback
   failure receipt is allowed because it explicitly says the nudge was not
   installed.
8. The normal cycle drains the trigger queue and pending-channel latch together.
   A doc-hidden integration-test seam performs those same two operations so the
   existing 1,800-second cooldown-boundary test measures the cooldown rather
   than a still-pending trigger.

The database loader reads every current channel row, not only rows stamped with
the newest generation, so a one-channel out-of-cycle commit does not hide the
other persisted channels on restart.

## Red-first evidence

Before the production fix:

    cargo test -p revops --test fee_scheduler failed_forward -- --nocapture

Result: 0 passed, 5 failed. The failures demonstrated all required defects:
Dropped mutated state; a Coalesced occurrence refreshed the nudge; accepted
events created no atomic cycle/generation; injected commit failure left the
in-memory mutation; restart replay had no durable event identity.

After implementation, the same focused suite passed 5/5.

The first complete scheduler run exposed one pre-existing test that combined two
independent gates: it expected an exactly-1,800-second occurrence to fire while
the first trigger was still pending. Task 53 makes pending Coalesced occurrences
authoritative zero-effect. The test now drains the pending trigger exactly as a
scheduled cycle does before checking the cooldown boundary. It still proves the
boundary fires and refreshes the posterior timestamp.

## Mutation evidence

Temporary mutation:

    let queue_outcome = TriggerOutcome::Enqueued;

This bypassed TriggerQueue::offer. The saturated-queue regression turned RED
and showed a real posterior_bias entry plus changed posterior mean despite the
queue being full. The mutation was then removed.

- Pre-mutation source SHA-256:
  3be93ec7fcc77b91c5bb8f716a567c4a121b7aac1b5a7617c41e71577000feae
- Post-restoration source SHA-256:
  3be93ec7fcc77b91c5bb8f716a567c4a121b7aac1b5a7617c41e71577000feae
- Restored regression: 1 passed, 0 failed

No mutation is present in the checkpoint.

## Verification

- Focused Task 53 regressions: 5 passed, 0 failed
- Full fee_scheduler integration suite: 82 passed, 0 failed
- Structural action surface: 3 passed, 0 failed
- cargo test --workspace --quiet: 2,250 passed, 0 failed, 2 ignored
- cargo test --workspace --release --quiet: 2,251 passed, 0 failed, 2 ignored
- cargo fmt --all -- --check: pass
- cargo clippy --workspace --all-targets -- -D warnings: pass
- git diff --check: pass
- Live/node/network contact: none

## Files

- crates/revops/src/fee_scheduler.rs
- crates/revops/tests/fee_scheduler.rs
- docs/superpowers/plans/2026-07-27-failed-forward-atomic-receipt.md
- crates/revops/TASK53-REPORT.md
