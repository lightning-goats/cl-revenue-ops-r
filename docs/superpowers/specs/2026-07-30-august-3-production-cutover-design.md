# August 3 Production Cutover Design

**Date:** 2026-07-30  
**Target:** The Rust `cl-revenue-ops-r` plugin takes whole-plugin production authority by 2026-08-03.  
**Primary constraint:** Python behavioral and operator-contract parity comes before the authority handoff. The deadline does not permit split authority, fabricated evidence, placeholder success, or bypassing fail-closed cutover gates.

## Current Verified Boundary

The latest clean Task 71 checkpoint is `a60b651`, with 2,811 workspace tests passing, strict Clippy green, and fmt green. Task 67 implementation is marked pass, but its independent Codex review remains deliberately failed until correction Task 71 closes.

Task 71 is completing the Rust-owned analytics evidence promised by Task 67. Its active profitability slice replaces fabricated classifier inputs with intended-store evidence. The production-database portion is one typed actor command and one SQLite read transaction so all-time revenue, 30-day revenue, costs, routing history, and diagnostic history describe one database snapshot. Fee posterior evidence comes from the Rust fee-state observer store, and channel opener evidence comes from a fresh bounded `listpeerchannels` snapshot. Missing, malformed, or unavailable required evidence refuses explicitly.

The checked-in Python registry contains 69 canonical RPC methods. Current Rust `main.rs` declares 53 names. The exact 19 canonical names still absent on 2026-07-30 are:

- `revenue-ban`
- `revenue-capex-status`
- `revenue-cleanup-closed`
- `revenue-clear-reservations`
- `revenue-econ-cycle`
- `revenue-econ-reconcile`
- `revenue-fee-authority-status`
- `revenue-fee-cycle`
- `revenue-ignore`
- `revenue-profile-preview`
- `revenue-set-fee`
- `revenue-spend-release`
- `revenue-spend-release-stale`
- `revenue-spend-reserve`
- `revenue-spend-settle`
- `revenue-total-cost-budget`
- `revenue-unban`
- `revenue-unignore`
- `revenue-wake-all`

Three declared names are Rust-only or renamed relative to the Python registry: `revenue-ping`, `revenue-rebalance-plan`, and `revenue-fee-wake`. They do not substitute for any missing canonical method.

## Release Architecture

Use a compressed whole-plugin release train. Preserve the existing all-or-none `WholePluginLiveCapability` boundary and finish only work that removes a demonstrated parity, lifecycle, authority-handoff, verification, or rollback blocker.

The release train has four ordered integration stages:

1. **Analytics closure:** finish Task 71, create a clean checkpoint, and immediately run the reserved independent Task 67 review.
2. **Canonical RPC closure:** register all 69 exact names, bind every call through the checked-in parameter contract, wire result-bearing owners, and eliminate every canonical `not_yet_ported`, nullable required field, fabricated zero, and Rust-only success vocabulary.
3. **Lifecycle closure:** finish strict datastore producers, all four notification subscriptions, hydration and deferred cursors, current-boot startup receipts, bounded drain and owner joins, restart-safe cursor behavior, and database device/inode identity checks.
4. **Authority handoff:** implement the reviewed v2 whole-plugin preflight, single-use arm and nonce consumption, exact release/inventory/store binding, all-or-none capability construction, postflight, and ambiguity-aware rollback.

No subsystem receives production mutation authority before stage 4 completes. Observation and local-fake testing remain default-off with respect to live mutation.

## Work Allocation and Integration

The Rust worker owns the active Task 71 files until its coherent checkpoint. Codex continuously supervises the transcript, reviews live diffs and mutation evidence, and keeps Rust supplied with one serial Hexmem-backed task pointer at a time.

At the Task 71 checkpoint, Codex performs Task 67 review before taking an isolated, non-overlapping Task 66 RPC slice. The Rust worker takes the next release-critical slice whose files do not overlap Codex's active slice. Cargo tests, Clippy, fmt, and integration mutations remain serialized to avoid resource contention and ambiguous results.

Every checkpoint is small enough for an independent reviewer to reject without discarding neighboring work. Integration happens only from clean, test-backed commits. Unrelated cleanup, refactors, scheduled host checks, and non-Rust projects are deferred.

## Evidence and Error Semantics

Parity means the Rust response and side-effect contract matches Python, not merely that a method name exists.

- Required evidence is typed and source-identified.
- Consulted-and-empty is distinct from unavailable, malformed, stale, or unregistered.
- Logically coupled reads from one SQLite actor use one command and one transaction.
- Cross-store evidence cannot be atomic; it therefore carries explicit source identity and refuses on missing or invalid required components.
- Canonical mutators return completed durable outcomes, not queue admission.
- Observer runtime cannot construct live action capabilities.
- Missing evidence can reduce confidence or deny an action; it cannot authorize one.
- Unknown external outcomes retain reservations and quarantine and forbid blind retry.

## Verification Gates

Each implementation slice must pass:

1. red-first focused tests;
2. mutations that kill the placeholder, fallback, split-await, stale-boot, wrong-store, or admitted-not-completed regression relevant to that slice;
3. focused crate tests; and
4. a clean commit with no unrelated diff.

Each integration checkpoint must pass fresh, serialized:

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo fmt --all -- --check`
- exact equality with the 69-RPC and 121-option generated inventories;
- all eight loop identities bound to the current boot/session and release identity;
- scans and manifest tests proving no canonical placeholder or required null gap remains;
- local-fake tests proving every mutator uses the correct typed owner/capability and reports completion.

## August 2 Rehearsal Gate

The exact release binary must complete a no-live-mutation rehearsal against local fakes and temporary stores. The rehearsal proves:

- source commit, binary digest, embedded inventory digest, runtime configuration, observer database, production database identity, and nonce ledger are exactly bound;
- Python authority-off proof is positive, epoch-bound, and cannot be inferred from absence or a missing RPC;
- every current-boot loop and reconciliation prerequisite is complete;
- preflight parses without mutation, re-verifies immediately before consumption, and burns one fresh nonce exactly once;
- the whole capability set constructs all-or-none;
- postflight verifies canonical namespace ownership and store state;
- rollback destroys Rust mutation capability first, preserves ambiguous reservations and quarantine, verifies Rust authority absence, and only then restores Python authority;
- a failed or interrupted transition is safe to retry only with a fresh nonce and explicit operator approval.

## August 3 Production Gate

Production authority transfer requires all repository and rehearsal gates green on the exact installed artifact, a fresh production preflight, and explicit operator approval immediately before arm consumption. Deployment, Python shutdown, namespace transition, and authority acquisition are separate observable steps with a rollback check after each step.

If any hard gate is red, unknown, stale, or cannot be independently verified, the production cutover stops. The deadline is not evidence and cannot override a failed gate.

## Release Success Criteria

The release succeeds only when:

- all 69 canonical Python RPC names and parameter contracts are effective in Rust;
- canonical responses contain no placeholders, fabricated required values, or false-success vocabulary;
- all production loops, subscriptions, datastore producers, startup receipts, and shutdown drains are Rust-owned and current-boot verified;
- the exact release passes focused, mutation, workspace, inventory, rehearsal, preflight, postflight, and rollback gates;
- Rust holds the one whole-plugin live capability, Python mutation authority is positively proven off, and no split-authority interval exists; and
- the operator receives the final evidence bundle and confirms the production cutover.
