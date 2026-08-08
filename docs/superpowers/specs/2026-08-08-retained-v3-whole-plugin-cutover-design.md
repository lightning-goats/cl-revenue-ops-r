# Retained-v3 Python-to-Rust Whole-Plugin Cutover Design

**Date:** 2026-08-08
**Status:** Operator-approved direction; written design pending operator review
**Supersedes for future implementation:** the retired-authority portions of
`2026-07-27-whole-plugin-rust-cutover-design.md`,
`2026-07-30-whole-plugin-cutover.md`, and
`2026-08-01-whole-plugin-authority-handoff-v2-design.md`

## 1. Goal and boundary

Replace Python `cl_revenue_ops` v3 with Rust as one drop-in plugin while
preserving the exact retained public contract and shrinking mutation authority
to the smallest set still required.

The canonical release exposes exactly the generated 39-method Python v3 RPC
set. Rust-only diagnostics stay outside the `revenue-*` namespace. Removed
Boltz, LN+, CapacityPlanner, automatic channel open/close, and planner
defibrillation names remain physically absent from source, manifests, options,
owners, transports, schedules, and capability sets.

Rust retains:

- fee observation, decisioning, and governed `setchannel` execution;
- revenue, profitability, flow, history, dashboard, and health reporting;
- configuration, policy, ignore/ban, generic `no_close`, and spend-ledger
  administration;
- capex and total-cost accounting without planner execution;
- governed ordinary circular rebalancing with atomic budget reservation,
  settlement, reconciliation, and unknown-outcome quarantine;
- the five v3 runtime owners: fee adjustment, rebalance check, flow analysis,
  startup snapshot, and financial snapshot.

Historical planner, Boltz, LN+, open, and close rows remain readable and
non-authorizing. Retired RPCs receive normal method-not-found responses.

## 2. Non-negotiable invariants

1. Python remains the sole production mutation authority until one explicitly
   approved coordinated cutover.
2. Rust shadow mode cannot construct, deserialize, clone, or retain any
   mutation capability or live action adapter.
3. There is never an interval where Python and Rust both hold mutation
   authority. A bounded neither-authoritative interval is the safe failure
   direction.
4. Canonical RPC registration is not authority. Before activation, every
   mutation handler returns a typed `authority_unassembled` denial without
   reserving budget, changing state, or contacting an external transport.
5. Every fee or rebalance action passes current authority, policy, governor,
   budget, reservation, freshness, idempotency, and owner-serialization gates.
6. Ambiguous post-submission outcomes retain reservations and enter quarantine.
   They are never treated as clean failures or automatically retried.
7. Read-only reporting and diagnostics never clean, settle, release, migrate,
   or otherwise mutate state.
8. A restart never reacquires live authority. Every live start requires a new
   short-lived arm, a new Python release receipt, and explicit operator
   approval.
9. Missing, malformed, stale, partial, or unverifiable evidence denies. No
   `unwrap_or_default`, fabricated zero, or success-shaped placeholder may
   satisfy a cutover gate.
10. No Sling, Hive, Mycelium, fleet coordinator, Boltz, LN+, or channel
    lifecycle dependency may enter the release.

## 3. Honest readiness model

The generated inventory remains the source of truth. Each of the 39 canonical
RPCs, five loops, and every retained external boundary is tracked through:

1. compiled;
2. reachable;
3. effective with a revert-discriminating test;
4. transport-proven through a local fake where an external boundary exists;
5. independently reviewed;
6. soaked where runtime behavior is recurring;
7. promotion-ready.

Cutover requires all 39 canonical methods to be full and independently
reviewed, all five loops to be effective and independently reviewed, every
required transport to be local-fake proven, and every promotion gate to be
green. Merely registering the exact method set is insufficient.

The inventory generator must classify every former Python boundary either:

- `retained_required`, with a concrete Rust adapter and transport proof; or
- `retired_or_replaced`, with a source-derived explanation and a negative
  reachability test.

No boundary may remain `missing` at promotion.

## 4. Runtime modes and namespace transition

There are exactly three modes:

| Mode | Namespace | Mutation capability | Purpose |
|---|---|---|---|
| passive observer | `revenue-r-*` plus sentinel | structurally absent | read-only inspection |
| autonomous shadow | `revenue-r-*` plus sentinel | structurally absent | full decision/state runway |
| canonical inert/live | exact 39 `revenue-*` plus sentinel | initially absent; may activate once | replacement process |

`revops-handoff-status` is a permanent non-colliding, read-only sentinel in
all modes. It reports release identity, mode, prepared-owner state, gate
verdicts, observed receipt epoch, nonce state, and capability state without
performing a write.

CLN fixes RPC names during `getmanifest`; therefore Rust does not attempt
dynamic registration after arming. The transition is:

1. The exact Rust candidate completes shadow runway under `revenue-r-*`.
2. The operator authorizes a cutover window and a v2 arm nonce.
3. Python disables every retained mutation owner and writes a positive handoff
   receipt while still running.
4. Python stops, releasing the canonical namespace.
5. The shadow Rust process stops without altering its evidence stores.
6. The exact same Rust binary starts in canonical mode. It registers all 39
   names but remains inert.
7. Rust prepares every store and owner in non-live form, performs strict
   preflight and immediate re-verification, durably burns the nonce, consumes
   the arm, and installs the complete capability set in one non-fallible move.
8. Strict postflight either proves live ownership or destroys Rust authority
   and enters rollback.

The bounded interval between steps 3 and 7 has no mutation authority. This is
intentional. Canonical name presence, Python process absence, or an RPC
method-not-found response is never evidence that Python authority is off.

## 5. Positive Python handoff receipt

Python writes one `authority_handoff` row in the production database inside
`BEGIN IMMEDIATE` after its mutation gate is disabled and before it stops.
The receipt contains:

- monotonic `epoch`;
- node identity;
- `released_at` and Python release identity;
- literal state `python_authority_off`;
- the v2 arm nonce binding;
- exact retained subsystem-set digest;
- Python source commit and entrypoint digest;
- production DB, Rust observer DB, and nonce-ledger `(device, inode)`
  identities;
- a canonical final-state snapshot digest covering configuration, policy,
  fee state, budget/reservation state, reconciliation state, and every other
  retained mutable input Rust must adopt;
- latest known ambiguous-operation and active-reservation counts;
- a canonical SHA-256 digest over every preceding field.

The Python gate is fail-closed: if receipt persistence fails, Python remains
disabled but does not stop automatically. A receipt is single-attempt evidence;
rollback or retry requires a newer epoch and a fresh nonce.

## 6. Embedded release manifest

The clean Rust binary embeds a `RetainedV3ReleaseManifest` containing:

- exact source commit and source-tree digest;
- running binary SHA-256;
- generator version and inventory digest;
- RPC parameter-contract digest;
- exact sorted 39-method canonical set;
- exact five-loop set;
- exact retained external-boundary classifications;
- exact sealed mutation-owner set;
- a negative retired-authority inventory digest;
- observer and production schema versions.

Dirty builds cannot produce an armable manifest. Runtime recomputes the binary
digest and compares every embedded contract to the arm.

### 6.1 Store ownership and state adoption

Rust never writes Python's production database. That database remains a
read-only historical and handoff-receipt source before, during, and after
cutover. Rust authoritative state lives in the existing Rust-owned observer
database, promoted to the `rust_authority_store` role only after activation.

After Python stops and before nonce consumption, canonical-inert Rust imports
the receipt-bound final-state snapshot into a new, uncommitted authority
generation in the Rust store. Import is idempotent by `(receipt_epoch,
snapshot_digest)`, may perform all required schema and integrity checks, and
may fail without consuming the arm. Preflight verifies that the complete
generation is present, byte-bound to the receipt, and not yet active.

The imported generation is committed as `prepared` and bound to the arm nonce
before consumption. That durable status is never authority by itself.
Activation selects the prepared generation only inside the in-memory
capability-bearing owner set; a crash cannot reconstruct that selection. The
first capability-protected postflight write records the live session. If that
write fails, Rust destroys the capability and rolls back with the nonce still
burned. No Python database handle changes posture, and no schema migration or
connection open occurs after nonce consumption. Read RPCs combine immutable
Python history with post-cutover Rust events using an explicit epoch boundary
so rows are neither lost nor counted twice.

## 7. Whole-plugin capability decomposition

One non-cloneable `WholePluginLiveCapability` owns the transition proof. It is
unconstructible outside its module and is produced only from a consumed v2 arm.
It contains three mutation sub-capabilities and no others:

1. `FeeMutationCapability`: authorizes only governed `setchannel` batches.
2. `CoreStateMutationCapability`: authorizes configuration, policy,
   ignore/ban, generic policy tags, reservation lifecycle, cleanup, and other
   exact retained database mutations.
3. `RebalanceMutationCapability`: authorizes only governed circular-rebalance
   preparation, Askrene layer operations required by that implementation,
   invoice/payment submission, wait/reconcile, and determinate cleanup.

Reporting, profitability, flow analysis, capex reporting, and history receive
read-only store handles, never a live capability. Datastore publication uses a
separate bounded publication handle that cannot invoke payment, fee, policy,
or reservation operations.

The capability and all three sub-capabilities are not `Clone`, `Copy`,
`Default`, `Serialize`, or `Deserialize`. Production constructors require the
parent capability by value; test constructors exist only under `cfg(test)` and
cannot compile into the release.

Every executable batch carries a canonical digest over its exact ordered
actions, evidence epoch, budget reservation IDs, and owner generation. A typed
authorization receipt binds that digest. Owners reject altered, reordered,
duplicated, stale, or partially authorized batches before transport contact.

## 8. Prepare, preflight, re-verify, burn, consume, activate

### 8.1 Prepare without authority

Canonical startup opens the production database read-only, opens the Rust-owned
store, validates schemas, imports the receipt-bound inactive authority
generation, constructs read-only evidence providers, and builds a
`PreparedOwnerSet`. Preparation may fail without burning the nonce.

Prepared owners contain no mutation capability. External connections may be
prepared only as private `DormantTransport` values: they expose no request
method, cannot be constructed in either observer mode, and can become callable
only when moved together with the matching typed sub-capability during final
activation. The raw client/socket is private to the transport module and has no
other accessor.

The process executes one no-action readiness pass for each of the five owners.
These passes prove current-process construction and evidence completeness; the
72-hour shadow attestation proves recurring behavior for the exact binary.

### 8.2 Strict preflight

One immutable snapshot evaluates:

- release, binary, inventory, RPC contract, five-loop, and retired-set digests;
- exact 39-method namespace and absence of Rust-only collisions;
- positive Python handoff receipt digest, epoch, nonce, and subsystem binding;
- production, observer, and nonce-ledger file identities;
- read-only production posture before activation;
- observer integrity, boot identity, and exact-binary runway attestation;
- all five current-process readiness passes;
- full/effective/reviewed inventory state with no placeholder marker;
- all required transport proofs;
- zero ambiguous fee/rebalance outcome, zero execution quarantine, and no
  unexplained active reservation;
- budget and governor reconciliation;
- valid, unconsumed, mode-0600, owner-matched, unexpired v2 arm.

Every gate returns a typed verdict with measured evidence. Preflight performs
no write.

### 8.3 Immediate re-verification

Immediately before consumption Rust re-reads every drift-prone input: receipt,
store identities and posture, current-process passes, reconciliation,
reservation/quarantine counts, nonce ledger, and arm timing. Failure leaves the
nonce unburned and all owners inert.

### 8.4 Burn and consume

Rust durably inserts and `fsync`s the nonce deny-ledger before atomically
renaming the arm into its consumed location. A crash at either boundary leaves
the arm inert and requires a fresh nonce.

### 8.5 Infallible activation

Every fallible resource is already present in `PreparedOwnerSet`. Consuming the
arm produces the one parent capability; activation moves the prepared owners
and their exact typed sub-capabilities into one `LiveOwnerSet` without I/O,
allocation, parsing, lookup, or external contact. The runtime swaps the complete
set into one write-once cell and selects the receipt-bound prepared generation
in memory. No durable row, flag, or restart path can recreate the capability.
There is no partial live owner state.

## 9. Rebalance transport safety

Ordinary rebalancing is the largest remaining replacement blocker. Promotion
requires local-fake transport tests through the same production adapters for:

- route/layer construction and cleanup;
- invoice creation and deletion;
- `sendpay` plus `waitsendpay`;
- `listsendpays` reconciliation and determinate `delpay` cleanup;
- timeout after submission, disconnect, malformed result, duplicate request,
  partial-amount ladder, and restart recovery;
- budget reserve before submission; settle exactly once on proven completion;
  release exactly once on proven absence; retain on ambiguity;
- owner serialization and duplicate-destination suppression.

An unknown submission outcome suspends the owner for that intent, preserves the
reservation, and requires reconciliation. Neither a timeout nor an RPC error is
proof that payment did not start.

Removed LN+/Boltz corrections in legacy Task 80 are discarded. Its retained
rebalance unknown-outcome and pending-settlement paging corrections remain
required and receive a narrowed task before implementation.

## 10. Shadow and promotion gates

The exact release binary must run autonomous shadow for at least 72 continuous
hours after the last decision, state, transport, scheduler, authority, or timer
change. A lightningd restart or candidate binary change resets the window.

The shadow candidate must prove:

- all five loops advance under the current boot and expected cadence;
- exact frozen-evidence decision parity for fees and rebalancing;
- all 39 read/refusal contracts match Python v3 without success-shaped gaps;
- mutation-call count, live fee broadcast count, live payment count,
  quarantine, and persistence failures remain zero;
- one binary and one observer-store identity across the window;
- restart-safe observer restoration is fail-closed and never starts live mode.

Promotion additionally requires independent review by an agent other than the
implementation owner, a clean worktree, full debug and release workspace tests,
strict clippy, formatting, generated-inventory checks, security scanning, and a
rollback rehearsal with fake processes and stores.

## 11. Postflight

Within a bounded deadline after activation, Rust verifies:

- all 39 canonical names belong to this process and Rust-only names do not
  collide;
- the handoff receipt has not changed and Python authority remains positively
  off;
- one parent capability and exactly three mutation sub-capabilities exist;
- all five owners report current-process healthy state;
- production and observer store identities match the arm;
- the first fee, flow, reporting, and rebalance planning cycles complete;
- no quarantine, persistence failure, duplicate owner, or unexpected external
  action occurs.

Postflight does not prove rebalancing by spending money. The first production
payment remains governed by normal evidence and budgets; cutover itself does
not force an economic action.

## 12. Rollback

Rollback ordering is fixed:

1. Set the Rust mutation gate off so every owner denies new work.
2. Inside the Rust authority store, commit a final `rust_authority_off` release
   receipt bound to a higher epoch, the exact post-cutover state snapshot,
   unresolved outcomes/reservations, release identity, and a fresh Python
   resumption nonce.
3. Destroy the `LiveOwnerSet` and parent capability, close every dormant/live
   transport, and verify all mutation owners are unassembled.
4. Preserve unknown outcomes, quarantine entries, active reservations, and
   action receipts unchanged.
5. Positively verify no new Rust mutation after the release receipt and that
   Python's production database was never writable from Rust.
6. Only then start Python in inert recovery mode. Python imports and verifies
   the Rust release snapshot, reconciles ambiguous external outcomes, and
   enables authority only after its own strict resumption preflight succeeds.
7. Permanently retain the burned Rust arm nonce; another Rust attempt needs a
   new arm, new receipts, and new operator approval.

If Rust authority absence cannot be proven, Python remains off and the
procedure escalates. Preventing split authority takes precedence over restoring
automation quickly.

## 13. Required denial tests

The implementation plan must introduce each test RED before production code:

- subset or superset subsystem set;
- any retired subsystem, RPC, option, loop, transport, or capability;
- canonical set other than the exact generated 39 names;
- missing RPC or stopped Python treated as authority-off proof;
- absent, stale, tampered, cross-node, cross-store, or cross-nonce receipt;
- dirty source, binary, inventory, RPC contract, loop, or retired-set digest;
- missing transport proof or partial/success-shaped RPC contract;
- prior-boot or wrong-binary runway evidence;
- null gate evidence or default substitution;
- prepared owner containing a live adapter or mutation capability;
- Python production database becoming writable from Rust;
- final-state import missing, partial, active before arm consumption, or not
  bound to the Python receipt;
- dormant transport exposing any request method before activation;
- fallible work after nonce consumption;
- nonce burn after arm rename, replay, or post-rollback reuse;
- partial owner activation or double capability construction;
- cloned, serialized, defaulted, or test-only capability in production;
- action batch digest mismatch, reordering, duplication, or stale generation;
- ambiguity or active unexplained reservation at preflight;
- payment timeout releasing a reservation or triggering an automatic retry;
- rollback restoring Python before Rust authority absence is proven;
- rollback omitting or tampering with the Rust release snapshot;
- read-only RPC causing any database or external mutation.

Every test has a mutation counterpart that must be observed failing when its
guard is removed or inverted.

## 14. Work decomposition

This umbrella design is implemented as separately reviewable projects:

1. **Inventory and durable-task correction:** rebase Task 69 onto the retained
   v3 set, narrow Task 80 to retained rebalance corrections, and obtain a real
   independent Task 88 review or record an explicit supersession.
2. **Canonical contract completion:** move 32 partial RPCs to full, honest,
   independently reviewed behavior without adding authority.
3. **Retained transport completion:** implement and fake-prove only the
   external boundaries required by fees, publication, and circular
   rebalancing.
4. **Python release receipt:** implement and independently review the
   fail-closed authority-off and resumption receipts in Python.
5. **Rust v2 arm and capability:** implement prepare/preflight/re-verify/burn/
   consume/activate plus the sentinel and rollback controller.
6. **Runway automation:** implement observer-only restart restoration and the
   exact-binary 72-hour five-loop gate.
7. **Rehearsal and production plan:** fake-process cutover/rollback rehearsal,
   independent security review, then a separately authorized production
   window.

No project may mark another project's review criterion. Production remains
blocked until all seven are independently green.

## 15. Explicit non-goals

- No automatic or date-triggered cutover.
- No fee-first, rebalance-first, or other split-authority deployment.
- No reintroduction of retired liquidity executors for parity.
- No live-node action used as a test.
- No migration that destroys historical rows before the rollback window ends.
- No self-healing that retries an ambiguous action or silently re-arms after a
  restart.

## 16. Acceptance criteria

The design is satisfied only when:

- the generated inventory reports 39/39 full, reviewed, and promotion-ready;
- five of five loops are reviewed and soaked on the exact binary;
- every retained external boundary is fake-proven or explicitly replaced with
  a reviewed negative reachability proof;
- the one parent capability and exact three mutation sub-capabilities are the
  only production mutation construction path;
- Python and Rust receipts, stores, arm, nonce ledger, and release manifest are
  mutually bound;
- cutover and rollback rehearsals prove no both-authoritative interval;
- no Sling/coordinator or retired liquidity authority is present;
- a separately authorized production cutover and independent postflight pass.
