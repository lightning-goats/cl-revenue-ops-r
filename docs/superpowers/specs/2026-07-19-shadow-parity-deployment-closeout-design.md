# Shadow Parity and Deployment Closeout Design

**Date:** 2026-07-19
**Status:** Approved design
**Scope:** `cl-revenue-ops-r`, the Python fee authority in
`cl_revenue_ops`, live shadow evidence on `lnnode`, and deployed-artifact
provenance

## Context

The Rust plugin is active on `lnnode` beside the authoritative Python plugin.
Live checks on 2026-07-19 established:

- Rust reports `mode=observer`, `observer=true`, and `fee-dryrun=true`.
- The Rust process opens the Python production database read-only and writes
  only its own observer database and dry-run artifacts.
- The Rust checkout is clean at `b161cca149d09251318f341c1196a1c1717e0114`,
  equal to `origin/main`.
- The deployed binary was built immediately after that commit, but its
  SHA-256 differs from the current local release artifact. The source commit
  is therefore strongly suggested by timestamps but not attested by exact
  artifact identity.
- Strict configuration comparison passes for 102 comparable keys. The four
  option-to-field remaps also match Python's effective runtime configuration
  when queried by their real `Config` field names. Twelve constructor-only
  Boltz options have no Python `Config` field and remain option-surface
  comparisons.
- Implemented read RPC fields match exactly.
- Forward ingestion matches exactly over the shared seven-day window:
  375 rows on each side and zero unmatched rows in either direction across
  the seven-column deduplication key.
- The old live fee-decision diff reports 607 Python changes and no Rust
  adjustments. Rust produced 3,822 dry-run rows, all non-broadcast:
  2,622 `skip_fee_unchanged`, 1,159 `skip_sleeping`, and 41
  `skip_waiting_time`.

The fee symptom is not valid parity evidence. Rust hydrates after Python has
already decided, mutated, and flushed state. Task 8b repaired two skip-gate
inputs with cross-cycle memory, but it did not make the full post-cycle state
an exact pre-decision input. Further timing or skip-gate patches would continue
to optimize an invalid observation boundary.

The approved
`2026-07-18-fee-cycle-replay-capture-design.md` in the Python capture worktree
is the authority for the capture schema, safety invariants, completeness
rules, and live-window minimums. This document defines how to finish that work,
repair the comparison harnesses, produce exact Rust replay evidence, and deploy
an attested Rust release.

## Goals

- Preserve Python as the sole live fee authority.
- Finish and review the existing default-off observational Python capture.
- Replay complete Python-oracle envelopes through Rust's pure fee-cycle kernel
  with no live RPC, production database, journal, ledger, or writable state
  access.
- Fix only decision mismatches proven by strict offline replay.
- Repair the two comparison-harness defects found during the live audit.
- Build the final Rust release from a clean, tested commit and prove that the
  exact local artifact is the artifact activated on `lnnode`.
- Re-run all valid live shadow gates after deployment.

## Non-goals

- Enabling Rust fee broadcasts or any other Rust execution authority.
- Removing, pausing, or replacing the Python authority.
- Triggering a fee cycle or another action RPC to manufacture validation data.
- Treating the existing post-cycle journal diff as a pass/fail gate.
- Expanding into rebalance, planner, Boltz execution, or cutover readiness.
- Introducing Sling, Hive, Mycelium, fleet coordination, or another
  coordinator.

## Safety invariants

1. Python remains loaded and authoritative for the entire validation window.
2. Python replay capture is disabled by default and records only naturally
   scheduled cycles.
3. Capture failures may invalidate evidence but may not change Python return
   values, exceptions, state transitions, or actions.
4. Rust replay is offline and fails closed on incomplete, malformed, missing,
   extra, or differently ordered inputs.
5. Rust remains `observer=true` and `fee-dryrun=true`; no action RPC path is
   introduced.
6. The production Python SQLite database remains read-only from Rust.
7. Existing uncommitted work in
   `/home/sat/bin/cl_revenue_ops/.worktrees/fee-cycle-replay-capture-python`
   is preserved and reviewed in place. It is never discarded or overwritten
   by a reset.
8. No test or validation command invokes a live fee, payment, rebalance,
   channel, planner, Boltz, or on-chain action RPC.
9. No parity claim is made from the old post-cycle fee diff.
10. Deployment is complete only when the remote binary checksum equals the
    checksum of the exact tested local artifact.

## Selected approach

Use the Python authority as an atomic pre-decision oracle and replay its
complete envelopes offline in Rust. Then deploy an exactly checksummed Rust
artifact and re-run the independent configuration, read-RPC, ingestion, and
runtime-safety gates.

Two alternatives are rejected:

- **Rebuild and redeploy only:** closes artifact provenance but leaves fee
  parity unknown.
- **Patch scheduler timing or more skip gates:** continues comparing
  post-decision state and can create false confidence without fixing input
  provenance.

## Workstreams

### 1. Python capture closeout

Continue from the existing capture worktree and its current three-file dirty
diff:

- `modules/fee_controller.py`
- `modules/fee_cycle_capture.py`
- `tests/test_fee_cycle_capture_integration.py`

Review the diff against the approved capture design and the worktree's
`AGENTS.md` invariants. Preserve the already-committed schema, wire helpers,
configuration surface, writer, retention, and lifecycle implementation.

The closeout must prove:

- every decision-relevant evidence, clock, entropy, governor, and execution
  operation is recorded once with stable semantic labels and ordinals;
- every evaluated channel has one terminal adjustment or explicit skip;
- pre-state is recorded before the first decision mutation;
- post-state is recorded after the authority path completes;
- capture-enabled and capture-disabled seeded executions return identical
  decisions and post-state;
- recorder and writer failures do not alter authority behavior;
- default-off and dynamic enable/disable behavior remain intact;
- no action RPC is introduced or invoked by capture validation.

The current focused baseline is 66 passing capture/replay tests. Any fix starts
with a failing test and keeps that suite green before broader regression
testing.

### 2. Strict Rust offline replay

Implement the versioned `fee_cycle_replay` version 0 parser and replay boundary
defined by the approved capture design.

The replay entry point:

```rust
fn replay_fee_capture(
    capture: &FeeCycleReplayV0,
) -> Result<FeeReplayResultV0, ReplayError>
```

must:

- validate schema name, version, canonical digest, completeness counts,
  sequence membership, and tagged numeric values;
- import captured controller state directly;
- provide strict in-memory evidence, clock, entropy, governor, execution, and
  state-sink adapters;
- call the existing pure `run_fee_cycle` kernel;
- compare ordered outcomes, traces, summaries, and post-state exactly;
- require complete transcript consumption;
- return structured mismatches and a nonzero process exit on any difference;
- have no construction path for `ClnRpc`, SQLite, a production path, a file
  journal, or an economic ledger.

The replay CLI reads only explicitly supplied local capture files and manifest.
It cannot accept a node name, Lightning RPC path, or production database path.

### 3. Harness repairs

Repair the live harnesses without broadening their pass criteria.

#### Read-RPC and ingestion harness

Change the default observer database from the invalid quoted
`~/.lightning/revops-r-observer.db` to the verified deployment path:

`/data/lightningd/.lightning/revops-r-observer.db`

Keep `--observer-db` available for other nodes. Add a self-test that pins the
default and prevents a regression to a tilde-relative value whose expansion is
suppressed by remote shell quoting.

#### Configuration harness

Compare the four remapped option suffixes through their actual Python
`Config` fields:

- `vegas-reflex` -> `enable_vegas_reflex`
- `vegas-decay` -> `vegas_decay_rate`
- `planner-max-fee-rate` -> `planner_max_fee_rate_sat_vb`
- `boltz-structural-budget-sats` ->
  `boltz_structural_budget_sats_per_day`

These four keys must no longer be skipped. Their comparison must include live
database overrides, which is why the Python `revenue-config get` value is
authoritative rather than the startup-only `listconfigs` value.

Keep the twelve constructor-only Boltz options explicitly skipped by the
`Config`-field harness. Validate them separately by comparing normalized CLN
`listconfigs revenue-ops-<suffix>` values with `revenue-r-config`.

#### Old fee journal diff

Mark `diff_fee_decisions.py` as diagnostic-only for post-cycle journals and
point operators to strict envelope replay for the fee parity gate. Do not
weaken or reinterpret its current mismatch output into a pass.

### 4. Artifact provenance and deployment

Build only after all source and harness changes are committed and both
repositories are clean.

For the Rust release:

1. Record the final Rust commit.
2. Run the complete required Rust verification suite.
3. Build `cargo build --release -p revops`.
4. Compute and record the local artifact's SHA-256, size, file type, dynamic
   library dependencies, and source commit.
5. Copy that exact file to a staging name outside CLN's auto-loaded plugin
   directory.
6. Compute the staging checksum on `lnnode` and require exact equality before
   stopping the active observer.
7. Preserve the previous deployed binary under a checksum-addressed rollback
   name.
8. Stop only the Rust observer, atomically install the verified staging
   artifact at `/home/lightningd/revops-r-deploy/revops`, and restart it with
   explicit options:
   - `revops-r-observer=true`
   - `revops-r-fee-dryrun=true`
   - `revops-r-db-path=/data/lightningd/.lightning/revenue_ops.db`
   - `revops-r-observer-db-path=/data/lightningd/.lightning/revops-r-observer.db`
   - `revops-r-journal-dir=/data/lightningd/.lightning`
9. Recompute the installed checksum and require equality with the tested local
   artifact.
10. Verify plugin liveness, observer mode, dry-run mode, read-only production
    database descriptors, and Python authority health.

Python instrumentation deployment is separate from Rust activation. Deploy the
reviewed Python capture with
`revenue-ops-fee-replay-capture-enabled=false`, restart only as required by the
existing plugin deployment procedure, and verify authority health before
enabling capture.

### 5. Fresh validation window

Archive the prior Rust dry-run journal as diagnostic history and begin a fresh
observer journal after the attested binary starts.

Enable only:

`revenue-ops-fee-replay-capture-enabled=true`

through CLN's dynamic configuration surface. Verify readback. Do not invoke
`revenue-fee-cycle`, `revenue-wake-all`, `revenue-set-fee`, `setchannel`, or
another action RPC.

Collect naturally scheduled cycles until the approved capture design's minimum
window is satisfied:

- at least six complete cycles;
- at least 100 channel evaluations;
- at least ten real fee adjustments;
- zero failed or dropped captures;
- zero sequence gaps.

Then disable capture, verify disabled readback, wait for the manifest to become
`closed` and queue-drained, freeze the consecutive retained files, and replay
every selected envelope offline.

If exact replay exposes a mismatch, classify it at the first differing
boundary: schema, state import, evidence, clock, entropy, governor, execution,
decision, trace, or post-state. Add the smallest failing test that reproduces
that boundary, implement one fix, and repeat offline replay. Never patch from
the old live diff symptom.

## Verification gates

### Python

- Focused capture, configuration, wire, lifecycle, writer, and integration
  tests pass.
- Decision-path regression tests pass.
- Architecture and no-Sling guards pass.
- Full Python suite passes.
- Capture-disabled and capture-enabled equivalence tests pass.

### Rust

- Formatting and lint gates pass with warnings denied.
- Workspace tests pass in development and release profiles.
- `no_setchannel_symbol_in_crate` remains green.
- Replay rejects malformed, incomplete, gapped, or unconsumed transcripts.
- Replay tests prove the absence of live I/O construction.

### Live shadow

- Installed Rust SHA-256 equals the exact tested local release SHA-256.
- Python and Rust plugins are both active.
- Rust reports `observer=true` and `fee-dryrun=true`.
- Rust production-database file descriptors are read-only.
- Strict comparable configuration values match, including the four remaps.
- The twelve constructor-only option values match through normalized
  `listconfigs` comparison.
- Read RPCs match on every implemented field.
- Ingestion counts and exact seven-column dedup keys match over the shared
  window.
- Python health and scheduled loops remain unaffected.
- Capture is disabled and drained after collection.
- Every selected envelope passes integrity and exact Rust replay with zero
  mismatches.

## Error handling and rollback

Before activation, any build, test, checksum, dependency, staging, or start
failure leaves the current Rust observer running.

After stopping Rust:

- a start or liveness failure restores the checksum-addressed previous binary
  and restarts it with the same explicit shadow options;
- Python remains loaded throughout and needs no state rollback;
- observer database and journals are Rust-owned artifacts and never replace or
  mutate Python state.

For Python capture:

- any capture health problem first disables the dynamic capture option;
- capture files and manifests are local observational artifacts;
- if instrumentation affects authority health, restore the prior Python plugin
  release using the established deployment rollback and leave capture off.

An exact replay mismatch blocks a parity claim but does not require live
rollback because replay is offline and Rust has no authority.

## Acceptance criteria

The task is complete only when:

- the Python capture implementation is reviewed, committed, and deployed
  default-off;
- the Rust strict offline replay implementation is reviewed and committed;
- both harness defects are fixed and their regression tests pass;
- the final Rust release is built from a clean tested commit;
- local, staged, installed, and running-artifact provenance is recorded with
  exact checksum equality;
- the independent live configuration, read-RPC, and ingestion gates pass;
- the required bounded capture window is complete, closed, and gap-free;
- every selected capture replays with zero mismatches;
- capture is disabled after collection;
- Python remained the sole live authority;
- no validation action RPC was invoked;
- no Sling or coordination dependency was introduced.

A passing result proves fee-decision parity only for the selected captured
window. It does not authorize Rust execution or fee cutover.

