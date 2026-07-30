# Whole-Plugin Cutover — Plan

**Status:** operator-directed 2026-07-30 — supersedes the staged fee-first
plan recorded here on 2026-07-29. Python `cl_revenue_ops` is turned OFF
entirely and the Rust port takes every subsystem. Rollback to Python is
available and cheap.

Staging notes only; the cutover itself remains a separate, explicitly
authorized act.

## READ THIS FIRST — two subsystems will provably do NOTHING after cutover

This is not a risk estimate. It is a known property of the code as it
stands today, and it is the single most important input to timing the
cutover.

**Capital planner: zero opens, zero closes, zero defibrillations.** Task 62
wired the planner rail and the frozen kernel, but its eleven evidence
fields (`winner_channels`, `loser_channels`, `redeployment_winner_evs`,
`defib_gates`, `close_gates`, `discovery`, `candidate_enrichment`,
`open_candidate_evidence`, `dual_fund_peers`, `open_guards`,
`recycle_candidates`) are still empty, declared as `EvidenceGap`s naming
Task 67 as their owner. The kernel is TOTAL over empty candidate sets: it
runs, plans nothing, and reports success. Python currently opens and closes
channels; Rust will not.

**Boltz auto-cycle: idle every pass.** Task 63's owner selects its mode from
treasury status and balance recommendations, both of which are Task-67
analytics that were explicitly left unported. `select_boltz_auto_cycle_mode`
therefore sees zero executable candidates and returns `Idle`. Python
currently executes swaps; Rust will not.

Both surfaces report *honestly* — typed gaps, not false success — so this
is discoverable rather than silent. But nothing about the cutover mechanism
prevents it, and a healthy-looking node doing no capital or swap work is
exactly the eight-day dead-automation pattern (`lessons:676`), just with
the cause known in advance.

**Three ways forward, operator's choice:**

1. **Fill the gaps first.** Port Python's `capital_efficiency` +
   `profitability_analyzer` paths that produce winners/losers and the Boltz
   candidate sets. This is the deferred Task-67 scope; it also makes Task
   63's deliberately-dead Boltz execution branch live, which is its own
   review. Largest effort, restores full parity of *behaviour*.
2. **Cut over accepting the pause.** Fee, rebalance, LN+, analytics and all
   read surfaces move to Rust; capital and Boltz automation stop until the
   gaps are filled. Acceptable if a days-long pause in channel opens and
   swaps is tolerable — those are the slowest-moving subsystems.
3. **Cut over and keep Python's capital/Boltz loops running.** Only viable
   if Python can run with fee authority off while its planner and Boltz
   loops continue. Needs verification that its loops do not depend on the
   fee-authority gate; if they share it, this option does not exist.

Whichever is chosen must be recorded in the cutover decision record, so
"capital did nothing for three days" is never discovered after the fact.

## What whole-plugin cutover means

**Rust takes:** every subsystem — fee, rebalance, capital/planner, Boltz,
LN+, analytics, econ, datastore producers, and all 69 RPC names under their
canonical spellings.

**Python is turned OFF:** the plugin is stopped, not merely gated. Its fee
authority must be positively OFF *before* Rust's goes on, and stopping it
is not by itself proof — a stopped plugin cannot answer, and absence of an
answer is not evidence of authority-off. The v2 arm's positive
authority-off proof still applies exactly as Task 69's brief specifies.

**Consequence for the v2 arm — this SIMPLIFIES Task 69.** The earlier
fee-first plan required a declared *subset* arm. A whole-plugin cutover
uses the full declared subsystem set, which is what Task 69's brief
specified in the first place: superset arms and subset arms both deny, and
the full set is the declared set. The subset machinery is no longer needed.

## Preconditions before the arm may be issued

1. `parity_matrix.py` green across every PAIRED method, with coverage
   STATED in the decision record — currently 34/69 declared, ceiling 38/69,
   because 31 methods are mutating and cannot be differentially tested on a
   production node (diffing `revenue-boltz-loop-out` executes two real
   swaps).
2. The 31 uncertified mutators listed explicitly in the decision record as
   carrying refusal-arm + source-read evidence only. Not assumed
   equivalent.
3. A completed soak on the EXACT cutover candidate: `autonomous_shadow`,
   generation advancing, `mutation_call_count` 0, `rust_broadcast_attempts`
   0, `rust_execution_quarantine` 0, `persistence_failures` 0, provenance
   agreeing three ways, Python authority `enabled: true` throughout.
4. `engagement_gate.py` GREEN on that candidate's OWN shadow outcomes.
5. Seed provenance VERIFIED BOUND on the cutover candidate's store (see
   below).
6. Rollback binary staged and its checksum in the decision record.
7. The capital/Boltz decision above recorded.

## Seed provenance (resolved 2026-07-29)

A candidate cannot inherit an older candidate's seed provenance. Rows
written before Task 42's F1 binding migration have NULL
`bound_cycle_id`/`bound_generation` = UNBOUND, and the stateful-shadow gate
refuses them: *"requires explicit verified reconciliation, never silent
acceptance."*

**Remedy: reseed on a fresh observer DB. Never backfill.**
`insert_successful_seed_locked` is reachable only from
`commit_fee_cycle_locked`, so a success row is written INSIDE the
generation-1 cycle commit and carries that cycle's identity by
construction. Backfilling would infer the binding — the silent acceptance
the gate exists to refuse. Executed on candidate `6204b3a4`: verified
`bound_generation=1`, `bound_cycle_id=rust-fee-1785367388-…`, 43 channels
seeded (one MORE than the stale July-22 seed's 42).

## Mode matrix (empirically confirmed — exactly three states)

| Mode | observer | fee-dryrun | fee-broadcast | stateful-shadow | arm |
|---|---|---|---|---|---|
| passive observer | true | false | false | false | absent |
| autonomous fee shadow | true | true | false | true | absent |
| live fee authority | false | false | true | false | valid + consumed |

Any other combination is refused as `invalid_mode_combination`. The
cutover moves the candidate from autonomous shadow to live authority in one
transition; there is no intermediate state to pause in.

## Post-cutover watch — what rollback does NOT cover

Rollback protects against a cutover that fails *visibly*. It does not
protect against one that runs, reports healthy, and quietly does less than
Python did. With all 31 mutators going live at once on refusal-arm
evidence, that is the failure mode to instrument for, and it must be
watched actively rather than waited on.

Watch for the first 24h, then daily:

- **Loop health, current-boot.** Every one of the eight loops must reach
  `passed` for THIS boot. `never_run_this_boot` persisting past a loop's
  cadence means it is not running at all — the state Task 67 added
  specifically so this is visible.
- **Action counts vs Python's own recent history**: fee broadcasts/day,
  rebalance attempts/day, channel opens+closes/week, Boltz swaps/week.
  A zero where Python had a nonzero baseline is the signal. Expect zero for
  capital and Boltz per the section above, and treat any OTHER zero as a
  finding.
- **Quarantine and persistence failures**: `rust_execution_quarantine` and
  `persistence_failures` must stay 0; any nonzero is an immediate
  investigation, and a quarantined money-path intent means funds may be
  committed without a recorded terminal.
- **Owner suspension.** Any owner reporting `suspended` means a settle could
  not be persisted and that subsystem has stopped accepting work.

## Rollback

Trigger on any of: quarantine nonzero, persistence failure nonzero, an
owner suspended, a loop stuck at `never_run_this_boot` past its cadence, or
an action count at zero where Python had a baseline (capital/Boltz
excepted per the recorded decision).

Ordering is unchanged from Task 69's brief: destroy Rust mutation
capability FIRST, preserve ambiguity/quarantine/reservations, verify
absence, then restore Python. A fresh nonce is required after any
coordinated rollback. Rollback binaries staged on the node:
`revops.rollback.97ba872c…`, `revops.rollback.0515236885aa…`, and older.

**Rollback restores Python's authority; it does not undo committed
on-chain actions.** A channel Rust opened stays open; a swap it made stays
made. Preserve every quarantined intent for reconciliation rather than
clearing it to make the rollback look clean.
