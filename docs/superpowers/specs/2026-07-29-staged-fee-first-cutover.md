# Staged Fee-First Cutover — Plan

**Status:** approved by operator 2026-07-29. Staging only; the cutover
itself remains a separate, explicitly-authorized act.

## Why staged, and why fee first

Fee is the ONLY subsystem with behavioural parity evidence. The
engagement gate compares actual fee DECISIONS against Python and returned
**GREEN** on 2026-07-29 (starvation 0/495 = 0.0%; rate 1.26x, 115 vs 91
would-broadcasts; flapper 3 Python runs with 0 unevaluated).

Every other subsystem's mutating surface has only refusal-arm evidence.
Measured the same day: **31 of 69 RPCs are mutating and cannot be
differentially tested on a production node** (diffing
`revenue-boltz-loop-out` executes two real swaps), so the live-diff
ceiling is 38/69. Cutting all 31 over at once would put them into live
service on source-read evidence alone. Fee-first converts an
all-or-nothing bet into a sequence of individually-evidenced steps.

The failure mode this avoids is specific and has precedent: not a loud
crash, but a silent under-performance — planner planning zero opens,
Boltz auto-cycle sitting at `Idle`, everything green — which is the
eight-day dead-LN+-automation pattern (`lessons:676`). Cheap rollback
does not protect against a failure nobody notices.

## What "fee-first" means concretely

**Rust takes:** the fee-adjustment loop only — per-channel fee decisions,
the v2 state machine, mempool/Vegas floor, trigger handling, and the
`setchannel` broadcast.

**Python keeps:** rebalance, capital/planner, Boltz, LN+, analytics,
econ, and every mutating RPC in those domains.

**Neither side may hold the same authority.** Python's fee authority must
be positively OFF (its own `revenue-fee-authority-status` reporting
`enabled: false`) before Rust's fee broadcast goes on. Absence of a
Python answer is NOT proof of off — that denial is already required by
task 69's brief and applies here unchanged.

## Preconditions before the fee arm may be issued

1. `parity_matrix.py` green across every PAIRED fee-domain method, with
   coverage stated (not implied) in the decision record.
2. A completed soak on the EXACT cutover candidate: mode
   `autonomous_shadow`, generation advancing, `mutation_call_count` 0,
   `rust_broadcast_attempts` 0, `rust_execution_quarantine` 0,
   `persistence_failures` 0, provenance agreeing three ways (installed =
   running = pinned), Python authority `enabled: true` throughout.
3. `engagement_gate.py` GREEN on that candidate's own shadow outcomes —
   not inherited from a previous candidate. (The 2026-07-29 GREEN belongs
   to `0515236885aa`, which predates tasks 62/63/67 and does NOT
   certify the cutover binary.)
4. Seed provenance VERIFIED BOUND on the cutover candidate's store — see
   below.
5. Rollback binary staged and its checksum recorded in the decision
   record.

## The seed-provenance precondition (resolved 2026-07-29)

A candidate cannot inherit an older candidate's seed provenance. Seed
rows written before task 42's F1 binding migration carry NULL
`bound_cycle_id`/`bound_generation` = UNBOUND, and the stateful-shadow
gate refuses them: *"requires explicit verified reconciliation, never
silent acceptance."*

**Remedy: reseed on a fresh observer DB. Never backfill.** The successful
seed writer (`insert_successful_seed_locked`) is reachable only from
`commit_fee_cycle_locked`, so a success row is written INSIDE the
generation-1 cycle commit and carries that cycle's identity by
construction. A fresh store binds automatically. Backfilling
`bound_cycle_id` would infer the binding from temporal correlation, which
is exactly the silent acceptance the gate exists to refuse.

Procedure (executed 2026-07-29 on candidate `6204b3a4`): stop the plugin;
move `revops-r-observer.db` plus its `-wal`/`-shm` aside as
`preserved-<ts>.*` (preserve — it holds the shadow-outcome history the
engagement gate reads); start with the autonomous-shadow flag set. The
store comes up virgin at generation 0 and seeds on its first cycle.

**Cost, stated:** reseeding discards accumulated shadow evidence (75
cycles / 3163 shadow outcomes at the time of writing). Since the
engagement gate must run on the cutover candidate's OWN outcomes anyway,
that evidence had to be rebuilt regardless.

**Task 69 must specify this.** The final cutover candidate hits the same
gate, so the v2 handoff design cannot leave seed reconciliation implicit.

## Mode matrix (empirically confirmed, exactly three states)

| Mode | observer | fee-dryrun | fee-broadcast | stateful-shadow | arm |
|---|---|---|---|---|---|
| passive observer | true | false | false | false | absent |
| autonomous fee shadow | true | true | false | true | absent |
| live fee authority | false | false | true | false | valid + consumed |

Any other combination is refused as `invalid_mode_combination`. There is
no halfway state, so a staged cutover moves the FEE subsystem straight
from autonomous shadow to live fee authority — there is no partial-fee
mode to hide in.

## Consequence for the v2 arm

The arm's subsystem set must be a DECLARED SUBSET, not a superset that
happens to be partly wired. Task 69's brief already requires that
superset AND subset arms both deny, so a fee-only arm must be a
first-class, explicitly-enumerated subset — carrying the fee subsystem's
digest and nothing else — rather than a whole-plugin arm with the other
subsystems left unassembled and hoped-inert.

## Rollback for the staged step

Ordering is unchanged from task 69's brief: destroy Rust fee mutation
capability first, preserve ambiguity/quarantine/reservations, verify
absence, then restore Python fee authority. A fresh nonce is required
after any coordinated rollback. Because only fee moved, the other
subsystems need no rollback action — which is the point of staging.

## Explicitly NOT covered by this step

Rebalance, capital/planner, Boltz, LN+, analytics, and econ mutators stay
with Python. Each becomes its own staged step, gated on its own
behavioural evidence. The 19 mutators outside the fund-moving twelve will
be recorded as uncertified in the decision record rather than assumed
equivalent.
