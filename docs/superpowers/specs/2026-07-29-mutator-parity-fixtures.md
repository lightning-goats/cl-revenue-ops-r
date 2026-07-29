# Fixture-Based Parity for the 31 Mutating RPCs — Scope

**Status:** scoping only, no implementation.
**Written:** 2026-07-29, after `parity_matrix.py`'s first real run.

## The problem this exists to solve

`parity_matrix.py` certifies a method by calling BOTH implementations live
on lnnode. That is sound for reads and unusable for mutators: diffing
`revenue-boltz-loop-out` executes two real swaps, `revenue-set-fee`
changes real fees, `revenue-planner-execute` opens real channels.

Measured 2026-07-29: **31 of 69 methods are mutating**, so the live-diff
ceiling is **38/69**. The other 31 need a different kind of evidence.

## What we have today, and why it is weaker than it looks

Every ported mutator already has unit tests asserting it returns Python's
uninitialized/refusal string. But those assertions check a string **I
typed after reading Python's source**. The parity matrix's first run found
three places where that reading was wrong (invented `count`/
`ignored_external_swaps` keys, wrong `config.*` key name, an over-gated
read). Reading the source correctly is not the same as matching the
running implementation, and we now have evidence of the gap.

The purpose of fixture parity is to convert *"I read the source
correctly"* into *"measured against the running implementation."*

## The load-bearing design element: prove non-mutation, never assume it

A refusal input is only safe if it refuses BEFORE acting. That must be
proven per input, not asserted by the author.

Every capture is bracketed by a mutation-evidence snapshot:

1. Snapshot mutation evidence (production DB counters for `forwards`,
   `spend_events`, `spend_reservations`, `peer_policies`,
   `planner_actions`, Boltz journal/ignores; plus `listpeerchannels`
   channel count and the Boltz swap list).
2. Call Python with the candidate input.
3. Re-snapshot.
4. **If ANYTHING changed, abort and mark the input UNSAFE — do not
   record a fixture.** A changed snapshot means the input mutates, so it
   can never be part of this harness.
5. Only on an unchanged snapshot, record Python's response as the golden
   fixture.
6. Call Rust with the same input and diff against the fixture.

This inverts the usual assumption. The harness's default is that an input
is dangerous; safety is something it demonstrates and re-demonstrates on
every run. Note the asymmetry with a normal test: a false "safe" here
spends real money, so the interlock runs on every capture, not once.

Known trap, already found in the source: Python's planner dry-run path
still writes a `planner_actions` row with `status="dry_run"`
(`capacity_planner.py`), so "dry run" does NOT imply side-effect-free.
The interlock catches exactly this.

## Buckets

### Bucket A — refusal-arm fixtures (all 31). RECOMMENDED.

Inputs chosen to refuse before acting: subsystem-uninitialized/disabled
arms, usage errors, argument-validation errors, and gate refusals
(budget, cooldown, hard cap, pending-swap block).

Value is disproportionate pre-cutover: with capabilities unassembled,
**the refusal arm is the only arm Rust can reach**, so this certifies
100% of currently-reachable mutator behaviour against live Python.
Effort: harness plus interlock, then an input table per method.

### Bucket B — audited dry-run fixtures (~4-6 methods). CONDITIONAL.

Candidates: `boltz-auto-cycle-run-now`, `boltz-balance-cycle`,
`boltz-expansion-treasury-cycle` (defaults `dry_run=True`),
`planner-execute` under `planner_dry_run`. Each requires a source audit
proving no write, and each must still pass the interlock. Any method
whose dry-run writes an audit row is disqualified from this bucket and
falls back to Bucket A.

### Bucket C — contract extraction from Python source (all 31). RECOMMENDED.

Per method, extract the response contract — key set, types, exact error
strings — and encode it as a structural test. Catches the invented-key and
wrong-error-string class systematically rather than one at a time. Not
runtime parity; it does not catch semantic divergence.

### Bucket D — regtest success-path parity. POST-CUTOVER.

A throwaway regtest CLN with both plugins, a fake boltzd, and LN+ stubs,
executing mutators for real against valueless coins. This is the ONLY
approach that reaches success-path parity, and it is the honest long-term
answer. It needs a node, a boltzd harness, and LN+ mocks — multiple days,
and it cannot land before Aug 3.

## Coverage this can and cannot buy

| Surface | Reachable pre-cutover? | By what |
|---|---|---|
| Mutator refusal arms | yes | Bucket A |
| Mutator response contracts | yes | Bucket C |
| A few dry-run paths | conditionally | Bucket B |
| **Mutator success paths** | **no** | Bucket D only, post-cutover |

Success-path parity for mutators is unreachable before cutover by
construction: proving it requires executing the action, and executing it
requires the authority that only exists after cutover. No harness changes
that. The first real success-path exercise of these 31 methods IS the
cutover, which is the strongest argument for staging it by subsystem and
for keeping rollback warm.

## Effort, honestly

- Bucket A harness + interlock: ~3-4h
- Bucket A input tables (31 methods): ~4-6h
- Bucket C extraction (31 methods): ~4-6h

Roughly a day and a half of focused work. That does **not** fit alongside
tasks 66, 67, 68, 69 and the v2 implementation before Aug 3.

## Recommendation

Do not attempt all three buckets before the 3rd. Instead:

1. **Build the Bucket A harness and interlock now** (~4h). It is reusable
   and the interlock is the safety-critical part.
2. **Populate it for the 12 fund-moving mutators only** — the Boltz six
   (loop-in/loop-out/chainswap/claim/refund/withdraw), the three cycle
   drivers, `planner-execute`, `set-fee`, `rebalance`. These are the ones
   where a parity defect costs money rather than a wrong-looking JSON key.
3. **Defer Buckets B and C to post-cutover**, and Bucket D with them.
4. Record the remaining 19 mutators as explicitly uncertified in the
   cutover decision record, so the gap is a stated risk rather than an
   assumption.

That yields, by the 3rd: the 38-method read surface differentially green,
the 12 highest-risk mutators refusal-certified against live Python, and
19 mutators carrying only source-read evidence — written down as such.
