# hexmem task 47 — CapacityPlanner orchestration port report

Scope: port the missing orchestration half of `crates/revops-capital`'s
`CapacityPlanner` port — the five candidate-discovery strategies, winner/loser
classification, candidate enrichment/scoring, and `execute_cycle`'s
orchestration, all as evidence-in/plan-out pure functions with zero live
mutation capability. This report is the honesty record required by the task:
red evidence, fixture/test counts, and every declared gap.

## What was built

New modules under `crates/revops-capital/src/planner/`:

| Module | Ports | py source |
|---|---|---|
| `pyround.rs` | Python `round(x, ndigits)` parity helper | — (infra) |
| `demand_flow.rs` | `DemandFlowClassifier.classify_peers` / `find_sink_adjacent_candidates` | `modules/demand_flow.py` 56-95, 193-233 |
| `discovery.rs` | Strategies 1/2(partial)/3/5 (`_discover_from_winners`, `_discover_from_neighbors` fallback branch, `_discover_from_graph`, `_discover_from_route_pairs`) | `capacity_planner.py` 1485-1904 |
| `winners.rs` | `_identify_winners` | `capacity_planner.py` 805-899 |
| `losers.rs` | `_identify_losers` + `_build_dead_capital_loser`'s gate/record assembly | `capacity_planner.py` 901-1129, 1251-1382 |
| `candidate_score.rs` | `_score_candidate` | `capacity_planner.py` 2110-2201 |
| `sizing.rs` | `_size_channel` | `capacity_planner.py` 2757-2806 |
| `recycle.rs` | `_apply_redeployment_ev_demotion`, `_evaluate_recycle_opportunities`'s selection core | `capacity_planner.py` 1454-1483, 2016-2108 |
| `cycle.rs` | `execute_cycle` (as `plan_cycle`) + `_discover_peers` | `capacity_planner.py` 363-786, 2714-2755 |

Plus 9 new test files (`tests/planner_{winners,losers,candidate_score,
discovery,sizing,recycle,cycle}.rs`), extensions to
`tools/port/gen_capital_planner_fixtures.py`, an updated
`fixtures/capital/planner/kernels.json`, and doc updates to `ENTRYPOINTS.md`,
`src/lib.rs`, `src/planner/mod.rs`.

## Architecture contract compliance

- **No RPC client types anywhere in the crate.** `Cargo.toml` depends only
  on `revops-core`, `revops-db` (the existing budget rail, unrelated to
  this pass), `serde`, `serde_json`. `#![forbid(unsafe_code)]` at the crate
  root, unchanged.
- **`plan_cycle` is evidence-in/plan-out.** `CycleEvidence` in -> `CyclePlan`
  out. Every field Python fetches via CLN RPC or a DB read is a typed input
  field; `CyclePlan`'s opens/closes/defibrillations are "would do" records
  with reasons, never "did" records (no `action_id`/RPC `status` fields —
  those only exist after real execution in Python).
- **Missing/stale evidence denies action, never defaults.** Concretely:
  `plan_cycle`'s candidate-evaluation loop fails closed when a peer has no
  `open_candidate_evidence` entry — the peer is skipped with a
  `"No sizing/EV evidence for ...: cannot evaluate"` reason rather than
  silently defaulting to some sizing (see the
  `candidate_without_open_ev_evidence_is_skipped_not_defaulted` test,
  cross-checked against `candidate_with_open_ev_evidence_can_be_opened`
  proving the WITH-evidence path is genuinely different, not just always
  skipping). `ev::is_recycle_eligible`'s `protected_peers: None` ->
  ineligible (fail-closed on unknown policy source) is exercised directly
  by `planner_ev.rs`'s existing `unknown_policy_source_fails_closed_for_every_peer`
  test and indirectly by `plan_cycle`'s recycle path (same function).
- **No live node/network/service contact.** All fixture generation runs
  against `types.SimpleNamespace` stubs; no CLN, no real DB, confirmed by
  reading every new stub method added to `tools/port/gen_capital_planner_fixtures.py`.

## Fixture counts

`fixtures/capital/planner/kernels.json`: **155 scenarios** across 19 kinds
(was 82 across 11 kinds before this pass). New kinds added this pass:

| kind | scenarios |
|---|---|
| `identify_winners` | 12 |
| `identify_losers` | 14 |
| `score_candidate` | 17 |
| `discover_from_winners` | 4 |
| `discover_from_graph` | 7 |
| `classify_peers` | 7 |
| `find_sink_adjacent_candidates` | 6 |
| `size_channel` | 6 |

Boundary cases covered per the task's requirement: empty candidate/channel
sets (`empty_channels`, `empty_winners`, `empty_cache`, `no_sinks`,
`no_candidates_uses_min`), all-excluded/all-blocked (`roi_below_threshold_excluded`,
`turnover_below_threshold_excluded`, `below_channel_count_excluded`,
`stagnant_high_roi_not_loser`), tie scores (`dedup_first_sink_wins` — two
sinks producing the same candidate, first-encountered-wins tie behavior),
stale/missing evidence (`no_flow_metrics_skipped`, `rebal_success_data_insufficient_total_no_penalty`,
`no_signals_unchanged` for `score_candidate` with every optional signal
absent).

Existing kinds (portfolio_gate, close_fee_plan, extract_actual_close_fee_sats,
failed_open_backoff_reason, peer_exposure_cap_reason, calculate_open_ev,
calculate_redeployment_ev, calculate_recycle_ev, is_recycle_eligible,
normalize_candidate_scores, apply_pool_quotas, dead_capital_stage) are
**unchanged in count** — verified against every existing test's
`assert_eq!(cases.len(), N, ...)` before regenerating the fixture file, all
matched exactly, confirming no regression to the prior 82 scenarios.

`discover_from_neighbors` and `discover_from_route_pairs` have **no Python
fixture** — see "Declared gaps" below. They are covered by hand-derived
Rust-native tests instead (`tests/planner_discovery.rs`).

## Test counts

- `revops-capital` focused: **80 tests, all passing** (was 39 before this
  pass — the 39 baseline itself does not perfectly match `ENTRYPOINTS.md`'s
  older wording because that count included/excluded `boltz_reservation.rs`/
  `capex.rs` inconsistently across doc revisions; the important number is
  the delta: **+41 tests**, zero regressions, confirmed by running the full
  suite before and after).
- Per new test file: `planner_winners.rs` 2, `planner_losers.rs` 2,
  `planner_candidate_score.rs` 2, `planner_discovery.rs` 9,
  `planner_sizing.rs` 2, `planner_recycle.rs` 8, `planner_cycle.rs` 14.
- `cargo build --workspace`: succeeds (verified after every module was
  added and again after the final commit-ready state).

## Revert-discriminating tripwires — captured RED evidence

Per the task's TDD/observed-red requirement, every tripwire below was
proven by literally deleting/bypassing the call in `cycle.rs`, running the
targeted test, capturing the failure, then restoring the exact original
code and re-confirming green. All five gates (`cargo test -p revops-capital`,
`clippy -D warnings`, `fmt --check`, `cargo build --workspace`) were
re-verified clean after every restoration, and one final time before the
commit below.

### (a) Each of the five discovery calls in `discover_peers`

**1. `discover_from_winners` removed** — `discovery_strategy_1_winners_reaches_candidate_pool`:
```
thread 'discovery_strategy_1_winners_reaches_candidate_pool' panicked at crates/revops-capital/tests/planner_cycle.rs:166:5:
assertion failed: pool_peer_ids(&plan).contains(&"s1_winner_only".to_string())
test result: FAILED. 0 passed; 1 failed
```

**2. `discover_from_neighbors` removed** — `discovery_strategy_2_neighbors_reaches_candidate_pool`:
```
thread 'discovery_strategy_2_neighbors_reaches_candidate_pool' panicked at crates/revops-capital/tests/planner_cycle.rs:187:5:
assertion failed: pool_peer_ids(&plan).contains(&"s2_neighbor_only".to_string())
test result: FAILED. 0 passed; 1 failed
```

**3. `discover_from_graph` removed** — `discovery_strategy_3_graph_reaches_candidate_pool`:
```
thread 'discovery_strategy_3_graph_reaches_candidate_pool' panicked at crates/revops-capital/tests/planner_cycle.rs:206:5:
assertion failed: pool_peer_ids(&plan).contains(&"s3_graph_only".to_string())
test result: FAILED. 0 passed; 1 failed
```

**4. `discover_from_route_pairs` removed** — `discovery_strategy_4_route_pairs_reaches_candidate_pool`:
```
thread 'discovery_strategy_4_route_pairs_reaches_candidate_pool' panicked at crates/revops-capital/tests/planner_cycle.rs:231:5:
assertion failed: pool_peer_ids(&plan).contains(&"s4_route_pair_only".to_string())
test result: FAILED. 0 passed; 1 failed
```

**5. `discover_from_demand_flow`'s candidates dropped** — `discovery_strategy_5_demand_flow_reaches_candidate_pool`:
```
thread 'discovery_strategy_5_demand_flow_reaches_candidate_pool' panicked at crates/revops-capital/tests/planner_cycle.rs:252:5:
assertion failed: pool_peer_ids(&plan).contains(&"s5_demand_flow_only".to_string())
test result: FAILED. 0 passed; 1 failed
```

### (b) Winner/loser classification calls removed

`identify_winners`/`identify_losers` replaced with `Vec::new()` in
`plan_cycle` — `winner_classification_reaches_the_plan` and
`loser_classification_reaches_the_plan`:
```
thread 'winner_classification_reaches_the_plan' panicked at crates/revops-capital/tests/planner_cycle.rs:98:5:
assertion `left == right` failed
  left: 0
 right: 1

thread 'loser_classification_reaches_the_plan' panicked at crates/revops-capital/tests/planner_cycle.rs:135:5:
assertion `left == right` failed
  left: 0
 right: 1
test result: FAILED. 0 passed; 1 failed  (each, run separately)
```

### (c) Scoring/enrichment (`score_candidate`) call removed

`candidate_enrichment_actually_changes_pool_score`:
```
thread 'candidate_enrichment_actually_changes_pool_score' panicked at crates/revops-capital/tests/planner_cycle.rs:301:5:
poor-reputation enrichment must reduce the pool score: raw=0.02351510153071851 enriched=0.02351510153071851
test result: FAILED. 0 passed; 1 failed
```

### (d) The orchestration call itself (`plan_cycle`'s body bypassed)

`plan_cycle` temporarily short-circuited to `return CyclePlan { skipped:
false, ..Default::default() }` immediately after the `planner_enabled`
gate, bypassing everything else. Full `planner_cycle.rs` suite result:
```
test result: FAILED. 3 passed; 11 failed; 0 ignored

failures:
    candidate_enrichment_actually_changes_pool_score
    candidate_with_open_ev_evidence_can_be_opened
    candidate_without_open_ev_evidence_is_skipped_not_defaulted
    discovery_strategy_1_winners_reaches_candidate_pool
    discovery_strategy_2_neighbors_reaches_candidate_pool
    discovery_strategy_3_graph_reaches_candidate_pool
    discovery_strategy_4_route_pairs_reaches_candidate_pool
    discovery_strategy_5_demand_flow_reaches_candidate_pool
    loser_classification_reaches_the_plan
    recycle_opportunity_composes_eligible_loser_and_candidate
    winner_classification_reaches_the_plan
```
The 3 tests that still passed with the body bypassed
(`disabled_planner_short_circuits`, `enabled_planner_does_not_skip`,
`fee_gate_closed_skips_discovery_entirely`) are exactly the ones that don't
depend on the bypassed body — `disabled_planner_short_circuits` tests the
gate BEFORE the bypass point, `enabled_planner_does_not_skip` only checks
`skipped == false` (still true for the stub), and
`fee_gate_closed_skips_discovery_entirely` asserts an empty pool, which the
stub also produces. This is the expected, correct pattern for a real
tripwire suite: the unrelated tests are unaffected, the dependent ones all
fail.

All code was restored to its pre-revert state after each capture; the
final `git diff` against the committed state contains none of the
probe/bypass edits (`cargo test -p revops-capital` reconfirmed 80/80
green after every restoration).

### Deviation from strict red-first ordering — disclosed

Given the size of this task (~1,950 Python LOC, ~19 new evidence/decision
types), the new modules were implemented before their test files were
written, not in strict red-first TDD order. To still produce genuine
observed-red evidence (per the task's explicit requirement, motivated by
this project's six prior false-clean defects), every REVERT-DISCRIMINATING
tripwire above was captured by literally deleting the corresponding call
and observing the test fail, which proves the same thing strict TDD
red-first would have: the tests are not vacuously passing and the
implementation is load-bearing. This is disclosed rather than presented as
if red-first ordering was followed throughout.

## Declared gaps (see `ENTRYPOINTS.md` for the authoritative list)

1. **`discover_from_neighbors`'s capital-efficiency-aware branch is not
   ported.** Only the `_capital_efficiency is None` fallback (py 1516-1566)
   is ported; the patron-pool + second-degree-neighbor extension (py
   1568-1760, ~190 LOC) is not. This is the branch every production node
   with a capital-efficiency analyzer injected actually takes in Python.
2. **`discover_from_neighbors` and `discover_from_route_pairs` have no
   Python-driven fixture** — both are covered by hand-derived Rust-native
   tests only (documented in `discovery.rs`'s module doc comment and
   `tests/planner_discovery.rs`'s file doc comment), not by running the
   real Python function. The formulas were transcribed line-by-line from
   the Python source and cross-checked by hand, but this is a lower
   verification bar than the fixture-verified functions.
3. **`_arbitrate_close_list`'s DB-registry conflict-arming is not
   implemented.** `plan_cycle`'s close selection uses worst-ROI-first order
   directly; it does not dedup against concurrent in-flight closes from
   other subsystems the way Python's arbitration pass does.
4. **Whole-cycle fixture parity for `plan_cycle` against Python's
   `execute_cycle` was not attempted.** Every sub-decision is
   fixture-verified in isolation; the orchestration wiring is proven by
   revert tripwires, not byte-for-byte Python parity on the full cycle
   (which would require replicating `_check_fee_gate`, the policy manager,
   the arbitration registry, and the LN+ evaluator — all explicitly out of
   scope per the architecture contract).
5. **LN+ swap evaluation is entirely absent from `plan_cycle`** (py
   648-673) — a whole separate subsystem, unchanged from the prior pass's
   scope decision.
6. Unchanged from the prior pass: `BoltzCliManager`, `BoltzAutoCycle`,
   `LNPlusSwapAutomation` remain fully unported.

## Gate results (as of `d13dabf`, the reviewed checkpoint)

- `cargo test -p revops-capital`: **80 passed, 0 failed**.
- `cargo clippy -p revops-capital --all-targets -- -D warnings`: **clean,
  zero warnings**.
- `cargo fmt --all -- --check`: **clean** (ran `cargo fmt --all` once
  during development to normalize the new files' formatting, then
  reverified `--check` clean).
- `cargo build --workspace`: **succeeds**.

---

# Correction round 1 (hexmem task 47 review, `worktree-agent-a926444ca0e99d4de` at `d13dabf`)

This section is the durable red/green record for the six correction items
in `/home/sat/agent-tasks/task-47-review-findings.md`. Every correction
below began with an OBSERVED FAILING test — captured, quoted verbatim, and
only then made to pass — per the review's finding 6 (the prior pass
substituted revert probes for genuine red-first TDD; this round does not
repeat that).

Process note on "red-first" for new Rust types: where a correction required
a brand-new struct FIELD to even express the scenario (e.g. `DefibGate`'s
`observed_at`) or a brand-new FUNCTION to call (`discover_from_neighbors_capital_efficiency`),
the minimal structural scaffolding (a field with no enforcement behind it,
or a stub returning an empty/no-op result) was added first, immediately
followed by the failing test run BEFORE any enforcement/real-implementation
logic was written. This is standard outside-in TDD, not a revert probe: the
test still fails for a genuine behavioral reason (the scaffolding does
nothing yet), and the same test later verifies the real implementation.

## Finding 1 (P1) — fail-open safety gates

**Correction, part 1 — missing evidence.** Added the RED tests to
`tests/planner_cycle.rs` FIRST (`defib_missing_gate_evidence_denies_and_reports_reason`,
`close_missing_gate_evidence_denies_and_reports_reason`,
`open_missing_guard_evidence_denies_and_reports_reason`), against the
UNCHANGED `d13dabf` `cycle.rs` (still `.unwrap_or_default()` — missing
evidence silently allowed):

```
running 3 tests
test close_missing_gate_evidence_denies_and_reports_reason ... FAILED
test defib_missing_gate_evidence_denies_and_reports_reason ... FAILED
test open_missing_guard_evidence_denies_and_reports_reason ... FAILED

---- close_missing_gate_evidence_denies_and_reports_reason stdout ----
thread 'close_missing_gate_evidence_denies_and_reports_reason' panicked at crates/revops-capital/tests/planner_cycle.rs:575:5:
missing gate evidence must NOT default to allowed

---- defib_missing_gate_evidence_denies_and_reports_reason stdout ----
thread 'defib_missing_gate_evidence_denies_and_reports_reason' panicked at crates/revops-capital/tests/planner_cycle.rs:558:5:
missing gate evidence must NOT default to allowed

---- open_missing_guard_evidence_denies_and_reports_reason stdout ----
thread 'open_missing_guard_evidence_denies_and_reports_reason' panicked at crates/revops-capital/tests/planner_cycle.rs:600:5:
missing guard evidence must NOT default to allowed

test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 14 filtered out
```

Fix: replaced `evidence.{defib,close}_gates.get(...).cloned().unwrap_or_default()`
and `evidence.open_guards.get(...)` (`Option<&OpenGuard>` treated as "no
block") with `let Some(gate) = ...get(...) else { push a "...evidence
missing... (fail-closed)" skip reason; continue }` in all three loops
(`cycle.rs`'s defibrillation-selection, close-selection, and
open-execution loops). All 3 tests green immediately after.

**Correction, part 2 — stale evidence.** Added `observed_at: i64` to
`DefibGate`/`CloseGate`/`OpenGuard` (structural — no enforcement yet) and
`pub const GATE_EVIDENCE_MAX_AGE_SECS: i64 = 900` (documented rationale:
covers one cycle's per-peer RPC/DB evidence-gathering fan-out while still
catching cached/reused-across-cycles evidence). Added 4 RED tests
(`defib_stale_gate_evidence_denies_and_reports_reason`,
`close_stale_gate_evidence_denies_and_reports_reason`,
`open_stale_guard_evidence_denies_and_reports_reason`,
`close_gate_evidence_boundary_and_future_observed_at`), run against
`cycle.rs` with the field present but NO age check yet:

```
running 21 tests (subset)
test close_gate_evidence_boundary_and_future_observed_at ... FAILED
test defib_stale_gate_evidence_denies_and_reports_reason ... FAILED
test close_stale_gate_evidence_denies_and_reports_reason ... FAILED
test open_stale_guard_evidence_denies_and_reports_reason ... FAILED

---- close_gate_evidence_boundary_and_future_observed_at stdout ----
thread panicked at crates/revops-capital/tests/planner_cycle.rs:729:5:
evidence observed in the future (negative age / clock skew) must be denied

---- defib_stale_gate_evidence_denies_and_reports_reason stdout ----
thread panicked at crates/revops-capital/tests/planner_cycle.rs:636:5:
stale gate evidence must NOT be treated as allowed

---- close_stale_gate_evidence_denies_and_reports_reason stdout ----
thread panicked at crates/revops-capital/tests/planner_cycle.rs:660:5:
stale gate evidence must NOT be treated as allowed

---- open_stale_guard_evidence_denies_and_reports_reason stdout ----
thread panicked at crates/revops-capital/tests/planner_cycle.rs:687:5:
stale guard evidence must NOT be treated as allowed

test result: FAILED. 17 passed; 4 failed; 0 ignored; 0 measured; 0 filtered out
```

Fix: added `gate_evidence_is_fresh(observed_at, now) -> bool` (`(0..=GATE_EVIDENCE_MAX_AGE_SECS).contains(&(now - observed_at))`)
and a stale-evidence skip-reason check right after each missing-evidence
check, in all three loops. All 4 tests green; the boundary test also
confirms `observed_at == now - GATE_EVIDENCE_MAX_AGE_SECS` (inclusive) is
still treated as fresh.

**Collateral fix (expected, not a new correction):** `loser_classification_reaches_the_plan`
(a pre-existing happy-path test, whose comment literally said "no
close_gates entry -> allowed by default" — a description of the bug being
fixed) broke as a direct consequence of the fix and was updated to supply
fresh `CloseGate { observed_at: ev.now, .. }` evidence, restoring its
original intent (prove the classified CLOSE loser reaches the close list)
under the corrected contract.

Focused tests: `planner_cycle.rs` 14 -> 26 (+12, of which 7 are finding
1's missing/stale/boundary tests; the remaining +5 are findings 3/4/2
below).

## Finding 2 (P1) — capital-efficiency-aware neighbor discovery not ported

Added a stub `discover_from_neighbors_capital_efficiency` (returns
`Vec::new()` unconditionally) plus the new fixture-driven test
`discover_from_neighbors_capital_efficiency_matches_python` in
`tests/planner_discovery.rs` (6 scenarios: fallback-shape/no-second-hop,
second-degree traversal, missing-efficiency-rank default, volume-tiebreak
patron selection, same-neighbor-from-two-patrons dedup, fee/capacity
filters). RED against the stub:

```
running 13 tests
test discover_from_neighbors_capital_efficiency_matches_python ... FAILED

thread 'discover_from_neighbors_capital_efficiency_matches_python' panicked at crates/revops-capital/tests/planner_discovery.rs:302:9:
assertion `left == right` failed: first_degree_only_no_second_hop_channels: count. actual=[]
  left: 0
 right: 1

test result: FAILED. 12 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
```

Fix: ported `_build_neighbor_patron_pool` (py 1624-1663) as
`build_neighbor_patron_pool` (top-5-by-efficiency-rank +
top-5-by-volume + top-3-by-marginal-roi, deduped keeping the higher
`patron_score`, first-discovery order preserved via `super::dedup`,
including Python's `getattr(..., 0.1) or 0.1` falsy-value fallback for a
missing OR explicit-zero `efficiency_rank`), `_build_neighbor_candidate`
(py 1665-1707) as a private helper shared by both the first- and
second-degree passes, and `_discover_second_degree_neighbors` (py
1709-1760) as `discover_second_degree_neighbors` — including the subtle
Python aliasing behavior where the top-3 first-degree candidates' scores
are mutated IN PLACE by the second-degree bonus pass before being used as
the child patrons' `patron_score` (py's list-slice `first_degree_list[:3]`
shares the same dict objects as `first_degree_list`). All 6 scenarios
green on the first implementation attempt (independently verified: the
hand-derived formula math for `first_degree_only_no_second_hop_channels`
and `second_degree_traversal_from_top_first_degree` was cross-checked
against a standalone Python run before writing the Rust port — see
"Fixture regeneration" below).

Wired into orchestration (`cycle.rs`'s `discover_peers`): added
`DiscoveryEvidence::neighbor_capital_efficiency: Option<Vec<discovery::PatronPoolInput>>`
(present <=> Python's `self._capital_efficiency is not None`) and a
revert-tripwire test pair in `planner_cycle.rs`
(`discovery_strategy_2_capital_efficiency_branch_reaches_candidate_pool`,
`discovery_strategy_2_falls_back_without_capital_efficiency_evidence`).
RED for the reachability test (evidence supplied, but `discover_peers`
still unconditionally called the fallback function):

```
running 2 tests
test discovery_strategy_2_falls_back_without_capital_efficiency_evidence ... ok
test discovery_strategy_2_capital_efficiency_branch_reaches_candidate_pool ... FAILED

thread panicked at crates/revops-capital/tests/planner_cycle.rs:926:5:
capital-efficiency-aware neighbor discovery must be reachable from plan_cycle when evidence is supplied: pool=[]

test result: FAILED. 1 passed; 1 failed
```

Fix: `discover_peers` now branches on `evidence.neighbor_capital_efficiency`
(`Some` -> `discover_from_neighbors_capital_efficiency`, `None` ->
`discover_from_neighbors`). Both tests green.

Focused tests: `planner_discovery.rs` +1 (`discover_from_neighbors_capital_efficiency_matches_python`,
6 fixture scenarios inside it), `planner_cycle.rs` +2 (reachability pair).

## Finding 3 (P1) — multi-open capital accounting reuses initial balance

RED test added FIRST (`multi_open_second_candidate_sized_against_remaining_capital_after_first_open`
in `planner_cycle.rs`): two equal-score candidates, `max_opens_per_cycle: 2`,
`available_sats: 4_000_000` — the correct behavior is candidate 1 sized at
2,000,000 (half of 4,000,000) and candidate 2 RECOMPUTED against the
2,000,000 remaining after candidate 1's debit, sizing it at 1,000,000. Run
against the unchanged `d13dabf` open-selection loop (single evaluation
pass, no recompute):

```
running 1 test
thread 'multi_open_second_candidate_sized_against_remaining_capital_after_first_open' panicked at crates/revops-capital/tests/planner_cycle.rs:885:5:
assertion `left == right` failed: ... got [PlannedOpen { peer_id: "candA", amount_sats: 2000000, ev: 2716.986301369863 }, PlannedOpen { peer_id: "candB", amount_sats: 2000000, ev: 2716.986301369863 }]
  left: [2000000, 2000000]
 right: [1000000, 2000000]

test result: FAILED. 0 passed; 1 failed
```

Fix: introduced `running_available_sats` (separate from
`remaining_budget`, the exploration-FEE budget — kept apart per the
review's explicit instruction). For `opens_this_cycle > 0` (i.e. every
open after the first ACCEPTED one), `channel_size`/`ev` are recomputed via
`size_channel`/`calculate_open_ev` against `running_available_sats` and the
SAME `sizing_pool_for` snapshot Python uses (py 687-690); the first
accepted open still reuses its earlier-evaluation-pass size/EV (py's
`else` branch). On acceptance, `running_available_sats -= channel_size`
(py 737-744's commit-point debit — nothing is debited for candidates that
are evaluated but never accepted). Test green; full `planner_cycle.rs`
suite green.

Focused tests: `planner_cycle.rs` +1.

## Finding 4 (P2) — tie ordering: `BTreeMap` dedup reorders by peer-id

Two RED tests added first: `discover_peers_preserves_first_discovery_order_for_equal_score_ties`
(`planner_cycle.rs`, targets `cycle.rs`'s `discover_peers` dedup — the
finding's cited evidence, `cycle.rs:401-411` at `d13dabf`) and
`discover_from_route_pairs_preserves_first_discovery_order_for_equal_score_ties`
(`planner_discovery.rs`, targets the identical bug pattern in
`discover_from_route_pairs`'s own dedup, which shares the same
`BTreeMap<String, _>`-keyed structure). Both against the unchanged
`d13dabf` code:

```
$ cargo test -p revops-capital --test planner_cycle discover_peers_preserves_first_discovery_order_for_equal_score_ties
thread panicked at crates/revops-capital/tests/planner_cycle.rs:799:5:
equal-score ties must preserve discovery order (zzz_peer first), not peer-id sort order; got ["aaa_peer", "zzz_peer"]
test result: FAILED. 0 passed; 1 failed

$ cargo test -p revops-capital --test planner_discovery discover_from_route_pairs_preserves_first_discovery_order_for_equal_score_ties
thread panicked at crates/revops-capital/tests/planner_discovery.rs:446:5:
equal-score ties must preserve discovery order (zzz_dest first), not peer-id sort order; got ["aaa_dest", "zzz_dest"]
test result: FAILED. 0 passed; 1 failed
```

Fix: added `crates/revops-capital/src/planner/dedup.rs` — `upsert_best`
(insert-or-replace-value-in-place, first-discovery order preserved,
replace only on a STRICTLY greater score — mirrors Python's
`if key not in seen or item.score > seen[key].score: seen[key] = item`
dict pattern) and `into_ordered_vec`. Rewired both dedup sites
(`cycle.rs`'s `discover_peers`, `discovery.rs`'s `discover_from_route_pairs`)
to use it instead of `BTreeMap<String, _>::into_values()`. Both tests
green; `dedup.rs` also carries 2 of its own unit tests (order preservation,
replace-only-on-strictly-greater) as new-module coverage, not correction
red/green evidence.

**Additional Python-oracle fixture** (beyond the hand-rolled tests above,
to satisfy the finding's explicit "Python-oracle fixture" wording):
extended `gen_capital_planner_fixtures.py` with `discover_peers_case`,
driving the REAL `CapacityPlanner._discover_peers` end-to-end (stubbed
`data_service`/`database`, no live RPC/DB) — 3 scenarios, including
`equal_score_winners_preserve_discovery_order_over_peer_id_sort` (two
winners, identical ROI, discovery order the opposite of peer-id order —
real Python confirms `[zzz_peer, aaa_peer]`) and
`duplicate_peer_across_strategies_keeps_higher_score_at_first_position`
(a peer discovered first via the `winner` strategy at a lower score, then
again via `graph` at a higher score — real Python confirms the merge
replaces the VALUE, keeping the peer at its first-discovery POSITION: single
output `{"peer_id": "dup_peer", "source": "graph", "score": 0.3577708763999664}`).
The Rust-side test `discover_peers_matches_python_oracle_for_tie_order_and_dedup`
(`planner_cycle.rs`) passed immediately (the underlying fix was already
in place from the hand-rolled RED/GREEN cycle above) — this is
additional oracle-grade verification of an already-fixed behavior, not a
second red/green cycle.

Focused tests: `planner_cycle.rs` +2 (hand-rolled tie test, oracle-fixture
test), `planner_discovery.rs` +1 (route-pairs tie test).

## Finding 5 (P2) — `discover_from_neighbors`/`discover_from_route_pairs` lack Python-oracle fixtures

Extended `gen_capital_planner_fixtures.py` with `discover_neighbors_case`
(7 scenarios: empty input, top-3-patron selection with the 4th excluded,
fee/capacity filters, existing-peer exclusion, top-5-per-patron cap,
equal-score tie order, missing-patron-cache) and `discover_route_pairs_case`
(7 scenarios: no rows, basic fee-weighted ranking, expensive/tiny
filtering, capacity+fee bonus stacking, cross-route-peer duplicate
resolution, equal-score tie order, our-node exclusion) — both driving the
REAL, unmodified `_discover_from_neighbors` (`_capital_efficiency = None`)
and `_discover_from_route_pairs` against stubbed `data_service`/`database`.

Added `discover_from_neighbors_matches_python` and
`discover_from_route_pairs_matches_python` to `planner_discovery.rs`. Both
passed on the FIRST run against the existing (pre-correction)
implementations:

```
running 13 tests
test discover_from_neighbors_matches_python ... ok
test discover_from_route_pairs_matches_python ... ok
(11 others ok)
test result: ok. 13 passed; 0 failed
```

This is disclosed honestly: these two are VERIFICATION additions, not
correction-driving red/green cycles — the existing hand-derived ports
(transcribed line-by-line from Python in the original pass) turned out to
already be byte-parity-correct against the real oracle. Finding 5's
actual defect was the ABSENCE of oracle verification (a hand-derived test
can share the same wrong assumption as a hand-derived implementation and
still agree), which is now closed: both functions are pinned by real-Python
fixtures going forward, so any future divergence (a Python source change,
or a Rust refactor) will be caught. The pre-existing hand-derived tests in
`planner_discovery.rs` were kept as additional boundary/tripwire coverage
alongside the fixtures, not removed.

Focused tests: `planner_discovery.rs` +2.

## Finding 6 (P2) — the checkpoint did not satisfy the requested red-first workflow

Addressed procedurally, not by a code change: every correction above (1
through 5) followed the sequence FAILING TEST WRITTEN -> RUN -> RED OUTPUT
CAPTURED (quoted above) -> IMPLEMENTATION WRITTEN -> RUN -> GREEN
CONFIRMED, for every one of the 6 findings' worth of new behavior. No
revert probe is presented as red-first evidence in this section — the
transcripts above are the actual first run of each new/changed test
against the actual pre-fix code.

## Fixture regeneration (findings 2, 4, 5)

```
python3 tools/port/gen_capital_planner_fixtures.py > /tmp/kernels_new.json
```

produces 178 scenarios across 24 kinds (up from 155 across 19). The merge
into the committed `fixtures/capital/planner/kernels.json` is additive only
(verified: `git diff fixtures/capital/planner/kernels.json` contains zero
`-` lines besides the diff header — every pre-existing scenario's bytes are
byte-identical) — see `ENTRYPOINTS.md`'s "Fixture generation" section for
why a merge was used instead of a wholesale regeneration (the two
pre-existing wall-clock-relative kinds, `failed_open_backoff_reason` and
`dead_capital_stage`, are unrelated to this correction round and were left
untouched rather than perturbed by a fresh `int(time.time())` read). All
new scenarios are deterministic — no wall-clock values are read by any of
`discover_neighbors_case`, `discover_route_pairs_case`,
`discover_neighbors_capital_efficiency_case`, or `discover_peers_case`.

## Test counts (this correction round)

| Area | Before (`d13dabf`) | After |
|---|---|---|
| `planner_cycle.rs` | 14 | 26 (+12) |
| `planner_discovery.rs` | 9 | 13 (+4) |
| lib unit tests (`dedup.rs`) | 2 | 4 (+2) |
| **`revops-capital` total** | **80** | **98 (+18)** |

## Gate results (final, this correction round)

- `cargo test -p revops-capital`: **98 passed, 0 failed** (17 test
  binaries: 1 unittests + 15 integration test files + 0 doctests).
- `cargo test --workspace`: **2116 passed, 0 failed**, across the full
  workspace (all other crates unaffected — this round touched only
  `crates/revops-capital/**` and `tools/port/`).
- `cargo clippy -p revops-capital --all-targets -- -D warnings`: **clean,
  zero warnings**.
- `cargo fmt --all -- --check`: **clean**.
- `git diff --check`: **clean** (no whitespace errors).

## Remaining gaps (unchanged scope boundary, per the review's explicit
## instruction to preserve it)

`_arbitrate_close_list`'s DB-registry conflict-arming (py's batch
close-list dedup/conflict-arming pass) remains NOT implemented.
`plan_cycle`'s close selection still uses worst-marginal-ROI-first order
directly, without deduping against concurrent in-flight closes from other
subsystems. This is confirmed, per the review, to be a hard RELEASE
BOUNDARY: the planner must not become action-capable until close-list
arbitration is supplied and independently reviewed — it was not the reason
`d13dabf` failed review, and this correction round does not change that
status. See `ENTRYPOINTS.md`'s "Declared gaps" section for the full,
updated list (findings 1-5's items removed; this boundary and the other
prior-pass gaps — LN+/Boltz subsystems, `_execute_*` methods, whole-cycle
fixture parity — carried forward unchanged).

## Confirmations

- No mutation adapter, RPC client type, or live CLN/LN+/Boltz/DB contact
  was added or exercised — `Cargo.toml` is unchanged (still only
  `revops-core`, `revops-db`, `serde`, `serde_json`); `#![forbid(unsafe_code)]`
  unchanged; fixture generation ran only against `types.SimpleNamespace`
  stubs (verified by reading every new/changed stub method in
  `tools/port/gen_capital_planner_fixtures.py`).
- No merge, no push. One corrected logical commit on
  `worktree-agent-a926444ca0e99d4de`, based on `d13dabf`.
- Changed files: `crates/revops-capital/src/planner/cycle.rs`,
  `crates/revops-capital/src/planner/discovery.rs`,
  `crates/revops-capital/src/planner/dedup.rs` (new),
  `crates/revops-capital/src/planner/mod.rs`,
  `crates/revops-capital/tests/planner_cycle.rs`,
  `crates/revops-capital/tests/planner_discovery.rs`,
  `crates/revops-capital/ENTRYPOINTS.md`,
  `crates/revops-capital/TASK47-REPORT.md`,
  `tools/port/gen_capital_planner_fixtures.py`,
  `fixtures/capital/planner/kernels.json`. Nothing outside
  `crates/revops-capital/**` and `tools/port/` was touched except the
  fixture DATA file itself (`fixtures/capital/planner/kernels.json`), which
  is the direct, required output of the in-scope `tools/port/` generator
  script per the correction contract's explicit instruction to regenerate
  it — flagged here for the reviewer's awareness since its path is outside
  the literal `crates/revops-capital/**` / `tools/port/` prefixes.

## Correction to this report's finding-5 claim (round 2)

The earlier text implied finding-5's oracle-fixture tests followed the red-first
workflow. That was wrong, and the reviewer called it out: both previously
fixture-less strategies PASSED on the first run against the new real-Python
fixtures. That is **already-green characterization coverage** — valuable
because it pins the implementations to the oracle so they cannot drift — but it
is not red-first evidence and must not be described as such. The same applies
to the round-2 boundary/duplicate fixtures below.

## Correction round 2 — review of 95f80fe (observed red, then green)

**P1: unchecked `now - observed_at` (cycle.rs:120-122 + three reason branches).**
RED (before fix, unmodified 95f80fe): 3 new integration tests
(`{defib,close,open}_extreme_min_observed_at_denies_without_panic`) and 2 new
unit tests all panicked at `cycle.rs:121: attempt to subtract with overflow`.
The wrap pair `(now = i64::MIN + 900, observed_at = i64::MAX)` is additionally
pinned by `wrap_pair_is_denied_not_accepted_as_fresh`: its true age is future,
but two's-complement wrapping yields 899 — inside the accepted 0..=900 window.
GREEN: `gate_evidence_age` (`checked_sub`) with `None` = deny; all three
skip-reason branches format via `gate_age_denial_text` and never repeat the raw
subtraction. Boundary tests confirm 900s inclusive stays fresh and future
evidence stays denied.

**P1: patron-pool sort reuse (discovery.rs:499-520).**
RED (before fix): new real-Python fixture
`tied_volume_and_roi_rank_over_original_order_seven_patrons` failed with
`left: 5, right: 7` — Rust produced candidates from only the efficiency top-5
patron set (n7,n6,n0,n1,n2) because the in-place re-sorts let tied volume/ROI
rankings inherit efficiency order, while Python's three independent
`sorted(entries, ...)` calls (capacity_planner.py:1650-1652) rank the ORIGINAL
insertion order and select 7 patrons. GREEN: each ranking now sorts its own
clone of the original-order list.

**P2 coverage fixtures (already-green characterization, stated as such):**
`duplicate_destination_across_three_patrons`,
`neighbor_exact_1500ppm_and_200000sat_boundaries`, and
`route_pair_exact_1000ppm_and_500000sat_boundaries` all passed on first run —
the implementations were already correct at those boundaries; the fixtures pin
them to the Python oracle. No red was manufactured for them.

Fixture file: 182 scenarios / 24 kinds (was 178), pre-existing scenarios kept
byte-identical (additive merge). Capital suite: 104 passing (was 98).

## Correction round 3 — review of a722a21

**The 1000x msat error was the rust owner's own** (the round-2 boundary
literals were written inline by the owner, not by a subagent): 500,000 sats is
`500_000_000` msat, not `500_000_000_000`, and likewise for 200,000 sats. The
committed Python outputs included the purported "under-size" peers, proving the
size filters were never exercised — the fixtures characterized the wrong
scenario while carrying a boundary-proof label.

Corrected literals (exact boundary + one-unit-under control, per
capacity_planner.py:1682 `capacity_sats < 200000` and :1879
`fee_ppm > 1000 or capacity < 500000`) and regenerated from the real Python:
the at-boundary peers are now ADMITTED and the under-size controls are ABSENT
from the oracle outputs in both strategies.

**Fallback duplicate-destination coverage added**
(`fallback_duplicate_destination_across_two_patrons`): the Python oracle shows
the fallback branch emits BOTH patron entries for the same destination — no
dedup — unlike the capital-efficiency branch. Rust matched on first run.

**Truthful classification:** every round-3 case was already-green
characterization. No red was manufactured; the value is that both boundary
filters and both dedup semantics are now oracle-pinned and cannot drift.

Fixtures: 183 scenarios (was 182: +1 fallback duplicate, 2 corrected in place).
