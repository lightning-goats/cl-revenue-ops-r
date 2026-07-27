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

## Gate results (final, pre-commit)

- `cargo test -p revops-capital`: **80 passed, 0 failed**.
- `cargo clippy -p revops-capital --all-targets -- -D warnings`: **clean,
  zero warnings**.
- `cargo fmt --all -- --check`: **clean** (ran `cargo fmt --all` once
  during development to normalize the new files' formatting, then
  reverified `--check` clean).
- `cargo build --workspace`: **succeeds**.
