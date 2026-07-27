# revops-capital — entry points

This crate is a **decision kernel with no caller today**. Nothing in
`crates/revops/src/main.rs` or any other crate calls into it. This file
enumerates every entry point the plugin would need to wire up, so this port
does not become a fourth instance of the "ported kernel, never called"
failure mode this project has been burned by three times already.

Per the task's hard rules, this crate makes **zero** live Lightning RPC
calls, opens **zero** production databases, and sends **zero**
payments/opens/closes. Every function below takes pre-fetched, typed
evidence and returns a decision or a plan; wiring the evidence-gathering
(RPC calls) and the plan-execution (RPC calls) is explicitly **NOT DONE**
and is out of scope for this pass. `#![forbid(unsafe_code)]` at the crate
root and the total absence of an RPC client dependency anywhere in
`Cargo.toml` make this a structural guarantee, not a convention.

## What exists

### Capex budget (unchanged from the prior pass)

| Module | Ports | Purity |
|---|---|---|
| `capex::compute_allocations` | `CapexBudgetEngine.compute_allocations` (py `modules/capex_budget.py` 142-330) | Pure function of `(CapexEvidence, CapexConfig)` |
| `capex::attribute_boltz_cost` | `CapexBudgetEngine.attribute_boltz_cost` (py 356-373) | Pure |
| `boltz_reservation::{reserve,settle,release}_boltz_swap_reservation`, `record_boltz_spend` | `CapexBudgetEngine`'s Boltz reservation lifecycle (py 375-511) | Orchestrates `&mut revops_db::budget::BudgetDb` — not pure, but never touches a production DB path |

### Capacity planner — pure kernels (prior pass)

| Module | Ports | Purity |
|---|---|---|
| `planner::portfolio_gate::check_portfolio_balance_gate` | `CapacityPlanner._check_portfolio_balance_gate` (py 321-361) | Pure |
| `planner::close_fee::*` | close-fee reserve/cap/feerange/extraction (py 2506-2597, 2968-3037) | Pure |
| `planner::dead_capital::advance_dead_capital_stage` | the stage-transition machine inside `_build_dead_capital_loser` (py 1241-1382) | Pure |
| `planner::ev::{calculate_open_ev, calculate_redeployment_ev, calculate_recycle_ev, is_recycle_eligible}` | py 1949-2012, 2859-2966 | Pure |
| `planner::gates::{failed_open_backoff_reason, peer_exposure_cap_reason}` | py 2410-2504 | Pure |
| `planner::scoring::{normalize_candidate_scores, apply_pool_quotas}` | py 2232-2304 | Pure |

### Capacity planner — orchestration (THIS pass, hexmem task 47)

| Module | Ports | Purity |
|---|---|---|
| `planner::winners::identify_winners` | `_identify_winners` (py 805-899) | Pure, evidence-in |
| `planner::losers::identify_losers` | `_identify_losers` (py 901-1129) + the `_build_dead_capital_loser` gate/record-assembly (py 1251-1382, composed with `dead_capital::advance_dead_capital_stage`) | Pure, evidence-in |
| `planner::candidate_score::score_candidate` | `_score_candidate` (py 2110-2201) | Pure, evidence-in |
| `planner::sizing::size_channel` | `_size_channel` (py 2757-2806) | Pure, evidence-in |
| `planner::demand_flow::{classify_peers, find_sink_adjacent_candidates}` | `DemandFlowClassifier` (py `modules/demand_flow.py` 56-95, 193-233) | Pure |
| `planner::discovery::discover_from_winners` | Strategy 1 (py 1485-1497) | Pure, evidence-in. **Fixture-verified.** |
| `planner::discovery::discover_from_neighbors` | Strategy 2's no-capital-efficiency FALLBACK branch (py 1516-1566) | Pure, evidence-in. **Fixture-verified** (Task 47 correction round 1, finding 5). |
| `planner::discovery::discover_from_neighbors_capital_efficiency` | Strategy 2's capital-efficiency-aware branch: `_build_neighbor_patron_pool`, `_build_neighbor_candidate`, `_discover_second_degree_neighbors` (py 1568-1760) | Pure, evidence-in. **Fixture-verified** (Task 47 correction round 1, finding 2 — covers fallback/first-degree/second-degree cases). |
| `planner::discovery::discover_from_graph` | Strategy 3 (py 1762-1806) | Pure, evidence-in. **Fixture-verified.** |
| `planner::discovery::discover_from_route_pairs` | Strategy 5 (py 1808-1904) | Pure, evidence-in. **Fixture-verified** (Task 47 correction round 1, finding 5). |
| `planner::discovery::discover_from_demand_flow` | Strategy 6 (py 1917-1947) | Pure, evidence-in. **Fixture-verified** (via `demand_flow`'s two functions). |
| `planner::cycle::discover_peers` | `_discover_peers` (py 2714-2755): runs all five strategies (branch-selecting Strategy 2's variant per `DiscoveryEvidence::neighbor_capital_efficiency`), normalizes, dedups (first-discovery order preserved — Task 47 correction round 1, finding 4), enriches, applies pool quotas | Pure, evidence-in. **Fixture-verified** for the cross-strategy merge/tie-order behavior. |
| `planner::recycle::apply_redeployment_ev_demotion` | `_apply_redeployment_ev_demotion` (py 1454-1483) | Pure, composes `ev::calculate_redeployment_ev` |
| `planner::recycle::find_best_recycle_pair` | the selection core of `_evaluate_recycle_opportunities` (py 2084-2108) | Pure, composes `ev::calculate_open_ev`/`calculate_recycle_ev` |
| `planner::cycle::plan_cycle` | `execute_cycle` (py 363-786), **minus every `_execute_*` RPC call** | Pure, evidence-in/plan-out. See "plan_cycle: what's real and what's hoisted" below. |

Every public item has a `py <file>:<line>` doc comment. **98 tests** (up
from 80 at the reviewed `d13dabf` checkpoint, up from 39 before the
original orchestration pass), covering the full pure-kernel and
orchestration surface. Fixture counts: `fixtures/capital/capex/allocations.json`
(26 scenarios, unchanged) and `fixtures/capital/planner/kernels.json`
(**178 scenarios**, up from 155, across 24 function kinds — see
`tools/port/gen_capital_planner_fixtures.py` for the full list; the four
new kinds are `discover_from_neighbors`, `discover_from_route_pairs`,
`discover_from_neighbors_capital_efficiency`, and `discover_peers`).
`crates/revops-capital/TASK47-REPORT.md` has the per-area fixture/test
breakdown and the captured RED output for every revert tripwire AND every
Task 47 correction-round-1 finding.

## `plan_cycle`: what's real and what's hoisted

[`planner::cycle::plan_cycle`] is a REAL composition — it calls
`identify_winners`, `identify_losers`,
`recycle::apply_redeployment_ev_demotion`, `discover_peers` (which itself
calls all five discovery strategies, `normalize_candidate_scores`,
`score_candidate`, `apply_pool_quotas`), `gates::failed_open_backoff_reason`,
`gates::peer_exposure_cap_reason`, `sizing::size_channel`,
`ev::calculate_open_ev`, `portfolio_gate::check_portfolio_balance_gate`, and
`recycle::find_best_recycle_pair` — not a shell that returns evidence
unchanged. `crates/revops-capital/tests/planner_cycle.rs` has one revert
tripwire per one of those calls (see TASK47-REPORT.md for the captured RED
output from deliberately deleting each one).

What `plan_cycle` does NOT do, by design (evidence is hoisted to the
caller rather than the logic being re-derived):
- **Fee gate** (`_check_fee_gate`, py 2330-2367): budget/capex-engine
  dependent; caller supplies `fee_gate_ok`/`fee_gate_reason`.
- **Cooldown / recently-attempted / defib-policy / close-allowed / safety-guard
  checks** (`_check_cooldown`, `_defib_recently_attempted`,
  `_check_defib_allowed`, `_check_close_allowed`, `_check_safety_guards`):
  policy-manager/DB dependent; caller supplies a per-peer `DefibGate`/
  `CloseGate`/`OpenGuard` entry with an `observed_at` timestamp and an
  `Option<String>` reason per sub-gate (`None` = allowed). Task 47
  correction round 1, finding 1: a peer with NO entry in the evidence map,
  or an entry whose `observed_at` is more than `GATE_EVIDENCE_MAX_AGE_SECS`
  (900s) from `CycleEvidence::now` (or in the future), is DENIED with an
  actionable reason — missing/stale evidence never defaults to "allowed".
- **`_arbitrate_close_list`** (py 3379+): a DB-registry-backed batch
  dedup/conflict-arming pass. NOT re-implemented — `plan_cycle` uses the
  worst-marginal-ROI-first order directly, which is what
  `_arbitrate_close_list` preserves among survivors anyway, but it does
  NOT dedup against concurrent in-flight closes from other subsystems.
- **LN+ swap evaluation** (py 648-673): a whole separate, unported
  subsystem (`LNPlusSwapAutomation`). Not present in `plan_cycle` at all.
- **Exploration-budget / capex-engine interaction beyond a running total**:
  `plan_cycle` takes `exploration_budget_sats` as a single starting number
  and decrements it per open; Python's capex-engine `get_fleet_exploration_budget()`
  call and its own error handling are not re-derived.

## Entry points this plugin will need — NONE are wired yet

Every one of these requires a caller in `crates/revops/` (or wherever the
capital-allocation loop eventually lives) that:
1. gathers the evidence via CLN RPC / `revops-db` reads,
2. calls the pure function(s),
3. for money-committing decisions, executes the RPC (fundchannel/close/
   Boltz swap) and reserves/settles/releases budget around it.

None of step 1 or step 3 exists. This crate only does step 2. `plan_cycle`
is the single evidence-in/plan-out entry point a future caller would drive
once per timer tick; see its doc comment for the full `CycleEvidence`
shape and which sub-evidence gathers map to which Python RPC/DB call.

## Declared gaps (honest, not swept under "future work")

Resolved in Task 47 correction round 1 (see `TASK47-REPORT.md`'s
"Correction round 1" section for red/green evidence): the fail-open safety
gates (finding 1), `discover_from_neighbors`'s capital-efficiency-aware
branch (finding 2), multi-open capital accounting reusing the initial
balance (finding 3), tie-ordering in candidate dedup (finding 4), and the
missing Python-oracle fixtures for `discover_from_neighbors`/
`discover_from_route_pairs` (finding 5) are no longer gaps.

Remaining gaps:

- **`_arbitrate_close_list`'s DB-registry conflict-arming is not
  implemented** (see "plan_cycle: what's real and what's hoisted" above).
  This is a hard release boundary, confirmed by the Task 47 review: the
  planner must not become action-capable until close-list dedup/conflict
  arbitration is supplied and independently reviewed. `plan_cycle`'s close
  selection uses worst-marginal-ROI-first order directly; it does NOT dedup
  against concurrent in-flight closes from other subsystems the way
  `_arbitrate_close_list` does.
- **LN+ swap evaluation, `BoltzCliManager`, `BoltzAutoCycle`,
  `LNPlusSwapAutomation`** — unchanged from the prior pass, still entirely
  unported (own porting projects).
- **Every `_execute_*` method** (`_execute_open`/`_execute_close`/
  `_execute_defibrillation`/`_rpc_fundchannel`/`_rpc_close`, py 3054-3973) —
  forbidden by hard rule #2, never present in this crate.
- **Whole-cycle fixture parity for `plan_cycle` against Python's
  `execute_cycle` was not attempted** — Python's orchestration is
  RPC/DB-heavy end to end in ways outside this crate's evidence contract
  (arbitration registry, policy manager, LN+ evaluator, capex engine).
  Every sub-decision `plan_cycle` calls IS fixture-verified in isolation
  (including, as of this correction round, the cross-strategy merge order
  in `discover_peers` — see `discover_peers_matches_python_oracle_for_tie_order_and_dedup`);
  the orchestration wiring itself is proven by the revert-tripwire tests in
  `tests/planner_cycle.rs`, not by byte-for-byte Python parity on the full
  cycle. Unit and kernel parity may support this checkpoint; it is not
  evidence that a later live adapter is safe.

## Fixture generation

`tools/port/gen_capex_fixtures.py` and
`tools/port/gen_capital_planner_fixtures.py` import
`modules/capex_budget.py` / `modules/capacity_planner.py` /
`modules/profitability_analyzer.py` / `modules/demand_flow.py` directly
from `/home/sat/bin/cl_revenue_ops` (unmodified) and drive the REAL Python
methods against constructed stub `profitability`/`database`/`config`
objects (plain `types.SimpleNamespace` duck-typing — no CLN, no real DB).
Output is committed at `fixtures/capital/capex/allocations.json` (26
scenarios) and `fixtures/capital/planner/kernels.json` (178 scenarios
across 24 function kinds). Re-run either script and diff against the
committed fixture to re-verify parity if the Python source changes.

Regeneration commands (Task 47 correction round 1):

```
python3 tools/port/gen_capital_planner_fixtures.py > /tmp/kernels_new.json
```

then merge the new `discover_from_neighbors` /
`discover_from_route_pairs` / `discover_from_neighbors_capital_efficiency`
/ `discover_peers` scenarios into the committed file (this correction round
merged rather than wholesale-replaced, to avoid perturbing the pre-existing
`failed_open_backoff_reason`/`dead_capital_stage` scenarios' wall-clock-relative
`created_at`/`entered_at` values, which are regenerated at whatever moment
the script runs — a pre-existing, out-of-scope non-determinism in those two
kinds, not introduced by this round; every scenario ADDED by this round is
fully deterministic, no wall-clock values).
