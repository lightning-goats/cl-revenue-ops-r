# revops-boltz — entry points and wiring status

This crate ports the **decision kernels** of the Python Boltz swap
subsystem (`BoltzCliManager` in `modules/boltz_manager.py`, ~2,670 LOC, and
`BoltzAutoCycle` in `cl-revenue-ops.py`, ~1,400 LOC). It is a library only:
**nothing in `crates/revops` calls it yet**, and per the task brief this crate
does not modify `crates/revops/src/main.rs` or any other existing crate. This
document is the map from "kernel exists" to "kernel is actually reachable
from a running plugin" — the gap the port brief calls the #1 failure mode in
this project.

Status legend: `[ ]` not wired · `[~]` kernel ported, no caller · `[x]` wired
(none in this crate, by construction).

## What a live adapter needs to provide

None of this exists in this crate or anywhere in `crates/revops` today. A
live adapter crate/module (NOT part of this task) needs to supply:

1. **A real `BoltzCli` implementation** (`revops_boltz::cli::BoltzCli`) that
   shells out to `boltzcli --datadir <dir> [sudo -n -u <user>] <args>` with a
   real `std::process::Command`, timeout enforcement, and stdout/stderr
   capture — porting py `_base_cmd`/`_run` (boltz_manager.py:437-467) minus
   the parts already covered by this crate (`cli::run_json` for the JSON
   decode step).
2. **Journal file I/O**: `open`/`read`/atomic `tmp+rename` writes for
   `cl_revenue_ops_swap_journal.json` and
   `cl_revenue_ops_ignored_external_swaps.json` under the boltzd datadir (py
   `_load_swap_journal`/`_save_swap_journal`/`_load_ignored_external_swaps`/
   `_save_ignored_external_swaps`, boltz_manager.py:1224-1305) — this crate's
   `journal::prune_swap_journal_entries`/`journal::merge_swap_results` are
   the pure logic these I/O functions should call.
3. **The capex budget engine** (`CapexBudgetEngine` — itself unported, see
   `docs/port/PARITY-CHECKLIST.md` Lens 4, "Capital allocation — essentially
   unported") providing `reserve_boltz_swap_budget`/
   `settle_boltz_swap_reservation`/`release_boltz_swap_reservation`/
   `get_channel_budget`/`get_tactical_budget`/`record_boltz_spend`. This
   crate's `budget::reservation_gate`/`budget::finalize_reservation_attempt`
   take the engine's yes/no answer as a plain parameter — they never call
   the engine themselves.
4. **CLN RPC client** (`listpeerchannels`, `listchannels`, `pay`, `decode`,
   `decodepay`, `connect`, `signmessage`, `getinfo`) for the first-hop
   pinning / external-pay logic (py boltz_manager.py:587-871) — NOT ported
   in this crate at all (see "Deliberately not ported" below).
5. **`CapacityPlanner.get_boltz_coordination()` / `rebalancer.get_boltz_coordination()`**
   — neither exists in Rust (Lens 4 again). `autocycle::select_boltz_auto_cycle_mode`
   takes the plan's *shape* (status/executable-count/recommendation-count) as
   plain parameters; it does not build the plan.

## Per-function wiring map

### `cli` module
- `[~]` `BoltzCli` trait — needs a real subprocess adapter (#1 above). No
  caller anywhere in `crates/revops` today.
- `[~]` `run_json` — same; called by nothing outside this crate's own
  tests.

### `address` module
- `[~]` `validate_onchain_address` / `validate_swap_destination` — pure,
  ready to call from a live `withdraw`/`refund`/`claim` adapter (py
  boltz_manager.py:2461-2637's validation-before-subprocess pattern). No
  caller yet — there is no Rust `withdraw`/`refund`/`claim` RPC at all.

### `fee` module
- `[~]` `estimate_swap_fee_sats` / `estimate_reverse_routing_fee_sats` —
  pure, ready to call from budget/journal/quote-enforcement code once a live
  adapter exists. Already consumed internally by `journal::merge_swap_results`
  and `budget::boltz_cost_components` (in-crate callers only).

### `state` module
- `[~]` `is_completed_swap` / `is_error_swap` / `is_terminal_swap` — pure,
  consumed internally by `budget` and `journal`. No external caller.

### `journal` module
- `[~]` `prune_swap_journal_entries` / `index_by_id` / `merge_swap_results` /
  `should_record_spend` — pure. A live adapter must: load the journal file,
  call `merge_swap_results`, for each record with `should_record_spend ==
  true` call the (unported) capex engine's `record_boltz_spend`, then
  `prune_swap_journal_entries` before an atomic tmp+rename write. **None of
  this runs today** — there is no Rust file-backed journal at all.

### `budget` module
- `[~]` `boltz_cost_components` / `budget_status` / `enforce_budget_for_quote`
  — pure aggregation. Needs a live adapter to supply the manual-only,
  journal-augmented swap list (`_listswaps_json(manual_only=True)` +
  `_augment_with_swap_journal`, both unported I/O) and the external-liquidity-
  cost numbers (from the unported rebalancer/capex engine).
- `[~]` `reservation_gate` / `finalize_reservation_attempt` / `finalize_action`
  — pure. Needs the (unported) capex engine's `reserve_boltz_swap_budget`
  call wired around it exactly as py `_open_swap_budget_reservation`/
  `_finalize_swap_budget_reservation` do (boltz_manager.py:1743-1843) — reserve
  BEFORE the create subprocess call, settle-or-release on EVERY exit path
  including exceptions/timeouts (`_pending_swap_budget` stash pattern, py
  boltz_manager.py:280-283).
- `[ ]` Phase 2G governor-facade path (`_governed_open_reservation`, py
  boltz_manager.py:1636-1741) — deliberately NOT ported (see below).

### `autocycle` module
- `[~]` `treasury_recommendation_executable` / `select_boltz_auto_cycle_mode`
  — pure. Needs the (unported) treasury/balance plan builders to supply
  `treasury_status_ok` / `treasury_executable_count` /
  `balance_recommendation_count`.
- `[~]` `AutoCycleErrorState` — pure state machine. A live daemon loop /
  manual-RPC handler needs to own one instance (mirroring
  `_boltz_auto_cycle_state['consecutive_errors']`, a Python module-level
  dict guarded by `_boltz_auto_cycle_state_lock`) and call `on_result`/
  `on_exception` around each cycle run. **No Rust daemon loop exists for
  Boltz at all** — `fee_scheduler.rs` only drives the fee cycle
  (`docs/port/PARITY-CHECKLIST.md` Lens 0: "background-scheduler-loops ...
  fee loop only; flow/rebalance/planner/boltz loops absent").
- `[~]` `cooldown_check` / `cooldown_after_attempt` — pure. A live adapter
  needs to own the per-channel `last_action_ts` map (mirroring Python's
  `_boltz_balance_last_action` dict + `_boltz_balance_lock`) and drive it
  through the pre-claim -> attempt -> `cooldown_after_attempt` sequence
  exactly as `tests/balance_cycle_candidate.rs` demonstrates. No such map
  exists in Rust today.

### `error` module
- `[~]` `CliError` / `CreateOutcome<T>` / `ManualActionOutcome` — pure
  types. A live adapter's create-swap path MUST route a `CliError::Timeout`
  through `CreateOutcome::Unknown` (never treat it as "did not happen");
  its refund/claim path MUST return `ManualActionOutcome::Unverified` on
  any exit-0 call (never a bespoke "success" variant) and require a
  follow-up `swap_status` call before recording spend or closing a journal
  entry. No such adapter exists yet.

## Deliberately NOT ported (and why)

1. **All `boltzcli` argv-construction glue for individual commands**
   (`quote`, `loop_in`, `loop_out`, `chainswap`, `refund`, `claim`,
   `withdraw`, `wallet` ops, `deposit_address`, `backup`/`backup_verify`,
   py boltz_manager.py:1848-2670) — this is orchestration (build an argv
   list, call `cli.run`, shape the JSON reply), not decision logic. The HARD
   RULES ask for pure decision logic with injected I/O; the argv assembly
   itself has almost no branching to test and belongs with the real
   `BoltzCli` adapter, which does not exist yet.
2. **CLN first-hop pinning / external-pay excludes-list logic**
   (`_resolve_peer_channel_ids`, `_resolve_first_hop_target`,
   `_build_first_hop_excludes`, `_pay_invoice_via_first_hop`,
   boltz_manager.py:587-871) — depends on a live CLN RPC client
   (`listpeerchannels`/`pay`/`decode`/`decodepay`) this crate does not have
   access to. Also depends on `_contains_chanids_cln_error` (ported, in
   `parsing.rs`) and the `_reverse_chanids_supported` capability cache
   (stateful, tied to a real `getinfo` probe — not pure).
3. **`BoltzAutoCycle`'s plan BUILDERS**
   (`_build_boltz_expansion_treasury_plan`, `_build_boltz_balance_plan`, and
   the candidate-scoring/profit-guard heuristics inside them,
   cl-revenue-ops.py — grep `_build_boltz_.*_plan`) — these depend on
   `CapacityPlanner`/`profitability_analyzer` output. Per
   `docs/port/PARITY-CHECKLIST.md` Lens 4, `CapacityPlanner` (~4,200 LOC) has
   **zero** Rust port. Porting the plan builders without a real planner
   behind them would mean testing against fabricated planner output — better
   to port the MODE SELECTION and STATE MACHINE that consume a plan's shape
   (done, in `autocycle.rs`) and leave plan construction for when
   `CapacityPlanner` itself is ported.
4. **Phase 2G governor-facade integration** (`_governed_open_reservation`,
   `_boltz_governor_enabled`, `_snapshot_ref`, boltz_manager.py:1607-1741) —
   flag-gated (`econ_governor_enabled_provider`), cross-module wiring into
   `revops-econ`'s `GovernorFacade`/`econ_shadow`/ledger/registry. The
   un-governed reservation path IS ported (`budget::reservation_gate`/
   `finalize_reservation_attempt`); the governed variant needs
   `revops-econ` types this crate deliberately does not depend on (keeping
   `revops-boltz` a narrow, reviewable dependency edge). A future pass can
   add a `governed` module once `revops-econ`'s `GovernorFacade` API for
   this call shape is confirmed stable.
5. **`swap_status`/`swap_history`/`manage_external_pay_ignores`/`get_budget_status`'s
   full read-path composition** (boltz_manager.py:1362-1541, 2394-2457) —
   these are I/O-heavy composition functions (multiple `boltzcli` calls +
   file reads + annotation) with very little decision logic of their own;
   the pure pieces they'd call (`parsing::primary_swap_entry`,
   `journal::index_by_id`, `state::*`) ARE ported. The composition itself
   belongs with a live adapter.
6. **LN+ swap automation** (`modules/lnplus_swaps.py`, ~2,099 LOC) — a
   separate component per `docs/port/port-map.json`'s "Capital allocation
   subsystem" lens, out of scope for this task.
7. **`get_boltz_cost_components`'s swap-list acquisition**
   (`_listswaps_json`, `_augment_with_swap_journal`, boltz_manager.py:891-950,
   1506-1535) — I/O (subprocess + file read); `budget::boltz_cost_components`
   takes the already-acquired list as a parameter instead.

## Why none of this is wired into `crates/revops` yet

Per the task brief: "Do NOT modify `crates/revops/src/main.rs` or other
existing crates ... parallel agents are working." Wiring requires, at
minimum, a real `BoltzCli` adapter, a real journal file store, and the
(currently nonexistent) `CapexBudgetEngine` port — none of which are in
scope for this task. This crate is a dependency-ready library; the next
step is a live-adapter crate (or a module in `crates/revops`) that:

1. implements `cli::BoltzCli` over `std::process::Command`,
2. owns the per-channel cooldown map and the auto-cycle error-state
   instance,
3. is called from a new scheduler loop analogous to `fee_scheduler.rs`
   (none exists for Boltz today), and
4. is exposed via new RPC methods analogous to the existing
   `revenue-boltz-*` Python RPCs (none exist in Rust today — Lens 0:
   "boltz-swap-rpcs-and-auto-cycle: `[ ]`").

Until that adapter exists, this crate is, honestly, exactly the "ported
kernel with no caller" shape the task brief warns against — documented here
rather than hidden.
