# revops-boltz — entry points and wiring status

This crate ports the **decision kernels** of the Python Boltz swap
subsystem (`BoltzCliManager` in `modules/boltz_manager.py`, ~2,670 LOC, and
`BoltzAutoCycle` in `cl-revenue-ops.py`, ~1,400 LOC), AND (as of the wiring
pass documented here) a **wiring layer** on top: a real subprocess
`BoltzCli`, per-command argv construction, `ExecutionMode`-gated command
wrappers, an autocycle balance-cycle-pass driver, and pure RPC response
builders. **Nothing in `crates/revops` calls any of it yet** — per the task
brief this crate still does not modify `crates/revops/src/main.rs` or any
other existing crate. See `REGISTER.md` for the exact paste-in a maintainer
needs to reach this crate from the running plugin.

Status legend: `[ ]` not wired · `[~]` kernel ported, no caller · `[x]`
wired (usable end-to-end from this crate's own driver/commands, but still
NOT called from `crates/revops` — see `REGISTER.md`).

## What a live adapter needs to provide

Wired in this crate now (`[x]` below): a real `BoltzCli`
(`process::ProcessBoltzCli`), argv construction, execution-gated command
wrappers, and a balance-cycle-pass driver. STILL not provided anywhere in
this crate or `crates/revops` (a live adapter — or `REGISTER.md`'s paste-in
plus new code in `crates/revops` — must supply):

1. ~~A real `BoltzCli` implementation~~ **DONE**: `process::ProcessBoltzCli`
   shells out to `boltzcli --datadir <dir> [sudo -n -u <user>] <args>` with
   a real `std::process::Command`, poll-based timeout enforcement (kill and
   reap on timeout), and stdout/stderr capture — ports py `_base_cmd`/`_run`
   (boltz_manager.py:437-467). `process::base_argv` has pure unit coverage;
   `ProcessBoltzCli::run` now has sandbox integration coverage using only
   test-owned temporary fake executables and datadirs. No test invokes a real
   `boltzcli`, service, node, network, or production file. This proves the
   subprocess transport boundary but does not make it reachable from the
   plugin entrypoint.
2. **Journal file I/O**: `open`/`read`/atomic `tmp+rename` writes for
   `cl_revenue_ops_swap_journal.json` and
   `cl_revenue_ops_ignored_external_swaps.json` under the boltzd datadir (py
   `_load_swap_journal`/`_save_swap_journal`/`_load_ignored_external_swaps`/
   `_save_ignored_external_swaps`, boltz_manager.py:1224-1305) — this crate's
   `journal::prune_swap_journal_entries`/`journal::merge_swap_results` are
   the pure logic these I/O functions should call. STILL NOT WIRED:
   `driver::run_balance_cycle_pass` does not call `journal::merge_swap_results`
   itself (it has no file handle to persist the result to) — a live adapter
   must call it around the driver, using each `ExecutedOutcome`.
3. **The capex budget engine** (`CapexBudgetEngine` — itself unported, see
   `docs/port/PARITY-CHECKLIST.md` Lens 4, "Capital allocation — essentially
   unported") providing `reserve_boltz_swap_budget`/
   `settle_boltz_swap_reservation`/`release_boltz_swap_reservation`/
   `get_channel_budget`/`get_tactical_budget`/`record_boltz_spend`. This
   crate's `budget::reservation_gate`/`budget::finalize_reservation_attempt`
   take the engine's yes/no answer as a plain parameter — they never call
   the engine themselves, and `driver::run_balance_cycle_pass` does NOT call
   them either (it only does the simpler pre-reservation-era
   `remaining_budget_sats -= fee` bookkeeping — see the driver's module
   docs). Wiring the atomic reservation into the driver is still open.
4. **CLN RPC client** (`listpeerchannels`, `listchannels`, `pay`, `decode`,
   `decodepay`, `connect`, `signmessage`, `getinfo`) for the first-hop
   pinning / external-pay logic (py boltz_manager.py:587-871) — NOT ported
   in this crate at all (see "Deliberately not ported" below).
   `argv::create_reverse_swap_argv`/`commands::execute_loop_out` cover only
   the plain (non-`--external-pay`) reverse-swap path.
5. **`CapacityPlanner.get_boltz_coordination()` / `rebalancer.get_boltz_coordination()`**
   — neither exists in Rust (Lens 4 again). `autocycle::select_boltz_auto_cycle_mode`
   takes the plan's *shape* (status/executable-count/recommendation-count) as
   plain parameters; it does not build the plan. Nor does
   `driver::run_balance_cycle_pass` — it takes an already-built
   `&[driver::BalanceCandidate]` slice as a parameter (candidate
   construction, currency `"auto"` selection, and wallet-name resolution are
   all the caller's job; `wallet::resolve_wallet_name` is available for the
   last one once the caller has fetched `wallet list --json`).
6. **Live daemon loop wiring**: `driver::run_balance_cycle_pass` is a single
   PASS, not a scheduler. A live adapter needs a loop (analogous to
   `fee_scheduler.rs`, which does not cover Boltz) that owns the
   `HashMap<String, i64>` cooldown map and the `AutoCycleErrorState`
   instance across calls, and ticks on some interval.
7. **RPC method registration**: `commands`/`driver`/`rpc` are all usable
   from `crates/revops` today (this crate exposes them as `pub`), but no
   `.rpcmethod(...)` calls them yet — see `REGISTER.md`.

## Per-function wiring map

### `cli` module
- `[x]` `BoltzCli` trait — `process::ProcessBoltzCli` is the real subprocess
  adapter. Called (via `dyn BoltzCli`) by `commands::execute_*` and
  `driver::run_balance_cycle_pass` within this crate; still no caller in
  `crates/revops` (see `REGISTER.md`).
- `[x]` `run_json` — called by `commands::run_create` (internal to
  `commands.rs`) for every create-type command.

### `address` module
- `[x]` `validate_onchain_address` / `validate_swap_destination` — called by
  `argv::withdraw_gate` and `argv::refund_swap_argv`/`argv::claim_swaps_argv`
  respectively, all exercised through `commands::execute_withdraw`/
  `execute_refund`/`execute_claim`.

### `fee` module
- `[x]` `estimate_swap_fee_sats` — also now called by `rpc::build_swap_history_response`/
  `rpc::build_quote_response` (in addition to the pre-existing in-crate
  `journal`/`budget` callers).
- `[~]` `estimate_reverse_routing_fee_sats` — still no caller; a live
  adapter computes this at the `quote` RPC boundary (before calling
  `rpc::build_quote_response`), which is I/O-adjacent enough (needs the
  configured `routing_fee_limit_ppm`) that it stays outside this crate's
  pure functions.

### `state` module
- `[x]` `is_error_swap` / `swap_entry_error_text` — now also called by
  `driver::create_outcome_to_swap_attempt`/`executed_outcome_of`.
- `[~]` `is_completed_swap` / `is_terminal_swap` — consumed internally by
  `budget`/`journal`/`rpc::build_swap_history_response`; no external caller
  beyond this crate.

### `journal` module
- `[~]` `prune_swap_journal_entries` / `index_by_id` / `merge_swap_results` /
  `should_record_spend` — pure, STILL not called by `driver.rs` (see item 2
  above). A live adapter must: load the journal file, call
  `merge_swap_results` around each `driver::CandidateResult::Attempted`
  outcome, for each record with `should_record_spend == true` call the
  (unported) capex engine's `record_boltz_spend`, then
  `prune_swap_journal_entries` before an atomic tmp+rename write.

### `budget` module
- `[~]` `boltz_cost_components` / `budget_status` / `enforce_budget_for_quote`
  — pure aggregation, now consumed by `rpc::build_budget_response`'s
  parameters (`CostComponents`/`BudgetStatus`), but a live adapter still
  must supply the manual-only, journal-augmented swap list
  (`_listswaps_json(manual_only=True)` + `_augment_with_swap_journal`, both
  unported I/O) and the external-liquidity-cost numbers to call them with.
- `[~]` `reservation_gate` / `finalize_reservation_attempt` / `finalize_action`
  — pure, STILL not called by `driver.rs` (see item 3 above). Needs the
  (unported) capex engine's `reserve_boltz_swap_budget` call wired around it
  exactly as py `_open_swap_budget_reservation`/
  `_finalize_swap_budget_reservation` do (boltz_manager.py:1743-1843) —
  reserve BEFORE the create subprocess call, settle-or-release on EVERY exit
  path including exceptions/timeouts (`_pending_swap_budget` stash pattern,
  py boltz_manager.py:280-283).
- `[ ]` Phase 2G governor-facade path (`_governed_open_reservation`, py
  boltz_manager.py:1636-1741) — deliberately NOT ported (see below).

### `autocycle` module
- `[~]` `treasury_recommendation_executable` / `select_boltz_auto_cycle_mode`
  — pure. Needs the (unported) treasury/balance plan builders to supply
  `treasury_status_ok` / `treasury_executable_count` /
  `balance_recommendation_count`; `driver::run_balance_cycle_pass` takes an
  already-selected candidate list, so it does not call this itself — a live
  adapter calls `select_boltz_auto_cycle_mode` to decide WHICH plan
  (treasury vs balance) to turn into `driver::BalanceCandidate`s.
- `[x]` `AutoCycleErrorState` — `driver::run_balance_cycle_pass` takes
  `&mut AutoCycleErrorState` and calls `on_result` at the end of every pass.
  A live daemon loop / manual-RPC handler still needs to OWN the instance
  across calls (mirroring `_boltz_auto_cycle_state['consecutive_errors']`,
  a Python module-level dict guarded by `_boltz_auto_cycle_state_lock`) and
  call `on_exception()` on a panic/early-return path the driver itself
  cannot see. **No Rust daemon loop exists for Boltz at all** —
  `fee_scheduler.rs` only drives the fee cycle (`docs/port/PARITY-CHECKLIST.md`
  Lens 0: "background-scheduler-loops ... fee loop only;
  flow/rebalance/planner/boltz loops absent").
- `[x]` `cooldown_check` / `cooldown_after_attempt` — driven end-to-end by
  `driver::run_balance_cycle_pass` through the pre-claim -> attempt ->
  restore-or-keep sequence `tests/balance_cycle_candidate.rs` demonstrated
  in isolation. A live adapter still needs to own the actual
  `HashMap<String, i64>` instance ACROSS passes (the driver takes it as
  `&mut`, it does not persist it).

### `error` module
- `[x]` `CliError` / `CreateOutcome<T>` / `ManualActionOutcome` — routed
  end-to-end by `commands.rs` (`run_create`/`run_manual_action`) and
  `driver.rs` (`create_outcome_to_swap_attempt`): a `CliError::Timeout` on a
  create call becomes `CreateOutcome::Unknown` -> `SwapAttemptOutcome::Unknown`
  (cooldown stays burned, budget not decremented further); a refund/claim
  exit-0 call becomes `ManualActionOutcome::Unverified` (never upgraded to a
  success variant) — a live adapter's manual-action RPC handler must still
  add the required follow-up `swap_status` call before recording spend or
  closing a journal entry (this crate does not call `swap_status`
  automatically).

## Deliberately NOT ported (and why)

1. **CLN first-hop pinning / external-pay excludes-list logic**
   (`_resolve_peer_channel_ids`, `_resolve_first_hop_target`,
   `_build_first_hop_excludes`, `_pay_invoice_via_first_hop`,
   boltz_manager.py:587-871) — depends on a live CLN RPC client
   (`listpeerchannels`/`pay`/`decode`/`decodepay`) this crate does not have
   access to. Also depends on `_contains_chanids_cln_error` (ported, in
   `parsing.rs`) and the `_reverse_chanids_supported` capability cache
   (stateful, tied to a real `getinfo` probe — not pure).
2. **`BoltzAutoCycle`'s plan BUILDERS**
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
3. **Phase 2G governor-facade integration** (`_governed_open_reservation`,
   `_boltz_governor_enabled`, `_snapshot_ref`, boltz_manager.py:1607-1741) —
   flag-gated (`econ_governor_enabled_provider`), cross-module wiring into
   `revops-econ`'s `GovernorFacade`/`econ_shadow`/ledger/registry. The
   un-governed reservation path IS ported (`budget::reservation_gate`/
   `finalize_reservation_attempt`); the governed variant needs
   `revops-econ` types this crate deliberately does not depend on (keeping
   `revops-boltz` a narrow, reviewable dependency edge). A future pass can
   add a `governed` module once `revops-econ`'s `GovernorFacade` API for
   this call shape is confirmed stable.
4. **`swap_status`/`swap_history`/`manage_external_pay_ignores`/`get_budget_status`'s
   full I/O ACQUISITION** (the `boltzcli` subprocess calls themselves, the
   swap-journal/ignored-external-swaps file reads, and the unified-budget/
   external-liquidity-cost provider calls, boltz_manager.py:1362-1541,
   2394-2457) — the pure SHAPING half of each of these is now ported
   (`rpc::build_status_response`/`build_budget_response`/
   `build_swap_history_response`/`build_quote_response`); the I/O to acquire
   their inputs still belongs with a live adapter.
5. **LN+ swap automation** (`modules/lnplus_swaps.py`, ~2,099 LOC) — a
   separate component per `docs/port/port-map.json`'s "Capital allocation
   subsystem" lens, out of scope for this task.
6. **`get_boltz_cost_components`'s swap-list acquisition**
   (`_listswaps_json`, `_augment_with_swap_journal`, boltz_manager.py:891-950,
   1506-1535) — I/O (subprocess + file read); `budget::boltz_cost_components`
   takes the already-acquired list as a parameter instead.
7. **`backup`/`backup_verify`** (boltz_manager.py:2647-2670) — no
   `argv`/`commands` wrapper exists for `swapmnemonic get`; low-risk
   (read-only except for the mnemonic-export warning path) but simply not
   part of this pass's command list (status/quote/create/claim/refund/
   wallet/deposit/withdraw).

## What's still needed to reach `crates/revops`

This crate's wiring layer (`process`/`argv`/`wallet`/`commands`/`driver`/
`rpc`/`execution`) is now usable end-to-end from Rust code — see
`REGISTER.md` for exactly what a maintainer pastes into `crates/revops` to
reach it from a running plugin. What is NOT included in this pass, still
needed for full parity:

1. journal file I/O + capex budget engine wiring around `driver.rs` (items
   2-3 in "What a live adapter needs to provide", above),
2. a live daemon loop owning the cooldown map / error-state instance and
   ticking `driver::run_balance_cycle_pass` on an interval (analogous to
   `fee_scheduler.rs`, which does not cover Boltz today), and
3. the actual `.rpcmethod(...)` registration in `crates/revops/src/main.rs`
   (see `REGISTER.md` — deliberately NOT done by this crate itself, per the
   task brief's crate-isolation rule).
