# revops-lnplus — entry points

Port of `modules/lnplus_swaps.py` (~2,099 LOC). Every decision kernel is
ported and tested (133 tests, all green — see the crate root doc comment
in `src/lib.rs`). **Nothing in this crate is wired into the plugin.** Per
HARD RULE 1, this file is the map from "kernel exists" to "kernel is
actually called by something live" — the exact gap the 2026-07-27 fee
audit found for A1-A3 and that PARITY-CHECKLIST.md's Lens 4 already flags
for this whole subsystem ("essentially unported... no manager, no
planner, no automation").

Legend: `[ ]` not wired · `[x]` wired · **BLOCKED** = cannot be wired until
a named prerequisite exists.

## 1. Scheduled loops (the two per-pass entry points)

- [ ] **Evaluator pass** (hourly or on a configurable interval, matches
      py's fee-cycle-adjacent scheduling) — calls
      [`evaluator::run_cycle`]. The caller must assemble `CycleInputs` by
      calling, in this order (matches py `run_cycle` 292-315):
      1. `breaker::tripped_message` (or read `LnPlusDb::get_breaker`)
      2. `LnPlusDb::inflight_swaps().is_empty() == false` for `has_inflight`
      3. `ChainPort::opening_feerate_perkw()`
      4. `reconcile::reconcile_ok(...)` — **only after** step 1-3 pass,
         matching py's short-circuit order exactly
      5. `LnPlusApi::get_applicable_swaps()`
      6. `evaluator::capture_peers_with_channels` /
         `evaluator::fetch_our_id` (PR 3d: cache `our_id` as a process
         constant across passes, do not refetch every cycle)
      7. `best_regular_ev` from the capacity planner — **BLOCKED**, see
         §3 below
      8. `ChainPort::confirmed_unreserved_sats()`
      9. `PlannerPort::capex_fleet_exploration_budget()` — **BLOCKED**,
         see §3

- [ ] **Watcher pass** (hourly, independent of the evaluator pass) —
      calls [`watcher::run_watcher_once`]. Needs `LnPlusApi`, `ChainPort`,
      `PolicyPort`, `IgnorePeerPort` (optional), `Logger`,
      `open::OpenExecParams` (from the capacity planner — **BLOCKED**,
      §3), `pending_timeout_days` (from config — **BLOCKED**, §5).
      py's `threading.Lock` "watcher already running" reentrancy guard
      (py 1300) is NOT modeled here — the wiring layer must serialize
      calls to `run_watcher_once` itself (a `Mutex` around the call site
      is sufficient; this crate assumes non-reentrant callers throughout,
      same as every other pure-decision crate in this workspace).

## 2. Injected trait implementations — none exist yet

Every port in `src/ports.rs` needs exactly one production implementation,
and per HARD RULE 2 **none of them may be written or tested in this
crate**:

- [ ] `ports::LnPlusApi` — a real HTTPS client for
      `https://lightningnetwork.plus/api/2`, signmessage-based auth
      (`validation::validate_challenge` — gate 15 — is ported and ready to
      call; the actual `signmessage` CLN RPC call and HTTP transport are
      not). Python's implementation (`LNPlusClient`, py 72-229) is stdlib
      `urllib`; the Rust equivalent needs an HTTP crate not currently a
      workspace dependency (this crate deliberately adds none).
- [ ] `ports::LnPlusDb` — **BLOCKED**: the `lnplus_swaps` / `lnplus_peers`
      tables do not exist in `revops-db`'s schema yet (PARITY-CHECKLIST
      §3 Lens 5 confirms: "config-planner-lnplus-policies: peer policies
      only"). A production impl needs those tables added to `revops-db`
      (out of scope for this crate — do not modify `revops-db` per the
      task's hard rule). Note the breaker persistence question this
      raises: `get_breaker`/`set_breaker`/`clear_breaker` need a
      serialization format for the structured `breaker::BreakerCause` —
      Python only ever stored a plain string in `config_overrides`
      (`_lnplus_breaker`). If Python and Rust ever read the SAME database
      concurrently, the two must agree on a wire format; if Rust owns the
      table exclusively post-cutover this is a non-issue. Flagging now so
      it isn't rediscovered as an incident later.
- [ ] `ports::ChainPort` — should wrap `revops-rpc`'s existing
      threadsafe CLN RPC proxy (`getinfo`, `listpeerchannels`,
      `feerates`, `listfunds`, `connect`, `fundchannel`). This is the
      most straightforward of the five — `revops-rpc` already exists and
      is exactly the right shape.
- [ ] `ports::PolicyPort` — should wrap whatever the Rust plugin uses for
      peer tags / operator bans. `revops-analytics::policy` has
      `FeeStrategy`/`PeerPolicy` concepts (see `close_protection`'s
      `policy_close_block`, which already reasons about `no_close`/
      `protect` tags) but no `add_tag`/`remove_tag`/`is_peer_banned`
      surface — that surface needs to be added wherever the Rust plugin's
      operator-tag store lives (likely `revops-db`).
- [ ] `ports::PlannerPort` — **BLOCKED**, see §3.
- [ ] `ports::IgnorePeerPort` — optional in Python (`ignore_peer_fn=None`
      is a valid construction); no Rust equivalent mechanism identified.
      Fine to wire as `None` initially — `finalize::finalize`'s defection
      path already handles that (no ignore call, same as Python).
- [ ] `ports::Logger` — trivial: wrap whatever `log`/`tracing` macro the
      Rust plugin already uses elsewhere (see how `revops-fees` does it).

## 3. Hard blocker: CapacityPlanner does not exist in Rust

`PlannerPort::calculate_open_ev` / `estimate_open_cost` /
`score_candidate` / `capex_fleet_exploration_budget` all delegate to
Python's `CapacityPlanner` (`_calculate_open_ev`, `_estimate_open_cost`,
`_score_candidate`, `_capex_engine.get_fleet_exploration_budget`).
Per `docs/port/port-map.json` lens 4, `CapacityPlanner` (~4,200 LOC) has
**no Rust port at all**. This means:

- The evaluator's EV math ([`evaluator::swap_ev`]) is fully ported and
  tested against a `FakePlanner`, but cannot run against real numbers
  until `CapacityPlanner` exists in Rust.
- `open::OpenExecParams.estimated_cost_sats` /
  `effective_budget_sats` / `budget_since_timestamp` — Python's
  `_estimate_open_cost_fn` / `_budget_params_fn` injection points — have
  no real source. `open::DEFAULT_OPEN_COST_SATS` (2500, py 672) is
  available as a fallback constant, matching Python's own fallback
  behavior when the capacity planner isn't wired.

This is the single largest reason "kernel ported" and "feature working"
are different claims for this crate, and it is not something this task
could resolve — porting `CapacityPlanner` is its own multi-thousand-LOC
task (see port-map lens 4's sibling components: `BoltzCliManager`,
`BoltzAutoCycle`, all likewise unported).

## 4. Operator RPC surface — none ported

Python exposes these as `cl-revenue-ops.py` RPC methods backed by this
module (method names inferred from log/docstring references in
`lnplus_swaps.py`; the RPC registration itself lives in the plugin entry
point, not this file):

- [ ] `revenue-lnplus-status` -> [`watcher::get_status`] (ported, pure,
      ready to call once `LnPlusDb` exists)
- [ ] `revenue-lnplus-backfill` -> [`backfill::backfill_from_lnplus`]
      (ported; the RPC handler must fetch `MySwaps` via `LnPlusApi` first,
      matching py's `my=None -> fetch` default)
- [ ] `revenue-lnplus-breaker-clear` -> [`breaker::clear_and_persist`]
      (ported; this is the ONLY sanctioned way to clear a
      never-auto-clear breaker cause — see `breaker.rs`'s module doc)
- [ ] `revenue-lnplus-abandon` (mark a row `failed`/`withdrawn` by
      operator request) — **not ported**: this method's Python home
      wasn't in `lnplus_swaps.py` itself (it's a thin RPC-layer wrapper
      around `db.lnplus_update_swap`); the equivalent in Rust is just
      `LnPlusDb::update_swap` with an `SwapPatch::default().status(...)`,
      already available once `LnPlusDb` exists — no new kernel needed.
- [ ] `revenue-lnplus-*` config get/set — **BLOCKED**, see §5.

## 5. Config wiring — no `lnplus_*` options registered in Rust

[`config::LnPlusConfig`] collects every field this module reads, each
annotated with its Python `getattr(cfg, ...)` call site and default. None
of these are registered as CLN plugin options in `options_table.rs` /
resolved in `config_resolve.rs` yet (PARITY-CHECKLIST §3 Lens 6 shows
only fee-relevant keys wired through `revenue-config`). Wiring
`LnPlusConfig::default()` to real `setconfig`/option values is
straightforward once someone adds the 15 `lnplus_*` options to the
manifest — this crate does not gate that on anything else.

## 6. Governed economics (Phase 2F) — deliberately not ported

Python's `_governed_reserve_spend` / `_lnplus_governor_enabled`
(py 703-833) route the swap-open budget reservation through
`revops-econ`'s `GovernorFacade` when a feature flag is set. This crate's
`open::execute_swap_open` only calls the legacy `LnPlusDb::reserve_spend`
path (matches Python's non-governed branch exactly, including its
fail-closed-on-budget-refusal semantics). The governed branch was NOT
ported because:

1. It is flag-gated and OFF by default in Python (matches
   `econ_governor_lnplus_enabled` defaulting false).
2. PARITY-CHECKLIST §3 Lens 3 already tracks this exact gap workspace
   -wide: *"governed execution call sites... fee intents wired;
   rebalance/boltz/planner intents exist as arbiter POLICY STRINGS with
   no producer"* — lnplus joins that same list, it does not need its own
   bespoke solution invented here.
3. Porting it faithfully requires constructing `revops_econ::intents`
   envelopes with the exact `OPEN_CHANNEL` / `CONTRACT_OBLIGATION` shape
   Python's version builds (py 736-794) — that is real, careful work
   (money-moving, `reversible: false`) that deserves its own task with
   its own review, not a rider on this port.

If/when lnplus is cut over to governed economics, the addition point is
exactly one call site: `open::execute_swap_open`'s
`db.reserve_spend(&reserve_req)` call, gated behind a new
`LnPlusConfig::econ_governor_lnplus_enabled` field this struct does not
yet have.

## 7. What this crate deliberately simplifies (not blockers, just notes)

- **Concurrency guards** (py's `threading.Lock` in `run_watcher_once`,
  the C-7 double-checked lock around backfill) are wiring-layer concerns,
  not decision logic — every function in this crate assumes it is called
  non-reentrantly by a correctly-sequencing caller, same convention as
  every other pure-decision crate in this workspace.
- **In-memory-only observability state** (`_last_watcher_pass`,
  `_recent_notifications`, py 859/2106) is owned by the plugin/orchestrator
  in this port, not tracked inside `revops-lnplus` — [`watcher::get_status`]
  takes it as an omitted concern (see its doc comment) and
  [`watcher::poll_notifications`] returns the "keep" list for the caller
  to retain, rather than mutating shared state itself.
- **DataService cache-coherent adapter vs. raw RPC** (Python prefers
  `self.data_service` when wired, falls back to `self.rpc`) collapses to
  a single `ChainPort` trait — which concrete implementation is injected
  (cache-coherent or not) is the wiring layer's choice, not this crate's.
- **Live-RPC fallback in `check_existing_channel`** (py's per-swap
  `listpeerchannels(peer)` call when the pass-frozen capture failed) is
  NOT modeled — [`evaluator::check_existing_channel`] fail-opens (admits
  the swap) when no frozen set is supplied, exactly matching Python's
  fail-open OUTCOME without re-issuing a live RPC call per swap. Real
  state is still re-checked at open time either way (I5(a)'s own
  justification in both the Python comment and this port's doc comment).
