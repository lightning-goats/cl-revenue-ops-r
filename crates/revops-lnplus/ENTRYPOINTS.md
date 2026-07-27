# revops-lnplus — entry points

Port of `modules/lnplus_swaps.py` (~2,099 LOC). Every decision kernel is
ported and tested (133 tests, all green — see the crate root doc comment
in `src/lib.rs`). Per HARD RULE 1, this file is the map from "kernel
exists" to "kernel is actually called by something live" — the exact gap
the 2026-07-27 fee audit found for A1-A3 and that PARITY-CHECKLIST.md's
Lens 4 already flags for this whole subsystem ("essentially unported... no
manager, no planner, no automation").

**2026-07-27 wiring-layer update:** the WIRING LAYER now exists inside
this crate (`src/exec_mode.rs`, `gated.rs`, `http.rs`, `sqlite_db.rs`,
`loop_drivers.rs`) — `evaluator_pass`/`watcher_pass` are real, tested,
callable functions with a real (if transport-less) `LnPlusApi` and a real
`LnPlusDb` behind them. **This crate still has NO caller** — nothing in
`crates/revops` invokes `loop_drivers::evaluator_pass`/`watcher_pass` yet.
That final step (registering RPC methods, spawning the loops, resolving
config) is `crates/revops`-side work this task's hard rule ("do NOT modify
any crate other than revops-lnplus") explicitly could not do — see
`REGISTER.md` for the exact paste-in snippets. The per-item status below is
updated accordingly; unchanged items still point at real, unresolved gaps.

Legend: `[ ]` not wired · `[x]` wired (inside this crate) · **BLOCKED** =
cannot be wired until a named prerequisite exists.

## 1. Scheduled loops (the two per-pass entry points)

- [x] **Evaluator pass** — [`loop_drivers::evaluator_pass`] assembles
      `CycleInputs` in exactly the order this section used to specify (that
      ordering logic now lives there, not in a future caller's head) and
      calls [`evaluator::run_cycle`] itself. Steps 7 (`best_regular_ev`)
      and 9 (`capex_fleet_exploration_budget`, called but returns whatever
      the injected `PlannerPort` says — still meaningless without a real
      planner) remain **BLOCKED** on §3; `evaluator_pass` takes
      `best_regular_ev` as an explicit caller-supplied parameter rather
      than pretending to compute it. **Still no caller in `crates/revops`**
      — see `REGISTER.md` §8.
- [x] **Watcher pass** — [`loop_drivers::watcher_pass`] wraps
      [`watcher::run_watcher_once`]; [`loop_drivers::WatcherLoop`] is the
      non-reentrancy guard this section used to say the wiring layer must
      build (py's `threading.Lock`, py 1300) — a real `Mutex`, `try_lock`,
      skip-not-queue. `open::OpenExecParams`/`pending_timeout_days` are
      still caller-supplied (§3/§5 respectively). **Still no caller in
      `crates/revops`** — see `REGISTER.md` §8.

Both passes are now **`ExecutionMode`-gated**: every mutating
`LnPlusApi`/`ChainPort` call is wrapped in [`gated::GatedLnPlusApi`] /
[`gated::GatedChainPort`] and refuses to reach the injected port unless
called with `ExecutionMode::Armed` — see `exec_mode.rs`'s module doc and
`REGISTER.md` §6.

## 2. Injected trait implementations

- [x] `ports::LnPlusApi` — [`http::LnPlusApiClient`] is a complete
      production implementation (auth flow, `_unwrap_list_envelope`, C-4
      structured-422 parsing, all ported and tested against a fake
      transport). It is generic over [`http::HttpTransport`] /
      [`http::Signer`], and **no concrete implementation of either ships in
      this crate** — no HTTP client crate exists anywhere in the workspace
      lockfile, and shipping one here would make "no test may make a live
      HTTP request" a matter of discipline instead of construction. See
      `REGISTER.md` §2 for the ~15-line `ureq`-backed adapter a maintainer
      adds in `crates/revops`.
- [x] `ports::LnPlusDb` — [`sqlite_db::SqliteLnPlusDb`], built INSIDE this
      crate (the blocker this line used to describe — "needs those tables
      added to `revops-db`" — is resolved by NOT adding them to
      `revops-db` at all; see `sqlite_db.rs`'s module doc). The breaker
      wire-format question flagged below is answered there too: JSON,
      documented as Rust-only (not Python-compatible), fail-open on a
      foreign/malformed read.

### SQLite ownership boundary

SqliteLnPlusDb is a write-capable store and MUST be opened only on the
Rust-owned observer database. It must never be pointed at the Python
production revenue_ops.db while Python is authoritative. The shared
_lnplus_breaker key has incompatible Python plain-text and Rust structured
JSON encodings; cross-owner migration requires an explicit one-time format
conversion and may not occur implicitly at read time.

- [ ] `ports::ChainPort` — still unwritten. Should wrap `revops-rpc`'s
      existing threadsafe CLN RPC proxy (`getinfo`, `listpeerchannels`,
      `feerates`, `listfunds`, `connect`, `fundchannel`) — see
      `REGISTER.md` §5 for the sync/async bridging this needs (every port
      in this crate is synchronous by design; the CLN RPC client available
      to `crates/revops` is async).
- [ ] `ports::PolicyPort` — still unwritten; unchanged from before this
      task. `revops-analytics::policy` has `FeeStrategy`/`PeerPolicy`
      concepts but no `add_tag`/`remove_tag`/`is_peer_banned` surface —
      needs to be added wherever the Rust plugin's operator-tag store
      lives (likely `revops-db`). `REGISTER.md` §10 gives a stub shape
      good enough to unblock the watcher pass in the meantime.
- [ ] `ports::PlannerPort` — **BLOCKED**, see §3. Unchanged.
- [ ] `ports::IgnorePeerPort` — optional; fine to wire as `None`, matching
      Python's `ignore_peer_fn=None`. Unchanged.
- [ ] `ports::Logger` — trivial; wrap whatever `log`/`tracing` macro the
      Rust plugin already uses elsewhere (see how `revops-fees` does it).
      Unchanged — genuinely a few lines whenever someone gets to it.

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

**Correction, 2026-07-27:** `crates/revops-capital` (a parallel, sibling
port task, landed in this same worktree lineage) now has pure
`CapacityPlanner`-adjacent kernels — `revops_capital::planner::ev::
calculate_open_ev` and `revops_capital::planner::close_fee::
estimate_open_cost_sats` are direct candidates for
`PlannerPort::calculate_open_ev`/`estimate_open_cost`. `score_candidate`
and `capex_fleet_exploration_budget` do not have an obvious equivalent
there yet (`revops_capital::planner::scoring::normalize_candidate_scores`
is a different shape — batch normalization, not a per-pubkey score
lookup). This does NOT fully unblock §3 — writing a `PlannerPort` adapter
over `revops-capital` is real integration work (matching this crate's
`(peer, capacity) -> f64` / `() -> i64` call shapes against whatever
`revops-capital` actually exposes, and resolving the missing two methods)
that was out of scope for the wiring task that produced this file — but it
is now meaningfully closer than "no Rust port at all," and worth checking
before anyone re-derives this from scratch.

## 4. Operator RPC surface — none ported

Python exposes these as `cl-revenue-ops.py` RPC methods backed by this
module (method names inferred from log/docstring references in
`lnplus_swaps.py`; the RPC registration itself lives in the plugin entry
point, not this file):

`LnPlusDb` existing no longer blocks any of these (§2) — what blocks all
four now is simply that nothing in `crates/revops` calls this crate yet.
`REGISTER.md` §7 has ready-to-paste `.rpcmethod(...)` bodies for all four:

- [ ] `revenue-r-lnplus-status` -> [`watcher::get_status`] (ported, pure,
      snippet ready in `REGISTER.md` §7)
- [ ] `revenue-r-lnplus-backfill` -> [`backfill::backfill_from_lnplus`]
      (ported; snippet fetches `MySwaps` via `LnPlusApi` first, matching
      py's `my=None -> fetch` default; read-only, safe under every
      `ExecutionMode`)
- [ ] `revenue-r-lnplus-breaker-clear` -> [`breaker::clear_and_persist`]
      (ported; this is the ONLY sanctioned way to clear a
      never-auto-clear breaker cause — see `breaker.rs`'s module doc)
- [ ] `revenue-r-lnplus-abandon` (mark a row `failed` by operator
      request) — snippet in `REGISTER.md` §7 uses
      `LnPlusDb::update_swap` + `SwapPatch::default().status("failed")`,
      exactly as this line originally proposed — no new kernel needed.
- [ ] `revenue-r-lnplus-*` config get/set — see §5 below (mostly
      unblocked now, one piece still missing).

## 5. Config wiring

**Correction to this section's original claim ("no `lnplus_*` options
registered in Rust"):** as of 2026-07-27, `fixtures/options.json` already
has all 15 `lnplus_*` Python options (auto-extracted from
`cl-revenue-ops.py`'s `plugin.add_option` calls — this happened
independently of this port), and `options_table.rs`'s generic loop in
`main.rs` already registers every one of them under its shadow name. That
part was never actually blocked; the audit that produced this line missed
that the generic extraction pipeline already covered `lnplus_*` along with
everything else.

What's still missing: resolving those already-registered values into a
[`config::LnPlusConfig`] (no code reads them into that struct yet). See
`REGISTER.md` §4 for the `configured.option(&opt)?` pattern (or, for full
`revenue-r-config` precedence consistency, routing through
`config_resolve.rs`'s existing 3-layer resolver instead).

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
  every other pure-decision crate in this workspace. **Update:** the
  `run_watcher_once` guard now HAS a wiring-layer home —
  [`loop_drivers::WatcherLoop`] (a real, tested `Mutex`-backed skip-not-
  queue). The backfill double-checked lock (C-7) is still not modeled
  anywhere; `maybe_run_backfill_once`'s flag check + set is not atomic
  across two racing callers — fine as long as `evaluator_pass`/
  `watcher_pass` are only ever driven by `WatcherLoop`-style
  single-flight scheduling, same assumption as everywhere else.
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
