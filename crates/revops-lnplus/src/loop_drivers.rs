//! The two scheduled-loop entry points `ENTRYPOINTS.md` §1 calls for:
//! [`evaluator_pass`] (py `run_cycle`, `lnplus_swaps.py:287-332`, called
//! from `capacity_planner.py`'s cycle — see `execute_cycle`,
//! `capacity_planner.py:649-653`) and [`watcher_pass`] /
//! [`WatcherLoop`] (py `run_watcher_once`, `lnplus_swaps.py:1299-1491`,
//! scheduled from `cl-revenue-ops.py`'s `lnplus_watcher_loop`, ~3367-3400).
//!
//! Both functions:
//! - Never hand the kernel a raw, ungated [`crate::ports::LnPlusApi`] /
//!   [`crate::ports::ChainPort`] — every call first wraps them in
//!   [`crate::gated::GatedLnPlusApi`] / [`crate::gated::GatedChainPort`]
//!   built from the caller's [`crate::exec_mode::ExecutionMode`]. A caller
//!   who does not explicitly pass `ExecutionMode::Armed` cannot reach a
//!   live `create_application`/`connect`/`fund_channel` no matter what
//!   `LnPlusConfig` says.
//! - Assemble kernel inputs by calling ports themselves, in the exact
//!   short-circuit order `ENTRYPOINTS.md` documents (matching Python's own
//!   early-return chain) — a disabled/tripped/in-flight/feerate-blocked
//!   pass never issues the LIVE, signed `get_my_swaps`/`get_applicable_swaps`
//!   calls that a proceeding pass would.

use std::sync::Mutex;

use crate::breaker;
use crate::config::LnPlusConfig;
use crate::evaluator::{self, CycleInputs, CycleOutcome, CyclePreflight};
use crate::exec_mode::ExecutionMode;
use crate::gated::{GatedChainPort, GatedLnPlusApi};
use crate::open::OpenExecParams;
use crate::ports::{
    ChainPort, IgnorePeerPort, LnPlusApi, LnPlusDb, Logger, PlannerPort, PolicyPort,
};
use crate::reconcile;
use crate::watcher::{self, WatcherSummary};

/// Externally-sourced inputs [`evaluator_pass`] cannot compute from the
/// injected ports alone:
/// - `best_regular_ev` — `ENTRYPOINTS.md` §3's hard blocker
///   (`CapacityPlanner` has no Rust port yet); Python computes this from
///   the SAME cycle's regular-candidate ranking
///   (`capacity_planner.py:651`) immediately before calling `run_cycle`.
///   Until a Rust `CapacityPlanner` exists, callers should pass `0.0` (the
///   conservative "no known regular alternative" value — `swap_ev`'s
///   `lockup_haircut` term becomes `0`, and the preference-margin gate in
///   `evaluator::select_and_apply` never rejects on it) or wire in a real
///   value once one exists.
/// - `cached_our_id` — PR 3d (py `_our_id`, 259-270): our node id is a
///   PROCESS CONSTANT, cached on first successful `getinfo` and reused
///   across passes. `evaluator.rs` deliberately leaves this caching to the
///   wiring layer (its own doc comment, `evaluator.rs:6-14`) — pass back
///   [`EvaluatorPassResult::resolved_our_id`] from the previous call.
#[derive(Debug, Clone)]
pub struct EvaluatorPassParams {
    pub best_regular_ev: f64,
    pub cached_our_id: Option<String>,
    pub now: i64,
}

/// [`evaluator_pass`]'s return: the gate-chain outcome, plus whatever
/// `our_id` resolved to this pass (feed back into the next call's
/// `cached_our_id`).
#[derive(Debug, Clone)]
pub struct EvaluatorPassResult {
    pub outcome: CycleOutcome,
    pub resolved_our_id: Option<String>,
}

/// One evaluator pass: breaker -> inflight -> feerate -> reconcile, each
/// gating the next (py `run_cycle` 292-305's exact short-circuit order),
/// then (only if every preflight step passed) `get_applicable_swaps` +
/// `evaluator::run_cycle`'s gate chain / EV ranking / apply-or-recommend.
///
/// `mode: ExecutionMode` is REQUIRED and has no default here on purpose —
/// see [`crate::exec_mode`]'s doc comment; passing
/// [`ExecutionMode::default()`] (`DryRun`) is always safe.
#[allow(clippy::too_many_arguments)]
pub fn evaluator_pass(
    cfg: &LnPlusConfig,
    mode: ExecutionMode,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    policy: Option<&dyn PolicyPort>,
    planner: &dyn PlannerPort,
    logger: &dyn Logger,
    params: EvaluatorPassParams,
) -> EvaluatorPassResult {
    let gated_api = GatedLnPlusApi::new(api, mode, logger);
    let gated_chain = GatedChainPort::new(chain, mode, logger);

    // Steps 1-2 (ENTRYPOINTS.md §1): local reads only, no network/API call
    // either way.
    let breaker_tripped = breaker::tripped_message(db);
    let has_inflight = !db.inflight_swaps().is_empty();

    let mut opening_feerate_perkw: Option<i64> = None;
    let mut reconcile_ok = false;
    let mut swaps = Vec::new();
    let mut our_id = params.cached_our_id.clone();
    let mut frozen_peers_with_channels = None;
    let mut confirmed_unreserved_sats = 0i64;
    let mut capex_budget_sats = None;

    // Matches py `run_cycle`'s nested early-returns exactly: each deeper
    // live call only fires when every shallower gate already passed.
    if cfg.lnplus_swaps_enabled && breaker_tripped.is_none() && !has_inflight {
        // Step 3: ChainPort::opening_feerate_perkw() — a read, unaffected
        // by `ExecutionMode`.
        opening_feerate_perkw = gated_chain.opening_feerate_perkw().ok();
        let feerate_ok = matches!(
            opening_feerate_perkw,
            Some(perkw) if perkw <= cfg.lnplus_apply_feerate_ceiling
        );
        if feerate_ok {
            // Step 4: reconcile_ok — ONLY after 1-3 pass (issues a signed
            // `get_my_swaps` call; must not fire on a pass that's about to
            // no-op anyway).
            reconcile_ok =
                reconcile::reconcile_ok(db, &gated_api, &gated_chain, logger, params.now);
            if reconcile_ok {
                // Steps 5-9.
                swaps = gated_api.get_applicable_swaps().unwrap_or_default();
                frozen_peers_with_channels = evaluator::capture_peers_with_channels(&gated_chain);
                our_id = our_id.or_else(|| evaluator::fetch_our_id(&gated_chain));
                confirmed_unreserved_sats = gated_chain.confirmed_unreserved_sats().unwrap_or(0);
                capex_budget_sats = planner.capex_fleet_exploration_budget();
            }
        }
    }

    let inputs = CycleInputs {
        cfg: cfg.clone(),
        preflight: CyclePreflight {
            breaker_tripped,
            has_inflight,
            reconcile_ok,
        },
        opening_feerate_perkw,
        swaps,
        our_id: our_id.clone(),
        frozen_peers_with_channels,
        best_regular_ev: params.best_regular_ev,
        confirmed_unreserved_sats,
        capex_budget_sats,
        now: params.now,
    };

    let outcome = evaluator::run_cycle(inputs, db, &gated_api, policy, planner, logger);
    EvaluatorPassResult {
        outcome,
        resolved_our_id: our_id,
    }
}

/// One watcher pass: [`watcher::run_watcher_once`] with every mutating
/// port pre-wrapped in its gated form. This function itself is NOT
/// reentrancy-guarded (see [`WatcherLoop`] for that) — matching every other
/// kernel entry point in this crate, callers must sequence calls.
///
/// `mode: ExecutionMode` is REQUIRED, no default — see
/// [`crate::exec_mode`]'s doc comment.
#[allow(clippy::too_many_arguments)]
pub fn watcher_pass(
    mode: ExecutionMode,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    policy: &dyn PolicyPort,
    ignore_peer: Option<&dyn IgnorePeerPort>,
    logger: &dyn Logger,
    open_exec: &OpenExecParams,
    pending_timeout_days: i64,
    now: i64,
) -> WatcherSummary {
    let gated_api = GatedLnPlusApi::new(api, mode, logger);
    let gated_chain = GatedChainPort::new(chain, mode, logger);
    watcher::run_watcher_once(
        db,
        &gated_api,
        &gated_chain,
        policy,
        ignore_peer,
        logger,
        open_exec,
        pending_timeout_days,
        now,
    )
}

/// Non-reentrant scheduling wrapper around [`watcher_pass`] — py's
/// `threading.Lock` "watcher already running" skip
/// (`lnplus_swaps.py:1300`), which `ENTRYPOINTS.md` §1 explicitly calls out
/// as NOT modeled inside `watcher.rs` ("the wiring layer must serialize
/// calls to `run_watcher_once` itself"). [`WatcherLoop::try_pass`] SKIPS
/// (returns `None`) when a pass is already in flight — it does not queue,
/// exactly matching Python.
pub struct WatcherLoop {
    /// `pub` so a scheduler/test can observe contention
    /// (`reentry_lock.try_lock().is_err()`) without a bespoke `is_busy()`
    /// accessor. Do not lock this directly outside a test — go through
    /// [`WatcherLoop::try_pass`].
    pub reentry_lock: Mutex<()>,
}

impl Default for WatcherLoop {
    fn default() -> Self {
        Self::new()
    }
}

impl WatcherLoop {
    pub fn new() -> Self {
        Self {
            reentry_lock: Mutex::new(()),
        }
    }

    /// `None` iff another [`WatcherLoop::try_pass`] call on this same
    /// instance is currently running (skip, not queue). `Some(summary)`
    /// otherwise.
    #[allow(clippy::too_many_arguments)]
    pub fn try_pass(
        &self,
        mode: ExecutionMode,
        db: &dyn LnPlusDb,
        api: &dyn LnPlusApi,
        chain: &dyn ChainPort,
        policy: &dyn PolicyPort,
        ignore_peer: Option<&dyn IgnorePeerPort>,
        logger: &dyn Logger,
        open_exec: &OpenExecParams,
        pending_timeout_days: i64,
        now: i64,
    ) -> Option<WatcherSummary> {
        let _guard = self.reentry_lock.try_lock().ok()?;
        Some(watcher_pass(
            mode,
            db,
            api,
            chain,
            policy,
            ignore_peer,
            logger,
            open_exec,
            pending_timeout_days,
            now,
        ))
    }
}
