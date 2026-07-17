//! `EvRebalancer` facade (port of `modules/rebalancer.py`'s `EVRebalancer`,
//! `~/bin/cl_revenue_ops-port`, branch `port`, v2.18.1): the operator-facing
//! entry over the T7 engine — automatic-cycle delegation, capital controls,
//! inbound-fee estimation, manual/explicit execution, and the defibrillator
//! diagnostic (`diagnostic_rebalance`, constants + pure helpers in
//! [`crate::defib`]).
//!
//! ## Safety contracts carried by this module
//!
//! - **Defibrillator ranked-source fallback** (v2.18.1,
//!   `rebalancer.py:1715-1927`): the shock tries up to
//!   [`DIAGNOSTIC_MAX_SOURCE_ATTEMPTS`] sources ranked by `spendable_sats`
//!   desc (stable ties keep listfunds order), advancing ONLY on
//!   [`is_source_route_failure`] (no payment was attempted, so a retry
//!   cannot double-pay); success/pending/`local_budget_block` — and any
//!   other failure — stop the sequence. Honest `shock_status`
//!   (`completed | pending | blocked | failed`); a blocked or failed shock
//!   delivered no liquidity and must never read as completed. The engine
//!   owns the SHARED history row's success fee (P4-025) — this facade
//!   records NO fee on a successful shock.
//! - **Capital controls** (`_check_capital_controls`,
//!   `rebalancer.py:2056-2158`): the wallet-reserve check fails OPEN on an
//!   RPC failure (a transient listfunds issue must not block all
//!   rebalancing); the daily/weekly budget checks are DB-only and fail
//!   CLOSED (`>=` comparisons — a zero budget blocks).
//! - **P4-009 release-only-when-not-sweepable**
//!   (`execute_rebalance`): a pending payment WITH a `payment_hash` keeps
//!   its budget reservation held (the engine parked the shared row as
//!   `pending_settlement`; `reconcile_pending_settlements` owns the
//!   terminal release/spend); a no-hash pending is not sweepable and is
//!   released like any terminal failure.
//! - **Manual hard cap** (DD2/P1-004, `manual_rebalance`): amounts clamp to
//!   `rebalance_max_amount` regardless of `force`.
//!
//! ## Port deviations (documented, deliberate)
//!
//! - Arc seams, no lifetime parameter (the plan sketch wrote
//!   `EvRebalancer<'a>`) — the T7 engine precedent.
//! - The engine is consumed through [`CandidateExecutor`] so tests script
//!   `ExecutionResult`s directly (the plan's "scripted engine double");
//!   the production impl is the T7 [`RebalanceEngine`].
//! - Facade-only DB reads/writes that are not part of the engine's
//!   [`RebalanceStore`] seam (stale-reservation cleanup, fee totals,
//!   inbound-fee history, probe flag, SCID/peer-keyed failure counts) live
//!   on [`FacadeStore`]; history/budget writes reuse [`RebalanceStore`]
//!   (`record_rebalance` -> `insert_or_adopt_history`,
//!   `update_rebalance_result` -> `update_history`/`update_history_success`,
//!   `Database.reserve_budget`'s `(reserved, remaining)` tuple ->
//!   `Result<ReservationId, BudgetBlock>` with `Refused { remaining }`).
//! - `cfg.dry_run` branches are NOT ported: dry-run discipline in this port
//!   is the store/`PaymentMode` gate (Global Constraints), not a config
//!   flag short-circuit.
//! - The external-liquidity-cost and global-budget-limit provider hooks are
//!   wiring-deferred exactly like the engine's (`_get_external_liquidity_
//!   costs` -> `{0, 0}`, `_get_global_budget_limit` -> `daily_budget_sats`);
//!   the operator-facing strings still format the zeros Python would print
//!   without a provider.
//! - `JobManager` is a stripped stub in Python (slots always available,
//!   active jobs always empty); the slot-check suppression path is
//!   unreachable and not ported. `_pending` bookkeeping (write-only in the
//!   Python module) and the growth-spend outcome reporter (payload built
//!   then deleted — cl-mycelium retired) are dropped.
//! - `execute_rebalance` returns the engine's [`ExecutionResult`] (plan's
//!   frozen signature) rather than Python's RPC dict; the dict's
//!   `message`/`error` strings survive where they are contracts (history
//!   error messages byte-preserved; `error = "local_budget_block"`).
//! - Python's failure path writes the history row's `failed` status twice
//!   (`rebalancer.py:1566` and `:1643`); mirrored for DB-call parity.
//! - `_get_channels_with_balances`' 1s inter-attempt sleep is dropped (the
//!   single retry is kept); no ambient clock/sleep in this crate.
//! - `_derive_hold_reason` needs the engine's last-cycle debug stream
//!   (audit richness deferred with `rebalance_audit_v2.py`); the hold
//!   summary reason falls back to `no_rebalance_candidates`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;

use crate::defib::{
    is_source_route_failure, shock_fee_envelope, DIAGNOSTIC_MAX_SOURCE_ATTEMPTS, SHOCK_AMOUNT_SATS,
};
use crate::engine::{
    BudgetBlock, CycleResult, EngineClock, HistoryRow, HistorySuccess, RebalanceCost,
    RebalanceEngine, RebalanceStore, ReservationId,
};
use crate::errors::LOCAL_BUDGET_BLOCK;
use crate::modes::{engine_kwargs, EngineKwargs};
use crate::router::RpcFailure;
use crate::types::{ExecutionResult, RebalanceCandidate};
use revops_db::budget::ReserveRequest;
use revops_fees::pyjson::OValue;

// ---------------------------------------------------------------------------
// Money helpers (module-local, the executor.rs precedent)
// ---------------------------------------------------------------------------

/// `utils.base_to_sats_ceil` (`-(-base // 1000)`), exact for all signs.
fn base_to_sats_ceil(base_msat: i64) -> i64 {
    base_msat.div_euclid(1000) + i64::from(base_msat.rem_euclid(1000) != 0)
}

/// `utils.base_to_sats_floor` (`base // 1000`, Python floor division).
fn base_to_sats_floor(base_msat: i64) -> i64 {
    base_msat.div_euclid(1000)
}

/// `utils.parse_msat` for the JSON shapes the facade meets (int / float /
/// `"Nmsat"` string / null); anything unparseable is 0.
fn parse_msat(v: &Value) -> i64 {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f.trunc() as i64))
            .unwrap_or(0),
        Value::String(s) => {
            let t = s.trim();
            let t = t.strip_suffix("msat").unwrap_or(t);
            t.parse::<i64>().unwrap_or(0)
        }
        _ => 0,
    }
}

fn json_str(v: &Value, key: &str) -> String {
    v.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

fn json_i64(v: &Value, key: &str) -> i64 {
    v.get(key).map(parse_msat).unwrap_or(0)
}

fn json_arr<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key).and_then(Value::as_array).map_or(&[], |a| a)
}

// ---------------------------------------------------------------------------
// Seams
// ---------------------------------------------------------------------------

/// Engine seam: the facade's slice of the T7 [`RebalanceEngine`] API, so
/// tests script `ExecutionResult`s directly.
pub trait CandidateExecutor: Send + Sync {
    /// [`RebalanceEngine::execute_candidate`] (`rebalance_id <= 0` = no
    /// caller-owned history row).
    fn execute_candidate(
        &self,
        candidate: &RebalanceCandidate,
        rebalance_id: i64,
        kw: EngineKwargs,
    ) -> ExecutionResult;
    /// [`RebalanceEngine::run_cycle`].
    fn run_cycle(&self) -> CycleResult;
}

impl CandidateExecutor for RebalanceEngine {
    fn execute_candidate(
        &self,
        candidate: &RebalanceCandidate,
        rebalance_id: i64,
        kw: EngineKwargs,
    ) -> ExecutionResult {
        RebalanceEngine::execute_candidate(self, candidate, rebalance_id, kw)
    }

    fn run_cycle(&self) -> CycleResult {
        RebalanceEngine::run_cycle(self)
    }
}

/// `Database.get_historical_inbound_fee_ppm`'s row (`database.py:4811-4915`;
/// `avg_fee_ppm`/`median_fee_ppm` are ints there — floor divisions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalInboundFee {
    /// `'high'` (10+ samples) / `'medium'` (5-9) / `'low'` (3-4).
    pub confidence: String,
    pub median_fee_ppm: i64,
    pub avg_fee_ppm: i64,
    pub sample_count: i64,
}

/// The `get_rebalance_history_by_peer` column subset the cost-curve floor
/// reads (`rebalancer.py:1074-1093`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRebalanceRecord {
    pub status: String,
    pub max_fee_sats: i64,
    pub amount_sats: i64,
}

/// Facade-only DB surface (py `Database` methods outside the engine's
/// [`RebalanceStore`] seam). The dry-run/production impls land with plugin
/// wiring at cutover, alongside the store's.
pub trait FacadeStore: Send + Sync {
    /// Issue #24 (`Database.cleanup_stale_reservations`): release
    /// reservations older than `timeout_seconds`; returns the count.
    fn cleanup_stale_reservations(&self, timeout_seconds: i64) -> i64;
    /// `Database.get_total_rebalance_fees(since)` — settled rebalance fees.
    fn get_total_rebalance_fees(&self, since_timestamp: i64) -> i64;
    fn get_historical_inbound_fee_ppm(&self, peer_id: &str) -> Option<HistoricalInboundFee>;
    fn get_rebalance_history_by_peer(&self, peer_id: &str, limit: i64) -> Vec<PeerRebalanceRecord>;
    /// `Database.set_channel_probe` (defibrillator Passive Lure flag).
    fn set_channel_probe(&self, channel_id: &str, probe_type: &str);
    /// Dest-keyed futility counters (`_futility_key`: peer id, DEF-063).
    fn reset_failure_count(&self, key: &str);
    fn increment_failure_count(
        &self,
        key: &str,
        attempted_ppm: i64,
        attempted_amount: i64,
        error_type: &str,
    );
}

/// Node-data RPC surface (py `data_service`).
pub trait FacadeRpc: Send + Sync {
    fn get_funds(&self) -> Result<Value, RpcFailure>;
    fn get_peer_channels(&self) -> Result<Value, RpcFailure>;
    /// `listchannels source=<peer>` (gossip last-hop fallback).
    fn get_channels_source(&self, source: &str) -> Result<Value, RpcFailure>;
    fn get_node_id(&self) -> Result<String, RpcFailure>;
}

// ---------------------------------------------------------------------------
// Config + summaries
// ---------------------------------------------------------------------------

/// The config-snapshot fields the facade reads, with the Python
/// `modules/config.py` dataclass defaults.
#[derive(Debug, Clone)]
pub struct FacadeConfig {
    /// py `daily_budget_sats`, default 5000.
    pub daily_budget_sats: i64,
    /// py `weekly_budget_sats`, default 35000.
    pub weekly_budget_sats: i64,
    /// py `min_wallet_reserve`, default 1_000_000.
    pub min_wallet_reserve: i64,
    /// py `total_cost_budget_window_hours`, default 24 (min 1 at use).
    pub total_cost_budget_window_hours: i64,
    /// py `inbound_fee_estimate_ppm`, default 50.
    pub inbound_fee_estimate_ppm: i64,
    /// py `diagnostic_rebalance_max_fee_sats`, default 400 (D4-clamped at
    /// use, [`shock_fee_envelope`]).
    pub diagnostic_rebalance_max_fee_sats: i64,
    /// py `rebalance_max_amount`, default 5_000_000 (DD2 hard cap).
    pub rebalance_max_amount: i64,
    /// py `reservation_timeout_hours`, default 4.
    pub reservation_timeout_hours: i64,
}

impl Default for FacadeConfig {
    fn default() -> Self {
        FacadeConfig {
            daily_budget_sats: 5000,
            weekly_budget_sats: 35_000,
            min_wallet_reserve: 1_000_000,
            total_cost_budget_window_hours: 24,
            inbound_fee_estimate_ppm: 50,
            diagnostic_rebalance_max_fee_sats: 400,
            rebalance_max_amount: 5_000_000,
            reservation_timeout_hours: 4,
        }
    }
}

/// `_last_decision_summary` (py `rebalancer.py:316-381`), the
/// `revenue-status`-facing decision record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionSummary {
    pub action: String,
    pub reason: String,
    pub dominant_input: String,
    pub safety_block: bool,
    pub budget_blocked: bool,
}

/// The Python `**kwargs`/candidate fields `execute_rebalance` reads that
/// are not part of T1's frozen [`RebalanceCandidate`] (`rebalance_type`
/// kwarg; `reason_code`, `dynamic_budget_override_sats` dataclass extras).
#[derive(Debug, Clone)]
pub struct ExecuteOpts {
    /// py `kwargs.get('rebalance_type', 'normal')`.
    pub rebalance_type: String,
    /// py `candidate.reason_code` (dataclass default `"ev_positive"`);
    /// `"capex_fallback"` switches the budget limit to the candidate's
    /// per-channel capex budget.
    pub reason_code: String,
    /// py `candidate.dynamic_budget_override_sats` (hot-channel protection),
    /// default 0.
    pub dynamic_budget_override_sats: i64,
}

impl Default for ExecuteOpts {
    fn default() -> Self {
        ExecuteOpts {
            rebalance_type: "normal".to_string(),
            reason_code: "ev_positive".to_string(),
            dynamic_budget_override_sats: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Facade
// ---------------------------------------------------------------------------

/// Peer inbound-fee cache entry (py `_peer_inbound_fees[peer_id]`).
#[derive(Debug, Clone, Copy)]
struct PeerFeeInfo {
    fee_ppm: i64,
    base_msat: i64,
}

/// One `_get_channels_with_balances` row (the field subset the ported
/// paths read; capacity/htlc metadata is unread here and dropped).
#[derive(Debug, Clone)]
struct ChannelBalance {
    spendable_sats: i64,
    peer_id: String,
    fee_ppm: i64,
    peer_inbound: Option<PeerFeeInfo>,
}

/// Everything the facade needs, injected (no ambient RPC/clock/DB).
pub struct EvRebalancerDeps {
    pub engine: Arc<dyn CandidateExecutor>,
    pub config: FacadeConfig,
    pub store: Arc<dyn RebalanceStore>,
    pub facade_store: Arc<dyn FacadeStore>,
    pub rpc: Arc<dyn FacadeRpc>,
    pub clock: Arc<dyn EngineClock>,
}

/// EV-based rebalancer facade (py `EVRebalancer`): the "Strategist" shell —
/// capital controls and operator entrypoints — delegating selection and
/// execution to the v2 engine.
pub struct EvRebalancer {
    engine: Arc<dyn CandidateExecutor>,
    config: FacadeConfig,
    store: Arc<dyn RebalanceStore>,
    facade_store: Arc<dyn FacadeStore>,
    rpc: Arc<dyn FacadeRpc>,
    clock: Arc<dyn EngineClock>,
    /// py `_fee_cache` (P2-005; per-run memo, cleared by
    /// `find_rebalance_candidates`).
    fee_cache: Mutex<HashMap<(String, i64), Option<i64>>>,
    /// py `_peer_inbound_fees`, rebuilt by `channels_with_balances`.
    peer_inbound_fees: Mutex<HashMap<String, PeerFeeInfo>>,
    /// py `_our_node_id` (F10: failures are not cached).
    our_node_id: Mutex<String>,
    /// py `_capital_control_blocker` (getattr default `daily_budget_sats`).
    capital_control_blocker: Mutex<String>,
    last_decision_summary: Mutex<DecisionSummary>,
}

impl EvRebalancer {
    pub fn new(deps: EvRebalancerDeps) -> Self {
        EvRebalancer {
            engine: deps.engine,
            config: deps.config,
            store: deps.store,
            facade_store: deps.facade_store,
            rpc: deps.rpc,
            clock: deps.clock,
            fee_cache: Mutex::new(HashMap::new()),
            peer_inbound_fees: Mutex::new(HashMap::new()),
            our_node_id: Mutex::new(String::new()),
            capital_control_blocker: Mutex::new("daily_budget_sats".to_string()),
            last_decision_summary: Mutex::new(DecisionSummary {
                action: "hold".to_string(),
                reason: "not_run".to_string(),
                dominant_input: "startup".to_string(),
                safety_block: false,
                budget_blocked: false,
            }),
        }
    }

    /// py `get_last_decision_summary`.
    pub fn last_decision_summary(&self) -> DecisionSummary {
        self.last_decision_summary
            .lock()
            .expect("summary mutex poisoned")
            .clone()
    }

    /// The blocker recorded by the most recent failing capital-controls
    /// check (py `_capital_control_blocker`).
    pub fn capital_control_blocker(&self) -> String {
        self.capital_control_blocker
            .lock()
            .expect("blocker mutex poisoned")
            .clone()
    }

    /// Test hook: seed the peer inbound-fee cache the way
    /// `channels_with_balances` would (py generator sets
    /// `_peer_inbound_fees` directly).
    #[doc(hidden)]
    pub fn debug_set_peer_inbound_fee(&self, peer_id: &str, fee_ppm: i64, base_msat: i64) {
        self.peer_inbound_fees
            .lock()
            .expect("peer fee mutex poisoned")
            .insert(peer_id.to_string(), PeerFeeInfo { fee_ppm, base_msat });
    }

    fn set_decision(
        &self,
        action: &str,
        reason: &str,
        dominant_input: &str,
        safety_block: bool,
        budget_blocked: bool,
    ) {
        *self
            .last_decision_summary
            .lock()
            .expect("summary mutex poisoned") = DecisionSummary {
            action: action.to_string(),
            reason: reason.to_string(),
            dominant_input: dominant_input.to_string(),
            safety_block,
            budget_blocked,
        };
    }

    fn now(&self) -> i64 {
        self.clock.now() as i64
    }

    // -- automatic cycle ---------------------------------------------------

    /// Run one automatic rebalance cycle through the v2 engine (py
    /// `find_rebalance_candidates`, `rebalancer.py:681-846`).
    ///
    /// E-4.5: despite the legacy name, this ALWAYS returns `[]` — the
    /// engine selects and executes internally (`run_cycle`). This method:
    /// 1. cleans up stale budget reservations;
    /// 2. checks capital controls (suppression path);
    /// 3. delegates selection AND execution to the engine.
    pub fn find_rebalance_candidates(&self) -> Vec<RebalanceCandidate> {
        self.fee_cache.lock().expect("fee cache poisoned").clear();
        // Issue #24: stale reservations release before each cycle so crashed
        // jobs cannot leak budget.
        let timeout_seconds = self.config.reservation_timeout_hours * 3600;
        let _cleaned = self
            .facade_store
            .cleanup_stale_reservations(timeout_seconds);

        // (py slot check: JobManager stub always has slots — unreachable.)

        if !self.check_capital_controls() {
            let blocker = self.capital_control_blocker();
            self.set_decision(
                "suppressed",
                "capital_controls_blocked",
                &blocker,
                true,
                blocker == "daily_budget_sats",
            );
            self.fee_cache.lock().expect("fee cache poisoned").clear();
            return Vec::new();
        }

        let cycle = self.engine.run_cycle();
        let executed = cycle.executions.len();
        let succeeded = cycle.executions.iter().filter(|e| e.success).count();
        if executed > 0 {
            self.set_decision(
                "rebalance",
                &format!("{succeeded}/{executed} rebalances succeeded"),
                "rebalance_engine",
                false,
                false,
            );
        } else if !cycle.candidates.is_empty() {
            self.set_decision(
                "suppressed",
                "candidates found but all executions failed",
                "rebalance_engine",
                false,
                false,
            );
        } else {
            // `_derive_hold_reason` is audit-stream-deferred; coarse fallback.
            self.set_decision(
                "hold",
                "no_rebalance_candidates",
                "rebalance_engine",
                false,
                false,
            );
        }
        self.fee_cache.lock().expect("fee cache poisoned").clear();
        Vec::new()
    }

    // -- capital controls --------------------------------------------------

    /// py `_check_capital_controls` (`rebalancer.py:2056-2158`): wallet
    /// reserve fails OPEN on an RPC failure; daily/weekly budget checks are
    /// DB-only and fail CLOSED (`>=`).
    pub fn check_capital_controls(&self) -> bool {
        // --- Reserve check (needs listfunds RPC; RpcError -> skip) ---
        if let Ok(listfunds) = self.rpc.get_funds() {
            let mut onchain_sats = 0i64;
            for output in json_arr(&listfunds, "outputs") {
                if json_str(output, "status") == "confirmed" {
                    onchain_sats += base_to_sats_floor(json_i64(output, "amount_msat"));
                }
            }
            let mut channel_spendable_sats = 0i64;
            for channel in json_arr(&listfunds, "channels") {
                if json_str(channel, "state") != "CHANNELD_NORMAL" {
                    continue;
                }
                let spendable = base_to_sats_floor(json_i64(channel, "our_amount_msat"));
                if spendable > 0 {
                    channel_spendable_sats += spendable;
                }
            }
            let total_reserve = onchain_sats + channel_spendable_sats;
            if total_reserve < self.config.min_wallet_reserve {
                // Attribution: the blocker is the reserve, not the budget.
                *self
                    .capital_control_blocker
                    .lock()
                    .expect("blocker mutex poisoned") = "min_wallet_reserve".to_string();
                return false;
            }
        }

        // --- Budget check (DB-only) ---
        let now = self.now();
        let budget_window_hours = self.config.total_cost_budget_window_hours.max(1);
        let effective_budget = self.config.daily_budget_sats;
        let fees_spent_24h = self
            .facade_store
            .get_total_rebalance_fees(now - budget_window_hours * 3600);
        // External liquidity costs: provider hook wiring-deferred -> {0, 0}.
        let (ext_spent, _ext_reserved) = (0i64, 0i64);
        let total_actual_spent = fees_spent_24h + ext_spent;
        if total_actual_spent >= effective_budget {
            *self
                .capital_control_blocker
                .lock()
                .expect("blocker mutex poisoned") = "daily_budget_sats".to_string();
            return false;
        }

        // --- Weekly budget check ---
        let effective_weekly = self.config.weekly_budget_sats;
        let weekly_fees_spent = self.facade_store.get_total_rebalance_fees(now - 7 * 86_400);
        let weekly_total_spent = weekly_fees_spent + ext_spent;
        if weekly_total_spent >= effective_weekly {
            *self
                .capital_control_blocker
                .lock()
                .expect("blocker mutex poisoned") = "weekly_budget_sats".to_string();
            return false;
        }

        true
    }

    // -- inbound-fee estimation --------------------------------------------

    /// py `_estimate_inbound_fee(peer_id)` at the default probe amount
    /// (100M msat).
    pub fn estimate_inbound_fee(&self, peer_id: &str) -> i64 {
        self.estimate_inbound_fee_at(peer_id, 100_000_000)
    }

    /// py `_estimate_inbound_fee` (`rebalancer.py:996-1113`),
    /// historical-first. Priority order: historical high (median) /
    /// medium (0.7*median + 0.3*last-hop) / low (avg*1.1); then last-hop +
    /// buffer with the failed-cost floor (+25); then the configured
    /// default. `int()` truncations go through f64 exactly like Python.
    pub fn estimate_inbound_fee_at(&self, peer_id: &str, amount_msat: i64) -> i64 {
        let hist_data = self.facade_store.get_historical_inbound_fee_ppm(peer_id);
        let last_hop = self.last_hop_fee(peer_id, amount_msat);

        if let Some(hist) = hist_data {
            return match hist.confidence.as_str() {
                // 10+ samples: trust the data, median is robust to outliers.
                "high" => hist.median_fee_ppm,
                // 5-9 samples: blend 70% historical / 30% raw last-hop
                // (DEF-067-S4: the median already carries the multi-hop
                // cost; no buffer here).
                "medium" => match last_hop {
                    Some(lh) => (hist.median_fee_ppm as f64 * 0.7 + lh as f64 * 0.3) as i64,
                    None => hist.median_fee_ppm,
                },
                // 3-4 samples: 10% uncertainty buffer.
                _ => (hist.avg_fee_ppm as f64 * 1.1) as i64,
            };
        }

        // Cost-curve floor from recent failures: the true cost is above the
        // highest PPM that still failed.
        let mut failed_floor = 0i64;
        for record in self
            .facade_store
            .get_rebalance_history_by_peer(peer_id, 20)
            .iter()
            .filter(|r| r.status == "failed")
        {
            if record.max_fee_sats > 0 && record.amount_sats > 0 {
                failed_floor = failed_floor
                    .max((record.max_fee_sats * 1_000_000).div_euclid(record.amount_sats));
            }
        }

        // Priority 5: last-hop fee + buffer.
        if let Some(lh) = last_hop {
            let mut estimate = lh + self.config.inbound_fee_estimate_ppm;
            if failed_floor > 0 && estimate <= failed_floor {
                estimate = failed_floor + 25;
            }
            return estimate;
        }

        // Priority 6: configured default.
        self.config.inbound_fee_estimate_ppm
    }

    /// py `_get_last_hop_fee` (`rebalancer.py:1115-1189`): actual peer fee
    /// from the listpeerchannels cache first, gossip fallback, memoized per
    /// run (P2-005: the lock is never held across the RPC).
    fn last_hop_fee(&self, peer_id: &str, amount_msat: i64) -> Option<i64> {
        let cache_key = (peer_id.to_string(), amount_msat);
        let peer_fee_info = {
            let cache = self.fee_cache.lock().expect("fee cache poisoned");
            if let Some(memo) = cache.get(&cache_key) {
                return *memo;
            }
            self.peer_inbound_fees
                .lock()
                .expect("peer fee mutex poisoned")
                .get(peer_id)
                .copied()
        };

        let base_ppm_at = |base_msat: i64| -> i64 {
            let ppm = (base_msat as i128 * 1_000_000) / i128::from(amount_msat.max(1));
            // Cap the base-fee ppm-equivalent at 100% (P4-011 on the gossip
            // path too).
            i64::try_from(ppm).unwrap_or(i64::MAX).min(1_000_000)
        };

        let mut result = None;
        if let Some(info) = peer_fee_info {
            // PRIORITY 1: actual peer inbound fee (updates.remote).
            result = Some(info.fee_ppm + base_ppm_at(info.base_msat));
        } else if let Some(our_id) = self.our_node_id() {
            // PRIORITY 2: gossip fallback; failures leave None (py except).
            if let Ok(channels) = self.rpc.get_channels_source(peer_id) {
                for ch in json_arr(&channels, "channels") {
                    if json_str(ch, "destination") == our_id {
                        let ppm = json_i64(ch, "fee_per_millionth");
                        let base_fee_msat = json_i64(ch, "base_fee_millisatoshi");
                        result = Some(ppm + base_ppm_at(base_fee_msat));
                        break;
                    }
                }
            }
        }

        self.fee_cache
            .lock()
            .expect("fee cache poisoned")
            .insert(cache_key, result);
        result
    }

    /// py `_get_our_node_id` (F10: failure is not cached, retried next call).
    fn our_node_id(&self) -> Option<String> {
        let mut cached = self.our_node_id.lock().expect("node id mutex poisoned");
        if !cached.is_empty() {
            return Some(cached.clone());
        }
        match self.rpc.get_node_id() {
            Ok(id) if !id.is_empty() => {
                *cached = id.clone();
                Some(id)
            }
            _ => None,
        }
    }

    // -- channel balances --------------------------------------------------

    /// py `_get_channels_with_balances` (`rebalancer.py:1201-1293`): one
    /// retry on RPC failure, listfunds order preserved (the defibrillator's
    /// stable ranking depends on it), and the peer inbound-fee cache
    /// rebuilt from `updates.remote` as a side effect.
    fn channels_with_balances(&self) -> Vec<(String, ChannelBalance)> {
        for _attempt in 0..2 {
            match self.try_channels_with_balances() {
                Ok(channels) => return channels,
                Err(_) => continue,
            }
        }
        Vec::new()
    }

    fn try_channels_with_balances(&self) -> Result<Vec<(String, ChannelBalance)>, RpcFailure> {
        let listfunds = self.rpc.get_funds()?;
        let listpeerchannels = self.rpc.get_peer_channels()?;

        struct PeerInfo {
            peer_id: String,
            fee_ppm: i64,
            inbound: Option<PeerFeeInfo>,
        }
        let mut peer_info: HashMap<String, PeerInfo> = HashMap::new();
        for ch in json_arr(&listpeerchannels, "channels") {
            let scid = json_str(ch, "short_channel_id");
            if scid.is_empty() || json_str(ch, "state") != "CHANNELD_NORMAL" {
                continue;
            }
            let remote = ch.get("updates").and_then(|u| u.get("remote"));
            let inbound = remote
                .and_then(|r| r.get("fee_proportional_millionths"))
                .map(|ppm| PeerFeeInfo {
                    fee_ppm: parse_msat(ppm),
                    base_msat: remote.map(|r| json_i64(r, "fee_base_msat")).unwrap_or(0),
                });
            peer_info.insert(
                scid.clone(),
                PeerInfo {
                    peer_id: json_str(ch, "peer_id"),
                    fee_ppm: json_i64(ch, "fee_proportional_millionths"),
                    inbound,
                },
            );
        }

        let mut channels: Vec<(String, ChannelBalance)> = Vec::new();
        for channel in json_arr(&listfunds, "channels") {
            if json_str(channel, "state") != "CHANNELD_NORMAL" {
                continue;
            }
            let scid = json_str(channel, "short_channel_id");
            if scid.is_empty() {
                continue;
            }
            let our_amount_msat = json_i64(channel, "our_amount_msat");
            let info = peer_info.get(&scid);
            let balance = ChannelBalance {
                spendable_sats: base_to_sats_floor(our_amount_msat),
                peer_id: info
                    .map(|i| i.peer_id.clone())
                    .filter(|p| !p.is_empty())
                    .unwrap_or_else(|| json_str(channel, "peer_id")),
                fee_ppm: info.map(|i| i.fee_ppm).unwrap_or(0),
                peer_inbound: info.and_then(|i| i.inbound),
            };
            // py dict assignment: duplicate SCIDs overwrite in place.
            match channels.iter_mut().find(|(existing, _)| *existing == scid) {
                Some(slot) => slot.1 = balance,
                None => channels.push((scid, balance)),
            }
        }

        // Rebuild the peer inbound-fee cache and swap it in whole (P2-005).
        let mut rebuilt: HashMap<String, PeerFeeInfo> = HashMap::new();
        for (_scid, balance) in &channels {
            if let Some(inbound) = balance.peer_inbound {
                if !balance.peer_id.is_empty() {
                    rebuilt.insert(balance.peer_id.clone(), inbound);
                }
            }
        }
        *self
            .peer_inbound_fees
            .lock()
            .expect("peer fee mutex poisoned") = rebuilt;

        Ok(channels)
    }

    // -- explicit execution ------------------------------------------------

    /// py `execute_rebalance` (`rebalancer.py:1325-1698`): reserve BEFORE
    /// execute; release only when NOT (payment_pending AND payment_hash) —
    /// P4-009. Runs the shared engine path with `engine_kwargs("manual")`
    /// (the facade owns reservation + accounting here).
    pub fn execute_rebalance(
        &self,
        candidate: &RebalanceCandidate,
        enforce_budget: bool,
        opts: &ExecuteOpts,
    ) -> ExecutionResult {
        let from_channel = candidate
            .source_candidates
            .first()
            .cloned()
            .unwrap_or_default();
        let to_channel = candidate.to_channel.clone();

        // HO-01: error out on empty channel ids before any store write.
        if from_channel.is_empty() || to_channel.is_empty() {
            self.set_decision(
                "suppressed",
                "invalid_channel_ids",
                "validation",
                true,
                false,
            );
            return facade_failed(
                "Invalid channel IDs - from_channel or to_channel is empty",
                0,
            );
        }

        let db_max_fee = candidate.max_budget_sats;
        let rebalance_row_id = self.store.insert_or_adopt_history(&HistoryRow {
            id: 0,
            from_channel,
            to_channel: to_channel.clone(),
            amount_sats: candidate.amount_sats,
            max_fee_sats: db_max_fee,
            expected_profit_sats: candidate.expected_profit_sats,
            status: "pending".to_string(),
            rebalance_type: opts.rebalance_type.clone(),
            reason_code: opts.reason_code.clone(),
            payment_hash: None,
        });
        let rebalance_id = (rebalance_row_id > 0).then_some(rebalance_row_id);

        let mut reserved_budget = false;
        if enforce_budget {
            // CRITICAL-01: atomic reservation BEFORE the job starts.
            let now = self.now();
            let budget_window_hours = self.config.total_cost_budget_window_hours.max(1);
            let since_24h = now - budget_window_hours * 3600;
            let effective_budget = self.config.daily_budget_sats;
            // (external liquidity costs: provider wiring-deferred -> zeros,
            // kept for the operator-facing strings only, P4-016.)
            let (ext_spent, ext_reserved) = (0i64, 0i64);
            let mut rebalance_budget_limit = effective_budget.max(0);
            // Capex candidates use the per-channel budget as their limit;
            // the global daily cap applies only when > 0.
            let is_capex = opts.reason_code == "capex_fallback";
            if is_capex {
                let capex_limit = candidate.max_budget_sats;
                rebalance_budget_limit = if self.config.daily_budget_sats > 0 {
                    capex_limit.min(rebalance_budget_limit)
                } else {
                    capex_limit
                };
            }
            let hot_override_limit = opts.dynamic_budget_override_sats;
            if hot_override_limit > 0 {
                // Candidate-specific protection budget may exceed the
                // standard limit, but aggregate hot spend is capped at the
                // effective daily budget.
                let protected_limit = hot_override_limit.max(0).min(effective_budget.max(0));
                if protected_limit > rebalance_budget_limit {
                    rebalance_budget_limit = protected_limit;
                }
            }

            let reservation_id = rebalance_row_id.to_string();
            let request = ReserveRequest {
                reservation_id: reservation_id.clone(),
                amount_sats: db_max_fee,
                category: "rebalance".to_string(),
                channel_id: Some(to_channel.clone()),
                effective_budget_sats: Some(rebalance_budget_limit),
                since_timestamp: Some(since_24h),
                weekly_budget_limit: Some(self.config.weekly_budget_sats),
                weekly_since_timestamp: Some(now - 7 * 86_400),
                ..ReserveRequest::default()
            };
            match self.store.reserve_budget(&request) {
                Ok(_rid) => reserved_budget = true,
                Err(block) => {
                    let remaining = match block {
                        BudgetBlock::Refused { remaining } => remaining,
                        // py: a rail exception surfaces via the outer except;
                        // here it maps to the same exhausted path, remaining 0.
                        BudgetBlock::Unavailable(_) => 0,
                    };
                    // Weekly-vs-daily attribution: remaining reflects the
                    // tighter headroom; remaining > 0 but under what daily
                    // alone would allow means weekly bound first.
                    let blocker = if remaining > 0 && remaining < rebalance_budget_limit {
                        "weekly_budget_sats"
                    } else {
                        "daily_budget_sats"
                    };
                    self.set_decision("suppressed", "budget_exhausted", blocker, true, true);
                    if let Some(id) = rebalance_id {
                        self.store.update_history(
                            id,
                            "failed",
                            Some(&format!(
                                "Unified liquidity budget exhausted: {remaining} sats remaining \
                                 for rebalances after external costs ({ext_spent} spent + \
                                 {ext_reserved} reserved) of total {effective_budget}"
                            )),
                        );
                    }
                    // Budget exhaustion is global; no per-channel backoff.
                    return facade_failed(LOCAL_BUDGET_BLOCK, candidate.amount_sats.max(0));
                }
            }
        }

        let exec_result =
            self.engine
                .execute_candidate(candidate, rebalance_row_id, engine_kwargs("manual"));

        if exec_result.success {
            let actual_fee_sats = self.record_successful_rebalance_fee(
                rebalance_id,
                &to_channel,
                &candidate.to_peer_id,
                candidate.amount_sats,
                exec_result.fee_msat,
            );
            // Success resets the failure count so the channel re-enters
            // rotation.
            self.facade_store
                .reset_failure_count(&futility_key(candidate));
            self.set_decision(
                "rebalance",
                "rebalance_completed",
                &opts.reason_code,
                false,
                false,
            );
            if reserved_budget {
                self.store.mark_budget_spent(
                    &ReservationId(rebalance_row_id.to_string()),
                    actual_fee_sats,
                );
            }
            return exec_result;
        }

        // Failure path.
        let error_str = exec_result
            .error
            .clone()
            .filter(|e| !e.is_empty())
            .unwrap_or_else(|| "no_routes".to_string());
        let payment_pending = exec_result.payment_pending;
        // The engine shares this history row; a pending payment keeps its
        // 'pending_settlement' status for the reconciliation sweep.
        if let Some(id) = rebalance_id {
            if !payment_pending {
                self.store.update_history(id, "failed", Some(&error_str));
            }
        }
        self.facade_store.increment_failure_count(
            &futility_key(candidate),
            candidate.max_fee_ppm,
            candidate.amount_sats,
            classify_error(&error_str),
        );
        self.set_decision(
            "suppressed",
            "start_job_failed",
            &opts.reason_code,
            false,
            false,
        );
        // py double-writes the failed status (rebalancer.py:1566 + :1643);
        // mirrored for DB-call parity.
        if let Some(id) = rebalance_id {
            if !payment_pending {
                self.store.update_history(id, "failed", Some(&error_str));
            }
        }
        // P4-009: hold the reservation ONLY for a sweepable pending payment
        // (pending AND payment_hash); everything else releases now.
        let sweepable_pending = payment_pending
            && exec_result
                .payment_hash
                .as_deref()
                .is_some_and(|h| !h.is_empty());
        if reserved_budget && !sweepable_pending {
            self.store
                .release_reservation(&ReservationId(rebalance_row_id.to_string()));
        }

        let mut result = exec_result;
        result.error = Some(error_str);
        result
    }

    /// py `_record_successful_rebalance_fee` (`rebalancer.py:1295-1323`):
    /// persist the settled fee in both the history row and the cost ledger.
    fn record_successful_rebalance_fee(
        &self,
        rebalance_id: Option<i64>,
        channel_id: &str,
        peer_id: &str,
        amount_sats: i64,
        fee_msat: i64,
    ) -> i64 {
        let persisted_fee_msat = fee_msat.max(0);
        let persisted_fee_sats = base_to_sats_ceil(persisted_fee_msat);
        if let Some(id) = rebalance_id {
            self.store.update_history_success(
                id,
                &HistorySuccess {
                    actual_fee_sats: persisted_fee_sats,
                    actual_fee_msat: persisted_fee_msat,
                    post_local_ratio: None,
                    amount_sats: None,
                },
            );
        }
        if persisted_fee_msat > 0 {
            self.store.record_rebalance_cost(&RebalanceCost {
                channel_id: channel_id.to_string(),
                peer_id: peer_id.to_string(),
                cost_sats: persisted_fee_sats,
                cost_msat: persisted_fee_msat,
                amount_sats,
                timestamp: self.now(),
            });
        }
        persisted_fee_sats
    }

    // -- manual rebalance --------------------------------------------------

    /// py `manual_rebalance` (`rebalancer.py:1929-2054`): operator
    /// override — capital controls warn but do not block; the DD2/P1-004
    /// hard amount cap binds regardless of `force`; fees are still
    /// recorded and count against the automated budget.
    pub fn manual_rebalance(
        &self,
        from_channel: &str,
        to_channel: &str,
        amount_sats: i64,
        max_fee_sats: Option<i64>,
        force: bool,
    ) -> OValue {
        // Normalize SCIDs to 'x' form for consistent DB storage.
        let from_channel = from_channel.replace(':', "x");
        let to_channel = to_channel.replace(':', "x");
        let mut amount_sats = amount_sats;
        let hard_max = self.config.rebalance_max_amount;
        if hard_max > 0 && amount_sats > hard_max {
            amount_sats = hard_max;
        }
        let capital_ok = self.check_capital_controls();

        let channels = self.channels_with_balances();
        let find = |scid: &str| channels.iter().find(|(c, _)| c == scid).map(|(_, b)| b);
        let (Some(f_info), Some(t_info)) = (find(&from_channel), find(&to_channel)) else {
            return OValue::obj(vec![(
                "error".to_string(),
                OValue::str("Channels not found"),
            )]);
        };

        let fee_ppm = t_info.fee_ppm;
        let src_ppm = f_info.fee_ppm;
        let est_in = self.estimate_inbound_fee(&t_info.peer_id);

        let max_fee_sats = max_fee_sats.unwrap_or_else(|| {
            // Manual push budget from the estimated spread, floor 100.
            let spread_fee =
                (amount_sats as f64 * (fee_ppm - est_in - src_ppm) as f64 / 1e6) as i64;
            if spread_fee <= 0 {
                100
            } else {
                spread_fee
            }
        });
        let max_fee_ppm = if amount_sats > 0 {
            (max_fee_sats as f64 * 1e6 / amount_sats as f64) as i64
        } else {
            0
        };

        let cand = RebalanceCandidate {
            source_candidates: vec![from_channel.clone()],
            to_channel: to_channel.clone(),
            primary_source_peer_id: f_info.peer_id.clone(),
            to_peer_id: t_info.peer_id.clone(),
            amount_sats,
            amount_msat: amount_sats * 1000,
            outbound_fee_ppm: fee_ppm,
            inbound_fee_ppm: est_in,
            source_fee_ppm: src_ppm,
            weighted_opp_cost_ppm: 0,
            spread_ppm: fee_ppm - est_in - src_ppm,
            max_budget_sats: max_fee_sats,
            max_budget_msat: max_fee_sats * 1000,
            max_fee_ppm,
            expected_profit_sats: 0,
            liquidity_ratio: 0.5,
            dest_flow_state: "manual".to_string(),
            dest_turnover_rate: 0.0,
            source_turnover_rate: 0.0,
        };

        // Manual rebalances bypass budget reservations; fees still recorded.
        let rebalance_id = self.store.insert_or_adopt_history(&HistoryRow {
            id: 0,
            from_channel,
            to_channel: to_channel.clone(),
            amount_sats,
            max_fee_sats,
            expected_profit_sats: 0,
            status: "pending".to_string(),
            rebalance_type: "manual".to_string(),
            reason_code: "manual".to_string(),
            payment_hash: None,
        });

        let exec_result =
            self.engine
                .execute_candidate(&cand, rebalance_id, engine_kwargs("manual"));

        let mut entries: Vec<(String, OValue)> = Vec::new();
        if exec_result.success {
            let fee_sats = self.record_successful_rebalance_fee(
                (rebalance_id > 0).then_some(rebalance_id),
                &to_channel,
                &t_info.peer_id,
                amount_sats,
                exec_result.fee_msat,
            );
            entries.push(("success".to_string(), OValue::Bool(true)));
            entries.push(("message".to_string(), OValue::str("completed")));
            entries.push(("actual_fee_sats".to_string(), OValue::Int(fee_sats)));
        } else {
            if !exec_result.payment_pending && rebalance_id > 0 {
                // Engine shares this history row; keep 'pending_settlement'
                // intact for the reconciliation sweep.
                self.store.update_history(
                    rebalance_id,
                    "failed",
                    Some(exec_result.error.as_deref().unwrap_or("")),
                );
            }
            entries.push(("success".to_string(), OValue::Bool(false)));
            entries.push((
                "error".to_string(),
                OValue::str(
                    exec_result
                        .error
                        .filter(|e| !e.is_empty())
                        .unwrap_or_else(|| "rebalance failed".to_string()),
                ),
            ));
        }
        if !capital_ok && !force {
            entries.push((
                "capital_controls_warning".to_string(),
                OValue::str("Budget exhausted or reserve low (manual override)"),
            ));
        }
        OValue::obj(entries)
    }

    // -- defibrillator -----------------------------------------------------

    /// py `diagnostic_rebalance` (`rebalancer.py:1715-1927`, v2.18.1): the
    /// "Channel Defibrillator" — set the bounded low-fee exploration flag
    /// (Passive Lure), then attempt a small active-shock rebalance through
    /// the ranked-source fallback loop. Message strings are byte-preserved
    /// RPC contracts.
    pub fn diagnostic_rebalance(&self, channel_id: &str) -> OValue {
        // 1. Exploration flag (the fee controller maps it to bounded
        // low-fee exploration above the configured floor).
        self.facade_store
            .set_channel_probe(channel_id, "bounded_low_fee");

        // 2. THE ACTIVE SHOCK.
        let channels = self.channels_with_balances();
        let Some((_, dest_info)) = channels.iter().find(|(scid, _)| scid == channel_id) else {
            return shock_result_raw(false, "failed", "Channel not found locally", None);
        };

        // Valid sources: healthy spendable, excluding the target (strict >).
        let valid_sources: Vec<&(String, ChannelBalance)> = channels
            .iter()
            .filter(|(scid, info)| scid != channel_id && info.spendable_sats > 100_000)
            .collect();
        if valid_sources.is_empty() {
            return shock_result_raw(
                true,
                "failed",
                "Exploration flag set, but no sources available for active shock.",
                None,
            );
        }

        // Rank by spendable desc (stable: ties keep listfunds order); the
        // shock retries down this list when route pricing says a source
        // cannot route (picking only the single largest source made the
        // diagnostic fail forever on an unroutable-peer source).
        let mut ranked_sources = valid_sources;
        ranked_sources.sort_by_key(|b| std::cmp::Reverse(b.1.spendable_sats));
        ranked_sources.truncate(DIAGNOSTIC_MAX_SOURCE_ATTEMPTS);

        // D4 shock fee envelope: the sat cap is the single binding knob.
        let (max_fee_sats, max_fee_ppm) = shock_fee_envelope(
            self.config.diagnostic_rebalance_max_fee_sats,
            self.config.daily_budget_sats,
        );

        // Estimated inbound fee (a small loss is an accepted diagnostic
        // cost; outbound is 0 — the shock is not the fee-controller path).
        let inbound_fee = self.estimate_inbound_fee(&dest_info.peer_id);

        // Capital controls: diagnostic rebalances count against the daily
        // budget. A blocked shock delivered no liquidity — report blocked,
        // never completed (defibrillation honesty).
        if !self.check_capital_controls() {
            return shock_result_raw(
                true,
                "blocked",
                "Zero-Fee flag set, but Active Shock blocked: daily budget exhausted or reserve too low",
                None,
            );
        }

        let dest_peer_id = dest_info.peer_id.clone();
        let mut actual_fee_sats: Option<i64> = None;
        let mut shock_ok = false;
        let mut shock_pending = false;
        let mut shock_budget_blocked = false;
        for (source_id, source_info) in ranked_sources {
            let candidate = RebalanceCandidate {
                source_candidates: vec![source_id.clone()],
                to_channel: channel_id.to_string(),
                primary_source_peer_id: source_info.peer_id.clone(),
                to_peer_id: dest_peer_id.clone(),
                amount_sats: SHOCK_AMOUNT_SATS,
                amount_msat: SHOCK_AMOUNT_SATS * 1000,
                outbound_fee_ppm: 0,
                inbound_fee_ppm: inbound_fee,
                source_fee_ppm: source_info.fee_ppm,
                weighted_opp_cost_ppm: 0,
                spread_ppm: 0, // likely negative; irrelevant for a diagnostic
                max_budget_sats: max_fee_sats,
                max_budget_msat: max_fee_sats * 1000,
                max_fee_ppm,
                expected_profit_sats: -50, // small expected diagnostic loss
                liquidity_ratio: 0.5,
                dest_flow_state: "diagnostic".to_string(),
                dest_turnover_rate: 0.0,
                source_turnover_rate: 0.0,
            };

            // Direct diagnostic execution bypasses the normal job flow but
            // records its own history row per attempt.
            let rebalance_id = self.store.insert_or_adopt_history(&HistoryRow {
                id: 0,
                from_channel: source_id.clone(),
                to_channel: channel_id.to_string(),
                amount_sats: SHOCK_AMOUNT_SATS,
                max_fee_sats,
                expected_profit_sats: -50,
                status: "pending".to_string(),
                rebalance_type: "diagnostic".to_string(),
                reason_code: "defibrillator".to_string(),
                payment_hash: None,
            });

            // P4-020: reserve_budget=True — the shock's fee cap reserves
            // atomically on the unified rail; exhaustion returns a
            // 'local_budget_block' result and never pays.
            // P4-025: account_costs=True — the ENGINE records the settled
            // fee and updates the shared history row to success; this
            // caller must NOT record the fee again (double count).
            let exec_result = self.engine.execute_candidate(
                &candidate,
                rebalance_id,
                engine_kwargs("diagnostic"),
            );

            if exec_result.success {
                actual_fee_sats = Some(base_to_sats_ceil(exec_result.fee_msat.max(0)));
            } else if !exec_result.payment_pending && rebalance_id > 0 {
                // Engine shares this history row; a pending row keeps its
                // 'pending_settlement' status for the reconcile sweep.
                self.store.update_history(
                    rebalance_id,
                    "failed",
                    Some(
                        exec_result
                            .error
                            .as_deref()
                            .filter(|e| !e.is_empty())
                            .unwrap_or("rebalance failed"),
                    ),
                );
            }
            shock_ok = exec_result.success;
            shock_pending = exec_result.payment_pending && !shock_ok;
            // A shock rejected by the atomic unified-budget reserve is a
            // capital-control block, not an execution failure (D4).
            shock_budget_blocked = !shock_ok
                && !shock_pending
                && exec_result
                    .error
                    .as_deref()
                    .unwrap_or("")
                    .contains(LOCAL_BUDGET_BLOCK);
            if shock_ok || shock_pending || shock_budget_blocked {
                break;
            }
            // Only a route-availability failure justifies the next source:
            // no payment was attempted, so a retry cannot double-pay. Any
            // other failure stops the sequence (a payment may have gone
            // out, or the cause is not source-specific).
            if !is_source_route_failure(exec_result.error.as_deref()) {
                break;
            }
        }

        // Defibrillation honesty: report the ACTUAL shock outcome.
        let shock_status = if shock_ok {
            "completed"
        } else if shock_pending {
            "pending"
        } else if shock_budget_blocked {
            "blocked"
        } else {
            "failed"
        };
        shock_result_raw(
            true,
            shock_status,
            &format!("Defibrillator active: Zero-Fee flag set + Shock {shock_status}"),
            actual_fee_sats,
        )
    }
}

/// py `_futility_key` (DEF-063): destination peer pubkey — stable across
/// splices — falling back to the SCID only when the peer id is unknown.
fn futility_key(candidate: &RebalanceCandidate) -> String {
    if candidate.to_peer_id.is_empty() {
        candidate.to_channel.clone()
    } else {
        candidate.to_peer_id.clone()
    }
}

/// py `_classify_error` (`rebalancer.py:522-532`): failure-informed routing
/// class for the dest futility breaker.
fn classify_error(error_msg: &str) -> &'static str {
    let msg = error_msg.to_lowercase();
    let any = |needles: &[&str]| needles.iter().any(|n| msg.contains(n));
    if any(&[
        "no route",
        "no_route",
        "unknown_next_peer",
        "no path",
        "no channels",
    ]) {
        "no_route"
    } else if any(&["timeout", "timed out", "deadline"]) {
        "timeout"
    } else if any(&["route_over_budget", "budget", "exceeded"]) {
        "budget_exceeded"
    } else {
        "other"
    }
}

/// Python-shaped failed `ExecutionResult` for facade-local refusals.
fn facade_failed(error: impl Into<String>, amount_sats: i64) -> ExecutionResult {
    ExecutionResult {
        success: false,
        attempts: 0,
        fee_sats: 0,
        fee_msat: 0,
        fee_ppm: 0,
        hops: 0,
        parts: 1,
        error: Some(error.into()),
        amount_sats,
        payment_pending: false,
        payment_hash: None,
        excluded_channels: Vec::new(),
        route_type: "native",
        failure_data: serde_json::json!({}),
    }
}

/// The diagnostic RPC response dict (py key order: success, shock_status,
/// message, then the conditional actual_fee_sats append).
fn shock_result_raw(
    success: bool,
    shock_status: &str,
    message: &str,
    actual_fee_sats: Option<i64>,
) -> OValue {
    let mut entries = vec![
        ("success".to_string(), OValue::Bool(success)),
        ("shock_status".to_string(), OValue::str(shock_status)),
        ("message".to_string(), OValue::str(message)),
    ];
    if let Some(fee) = actual_fee_sats {
        entries.push(("actual_fee_sats".to_string(), OValue::Int(fee)));
    }
    OValue::obj(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_error_table() {
        // Transcribed from rebalancer.py:522-532 (order matters: earlier
        // branches shadow later ones — "no_route budget" is no_route).
        let cases: &[(&str, &str)] = &[
            ("no route found", "no_route"),
            ("no_route", "no_route"),
            ("unknown_next_peer", "no_route"),
            ("askrene: no path", "no_route"),
            ("peer has no channels", "no_route"),
            ("payment timeout", "timeout"),
            ("timed out waiting", "timeout"),
            ("deadline exceeded", "timeout"), // "deadline" wins before "exceeded"
            ("route_over_budget: 5 > 2", "budget_exceeded"),
            ("local_budget_block", "budget_exceeded"), // contains "budget"
            ("limit exceeded", "budget_exceeded"),
            ("native_sendpay_error: WIRE_PERM", "other"),
            ("", "other"),
        ];
        for (input, expected) in cases {
            assert_eq!(classify_error(input), *expected, "input={input:?}");
        }
    }

    #[test]
    fn futility_key_prefers_peer_id() {
        let mut c = RebalanceCandidate {
            source_candidates: vec!["a".into()],
            to_channel: "chan".into(),
            primary_source_peer_id: String::new(),
            to_peer_id: "peer".into(),
            amount_sats: 1,
            amount_msat: 1000,
            outbound_fee_ppm: 0,
            inbound_fee_ppm: 0,
            source_fee_ppm: 0,
            weighted_opp_cost_ppm: 0,
            spread_ppm: 0,
            max_budget_sats: 0,
            max_budget_msat: 0,
            max_fee_ppm: 0,
            expected_profit_sats: 0,
            liquidity_ratio: 0.0,
            dest_flow_state: String::new(),
            dest_turnover_rate: 0.0,
            source_turnover_rate: 0.0,
        };
        assert_eq!(futility_key(&c), "peer");
        c.to_peer_id.clear();
        assert_eq!(futility_key(&c), "chan");
    }

    #[test]
    fn base_helpers_match_python_floor_ceil() {
        assert_eq!(base_to_sats_ceil(0), 0);
        assert_eq!(base_to_sats_ceil(1), 1);
        assert_eq!(base_to_sats_ceil(999), 1);
        assert_eq!(base_to_sats_ceil(1000), 1);
        assert_eq!(base_to_sats_ceil(1001), 2);
        assert_eq!(base_to_sats_floor(999), 0);
        assert_eq!(base_to_sats_floor(1999), 1);
    }
}
