//! `RebalanceEngine` orchestration + `RebalanceStore` seam + dry-run store
//! (port of `modules/rebalance_engine_v2.py`, `~/bin/cl_revenue_ops-port`,
//! branch `port`, v2.18.1): wires the T2 planner, T4 router, T5 executor,
//! T6 EV gate/cooldowns, and the MERGED unified budget rail
//! (`revops_db::budget::ReserveRequest` through [`RebalanceStore`]).
//!
//! ## Safety contracts carried by this module
//!
//! - **v2.18.1 pricing early-return** ([`RebalanceEngine::execute_candidate`],
//!   py `_execute_candidate_locked`, `rebalance_engine_v2.py:3300-3345`): a
//!   failed pricing returns `route_pricing_failed: ...` IMMEDIATELY — it
//!   never reaches `_execute_pair` with an empty route (which masked the
//!   real error as `native_route_invalid: missing_route` and burned a
//!   budget reservation per attempt). The two Python wrap formats are
//!   DISTINCT and both ported: a pricing EXCEPTION (the router seam's
//!   `Err`) wraps as `route_pricing_failed: {e}` (py `:3314`); a failed
//!   `RouteResult` wraps as `route_pricing_failed: {err} ({route_label})`
//!   (py `:3336`).
//! - **Pricing EXCEPTION aborts the whole cycle** (fix round 1, `p5-t7`):
//!   a pricing EXCEPTION inside `find_candidates` (the router seam's
//!   `Err` — distinct from a failed `RouteResult`, which stays a per-pair
//!   `no_route` skip, py `find_candidates` / `rebalance_engine_v2.py`
//!   pricing loop) propagates out and ABORTS the cycle with ZERO
//!   executions, matching Python's uncaught raise (`_route_pair` ->
//!   `_market_price_pair` -> `router.price_pair`, unprotected in the
//!   `find_candidates` loop) which propagates through `_run_cycle_locked`
//!   (`:3393`) and `rebalancer.py` (`:721`/`:844`, `finally` only, no
//!   `except`) — on a mid-cycle askrene flake, Python pays nothing, even
//!   for pairs that already priced. `run_cycle_locked` reports the abort
//!   as a single `audit_records` entry (reason `route_pricing_failed`,
//!   `detail` carrying the EXCEPTION wrap form `route_pricing_failed: {e}`
//!   — the same template as `execute_candidate_locked`'s py `:3314` site),
//!   with empty `candidates`/`executions` (Python never returns a value
//!   when `find_candidates` raises).
//! - **payment_pending** (P4-007/P4-009/P8-001): the engine never retries
//!   on top of a pending payment (exclusion retry and the partial-amount
//!   ladder both break on pending) and HOLDS the budget reservation only
//!   when `payment_pending && payment_hash` (a no-hash pending row can
//!   never be swept, so it records `failed` and releases now).
//! - **Budget rail** (P4-016): the FULL unified budget goes into
//!   [`RebalanceStore::reserve_budget`]'s `effective_budget_sats` — never
//!   pre-subtracted; budget blocks record history `skipped` (never
//!   `failed`) and never charge futility strikes or persisted cooldowns; a
//!   reservation refusal is refuse-and-continue at the cycle boundary,
//!   never an early `?` (3B checklist).
//! - **Single-flight**: `run_cycle` skips with a `cycle_already_running`
//!   audit marker; `execute_candidate` fails fast with `engine_busy`; the
//!   in-flight-destination guard is a COUNTED map ([`InflightDests`],
//!   P4-008), registered for the whole reserve+pay window.
//! - **120s cycle ceiling ABANDONS, never cancels**: an unfinished worker
//!   keeps running (its payment may already be sent); it finishes its own
//!   bookkeeping (record result, settle/release reservation) and clears its
//!   own inflight-dest registration. `run_cycle` returns without it.
//!
//! ## Port deviations (documented, deliberate)
//!
//! - `RebalanceEngine` has no lifetime parameter (the plan sketch wrote
//!   `RebalanceEngine<'a>`): the abandon-not-join concurrency contract
//!   forces worker threads to own `'static` handles, so every seam is an
//!   `Arc` in [`EngineDeps`] and the engine itself is a cheap [`Clone`]
//!   over one shared state block.
//! - `update_history`'s frozen `(id, status, err)` shape cannot carry the
//!   success row's settled amount / fee / `post_local_ratio` anchor or the
//!   pending row's `payment_hash`, so the trait adds the two richer
//!   siblings [`RebalanceStore::update_history_success`] and
//!   [`RebalanceStore::update_history_pending_settlement`] (the plan's
//!   trait sketch is elided — `record_rebalance_cost(&self, ...)` — so
//!   additions are in-contract; all sketched methods exist verbatim).
//!   Python's failed-path `actual_fee_sats/_msat` diagnostics are dropped
//!   at this seam (the narrow frozen signature wins; the full
//!   `ExecutionResult` is journaled/audited elsewhere).
//! - Audit richness (`RebalanceAudit` log streams, score-decomposition
//!   updates, `considered_candidates` deep copies, drain-demand netting for
//!   the Boltz loop-out) is deferred with `rebalance_state_v2.py` /
//!   `rebalance_audit_v2.py` per the plan's "Explicitly Deferred" section;
//!   skip records that Python ALSO appends to the plan/cycle result are
//!   kept (they are engine data, not log stream).
//! - The sats-EV gate consumes an [`EvProvider`] seam (the decomposition's
//!   raw inputs live in the deferred state builder); `failure_count *
//!   fee_sats * FAILURE_COST_RATE` is passed to [`sats_ev_gate`] as its OWN
//!   `failure_penalty_sats` term (Task 9 fix — `EvInputs` no longer folds
//!   this into the activity-penalty term, which disagreed with Python's
//!   left-to-right subtraction order in ~0.4% of randomized cases; pinned
//!   by `fixtures/rebalance/ev.json`'s `failure_penalty_fold_cases` and
//!   re-exercised via conformance scenario 17).
//! - `_get_global_budget_limit`'s optional provider hook is wiring-deferred:
//!   the limit is `max(0, config.daily_budget_sats)`.
//! - `executor.is_available()` reduces to the our-id self-heal probe
//!   ([`PaymentRpc::getinfo_id`]): the T5 executor is always constructible
//!   behind [`PairExecutor`], so "RPC surface unavailable" collapses to
//!   "node id unobtainable".

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::cooldowns::PairFutility;
use crate::errors::{
    classify_failure, cooldown_base_secs, CYCLE_ALREADY_RUNNING, DEST_INFLIGHT, ENGINE_BUSY,
    LOCAL_BUDGET_BLOCK, ROUTE_PRICING_FAILED_PREFIX, ZERO_BUDGET_BLOCKS_AUTO_REBALANCE,
};
use crate::ev::{
    per_attempt_ceiling, sats_ev_gate, EvInputs, BELOW_HOLD_MARGIN, FAILURE_COST_RATE,
};
use crate::executor::{ExecuteRequest, NativeRouteExecutor, PaymentMode, PaymentRpc};
use crate::modes::EngineKwargs;
use crate::planner::{self, PlannerChannel};
use crate::route_policy::{decide_route_policy, RoutePriority};
use crate::router::{CycleRouter, PlannedPairCtx, RouteResult, RouterRpc, RpcFailure, SendpayHop};
use crate::segstore::{SegmentObservationStore, DATASTORE_KEY};
use crate::types::{ExecutionResult, RebalanceCandidate, SkipRecord};
use revops_db::budget::ReserveRequest;
use revops_econ::pyfloat::py_round;
use revops_fees::pyjson::OValue;

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// Port of `RebalanceEngine._native_partial_amounts`
/// (`rebalance_engine_v2.py:2484-2510`): bounded descending partial-fill
/// retry amounts. Floor `min(original - 1, max(1_000, min(5_000,
/// original // 2)))`, halving steps, hard cap 7 amounts. Golden parity:
/// `fixtures/rebalance/partial_amounts.json`
/// (`tools/port/gen_rebalance_fixtures.py partial_amounts`).
pub fn native_partial_amounts(amount_sats: i64) -> Vec<i64> {
    let original = amount_sats;
    if original <= 0 {
        return Vec::new();
    }
    let min_amount = (original - 1).min(1_000.max(5_000.min(original / 2)));
    if min_amount <= 0 {
        return Vec::new();
    }
    let mut amounts: Vec<i64> = Vec::new();
    let mut current = original / 2;
    while current >= min_amount && amounts.len() < 6 {
        if current < original && !amounts.contains(&current) {
            amounts.push(current);
        }
        let next = current / 2;
        if next == current {
            break;
        }
        current = next;
    }
    if min_amount < original && !amounts.contains(&min_amount) {
        amounts.push(min_amount);
    }
    amounts.truncate(7);
    amounts
}

/// Port of `_classify_failure_kind` (`rebalance_engine_v2.py:1717-1735`)
/// returning the PERSISTED cooldown-table kind string (richer than
/// [`crate::errors::classify_failure`]'s enum: `local_execution_failed`
/// and `other_retriable` stay distinct strings even though both cool down
/// 600s). The bare `no_route` contract token classifies transient, matching
/// the documented T1 extension in `errors.rs`.
fn classify_failure_kind_str(error: &str) -> &'static str {
    let e = error.to_lowercase();
    if e.contains("temporary_channel_failure") {
        return "temporary_channel_failure";
    }
    if e.contains("noroutes")
        || e.contains("no_routes")
        || e.contains("no route")
        || e.contains("no_route")
    {
        return "temporary_channel_failure";
    }
    if e.contains("fee_insufficient") {
        return "fee_insufficient";
    }
    if e.contains("incorrect_cltv_expiry") {
        return "incorrect_cltv_expiry";
    }
    if e.contains("permanent_failure") {
        return "permanent_failure";
    }
    if e.contains("payment_pending_timeout") {
        return "payment_pending_timeout";
    }
    if e.contains("local_execution_failed") {
        return "local_execution_failed";
    }
    "other_retriable"
}

/// Port of `_probability_adjusted_budget`
/// (`rebalance_engine_v2.py:1775-1801`): relax the raw pair budget by a
/// probability-weighted bonus; bonus rate `<= 0` or no probability keeps
/// the budget unchanged. Python truncates the scaled float with `int()`.
fn probability_adjusted_budget(
    pair_budget_sats: i64,
    probability_ppm: i64,
    bonus_rate: f64,
) -> i64 {
    if bonus_rate <= 0.0 || probability_ppm <= 0 {
        return pair_budget_sats;
    }
    let clamped = (probability_ppm as f64 / 1_000_000.0).clamp(0.0, 1.0);
    (pair_budget_sats as f64 * (1.0 + clamped * bonus_rate)) as i64
}

/// `utils.base_to_sats_ceil` on non-negative msat (never undercount cost).
fn base_to_sats_ceil(base_msat: i64) -> i64 {
    debug_assert!(base_msat >= 0);
    (base_msat + 999) / 1000
}

/// `utils.base_to_sats_floor` on non-negative msat (never overcount
/// balances/filled amounts).
fn base_to_sats_floor(base_msat: i64) -> i64 {
    debug_assert!(base_msat >= 0);
    base_msat / 1000
}

/// `utils.parse_msat` for the JSON shapes reconcile meets (int / float /
/// `"Nmsat"` string / null; unparseable -> 0).
fn parse_msat(v: &Value) -> i64 {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f as i64))
            .unwrap_or(0),
        Value::String(s) => {
            let s = s.trim();
            let s = s.strip_suffix("msat").unwrap_or(s);
            s.parse().unwrap_or(0)
        }
        _ => 0,
    }
}

/// Integer-exact `max(1, ceil(a * b / c))` for the partial-ladder budget
/// scaling (`-(-original_budget * amount // original)` in Python), through
/// an i128 intermediate so the money path cannot overflow.
fn scaled_budget_ceil(original_budget: i64, amount: i64, original_amount: i64) -> i64 {
    debug_assert!(original_budget > 0 && amount > 0 && original_amount > 0);
    let product = original_budget as i128 * amount as i128;
    let ceil = (product + original_amount as i128 - 1) / original_amount as i128;
    i64::try_from(ceil).unwrap_or(i64::MAX).max(1)
}

/// Python-shaped failed `ExecutionResult` (dataclass defaults:
/// `attempts=0`, `route_type="native"`, `failure_data={}`).
fn failed_result(error: impl Into<String>, amount_sats: i64) -> ExecutionResult {
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
        failure_data: json!({}),
    }
}

/// `_is_budget_block` (`rebalance_engine_v2.py:2876-2884`): a budget-
/// reservation block attempted no route, so it must never count as a
/// routing failure anywhere (futility, persisted cooldowns, success rates).
fn is_budget_block(result: &ExecutionResult) -> bool {
    if result.success {
        return false;
    }
    let error = result.error.as_deref().unwrap_or("");
    error.starts_with(LOCAL_BUDGET_BLOCK) || error.starts_with("zero_budget_blocks")
}

/// `dict(getattr(result, "failure_data", {}) or {})`.
fn failure_data_map(v: &Value) -> Map<String, Value> {
    match v {
        Value::Object(m) => m.clone(),
        _ => Map::new(),
    }
}

fn setdefault(map: &mut Map<String, Value>, key: &str, value: Value) {
    if !map.contains_key(key) {
        map.insert(key.to_string(), value);
    }
}

// ---------------------------------------------------------------------------
// Store seam
// ---------------------------------------------------------------------------

/// One `rebalance_history` row as the engine reads/writes it (the column
/// subset the T7 flows touch; `id == 0` on insert means "assign one").
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRow {
    pub id: i64,
    pub from_channel: String,
    pub to_channel: String,
    pub amount_sats: i64,
    pub max_fee_sats: i64,
    pub expected_profit_sats: i64,
    pub status: String,
    pub rebalance_type: String,
    pub reason_code: String,
    pub payment_hash: Option<String>,
}

/// The success-row update (py `update_rebalance_result(..., "success",
/// actual_fee_sats=, actual_fee_msat=, post_local_ratio=, amount_sats=)`):
/// persists the Phase 3.3 post-rebalance anchor and the SETTLED amount
/// (partial fills correct `amount_sats` from the sendpay `amount_msat`).
#[derive(Debug, Clone, PartialEq)]
pub struct HistorySuccess {
    pub actual_fee_sats: i64,
    pub actual_fee_msat: i64,
    pub post_local_ratio: Option<f64>,
    /// `None` keeps the row's planned amount (py passes `None`).
    pub amount_sats: Option<i64>,
}

/// One `rebalance_costs` write (py `Database.record_rebalance_cost`).
#[derive(Debug, Clone, PartialEq)]
pub struct RebalanceCost {
    pub channel_id: String,
    pub peer_id: String,
    pub cost_sats: i64,
    pub cost_msat: i64,
    pub amount_sats: i64,
    pub timestamp: i64,
}

/// Opaque reservation handle (`reservation_id = str(row id)` for engine
/// reservations; the reconcile sweep reconstructs it the same way).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservationId(pub String);

/// Why `reserve_budget` did not grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetBlock {
    /// The unified rail refused: `remaining` headroom of the caller's
    /// budget limit. The engine formats the Python contract string
    /// `local_budget_block: {remaining} sats remaining of {limit} unified
    /// budget`.
    Refused { remaining: i64 },
    /// The rail itself failed (py: any exception out of
    /// `Database.reserve_budget` becomes `local_budget_block: {exc}`).
    Unavailable(String),
}

/// ALL engine DB writes go through this seam. The [`DryRunStore`] impl
/// journals JSONL to `<journal_dir>/rebalance_dryrun_journal.jsonl` and
/// holds reservations in-memory; the production impl (real
/// `rebalance_history`/`rebalance_costs`/`revops_db::budget::BudgetDb`
/// writes) lands at cutover, NOT in this plan.
pub trait RebalanceStore: Send + Sync {
    /// Insert a fresh `pending` row (`row.id == 0`) or adopt the caller's
    /// existing row id. Returns the row id; `<= 0` means the DB is
    /// unavailable (bookkeeping failure must not prevent execution — py
    /// `_record_rebalance_pending` returns `None`).
    fn insert_or_adopt_history(&self, row: &HistoryRow) -> i64;
    /// `pending -> success/failed/skipped/pending_settlement` status flip
    /// with the optional error message (frozen narrow shape; the richer
    /// success/pending-settlement writes have dedicated methods below).
    fn update_history(&self, id: i64, status: &str, err: Option<&str>);
    /// Success-row update — see [`HistorySuccess`].
    fn update_history_success(&self, id: i64, update: &HistorySuccess);
    /// Park a sweepable pending row as `pending_settlement` with its
    /// `payment_hash` (py `update_rebalance_result(..., payment_hash=)`).
    fn update_history_pending_settlement(&self, id: i64, error: &str, payment_hash: &str);
    /// Atomic reservation on the FULL unified budget (P4-016): the request
    /// carries the whole effective budget in `effective_budget_sats`,
    /// category `"rebalance"` — exactly `revops_db::budget::BudgetDb::
    /// reserve_budget`'s mapping.
    fn reserve_budget(&self, req: &ReserveRequest) -> Result<ReservationId, BudgetBlock>;
    fn mark_budget_spent(&self, rid: &ReservationId, actual_fee_sats: i64);
    fn release_reservation(&self, rid: &ReservationId);
    fn record_rebalance_cost(&self, cost: &RebalanceCost);
    /// Persist one pair failure with SQL-side backoff (py
    /// `Database.record_pair_rebalance_failure`: post-increment count,
    /// `cooldown_until = now + base * min(max(count, 1), 6)`).
    fn record_pair_failure(
        &self,
        source_channel_id: &str,
        dest_channel_id: &str,
        failure_kind: &str,
        base_cooldown_secs: i64,
        now: i64,
    );
    fn clear_pair_failures(&self, source_channel_id: &str, dest_channel_id: &str);
    /// Active persisted cooldown for a pair, if any (py
    /// `Database.get_pair_rebalance_cooldown` returning a row only while
    /// `cooldown_until > now`).
    fn pair_cooldown_until(
        &self,
        source_channel_id: &str,
        dest_channel_id: &str,
        now: i64,
    ) -> Option<i64>;
    fn pending_settlement_rows(&self) -> Vec<HistoryRow>;
    /// CLN datastore write-through (`create-or-replace`) — dry-run journals
    /// it instead of touching the node.
    fn datastore_export(&self, key: &[&str], payload: &OValue);
}

// ---------------------------------------------------------------------------
// Engine seams
// ---------------------------------------------------------------------------

/// One cycle-scoped pricing handle. `Err` is the Python EXCEPTION path
/// (wrapped `route_pricing_failed: {e}`); `Ok` with `success == false` is
/// the failed-`RouteResult` path (wrapped `route_pricing_failed: {err}
/// ({route_label})`).
pub trait EngineRouter {
    fn price_pair(
        &mut self,
        ctx: &PlannedPairCtx,
        exclude: &[String],
    ) -> Result<RouteResult, RpcFailure>;
}

/// Mints cycle-scoped routers. `None` = router unavailable (askrene
/// missing — fail closed, py `_active_router() is None`). Retries construct
/// a FRESH router per pricing call for Python's ephemeral out-of-cycle
/// layer semantics (each throwaway exclude layer is created and torn down
/// around the single call).
pub trait RouterFactory: Send + Sync {
    fn begin_cycle(&self) -> Option<Box<dyn EngineRouter + '_>>;
}

/// Production [`RouterFactory`] over the T4 [`CycleRouter`] (via
/// `try_price_pair`, which preserves the exception-path distinction).
/// Always available — the askrene probe/fail-closed gate is plugin-wiring's
/// concern at cutover.
pub struct CycleRouterFactory<R: RouterRpc + Send + Sync> {
    pub rpc: R,
}

struct CycleRouterAdapter<'a> {
    router: CycleRouter<'a>,
}

impl EngineRouter for CycleRouterAdapter<'_> {
    fn price_pair(
        &mut self,
        ctx: &PlannedPairCtx,
        exclude: &[String],
    ) -> Result<RouteResult, RpcFailure> {
        self.router.try_price_pair(ctx, exclude)
    }
}

impl<R: RouterRpc + Send + Sync> RouterFactory for CycleRouterFactory<R> {
    fn begin_cycle(&self) -> Option<Box<dyn EngineRouter + '_>> {
        Some(Box::new(CycleRouterAdapter {
            router: CycleRouter::begin_cycle(&self.rpc),
        }))
    }
}

/// Execution seam: the engine drives the T5 executor through this so tests
/// can script `ExecutionResult`s directly (the plan's "scripted ...
/// executor doubles").
pub trait PairExecutor: Send + Sync {
    fn execute(&self, req: &ExecuteRequest) -> ExecutionResult;
}

/// Production [`PairExecutor`]: a fresh [`NativeRouteExecutor`] per call
/// (py `_make_executor` per cycle) over the T5 payment seam.
pub struct NativeExecutorSeam {
    pub rpc: Arc<dyn PaymentRpc + Send + Sync>,
    pub mode: PaymentMode,
    pub segstore: Arc<SegmentObservationStore>,
}

impl PairExecutor for NativeExecutorSeam {
    fn execute(&self, req: &ExecuteRequest) -> ExecutionResult {
        NativeRouteExecutor {
            rpc: &*self.rpc,
            mode: self.mode,
            segstore: &self.segstore,
        }
        .execute(req)
    }
}

/// The peer-policy read the pair gate needs (py `policy_manager
/// .get_policy(peer_id)` reduced to the two consulted fields; enum values
/// arrive as their Python string forms).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerPolicy {
    pub strategy: String,
    pub rebalance_mode: String,
}

/// Policy source. The ENGINE fails closed when it has none (py
/// `_pair_policy_allowed`: "policy manager unavailable (fail closed)");
/// an `Err` from the impl also fails closed.
pub trait PolicyManager: Send + Sync {
    fn get_policy(&self, peer_id: &str) -> Result<PeerPolicy, String>;
}

/// Optional cycle arbitration (Workstream H). Fail OPEN: an `Err` falls
/// back to the un-arbitrated list (downstream authorization still fails
/// closed on the budget rail).
pub trait Arbiter: Send + Sync {
    fn arbitrate(&self, candidates: &[PairCandidate]) -> Result<Vec<PairCandidate>, String>;
}

/// Already-computed sats-EV decomposition terms for one pair (the raw
/// reduction lives in the deferred state builder; see the module docs).
#[derive(Debug, Clone, PartialEq)]
pub struct EvTerms {
    pub dest_attempts: i64,
    pub dest_success_rate: f64,
    pub efv_sats: f64,
    pub source_opportunity_sats: f64,
    pub activity_penalty_sats: f64,
}

/// Sats-EV term source. `None` (no provider, or no terms for a pair) skips
/// the hold-margin gate — matching Python's `final_score_present` guard
/// (`rebalance_engine_v2.py:1464-1472`).
pub trait EvProvider: Send + Sync {
    fn ev_terms(&self, pair: &PairCandidate) -> Option<EvTerms>;
}

/// Channel snapshot source in the T2 `PlannerChannel` shape (the
/// `rebalance_state_v2.py` builder is ported with plugin wiring; this seam
/// keeps the boundary explicit). `None`/empty = no cycle.
pub trait SnapshotProvider: Send + Sync {
    fn channels(&self) -> Option<Vec<PlannerChannel>>;
}

/// Injected wall clock (fractional seconds since the epoch).
pub trait EngineClock: Send + Sync {
    fn now(&self) -> f64;
}

/// Production [`EngineClock`].
pub struct SystemClock;

impl EngineClock for SystemClock {
    fn now(&self) -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }
}

/// Reconcile-sweep RPC surface (`listsendpays` per parked payment_hash;
/// one `listpeerchannels` for SCID -> peer cost attribution). `delpay`
/// reuses the T5 [`PaymentRpc`] seam.
pub trait ReconcileRpc: Send + Sync {
    fn listsendpays(&self, payment_hash: &str) -> Result<Value, RpcFailure>;
    fn listpeerchannels_full(&self) -> Result<Value, RpcFailure>;
}

// ---------------------------------------------------------------------------
// Engine data types
// ---------------------------------------------------------------------------

/// The engine's working pair (py `PairCandidate`, the field subset the T7
/// flows read/mutate). `dest_capacity_sats`/`dest_local_ratio` feed the
/// Phase 3.3 post-rebalance anchor.
#[derive(Debug, Clone, PartialEq)]
pub struct PairCandidate {
    pub source_channel_id: String,
    pub dest_channel_id: String,
    pub source_peer_id: String,
    pub dest_peer_id: String,
    pub amount_sats: i64,
    pub pair_budget_sats: i64,
    /// find_candidates' per-attempt ceiling (audit F1); `None` for manual
    /// pairs — execution falls back to the raw authorized budget
    /// (py `_pair_max_fee_sats`).
    pub effective_budget_sats: Option<i64>,
    pub route_cost_sats: i64,
    pub route: Vec<SendpayHop>,
    pub reason_code: String,
    pub score: f64,
    pub dest_capacity_sats: i64,
    pub dest_local_ratio: f64,
}

/// Full result of one v2 rebalance cycle (py `CycleResult`, minus the
/// deferred snapshot/plan/considered-candidates audit richness).
#[derive(Debug, Clone, Default)]
pub struct CycleResult {
    pub candidates: Vec<PairCandidate>,
    pub executions: Vec<ExecutionResult>,
    pub audit_records: Vec<SkipRecord>,
}

/// Engine configuration (the config-snapshot fields the T7 flows read,
/// with the Python config defaults).
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// py `rebalance_max_amount` (max planner chunk), default 2_000_000.
    pub rebalance_max_amount: i64,
    /// py `max_concurrent_jobs`, default 5, clamped 1..=20.
    pub max_concurrent_jobs: i64,
    /// py `pair_fee_cap_ppm`, default 0 (ceiling disabled).
    pub pair_fee_cap_ppm: i64,
    /// py `capex_probability_budget_bonus`, default 0.0.
    pub capex_probability_budget_bonus: f64,
    /// py `rebalance_hold_margin`, default 0.0.
    pub rebalance_hold_margin: f64,
    /// py `daily_budget_sats` — the unified global budget limit fallback
    /// (`_get_global_budget_limit`).
    pub daily_budget_sats: i64,
    /// py `weekly_budget_sats` (None = no weekly rail).
    pub weekly_budget_sats: Option<i64>,
    /// py `total_cost_budget_window_hours`, default 24 (min 1).
    pub total_cost_budget_window_hours: i64,
    /// py `allow_zero_cost_auto_rebalance_when_budget_zero`, default false.
    pub allow_zero_cost_auto_rebalance_when_budget_zero: bool,
    /// Configured askrene layer names (post `configured_layer_names`).
    pub askrene_layer_names: Vec<String>,
    /// The node's `cltv-final` (py `_get_invoice_final_cltv` default 18).
    pub invoice_final_cltv: i64,
    /// The `as_completed` collection ceiling — 120s in production
    /// (`rebalance_engine_v2.py:3597`), injectable for tests.
    pub cycle_timeout_secs: f64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            rebalance_max_amount: 2_000_000,
            max_concurrent_jobs: 5,
            pair_fee_cap_ppm: 0,
            capex_probability_budget_bonus: 0.0,
            rebalance_hold_margin: 0.0,
            daily_budget_sats: 0,
            weekly_budget_sats: None,
            total_cost_budget_window_hours: 24,
            allow_zero_cost_auto_rebalance_when_budget_zero: false,
            askrene_layer_names: Vec::new(),
            invoice_final_cltv: 18,
            cycle_timeout_secs: 120.0,
        }
    }
}

/// P4-008 in-flight-destination guard: a COUNTED map (never a set) so
/// overlapping executions to one destination release correctly. Each
/// execution registers its dest for the whole reserve+pay window and
/// unregisters in a drop guard; `find_candidates` and explicit executions
/// drop/refuse any registered destination.
#[derive(Debug, Default)]
pub struct InflightDests {
    inner: Mutex<HashMap<String, i64>>,
}

impl InflightDests {
    /// Py `_register_inflight_dest` (empty ids ignored).
    pub fn register(&self, dest_channel_id: &str) {
        if dest_channel_id.is_empty() {
            return;
        }
        *self
            .inner
            .lock()
            .expect("inflight mutex poisoned")
            .entry(dest_channel_id.to_string())
            .or_insert(0) += 1;
    }

    /// Py `_unregister_inflight_dest`: decrement, dropping the entry at
    /// zero (never negative).
    pub fn unregister(&self, dest_channel_id: &str) {
        if dest_channel_id.is_empty() {
            return;
        }
        let mut map = self.inner.lock().expect("inflight mutex poisoned");
        match map.get_mut(dest_channel_id) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                map.remove(dest_channel_id);
            }
            None => {}
        }
    }

    /// Py `_inflight_dest_snapshot`.
    pub fn snapshot(&self) -> HashSet<String> {
        self.inner
            .lock()
            .expect("inflight mutex poisoned")
            .keys()
            .cloned()
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Everything the engine needs, injected (no ambient RPC, no ambient
/// clock, no ambient DB — Global Constraints).
pub struct EngineDeps {
    pub config: EngineConfig,
    pub store: Arc<dyn RebalanceStore>,
    pub router_factory: Arc<dyn RouterFactory>,
    pub executor: Arc<dyn PairExecutor>,
    pub payment_rpc: Arc<dyn PaymentRpc + Send + Sync>,
    pub reconcile_rpc: Arc<dyn ReconcileRpc>,
    pub segstore: Arc<SegmentObservationStore>,
    pub snapshot: Arc<dyn SnapshotProvider>,
    pub clock: Arc<dyn EngineClock>,
    pub policy: Option<Arc<dyn PolicyManager>>,
    pub arbiter: Option<Arc<dyn Arbiter>>,
    pub ev: Option<Arc<dyn EvProvider>>,
}

struct EngineShared {
    config: EngineConfig,
    store: Arc<dyn RebalanceStore>,
    router_factory: Arc<dyn RouterFactory>,
    executor: Arc<dyn PairExecutor>,
    payment_rpc: Arc<dyn PaymentRpc + Send + Sync>,
    reconcile_rpc: Arc<dyn ReconcileRpc>,
    segstore: Arc<SegmentObservationStore>,
    snapshot: Arc<dyn SnapshotProvider>,
    clock: Arc<dyn EngineClock>,
    policy: Option<Arc<dyn PolicyManager>>,
    arbiter: Option<Arc<dyn Arbiter>>,
    ev: Option<Arc<dyn EvProvider>>,
    cycle_lock: Mutex<()>,
    inflight: InflightDests,
    pair_futility: Mutex<PairFutility>,
    our_id: Mutex<String>,
    /// Fallback reservation-id nonce (py `time.time_ns()` uniqueness).
    reservation_nonce: AtomicI64,
}

/// V2 rebalance engine — unified, actual-fee-based (py `RebalanceEngine`).
/// Cheap [`Clone`]: all clones share one state block (cycle lock, inflight
/// guard, futility tracker).
#[derive(Clone)]
pub struct RebalanceEngine {
    shared: Arc<EngineShared>,
}

/// Unregisters the inflight dest even when the worker path unwinds
/// (py `finally: self._unregister_inflight_dest(...)`).
struct InflightGuard<'a> {
    inflight: &'a InflightDests,
    dest: String,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.inflight.unregister(&self.dest);
    }
}

impl RebalanceEngine {
    pub fn new(deps: EngineDeps) -> Self {
        RebalanceEngine {
            shared: Arc::new(EngineShared {
                config: deps.config,
                store: deps.store,
                router_factory: deps.router_factory,
                executor: deps.executor,
                payment_rpc: deps.payment_rpc,
                reconcile_rpc: deps.reconcile_rpc,
                segstore: deps.segstore,
                snapshot: deps.snapshot,
                clock: deps.clock,
                policy: deps.policy,
                arbiter: deps.arbiter,
                ev: deps.ev,
                cycle_lock: Mutex::new(()),
                inflight: InflightDests::default(),
                pair_futility: Mutex::new(PairFutility::new()),
                our_id: Mutex::new(String::new()),
                reservation_nonce: AtomicI64::new(1),
            }),
        }
    }

    /// Live execution: find candidates (already priced), execute
    /// concurrently (py `run_cycle`, `rebalance_engine_v2.py:3356-3626`).
    /// Non-blocking single-flight: a held engine lock skips with a
    /// `cycle_already_running` audit marker.
    pub fn run_cycle(&self) -> CycleResult {
        let Ok(_guard) = self.shared.cycle_lock.try_lock() else {
            return CycleResult {
                audit_records: vec![SkipRecord {
                    channel_id: String::new(),
                    reason: CYCLE_ALREADY_RUNNING.to_string(),
                    value_class: "none".to_string(),
                    remaining_budget_sats: 0,
                    detail: Some("engine cycle lock held by another caller".to_string()),
                }],
                ..CycleResult::default()
            };
        };
        self.shared.run_cycle_locked()
    }

    /// Price and execute one explicit candidate on the v2 stack (py
    /// `execute_candidate`, `rebalance_engine_v2.py:3196-3354`).
    /// Non-blocking single-flight: fails fast with `engine_busy`.
    /// `rebalance_id <= 0` means "no caller-owned history row" (py `None`).
    pub fn execute_candidate(
        &self,
        candidate: &RebalanceCandidate,
        rebalance_id: i64,
        kw: EngineKwargs,
    ) -> ExecutionResult {
        let Ok(_guard) = self.shared.cycle_lock.try_lock() else {
            return failed_result(ENGINE_BUSY, candidate.amount_sats.max(0));
        };
        self.shared.execute_candidate_locked(
            candidate,
            if rebalance_id > 0 {
                Some(rebalance_id)
            } else {
                None
            },
            kw,
        )
    }

    /// Resolve rebalance payments that previously timed out unresolved (py
    /// `reconcile_pending_settlements`, `rebalance_engine_v2.py:2992-3034`).
    /// Returns the number of rows resolved either way.
    pub fn reconcile_pending_settlements(&self) -> usize {
        self.shared.reconcile_pending_settlements()
    }

    /// Read-only probe of the in-memory pair futility breaker (py
    /// `_is_pair_in_futility`; exposed for callers/tests that need the
    /// boolean without mutating anything but the prune).
    pub fn is_pair_in_futility(&self, source_channel_id: &str, dest_channel_id: &str) -> bool {
        let now = self.shared.clock.now();
        self.shared
            .pair_futility
            .lock()
            .expect("futility mutex poisoned")
            .is_futile(source_channel_id, dest_channel_id, now)
    }

    /// Test hook: charge one in-memory pair failure (py
    /// `_record_pair_failure`) without driving a whole failed cycle.
    #[doc(hidden)]
    pub fn debug_note_pair_failure(&self, source_channel_id: &str, dest_channel_id: &str) {
        let now = self.shared.clock.now();
        self.shared
            .pair_futility
            .lock()
            .expect("futility mutex poisoned")
            .record_failure(source_channel_id, dest_channel_id, now);
    }
}

impl EngineShared {
    // -- shared plumbing ---------------------------------------------------

    /// Cached node id with the per-call getinfo self-heal
    /// (`rebalance_router_v3.py:346-359`, an incident fix: a transient
    /// getinfo failure at init must not freeze `our_node_id=""` for the
    /// process lifetime). The engine guarantees the ctx it passes to the
    /// router seam is populated whenever getinfo can answer.
    fn ensure_our_id(&self) -> String {
        let mut cached = self.our_id.lock().expect("our_id mutex poisoned");
        if cached.is_empty() {
            if let Ok(id) = self.payment_rpc.getinfo_id() {
                *cached = id.trim().to_string();
            }
        }
        cached.clone()
    }

    fn now(&self) -> f64 {
        self.clock.now()
    }

    fn max_concurrent_jobs(&self) -> usize {
        self.config.max_concurrent_jobs.clamp(1, 20) as usize
    }

    fn pair_ctx(&self, pair: &PairCandidate, our_id: &str) -> PlannedPairCtx {
        PlannedPairCtx {
            our_node_id: our_id.to_string(),
            source_channel_id: pair.source_channel_id.clone(),
            dest_channel_id: pair.dest_channel_id.clone(),
            source_peer_id: pair.source_peer_id.clone(),
            dest_peer_id: pair.dest_peer_id.clone(),
            amount_sats: pair.amount_sats,
            layer_names: self.config.askrene_layer_names.clone(),
            invoice_final_cltv: self.config.invoice_final_cltv,
        }
    }

    /// Py `_pair_max_fee_sats`: the stored per-attempt ceiling IS the fee
    /// envelope when present, even below the raw budget.
    fn pair_max_fee_sats(pair: &PairCandidate) -> i64 {
        pair.effective_budget_sats.unwrap_or(pair.pair_budget_sats)
    }

    /// Py `_pair_policy_allowed` (`rebalance_engine_v2.py:2710-2744`):
    /// source forbids draining under disabled/sink_only; dest forbids
    /// filling under disabled/source_only; passive forbids the pair. Fails
    /// CLOSED on a missing/broken policy source.
    fn pair_policy_allowed(&self, pair: &PairCandidate) -> (bool, String) {
        let Some(policy_manager) = &self.policy else {
            return (
                false,
                "policy manager unavailable (fail closed)".to_string(),
            );
        };
        let checks: [(&str, &str, [&str; 2]); 2] = [
            (
                pair.source_peer_id.as_str(),
                "source",
                ["disabled", "sink_only"],
            ),
            (
                pair.dest_peer_id.as_str(),
                "dest",
                ["disabled", "source_only"],
            ),
        ];
        for (peer_id, side, forbidden) in checks {
            if peer_id.is_empty() {
                continue;
            }
            let policy = match policy_manager.get_policy(peer_id) {
                Ok(p) => p,
                Err(e) => return (false, format!("policy check failed (fail closed): {e}")),
            };
            if policy.strategy == "passive" {
                return (false, format!("{side} peer policy is passive"));
            }
            let mode = if policy.rebalance_mode.is_empty() {
                "enabled".to_string()
            } else {
                policy.rebalance_mode.clone()
            };
            if forbidden.contains(&mode.as_str()) {
                return (
                    false,
                    format!("{side} rebalance_mode={mode} forbids this direction"),
                );
            }
        }
        (true, "policy allows pair".to_string())
    }

    /// One pricing call on a FRESH router (retry paths: Python's
    /// out-of-cycle `price_pair` uses per-call ephemeral layer semantics).
    /// `None` router keeps Python's "return prior result unchanged"
    /// contract at the callers.
    fn route_pair_fresh(
        &self,
        pair: &PairCandidate,
        exclude: &[String],
    ) -> Option<Result<(RouteResult, &'static str), RpcFailure>> {
        let mut router = self.router_factory.begin_cycle()?;
        let ctx = self.pair_ctx(pair, &self.ensure_our_id());
        Some(router.price_pair(&ctx, exclude).map(|r| (r, "market")))
    }

    // -- budget rail -------------------------------------------------------

    /// Py `_get_global_budget_limit` (provider hook wiring-deferred).
    fn global_budget_limit(&self) -> i64 {
        self.config.daily_budget_sats.max(0)
    }

    /// Py `_reserve_execution_budget` (`rebalance_engine_v2.py:1887-1991`).
    /// Returns `(reserved, Some(block-result))`; a `Some` result is
    /// refuse-and-continue at the caller — never an early `?`.
    fn reserve_execution_budget(
        &self,
        pair: &PairCandidate,
        reservation_id: &str,
    ) -> (bool, Option<ExecutionResult>) {
        let max_fee_sats = Self::pair_max_fee_sats(pair).max(0);
        let effective_budget = self.global_budget_limit();
        if effective_budget <= 0 {
            if max_fee_sats <= 0 && self.config.allow_zero_cost_auto_rebalance_when_budget_zero {
                return (false, None);
            }
            return (
                false,
                Some(failed_result(
                    ZERO_BUDGET_BLOCKS_AUTO_REBALANCE,
                    pair.amount_sats.max(0),
                )),
            );
        }
        if max_fee_sats <= 0 {
            return (false, None);
        }

        let now = self.now() as i64;
        let window_hours = self.config.total_cost_budget_window_hours.max(1);
        let request = ReserveRequest {
            reservation_id: reservation_id.to_string(),
            amount_sats: max_fee_sats,
            category: "rebalance".to_string(),
            channel_id: Some(pair.dest_channel_id.clone()),
            // P4-016: the FULL unified budget — the rail counts every
            // category exactly once inside its own transaction.
            effective_budget_sats: Some(effective_budget),
            since_timestamp: Some(now - window_hours * 3600),
            weekly_budget_limit: self.config.weekly_budget_sats,
            weekly_since_timestamp: Some(now - 7 * 86_400),
            ..ReserveRequest::default()
        };
        match self.store.reserve_budget(&request) {
            Ok(_rid) => (true, None),
            Err(BudgetBlock::Refused { remaining }) => (
                false,
                Some(failed_result(
                    format!(
                        "{LOCAL_BUDGET_BLOCK}: {remaining} sats remaining of \
                         {effective_budget} unified budget"
                    ),
                    pair.amount_sats.max(0),
                )),
            ),
            Err(BudgetBlock::Unavailable(msg)) => (
                false,
                Some(failed_result(
                    format!("{LOCAL_BUDGET_BLOCK}: {msg}"),
                    pair.amount_sats.max(0),
                )),
            ),
        }
    }

    /// Py `_finish_execution_budget` (`rebalance_engine_v2.py:2289-2334`):
    /// success -> mark spent (actual fee); pending WITH hash -> HOLD for
    /// the reconcile sweep (P4-007); everything else -> release.
    fn finish_execution_budget(
        &self,
        reservation_id: &str,
        reserved_budget: bool,
        result: &ExecutionResult,
    ) {
        if !reserved_budget {
            return;
        }
        let rid = ReservationId(reservation_id.to_string());
        if result.payment_pending {
            let sweepable = result
                .payment_hash
                .as_deref()
                .is_some_and(|h| !h.is_empty());
            if sweepable {
                // Keep the reservation active: releasing here would let the
                // next cycle spend the same budget while the HTLC can still
                // settle.
                return;
            }
            self.store.release_reservation(&rid);
            return;
        }
        if result.success {
            self.store.mark_budget_spent(&rid, result.fee_sats.max(0));
        } else {
            self.store.release_reservation(&rid);
        }
    }

    // -- history bookkeeping ----------------------------------------------

    /// Py `_record_rebalance_pending` (`rebalance_engine_v2.py:2845-2874`).
    fn record_rebalance_pending(&self, pair: &PairCandidate) -> Option<i64> {
        let reason_code = if pair.reason_code.is_empty() {
            "ev_positive".to_string()
        } else {
            pair.reason_code.clone()
        };
        let id = self.store.insert_or_adopt_history(&HistoryRow {
            id: 0,
            from_channel: pair.source_channel_id.clone(),
            to_channel: pair.dest_channel_id.clone(),
            amount_sats: pair.amount_sats,
            max_fee_sats: pair.pair_budget_sats.max(0),
            expected_profit_sats: 0,
            status: "pending".to_string(),
            rebalance_type: "normal".to_string(),
            reason_code,
            payment_hash: None,
        });
        (id > 0).then_some(id)
    }

    /// Py `_record_rebalance_result` (`rebalance_engine_v2.py:2885-2990`).
    fn record_rebalance_result(
        &self,
        rebalance_id: Option<i64>,
        result: &ExecutionResult,
        pair: &PairCandidate,
        account_costs: bool,
    ) {
        let Some(id) = rebalance_id else { return };
        if result.success {
            // Phase 3.3: project the destination's post-rebalance local
            // ratio anchor from the pre-rebalance ratio + amount.
            let post_local_ratio = (pair.dest_capacity_sats > 0).then(|| {
                (pair.dest_local_ratio + pair.amount_sats as f64 / pair.dest_capacity_sats as f64)
                    .clamp(0.0, 1.0)
            });
            let amount_sats = if result.amount_sats > 0 {
                result.amount_sats
            } else {
                pair.amount_sats
            };
            self.store.update_history_success(
                id,
                &HistorySuccess {
                    actual_fee_sats: result.fee_sats.max(0),
                    actual_fee_msat: result.fee_msat.max(0),
                    post_local_ratio,
                    amount_sats: Some(amount_sats.max(0)),
                },
            );
            if account_costs {
                let mut fee_msat = result.fee_msat;
                if fee_msat <= 0 {
                    fee_msat = result.fee_sats.max(0) * 1000;
                }
                if fee_msat > 0 {
                    self.store.record_rebalance_cost(&RebalanceCost {
                        channel_id: pair.dest_channel_id.clone(),
                        peer_id: pair.dest_peer_id.clone(),
                        cost_sats: base_to_sats_ceil(fee_msat),
                        cost_msat: fee_msat,
                        amount_sats: amount_sats.max(0),
                        timestamp: self.now() as i64,
                    });
                }
            }
            return;
        }
        if result.payment_pending {
            let error = result.error.clone().unwrap_or_default();
            match result.payment_hash.as_deref().filter(|h| !h.is_empty()) {
                Some(hash) => {
                    // Park for the reconciliation sweep; the cost lands once
                    // listsendpays reports a terminal state.
                    self.store
                        .update_history_pending_settlement(id, &error, hash);
                }
                None => {
                    // Never sweepable — record a terminal failure instead of
                    // a dead parked row.
                    let message = format!(
                        "{error} (payment pending but missing payment_hash; not sweepable)"
                    );
                    self.store
                        .update_history(id, "failed", Some(message.trim()));
                }
            }
            return;
        }
        let status = if is_budget_block(result) {
            // Budget blocks never attempted a route: 'skipped' keeps them
            // out of the terminal success-rate aggregate.
            "skipped"
        } else {
            "failed"
        };
        self.store
            .update_history(id, status, Some(result.error.as_deref().unwrap_or("")));
    }

    // -- retries -----------------------------------------------------------

    /// Py `_retry_native_pair_with_exclusions`
    /// (`rebalance_engine_v2.py:2378-2483`): retry once after excluding
    /// failed route segments; NEVER on top of a pending payment.
    fn retry_with_exclusions(
        &self,
        pair: &mut PairCandidate,
        mut first_result: ExecutionResult,
    ) -> ExecutionResult {
        if first_result.success || first_result.payment_pending {
            return first_result;
        }
        let mut exclusions: Vec<String> = Vec::new();
        for entry in &first_result.excluded_channels {
            let value = entry.trim().to_string();
            if !value.is_empty() && !exclusions.contains(&value) {
                exclusions.push(value);
            }
        }
        if exclusions.is_empty() {
            return first_result;
        }

        let mut retry_data = failure_data_map(&first_result.failure_data);
        retry_data.insert(
            "retry_excluded_channels".to_string(),
            json!(exclusions.clone()),
        );
        let Some(priced) = self.route_pair_fresh(pair, &exclusions) else {
            return first_result; // router unavailable: prior result unchanged
        };
        let route_result = match priced {
            Err(exc) => {
                let prior = first_result.error.clone().unwrap_or_default();
                first_result.error = Some(format!("{prior}; retry_pricing_failed: {exc}"));
                retry_data.insert("retry_error".to_string(), json!(exc.message));
                first_result.failure_data = Value::Object(retry_data);
                first_result.attempts = first_result.attempts.max(1) + 1;
                return first_result;
            }
            Ok((rr, _label)) if !rr.success => {
                let detail = rr
                    .error
                    .clone()
                    .filter(|e| !e.is_empty())
                    .unwrap_or_else(|| "no_route".to_string());
                let prior = first_result.error.clone().unwrap_or_default();
                first_result.error = Some(format!("{prior}; retry_no_route: {detail}"));
                retry_data.insert("retry_error".to_string(), json!(detail));
                first_result.failure_data = Value::Object(retry_data);
                first_result.attempts = first_result.attempts.max(1) + 1;
                return first_result;
            }
            Ok((rr, _label)) => rr,
        };

        let mut effective_budget = probability_adjusted_budget(
            pair.pair_budget_sats,
            route_result.probability_ppm,
            self.config.capex_probability_budget_bonus,
        );
        // Audit F1: the retry route must honor the same per-attempt ceiling
        // as the original acceptance.
        effective_budget = per_attempt_ceiling(
            effective_budget,
            pair.amount_sats,
            self.config.pair_fee_cap_ppm,
        );
        if route_result.route_cost_sats > effective_budget {
            let prior = first_result.error.clone().unwrap_or_default();
            first_result.error = Some(format!(
                "{prior}; retry_route_over_budget: {} > {effective_budget}",
                route_result.route_cost_sats
            ));
            retry_data.insert("retry_error".to_string(), json!("route_over_budget"));
            first_result.failure_data = Value::Object(retry_data);
            first_result.attempts = first_result.attempts.max(1) + 1;
            return first_result;
        }

        pair.route = route_result.route.clone();
        pair.route_cost_sats = route_result.route_cost_sats;
        let mut retry_result = self.executor.execute(&self.execution_request(pair));
        let first_attempts = first_result.attempts.max(1);
        let retry_attempts = retry_result.attempts.max(1);
        retry_result.attempts = first_attempts + retry_attempts;
        let mut merged = failure_data_map(&retry_result.failure_data);
        setdefault(
            &mut merged,
            "previous_failure",
            json!(first_result.error.clone().unwrap_or_default()),
        );
        setdefault(
            &mut merged,
            "retry_excluded_channels",
            json!(exclusions.clone()),
        );
        setdefault(
            &mut merged,
            "retry_route_cost_sats",
            json!(pair.route_cost_sats),
        );
        retry_result.failure_data = Value::Object(merged);
        if !retry_result.success {
            let mut combined = retry_result.excluded_channels.clone();
            for entry in exclusions {
                if !combined.contains(&entry) {
                    combined.push(entry);
                }
            }
            retry_result.excluded_channels = combined;
        }
        retry_result
    }

    /// Py `_native_failure_allows_partial`
    /// (`rebalance_engine_v2.py:2512-2521`).
    fn failure_allows_partial(result: &ExecutionResult) -> bool {
        if result.success || result.route_type != "native" {
            return false;
        }
        let class = result.failure_data["failure_class"]
            .as_str()
            .unwrap_or("")
            .to_lowercase();
        if class == "liquidity" {
            return true;
        }
        result
            .error
            .as_deref()
            .unwrap_or("")
            .to_lowercase()
            .contains("temporary_channel_failure")
    }

    /// Py `_retry_native_pair_with_partial_amounts`
    /// (`rebalance_engine_v2.py:2523-2708`): halving ladder after a
    /// liquidity failure — budget scaled ceil-proportionally, per-attempt
    /// ceiling re-applied at the retry amount, **break on pending**
    /// (P8-001).
    fn retry_with_partial_amounts(
        &self,
        pair: &mut PairCandidate,
        mut prior_result: ExecutionResult,
    ) -> ExecutionResult {
        if prior_result.payment_pending {
            // The first payment may still settle — never pay again on top.
            return prior_result;
        }
        if !Self::failure_allows_partial(&prior_result) {
            return prior_result;
        }

        let original_amount = pair.amount_sats;
        let original_route = pair.route.clone();
        let original_route_cost = pair.route_cost_sats;
        let original_budget = pair.pair_budget_sats;
        let original_effective_budget = pair.effective_budget_sats;
        let mut partial_attempts: Vec<Value> = Vec::new();
        let mut total_attempts = prior_result.attempts.max(1);

        for amount_sats in native_partial_amounts(original_amount) {
            pair.amount_sats = amount_sats;
            // Scale the fee budget proportionally to the retry amount so the
            // partial fill keeps the original plan's fee-rate ceiling.
            let scaled_budget = if original_amount > 0 && original_budget > 0 {
                scaled_budget_ceil(original_budget, amount_sats, original_amount)
            } else {
                original_budget
            };
            pair.pair_budget_sats = scaled_budget;

            let Some(priced) = self.route_pair_fresh(pair, &[]) else {
                break; // router unavailable: return prior (restored below)
            };
            let route_result = match priced {
                Err(exc) => {
                    partial_attempts.push(json!({
                        "amount_sats": amount_sats,
                        "status": "pricing_error",
                        "error": exc.message,
                    }));
                    continue;
                }
                Ok((rr, _label)) if !rr.success => {
                    let detail = rr
                        .error
                        .clone()
                        .filter(|e| !e.is_empty())
                        .unwrap_or_else(|| "no_route".to_string());
                    partial_attempts.push(json!({
                        "amount_sats": amount_sats,
                        "status": "no_route",
                        "error": detail,
                    }));
                    continue;
                }
                Ok((rr, _label)) => rr,
            };

            let route_cost = route_result.route_cost_sats;
            let mut effective_budget = probability_adjusted_budget(
                scaled_budget,
                route_result.probability_ppm,
                self.config.capex_probability_budget_bonus,
            );
            // Audit F1: partial fills honor the ppm ceiling at the retry
            // amount, same as the original acceptance.
            effective_budget =
                per_attempt_ceiling(effective_budget, amount_sats, self.config.pair_fee_cap_ppm);
            pair.effective_budget_sats = Some(effective_budget);
            if route_cost > effective_budget {
                partial_attempts.push(json!({
                    "amount_sats": amount_sats,
                    "status": "route_over_budget",
                    "route_cost_sats": route_cost,
                    "effective_budget_sats": effective_budget,
                }));
                continue;
            }

            pair.route = route_result.route.clone();
            pair.route_cost_sats = route_cost;
            let mut retry_result = self.executor.execute(&self.execution_request(pair));
            total_attempts += retry_result.attempts.max(1);

            if retry_result.success {
                retry_result.attempts = total_attempts;
                if retry_result.amount_sats <= 0 {
                    retry_result.amount_sats = amount_sats;
                }
                let mut merged = failure_data_map(&retry_result.failure_data);
                setdefault(
                    &mut merged,
                    "previous_failure",
                    json!(prior_result.error.clone().unwrap_or_default()),
                );
                let mut attempts_trail = partial_attempts.clone();
                attempts_trail.push(json!({"amount_sats": amount_sats, "status": "success"}));
                merged.insert(
                    "partial_fill".to_string(),
                    json!({
                        "planned_amount_sats": original_amount,
                        "executed_amount_sats": retry_result.amount_sats,
                        "route_cost_sats": route_cost,
                        "attempts": attempts_trail,
                    }),
                );
                retry_result.failure_data = Value::Object(merged);
                // Pair stays mutated at the partial amount (py behavior):
                // the success bookkeeping anchors on the FILLED amount.
                return retry_result;
            }

            if retry_result.payment_pending {
                // P8-001: the HTLC is still in flight against this
                // reservation — BREAK, never dispatch another amount on top.
                retry_result.attempts = total_attempts;
                let mut merged = failure_data_map(&retry_result.failure_data);
                setdefault(
                    &mut merged,
                    "previous_failure",
                    json!(prior_result.error.clone().unwrap_or_default()),
                );
                let mut attempts_trail = partial_attempts.clone();
                attempts_trail
                    .push(json!({"amount_sats": amount_sats, "status": "payment_pending"}));
                merged.insert(
                    "partial_fill".to_string(),
                    json!({
                        "planned_amount_sats": original_amount,
                        "executed_amount_sats": 0,
                        "attempts": attempts_trail,
                    }),
                );
                retry_result.failure_data = Value::Object(merged);
                return retry_result;
            }

            partial_attempts.push(json!({
                "amount_sats": amount_sats,
                "status": "execution_failed",
                "error": retry_result.error.clone().unwrap_or_default(),
                "excluded_channels": retry_result.excluded_channels.clone(),
            }));
        }

        // Ladder exhausted: restore the pair and annotate the prior result.
        pair.amount_sats = original_amount;
        pair.route = original_route;
        pair.route_cost_sats = original_route_cost;
        pair.pair_budget_sats = original_budget;
        pair.effective_budget_sats = original_effective_budget;
        let mut data = failure_data_map(&prior_result.failure_data);
        data.insert(
            "partial_fill".to_string(),
            json!({
                "planned_amount_sats": original_amount,
                "executed_amount_sats": 0,
                "attempts": partial_attempts.clone(),
            }),
        );
        prior_result.failure_data = Value::Object(data);
        prior_result.attempts = total_attempts;
        if let Some(last) = partial_attempts.last() {
            let prior_error = prior_result.error.clone().unwrap_or_default();
            prior_result.error = Some(format!(
                "{prior_error}; partial_retry_failed: {}",
                last["status"].as_str().unwrap_or("")
            ));
        }
        prior_result
    }

    // -- execution ---------------------------------------------------------

    fn execution_request(&self, pair: &PairCandidate) -> ExecuteRequest {
        ExecuteRequest {
            route: pair.route.clone(),
            amount_sats: pair.amount_sats,
            source_channel_id: pair.source_channel_id.clone(),
            dest_channel_id: pair.dest_channel_id.clone(),
            max_fee_sats: Self::pair_max_fee_sats(pair),
            our_id: self.ensure_our_id(),
            now_ms: (self.now() * 1000.0) as i64,
        }
    }

    /// Py `_execute_pair` (`rebalance_engine_v2.py:2745-2843`): the whole
    /// reserve+pay critical section of one worker.
    fn execute_pair(
        &self,
        pair: &mut PairCandidate,
        reserve_budget: bool,
        account_costs: bool,
        rebalance_id: Option<i64>,
    ) -> ExecutionResult {
        // Lazy re-check at the execution sink: candidate selection may be
        // minutes old and policy is a fresh write-through read.
        let (policy_ok, policy_reason) = self.pair_policy_allowed(pair);
        if !policy_ok {
            return failed_result(
                format!("policy_blocked: {policy_reason}"),
                pair.amount_sats.max(0),
            );
        }

        let rebalance_id = match rebalance_id {
            Some(id) => Some(id),
            None => self.record_rebalance_pending(pair),
        };
        let reservation_id = match rebalance_id {
            Some(id) => id.to_string(),
            None => format!(
                "v2-{}-{}-{}",
                self.reservation_nonce.fetch_add(1, Ordering::Relaxed),
                pair.source_channel_id,
                pair.dest_channel_id
            ),
        };

        // P4-008: guard the destination for the ENTIRE reserve+pay window;
        // the drop guard clears it however this path ends (that self-clean
        // is what lets an abandoned worker release the dest later).
        self.inflight.register(&pair.dest_channel_id);
        let _inflight = InflightGuard {
            inflight: &self.inflight,
            dest: pair.dest_channel_id.clone(),
        };

        let mut reserved_budget = false;
        if reserve_budget {
            let (reserved, block) = self.reserve_execution_budget(pair, &reservation_id);
            reserved_budget = reserved;
            if let Some(block_result) = block {
                self.record_rebalance_result(rebalance_id, &block_result, pair, false);
                return block_result;
            }
        }

        let result = self.executor.execute(&self.execution_request(pair));
        let result = self.retry_with_exclusions(pair, result);
        let result = self.retry_with_partial_amounts(pair, result);

        self.record_rebalance_result(rebalance_id, &result, pair, account_costs);
        self.finish_execution_budget(&reservation_id, reserved_budget, &result);
        if !result.success {
            self.push_segment_observation_snapshot();
        }
        result
    }

    /// Py `_execute_candidate_locked` (`rebalance_engine_v2.py:3243-3354`)
    /// — including the v2.18.1 `route_pricing_failed` early-return, the
    /// VERBATIM template for both wrap formats.
    fn execute_candidate_locked(
        &self,
        candidate: &RebalanceCandidate,
        rebalance_id: Option<i64>,
        kw: EngineKwargs,
    ) -> ExecutionResult {
        let source_channel_id = candidate
            .source_candidates
            .first()
            .cloned()
            .unwrap_or_default();
        let dest_channel_id = candidate.to_channel.clone();
        let source_peer_id = candidate.primary_source_peer_id.clone();
        let dest_peer_id = candidate.to_peer_id.clone();
        let amount_sats = candidate.amount_sats;
        let max_fee_sats = candidate.max_budget_sats;

        if source_channel_id.is_empty() || dest_channel_id.is_empty() {
            return failed_result("invalid_channel_ids", 0);
        }
        if source_peer_id.is_empty() || dest_peer_id.is_empty() {
            return failed_result("missing_peer_ids", 0);
        }
        if amount_sats <= 0 {
            return failed_result("invalid_amount", 0);
        }

        // P4-008 applies to explicit executions too: a prior cycle's
        // orphaned worker may still hold an unresolved sendpay here.
        if self.inflight.snapshot().contains(&dest_channel_id) {
            return failed_result(DEST_INFLIGHT, amount_sats);
        }

        let mut pair = PairCandidate {
            source_channel_id,
            dest_channel_id,
            source_peer_id,
            dest_peer_id,
            amount_sats,
            pair_budget_sats: max_fee_sats,
            effective_budget_sats: None,
            route_cost_sats: max_fee_sats,
            route: Vec::new(),
            // T1's frozen RebalanceCandidate carries no reason_code field;
            // explicit executions take Python's `or "manual"` fallback.
            reason_code: "manual".to_string(),
            score: 0.0,
            dest_capacity_sats: 0,
            dest_local_ratio: 0.0,
        };

        let Some(mut router) = self.router_factory.begin_cycle() else {
            return failed_result("router_unavailable", amount_sats);
        };
        let ctx = self.pair_ctx(&pair, &self.ensure_our_id());
        let priced = router.price_pair(&ctx, &[]);
        drop(router); // end_cycle (tears down throwaway layers)
        match priced {
            Err(exc) => {
                // Pricing EXCEPTION wrap (py :3314): NO route label.
                return failed_result(format!("{ROUTE_PRICING_FAILED_PREFIX}{exc}"), amount_sats);
            }
            Ok(route_result) if !route_result.success => {
                // v2.18.1: a failed pricing means there is no route to
                // execute. Falling through to _execute_pair with an empty
                // route masked the real getroutes error as
                // `native_route_invalid: missing_route` and burned a budget
                // reservation per attempt. Failed-RouteResult wrap
                // (py :3336): WITH the route label.
                let detail = route_result
                    .error
                    .filter(|e| !e.is_empty())
                    .unwrap_or_else(|| "no_route".to_string());
                return failed_result(
                    format!("{ROUTE_PRICING_FAILED_PREFIX}{detail} (market)"),
                    amount_sats,
                );
            }
            Ok(route_result) => {
                pair.route_cost_sats = route_result.route_cost_sats;
                pair.route = route_result.route;
            }
        }

        self.execute_pair(&mut pair, kw.reserve_budget, kw.account_costs, rebalance_id)
    }

    // -- candidate selection ----------------------------------------------

    /// Py `find_candidates` (`rebalance_engine_v2.py:1220-1601`), reduced
    /// per the module docs (audit stream + score-decomposition enrichment
    /// deferred). Returns the priced selection plus every skip record.
    ///
    /// `Err` is the pricing-EXCEPTION abort path (fix round 1, `p5-t7`): a
    /// pricing EXCEPTION (the router seam's `Err`) aborts the ENTIRE
    /// function immediately, discarding every skip collected so far in this
    /// call — matching Python's uncaught raise out of `find_candidates`,
    /// which never reaches its own `return`. A failed `RouteResult` is
    /// UNCHANGED: still a per-pair `no_route` skip that continues the loop.
    fn find_candidates(&self) -> Result<(Vec<PairCandidate>, Vec<SkipRecord>), RpcFailure> {
        let Some(channels) = self.snapshot.channels() else {
            return Ok((Vec::new(), Vec::new()));
        };
        if channels.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let Some(mut router) = self.router_factory.begin_cycle() else {
            // askrene unavailable; the v3 router is required (fail closed).
            return Ok((Vec::new(), Vec::new()));
        };

        let cfg = &self.config;
        let plan = planner::plan(
            &channels,
            cfg.rebalance_max_amount,
            self.max_concurrent_jobs(),
            cfg.pair_fee_cap_ppm,
        );
        let mut skips = plan.skips;

        let by_id: HashMap<&str, &PlannerChannel> = channels
            .iter()
            .map(|c| (c.channel_id.as_str(), c))
            .collect();
        let local_ratio = |ch: &PlannerChannel| -> f64 {
            if ch.capacity_sats <= 0 {
                return 0.0;
            }
            py_round(
                (ch.spendable_sats.max(0) as f64 / ch.capacity_sats as f64).clamp(0.0, 1.0),
                6,
            )
        };

        let mut selected: Vec<PairCandidate> = Vec::new();
        let inflight = self.inflight.snapshot();
        for planned in &plan.pairs {
            let source = by_id.get(planned.source.as_str());
            let dest = by_id.get(planned.dest.as_str());
            let pair = PairCandidate {
                source_channel_id: planned.source.clone(),
                dest_channel_id: planned.dest.clone(),
                source_peer_id: source.map(|c| c.peer_id.clone()).unwrap_or_default(),
                dest_peer_id: dest.map(|c| c.peer_id.clone()).unwrap_or_default(),
                amount_sats: planned.amount_sats,
                pair_budget_sats: planned.pair_budget_sats,
                effective_budget_sats: None,
                route_cost_sats: 0,
                route: Vec::new(),
                reason_code: "ev_positive".to_string(),
                score: planned.score,
                dest_capacity_sats: dest.map(|c| c.capacity_sats).unwrap_or(0),
                dest_local_ratio: dest.map(|c| local_ratio(c)).unwrap_or(0.0),
            };
            // P4-008: drop any selected pair whose destination still has an
            // outstanding reserve+pay in flight.
            if inflight.contains(&pair.dest_channel_id) {
                skips.push(SkipRecord {
                    channel_id: pair.dest_channel_id.clone(),
                    reason: DEST_INFLIGHT.to_string(),
                    value_class: "valuable".to_string(),
                    remaining_budget_sats: pair.pair_budget_sats.max(0),
                    detail: Some(format!(
                        "src={} dest has an in-flight/unresolved payment from a prior cycle",
                        pair.source_channel_id
                    )),
                });
                continue;
            }
            selected.push(pair);
        }

        // Route-policy priority ordering (all pairs are EV_POSITIVE with the
        // frozen stub, so this reduces to a stable score-descending sort —
        // the full key is kept for shape parity).
        let priority_rank = |p: RoutePriority| -> i64 {
            match p {
                RoutePriority::EvPositive => 2,
                RoutePriority::Background => 3,
            }
        };
        selected.sort_by(|a, b| {
            let da = decide_route_policy(&a.reason_code);
            let db = decide_route_policy(&b.reason_code);
            priority_rank(da.priority)
                .cmp(&priority_rank(db.priority))
                .then(db.priority_score.total_cmp(&da.priority_score))
                .then(b.score.total_cmp(&a.score))
        });

        // Price the selection on the cycle router.
        let our_id = self.ensure_our_id();
        let now = self.now();
        let mut priced: Vec<PairCandidate> = Vec::new();
        for mut pair in selected {
            // Persisted cooldown, then in-memory futility, BEFORE pricing
            // (a doomed pair must not spend the router RPC budget).
            if let Some(until) = self.store.pair_cooldown_until(
                &pair.source_channel_id,
                &pair.dest_channel_id,
                now as i64,
            ) {
                skips.push(SkipRecord {
                    channel_id: pair.dest_channel_id.clone(),
                    reason: "pair_cooldown".to_string(),
                    value_class: "valuable".to_string(),
                    remaining_budget_sats: 0,
                    // Reduced detail: the frozen store seam returns only
                    // cooldown_until (py also formats kind/count).
                    detail: Some(format!(
                        "src={} cooldown_until={until}",
                        pair.source_channel_id
                    )),
                });
                continue;
            }
            {
                let mut futility = self.pair_futility.lock().expect("futility mutex poisoned");
                if futility.is_futile(&pair.source_channel_id, &pair.dest_channel_id, now) {
                    let fresh = futility.fresh_failure_count(
                        &pair.source_channel_id,
                        &pair.dest_channel_id,
                        now,
                    );
                    skips.push(SkipRecord {
                        channel_id: pair.dest_channel_id.clone(),
                        reason: "pair_futility".to_string(),
                        value_class: "valuable".to_string(),
                        remaining_budget_sats: pair.pair_budget_sats.max(0),
                        detail: Some(format!(
                            "src={} failures={fresh} in window {}s",
                            pair.source_channel_id,
                            PairFutility::WINDOW_SECS as i64
                        )),
                    });
                    continue;
                }
            }

            let ctx = self.pair_ctx(&pair, &our_id);
            let route_result = match router.price_pair(&ctx, &[]) {
                Ok(rr) => rr,
                Err(exc) => {
                    // Python-true (fix round 1, `p5-t7`): a pricing
                    // EXCEPTION aborts the WHOLE cycle — it never reaches
                    // Python's own `return`, so every skip collected in
                    // this call (including this one) is discarded too.
                    drop(router); // end_cycle before unwinding
                    return Err(exc);
                }
            };
            if !route_result.success {
                // Native execution requires a priced explicit route.
                skips.push(SkipRecord {
                    channel_id: pair.dest_channel_id.clone(),
                    reason: "no_route".to_string(),
                    value_class: "valuable".to_string(),
                    remaining_budget_sats: pair.pair_budget_sats.max(0),
                    detail: Some(
                        route_result
                            .error
                            .filter(|e| !e.is_empty())
                            .unwrap_or_else(|| "router_no_route".to_string()),
                    ),
                });
                continue;
            }

            pair.route_cost_sats = route_result.route_cost_sats;
            pair.route = route_result.route.clone();
            let mut effective_budget = probability_adjusted_budget(
                pair.pair_budget_sats,
                route_result.probability_ppm,
                cfg.capex_probability_budget_bonus,
            );
            // Audit F1: the per-attempt fee ceiling bounds route acceptance;
            // the pair budget remains the reservation amount cap.
            effective_budget =
                per_attempt_ceiling(effective_budget, pair.amount_sats, cfg.pair_fee_cap_ppm);
            pair.effective_budget_sats = Some(effective_budget);

            if route_result.route_cost_sats > effective_budget {
                skips.push(SkipRecord {
                    channel_id: pair.dest_channel_id.clone(),
                    reason: "route_over_budget".to_string(),
                    value_class: "valuable".to_string(),
                    remaining_budget_sats: pair.pair_budget_sats.max(0),
                    detail: Some(format!(
                        "route_cost={} effective_budget={effective_budget} probability_ppm={}",
                        route_result.route_cost_sats, route_result.probability_ppm
                    )),
                });
                continue;
            }

            // Phase 4.3 / audit F2: sats-denominated do-nothing hard gate.
            // Zero-cost routes bypass it inside sats_ev_gate (they spend no
            // capital — the zero-budget equalization invariant); a missing
            // EV provider or missing terms matches py's
            // `final_score_present` guard.
            if let Some(terms) = self.ev.as_ref().and_then(|p| p.ev_terms(&pair)) {
                let failure_count = self
                    .pair_futility
                    .lock()
                    .expect("futility mutex poisoned")
                    .fresh_failure_count(&pair.source_channel_id, &pair.dest_channel_id, now);
                // Task 9 fix: failure_penalty_sats is EvInputs's own field,
                // subtracted by sats_ev_gate in Python's exact sequential
                // order (no longer folded into activity_penalty_sats).
                let failure_penalty_sats =
                    failure_count as f64 * route_result.route_cost_sats as f64 * FAILURE_COST_RATE;
                let verdict = sats_ev_gate(&EvInputs {
                    probability_ppm: route_result.probability_ppm,
                    dest_attempts: terms.dest_attempts,
                    dest_success_rate: terms.dest_success_rate,
                    efv_sats: terms.efv_sats,
                    fee_sats: route_result.route_cost_sats,
                    source_opportunity_sats: terms.source_opportunity_sats,
                    failure_penalty_sats,
                    activity_penalty_sats: terms.activity_penalty_sats,
                    hold_margin_sats: self.config.rebalance_hold_margin,
                });
                if !verdict.pass {
                    skips.push(SkipRecord {
                        channel_id: pair.dest_channel_id.clone(),
                        reason: BELOW_HOLD_MARGIN.to_string(),
                        value_class: "valuable".to_string(),
                        remaining_budget_sats: 0,
                        detail: Some(format!(
                            "src={} score={:.4} margin={:.4}",
                            pair.source_channel_id,
                            verdict.final_score_sats,
                            self.config.rebalance_hold_margin
                        )),
                    });
                    continue;
                }
            }

            priced.push(pair);
        }
        drop(router); // end_cycle

        Ok((priced, skips))
    }

    // -- cycle -------------------------------------------------------------

    /// Py `_run_cycle_locked` (`rebalance_engine_v2.py:3388-3626`).
    fn run_cycle_locked(self: &Arc<Self>) -> CycleResult {
        self.reconcile_pending_settlements();
        let (candidates, skips) = match self.find_candidates() {
            Ok(pair) => pair,
            Err(exc) => {
                // Python-true abort surface (fix round 1, `p5-t7`): a
                // pricing EXCEPTION aborts the whole cycle with ZERO
                // executions and ZERO candidates -- Python's `run_cycle`
                // never returns a value when `find_candidates` raises (the
                // exception propagates through `_run_cycle_locked` and
                // `rebalancer.py`, which only has `finally`, never
                // `except`). Reported the same way this module already
                // reports other whole-cycle aborts (`cycle_already_running`):
                // a single `audit_records` entry, `detail` carrying the
                // EXCEPTION wrap form (`route_pricing_failed: {e}`, the
                // same template as `execute_candidate_locked`'s py `:3314`
                // site -- no route label, unlike the failed-`RouteResult`
                // wrap).
                return CycleResult {
                    candidates: Vec::new(),
                    executions: Vec::new(),
                    audit_records: vec![SkipRecord {
                        channel_id: String::new(),
                        reason: "route_pricing_failed".to_string(),
                        value_class: "none".to_string(),
                        remaining_budget_sats: 0,
                        detail: Some(format!("{ROUTE_PRICING_FAILED_PREFIX}{exc}")),
                    }],
                };
            }
        };
        let mut result = CycleResult {
            candidates: candidates.clone(),
            executions: Vec::new(),
            audit_records: skips,
        };
        if candidates.is_empty() {
            return result;
        }

        // Policy gate (fail closed) + futility backstop. Python emits these
        // to the audit log stream only; they do not join audit_records.
        let now = self.now();
        let mut live_candidates: Vec<PairCandidate> = Vec::new();
        for pair in candidates {
            let (policy_ok, _reason) = self.pair_policy_allowed(&pair);
            if !policy_ok {
                continue;
            }
            let futile = self
                .pair_futility
                .lock()
                .expect("futility mutex poisoned")
                .is_futile(&pair.source_channel_id, &pair.dest_channel_id, now);
            if futile {
                continue;
            }
            live_candidates.push(pair);
        }
        result.candidates = live_candidates.clone();
        if live_candidates.is_empty() {
            return result;
        }

        // Workstream H: optional batch arbitration. Fail OPEN — an
        // arbitration-stage error falls back to the legacy list.
        if let Some(arbiter) = &self.arbiter {
            if let Ok(arbitrated) = arbiter.arbitrate(&live_candidates) {
                live_candidates = arbitrated;
            }
            result.candidates = live_candidates.clone();
            if live_candidates.is_empty() {
                return result;
            }
        }

        // Executor availability probe (reduced: our-id self-heal — see the
        // module docs).
        if self.ensure_our_id().is_empty() {
            result.candidates = Vec::new();
            return result;
        }

        // Execution cap: overflow pairs get max_pairs_reached skip records
        // (these DO join audit_records — py appends the SkipRecord).
        let execution_limit = self.max_concurrent_jobs();
        if live_candidates.len() > execution_limit {
            for pair in live_candidates.split_off(execution_limit) {
                result.audit_records.push(SkipRecord {
                    channel_id: pair.dest_channel_id.clone(),
                    reason: "max_pairs_reached".to_string(),
                    value_class: "valuable".to_string(),
                    remaining_budget_sats: pair.pair_budget_sats.max(0),
                    detail: Some(format!(
                        "src={} max_concurrent_jobs={execution_limit}",
                        pair.source_channel_id
                    )),
                });
            }
            result.candidates = live_candidates.clone();
        }

        // Spawn one worker per pair. Workers own Arc'd shared state so the
        // collection ceiling can ABANDON them without cancelling the
        // reserve+pay critical section (the payment may already be sent).
        let (tx, rx) = mpsc::channel::<(String, String, ExecutionResult)>();
        let worker_count = live_candidates.len();
        for pair in live_candidates {
            let shared = Arc::clone(self);
            let tx = tx.clone();
            std::thread::spawn(move || {
                let mut pair = pair;
                let src = pair.source_channel_id.clone();
                let dst = pair.dest_channel_id.clone();
                let exec_result = shared.execute_pair(&mut pair, true, true, None);
                // The receiver may have abandoned us; bookkeeping above is
                // already durable, so a failed send is fine.
                let _ = tx.send((src, dst, exec_result));
            });
        }
        drop(tx);

        // Collect as they complete, with the cycle ceiling. Python's
        // future.cancel() branch is unreachable here: every worker thread
        // has already started (the pool never queues beyond the cap).
        let deadline =
            Instant::now() + Duration::from_secs_f64(self.config.cycle_timeout_secs.max(0.0));
        let mut consumed = 0usize;
        while consumed < worker_count {
            let timeout = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(timeout) {
                Ok((src, dst, exec_result)) => {
                    self.consume_worker_result(&src, &dst, &exec_result, &mut result);
                    consumed += 1;
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Drain workers that finished in the same instant
                    // (py: `future.done()` after the TimeoutError)...
                    while let Ok((src, dst, exec_result)) = rx.try_recv() {
                        self.consume_worker_result(&src, &dst, &exec_result, &mut result);
                    }
                    // ...then ABANDON the rest: they finish bookkeeping
                    // asynchronously and self-clean via the counted
                    // inflight-dest map.
                    break;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        result
    }

    /// Py `consume_future_result` (`rebalance_engine_v2.py:3554-3585`).
    fn consume_worker_result(
        &self,
        source_channel_id: &str,
        dest_channel_id: &str,
        exec_result: &ExecutionResult,
        result: &mut CycleResult,
    ) {
        result.executions.push(exec_result.clone());
        if exec_result.success {
            self.pair_futility
                .lock()
                .expect("futility mutex poisoned")
                .record_success(source_channel_id, dest_channel_id);
            self.store
                .clear_pair_failures(source_channel_id, dest_channel_id);
        } else if is_budget_block(exec_result) {
            // No route was attempted — a depleted unified budget says
            // nothing about this pair's routability: no futility strikes,
            // no persisted cooldowns.
        } else {
            let now = self.now();
            self.pair_futility
                .lock()
                .expect("futility mutex poisoned")
                .record_failure(source_channel_id, dest_channel_id, now);
            let error = exec_result.error.as_deref().unwrap_or("");
            let kind = classify_failure_kind_str(error);
            let base = cooldown_base_secs(classify_failure(error));
            self.store.record_pair_failure(
                source_channel_id,
                dest_channel_id,
                kind,
                base,
                now as i64,
            );
        }
    }

    // -- reconcile sweep ---------------------------------------------------

    fn reconcile_pending_settlements(&self) -> usize {
        let rows = self.store.pending_settlement_rows();
        let mut resolved = 0;
        // SCID -> peer map is built at most once per sweep, lazily.
        let mut peer_map: Option<HashMap<String, String>> = None;
        for row in rows {
            if self.reconcile_pending_row(&row, &mut peer_map) {
                resolved += 1;
            }
        }
        resolved
    }

    fn scid_peer_map(&self) -> HashMap<String, String> {
        let mut mapping = HashMap::new();
        if let Ok(response) = self.reconcile_rpc.listpeerchannels_full() {
            for channel in response["channels"].as_array().into_iter().flatten() {
                let scid = channel["short_channel_id"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .or_else(|| channel["alias"]["local"].as_str().filter(|s| !s.is_empty()));
                let Some(scid) = scid else { continue };
                mapping
                    .entry(scid.to_string())
                    .or_insert_with(|| channel["peer_id"].as_str().unwrap_or("").to_string());
            }
        }
        mapping
    }

    /// Py `_reconcile_pending_row` (`rebalance_engine_v2.py:3036-3114`).
    fn reconcile_pending_row(
        &self,
        row: &HistoryRow,
        peer_map: &mut Option<HashMap<String, String>>,
    ) -> bool {
        let rebalance_id = row.id;
        let payment_hash = row.payment_hash.clone().unwrap_or_default();
        if rebalance_id <= 0 || payment_hash.is_empty() {
            return false;
        }
        let Ok(response) = self.reconcile_rpc.listsendpays(&payment_hash) else {
            // py: the per-row exception is caught, logged, and skipped.
            return false;
        };
        let empty = Vec::new();
        let payments: Vec<&Value> = response["payments"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter(|p| p.is_object())
            .collect();
        let statuses: HashSet<&str> = payments
            .iter()
            .map(|p| p["status"].as_str().unwrap_or(""))
            .collect();

        if statuses.contains("pending") {
            return false;
        }

        let reservation_id = ReservationId(rebalance_id.to_string());
        if let Some(settled) = payments
            .iter()
            .find(|p| p["status"].as_str() == Some("complete"))
        {
            let amount_msat = parse_msat(&settled["amount_msat"]);
            let sent_msat = parse_msat(&settled["amount_sent_msat"]);
            let fee_msat = (sent_msat - amount_msat).max(0);
            let fee_sats = base_to_sats_ceil(fee_msat);
            let dest_channel = row.to_channel.clone();
            // amount_msat on a sendpay entry is the amount DELIVERED — the
            // true filled amount for a partial retry. Correct the history
            // row with it; keep the planned amount when missing.
            let mut settled_amount_sats = base_to_sats_floor(amount_msat.max(0));
            if settled_amount_sats <= 0 {
                settled_amount_sats = row.amount_sats;
            }
            self.store.update_history_success(
                rebalance_id,
                &HistorySuccess {
                    actual_fee_sats: fee_sats,
                    actual_fee_msat: fee_msat,
                    post_local_ratio: None,
                    amount_sats: (settled_amount_sats > 0).then_some(settled_amount_sats),
                },
            );
            if fee_msat > 0 {
                let mapping = peer_map.get_or_insert_with(|| self.scid_peer_map());
                let peer_id = mapping.get(&dest_channel).cloned().unwrap_or_default();
                self.store.record_rebalance_cost(&RebalanceCost {
                    channel_id: dest_channel.clone(),
                    peer_id,
                    cost_sats: fee_sats,
                    cost_msat: fee_msat,
                    amount_sats: settled_amount_sats,
                    timestamp: self.now() as i64,
                });
            }
            self.store.mark_budget_spent(&reservation_id, fee_sats);
            // Python truth: settlement clears the IN-MEMORY tracker only
            // (`_record_pair_success`); the persisted clear_pair_failures
            // write belongs to the cycle success path.
            self.pair_futility
                .lock()
                .expect("futility mutex poisoned")
                .record_success(&row.from_channel, &dest_channel);
            return true;
        }

        // No pending and no complete: the payment failed or never existed.
        self.store.update_history(
            rebalance_id,
            "failed",
            Some("payment_pending_resolved_failed"),
        );
        self.store.release_reservation(&reservation_id);
        let _ = self.payment_rpc.delpay(&payment_hash, "failed"); // best-effort
        true
    }

    // -- segment observations ----------------------------------------------

    /// Py `_push_segment_observation_snapshot`
    /// (`rebalance_engine_v2.py:3167-3194`): exported after failed
    /// executions; the engine merges in `observer_member_id` (the T3
    /// snapshot deliberately omits it — plan-frozen signature).
    fn push_segment_observation_snapshot(&self) -> bool {
        let observer_member_id = self.ensure_our_id();
        if observer_member_id.is_empty() {
            return false;
        }
        let snapshot = self.segstore.export_snapshot(self.now() as i64);
        let has_observations = snapshot
            .get("segment_observations")
            .and_then(OValue::as_arr)
            .is_some_and(|a| !a.is_empty());
        if !has_observations {
            return false;
        }
        // Merge observer_member_id at Python's dict position (between
        // schema_version and segment_observations).
        let Some(entries) = snapshot.as_obj() else {
            return false;
        };
        let mut merged: Vec<(String, OValue)> = Vec::with_capacity(entries.len() + 1);
        for (key, value) in entries {
            if key == "segment_observations" {
                merged.push((
                    "observer_member_id".to_string(),
                    OValue::str(observer_member_id.trim()),
                ));
            }
            merged.push((key.clone(), value.clone()));
        }
        self.store
            .datastore_export(&DATASTORE_KEY, &OValue::obj(merged));
        true
    }
}

// ---------------------------------------------------------------------------
// Dry-run store
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DryRunReservationStatus {
    Active,
    Spent,
    Released,
}

#[derive(Debug, Clone)]
struct DryRunReservation {
    reserved_sats: i64,
    spent_sats: i64,
    status: DryRunReservationStatus,
}

#[derive(Debug, Clone)]
struct DryRunPairFailure {
    failure_count: i64,
    cooldown_until: i64,
}

#[derive(Default)]
struct DryRunState {
    next_history_id: i64,
    rows: BTreeMap<i64, HistoryRow>,
    row_errors: HashMap<i64, String>,
    reservations: HashMap<String, DryRunReservation>,
    pair_failures: HashMap<(String, String), DryRunPairFailure>,
}

/// The plan-mandated dry-run [`RebalanceStore`]: every write is journaled
/// as one JSONL line to `<journal_dir>/rebalance_dryrun_journal.jsonl`,
/// reservations are held in-memory with committed accounting (active
/// reserved + spent actual both count against the caller's
/// `effective_budget_sats` — the same no-double-spend shape as the real
/// rail, here only for shadow observation; the production impl consumes
/// `revops_db::budget::BudgetDb` at cutover and is NOT in this plan; the
/// weekly limit fields are journaled but not enforced in dry-run).
/// Journal I/O is best-effort: bookkeeping failures never block execution
/// (py `_record_rebalance_pending`'s contract).
pub struct DryRunStore {
    journal_path: PathBuf,
    state: Mutex<DryRunState>,
}

impl DryRunStore {
    pub fn new(journal_dir: &Path) -> Self {
        DryRunStore {
            journal_path: journal_dir.join("rebalance_dryrun_journal.jsonl"),
            state: Mutex::new(DryRunState::default()),
        }
    }

    fn journal(&self, event: &str, mut fields: Map<String, Value>) {
        fields.insert("event".to_string(), json!(event));
        let line = Value::Object(fields).to_string();
        let opened = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path);
        if let Ok(mut file) = opened {
            let _ = writeln!(file, "{line}");
        }
    }

    fn journal_kv(&self, event: &str, fields: Vec<(&str, Value)>) {
        let mut map = Map::new();
        for (key, value) in fields {
            map.insert(key.to_string(), value);
        }
        self.journal(event, map);
    }

    /// Committed sats: active reservations at their reserved amount, spent
    /// reservations at their actual settled amount.
    fn committed_sats(state: &DryRunState) -> i64 {
        state
            .reservations
            .values()
            .map(|r| match r.status {
                DryRunReservationStatus::Active => r.reserved_sats,
                DryRunReservationStatus::Spent => r.spent_sats,
                DryRunReservationStatus::Released => 0,
            })
            .sum()
    }
}

impl RebalanceStore for DryRunStore {
    fn insert_or_adopt_history(&self, row: &HistoryRow) -> i64 {
        let mut state = self.state.lock().expect("dryrun mutex poisoned");
        let id = if row.id > 0 {
            row.id
        } else {
            state.next_history_id += 1;
            state.next_history_id
        };
        let mut stored = row.clone();
        stored.id = id;
        state.rows.insert(id, stored);
        drop(state);
        self.journal_kv(
            "history_insert",
            vec![
                ("id", json!(id)),
                ("from_channel", json!(row.from_channel)),
                ("to_channel", json!(row.to_channel)),
                ("amount_sats", json!(row.amount_sats)),
                ("max_fee_sats", json!(row.max_fee_sats)),
                ("status", json!(row.status)),
                ("rebalance_type", json!(row.rebalance_type)),
                ("reason_code", json!(row.reason_code)),
            ],
        );
        id
    }

    fn update_history(&self, id: i64, status: &str, err: Option<&str>) {
        let mut state = self.state.lock().expect("dryrun mutex poisoned");
        if let Some(row) = state.rows.get_mut(&id) {
            row.status = status.to_string();
        }
        if let Some(err) = err {
            state.row_errors.insert(id, err.to_string());
        }
        drop(state);
        self.journal_kv(
            "history_update",
            vec![
                ("id", json!(id)),
                ("status", json!(status)),
                ("error", json!(err)),
            ],
        );
    }

    fn update_history_success(&self, id: i64, update: &HistorySuccess) {
        let mut state = self.state.lock().expect("dryrun mutex poisoned");
        if let Some(row) = state.rows.get_mut(&id) {
            row.status = "success".to_string();
            if let Some(amount) = update.amount_sats {
                row.amount_sats = amount;
            }
        }
        drop(state);
        self.journal_kv(
            "history_update",
            vec![
                ("id", json!(id)),
                ("status", json!("success")),
                ("actual_fee_sats", json!(update.actual_fee_sats)),
                ("actual_fee_msat", json!(update.actual_fee_msat)),
                ("post_local_ratio", json!(update.post_local_ratio)),
                ("amount_sats", json!(update.amount_sats)),
            ],
        );
    }

    fn update_history_pending_settlement(&self, id: i64, error: &str, payment_hash: &str) {
        let mut state = self.state.lock().expect("dryrun mutex poisoned");
        if let Some(row) = state.rows.get_mut(&id) {
            row.status = "pending_settlement".to_string();
            row.payment_hash = Some(payment_hash.to_string());
        }
        drop(state);
        self.journal_kv(
            "history_update",
            vec![
                ("id", json!(id)),
                ("status", json!("pending_settlement")),
                ("error", json!(error)),
                ("payment_hash", json!(payment_hash)),
            ],
        );
    }

    fn reserve_budget(&self, req: &ReserveRequest) -> Result<ReservationId, BudgetBlock> {
        let state = self.state.lock().expect("dryrun mutex poisoned");
        let amount = req.amount_sats;
        if amount <= 0 || req.reservation_id.trim().is_empty() {
            drop(state);
            self.journal_kv(
                "budget_refused",
                vec![
                    ("reservation_id", json!(req.reservation_id)),
                    ("amount_sats", json!(amount)),
                    ("reason", json!("sanitize")),
                ],
            );
            return Err(BudgetBlock::Refused { remaining: 0 });
        }
        if let Some(budget) = req.effective_budget_sats {
            let remaining = budget - Self::committed_sats(&state);
            if amount > remaining {
                drop(state);
                self.journal_kv(
                    "budget_refused",
                    vec![
                        ("reservation_id", json!(req.reservation_id)),
                        ("amount_sats", json!(amount)),
                        ("remaining", json!(remaining)),
                        ("budget", json!(budget)),
                    ],
                );
                return Err(BudgetBlock::Refused { remaining });
            }
        }
        let mut state = state;
        state.reservations.insert(
            req.reservation_id.clone(),
            DryRunReservation {
                reserved_sats: amount,
                spent_sats: 0,
                status: DryRunReservationStatus::Active,
            },
        );
        drop(state);
        self.journal_kv(
            "budget_reserve",
            vec![
                ("reservation_id", json!(req.reservation_id)),
                ("amount_sats", json!(amount)),
                ("category", json!(req.category)),
                ("channel_id", json!(req.channel_id)),
                ("effective_budget_sats", json!(req.effective_budget_sats)),
            ],
        );
        Ok(ReservationId(req.reservation_id.clone()))
    }

    fn mark_budget_spent(&self, rid: &ReservationId, actual_fee_sats: i64) {
        let mut state = self.state.lock().expect("dryrun mutex poisoned");
        if let Some(reservation) = state.reservations.get_mut(&rid.0) {
            reservation.status = DryRunReservationStatus::Spent;
            reservation.spent_sats = actual_fee_sats.max(0);
        }
        drop(state);
        self.journal_kv(
            "budget_spent",
            vec![
                ("reservation_id", json!(rid.0)),
                ("actual_fee_sats", json!(actual_fee_sats)),
            ],
        );
    }

    fn release_reservation(&self, rid: &ReservationId) {
        let mut state = self.state.lock().expect("dryrun mutex poisoned");
        if let Some(reservation) = state.reservations.get_mut(&rid.0) {
            reservation.status = DryRunReservationStatus::Released;
        }
        drop(state);
        self.journal_kv("budget_release", vec![("reservation_id", json!(rid.0))]);
    }

    fn record_rebalance_cost(&self, cost: &RebalanceCost) {
        self.journal_kv(
            "rebalance_cost",
            vec![
                ("channel_id", json!(cost.channel_id)),
                ("peer_id", json!(cost.peer_id)),
                ("cost_sats", json!(cost.cost_sats)),
                ("cost_msat", json!(cost.cost_msat)),
                ("amount_sats", json!(cost.amount_sats)),
                ("timestamp", json!(cost.timestamp)),
            ],
        );
    }

    fn record_pair_failure(
        &self,
        source_channel_id: &str,
        dest_channel_id: &str,
        failure_kind: &str,
        base_cooldown_secs: i64,
        now: i64,
    ) {
        let mut state = self.state.lock().expect("dryrun mutex poisoned");
        let key = (source_channel_id.to_string(), dest_channel_id.to_string());
        let entry = state.pair_failures.entry(key).or_insert(DryRunPairFailure {
            failure_count: 0,
            cooldown_until: 0,
        });
        entry.failure_count += 1;
        // py record_pair_rebalance_failure: cooldown_until = now +
        // base * min(max(count, 1), 6) with the POST-increment count.
        entry.cooldown_until = now + base_cooldown_secs.max(1) * entry.failure_count.clamp(1, 6);
        let (count, until) = (entry.failure_count, entry.cooldown_until);
        drop(state);
        self.journal_kv(
            "pair_failure",
            vec![
                ("source_channel_id", json!(source_channel_id)),
                ("dest_channel_id", json!(dest_channel_id)),
                ("failure_kind", json!(failure_kind)),
                ("failure_count", json!(count)),
                ("cooldown_until", json!(until)),
            ],
        );
    }

    fn clear_pair_failures(&self, source_channel_id: &str, dest_channel_id: &str) {
        let mut state = self.state.lock().expect("dryrun mutex poisoned");
        state
            .pair_failures
            .remove(&(source_channel_id.to_string(), dest_channel_id.to_string()));
        drop(state);
        self.journal_kv(
            "pair_failure_clear",
            vec![
                ("source_channel_id", json!(source_channel_id)),
                ("dest_channel_id", json!(dest_channel_id)),
            ],
        );
    }

    fn pair_cooldown_until(
        &self,
        source_channel_id: &str,
        dest_channel_id: &str,
        now: i64,
    ) -> Option<i64> {
        let state = self.state.lock().expect("dryrun mutex poisoned");
        state
            .pair_failures
            .get(&(source_channel_id.to_string(), dest_channel_id.to_string()))
            .filter(|f| f.cooldown_until > now)
            .map(|f| f.cooldown_until)
    }

    fn pending_settlement_rows(&self) -> Vec<HistoryRow> {
        let state = self.state.lock().expect("dryrun mutex poisoned");
        state
            .rows
            .values()
            .filter(|r| r.status == "pending_settlement")
            .cloned()
            .collect()
    }

    fn datastore_export(&self, key: &[&str], payload: &OValue) {
        let text = revops_fees::pyjson::dumps_python(payload);
        let payload_json: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        self.journal_kv(
            "datastore_export",
            vec![("key", json!(key)), ("payload", payload_json)],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_partial_amounts_contract_points() {
        // Floor min(orig-1, max(1000, min(5000, orig//2))).
        assert_eq!(native_partial_amounts(0), Vec::<i64>::new());
        assert_eq!(native_partial_amounts(1), Vec::<i64>::new());
        assert_eq!(native_partial_amounts(2), vec![1]);
        assert_eq!(
            native_partial_amounts(100_000),
            vec![50_000, 25_000, 12_500, 6_250, 5_000]
        );
        assert!(native_partial_amounts(i64::from(u32::MAX)).len() <= 7);
    }

    #[test]
    fn classify_failure_kind_strings_match_python_table() {
        let cases = [
            (
                "WIRE_TEMPORARY_CHANNEL_FAILURE",
                "temporary_channel_failure",
            ),
            (
                "no_route: getroutes returned empty",
                "temporary_channel_failure",
            ),
            ("NoRoutes found", "temporary_channel_failure"),
            ("fee_insufficient at hop 1", "fee_insufficient"),
            ("incorrect_cltv_expiry", "incorrect_cltv_expiry"),
            ("permanent_failure", "permanent_failure"),
            ("payment_pending_timeout: x", "payment_pending_timeout"),
            ("local_execution_failed", "local_execution_failed"),
            ("mystery", "other_retriable"),
        ];
        for (input, expected) in cases {
            assert_eq!(classify_failure_kind_str(input), expected, "{input}");
        }
    }

    #[test]
    fn is_budget_block_matches_python_prefixes() {
        assert!(is_budget_block(&failed_result(
            "local_budget_block: 0 sats remaining of 100 unified budget",
            1
        )));
        assert!(is_budget_block(&failed_result(
            "zero_budget_blocks_auto_rebalance",
            1
        )));
        assert!(!is_budget_block(&failed_result("no_route", 1)));
        let mut ok = failed_result("local_budget_block: x", 1);
        ok.success = true;
        assert!(!is_budget_block(&ok));
    }

    #[test]
    fn scaled_budget_is_exact_ceil() {
        // -(-2000 * 50000 // 100000) = 1000
        assert_eq!(scaled_budget_ceil(2000, 50_000, 100_000), 1000);
        // -(-3 * 1 // 2) = 2 (ceil of 1.5)
        assert_eq!(scaled_budget_ceil(3, 1, 2), 2);
        assert_eq!(scaled_budget_ceil(1, 1, 1_000_000), 1); // max(1, ...)
    }
}
