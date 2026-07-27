//! Every external dependency the Python module reaches out to
//! (`LNPlusClient`/`self.rpc`/`self._db`/`self._policy`/`self._planner`/
//! `ignore_peer_fn`), re-expressed as an injected trait. HARD RULE 2: this
//! is the ONLY way the kernel talks to the outside world — no live HTTP,
//! SQL, or CLN RPC call lives in this crate. `tests/` implements every
//! trait with an in-memory fake; a real implementation (an actual HTTPS
//! client, the production `revops-db` connection, a CLN RPC client) is
//! plugin-layer wiring tracked in `ENTRYPOINTS.md`, not this crate's job.

use crate::db_types::{PeerRow, SwapPatch, SwapRow};
use crate::error::LnPlusError;
use crate::types::{Metadata, MySwaps, NotificationEntry, Rating, SwapDetail, SwapListing};

/// Generic port-level error for the DB / policy / planner / chain traits
/// (distinct from [`LnPlusError`], which is specifically the LN+ HTTP
/// API's error shape). A plain message is enough for every call site in
/// this module: every failure path here is "log and treat as a
/// retry-next-pass / fail-closed signal", never a typed match on cause.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PortError(pub String);

impl PortError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

pub type PortResult<T> = Result<T, PortError>;

/// `LNPlusClient` (py 72-229): the LN+ v2 HTTPS API. Every method here is
/// the POST/GET call already stripped of transport concerns (auth-param
/// building, JSON envelope unwrapping) — those are pure and live in
/// `validation.rs` / the concrete implementation, not this trait.
pub trait LnPlusApi {
    /// py 151-159 `get_applicable_swaps`.
    fn get_applicable_swaps(&self) -> Result<Vec<SwapListing>, LnPlusError>;
    /// py 161-167 `get_swap`.
    fn get_swap(&self, swap_id: &str) -> Result<SwapDetail, LnPlusError>;
    /// py 169-181 `get_my_swaps`.
    fn get_my_swaps(&self) -> Result<MySwaps, LnPlusError>;
    /// py 197-200 `create_application`.
    fn create_application(&self, swap_id: &str) -> Result<(), LnPlusError>;
    /// py 202-205 `delete_application`.
    fn delete_application(&self, swap_id: &str) -> Result<(), LnPlusError>;
    /// py 207-210 `complete_application`.
    fn complete_application(&self, swap_id: &str) -> Result<(), LnPlusError>;
    /// py 212-217 `get_notifications`.
    fn get_notifications(&self) -> Result<Vec<NotificationEntry>, LnPlusError>;
    /// py 219-221 `mark_read_notifications`.
    fn mark_read_notifications(&self) -> Result<(), LnPlusError>;
    /// py 223-229 `create_rating`.
    fn create_rating(&self, swap_id: &str, rating: Rating) -> Result<(), LnPlusError>;
}

/// A `record_planner_action` call (py 628-634, 1795-1806, 1841-1850).
#[derive(Debug, Clone, Default)]
pub struct PlannerActionRequest {
    pub action_type: &'static str,
    pub peer_id: String,
    pub amount_sats: Option<i64>,
    pub estimated_cost_sats: Option<i64>,
    pub reason: String,
    pub metadata: Option<Metadata>,
}

/// `reserve_spend(...)` kwargs (py 1640-1648 legacy path).
#[derive(Debug, Clone)]
pub struct ReserveSpendRequest {
    pub reservation_id: String,
    pub amount_sats: i64,
    pub category: &'static str,
    pub subcategory: &'static str,
    pub metadata: Metadata,
    pub effective_budget_sats: Option<i64>,
    pub since_timestamp: Option<i64>,
}

/// The local SQLite ledger (`database.py`'s `lnplus_*` methods +
/// budget-rail methods this module calls). Deliberately one trait per
/// concern would fragment fakes across too many mocks for tests that
/// exercise a whole gate chain — Python's `self._db` is one object too.
pub trait LnPlusDb {
    // -- swap ledger --------------------------------------------------
    /// py `lnplus_record_swap` — INSERT OR REPLACE (idempotent on
    /// `swap_id`; a second call with the same id overwrites the row,
    /// which is exactly what backfill's "skip if a row already exists"
    /// guard is designed to avoid triggering unintentionally).
    fn record_swap(&self, row: &SwapRow);
    fn update_swap(&self, swap_id: &str, patch: &SwapPatch);
    fn get_swap(&self, swap_id: &str) -> Option<SwapRow>;
    fn get_swaps_by_status(&self, statuses: &[&str]) -> Vec<SwapRow>;
    fn inflight_swaps(&self) -> Vec<SwapRow> {
        self.get_swaps_by_status(&crate::db_types::INFLIGHT_STATUSES)
    }
    fn prune_terminal(&self, older_than_days: i64, now: i64) -> usize;

    // -- peer reputation ------------------------------------------------
    fn get_peer(&self, pubkey: &str) -> Option<PeerRow>;
    fn bump_peer(&self, pubkey: &str, defection: bool, rating: Option<Rating>);

    // -- backfill flag (config_overrides) ------------------------------
    fn get_config_override(&self, key: &str) -> Option<String>;
    fn set_config_override(&self, key: &str, value: &str);
    fn delete_config_override(&self, key: &str);

    // -- circuit breaker --------------------------------------------------
    // Dedicated (rather than routed through `*_config_override`) so the
    // STRUCTURED `BreakerCause` (crate::breaker — new in this port, not
    // present in Python's plain string) survives round-trip without this
    // crate inventing a serialization format. A concrete implementation is
    // free to persist it into the same `_lnplus_breaker` config_overrides
    // row Python uses, encoded however it likes — see ENTRYPOINTS.md.
    fn get_breaker(&self) -> Option<crate::breaker::BreakerState>;
    fn set_breaker(&self, state: &crate::breaker::BreakerState);
    fn clear_breaker(&self);

    // -- planner-action breadcrumbs --------------------------------------
    /// Returns the new action id.
    fn record_planner_action(&self, req: &PlannerActionRequest) -> i64;
    fn update_planner_action(&self, action_id: i64, status: &str);

    // -- unified budget rail ----------------------------------------------
    fn reserve_spend(&self, req: &ReserveSpendRequest) -> PortResult<bool>;
    fn release_spend_reservation(&self, reservation_id: &str) -> PortResult<()>;
    fn mark_spend_reservation_spent(
        &self,
        reservation_id: &str,
        actual_spent_sats: i64,
        source: &str,
    ) -> PortResult<bool>;
}

/// `policy_manager.get_policy(peer)`'s tag surface (py `has_tag`).
pub trait PeerPolicy {
    fn has_tag(&self, tag: &str) -> bool;
}

/// `policy_manager` (py `_check_participants`/`_protect_peer_no_close`/
/// `_release_no_close_if_ours`). `is_peer_banned` is FAIL-CLOSED at the
/// call site (an `Err` here must reject the swap, never admit it) — see
/// `evaluator::check_participants`.
pub trait PolicyPort {
    fn get_policy(&self, peer: &str) -> PortResult<Option<Box<dyn PeerPolicy>>>;
    fn add_tag(&self, peer: &str, tag: &str) -> PortResult<()>;
    fn remove_tag(&self, peer: &str, tag: &str) -> PortResult<()>;
    fn is_peer_banned(&self, pubkey: &str) -> PortResult<bool>;
}

/// `self._planner` (`CapacityPlanner`, methods `_calculate_open_ev` /
/// `_estimate_open_cost` / `_score_candidate` / `_capex_engine`).
pub trait PlannerPort {
    /// `_calculate_open_ev(peer, capacity_sats, cfg)`. `peer: None` mirrors
    /// Python passing a possibly-`None` `outbound_peer`/`incoming_peer`
    /// assignment straight through (py 518-519, 527-528) — the planner
    /// itself decides what a `None` peer means; this port does not
    /// special-case it.
    fn calculate_open_ev(&self, peer: Option<&str>, capacity_sats: i64) -> f64;
    /// `_estimate_open_cost()`.
    fn estimate_open_cost(&self) -> i64;
    /// `_score_candidate(pubkey, base_score)`.
    fn score_candidate(&self, pubkey: &str, base: f64) -> f64;
    /// `getattr(self._planner, "_capex_engine", None)` then
    /// `.get_fleet_exploration_budget()`. `None` means the capex engine is
    /// absent (py 614-615: gate 9 is skipped entirely, matching
    /// `_select_and_apply`'s `if capex is not None:` guard).
    fn capex_fleet_exploration_budget(&self) -> Option<i64>;
}

/// One channel row as returned by `listpeerchannels` — only the fields
/// `lnplus_swaps.py` actually reads.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ChannelInfo {
    pub peer_id: String,
    pub state: String,
    pub total_msat: i64,
    pub to_us_msat: i64,
    pub funding_txid: Option<String>,
}

/// One confirmed, unreserved on-chain UTXO amount (already filtered —
/// see [`ChainPort::confirmed_unreserved_sats`]'s doc comment for why the
/// filter is the port's job, not the caller's).
#[derive(Debug, Clone)]
pub struct FundChannelResult {
    pub txid: Option<String>,
}

/// A feerate directive for `fundchannel`, mirroring the three string
/// literals `_execute_swap_open` picks from deadline slack (py 1583).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feerate {
    Slow,
    Normal,
    Urgent,
}

impl Feerate {
    pub fn as_str(&self) -> &'static str {
        match self {
            Feerate::Slow => "slow",
            Feerate::Normal => "normal",
            Feerate::Urgent => "urgent",
        }
    }
}

/// The CLN RPC surface this module touches (`self.rpc` / the
/// `data_service` cache-coherent adapter Python prefers when wired — that
/// preference is a wiring decision for whichever concrete `ChainPort` impl
/// the plugin injects, not modeled as two code paths here).
pub trait ChainPort {
    /// `rpc.getinfo()["id"]`, cached by the caller (evaluator) as a
    /// process constant (PR 3d) — this port itself is stateless.
    fn our_node_id(&self) -> PortResult<String>;
    /// `rpc.listpeerchannels()` or `rpc.listpeerchannels(peer)`.
    fn list_peer_channels(&self, peer: Option<&str>) -> PortResult<Vec<ChannelInfo>>;
    /// `rpc.feerates("perkw")["perkw"]["opening"]`.
    fn opening_feerate_perkw(&self) -> PortResult<i64>;
    /// `rpc.listfunds().outputs`, already summed to confirmed +
    /// unreserved sats (py 594-601) — kept as one call rather than
    /// exposing raw UTXOs, since nothing else in this module needs
    /// per-UTXO detail.
    fn confirmed_unreserved_sats(&self) -> PortResult<i64>;
    /// `rpc.connect(target)` / `data_service.connect_peer(target)`.
    fn connect(&self, target: &str) -> PortResult<()>;
    /// `rpc.fundchannel(peer, amount, feerate=...)` /
    /// `data_service.fund_channel(...)`.
    fn fund_channel(
        &self,
        peer: &str,
        amount_sats: i64,
        feerate: Feerate,
    ) -> PortResult<FundChannelResult>;
}

/// `ignore_peer_fn` (py `SwapLifecycle.__init__`'s optional injected
/// callback, invoked at py 1924-1929).
pub trait IgnorePeerPort {
    fn ignore_peer(&self, pubkey: &str, reason: &str) -> PortResult<()>;
}

/// `self._plugin.log(msg, level=...)`. A trait (not a bare closure) so
/// fakes can assert on emitted lines — several of the defects this port
/// exists specifically to make testable (e.g. defect #1/#4) are only
/// observable via a state transition PLUS a log line.
pub trait Logger {
    fn log(&self, level: LogLevel, message: &str);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}
