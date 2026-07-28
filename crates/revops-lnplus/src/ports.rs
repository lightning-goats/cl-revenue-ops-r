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

/// Acknowledged outcome of [`LnPlusDb::insert_swap_new`]: creating a row
/// is typed, never an `INSERT OR REPLACE` that can silently clobber
/// automation-owned state (Task 61 4A).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    /// A row with this `swap_id` already exists. The existing row is
    /// untouched — the caller decides whether that is a skip (backfill)
    /// or an invariant violation (evaluator intent rows).
    AlreadyExists,
}

/// Acknowledged outcome of [`LnPlusDb::cas_swap`]: a lifecycle patch
/// applies only from an expected status (compare-and-set on the one
/// column every transition is keyed by), so a stale writer can never
/// blindly overwrite a row that has since moved on (Task 61 4A).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasOutcome {
    Applied,
    /// Nothing was written. `actual` is the row's current status, `None`
    /// when no row with that `swap_id` exists at all.
    Conflict {
        actual: Option<String>,
    },
}

/// The compound guard for [`LnPlusDb::terminalize_and_trip`]: which
/// statuses the row may be in, and whether an already-recorded funding
/// txid vetoes the terminalization (the deadline-miss guard: a funded
/// row must never be failed out from under its channel).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalizeSpec<'a> {
    pub swap_id: &'a str,
    pub expected_statuses: &'a [&'a str],
    pub require_null_funding_txid: bool,
}

/// How the breaker half of a compound landed: a fresh trip, or B10
/// first-cause preservation (the breaker was already tripped and its
/// original cause is untouched).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TripAck {
    NewTrip,
    AlreadyTripped,
}

/// Acknowledged outcome of [`LnPlusDb::terminalize_and_trip`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompoundOutcome {
    /// The row terminalized AND the breaker state advanced, in one
    /// atomic transaction.
    Terminalized { breaker: TripAck },
    /// The guard did not hold (wrong status, or a funding txid exists
    /// where the spec forbids one). NOTHING was written — no row change,
    /// no breaker change.
    Conflict { actual: Option<String> },
}

/// Which external submit an attempt guards (Task 61 4B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptKind {
    /// `LnPlusApi::create_application` — the irreversible LN+ commitment.
    Apply,
    /// `ChainPort::fund_channel` — the money-moving channel open (carries
    /// a budget reservation).
    Fund,
}

impl AttemptKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            AttemptKind::Apply => "apply",
            AttemptKind::Fund => "fund",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "apply" => Some(AttemptKind::Apply),
            "fund" => Some(AttemptKind::Fund),
            _ => None,
        }
    }
}

/// Durable attempt lifecycle state (Task 61 4B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptState {
    /// Persisted BEFORE the external submit; a live pass resolves it in
    /// the same call. Found at restart = crashed in flight = unknown by
    /// definition (see `quarantine_stale_intents`).
    Intent,
    /// Known clean failure before the wire write — reservation released.
    NotSubmitted,
    /// The submit landed — for Fund, row txid + settle + receipt landed
    /// atomically with this state.
    Committed,
    /// Post-submit ambiguity: reservation RETAINED, attempt quarantined,
    /// no new attempt for this (swap, kind) until reconciled.
    OutcomeUnknown,
}

impl AttemptState {
    pub fn as_str(&self) -> &'static str {
        match self {
            AttemptState::Intent => "intent",
            AttemptState::NotSubmitted => "not_submitted",
            AttemptState::Committed => "committed",
            AttemptState::OutcomeUnknown => "outcome_unknown",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "intent" => Some(AttemptState::Intent),
            "not_submitted" => Some(AttemptState::NotSubmitted),
            "committed" => Some(AttemptState::Committed),
            "outcome_unknown" => Some(AttemptState::OutcomeUnknown),
            _ => None,
        }
    }
}

/// The durable pre-submit record: stable attempt id, the reservation it
/// binds (Fund only), and what/whom it targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptIntent {
    pub attempt_id: String,
    pub swap_id: String,
    pub kind: AttemptKind,
    pub reservation_id: Option<String>,
    pub peer_id: Option<String>,
    pub amount_sats: Option<i64>,
    pub created_at: i64,
}

/// One persisted attempt row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRow {
    pub attempt_id: String,
    pub swap_id: String,
    pub kind: AttemptKind,
    pub state: AttemptState,
    pub reservation_id: Option<String>,
    pub peer_id: Option<String>,
    pub amount_sats: Option<i64>,
    pub detail: Option<String>,
    pub created_at: i64,
    pub resolved_at: Option<i64>,
}

/// Acknowledged outcome of [`LnPlusDb::begin_attempt`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginAttemptAck {
    Started,
    /// An attempt for this (swap, kind) is already in flight or
    /// quarantined — the no-auto-resubmit rail, enforced by the store.
    Blocked {
        existing_attempt_id: String,
        state: AttemptState,
    },
}

/// Typed resolution of an attempt (Task 61 4B).
#[derive(Debug, Clone, PartialEq)]
pub enum AttemptResolution {
    /// Clean failure before the wire write. Releases the bound
    /// reservation in the same transaction.
    NotSubmitted { detail: String },
    /// The application landed on LN+.
    CommittedApply,
    /// The channel funded: ONE transaction covers the row's funding
    /// txid/opened_at CAS, the reservation settle, the receipt event, and
    /// the attempt state. `actual_cost_sats: None` settles at the
    /// reservation's reserved amount (the restart-reconciliation path,
    /// where the true fee is unknowable).
    CommittedFund {
        txid: String,
        actual_cost_sats: Option<i64>,
    },
    /// Post-submit ambiguity: quarantine. Reservation untouched (HELD).
    OutcomeUnknown { detail: String },
}

/// Acknowledged outcome of [`LnPlusDb::resolve_attempt`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveAck {
    Resolved,
    /// The attempt already reached a terminal state — exactly-once
    /// replay: NOTHING was written by this call.
    AlreadyResolved {
        state: AttemptState,
    },
    /// No attempt with that id exists.
    UnknownAttempt,
}

/// The local SQLite ledger (`database.py`'s `lnplus_*` methods +
/// budget-rail methods this module calls). Deliberately one trait per
/// concern would fragment fakes across too many mocks for tests that
/// exercise a whole gate chain — Python's `self._db` is one object too.
///
/// Task 61 4A: every lifecycle write on this trait is FALLIBLE and
/// ACKNOWLEDGED — an implementation must surface a persistence failure
/// as `Err`, never swallow it into a log line and return as if the write
/// happened. Row creation is a typed insert, row mutation is a CAS
/// transition, and the compound terminal+breaker change is atomic.
pub trait LnPlusDb {
    // -- swap ledger --------------------------------------------------
    /// Typed INSERT of a complete [`SwapRow`] (every field persisted in
    /// this one write). Never overwrites: an existing `swap_id` returns
    /// [`InsertOutcome::AlreadyExists`] with the stored row untouched.
    fn insert_swap_new(&self, row: &SwapRow) -> PortResult<InsertOutcome>;
    /// Compare-and-set lifecycle transition: `patch` applies iff the
    /// row's current status is one of `expected_statuses`; otherwise a
    /// typed [`CasOutcome::Conflict`] with nothing written.
    fn cas_swap(
        &self,
        swap_id: &str,
        expected_statuses: &[&str],
        patch: &SwapPatch,
    ) -> PortResult<CasOutcome>;
    fn get_swap(&self, swap_id: &str) -> Option<SwapRow>;
    fn get_swaps_by_status(&self, statuses: &[&str]) -> Vec<SwapRow>;
    fn inflight_swaps(&self) -> Vec<SwapRow> {
        self.get_swaps_by_status(&crate::db_types::INFLIGHT_STATUSES)
    }
    fn prune_terminal(&self, older_than_days: i64, now: i64) -> PortResult<usize>;

    // -- peer reputation ------------------------------------------------
    fn get_peer(&self, pubkey: &str) -> Option<PeerRow>;
    fn bump_peer(&self, pubkey: &str, defection: bool, rating: Option<Rating>) -> PortResult<()>;

    // -- backfill flag (config_overrides) ------------------------------
    fn get_config_override(&self, key: &str) -> Option<String>;
    fn set_config_override(&self, key: &str, value: &str) -> PortResult<()>;
    fn delete_config_override(&self, key: &str) -> PortResult<()>;

    // -- circuit breaker --------------------------------------------------
    // Dedicated (rather than routed through `*_config_override`) so the
    // STRUCTURED `BreakerCause` (crate::breaker — new in this port, not
    // present in Python's plain string) survives round-trip without this
    // crate inventing a serialization format. A concrete implementation is
    // free to persist it into the same `_lnplus_breaker` config_overrides
    // row Python uses, encoded however it likes — see ENTRYPOINTS.md.
    //
    // Task 61 4A: the read fails CLOSED — a persisted value the
    // implementation cannot decode is `Err` (corruption evidence), never
    // silently "untripped".
    fn get_breaker(&self) -> PortResult<Option<crate::breaker::BreakerState>>;
    /// Operator-path unconditional write (last-writer-wins). Production
    /// TRIP paths must use [`LnPlusDb::trip_breaker_if_untripped`] instead
    /// — this method cannot preserve a first cause.
    fn set_breaker(&self, state: &crate::breaker::BreakerState) -> PortResult<()>;
    /// Operator-path unconditional clear ("clear whatever is latched" IS
    /// the operator's intent). Automation must use
    /// [`LnPlusDb::clear_breaker_if_cause`].
    fn clear_breaker(&self) -> PortResult<()>;
    /// ATOMIC insert-if-absent trip (Task 61 4A): persists `state` iff no
    /// breaker is currently latched, in one transaction — the B10
    /// first-cause guarantee at the STORE level, not just in caller logic.
    /// An existing undecodable value is `Err` (fail closed).
    fn trip_breaker_if_untripped(
        &self,
        state: &crate::breaker::BreakerState,
    ) -> PortResult<TripAck>;
    /// ATOMIC exact-cause clear (Task 61 4A): clears iff the currently
    /// latched cause equals `expected` — an auto-clear can only remove the
    /// exact cause it re-verified, never whatever happens to be latched.
    /// Returns `Ok(true)` iff it cleared. Undecodable persisted value is
    /// `Err` (fail closed).
    fn clear_breaker_if_cause(&self, expected: &crate::breaker::BreakerCause) -> PortResult<bool>;

    /// ATOMIC compound: CAS-terminalize the row per `spec`/`patch` AND
    /// advance the breaker (trip with B10 first-cause preservation) in
    /// one transaction. Either both land or neither does — a failure on
    /// either half rolls the other back and returns `Err`.
    fn terminalize_and_trip(
        &self,
        spec: &TerminalizeSpec<'_>,
        patch: &SwapPatch,
        cause: crate::breaker::BreakerCause,
        now: i64,
    ) -> PortResult<CompoundOutcome>;

    // -- attempt/reservation identity (Task 61 4B) -----------------------
    /// Durably record the pre-submit intent. Blocked (typed, nothing
    /// written) while another attempt for the same (swap, kind) is in
    /// flight or quarantined — the store-level no-auto-resubmit rail.
    fn begin_attempt(&self, intent: &AttemptIntent) -> PortResult<BeginAttemptAck>;
    /// Resolve an attempt exactly once. Fund-committed and not-submitted
    /// resolutions are COMPOUNDS: row/settle/receipt/release join the
    /// attempt transition in one transaction — all-or-nothing.
    fn resolve_attempt(
        &self,
        attempt_id: &str,
        resolution: &AttemptResolution,
        now: i64,
    ) -> PortResult<ResolveAck>;
    fn get_attempt(&self, attempt_id: &str) -> PortResult<Option<AttemptRow>>;
    /// Every attempt currently in `OutcomeUnknown` — the restart
    /// reconciliation work list.
    fn unknown_attempts(&self) -> PortResult<Vec<AttemptRow>>;
    /// Promote stale in-flight `Intent` rows (a crashed process died
    /// between begin and resolve) to `OutcomeUnknown` — quarantine
    /// survives restart. Returns how many were promoted.
    fn quarantine_stale_intents(&self, detail: &str, now: i64) -> PortResult<usize>;

    // -- planner-action breadcrumbs --------------------------------------
    /// Returns the new action id.
    fn record_planner_action(&self, req: &PlannerActionRequest) -> PortResult<i64>;
    fn update_planner_action(&self, action_id: i64, status: &str) -> PortResult<()>;

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

/// Task 61 4B: the typed submit outcome of [`ChainPort::fund_channel`].
/// `Err(PortError)` on the method is reserved for CLEAN pre-submit
/// failures (nothing reached the node — refused connection, invalid
/// params); post-submit ambiguity is this enum's `OutcomeUnknown` variant
/// so callers CANNOT conflate the two.
#[derive(Debug, Clone)]
pub enum FundChannelOutcome {
    /// The node answered: the channel funded (txid when it reported one).
    Funded(FundChannelResult),
    /// The request may have reached the node but no answer came back
    /// (timeout / disconnect after submit). Funds MAY be committed —
    /// quarantine, never retry or release automatically.
    OutcomeUnknown { detail: String },
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
    /// `data_service.fund_channel(...)`. Task 61 4B: `Err` = CLEAN
    /// pre-submit failure only; post-submit ambiguity is
    /// [`FundChannelOutcome::OutcomeUnknown`].
    fn fund_channel(
        &self,
        peer: &str,
        amount_sats: i64,
        feerate: Feerate,
    ) -> PortResult<FundChannelOutcome>;
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
