//! The guarded CLN fee broadcaster (stateful-shadow revision plan, Task
//! 9): the ONLY component in this entire codebase that may ever send a
//! live `setchannel` to CLN. Everything before it -- the mode matrix
//! (`fee_mode`), the cutover arm (`cutover_arm`), and the read-only
//! Python-authority client (`python_authority`) -- exists to make
//! constructing this thing hard. Nothing here weakens that: it stays
//! hard.
//!
//! ## The two capabilities that gate every call
//!
//! 1. [`ClnFeeBroadcaster`] can only be built from a whole
//!    [`crate::fee_mode::LiveMode`] -- the type with no public
//!    constructor other than [`crate::fee_mode::validate_fee_mode`], whose
//!    only path to `LiveMode` requires a genuinely validated-and-consumed
//!    [`crate::cutover_arm::LiveSessionArm`]. [`ClnFeeBroadcaster::new`]
//!    is the TERMINAL consumer of that arm: it stores it in a private
//!    `_session` field with no accessor, so once a broadcaster exists
//!    there is no way to extract the arm back out, mint a second
//!    broadcaster from the same `LiveMode`, or otherwise route around the
//!    one-session-per-arm contract.
//! 2. [`ClnFeeBroadcaster::broadcast_batch`] additionally requires a
//!    freshly-constructed [`LiveBatchAuthorization`], consumed by value.
//!    Its only constructor, [`LiveBatchAuthorization::authorize`], is
//!    itself a VERIFIER, not a witness (fix round 1, review finding 4):
//!    the quarantine-empty observation and the current state generation
//!    are read directly from the Rust-owned store INSIDE `authorize`,
//!    never accepted as caller-supplied parameters a bug (or a race)
//!    could mint with a stale or absent value. `authorize` denies
//!    construction outright (see [`LiveBatchDenyReason`]) unless the
//!    store-read quarantine is empty, the store-read state generation
//!    matches the candidate's, a stable bracketed Python-authority epoch
//!    holds, the governor authorized, and a non-empty ledger reservation
//!    was supplied. Every denial path returns before `broadcast_batch` is
//!    ever callable -- there is no partial authorization and no way to
//!    retry past a denial without a fresh set of readings.
//!
//! ## Persist intent before submission, result afterward
//!
//! [`ClnFeeBroadcaster::broadcast_batch`] writes a
//! [`revops_db::fee_runway::BroadcastAttemptIntent`] row through the
//! Rust-owned observer store BEFORE the socket is ever dialed, and a
//! terminal [`revops_db::fee_runway::BroadcastAttemptOutcome`] afterward.
//! A process death between those two writes leaves the intent row with no
//! recorded outcome -- ambiguous by construction, never silently lost --
//! and [`ClnFeeBroadcaster::new`] runs
//! [`revops_db::owner::ObserverHandle::reconcile_quarantine_on_restart`]
//! as its FIRST action, REFUSING construction outright if it fails (fix
//! round 1, review finding 2) -- the arm it is given never becomes usable
//! unless reconciliation succeeded. A restart can never accept a fresh
//! cutover arm without first restoring whatever quarantine state the
//! prior process left behind.
//!
//! ## Transport classification (conservative by design)
//!
//! [`ClnFeeBroadcaster`] classifies every attempt into exactly one of:
//!
//! - **Success**: CLN answered with a `result`.
//! - **Rejected**: CLN answered with an explicit `error` object (a
//!   genuine JSON-RPC error CODE is present) -- CLN plainly refused the
//!   request. Terminal; never quarantines.
//! - **CleanFailure**: the connection to `lightning-rpc` itself could not
//!   be established -- no bytes could possibly have reached lightningd.
//!   Terminal; never quarantines (nothing to be uncertain about).
//! - **Ambiguous**: bytes may have reached lightningd but no definite
//!   answer came back (a disconnect, a timeout, or an undecodable
//!   response) -- the request may or may not have been applied.
//!   [`broadcast_batch`](ClnFeeBroadcaster::broadcast_batch) inserts a
//!   persistent execution quarantine for this outcome BEFORE recording
//!   any terminal result (fix round 1, review finding 1) and stops the
//!   batch immediately: quarantine is never retried automatically (see
//!   `revops_db::fee_runway`'s module doc), only restored across a
//!   restart. If the quarantine insert ITSELF fails, the intent row is
//!   left unresolved (never marked with a terminal outcome, so restart
//!   reconciliation still picks it up) AND this process poisons itself
//!   in memory (see [`ClnFeeBroadcaster::broadcast_batch`]'s doc
//!   comment) -- fail-open, in either direction, is not an option.
//!
//! The only action call site in this entire module (and, by the removed
//! `tests/fee_scheduler.rs` source-scan guard, this entire crate) is
//! inside [`ClnFeeBroadcaster::attempt_send`]:
//! `rpc.call_raw::<serde_json::Value, serde_json::Value>("setchannel",
//! &request.to_params())`.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::Value;

use revops_db::fee_runway::{BroadcastAttemptIntent, BroadcastAttemptOutcome, QuarantineEntry};
use revops_db::owner::ObserverHandle;
use revops_fees::execution::SetChannelRequest;

use crate::fee_mode::LiveMode;
use crate::python_authority::{
    validate_stable_epoch, PythonAuthorityDenyReason, PythonAuthorityOff,
};

/// The only RPC method [`ClnFeeBroadcaster`] can ever call. A named
/// constant (rather than an inline literal) for the same reason
/// `python_authority::AUTHORITY_STATUS_METHOD` is: widening this
/// broadcaster's surface to a second method requires visibly touching
/// this line.
pub const SETCHANNEL_METHOD: &str = "setchannel";

/// Wall-clock budget for the two Rust-owned-store reads
/// [`LiveBatchAuthorization::authorize`] performs (final-review finding
/// I7, 2026-07-26). Deliberately a little ABOVE
/// [`revops_db::BUSY_TIMEOUT_MS`] so a legitimate sqlite lock wait is
/// never cut short and misreported as a wedged actor -- see [`budgeted`]
/// for why any budget at all is required. `broadcast_batch` uses the
/// broadcaster's own `timeout_seconds` instead, so the whole live path has
/// exactly one operator-visible number.
const AUTHORIZE_STORE_BUDGET: Duration = Duration::from_millis(revops_db::BUSY_TIMEOUT_MS + 2_000);

/// Budget ONE Rust-owned-store call and turn an expiry into a denial
/// string (final-review finding I7, 2026-07-26).
///
/// The store is a single-owner actor behind a channel: a wedged actor (a
/// blocking task stuck on a lock it never gets, a reply that never comes)
/// makes an unbudgeted `.await` here hang the CLN handler that called in,
/// forever. The RPC half of this module has been budgeted since Task 9
/// (see [`ClnFeeBroadcaster::attempt_send`]); this is its store-side
/// counterpart, so every wait on the live path either answers or DENIES.
/// Failing closed is always available -- the deny reasons already exist --
/// whereas a hang is not a decision at all.
async fn budgeted<T>(
    budget: Duration,
    what: &str,
    call: impl std::future::Future<Output = anyhow::Result<T>>,
) -> Result<T, String> {
    match tokio::time::timeout(budget, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(format!("{e:#}")),
        Err(_) => Err(format!(
            "{what} exceeded its {:?} Rust-owned-store budget: the single-owner actor did not \
             answer, so this call fails closed rather than hanging its caller",
            budget
        )),
    }
}

/// One prepared, would-broadcast fee request, bound to its cycle/request
/// identity for persistence and audit (the interface note's
/// "`PersistedFeeRequest`-like rows exist as `rust_fee_requests`" --
/// this type is the live-mode analog `ClnFeeBroadcaster::broadcast_batch`
/// consumes; `revops_db::fee_runway::PreparedFeeActionRow` remains the
/// shadow-mode audit row, unrelated to this live path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedFeeRequest {
    /// The shadow/live cycle this request belongs to, if any -- carried
    /// through to the broadcast-attempt ledger, never to
    /// `rust_fee_cycles` (no FK; see that table's DDL comment).
    pub cycle_id: Option<String>,
    pub channel_id: String,
    /// Unique request identity (e.g. the governor's idempotency key) --
    /// threaded through to `rust_broadcast_attempts.request_id`, which is
    /// itself `UNIQUE`.
    pub request_id: String,
    pub params: SetChannelRequest,
}

impl PersistedFeeRequest {
    /// The exact wire params for the one action call site.
    pub fn to_params(&self) -> Value {
        self.params.to_params()
    }
}

/// Every fail-closed reason [`LiveBatchAuthorization::authorize`] can
/// return. Each is stable and mutually exclusive; never reword an
/// existing variant -- add a new one instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveBatchDenyReason {
    /// The store-backed active-quarantine read itself failed (fail
    /// closed: an unreadable quarantine state is treated exactly like an
    /// active one -- never as "probably empty").
    QuarantineCheckFailed(String),
    /// An active execution quarantine is already on record (either from a
    /// prior ambiguous transport outcome, or restored at restart) --
    /// checked FIRST, before any other input is even considered.
    QuarantineActive { reason: String, entered_at: i64 },
    /// The store-backed current-state-generation read itself failed
    /// (fail closed, same rationale as [`Self::QuarantineCheckFailed`]).
    StateGenerationCheckFailed(String),
    /// The candidate batch was computed against a Rust-owned state
    /// generation that is no longer current -- the store advanced (or
    /// diverged) between batch assembly and authorization.
    StateGenerationStale {
        authorized_against: u64,
        current: u64,
    },
    /// The two bracketing Python-authority reads (see
    /// `crate::python_authority`'s "Two-read batch bracketing") disagreed,
    /// or the second did not genuinely advance past the first.
    PythonAuthority(PythonAuthorityDenyReason),
    /// The governor did not authorize this batch.
    GovernorDenied { reason_code: String },
    /// No ledger reservation identity was supplied -- an empty string is
    /// never treated as "a reservation happened."
    LedgerReservationMissing,
}

impl LiveBatchDenyReason {
    /// Stable, machine-matchable code -- logged and reported verbatim.
    pub fn code(&self) -> &'static str {
        match self {
            Self::QuarantineCheckFailed(_) => "live_batch_quarantine_check_failed",
            Self::QuarantineActive { .. } => "live_batch_quarantine_active",
            Self::StateGenerationCheckFailed(_) => "live_batch_state_generation_check_failed",
            Self::StateGenerationStale { .. } => "live_batch_state_generation_stale",
            Self::PythonAuthority(_) => "live_batch_python_authority_denied",
            Self::GovernorDenied { .. } => "live_batch_governor_denied",
            Self::LedgerReservationMissing => "live_batch_ledger_reservation_missing",
        }
    }
}

impl std::fmt::Display for LiveBatchDenyReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QuarantineCheckFailed(detail) => write!(f, "{}: {detail}", self.code()),
            Self::QuarantineActive { reason, entered_at } => write!(
                f,
                "{}: active execution quarantine since {entered_at} ({reason})",
                self.code(),
            ),
            Self::StateGenerationCheckFailed(detail) => write!(f, "{}: {detail}", self.code()),
            Self::StateGenerationStale {
                authorized_against,
                current,
            } => write!(
                f,
                "{}: candidate batch authorized against generation {authorized_against}, but \
                 the current generation is {current}",
                self.code(),
            ),
            Self::PythonAuthority(inner) => write!(f, "{}: {inner}", self.code()),
            Self::GovernorDenied { reason_code } => {
                write!(f, "{}: {reason_code}", self.code())
            }
            Self::LedgerReservationMissing => write!(
                f,
                "{}: no ledger reservation identity was supplied",
                self.code(),
            ),
        }
    }
}

impl std::error::Error for LiveBatchDenyReason {}

/// The single-use capability required to call
/// [`ClnFeeBroadcaster::broadcast_batch`]. Every field is private and
/// there is no public constructor other than
/// [`LiveBatchAuthorization::authorize`] -- there is deliberately no
/// `Clone`/`Copy` impl, so a value of this type can authorize at most one
/// `broadcast_batch` call (it is consumed by value).
///
/// Binds: the candidate batch's own content hash (`candidate_sha256`),
/// the Rust-owned state generation it was computed against, the
/// bracketed Python-authority generation, the governor's reason code, and
/// the ledger reservation identity that backs this batch's cost.
#[derive(Debug)]
pub struct LiveBatchAuthorization {
    candidate_sha256: String,
    state_generation: u64,
    python_authority_generation: u64,
    governor_reason_code: String,
    ledger_reservation_id: String,
}

impl LiveBatchAuthorization {
    /// Authorize one live batch. Check order (each a fail-closed, stable,
    /// mutually exclusive reason): active quarantine, stale state
    /// generation, unstable/non-advancing Python-authority epoch,
    /// governor denial, missing ledger reservation.
    ///
    /// Fix round 1 (review finding 4): the quarantine-empty observation
    /// and the current state generation are NOT caller-supplied -- a
    /// caller-supplied witness can always be minted with `None`/a stale
    /// value by a bug or a race, which would make this authorizer a
    /// witness rather than a verifier. Both are read HERE, directly from
    /// `store`, so no caller can construct an authorization the store
    /// itself disagrees with. `first_authority_reading`/
    /// `second_authority_reading` remain injected: they come from the RPC
    /// client (a later task's caller performs two genuinely separate
    /// fetches), and their bracketing is enforced by
    /// [`validate_stable_epoch`] (including
    /// `NonAdvancingObservation`, which rejects a duplicated/non-advancing
    /// pair) -- injecting them keeps this function testable against
    /// fixtures without a live Python-authority RPC.
    #[allow(clippy::too_many_arguments)]
    pub async fn authorize(
        store: &ObserverHandle,
        candidate_sha256: impl Into<String>,
        candidate_state_generation: u64,
        first_authority_reading: &PythonAuthorityOff,
        second_authority_reading: &PythonAuthorityOff,
        governor_authorized: bool,
        governor_reason_code: impl Into<String>,
        ledger_reservation_id: impl Into<String>,
    ) -> Result<Self, LiveBatchDenyReason> {
        let active_quarantine = budgeted(
            AUTHORIZE_STORE_BUDGET,
            "the active-quarantine read",
            store.active_execution_quarantine(),
        )
        .await
        .map_err(LiveBatchDenyReason::QuarantineCheckFailed)?;
        if let Some(q) = active_quarantine {
            return Err(LiveBatchDenyReason::QuarantineActive {
                reason: q.reason,
                entered_at: q.entered_at,
            });
        }

        // `current_state_generation` (a single scalar SELECT), NOT
        // `load_latest_fee_state().generation` -- final-review finding I7,
        // 2026-07-26. The latter materialises EVERY channel's state row
        // through the same single-owner actor the cycle loop writes
        // through, and then discards all of them; this scalar sibling
        // exists precisely to avoid that head-of-line stall. Same answer,
        // by construction (both read `rust_fee_state_generation`).
        let current_state_generation = budgeted(
            AUTHORIZE_STORE_BUDGET,
            "the state-generation read",
            store.current_state_generation(),
        )
        .await
        .map_err(LiveBatchDenyReason::StateGenerationCheckFailed)?;
        if candidate_state_generation != current_state_generation {
            return Err(LiveBatchDenyReason::StateGenerationStale {
                authorized_against: candidate_state_generation,
                current: current_state_generation,
            });
        }

        validate_stable_epoch(first_authority_reading, second_authority_reading)
            .map_err(LiveBatchDenyReason::PythonAuthority)?;

        let governor_reason_code = governor_reason_code.into();
        if !governor_authorized {
            return Err(LiveBatchDenyReason::GovernorDenied {
                reason_code: governor_reason_code,
            });
        }

        let ledger_reservation_id = ledger_reservation_id.into();
        if ledger_reservation_id.is_empty() {
            return Err(LiveBatchDenyReason::LedgerReservationMissing);
        }

        Ok(Self {
            candidate_sha256: candidate_sha256.into(),
            state_generation: current_state_generation,
            python_authority_generation: second_authority_reading.generation,
            governor_reason_code,
            ledger_reservation_id,
        })
    }

    pub fn candidate_sha256(&self) -> &str {
        &self.candidate_sha256
    }

    pub fn state_generation(&self) -> u64 {
        self.state_generation
    }

    pub fn python_authority_generation(&self) -> u64 {
        self.python_authority_generation
    }

    pub fn governor_reason_code(&self) -> &str {
        &self.governor_reason_code
    }

    pub fn ledger_reservation_id(&self) -> &str {
        &self.ledger_reservation_id
    }
}

/// One successfully-broadcast request's raw CLN response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRequestOutcome {
    pub request_id: String,
    pub channel_id: String,
    pub response: Value,
}

/// The result of a fully-successful [`ClnFeeBroadcaster::broadcast_batch`]
/// call: one outcome per request, in submission order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BatchReceipt {
    pub outcomes: Vec<BatchRequestOutcome>,
}

/// Every way [`ClnFeeBroadcaster::broadcast_batch`] can fail. Each
/// variant names the request that failed; requests submitted earlier in
/// the same batch already succeeded (see their `request_id`s in the
/// error's own logging context -- the batch stops at the first
/// non-success outcome and never proceeds past it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcastError {
    /// Defense in depth: an active quarantine was observed INSIDE
    /// `broadcast_batch` itself (e.g. inserted by a concurrent caller
    /// after this authorization was constructed but before it was
    /// consumed here) -- refused before any RPC call.
    Quarantined,
    /// Fix round 1 (review finding 1): a PRIOR call on this exact
    /// broadcaster observed an ambiguous transport outcome and then
    /// failed to persist the resulting quarantine -- this process can no
    /// longer trust its own store-backed quarantine check, so it refuses
    /// every further batch (in memory, immediately, zero calls) until a
    /// restart re-reconciles. Distinct from [`Self::Quarantined`], which
    /// means the store itself reported an active quarantine; this means
    /// the store COULD NOT be trusted to report one.
    Poisoned,
    /// CLN explicitly refused the request (a genuine JSON-RPC error
    /// code). Terminal; never quarantines.
    Rejected { request_id: String, detail: String },
    /// The connection to `lightning-rpc` could not be established at all
    /// -- no bytes could possibly have reached lightningd. Terminal;
    /// never quarantines.
    CleanFailure { request_id: String, detail: String },
    /// Bytes may have reached lightningd but no definite answer came
    /// back. This is the ONLY outcome that quarantines execution.
    Ambiguous { request_id: String, detail: String },
    /// The intent/result ledger itself could not be written. Fails
    /// closed: an unrecorded intent must never be submitted, and an
    /// unrecorded result must never be silently dropped.
    Persistence(String),
}

impl std::fmt::Display for BroadcastError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Quarantined => {
                write!(
                    f,
                    "execution quarantine active: refusing batch with zero RPC calls"
                )
            }
            Self::Poisoned => write!(
                f,
                "this process is poisoned after a quarantine-insert failure following an \
                 ambiguous transport outcome; refusing all further batches until restart"
            ),
            Self::Rejected { request_id, detail } => {
                write!(
                    f,
                    "explicit CLN rejection for request {request_id}: {detail}"
                )
            }
            Self::CleanFailure { request_id, detail } => write!(
                f,
                "clean transport failure for request {request_id} (no bytes could have \
                 reached lightningd): {detail}"
            ),
            Self::Ambiguous { request_id, detail } => write!(
                f,
                "ambiguous transport outcome for request {request_id} -- execution quarantined: \
                 {detail}"
            ),
            Self::Persistence(detail) => {
                write!(f, "failed to persist broadcast intent/result: {detail}")
            }
        }
    }
}

impl std::error::Error for BroadcastError {}

/// How one `setchannel` attempt's transport resolved -- internal to
/// [`ClnFeeBroadcaster::attempt_send`], never exposed outside this
/// module.
enum SendOutcome {
    Success(Value),
    Rejected(String),
    CleanFailure(String),
    Ambiguous(String),
}

/// Fix round 1 (review finding 2): [`ClnFeeBroadcaster::new`]'s only
/// failure mode -- restart quarantine reconciliation itself failed. The
/// arm is REFUSED (never consumed) in this case: `live_mode.into_arm()`
/// is only ever reached on the success path, so a reconciliation failure
/// can never hand back a usable broadcaster.
#[derive(Debug)]
pub struct QuarantineReconciliationFailed(String);

impl std::fmt::Display for QuarantineReconciliationFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "quarantine restart reconciliation failed ({}); refusing to accept this cutover arm \
             -- a restart must be able to trust its own quarantine state before any arm is \
             accepted",
            self.0
        )
    }
}

impl std::error::Error for QuarantineReconciliationFailed {}

/// The guarded CLN fee broadcaster: the ONLY component that may ever send
/// a live `setchannel`. See the module doc for the full contract.
pub struct ClnFeeBroadcaster {
    socket_path: PathBuf,
    store: ObserverHandle,
    timeout_seconds: u64,
    /// Fix round 1 (review finding 1): set (and only ever set) when an
    /// ambiguous transport outcome's quarantine-insert ITSELF fails --
    /// i.e. the one moment this process can no longer trust its own
    /// store-backed quarantine check. Checked FIRST in
    /// [`Self::broadcast_batch`], before even reading the store, and
    /// never cleared for the lifetime of this broadcaster (only a
    /// restart -- a fresh process, fresh reconciliation -- can clear it).
    poisoned: AtomicBool,
    /// The consumed cutover arm this broadcaster's whole session rests
    /// on. Private, no accessor: once stored here there is no way to
    /// extract it back out or mint a second broadcaster from the
    /// [`LiveMode`] it came from (move semantics already guarantee that;
    /// this field simply never offers a path around them).
    _session: crate::cutover_arm::LiveSessionArm,
}

impl ClnFeeBroadcaster {
    /// Construct the broadcaster -- the TERMINAL consumer of `live_mode`'s
    /// arm. Runs restart quarantine reconciliation
    /// (`ObserverHandle::reconcile_quarantine_on_restart`) as its FIRST
    /// action, so a restart can never accept a fresh arm without first
    /// restoring whatever quarantine state the prior process left behind
    /// (brief Step 2: "restart restores quarantine before any arm is
    /// accepted").
    ///
    /// Fix round 1 (review finding 2): a reconciliation failure now
    /// REFUSES construction outright (`live_mode` is dropped, never
    /// converted via `into_arm()`) rather than logging and returning a
    /// usable broadcaster anyway -- the prior behavior meant a wedged
    /// observer DB at exactly the wrong moment would hand back a
    /// broadcaster that skipped restoring quarantine, silently.
    pub async fn new(
        socket_path: PathBuf,
        store: ObserverHandle,
        timeout_seconds: u64,
        live_mode: LiveMode,
    ) -> Result<Self, QuarantineReconciliationFailed> {
        store
            .reconcile_quarantine_on_restart(crate::now_unix())
            .await
            .map_err(|e| QuarantineReconciliationFailed(e.to_string()))?;
        Ok(Self {
            socket_path,
            store,
            timeout_seconds,
            poisoned: AtomicBool::new(false),
            _session: live_mode.into_arm(),
        })
    }

    /// Broadcast one authorized batch. Persists an intent row BEFORE each
    /// request's socket write and a terminal result row afterward (see
    /// the module doc). Stops at the first non-success outcome: any
    /// requests after it in `requests` are never attempted (never even
    /// given an intent row).
    ///
    /// Refuses immediately, with zero RPC calls, if: this broadcaster is
    /// [`poisoned`](Self) (a prior call's quarantine-insert failed after
    /// an ambiguous outcome -- checked FIRST, before the store) or the
    /// store reports an active quarantine (checked second -- the only
    /// store-backed quarantine enforcement actually on this call path,
    /// since `authorization`'s own quarantine read already happened and
    /// returned before this method was ever invoked).
    pub async fn broadcast_batch(
        &self,
        authorization: LiveBatchAuthorization,
        requests: &[PersistedFeeRequest],
    ) -> Result<BatchReceipt, BroadcastError> {
        // Fix round 1 (review finding 1): checked FIRST, before even
        // touching the store -- once poisoned, this process no longer
        // trusts its own store-backed quarantine check (see `poisoned`'s
        // doc comment), so nothing past this point may run.
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(BroadcastError::Poisoned);
        }
        // Defense in depth (review finding 3): `authorization` already
        // bound a quarantine-empty observation at construction time (now
        // read directly from the store -- see
        // `LiveBatchAuthorization::authorize`'s fix-round-1 doc comment),
        // but a concurrent caller could have inserted one since. This is
        // the ONLY store-backed quarantine enforcement actually on the
        // send path (`authorize` ran and returned before this call even
        // began) -- re-checking here costs one DB read and means this
        // path, too, sends zero calls while quarantined.
        if budgeted(
            self.store_budget(),
            "the send-path active-quarantine read",
            self.store.active_execution_quarantine(),
        )
        .await
        .map_err(BroadcastError::Persistence)?
        .is_some()
        {
            return Err(BroadcastError::Quarantined);
        }
        // `authorization` is consumed by value (the whole point of the
        // capability) even though its fields are only used for audit
        // logging below -- there is no way to call this method twice with
        // the same authorization.
        let _authorization = authorization;

        let mut outcomes = Vec::with_capacity(requests.len());
        for request in requests {
            // NOT `unwrap_or_default()` -- final-review finding I7,
            // 2026-07-26. An empty `params_json` in the audit row while
            // the wire call re-serializes the real params (see
            // `attempt_send`) means the ledger and the socket disagree
            // about what was sent, on the one path that touches real
            // funds. Unrecordable intent, unsendable batch.
            let params_json = serde_json::to_string(&request.to_params()).map_err(|e| {
                BroadcastError::Persistence(format!(
                    "serialize setchannel params for request {}: {e}",
                    request.request_id
                ))
            })?;
            let submitted_at = crate::now_unix();
            let intent = BroadcastAttemptIntent {
                cycle_id: request.cycle_id.clone(),
                channel_id: request.channel_id.clone(),
                request_id: request.request_id.clone(),
                method: SETCHANNEL_METHOD.to_string(),
                params_json,
                submitted_at,
            };
            let attempt_id = budgeted(
                self.store_budget(),
                "the broadcast-attempt intent write",
                self.store.insert_broadcast_attempt(intent),
            )
            .await
            .map_err(BroadcastError::Persistence)?;

            match self.attempt_send(request).await {
                SendOutcome::Success(response) => {
                    self.record_result(attempt_id, BroadcastAttemptOutcome::Success, None)
                        .await;
                    outcomes.push(BatchRequestOutcome {
                        request_id: request.request_id.clone(),
                        channel_id: request.channel_id.clone(),
                        response,
                    });
                }
                SendOutcome::Rejected(detail) => {
                    self.record_result(
                        attempt_id,
                        BroadcastAttemptOutcome::Rejected,
                        Some(detail.clone()),
                    )
                    .await;
                    return Err(BroadcastError::Rejected {
                        request_id: request.request_id.clone(),
                        detail,
                    });
                }
                SendOutcome::CleanFailure(detail) => {
                    self.record_result(
                        attempt_id,
                        BroadcastAttemptOutcome::CleanFailure,
                        Some(detail.clone()),
                    )
                    .await;
                    return Err(BroadcastError::CleanFailure {
                        request_id: request.request_id.clone(),
                        detail,
                    });
                }
                SendOutcome::Ambiguous(detail) => {
                    // Fix round 1 (review finding 1, CRITICAL): quarantine
                    // is inserted BEFORE the terminal result is recorded,
                    // and on a quarantine-insert FAILURE the result is
                    // never recorded at all -- the intent row is left
                    // `outcome IS NULL`, exactly what
                    // `unresolved_broadcast_attempts`/restart
                    // reconciliation keys on. The prior ordering (result
                    // first, quarantine second, quarantine failure only
                    // logged) meant a quarantine-insert failure left an
                    // `outcome = 'ambiguous'` row that reconciliation
                    // would never see again AND no in-memory signal --
                    // fail-open in-process and across restart, exactly
                    // what the doc comment two lines below used to
                    // (wrongly) claim was prevented.
                    if let Err(quarantine_err) = self.quarantine(request, &detail).await {
                        self.poisoned.store(true, Ordering::SeqCst);
                        eprintln!(
                            "revops: FAILED TO PERSIST EXECUTION QUARANTINE after an ambiguous \
                             transport outcome for attempt {attempt_id} ({quarantine_err:#}); \
                             its intent row is left UNRESOLVED (restart reconciliation will \
                             pick it up) and THIS PROCESS IS NOW POISONED -- refusing every \
                             further batch until restart"
                        );
                        return Err(BroadcastError::Ambiguous {
                            request_id: request.request_id.clone(),
                            detail: format!(
                                "{detail} (quarantine insert ALSO failed: {quarantine_err:#}; \
                                 this process is now poisoned)"
                            ),
                        });
                    }
                    self.record_result(
                        attempt_id,
                        BroadcastAttemptOutcome::Ambiguous,
                        Some(detail.clone()),
                    )
                    .await;
                    return Err(BroadcastError::Ambiguous {
                        request_id: request.request_id.clone(),
                        detail,
                    });
                }
            }
        }
        Ok(BatchReceipt { outcomes })
    }

    /// The store budget for every call `broadcast_batch` and its helpers
    /// make (final-review finding I7, 2026-07-26): the SAME number that
    /// budgets the RPC half, so a broadcaster has exactly one
    /// operator-visible timeout. See [`budgeted`].
    fn store_budget(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds)
    }

    async fn record_result(
        &self,
        attempt_id: i64,
        outcome: BroadcastAttemptOutcome,
        detail: Option<String>,
    ) {
        if let Err(e) = budgeted(
            self.store_budget(),
            "the broadcast-attempt result write",
            self.store.record_broadcast_attempt_result(
                attempt_id,
                outcome,
                detail,
                crate::now_unix(),
            ),
        )
        .await
        {
            eprintln!(
                "revops: broadcast attempt result record failed ({e}) for attempt {attempt_id}; \
                 the intent row stays unresolved and will be treated as ambiguous on the next \
                 restart"
            );
        }
    }

    /// Insert an execution quarantine after an ambiguous transport
    /// outcome. Returns the underlying store error on failure -- the
    /// caller (the `Ambiguous` arm of [`Self::broadcast_batch`]) is what
    /// decides how to fail closed on that (leave the intent row
    /// unresolved, poison the process); this method never itself decides
    /// that policy or swallows the error.
    async fn quarantine(&self, request: &PersistedFeeRequest, detail: &str) -> anyhow::Result<()> {
        let entry = QuarantineEntry {
            reason: format!("ambiguous post-submission transport outcome: {detail}"),
            cycle_id: None, // see fee_runway's DDL comment: no FK guarantee for live-mode cycles
            channel_id: Some(request.channel_id.clone()),
            request_id: Some(request.request_id.clone()),
            entered_at: crate::now_unix(),
        };
        budgeted(
            self.store_budget(),
            "the execution-quarantine write",
            self.store.insert_execution_quarantine(entry),
        )
        .await
        .map_err(|detail| anyhow::anyhow!(detail))?;
        Ok(())
    }

    /// The only action call site in this module (and, by the removed
    /// `tests/fee_scheduler.rs` source-scan guard, this whole crate).
    /// Connect and call are each budgeted independently: a connect
    /// failure/timeout means no bytes could possibly have been sent
    /// ([`SendOutcome::CleanFailure`]); a call-phase timeout means bytes
    /// were already written and the response is simply unknown
    /// ([`SendOutcome::Ambiguous`]).
    async fn attempt_send(&self, request: &PersistedFeeRequest) -> SendOutcome {
        let budget = Duration::from_secs(self.timeout_seconds);

        let connected = tokio::time::timeout(budget, cln_rpc::ClnRpc::new(&self.socket_path)).await;
        let mut rpc = match connected {
            Err(_) => {
                return SendOutcome::CleanFailure(
                    "connect to lightning-rpc timed out before any write was attempted".to_string(),
                )
            }
            Ok(Err(e)) => return SendOutcome::CleanFailure(format!("connect failed: {e}")),
            Ok(Ok(rpc)) => rpc,
        };

        let called = tokio::time::timeout(
            budget,
            rpc.call_raw::<Value, Value>(SETCHANNEL_METHOD, &request.to_params()),
        )
        .await;

        match called {
            Err(_) => SendOutcome::Ambiguous(
                "no response within the timeout budget after the request was sent".to_string(),
            ),
            Ok(Ok(value)) => SendOutcome::Success(value),
            Ok(Err(rpc_err)) => {
                if rpc_err.code.is_some() {
                    // A genuine JSON-RPC error object from lightningd
                    // itself -- an explicit, definite rejection.
                    SendOutcome::Rejected(rpc_err.to_string())
                } else {
                    // Every OTHER `call_raw` error is synthesized locally
                    // (write failure, EOF/no-response, undecodable
                    // framing, a response missing both `result` and
                    // `error`) -- in every one of those cases bytes may
                    // already have left this process, so the true outcome
                    // is unknown.
                    SendOutcome::Ambiguous(rpc_err.to_string())
                }
            }
        }
    }
}
