//! Single-owner fee-cycle scheduler + dry-run wiring (Phase 4b Task 6,
//! checklist item 5).
//!
//! ## Shape
//!
//! [`spawn`] starts two halves:
//!
//! (a) **One dedicated `std::thread`** ([`CycleOwner`]) that OWNS
//!     `ControllerState` and the ONE long-lived [`PyRandom`] for the whole
//!     plugin lifetime. The RNG is seeded exactly once, at spawn, from
//!     `now_unix()` (Global Constraint: "ONE long-lived PyRandom seeded
//!     once at scheduler start"). Nothing else ever holds the state or the
//!     RNG -- the same single-owner discipline `revops_db::actor` uses for
//!     its `Connection`.
//!
//! (b) **One tokio trigger task** that decides WHEN a cycle runs (see
//!     "Cycle triggering" below) and then performs the ASYNC half of the
//!     cycle -- `fee_config::resolve_fee_cfg` (T1, per cycle so runtime
//!     `revenue-config set` changes on the Python side stay visible), the
//!     `neighbor_median_min_competitors` resolution (Phase 4b Task 8a:
//!     any resolvable positive-integer threshold, fail-closed only on an
//!     unresolvable value),
//!     and `fee_evidence::prefetch_rpc` (T2) -- and sends the prepared
//!     inputs to the owner thread as one [`CycleMsg::RunPrepared`]
//!     message.
//!
//! ## Cycle triggering (Design Note 1, T6b)
//!
//! The window's lifecycle is re-hydrate-per-cycle: every cycle re-reads
//! Python's persisted `v2_state_json` flush so both controllers start the
//! cycle from the same state. That only works if Rust hydrates AFTER
//! Python's end-of-cycle flush. Production Python is NOT phase-locked:
//! `fee_adjustment_loop` (cl-revenue-ops.py) starts at +90s and then
//! sleeps `interval +/- 20% jitter` AFTER each cycle -- an unphased random
//! walk (+/-360s per step at the default 1800s interval), so any fixed
//! wall-phase offset decays within a few cycles and Rust would hydrate
//! mid-Python-cycle from stale state, emitting decision mismatches for
//! timing (not porting) reasons.
//!
//! [`TriggerMode::FlushTriggered`] (the window default) therefore keys
//! every Rust cycle off the OBSERVED flush: poll the production DB
//! read-only every `poll_secs` (cheap single-row [`read_flush_marker`]
//! query), and when the marker changes, wait `settle_secs` of quiescence
//! (the flush transaction plus Python's immediate cycle-tail writes, e.g.
//! `_prune_stale_states`) before running exactly one cycle. If no advance
//! is observed for more than 2x `fee_interval`, the trigger logs loudly
//! (Python may be dead or paused) and keeps polling -- it never runs a
//! cycle on stale state. [`FlushWatcher`] holds that state machine;
//! `tests/fee_scheduler.rs` drives it synchronously.
//!
//! [`TriggerMode::FixedInterval`] preserves the T6 wall-clock cadence
//! (`fee_interval` + phase offset from plugin start) for cutover, where
//! Python is gone, nothing flushes, and wall-clock cadence is correct. At
//! cutover the [`StateLifecycle::SeedOnce`] variant likewise flips
//! hydration to once-at-start-then-evolve-in-memory: scheduler config
//! changes, not a rework (Design Note 1's recorded consequence).
//!
//! ## Clock discipline
//!
//! `now_unix()` is read EXACTLY once per cycle, at the top of
//! [`CycleOwner::run_cycle`], and that single value is threaded through
//! `FixedDecisionClock` / `build_evidence_snapshot` to every downstream
//! consumer (Global Constraint: "clock once per cycle"). The clock is an
//! injected `FnMut() -> i64` so tests can count reads; production passes
//! `crate::now_unix`.
//!
//! ## What this module never does
//!
//! No broadcast: there is no fee-broadcast RPC call anywhere in THIS
//! module (or anywhere else `CycleOwner`/`spawn`/`spawn_with_thread_spawner`
//! reach). [`StateLifecycle::SeedOnce`]'s executor swap
//! (`RecordingFeeExecutor`) is capability-free by construction -- it owns
//! no socket, RPC client, or broadcaster of any kind (see
//! `revops_fees::execution::RecordingFeeExecutor`'s own doc comment) --
//! so the autonomous-shadow cycle path is structurally connection-free,
//! never merely policy-gated. `tests/fee_scheduler.rs`'s
//! `seedonce_cycle_makes_zero_connections_to_a_live_cln_socket` proves
//! this against a REAL live listener, not just the type-level guarantee.
//!
//! Prior to the stateful-shadow revision plan's Task 9, a source-scan
//! guard in `tests/fee_scheduler.rs` additionally asserted the broadcast
//! RPC's literal method name was absent from this whole CRATE. That guard
//! is gone (superseded by Task 10's workspace-wide
//! `tests/action_surface.rs` allowlist): `crate::fee_execution` now holds
//! the one guarded action call site, behind the guarded live broadcaster
//! type this module never constructs, holds, or even names -- itself
//! behind the [`crate::fee_mode::LiveMode`] capability this module never
//! constructs or holds either. The production DB is opened read-only (via
//! `fee_evidence`), and every write target this module itself produces
//! (decision journal, state JSONL, dry-run econ ledger, and -- for
//! `SeedOnce` -- the Rust-owned state/audit commit) is a Rust-owned file
//! or store, never a live broadcast. Python stays authoritative for the
//! whole dry-run window regardless of which lifecycle is active.
//!
//! ## Wake/policy triggers + the fee-debug query (Phase 4b Task 7)
//!
//! [`CycleMsg`] gets four more variants on top of T6's `RunPrepared`/
//! `RunCycleNow`/`Shutdown`: `PolicyChanged`, `VegasSpikeCheck`, `WakeAll`,
//! `Query`. Every one of them is a HINT delivered to the single owner
//! thread over the same bounded [`SchedulerIngress`] `RunPrepared` already
//! uses -- never a direct call into `ControllerState` from wherever the
//! trigger originates, and never an inline cycle run inside a notification
//! handler. That is the same settle/coalesce discipline T6b built for the
//! flush trigger: a wake changes IN-MEMORY sleep/edge-trigger bookkeeping
//! on the owner thread (cheap, synchronous, no IO), it never itself runs
//! `run_fee_cycle` -- the NEXT scheduled cycle (flush-triggered or, at
//! cutover, wall-clock) is what actually re-evaluates fees, now unblocked
//! by the just-cleared sleep state. This mirrors Python's own wake
//! functions (`wake_all_sleeping_channels`/`_maybe_wake_for_vegas_spike`/
//! `_handle_policy_change`): they only clear `is_sleeping`/backdate
//! `last_update`, and the SAME `adjust_all_fees` cycle loop that would
//! have run anyway is what reads the cleared state.
//!
//! Two triggers are wired live for the dry-run window:
//!
//! - **`WakeAll`**: the manual `revenue-r-fee-wake` RPC (`main.rs`),
//!   operator/diagnostic use, mirrors Python's `revenue-wake-all`
//!   semantics. Fire-and-forget over the mpsc channel (the variant carries
//!   no reply sender) -- unlike Python's synchronous `channels_woken`
//!   count, the RPC's ack cannot report how many channels woke without a
//!   round trip; `CycleMsg::Query` exists precisely for a caller who wants
//!   to see the resulting state afterward.
//! - **`VegasSpikeCheck`**: sent by the flush-trigger loop (`trigger_loop`,
//!   `TriggerMode::FlushTriggered` only) on every poll that does NOT
//!   already dispatch a full cycle -- a full cycle's `run_fee_cycle` calls
//!   `maybe_wake_for_vegas_spike` itself, so sending it again the same
//!   poll would be redundant. **Cutover watch item**: production Python
//!   checks Vegas spikes off its live HTLC/mempool-fee monitor (continuous,
//!   event-driven); this dry-run's ticker cadence (`DEFAULT_FLUSH_POLL_SECS`
//!   = 30s) is the faithful-enough stand-in for the window, not a claim of
//!   identical latency -- re-derive the real trigger at cutover once
//!   Python's monitor loop has a Rust port to key off instead.
//!
//! `PolicyChanged` is constructed by nothing yet (`main.rs` has no
//! cross-plugin observation of a Python-side `setconfig`/policy-RPC during
//! this window) -- it exists so the cutover's own policy-RPC lands on an
//! already-stable enum rather than a later breaking change; the handler
//! itself (owner-thread match arm below) is real and tested, only the
//! caller is future work.
//!
//! `Query` answers the `revenue-r-fee-debug` RPC synchronously: the owner
//! thread reads `ControllerState` (never blocking on RPC/DB IO to answer
//! it) and replies over the included `std::sync::mpsc::Sender`; `main.rs`
//! receives that reply off the async runtime via `spawn_blocking` (a plain
//! `std::sync::mpsc::Receiver::recv` would otherwise stall a tokio worker
//! thread).

use std::collections::{HashMap, HashSet};

use revops_fees::thompson::dynamics;
use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc as tokio_mpsc;

use cln_plugin::options::Value as OptValue;
use revops_analytics::policy::{FeeStrategy, PeerPolicy};
use revops_db::actor::DbHandle;
use revops_db::fee_runway::{
    FeeCycleCommit, FeeRestartMarkerRow, GovernorAuditRow, PreparedFeeActionRow,
    ShadowCycleOutcomeRow,
};
use revops_fees::cycle::{
    handle_policy_change, maybe_wake_for_vegas_spike, run_fee_cycle, wake_all_sleeping_channels,
    ChannelCycleState, ChannelFeeState, ChannelInfo, ChannelStateRow, ControllerState, CycleDeps,
    DecisionClock, FeeCfgSnapshot, FixedDecisionClock, StateSink,
};
use revops_fees::execution::{
    decide_set_channel_fee, FeeAuthorizationRequest, FeeAuthorizer, FeeExecutor,
    GovernedFeeAuthorizer, GovernedTrace, PreparedFeeAction, PureFeeExecutor, RecordingFeeExecutor,
    SetFeeRequest,
};
use revops_fees::journal::{FeeDecision, Journal};
use revops_fees::market::{FeePrior, INITIAL_PRIOR_NUDGE_WEIGHT};
use revops_fees::profiles::fee_profile;
use revops_fees::pyjson::OValue as PyOValue;
use revops_fees::pyrand::{DecisionEntropy, PyRandom};
use revops_fees::thompson::GaussianThompsonState;

use crate::config_resolve::PythonOptionCache;
use crate::fee_config;
use crate::fee_evidence::{
    build_evidence_snapshot, prefetch_rpc, EvidenceSnapshot, MempoolEvidenceSource, RpcPrefetch,
    MEMPOOL_MA_WINDOW_SECONDS,
};
use crate::fee_governor::GovernorWiring;
use crate::fee_state::{
    rehydrate, rehydrate_from_rows, seed_once_from_python, serialize_state_envelope,
    set_skip_gates_to_owned, HydrationSource, JournalStateSink, RunwayStateStore, SeedOutcome,
};
use crate::fee_triggers::{build_receipt, FeeTrigger, TriggerOutcome, TriggerQueue};

/// T6's fixed tick phase offset from plugin start, kept as the
/// [`TriggerMode::FixedInterval`] default for cutover. During the dry-run
/// window it is NOT a hydrate-after-flush guarantee (Python's jittered
/// sleep is an unphased random walk; see the module doc) -- that is what
/// [`TriggerMode::FlushTriggered`] exists for.
pub const TICK_PHASE_OFFSET_SECS: u64 = 120;

/// Flush-trigger poll cadence default: a single-row read-only query every
/// 30s is negligible against Python's own per-cycle DB traffic.
pub const DEFAULT_FLUSH_POLL_SECS: u64 = 30;

/// Flush-trigger settle default: observed-advance -> cycle delay, letting
/// the flush transaction and Python's immediate cycle-tail writes
/// (`_prune_stale_states`, decision-summary bookkeeping) go quiescent.
pub const DEFAULT_FLUSH_SETTLE_SECS: u64 = 30;

/// Bounded trigger-queue capacity (Task 6 step 4): sized generously above
/// the five trigger KINDS coalesced by key -- in practice only a handful
/// of distinct pending (kind, scope) entries exist at once (one
/// `FixedInterval`/`WakeAll`/`VegasSpike` slot each, plus one per
/// distinct channel/peer with a pending `FailedForward`/`PolicyChanged`).
/// Saturating this many DISTINCT keys between cycles is itself loud
/// evidence of a real backlog, not routine operation.
pub const TRIGGER_QUEUE_CAPACITY: usize = 64;

/// Fixed admission capacity for every scheduler producer. The owner has
/// exactly one Tokio MPSC receiver; async callers await capacity and the
/// dedicated synchronous callback/owner threads use `blocking_send`.
pub const OWNER_QUEUE_CAPACITY: usize = 64;

/// The private-sender boundary for scheduler admission. Exposing this
/// wrapper (rather than Tokio's raw sender) makes async backpressure the
/// only public production send API.
///
/// ~~~compile_fail,E0451
/// fn forge(
///     tx: tokio::sync::mpsc::Sender<revops::fee_scheduler::CycleMsg>,
/// ) -> revops::fee_scheduler::SchedulerIngress {
///     revops::fee_scheduler::SchedulerIngress { tx }
/// }
/// ~~~
#[derive(Clone)]
pub struct SchedulerIngress {
    tx: tokio_mpsc::Sender<CycleMsg>,
}

impl SchedulerIngress {
    pub async fn send(&self, msg: CycleMsg) -> Result<(), tokio_mpsc::error::SendError<CycleMsg>> {
        self.tx.send(msg).await
    }

    pub(crate) fn blocking_send(
        &self,
        msg: CycleMsg,
    ) -> Result<(), tokio_mpsc::error::SendError<CycleMsg>> {
        self.tx.blocking_send(msg)
    }

    /// Task 59 §3.3 phase 1: Query-only, non-parking admission. Tokio's
    /// `try_send` either transfers the message or returns it -- there is
    /// no later enqueue after a refusal, so `Err` proves nothing was
    /// admitted and the caller never parks against the bounded queue.
    /// By construction this path can carry ONLY `CycleMsg::Query`
    /// (diagnostic reads); every effectful message type keeps Task 57's
    /// pinned async-backpressure-only public send contract.
    pub fn try_send_query(
        &self,
        query: FeeDebugQuery,
        reply: std_mpsc::Sender<serde_json::Value>,
    ) -> Result<(), QueryAdmissionRefused> {
        self.tx
            .try_send(CycleMsg::Query(query, reply))
            .map_err(|e| match e {
                tokio_mpsc::error::TrySendError::Full(_) => QueryAdmissionRefused::Saturated,
                tokio_mpsc::error::TrySendError::Closed(_) => QueryAdmissionRefused::OwnerGone,
            })
    }

    /// Raw receiver construction is crate-private: external code may submit
    /// through `send`, but can never install an arbitrary consumer behind a
    /// vetted observer pass.
    ///
    /// ~~~compile_fail,E0624
    /// let _ = revops::fee_scheduler::SchedulerIngress::bounded_channel(1);
    /// ~~~
    pub(crate) fn bounded_channel(capacity: usize) -> (Self, tokio_mpsc::Receiver<CycleMsg>) {
        let (tx, rx) = tokio_mpsc::channel(capacity.max(1));
        (Self { tx }, rx)
    }
}

/// Task 59 §3.3: typed refusal from the Query-only try-path. Both
/// readings are section-local read failures (F13) -- retryable, never
/// proof the owner died, never a trip condition.
#[derive(Debug)]
pub enum QueryAdmissionRefused {
    /// The bounded owner queue is at capacity admitting real work;
    /// nothing was enqueued (`owner_queue_saturated`).
    Saturated,
    /// The owner task is gone; nothing can be enqueued at all.
    OwnerGone,
}

/// Task 59 §3.3: diagnostic-freshness bound for the RPC bridge's
/// response phase. Explicitly NOT a legitimate-backlog bound: a fully
/// store-contended 63-deep heavy backlog is minutes-scale, and no
/// constant both covers it and stays useful as a diagnostic. Everything
/// slower converts into the retryable typed `owner_response_timeout`.
pub const RPC_BRIDGE_RECV_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Task 59 §3.6: pending A3 age past which fee-debug logs a warning.
/// Visibility only -- there is deliberately no cancellation (an owner
/// timeout could not cancel the queued command and would recreate the
/// scheduled-commit ambiguity F7 killed).
pub const A3_PENDING_AGE_WARN_SECONDS: i64 = 60;

/// Task 59 §3.3: the two-phase bounded diagnostic-query bridge, replacing
/// the two unbounded waits (a parked `send().await` admission and an
/// unbounded `recv()`). Phase 1 refuses admission typed and immediately;
/// phase 2 bounds the response wait. Both error shapes carry DISTINCT
/// stable codes and are section-local read failures -- retryable, loud,
/// never evidence the owner died.
pub async fn query_owner_bounded(
    ingress: &SchedulerIngress,
    query: FeeDebugQuery,
    budget: std::time::Duration,
) -> serde_json::Value {
    let (reply_tx, reply_rx) = std_mpsc::channel();
    match ingress.try_send_query(query, reply_tx) {
        Err(QueryAdmissionRefused::Saturated) => {
            return serde_json::json!({"error": {
                "code": "owner_queue_saturated",
                "message": "the fee owner's bounded queue is at capacity admitting real \
                            work; nothing was enqueued -- the owner is busy, not dead; retry"
            }});
        }
        Err(QueryAdmissionRefused::OwnerGone) => {
            return serde_json::json!({"error": {
                "code": "owner_gone",
                "message": "fee-cycle owner thread not running"
            }});
        }
        Ok(()) => {}
    }
    // `recv_timeout` is a blocking std call -- `spawn_blocking` keeps it
    // off the tokio worker thread this async fn is polled on.
    match tokio::task::spawn_blocking(move || reply_rx.recv_timeout(budget)).await {
        Ok(Ok(value)) => value,
        Ok(Err(std_mpsc::RecvTimeoutError::Timeout)) => serde_json::json!({"error": {
            "code": "owner_response_timeout",
            "message": format!(
                "the admitted diagnostic query got no answer within {}s: the owner is \
                 either busy behind a legitimate store-contended backlog or genuinely \
                 stuck -- see loop health; retryable, never a trip condition",
                budget.as_secs()
            )
        }}),
        _ => serde_json::json!({"error": {
            "code": "owner_gone",
            "message": "fee-cycle owner thread exited before answering"
        }}),
    }
}

/// The binary's source-commit identity for provenance rows (seed events,
/// cycle commits, restart markers). Release builds inject the real commit
/// via the `REVOPS_SOURCE_COMMIT` build-time env var; a plain `cargo
/// build` falls back to the crate version so the column is never empty.
pub fn source_commit() -> &'static str {
    match option_env!("REVOPS_SOURCE_COMMIT") {
        Some(commit) if !commit.is_empty() => commit,
        _ => concat!("cargo:", env!("CARGO_PKG_VERSION")),
    }
}

/// sha256 (hex) of the running binary, computed once per process (the
/// `rust_fee_cycles.binary_sha256` identity column). `"unavailable"` when
/// the executable cannot be read -- identity degrades loudly-typed, never
/// panics.
pub fn binary_sha256() -> &'static str {
    use std::sync::OnceLock;
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        let bytes = std::env::current_exe()
            .ok()
            .and_then(|path| std::fs::read(path).ok());
        match bytes {
            Some(bytes) => {
                use sha2::{Digest, Sha256};
                let digest = Sha256::digest(&bytes);
                let mut hex = String::with_capacity(64);
                for byte in digest {
                    use std::fmt::Write;
                    let _ = write!(hex, "{byte:02x}");
                }
                hex
            }
            None => "unavailable".to_string(),
        }
    })
}

/// Undo an in-memory hydration that could not be fully recorded (seed
/// provenance / restart marker write failure): back to the pre-hydration
/// empty maps so the next cycle's retry starts clean.
fn clear_hydrated_state(state: &mut ControllerState) {
    state.cycle_states.clear();
    state.fee_states.clear();
    state.skip_gate_prev.clear();
    state.skip_gate_seen.clear();
}

/// When a cycle runs (T6b's decision enum; see the module doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerMode {
    /// Window default: run one cycle `settle_secs` after each observed
    /// advance of Python's flush marker, polling every `poll_secs`.
    FlushTriggered { poll_secs: u64, settle_secs: u64 },
    /// Cutover mode: wall-clock cadence (`fee_interval`, first tick offset
    /// by `phase_offset_secs` from spawn) -- T6's behavior, correct once
    /// Python is gone and there is no flush to observe.
    FixedInterval { phase_offset_secs: u64 },
    /// Owner-only mode: cadence requests arrive through the bounded observer runtime.
    ExternalOnly,
}

impl Default for TriggerMode {
    fn default() -> Self {
        TriggerMode::FlushTriggered {
            poll_secs: DEFAULT_FLUSH_POLL_SECS,
            settle_secs: DEFAULT_FLUSH_SETTLE_SECS,
        }
    }
}

/// Read Python's fee-state flush marker: `MAX(_rowid_)` over
/// `fee_strategy_state` (`Ok(None)` = empty table).
///
/// ## Why this column is the marker (verified against production Python)
///
/// The requirement is a value that steps exactly once per end-of-cycle
/// state flush. In `modules/fee_controller.py`, `adjust_all_fees` defers
/// every per-channel row to `_flush_pending_fee_strategy_rows`, which
/// lands them via `database.update_fee_strategy_states_batch` -- ONE
/// `BEGIN IMMEDIATE` transaction of `INSERT OR REPLACE` statements
/// (modules/database.py). `INSERT OR REPLACE` deletes the conflicting row
/// and re-inserts WITHOUT an explicit rowid, so every flushed row gets a
/// fresh `MAX(rowid)+1` rowid: the marker steps once per flush commit
/// EVEN when every column value is byte-identical (verified: the table's
/// only writers are `INSERT OR REPLACE` and `DELETE` -- no `UPDATE`
/// statements exist).
///
/// The rejected candidates:
/// - `MAX(last_update)`: that column is the observation-window CURSOR
///   (`ChannelCycleState.last_update`), advanced only when a channel
///   ingests an observation/adjusts; a no-adjustment cycle flushes rows
///   with unchanged cursors, and wake paths even BACKDATE it
///   (`fee_controller.py` `_wake_...`/backdating around line 4327). It
///   stalls exactly when fees are stable -- most of the time.
/// - a `v2_state_json` cycle counter: none exists.
///   `ChannelFeeState.to_v2_dict` (fee_controller.py) carries posterior /
///   PID / timer fields only, none of which move on skip paths.
///
/// Caveats, all handled by the [`FlushWatcher`] contract of "any CHANGE
/// is an advance" plus the settle delay:
/// - `_prune_stale_states` DELETEs rows right after the flush and VACUUM
///   renumbers rowids, so the marker can DECREASE -- still a change, and
///   the next flush moves it again, so nothing becomes unobservable.
/// - Out-of-cycle immediate writes (hook threads, manual RPC paths,
///   `set_initial_fee`) also step it: the extra Rust cycle they trigger is
///   an extra parity trial on freshly-flushed state -- valid, just
///   unscheduled.
pub fn read_flush_marker(db_path: &Path) -> anyhow::Result<Option<i64>> {
    let conn = revops_db::open_read_only(db_path)?;
    let marker = conn.query_row("SELECT MAX(_rowid_) FROM fee_strategy_state", [], |row| {
        row.get::<_, Option<i64>>(0)
    })?;
    Ok(marker)
}

/// T7's `PolicyChanged` handler needs `channel_id -> peer_id` to resolve
/// which channels belong to the changed peer (`handle_policy_change`
/// filters on it). A fresh, unpinned read-only open + query -- this is an
/// out-of-cycle action, not a per-cycle evidence read, so it does not need
/// (and must not reuse) the per-cycle snapshot's pinned transaction.
fn read_channel_states_readonly(db_path: &Path) -> anyhow::Result<Vec<ChannelStateRow>> {
    let conn = revops_db::open_read_only(db_path)?;
    crate::fee_evidence::read_channel_states(&conn)
}

/// Per-poll parameters for [`FlushWatcher::on_poll`] (passed per call so
/// a runtime `fee_interval` change moves the staleness bound immediately).
#[derive(Debug, Clone, Copy)]
pub struct WatchParams {
    /// Observed-advance -> cycle delay.
    pub settle_secs: u64,
    /// Loud-log bound: no advance for LONGER than this (2x `fee_interval`)
    /// means Python may be dead/paused.
    pub stale_after_secs: u64,
}

/// What one poll observation means for the trigger loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    /// First successful read: recorded as the baseline, NEVER a trigger
    /// (the marker's age is unknown at plugin start -- Python could be
    /// mid-cycle right now).
    Baselined,
    /// Marker changed: settle delay (re-)armed. A change while already
    /// settling re-arms it -- rapid successive writes coalesce into ONE
    /// cycle once the DB goes quiescent.
    Advanced,
    /// Settle elapsed after an advance: run exactly one cycle NOW.
    RunCycle,
    /// Nothing to do this poll.
    Idle,
    /// No advance for `silent_secs` (> `stale_after_secs`): log loudly,
    /// keep polling, do NOT run a cycle on stale state. Re-armed every
    /// `stale_after_secs` of continued silence (loud, not spammy).
    StaleNoFlush { silent_secs: i64 },
}

/// The flush-observation state machine ([`TriggerMode::FlushTriggered`]'s
/// core), deliberately synchronous and clock-injected: the tokio loop
/// feeds it real polls, the tests scripted timelines.
#[derive(Debug)]
pub struct FlushWatcher {
    /// `None` until the first successful marker read (which baselines).
    last_marker: Option<Option<i64>>,
    /// Last observed change (or baseline) -- the staleness anchor.
    last_advance_at: i64,
    /// Armed by an observed change: cycle at the first poll at/after this.
    settle_deadline: Option<i64>,
    /// Rate limit for [`PollOutcome::StaleNoFlush`].
    next_stale_report_at: Option<i64>,
}

impl FlushWatcher {
    pub fn new(now: i64) -> FlushWatcher {
        FlushWatcher {
            last_marker: None,
            last_advance_at: now,
            settle_deadline: None,
            next_stale_report_at: None,
        }
    }

    /// Feed one successful marker read. Read ERRORS must not reach this
    /// method (the loop logs and skips them): an unreadable DB is not an
    /// advance and must never fire a cycle.
    pub fn on_poll(&mut self, marker: Option<i64>, now: i64, params: &WatchParams) -> PollOutcome {
        let Some(prev) = self.last_marker else {
            self.last_marker = Some(marker);
            self.last_advance_at = now;
            return PollOutcome::Baselined;
        };
        if prev != marker {
            self.last_marker = Some(marker);
            self.last_advance_at = now;
            self.settle_deadline = Some(now + params.settle_secs as i64);
            self.next_stale_report_at = None;
            return PollOutcome::Advanced;
        }
        if let Some(deadline) = self.settle_deadline {
            if now >= deadline {
                self.settle_deadline = None;
                return PollOutcome::RunCycle;
            }
            return PollOutcome::Idle;
        }
        let silent_secs = now - self.last_advance_at;
        if silent_secs > params.stale_after_secs as i64
            && self.next_stale_report_at.is_none_or(|t| now >= t)
        {
            self.next_stale_report_at = Some(now + params.stale_after_secs as i64);
            return PollOutcome::StaleNoFlush { silent_secs };
        }
        PollOutcome::Idle
    }
}

/// State lifecycle for the owner thread (Design Note 1's decision enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateLifecycle {
    /// Dry-run window mode (DECIDED for the whole window): re-read
    /// Python's persisted `v2_state_json` at the top of EVERY cycle, so
    /// each cycle is an independent parity trial.
    RehydratePerCycle,
    /// Cutover mode (the recorded flip): hydrate ONCE from the production
    /// DB on the first cycle (Python's final flush is the seed), then
    /// evolve in memory.
    SeedOnce,
}

/// Scheduler configuration, resolved by `main.rs` at plugin init.
pub struct SchedulerConfig {
    /// Production DB (read-only, `revops-r-db-path` expanded).
    pub db_path: PathBuf,
    /// `lightning-rpc` unix socket for the async prefetch half.
    pub socket_path: PathBuf,
    /// T3 resolution (`resolve_journal_dir`): every write target lives
    /// under here -- decision journal, state JSONL, dry-run econ ledger.
    pub journal_dir: PathBuf,
    pub lifecycle: StateLifecycle,
    /// When cycles run: flush-observation (window default) or wall-clock
    /// (cutover). See [`TriggerMode`].
    pub trigger: TriggerMode,
}

/// What a `revenue-r-fee-debug` [`CycleMsg::Query`] asks the owner thread
/// for -- see the module doc's "Wake/policy triggers" section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeeDebugQuery {
    /// One channel's DTS/cycle summary (`ControllerState::dts_summary`,
    /// itself the `get_dts_summary` port, py 5087-5122): `{posterior_mean,
    /// posterior_std, broadcast_fee_ppm, forward_count}`, or `{"error"}`
    /// if no state exists yet for that channel.
    Channel(String),
    /// The controller-wide summary: `last_decision_summary`
    /// (`_set_last_decision_summary`/`get_last_decision_summary`, py
    /// 3031-3048) plus a `channels` map of every tracked channel's SAME
    /// per-channel shape as `Channel` above, keyed by channel_id.
    Summary,
    /// Task 10: the in-memory counters `revenue-r-fee-runway-status`
    /// (the read-only runway status RPC) surfaces -- lifecycle,
    /// hydration/seed bookkeeping, persistence failures, trigger-queue
    /// pending/dropped counts, the last cycle's timestamp/outcome, and
    /// whether the dry-run governor/ledger is open. Answered synchronously
    /// off this owner's own fields, no IO -- never blocks the cycle loop.
    RunwayCounters,
}

/// Messages on the owner thread's channel.
///
/// T7 extends this enum with its wake/debug variants (`PolicyChanged`,
/// `VegasSpikeCheck`, `WakeAll`, `Query`) -- the owner-thread channel is
/// the stable seam those triggers land on. See the module doc's
/// "Wake/policy triggers" section for the full design.
pub enum CycleMsg {
    /// One cycle's prepared inputs from the async prefetch half; one
    /// message == one cycle on the owner thread.
    RunPrepared(
        Box<PreparedCycle>,
        tokio::sync::oneshot::Sender<Result<FeeCycleCompletion, String>>,
    ),
    /// Ask for an immediate out-of-schedule cycle: the owner thread
    /// forwards this to the async half (only IT can prefetch), which
    /// prepares inputs and sends back a `RunPrepared`.
    RunCycleNow,
    /// `_handle_policy_change` (py 7356-7400): wake `peer_id`'s sleeping
    /// channels so the NEXT cycle applies whatever policy changed.
    /// Constructed by nothing yet (see module doc) -- the seam the
    /// cutover's policy-RPC lands on.
    PolicyChanged {
        peer_id: String,
    },
    /// `_maybe_wake_for_vegas_spike` (py 4386-4411): the edge-triggered
    /// Vegas-spike wake, sent by the trigger loop between full cycles.
    VegasSpikeCheck,
    /// `wake_all_sleeping_channels` (py 4295-4384): wake every sleeping
    /// channel. Fire-and-forget (see module doc) -- the manual
    /// `revenue-r-fee-wake` RPC's trigger.
    WakeAll,
    /// Task 66 canonical `revenue-wake-all`: the same owner-thread mutation,
    /// but with a typed reply sent only after the sleeping-channel state was
    /// changed. Queue admission alone is never reported as success.
    WakeAllWithReply(tokio::sync::oneshot::Sender<FeeWakeCompletion>),
    /// `record_failed_forward`'s scheduler-facing hook (py
    /// `fee_controller.py:9179`): a fee-relevant failed forward on the
    /// OUTGOING `channel_id`.
    ///
    /// Task 44 (2026-07-27) wired the EFFECT that Task 6 deferred. The
    /// payload carries everything the nudge needs, including
    /// `event_ts` -- the notification's OWN timestamp. That matters: the
    /// effect is applied on the owner thread when this message is
    /// dispatched, not on the notification thread as Python does under
    /// `_state_lock`, and both cooldown windows below must still be
    /// measured from when the forward actually failed. Using the dispatch
    /// clock instead would silently widen them.
    FailedForward(Box<FailedForwardSignal>),
    /// Fix round 1 (review finding 2): CLN's own `forward_event`
    /// notification (`main.rs`'s subscription) offering `channel_id` to
    /// the trigger queue -- recording-only, same "handler is real, effect
    /// is future work" posture [`CycleMsg::FailedForward`] already
    /// carries (see `CycleOwner::handle_forward_event`). Wired
    /// ALONGSIDE, not in place of, the existing `notify::on_forward_event`
    /// dedup-insert.
    ForwardEvent {
        channel_id: String,
    },
    /// Task 44 / A3: a fully-prepared new-channel initial-fee decision,
    /// frozen by the async preparation stage (contract §3.1 stage 2/3).
    /// The owner offers a channel-scoped trigger to the bounded/coalescing
    /// discipline BEFORE applying any effect -- a dropped occurrence must
    /// never mutate state or create a would-broadcast action.
    NewChannel(Box<NewChannelPreparation>),
    /// Task 44 / A3 (live-review finding F5): an off-owner store
    /// operation for the new-channel path completed; its result is routed
    /// back onto the owner's OWN queue so the owner never blocks on a
    /// store (SQLite-actor) reply. Because both new events and results
    /// arrive as messages on the same single-consumer loop, the pending
    /// map is the only synchronization the path needs.
    InitialFeeStoreResult(InitialFeeStoreResult),
    /// A `revenue-r-fee-debug` query; the owner thread answers over the
    /// included reply channel without ever blocking on IO.
    Query(FeeDebugQuery, std_mpsc::Sender<serde_json::Value>),
    Shutdown,
}

/// Task 44 / A3, live-review finding F5: one off-owner store operation's
/// result. `Idempotency`/`Commit` are BOUND to both the event identity
/// (`event_key`, the stable content-derived key) and the owner's own
/// dispatch `generation` for that pending occurrence -- the owner
/// installs/acts ONLY when both match its pending entry; any mismatch is
/// a fail-closed conflict (counted red, never a silent install).
#[derive(Debug)]
pub enum InitialFeeStoreResult {
    /// `dispatch_cycle_exists_with_generation`'s answer for a pending
    /// occurrence: (already-committed?, current state generation) -- the
    /// generation is the basis the coming decision's guarded commit will
    /// CAS against (F7).
    Idempotency {
        channel_id: String,
        event_key: String,
        generation: u64,
        result: Result<(bool, u64), String>,
    },
    /// `dispatch_commit_fee_cycle_guarded`'s answer for a pending
    /// occurrence (F7): `Committed` or an in-band `GenerationConflict`
    /// (the store advanced past the decision's basis; nothing was
    /// written).
    Commit {
        channel_id: String,
        event_key: String,
        generation: u64,
        result: Result<revops_db::fee_runway::GuardedCommitOutcome, String>,
    },
    /// `dispatch_record_trigger_event`'s answer for a stand-alone A3
    /// receipt (a refusal/dropped/coalesced/duplicate occurrence -- no
    /// staged state depends on it). A failure is loud and counted; there
    /// is nothing to roll back.
    Receipt {
        context: String,
        result: Result<(), String>,
    },
    /// Task 59: `dispatch_run_retention_sweep`'s answer. Success carries
    /// the owner's next fairness cursor; a failure increments the
    /// never-reset red retention counter. Nothing staged depends on it.
    Retention {
        result: Result<revops_db::retention::RetentionReport, String>,
    },
}

/// Result-only callback capability for A3 store completions. Unlike
/// `SchedulerIngress`, this cannot receive wake, query, cycle, notification,
/// or any future action-bearing owner messages.
#[derive(Clone)]
struct InitialFeeResultSink {
    deliver: Arc<dyn Fn(InitialFeeStoreResult) -> bool + Send + Sync + 'static>,
}

impl InitialFeeResultSink {
    fn new(deliver: impl Fn(InitialFeeStoreResult) -> bool + Send + Sync + 'static) -> Self {
        Self {
            deliver: Arc::new(deliver),
        }
    }

    fn deliver(&self, result: InitialFeeStoreResult) -> bool {
        (self.deliver)(result)
    }

    fn scheduler(ingress: SchedulerIngress) -> Self {
        Self::new(move |result| {
            ingress
                .blocking_send(CycleMsg::InitialFeeStoreResult(result))
                .is_ok()
        })
    }
}

/// Safe integration-test receiver for A3 store results. The raw
/// `Receiver<CycleMsg>` stays private and any non-result message is rejected.
pub struct A3ResultReceiver {
    rx: std::sync::Mutex<tokio_mpsc::Receiver<CycleMsg>>,
}

impl A3ResultReceiver {
    pub fn try_recv(&self) -> Result<InitialFeeStoreResult, tokio_mpsc::error::TryRecvError> {
        match self
            .rx
            .lock()
            .expect("A3 result receiver poisoned")
            .try_recv()?
        {
            CycleMsg::InitialFeeStoreResult(result) => Ok(result),
            _ => panic!("A3 result receiver observed a non-result owner message"),
        }
    }
}

/// One in-flight A3 occurrence's owner-side bookkeeping, keyed by
/// channel_id in `CycleOwner::pending_initial_fees`. While an entry
/// exists for a channel, further A3 events for that channel are refused
/// fail-closed (live-review finding F5's same-channel race rule).
enum PendingInitialFee {
    /// Waiting for the off-owner `dispatch_cycle_exists` answer; the
    /// frozen preparation is parked here -- NO decision or RNG draw has
    /// happened yet.
    CheckingIdempotency {
        event_key: String,
        generation: u64,
        prepared: Box<PreparedInitialFee>,
        /// Task 59 §3.6: wall-clock stamp of this phase's dispatch, for
        /// the `oldest_pending_age_seconds` diagnostic. Visibility only
        /// -- nothing may ever cancel on it.
        dispatched_at: i64,
    },
    /// Waiting for the off-owner `dispatch_commit_fee_cycle_guarded`
    /// answer; `staged` (clones -- nothing is installed yet) installs
    /// ONLY on a successful, identity-matched `Committed` result whose
    /// generation is exactly `expected_prior_generation + 1` (F7).
    Committing {
        event_key: String,
        generation: u64,
        /// The state generation the decision was computed against -- the
        /// guarded commit's CAS basis.
        expected_prior_generation: u64,
        staged: Option<Box<(ChannelFeeState, ChannelCycleState)>>,
        /// Task 59 §3.6: wall-clock stamp of this phase's dispatch (each
        /// phase re-stamps; the long waits live within a phase).
        dispatched_at: i64,
    },
}

impl PendingInitialFee {
    fn event_key(&self) -> &str {
        match self {
            PendingInitialFee::CheckingIdempotency { event_key, .. }
            | PendingInitialFee::Committing { event_key, .. } => event_key,
        }
    }

    fn generation(&self) -> u64 {
        match self {
            PendingInitialFee::CheckingIdempotency { generation, .. }
            | PendingInitialFee::Committing { generation, .. } => *generation,
        }
    }

    fn dispatched_at(&self) -> i64 {
        match self {
            PendingInitialFee::CheckingIdempotency { dispatched_at, .. }
            | PendingInitialFee::Committing { dispatched_at, .. } => *dispatched_at,
        }
    }

    fn backdate(&mut self, seconds: i64) {
        match self {
            PendingInitialFee::CheckingIdempotency { dispatched_at, .. }
            | PendingInitialFee::Committing { dispatched_at, .. } => {
                *dispatched_at -= seconds;
            }
        }
    }
}

/// The async half's per-cycle output: everything the owner thread needs
/// to run one cycle without performing any IO of its own besides the
/// read-only evidence snapshot.
pub struct PreparedCycle {
    /// T1: freshly resolved 22-field snapshot (per cycle, so DB overrides
    /// written by Python's `revenue-config set` stay visible).
    pub cfg: FeeCfgSnapshot,
    /// The typed per-cycle resolution of `neighbor_median_min_competitors`
    /// (NOT a `FeeCfgSnapshot` field) -- validated by
    /// [`fee_config::resolve_min_competitors`] in [`CycleOwner::run_cycle`]
    /// before the cycle proceeds (Phase 4b Task 8a).
    pub min_competitors: serde_json::Value,
    /// T2: the cycle's frozen RPC prefetch.
    pub rpc: RpcPrefetch,
}

/// Async half of one cycle (runs on the tokio side, BEFORE the cycle
/// starts): resolve config + the min-competitors gate value, then
/// prefetch every RPC snapshot. An `Err` (getinfo/listpeerchannels/
/// listchannels failure) means the cycle is skipped -- the owner never
/// runs on evidence Python didn't run on.
pub async fn prepare_cycle(
    socket_path: &Path,
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, OptValue>,
) -> anyhow::Result<PreparedCycle> {
    let cfg = fee_config::resolve_fee_cfg(db, python_option_values).await;
    let min_competitors =
        fee_config::resolve_neighbor_median_min_competitors(db, python_option_values).await;
    let rpc = prefetch_rpc(socket_path).await?;
    Ok(PreparedCycle {
        cfg,
        min_competitors,
        rpc,
    })
}

/// Task 44 / A3's async preparation half (contract §3.1 stage 2): resolve
/// the canonical NORMAL channel, the peer's policy, the current fee
/// config, and the optional gossip-derived prior -- ALL off the owner
/// thread, using a fresh (uncached) RPC prefetch. Live-review finding F1:
/// ALWAYS returns a value the caller sends to the owner -- a refusal
/// (timeout, malformed evidence, ambiguity, missing peer/channel, or a
/// policy/config read failure) is [`NewChannelPreparation::Refused`], not
/// a swallowed `None`, so it becomes a durable receipt rather than only a
/// log line (contract §4.1 test 4: the owner effect is "a refusal
/// receipt, no decision/state/action", not "nothing at all"). No RNG draw
/// happens here.
pub async fn prepare_new_channel(
    socket_path: &Path,
    db_path: &Path,
    db: Option<&DbHandle>,
    python_options: &PythonOptionCache,
    signal: crate::notify::NewChannelSignal,
) -> NewChannelPreparation {
    let peer_id = signal.peer_id.clone();
    let now = signal.event_ts;
    let channel_hint = signal
        .event_scid
        .clone()
        .or_else(|| signal.event_channel_id.clone())
        .unwrap_or_default();
    let refused = |reason: String| NewChannelPreparation::Refused {
        peer_id: peer_id.clone(),
        channel_hint: channel_hint.clone(),
        event_ts: now,
        reason,
    };

    // Live-review finding F8 (A3 config freshness): refresh `listconfigs`
    // NOW, before any evidence is frozen, so a dynamic `setconfig` since
    // the last scheduled cycle reaches this decision (Python's handler
    // reads config the cycle's `_refresh_dynamic_config` keeps current).
    // A3 is STRICT: a failed refresh REFUSES -- even though a stale
    // cached snapshot exists -- because an initial fee decided on stale
    // config is a silently wrong decision, not a degraded one. The shared
    // scheduled-cycle path (`dispatch_cycle`) keeps its keep-last-good
    // posture; only this caller is strict.
    if !python_options.refresh(socket_path).await {
        let reason = "config refresh (listconfigs) failed; refusing rather than deciding on a \
                      stale cached config snapshot"
            .to_string();
        eprintln!("revops: A3 new-channel prep REFUSED for peer {peer_id}: {reason}");
        return refused(reason);
    }
    let python_option_values = &python_options.snapshot();

    let rpc = match prefetch_rpc(socket_path).await {
        Ok(rpc) => rpc,
        Err(e) => {
            let reason = format!("RPC prefetch (listpeerchannels/listchannels) failed: {e:#}");
            eprintln!("revops: A3 new-channel prep REFUSED for peer {peer_id}: {reason}");
            return refused(reason);
        }
    };

    let channel = match crate::fee_evidence::resolve_new_channel(
        &rpc.peer_channels,
        signal.event_scid.as_deref(),
        signal.event_channel_id.as_deref(),
        &peer_id,
    ) {
        crate::fee_evidence::ChannelResolution::Resolved(info) => *info,
        crate::fee_evidence::ChannelResolution::Ambiguous => {
            let reason = "AMBIGUOUS: multiple NORMAL channels, no exact identifier match \
                          (refusing rather than guessing)"
                .to_string();
            eprintln!("revops: A3 new-channel prep REFUSED for peer {peer_id}: {reason}");
            return refused(reason);
        }
        crate::fee_evidence::ChannelResolution::NotFound => {
            let reason = format!(
                "NOT FOUND: no matching/single-fallback NORMAL channel (scid={:?} \
                 channel_id={:?})",
                signal.event_scid, signal.event_channel_id
            );
            eprintln!("revops: A3 new-channel prep REFUSED for peer {peer_id}: {reason}");
            return refused(reason);
        }
    };

    let policy = match crate::fee_evidence::resolve_peer_policy_async(
        db_path.to_path_buf(),
        peer_id.clone(),
        now,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            let reason = format!("policy read failed: {e:#}");
            eprintln!("revops: A3 new-channel prep REFUSED for peer {peer_id}: {reason}");
            return refused(reason);
        }
    };

    // Live-review finding F6 (config half): the A3 path is STRICT --
    // any config-store QUERY failure (not a legitimately absent override
    // row) refuses the preparation rather than deciding on struct
    // defaults. The shared per-cycle `resolve_fee_cfg` deliberately keeps
    // its log-and-default posture; only this caller observes and refuses.
    let resolution = fee_config::resolve_fee_cfg_observed(db, python_option_values).await;
    if resolution.db_query_failures > 0 {
        let reason = format!(
            "config resolution failed: {} config-store override query failure(s); refusing \
             rather than deciding on defaults",
            resolution.db_query_failures
        );
        eprintln!("revops: A3 new-channel prep REFUSED for peer {peer_id}: {reason}");
        return refused(reason);
    }
    let cfg = resolution.cfg;

    let peer_gossip = crate::fee_evidence::peer_own_gossip_channels(&rpc.gossip_channels, &peer_id);
    let candidates: Vec<FeePrior> = revops_fees::market::network_fee_prior(&peer_gossip)
        .into_iter()
        .collect();
    let prior = revops_fees::market::select_best_fee_prior(&candidates);

    let event_key = new_channel_event_key(
        &channel.channel_id,
        &signal.old_state,
        &signal.new_state,
        now,
    );

    NewChannelPreparation::Ready(Box::new(PreparedInitialFee {
        channel,
        peer_id,
        policy,
        cfg,
        prior,
        event_ts: now,
        event_key,
    }))
}

/// Task 42 correction F2: where the SeedOnce bootstrap stands. No
/// out-of-cycle generation-advancing commit (A3 new-channel,
/// failed-forward nudge) may run unless `Ready` — anything else could
/// make a virgin store nonvirgin with partial state (before hydration)
/// or take the generation the pending seed provenance is bound to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedOnceBootstrapState {
    /// SeedOnce, first hydration has not happened this process lifetime.
    NotStarted,
    /// Hydration seeded from Python; provenance awaits its atomic
    /// generation-1 commit.
    PendingSeedCommit,
    /// The one-time seed was refused; the owner stays passive.
    Refused,
    /// Bootstrap complete (or lifecycle is strict-replay, which has no
    /// bootstrap).
    Ready,
}

/// What one `run_cycle` call did -- the loud-logging skip taxonomy the
/// per-cycle sequence requires (skips log, never panic: the hub
/// precedent).
#[derive(Debug, PartialEq, Eq)]
pub enum CycleOutcome {
    /// Ran to completion; `decisions` FeeDecision lines appended.
    Ran {
        decisions: usize,
        adjusted_channels: usize,
    },
    /// Fail-closed rule (Phase 4b Task 8a): `neighbor_median_min_competitors`
    /// resolved to something unusable -- missing, non-integer, or
    /// non-positive. Any resolvable positive integer (2, 3, or otherwise)
    /// now proceeds; this variant is for genuinely unresolvable values
    /// only.
    SkippedMinCompetitors,
    /// `build_evidence_snapshot` failed (DB open/read error).
    SkippedEvidence,
    /// A replayable clock, entropy, or authorizer input failed. The cycle
    /// stops before journaling any partial decision set.
    SkippedDecisionInput,
    /// `SeedOnce` fail-closed (Task 5): the Rust-owned state was
    /// unavailable -- no store configured, the store load failed, a
    /// recorded generation's rows were corrupt/missing (NEVER reseeded
    /// from Python), or the one-time seed was refused. No cycle runs.
    SkippedStateUnavailable,
    /// `SeedOnce` fail-closed (Task 42): the combined autonomous
    /// mempool-evidence refresh (insert current sample + prune + read
    /// the Rust-only aggregate) failed at the store. Distinct from
    /// [`CycleOutcome::SkippedEvidence`] (the read-only snapshot) and
    /// NEVER degraded to "no fresh evidence": a store that cannot answer
    /// must stop the cycle BEFORE hydration, not let it proceed on an
    /// empty window.
    SkippedAutonomousEvidence,
    /// `SeedOnce`: the cycle ran but the atomic Rust-owned commit failed.
    /// The generation did not advance and the red
    /// [`CycleOwner::persistence_failures`] counter was incremented.
    PersistenceFailed,
}

/// Human-readable WHY for the `FixedInterval` trigger receipt (Task 6
/// step 4: "shadow records WHY each trigger did or did not produce a
/// cycle").
fn describe_cycle_outcome(outcome: &CycleOutcome) -> String {
    match outcome {
        CycleOutcome::Ran { decisions, .. } => format!("ran: {decisions} decision(s)"),
        CycleOutcome::SkippedMinCompetitors => {
            "skipped: neighbor_median_min_competitors unresolvable".to_string()
        }
        CycleOutcome::SkippedEvidence => "skipped: evidence snapshot failed".to_string(),
        CycleOutcome::SkippedDecisionInput => {
            "skipped: replayable decision input failed".to_string()
        }
        CycleOutcome::SkippedStateUnavailable => {
            "skipped: SeedOnce state unavailable (fail-closed)".to_string()
        }
        CycleOutcome::SkippedAutonomousEvidence => {
            "skipped: autonomous mempool-evidence refresh failed (fail-closed, before              hydration)"
                .to_string()
        }
        CycleOutcome::PersistenceFailed => {
            "ran but PersistenceFailed: atomic commit failed, generation not advanced".to_string()
        }
    }
}

fn cycle_completion(
    owner: &CycleOwner,
    outcome: &CycleOutcome,
) -> Result<FeeCycleCompletion, String> {
    match outcome {
        CycleOutcome::Ran {
            adjusted_channels, ..
        } => Ok(FeeCycleCompletion {
            adjusted_channels: *adjusted_channels,
            generation: owner.state_generation,
            completed_at: owner.last_cycle_at.unwrap_or_default(),
            fee_debug: owner.fee_debug(&FeeDebugQuery::Summary),
        }),
        other => Err(describe_cycle_outcome(other)),
    }
}

// ---------------------------------------------------------------------------
// Task 44 / A3: new-channel initial fee -- prepared message + pure decision
// ---------------------------------------------------------------------------

/// The async preparation stage's typed result (live-review finding F1):
/// EVERY occurrence -- ready or refused -- is sent to the owner and
/// produces a durable receipt. A refusal is never just a log line; it is
/// auditable evidence that this event was seen and why it went no
/// further.
#[derive(Debug, Clone, PartialEq)]
pub enum NewChannelPreparation {
    Ready(Box<PreparedInitialFee>),
    /// Timeout, malformed RPC evidence, ambiguous/missing channel
    /// resolution, or a policy/config read failure. `channel_hint` is the
    /// best-effort identifier for the receipt (the raw event scid/
    /// channel_id -- never guessed/resolved, since resolution is exactly
    /// what failed).
    Refused {
        peer_id: String,
        channel_hint: String,
        event_ts: i64,
        reason: String,
    },
}

/// Frozen evidence for one new-channel initial-fee decision, produced by
/// the async preparation stage (contract §3.1 stage 2) and handed to the
/// owner thread as [`CycleMsg::NewChannel`]. Everything here is already
/// resolved: no RPC, no DB read, no RNG draw happens after this point.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedInitialFee {
    pub channel: ChannelInfo,
    pub peer_id: String,
    pub policy: PeerPolicy,
    pub cfg: FeeCfgSnapshot,
    /// `_select_best_fee_prior` (py 7924-7948): `None` when no gossip-
    /// derived prior exists for this peer.
    pub prior: Option<FeePrior>,
    /// The notification's own clock read -- travels through to the DTS
    /// nudge timestamp and the modeled broadcast-sync epoch. NEVER the
    /// drain/dispatch clock.
    pub event_ts: i64,
    /// Live-review finding F3: a STABLE identity for this exact
    /// opening-to-NORMAL event, derived ONLY from the event's own content
    /// (resolved channel id, the old->new state transition, and the
    /// event's own timestamp) -- NEVER wall-clock-at-processing or a
    /// process id. The SAME notification replayed after a restart
    /// recomputes this SAME key, which [`CycleOwner::handle_new_channel`]
    /// uses both as an explicit pre-decision idempotency check and as the
    /// atomic commit's `cycle_id` (a `PRIMARY KEY`, so even a raced
    /// duplicate is rejected by the transaction itself).
    pub event_key: String,
}

/// Build the stable, content-only event identity (contract finding F3).
/// `resolved_channel_id` is the CANONICAL post-resolution id (not the raw
/// event scid/channel_id) so a duplicate delivery that resolves through a
/// different raw identifier for the SAME channel still collapses to the
/// SAME key.
pub fn new_channel_event_key(
    resolved_channel_id: &str,
    old_state: &str,
    new_state: &str,
    event_ts: i64,
) -> String {
    format!("rust-a3-{resolved_channel_id}-{old_state}-{new_state}-{event_ts}")
}

/// The typed terminal outcome of one new-channel initial-fee decision
/// (contract §3.1 stage 3's outcome taxonomy). Receipt text must be
/// derived from this, never from message receipt alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitialFeeOutcome {
    /// PASSIVE policy: no fee action, no DTS state creation or seed.
    Passive,
    /// A prepared action exists but the governor did not authorize it. A
    /// gossip-derived prior seed may still be present in `fee_state`
    /// (Python seeds before authorization runs).
    GovernorDenied { reason_code: String },
    /// The governor authorized the request; `action` carries the exact
    /// broadcast parameters that WOULD be sent (shadow mode never sends
    /// them).
    WouldBroadcast { reason_code: String },
}

/// The pure result of [`decide_initial_fee`]: candidate persistent state
/// (staged, not yet installed -- the caller installs only after a
/// successful atomic commit) plus the typed outcome.
#[derive(Debug, Clone, PartialEq)]
pub struct InitialFeeDecision {
    /// `Some` exactly when persistent state changed (a prior seed/nudge,
    /// or a successful-broadcast state sync) -- `None` means nothing about
    /// this channel's persistent state needs to be written at all (e.g.
    /// PASSIVE, or a DYNAMIC decision with no gossip prior that was then
    /// governor-denied).
    pub fee_state: Option<ChannelFeeState>,
    pub cycle_state: Option<ChannelCycleState>,
    pub action: Option<PreparedFeeAction>,
    pub governed_trace: Option<GovernedTrace>,
    pub outcome: InitialFeeOutcome,
    /// `"channel_open"` | `"policy_static"` -- the exact reason code
    /// (contract §4.2 test 12).
    pub reason_code: &'static str,
}

/// `_set_initial_fee_authorized`'s decision core (py 8617-8772), as a pure
/// function: policy precedence, the throwaway/persistent DTS split, the
/// existing execution clamp, and governed authorization. Draws from `rng`
/// ONLY on the DYNAMIC path (never PASSIVE, never STATIC-with-target) --
/// the ONE long-lived `PyRandom` the owner thread holds. `existing_fee`/
/// `existing_cycle` are the channel's CURRENT persistent rows, if any
/// (`None` for a brand-new channel, the overwhelmingly common case).
pub fn decide_initial_fee(
    prepared: &PreparedInitialFee,
    existing_fee: Option<&ChannelFeeState>,
    existing_cycle: Option<&ChannelCycleState>,
    authorizer: &dyn FeeAuthorizer,
    rng: &mut dyn DecisionEntropy,
) -> InitialFeeDecision {
    let now = prepared.event_ts;

    if prepared.policy.strategy == FeeStrategy::Passive {
        return InitialFeeDecision {
            fee_state: None,
            cycle_state: None,
            action: None,
            governed_trace: None,
            outcome: InitialFeeOutcome::Passive,
            reason_code: "policy_passive",
        };
    }

    let (target, reason, reason_code, seeded_fee_state, seeded_cycle_state) =
        if prepared.policy.strategy == FeeStrategy::Static {
            if let Some(fee_ppm_target) = prepared.policy.fee_ppm_target {
                (
                    fee_ppm_target,
                    "Initial fee: STATIC policy",
                    "policy_static",
                    existing_fee.cloned(),
                    existing_cycle.cloned(),
                )
            } else {
                dynamic_initial_fee(prepared, existing_fee, existing_cycle, rng)
            }
        } else {
            dynamic_initial_fee(prepared, existing_fee, existing_cycle, rng)
        };

    finalize_initial_fee(
        prepared,
        target,
        reason,
        reason_code,
        seeded_fee_state,
        seeded_cycle_state,
        authorizer,
        now,
    )
}

/// The DYNAMIC arm (py 8711-8765): the throwaway/persistent DTS split.
/// Returns the sampled target plus the (possibly gossip-seeded) candidate
/// persistent rows -- `existing_fee`/`existing_cycle` UNCHANGED (cloned
/// verbatim) when there is no gossip prior, matching contract §4.2 test 9
/// ("no prior seed/nudge is created before action handling").
fn dynamic_initial_fee(
    prepared: &PreparedInitialFee,
    existing_fee: Option<&ChannelFeeState>,
    existing_cycle: Option<&ChannelCycleState>,
    rng: &mut dyn DecisionEntropy,
) -> (
    i64,
    &'static str,
    &'static str,
    Option<ChannelFeeState>,
    Option<ChannelCycleState>,
) {
    let now = prepared.event_ts;

    // A FRESH, throwaway state for the initial sample -- never the
    // channel's persistent state (contract §2.2, test 11: sampling the
    // newly nudged persistent state would apply bias Python's initial
    // draw never sees).
    let mut throwaway = GaussianThompsonState {
        prior_std_fee: prepared.cfg.thompson_prior_std_fee as f64,
        ..GaussianThompsonState::default()
    };

    let (seeded_fee_state, seeded_cycle_state) = if let Some(prior) = &prepared.prior {
        throwaway.prior_mean_fee = prior.mean as f64;
        throwaway.prior_std_fee = prior.std as f64;

        // The SEPARATE persistent state: create-or-load, seed the SAME
        // prior mean/std, and record ONE durable nudge at the event
        // timestamp (never drain time) -- py 8726-8745.
        let mut fee_state = existing_fee.cloned().unwrap_or_default();
        fee_state.thompson.prior_mean_fee = prior.mean as f64;
        fee_state.thompson.prior_std_fee = prior.std as f64;
        dynamics::record_posterior_nudge(
            &mut fee_state.thompson,
            prior.mean as f64,
            INITIAL_PRIOR_NUDGE_WEIGHT,
            now,
        );
        // `build_cycle_commit`/the generic serializer only persists a
        // channel present in BOTH maps (fee_scheduler.rs:1325-1339) -- a
        // fresh cycle-state row is required for this seed to be durable,
        // even though Python's own seed touches only `ChannelFeeState`.
        let cycle_state = existing_cycle.cloned().unwrap_or_default();
        (Some(fee_state), Some(cycle_state))
    } else {
        (None, None)
    };

    // Sample the THROWAWAY state, not the persistent one (test 11). Uses
    // the entropy-fallible form (not the panicking `sample_fee`
    // convenience wrapper) so this function can accept the injected
    // `&mut dyn DecisionEntropy` a test's counting/fake stream provides;
    // the owner's real `PyRandom` stream never returns an error for a
    // static, non-empty label, matching `sample_fee`'s own doc rationale.
    let sampled_fee = revops_fees::thompson::sampling::sample_fee_with_entropy(
        &mut throwaway,
        prepared.cfg.min_fee_ppm,
        prepared.cfg.max_fee_ppm,
        None,
        rng,
        now,
    )
    .expect("PyRandom (or a test double honoring the same label contract) with a static, non-empty label cannot fail");

    (
        sampled_fee,
        "Initial fee: channel open",
        "channel_open",
        seeded_fee_state,
        seeded_cycle_state,
    )
}

/// The shared `set_channel_fee` boundary (py 8173-8524, execution-layer
/// slice): the existing pure clamp, then governed authorization. On
/// authorization, models Python's successful state synchronization
/// (`last_fee_ppm`/`last_broadcast_fee_ppm`/`last_broadcast_at`/
/// `last_update`, all at the event timestamp) so the next scheduled cycle
/// observes the same waiting-window posture Python's real apply would
/// leave behind. On denial, the (possibly prior-seeded) candidate state is
/// returned UNCHANGED by the sync fields -- a gossip seed persists, but no
/// action and no post-broadcast sync (contract §3.3).
#[allow(clippy::too_many_arguments)]
fn finalize_initial_fee(
    prepared: &PreparedInitialFee,
    target_fee_ppm: i64,
    reason: &'static str,
    reason_code: &'static str,
    seeded_fee_state: Option<ChannelFeeState>,
    seeded_cycle_state: Option<ChannelCycleState>,
    authorizer: &dyn FeeAuthorizer,
    now: i64,
) -> InitialFeeDecision {
    // py `_set_channel_fee_inner` (fee_controller.py:8352): the pre-action
    // fee is ALWAYS read from the live CLN-announced policy
    // (`channel_info["fee_proportional_millionths"]`), never from
    // Rust-owned Thompson/posterior state. A brand-new channel is not at 0
    // ppm -- it carries CLN's default policy (or whatever `fundchannel`
    // set) -- and that is what the governor request and the post-decision
    // bookkeeping must see. Persisted state is Thompson/posterior evidence
    // only, never a fee-delta source.
    let old_fee_ppm = prepared.channel.fee_proportional_millionths;

    // Policy is already resolved by the caller (PASSIVE returned early;
    // STATIC/DYNAMIC already picked `target_fee_ppm`) -- pass `None` here
    // so the shared clamp kernel does not re-apply policy precedence.
    let req = SetFeeRequest {
        channel_id: prepared.channel.channel_id.clone(),
        fee_ppm: target_fee_ppm,
        enforce_limits: true,
        effective_min_fee_ppm: None,
        htlcmin_msat: None,
        htlcmax_msat: None,
        base_fee_msat: prepared.cfg.base_fee_msat,
    };

    let auth_request = FeeAuthorizationRequest {
        channel_id: prepared.channel.channel_id.clone(),
        // py 8415: the governor sees the CLAMPED target, computed against
        // `req.fee_ppm` -- resolve it via the SAME pure kernel the
        // executor below uses, so the governor and the executor never
        // disagree about what fee is being authorized.
        fee_ppm: decide_set_channel_fee(&req, &prepared.cfg, None).clamped_fee_ppm,
        old_fee_ppm: Some(old_fee_ppm),
        reason: reason.to_string(),
        reason_code: Some(reason_code.to_string()),
        now,
    };
    let auth = authorizer.authorize(&auth_request).unwrap_or_else(|e| {
        revops_fees::execution::FeeAuthorizationResult {
            authorized: false,
            reason_code: format!("internal_error ({e})"),
            trace: None,
        }
    });

    // Reuse the SAME capability-free `RecordingFeeExecutor` shadow mode
    // already uses per-cycle (contract §5: "prefer reusing... unchanged")
    // -- it owns the one broadcast-request-typed construction path
    // (`execution.rs`, the action_surface allowlisted module), so this
    // caller never names that type directly.
    let exec_request = revops_fees::execution::FeeExecutionRequest {
        decision: req,
        wire_request: PyOValue::Null,
        authorized: auth.authorized,
        old_fee_ppm,
        expected_base_fee_msat: prepared.cfg.base_fee_msat,
    };
    let recorder = RecordingFeeExecutor::default();
    let decision = match recorder.execute(&exec_request, &prepared.cfg, None) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("revops: A3 initial-fee execution kernel failed: {e}");
            return InitialFeeDecision {
                fee_state: seeded_fee_state,
                cycle_state: seeded_cycle_state,
                action: None,
                governed_trace: auth.trace,
                outcome: InitialFeeOutcome::GovernorDenied {
                    reason_code: format!("internal_error ({e})"),
                },
                reason_code,
            };
        }
    };

    if !auth.authorized {
        return InitialFeeDecision {
            fee_state: seeded_fee_state,
            cycle_state: seeded_cycle_state,
            action: None,
            governed_trace: auth.trace,
            outcome: InitialFeeOutcome::GovernorDenied {
                reason_code: auth.reason_code,
            },
            reason_code,
        };
    }

    let action = recorder.recorded_actions().pop();

    // Model Python's successful synchronization (py 8446-8458, 8484-8524):
    // both state objects observe the SAME applied fee at the SAME event
    // timestamp.
    let mut fee_state = seeded_fee_state.unwrap_or_default();
    fee_state.last_fee_ppm = decision.clamped_fee_ppm;
    fee_state.last_broadcast_fee_ppm = decision.clamped_fee_ppm;
    fee_state.set_last_broadcast_at(now);
    fee_state.last_update = now;

    let mut cycle_state = seeded_cycle_state.unwrap_or_default();
    cycle_state.last_fee_ppm = decision.clamped_fee_ppm;
    cycle_state.last_broadcast_fee_ppm = decision.clamped_fee_ppm;
    cycle_state.set_last_broadcast_at(now);
    cycle_state.last_update = now;

    InitialFeeDecision {
        fee_state: Some(fee_state),
        cycle_state: Some(cycle_state),
        action,
        governed_trace: auth.trace,
        outcome: InitialFeeOutcome::WouldBroadcast {
            reason_code: auth.reason_code,
        },
        reason_code,
    }
}

/// The single owner of `ControllerState` + the ONE long-lived `PyRandom`.
/// Lives on the dedicated cycle thread for the plugin's whole lifetime;
/// tests drive it synchronously.
/// One fee-relevant failed forward, as delivered to the owner thread.
///
/// `channel_id` is the OUTGOING channel and nothing else (py audit DTS-4a):
/// per BOLT 7 the fee a sender pays to traverse this node is OUR policy on
/// the out channel, so an in-channel failure is evidence about the PEER's
/// fee, not ours.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailedForwardSignal {
    pub channel_id: String,
    /// The INCOMING amount of the failed forward (py `failed_in_msat`),
    /// which drives the log10 weight boost.
    pub amount_msat: i64,
    pub failcode: Option<i64>,
    pub failreason: Option<String>,
    /// The notification's own clock read -- see [`CycleMsg::FailedForward`].
    pub event_ts: i64,
}

/// Stable content identity for one failed-forward effect. Length-prefixed
/// fields keep optional/free-text boundaries unambiguous; SHA-256 keeps the
/// database cycle key bounded even when CLN supplies a long failreason.
pub fn failed_forward_event_key(signal: &FailedForwardSignal) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    let mut field = |bytes: &[u8]| {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    };
    field(signal.channel_id.as_bytes());
    field(&signal.amount_msat.to_be_bytes());
    match signal.failcode {
        Some(code) => {
            field(&[1]);
            field(&code.to_be_bytes());
        }
        None => field(&[0]),
    }
    match signal.failreason.as_deref() {
        Some(reason) => {
            field(&[1]);
            field(reason.as_bytes());
        }
        None => field(&[0]),
    }
    field(&signal.event_ts.to_be_bytes());

    let digest = hasher.finalize();
    let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("rust-a1-{hex}")
}

/// A failure nudge evaluated against cloned state. The owner installs the
/// clone only after the state row and effect receipt commit atomically.
struct StagedFailureNudge {
    outcome: NudgeOutcome,
    fee_state: Option<ChannelFeeState>,
}
/// What [`CycleOwner::stage_failure_nudge`] decided, so the trigger
/// receipt records the truth rather than a hopeful summary. Every skip
/// names the guard that fired.
#[derive(Debug, Clone, PartialEq)]
pub enum NudgeOutcome {
    Applied { implied_fee: i64, weight: f64 },
    Skipped(&'static str),
}

/// py `FeeController.FAILURE_NUDGE_GOSSIP_SETTLE_SECONDS` (2635): after we
/// apply a fee, gossip needs time to propagate, so failures inside this
/// window are still being routed against the OLD fee and are not evidence
/// about the new one (audit SL-2).
pub const FAILURE_NUDGE_GOSSIP_SETTLE_SECONDS: i64 = 3600;

/// py `FeeController.FAILURE_NUDGE_MIN_INTERVAL_SECONDS` (2636): at most one
/// nudge per channel per window, so a burst of failures from one payment
/// attempt cannot stack into a large synthetic signal (audit SL-2).
pub const FAILURE_NUDGE_MIN_INTERVAL_SECONDS: i64 = 1800;

pub struct CycleOwner {
    state: ControllerState,
    /// py `_last_fee_apply_ts` (3011, set at 8456): OUR OWN successful fee
    /// applications, keyed by channel. Deliberately an in-memory map that
    /// starts empty on restart, exactly as Python's does -- NOT the
    /// persisted `last_broadcast_at`, which would also count Python's
    /// applications and silently suppress nudges Python would have taken.
    /// Parity here beats robustness: diverging is how the decision surface
    /// drifts.
    last_fee_apply_ts: HashMap<String, i64>,
    /// py `_last_failure_nudge_ts` (3011): when this channel last took a
    /// failure nudge, for the per-window rate limit.
    last_failure_nudge_ts: HashMap<String, i64>,
    // Task 53: channels whose pending trigger already committed one nudge.
    // Cleared exactly when the bounded trigger queue drains for a cycle.
    applied_failure_nudges_pending: HashSet<String>,
    /// Task 44 / A2: last seen `peer_policies.updated_at` per peer, so a
    /// change can be detected whoever made it (see
    /// [`Self::detect_policy_changes`]).
    last_policy_updated_at: HashMap<String, i64>,
    /// Seeded exactly ONCE, in [`CycleOwner::new`] (production: from
    /// `now_unix()` at spawn). Never reseeded -- every cycle continues
    /// this one stream, mirroring Python's module-level `random` instance.
    rng: PyRandom,
    lifecycle: StateLifecycle,
    /// `SeedOnce` only: whether the one-time hydration has happened.
    hydrated_once: bool,
    /// Task 42, `SeedOnce` only: successful Python-import provenance
    /// awaiting its atomic generation-1 commit. Set by hydration's virgin
    /// path, carried into `FeeCycleCommit::pending_seed`, cleared ONLY
    /// after `commit_fee_cycle` returns success. While `Some`, the
    /// out-of-cycle commit paths (A3 new-channel, failed-forward nudges)
    /// refuse: they would otherwise take generation 1 without the seed
    /// row and orphan this provenance permanently (the DB gate would then
    /// reject every retry).
    pending_seed: Option<revops_db::fee_runway::FeeSeedEventRow>,
    /// `SeedOnce` only (Task 5): the one-time seed was refused this
    /// process lifetime -- every later cycle fails closed without
    /// re-attempting (and re-recording) the refusal; a restart re-attempts
    /// against the then-current snapshot.
    seed_refused: bool,
    /// The Rust-owned state store (Task 5). REQUIRED for `SeedOnce`
    /// (cycles fail closed without it); ignored by `RehydratePerCycle`,
    /// which never reads or writes Rust-owned state.
    store: Option<Box<dyn RunwayStateStore>>,
    /// Spawn-time clock read, recorded as the restart marker's startup
    /// timestamp (distinct from the per-cycle clock reads).
    spawn_now: i64,
    /// Red error counter (Task 5 step 3): committed-cycle persistence
    /// failures. Never reset.
    persistence_failures: u64,
    /// Task 59: red error counter for failed/undispatchable retention
    /// sweeps. Never reset; a retention failure never fails a cycle.
    retention_failures: u64,
    /// Task 59: the next sweep's fairness continuation, updated only from
    /// a delivered sweep report.
    retention_cursor: revops_db::retention::RetentionCursor,
    /// Task 59: at most one sweep in flight -- a commit landing while the
    /// previous sweep's report is outstanding schedules nothing (the next
    /// committed cycle re-schedules).
    retention_in_flight: bool,
    db_path: PathBuf,
    /// `None` only if the journal dir could not be created -- logged
    /// loudly at construction; cycles still run (decisions are lost to
    /// disk but the plugin must never crash over bookkeeping IO).
    journal: Option<Journal>,
    state_sink: Option<JournalStateSink>,
    governor: GovernorWiring,
    /// Task 6: the bounded, coalescing trigger queue every out-of-cycle
    /// wake (and the fixed-interval cycle-dispatch trigger itself) is
    /// offered to before the owner thread acts -- see `fee_triggers.rs`'s
    /// module doc.
    trigger_queue: TriggerQueue,
    /// T7: the last cycle's resolved `fee_profile` name, consulted by the
    /// out-of-cycle wake handlers (`wake_all`/`vegas_spike_check`/
    /// `policy_changed`), none of which have a fresh `PreparedCycle` to
    /// hand them a config snapshot. Mirrors Python's own instance-level
    /// `cfg_snap`/`config` attribute, read by `get_fee_profile_settings`
    /// regardless of which call triggered it. Seeded to Python's own
    /// documented default (`"active"`, `_resolve_fee_profile`'s fallback)
    /// so a wake BEFORE the first cycle still resolves a real profile.
    last_profile: String,
    /// Task 10: wall-clock time of the last `run_cycle_at` invocation
    /// (whatever it did -- ran, skipped, or persistence-failed), for the
    /// runway status RPC's "last cycle" field. `None` until the first
    /// cycle attempt.
    last_cycle_at: Option<i64>,
    /// Task 10: a short, stable label for the last cycle's
    /// [`CycleOutcome`] (via [`describe_cycle_outcome`]), paired with
    /// `last_cycle_at`.
    last_cycle_outcome: Option<String>,
    /// Task 44 / A3, live-review finding F5: the owner's OWN queue
    /// sender, so off-owner store dispatches can route their results back
    /// as [`CycleMsg::InitialFeeStoreResult`] messages. Production wiring
    /// (`spawn_with_thread_spawner`) always sets this; a bare test-driven
    /// owner without one fails the A3 path CLOSED (a commit whose result
    /// could never return must not be dispatched).
    result_sink: Option<InitialFeeResultSink>,
    /// F5: in-flight A3 occurrences, keyed by channel_id. An entry's
    /// presence is the same-channel fail-closed race guard; its staged
    /// clones install only on a successful identity-matched commit
    /// result.
    pending_initial_fees: HashMap<String, PendingInitialFee>,
    /// F5: monotonically increasing dispatch generation -- binds each
    /// result message to the exact pending occurrence that dispatched it,
    /// so a stale/foreign result can never be mistaken for the awaited
    /// one.
    initial_fee_dispatch_seq: u64,
    /// F5's red counter: result messages whose identity (channel, event
    /// key, generation, or phase) did not match the pending entry --
    /// each one a fail-closed discard, never an install. Never reset.
    initial_fee_conflicts: u64,
    /// F7: the owner's view of the persisted Rust-store state generation
    /// (`None` until first established by hydration, a scheduled commit,
    /// or an A3 idempotency answer). Every A3 guarded commit CASes
    /// against this basis.
    state_generation: Option<u64>,
    /// F7 refinement (Python-parity sequencing): a full prepared cycle
    /// that arrived while an A3 store result was pending -- deferred so
    /// the cycle runs AFTER the A3 install/refusal and therefore sees the
    /// synchronized state, exactly as Python's `_state_lock` serializes
    /// the two. Bounded to ONE entry: a newer prepared snapshot
    /// supersedes an older deferred one (loudly, counted).
    deferred_cycle: Option<Box<PreparedCycle>>,
    deferred_cycle_ack: Option<tokio::sync::oneshot::Sender<Result<FeeCycleCompletion, String>>>,
    /// F7: deferred prepared cycles that were superseded by a newer one
    /// before they could run (each one loud, never silent). Never reset.
    deferred_cycles_superseded: u64,
}

impl CycleOwner {
    /// Build the owner: opens journal + state sink + dry-run governor
    /// ledger under `cfg.journal_dir`, and seeds the ONE `PyRandom` from
    /// `seed_now` (production: `now_unix()` at spawn -- a spawn-time read,
    /// distinct from the per-cycle clock read in [`run_cycle`]).
    ///
    /// Never panics: any IO failure degrades that one output channel with
    /// a loud stderr line, matching `JournalStateSink`/`GovernorWiring`'s
    /// log-and-continue posture.
    pub fn new(
        cfg: &SchedulerConfig,
        seed_now: i64,
        store: Option<Box<dyn RunwayStateStore>>,
    ) -> CycleOwner {
        let journal = match Journal::open_dir(&cfg.journal_dir) {
            Ok(j) => Some(j),
            Err(e) => {
                eprintln!(
                    "revops: DRY-RUN JOURNAL UNAVAILABLE ({}): {e}; decisions will not be \
                     recorded (window data invalid until fixed)",
                    cfg.journal_dir.display()
                );
                None
            }
        };
        let state_sink = match JournalStateSink::open_dir(&cfg.journal_dir) {
            Ok(s) => Some(s),
            Err(e) => {
                eprintln!(
                    "revops: dry-run state journal unavailable ({}): {e}; state flushes will \
                     not be recorded",
                    cfg.journal_dir.display()
                );
                None
            }
        };
        CycleOwner {
            state: ControllerState::new(),
            rng: PyRandom::seed_from_u64(seed_now.max(0) as u64),
            lifecycle: cfg.lifecycle,
            last_fee_apply_ts: HashMap::new(),
            last_failure_nudge_ts: HashMap::new(),
            applied_failure_nudges_pending: HashSet::new(),
            last_policy_updated_at: HashMap::new(),
            hydrated_once: false,
            pending_seed: None,
            seed_refused: false,
            store,
            spawn_now: seed_now,
            persistence_failures: 0,
            retention_failures: 0,
            retention_cursor: revops_db::retention::RetentionCursor::default(),
            retention_in_flight: false,
            db_path: cfg.db_path.clone(),
            journal,
            state_sink,
            governor: GovernorWiring::open(Some(&cfg.journal_dir)),
            trigger_queue: TriggerQueue::new(TRIGGER_QUEUE_CAPACITY),
            last_profile: "active".to_string(),
            last_cycle_at: None,
            last_cycle_outcome: None,
            result_sink: None,
            pending_initial_fees: HashMap::new(),
            initial_fee_dispatch_seq: 0,
            initial_fee_conflicts: 0,
            state_generation: None,
            deferred_cycle: None,
            deferred_cycle_ack: None,
            deferred_cycles_superseded: 0,
        }
    }

    /// F5: wire the private result-only sink. `spawn_with_thread_spawner`
    /// calls this before entering the message loop; tests driving an owner
    /// directly attach the result-only receiver and pump those results back in.
    fn set_initial_fee_result_sink(&mut self, sink: InitialFeeResultSink) {
        self.result_sink = Some(sink);
    }

    #[doc(hidden)]
    pub fn attach_a3_result_receiver_for_tests(&mut self, capacity: usize) -> A3ResultReceiver {
        let (ingress, rx) = SchedulerIngress::bounded_channel(capacity);
        self.set_initial_fee_result_sink(InitialFeeResultSink::scheduler(ingress));
        A3ResultReceiver {
            rx: std::sync::Mutex::new(rx),
        }
    }

    /// F5's red conflict counter (see the `initial_fee_conflicts` field
    /// doc).
    pub fn initial_fee_conflicts(&self) -> u64 {
        self.initial_fee_conflicts
    }

    /// F5: how many A3 occurrences are currently in flight (pending an
    /// off-owner store result).
    pub fn initial_fee_pending(&self) -> usize {
        self.pending_initial_fees.len()
    }

    /// The owned controller state (read-only view; T7's debug RPC and the
    /// lifecycle tests read through this).
    pub fn state(&self) -> &ControllerState {
        &self.state
    }

    /// Test seam for the RNG-continuity contract ("seeded once, never
    /// reseeded"): drawing from here advances the ONE stream, so only
    /// tests may use it.
    #[doc(hidden)]
    pub fn rng_mut(&mut self) -> &mut PyRandom {
        &mut self.rng
    }

    /// One full cycle on the owner thread -- the numbered per-cycle
    /// sequence from the plan, each point tested in
    /// `tests/fee_scheduler.rs`:
    ///
    /// 1. `clock()` EXACTLY once; the value feeds every downstream
    ///    consumer (evidence snapshot windows, `FixedDecisionClock`).
    /// 2. Fail closed if `neighbor_median_min_competitors` is unresolvable
    ///    (Phase 4b Task 8a; any resolvable positive integer threads
    ///    through to `CycleDeps::min_competitors` instead of the old
    ///    baked `MIN_COMPETITORS = 3` verify gate).
    /// 3. Build the frozen evidence snapshot; on error log + skip (never
    ///    panic).
    /// 4. Lifecycle hydration (per-cycle, or once for `SeedOnce`) over
    ///    the SNAPSHOT's own pinned connection.
    /// 5. + 6. `run_fee_cycle` with the one RNG and this cycle's deps.
    /// 7. Append decisions to the journal, loudly on failure (a silent
    ///    journal gap invalidates the window) but never crash.
    pub fn run_cycle(
        &mut self,
        prepared: PreparedCycle,
        clock: &mut dyn FnMut() -> i64,
    ) -> CycleOutcome {
        // (1) The cycle's single clock read.
        let now = clock();
        let mut decision_clock = FixedDecisionClock::new(now);
        self.run_cycle_at(prepared, now, &mut decision_clock)
    }

    /// `run_cycle`'s wrapper: offers the `FixedInterval` trigger (Task 6
    /// step 4 -- this IS the drain point every pending wake-only trigger
    /// waits for) BEFORE the cycle body, then persists the trigger's own
    /// receipt afterward, carrying the cycle identity (R8 amendment item
    /// 3's shared `cycle_ts`) when the cycle actually committed.
    fn run_cycle_at(
        &mut self,
        prepared: PreparedCycle,
        now: i64,
        decision_clock: &mut dyn DecisionClock,
    ) -> CycleOutcome {
        let fixed_interval_outcome = self.trigger_queue.offer(FeeTrigger::FixedInterval, now);
        let coalesced = matches!(fixed_interval_outcome, TriggerOutcome::Coalesced);
        // Every wake-only trigger pending since the last cycle is now
        // covered by this cycle's own evaluation -- free the queue
        // regardless of what this cycle's outcome turns out to be (a
        // skipped/failed cycle still means the NEXT cycle re-evaluates
        // from current state, so there is nothing left to keep pending).
        self.trigger_queue.drain_all();
        self.applied_failure_nudges_pending.clear();

        let (outcome, cycle_id) = self.run_cycle_body(prepared, now, decision_clock);

        // Task 10: record this attempt for the runway status RPC,
        // regardless of outcome -- "last cycle" means the last time this
        // owner thread attempted one, not only the last successful one.
        self.last_cycle_at = Some(now);
        self.last_cycle_outcome = Some(describe_cycle_outcome(&outcome));

        let cycle = match (&outcome, &cycle_id) {
            (CycleOutcome::Ran { .. }, Some(id)) => Some((id.as_str(), now)),
            _ => None,
        };
        self.record_trigger_receipt(
            &FeeTrigger::FixedInterval,
            now,
            coalesced,
            cycle,
            describe_cycle_outcome(&outcome),
        );
        outcome
    }

    /// The per-cycle sequence proper (see [`Self::run_cycle`]'s doc
    /// comment for the numbered steps). Returns the outcome plus, ONLY on
    /// a `SeedOnce` cycle whose commit succeeded, that commit's
    /// `cycle_id` -- [`Self::run_cycle_at`] uses it to key the
    /// `FixedInterval` trigger receipt to the SAME cycle identity/`cycle_ts`
    /// the commit's `rust_fee_shadow_outcomes` rows carry.
    fn run_cycle_body(
        &mut self,
        prepared: PreparedCycle,
        now: i64,
        decision_clock: &mut dyn DecisionClock,
    ) -> (CycleOutcome, Option<String>) {
        // T7: capture this cycle's resolved profile name for the
        // out-of-cycle wake handlers (see `last_profile`'s doc comment).
        // Captured even on a skip path below -- config still resolved
        // successfully; only the min-competitors/evidence gates failed.
        self.last_profile = prepared.cfg.fee_profile.clone();

        // (2) Fail-closed gate: refuse only when the resolved value is
        // genuinely unusable. Any resolvable positive integer (production
        // runs 2, not the Task 8 baked 3) proceeds.
        let min_competitors = match fee_config::resolve_min_competitors(&prepared.min_competitors) {
            Ok(n) => n,
            Err(reason) => {
                eprintln!(
                    "revops: fee cycle disabled: neighbor_median_min_competitors unresolvable \
                     (value={}): {reason} (skipping cycle)",
                    prepared.min_competitors
                );
                return (CycleOutcome::SkippedMinCompetitors, None);
            }
        };

        // (3) Per-cycle-frozen evidence (read-only DB + prefetched RPC).
        //
        // Task 6 / R8 amendment: `FeeEvidence::mempool_ma_24h` reads
        // Python's rows in strict replay (`RehydratePerCycle`) and ONLY
        // fresh Rust-owned rows in autonomous (`SeedOnce`) mode -- the
        // Rust-owned query happens HERE, before the snapshot freezes,
        // mirroring `RpcPrefetch`'s prefetch-then-freeze shape.
        let mempool_source = match self.lifecycle {
            StateLifecycle::RehydratePerCycle => MempoolEvidenceSource::Python,
            StateLifecycle::SeedOnce => {
                // Task 42: the current sample is made durable and folded
                // into the Rust-only aggregate BEFORE the snapshot
                // freezes, so a VIRGIN store's first cycle sees its own
                // observation instead of deterministically failing into a
                // two-cycle bootstrap. Any store failure here fails the
                // cycle CLOSED before hydration -- never "no evidence".
                match self.refresh_autonomous_mempool_evidence(now, &prepared) {
                    Ok(average) => MempoolEvidenceSource::Rust(average),
                    Err(reason) => {
                        eprintln!(
                            "revops: FEE CYCLE FAIL-CLOSED (autonomous mempool evidence \
                             refresh failed, before hydration): {reason}"
                        );
                        return (CycleOutcome::SkippedAutonomousEvidence, None);
                    }
                }
            }
        };
        let snapshot =
            match build_evidence_snapshot(&self.db_path, prepared.rpc, now, mempool_source) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("revops: fee cycle skipped: evidence snapshot failed: {e:#}");
                    return (CycleOutcome::SkippedEvidence, None);
                }
            };

        // Task 6 step 2: Rust's own mempool recorder + the shadow-window
        // comparison against Python's rows -- see
        // `Self::record_mempool_evidence`'s doc comment. Runs in BOTH
        // lifecycles (building continuity ahead of cutover); never fails
        // the cycle.
        self.record_mempool_evidence(now, &snapshot, &prepared.cfg);

        // Task 44 / A2: the policy-change PRODUCER. See
        // `Self::detect_policy_changes`.
        self.detect_policy_changes(&snapshot, now);

        // (4) State lifecycle (Design Note 1), over the snapshot's pinned
        // read-only connection -- hydration sees the exact same frozen DB
        // view as every other evidence read this cycle.
        //
        // Task 5: `SeedOnce` is restart-persistent. Rust-owned state is
        // queried FIRST; Python is consulted only when Rust reports no
        // prior generation, and once a Rust generation exists Python is
        // never an autonomous-state source again. Every failure path is
        // fail-closed (no cycle, no reseed).
        match self.lifecycle {
            StateLifecycle::RehydratePerCycle => rehydrate(&mut self.state, snapshot.conn()),
            StateLifecycle::SeedOnce => {
                if !self.hydrated_once {
                    if let Err(reason) = self.seed_once_hydrate(snapshot.conn(), now) {
                        eprintln!(
                            "revops: FEE CYCLE FAIL-CLOSED (SeedOnce state unavailable): {reason}"
                        );
                        return (CycleOutcome::SkippedStateUnavailable, None);
                    }
                    self.hydrated_once = true;
                } else {
                    // Top-of-cycle epoch refresh: under SeedOnce, Rust
                    // owns the state, so the T8b pre-decision epoch the
                    // skip gate consumes IS the owned live epoch (which
                    // also folds in any out-of-cycle wake since the last
                    // cycle) -- exactly Python's own pre-decision read.
                    set_skip_gates_to_owned(&mut self.state);
                }
            }
        }

        // (5) + (6) The cycle proper, on the ONE RNG.
        //
        // `journal: None` is deliberate, not an omission of the plan's
        // step-5 sketch: `run_fee_cycle` would append the SAME decisions
        // itself (silently -- its internal `append_all` result is
        // discarded), so passing the journal there AND appending in step
        // (7) would double-write every line, and relying on the internal
        // append alone would lose failures the window contract requires
        // logged loudly. Step (7) below is the single, loud append.
        //
        // Task 5: `SeedOnce` swaps in a capability-free
        // `RecordingFeeExecutor` (it owns no socket or broadcaster) so the
        // cycle's would-broadcast prepared intents can be drained into the
        // atomic Rust-owned commit; the state JSONL sink is bypassed --
        // the transactional store commit below IS the state persistence.
        let governed = self.governor.governed_deps(&prepared.cfg);
        let authorizer = GovernedFeeAuthorizer::new(&governed);
        let pure_executor = PureFeeExecutor;
        let recording_executor =
            matches!(self.lifecycle, StateLifecycle::SeedOnce).then(RecordingFeeExecutor::default);
        let executor: &dyn FeeExecutor = match &recording_executor {
            Some(recording) => recording,
            None => &pure_executor,
        };
        let state_sink = match self.lifecycle {
            StateLifecycle::RehydratePerCycle => {
                self.state_sink.as_ref().map(|s| s as &dyn StateSink)
            }
            StateLifecycle::SeedOnce => None,
        };
        let mut deps = CycleDeps {
            evidence: &snapshot,
            cfg: &prepared.cfg,
            rng: &mut self.rng,
            clock: decision_clock,
            authorizer: Some(&authorizer),
            executor,
            journal: None,
            state_sink,
            min_competitors,
        };
        let decisions = match run_fee_cycle(&mut self.state, &mut deps) {
            Ok(decisions) => decisions,
            Err(error) => {
                eprintln!("revops: fee cycle stopped: replayable decision input failed: {error}");
                return (CycleOutcome::SkippedDecisionInput, None);
            }
        };

        // Task 5 / amendment R5: end-of-cycle epoch refresh. The owned
        // post-cycle epochs are the NEXT cycle's pre-decision epochs, and
        // the invariant `skip_gate_prev == owned cycle.last_update` after
        // every SeedOnce cycle is exactly what the epoch-identity test
        // pins (a divergence would mean the RehydratePerCycle-era
        // post-decision-epoch bug was reintroduced).
        if matches!(self.lifecycle, StateLifecycle::SeedOnce) {
            set_skip_gates_to_owned(&mut self.state);
        }

        // (7) The one journal append -- loud on failure, never fatal.
        if let Some(journal) = &self.journal {
            if let Err(e) = journal.append_all(&decisions) {
                eprintln!(
                    "revops: DRY-RUN JOURNAL WRITE FAILED ({}): {e}; {} decision(s) lost this \
                     cycle (window data invalid until fixed)",
                    journal.path().display(),
                    decisions.len()
                );
            }
        }

        // (8) Task 5 step 3: `SeedOnce` commits state + the full audit
        // batch atomically after each successful cycle. A commit error is
        // `PersistenceFailed`: the generation does not advance, the red
        // counter increments, and the next cycle continues from in-memory
        // state (a restart would resume from the last COMMITTED
        // generation, discarding this cycle -- recorded divergence, never
        // silent).
        let mut committed_cycle_id = None;
        if matches!(self.lifecycle, StateLifecycle::SeedOnce) {
            let intents = recording_executor
                .as_ref()
                .map(|r| r.recorded_actions())
                .unwrap_or_default();
            let commit = self.build_cycle_commit(now, &decisions, &intents);
            let cycle_id = commit.cycle_id.clone();
            let store = self
                .store
                .as_ref()
                .expect("SeedOnce hydration guarantees a store");
            match store.commit_fee_cycle(commit) {
                Ok(generation) => {
                    // F7: keep the owner's persisted-generation view
                    // current -- the A3 guarded commit CASes against it.
                    self.state_generation = Some(generation);
                    // Task 42: the pending seed provenance (if any) is
                    // now durable INSIDE the committed transaction --
                    // only success clears it; a failure retains it for
                    // the same-process retry.
                    self.pending_seed = None;
                }
                Err(e) => {
                    self.persistence_failures += 1;
                    eprintln!(
                        "revops: FEE CYCLE PERSISTENCE FAILED (failure #{}): {e:#}; generation \
                         NOT advanced; this cycle's state evolution is uncommitted (a restart \
                         resumes from the last committed generation)",
                        self.persistence_failures
                    );
                    return (CycleOutcome::PersistenceFailed, None);
                }
            }
            committed_cycle_id = Some(cycle_id);
            // Task 59: retention rides only successful scheduled commits,
            // off this owner thread; it can never fail the cycle.
            self.schedule_retention_sweep(now);
        }

        let adjusted_channels = decisions
            .iter()
            .filter(|decision| decision.would_broadcast)
            .count();
        (
            CycleOutcome::Ran {
                decisions: decisions.len(),
                adjusted_channels,
            },
            committed_cycle_id,
        )
    }

    /// Red error counter: SeedOnce cycles whose atomic commit failed.
    pub fn persistence_failures(&self) -> u64 {
        self.persistence_failures
    }

    /// Task 59 red error counter: retention sweeps that failed or could
    /// not be dispatched. Never reset.
    pub fn retention_failures(&self) -> u64 {
        self.retention_failures
    }

    /// Task 59 §3.6: age of the OLDEST in-flight A3 occurrence, if any.
    fn oldest_pending_age_seconds(&self, now: i64) -> Option<i64> {
        self.pending_initial_fees
            .values()
            .map(|pending| now.saturating_sub(pending.dispatched_at()))
            .max()
    }

    /// Test-only seam for the §3.6 threshold test: shift every pending
    /// stamp into the past. Touches ONLY the diagnostic stamp -- no
    /// occurrence state, no cancellation path exists to reach.
    pub fn backdate_pending_initial_fees_for_tests(&mut self, seconds: i64) {
        for pending in self.pending_initial_fees.values_mut() {
            pending.backdate(seconds);
        }
    }

    /// Task 59: schedule one bounded Class-W retention sweep off this
    /// owner thread. Called ONLY after a successful SCHEDULED cycle
    /// commit. At most one sweep is in flight; a launch failure is
    /// counted red and logged, and NEVER affects the committed cycle.
    fn schedule_retention_sweep(&mut self, now: i64) {
        if self.retention_in_flight {
            return;
        }
        let (Some(result_sink), Some(store)) = (self.result_sink.clone(), self.store.as_ref())
        else {
            return;
        };
        let launch = store.dispatch_run_retention_sweep(
            now,
            self.retention_cursor,
            Box::new(move |result| {
                if !result_sink.deliver(InitialFeeStoreResult::Retention {
                    result: result.map_err(|e| format!("{e:#}")),
                }) {
                    eprintln!("revops: retention sweep result undeliverable (owner gone)");
                }
            }),
        );
        match launch {
            Ok(()) => self.retention_in_flight = true,
            Err(e) => {
                self.retention_failures += 1;
                eprintln!(
                    "revops: RETENTION SWEEP DISPATCH FAILED (failure #{}): {e:#}",
                    self.retention_failures
                );
            }
        }
    }

    /// Task 6 step 2: Rust's own mempool recorder, plus (during the
    /// `RehydratePerCycle` shadow window) a comparison against Python's
    /// evidence value.
    ///
    /// Records ONE sample this cycle -- `chain_costs.sat_per_vbyte` --
    /// gated the EXACT same way Python's `record_mempool_fee` call site
    /// is (`cfg.enable_vegas_reflex && chain_costs`, fee_controller.py:
    /// 4583-4587), so Rust's own history stays sample-for-sample
    /// comparable to Python's, transactionally pruned to
    /// [`MEMPOOL_MA_WINDOW_SECONDS`] (`fee_runway::
    /// record_mempool_sample_pruned`'s contract). Runs in BOTH lifecycles
    /// (`RehydratePerCycle` AND `SeedOnce`) so continuity exists BEFORE
    /// cutover ever needs it; a missing store or a write failure is
    /// logged loudly and never fails the cycle (this is bookkeeping, not
    /// decision-relevant evidence in `RehydratePerCycle` mode -- in
    /// `SeedOnce` mode the decision-relevant read already happened
    /// earlier, in `run_cycle_body`'s evidence-source selection).
    ///
    /// The shadow-window comparison (R8 binding constraint: "during
    /// shadow, compare the Rust 24h MA against Python's and record the
    /// comparison") is BOTH a loud log line AND a persisted
    /// `rust_mempool_ma_comparison` row (fix round 1, review finding 1:
    /// the daily rollup consumes DB evidence, never logs). A store-write
    /// failure here is logged loudly and never fails the cycle -- this
    /// recorder is bookkeeping, not decision-relevant evidence, in
    /// `RehydratePerCycle` mode.
    /// Task 42 correction F2: the EXPLICIT SeedOnce bootstrap state the
    /// out-of-cycle commit guards consume — `pending_seed` alone is not
    /// the state machine (before the first hydration it is `None` while
    /// the store is still virgin and unproven).
    ///
    /// Derived, not duplicated: computed from the owner's authoritative
    /// fields so it can never drift from reality.
    pub fn bootstrap_state(&self) -> SeedOnceBootstrapState {
        match self.lifecycle {
            // Strict-replay mode has no bootstrap: Python remains the
            // state authority and generation semantics don't apply.
            StateLifecycle::RehydratePerCycle => SeedOnceBootstrapState::Ready,
            StateLifecycle::SeedOnce => {
                if self.seed_refused {
                    SeedOnceBootstrapState::Refused
                } else if !self.hydrated_once {
                    SeedOnceBootstrapState::NotStarted
                } else if self.pending_seed.is_some() {
                    SeedOnceBootstrapState::PendingSeedCommit
                } else {
                    SeedOnceBootstrapState::Ready
                }
            }
        }
    }

    /// Task 42 (F-R2 complete matrix): THE single admissibility gate for
    /// every out-of-cycle generation-advancing commit path. `None` means
    /// admissible (bootstrap `Ready`); `Some(state)` carries the exact
    /// refusing state for the path's receipt. Both production guards (A3
    /// new-channel, failed-forward nudge) delegate here, so a
    /// state-window hole cannot be opened on one path without mutating
    /// the shared gate itself.
    fn out_of_cycle_commit_refusal(&self) -> Option<SeedOnceBootstrapState> {
        let bootstrap = self.bootstrap_state();
        if bootstrap == SeedOnceBootstrapState::Ready {
            None
        } else {
            Some(bootstrap)
        }
    }

    /// Task 42: the autonomous (`SeedOnce`) mempool-evidence refresh,
    /// run BEFORE the evidence snapshot freezes. Mirrors the Python
    /// recorder gate (`record_mempool_fee`'s call site: Vegas Reflex
    /// enabled and chain costs resolved this cycle):
    ///
    /// * Vegas disabled -> no sample, no aggregate (`Ok(None)`; the
    ///   kernel never consults the MA).
    /// * Vegas enabled + chain costs resolved -> ONE store transaction
    ///   inserts the current sample, prunes the 24h window, and returns
    ///   the Rust-only average the decision will consume.
    /// * Vegas enabled, chain costs unresolved (feerates prefetch
    ///   absent) -> no insert (cadence gate), read-only window average.
    ///
    /// EVERY store failure is `Err` -- fail-closed before hydration,
    /// never degraded to an empty window (the audit's "query errors are
    /// converted to an empty vector" defect).
    fn refresh_autonomous_mempool_evidence(
        &self,
        now: i64,
        prepared: &PreparedCycle,
    ) -> Result<Option<f64>, String> {
        if !prepared.cfg.enable_vegas_reflex {
            return Ok(None);
        }
        let Some(store) = self.store.as_ref() else {
            // No store at all is a STATE-unavailability condition, not an
            // evidence failure: return no evidence and let
            // `seed_once_hydrate` fail the cycle closed with its accurate
            // `SkippedStateUnavailable` reason (nothing here could have
            // been recorded anyway).
            return Ok(None);
        };
        let retain_since = now - MEMPOOL_MA_WINDOW_SECONDS;
        match crate::fee_evidence::chain_costs_from_feerates(prepared.rpc.feerates.as_ref()) {
            Some(costs) => store
                .refresh_mempool_window(now, costs.sat_per_vbyte, retain_since)
                .map(|window| window.average)
                .map_err(|e| format!("mempool window refresh failed: {e:#}")),
            None => {
                // Same cadence gate as the recorder: no resolved chain
                // costs, no new sample -- but existing fresh samples are
                // still legitimate evidence.
                let rows = store
                    .query_mempool_samples_since(retain_since)
                    .map_err(|e| format!("mempool window read failed: {e:#}"))?;
                if rows.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(
                        rows.iter().map(|r| r.sat_per_vbyte).sum::<f64>() / rows.len() as f64,
                    ))
                }
            }
        }
    }

    fn record_mempool_evidence(&self, now: i64, snapshot: &EvidenceSnapshot, cfg: &FeeCfgSnapshot) {
        // Task 42: in `SeedOnce` the current sample is inserted (and the
        // window pruned) by `refresh_autonomous_mempool_evidence` BEFORE
        // the snapshot froze -- recording it again here would double the
        // cadence, and the Python-MA comparison below is
        // `RehydratePerCycle`-only anyway.
        if !matches!(self.lifecycle, StateLifecycle::RehydratePerCycle) {
            return;
        }
        if !cfg.enable_vegas_reflex {
            return;
        }
        let Some(costs) = snapshot.chain_costs() else {
            return;
        };
        let Some(store) = self.store.as_ref() else {
            return;
        };
        let retain_since = now - MEMPOOL_MA_WINDOW_SECONDS;
        if let Err(e) = store.record_mempool_sample_pruned(now, costs.sat_per_vbyte, retain_since) {
            eprintln!(
                "revops: Rust-owned mempool sample record failed ({e:#}); comparison history \
                 has a gap this cycle"
            );
            return;
        }

        if !matches!(self.lifecycle, StateLifecycle::RehydratePerCycle) {
            // SeedOnce already reads Rust rows as the decision-relevant
            // evidence itself -- nothing to compare against.
            return;
        }
        match store.query_mempool_samples_since(retain_since) {
            Ok(rows) if !rows.is_empty() => {
                let rust_ma = rows.iter().map(|r| r.sat_per_vbyte).sum::<f64>() / rows.len() as f64;
                let (python_ma, delta) = match snapshot.mempool_ma_24h() {
                    Ok(python_ma) => {
                        eprintln!(
                            "revops: mempool 24h MA comparison (shadow): rust={rust_ma:.4} \
                             python={python_ma:.4} delta={:.4} rust_samples={}",
                            rust_ma - python_ma,
                            rows.len()
                        );
                        (Some(python_ma), Some(rust_ma - python_ma))
                    }
                    Err(e) => {
                        eprintln!(
                            "revops: mempool 24h MA comparison (shadow): rust={rust_ma:.4} \
                             python=<unavailable: {e}> rust_samples={}",
                            rows.len()
                        );
                        // Fix round 1 (review finding 1): absence is
                        // itself evidence -- record the row with
                        // `python_ma`/`delta` NULL rather than skipping
                        // it.
                        (None, None)
                    }
                };
                if let Err(e) = store.record_mempool_ma_comparison(
                    revops_db::fee_runway::MempoolMaComparisonRow {
                        at: now,
                        cycle_ts: now,
                        rust_ma,
                        python_ma,
                        delta,
                    },
                ) {
                    eprintln!(
                        "revops: mempool MA comparison record failed ({e:#}); shadow-window \
                         evidence for this cycle has a gap"
                    );
                }
            }
            Ok(_) => {} // no Rust samples in the window yet -- nothing to compare
            Err(e) => eprintln!("revops: Rust mempool comparison query failed ({e:#})"),
        }
    }

    /// The one-time `SeedOnce` hydration decision (Task 5 step 2):
    /// Rust-owned state first; Python only on a genuinely empty store;
    /// everything else fails closed with a loud reason.
    fn seed_once_hydrate(
        &mut self,
        prod_conn: &rusqlite::Connection,
        now: i64,
    ) -> Result<(), String> {
        let Some(store) = self.store.as_ref() else {
            return Err(
                "no Rust-owned state store configured (SeedOnce requires one); \
                 refusing to run autonomous cycles"
                    .to_string(),
            );
        };
        if self.seed_refused {
            return Err(
                "the one-time seed was refused earlier this process lifetime; staying \
                 passive-observer (restart to re-attempt against the current snapshot)"
                    .to_string(),
            );
        }

        let stored = store
            .load_latest_state()
            .map_err(|e| format!("Rust-owned state load failed: {e:#}"))?;
        let source = if stored.generation > 0 {
            // Task 42 correction F2.3/F1: EVERY nonvirgin store must carry
            // verified bound seed provenance. A generation without it (an
            // out-of-cycle-first store, a legacy unbound row, a refusal-
            // only store, duplicates, a dangling binding) is refused
            // outright — fail-closed, never reseeded, never trusted.
            match store.verified_seed_binding() {
                Ok(revops_db::fee_runway::SeedBindingState::VerifiedBound { .. }) => {}
                Ok(other) => {
                    return Err(format!(
                        "generation {} store failed seed-binding verification: {other:?}; \
                         refusing to hydrate (fail-closed, no reseed)",
                        stored.generation
                    ));
                }
                Err(e) => {
                    return Err(format!("seed-binding verification read failed: {e:#}"));
                }
            }
            if stored.rows.is_empty() {
                return Err(format!(
                    "generation {} is recorded but no state rows exist: corrupt Rust-owned \
                     store; refusing to reseed from Python",
                    stored.generation
                ));
            }
            rehydrate_from_rows(&mut self.state, &stored.rows).map_err(|e| {
                format!(
                    "corrupt Rust-owned state at generation {}: {e}; refusing to reseed \
                     from Python",
                    stored.generation
                )
            })?;
            HydrationSource::RustGeneration(stored.generation)
        } else {
            if !stored.rows.is_empty() {
                return Err(
                    "state rows exist at generation 0: corrupt Rust-owned store".to_string()
                );
            }
            let source_db_path = self.db_path.display().to_string();
            match seed_once_from_python(
                &mut self.state,
                prod_conn,
                &source_db_path,
                now,
                source_commit(),
            ) {
                SeedOutcome::Seeded(event) => {
                    // Task 42: success provenance is NOT written here.
                    // It becomes the owner's pending in-memory value and
                    // commits ATOMICALLY with the generation-1
                    // transaction (`FeeCycleCommit::pending_seed`) -- a
                    // standalone 'seeded' row before that commit would be
                    // a false durable success claim (the audit's false
                    // seed event). A same-process retry after a failed
                    // commit retains this pending value; a restart with
                    // generation 0 re-derives it from the new pinned
                    // snapshot.
                    self.pending_seed = Some(event);
                    HydrationSource::PythonSeed
                }
                SeedOutcome::Refused(event) => {
                    self.seed_refused = true;
                    // Refusal IS the terminal fact -- it stays a
                    // standalone durable event (Task 42 keeps this path).
                    if let Err(e) = store.record_seed_refusal(event) {
                        eprintln!(
                            "revops: seed refusal could not be recorded in the Rust-owned \
                             store: {e:#} (refusal still enforced in-process)"
                        );
                    }
                    return Err(
                        "the one-time seed was refused (see the SEED REFUSED log line); \
                         staying passive-observer"
                            .to_string(),
                    );
                }
            }
        };

        // Task 5 step 4: the restart marker -- process identity, prior
        // generation, hydration source, startup timestamp. A store that
        // cannot record it could not commit the coming cycle either, so
        // this too fails closed (hydration rolled back; retried next
        // cycle -- both hydration paths are deterministic reads).
        let marker = FeeRestartMarkerRow {
            started_at: self.spawn_now,
            process_id: std::process::id() as i64,
            prior_generation: stored.generation as i64,
            hydration_source: source.label(),
            source_commit: source_commit().to_string(),
        };
        if let Err(e) = store.record_restart_marker(marker) {
            clear_hydrated_state(&mut self.state);
            // Task 42: hydration is rolled back, so the pending seed it
            // may have produced is rolled back with it (the retry
            // re-derives deterministically from the pinned snapshot).
            self.pending_seed = None;
            return Err(format!("restart marker record failed: {e:#}"));
        }
        // F7: hydration establishes the owner's view of the persisted
        // state generation -- the basis every A3 guarded commit CASes
        // against. Set only HERE (hydration success), on a scheduled
        // SeedOnce commit, and on an A3 install.
        self.state_generation = Some(stored.generation);
        Ok(())
    }

    /// Build the atomic per-cycle commit (Task 5 step 3): every channel's
    /// CURRENT owned state (all channels, not just cycle-dirty ones, so a
    /// restart hydrates the complete set), the drained would-broadcast
    /// intents, the governor traces, and one terminal outcome row per
    /// decision. `ledger` rows stay empty here: `EconLedger` events keep
    /// their own Rust-owned dry-run ledger DB (`GovernorWiring`), which is
    /// not part of the per-cycle transactional batch.
    fn build_cycle_commit(
        &self,
        now: i64,
        decisions: &[FeeDecision],
        intents: &[revops_fees::execution::PreparedFeeAction],
    ) -> FeeCycleCommit {
        use std::sync::atomic::{AtomicU64, Ordering};
        // Unique cycle identity even under a frozen test clock or a
        // same-second restart: wall time + pid + a process-wide sequence.
        static COMMIT_SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = COMMIT_SEQ.fetch_add(1, Ordering::Relaxed);
        let cycle_id = format!("rust-fee-{now}-{}-{seq}", std::process::id());

        let mut state_rows = Vec::with_capacity(self.state.cycle_states.len());
        for (channel_id, cycle) in &self.state.cycle_states {
            let Some(fee) = self.state.fee_states.get(channel_id) else {
                eprintln!(
                    "revops: channel {channel_id} has cycle state but no fee state; \
                     excluded from this commit"
                );
                continue;
            };
            state_rows.push(revops_db::fee_runway::FeeStateRow {
                channel_id: channel_id.clone(),
                v2_state_json: serialize_state_envelope(cycle, fee),
                last_update: cycle.last_update,
            });
        }

        let governor: Vec<GovernorAuditRow> = decisions
            .iter()
            .filter_map(|d| {
                d.governed.as_ref().map(|g| GovernorAuditRow {
                    channel_id: d.channel_id.clone(),
                    authorized: g.authorized,
                    reason_code: g.reason_code.clone(),
                    intent_id: g.intent_id.clone(),
                    idempotency_key: g.idempotency_key.clone(),
                    at: d.at,
                })
            })
            .collect();

        let outcomes: Vec<ShadowCycleOutcomeRow> = decisions
            .iter()
            .map(|d| ShadowCycleOutcomeRow {
                cycle_ts: now,
                channel_id: d.channel_id.clone(),
                would_broadcast: d.would_broadcast,
                has_algorithm_values: !matches!(d.algorithm_values, PyOValue::Null),
                disposition: d
                    .trace
                    .get("disposition")
                    .and_then(PyOValue::as_str)
                    .map(str::to_string),
                skip_gate_comparable: !matches!(
                    d.trace.get("skip_gate_comparable"),
                    Some(PyOValue::Bool(false))
                ),
            })
            .collect();

        let requests: Vec<PreparedFeeActionRow> = intents
            .iter()
            .map(|action| {
                let idempotency_key = decisions
                    .iter()
                    .find(|d| d.channel_id == action.request.id)
                    .and_then(|d| d.governed.as_ref())
                    .map(|g| g.idempotency_key.clone());
                PreparedFeeActionRow {
                    channel_id: action.request.id.clone(),
                    idempotency_key,
                    old_fee_ppm: action.old_fee_ppm,
                    new_fee_ppm: action.decision.clamped_fee_ppm,
                    feebase_msat: action.expected_base_fee_msat,
                    htlcmin_msat: action.request.htlcmin.map(|v| v as i64),
                    htlcmax_msat: action.request.htlcmax.map(|v| v as i64),
                    message: action.decision.message.clone(),
                    at: now,
                }
            })
            .collect();

        FeeCycleCommit {
            cycle_id,
            started_at: now,
            completed_at: now,
            source_commit: source_commit().to_string(),
            binary_sha256: binary_sha256().to_string(),
            state_rows,
            requests,
            governor,
            ledger: Vec::new(),
            outcomes,
            // Task 42: the pending seed provenance (virgin bootstrap
            // only) commits atomically with this cycle or not at all.
            pending_seed: self.pending_seed.clone(),
            // Regular scheduled cycles keep using the existing separate
            // (non-atomic) `record_trigger_event` path for the
            // `FixedInterval` receipt -- see `FeeCycleCommit::
            // trigger_receipt`'s doc comment for why that is fine here and
            // NOT fine for A3's out-of-cycle commit.
            trigger_receipt: None,
        }
    }

    /// Test seam, mirroring [`rng_mut`](Self::rng_mut): direct mutable
    /// access to the owned `ControllerState` so tests can seed sleep/edge-
    /// trigger fixtures without driving a whole cycle. Production code
    /// only ever reaches `ControllerState` through the handler methods
    /// below (`wake_all`/`vegas_spike_check`/`policy_changed`/
    /// `fee_debug`) or `run_cycle`.
    #[doc(hidden)]
    pub fn state_mut(&mut self) -> &mut ControllerState {
        &mut self.state
    }

    /// `wake_all_sleeping_channels` (py 4295-4384) -- [`CycleMsg::WakeAll`]'s
    /// handler. `now` is a fresh, one-off clock read (NOT a per-cycle read;
    /// the Global Constraint's "clock once per cycle" governs
    /// [`run_cycle`](Self::run_cycle), not this out-of-cycle wake action).
    /// Returns the count woken, for callers (currently only tests; the
    /// `revenue-r-fee-wake` RPC is fire-and-forget -- see the module doc).
    pub fn wake_all(&mut self, now: i64) -> i64 {
        let (_, profile) = fee_profile(&self.last_profile);
        wake_all_sleeping_channels(&mut self.state, profile, now)
    }

    /// `_maybe_wake_for_vegas_spike` (py 4386-4411) --
    /// [`CycleMsg::VegasSpikeCheck`]'s handler. Returns whether this call
    /// fired the edge-triggered wake.
    pub fn vegas_spike_check(&mut self, now: i64) -> bool {
        let (_, profile) = fee_profile(&self.last_profile);
        maybe_wake_for_vegas_spike(&mut self.state, profile, now)
    }

    /// `_handle_policy_change` (py 7356-7400) -- [`CycleMsg::PolicyChanged`]'s
    /// handler. Returns the count woken.
    pub fn policy_changed(&mut self, channel_states: &[ChannelStateRow], peer_id: &str) -> i64 {
        handle_policy_change(&mut self.state, channel_states, peer_id)
    }

    // -----------------------------------------------------------------
    // Task 6 step 4: bounded/coalescing trigger-queue wrappers. Every
    // out-of-cycle trigger source in `spawn_with_thread_spawner`'s owner
    // message loop calls ONE of these (never the raw `wake_all`/
    // `vegas_spike_check`/`policy_changed` methods above directly) so
    // every occurrence is offered to `self.trigger_queue` and gets a
    // persisted receipt recording WHY it did or did not produce a cycle.
    // These wrappers enqueue and record ONLY -- they never run a cycle
    // inline (module doc, `fee_scheduler.rs` top + `fee_triggers.rs`).
    // -----------------------------------------------------------------

    /// Persist one trigger receipt via `self.store`, if any is configured.
    /// Never fatal: a store-write failure here only means this ONE
    /// receipt is lost (loudly logged), matching every other
    /// log-and-continue posture in this module.
    fn record_trigger_receipt(
        &self,
        trigger: &FeeTrigger,
        received_at: i64,
        coalesced: bool,
        cycle: Option<(&str, i64)>,
        detail: impl Into<String>,
    ) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        let row = build_receipt(trigger, received_at, coalesced, cycle, detail);
        if let Err(e) = store.record_trigger_event(row) {
            eprintln!(
                "revops: trigger receipt record failed ({e:#}): {} at {received_at}",
                trigger.trigger_type()
            );
        }
    }

    /// [`CycleMsg::WakeAll`]'s full handling (see [`Self::wake_all`] for
    /// the underlying effect). Backpressure-dropped occurrences skip the
    /// wake entirely -- a red, always-recorded event, never silent.
    pub fn handle_wake_all(&mut self, now: i64) -> i64 {
        let outcome = self.trigger_queue.offer(FeeTrigger::WakeAll, now);
        if matches!(outcome, TriggerOutcome::Dropped) {
            eprintln!("revops: TRIGGER DROPPED (bounded queue at capacity): wake_all at {now}");
            self.record_trigger_receipt(
                &FeeTrigger::WakeAll,
                now,
                false,
                None,
                "DROPPED: bounded trigger queue at capacity (backpressure)",
            );
            return 0;
        }
        let woken = self.wake_all(now);
        self.record_trigger_receipt(
            &FeeTrigger::WakeAll,
            now,
            matches!(outcome, TriggerOutcome::Coalesced),
            None,
            format!(
                "applied in-memory wake ({woken} channel(s)); did not itself run a cycle -- \
                 the next scheduled cycle observes it"
            ),
        );
        woken
    }

    /// Completion-bearing wrapper for canonical `revenue-wake-all`.
    pub fn handle_wake_all_completion(&mut self, now: i64) -> FeeWakeCompletion {
        FeeWakeCompletion {
            channels_woken: self.handle_wake_all(now),
            completed_at: now,
        }
    }

    /// [`CycleMsg::VegasSpikeCheck`]'s full handling (see
    /// [`Self::vegas_spike_check`] for the underlying effect).
    pub fn handle_vegas_spike_check(&mut self, now: i64) -> bool {
        let outcome = self.trigger_queue.offer(FeeTrigger::VegasSpike, now);
        if matches!(outcome, TriggerOutcome::Dropped) {
            eprintln!(
                "revops: TRIGGER DROPPED (bounded queue at capacity): vegas_spike_check at {now}"
            );
            self.record_trigger_receipt(
                &FeeTrigger::VegasSpike,
                now,
                false,
                None,
                "DROPPED: bounded trigger queue at capacity (backpressure)",
            );
            return false;
        }
        let fired = self.vegas_spike_check(now);
        self.record_trigger_receipt(
            &FeeTrigger::VegasSpike,
            now,
            matches!(outcome, TriggerOutcome::Coalesced),
            None,
            format!(
                "edge-triggered check (fired={fired}); did not itself run a cycle -- the next \
                 scheduled cycle observes it"
            ),
        );
        fired
    }

    /// [`CycleMsg::PolicyChanged`]'s full handling (see
    /// [`Self::policy_changed`] for the underlying effect). `channel_states`
    /// is the fresh, unpinned read the caller already fetched (see
    /// `read_channel_states_readonly`).
    pub fn handle_policy_changed(
        &mut self,
        channel_states: &[ChannelStateRow],
        peer_id: &str,
        now: i64,
    ) -> i64 {
        let trigger = FeeTrigger::PolicyChanged {
            channel_id: peer_id.to_string(),
        };
        let outcome = self.trigger_queue.offer(trigger.clone(), now);
        if matches!(outcome, TriggerOutcome::Dropped) {
            eprintln!(
                "revops: TRIGGER DROPPED (bounded queue at capacity): policy_changed peer \
                 {peer_id} at {now}"
            );
            self.record_trigger_receipt(
                &trigger,
                now,
                false,
                None,
                "DROPPED: bounded trigger queue at capacity (backpressure)",
            );
            return 0;
        }
        let woken = self.policy_changed(channel_states, peer_id);
        self.record_trigger_receipt(
            &trigger,
            now,
            matches!(outcome, TriggerOutcome::Coalesced),
            None,
            format!(
                "applied in-memory wake ({woken} channel(s)); did not itself run a cycle -- \
                 the next scheduled cycle observes it"
            ),
        );
        woken
    }

    /// [`CycleMsg::FailedForward`]'s handler -- the port of
    /// `record_failed_forward` (py `fee_controller.py:9179`).
    ///
    /// Task 44 (2026-07-27): this used to record a receipt and stop. The
    /// kernel was already ported and oracle-tested against Python
    /// (`is_fee_relevant_failure`, `failed_forward_nudge_weight`,
    /// `failed_forward_implied_fee`, `record_posterior_nudge`); only the
    /// wiring was missing, so after cutover -- with Python off -- nothing
    /// would ever have written a bias nudge again and the DTS posterior
    /// would have lost its negative-evidence channel silently.
    ///
    /// Guard order is Python's, verbatim, because each guard is
    /// incident-derived and the ORDER decides what gets counted:
    ///
    /// 1. empty channel or `current_fee_ppm <= 0` -> nothing to imply;
    /// 2. `is_fee_relevant_failure` (audit DTS-4b) -- liquidity and
    ///    downstream failures, and payloads with no usable failcode or
    ///    failreason, are dropped rather than misread as fee evidence;
    /// 3. gossip-settle cooldown against our own last apply;
    /// 4. per-window rate limit against the last nudge;
    /// 5. the channel must ALREADY have posterior state.
    ///
    /// On (5) this deliberately does NOT reproduce Python's E-4.9 lazy
    /// seed from the persisted row. That patch exists because Python's
    /// `_channel_fee_states` is a lazily-populated cache that is empty
    /// after a restart until the fee loop next touches a channel, so every
    /// nudge in that gap was a silent no-op. Rust's `fee_states` is
    /// hydrated once (SeedOnce) or per cycle and held for the process
    /// lifetime -- the gap does not exist. Absence therefore carries the
    /// same meaning Python's `has_persisted_dts` check enforces: no
    /// persisted DTS evidence, so return. The invariant both preserve is
    /// that a failed forward is NEVER a channel's first posterior
    /// evidence.
    pub fn handle_failed_forward(&mut self, signal: &FailedForwardSignal) {
        let channel_id = signal.channel_id.as_str();
        let now = signal.event_ts;
        let trigger = FeeTrigger::FailedForward {
            channel_id: channel_id.to_string(),
        };

        // Task 53: admission is the first operation. A bounded-queue
        // refusal must be byte-identical at the owner-state boundary.
        let queue_outcome = self.trigger_queue.offer(trigger.clone(), now);
        if matches!(queue_outcome, TriggerOutcome::Dropped) {
            eprintln!(
                "revops: TRIGGER DROPPED (bounded queue at capacity): failed_forward channel \
                 {channel_id} at {now}"
            );
            self.record_trigger_receipt(
                &trigger,
                now,
                false,
                None,
                "DROPPED: bounded trigger queue at capacity (backpressure); no nudge evaluated",
            );
            return;
        }

        let coalesced = matches!(queue_outcome, TriggerOutcome::Coalesced);

        // Task 42 correction F2 (fail-closed): same admissibility rule as
        // the A3 new-channel guard — out-of-cycle commits only once the
        // SeedOnce bootstrap is complete (see `bootstrap_state`).
        if let Some(bootstrap) = self.out_of_cycle_commit_refusal() {
            eprintln!(
                "revops: FAILED-FORWARD NUDGE REFUSED (fail-closed): SeedOnce bootstrap \
                 is {bootstrap:?} (channel {channel_id} at {now})"
            );
            self.record_trigger_receipt(
                &trigger,
                now,
                coalesced,
                None,
                format!(
                    "REFUSED (no decision, no state, no action): SeedOnce bootstrap is \
                     {bootstrap:?}; out-of-cycle commits require a complete committed \
                     bootstrap"
                )
                .as_str(),
            );
            return;
        }

        if coalesced && self.applied_failure_nudges_pending.contains(channel_id) {
            self.record_trigger_receipt(
                &trigger,
                now,
                true,
                None,
                "COALESCED: an earlier pending occurrence already committed one nudge; this \
                 occurrence made no state change",
            );
            return;
        }

        let event_key = failed_forward_event_key(signal);
        let cycle_exists = match self.store.as_ref() {
            Some(store) => store.cycle_exists(&event_key),
            None => {
                self.persistence_failures += 1;
                eprintln!(
                    "revops: failed_forward persistence failure #{}: no Rust-owned store; \
                     channel {channel_id} at {now} was not evaluated",
                    self.persistence_failures
                );
                return;
            }
        };
        match cycle_exists {
            Ok(true) => {
                self.record_trigger_receipt(
                    &trigger,
                    now,
                    coalesced,
                    None,
                    format!(
                        "DUPLICATE: failed-forward event {event_key} already committed; no \
                         second nudge"
                    ),
                );
                return;
            }
            Ok(false) => {}
            Err(e) => {
                self.persistence_failures += 1;
                eprintln!(
                    "revops: failed_forward idempotency read failure #{} for channel \
                     {channel_id} at {now}: {e:#}; no nudge evaluated",
                    self.persistence_failures
                );
                self.record_trigger_receipt(
                    &trigger,
                    now,
                    coalesced,
                    None,
                    "PERSISTENCE FAILED: idempotency read failed; no nudge evaluated",
                );
                return;
            }
        }

        let staged = self.stage_failure_nudge(signal);
        let (implied_fee, weight) = match staged.outcome {
            NudgeOutcome::Applied {
                implied_fee,
                weight,
            } => (implied_fee, weight),
            NudgeOutcome::Skipped(reason) => {
                self.record_trigger_receipt(
                    &trigger,
                    now,
                    coalesced,
                    None,
                    format!(
                        "failed-forward nudge NOT applied ({reason}) -- did not itself run a cycle"
                    ),
                );
                return;
            }
        };
        let fee_state = staged
            .fee_state
            .expect("an applied failed-forward nudge always carries staged fee state");
        let Some(cycle_state) = self.state.cycle_states.get(channel_id).cloned() else {
            self.record_trigger_receipt(
                &trigger,
                now,
                coalesced,
                None,
                "failed-forward nudge NOT applied (no paired cycle state to persist atomically)",
            );
            return;
        };

        let detail = format!(
            "failed-forward posterior nudge APPLIED: target {implied_fee} ppm, weight \
             {weight:.4} -- committed atomically with state; did not itself run a cycle"
        );
        let receipt = build_receipt(&trigger, now, coalesced, Some((&event_key, now)), detail);
        let commit = FeeCycleCommit {
            cycle_id: event_key.clone(),
            started_at: now,
            completed_at: now,
            source_commit: source_commit().to_string(),
            binary_sha256: binary_sha256().to_string(),
            state_rows: vec![revops_db::fee_runway::FeeStateRow {
                channel_id: channel_id.to_string(),
                v2_state_json: serialize_state_envelope(&cycle_state, &fee_state),
                last_update: cycle_state.last_update,
            }],
            trigger_receipt: Some(receipt),
            ..FeeCycleCommit::default()
        };

        let commit_result = self
            .store
            .as_ref()
            .expect("store presence checked before staging")
            .commit_fee_cycle(commit);
        match commit_result {
            Ok(generation) => {
                self.state
                    .fee_states
                    .insert(channel_id.to_string(), fee_state);
                self.last_failure_nudge_ts
                    .insert(channel_id.to_string(), now);
                self.applied_failure_nudges_pending
                    .insert(channel_id.to_string());
                self.state_generation = Some(generation);
            }
            Err(e) => {
                self.persistence_failures += 1;
                eprintln!(
                    "revops: failed_forward atomic commit failure #{} for channel {channel_id} \
                     at {now}: {e:#}; staged state was not installed",
                    self.persistence_failures
                );
                self.record_trigger_receipt(
                    &trigger,
                    now,
                    coalesced,
                    None,
                    "PERSISTENCE FAILED: posterior nudge was not installed; atomic state/receipt \
                     commit rolled back",
                );
            }
        }
    }

    /// Evaluate a nudge against cloned state. Nothing owned by the
    /// scheduler changes until handle_failed_forward commits the clone
    /// and its effect receipt in one transaction.
    fn stage_failure_nudge(&self, signal: &FailedForwardSignal) -> StagedFailureNudge {
        let channel_id = signal.channel_id.as_str();
        let now = signal.event_ts;

        if channel_id.is_empty() {
            return StagedFailureNudge {
                outcome: NudgeOutcome::Skipped("no outgoing channel on the event"),
                fee_state: None,
            };
        }
        if !dynamics::is_fee_relevant_failure(signal.failcode, signal.failreason.as_deref()) {
            return StagedFailureNudge {
                outcome: NudgeOutcome::Skipped("not a fee-relevant failure (audit DTS-4b)"),
                fee_state: None,
            };
        }
        if let Some(applied_ts) = self.last_fee_apply_ts.get(channel_id) {
            if *applied_ts != 0 && now - *applied_ts < FAILURE_NUDGE_GOSSIP_SETTLE_SECONDS {
                return StagedFailureNudge {
                    outcome: NudgeOutcome::Skipped(
                        "inside the gossip-settle window after our own apply",
                    ),
                    fee_state: None,
                };
            }
        }
        if let Some(last_nudge) = self.last_failure_nudge_ts.get(channel_id) {
            if *last_nudge != 0 && now - *last_nudge < FAILURE_NUDGE_MIN_INTERVAL_SECONDS {
                return StagedFailureNudge {
                    outcome: NudgeOutcome::Skipped("rate limited: already nudged this window"),
                    fee_state: None,
                };
            }
        }
        let Some(mut fee_state) = self.state.fee_states.get(channel_id).cloned() else {
            return StagedFailureNudge {
                outcome: NudgeOutcome::Skipped(
                    "no persisted DTS evidence for this channel: a failed forward must never be a \
                     channel's first posterior evidence",
                ),
                fee_state: None,
            };
        };

        // Python reads cfs.last_fee_ppm in the producer under its state
        // lock and skips when it is not positive. Rust owns that state on
        // this thread, so the equivalent read is local.
        let current_fee_ppm = fee_state.last_fee_ppm;
        if current_fee_ppm <= 0 {
            return StagedFailureNudge {
                outcome: NudgeOutcome::Skipped("channel has no positive current fee to imply from"),
                fee_state: None,
            };
        }
        let implied_fee = dynamics::failed_forward_implied_fee(current_fee_ppm);
        let weight = dynamics::failed_forward_nudge_weight(signal.amount_msat as f64 / 1000.0);
        dynamics::record_posterior_nudge(&mut fee_state.thompson, implied_fee as f64, weight, now);
        StagedFailureNudge {
            outcome: NudgeOutcome::Applied {
                implied_fee,
                weight,
            },
            fee_state: Some(fee_state),
        }
    }

    /// py `_last_fee_apply_ts[channel] = now` (8456): record that WE
    /// applied a fee, starting this channel's gossip-settle window.
    pub fn note_fee_applied(&mut self, channel_id: &str, now: i64) {
        self.last_fee_apply_ts.insert(channel_id.to_string(), now);
    }

    /// Task 44 / A2: the producer for `_handle_policy_change` (py 7871).
    ///
    /// The EFFECT was already ported ([`Self::policy_changed`] ->
    /// `revops_fees::cycle::handle_policy_change`); nothing ever called it,
    /// because nothing constructs [`CycleMsg::PolicyChanged`].
    ///
    /// Python's producer is the process that MADE the change: its policy
    /// RPC calls the handler directly. Rust cannot copy that — Python owns
    /// the `revenue-policy-*` RPCs today, so a Rust-side RPC hook would
    /// observe nothing during the whole shadow and cutover window. Instead
    /// this remembers each peer's `peer_policies.updated_at` and fires when
    /// it ADVANCES, which detects the change whoever made it. That keeps
    /// working unchanged after cutover, when Rust owns the RPC.
    ///
    /// First observation of a peer NEVER wakes: on a fresh process every
    /// peer would look "changed", which would wake the whole node once per
    /// restart — a restart is not a policy change.
    fn detect_policy_changes(&mut self, snapshot: &EvidenceSnapshot, now: i64) {
        let channel_states = snapshot.channel_states();
        let mut seen: Vec<&str> = Vec::new();
        for row in &channel_states {
            let peer_id = row.peer_id.as_str();
            if peer_id.is_empty() || seen.contains(&peer_id) {
                continue;
            }
            seen.push(peer_id);

            let Some(policy) = snapshot.policy(peer_id) else {
                continue;
            };
            match self.last_policy_updated_at.get(peer_id) {
                None => {
                    // Baseline only.
                    self.last_policy_updated_at
                        .insert(peer_id.to_string(), policy.updated_at);
                }
                Some(&prev) if policy.updated_at > prev => {
                    self.last_policy_updated_at
                        .insert(peer_id.to_string(), policy.updated_at);
                    self.handle_policy_changed(&channel_states, peer_id, now);
                }
                Some(_) => {}
            }
        }
    }

    /// [`CycleMsg::ForwardEvent`]'s handler -- fix round 1 (review finding
    /// 2): CLN's own `forward_event` notification (`main.rs`'s
    /// subscription), offered to the trigger queue alongside the existing
    /// dedup-insert (`notify::on_forward_event`), which this does not
    /// replace or gate. Recording-only, the EXACT same posture
    /// [`Self::handle_failed_forward`] carries: no fee-nudge or posterior
    /// effect runs here -- porting that effect is unported scheduler-side
    /// work, explicitly deferred to cutover.
    pub fn handle_forward_event(&mut self, channel_id: &str, now: i64) {
        let trigger = FeeTrigger::ForwardEvent {
            channel_id: channel_id.to_string(),
        };
        let outcome = self.trigger_queue.offer(trigger.clone(), now);
        if matches!(outcome, TriggerOutcome::Dropped) {
            eprintln!(
                "revops: TRIGGER DROPPED (bounded queue at capacity): forward_event channel \
                 {channel_id} at {now}"
            );
            self.record_trigger_receipt(
                &trigger,
                now,
                false,
                None,
                "DROPPED: bounded trigger queue at capacity (backpressure)",
            );
            return;
        }
        self.record_trigger_receipt(
            &trigger,
            now,
            matches!(outcome, TriggerOutcome::Coalesced),
            None,
            "forward_event received; fee-nudge/posterior effect application is not yet wired \
             to the scheduler (recording only) -- did not itself run a cycle",
        );
    }

    /// Live-review finding F1: a preparation REFUSAL (timeout, malformed
    /// evidence, ambiguity, missing peer/channel, or a policy/config read
    /// failure) is not just logged -- it is offered to the SAME
    /// channel-scoped trigger discipline (so backpressure accounting stays
    /// unified across ready/refused occurrences) and produces a durable
    /// receipt, so the refusal survives a restart and can never be
    /// silently re-read as "nothing happened". No RNG draw, no state row,
    /// no action -- ever.
    pub fn handle_new_channel_refused(
        &mut self,
        peer_id: String,
        channel_hint: String,
        event_ts: i64,
        reason: String,
    ) {
        let trigger = FeeTrigger::NewChannel {
            channel_id: if channel_hint.is_empty() {
                peer_id.clone()
            } else {
                channel_hint.clone()
            },
        };
        let outcome = self.trigger_queue.offer(trigger.clone(), event_ts);
        let coalesced = matches!(outcome, TriggerOutcome::Coalesced);
        if matches!(outcome, TriggerOutcome::Dropped) {
            eprintln!(
                "revops: TRIGGER DROPPED (bounded queue at capacity): new_channel REFUSAL for \
                 peer {peer_id} at {event_ts} ({reason})"
            );
        } else {
            eprintln!(
                "revops: A3 NEW-CHANNEL PREPARATION REFUSED for peer {peer_id} \
                 (channel_hint={channel_hint:?}) at {event_ts}: {reason}"
            );
        }
        self.dispatch_a3_receipt(
            &trigger,
            event_ts,
            coalesced,
            None,
            format!(
                "REFUSED (no decision, no state, no action): {reason}{}",
                if matches!(outcome, TriggerOutcome::Dropped) {
                    " [ALSO DROPPED: bounded trigger queue at capacity]"
                } else {
                    ""
                }
            ),
            "new_channel preparation-refusal receipt",
        );
    }

    /// [`CycleMsg::NewChannel`]'s handler -- Task 44 / A3's owner-thread
    /// decision + atomic out-of-cycle commit.
    ///
    /// STRICTLY offer-first (contract §3.1, pre-audit hazard fix): the
    /// trigger is offered to the bounded/coalescing discipline BEFORE any
    /// decision, RNG draw, or state mutation runs. A Dropped occurrence
    /// returns with ZERO effect. This is deliberately NOT
    /// [`Self::handle_failed_forward`]'s ordering -- that legacy path
    /// applies its effect BEFORE offering, which means a Dropped outcome
    /// there still leaves a mutated in-memory nudge; that is a recorded
    /// follow-up finding against A1/A2, not A3's to inherit or fix here
    /// (contract §6: no changes to A1/A2 behavior).
    pub fn handle_new_channel(&mut self, prepared: PreparedInitialFee) {
        let channel_id = prepared.channel.channel_id.clone();
        let now = prepared.event_ts;
        let trigger = FeeTrigger::NewChannel {
            channel_id: channel_id.clone(),
        };

        let outcome = self.trigger_queue.offer(trigger.clone(), now);
        // Live-review finding F2: ONLY a newly `Enqueued` occurrence may
        // reach decision/effect. `Coalesced` means "an earlier occurrence
        // for this exact channel is already pending" -- treating it as
        // "informational, keep going" would draw the single owner RNG
        // stream a second time and could create a duplicate nudge/action
        // for the same channel. Both `Dropped` and `Coalesced` are
        // therefore zero-RNG, zero-mutation, receipt-only outcomes; the
        // ONLY difference between them is the log/detail text.
        if !matches!(outcome, TriggerOutcome::Enqueued) {
            let (headline, detail): (&str, String) = if matches!(outcome, TriggerOutcome::Dropped) {
                (
                    "DROPPED (bounded queue at capacity)",
                    "DROPPED: bounded trigger queue at capacity (backpressure); no decision was \
                     made"
                        .to_string(),
                )
            } else {
                (
                    "COALESCED (an earlier occurrence for this channel is already pending)",
                    "COALESCED: an earlier new_channel occurrence for this channel is already \
                     pending; this occurrence made NO decision and drew NO entropy -- the \
                     pending occurrence's outcome covers it"
                        .to_string(),
                )
            };
            eprintln!("revops: TRIGGER {headline}: new_channel channel {channel_id} at {now}");
            // Stand-alone receipts outside the atomic commit -- by
            // definition neither has an effect to be atomic with
            // (contract's pre-audit hazard fix, point 2, extended to
            // Coalesced by live-review finding F2). Dispatched off-owner
            // (F5): a receipt write must not block the owner either.
            self.dispatch_a3_receipt(
                &trigger,
                now,
                matches!(outcome, TriggerOutcome::Coalesced),
                None,
                detail,
                "new_channel dropped/coalesced receipt",
            );
            return;
        }

        // Task 42 correction F2 (fail-closed): out-of-cycle commits are
        // admissible ONLY once the SeedOnce bootstrap is complete. Before
        // the first hydration an A3 commit would make the virgin store
        // nonvirgin with a single channel's partial state (hydration then
        // skips the complete Python import FOREVER); while provenance is
        // pending it would take the generation the seed is bound to and
        // orphan it permanently. Refuse; the event replays later.
        if let Some(bootstrap) = self.out_of_cycle_commit_refusal() {
            eprintln!(
                "revops: A3 NEW-CHANNEL REFUSED (fail-closed): SeedOnce bootstrap is \
                 {bootstrap:?}; no out-of-cycle commit may run before a complete, \
                 committed bootstrap (channel {channel_id} at {now})"
            );
            self.dispatch_a3_receipt(
                &trigger,
                now,
                false,
                None,
                format!(
                    "REFUSED (no decision, no state, no action): SeedOnce bootstrap is \
                     {bootstrap:?}; out-of-cycle commits require a complete committed \
                     bootstrap"
                ),
                "new_channel bootstrap-incomplete refusal receipt",
            );
            return;
        }

        // F5 (same-channel pending/race, fail-closed): while ANY store
        // operation for this channel is still in flight, a further
        // occurrence -- one the drained trigger queue no longer coalesces
        // -- must be refused rather than processed. Refusing is safe (the
        // in-flight occurrence's own outcome governs the channel, and a
        // genuinely new event replays later); processing would overwrite
        // the in-flight bookkeeping, orphan its result into a conflict,
        // and double-decide the channel.
        if self.pending_initial_fees.contains_key(&channel_id) {
            eprintln!(
                "revops: A3 NEW-CHANNEL REFUSED (fail-closed): a store operation for channel \
                 {channel_id} is already in flight; this occurrence at {now} made no decision \
                 and drew no entropy"
            );
            self.dispatch_a3_receipt(
                &trigger,
                now,
                false,
                None,
                "REFUSED: a store operation for this channel is already in flight \
                 (fail-closed); no decision was made, no entropy was drawn",
                "new_channel in-flight race refusal receipt",
            );
            return;
        }

        // Live-review finding F6: an unavailable Rust-owned store must be
        // rejected BEFORE decision/RNG -- not discovered only after a
        // decision (and an RNG draw) already happened, with the result
        // then silently discarded. No store means this occurrence can
        // never be durable, so it must never consume entropy or touch
        // state either.
        if self.store.is_none() {
            self.persistence_failures += 1;
            eprintln!(
                "revops: A3 NEW-CHANNEL REFUSED (failure #{}): no Rust-owned store is \
                 configured; channel {channel_id} at {now} -- no decision was made, no entropy \
                 was drawn",
                self.persistence_failures
            );
            self.dispatch_a3_receipt(
                &trigger,
                now,
                false,
                None,
                "REFUSED: no Rust-owned store configured; no decision was made",
                "new_channel no-store refusal receipt",
            );
            return;
        }

        // F5: without a result sink the off-owner store results could
        // never return to the owner, so the occurrence must fail CLOSED
        // before any decision/RNG -- never dispatch work whose outcome is
        // undeliverable. Production wiring always sets one.
        let Some(result_sink) = self.result_sink.clone() else {
            self.persistence_failures += 1;
            eprintln!(
                "revops: A3 NEW-CHANNEL REFUSED (failure #{}): owner has no result sink for \
                 commit-result routing; channel {channel_id} at {now} -- no decision was made, \
                 no entropy was drawn",
                self.persistence_failures
            );
            self.dispatch_a3_receipt(
                &trigger,
                now,
                false,
                None,
                "REFUSED: owner has no result sink for commit-result routing; no decision was \
                 made",
                "new_channel no-result-sink refusal receipt",
            );
            return;
        };

        // Live-review finding F3: cross-restart event idempotency. Checked
        // BEFORE decision/RNG -- a replay of the SAME event (same
        // resolved channel, same transition, same event timestamp,
        // possibly after a restart) must consume ZERO entropy and create
        // ZERO duplicate state/action. `event_key` is derived purely from
        // the event's own content (never wall-clock-at-processing or a
        // process id), so a genuine replay recomputes the SAME key.
        //
        // F5: the check itself is dispatched OFF-owner; the frozen
        // preparation parks in `pending_initial_fees` until the answer
        // comes back as an identity-bound message. No decision, RNG draw,
        // or state mutation has happened yet.
        self.initial_fee_dispatch_seq += 1;
        let generation = self.initial_fee_dispatch_seq;
        let event_key = prepared.event_key.clone();
        self.pending_initial_fees.insert(
            channel_id.clone(),
            PendingInitialFee::CheckingIdempotency {
                event_key: event_key.clone(),
                generation,
                prepared: Box::new(prepared),
                dispatched_at: crate::now_unix(),
            },
        );
        let store = self.store.as_ref().expect("checked store.is_none() above");
        let reply_key = event_key.clone();
        let callback_channel_id = channel_id.clone();
        let callback_reply_key = reply_key.clone();
        let launch = store.dispatch_cycle_exists_with_generation(
            event_key,
            Box::new(move |result| {
                if !result_sink.deliver(InitialFeeStoreResult::Idempotency {
                    channel_id: callback_channel_id,
                    event_key: callback_reply_key,
                    generation,
                    result: result.map_err(|e| format!("{e:#}")),
                }) {
                    eprintln!("revops: A3 idempotency result lost: owner ingress closed");
                }
            }),
        );
        if let Err(e) = launch {
            self.handle_idempotency_result(
                channel_id,
                reply_key,
                generation,
                Err(format!("{e:#}")),
            );
        }
    }

    /// [`CycleMsg::InitialFeeStoreResult`]'s handler (live-review finding
    /// F5): the owner-side continuation for each off-owner store
    /// operation. Runs on the owner thread like every other `CycleMsg`,
    /// so no lock is ever needed -- the pending map is only touched here
    /// and in [`Self::handle_new_channel`]. `clock` serves the F7
    /// sequencing rule: once the pending map clears, a prepared cycle
    /// deferred behind the in-flight A3 occurrence runs immediately (and
    /// reads ITS OWN fresh clock), so the next cycle always sees the
    /// synchronized post-A3 state, mirroring Python's `_state_lock`
    /// serialization.
    pub fn handle_initial_fee_store_result(
        &mut self,
        result: InitialFeeStoreResult,
        clock: &mut dyn FnMut() -> i64,
    ) {
        match result {
            InitialFeeStoreResult::Receipt { context, result } => {
                if let Err(e) = result {
                    // Nothing staged depends on a stand-alone receipt;
                    // its loss is loud and counted, never silent
                    // (finding F1's durability posture -- the refusal
                    // stays enforced in-process either way).
                    self.persistence_failures += 1;
                    eprintln!(
                        "revops: A3 receipt record FAILED (failure #{}): {e}: {context}",
                        self.persistence_failures
                    );
                }
            }
            InitialFeeStoreResult::Idempotency {
                channel_id,
                event_key,
                generation,
                result,
            } => self.handle_idempotency_result(channel_id, event_key, generation, result),
            InitialFeeStoreResult::Commit {
                channel_id,
                event_key,
                generation,
                result,
            } => self.handle_commit_result(channel_id, event_key, generation, result),
            InitialFeeStoreResult::Retention { result } => {
                self.retention_in_flight = false;
                match result {
                    Ok(report) => {
                        self.retention_cursor = report.next_cursor;
                    }
                    Err(e) => {
                        // Loud + counted red, never reset -- and never
                        // any effect on cycle outcomes (the sweep is pure
                        // Class-W housekeeping).
                        self.retention_failures += 1;
                        eprintln!(
                            "revops: RETENTION SWEEP FAILED (failure #{}): {e}",
                            self.retention_failures
                        );
                    }
                }
            }
        }
        // F7 sequencing: the A3 occurrence(s) settled -- run the cycle
        // that was deferred behind them, on this same owner thread, so it
        // consumes the just-synchronized state.
        if self.pending_initial_fees.is_empty() {
            if let Some(deferred) = self.deferred_cycle.take() {
                eprintln!(
                    "revops: running the prepared cycle deferred behind an in-flight A3 \
                     commit (the cycle now sees the synchronized post-A3 state)"
                );
                let outcome = self.run_cycle(*deferred, clock);
                if let Some(completion) = self.deferred_cycle_ack.take() {
                    let _ = completion.send(cycle_completion(self, &outcome));
                }
            }
        }
    }

    /// Phase-B continuation: the off-owner idempotency answer for a
    /// parked preparation. Only an identity-matched answer may unpark it;
    /// only `Ok(false)` (genuinely new) proceeds to decision/RNG.
    fn handle_idempotency_result(
        &mut self,
        channel_id: String,
        event_key: String,
        generation: u64,
        result: Result<(bool, u64), String>,
    ) {
        // Exact identity binding (the binding recovery contract): the
        // result must match the awaited occurrence's phase AND event_key
        // AND dispatch generation. Anything else -- stale, forged,
        // foreign -- is discarded as a red conflict WITHOUT touching the
        // pending entry, whose genuine result is still owed.
        let expected = match self.pending_initial_fees.get(&channel_id) {
            Some(PendingInitialFee::CheckingIdempotency { .. }) => {
                let entry = &self.pending_initial_fees[&channel_id];
                entry.event_key() == event_key && entry.generation() == generation
            }
            _ => false,
        };
        if !expected {
            self.initial_fee_conflicts += 1;
            eprintln!(
                "revops: A3 CONFLICT (conflict #{}): idempotency result for channel \
                 {channel_id} (event_key={event_key}, dispatch generation {generation}) matches \
                 no awaited occurrence; discarded fail-closed",
                self.initial_fee_conflicts
            );
            return;
        }
        let Some(PendingInitialFee::CheckingIdempotency {
            event_key: _matched_key,
            generation: _matched_generation,
            prepared,
            dispatched_at: _,
        }) = self.pending_initial_fees.remove(&channel_id)
        else {
            unreachable!("matched CheckingIdempotency above");
        };
        let now = prepared.event_ts;
        let trigger = FeeTrigger::NewChannel {
            channel_id: channel_id.clone(),
        };
        match result {
            Ok((true, _store_generation)) => {
                eprintln!(
                    "revops: A3 NEW-CHANNEL DUPLICATE (event_key={}): an identical event was \
                     already committed; no second decision, no second entropy draw, no second \
                     action",
                    prepared.event_key
                );
                self.dispatch_a3_receipt(
                    &trigger,
                    now,
                    false,
                    None,
                    format!(
                        "DUPLICATE (event_key={}): an identical event was already durably \
                         committed; no decision was made, no entropy was drawn",
                        prepared.event_key
                    ),
                    "new_channel duplicate receipt",
                );
            }
            Ok((false, store_generation)) => {
                // F7: the CAS basis. The owner's own tracked view wins
                // when it exists -- it is CURRENT as of this message
                // (any interleaved scheduled commit already updated it),
                // while the answer's generation was read at dispatch
                // time and may be one epoch stale. With no tracked view
                // yet (e.g. RehydratePerCycle before any A3 commit, or
                // SeedOnce before first hydration) adopt the store's.
                // Either way the guarded commit re-checks atomically.
                let expected_prior = match self.state_generation {
                    Some(tracked) => {
                        if tracked != store_generation {
                            eprintln!(
                                "revops: A3 note: idempotency answer read generation \
                                 {store_generation} but the owner has since advanced to \
                                 {tracked}; deciding against {tracked} (the guarded commit \
                                 CASes on it)"
                            );
                        }
                        tracked
                    }
                    None => {
                        self.state_generation = Some(store_generation);
                        store_generation
                    }
                };
                self.decide_and_dispatch_commit(prepared, generation, expected_prior)
            }
            Err(e) => {
                // Fail CLOSED: an unreadable idempotency check must never
                // be treated as "probably new" -- that could silently
                // duplicate an action. Refuse instead (live-review F6's
                // "red typed refusal" posture, extended to this check).
                self.persistence_failures += 1;
                eprintln!(
                    "revops: A3 NEW-CHANNEL REFUSED (failure #{}): idempotency check failed \
                     ({e}) for event_key={}; refusing rather than risking a duplicate",
                    self.persistence_failures, prepared.event_key
                );
                self.dispatch_a3_receipt(
                    &trigger,
                    now,
                    false,
                    None,
                    format!(
                        "REFUSED: idempotency check failed ({e}); no decision was made, no \
                         entropy was drawn"
                    ),
                    "new_channel idempotency-failure refusal receipt",
                );
            }
        }
    }

    /// Phase-C continuation: the off-owner atomic-commit answer. Staged
    /// clones install ONLY here, only on `Ok`, only when the result's
    /// identity matches the pending entry.
    fn handle_commit_result(
        &mut self,
        channel_id: String,
        event_key: String,
        generation: u64,
        result: Result<revops_db::fee_runway::GuardedCommitOutcome, String>,
    ) {
        use revops_db::fee_runway::GuardedCommitOutcome;
        // Exact identity binding, same rule as the idempotency phase: a
        // generation/event_key mismatch at result-time is a CONFLICT --
        // discarded fail-closed (no install, no discard of the staged
        // state, no persistence-failure count), because the awaited
        // occurrence's genuine result is still owed.
        let expected = match self.pending_initial_fees.get(&channel_id) {
            Some(PendingInitialFee::Committing { .. }) => {
                let entry = &self.pending_initial_fees[&channel_id];
                entry.event_key() == event_key && entry.generation() == generation
            }
            _ => false,
        };
        if !expected {
            self.initial_fee_conflicts += 1;
            eprintln!(
                "revops: A3 CONFLICT (conflict #{}): commit result for channel {channel_id} \
                 (event_key={event_key}, dispatch generation {generation}) matches no awaited \
                 occurrence; discarded fail-closed -- nothing was installed",
                self.initial_fee_conflicts
            );
            return;
        }
        let Some(PendingInitialFee::Committing {
            event_key: _matched_key,
            generation: _matched_generation,
            expected_prior_generation,
            staged,
            dispatched_at: _,
        }) = self.pending_initial_fees.remove(&channel_id)
        else {
            unreachable!("matched Committing above");
        };
        match result {
            Ok(GuardedCommitOutcome::Committed(committed_generation)) => {
                // F7 install rule: the commit must be exactly the next
                // generation after the decision's basis, AND the owner
                // must not have advanced past that basis in the meantime
                // (the RunPrepared deferral makes an advance structurally
                // impossible, so this is a fail-closed invariant check,
                // not an expected path).
                let owner_unadvanced = self.state_generation == Some(expected_prior_generation);
                if owner_unadvanced && committed_generation == expected_prior_generation + 1 {
                    self.state_generation = Some(committed_generation);
                    if let Some(staged) = staged {
                        let (fee, cycle) = *staged;
                        self.state.fee_states.insert(channel_id.clone(), fee);
                        self.state.cycle_states.insert(channel_id, cycle);
                    }
                } else {
                    self.initial_fee_conflicts += 1;
                    eprintln!(
                        "revops: A3 CONFLICT (conflict #{}): commit for channel {channel_id} \
                         landed as generation {committed_generation} against decision basis \
                         {expected_prior_generation} (owner now at {:?}); staged state \
                         DISCARDED fail-closed -- the store row stands as recorded evidence, \
                         the in-memory state keeps the newer epoch",
                        self.initial_fee_conflicts, self.state_generation
                    );
                    // Adopt the store's actual generation so the next
                    // occurrence CASes against reality instead of
                    // conflicting forever.
                    if self
                        .state_generation
                        .is_some_and(|g| g < committed_generation)
                    {
                        self.state_generation = Some(committed_generation);
                    }
                }
            }
            Ok(GuardedCommitOutcome::GenerationConflict { expected, found }) => {
                self.initial_fee_conflicts += 1;
                eprintln!(
                    "revops: A3 CONFLICT (conflict #{}): guarded commit for channel \
                     {channel_id} refused by the store -- state generation advanced from \
                     {expected} to {found} after the decision; NOTHING was written, staged \
                     state DISCARDED fail-closed",
                    self.initial_fee_conflicts
                );
                // The store is authoritative about its own generation.
                self.state_generation = Some(found);
            }
            Err(e) => {
                self.persistence_failures += 1;
                eprintln!(
                    "revops: A3 NEW-CHANNEL COMMIT FAILED (failure #{}): {e}; NO state was \
                     installed, NO action is authoritative, and this receipt does not claim an \
                     applied fee",
                    self.persistence_failures
                );
            }
        }
    }

    /// The decision + atomic-commit build for one genuinely-new (offer
    /// Enqueued, idempotency-cleared) occurrence -- the only place on the
    /// A3 path that draws the single owner RNG. The commit is dispatched
    /// OFF-owner (F5); the staged clones install in
    /// [`Self::handle_commit_result`] on success only.
    fn decide_and_dispatch_commit(
        &mut self,
        prepared: Box<PreparedInitialFee>,
        generation: u64,
        expected_prior_generation: u64,
    ) {
        let prepared = *prepared;
        let channel_id = prepared.channel.channel_id.clone();
        let now = prepared.event_ts;
        let trigger = FeeTrigger::NewChannel {
            channel_id: channel_id.clone(),
        };

        let existing_fee = self.state.fee_states.get(&channel_id).cloned();
        let existing_cycle = self.state.cycle_states.get(&channel_id).cloned();

        let governed = self.governor.governed_deps(&prepared.cfg);
        let authorizer = GovernedFeeAuthorizer::new(&governed);
        let decision = decide_initial_fee(
            &prepared,
            existing_fee.as_ref(),
            existing_cycle.as_ref(),
            &authorizer,
            &mut self.rng,
        );

        // Live-review finding F4: the receipt/outcome text must state
        // exactly what happened and must NEVER be readable as a completed
        // live broadcast. `decision.reason_code` is the FEE reason
        // identity (`channel_open` / `policy_static`); the governor's own
        // `reason_code` (inside `GovernorDenied`/carried in
        // `governed_trace`) is separate authorization metadata and is
        // named explicitly as such, never substituted for the fee reason.
        let detail = match &decision.outcome {
            InitialFeeOutcome::Passive => {
                "PASSIVE policy: no fee action, no DTS state created".to_string()
            }
            InitialFeeOutcome::GovernorDenied {
                reason_code: governor_reason_code,
            } => format!(
                "SHADOW MODE, NOT APPLIED: fee_reason={} governor_denied \
                 (governor_reason_code={governor_reason_code}); no action, no post-broadcast \
                 state sync (a gossip-derived prior seed may still be recorded)",
                decision.reason_code
            ),
            InitialFeeOutcome::WouldBroadcast {
                reason_code: governor_reason_code,
            } => format!(
                "SHADOW MODE, NOT APPLIED: would-broadcast RECORDED ONLY \
                 (fee_reason={}, governor_reason_code={governor_reason_code}); no live \
                 mutation, no live broadcast call was made",
                decision.reason_code
            ),
        };

        let has_state_row = decision.fee_state.is_some() && decision.cycle_state.is_some();
        // Only the one Enqueued occurrence reaches this point (Dropped/
        // Coalesced returned before any dispatch) -- this receipt is
        // never coalesced.
        let receipt = build_receipt(&trigger, now, false, None, detail);

        let mut commit = FeeCycleCommit {
            // Live-review finding F3: STABLE, content-derived identity --
            // never PID/wall-clock-at-processing. `rust_fee_cycles.
            // cycle_id` is a PRIMARY KEY, so even a raced duplicate
            // (idempotency check above passed, but a second copy of the
            // SAME event reaches this point before the first commits) is
            // rejected by the transaction itself, not just by the earlier
            // advisory check.
            cycle_id: prepared.event_key.clone(),
            started_at: now,
            completed_at: now,
            source_commit: source_commit().to_string(),
            binary_sha256: binary_sha256().to_string(),
            trigger_receipt: Some(receipt),
            ..FeeCycleCommit::default()
        };

        if let (Some(fee), Some(cycle)) = (&decision.fee_state, &decision.cycle_state) {
            commit.state_rows.push(revops_db::fee_runway::FeeStateRow {
                channel_id: channel_id.clone(),
                v2_state_json: serialize_state_envelope(cycle, fee),
                last_update: cycle.last_update,
            });
        }
        if let Some(action) = &decision.action {
            commit.requests.push(PreparedFeeActionRow {
                channel_id: channel_id.clone(),
                idempotency_key: decision
                    .governed_trace
                    .as_ref()
                    .map(|t| t.idempotency_key.clone()),
                old_fee_ppm: action.old_fee_ppm,
                new_fee_ppm: action.decision.clamped_fee_ppm,
                feebase_msat: action.expected_base_fee_msat,
                htlcmin_msat: action.request.htlcmin.map(|v| v as i64),
                htlcmax_msat: action.request.htlcmax.map(|v| v as i64),
                message: action.decision.message.clone(),
                at: now,
            });
        }
        if let Some(trace) = &decision.governed_trace {
            commit.governor.push(GovernorAuditRow {
                channel_id: channel_id.clone(),
                authorized: trace.authorized,
                reason_code: trace.reason_code.clone(),
                intent_id: trace.intent_id.clone(),
                idempotency_key: trace.idempotency_key.clone(),
                at: now,
            });
        }
        commit.outcomes.push(ShadowCycleOutcomeRow {
            cycle_ts: now,
            channel_id: channel_id.clone(),
            would_broadcast: decision.action.is_some(),
            has_algorithm_values: has_state_row,
            disposition: Some(
                match &decision.outcome {
                    InitialFeeOutcome::Passive => "new_channel_passive",
                    InitialFeeOutcome::GovernorDenied { .. } => "new_channel_governor_denied",
                    // F4: "would_broadcast", never "broadcast" -- shadow
                    // mode never applies a live fee change.
                    InitialFeeOutcome::WouldBroadcast { .. } => "new_channel_would_broadcast",
                }
                .to_string(),
            ),
            skip_gate_comparable: false,
        });

        // Stage on clones, install ONLY after a successful atomic commit
        // (contract §3.2) -- and, per F5, the commit itself runs
        // OFF-owner: the staged pair parks in the pending map and
        // installs in [`Self::handle_commit_result`] when (and only when)
        // the identity-bound success message comes back. A commit failure
        // must never leave mutated in-memory state installed.
        let staged = match (decision.fee_state, decision.cycle_state) {
            (Some(fee), Some(cycle)) => Some(Box::new((fee, cycle))),
            _ => None,
        };
        self.pending_initial_fees.insert(
            channel_id.clone(),
            PendingInitialFee::Committing {
                event_key: prepared.event_key.clone(),
                generation,
                expected_prior_generation,
                staged,
                dispatched_at: crate::now_unix(),
            },
        );
        // Both were verified in `handle_new_channel` before the
        // idempotency dispatch and are never unset; fail closed anyway
        // rather than panic the owner thread if that invariant ever
        // breaks.
        let (Some(result_sink), Some(store)) = (self.result_sink.clone(), self.store.as_ref())
        else {
            self.pending_initial_fees.remove(&channel_id);
            self.persistence_failures += 1;
            eprintln!(
                "revops: A3 NEW-CHANNEL COMMIT NOT DISPATCHED (failure #{}): store or \
                 result sink vanished mid-flight for channel {channel_id}; nothing was \
                 committed, nothing was installed",
                self.persistence_failures
            );
            return;
        };
        let event_key = prepared.event_key.clone();
        let callback_channel_id = channel_id.clone();
        let callback_event_key = event_key.clone();
        let launch = store.dispatch_commit_fee_cycle_guarded(
            commit,
            expected_prior_generation,
            Box::new(move |result| {
                if !result_sink.deliver(InitialFeeStoreResult::Commit {
                    channel_id: callback_channel_id,
                    event_key: callback_event_key,
                    generation,
                    result: result.map_err(|e| format!("{e:#}")),
                }) {
                    eprintln!("revops: A3 commit result lost: owner ingress closed");
                }
            }),
        );
        if let Err(e) = launch {
            self.handle_commit_result(channel_id, event_key, generation, Err(format!("{e:#}")));
        }
    }

    /// [`CycleMsg::RunPrepared`]'s owner-side entry. F7 sequencing rule
    /// (Python-parity): while ANY A3 store result is pending, the full
    /// cycle is DEFERRED -- Python's `_state_lock` serializes
    /// `_handle_channel_open` against the cycle, so the cycle must see
    /// the synchronized post-A3 state, never a pre-install epoch it
    /// would commit over the in-flight A3 commit (orphaning it into a
    /// CAS conflict). Returns `None` when deferred; the deferred cycle
    /// runs from [`Self::handle_initial_fee_store_result`] the moment the
    /// pending map clears. Bounded to ONE slot: a newer prepared
    /// snapshot supersedes an older deferred one (the newer inputs are
    /// strictly fresher evidence for the SAME per-cycle evaluation),
    /// loudly and counted -- never an unbounded queue, never silent.
    pub fn run_or_defer_cycle(
        &mut self,
        prepared: Box<PreparedCycle>,
        clock: &mut dyn FnMut() -> i64,
    ) -> Option<CycleOutcome> {
        if self.pending_initial_fees.is_empty() {
            return Some(self.run_cycle(*prepared, clock));
        }
        if self.deferred_cycle.is_some() {
            self.deferred_cycles_superseded += 1;
            eprintln!(
                "revops: DEFERRED CYCLE SUPERSEDED (#{}): a newer prepared cycle arrived while \
                 an A3 store result is still pending; the older deferred inputs are dropped \
                 (the newer snapshot covers the same evaluation)",
                self.deferred_cycles_superseded
            );
        } else {
            eprintln!(
                "revops: prepared cycle DEFERRED behind {} in-flight A3 store result(s); it \
                 runs as soon as they settle",
                self.pending_initial_fees.len()
            );
        }
        self.deferred_cycle = Some(prepared);
        None
    }

    pub fn run_or_defer_cycle_with_ack(
        &mut self,
        prepared: Box<PreparedCycle>,
        clock: &mut dyn FnMut() -> i64,
        completion: tokio::sync::oneshot::Sender<Result<FeeCycleCompletion, String>>,
    ) {
        let prior_completion = if !self.pending_initial_fees.is_empty() {
            self.deferred_cycle_ack.take()
        } else {
            None
        };
        match self.run_or_defer_cycle(prepared, clock) {
            Some(outcome) => {
                let _ = completion.send(cycle_completion(self, &outcome));
            }
            None => {
                if let Some(prior) = prior_completion {
                    let _ =
                        prior.send(Err("deferred cycle superseded before execution".to_string()));
                }
                self.deferred_cycle_ack = Some(completion);
            }
        }
    }

    /// F7: deferred prepared cycles superseded by a newer one (red
    /// counter; see the field doc).
    pub fn deferred_cycles_superseded(&self) -> u64 {
        self.deferred_cycles_superseded
    }

    /// F5: A3's stand-alone receipts (refusals, dropped/coalesced,
    /// duplicates) are dispatched off-owner like every other A3 store
    /// interaction -- a receipt write against a stalled store must not
    /// wedge the owner either. The write's own failure comes back as an
    /// [`InitialFeeStoreResult::Receipt`] (loud + counted) when a
    /// result sink is wired; without one it is logged from the dispatch
    /// thread. Without a store this is a no-op, exactly like
    /// [`Self::record_trigger_receipt`]'s posture (the refusal stays
    /// loudly logged by the caller and enforced in-process).
    #[allow(clippy::too_many_arguments)]
    fn dispatch_a3_receipt(
        &mut self,
        trigger: &FeeTrigger,
        received_at: i64,
        coalesced: bool,
        cycle: Option<(&str, i64)>,
        detail: impl Into<String>,
        context: &str,
    ) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        let row = build_receipt(trigger, received_at, coalesced, cycle, detail);
        let context = context.to_string();
        let launch = match self.result_sink.clone() {
            Some(result_sink) => {
                let callback_context = context.clone();
                store.dispatch_record_trigger_event(
                    row,
                    Box::new(move |result| {
                        if !result_sink.deliver(InitialFeeStoreResult::Receipt {
                            context: callback_context,
                            result: result.map_err(|e| format!("{e:#}")),
                        }) {
                            eprintln!("revops: A3 receipt result lost: owner ingress closed");
                        }
                    }),
                )
            }
            None => {
                let callback_context = context.clone();
                store.dispatch_record_trigger_event(
                    row,
                    Box::new(move |result| {
                        if let Err(e) = result {
                            eprintln!(
                                "revops: A3 receipt record failed ({e:#}): {callback_context}"
                            );
                        }
                    }),
                )
            }
        };
        if let Err(e) = launch {
            self.persistence_failures += 1;
            eprintln!(
                "revops: A3 receipt dispatch FAILED (failure #{}): {e:#}: {context}",
                self.persistence_failures
            );
        }
    }

    /// Total triggers ever dropped for backpressure (Task 6's red
    /// counter, alongside [`Self::persistence_failures`]).
    pub fn trigger_queue_dropped_total(&self) -> u64 {
        self.trigger_queue.dropped_total()
    }

    // Integration-test seam: a real scheduled cycle drains both pieces
    // together before evaluating. Tests use this only to isolate cooldown
    // boundary semantics from pending-trigger coalescing.
    #[doc(hidden)]
    pub fn drain_pending_triggers_for_test(&mut self) {
        self.trigger_queue.drain_all();
        self.applied_failure_nudges_pending.clear();
    }

    /// [`CycleMsg::Query`]'s handler -- the `revenue-r-fee-debug` RPC's
    /// response body (see [`FeeDebugQuery`]'s doc comment for the exact
    /// shape of each variant). Read-only, no IO: answers straight out of
    /// the owned `ControllerState`.
    pub fn fee_debug(&self, query: &FeeDebugQuery) -> serde_json::Value {
        match query {
            FeeDebugQuery::Channel(channel_id) => match self.state.dts_summary(channel_id) {
                Some(summary) => summary.to_serde_json(),
                None => serde_json::json!({
                    "error": format!("no fee/cycle state for channel_id {channel_id}")
                }),
            },
            FeeDebugQuery::Summary => {
                let mut channel_ids: std::collections::BTreeSet<&String> =
                    std::collections::BTreeSet::new();
                channel_ids.extend(self.state.fee_states.keys());
                channel_ids.extend(self.state.cycle_states.keys());
                let mut channels = serde_json::Map::new();
                for channel_id in channel_ids {
                    if let Some(summary) = self.state.dts_summary(channel_id) {
                        channels.insert(channel_id.clone(), summary.to_serde_json());
                    }
                }
                let d = &self.state.last_decision_summary;
                serde_json::json!({
                    "last_cycle_decision": {
                        "action": d.action,
                        "reason": d.reason,
                        "dominant_input": d.dominant_input,
                        "safety_block": d.safety_block,
                    },
                    "channels": channels,
                    // Task 6: bounded trigger-queue observability.
                    "trigger_queue": {
                        "pending": self.trigger_queue.pending_len(),
                        "dropped_total": self.trigger_queue.dropped_total(),
                    },
                })
            }
            FeeDebugQuery::RunwayCounters => {
                // Task 59 §3.6: warn (visibility only) when the oldest
                // in-flight A3 occurrence crosses the threshold.
                let oldest_pending_age = self.oldest_pending_age_seconds(crate::now_unix());
                if let Some(age) = oldest_pending_age {
                    if age > A3_PENDING_AGE_WARN_SECONDS {
                        eprintln!(
                            "revops: A3 occurrence pending for {age}s (warn threshold \
                             {A3_PENDING_AGE_WARN_SECONDS}s) -- an off-owner store result \
                             is outstanding; visibility only, no cancellation (see loop \
                             health)"
                        );
                    }
                }
                serde_json::json!({
                "lifecycle": match self.lifecycle {
                    StateLifecycle::RehydratePerCycle => "rehydrate_per_cycle",
                    StateLifecycle::SeedOnce => "seed_once",
                },
                "hydrated_once": self.hydrated_once,
                "seed_refused": self.seed_refused,
                "pending_seed": self.pending_seed.is_some(),
                "bootstrap_state": format!("{:?}", self.bootstrap_state()),
                "persistence_failures": self.persistence_failures,
                "trigger_queue": {
                    "pending": self.trigger_queue.pending_len(),
                    "dropped_total": self.trigger_queue.dropped_total(),
                },
                // Task 44 / A3, live-review finding F5: in-flight
                // occurrences awaiting an off-owner store result, and the
                // red identity-mismatch conflict counter.
                "initial_fee": {
                    "pending": self.pending_initial_fees.len(),
                    // Task 59 §3.6: stuck-pending visibility (there is
                    // deliberately no cancellation to pair with it).
                    "oldest_pending_age_seconds": oldest_pending_age,
                    "conflicts": self.initial_fee_conflicts,
                    // F7 observability: the CAS basis and the deferral
                    // bookkeeping.
                    "state_generation": self.state_generation,
                    "deferred_cycle_pending": self.deferred_cycle.is_some(),
                    "deferred_cycles_superseded": self.deferred_cycles_superseded,
                },
                "last_cycle": {
                    "at": self.last_cycle_at,
                    "outcome": self.last_cycle_outcome,
                },
                "last_profile": self.last_profile,
                "governor_ledger_open": self.governor.ledger_open(),
                // Task 59: never-reset red retention counter + in-flight
                // sweep visibility.
                "retention": {
                    "failures": self.retention_failures,
                    "in_flight": self.retention_in_flight,
                },
                })
            }
        }
    }
}

/// Cheap handle to the running scheduler (stored in `main.rs`' `State`
/// for T7's RPC/wake senders).
pub struct SchedulerHandle {
    /// Owner-thread channel (cycle messages; T7's debug/wake variants).
    pub tx: SchedulerIngress,
}

/// Completed result of one serialized fee cycle. Generation and completion time
/// are internal receipt evidence; the Python RPC response intentionally exposes
/// only adjusted_channels and the post-cycle fee_debug object.
#[derive(Debug, Clone, PartialEq)]
pub struct FeeCycleCompletion {
    pub adjusted_channels: usize,
    pub generation: Option<u64>,
    pub completed_at: i64,
    pub fee_debug: serde_json::Value,
}

/// Exact Python-compatible success response for a completed revenue-fee-cycle.
pub fn build_fee_cycle_response(completed: &FeeCycleCompletion) -> serde_json::Value {
    serde_json::json!({
        "ok": true,
        "adjusted_channels": completed.adjusted_channels,
        "fee_debug": completed.fee_debug,
    })
}

/// Completed result of one owner-thread wake-all mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeWakeCompletion {
    pub channels_woken: i64,
    pub completed_at: i64,
}

/// Exact Python-compatible response for a completed `revenue-wake-all`.
///
/// The completion value can only be produced after the owner thread has
/// applied the wake, so this helper cannot turn queue admission into success.
pub fn build_wake_all_response(completed: &FeeWakeCompletion) -> serde_json::Value {
    serde_json::json!({
        "status": "ok",
        "channels_woken": completed.channels_woken,
        "message": format!(
            "Woke {} sleeping channel(s). They will be evaluated on the next fee cycle.",
            completed.channels_woken
        ),
    })
}

impl SchedulerHandle {
    /// Run wake-all through the bounded owner ingress and wait for the owner
    /// to confirm the state transition. A closed queue or dropped reply is
    /// failure, never successful admission.
    pub async fn wake_all(&self) -> Result<FeeWakeCompletion, String> {
        let (reply, completed) = tokio::sync::oneshot::channel();
        self.tx
            .send(CycleMsg::WakeAllWithReply(reply))
            .await
            .map_err(|_| "fee-cycle owner thread not running".to_string())?;
        completed
            .await
            .map_err(|_| "fee-cycle owner thread exited before completing wake-all".to_string())
    }
}

pub fn spawn_owner_for_runtime(
    mut cfg: SchedulerConfig,
    store: Option<Box<dyn RunwayStateStore>>,
) -> anyhow::Result<SchedulerHandle> {
    cfg.trigger = TriggerMode::ExternalOnly;
    spawn(cfg, None, PythonOptionCache::empty(), store)
}

pub struct FeeObserverPass {
    socket_path: PathBuf,
    db_handle: Option<DbHandle>,
    python_options: PythonOptionCache,
    tx: SchedulerIngress,
    interval_secs: std::sync::atomic::AtomicU64,
}

impl FeeObserverPass {
    pub fn new(
        socket_path: PathBuf,
        db_handle: Option<DbHandle>,
        python_options: PythonOptionCache,
        tx: SchedulerIngress,
        initial_interval_secs: u64,
    ) -> Self {
        Self {
            socket_path,
            db_handle,
            python_options,
            tx,
            interval_secs: std::sync::atomic::AtomicU64::new(initial_interval_secs.max(1)),
        }
    }
    /// Prepare one fresh cycle on the async side, dispatch it through the
    /// bounded owner ingress, and wait for the real serialized outcome.
    pub async fn run_with_completion(&self) -> anyhow::Result<FeeCycleCompletion> {
        let _ = self.python_options.refresh(&self.socket_path).await;
        let prepared = prepare_cycle(
            &self.socket_path,
            self.db_handle.as_ref(),
            &self.python_options.snapshot(),
        )
        .await
        .map_err(|error| error.context("fee prefetch"))?;
        self.interval_secs.store(
            prepared.cfg.fee_interval.max(1) as u64,
            std::sync::atomic::Ordering::SeqCst,
        );
        let (completion, acknowledged) = tokio::sync::oneshot::channel();
        self.tx
            .send(CycleMsg::RunPrepared(Box::new(prepared), completion))
            .await
            .map_err(|_| anyhow::anyhow!("fee owner disconnected before dispatch"))?;
        acknowledged
            .await
            .map_err(|error| anyhow::anyhow!("fee owner disconnected before completion: {error}"))?
            .map_err(anyhow::Error::msg)
    }

    pub fn interval_secs(&self) -> u64 {
        self.interval_secs.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl crate::loop_health::ObserverPass for FeeObserverPass {
    fn run(
        &self,
        _key: crate::loop_health::RequestKey,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
        Box::pin(async move { self.run_with_completion().await.map(|_| ()) })
    }
}

pub struct FeeCadenceActivation {
    handle: crate::loop_health::LoopHandle,
    pass: std::sync::Arc<FeeObserverPass>,
    phase_offset_secs: u64,
}

impl FeeCadenceActivation {
    pub fn new(
        handle: crate::loop_health::LoopHandle,
        pass: std::sync::Arc<FeeObserverPass>,
        phase_offset_secs: u64,
    ) -> Self {
        Self {
            handle,
            pass,
            phase_offset_secs,
        }
    }

    pub fn activate(self) {
        let Self {
            handle,
            pass,
            phase_offset_secs,
        } = self;
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let delay =
                    pass.interval_secs()
                        .saturating_add(if first { phase_offset_secs } else { 0 });
                first = false;
                tokio::time::sleep(Duration::from_secs(delay.max(1))).await;
                match handle
                    .request(crate::loop_health::RequestKey::from("fixed_interval"))
                    .await
                {
                    Ok(
                        crate::loop_health::Admission::Enqueued
                        | crate::loop_health::Admission::Coalesced,
                    ) => {}
                    Ok(crate::loop_health::Admission::Dropped) => {
                        eprintln!("revops: fee loop request dropped by bounded runtime")
                    }
                    Err(error) => {
                        eprintln!(
                            "revops: fee loop request persistence failed: {error:#}; trigger exiting"
                        );
                        return;
                    }
                }
            }
        });
    }
}

/// Spawn the scheduler: the owner thread (a) and the trigger task (b).
/// Must be called from within the plugin's tokio runtime. Returns the
/// cheap [`SchedulerHandle`]; dropping every clone of `handle.tx` plus a
/// `Shutdown` message winds both halves down.
///
/// T6b (T6 review Minor): a failed owner-thread spawn is `Err`, not a
/// usable-looking handle whose sends silently vanish -- the caller
/// decides how loudly to disable the dry-run.
pub fn spawn(
    cfg: SchedulerConfig,
    db_handle: Option<DbHandle>,
    python_options: PythonOptionCache,
    store: Option<Box<dyn RunwayStateStore>>,
) -> anyhow::Result<SchedulerHandle> {
    spawn_with_thread_spawner(cfg, db_handle, python_options, store, |name, body| {
        std::thread::Builder::new()
            .name(name.to_string())
            .spawn(body)
            .map(|_join| ())
    })
}

/// [`spawn`] with the owner-thread spawner injected -- the test seam for
/// the spawn-failure contract (`std::thread::Builder::spawn` failure is
/// not forceable from a test). Production passes the real builder.
pub fn spawn_with_thread_spawner<S>(
    cfg: SchedulerConfig,
    db_handle: Option<DbHandle>,
    python_options: PythonOptionCache,
    store: Option<Box<dyn RunwayStateStore>>,
    thread_spawner: S,
) -> anyhow::Result<SchedulerHandle>
where
    S: FnOnce(&str, Box<dyn FnOnce() + Send + 'static>) -> std::io::Result<()>,
{
    let (tx, mut rx) = SchedulerIngress::bounded_channel(OWNER_QUEUE_CAPACITY);
    let (wake_tx, wake_rx) = tokio_mpsc::channel::<()>(1);
    let socket_path = cfg.socket_path.clone();
    let db_path = cfg.db_path.clone();
    let trigger = cfg.trigger;

    // (a) The owner thread: state + the ONE PyRandom live here, nowhere
    // else. `now_unix()` here is the spawn-time SEED read; per-cycle
    // clock reads happen inside `run_cycle` (exactly one each). Spawned
    // FIRST: if it fails, the trigger task is never started and the
    // caller gets `Err` instead of a dead-letter handle.
    let owner_wake = wake_tx.clone();
    let owner_self_tx = tx.clone();
    let owner_body: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
        let mut owner = CycleOwner::new(&cfg, crate::now_unix(), store);
        // F5: off-owner store dispatches route their results back onto
        // this same queue -- the owner never blocks on a store reply.
        owner.set_initial_fee_result_sink(InitialFeeResultSink::scheduler(owner_self_tx));
        let mut clock = crate::now_unix;
        while let Some(msg) = rx.blocking_recv() {
            match msg {
                CycleMsg::RunPrepared(prepared, completion) => {
                    owner.run_or_defer_cycle_with_ack(prepared, &mut clock, completion);
                }
                CycleMsg::RunCycleNow => {
                    // Only the async half can prefetch; hand over.
                    match owner_wake.try_send(()) {
                        Ok(()) | Err(tokio_mpsc::error::TrySendError::Full(())) => {}
                        Err(tokio_mpsc::error::TrySendError::Closed(())) => {
                            eprintln!("revops: RunCycleNow wake lost: trigger task closed")
                        }
                    }
                }
                CycleMsg::PolicyChanged { peer_id } => {
                    // A fresh read-only channel_states read (not the
                    // per-cycle pinned snapshot -- this is an out-of-cycle
                    // hint, not a cycle). An unreadable DB just means the
                    // wake is skipped this time; the NEXT scheduled cycle
                    // still re-hydrates state normally.
                    match read_channel_states_readonly(&owner.db_path) {
                        Ok(rows) => {
                            let _ = owner.handle_policy_changed(&rows, &peer_id, crate::now_unix());
                        }
                        Err(e) => eprintln!(
                            "revops: policy-change wake for peer {peer_id} skipped: \
                             channel_states read failed: {e:#}"
                        ),
                    }
                }
                CycleMsg::VegasSpikeCheck => {
                    let _ = owner.handle_vegas_spike_check(crate::now_unix());
                }
                CycleMsg::WakeAll => {
                    let _ = owner.handle_wake_all(crate::now_unix());
                }
                CycleMsg::WakeAllWithReply(reply) => {
                    let completed = owner.handle_wake_all_completion(crate::now_unix());
                    let _ = reply.send(completed);
                }
                CycleMsg::FailedForward(signal) => {
                    owner.handle_failed_forward(&signal);
                }
                CycleMsg::ForwardEvent { channel_id } => {
                    owner.handle_forward_event(&channel_id, crate::now_unix());
                }
                CycleMsg::NewChannel(preparation) => match *preparation {
                    NewChannelPreparation::Ready(prepared) => owner.handle_new_channel(*prepared),
                    NewChannelPreparation::Refused {
                        peer_id,
                        channel_hint,
                        event_ts,
                        reason,
                    } => owner.handle_new_channel_refused(peer_id, channel_hint, event_ts, reason),
                },
                CycleMsg::InitialFeeStoreResult(result) => {
                    owner.handle_initial_fee_store_result(result, &mut clock);
                }
                CycleMsg::Query(query, reply) => {
                    // Never block the owner thread on a slow/uncooperative
                    // caller: a dropped receiver just means this reply is
                    // lost, matching every other loud-log-and-continue
                    // posture in this loop.
                    let _ = reply.send(owner.fee_debug(&query));
                }
                CycleMsg::Shutdown => break,
            }
        }
    });
    thread_spawner("revops-fee-cycle", owner_body).map_err(|e| {
        anyhow::anyhow!("failed to spawn fee-cycle owner thread: {e}; fee dry-run cannot start")
    })?;

    // (b) The trigger task (flush-observation or wall-clock; module doc).
    let tick_tx = tx.clone();
    tokio::spawn(trigger_loop(
        trigger,
        db_path,
        socket_path,
        db_handle,
        python_options,
        tick_tx,
        wake_rx,
    ));

    Ok(SchedulerHandle { tx })
}

/// One dispatch on the async side: prepare a cycle and send it to the
/// owner thread.
enum Dispatch {
    /// Sent; carries the freshly resolved `fee_interval` (the per-cycle
    /// authoritative cadence/staleness bound).
    Sent(u64),
    /// Prefetch failed; logged, cycle skipped.
    Skipped,
    /// Owner thread gone -- the trigger loop must exit.
    OwnerGone,
}

async fn dispatch_cycle(
    socket_path: &Path,
    db_handle: Option<&DbHandle>,
    python_options: &PythonOptionCache,
    tick_tx: &SchedulerIngress,
) -> Dispatch {
    // 2026-07-22 audit M3: Python re-reads `listconfigs` every cycle
    // (`_refresh_dynamic_config`), so refresh layer (b) before resolving —
    // a `setconfig` on a dynamic option takes effect on the next cycle,
    // and an init-time listconfigs outage heals instead of pinning the
    // whole window to fixture defaults. A failed refresh keeps the last
    // good snapshot (and logs); resolution then proceeds as before.
    let _ = python_options.refresh(socket_path).await;
    let snapshot = python_options.snapshot();
    match prepare_cycle(socket_path, db_handle, &snapshot).await {
        Ok(prepared) => {
            let interval_secs = prepared.cfg.fee_interval.max(1) as u64;
            let (completion, acknowledged) = tokio::sync::oneshot::channel();
            if tick_tx
                .send(CycleMsg::RunPrepared(Box::new(prepared), completion))
                .await
                .is_err()
            {
                return Dispatch::OwnerGone;
            }
            match acknowledged.await {
                Ok(Ok(_)) => Dispatch::Sent(interval_secs),
                Ok(Err(error)) => {
                    eprintln!("revops: fee cycle owner reported failure: {error}");
                    Dispatch::Skipped
                }
                Err(_) => Dispatch::OwnerGone,
            }
        }
        Err(e) => {
            eprintln!("revops: fee cycle prefetch failed ({e:#}); cycle skipped");
            Dispatch::Skipped
        }
    }
}

/// The trigger task body: decides WHEN cycles run (module doc, "Cycle
/// triggering"), in either mode also servicing `RunCycleNow` wakes.
async fn trigger_loop(
    trigger: TriggerMode,
    db_path: PathBuf,
    socket_path: PathBuf,
    db_handle: Option<DbHandle>,
    python_options: PythonOptionCache,
    tick_tx: SchedulerIngress,
    mut wake_rx: tokio_mpsc::Receiver<()>,
) {
    // Initial cadence resolution -- schedule/staleness seed only; every
    // cycle's authoritative cfg is resolved in prepare_cycle.
    let mut interval_secs =
        fee_config::resolve_fee_cfg(db_handle.as_ref(), &python_options.snapshot())
            .await
            .fee_interval
            .max(1) as u64;

    match trigger {
        TriggerMode::ExternalOnly => {
            while wake_rx.recv().await.is_some() {
                eprintln!("revops: ignored legacy RunCycleNow in ExternalOnly mode; use bounded LoopHandle ingress");
            }
        }
        TriggerMode::FixedInterval { phase_offset_secs } => {
            let mut next = tokio::time::Instant::now()
                + Duration::from_secs(interval_secs + phase_offset_secs);
            loop {
                // A tick advances the phase-locked schedule; a wake runs
                // an extra cycle without disturbing it.
                let ticked = tokio::select! {
                    _ = tokio::time::sleep_until(next) => true,
                    wake = wake_rx.recv() => {
                        if wake.is_none() {
                            return; // every wake sender dropped
                        }
                        false
                    }
                };
                match dispatch_cycle(&socket_path, db_handle.as_ref(), &python_options, &tick_tx)
                    .await
                {
                    Dispatch::Sent(interval) => interval_secs = interval,
                    Dispatch::Skipped => {}
                    Dispatch::OwnerGone => return,
                }
                if ticked {
                    next += Duration::from_secs(interval_secs);
                }
            }
        }
        TriggerMode::FlushTriggered {
            poll_secs,
            settle_secs,
        } => {
            let poll = Duration::from_secs(poll_secs.max(1));
            let mut watcher = FlushWatcher::new(crate::now_unix());
            loop {
                let polled = tokio::select! {
                    _ = tokio::time::sleep(poll) => true,
                    wake = wake_rx.recv() => {
                        if wake.is_none() {
                            return; // every wake sender dropped
                        }
                        false
                    }
                };
                if !polled {
                    // RunCycleNow wake: an extra cycle outside the flush
                    // schedule; the watcher is not disturbed.
                    match dispatch_cycle(
                        &socket_path,
                        db_handle.as_ref(),
                        &python_options,
                        &tick_tx,
                    )
                    .await
                    {
                        Dispatch::Sent(interval) => interval_secs = interval,
                        Dispatch::Skipped => {}
                        Dispatch::OwnerGone => return,
                    }
                    continue;
                }
                // An unreadable marker is NOT an advance: log, retry next
                // poll, never run a cycle on unknown state.
                let marker = match read_flush_marker(&db_path) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!(
                            "revops: flush-marker poll failed ({e:#}); retrying (no cycle on \
                             unknown state)"
                        );
                        continue;
                    }
                };
                let params = WatchParams {
                    settle_secs,
                    stale_after_secs: interval_secs.saturating_mul(2),
                };
                let outcome = watcher.on_poll(marker, crate::now_unix(), &params);
                match outcome {
                    PollOutcome::RunCycle => {
                        match dispatch_cycle(
                            &socket_path,
                            db_handle.as_ref(),
                            &python_options,
                            &tick_tx,
                        )
                        .await
                        {
                            Dispatch::Sent(interval) => interval_secs = interval,
                            Dispatch::Skipped => {}
                            Dispatch::OwnerGone => return,
                        }
                    }
                    PollOutcome::StaleNoFlush { silent_secs } => {
                        eprintln!(
                            "revops: NO PYTHON FEE-STATE FLUSH OBSERVED for {silent_secs}s \
                             (> 2x fee_interval={interval_secs}s): Python may be dead or \
                             paused; NOT running cycles on stale state, still polling"
                        );
                    }
                    PollOutcome::Baselined | PollOutcome::Advanced | PollOutcome::Idle => {}
                }
                // T7: the Vegas-spike wake check, BETWEEN full cycles (see
                // module doc) -- skipped on a poll that just ran a full
                // cycle, since `run_fee_cycle` already calls
                // `maybe_wake_for_vegas_spike` itself this same poll.
                if !matches!(outcome, PollOutcome::RunCycle)
                    && tick_tx.send(CycleMsg::VegasSpikeCheck).await.is_err()
                {
                    return; // owner thread gone
                }
            }
        }
    }
}

#[cfg(test)]
mod bounded_ingress_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn a3_callback_blocking_send_backpressures_and_preserves_fifo_exactly_once() {
        let (tx, mut rx) = SchedulerIngress::bounded_channel(1);
        tx.blocking_send(CycleMsg::ForwardEvent {
            channel_id: "first".to_string(),
        })
        .expect("fill ingress");

        let callback_finished = std::sync::Arc::new(AtomicBool::new(false));
        let callback_finished_in_thread = callback_finished.clone();
        let callback_tx = tx.clone();
        let callback = std::thread::spawn(move || {
            callback_tx
                .blocking_send(CycleMsg::InitialFeeStoreResult(
                    InitialFeeStoreResult::Receipt {
                        context: "a3-callback".to_string(),
                        result: Ok(()),
                    },
                ))
                .expect("A3 callback admitted after drain");
            callback_finished_in_thread.store(true, Ordering::SeqCst);
        });

        std::thread::sleep(std::time::Duration::from_millis(25));
        assert!(
            !callback_finished.load(Ordering::SeqCst),
            "A3 callback must block while bounded ingress is saturated"
        );
        assert!(matches!(
            rx.blocking_recv().expect("first queued message"),
            CycleMsg::ForwardEvent { channel_id } if channel_id == "first"
        ));
        callback.join().unwrap();
        assert!(callback_finished.load(Ordering::SeqCst));
        match rx.blocking_recv().expect("one callback result after first") {
            CycleMsg::InitialFeeStoreResult(InitialFeeStoreResult::Receipt { context, result }) => {
                assert_eq!(context, "a3-callback");
                assert!(result.is_ok());
            }
            _ => panic!("callback result must remain second in FIFO order"),
        }
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "the callback result must be delivered exactly once"
        );
    }

    /// T2a (Task 59 §3.3, R2-F1): a FULL owner queue refuses Query
    /// admission typed and IMMEDIATELY -- the try-path either transfers
    /// the message or returns it, so a refusal proves nothing was
    /// enqueued and the RPC never parks on admission.
    #[tokio::test]
    async fn saturated_queue_refuses_query_admission_typed() {
        let (tx, mut rx) = SchedulerIngress::bounded_channel(OWNER_QUEUE_CAPACITY);
        for _ in 0..OWNER_QUEUE_CAPACITY {
            tx.tx.try_send(CycleMsg::WakeAll).expect("fill to capacity");
        }

        let (reply_tx, _keep_reply_open) = std_mpsc::channel();
        let started = std::time::Instant::now();
        let refused = tx
            .try_send_query(FeeDebugQuery::Summary, reply_tx)
            .expect_err("a full bounded queue must refuse Query admission");
        assert!(
            matches!(refused, QueryAdmissionRefused::Saturated),
            "{refused:?}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_millis(200),
            "admission refusal must be immediate"
        );

        // The bridge composes the refusal into the DISTINCT typed JSON
        // error -- a section-local read failure, never a trip condition.
        let value = query_owner_bounded(&tx, FeeDebugQuery::Summary, RPC_BRIDGE_RECV_TIMEOUT).await;
        assert_eq!(value["error"]["code"], "owner_queue_saturated");

        // Neither attempt enqueued anything: the queue still holds
        // exactly the legitimate fill, nothing else.
        let mut drained = 0usize;
        while let Ok(msg) = rx.try_recv() {
            assert!(matches!(msg, CycleMsg::WakeAll), "foreign message enqueued");
            drained += 1;
        }
        assert_eq!(drained, OWNER_QUEUE_CAPACITY);
    }

    /// T2b (Task 59 §3.3, R2-F1/F13): behind the documented worst-case
    /// legitimate backlog (a store-contended heavy handler plus a full
    /// bounded queue), an admitted Query whose budget expires yields the
    /// DISTINCT typed `owner_response_timeout` -- and a retry still
    /// ANSWERS once the backlog drains: expiry is a section-local read
    /// failure, never proof the owner died.
    ///
    /// The owner loop here is synthetic (the real `FailedForward` chain
    /// needs a hydrated SeedOnce owner): one heavy handler stall, cheap
    /// messages otherwise, Query answered off the loop -- the §3.3
    /// derivation's shape. The bounded-bridge composition under test is
    /// the production `query_owner_bounded` verbatim.
    #[tokio::test]
    async fn admitted_query_answers_behind_max_legitimate_backlog() {
        let (tx, mut rx) = SchedulerIngress::bounded_channel(OWNER_QUEUE_CAPACITY);
        let owner = std::thread::spawn(move || {
            while let Some(msg) = rx.blocking_recv() {
                match msg {
                    CycleMsg::FailedForward(_) => {
                        // The store-contended heavy handler (§3.3: the
                        // synchronous idempotency read + guarded nudge
                        // commit under lock contention).
                        std::thread::sleep(std::time::Duration::from_secs(2));
                    }
                    CycleMsg::Query(_query, reply) => {
                        // A reply to an expired bridge is a send to a
                        // dropped receiver -- ignored, exactly like the
                        // production loop.
                        let _ = reply.send(serde_json::json!({"ok": true}));
                    }
                    CycleMsg::Shutdown => return,
                    _ => {}
                }
            }
        });

        // Heavy handler first, then fill the rest of the queue.
        tx.send(CycleMsg::FailedForward(Box::new(FailedForwardSignal {
            channel_id: "700x1x0".to_string(),
            amount_msat: 0,
            failcode: Some(4108),
            failreason: None,
            event_ts: 1_800_000_000,
        })))
        .await
        .expect("send heavy handler");
        for _ in 0..(OWNER_QUEUE_CAPACITY - 1) {
            tx.send(CycleMsg::WakeAll).await.expect("fill backlog");
        }
        // Let the owner dequeue the heavy handler (freeing one slot) and
        // enter its 2 s stall before admitting the query.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Admitted (one slot free after the heavy handler was dequeued),
        // then the small budget expires while the owner is mid-stall.
        let value = query_owner_bounded(
            &tx,
            FeeDebugQuery::Summary,
            std::time::Duration::from_millis(100),
        )
        .await;
        assert_eq!(value["error"]["code"], "owner_response_timeout");

        // Retryable: keep retrying through saturation refusals while the
        // backlog drains; the admitted retry must genuinely answer.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let answered = loop {
            let value = query_owner_bounded(
                &tx,
                FeeDebugQuery::Summary,
                std::time::Duration::from_secs(30),
            )
            .await;
            if value.get("ok").is_some() {
                break value;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "an admitted query behind a legitimate backlog must eventually answer, \
                 kept getting {value:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        };
        assert_eq!(answered, serde_json::json!({"ok": true}));

        tx.send(CycleMsg::Shutdown).await.expect("shutdown");
        owner.join().unwrap();
    }
}

#[cfg(test)]
mod decision_clock_tests {
    use super::*;
    use crate::fee_state::STATE_JOURNAL_FILE_NAME;
    use revops_fees::journal::JOURNAL_FILE_NAME;
    use revops_fees::pyrand::DecisionInputError;
    use rusqlite::Connection;
    use serde_json::{json, Value};

    const TEST_NOW: i64 = 1_800_000_000;

    struct ExhaustOnSecondEvaluation {
        evaluation_calls: usize,
        labels: Vec<String>,
    }

    impl DecisionClock for ExhaustOnSecondEvaluation {
        fn now(&mut self, label: &str) -> Result<i64, DecisionInputError> {
            self.labels.push(label.to_string());
            if label == "cycle.channel.evaluate" {
                self.evaluation_calls += 1;
                if self.evaluation_calls == 2 {
                    return Err(DecisionInputError::new(
                        "scripted clock exhausted on second channel",
                    ));
                }
            }
            Ok(TEST_NOW)
        }
    }

    fn peer(byte: &str) -> String {
        format!("02{}", byte.repeat(32))
    }

    fn channel(scid: &str, full_id: &str, peer_id: &str, fee_ppm: i64) -> Value {
        json!({
            "state": "CHANNELD_NORMAL",
            "short_channel_id": scid,
            "channel_id": full_id,
            "peer_id": peer_id,
            "total_msat": 2_000_000_000_i64,
            "to_us_msat": 1_100_000_000_i64,
            "spendable_msat": 1_000_000_000_i64,
            "receivable_msat": 900_000_000_i64,
            "updates": {"local": {
                "fee_base_msat": 0,
                "fee_proportional_millionths": fee_ppm,
                "htlc_minimum_msat": 1000,
                "htlc_maximum_msat": 1_980_000_000_i64,
            }},
            "opener": "local",
            "max_accepted_htlcs": 483,
            "htlcs": [],
        })
    }

    fn line_count(path: &Path) -> usize {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .count()
    }

    #[test]
    fn second_channel_clock_exhaustion_discards_all_cycle_journal_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("prod.db");
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db");
        std::fs::copy(fixture, &db_path).expect("copy fixture db");
        let conn = Connection::open(&db_path).expect("open db");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("wal");
        for (scid, peer_id) in [("100x1x0", peer("aa")), ("200x1x0", peer("bb"))] {
            conn.execute(
                "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, \
                 sats_out, capacity, updated_at, kalman_flow_ratio, kalman_velocity) \
                 VALUES (?1, ?2, 'balanced', 0.1, 0, 0, 2000000, ?3, 0.05, 0.01)",
                rusqlite::params![scid, peer_id, TEST_NOW - 60],
            )
            .expect("insert channel state");
        }
        drop(conn);

        let journal_dir = dir.path().join("journal");
        let mut owner = CycleOwner::new(
            &SchedulerConfig {
                db_path,
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: journal_dir.clone(),
                lifecycle: StateLifecycle::RehydratePerCycle,
                trigger: TriggerMode::default(),
            },
            42,
            None,
        );
        let prepared = PreparedCycle {
            cfg: FeeCfgSnapshot {
                enable_vegas_reflex: false,
                ..FeeCfgSnapshot::default()
            },
            min_competitors: json!(3),
            rpc: RpcPrefetch {
                our_node_id: peer("ee"),
                peer_channels: vec![
                    channel("100:1:0", "full_a", &peer("aa"), 150),
                    channel("200:1:0", "full_b", &peer("bb"), 250),
                ],
                gossip_channels: Vec::new(),
                feerates: None,
            },
        };
        let mut decision_clock = ExhaustOnSecondEvaluation {
            evaluation_calls: 0,
            labels: Vec::new(),
        };

        let outcome = owner.run_cycle_at(prepared, TEST_NOW, &mut decision_clock);

        assert_eq!(outcome, CycleOutcome::SkippedDecisionInput);
        assert_eq!(decision_clock.evaluation_calls, 2);
        assert_eq!(
            owner.state().cycle_states.len(),
            1,
            "the first channel must finish before the second transcript read fails"
        );
        assert_eq!(line_count(&journal_dir.join(JOURNAL_FILE_NAME)), 0);
        assert_eq!(line_count(&journal_dir.join(STATE_JOURNAL_FILE_NAME)), 0);

        let exported_bypass = ["pub fn run_cycle_with_", "decision_clock"].concat();
        assert!(
            !include_str!("fee_scheduler.rs").contains(&exported_bypass),
            "downstream crates must not be able to inject a semantic decision clock"
        );
    }
}

// ---------------------------------------------------------------------------
// Task 44 / A3: decide_initial_fee -- precedence and state tests (contract
// §4.2)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod initial_fee_decision_tests {
    use super::*;
    use revops_analytics::policy::RebalanceMode;
    use revops_fees::pyrand::DecisionInputError;

    const EVENT_TS: i64 = 1_800_000_000;

    fn wide_cfg() -> FeeCfgSnapshot {
        FeeCfgSnapshot {
            min_fee_ppm: 0,
            max_fee_ppm: 100_000,
            thompson_prior_std_fee: 100,
            base_fee_msat: 0,
            ..FeeCfgSnapshot::default()
        }
    }

    fn channel_info(fee_proportional_millionths: i64) -> ChannelInfo {
        ChannelInfo {
            channel_id: "1x1x0".to_string(),
            short_channel_id: "1x1x0".to_string(),
            full_channel_id: "deadbeef".to_string(),
            peer_id: "02peer".to_string(),
            capacity_sats: 1_000_000,
            spendable_msat: 500_000_000,
            receivable_msat: 500_000_000,
            fee_base_msat: 0,
            fee_proportional_millionths,
            htlc_minimum_msat: 1,
            htlc_min_msat: 1,
            htlc_maximum_msat: 100_000,
            htlc_max_msat: 100_000,
            opener: "remote".to_string(),
            has_htlc_data: false,
            max_accepted_htlcs: 483,
            our_htlcs_in_flight: 0,
        }
    }

    fn policy(strategy: FeeStrategy, fee_ppm_target: Option<i64>) -> PeerPolicy {
        PeerPolicy {
            peer_id: "02peer".to_string(),
            strategy,
            rebalance_mode: RebalanceMode::Enabled,
            fee_ppm_target,
            tags: Vec::new(),
            updated_at: 0,
            fee_multiplier_min: None,
            fee_multiplier_max: None,
            expires_at: None,
        }
    }

    fn prepared(
        policy: PeerPolicy,
        prior: Option<FeePrior>,
        channel_fee_ppm: i64,
    ) -> PreparedInitialFee {
        PreparedInitialFee {
            channel: channel_info(channel_fee_ppm),
            peer_id: "02peer".to_string(),
            policy,
            cfg: wide_cfg(),
            prior,
            event_ts: EVENT_TS,
            event_key: format!("test-event-key-{EVENT_TS}"),
        }
    }

    struct AlwaysAuthorize;
    impl FeeAuthorizer for AlwaysAuthorize {
        fn authorize(
            &self,
            request: &FeeAuthorizationRequest,
        ) -> Result<revops_fees::execution::FeeAuthorizationResult, DecisionInputError> {
            Ok(revops_fees::execution::FeeAuthorizationResult {
                authorized: true,
                reason_code: "".to_string(),
                trace: Some(GovernedTrace {
                    authorized: true,
                    reason_code: "".to_string(),
                    intent_id: "test-intent".to_string(),
                    idempotency_key: format!("test-{}", request.channel_id),
                }),
            })
        }
    }

    struct AlwaysDeny;
    impl FeeAuthorizer for AlwaysDeny {
        fn authorize(
            &self,
            _request: &FeeAuthorizationRequest,
        ) -> Result<revops_fees::execution::FeeAuthorizationResult, DecisionInputError> {
            Ok(revops_fees::execution::FeeAuthorizationResult {
                authorized: false,
                reason_code: "governor_test_denied".to_string(),
                trace: Some(GovernedTrace {
                    authorized: false,
                    reason_code: "governor_test_denied".to_string(),
                    intent_id: "test-intent".to_string(),
                    idempotency_key: "test-key".to_string(),
                }),
            })
        }
    }

    /// Counts every entropy draw, delegating to a real seeded `PyRandom` --
    /// proves RNG consumption (or its absence) without inspecting
    /// `PyRandom`'s private internal state.
    struct CountingEntropy {
        inner: PyRandom,
        draws: usize,
    }
    impl CountingEntropy {
        fn seeded(seed: u64) -> Self {
            CountingEntropy {
                inner: PyRandom::seed_from_u64(seed),
                draws: 0,
            }
        }
    }
    impl DecisionEntropy for CountingEntropy {
        fn random(&mut self, label: &str) -> Result<f64, DecisionInputError> {
            self.draws += 1;
            DecisionEntropy::random(&mut self.inner, label)
        }
        fn gauss(&mut self, label: &str, mu: f64, sigma: f64) -> Result<f64, DecisionInputError> {
            self.draws += 1;
            DecisionEntropy::gauss(&mut self.inner, label, mu, sigma)
        }
    }

    /// Contract §4.2 test 6: PASSIVE consumes no RNG, creates no state row,
    /// no action, and an explicit skipped outcome.
    #[test]
    fn passive_skips_with_no_rng_no_state_no_action() {
        let mut rng = CountingEntropy::seeded(1);
        let p = prepared(policy(FeeStrategy::Passive, None), None, 500);
        let out = decide_initial_fee(&p, None, None, &AlwaysAuthorize, &mut rng);
        assert_eq!(rng.draws, 0, "PASSIVE must never draw entropy");
        assert_eq!(out.fee_state, None);
        assert_eq!(out.cycle_state, None);
        assert_eq!(out.action, None);
        assert_eq!(out.outcome, InitialFeeOutcome::Passive);
    }

    /// Contract §4.2 test 7: STATIC with a target sends that EXACT target
    /// (after the ordinary safety clamp), consumes no RNG, seeds no DTS
    /// prior/nudge, and carries `policy_static`.
    #[test]
    fn static_with_target_uses_exact_target_no_rng() {
        let mut rng = CountingEntropy::seeded(2);
        let p = prepared(policy(FeeStrategy::Static, Some(777)), None, 500);
        let out = decide_initial_fee(&p, None, None, &AlwaysAuthorize, &mut rng);
        assert_eq!(rng.draws, 0, "STATIC-with-target must never draw entropy");
        assert_eq!(out.reason_code, "policy_static");
        let action = out
            .action
            .expect("authorized STATIC must prepare an action");
        assert_eq!(action.request.feeppm, 777);
        assert!(
            out.fee_state.unwrap().thompson.posterior_bias.is_empty(),
            "STATIC must never seed/nudge the DTS prior"
        );
    }

    /// Contract §4.2 test 8: STATIC without a target falls through to
    /// DYNAMIC and consumes DYNAMIC's entropy (at least one draw).
    #[test]
    fn static_without_target_falls_through_to_dynamic() {
        let mut rng = CountingEntropy::seeded(3);
        let p = prepared(policy(FeeStrategy::Static, None), None, 500);
        let out = decide_initial_fee(&p, None, None, &AlwaysAuthorize, &mut rng);
        assert!(
            rng.draws > 0,
            "STATIC-without-target must fall through to DYNAMIC's sample"
        );
        assert_eq!(out.reason_code, "channel_open");
    }

    /// Contract §4.2 test 9: DYNAMIC without gossip samples a fresh
    /// default-mean/configured-std throwaway state; no prior seed/nudge
    /// exists before action handling (no persistent state row at all,
    /// since nothing changed).
    #[test]
    fn dynamic_without_gossip_samples_default_throwaway() {
        let mut rng = CountingEntropy::seeded(4);
        let p = prepared(policy(FeeStrategy::Dynamic, None), None, 500);
        let out = decide_initial_fee(&p, None, None, &AlwaysAuthorize, &mut rng);
        assert!(rng.draws > 0);
        assert_eq!(out.reason_code, "channel_open");
        // The only fee_state present is the POST-broadcast sync (created
        // fresh, default posterior/prior) -- never touched by a gossip
        // seed/nudge (posterior_bias stays empty).
        let fs = out
            .fee_state
            .expect("authorized broadcast still creates a sync row");
        assert!(fs.thompson.posterior_bias.is_empty());
        assert_eq!(
            fs.thompson.prior_mean_fee, 200.0,
            "default prior, no gossip"
        );
    }

    /// Contract §4.2 test 10: DYNAMIC with gossip installs the exact
    /// mean/std in persistent state and records exactly one `.3` nudge at
    /// the event timestamp.
    #[test]
    fn dynamic_with_gossip_seeds_persistent_prior_and_nudge() {
        let mut rng = CountingEntropy::seeded(5);
        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let p = prepared(policy(FeeStrategy::Dynamic, None), Some(prior), 500);
        let out = decide_initial_fee(&p, None, None, &AlwaysAuthorize, &mut rng);
        let fs = out
            .fee_state
            .expect("gossip-backed DYNAMIC must produce a fee_state row");
        assert_eq!(fs.thompson.prior_mean_fee, 300.0);
        assert_eq!(fs.thompson.prior_std_fee, 40.0);
        assert_eq!(fs.thompson.posterior_bias.len(), 1, "exactly one nudge");
        let (target, weight, ts) = fs.thompson.posterior_bias[0];
        assert_eq!(target, 300.0);
        assert_eq!(weight, INITIAL_PRIOR_NUDGE_WEIGHT);
        assert_eq!(
            ts, EVENT_TS,
            "the nudge is stamped with EVENT time, never drain time"
        );
        // A cycle_state row must also exist (the generic serializer needs
        // both maps populated for this channel to be durable).
        assert!(out.cycle_state.is_some());
    }

    /// Contract §4.2 test 11 (the load-bearing one): scripted entropy so
    /// sampling the throwaway state yields a DIFFERENT fee than sampling
    /// the newly nudged persistent state would -- and `decide_initial_fee`
    /// must produce the THROWAWAY result, never the persistent one.
    #[test]
    fn throwaway_and_persistent_states_diverge_and_throwaway_wins() {
        const SEED: u64 = 777;
        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };

        // (a) What decide_initial_fee actually produces.
        let mut rng_a = CountingEntropy::seeded(SEED);
        let p = prepared(policy(FeeStrategy::Dynamic, None), Some(prior.clone()), 500);
        let out = decide_initial_fee(&p, None, None, &AlwaysAuthorize, &mut rng_a);
        let action = out
            .action
            .clone()
            .expect("authorized DYNAMIC must prepare an action");
        let produced_fee = action.request.feeppm as i64;

        // (b) A truly fresh throwaway sampled independently, same seed --
        // must match (a) exactly: proves (a) sampled the throwaway.
        let mut rng_b = CountingEntropy::seeded(SEED);
        let mut throwaway = GaussianThompsonState {
            prior_mean_fee: 300.0,
            prior_std_fee: 40.0,
            ..GaussianThompsonState::default()
        };
        let throwaway_fee = revops_fees::thompson::sampling::sample_fee_with_entropy(
            &mut throwaway,
            0,
            100_000,
            None,
            &mut rng_b,
            EVENT_TS,
        )
        .unwrap();
        assert_eq!(
            produced_fee, throwaway_fee,
            "decide_initial_fee must sample the THROWAWAY state"
        );

        // (c) The persistent (nudged) state, sampled with the SAME seed --
        // must DIFFER from (a)/(b): the nudge's posterior_bias shift only
        // applies to the persistent object, proving throwaway and
        // persistent really are distinct draws.
        let mut rng_c = CountingEntropy::seeded(SEED);
        let mut persistent = ChannelFeeState::default();
        persistent.thompson.prior_mean_fee = 300.0;
        persistent.thompson.prior_std_fee = 40.0;
        dynamics::record_posterior_nudge(
            &mut persistent.thompson,
            300.0,
            INITIAL_PRIOR_NUDGE_WEIGHT,
            EVENT_TS,
        );
        let persistent_fee = revops_fees::thompson::sampling::sample_fee_with_entropy(
            &mut persistent.thompson,
            0,
            100_000,
            None,
            &mut rng_c,
            EVENT_TS,
        )
        .unwrap();
        assert_ne!(
            produced_fee, persistent_fee,
            "sampling the nudged PERSISTENT state must differ from the throwaway result -- \
             this is exactly the attractive-but-wrong reuse contract §2.2 forbids"
        );
    }

    /// Contract §4.2 test 12: the target traverses the existing pure
    /// execution clamp and carries the exact reason identity.
    #[test]
    fn clamp_and_reason_contract() {
        let mut rng = CountingEntropy::seeded(6);
        let mut cfg = wide_cfg();
        cfg.min_fee_ppm = 100;
        cfg.max_fee_ppm = 200;
        let p = PreparedInitialFee {
            cfg,
            ..prepared(policy(FeeStrategy::Static, Some(50)), None, 500)
        };
        let out = decide_initial_fee(&p, None, None, &AlwaysAuthorize, &mut rng);
        assert_eq!(out.reason_code, "policy_static");
        let action = out
            .action
            .expect("authorized STATIC must prepare an action");
        assert_eq!(
            action.request.feeppm, 100,
            "50 must clamp up to the configured floor"
        );
        assert!(action.decision.clamp_log.is_some());
    }

    /// Contract §4.2 test 13: governor denial preserves the prior-seed
    /// ordering (Python seeds before authorization), but records no action
    /// and no post-broadcast state sync.
    #[test]
    fn governor_denial_keeps_seed_but_no_action_or_sync() {
        let mut rng = CountingEntropy::seeded(7);
        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let p = prepared(policy(FeeStrategy::Dynamic, None), Some(prior), 500);
        let out = decide_initial_fee(&p, None, None, &AlwaysDeny, &mut rng);
        assert_eq!(
            out.outcome,
            InitialFeeOutcome::GovernorDenied {
                reason_code: "governor_test_denied".to_string()
            }
        );
        assert_eq!(
            out.action, None,
            "a denied decision must never carry a prepared action"
        );
        let fs = out
            .fee_state
            .expect("the gossip prior seed persists despite denial (py ordering)");
        assert_eq!(fs.thompson.prior_mean_fee, 300.0);
        assert_eq!(fs.thompson.posterior_bias.len(), 1);
        // No post-broadcast sync: last_fee_ppm/last_broadcast_fee_ppm/
        // last_update were never touched by a denied decision.
        assert_eq!(fs.last_fee_ppm, 0);
        assert_eq!(fs.last_update, 0);
    }

    /// A brand-new channel is not at 0 ppm -- the pre-action fee the
    /// governor/action see must come from the live CLN-announced policy
    /// (`ChannelInfo.fee_proportional_millionths`), never from absent
    /// persisted state.
    #[test]
    fn old_fee_ppm_comes_from_channel_info_not_persisted_state() {
        let mut rng = CountingEntropy::seeded(8);
        // Control: a channel whose CLN-announced fee is 0 (the vacuous
        // case that could pass even with the bug).
        let p_zero = prepared(policy(FeeStrategy::Static, Some(500)), None, 0);
        let out_zero = decide_initial_fee(&p_zero, None, None, &AlwaysAuthorize, &mut rng);
        assert_eq!(out_zero.action.unwrap().old_fee_ppm, 0);

        // The real case: CLN-announced fee is nonzero and there is NO
        // persisted state at all -- old_fee_ppm must still be the real
        // channel_info value, not 0.
        let mut rng2 = CountingEntropy::seeded(9);
        let p = prepared(policy(FeeStrategy::Static, Some(500)), None, 321);
        let out = decide_initial_fee(&p, None, None, &AlwaysAuthorize, &mut rng2);
        assert_eq!(
            out.action.unwrap().old_fee_ppm,
            321,
            "old_fee_ppm must come from ChannelInfo.fee_proportional_millionths, not persisted state"
        );
    }
}
