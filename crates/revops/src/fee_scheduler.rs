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
//! thread over the same `mpsc::Sender<CycleMsg>` `RunPrepared` already
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

use std::collections::HashMap;

use revops_fees::thompson::dynamics;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use cln_plugin::options::Value as OptValue;
use revops_db::actor::DbHandle;
use revops_db::fee_runway::{
    FeeCycleCommit, FeeRestartMarkerRow, GovernorAuditRow, PreparedFeeActionRow,
    ShadowCycleOutcomeRow,
};
use revops_fees::cycle::{
    handle_policy_change, maybe_wake_for_vegas_spike, run_fee_cycle, wake_all_sleeping_channels,
    ChannelStateRow, ControllerState, CycleDeps, DecisionClock, FeeCfgSnapshot, FixedDecisionClock,
    StateSink,
};
use revops_fees::execution::{
    FeeExecutor, GovernedFeeAuthorizer, PureFeeExecutor, RecordingFeeExecutor,
};
use revops_fees::journal::{FeeDecision, Journal};
use revops_fees::profiles::fee_profile;
use revops_fees::pyjson::OValue as PyOValue;
use revops_fees::pyrand::PyRandom;

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
    RunPrepared(Box<PreparedCycle>),
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
    /// A `revenue-r-fee-debug` query; the owner thread answers over the
    /// included reply channel without ever blocking on IO.
    Query(FeeDebugQuery, mpsc::Sender<serde_json::Value>),
    Shutdown,
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

/// What one `run_cycle` call did -- the loud-logging skip taxonomy the
/// per-cycle sequence requires (skips log, never panic: the hub
/// precedent).
#[derive(Debug, PartialEq, Eq)]
pub enum CycleOutcome {
    /// Ran to completion; `decisions` FeeDecision lines appended.
    Ran { decisions: usize },
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
        CycleOutcome::Ran { decisions } => format!("ran: {decisions} decision(s)"),
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
        CycleOutcome::PersistenceFailed => {
            "ran but PersistenceFailed: atomic commit failed, generation not advanced".to_string()
        }
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

/// What [`CycleOwner::apply_failure_nudge`] actually did, so the trigger
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
            last_policy_updated_at: HashMap::new(),
            hydrated_once: false,
            seed_refused: false,
            store,
            spawn_now: seed_now,
            persistence_failures: 0,
            db_path: cfg.db_path.clone(),
            journal,
            state_sink,
            governor: GovernorWiring::open(Some(&cfg.journal_dir)),
            trigger_queue: TriggerQueue::new(TRIGGER_QUEUE_CAPACITY),
            last_profile: "active".to_string(),
            last_cycle_at: None,
            last_cycle_outcome: None,
        }
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
                let rows = match self.store.as_ref() {
                    Some(store) => store
                        .query_mempool_samples_since(now - MEMPOOL_MA_WINDOW_SECONDS)
                        .unwrap_or_else(|e| {
                            eprintln!(
                                "revops: SeedOnce mempool sample query failed ({e:#}); \
                                 treating as no fresh autonomous evidence this cycle"
                            );
                            Vec::new()
                        }),
                    None => Vec::new(),
                };
                MempoolEvidenceSource::Rust(rows)
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
            if let Err(e) = store.commit_fee_cycle(commit) {
                self.persistence_failures += 1;
                eprintln!(
                    "revops: FEE CYCLE PERSISTENCE FAILED (failure #{}): {e:#}; generation NOT \
                     advanced; this cycle's state evolution is uncommitted (a restart resumes \
                     from the last committed generation)",
                    self.persistence_failures
                );
                return (CycleOutcome::PersistenceFailed, None);
            }
            committed_cycle_id = Some(cycle_id);
        }

        (
            CycleOutcome::Ran {
                decisions: decisions.len(),
            },
            committed_cycle_id,
        )
    }

    /// Red error counter: SeedOnce cycles whose atomic commit failed.
    pub fn persistence_failures(&self) -> u64 {
        self.persistence_failures
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
    fn record_mempool_evidence(&self, now: i64, snapshot: &EvidenceSnapshot, cfg: &FeeCfgSnapshot) {
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
                    if let Err(e) = store.record_seed_event(event) {
                        // Provenance is part of the seed contract: without
                        // it the import didn't happen. Roll the hydration
                        // back and retry next cycle (the seed is
                        // deterministic over the snapshot).
                        clear_hydrated_state(&mut self.state);
                        return Err(format!(
                            "seed provenance record failed: {e:#} (seed rolled back; will \
                             retry next cycle)"
                        ));
                    }
                    HydrationSource::PythonSeed
                }
                SeedOutcome::Refused(event) => {
                    self.seed_refused = true;
                    if let Err(e) = store.record_seed_event(event) {
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
            return Err(format!("restart marker record failed: {e:#}"));
        }
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

        let applied = self.apply_failure_nudge(signal);

        let trigger = FeeTrigger::FailedForward {
            channel_id: channel_id.to_string(),
        };
        let detail = match applied {
            NudgeOutcome::Applied {
                implied_fee,
                weight,
            } => format!(
                "failed-forward posterior nudge APPLIED: target {implied_fee} ppm, weight \
                 {weight:.4} -- did not itself run a cycle"
            ),
            NudgeOutcome::Skipped(reason) => {
                format!("failed-forward nudge NOT applied ({reason}) -- did not itself run a cycle")
            }
        };
        let outcome = self.trigger_queue.offer(trigger.clone(), now);
        if matches!(outcome, TriggerOutcome::Dropped) {
            eprintln!(
                "revops: TRIGGER DROPPED (bounded queue at capacity): failed_forward channel \
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
            &detail,
        );
    }

    /// The nudge itself. Split out so every guard can be driven directly
    /// and the receipt text above cannot claim an application that did not
    /// happen -- the caller consumes this return value, so the effect
    /// cannot be deleted without the call site failing to compile.
    fn apply_failure_nudge(&mut self, signal: &FailedForwardSignal) -> NudgeOutcome {
        let channel_id = signal.channel_id.as_str();
        let now = signal.event_ts;

        if channel_id.is_empty() {
            return NudgeOutcome::Skipped("no outgoing channel on the event");
        }
        if !dynamics::is_fee_relevant_failure(signal.failcode, signal.failreason.as_deref()) {
            return NudgeOutcome::Skipped("not a fee-relevant failure (audit DTS-4b)");
        }
        if let Some(applied_ts) = self.last_fee_apply_ts.get(channel_id) {
            if *applied_ts != 0 && now - *applied_ts < FAILURE_NUDGE_GOSSIP_SETTLE_SECONDS {
                return NudgeOutcome::Skipped(
                    "inside the gossip-settle window after our own apply",
                );
            }
        }
        if let Some(last_nudge) = self.last_failure_nudge_ts.get(channel_id) {
            if *last_nudge != 0 && now - *last_nudge < FAILURE_NUDGE_MIN_INTERVAL_SECONDS {
                return NudgeOutcome::Skipped("rate limited: already nudged this window");
            }
        }
        let Some(fee_state) = self.state.fee_states.get_mut(channel_id) else {
            return NudgeOutcome::Skipped(
                "no persisted DTS evidence for this channel: a failed forward must never be a \
                 channel's first posterior evidence",
            );
        };

        // py reads `cfs.last_fee_ppm` in the PRODUCER under `_state_lock`
        // (cl-revenue-ops.py:6932) and skips when it is not positive; Rust
        // owns that state on this thread, so the read happens here instead.
        // Same guard, same outcome -- only the order relative to the state
        // lookup differs, and both orders end in "no nudge".
        let current_fee_ppm = fee_state.last_fee_ppm;
        if current_fee_ppm <= 0 {
            return NudgeOutcome::Skipped("channel has no positive current fee to imply from");
        }
        let implied_fee = dynamics::failed_forward_implied_fee(current_fee_ppm);
        // py: amount_sats = amount_msat / 1000, and the weight helper
        // reproduces the 0.1 base plus the log10 boost exactly.
        let weight = dynamics::failed_forward_nudge_weight(signal.amount_msat as f64 / 1000.0);
        dynamics::record_posterior_nudge(&mut fee_state.thompson, implied_fee as f64, weight, now);
        self.last_failure_nudge_ts
            .insert(channel_id.to_string(), now);
        NudgeOutcome::Applied {
            implied_fee,
            weight,
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

    /// Total triggers ever dropped for backpressure (Task 6's red
    /// counter, alongside [`Self::persistence_failures`]).
    pub fn trigger_queue_dropped_total(&self) -> u64 {
        self.trigger_queue.dropped_total()
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
            FeeDebugQuery::RunwayCounters => serde_json::json!({
                "lifecycle": match self.lifecycle {
                    StateLifecycle::RehydratePerCycle => "rehydrate_per_cycle",
                    StateLifecycle::SeedOnce => "seed_once",
                },
                "hydrated_once": self.hydrated_once,
                "seed_refused": self.seed_refused,
                "persistence_failures": self.persistence_failures,
                "trigger_queue": {
                    "pending": self.trigger_queue.pending_len(),
                    "dropped_total": self.trigger_queue.dropped_total(),
                },
                "last_cycle": {
                    "at": self.last_cycle_at,
                    "outcome": self.last_cycle_outcome,
                },
                "last_profile": self.last_profile,
                "governor_ledger_open": self.governor.ledger_open(),
            }),
        }
    }
}

/// Cheap handle to the running scheduler (stored in `main.rs`' `State`
/// for T7's RPC/wake senders).
pub struct SchedulerHandle {
    /// Owner-thread channel (cycle messages; T7's debug/wake variants).
    pub tx: mpsc::Sender<CycleMsg>,
    /// Async-side wake channel: one `()` == "prefetch and run a cycle
    /// NOW" (the tokio half of the `RunCycleNow` path).
    pub wake_tx: tokio::sync::mpsc::UnboundedSender<()>,
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
    let (tx, rx) = mpsc::channel::<CycleMsg>();
    let (wake_tx, wake_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let socket_path = cfg.socket_path.clone();
    let db_path = cfg.db_path.clone();
    let trigger = cfg.trigger;

    // (a) The owner thread: state + the ONE PyRandom live here, nowhere
    // else. `now_unix()` here is the spawn-time SEED read; per-cycle
    // clock reads happen inside `run_cycle` (exactly one each). Spawned
    // FIRST: if it fails, the trigger task is never started and the
    // caller gets `Err` instead of a dead-letter handle.
    let owner_wake = wake_tx.clone();
    let owner_body: Box<dyn FnOnce() + Send + 'static> = Box::new(move || {
        let mut owner = CycleOwner::new(&cfg, crate::now_unix(), store);
        let mut clock = crate::now_unix;
        while let Ok(msg) = rx.recv() {
            match msg {
                CycleMsg::RunPrepared(prepared) => {
                    // Outcome logging happens inside run_cycle; the
                    // loop must survive every outcome.
                    let _ = owner.run_cycle(*prepared, &mut clock);
                }
                CycleMsg::RunCycleNow => {
                    // Only the async half can prefetch; hand over.
                    let _ = owner_wake.send(());
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
                CycleMsg::FailedForward(signal) => {
                    owner.handle_failed_forward(&signal);
                }
                CycleMsg::ForwardEvent { channel_id } => {
                    owner.handle_forward_event(&channel_id, crate::now_unix());
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

    Ok(SchedulerHandle { tx, wake_tx })
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
    tick_tx: &mpsc::Sender<CycleMsg>,
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
            if tick_tx
                .send(CycleMsg::RunPrepared(Box::new(prepared)))
                .is_err()
            {
                Dispatch::OwnerGone
            } else {
                Dispatch::Sent(interval_secs)
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
    tick_tx: mpsc::Sender<CycleMsg>,
    mut wake_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
) {
    // Initial cadence resolution -- schedule/staleness seed only; every
    // cycle's authoritative cfg is resolved in prepare_cycle.
    let mut interval_secs =
        fee_config::resolve_fee_cfg(db_handle.as_ref(), &python_options.snapshot())
            .await
            .fee_interval
            .max(1) as u64;

    match trigger {
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
                    && tick_tx.send(CycleMsg::VegasSpikeCheck).is_err()
                {
                    return; // owner thread gone
                }
            }
        }
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
