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
//!     `neighbor_median_min_competitors` resolution (T1's verify==3 rule),
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
//! `CycleDeps::now` / `build_evidence_snapshot` to every downstream
//! consumer (Global Constraint: "clock once per cycle"). The clock is an
//! injected `FnMut() -> i64` so tests can count reads; production passes
//! `crate::now_unix`.
//!
//! ## What this module never does
//!
//! No broadcast: there is no fee-broadcast RPC call anywhere in this crate
//! (structural dry-run safety -- `tests/fee_scheduler.rs`' source-scan
//! guard enforces the literal's absence). The production DB is opened
//! read-only (via `fee_evidence`), and every write target (decision
//! journal, state JSONL, dry-run econ ledger) is a Rust-owned file under
//! the journal directory. Python stays authoritative for the whole window.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use cln_plugin::options::Value as OptValue;
use revops_db::actor::DbHandle;
use revops_fees::cycle::{run_fee_cycle, ControllerState, CycleDeps, FeeCfgSnapshot, StateSink};
use revops_fees::journal::Journal;
use revops_fees::pyrand::PyRandom;

use crate::fee_config;
use crate::fee_evidence::{build_evidence_snapshot, prefetch_rpc, RpcPrefetch};
use crate::fee_governor::GovernorWiring;
use crate::fee_state::{rehydrate, JournalStateSink};

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

/// Messages on the owner thread's channel.
///
/// T7 extends this enum with its wake/debug variants (`PolicyChanged`,
/// `VegasSpikeCheck`, `WakeAll`, `Query`) -- the owner-thread channel is
/// the stable seam those triggers land on.
pub enum CycleMsg {
    /// One cycle's prepared inputs from the async prefetch half; one
    /// message == one cycle on the owner thread.
    RunPrepared(Box<PreparedCycle>),
    /// Ask for an immediate out-of-schedule cycle: the owner thread
    /// forwards this to the async half (only IT can prefetch), which
    /// prepares inputs and sends back a `RunPrepared`.
    RunCycleNow,
    Shutdown,
}

/// The async half's per-cycle output: everything the owner thread needs
/// to run one cycle without performing any IO of its own besides the
/// read-only evidence snapshot.
pub struct PreparedCycle {
    /// T1: freshly resolved 22-field snapshot (per cycle, so DB overrides
    /// written by Python's `revenue-config set` stay visible).
    pub cfg: FeeCfgSnapshot,
    /// T1 verify==3 rule: the typed per-cycle resolution of
    /// `neighbor_median_min_competitors` (NOT a `FeeCfgSnapshot` field).
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
    /// T1 fail-closed rule: `neighbor_median_min_competitors` resolved to
    /// something other than the baked 3.
    SkippedMinCompetitors,
    /// `build_evidence_snapshot` failed (DB open/read error).
    SkippedEvidence,
}

/// The single owner of `ControllerState` + the ONE long-lived `PyRandom`.
/// Lives on the dedicated cycle thread for the plugin's whole lifetime;
/// tests drive it synchronously.
pub struct CycleOwner {
    state: ControllerState,
    /// Seeded exactly ONCE, in [`CycleOwner::new`] (production: from
    /// `now_unix()` at spawn). Never reseeded -- every cycle continues
    /// this one stream, mirroring Python's module-level `random` instance.
    rng: PyRandom,
    lifecycle: StateLifecycle,
    /// `SeedOnce` only: whether the one-time hydration has happened.
    hydrated_once: bool,
    db_path: PathBuf,
    /// `None` only if the journal dir could not be created -- logged
    /// loudly at construction; cycles still run (decisions are lost to
    /// disk but the plugin must never crash over bookkeeping IO).
    journal: Option<Journal>,
    state_sink: Option<JournalStateSink>,
    governor: GovernorWiring,
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
    pub fn new(cfg: &SchedulerConfig, seed_now: i64) -> CycleOwner {
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
            hydrated_once: false,
            db_path: cfg.db_path.clone(),
            journal,
            state_sink,
            governor: GovernorWiring::open(Some(&cfg.journal_dir)),
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
    ///    consumer (evidence snapshot windows, `CycleDeps::now`).
    /// 2. Fail closed if `min_competitors != 3` (T1 rule; the market
    ///    functions bake `MIN_COMPETITORS = 3`).
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

        // (2) T1 fail-closed verify==3 gate.
        if !fee_config::neighbor_median_min_competitors_ok(&prepared.min_competitors) {
            eprintln!(
                "revops: fee cycle disabled: neighbor_median_min_competitors={} != baked 3 \
                 (skipping cycle)",
                prepared.min_competitors
            );
            return CycleOutcome::SkippedMinCompetitors;
        }

        // (3) Per-cycle-frozen evidence (read-only DB + prefetched RPC).
        let snapshot = match build_evidence_snapshot(&self.db_path, prepared.rpc, now) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("revops: fee cycle skipped: evidence snapshot failed: {e:#}");
                return CycleOutcome::SkippedEvidence;
            }
        };

        // (4) State lifecycle (Design Note 1), over the snapshot's pinned
        // read-only connection -- hydration sees the exact same frozen DB
        // view as every other evidence read this cycle.
        match self.lifecycle {
            StateLifecycle::RehydratePerCycle => rehydrate(&mut self.state, snapshot.conn()),
            StateLifecycle::SeedOnce => {
                if !self.hydrated_once {
                    rehydrate(&mut self.state, snapshot.conn());
                    self.hydrated_once = true;
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
        let governed = self.governor.governed_deps(&prepared.cfg);
        let mut deps = CycleDeps {
            evidence: &snapshot,
            cfg: &prepared.cfg,
            rng: &mut self.rng,
            now,
            governed: Some(&governed),
            journal: None,
            state_sink: self.state_sink.as_ref().map(|s| s as &dyn StateSink),
        };
        let decisions = run_fee_cycle(&mut self.state, &mut deps);

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

        CycleOutcome::Ran {
            decisions: decisions.len(),
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
    python_option_values: HashMap<String, OptValue>,
) -> anyhow::Result<SchedulerHandle> {
    spawn_with_thread_spawner(cfg, db_handle, python_option_values, |name, body| {
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
    python_option_values: HashMap<String, OptValue>,
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
        let mut owner = CycleOwner::new(&cfg, crate::now_unix());
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
        python_option_values,
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
    python_option_values: &HashMap<String, OptValue>,
    tick_tx: &mpsc::Sender<CycleMsg>,
) -> Dispatch {
    match prepare_cycle(socket_path, db_handle, python_option_values).await {
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
    python_option_values: HashMap<String, OptValue>,
    tick_tx: mpsc::Sender<CycleMsg>,
    mut wake_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
) {
    // Initial cadence resolution -- schedule/staleness seed only; every
    // cycle's authoritative cfg is resolved in prepare_cycle.
    let mut interval_secs = fee_config::resolve_fee_cfg(db_handle.as_ref(), &python_option_values)
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
                match dispatch_cycle(
                    &socket_path,
                    db_handle.as_ref(),
                    &python_option_values,
                    &tick_tx,
                )
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
                        &python_option_values,
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
                match watcher.on_poll(marker, crate::now_unix(), &params) {
                    PollOutcome::RunCycle => {
                        match dispatch_cycle(
                            &socket_path,
                            db_handle.as_ref(),
                            &python_option_values,
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
            }
        }
    }
}
