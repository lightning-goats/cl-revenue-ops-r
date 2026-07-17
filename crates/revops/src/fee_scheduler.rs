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
//! (b) **One tokio ticker task** that, each `fee_interval`, performs the
//!     ASYNC half of a cycle -- `fee_config::resolve_fee_cfg` (T1, per
//!     cycle so runtime `revenue-config set` changes on the Python side
//!     stay visible), the `neighbor_median_min_competitors` resolution
//!     (T1's verify==3 rule), and `fee_evidence::prefetch_rpc` (T2) -- and
//!     sends the prepared inputs to the owner thread as one
//!     [`CycleMsg::RunPrepared`] message.
//!
//! ## Tick alignment (Design Note 1)
//!
//! The window's lifecycle is re-hydrate-per-cycle: every cycle re-reads
//! Python's persisted `v2_state_json` flush so both controllers start the
//! cycle from the same state. That only works if Rust hydrates AFTER
//! Python's end-of-cycle flush, so the ticker fires at `fee_interval` with
//! a fixed [`TICK_PHASE_OFFSET_SECS`] (+120s) phase offset from plugin
//! start -- the same 120s tolerance `tools/diff-harness/
//! diff_fee_decisions.py` matches decision pairs with. At cutover the
//! [`StateLifecycle::SeedOnce`] variant flips hydration to
//! once-at-start-then-evolve-in-memory: a scheduler config change, not a
//! rework (Design Note 1's recorded consequence).
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

/// Fixed tick phase offset from plugin start: Rust's hydrate must land
/// AFTER Python's end-of-cycle state flush (Design Note 1, T6 Step 4) --
/// the same 120s tolerance the diff harness matches with.
pub const TICK_PHASE_OFFSET_SECS: u64 = 120;

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

/// Spawn the scheduler: the owner thread (a) and the ticker task (b).
/// Must be called from within the plugin's tokio runtime. Returns the
/// cheap [`SchedulerHandle`]; dropping every clone of `handle.tx` plus a
/// `Shutdown` message winds both halves down.
pub fn spawn(
    cfg: SchedulerConfig,
    db_handle: Option<DbHandle>,
    python_option_values: HashMap<String, OptValue>,
) -> SchedulerHandle {
    let (tx, rx) = mpsc::channel::<CycleMsg>();
    let (wake_tx, mut wake_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let socket_path = cfg.socket_path.clone();

    // (a) The owner thread: state + the ONE PyRandom live here, nowhere
    // else. `now_unix()` here is the spawn-time SEED read; per-cycle
    // clock reads happen inside `run_cycle` (exactly one each).
    let owner_wake = wake_tx.clone();
    let spawned = std::thread::Builder::new()
        .name("revops-fee-cycle".to_string())
        .spawn(move || {
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
    if let Err(e) = spawned {
        // Handle stays usable (sends just go nowhere); a node that cannot
        // spawn a thread has bigger problems, but the plugin must not
        // panic over the fee cycle.
        eprintln!("revops: failed to spawn fee-cycle owner thread: {e}; fee dry-run disabled");
    }

    // (b) The ticker: async prefetch each fee_interval, phase-offset
    // +120s from plugin start (see TICK_PHASE_OFFSET_SECS' doc comment).
    let tick_tx = tx.clone();
    tokio::spawn(async move {
        // Initial cadence resolution -- for the FIRST tick's schedule
        // only; every cycle's authoritative cfg is resolved in
        // prepare_cycle below.
        let mut interval_secs =
            fee_config::resolve_fee_cfg(db_handle.as_ref(), &python_option_values)
                .await
                .fee_interval
                .max(1) as u64;
        let mut next = tokio::time::Instant::now()
            + Duration::from_secs(interval_secs + TICK_PHASE_OFFSET_SECS);
        loop {
            // A tick advances the phase-locked schedule; a wake runs an
            // extra cycle without disturbing it.
            let ticked = tokio::select! {
                _ = tokio::time::sleep_until(next) => true,
                wake = wake_rx.recv() => {
                    if wake.is_none() {
                        break; // every wake sender dropped
                    }
                    false
                }
            };
            match prepare_cycle(&socket_path, db_handle.as_ref(), &python_option_values).await {
                Ok(prepared) => {
                    interval_secs = prepared.cfg.fee_interval.max(1) as u64;
                    if tick_tx
                        .send(CycleMsg::RunPrepared(Box::new(prepared)))
                        .is_err()
                    {
                        break; // owner thread gone
                    }
                }
                Err(e) => {
                    eprintln!("revops: fee cycle prefetch failed ({e:#}); cycle skipped");
                }
            }
            if ticked {
                next += Duration::from_secs(interval_secs);
            }
        }
    });

    SchedulerHandle { tx, wake_tx }
}
