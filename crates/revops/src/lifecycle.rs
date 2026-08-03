//! R68-1: the lifecycle kernel -- store identity, the current-boot startup
//! receipt, and bounded shutdown ordering.
//!
//! Contract re-derived from `fixtures/port/plugin_inventory.json`:
//!
//! - `shutdown`: `{name: "rpc-shutdown", bounded: true,
//!   join_timeout_seconds: 10.0, semantics: "daemon drain thread; bounded
//!   wait; process exit proceeds on timeout"}` (cl-revenue-ops.py:599).
//! - the four `@plugin.subscribe` bindings: `forward_event`, `connect`,
//!   `disconnect`, `channel_state_changed`.
//!
//! Kept free of `main.rs` and the canonical RPC surface on purpose (R68-1
//! runs in parallel with Task 66's RPC closure), so everything here is
//! reachable from an integration test.

use std::path::{Path, PathBuf};
use std::time::Duration;

// =====================================================================
// store identity
// =====================================================================

/// Why a configured pair of stores cannot be accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreIdentityRefusal {
    /// The two paths name ONE file.
    Alias {
        observer: PathBuf,
        production: PathBuf,
        detail: String,
    },
    /// A configured path could not be identified at all. Deliberately not
    /// folded into "distinct": an unidentifiable database is not a
    /// different one.
    Unreadable { path: PathBuf, detail: String },
}

impl StoreIdentityRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Alias { .. } => "store_identity_alias",
            Self::Unreadable { .. } => "store_identity_unreadable",
        }
    }
}

/// `(st_dev, st_ino)` -- the only thing that actually identifies a file.
fn file_identity(path: &Path) -> std::io::Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(path)?;
    Ok((meta.dev(), meta.ino()))
}

/// Resolve a path that may not exist yet, by canonicalizing its PARENT.
/// A symlinked directory is how a not-yet-created file still lands on an
/// existing one.
fn resolve_absent(path: &Path) -> PathBuf {
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) => match std::fs::canonicalize(parent) {
            Ok(parent) => parent.join(name),
            Err(_) => path.to_path_buf(),
        },
        _ => path.to_path_buf(),
    }
}

/// Refuse when the observer store and the production database are the same
/// file.
///
/// The guard this replaces compared CANONICALIZED PATHS. That resolves
/// symlinks but is blind to HARD LINKS: two distinct paths, two distinct
/// canonical paths, one inode. A hardlinked observer path would then be
/// opened read-WRITE while being the production database -- the single
/// invariant the whole shadow window rests on.
///
/// Identity is therefore `(device, inode)` whenever both files exist. When
/// the observer file does not exist yet -- the ordinary first-run case --
/// it cannot be a hard link to anything, so the check falls back to
/// comparing parent-resolved paths, which still catches a symlinked
/// directory.
///
/// A configured production path that exists but cannot be stat'd REFUSES.
/// A failed stat is not evidence of distinctness.
pub fn assert_distinct_stores(
    observer: &Path,
    production: Option<&Path>,
) -> Result<(), StoreIdentityRefusal> {
    let Some(production) = production else {
        return Ok(());
    };

    let production_identity = match file_identity(production) {
        Ok(identity) => Some(identity),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(StoreIdentityRefusal::Unreadable {
                path: production.to_path_buf(),
                detail: format!(
                    "the configured production database could not be identified ({error}); \
                     an unidentifiable database is not a distinct one"
                ),
            })
        }
    };

    match file_identity(observer) {
        Ok(observer_identity) => {
            if Some(observer_identity) == production_identity {
                let (dev, ino) = observer_identity;
                return Err(StoreIdentityRefusal::Alias {
                    observer: observer.to_path_buf(),
                    production: production.to_path_buf(),
                    detail: format!(
                        "both paths resolve to device {dev} inode {ino}; opening this \
                         read-write would write the production database"
                    ),
                });
            }
            Ok(())
        }
        // Not created yet: cannot be a hard link. Still resolve through a
        // symlinked parent.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if resolve_absent(observer) == resolve_absent(production) {
                return Err(StoreIdentityRefusal::Alias {
                    observer: observer.to_path_buf(),
                    production: production.to_path_buf(),
                    detail: "the observer path does not exist yet but resolves onto the \
                             production database"
                        .to_string(),
                });
            }
            Ok(())
        }
        Err(error) => Err(StoreIdentityRefusal::Unreadable {
            path: observer.to_path_buf(),
            detail: format!("the configured observer store could not be identified ({error})"),
        }),
    }
}

// =====================================================================
// current-boot startup receipt
// =====================================================================

/// Proof that startup completed -- in THIS process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupReceipt {
    pub boot_id: String,
    pub completed_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupRefusal {
    /// Nothing recorded. Absence is not completion.
    NoReceiptThisBoot,
    /// A receipt exists, but a PREVIOUS process wrote it.
    StalePriorBoot { row_boot_id: String },
}

impl StartupRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NoReceiptThisBoot => "startup_no_receipt_this_boot",
            Self::StalePriorBoot { .. } => "startup_stale_prior_boot",
        }
    }
}

/// A receipt counts only when this boot wrote it.
///
/// The failure this prevents is the durable-store equivalent of a stale
/// pass: a receipt left in the store by a previous process, read by a
/// fresh one as proof that IT finished starting up.
pub fn classify_startup_receipt(
    receipt: Option<&StartupReceipt>,
    boot_id: &str,
) -> Result<(), StartupRefusal> {
    let Some(receipt) = receipt else {
        return Err(StartupRefusal::NoReceiptThisBoot);
    };
    if receipt.boot_id != boot_id {
        return Err(StartupRefusal::StalePriorBoot {
            row_boot_id: receipt.boot_id.clone(),
        });
    }
    Ok(())
}

// =====================================================================
// R68-3: startup ordering
// =====================================================================

/// The ordered startup phases. Like [`Phase`], the order IS the contract.
///
/// Two orderings carry the weight. Intake is bound only after the stores
/// are open and the deferred cursors are hydrated -- bound earlier, every
/// notification in the gap is a `Dropped` (R68-2 counts those, but not
/// creating the hole beats counting it) and hydration would re-derive a
/// cursor from rows arriving underneath it. And the current-boot receipt
/// is written LAST, so [`classify_startup_receipt`] can never pass for a
/// process that only got halfway up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StartupPhase {
    /// [`assert_distinct_stores`] -- before anything is opened read-write.
    StoresIdentified,
    StoresOpened,
    CursorsHydrated,
    OwnersStarted,
    IntakeBound,
    ReceiptWritten,
}

impl StartupPhase {
    pub const ALL: [StartupPhase; 6] = [
        StartupPhase::StoresIdentified,
        StartupPhase::StoresOpened,
        StartupPhase::CursorsHydrated,
        StartupPhase::OwnersStarted,
        StartupPhase::IntakeBound,
        StartupPhase::ReceiptWritten,
    ];
}

/// What actually came up. `completed` is true only when every phase ran
/// and none failed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StartupOutcome {
    pub completed: bool,
    pub reached: Vec<StartupPhase>,
    pub failure: Option<String>,
}

/// The steps a real runtime supplies, in the same shape as
/// [`LifecycleSteps`] so both halves of the process lifetime are driven
/// by one testable contract.
pub trait StartupSteps: Send + Sync {
    fn identify_stores(&self) -> StepFuture<'_>;
    fn open_stores(&self) -> StepFuture<'_>;
    fn hydrate_cursors(&self) -> StepFuture<'_>;
    fn start_owners(&self) -> StepFuture<'_>;
    fn bind_intake(&self) -> StepFuture<'_>;
    fn write_receipt(&self) -> StepFuture<'_>;
}

/// Bring the plugin up in order, ABORTING at the first failed step.
///
/// That is the deliberate asymmetry against [`shutdown`], which runs its
/// remaining steps even after one fails. Shutdown continues because
/// skipping a later step loses data the failure itself did not. Startup
/// stops because every step after a failure -- above all binding the four
/// subscriptions -- would accept work this process has nowhere to keep.
///
/// Unbounded on purpose: Python's plugin init is not time-bounded either,
/// and a bound here would mean "give up waiting for the store and start
/// taking notifications anyway", which is the exact failure the ordering
/// exists to prevent. The bound belongs on shutdown, where the alternative
/// to giving up is never exiting.
pub async fn startup<S>(steps: std::sync::Arc<S>) -> StartupOutcome
where
    S: StartupSteps + 'static,
{
    let mut outcome = StartupOutcome::default();
    for phase in StartupPhase::ALL {
        let result = match phase {
            StartupPhase::StoresIdentified => steps.identify_stores().await,
            StartupPhase::StoresOpened => steps.open_stores().await,
            StartupPhase::CursorsHydrated => steps.hydrate_cursors().await,
            StartupPhase::OwnersStarted => steps.start_owners().await,
            StartupPhase::IntakeBound => steps.bind_intake().await,
            StartupPhase::ReceiptWritten => steps.write_receipt().await,
        };
        outcome.reached.push(phase);
        if let Err(detail) = result {
            outcome.failure = Some(format!("{phase:?}: {detail}"));
            return outcome;
        }
    }
    outcome.completed = true;
    outcome
}

// =====================================================================
// R68-3: retention health
// =====================================================================

/// How long retention may produce nothing before that is a fault.
///
/// A sweep rides only a SUCCESSFUL SCHEDULED CYCLE COMMIT, and py's
/// `fee_interval` defaults to 1800s (cl-revenue-ops.py:4369), so the
/// window must tolerate several non-committing intervals in a row. Four
/// hours is eight default intervals.
pub const RETENTION_MAX_SILENCE_SECONDS: i64 = 4 * 3600;

/// How long an unbroken run of TRUNCATED sweeps may last before it stops
/// being catch-up and starts being a growth trend.
///
/// Each sweep deletes up to `RETENTION_MAX_BATCHES_PER_SWEEP *
/// RETENTION_BATCH_ROWS` rows, so a backlog left by a bounded outage
/// drains in a bounded number of sweeps. Truncation still continuous a
/// full day later means arrivals are outrunning deletions.
pub const RETENTION_TRUNCATION_TOLERANCE_SECONDS: i64 = 24 * 3600;

/// What the sweep owner has observed. Produced by the fee scheduler, which
/// is the only thing that schedules sweeps.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RetentionObservation {
    /// Task 59's never-reset red counter.
    pub failures: u64,
    /// Reported, never gated on: a count says nothing about WHEN.
    pub sweeps_completed: u64,
    /// When the last sweep RETURNED. `None` = none has, this process.
    pub last_sweep_at: Option<i64>,
    /// When the current unbroken run of truncated sweeps began. Cleared by
    /// any sweep that completes without truncating.
    pub truncated_since: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionRefusal {
    /// A sweep that ran, failed.
    Failing { failures: u64 },
    /// No sweep has run at all this process.
    NeverSwept { silent_seconds: i64 },
    /// Sweeps ran and then stopped.
    Stalled { silent_seconds: i64 },
    /// Sweeps run, succeed, and never catch up.
    PersistentlyTruncated { truncated_seconds: i64 },
}

impl RetentionRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Failing { .. } => "retention_sweep_failing",
            Self::NeverSwept { .. } => "retention_never_swept",
            Self::Stalled { .. } => "retention_stalled",
            Self::PersistentlyTruncated { .. } => "retention_persistently_truncated",
        }
    }
}

/// Age in seconds.
///
/// Saturating because both operands come from wall clocks that a
/// correction, a bad config or a corrupt row can put arbitrarily far
/// apart, and a health check that PANICS on a strange timestamp is worse
/// than one that reports a strange age.
///
/// Deliberately NOT clamped at zero. A `.max(0)` was here and no input
/// could observe it -- a negative age already compares as "well inside the
/// window", which is the right answer for a clock that stepped backwards.
/// An unfalsifiable clause is one a later refactor can quietly invert.
fn age(now: i64, since: i64) -> i64 {
    now.saturating_sub(since)
}

/// Readiness over retention.
///
/// Task 59 counts sweeps that FAILED. Nothing counted the two states that
/// precede disk exhaustion with `failures == 0` the whole way: sweeps that
/// stopped happening, and sweeps that run forever without catching up.
/// This node has already run itself out of disk once.
///
/// No Python parity applies: Python has no automated retention at all
/// (only an operator-run `DELETE FROM` hint, cl-revenue-ops.py:7447).
pub fn retention_is_healthy(
    observation: &RetentionObservation,
    startup_at: i64,
    now: i64,
) -> Result<(), RetentionRefusal> {
    // An explicit error first: when retention has both failed and gone
    // quiet, the failure is the actionable cause, and reporting the
    // silence would send the operator after a stopped cycle loop instead
    // of reading the error.
    if observation.failures > 0 {
        return Err(RetentionRefusal::Failing {
            failures: observation.failures,
        });
    }

    // Silence is measured from the last sweep, falling back to boot so a
    // process that has never swept is judged from when it could have.
    let silent_seconds = age(now, observation.last_sweep_at.unwrap_or(startup_at));
    if silent_seconds > RETENTION_MAX_SILENCE_SECONDS {
        return Err(match observation.last_sweep_at {
            None => RetentionRefusal::NeverSwept { silent_seconds },
            Some(_) => RetentionRefusal::Stalled { silent_seconds },
        });
    }

    if let Some(since) = observation.truncated_since {
        let truncated_seconds = age(now, since);
        if truncated_seconds > RETENTION_TRUNCATION_TOLERANCE_SECONDS {
            return Err(RetentionRefusal::PersistentlyTruncated { truncated_seconds });
        }
    }

    Ok(())
}

// =====================================================================
// bounded shutdown
// =====================================================================

/// py `join_timeout_seconds: 10.0` from the generated shutdown contract.
pub const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The ordered phases. The order is the contract, and it is not arbitrary:
/// cursors persist only after intake stops (otherwise the cursor races new
/// work), and ACTION owners drain before OBSERVER owners (an action still
/// in flight can produce observations the observer must still record).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    IntakeStopped,
    CursorsPersisted,
    ActionOwnersDrained,
    ObserverOwnersDrained,
    StoresFlushed,
    Joined,
}

/// What actually happened. `completed` is true ONLY for a clean, fully
/// joined shutdown.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShutdownOutcome {
    pub completed: bool,
    pub timed_out: bool,
    pub reached: Vec<Phase>,
    pub failures: Vec<String>,
}

/// One step's future.
///
/// Spelled out as a boxed `Send` future rather than a bare `async fn`
/// because the sequence is SPAWNED (see [`shutdown`]) -- native
/// async-fn-in-trait gives no `Send` guarantee, and spawning is what makes
/// this a faithful model of Python's daemon drain thread. `Pin`/`Box`/
/// `Future` are all std, so this needs no new dependency.
pub type StepFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>;

/// One owner's drain acknowledgement, in flight.
pub type DrainFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = DrainAck> + Send + 'a>>;

/// One owner's join acknowledgement, in flight.
pub type JoinFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = JoinAck> + Send + 'a>>;

/// The steps a real runtime supplies. Kept as a trait so the ordering, the
/// failure handling and the bound are testable without a live plugin.
///
/// R68-5: the owner half reports PER OWNER. `drain_action_owners` and
/// friends returned one `Result<(), String>` for all seven owners at
/// once, which is exactly the `.ok()`-to-null conversion Task 68 forbids
/// -- a single `Ok(())` cannot distinguish an owner that finished from
/// one that was already gone, never answered, or stopped with work still
/// queued, nor a legitimately absent owner from one nobody asked.
///
/// The roster is walked by [`shutdown`] rather than by the implementer,
/// so the action-before-observer order is a property of the kernel.
pub trait LifecycleSteps: Send + Sync {
    fn stop_intake(&self) -> StepFuture<'_>;
    fn persist_cursors(&self) -> StepFuture<'_>;
    fn flush_stores(&self) -> StepFuture<'_>;
    /// Ask ONE owner to stop, and report what it actually said.
    fn drain_owner(&self, owner: Owner) -> DrainFuture<'_>;
    /// Wait for ONE owner's task to end, and report how it ended.
    fn join_owner(&self, owner: Owner) -> JoinFuture<'_>;
}

/// Run the whole shutdown under ONE bound.
///
/// py runs the drain on a daemon thread and joins with a timeout, so
/// "process exit proceeds on timeout". The sequence is spawned here for
/// the same reason, and it buys the same two properties: a wedged owner
/// cannot hold exit open, and a PANICKING owner is caught by the runtime
/// and surfaces as a named failure instead of unwinding the caller. The
/// bound covers the whole sequence, not each step -- a per-step bound
/// would let six wedged steps take six times as long.
///
/// A failing step does NOT skip the remaining ones. Leaving stores
/// unflushed because an earlier owner failed loses data the failure itself
/// did not.
///
/// R68-5: `ledger` is supplied by the CALLER and outlives the call. The
/// bound can cut the roster short, and an owner past the cut was never
/// asked -- building the ledger inside the spawned task would throw that
/// evidence away with the task, leaving "wedged before we reached it"
/// indistinguishable from "reported clean". The verdict in `failures` is
/// a snapshot taken at decision time: on timeout the detached task may
/// still be running (py: "process exit proceeds on timeout"), so a later
/// read of the ledger can differ from the outcome that was returned.
pub async fn shutdown<S>(
    steps: std::sync::Arc<S>,
    ledger: std::sync::Arc<DrainLedger>,
    timeout: Duration,
) -> ShutdownOutcome
where
    S: LifecycleSteps + 'static,
{
    let mut outcome = ShutdownOutcome::default();

    let owner_ledger = std::sync::Arc::clone(&ledger);
    let handle = tokio::spawn(async move {
        let mut reached = Vec::new();
        let mut failures = Vec::new();
        macro_rules! run {
            ($phase:expr, $call:expr) => {{
                match $call.await {
                    Ok(()) => reached.push($phase),
                    Err(detail) => {
                        reached.push($phase);
                        failures.push(format!("{:?}: {detail}", $phase));
                    }
                }
            }};
        }
        run!(Phase::IntakeStopped, steps.stop_intake());
        run!(Phase::CursorsPersisted, steps.persist_cursors());

        // Both halves walk the WHOLE roster, not the handles that happen
        // to exist: an owner nobody asked about must be `Unreported`, and
        // absence must be declared by the owner half itself.
        for owner in Owner::ALL {
            if owner.class() == OwnerClass::Action {
                owner_ledger.record(owner, steps.drain_owner(owner).await);
            }
        }
        reached.push(Phase::ActionOwnersDrained);
        for owner in Owner::ALL {
            if owner.class() == OwnerClass::Observer {
                owner_ledger.record(owner, steps.drain_owner(owner).await);
            }
        }
        reached.push(Phase::ObserverOwnersDrained);

        run!(Phase::StoresFlushed, steps.flush_stores());

        for owner in Owner::ALL {
            owner_ledger.record_join(owner, steps.join_owner(owner).await);
        }
        reached.push(Phase::Joined);
        (reached, failures)
    });

    match tokio::time::timeout(timeout, handle).await {
        Err(_elapsed) => {
            outcome.timed_out = true;
            outcome
                .failures
                .push(format!("shutdown exceeded its {timeout:?} bound"));
        }
        // A panicking owner is a LOST owner, not a clean one.
        Ok(Err(join_error)) => {
            outcome
                .failures
                .push(format!("an owner was lost during shutdown: {join_error}"));
        }
        Ok(Ok((reached, failures))) => {
            outcome.reached = reached;
            outcome.failures = failures;
        }
    }

    // R68-5: the owner acknowledgements are part of the verdict, not a
    // side report. Every step can succeed while an owner was never
    // reached, answered nothing, or ended with work still queued -- and
    // under the old trait each of those returned a clean shutdown.
    if let Err(refusal) = drain_is_clean(&ledger) {
        outcome
            .failures
            .push(format!("{}: {}", refusal.code(), refusal.owner().as_str()));
    }
    if let Err(refusal) = join_is_clean(&ledger) {
        outcome
            .failures
            .push(format!("{}: {}", refusal.code(), refusal.owner().as_str()));
    }
    if let Err(refusal) = shutdown_acks_are_consistent(&ledger) {
        outcome
            .failures
            .push(format!("{}: {}", refusal.code(), refusal.owner().as_str()));
    }

    // Two conditions, not three. A `reached.last() == Joined` clause was
    // here and is redundant: every path that misses the join -- a step
    // error, a lost owner, the bound expiring -- already populates
    // `failures` or `timed_out`. It was also unfalsifiable, so no mutation
    // could pin it, and an unpinnable condition is one a future refactor
    // can quietly invert.
    outcome.completed = outcome.failures.is_empty() && !outcome.timed_out;
    outcome
}

// =====================================================================
// R68-4: owner drain and join acknowledgements
// =====================================================================

/// The serialized owners that hold their own task or thread, derived from
/// `main.rs`' retained reporting, fee, and ordinary-rebalance state.
///
/// The roster is fixed rather than "whatever handles happened to be
/// `Some`". Optional action owners must declare absence with
/// [`DrainAck::NotSpawned`] rather than disappearing from shutdown
/// accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Owner {
    /// `State.db` -- the read-only production-DB actor.
    ProductionDb,
    /// `State.observer_db` -- the read-write observer store owner.
    ObserverStore,
    /// `State.scheduler` -- the fee-cycle owner thread.
    FeeScheduler,
    Rebalance,
}

/// Which half of R68-1's shutdown order an owner belongs to.
///
/// Action owners drain BEFORE observer owners because an action still in
/// flight can produce observations the observer must still record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerClass {
    Action,
    Observer,
}

impl Owner {
    pub const ALL: [Owner; 4] = [
        Owner::ProductionDb,
        Owner::ObserverStore,
        Owner::FeeScheduler,
        Owner::Rebalance,
    ];

    pub fn class(self) -> OwnerClass {
        match self {
            Self::ProductionDb | Self::ObserverStore => OwnerClass::Observer,
            Self::FeeScheduler | Self::Rebalance => OwnerClass::Action,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProductionDb => "production_db",
            Self::ObserverStore => "observer_store",
            Self::FeeScheduler => "fee_scheduler",
            Self::Rebalance => "rebalance",
        }
    }
}

/// What ONE owner reported when asked to stop.
///
/// The four states a bare `Ok(())` cannot tell apart. Sending a stop
/// message into a channel returns `Ok` whether or not anyone was
/// listening, so the acknowledgement has to come back FROM the owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainAck {
    /// Not running in this process. Legitimate, and declared.
    NotSpawned,
    /// The owner itself confirmed it stopped. `pending_at_stop` is what
    /// was still queued at that moment -- "I stopped" is not "I
    /// finished".
    Drained { pending_at_stop: u64 },
    /// The stop could not be delivered: the owner was already gone, so
    /// whatever it had queued was never processed. NOT a drain.
    Unreachable { detail: String },
    /// Delivered, and no acknowledgement came back.
    NoAck { waited: Duration },
}

/// What ONE owner's task did after acknowledging the stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinAck {
    NotSpawned,
    Joined,
    Timeout {
        waited: Duration,
    },
    /// R68-1's rule, kept per-owner: a panicking owner is a LOST owner,
    /// not a clean one. Folding a panic into "the task is no longer
    /// running, so it ended" is how a crash reads as a graceful exit.
    Panicked {
        detail: String,
    },
}

/// Both halves of every owner's shutdown report.
///
/// One structure rather than two so the cross-check
/// ([`shutdown_acks_are_consistent`]) cannot be handed mismatched
/// ledgers.
#[derive(Debug, Default)]
pub struct DrainLedger {
    drain: Mutex<[Option<DrainAck>; 4]>,
    join: Mutex<[Option<JoinAck>; 4]>,
}

fn owner_slot(owner: Owner) -> usize {
    match owner {
        Owner::ProductionDb => 0,
        Owner::ObserverStore => 1,
        Owner::FeeScheduler => 2,
        Owner::Rebalance => 3,
    }
}

impl DrainLedger {
    pub fn record(&self, owner: Owner, ack: DrainAck) {
        self.drain.lock().expect("drain ledger poisoned")[owner_slot(owner)] = Some(ack);
    }

    pub fn record_join(&self, owner: Owner, ack: JoinAck) {
        self.join.lock().expect("drain ledger poisoned")[owner_slot(owner)] = Some(ack);
    }

    pub fn drain_ack(&self, owner: Owner) -> Option<DrainAck> {
        self.drain.lock().expect("drain ledger poisoned")[owner_slot(owner)].clone()
    }

    pub fn join_ack(&self, owner: Owner) -> Option<JoinAck> {
        self.join.lock().expect("drain ledger poisoned")[owner_slot(owner)].clone()
    }

    /// Un-report one owner, so a test can prove that a MISSING entry
    /// refuses rather than being read as absence.
    pub fn clear_for_tests(&self, owner: Owner) {
        self.drain.lock().expect("drain ledger poisoned")[owner_slot(owner)] = None;
    }

    pub fn clear_join_for_tests(&self, owner: Owner) {
        self.join.lock().expect("drain ledger poisoned")[owner_slot(owner)] = None;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainRefusal {
    /// Nobody reported on this owner at all.
    Unreported {
        owner: Owner,
    },
    Unreachable {
        owner: Owner,
        detail: String,
    },
    NoAck {
        owner: Owner,
        waited: Duration,
    },
    /// Acknowledged the stop with work still queued.
    LeftWork {
        owner: Owner,
        pending: u64,
    },
}

impl DrainRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unreported { .. } => "drain_owner_unreported",
            Self::Unreachable { .. } => "drain_owner_unreachable",
            Self::NoAck { .. } => "drain_owner_no_ack",
            Self::LeftWork { .. } => "drain_owner_left_work",
        }
    }

    /// Which owner. A refusal code alone tells an operator what went
    /// wrong but not where to look.
    pub fn owner(&self) -> Owner {
        match self {
            Self::Unreported { owner }
            | Self::Unreachable { owner, .. }
            | Self::NoAck { owner, .. }
            | Self::LeftWork { owner, .. } => *owner,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinRefusal {
    Unreported {
        owner: Owner,
    },
    Timeout {
        owner: Owner,
        waited: Duration,
    },
    Panicked {
        owner: Owner,
        detail: String,
    },
    /// The cross-check neither ledger can make alone.
    JoinedWithoutDraining {
        owner: Owner,
    },
}

impl JoinRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unreported { .. } => "join_owner_unreported",
            Self::Timeout { .. } => "join_owner_timeout",
            Self::Panicked { .. } => "join_owner_panicked",
            Self::JoinedWithoutDraining { .. } => "join_without_drain",
        }
    }

    pub fn owner(&self) -> Owner {
        match self {
            Self::Unreported { owner }
            | Self::Timeout { owner, .. }
            | Self::Panicked { owner, .. }
            | Self::JoinedWithoutDraining { owner } => *owner,
        }
    }
}

/// Readiness over the drain half.
///
/// Every owner on the roster, not just the ones a handle existed for --
/// and every owner, not just the first, so the verdict never depends on
/// iteration order.
pub fn drain_is_clean(ledger: &DrainLedger) -> Result<(), DrainRefusal> {
    for owner in Owner::ALL {
        match ledger.drain_ack(owner) {
            None => return Err(DrainRefusal::Unreported { owner }),
            Some(DrainAck::NotSpawned) => {}
            Some(DrainAck::Drained { pending_at_stop: 0 }) => {}
            Some(DrainAck::Drained { pending_at_stop }) => {
                return Err(DrainRefusal::LeftWork {
                    owner,
                    pending: pending_at_stop,
                })
            }
            Some(DrainAck::Unreachable { detail }) => {
                return Err(DrainRefusal::Unreachable { owner, detail })
            }
            Some(DrainAck::NoAck { waited }) => return Err(DrainRefusal::NoAck { owner, waited }),
        }
    }
    Ok(())
}

/// Readiness over the join half.
pub fn join_is_clean(ledger: &DrainLedger) -> Result<(), JoinRefusal> {
    for owner in Owner::ALL {
        match ledger.join_ack(owner) {
            None => return Err(JoinRefusal::Unreported { owner }),
            Some(JoinAck::NotSpawned) | Some(JoinAck::Joined) => {}
            Some(JoinAck::Timeout { waited }) => {
                return Err(JoinRefusal::Timeout { owner, waited })
            }
            Some(JoinAck::Panicked { detail }) => {
                return Err(JoinRefusal::Panicked { owner, detail })
            }
        }
    }
    Ok(())
}

/// The cross-check neither half can make alone.
///
/// An owner reported JOINED that was never reported DRAINED ended while
/// work was still queued -- and both ledgers look clean in isolation.
/// Conversely an owner that acknowledged a drain and is then declared
/// absent at join time is a bookkeeping lie: it plainly existed a moment
/// earlier.
pub fn shutdown_acks_are_consistent(ledger: &DrainLedger) -> Result<(), JoinRefusal> {
    for owner in Owner::ALL {
        let drained = matches!(ledger.drain_ack(owner), Some(DrainAck::Drained { .. }));
        match ledger.join_ack(owner) {
            Some(JoinAck::Joined) if !drained => {
                return Err(JoinRefusal::JoinedWithoutDraining { owner })
            }
            Some(JoinAck::NotSpawned) if drained => {
                return Err(JoinRefusal::JoinedWithoutDraining { owner })
            }
            _ => {}
        }
    }
    Ok(())
}

// =====================================================================
// R68-2: notification intake
// =====================================================================

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// The four `@plugin.subscribe` bindings, from cl-revenue-ops.py.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Subscription {
    ForwardEvent,
    Connect,
    Disconnect,
    ChannelStateChanged,
}

impl Subscription {
    /// Exactly the Python set. A fifth entry would be a Rust-only intake
    /// path; a missing one is an unbound notification.
    pub const ALL: [Subscription; 4] = [
        Subscription::ForwardEvent,
        Subscription::Connect,
        Subscription::Disconnect,
        Subscription::ChannelStateChanged,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ForwardEvent => "forward_event",
            Self::Connect => "connect",
            Self::Disconnect => "disconnect",
            Self::ChannelStateChanged => "channel_state_changed",
        }
    }
}

/// What one notification actually did.
///
/// `Skipped` and `Dropped` are deliberately different: Python returns
/// early for a non-settled forward (a decision), and separately swallows
/// write failures into a log line (a loss). Collapsing them is what makes
/// a lossy node look healthy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntakeOutcome {
    Recorded,
    Skipped(&'static str),
    Dropped(String),
}

/// Process-lifetime intake counters, one set per subscription.
///
/// Interior-mutable and `Sync` because the subscription handlers are
/// independent async tasks with only a shared reference.
#[derive(Debug, Default)]
pub struct IntakeLedger {
    recorded: [AtomicU64; 4],
    skipped: [AtomicU64; 4],
    dropped: [AtomicU64; 4],
    last_reason: Mutex<Option<(Subscription, String)>>,
}

fn slot(subscription: Subscription) -> usize {
    match subscription {
        Subscription::ForwardEvent => 0,
        Subscription::Connect => 1,
        Subscription::Disconnect => 2,
        Subscription::ChannelStateChanged => 3,
    }
}

impl IntakeLedger {
    pub fn record(&self, subscription: Subscription, outcome: IntakeOutcome) {
        let i = slot(subscription);
        match outcome {
            IntakeOutcome::Recorded => {
                self.recorded[i].fetch_add(1, Ordering::Relaxed);
            }
            IntakeOutcome::Skipped(_) => {
                self.skipped[i].fetch_add(1, Ordering::Relaxed);
            }
            IntakeOutcome::Dropped(reason) => {
                self.dropped[i].fetch_add(1, Ordering::Relaxed);
                *self.last_reason.lock().expect("intake ledger poisoned") =
                    Some((subscription, reason));
            }
        }
    }

    pub fn recorded(&self, subscription: Subscription) -> u64 {
        self.recorded[slot(subscription)].load(Ordering::Relaxed)
    }
    pub fn skipped(&self, subscription: Subscription) -> u64 {
        self.skipped[slot(subscription)].load(Ordering::Relaxed)
    }
    pub fn dropped(&self, subscription: Subscription) -> u64 {
        self.dropped[slot(subscription)].load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntakeRefusal {
    Dropped {
        subscription: Subscription,
        count: u64,
        last_reason: String,
    },
}

impl IntakeRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Dropped { .. } => "intake_notification_dropped",
        }
    }
}

/// Readiness over notification intake.
///
/// A dropped notification is data this node SAW and did not keep. The
/// subscription handlers swallow the failure into a log line -- correctly,
/// since propagating would take down CLN event processing -- so the count
/// is the only durable trace, and Task 68's rule is that no
/// `.ok()`-to-null conversion may satisfy preflight.
///
/// The DATA cannot show the loss: the forward cursor is `MAX(timestamp)`
/// over persisted rows, so a later success advances it straight past the
/// hole. Only this ledger remembers.
pub fn intake_is_clean(ledger: &IntakeLedger) -> Result<(), IntakeRefusal> {
    // Every subscription, not just the first: a verdict that depended on
    // iteration order would pass or fail by accident.
    for subscription in Subscription::ALL {
        let count = ledger.dropped(subscription);
        if count > 0 {
            let last_reason = ledger
                .last_reason
                .lock()
                .expect("intake ledger poisoned")
                .as_ref()
                .filter(|(which, _)| *which == subscription)
                .map(|(_, reason)| reason.clone())
                .unwrap_or_else(|| "reason not retained".to_string());
            return Err(IntakeRefusal::Dropped {
                subscription,
                count,
                last_reason,
            });
        }
    }
    Ok(())
}
