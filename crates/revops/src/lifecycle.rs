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

/// The steps a real runtime supplies. Kept as a trait so the ordering, the
/// failure handling and the bound are testable without a live plugin.
pub trait LifecycleSteps: Send + Sync {
    fn stop_intake(&self) -> StepFuture<'_>;
    fn persist_cursors(&self) -> StepFuture<'_>;
    fn drain_action_owners(&self) -> StepFuture<'_>;
    fn drain_observer_owners(&self) -> StepFuture<'_>;
    fn flush_stores(&self) -> StepFuture<'_>;
    fn join_owners(&self) -> StepFuture<'_>;
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
pub async fn shutdown<S>(steps: std::sync::Arc<S>, timeout: Duration) -> ShutdownOutcome
where
    S: LifecycleSteps + 'static,
{
    let mut outcome = ShutdownOutcome::default();

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
        run!(Phase::ActionOwnersDrained, steps.drain_action_owners());
        run!(Phase::ObserverOwnersDrained, steps.drain_observer_owners());
        run!(Phase::StoresFlushed, steps.flush_stores());
        run!(Phase::Joined, steps.join_owners());
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

    // Two conditions, not three. A `reached.last() == Joined` clause was
    // here and is redundant: every path that misses the join -- a step
    // error, a lost owner, the bound expiring -- already populates
    // `failures` or `timed_out`. It was also unfalsifiable, so no mutation
    // could pin it, and an unpinnable condition is one a future refactor
    // can quietly invert.
    outcome.completed = outcome.failures.is_empty() && !outcome.timed_out;
    outcome
}
