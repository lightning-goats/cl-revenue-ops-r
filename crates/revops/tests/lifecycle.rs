//! R68-1 (RED): the lifecycle kernel -- store identity, the current-boot
//! startup receipt, and bounded shutdown ordering.
//!
//! Contract re-derived from `fixtures/port/plugin_inventory.json`:
//!
//! - `shutdown`: `{name: "rpc-shutdown", bounded: true,
//!   join_timeout_seconds: 10.0, semantics: "daemon drain thread; bounded
//!   wait; process exit proceeds on timeout"}` (cl-revenue-ops.py:599).
//! - the four subscriptions, from `@plugin.subscribe`: `forward_event`,
//!   `connect`, `disconnect`, `channel_state_changed`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use revops::lifecycle::{
    assert_distinct_stores, classify_startup_receipt, shutdown, LifecycleSteps, Phase,
    StartupReceipt, StartupRefusal, StepFuture, StoreIdentityRefusal, SHUTDOWN_JOIN_TIMEOUT,
};

// =====================================================================
// store identity: (device, inode), not path text
// =====================================================================

/// The gap R68-1 exists to close.
///
/// The pre-existing guard compared CANONICALIZED PATHS, which resolves
/// symlinks but is blind to hard links: two distinct paths, two distinct
/// canonical paths, ONE file. A hardlinked observer path would then be
/// opened READ-WRITE while being the production database -- the single
/// invariant the whole shadow window rests on.
#[test]
fn a_hardlinked_observer_path_is_refused_even_though_the_paths_differ() {
    let dir = tempfile::tempdir().unwrap();
    let production = dir.path().join("revenue_ops.db");
    std::fs::write(&production, b"x").unwrap();
    let observer = dir.path().join("revops-r-observer.db");
    std::fs::hard_link(&production, &observer).expect("hard link");

    assert_ne!(
        std::fs::canonicalize(&observer).unwrap(),
        std::fs::canonicalize(&production).unwrap(),
        "precondition: canonical paths DIFFER, which is why a path check misses this"
    );

    let refusal = assert_distinct_stores(&observer, Some(&production))
        .expect_err("a hard link is the same file");
    assert!(matches!(refusal, StoreIdentityRefusal::Alias { .. }));
}

#[test]
fn a_symlinked_observer_path_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let production = dir.path().join("revenue_ops.db");
    std::fs::write(&production, b"x").unwrap();
    let observer = dir.path().join("link.db");
    std::os::unix::fs::symlink(&production, &observer).unwrap();

    assert!(assert_distinct_stores(&observer, Some(&production)).is_err());
}

#[test]
fn the_same_path_twice_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let production = dir.path().join("revenue_ops.db");
    std::fs::write(&production, b"x").unwrap();

    assert!(assert_distinct_stores(&production, Some(&production)).is_err());
}

#[test]
fn two_genuinely_distinct_files_are_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let production = dir.path().join("revenue_ops.db");
    let observer = dir.path().join("revops-r-observer.db");
    std::fs::write(&production, b"x").unwrap();
    std::fs::write(&observer, b"y").unwrap();

    assert!(assert_distinct_stores(&observer, Some(&production)).is_ok());
}

/// First run: the observer file does not exist yet. It cannot be a hard
/// link to anything, so this must be ALLOWED -- refusing would make a
/// fresh install unstartable.
#[test]
fn an_observer_file_that_does_not_exist_yet_is_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let production = dir.path().join("revenue_ops.db");
    std::fs::write(&production, b"x").unwrap();

    assert!(assert_distinct_stores(&dir.path().join("new.db"), Some(&production)).is_ok());
}

/// ...and a path that does not exist YET can still be the production
/// database, when both are still to be created and a symlinked parent
/// makes them the same location.
///
/// This exercises the ABSENT branch specifically: an earlier version of
/// this test pointed at a symlinked parent whose target file already
/// existed, so `stat` followed the link and the `(dev, inode)` check
/// caught it -- the fallback was never reached and a mutation that
/// deleted it survived.
#[test]
fn two_not_yet_created_paths_that_resolve_to_one_location_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    std::fs::create_dir(&real).unwrap();
    let linked = dir.path().join("linked");
    std::os::unix::fs::symlink(&real, &linked).unwrap();

    // NEITHER file exists; both would be created at the same location.
    let production = real.join("revenue_ops.db");
    let observer = linked.join("revenue_ops.db");
    assert!(!production.exists() && !observer.exists(), "precondition");

    assert!(
        assert_distinct_stores(&observer, Some(&production)).is_err(),
        "a to-be-created collision is still a collision"
    );
}

/// The same shape once the file DOES exist, reached through a symlinked
/// parent -- caught by identity rather than by the fallback.
#[test]
fn an_existing_file_reached_through_a_symlinked_parent_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    std::fs::create_dir(&real).unwrap();
    let production = real.join("revenue_ops.db");
    std::fs::write(&production, b"x").unwrap();
    let linked = dir.path().join("linked");
    std::os::unix::fs::symlink(&real, &linked).unwrap();

    assert!(assert_distinct_stores(&linked.join("revenue_ops.db"), Some(&production)).is_err());
}

#[test]
fn no_production_database_configured_is_not_a_collision() {
    let dir = tempfile::tempdir().unwrap();
    assert!(assert_distinct_stores(&dir.path().join("observer.db"), None).is_ok());
}

/// A failed stat is NOT proof of distinctness.
///
/// The production path is configured and present but unreadable. Treating
/// that as "different files" would let the observer open read-write beside
/// a database it could not identify.
#[test]
fn an_unstattable_production_path_refuses_rather_than_assuming_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let observer = dir.path().join("observer.db");
    std::fs::write(&observer, b"y").unwrap();

    // A path whose PARENT is not a directory: stat fails with ENOTDIR
    // rather than "not found", so this is an unreadable path, not an
    // absent one.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"not a dir").unwrap();
    let production = blocker.join("revenue_ops.db");

    let refusal = assert_distinct_stores(&observer, Some(&production))
        .expect_err("an unreadable production path is not a distinct one");
    assert!(matches!(refusal, StoreIdentityRefusal::Unreadable { .. }));
}

// =====================================================================
// current-boot startup receipt
// =====================================================================

fn receipt(boot_id: &str) -> StartupReceipt {
    StartupReceipt {
        boot_id: boot_id.to_string(),
        completed_at: 1_800_000_000,
    }
}

#[test]
fn a_receipt_from_this_boot_is_accepted() {
    assert!(classify_startup_receipt(Some(&receipt("boot-a")), "boot-a").is_ok());
}

/// The failure the whole receipt exists to prevent: a receipt written by a
/// PREVIOUS process, still sitting in the store, read as proof that THIS
/// process completed startup.
#[test]
fn a_prior_boots_receipt_is_refused() {
    let refusal = classify_startup_receipt(Some(&receipt("boot-a")), "boot-b")
        .expect_err("a prior boot's receipt is not this boot's startup");
    assert!(matches!(refusal, StartupRefusal::StalePriorBoot { .. }));
}

#[test]
fn no_receipt_at_all_is_refused_rather_than_assumed_complete() {
    let refusal = classify_startup_receipt(None, "boot-a")
        .expect_err("absence of a receipt is not completion");
    assert!(matches!(refusal, StartupRefusal::NoReceiptThisBoot));
}

// =====================================================================
// bounded shutdown ordering
// =====================================================================

/// Records the order steps ran in, and can be told to fail, panic, or hang.
#[derive(Default)]
struct FakeSteps {
    order: Mutex<Vec<Phase>>,
    fail_at: Option<Phase>,
    panic_at: Option<Phase>,
    hang_at: Option<Phase>,
    joins: AtomicUsize,
}

impl FakeSteps {
    fn recording() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn seen(&self) -> Vec<Phase> {
        self.order.lock().unwrap().clone()
    }
    async fn step(&self, phase: Phase) -> Result<(), String> {
        if self.hang_at == Some(phase) {
            // Longer than any test timeout; the bounded wait must win.
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        if self.panic_at == Some(phase) {
            panic!("owner lost at {phase:?}");
        }
        self.order.lock().unwrap().push(phase);
        if self.fail_at == Some(phase) {
            return Err(format!("{phase:?} failed"));
        }
        Ok(())
    }
}

impl LifecycleSteps for FakeSteps {
    fn stop_intake(&self) -> StepFuture<'_> {
        Box::pin(self.step(Phase::IntakeStopped))
    }
    fn persist_cursors(&self) -> StepFuture<'_> {
        Box::pin(self.step(Phase::CursorsPersisted))
    }
    fn drain_action_owners(&self) -> StepFuture<'_> {
        Box::pin(self.step(Phase::ActionOwnersDrained))
    }
    fn drain_observer_owners(&self) -> StepFuture<'_> {
        Box::pin(self.step(Phase::ObserverOwnersDrained))
    }
    fn flush_stores(&self) -> StepFuture<'_> {
        Box::pin(self.step(Phase::StoresFlushed))
    }
    fn join_owners(&self) -> StepFuture<'_> {
        self.joins.fetch_add(1, Ordering::SeqCst);
        Box::pin(self.step(Phase::Joined))
    }
}

/// The ordering is the contract, and it is not arbitrary: cursors must be
/// persisted only after intake stops (or the cursor races new work), and
/// ACTION owners must drain before OBSERVER owners (an action still in
/// flight can produce observations the observer must still record).
#[tokio::test]
async fn a_clean_shutdown_runs_every_phase_in_order() {
    let steps = FakeSteps::recording();
    let outcome = shutdown(steps.clone(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(outcome.completed, "{outcome:?}");
    assert!(!outcome.timed_out);
    assert_eq!(
        steps.seen(),
        vec![
            Phase::IntakeStopped,
            Phase::CursorsPersisted,
            Phase::ActionOwnersDrained,
            Phase::ObserverOwnersDrained,
            Phase::StoresFlushed,
            Phase::Joined,
        ]
    );
}

/// Success is reported only AFTER the join. Reporting it earlier is the
/// bug that makes a shutdown look clean while owners are still running.
#[tokio::test]
async fn success_is_never_reported_before_the_owners_are_joined() {
    let steps = FakeSteps::recording();
    let outcome = shutdown(steps.clone(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(outcome.completed);
    assert_eq!(steps.joins.load(Ordering::SeqCst), 1, "join actually ran");
    assert_eq!(
        steps.seen().last(),
        Some(&Phase::Joined),
        "join is the LAST phase"
    );
}

/// A failing phase must surface, and must not be reported as a clean
/// shutdown -- but later phases still run, because leaving stores
/// unflushed because an earlier owner failed loses data the failure did
/// not.
#[tokio::test]
async fn a_failed_phase_is_surfaced_and_does_not_skip_the_remaining_ones() {
    let steps = Arc::new(FakeSteps {
        fail_at: Some(Phase::ActionOwnersDrained),
        ..Default::default()
    });
    let outcome = shutdown(steps.clone(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed, "a failed drain is not a clean shutdown");
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("ActionOwnersDrained")),
        "{outcome:?}"
    );
    assert!(
        steps.seen().contains(&Phase::StoresFlushed) && steps.seen().contains(&Phase::Joined),
        "stores must still flush and owners must still join: {:?}",
        steps.seen()
    );
}

/// An owner that PANICS is a lost owner, not a clean one.
#[tokio::test]
async fn a_panicking_owner_is_reported_rather_than_swallowed() {
    let steps = Arc::new(FakeSteps {
        panic_at: Some(Phase::ObserverOwnersDrained),
        ..Default::default()
    });
    let outcome = shutdown(steps.clone(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed);
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("ObserverOwnersDrained")),
        "the lost owner must be named: {outcome:?}"
    );
}

/// py: "daemon drain thread; bounded wait; process exit proceeds on
/// timeout". A wedged owner must not hold shutdown open forever.
#[tokio::test(start_paused = true)]
async fn a_wedged_owner_times_out_rather_than_blocking_forever() {
    let steps = Arc::new(FakeSteps {
        hang_at: Some(Phase::ActionOwnersDrained),
        ..Default::default()
    });
    let outcome = shutdown(steps.clone(), Duration::from_secs(10)).await;

    assert!(outcome.timed_out, "{outcome:?}");
    assert!(!outcome.completed);
}

/// The generated inventory pins the bound at 10 seconds.
#[test]
fn the_join_timeout_matches_the_generated_shutdown_contract() {
    assert_eq!(SHUTDOWN_JOIN_TIMEOUT, Duration::from_secs_f64(10.0));
}
