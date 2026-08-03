//! R68-1 (RED): the lifecycle kernel -- store identity, the current-boot
//! startup receipt, and bounded shutdown ordering.
//!
//! R68-5 extends the shutdown half: the three coarse owner steps are gone,
//! and `shutdown` now drives the seven-owner roster itself, recording a
//! typed acknowledgement per owner. See the "bounded shutdown ordering"
//! section below.
//!
//! Contract re-derived from `fixtures/port/plugin_inventory.json`:
//!
//! - `shutdown`: `{name: "rpc-shutdown", bounded: true,
//!   join_timeout_seconds: 10.0, semantics: "daemon drain thread; bounded
//!   wait; process exit proceeds on timeout"}` (cl-revenue-ops.py:599).
//! - the four subscriptions, from `@plugin.subscribe`: `forward_event`,
//!   `connect`, `disconnect`, `channel_state_changed`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use revops::lifecycle::{
    assert_distinct_stores, classify_startup_receipt, drain_is_clean, shutdown, DrainAck,
    DrainFuture, DrainLedger, JoinAck, JoinFuture, LifecycleSteps, Owner, OwnerClass, Phase,
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
//
// R68-5: `drain_action_owners` / `drain_observer_owners` / `join_owners`
// each returned a bare `Result<(), String>`, so ONE `Ok(())` stood for all
// seven owners at once. That is precisely the `.ok()`-to-null conversion
// Task 68 forbids: it cannot distinguish an owner that finished from one
// that was already gone, never answered, or stopped with work still
// queued -- and it cannot distinguish an owner that legitimately does not
// run in this process from one nobody remembered to ask.
//
// The trait now reports PER OWNER, and `shutdown` drives the roster
// itself so the action-before-observer order is enforced by the kernel
// rather than by whoever implements the trait.

/// Records what ran, and can be told to fail, panic, or hang -- at a
/// phase, or at one specific owner's drain.
#[derive(Default)]
struct FakeSteps {
    order: Mutex<Vec<Phase>>,
    drain_order: Mutex<Vec<Owner>>,
    join_order: Mutex<Vec<Owner>>,
    fail_at: Option<Phase>,
    panic_at: Option<Phase>,
    hang_at: Option<Phase>,
    hang_draining: Option<Owner>,
    drain_acks: Mutex<HashMap<Owner, DrainAck>>,
    join_acks: Mutex<HashMap<Owner, JoinAck>>,
    joins: AtomicUsize,
}

impl FakeSteps {
    fn recording() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Override one owner's drain acknowledgement. Everything unset
    /// answers cleanly, so each test perturbs exactly one thing.
    fn drains_as(self, owner: Owner, ack: DrainAck) -> Self {
        self.drain_acks.lock().unwrap().insert(owner, ack);
        self
    }

    fn joins_as(self, owner: Owner, ack: JoinAck) -> Self {
        self.join_acks.lock().unwrap().insert(owner, ack);
        self
    }

    fn seen(&self) -> Vec<Phase> {
        self.order.lock().unwrap().clone()
    }
    fn drained(&self) -> Vec<Owner> {
        self.drain_order.lock().unwrap().clone()
    }
    fn joined(&self) -> Vec<Owner> {
        self.join_order.lock().unwrap().clone()
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

    async fn drain(&self, owner: Owner) -> DrainAck {
        if self.hang_draining == Some(owner) {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        self.drain_order.lock().unwrap().push(owner);
        self.drain_acks
            .lock()
            .unwrap()
            .get(&owner)
            .cloned()
            .unwrap_or(DrainAck::Drained { pending_at_stop: 0 })
    }

    async fn join(&self, owner: Owner) -> JoinAck {
        self.joins.fetch_add(1, Ordering::SeqCst);
        self.join_order.lock().unwrap().push(owner);
        self.join_acks
            .lock()
            .unwrap()
            .get(&owner)
            .cloned()
            .unwrap_or(JoinAck::Joined)
    }
}

impl LifecycleSteps for FakeSteps {
    fn stop_intake(&self) -> StepFuture<'_> {
        Box::pin(self.step(Phase::IntakeStopped))
    }
    fn persist_cursors(&self) -> StepFuture<'_> {
        Box::pin(self.step(Phase::CursorsPersisted))
    }
    fn flush_stores(&self) -> StepFuture<'_> {
        Box::pin(self.step(Phase::StoresFlushed))
    }
    fn drain_owner(&self, owner: Owner) -> DrainFuture<'_> {
        Box::pin(self.drain(owner))
    }
    fn join_owner(&self, owner: Owner) -> JoinFuture<'_> {
        Box::pin(self.join(owner))
    }
}

fn ledger() -> Arc<DrainLedger> {
    Arc::new(DrainLedger::default())
}

/// The ordering is the contract, and it is not arbitrary: cursors must be
/// persisted only after intake stops (or the cursor races new work), and
/// ACTION owners must drain before OBSERVER owners (an action still in
/// flight can produce observations the observer must still record).
///
/// Asserted against a LITERAL phase list rather than against whatever
/// order the kernel iterates, so reordering the kernel cannot move both
/// sides of the comparison at once.
#[tokio::test]
async fn a_clean_shutdown_runs_every_phase_in_order() {
    let steps = FakeSteps::recording();
    let outcome = shutdown(steps.clone(), ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(outcome.completed, "{outcome:?}");
    assert!(!outcome.timed_out);
    assert_eq!(
        outcome.reached,
        vec![
            Phase::IntakeStopped,
            Phase::CursorsPersisted,
            Phase::ActionOwnersDrained,
            Phase::ObserverOwnersDrained,
            Phase::StoresFlushed,
            Phase::Joined,
        ]
    );
    assert_eq!(
        steps.seen(),
        vec![
            Phase::IntakeStopped,
            Phase::CursorsPersisted,
            Phase::StoresFlushed
        ],
        "the non-owner steps still run, and only those"
    );
}

/// Success is reported only AFTER the join. Reporting it earlier is the
/// bug that makes a shutdown look clean while owners are still running.
#[tokio::test]
async fn success_is_never_reported_before_the_owners_are_joined() {
    let steps = FakeSteps::recording();
    let outcome = shutdown(steps.clone(), ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(outcome.completed);
    assert_eq!(
        steps.joins.load(Ordering::SeqCst),
        Owner::ALL.len(),
        "every owner is actually joined, not just counted"
    );
    assert_eq!(
        outcome.reached.last(),
        Some(&Phase::Joined),
        "join is the LAST phase"
    );
}

/// A failing phase must surface, and must not be reported as a clean
/// shutdown -- but later phases still run, because leaving stores
/// unflushed because an earlier step failed loses data the failure did
/// not.
#[tokio::test]
async fn a_failed_phase_is_surfaced_and_does_not_skip_the_remaining_ones() {
    let steps = Arc::new(FakeSteps {
        fail_at: Some(Phase::CursorsPersisted),
        ..Default::default()
    });
    let outcome = shutdown(steps.clone(), ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(
        !outcome.completed,
        "a failed cursor persist is not a clean shutdown"
    );
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("CursorsPersisted")),
        "{outcome:?}"
    );
    assert!(
        steps.seen().contains(&Phase::StoresFlushed),
        "stores must still flush: {:?}",
        steps.seen()
    );
    assert_eq!(
        steps.joined().len(),
        Owner::ALL.len(),
        "owners must still be joined: {:?}",
        steps.joined()
    );
}

/// A step that PANICS loses the owner, not just the step.
#[tokio::test]
async fn a_panicking_step_is_reported_rather_than_swallowed() {
    let steps = Arc::new(FakeSteps {
        panic_at: Some(Phase::StoresFlushed),
        ..Default::default()
    });
    let outcome = shutdown(steps.clone(), ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed);
    assert!(
        outcome.failures.iter().any(|f| f.contains("lost")),
        "the lost owner must be named: {outcome:?}"
    );
}

/// py: "daemon drain thread; bounded wait; process exit proceeds on
/// timeout". A wedged owner must not hold shutdown open forever.
#[tokio::test(start_paused = true)]
async fn a_wedged_step_times_out_rather_than_blocking_forever() {
    let steps = Arc::new(FakeSteps {
        hang_at: Some(Phase::CursorsPersisted),
        ..Default::default()
    });
    let outcome = shutdown(steps.clone(), ledger(), Duration::from_secs(10)).await;

    assert!(outcome.timed_out, "{outcome:?}");
    assert!(!outcome.completed);
}

/// The generated inventory pins the bound at 10 seconds.
#[test]
fn the_join_timeout_matches_the_generated_shutdown_contract() {
    assert_eq!(SHUTDOWN_JOIN_TIMEOUT, Duration::from_secs_f64(10.0));
}

// ---------------------------------------------------------------------
// R68-5: the roster is driven by the kernel
// ---------------------------------------------------------------------

/// Every owner is ASKED. A shutdown that iterated only the handles it
/// happened to hold would report clean over an owner it never contacted.
#[tokio::test]
async fn every_owner_on_the_roster_is_asked_to_drain_and_to_join() {
    let steps = FakeSteps::recording();
    let outcome = shutdown(steps.clone(), ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(outcome.completed, "{outcome:?}");
    for owner in Owner::ALL {
        assert!(
            steps.drained().contains(&owner),
            "{} was never asked to drain: {:?}",
            owner.as_str(),
            steps.drained()
        );
        assert!(
            steps.joined().contains(&owner),
            "{} was never joined: {:?}",
            owner.as_str(),
            steps.joined()
        );
    }
}

/// R68-1's phase order, now enforced per OWNER rather than per step: an
/// action still in flight can produce observations the observer store
/// must still record, so no observer owner may drain before the last
/// action owner has.
#[tokio::test]
async fn action_owners_drain_before_observer_owners() {
    let steps = FakeSteps::recording();
    shutdown(steps.clone(), ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    let drained = steps.drained();
    let last_action = drained
        .iter()
        .rposition(|o| o.class() == OwnerClass::Action)
        .expect("some action owner drained");
    let first_observer = drained
        .iter()
        .position(|o| o.class() == OwnerClass::Observer)
        .expect("some observer owner drained");
    assert!(
        last_action < first_observer,
        "every action owner must drain before any observer owner: {drained:?}"
    );
}

/// The ledger is the operator's evidence, so it must outlive the call --
/// and be the SAME one the caller passed in, not a private copy the
/// kernel threw away.
#[tokio::test]
async fn the_ledger_the_caller_passed_in_holds_every_owners_report_afterwards() {
    let steps = FakeSteps::recording();
    let ledger = ledger();
    let outcome = shutdown(steps.clone(), ledger.clone(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(outcome.completed, "{outcome:?}");
    for owner in Owner::ALL {
        assert_eq!(
            ledger.drain_ack(owner),
            Some(DrainAck::Drained { pending_at_stop: 0 }),
            "{}",
            owner.as_str()
        );
        assert_eq!(
            ledger.join_ack(owner),
            Some(JoinAck::Joined),
            "{}",
            owner.as_str()
        );
    }
}

// ---------------------------------------------------------------------
// R68-5: a faulty acknowledgement is not a clean shutdown
// ---------------------------------------------------------------------
//
// Each of these ran to completion with no failing STEP -- under the old
// `Result<(), String>` trait every one of them reported `completed`.

#[tokio::test]
async fn an_owner_that_never_acknowledged_the_stop_is_not_a_clean_shutdown() {
    let steps = Arc::new(FakeSteps::default().drains_as(
        Owner::FeeScheduler,
        DrainAck::NoAck {
            waited: Duration::from_secs(10),
        },
    ));
    let outcome = shutdown(steps, ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed, "{outcome:?}");
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("drain_owner_no_ack") && f.contains("fee_scheduler")),
        "the refusal must name both the fault and the owner: {outcome:?}"
    );
}

/// "I stopped" is not "I finished". An owner that acknowledged the stop
/// with work still queued dropped that work on the floor.
#[tokio::test]
async fn an_owner_that_stopped_with_work_still_queued_is_not_a_clean_shutdown() {
    let steps = Arc::new(
        FakeSteps::default().drains_as(Owner::Rebalance, DrainAck::Drained { pending_at_stop: 3 }),
    );
    let outcome = shutdown(steps, ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed, "{outcome:?}");
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("drain_owner_left_work") && f.contains("rebalance")),
        "{outcome:?}"
    );
}

/// Sending a stop into a channel nobody is reading returns `Ok`. The
/// owner was already gone, so whatever it had queued was never processed
/// -- that is a LOST owner, not a drained one.
#[tokio::test]
async fn an_owner_that_was_already_gone_is_not_a_drained_one() {
    let steps = Arc::new(FakeSteps::default().drains_as(
        Owner::ObserverStore,
        DrainAck::Unreachable {
            detail: "channel closed".to_string(),
        },
    ));
    let outcome = shutdown(steps, ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed, "{outcome:?}");
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("drain_owner_unreachable") && f.contains("observer_store")),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn an_owner_that_timed_out_joining_is_not_a_clean_shutdown() {
    let steps = Arc::new(FakeSteps::default().joins_as(
        Owner::Rebalance,
        JoinAck::Timeout {
            waited: Duration::from_secs(10),
        },
    ));
    let outcome = shutdown(steps, ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed, "{outcome:?}");
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("join_owner_timeout") && f.contains("rebalance")),
        "{outcome:?}"
    );
}

/// R68-1's rule, kept per-owner: folding a panic into "the task is no
/// longer running, so it ended" is how a crash reads as a graceful exit.
#[tokio::test]
async fn a_panicking_owner_is_not_a_joined_one() {
    let steps = Arc::new(FakeSteps::default().joins_as(
        Owner::ObserverStore,
        JoinAck::Panicked {
            detail: "index out of bounds".to_string(),
        },
    ));
    let outcome = shutdown(steps, ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed, "{outcome:?}");
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("join_owner_panicked") && f.contains("observer_store")),
        "{outcome:?}"
    );
}

/// The cross-check neither ledger can make alone: both halves look clean
/// in isolation -- the drain half sees a legitimately absent owner, the
/// join half sees a clean join -- and together they are a contradiction.
#[tokio::test]
async fn an_owner_joined_without_draining_fails_the_cross_check() {
    let steps = Arc::new(
        FakeSteps::default()
            .drains_as(Owner::FeeScheduler, DrainAck::NotSpawned)
            .joins_as(Owner::FeeScheduler, JoinAck::Joined),
    );
    let outcome = shutdown(steps, ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(!outcome.completed, "{outcome:?}");
    assert!(
        outcome
            .failures
            .iter()
            .any(|f| f.contains("join_without_drain") && f.contains("fee_scheduler")),
        "{outcome:?}"
    );
}

/// ...and the gate must not over-refuse. A passive-observer boot runs
/// NONE of the four action owners; declaring that absence is a clean
/// shutdown, not a fault. A gate that reddened every observer-only boot
/// would be turned off within a day.
#[tokio::test]
async fn a_passive_observer_boot_shuts_down_cleanly_when_it_declares_its_absent_owners() {
    let mut steps = FakeSteps::default();
    for owner in Owner::ALL {
        if owner.class() == OwnerClass::Action {
            steps = steps
                .drains_as(owner, DrainAck::NotSpawned)
                .joins_as(owner, JoinAck::NotSpawned);
        }
    }
    let outcome = shutdown(Arc::new(steps), ledger(), SHUTDOWN_JOIN_TIMEOUT).await;

    assert!(
        outcome.completed,
        "declared absence is not a fault: {outcome:?}"
    );
}

/// The bound can cut the roster short, and the owners past the cut were
/// never asked. Their silence must read as UNREPORTED -- the one state
/// the old `Ok(())` could never express, and the reason the ledger is
/// passed in rather than built inside the spawned task.
#[tokio::test(start_paused = true)]
async fn a_shutdown_that_timed_out_mid_drain_still_names_the_owners_it_never_heard_from() {
    let steps = Arc::new(FakeSteps {
        hang_draining: Some(Owner::Rebalance),
        ..Default::default()
    });
    let ledger = ledger();
    let outcome = shutdown(steps, ledger.clone(), Duration::from_secs(10)).await;

    assert!(outcome.timed_out, "{outcome:?}");
    assert!(!outcome.completed);
    assert_eq!(
        ledger.drain_ack(Owner::Rebalance),
        None,
        "the wedged owner never reported"
    );
    assert!(
        drain_is_clean(&ledger).is_err(),
        "an unreported owner is not a drained one"
    );
}
