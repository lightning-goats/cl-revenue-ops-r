//! R68-3 (RED): startup ordering.
//!
//! R68-1 pinned the SHUTDOWN order and the current-boot receipt. Startup
//! was left as whatever `main.rs` happened to do in sequence, which means
//! the ordering that keeps notifications from being accepted before there
//! is anywhere to put them is enforced by nothing.
//!
//! Two orderings carry the weight:
//!
//!   * intake is bound AFTER the stores are open and the deferred cursors
//!     are hydrated. Bound earlier, every notification in the gap is a
//!     `Dropped` (R68-2 now counts them, but not creating the hole is
//!     better than counting it), and hydration would re-derive a cursor
//!     from rows that arrived mid-hydration.
//!   * the current-boot receipt is written LAST, so
//!     `classify_startup_receipt` can never pass for a process that only
//!     got halfway up.
//!
//! And one asymmetry against shutdown, which is deliberate: startup ABORTS
//! at the first failed step, while shutdown continues past one. Shutdown
//! continues because skipping a later step loses data the failure itself
//! did not. Startup stops because every step after a failure would accept
//! work this process cannot keep.

use std::sync::{Arc, Mutex};

use revops::lifecycle::{startup, Phase, StartupPhase, StartupSteps, StepFuture};

/// Records the order steps actually ran in, and can fail one of them.
struct Recorder {
    ran: Mutex<Vec<StartupPhase>>,
    fail_at: Option<StartupPhase>,
}

impl Recorder {
    fn new(fail_at: Option<StartupPhase>) -> Arc<Self> {
        Arc::new(Self {
            ran: Mutex::new(Vec::new()),
            fail_at,
        })
    }

    fn step(&self, phase: StartupPhase) -> StepFuture<'_> {
        self.ran.lock().unwrap().push(phase);
        let failed = self.fail_at == Some(phase);
        Box::pin(async move {
            if failed {
                // Deliberately does NOT name the phase: if the injected
                // detail carried it, a `startup` that dropped the phase
                // from its own failure line would still look correct.
                Err("injected step failure".to_string())
            } else {
                Ok(())
            }
        })
    }

    fn ran(&self) -> Vec<StartupPhase> {
        self.ran.lock().unwrap().clone()
    }
}

impl StartupSteps for Recorder {
    fn identify_stores(&self) -> StepFuture<'_> {
        self.step(StartupPhase::StoresIdentified)
    }
    fn open_stores(&self) -> StepFuture<'_> {
        self.step(StartupPhase::StoresOpened)
    }
    fn hydrate_cursors(&self) -> StepFuture<'_> {
        self.step(StartupPhase::CursorsHydrated)
    }
    fn start_owners(&self) -> StepFuture<'_> {
        self.step(StartupPhase::OwnersStarted)
    }
    fn bind_intake(&self) -> StepFuture<'_> {
        self.step(StartupPhase::IntakeBound)
    }
    fn write_receipt(&self) -> StepFuture<'_> {
        self.step(StartupPhase::ReceiptWritten)
    }
}

// =====================================================================
// the order
// =====================================================================

#[tokio::test]
async fn a_clean_startup_runs_every_phase_in_the_declared_order() {
    let steps = Recorder::new(None);
    let outcome = startup(steps.clone()).await;

    assert!(outcome.completed, "{outcome:?}");
    assert_eq!(outcome.failure, None);
    assert_eq!(outcome.reached, StartupPhase::ALL.to_vec());
    assert_eq!(
        steps.ran(),
        StartupPhase::ALL.to_vec(),
        "the steps must RUN in the declared order, not merely be reported in it"
    );
}

#[test]
fn intake_is_bound_only_after_the_stores_are_open_and_the_cursors_hydrated() {
    assert!(StartupPhase::StoresOpened < StartupPhase::IntakeBound);
    assert!(StartupPhase::CursorsHydrated < StartupPhase::IntakeBound);
}

#[test]
fn the_receipt_is_the_last_phase_so_it_cannot_certify_a_half_start() {
    assert_eq!(
        StartupPhase::ALL.last(),
        Some(&StartupPhase::ReceiptWritten)
    );
    for phase in StartupPhase::ALL {
        assert!(phase <= StartupPhase::ReceiptWritten);
    }
}

/// Startup and shutdown must agree about intake and the cursors, or a
/// restart loop can persist a cursor while notifications are still
/// arriving. Intake comes up after hydration and goes down before the
/// cursors are persisted -- the cursor is never live-adjacent to
/// unbounded intake in either direction.
#[test]
fn startup_and_shutdown_bracket_intake_around_the_cursors_consistently() {
    assert!(StartupPhase::CursorsHydrated < StartupPhase::IntakeBound);
    assert!(Phase::IntakeStopped < Phase::CursorsPersisted);
}

#[test]
fn the_phase_list_has_no_duplicates() {
    let unique: std::collections::BTreeSet<_> = StartupPhase::ALL.iter().collect();
    assert_eq!(unique.len(), StartupPhase::ALL.len());
}

/// `ALL` is what `startup` iterates, and the ordinal comparisons above pin
/// the ENUM's order -- two separate declarations that can drift apart.
/// Reordering `ALL` alone would otherwise be invisible: every assertion
/// that compares the run order against `ALL` would simply agree with the
/// mutation. Anchoring `ALL` to the enum's own ordering closes that.
#[test]
fn the_phase_list_runs_in_the_enums_declared_order() {
    assert!(
        StartupPhase::ALL.windows(2).all(|pair| pair[0] < pair[1]),
        "ALL must be strictly ascending: {:?}",
        StartupPhase::ALL
    );
    assert_eq!(
        StartupPhase::ALL.len(),
        6,
        "a phase added to the enum must be added to ALL, or startup skips it"
    );
}

// =====================================================================
// failure aborts the remainder
// =====================================================================

/// The whole point of ordering: a store that will not open must not be
/// followed by binding the four subscriptions.
#[tokio::test]
async fn a_failed_store_open_never_binds_intake() {
    let steps = Recorder::new(Some(StartupPhase::StoresOpened));
    let outcome = startup(steps.clone()).await;

    assert!(!outcome.completed);
    assert!(outcome.failure.is_some());
    assert_eq!(
        outcome.reached,
        vec![StartupPhase::StoresIdentified, StartupPhase::StoresOpened]
    );
    assert!(
        !steps.ran().contains(&StartupPhase::IntakeBound),
        "intake must not be bound after a failed store open: {:?}",
        steps.ran()
    );
    assert!(!steps.ran().contains(&StartupPhase::ReceiptWritten));
}

/// Every phase, not just the first: a gate that only aborted on step one
/// would pass or fail by accident depending on where the fault landed.
#[tokio::test]
async fn a_failure_at_any_phase_aborts_the_remaining_phases() {
    for (index, phase) in StartupPhase::ALL.into_iter().enumerate() {
        let steps = Recorder::new(Some(phase));
        let outcome = startup(steps.clone()).await;

        assert!(!outcome.completed, "{phase:?} failure must not complete");
        assert_eq!(
            outcome.reached,
            StartupPhase::ALL[..=index].to_vec(),
            "{phase:?} must be the last phase reached"
        );
        assert_eq!(steps.ran(), StartupPhase::ALL[..=index].to_vec());
    }
}

#[tokio::test]
async fn the_failure_detail_names_the_phase_that_failed() {
    let steps = Recorder::new(Some(StartupPhase::CursorsHydrated));
    let outcome = startup(steps).await;
    let failure = outcome.failure.expect("a failed startup reports why");
    assert!(
        failure.contains("CursorsHydrated"),
        "the phase must survive to the operator: {failure}"
    );
    assert!(
        failure.contains("injected"),
        "the step's own detail must survive too: {failure}"
    );
}

/// The asymmetry against shutdown, stated as a test so a later refactor
/// cannot quietly make them the same. Shutdown runs all six phases even
/// when one fails; startup runs none after a failure.
#[tokio::test]
async fn startup_aborts_where_shutdown_would_continue() {
    let steps = Recorder::new(Some(StartupPhase::StoresIdentified));
    let outcome = startup(steps.clone()).await;
    assert_eq!(steps.ran().len(), 1);
    assert_eq!(outcome.reached, vec![StartupPhase::StoresIdentified]);
}
