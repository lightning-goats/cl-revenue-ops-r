//! Task 63 slices 4+5: the serialized Boltz owner's governed submit
//! rail -- gate ordering, durable attempt-before-spawn, exactly-once
//! settlement, quarantine + pending-gate discipline, durable cooldowns,
//! suspension, reconcile-on-start, and the auto-cycle arms.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use revops::boltz_boundaries::{BoltzActionCapability, BoltzSubmitOutcome};
use revops::boltz_owner::{
    spawn_boltz_owner, BoltzAction, BoltzOwnerConfig, BoltzOwnerDeps, BoltzOwnerHandle,
    BoltzRefusal, StructuralSpendDb,
};
use revops::capital_boundaries::{GovernorFacade, GovernorVerdict};
use revops_boltz::cli::BoltzCli;
use revops_boltz::error::CliError;
use revops_db::owner::{spawn_read_write, ObserverHandle};
use serde_json::json;

const NOW: i64 = 1_800_000_000;

// -- scripted seams ---------------------------------------------------------

/// Scripted transport usable as BOTH the query seam and (wrapped in the
/// capability) the action seam. Counts calls, records argv, and can
/// assert on the durable store at call time (the attempt-before-spawn
/// proof).
struct ScriptedCli {
    calls: AtomicUsize,
    argv: Mutex<Vec<Vec<String>>>,
    replies: Mutex<Vec<Result<String, CliError>>>,
    /// When set, every call asserts >=1 unresolved attempt exists in the
    /// store BEFORE the transport is reached.
    assert_attempt_first: Option<ObserverHandle>,
}

impl ScriptedCli {
    fn returning(replies: Vec<Result<String, CliError>>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            argv: Mutex::new(Vec::new()),
            replies: Mutex::new(replies),
            assert_attempt_first: None,
        })
    }
    fn returning_with_store(
        replies: Vec<Result<String, CliError>>,
        store: ObserverHandle,
    ) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            argv: Mutex::new(Vec::new()),
            replies: Mutex::new(replies),
            assert_attempt_first: Some(store),
        })
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl BoltzCli for ScriptedCli {
    fn run(&self, args: &[&str], _timeout_secs: u64) -> Result<String, CliError> {
        if let Some(store) = &self.assert_attempt_first {
            let unresolved = store
                .blocking_unresolved_boltz_attempts()
                .expect("store readable from transport");
            assert!(
                !unresolved.is_empty(),
                "the durable attempt row must exist BEFORE any spawn"
            );
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.argv
            .lock()
            .unwrap()
            .push(args.iter().map(|s| s.to_string()).collect());
        self.replies.lock().unwrap().remove(0)
    }
}

struct AllowAll;
impl GovernorFacade for AllowAll {
    fn authorize(&self, _kind: &str, _amount_sats: i64) -> GovernorVerdict {
        GovernorVerdict::Authorized {
            reason_code: "test".into(),
        }
    }
}
struct DenyAll;
impl GovernorFacade for DenyAll {
    fn authorize(&self, _kind: &str, _amount_sats: i64) -> GovernorVerdict {
        GovernorVerdict::Denied {
            reason_code: "boltz_governor_denied".into(),
        }
    }
}

struct ScriptedStructural(Result<i64, String>);
impl StructuralSpendDb for ScriptedStructural {
    fn structural_spend_sats_24h(&self) -> Result<i64, String> {
        self.0.clone()
    }
}

fn empty_listswaps() -> Result<String, CliError> {
    Ok(json!({"swaps": []}).to_string())
}

fn config() -> BoltzOwnerConfig {
    BoltzOwnerConfig {
        daily_budget_sats: 3_000,
        budget_window_hours: 24,
        structural_envelope_sats: 1_000,
        allow_concurrent_swaps: false,
        default_cooldown_seconds: 3_600,
        auto_cycle_enabled: false,
    }
}

struct Harness {
    handle: BoltzOwnerHandle,
    store: ObserverHandle,
    _dir: tempfile::TempDir,
}

async fn harness(
    capability: Option<BoltzActionCapability>,
    governor: Option<Arc<dyn GovernorFacade>>,
    query: Arc<ScriptedCli>,
    structural: Arc<dyn StructuralSpendDb>,
    cfg: BoltzOwnerConfig,
) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let handle = spawn_boltz_owner(BoltzOwnerDeps {
        capability: capability.map(Arc::new),
        governor,
        query,
        structural,
        store: store.clone(),
        config: cfg,
        clock: Box::new(|| NOW),
    });
    Harness {
        handle,
        store,
        _dir: dir,
    }
}

fn loop_out(fee: i64, structural: bool) -> BoltzAction {
    BoltzAction::LoopOut {
        amount_sats: 500_000,
        currency: "BTC".into(),
        address: None,
        wallet_name: None,
        chan_ids: vec!["700x1x0".into()],
        routing_fee_limit_ppm: 2_000,
        channel_id: Some("700x1x0".into()),
        estimated_fee_sats: fee,
        structural,
    }
}

fn cap(action: Arc<ScriptedCli>) -> BoltzActionCapability {
    BoltzActionCapability::assemble_injected(action, 1_000_000)
}

// -- gate ordering ------------------------------------------------------------

/// Every pre-spawn gate refuses BEFORE the action transport and BEFORE
/// any durable write: no capability, no governor, denied governor,
/// pending swap visible in listswaps, unreadable listswaps, budget
/// exhausted, structural envelope fail-closed, cooldown.
#[tokio::test]
async fn gates_refuse_before_spawn_and_before_any_write() {
    // No capability (pre-cutover production).
    let action = ScriptedCli::returning(vec![]);
    let h = harness(
        None,
        Some(Arc::new(AllowAll)),
        ScriptedCli::returning(vec![empty_listswaps()]),
        Arc::new(ScriptedStructural(Ok(0))),
        config(),
    )
    .await;
    let err = h.handle.execute(loop_out(500, false)).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::CapabilityNotAssembled),
        "{err:?}"
    );
    assert_eq!(err.code(), "boltz_capability_not_assembled");

    // Governor absent, then denying.
    let h = harness(
        Some(cap(action.clone())),
        None,
        ScriptedCli::returning(vec![empty_listswaps()]),
        Arc::new(ScriptedStructural(Ok(0))),
        config(),
    )
    .await;
    let err = h.handle.execute(loop_out(500, false)).await.unwrap_err();
    assert!(matches!(err, BoltzRefusal::GovernorNotAssembled), "{err:?}");

    let h = harness(
        Some(cap(action.clone())),
        Some(Arc::new(DenyAll)),
        ScriptedCli::returning(vec![empty_listswaps()]),
        Arc::new(ScriptedStructural(Ok(0))),
        config(),
    )
    .await;
    let err = h.handle.execute(loop_out(500, false)).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::GovernorDenied { .. }),
        "{err:?}"
    );

    // A pending swap in listswaps blocks (py `_boltz_pending_swap_count`).
    let pending = ScriptedCli::returning(vec![Ok(json!({"swaps": [
        {"id": "ext-1", "status": "transaction.mempool"}
    ]})
    .to_string())]);
    let h = harness(
        Some(cap(action.clone())),
        Some(Arc::new(AllowAll)),
        pending,
        Arc::new(ScriptedStructural(Ok(0))),
        config(),
    )
    .await;
    let err = h.handle.execute(loop_out(500, false)).await.unwrap_err();
    match &err {
        BoltzRefusal::PendingSwapsBlocked { count } => assert_eq!(*count, 1),
        other => panic!("{other:?}"),
    }

    // An UNREADABLE listswaps refuses fail-closed (never "no pending").
    let unreadable = ScriptedCli::returning(vec![Err(CliError::Timeout {
        timeout_secs: 30,
        command: "listswaps (1 args redacted)".into(),
    })]);
    let h = harness(
        Some(cap(action.clone())),
        Some(Arc::new(AllowAll)),
        unreadable,
        Arc::new(ScriptedStructural(Ok(0))),
        config(),
    )
    .await;
    let err = h.handle.execute(loop_out(500, false)).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::PendingEvidenceUnavailable(_)),
        "{err:?}"
    );

    // Budget: fee above remaining refuses.
    let h = harness(
        Some(cap(action.clone())),
        Some(Arc::new(AllowAll)),
        ScriptedCli::returning(vec![empty_listswaps()]),
        Arc::new(ScriptedStructural(Ok(0))),
        config(),
    )
    .await;
    let err = h.handle.execute(loop_out(5_000, false)).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::BudgetExhausted { .. }),
        "{err:?}"
    );

    // Structural envelope: unreadable spend refuses fail-closed; an
    // exhausted envelope refuses.
    let h = harness(
        Some(cap(action.clone())),
        Some(Arc::new(AllowAll)),
        ScriptedCli::returning(vec![empty_listswaps(), empty_listswaps()]),
        Arc::new(ScriptedStructural(Err("spend read failed".into()))),
        config(),
    )
    .await;
    let err = h.handle.execute(loop_out(500, true)).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::StructuralEvidenceUnavailable(_)),
        "{err:?}"
    );
    let h = harness(
        Some(cap(action.clone())),
        Some(Arc::new(AllowAll)),
        ScriptedCli::returning(vec![empty_listswaps()]),
        Arc::new(ScriptedStructural(Ok(1_000))),
        config(),
    )
    .await;
    let err = h.handle.execute(loop_out(500, true)).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::StructuralEnvelopeExhausted { .. }),
        "{err:?}"
    );

    // Nothing above reached the action transport or the store.
    assert_eq!(action.calls(), 0);
    assert!(h
        .store
        .unresolved_boltz_attempts()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(h.store.active_boltz_reserved_sats(0).await.unwrap(), 0);
}

/// The happy loop-out: the durable attempt EXISTS BEFORE the spawn (the
/// transport itself asserts it), settles committed with the swap id,
/// writes the journal, burns the durable cooldown, and a second attempt
/// on the same channel refuses on cooldown.
#[tokio::test]
async fn happy_loop_out_attempt_precedes_spawn_and_burns_cooldown() {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let action = ScriptedCli::returning_with_store(
        vec![Ok(
            json!({"id": "swap-aa", "status": "swap.created"}).to_string()
        )],
        store.clone(),
    );
    let handle = spawn_boltz_owner(BoltzOwnerDeps {
        capability: Some(Arc::new(cap(action.clone()))),
        governor: Some(Arc::new(AllowAll)),
        query: ScriptedCli::returning(vec![empty_listswaps(), empty_listswaps()]),
        structural: Arc::new(ScriptedStructural(Ok(0))),
        store: store.clone(),
        config: config(),
        clock: Box::new(|| NOW),
    });

    let outcome = handle.execute(loop_out(500, false)).await.unwrap();
    match &outcome.outcome {
        BoltzSubmitOutcome::Committed { swap_id } => {
            assert_eq!(swap_id.as_deref(), Some("swap-aa"))
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(action.calls(), 1);
    // Terminal: settled, journaled, cooldown burned durably.
    assert!(store.unresolved_boltz_attempts().await.unwrap().is_empty());
    assert_eq!(store.active_boltz_reserved_sats(0).await.unwrap(), 0);
    let journal = store.boltz_journal().await.unwrap();
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].swap_id, "swap-aa");
    assert_eq!(store.boltz_cooldowns().await.unwrap()[0].0, "700x1x0");

    // Same channel again: cooldown refuses (durable, no transport call).
    let err = handle.execute(loop_out(500, false)).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::CooldownActive { .. }),
        "{err:?}"
    );
    assert_eq!(action.calls(), 1);
}

/// A timed-out create QUARANTINES: reservation held, cooldown kept
/// burned, and the PENDING GATE blocks every later submission until the
/// quarantine is resolved -- structurally no resubmit.
#[tokio::test]
async fn unknown_outcome_quarantines_and_pending_gate_blocks_resubmit() {
    let action = ScriptedCli::returning(vec![Err(CliError::Timeout {
        timeout_secs: 120,
        command: "createreverseswap (6 args redacted)".into(),
    })]);
    let h = harness(
        Some(cap(action.clone())),
        Some(Arc::new(AllowAll)),
        ScriptedCli::returning(vec![empty_listswaps(), empty_listswaps()]),
        Arc::new(ScriptedStructural(Ok(0))),
        config(),
    )
    .await;

    let outcome = h.handle.execute(loop_out(500, false)).await.unwrap();
    assert!(
        matches!(
            outcome.outcome,
            BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
        ),
        "{outcome:?}"
    );
    assert_eq!(action.calls(), 1);
    // Quarantined: terminal but the fee stays held.
    assert!(h
        .store
        .unresolved_boltz_attempts()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(h.store.quarantined_boltz_attempts().await.unwrap().len(), 1);
    assert_eq!(h.store.active_boltz_reserved_sats(0).await.unwrap(), 500);

    // The quarantine itself blocks the next submission -- on a DIFFERENT
    // channel, before any transport call.
    let mut other = loop_out(500, false);
    if let BoltzAction::LoopOut {
        chan_ids,
        channel_id,
        ..
    } = &mut other
    {
        *chan_ids = vec!["800x1x0".into()];
        *channel_id = Some("800x1x0".into());
    }
    let err = h.handle.execute(other).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::PendingSwapsBlocked { .. }),
        "{err:?}"
    );
    assert_eq!(action.calls(), 1, "no second spawn past a quarantine");
}

/// A settle-persistence failure SUSPENDS the owner.
#[tokio::test]
async fn settle_failure_suspends_further_submissions() {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let action = ScriptedCli::returning(vec![Ok(
        json!({"id": "swap-aa", "status": "swap.created"}).to_string(),
    )]);
    let handle = spawn_boltz_owner(BoltzOwnerDeps {
        capability: Some(Arc::new(cap(action.clone()))),
        governor: Some(Arc::new(AllowAll)),
        query: ScriptedCli::returning(vec![empty_listswaps(), empty_listswaps()]),
        structural: Arc::new(ScriptedStructural(Ok(0))),
        store: store.clone(),
        config: config(),
        clock: Box::new(|| NOW),
    });

    let raw = rusqlite::Connection::open(dir.path().join("observer.db")).unwrap();
    raw.execute_batch(
        "CREATE TRIGGER poison_settle BEFORE UPDATE ON rust_boltz_attempts
         BEGIN SELECT RAISE(ABORT, 'injected settle failure'); END;",
    )
    .unwrap();

    let err = handle.execute(loop_out(500, false)).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::SettlePersistenceUnknown { .. }),
        "{err:?}"
    );
    let err = handle.execute(loop_out(500, false)).await.unwrap_err();
    assert!(matches!(err, BoltzRefusal::Suspended), "{err:?}");
    assert_eq!(action.calls(), 1);
}

/// Reconcile-on-start: an unresolved attempt (crash between spawn and
/// settle) has no recorded swap id, so nothing can prove absence -- it
/// QUARANTINES, keeps its fee hold, and blocks the pending gate.
#[tokio::test]
async fn reconcile_quarantines_unresolved_attempts() {
    let h = harness(
        None,
        None,
        ScriptedCli::returning(vec![empty_listswaps()]),
        Arc::new(ScriptedStructural(Ok(0))),
        config(),
    )
    .await;
    h.store
        .insert_boltz_attempt(revops_db::fee_runway::BoltzAttempt {
            request_id: "orphan-1".into(),
            kind: "loop_out".into(),
            channel_id: Some("700x1x0".into()),
            amount_sats: 500_000,
            estimated_fee_sats: 700,
            argv_digest: "d".into(),
            submitted_at: NOW - 900,
        })
        .await
        .unwrap();

    let summary = h.handle.reconcile_on_start().await.unwrap();
    assert_eq!(summary.quarantined, 1);
    assert!(h
        .store
        .unresolved_boltz_attempts()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(h.store.quarantined_boltz_attempts().await.unwrap().len(), 1);
    assert_eq!(h.store.active_boltz_reserved_sats(0).await.unwrap(), 700);
}

/// Auto-cycle arms: config-disabled returns Python's exact disabled
/// shape; enabled with no executable candidates lands idle (candidate
/// analytics are Task 67 -- surfaced, not fabricated).
#[tokio::test]
async fn auto_cycle_disabled_and_idle_arms() {
    let h = harness(
        Some(cap(ScriptedCli::returning(vec![]))),
        Some(Arc::new(AllowAll)),
        ScriptedCli::returning(vec![empty_listswaps()]),
        Arc::new(ScriptedStructural(Ok(0))),
        config(), // auto_cycle_enabled: false
    )
    .await;
    let result = h.handle.auto_cycle_run_now(false, true).await;
    assert_eq!(result["status"], "disabled");
    assert_eq!(result["reason"], "boltz auto-cycle disabled by config");

    // force=true bypasses the config gate (py parity) and, with no
    // candidate source, selects Idle.
    let result = h.handle.auto_cycle_run_now(true, true).await;
    assert_eq!(result["status"], "idle", "{result:?}");
}
