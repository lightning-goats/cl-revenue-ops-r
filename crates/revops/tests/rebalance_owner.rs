//! Task 60 slice 4: the serialized rebalance owner -- evidence
//! revalidation, intent-before-execute, exactly-once settlement,
//! suspend-on-persistence-failure, and restart reconciliation.

use revops::rebalance_adapters::ReconcileLookup;
use revops::rebalance_execution::RebalanceSubmitOutcome;
use revops::rebalance_owner::{
    spawn_rebalance_owner, ManualRebalanceParams, RebalanceOwnerConfig, RebalanceOwnerDeps,
    RebalanceRefusal,
};
use revops_db::owner::spawn_read_write;
use revops_rebalance::engine::CycleResult;
use revops_rebalance::executor::DRYRUN_GATE_SENDPAY_DISABLED;
use revops_rebalance::facade::{CandidateExecutor, FacadeRpc};
use revops_rebalance::modes::EngineKwargs;
use revops_rebalance::router::RpcFailure;
use revops_rebalance::types::{ExecutionResult, RebalanceCandidate};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Scripted seams
// ---------------------------------------------------------------------------

/// Scripted engine: returns the queued results, counts calls.
struct ScriptedEngine {
    execute_calls: AtomicUsize,
    results: Mutex<Vec<ExecutionResult>>,
}

impl ScriptedEngine {
    fn returning(results: Vec<ExecutionResult>) -> Arc<Self> {
        Arc::new(Self {
            execute_calls: AtomicUsize::new(0),
            results: Mutex::new(results),
        })
    }
    fn calls(&self) -> usize {
        self.execute_calls.load(Ordering::SeqCst)
    }
}

impl CandidateExecutor for ScriptedEngine {
    fn execute_candidate(
        &self,
        _candidate: &RebalanceCandidate,
        _rebalance_id: i64,
        _kw: EngineKwargs,
    ) -> ExecutionResult {
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        self.results
            .lock()
            .unwrap()
            .pop()
            .expect("scripted engine exhausted")
    }
    fn run_cycle(&self) -> CycleResult {
        CycleResult::default()
    }
}

/// Scripted evidence: healthy by default, or failing.
struct ScriptedEvidence {
    fail: bool,
}

impl FacadeRpc for ScriptedEvidence {
    fn get_funds(&self) -> Result<Value, RpcFailure> {
        if self.fail {
            return Err(RpcFailure {
                message: "listfunds unavailable".into(),
            });
        }
        Ok(json!({"channels": [{"short_channel_id": "100x1x0"}]}))
    }
    fn get_peer_channels(&self) -> Result<Value, RpcFailure> {
        if self.fail {
            return Err(RpcFailure {
                message: "listpeerchannels unavailable".into(),
            });
        }
        Ok(json!({"channels": []}))
    }
    fn get_channels_source(&self, _source: &str) -> Result<Value, RpcFailure> {
        Ok(json!({"channels": []}))
    }
    fn get_node_id(&self) -> Result<String, RpcFailure> {
        Ok("02aa".repeat(16))
    }
}

/// Scripted reconciliation lookups keyed by payment hash.
struct ScriptedReconcile {
    by_hash: Mutex<std::collections::HashMap<String, Result<Value, String>>>,
}

impl ReconcileLookup for ScriptedReconcile {
    fn listsendpays(&self, payment_hash: &str) -> Result<Value, RpcFailure> {
        match self.by_hash.lock().unwrap().get(payment_hash) {
            Some(Ok(v)) => Ok(v.clone()),
            Some(Err(msg)) => Err(RpcFailure {
                message: msg.clone(),
            }),
            None => Ok(json!({"payments": []})),
        }
    }
}

fn dryrun_result() -> ExecutionResult {
    ExecutionResult {
        success: false,
        attempts: 1,
        fee_sats: 3,
        fee_msat: 2_500,
        fee_ppm: 12,
        hops: 3,
        parts: 1,
        error: Some(DRYRUN_GATE_SENDPAY_DISABLED.to_string()),
        amount_sats: 250_000,
        payment_pending: false,
        payment_hash: None,
        excluded_channels: Vec::new(),
        route_type: "native",
        failure_data: json!({}),
    }
}

fn pending_result() -> ExecutionResult {
    let mut r = dryrun_result();
    r.error = Some("payment_pending_timeout: code 200".into());
    r.payment_pending = true;
    r.payment_hash = Some("hash-pend".into());
    r
}

fn params(amount: i64, force: bool) -> ManualRebalanceParams {
    ManualRebalanceParams {
        from_channel: "100x1x0".into(),
        to_channel: "200x2x0".into(),
        amount_sats: amount,
        max_fee_sats: Some(300),
        force,
    }
}

struct Harness {
    handle: revops::rebalance_owner::RebalanceOwnerHandle,
    store: revops_db::owner::ObserverHandle,
    clock: Arc<AtomicI64>,
    _dir: tempfile::TempDir,
}

async fn harness_with(
    engine: Arc<ScriptedEngine>,
    evidence_fail: bool,
    reconcile: Arc<ScriptedReconcile>,
) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let clock = Arc::new(AtomicI64::new(1_800_000_000));
    let clock_for_owner = clock.clone();
    let handle = spawn_rebalance_owner(RebalanceOwnerDeps {
        engine: Some(engine.clone()),
        evidence: Arc::new(ScriptedEvidence {
            fail: evidence_fail,
        }),
        store: store.clone(),
        reconcile,
        config: RebalanceOwnerConfig {
            daily_budget_sats: 5_000_000,
            budget_window_hours: 24,
            rebalance_max_amount: 5_000_000,
            pair_cooldown_seconds: 3_600,
        },
        clock: Box::new(move || clock_for_owner.load(Ordering::SeqCst)),
    });
    let _ = engine;
    Harness {
        handle,
        store,
        clock,
        _dir: dir,
    }
}

fn empty_reconcile() -> Arc<ScriptedReconcile> {
    Arc::new(ScriptedReconcile {
        by_hash: Mutex::new(std::collections::HashMap::new()),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The happy manual path: evidence revalidated, intent BEFORE execute,
/// classified and settled exactly once, attempt terminal in the ledger.
#[tokio::test]
async fn manual_submission_runs_the_full_rail() {
    let engine = ScriptedEngine::returning(vec![dryrun_result()]);
    let h = harness_with(engine.clone(), false, empty_reconcile()).await;

    let outcome = h
        .handle
        .manual(params(250_000, false))
        .await
        .expect("dry-run manual submission completes");
    assert!(matches!(
        outcome.outcome,
        RebalanceSubmitOutcome::CleanFailureBeforeWrite { .. }
    ));
    assert_eq!(engine.calls(), 1);

    // Terminal in the ledger, reservation released (clean failure).
    assert!(h
        .store
        .unresolved_rebalance_attempts()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        h.store
            .active_rebalance_reserved_sats(1_700_000_000)
            .await
            .unwrap(),
        0
    );
}

/// Fail-closed evidence: a failing listfunds refuses BEFORE any intent or
/// execution -- zero engine calls, zero ledger rows.
#[tokio::test]
async fn evidence_failure_refuses_before_intent_and_execution() {
    let engine = ScriptedEngine::returning(vec![dryrun_result()]);
    let h = harness_with(engine.clone(), true, empty_reconcile()).await;

    let err = h
        .handle
        .manual(params(250_000, false))
        .await
        .expect_err("evidence failure must refuse");
    assert!(
        matches!(err, RebalanceRefusal::EvidenceUnavailable(_)),
        "{err:?}"
    );
    assert_eq!(engine.calls(), 0, "no execution without fresh evidence");
    let conn_count = h.store.unresolved_rebalance_attempts().await.unwrap().len();
    assert_eq!(conn_count, 0, "no intent row either");
}

/// The hard amount cap refuses REGARDLESS of force; soft gates (budget,
/// cooldown) yield to force only.
#[tokio::test]
async fn force_bypasses_soft_gates_only() {
    let engine = ScriptedEngine::returning(vec![dryrun_result(), dryrun_result()]);
    let h = harness_with(engine.clone(), false, empty_reconcile()).await;

    // Hard cap: refused under BOTH force values.
    for force in [false, true] {
        let err = h
            .handle
            .manual(params(5_000_001, force))
            .await
            .expect_err("over-cap must refuse");
        assert!(
            matches!(err, RebalanceRefusal::HardCapExceeded { .. }),
            "force={force}: {err:?}"
        );
    }
    assert_eq!(engine.calls(), 0);

    // Cooldown: the first submission arms the pair cooldown; a second
    // non-force submission refuses; force bypasses it.
    h.handle
        .manual(params(250_000, false))
        .await
        .expect("first submission");
    let err = h
        .handle
        .manual(params(250_000, false))
        .await
        .expect_err("pair cooldown must refuse a non-force retry");
    assert!(
        matches!(err, RebalanceRefusal::CooldownActive { .. }),
        "{err:?}"
    );
    h.handle
        .manual(params(250_000, true))
        .await
        .expect("force bypasses the cooldown");
    assert_eq!(engine.calls(), 2);
}

/// An unknown outcome quarantines its reservation; the budget gate then
/// counts it against later submissions (a quarantined hold is spendable
/// money until reconciliation proves otherwise).
#[tokio::test]
async fn unknown_outcome_quarantines_and_budget_counts_it() {
    let engine = ScriptedEngine::returning(vec![dryrun_result(), pending_result()]);
    let h = harness_with(engine.clone(), false, empty_reconcile()).await;

    let outcome = h
        .handle
        .manual(params(3_000_000, false))
        .await
        .expect("pending submission records");
    assert!(matches!(
        outcome.outcome,
        RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
    ));
    assert_eq!(
        h.store
            .active_rebalance_reserved_sats(1_700_000_000)
            .await
            .unwrap(),
        3_000_000,
        "the quarantined hold persists"
    );

    // Budget: 5M window budget - 3M held = 2M headroom; 2.5M refuses
    // (non-force), and the refusal happens BEFORE the engine runs.
    h.clock.fetch_add(7_200, Ordering::SeqCst); // clear the pair cooldown
    let err = h
        .handle
        .manual(params(2_500_000, false))
        .await
        .expect_err("budget must count the quarantined hold");
    assert!(
        matches!(err, RebalanceRefusal::BudgetExhausted { .. }),
        "{err:?}"
    );
    assert_eq!(engine.calls(), 1, "budget refusal precedes execution");
}

/// A settle-persistence failure SUSPENDS the owner: the failed submission
/// surfaces typed, and every later submission refuses until restart.
#[tokio::test]
async fn settle_failure_suspends_further_submissions() {
    let engine = ScriptedEngine::returning(vec![dryrun_result(), dryrun_result()]);
    let h = harness_with(engine.clone(), false, empty_reconcile()).await;

    // Sabotage ONLY the settle path: a BEFORE UPDATE trigger on the
    // attempts table poisons the terminal write while the intent INSERT
    // (and everything before it) stays healthy -- the submission runs
    // its full rail and fails exactly at settlement.
    let db_path = h._dir.path().join("observer.db");
    let raw = rusqlite::Connection::open(&db_path).unwrap();
    raw.execute_batch(
        "CREATE TRIGGER poison_settle BEFORE UPDATE ON rust_rebalance_attempts
         BEGIN SELECT RAISE(ABORT, 'injected settle failure'); END;",
    )
    .unwrap();

    let err = h
        .handle
        .manual(params(250_000, false))
        .await
        .expect_err("the unsettleable submission surfaces typed");
    assert!(
        matches!(err, RebalanceRefusal::SettlePersistenceUnknown { .. }),
        "{err:?}"
    );

    let err = h
        .handle
        .manual(params(250_000, true))
        .await
        .expect_err("the owner is suspended");
    assert!(matches!(err, RebalanceRefusal::Suspended), "{err:?}");
    assert_eq!(engine.calls(), 1, "no execution while suspended");
}

/// Restart reconciliation: a definite completion settles success, a
/// definite failure releases, pending and lookup failures QUARANTINE --
/// nothing is ever silently released.
#[tokio::test]
async fn reconcile_on_start_settles_definite_and_quarantines_the_rest() {
    let reconcile = Arc::new(ScriptedReconcile {
        by_hash: Mutex::new(
            [
                (
                    "h-complete".to_string(),
                    Ok(json!({"payments": [{"status": "complete",
                        "amount_msat": 250_000_000i64, "amount_sent_msat": 250_002_500i64}]})),
                ),
                (
                    "h-failed".to_string(),
                    Ok(json!({"payments": [{"status": "failed"}]})),
                ),
                (
                    "h-pending".to_string(),
                    Ok(json!({"payments": [{"status": "pending"}]})),
                ),
                ("h-lost".to_string(), Err("rpc timeout".to_string())),
            ]
            .into_iter()
            .collect(),
        ),
    });
    let engine = ScriptedEngine::returning(vec![]);
    let h = harness_with(engine, false, reconcile).await;

    // Seed four unresolved attempts directly through the store rails.
    for (rid, hash) in [
        ("rc-1", "h-complete"),
        ("rc-2", "h-failed"),
        ("rc-3", "h-pending"),
        ("rc-4", "h-lost"),
    ] {
        h.store
            .insert_rebalance_attempt(revops_db::fee_runway::RebalanceAttemptIntent {
                request_id: rid.into(),
                source_channel: "100x1x0".into(),
                dest_channel: "200x2x0".into(),
                amount_sats: 100_000,
                max_fee_sats: 300,
                trigger: "manual".into(),
                submitted_at: 1_799_999_000,
            })
            .await
            .unwrap();
        // Stamp the payment hash the way a submission would have.
        let raw = rusqlite::Connection::open(h._dir.path().join("observer.db")).unwrap();
        raw.execute(
            "UPDATE rust_rebalance_attempts SET payment_hash = ?2 WHERE request_id = ?1",
            rusqlite::params![rid, hash],
        )
        .unwrap();
    }

    let summary = h.handle.reconcile_on_start().await.expect("reconcile runs");
    assert_eq!(summary.settled_success, 1);
    assert_eq!(summary.settled_failed, 1);
    assert_eq!(
        summary.quarantined, 2,
        "pending AND lookup-failure quarantine"
    );

    assert!(h
        .store
        .unresolved_rebalance_attempts()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        h.store
            .active_rebalance_reserved_sats(1_700_000_000)
            .await
            .unwrap(),
        200_000,
        "exactly the two quarantined holds remain"
    );
}

/// No assembled engine (production today): manual and cycle refuse with
/// the Python-parity uninitialized arm; reconciliation still works.
#[tokio::test]
async fn missing_engine_refuses_like_python_uninitialized() {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let handle = spawn_rebalance_owner(RebalanceOwnerDeps {
        engine: None,
        evidence: Arc::new(ScriptedEvidence { fail: false }),
        store: store.clone(),
        reconcile: empty_reconcile(),
        config: RebalanceOwnerConfig {
            daily_budget_sats: 5_000_000,
            budget_window_hours: 24,
            rebalance_max_amount: 5_000_000,
            pair_cooldown_seconds: 3_600,
        },
        clock: Box::new(|| 1_800_000_000),
    });

    let err = handle
        .manual(params(250_000, false))
        .await
        .expect_err("no engine, no submission");
    assert!(
        matches!(err, RebalanceRefusal::EngineNotAssembled),
        "{err:?}"
    );
    let err = handle.run_cycle().await.expect_err("no engine, no cycle");
    assert!(
        matches!(err, RebalanceRefusal::EngineNotAssembled),
        "{err:?}"
    );
    handle
        .reconcile_on_start()
        .await
        .expect("reconciliation is store+lookup only");
}

/// Intent-before-execute: a submission whose intent write FAILS runs no
/// execution at all -- the durable record precedes the wire, always.
#[tokio::test]
async fn intent_write_failure_prevents_execution() {
    let engine = ScriptedEngine::returning(vec![dryrun_result()]);
    let h = harness_with(engine.clone(), false, empty_reconcile()).await;

    // Sabotage the INTENT write specifically (the attempts insert).
    let raw = rusqlite::Connection::open(h._dir.path().join("observer.db")).unwrap();
    raw.execute_batch(
        "CREATE TRIGGER poison_intent BEFORE INSERT ON rust_rebalance_attempts
         BEGIN SELECT RAISE(ABORT, 'injected intent failure'); END;",
    )
    .unwrap();

    let err = h
        .handle
        .manual(params(250_000, false))
        .await
        .expect_err("a failed intent write refuses the submission");
    assert!(matches!(err, RebalanceRefusal::StoreFailed(_)), "{err:?}");
    assert_eq!(
        engine.calls(),
        0,
        "the durable intent precedes the wire: no execution after a failed write"
    );
}
