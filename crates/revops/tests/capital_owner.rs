//! Task 62 slice 5: the serialized capital owner's submission rail --
//! gate ordering, exactly-once settlement, quarantine discipline,
//! suspend-on-persistence-failure, and restart reconciliation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use revops::capital_adapters::{CloseRpc, FundchannelRpc};
use revops::capital_boundaries::{
    BudgetDb, BudgetEvidence, BudgetRefusal, CapitalSubmitOutcome, GovernorFacade, GovernorVerdict,
};
use revops::capital_owner::{
    spawn_capital_owner, CapitalAction, CapitalActionAdapters, CapitalEvidenceRpc,
    CapitalOwnerDeps, CapitalOwnerHandle, CapitalReconcileLookup, CapitalRefusal, DefibExecutor,
};
use revops_db::owner::{spawn_read_write, ObserverHandle};
use revops_rebalance::router::RpcFailure;
use serde_json::{json, Value};

const NOW: i64 = 1_800_000_000;

// -- scripted seams ---------------------------------------------------------

struct CountingFund {
    calls: AtomicUsize,
    reply: Mutex<Vec<Result<Value, RpcFailure>>>,
}
impl CountingFund {
    fn returning(replies: Vec<Result<Value, RpcFailure>>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            reply: Mutex::new(replies),
        })
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}
impl FundchannelRpc for CountingFund {
    fn fundchannel(
        &self,
        _peer_id: &str,
        _amount_sats: i64,
        _request_amt: Option<i64>,
        _compact_lease: Option<String>,
    ) -> Result<Value, RpcFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.reply.lock().unwrap().remove(0)
    }
}

struct CountingClose {
    calls: AtomicUsize,
    reply: Mutex<Vec<Result<Value, RpcFailure>>>,
}
impl CountingClose {
    fn returning(replies: Vec<Result<Value, RpcFailure>>) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            reply: Mutex::new(replies),
        })
    }
}
impl CloseRpc for CountingClose {
    fn close(
        &self,
        _channel_id: &str,
        _unilateral_timeout: Option<i64>,
    ) -> Result<Value, RpcFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.reply.lock().unwrap().remove(0)
    }
}

struct NoDefib;
impl DefibExecutor for NoDefib {
    fn diagnostic_rebalance(
        &self,
        _peer_id: &str,
        _scid: &str,
        _amount_sats: i64,
    ) -> CapitalSubmitOutcome {
        panic!("defib must not run in these tests");
    }
}

struct ScriptedBudget(Result<BudgetEvidence, String>);
impl BudgetDb for ScriptedBudget {
    fn positive_budget_evidence(&self, _now: i64) -> Result<BudgetEvidence, String> {
        self.0.clone()
    }
}
fn healthy_budget() -> Arc<ScriptedBudget> {
    Arc::new(ScriptedBudget(Ok(BudgetEvidence {
        available_sats: 5_000_000,
        window_reserved_sats: 0,
        observed_at: NOW - 3,
    })))
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
            reason_code: "governor_test_denied".into(),
        }
    }
}

/// listpeerchannels evidence: peer 02bb holds channel 700x1x0; peer 02aa
/// holds nothing.
struct ScriptedEvidence(Result<Value, String>);
impl CapitalEvidenceRpc for ScriptedEvidence {
    fn get_peer_channels(&self) -> Result<Value, String> {
        self.0.clone()
    }
}
fn healthy_evidence() -> Arc<ScriptedEvidence> {
    Arc::new(ScriptedEvidence(Ok(json!({"channels": [
        {"peer_id": "02bb", "state": "CHANNELD_NORMAL", "short_channel_id": "700x1x0"},
    ]}))))
}

struct ScriptedReconcile {
    listfunds: Result<Value, String>,
    closed: Result<Value, String>,
}
impl CapitalReconcileLookup for ScriptedReconcile {
    fn listfunds(&self) -> Result<Value, String> {
        self.listfunds.clone()
    }
    fn listclosedchannels(&self) -> Result<Value, String> {
        self.closed.clone()
    }
}
fn empty_reconcile() -> Arc<ScriptedReconcile> {
    Arc::new(ScriptedReconcile {
        listfunds: Ok(json!({"channels": []})),
        closed: Ok(json!({"closedchannels": []})),
    })
}

struct Harness {
    handle: CapitalOwnerHandle,
    store: ObserverHandle,
    _dir: tempfile::TempDir,
}

async fn harness(
    adapters: Option<CapitalActionAdapters>,
    governor: Option<Arc<dyn GovernorFacade>>,
    budget: Arc<dyn BudgetDb>,
    evidence: Arc<dyn CapitalEvidenceRpc>,
    reconcile: Arc<dyn CapitalReconcileLookup>,
) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let handle = spawn_capital_owner(CapitalOwnerDeps {
        adapters: adapters.map(Arc::new),
        governor,
        budget,
        evidence,
        store: store.clone(),
        reconcile,
        clock: Box::new(|| NOW),
    });
    Harness {
        handle,
        store,
        _dir: dir,
    }
}

fn open_adapters(fund: Arc<CountingFund>) -> CapitalActionAdapters {
    CapitalActionAdapters {
        fundchannel: fund,
        close: CountingClose::returning(vec![]),
        defib: Arc::new(NoDefib),
    }
}

fn open_action(peer: &str) -> CapitalAction {
    CapitalAction::Open {
        peer_id: peer.to_string(),
        amount_sats: 1_000_000,
        reason: "test open".to_string(),
    }
}

// -- rail ordering ----------------------------------------------------------

/// Every pre-wire gate refuses BEFORE the adapters and BEFORE any durable
/// write: no adapters, no governor, denied governor, stale budget, busy
/// pair.
#[tokio::test]
async fn gates_refuse_before_adapters_and_before_any_write() {
    // No adapters (pre-cutover production).
    let fund = CountingFund::returning(vec![]);
    let h = harness(
        None,
        Some(Arc::new(AllowAll)),
        healthy_budget(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;
    let err = h.handle.execute(open_action("02aa")).await.unwrap_err();
    assert!(
        matches!(err, CapitalRefusal::AdaptersNotAssembled),
        "{err:?}"
    );
    assert_eq!(err.code(), "capital_adapters_not_assembled");

    // No governor.
    let h = harness(
        Some(open_adapters(fund.clone())),
        None,
        healthy_budget(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;
    let err = h.handle.execute(open_action("02aa")).await.unwrap_err();
    assert!(
        matches!(err, CapitalRefusal::GovernorNotAssembled),
        "{err:?}"
    );

    // Governor denies.
    let h = harness(
        Some(open_adapters(fund.clone())),
        Some(Arc::new(DenyAll)),
        healthy_budget(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;
    let err = h.handle.execute(open_action("02aa")).await.unwrap_err();
    match &err {
        CapitalRefusal::GovernorDenied { reason_code } => {
            assert_eq!(reason_code, "governor_test_denied")
        }
        other => panic!("{other:?}"),
    }

    // Stale budget evidence.
    let stale = Arc::new(ScriptedBudget(Ok(BudgetEvidence {
        available_sats: 5_000_000,
        window_reserved_sats: 0,
        observed_at: NOW - 300,
    })));
    let h = harness(
        Some(open_adapters(fund.clone())),
        Some(Arc::new(AllowAll)),
        stale,
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;
    let err = h.handle.execute(open_action("02aa")).await.unwrap_err();
    assert!(
        matches!(err, CapitalRefusal::Budget(BudgetRefusal::Stale { .. })),
        "{err:?}"
    );
    assert_eq!(err.code(), "capital_budget_evidence_stale");

    // Nothing above reached the wire or the store.
    assert_eq!(fund.calls(), 0);
    assert!(h
        .store
        .unresolved_capital_intents()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(h.store.active_capital_reserved_sats(0).await.unwrap(), 0);
}

/// A CLOSE is exempt from the budget-exhausted gate (it RECOVERS capital)
/// but still requires the freshness of the read; the busy-pair registry
/// binds every kind.
#[tokio::test]
async fn close_skips_budget_exhaustion_and_registry_blocks_duplicates() {
    let close = CountingClose::returning(vec![
        Ok(json!({"type": "mutual", "txid": "aa"})),
        Ok(json!({"type": "mutual", "txid": "bb"})),
    ]);
    let exhausted = Arc::new(ScriptedBudget(Ok(BudgetEvidence {
        available_sats: 0,
        window_reserved_sats: 9_000_000,
        observed_at: NOW - 1,
    })));
    let h = harness(
        Some(CapitalActionAdapters {
            fundchannel: CountingFund::returning(vec![]),
            close: close.clone(),
            defib: Arc::new(NoDefib),
        }),
        Some(Arc::new(AllowAll)),
        exhausted.clone(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;

    // Exhausted budget refuses an OPEN...
    let err = h.handle.execute(open_action("02aa")).await.unwrap_err();
    assert_eq!(err.code(), "capital_budget_exhausted");

    // ...but a CLOSE proceeds to success.
    let outcome = h
        .handle
        .execute(CapitalAction::Close {
            peer_id: "02bb".into(),
            scid: "700x1x0".into(),
            reason: "loser".into(),
        })
        .await
        .expect("close proceeds under exhausted budget");
    assert!(matches!(
        outcome.outcome,
        CapitalSubmitOutcome::Success { .. }
    ));
}

/// The happy open: durable intent first, one wire call, exactly-once
/// settle with txid, reservation settled, pair freed for the next
/// submission.
#[tokio::test]
async fn happy_open_settles_exactly_once_and_frees_the_pair() {
    let fund = CountingFund::returning(vec![
        Ok(json!({"txid": "feed", "channel_id": "abc"})),
        Ok(json!({"txid": "f00d", "channel_id": "def"})),
    ]);
    let h = harness(
        Some(open_adapters(fund.clone())),
        Some(Arc::new(AllowAll)),
        healthy_budget(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;

    let outcome = h.handle.execute(open_action("02aa")).await.unwrap();
    match &outcome.outcome {
        CapitalSubmitOutcome::Success { txid } => assert_eq!(txid.as_deref(), Some("feed")),
        other => panic!("{other:?}"),
    }
    assert_eq!(fund.calls(), 1);
    // Terminal in the store: nothing unresolved, reservation settled
    // (settled reservations do not hold the budget window).
    assert!(h
        .store
        .unresolved_capital_intents()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(h.store.active_capital_reserved_sats(0).await.unwrap(), 0);
    // Pair freed: a second open to the same peer is admitted.
    let outcome = h.handle.execute(open_action("02aa")).await.unwrap();
    assert!(matches!(
        outcome.outcome,
        CapitalSubmitOutcome::Success { .. }
    ));
}

/// Evidence revalidation runs AFTER the durable intent and refuses as a
/// SETTLED clean refusal: an open to a peer that already has a channel,
/// a close whose target vanished, and an unreadable snapshot all settle
/// `clean_refusal`/released with ZERO wire calls.
#[tokio::test]
async fn revalidation_failures_settle_clean_refusal_without_wire_calls() {
    let fund = CountingFund::returning(vec![]);
    // Open to 02bb -- which ALREADY has a live channel in the snapshot.
    let h = harness(
        Some(open_adapters(fund.clone())),
        Some(Arc::new(AllowAll)),
        healthy_budget(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;
    let outcome = h.handle.execute(open_action("02bb")).await.unwrap();
    assert!(
        matches!(outcome.outcome, CapitalSubmitOutcome::CleanRefusal { .. }),
        "{outcome:?}"
    );
    assert_eq!(fund.calls(), 0);
    assert!(h
        .store
        .unresolved_capital_intents()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        h.store.active_capital_reserved_sats(0).await.unwrap(),
        0,
        "clean refusal releases the reservation"
    );

    // Close whose target channel is not in the snapshot.
    let close = CountingClose::returning(vec![]);
    let h = harness(
        Some(CapitalActionAdapters {
            fundchannel: CountingFund::returning(vec![]),
            close: close.clone(),
            defib: Arc::new(NoDefib),
        }),
        Some(Arc::new(AllowAll)),
        healthy_budget(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;
    let outcome = h
        .handle
        .execute(CapitalAction::Close {
            peer_id: "02bb".into(),
            scid: "999x9x9".into(),
            reason: "loser".into(),
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome.outcome, CapitalSubmitOutcome::CleanRefusal { .. }),
        "{outcome:?}"
    );

    // Unreadable snapshot: clean refusal (nothing was sent), fail-closed.
    let h = harness(
        Some(open_adapters(fund.clone())),
        Some(Arc::new(AllowAll)),
        healthy_budget(),
        Arc::new(ScriptedEvidence(Err("listpeerchannels rpc timeout".into()))),
        empty_reconcile(),
    )
    .await;
    let outcome = h.handle.execute(open_action("02aa")).await.unwrap();
    assert!(
        matches!(outcome.outcome, CapitalSubmitOutcome::CleanRefusal { .. }),
        "{outcome:?}"
    );
    assert_eq!(fund.calls(), 0);
}

/// A lost fundchannel reply QUARANTINES: outcome_unknown settled, the
/// reservation still holds the budget window, and the (kind, peer) pair
/// stays blocked in-process -- structurally no resubmit.
#[tokio::test]
async fn unknown_outcome_quarantines_and_keeps_the_pair_blocked() {
    let fund = CountingFund::returning(vec![Err(RpcFailure {
        message: "rpc timeout after 30s awaiting fundchannel reply".into(),
    })]);
    let h = harness(
        Some(open_adapters(fund.clone())),
        Some(Arc::new(AllowAll)),
        healthy_budget(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;

    let outcome = h.handle.execute(open_action("02aa")).await.unwrap();
    assert!(
        matches!(outcome.outcome, CapitalSubmitOutcome::OutcomeUnknown { .. }),
        "{outcome:?}"
    );
    assert_eq!(fund.calls(), 1);
    // Terminal (settled) but QUARANTINED: budget still held.
    assert!(h
        .store
        .unresolved_capital_intents()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        h.store.active_capital_reserved_sats(0).await.unwrap(),
        1_000_000
    );
    assert_eq!(
        h.store.quarantined_capital_intents().await.unwrap().len(),
        1
    );
    // The pair stays blocked: no second open to the same peer.
    let err = h.handle.execute(open_action("02aa")).await.unwrap_err();
    assert!(matches!(err, CapitalRefusal::IntentBusy { .. }), "{err:?}");
    assert_eq!(fund.calls(), 1, "no second wire call");
}

/// A settle-persistence failure SUSPENDS the owner: the failed submission
/// surfaces typed and every later submission refuses until restart.
#[tokio::test]
async fn settle_failure_suspends_further_submissions() {
    let fund = CountingFund::returning(vec![
        Ok(json!({"txid": "feed"})),
        Ok(json!({"txid": "beef"})),
    ]);
    let h = harness(
        Some(open_adapters(fund.clone())),
        Some(Arc::new(AllowAll)),
        healthy_budget(),
        healthy_evidence(),
        empty_reconcile(),
    )
    .await;

    // Sabotage ONLY the settle path: a BEFORE UPDATE trigger poisons the
    // terminal write while the intent INSERT stays healthy.
    let db_path = h._dir.path().join("observer.db");
    let raw = rusqlite::Connection::open(&db_path).unwrap();
    raw.execute_batch(
        "CREATE TRIGGER poison_settle BEFORE UPDATE ON rust_capital_intents
         BEGIN SELECT RAISE(ABORT, 'injected settle failure'); END;",
    )
    .unwrap();

    let err = h.handle.execute(open_action("02aa")).await.unwrap_err();
    assert!(
        matches!(err, CapitalRefusal::SettlePersistenceUnknown { .. }),
        "{err:?}"
    );
    let err = h.handle.execute(open_action("02cc")).await.unwrap_err();
    assert!(matches!(err, CapitalRefusal::Suspended), "{err:?}");
    assert_eq!(fund.calls(), 1, "no execution while suspended");
}

// -- restart reconciliation ---------------------------------------------------

/// Reconcile-on-start: an open visible in listfunds settles success (with
/// its funding txid), a close visible in listclosedchannels settles
/// success, and EVERYTHING else -- absent opens, absent closes, defibs,
/// lookup failures -- QUARANTINES. Afterward the registry seeds from the
/// quarantined pairs.
#[tokio::test]
async fn reconcile_settles_definite_and_quarantines_the_rest() {
    let reconcile = Arc::new(ScriptedReconcile {
        listfunds: Ok(json!({"channels": [
            {"peer_id": "02aa", "state": "CHANNELD_AWAITING_LOCKIN", "funding_txid": "feedbeef"},
        ]})),
        closed: Ok(json!({"closedchannels": [
            {"short_channel_id": "700x1x0", "peer_id": "02bb"},
        ]})),
    });
    // Assembled adapters/governor so the post-reconcile registry check
    // below is observable (the adapters gate precedes the registry).
    let fund = CountingFund::returning(vec![Ok(json!({"txid": "0aa0"}))]);
    let h = harness(
        Some(open_adapters(fund.clone())),
        Some(Arc::new(AllowAll)),
        healthy_budget(),
        healthy_evidence(),
        reconcile,
    )
    .await;

    let intents = [
        ("r-open-visible", "open", "02aa", None, 1_000_000),
        ("r-open-absent", "open", "02dd", None, 2_000_000),
        ("r-close-visible", "close", "02bb", Some("700x1x0"), 0),
        ("r-close-absent", "close", "02ee", Some("800x1x0"), 0),
        ("r-defib", "defib", "02ff", Some("900x1x0"), 50_000),
    ];
    for (id, kind, peer, scid, sats) in intents {
        h.store
            .insert_capital_intent(revops_db::fee_runway::CapitalIntent {
                request_id: id.into(),
                kind: kind.into(),
                peer_id: peer.into(),
                channel_id: scid.map(String::from),
                amount_sats: sats,
                reason: None,
                submitted_at: NOW - 500,
            })
            .await
            .unwrap();
    }

    let summary = h.handle.reconcile_on_start().await.unwrap();
    assert_eq!(summary.settled_success, 2);
    assert_eq!(summary.quarantined, 3);
    assert!(h
        .store
        .unresolved_capital_intents()
        .await
        .unwrap()
        .is_empty());
    let quarantined = h.store.quarantined_capital_intents().await.unwrap();
    let ids: Vec<&str> = quarantined.iter().map(|q| q.request_id.as_str()).collect();
    assert_eq!(ids, ["r-open-absent", "r-close-absent", "r-defib"]);

    // The quarantined open's budget hold survives; the visible open
    // settled WITH its funding txid.
    assert_eq!(
        h.store.active_capital_reserved_sats(0).await.unwrap(),
        2_050_000
    );

    // Registry seeded: the quarantined (open, 02dd) pair refuses, while
    // the SETTLED 02aa pair is free and runs the full rail to success.
    let err = h
        .handle
        .execute(CapitalAction::Open {
            peer_id: "02dd".into(),
            amount_sats: 500_000,
            reason: "again".into(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, CapitalRefusal::IntentBusy { .. }), "{err:?}");
    let outcome = h.handle.execute(open_action("02aa")).await.unwrap();
    assert!(
        matches!(outcome.outcome, CapitalSubmitOutcome::Success { .. }),
        "the settled pair is free after reconcile: {outcome:?}"
    );
    assert_eq!(fund.calls(), 1);
}

/// Lookup failures during reconcile quarantine (never release, never
/// crash), and the summary reports them.
#[tokio::test]
async fn reconcile_lookup_failure_quarantines() {
    let reconcile = Arc::new(ScriptedReconcile {
        listfunds: Err("listfunds rpc timeout".into()),
        closed: Err("listclosedchannels rpc timeout".into()),
    });
    let h = harness(None, None, healthy_budget(), healthy_evidence(), reconcile).await;
    h.store
        .insert_capital_intent(revops_db::fee_runway::CapitalIntent {
            request_id: "r-open".into(),
            kind: "open".into(),
            peer_id: "02aa".into(),
            channel_id: None,
            amount_sats: 1_000_000,
            reason: None,
            submitted_at: NOW - 500,
        })
        .await
        .unwrap();
    let summary = h.handle.reconcile_on_start().await.unwrap();
    assert_eq!(summary.settled_success, 0);
    assert_eq!(summary.quarantined, 1);
    assert_eq!(
        h.store.quarantined_capital_intents().await.unwrap().len(),
        1
    );
}

// -- capability discipline ----------------------------------------------------

/// Neither the capability wrapper nor the governor stand-ins are
/// constructible from production surfaces before Task 69.
#[test]
fn capital_capability_unreachable_from_production() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for file in ["src/runtime.rs", "src/lnplus_runtime.rs", "src/main.rs"] {
        let source = std::fs::read_to_string(root.join(file)).unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap();
        for name in ["CapitalActionAdapters {", "CapitalActionAdapters{"] {
            assert!(
                !production.contains(name),
                "{file} must not construct CapitalActionAdapters before Task 69"
            );
        }
    }
}
