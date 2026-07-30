//! Task 62 slice 5: `revenue-planner-execute` -- Python's verbatim
//! uninitialized arm pre-cutover, the frozen kernel + owner rail behind
//! an assembled owner, and the pure plan-to-action mapping.

use std::collections::BTreeMap;
use std::sync::Arc;

use revops::capital_boundaries::{BudgetDb, BudgetEvidence, GovernorFacade, GovernorVerdict};
use revops::capital_evidence::{assemble_cycle_evidence, EvidenceDeps, EvidenceRefusal};
use revops::capital_owner::{
    spawn_capital_owner, CapitalAction, CapitalEvidenceRpc, CapitalOwnerDeps,
    CapitalReconcileLookup, DEFIB_AMOUNT_SATS,
};
use revops::rpc_planner_execute::{handle_planner_execute, planned_actions};
use revops_capital::planner::cycle::{CyclePlan, PlannedClose, PlannedDefibrillation, PlannedOpen};
use revops_db::owner::spawn_read_write;
use serde_json::{json, Value};

const NOW: i64 = 1_800_000_000;

struct ScriptedBudget(Result<BudgetEvidence, String>);
impl BudgetDb for ScriptedBudget {
    fn positive_budget_evidence(&self, _now: i64) -> Result<BudgetEvidence, String> {
        self.0.clone()
    }
}

struct ScriptedEvidence(Value);
impl CapitalEvidenceRpc for ScriptedEvidence {
    fn get_peer_channels(&self) -> Result<Value, String> {
        Ok(self.0.clone())
    }
}

struct EmptyReconcile;
impl CapitalReconcileLookup for EmptyReconcile {
    fn listfunds(&self) -> Result<Value, String> {
        Ok(json!({"channels": []}))
    }
    fn listclosedchannels(&self) -> Result<Value, String> {
        Ok(json!({"closedchannels": []}))
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

/// The mapping from the frozen kernel's plan to owner actions preserves
/// the Python execution order (defibrillations, closes, opens) and the
/// bounded defib constants.
#[test]
fn planned_actions_map_in_python_execution_order() {
    let mut plan = CyclePlan::default();
    plan.opens.push(PlannedOpen {
        peer_id: "02aa".into(),
        amount_sats: 1_500_000,
        ev: 12.5,
    });
    plan.closes.push(PlannedClose {
        scid: "700x1x0".into(),
        peer_id: "02bb".into(),
        reason: "loser".into(),
    });
    plan.defibrillations.push(PlannedDefibrillation {
        scid: "800x1x0".into(),
        peer_id: "02cc".into(),
        reason: "stagnant".into(),
    });

    let actions = planned_actions(&plan);
    assert_eq!(
        actions,
        vec![
            CapitalAction::Defibrillate {
                peer_id: "02cc".into(),
                scid: "800x1x0".into(),
                reason: "stagnant".into(),
            },
            CapitalAction::Close {
                peer_id: "02bb".into(),
                scid: "700x1x0".into(),
                reason: "loser".into(),
            },
            CapitalAction::Open {
                peer_id: "02aa".into(),
                amount_sats: 1_500_000,
                reason: "planned open ev=12.5".to_string(),
            },
        ]
    );
    // The defib amount is not part of the plan shape -- the owner uses
    // the bounded Python constant.
    assert_eq!(DEFIB_AMOUNT_SATS, 50_000);
}

/// Python's verbatim uninitialized arm: no owner at all, AND an owner
/// whose adapters are unassembled (pre-cutover production). In the
/// second case evidence assembly must never run.
#[tokio::test]
async fn uninitialized_arms_are_python_verbatim() {
    let expected = json!({"error": "Capacity planner not initialized"});

    let response =
        handle_planner_execute(None, || panic!("assembly must not run without an owner")).await;
    assert_eq!(response, expected);

    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let handle = spawn_capital_owner(CapitalOwnerDeps {
        adapters: None,
        governor: None,
        budget: Arc::new(ScriptedBudget(Err("unassembled".into()))),
        evidence: Arc::new(ScriptedEvidence(json!({"channels": []}))),
        store,
        reconcile: Arc::new(EmptyReconcile),
        clock: Box::new(|| NOW),
    });
    let response = handle_planner_execute(Some(&handle), || {
        panic!("assembly must not run while adapters are unassembled")
    })
    .await;
    assert_eq!(response, expected);
}

fn healthy_deps(budget: &ScriptedBudget) -> EvidenceDeps<'_> {
    EvidenceDeps {
        planner_enabled: true,
        fee_gate: Ok(()),
        peer_channels_raw: Ok(json!({"channels": []})),
        budget,
        backoff_actions: Ok(BTreeMap::new()),
        defibrillation_limit: 1,
        close_execution_enabled: false,
        close_limit: None,
        max_channel_sats: 5_000_000,
        min_channel_sats: 500_000,
        max_opens_per_cycle: 2,
        exploration_budget_sats: 0,
        estimated_open_cost_sats: 2_000,
        recycle_block_height: 900_000,
        recycle_close_cost_sats: 1_000,
        now: NOW,
        winner_channels: Vec::new(),
        loser_channels: Vec::new(),
        defib_gates: Default::default(),
        close_gates: Default::default(),
        open_guards: Default::default(),
    }
}

/// An assembled owner: refused assembly surfaces typed; a healthy empty
/// cycle reports zero actions WITH the honest analytics gaps.
#[tokio::test]
async fn assembled_owner_runs_the_cycle_and_reports_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let handle = spawn_capital_owner(CapitalOwnerDeps {
        adapters: Some(Arc::new(revops::capital_owner::CapitalActionAdapters {
            fundchannel: Arc::new(NeverCalled),
            close: Arc::new(NeverCalled),
            defib: Arc::new(NeverCalled),
        })),
        governor: Some(Arc::new(AllowAll)),
        budget: Arc::new(ScriptedBudget(Ok(BudgetEvidence {
            available_sats: 5_000_000,
            window_reserved_sats: 0,
            observed_at: NOW - 1,
        }))),
        evidence: Arc::new(ScriptedEvidence(json!({"channels": []}))),
        store,
        reconcile: Arc::new(EmptyReconcile),
        clock: Box::new(|| NOW),
    });

    // Refused assembly (stale budget) surfaces typed, not the arm.
    let stale = ScriptedBudget(Ok(BudgetEvidence {
        available_sats: 5_000_000,
        window_reserved_sats: 0,
        observed_at: NOW - 900,
    }));
    let response = handle_planner_execute(Some(&handle), || {
        assemble_cycle_evidence(healthy_deps(&stale))
    })
    .await;
    assert_eq!(response["status"], "error");
    assert_eq!(response["code"], "capital_budget_evidence_stale");

    // Healthy empty evidence: the frozen kernel plans nothing; the
    // response is success-shaped with zero results and the gap list.
    let healthy = ScriptedBudget(Ok(BudgetEvidence {
        available_sats: 5_000_000,
        window_reserved_sats: 0,
        observed_at: NOW - 1,
    }));
    let response = handle_planner_execute(Some(&handle), || {
        assemble_cycle_evidence(healthy_deps(&healthy))
    })
    .await;
    assert_eq!(response["status"], "success", "{response:?}");
    assert_eq!(response["planned"], 0);
    assert_eq!(response["results"], json!([]));
    let gaps = response["evidence_gaps"].as_array().expect("gap list");
    // Task 67b closed winner_channels/loser_channels; planner-status must
    // stop advertising them as gaps or it understates what the planner can
    // now see.
    assert!(
        !gaps.iter().any(|g| g["field"] == "winner_channels"),
        "winner_channels is supplied by task 67b: {gaps:?}"
    );
    // The genuinely-remaining analytics gaps are still surfaced.
    assert!(gaps.iter().any(|g| g["field"] == "discovery"), "{gaps:?}");

    // planner_enabled=false: the kernel skips; the response says so.
    let mut deps = healthy_deps(&healthy);
    deps.planner_enabled = false;
    let response = handle_planner_execute(Some(&handle), || assemble_cycle_evidence(deps)).await;
    assert_eq!(response["status"], "skipped");
    assert_eq!(response["skip_reason"], "planner disabled");
}

struct NeverCalled;
impl revops::capital_adapters::FundchannelRpc for NeverCalled {
    fn fundchannel(
        &self,
        _peer_id: &str,
        _amount_sats: i64,
        _request_amt: Option<i64>,
        _compact_lease: Option<String>,
    ) -> Result<Value, revops_rebalance::router::RpcFailure> {
        panic!("no wire calls in these tests");
    }
}
impl revops::capital_adapters::CloseRpc for NeverCalled {
    fn close(
        &self,
        _channel_id: &str,
        _unilateral_timeout: Option<i64>,
    ) -> Result<Value, revops_rebalance::router::RpcFailure> {
        panic!("no wire calls in these tests");
    }
}
impl revops::capital_owner::DefibExecutor for NeverCalled {
    fn diagnostic_rebalance(
        &self,
        _peer_id: &str,
        _scid: &str,
        _amount_sats: i64,
    ) -> revops::capital_boundaries::CapitalSubmitOutcome {
        panic!("no defib in these tests");
    }
}

/// Evidence-assembly refusals never panic the handler and carry the
/// refusal's stable code.
#[tokio::test]
async fn assembly_refusal_is_typed_json() {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let handle = spawn_capital_owner(CapitalOwnerDeps {
        adapters: Some(Arc::new(revops::capital_owner::CapitalActionAdapters {
            fundchannel: Arc::new(NeverCalled),
            close: Arc::new(NeverCalled),
            defib: Arc::new(NeverCalled),
        })),
        governor: Some(Arc::new(AllowAll)),
        budget: Arc::new(ScriptedBudget(Err("unused".into()))),
        evidence: Arc::new(ScriptedEvidence(json!({"channels": []}))),
        store,
        reconcile: Arc::new(EmptyReconcile),
        clock: Box::new(|| NOW),
    });
    let response = handle_planner_execute(Some(&handle), || {
        Err(EvidenceRefusal::PeerChannelsUnavailable(
            "listpeerchannels rpc timeout".into(),
        ))
    })
    .await;
    assert_eq!(response["status"], "error");
    assert_eq!(
        response["code"],
        "capital_evidence_peer_channels_unavailable"
    );
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("listpeerchannels rpc timeout"));
}
