//! Task 62 slice 3: fail-closed assembly + the frozen kernel accepting
//! assembled evidence deterministically.

use revops::capital_boundaries::{BudgetDb, BudgetEvidence};
use revops::capital_evidence::{assemble_cycle_evidence, EvidenceDeps, EvidenceRefusal};
use revops_capital::planner::cycle::plan_cycle;
use serde_json::json;
use std::collections::BTreeMap;

struct ScriptedBudget(Result<BudgetEvidence, String>);
impl BudgetDb for ScriptedBudget {
    fn positive_budget_evidence(&self, _now: i64) -> Result<BudgetEvidence, String> {
        self.0.clone()
    }
}

const NOW: i64 = 1_800_000_000;

fn healthy_budget() -> ScriptedBudget {
    ScriptedBudget(Ok(BudgetEvidence {
        available_sats: 2_000_000,
        window_reserved_sats: 0,
        observed_at: NOW - 3,
    }))
}

fn deps(budget: &ScriptedBudget) -> EvidenceDeps<'_> {
    EvidenceDeps {
        planner_enabled: true,
        fee_gate: Ok(()),
        peer_channels_raw: Ok(json!({"channels": [
            {"peer_id": "02aa", "state": "CHANNELD_NORMAL",
             "to_us_msat": 600_000_000i64, "total_msat": 2_000_000_000i64},
            {"peer_id": "02bb", "state": "CHANNELD_NORMAL",
             "to_us_msat": 100_000_000i64, "total_msat": 1_000_000_000i64},
        ]})),
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
    }
}

/// Every required source refuses typed when it fails.
#[test]
fn required_sources_refuse_typed() {
    let budget = healthy_budget();

    let mut d = deps(&budget);
    d.peer_channels_raw = Err("listpeerchannels rpc timeout".into());
    let err = assemble_cycle_evidence(d).expect_err("channels failure refuses");
    assert_eq!(err.code(), "capital_evidence_peer_channels_unavailable");

    let stale = ScriptedBudget(Ok(BudgetEvidence {
        available_sats: 2_000_000,
        window_reserved_sats: 0,
        observed_at: NOW - 300,
    }));
    let err = assemble_cycle_evidence(deps(&stale)).expect_err("stale budget refuses");
    assert_eq!(err.code(), "capital_budget_evidence_stale");

    let mut d = deps(&budget);
    d.backoff_actions = Err("planner_actions read failed".into());
    let err = assemble_cycle_evidence(d).expect_err("backoff failure refuses");
    assert_eq!(err.code(), "capital_evidence_backoff_unavailable");

    // A reply without a channels array is unusable evidence, not "no
    // channels".
    let mut d = deps(&budget);
    d.peer_channels_raw = Ok(json!({"result": "ok"}));
    let err = assemble_cycle_evidence(d).expect_err("shapeless reply refuses");
    assert!(matches!(err, EvidenceRefusal::PeerChannelsUnavailable(_)));
}

/// A healthy assembly feeds the FROZEN kernel deterministically, carries
/// the parsed balances and budget, and reports the analytics gaps
/// honestly.
#[test]
fn healthy_assembly_feeds_the_frozen_kernel() {
    let budget = healthy_budget();
    let assembled = assemble_cycle_evidence(deps(&budget)).expect("healthy assembly");

    assert_eq!(assembled.evidence.peer_channels.len(), 2);
    assert_eq!(assembled.evidence.peer_channels[0].to_us_msat, 600_000_000);
    assert_eq!(assembled.evidence.exposure_channels[1].peer_id, "02bb");
    assert_eq!(assembled.evidence.available_sats, 2_000_000);

    let gap_fields: Vec<&str> = assembled.gaps.iter().map(|g| g.field).collect();
    // Task 67b CLOSED these two: winners/losers are now supplied by the
    // profitability + flow assemblers, so they must NO LONGER be declared
    // gaps. A gap that reappears here means the analytics regressed.
    for closed in ["winner_channels", "loser_channels"] {
        assert!(
            !gap_fields.contains(&closed),
            "{closed} is supplied by task 67b and must not be a declared gap: {gap_fields:?}"
        );
    }
    // The remaining analytics gaps are still honestly declared.
    for expected in ["discovery", "recycle_candidates"] {
        assert!(gap_fields.contains(&expected), "missing gap for {expected}");
    }

    // The frozen kernel is total over the assembled evidence: with empty
    // candidate sets it plans NO actions (and does not skip -- the
    // planner ran, there was simply nothing to do).
    let plan = plan_cycle(&assembled.evidence);
    assert!(plan.opens.is_empty());
    assert!(plan.closes.is_empty());
    assert!(plan.defibrillations.is_empty());

    // Fee-gate failure propagates verbatim as a recorded reason (the
    // kernel's Python-parity semantic: per-action gating, not a
    // whole-cycle skip)...
    let mut d = deps(&budget);
    d.fee_gate = Err("fee loop unhealthy".into());
    let assembled = assemble_cycle_evidence(d).expect("assembles with a failed gate");
    let plan = plan_cycle(&assembled.evidence);
    assert!(!plan.skipped);
    assert!(
        plan.skipped_reasons
            .contains(&"fee loop unhealthy".to_string()),
        "{:?}",
        plan.skipped_reasons
    );

    // ...while planner_enabled=false skips the whole cycle.
    let mut d = deps(&budget);
    d.planner_enabled = false;
    let assembled = assemble_cycle_evidence(d).expect("assembles disabled");
    let plan = plan_cycle(&assembled.evidence);
    assert!(plan.skipped);
    assert_eq!(plan.skip_reason.as_deref(), Some("planner disabled"));
}
