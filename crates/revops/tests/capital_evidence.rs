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
        defib_gates: Default::default(),
        close_gates: Default::default(),
        open_guards: Default::default(),
        discovery: Default::default(),
        candidate_enrichment: Default::default(),
        open_candidate_evidence: Default::default(),
        dual_fund_peers: Default::default(),
        redeployment_winner_evs: Vec::new(),
        recycle_candidates: Vec::new(),
        // Default to the FAIL-CLOSED protection state, matching what a
        // caller that never read policies would honestly have.
        recycle_protected_peers: None,
        recycle_route_pair_scids: Default::default(),
        recycle_close_protection: Default::default(),
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
    // Task 67c closed the remaining SIX. There are now no declared gaps at
    // all: the planner's evidence is complete, so a gap reappearing here is
    // a regression in the open side, not an honest disclosure.
    assert!(
        gap_fields.is_empty(),
        "task 67c closed every gap; still declared: {gap_fields:?}"
    );

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

/// Task 67c: all six formerly-gapped fields carry through to the frozen
/// kernel. Passing them is what makes the planner able to OPEN and RECYCLE
/// rather than running, finding nothing, and reporting success.
#[test]
fn task_67c_fields_reach_the_kernel() {
    use revops_capital::planner::candidate_score::CandidateEnrichmentEvidence;
    use revops_capital::planner::cycle::{OpenCandidateEvidence, RecycleCandidateOwned};
    use revops_capital::planner::ev::OpenEvInputs;

    let ev_template = OpenEvInputs {
        channel_size_sats: 2_000_000,
        closed_channel_daily_net_est_sats: None,
        observed_node_daily_ppm: Some(15.0),
        open_cost_sats: 1_400,
        close_cost_sats: 400,
        inbound_median_fee_ppm: Some(120.0),
        min_annual_roi_pct: 1.0,
    };
    let enrichment = CandidateEnrichmentEvidence {
        reputation: None,
        closed_channel_profit: None,
        uptime_pct: Some(100.0),
        has_clearnet_address: true,
        inbound_median_fee_ppm: Some(120.0),
        dest_channel_capacities_sats: vec![3_000_000],
        is_sink_adjacent: false,
        demand_flow_role: None,
    };

    let budget = healthy_budget();
    let mut d = deps(&budget);
    d.candidate_enrichment = BTreeMap::from([("02cc".to_string(), enrichment.clone())]);
    d.open_candidate_evidence = BTreeMap::from([(
        "02cc".to_string(),
        OpenCandidateEvidence {
            peer_dest_channel_capacities_sats: vec![3_000_000],
            open_ev_template: ev_template,
            enrichment,
        },
    )]);
    d.dual_fund_peers = ["02cc".to_string()].into_iter().collect();
    d.redeployment_winner_evs = vec![("02aa".to_string(), ev_template)];
    d.recycle_candidates = vec![RecycleCandidateOwned {
        peer_id: "02cc".to_string(),
        score: 0.9,
        open_ev_template: ev_template,
    }];
    d.recycle_protected_peers = Some(["02protected".to_string()].into_iter().collect());
    d.recycle_route_pair_scids = ["900000x1x0".to_string()].into_iter().collect();

    let assembled = assemble_cycle_evidence(d).expect("healthy assembly");
    let e = &assembled.evidence;

    assert!(e.candidate_enrichment.contains_key("02cc"));
    assert!(e.open_candidate_evidence.contains_key("02cc"));
    assert!(e.dual_fund_peers.contains("02cc"));
    // F71-R10: a TEMPLATE, not a precomputed scalar -- the loser's
    // capacity is substituted at pricing time.
    assert_eq!(e.redeployment_winner_evs.len(), 1);
    assert_eq!(e.redeployment_winner_evs[0].0, "02aa");
    assert_eq!(e.recycle_candidates.len(), 1);
    assert_eq!(e.recycle_route_pair_scids.len(), 1);
    assert!(
        e.recycle_protected_peers
            .as_ref()
            .expect("protection state carried")
            .contains("02protected"),
        "the protection set must survive assembly -- dropping it to None \
         would block every recycle, and dropping it to empty would expose \
         protected peers"
    );
    assert!(assembled.gaps.is_empty());
}

/// The gap marker itself is DELETED, not merely unused. A constant left
/// behind is a live invitation to re-declare a gap that no longer exists.
#[test]
fn the_analytics_gap_marker_is_gone() {
    let src = include_str!("../src/capital_evidence.rs");
    assert!(
        !src.contains("ANALYTICS_GAP"),
        "ANALYTICS_GAP must be deleted now that every field is supplied"
    );
}
