//! `planner::cycle::plan_cycle` — orchestration composition tests.
//!
//! Whole-cycle fixture parity against Python's `execute_cycle` is NOT
//! attempted here (see `crates/revops-capital/TASK47-REPORT.md`): Python's
//! orchestration is RPC/DB-heavy end to end (arbitration registry, policy
//! manager, LN+ evaluator, capex engine) in ways this crate deliberately
//! keeps out of its pure-evidence contract. Every sub-decision `plan_cycle`
//! calls into (`identify_winners`, `identify_losers`, the five discovery
//! strategies, `score_candidate`, `size_channel`, `calculate_open_ev`, the
//! portfolio/close-fee/dead-capital gates) IS already fixture-verified
//! against real Python in its own test file. What these tests prove is
//! that `plan_cycle` actually WIRES them together — each test below is a
//! REVERT TRIPWIRE: deleting the corresponding call from `cycle.rs` makes
//! the named test fail. See TASK47-REPORT.md for the captured RED output
//! from deliberately reverting each one.

use revops_capital::planner::cycle::{
    CycleEvidence, DiscoveryEvidence, FlowContribution, NeighborEdge, RecycleCandidateOwned,
};
use revops_capital::planner::demand_flow::SinkChannelEdge;
use revops_capital::planner::discovery::{GraphChannelEdge, PatronCandidate, RoutePairRow};
use revops_capital::planner::ev::OpenEvInputs;
use revops_capital::planner::losers::LoserChannelEvidence;
use revops_capital::planner::winners::WinnerCandidateEvidence;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/capital/planner/kernels.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn fixture_scenarios(kind: &str) -> Vec<Value> {
    fixture()["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == kind)
        .cloned()
        .collect()
}

fn base_evidence() -> CycleEvidence {
    CycleEvidence {
        planner_enabled: true,
        fee_gate_ok: true,
        defibrillation_limit: 5,
        close_execution_enabled: false,
        close_limit: None,
        max_channel_sats: 10_000_000,
        min_channel_sats: 500_000,
        available_sats: 0,
        max_opens_per_cycle: 3,
        exploration_budget_sats: 0,
        estimated_open_cost_sats: 5000,
        recycle_block_height: 800_000,
        now: 1_800_000_000,
        discovery: DiscoveryEvidence {
            our_node_id: "us".to_string(),
            max_candidate_pool: 32,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn pool_peer_ids(plan: &revops_capital::planner::cycle::CyclePlan) -> Vec<String> {
    plan.candidate_pool
        .iter()
        .map(|c| c.peer_id.clone())
        .collect()
}

// --- Tripwire: planner_enabled gate is real ---------------------------------

#[test]
fn disabled_planner_short_circuits() {
    let mut ev = base_evidence();
    ev.planner_enabled = false;
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(plan.skipped);
    assert!(plan.winners.is_empty());
    assert!(plan.candidate_pool.is_empty());
}

#[test]
fn enabled_planner_does_not_skip() {
    let plan = revops_capital::planner::cycle::plan_cycle(&base_evidence());
    assert!(!plan.skipped);
}

// --- Tripwire: winner/loser classification is real, not bypassed -----------

#[test]
fn winner_classification_reaches_the_plan() {
    let mut ev = base_evidence();
    ev.winner_channels = vec![WinnerCandidateEvidence {
        scid: "700000:1:0".to_string(),
        peer_id: "winner_peer".to_string(),
        capacity_sats: 1_000_000,
        marginal_roi_percent: 45.0,
        flow: Some(revops_capital::planner::winners::WinnerFlowEvidence {
            daily_volume: 600_000.0,
            flow_ratio: 0.9,
            kalman_velocity: 0.0,
            is_congested: false,
        }),
        rebalance_success: None,
        sourced_fee_contribution_sats: 0,
        channel_role: None,
        dts_posterior_mean: None,
    }];
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert_eq!(plan.winner_count, 1);
    assert_eq!(plan.winners[0].peer_id, "winner_peer");
    // The winner ALSO reaches discovery Strategy 1 (roi 45 > 30) — proof
    // the SAME winners list plan_cycle computed is the one fed to discovery,
    // not a stray/independent copy.
    assert!(pool_peer_ids(&plan).contains(&"winner_peer".to_string()));
}

#[test]
fn loser_classification_reaches_the_plan() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![LoserChannelEvidence {
        scid: "700000:1:0".to_string(),
        peer_id: "loser_peer".to_string(),
        capacity_sats: 500_000,
        roi_percent: -70.0,
        marginal_roi_percent: -60.0,
        marginal_profit_30d_sats: -200,
        classification: "zombie".to_string(),
        days_open: 100,
        opener: "local".to_string(),
        flow: Some(revops_capital::planner::losers::LoserFlowEvidence {
            flow_ratio: 0.0,
            capacity: 500_000,
            daily_volume: 1000.0,
            kalman_regime_change: false,
        }),
        rebalance_success: None,
        diagnostic_attempt_count: 2,
        is_hard_bleeder: false,
        defib_policy_blocked: false,
        close_protection_reason: None,
        uptime_pct: None,
        estimated_closure_cost_sats: 3000,
        dead_capital: None,
    }];
    // Fresh, unblocked close-gate evidence: the classified CLOSE loser must
    // reach the close-selection step (Task 47 finding 1: missing evidence
    // now denies, so a happy-path test must supply it explicitly).
    ev.close_gates.insert(
        "loser_peer".to_string(),
        revops_capital::planner::cycle::CloseGate {
            observed_at: ev.now,
            ..Default::default()
        },
    );
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert_eq!(plan.loser_count, 1);
    assert_eq!(plan.losers[0].peer_id, "loser_peer");
    assert!(plan.loser_close_scids.contains("700000x1x0"));
    assert_eq!(plan.closes.len(), 1);
    assert_eq!(plan.closes[0].peer_id, "loser_peer");
}

// --- Tripwire: each of the five discovery strategies is actually called ----

#[test]
fn discovery_strategy_1_winners_reaches_candidate_pool() {
    let mut ev = base_evidence();
    ev.winner_channels = vec![WinnerCandidateEvidence {
        scid: "700000:1:0".to_string(),
        peer_id: "s1_winner_only".to_string(),
        capacity_sats: 1_000_000,
        marginal_roi_percent: 45.0,
        flow: Some(revops_capital::planner::winners::WinnerFlowEvidence {
            daily_volume: 600_000.0,
            flow_ratio: 0.9,
            kalman_velocity: 0.0,
            is_congested: false,
        }),
        rebalance_success: None,
        sourced_fee_contribution_sats: 0,
        channel_role: None,
        dts_posterior_mean: None,
    }];
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(pool_peer_ids(&plan).contains(&"s1_winner_only".to_string()));
}

#[test]
fn discovery_strategy_2_neighbors_reaches_candidate_pool() {
    let mut ev = base_evidence();
    ev.discovery.all_channels = vec![PatronCandidate {
        peer_id: "patronA".to_string(),
        marginal_roi_percent: 90.0,
    }];
    let mut patron_channels: BTreeMap<String, Vec<NeighborEdge>> = BTreeMap::new();
    patron_channels.insert(
        "patronA".to_string(),
        vec![NeighborEdge {
            destination: "s2_neighbor_only".to_string(),
            amount_msat: 1_000_000_000,
            fee_per_millionth: 10,
        }],
    );
    ev.discovery.neighbor_patron_source_channels = patron_channels;
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(pool_peer_ids(&plan).contains(&"s2_neighbor_only".to_string()));
}

#[test]
fn discovery_strategy_3_graph_reaches_candidate_pool() {
    let mut ev = base_evidence();
    let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    graph.insert(
        "s3_graph_only".to_string(),
        vec![
            GraphChannelEdge {
                active: true,
                amount_msat: 1_000_000_000
            };
            6
        ],
    );
    ev.discovery.graph_cached_source_channels = graph;
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(pool_peer_ids(&plan).contains(&"s3_graph_only".to_string()));
}

#[test]
fn discovery_strategy_4_route_pairs_reaches_candidate_pool() {
    let mut ev = base_evidence();
    ev.discovery.route_pair_rows = vec![RoutePairRow {
        in_channel: "700000x1x0".to_string(),
        out_channel: "700000x2x0".to_string(),
        total_fee_msat: 5_000_000,
    }];
    let mut channel_to_peer = BTreeMap::new();
    channel_to_peer.insert("700000x1x0".to_string(), "route_peerA".to_string());
    ev.discovery.channel_to_peer = channel_to_peer;
    let mut route_peer_channels: BTreeMap<String, Vec<NeighborEdge>> = BTreeMap::new();
    route_peer_channels.insert(
        "route_peerA".to_string(),
        vec![NeighborEdge {
            destination: "s4_route_pair_only".to_string(),
            amount_msat: 6_000_000_000,
            fee_per_millionth: 100,
        }],
    );
    ev.discovery.route_peer_source_channels = route_peer_channels;
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(pool_peer_ids(&plan).contains(&"s4_route_pair_only".to_string()));
}

#[test]
fn discovery_strategy_5_demand_flow_reaches_candidate_pool() {
    let mut ev = base_evidence();
    ev.discovery.demand_flows = vec![FlowContribution {
        peer_id: "sink_peer".to_string(),
        sats_in: 100_000,
        sats_out: 900_000,
    }];
    let mut sink_channels: BTreeMap<String, Vec<SinkChannelEdge>> = BTreeMap::new();
    sink_channels.insert(
        "sink_peer".to_string(),
        vec![SinkChannelEdge {
            destination: "s5_demand_flow_only".to_string(),
            active: true,
        }],
    );
    ev.discovery.demand_flow_sink_channels = sink_channels;
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(pool_peer_ids(&plan).contains(&"s5_demand_flow_only".to_string()));
}

// --- Tripwire: candidate enrichment (`score_candidate`) is actually applied

#[test]
fn candidate_enrichment_actually_changes_pool_score() {
    let mut ev = base_evidence();
    let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    graph.insert(
        "enriched_peer".to_string(),
        vec![
            GraphChannelEdge {
                active: true,
                amount_msat: 1_000_000_000
            };
            6
        ],
    );
    ev.discovery.graph_cached_source_channels = graph;

    let plan_unenriched = revops_capital::planner::cycle::plan_cycle(&ev);
    let raw_score = plan_unenriched
        .candidate_pool
        .iter()
        .find(|c| c.peer_id == "enriched_peer")
        .expect("candidate present")
        .score;

    let mut enrichment = BTreeMap::new();
    enrichment.insert(
        "enriched_peer".to_string(),
        revops_capital::planner::candidate_score::CandidateEnrichmentEvidence {
            reputation: Some(revops_capital::planner::candidate_score::PeerReputation {
                successes: 0,
                failures: 99,
            }),
            ..Default::default()
        },
    );
    ev.candidate_enrichment = enrichment;
    let plan_enriched = revops_capital::planner::cycle::plan_cycle(&ev);
    let enriched_score = plan_enriched
        .candidate_pool
        .iter()
        .find(|c| c.peer_id == "enriched_peer")
        .expect("candidate present")
        .score;

    assert!(
        enriched_score < raw_score,
        "poor-reputation enrichment must reduce the pool score: raw={raw_score} enriched={enriched_score}"
    );
}

// --- Fee gate governs whether discovery runs at all -------------------------

#[test]
fn fee_gate_closed_skips_discovery_entirely() {
    let mut ev = base_evidence();
    ev.fee_gate_ok = false;
    let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    graph.insert(
        "should_not_appear".to_string(),
        vec![
            GraphChannelEdge {
                active: true,
                amount_msat: 1_000_000_000
            };
            6
        ],
    );
    ev.discovery.graph_cached_source_channels = graph;
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(plan.candidate_pool.is_empty());
}

// --- Open sizing/EV evaluation is real: fail-closed on missing evidence ----

#[test]
fn candidate_without_open_ev_evidence_is_skipped_not_defaulted() {
    let mut ev = base_evidence();
    ev.available_sats = 5_000_000;
    ev.exploration_budget_sats = 100_000;
    let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    graph.insert(
        "no_evidence_peer".to_string(),
        vec![
            GraphChannelEdge {
                active: true,
                amount_msat: 1_000_000_000
            };
            6
        ],
    );
    ev.discovery.graph_cached_source_channels = graph;
    // No `open_candidate_evidence` entry supplied for "no_evidence_peer".
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(plan.opens.iter().all(|o| o.peer_id != "no_evidence_peer"));
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("no_evidence_peer") && r.contains("No sizing/EV evidence")));
}

#[test]
fn candidate_with_open_ev_evidence_can_be_opened() {
    let mut ev = base_evidence();
    ev.available_sats = 5_000_000;
    ev.exploration_budget_sats = 100_000;
    let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    graph.insert(
        "openable_peer".to_string(),
        vec![
            GraphChannelEdge {
                active: true,
                amount_msat: 1_000_000_000
            };
            6
        ],
    );
    ev.discovery.graph_cached_source_channels = graph;

    let mut open_evidence = BTreeMap::new();
    open_evidence.insert(
        "openable_peer".to_string(),
        revops_capital::planner::cycle::OpenCandidateEvidence {
            peer_dest_channel_capacities_sats: vec![],
            open_ev_template: OpenEvInputs {
                channel_size_sats: 0, // overwritten by plan_cycle's sizing pass
                closed_channel_daily_net_est_sats: None,
                observed_node_daily_ppm: Some(50.0),
                open_cost_sats: 1000,
                close_cost_sats: 1000,
                inbound_median_fee_ppm: None,
                min_annual_roi_pct: 1.0,
            },
            enrichment: Default::default(),
        },
    );
    ev.open_candidate_evidence = open_evidence;

    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.opens.iter().any(|o| o.peer_id == "openable_peer")
            || plan
                .evaluated_open_candidates
                .iter()
                .any(|c| c.peer_id == "openable_peer"),
        "candidate WITH evidence must at least be evaluated, unlike the no-evidence case"
    );
}

// --- recycle_candidates is real: reachable only through find_best_recycle_pair

#[test]
fn recycle_opportunity_composes_eligible_loser_and_candidate() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![LoserChannelEvidence {
        scid: "700000:1:0".to_string(),
        peer_id: "loser_peer".to_string(),
        capacity_sats: 2_000_000,
        roi_percent: -70.0,
        marginal_roi_percent: -60.0,
        marginal_profit_30d_sats: -1000,
        classification: "underwater".to_string(),
        days_open: 200,
        opener: "local".to_string(),
        flow: Some(revops_capital::planner::losers::LoserFlowEvidence {
            flow_ratio: 0.9,
            capacity: 2_000_000,
            daily_volume: 5000.0,
            kalman_regime_change: false,
        }),
        rebalance_success: None,
        diagnostic_attempt_count: 2,
        is_hard_bleeder: false,
        defib_policy_blocked: false,
        close_protection_reason: None,
        uptime_pct: None,
        estimated_closure_cost_sats: 3000,
        dead_capital: None,
    }];
    // Need a non-empty candidate pool for the recycle branch to even run
    // (py 750: `if candidates and losers`).
    let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    graph.insert(
        "graph_filler".to_string(),
        vec![
            GraphChannelEdge {
                active: true,
                amount_msat: 1_000_000_000
            };
            6
        ],
    );
    ev.discovery.graph_cached_source_channels = graph;

    // An empty (not `None`) protected-peers set means "policy source
    // resolved successfully, nobody is protected" — `None` fails closed
    // (see `ev::is_recycle_eligible`'s doc comment).
    ev.recycle_protected_peers = Some(std::collections::BTreeSet::new());
    ev.recycle_candidates = vec![RecycleCandidateOwned {
        peer_id: "recycle_target".to_string(),
        score: 1.0,
        open_ev_template: OpenEvInputs {
            channel_size_sats: 0,
            closed_channel_daily_net_est_sats: None,
            observed_node_daily_ppm: Some(200.0),
            open_cost_sats: 1000,
            close_cost_sats: 1000,
            inbound_median_fee_ppm: None,
            min_annual_roi_pct: 1.0,
        },
    }];
    ev.recycle_close_cost_sats = 1000;

    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    let opp = plan
        .recycle_opportunity
        .expect("expected a recycle opportunity to be found");
    assert_eq!(opp.loser_peer_id, "loser_peer");
    assert_eq!(opp.candidate_peer_id, "recycle_target");
}

// --- Task 47 correction round 1, finding 1: fail-open safety gates --------
//
// The approved whole-plugin invariant is "missing or stale evidence DENIES
// action". Below: a defibrillation-eligible loser, a close-eligible loser,
// and an open candidate with full sizing/EV evidence but NO defib/close/open
// GATE evidence supplied. Each must be denied with an actionable reason, not
// defaulted to "allowed" the way `unwrap_or_default()` previously behaved.

fn defib_eligible_loser(peer_id: &str) -> LoserChannelEvidence {
    LoserChannelEvidence {
        scid: "700000:1:0".to_string(),
        peer_id: peer_id.to_string(),
        capacity_sats: 500_000,
        roi_percent: -70.0,
        marginal_roi_percent: -60.0,
        marginal_profit_30d_sats: -200,
        classification: "zombie".to_string(),
        days_open: 100,
        opener: "local".to_string(),
        flow: Some(revops_capital::planner::losers::LoserFlowEvidence {
            flow_ratio: 0.0,
            capacity: 500_000,
            daily_volume: 1000.0,
            kalman_regime_change: false,
        }),
        rebalance_success: None,
        diagnostic_attempt_count: 0,
        is_hard_bleeder: false,
        defib_policy_blocked: false,
        close_protection_reason: None,
        uptime_pct: None,
        estimated_closure_cost_sats: 3000,
        dead_capital: None,
    }
}

fn close_eligible_loser(peer_id: &str) -> LoserChannelEvidence {
    let mut ev = defib_eligible_loser(peer_id);
    // diagnostic_attempt_count >= 2 (and no regime change) routes to CLOSE
    // instead of DEFIBRILLATE (see losers.rs's `action` selection).
    ev.diagnostic_attempt_count = 2;
    ev
}

fn open_ready_graph_evidence(peer_id: &str) -> BTreeMap<String, Vec<GraphChannelEdge>> {
    let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    graph.insert(
        peer_id.to_string(),
        vec![
            GraphChannelEdge {
                active: true,
                amount_msat: 1_000_000_000
            };
            6
        ],
    );
    graph
}

fn open_ready_evidence() -> revops_capital::planner::cycle::OpenCandidateEvidence {
    revops_capital::planner::cycle::OpenCandidateEvidence {
        peer_dest_channel_capacities_sats: vec![],
        open_ev_template: OpenEvInputs {
            channel_size_sats: 0,
            closed_channel_daily_net_est_sats: None,
            observed_node_daily_ppm: Some(200.0),
            open_cost_sats: 1000,
            close_cost_sats: 1000,
            inbound_median_fee_ppm: None,
            min_annual_roi_pct: 1.0,
        },
        enrichment: Default::default(),
    }
}

#[test]
fn defib_missing_gate_evidence_denies_and_reports_reason() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![defib_eligible_loser("defib_no_gate_ev")];
    // No `defib_gates` entry supplied for "defib_no_gate_ev".
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.defibrillations
            .iter()
            .all(|d| d.peer_id != "defib_no_gate_ev"),
        "missing gate evidence must NOT default to allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("defib_no_gate_ev") && r.to_lowercase().contains("missing")));
}

#[test]
fn close_missing_gate_evidence_denies_and_reports_reason() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![close_eligible_loser("close_no_gate_ev")];
    // No `close_gates` entry supplied for "close_no_gate_ev".
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.closes.iter().all(|c| c.peer_id != "close_no_gate_ev"),
        "missing gate evidence must NOT default to allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("close_no_gate_ev") && r.to_lowercase().contains("missing")));
}

#[test]
fn open_missing_guard_evidence_denies_and_reports_reason() {
    let mut ev = base_evidence();
    ev.available_sats = 5_000_000;
    ev.exploration_budget_sats = 100_000;
    ev.discovery.graph_cached_source_channels = open_ready_graph_evidence("open_no_guard_ev");
    let mut open_evidence = BTreeMap::new();
    open_evidence.insert("open_no_guard_ev".to_string(), open_ready_evidence());
    ev.open_candidate_evidence = open_evidence;
    // No `open_guards` entry supplied for "open_no_guard_ev".
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.opens.iter().all(|o| o.peer_id != "open_no_guard_ev"),
        "missing guard evidence must NOT default to allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("open_no_guard_ev") && r.to_lowercase().contains("missing")));
}

// --- Task 47 correction round 1, finding 1 (part 2): STALE gate evidence --
//
// An entry IS present but its `observed_at` is older than
// `GATE_EVIDENCE_MAX_AGE_SECS` relative to `CycleEvidence::now` — this must
// deny exactly like missing evidence, with an actionable "stale" reason.

const STALE_OFFSET_SECS: i64 = revops_capital::planner::cycle::GATE_EVIDENCE_MAX_AGE_SECS + 1;

#[test]
fn defib_stale_gate_evidence_denies_and_reports_reason() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![defib_eligible_loser("defib_stale_ev")];
    ev.defib_gates.insert(
        "defib_stale_ev".to_string(),
        revops_capital::planner::cycle::DefibGate {
            observed_at: ev.now - STALE_OFFSET_SECS,
            ..Default::default()
        },
    );
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.defibrillations
            .iter()
            .all(|d| d.peer_id != "defib_stale_ev"),
        "stale gate evidence must NOT be treated as allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("defib_stale_ev") && r.to_lowercase().contains("stale")));
}

#[test]
fn close_stale_gate_evidence_denies_and_reports_reason() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![close_eligible_loser("close_stale_ev")];
    ev.close_gates.insert(
        "close_stale_ev".to_string(),
        revops_capital::planner::cycle::CloseGate {
            observed_at: ev.now - STALE_OFFSET_SECS,
            ..Default::default()
        },
    );
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.closes.iter().all(|c| c.peer_id != "close_stale_ev"),
        "stale gate evidence must NOT be treated as allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("close_stale_ev") && r.to_lowercase().contains("stale")));
}

#[test]
fn open_stale_guard_evidence_denies_and_reports_reason() {
    let mut ev = base_evidence();
    ev.available_sats = 5_000_000;
    ev.exploration_budget_sats = 100_000;
    ev.discovery.graph_cached_source_channels = open_ready_graph_evidence("open_stale_ev");
    let mut open_evidence = BTreeMap::new();
    open_evidence.insert("open_stale_ev".to_string(), open_ready_evidence());
    ev.open_candidate_evidence = open_evidence;
    ev.open_guards.insert(
        "open_stale_ev".to_string(),
        revops_capital::planner::cycle::OpenGuard {
            observed_at: ev.now - STALE_OFFSET_SECS,
            blocked: None,
        },
    );
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.opens.iter().all(|o| o.peer_id != "open_stale_ev"),
        "stale guard evidence must NOT be treated as allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("open_stale_ev") && r.to_lowercase().contains("stale")));
}

/// Control: evidence observed exactly `GATE_EVIDENCE_MAX_AGE_SECS` ago
/// (the inclusive boundary) is still fresh, and evidence from 1s in the
/// FUTURE (negative age) is denied exactly like staleness — clock skew is
/// equally untrusted.
#[test]
fn close_gate_evidence_boundary_and_future_observed_at() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![close_eligible_loser("close_boundary_ev")];
    ev.close_gates.insert(
        "close_boundary_ev".to_string(),
        revops_capital::planner::cycle::CloseGate {
            observed_at: ev.now - revops_capital::planner::cycle::GATE_EVIDENCE_MAX_AGE_SECS,
            ..Default::default()
        },
    );
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert_eq!(
        plan.closes.len(),
        1,
        "evidence exactly at the max-age boundary must still be fresh"
    );

    let mut ev2 = base_evidence();
    ev2.loser_channels = vec![close_eligible_loser("close_future_ev")];
    ev2.close_gates.insert(
        "close_future_ev".to_string(),
        revops_capital::planner::cycle::CloseGate {
            observed_at: ev2.now + 1,
            ..Default::default()
        },
    );
    let plan2 = revops_capital::planner::cycle::plan_cycle(&ev2);
    assert!(
        plan2.closes.iter().all(|c| c.peer_id != "close_future_ev"),
        "evidence observed in the future (negative age / clock skew) must be denied"
    );
    assert!(plan2
        .skipped_reasons
        .iter()
        .any(|r| r.contains("close_future_ev") && r.to_lowercase().contains("stale")));
}

// --- Task 47 correction round 1, finding 4: tie ordering -------------------
//
// Two equal-scoring winner candidates whose discovery (input list) order
// differs from their peer-id sort order. Python's `_discover_peers` merges
// via a `dict` (insertion order preserved) then a STABLE score-sort — ties
// keep first-discovery order. A `BTreeMap`-keyed dedup instead reorders
// every candidate into peer-id sort order, even ones that never collided.

#[test]
fn discover_peers_preserves_first_discovery_order_for_equal_score_ties() {
    let mut ev = base_evidence();
    // Discovery (list) order: "zzz_peer" first, "aaa_peer" second — the
    // OPPOSITE of peer-id sort order ("aaa_peer" < "zzz_peer").
    ev.winner_channels = vec![
        WinnerCandidateEvidence {
            scid: "700000:1:0".to_string(),
            peer_id: "zzz_peer".to_string(),
            capacity_sats: 1_000_000,
            marginal_roi_percent: 45.0,
            flow: Some(revops_capital::planner::winners::WinnerFlowEvidence {
                daily_volume: 600_000.0,
                flow_ratio: 0.9,
                kalman_velocity: 0.0,
                is_congested: false,
            }),
            rebalance_success: None,
            sourced_fee_contribution_sats: 0,
            channel_role: None,
            dts_posterior_mean: None,
        },
        WinnerCandidateEvidence {
            scid: "700000:2:0".to_string(),
            peer_id: "aaa_peer".to_string(),
            capacity_sats: 1_000_000,
            marginal_roi_percent: 45.0,
            flow: Some(revops_capital::planner::winners::WinnerFlowEvidence {
                daily_volume: 600_000.0,
                flow_ratio: 0.9,
                kalman_velocity: 0.0,
                is_congested: false,
            }),
            rebalance_success: None,
            sourced_fee_contribution_sats: 0,
            channel_role: None,
            dts_posterior_mean: None,
        },
    ];
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    let ids = pool_peer_ids(&plan);
    let zzz_idx = ids
        .iter()
        .position(|p| p == "zzz_peer")
        .expect("zzz_peer present");
    let aaa_idx = ids
        .iter()
        .position(|p| p == "aaa_peer")
        .expect("aaa_peer present");
    assert!(
        zzz_idx < aaa_idx,
        "equal-score ties must preserve discovery order (zzz_peer first), not peer-id sort \
         order; got {ids:?}"
    );
}

// --- Task 47 correction round 1, finding 3: multi-open capital accounting -
//
// Two equal-score candidates ("candA", "candB"), `max_opens_per_cycle: 2`,
// `available_sats: 4_000_000`. Both candidates' sizing pool is [A, B] with
// equal score -> `roi_weight` 0.5 each. Sized against the ORIGINAL
// available balance both would get 2_000_000 sats (half of 4_000_000,
// capped at half-available). Python recomputes the SECOND accepted open's
// size/EV against the balance remaining AFTER the first open's debit
// (capacity_planner.py:687-693, 737-744): after candidate 1 is accepted at
// 2_000_000, the remaining balance is 2_000_000, so candidate 2's
// `roi_weight` (still 0.5, unaffected) applied to the smaller remaining
// balance sizes it at 1_000_000, not 2_000_000. A planner that reuses the
// initial `available_sats` for every accepted open would size (and EV-rank)
// both at 2_000_000 -- over-committing capital the first open already
// spent.
#[test]
fn multi_open_second_candidate_sized_against_remaining_capital_after_first_open() {
    let mut ev = base_evidence();
    ev.available_sats = 4_000_000;
    ev.max_opens_per_cycle = 2;
    ev.exploration_budget_sats = 1_000_000;
    ev.estimated_open_cost_sats = 1000;
    ev.max_channel_sats = 10_000_000;
    ev.min_channel_sats = 500_000;

    let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    for peer in ["candA", "candB"] {
        graph.insert(
            peer.to_string(),
            vec![
                GraphChannelEdge {
                    active: true,
                    amount_msat: 1_000_000_000
                };
                6
            ],
        );
    }
    ev.discovery.graph_cached_source_channels = graph;

    let mut open_evidence = BTreeMap::new();
    let mut open_guards = BTreeMap::new();
    for peer in ["candA", "candB"] {
        open_evidence.insert(
            peer.to_string(),
            revops_capital::planner::cycle::OpenCandidateEvidence {
                peer_dest_channel_capacities_sats: vec![],
                open_ev_template: OpenEvInputs {
                    channel_size_sats: 0,
                    closed_channel_daily_net_est_sats: None,
                    observed_node_daily_ppm: Some(200.0),
                    open_cost_sats: 1000,
                    close_cost_sats: 1000,
                    inbound_median_fee_ppm: None,
                    min_annual_roi_pct: 1.0,
                },
                enrichment: Default::default(),
            },
        );
        open_guards.insert(
            peer.to_string(),
            revops_capital::planner::cycle::OpenGuard {
                observed_at: ev.now,
                blocked: None,
            },
        );
    }
    ev.open_candidate_evidence = open_evidence;
    ev.open_guards = open_guards;

    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert_eq!(
        plan.opens.len(),
        2,
        "both equal-score, positive-EV candidates should be planned: {:?}",
        plan.opens
    );
    let mut amounts: Vec<i64> = plan.opens.iter().map(|o| o.amount_sats).collect();
    amounts.sort_unstable();
    assert_eq!(
        amounts,
        vec![1_000_000, 2_000_000],
        "the SECOND accepted open must be sized against the balance remaining after the \
         first open's debit (2_000_000 - the first open's 2_000_000 channel = 2_000_000 \
         remaining, halved again by the 0.5/0.5 roi_weight = 1_000_000), not the original \
         4_000_000 available balance reused for both: got {:?}",
        plan.opens
    );
}

// --- Task 47 correction round 1, finding 2: capital-efficiency-aware
// neighbor discovery is REACHABLE from the whole-cycle orchestration -------
//
// Revert tripwire (matching the existing discovery_strategy_N_* pattern):
// when `DiscoveryEvidence::neighbor_capital_efficiency` evidence is
// supplied, `discover_peers` must route through
// `discovery::discover_from_neighbors_capital_efficiency`, not silently
// fall back to the no-capital-efficiency path.
#[test]
fn discovery_strategy_2_capital_efficiency_branch_reaches_candidate_pool() {
    let mut ev = base_evidence();
    let mut patron_channels: BTreeMap<String, Vec<NeighborEdge>> = BTreeMap::new();
    patron_channels.insert(
        "patronCE".to_string(),
        vec![NeighborEdge {
            destination: "ce_neighbor_only".to_string(),
            amount_msat: 1_000_000_000,
            fee_per_millionth: 50,
        }],
    );
    ev.discovery.neighbor_patron_source_channels = patron_channels;
    ev.discovery.neighbor_capital_efficiency =
        Some(vec![revops_capital::planner::discovery::PatronPoolInput {
            peer_id: "patronCE".to_string(),
            efficiency_rank: Some(0.9),
            volume_routed_sats: 1000,
            marginal_roi_percent: 50.0,
        }]);
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        pool_peer_ids(&plan).contains(&"ce_neighbor_only".to_string()),
        "capital-efficiency-aware neighbor discovery must be reachable from plan_cycle when \
         evidence is supplied: pool={:?}",
        pool_peer_ids(&plan)
    );
}

/// Control: WITHOUT `neighbor_capital_efficiency` evidence, discovery uses
/// the plain fallback (`discover_from_neighbors`), matching Python's
/// `self._capital_efficiency is None` branch — proves the branch selection
/// is evidence-driven, not "capital-efficiency always wins".
#[test]
fn discovery_strategy_2_falls_back_without_capital_efficiency_evidence() {
    let mut ev = base_evidence();
    ev.discovery.all_channels = vec![PatronCandidate {
        peer_id: "patronFallback".to_string(),
        marginal_roi_percent: 90.0,
    }];
    let mut patron_channels: BTreeMap<String, Vec<NeighborEdge>> = BTreeMap::new();
    patron_channels.insert(
        "patronFallback".to_string(),
        vec![NeighborEdge {
            destination: "fallback_neighbor_only".to_string(),
            amount_msat: 1_000_000_000,
            fee_per_millionth: 10,
        }],
    );
    ev.discovery.neighbor_patron_source_channels = patron_channels;
    // ev.discovery.neighbor_capital_efficiency left at its default `None`.
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(pool_peer_ids(&plan).contains(&"fallback_neighbor_only".to_string()));
}

// --- Task 47 correction round 1, finding 4: Python-oracle fixture for the
// cross-strategy merge tie-order/dedup fix (`discover_peers`, py
// `_discover_peers` 2714-2755) ----------------------------------------------

fn winner_from_fixture(w: &Value) -> revops_capital::planner::winners::Winner {
    revops_capital::planner::winners::Winner {
        scid: w["scid"].as_str().unwrap_or("700000x1x0").to_string(),
        peer_id: w["peer_id"].as_str().unwrap().to_string(),
        roi: w["roi"].as_f64().unwrap(),
        flow_ratio: 0.0,
        turnover: 0.0,
        capacity: 0,
        rebal_difficulty: 0.0,
        velocity_urgency: false,
        congestion_urgent: false,
        sourced_fee_contribution_sats: 0,
        channel_role: None,
        dts_posterior_mean: None,
    }
}

#[test]
fn discover_peers_matches_python_oracle_for_tie_order_and_dedup() {
    let cases = fixture_scenarios("discover_peers");
    assert_eq!(cases.len(), 3, "expected 3 discover_peers scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let winners: Vec<revops_capital::planner::winners::Winner> = case["input"]["winners"]
            .as_array()
            .unwrap()
            .iter()
            .map(winner_from_fixture)
            .collect();

        let mut graph: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
        if let Some(cache) = case["input"]["graph_cache"].as_object() {
            for (node, chans) in cache {
                let edges = chans
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| GraphChannelEdge {
                        active: c["active"].as_bool().unwrap(),
                        amount_msat: c["amount_msat"].as_i64().unwrap(),
                    })
                    .collect();
                graph.insert(node.clone(), edges);
            }
        }

        let discovery_evidence = DiscoveryEvidence {
            our_node_id: case["input"]["our_node_id"]
                .as_str()
                .unwrap_or("us")
                .to_string(),
            graph_cached_source_channels: graph,
            max_candidate_pool: 32,
            ..Default::default()
        };

        let actual = revops_capital::planner::cycle::discover_peers(
            &winners,
            &discovery_evidence,
            &BTreeMap::new(),
        );
        let expected = case["output"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len(), "{name}: count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(
                a.peer_id,
                e["peer_id"].as_str().unwrap(),
                "{name}: peer_id order"
            );
            assert_eq!(a.source, e["source"].as_str().unwrap(), "{name}: source");
            assert!(
                (a.score - e["score"].as_f64().unwrap()).abs() < 1e-9,
                "{name}: score for {}",
                a.peer_id
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Correction round 2 (review of 95f80fe): unchecked `now - observed_at`
// panics in debug on extreme i64 evidence, and under wrapping arithmetic
// some pairs wrap INTO the accepted 0..=900 window — untrusted evidence
// becoming fresh evidence. Arithmetic failure must DENY, for all three
// action families, and the skip reason must not repeat the subtraction.
// ---------------------------------------------------------------------------

/// Debug-panic pair: `now - i64::MIN` overflows. Must deny, not panic.
#[test]
fn defib_extreme_min_observed_at_denies_without_panic() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![defib_eligible_loser("defib_i64min_ev")];
    ev.defib_gates.insert(
        "defib_i64min_ev".to_string(),
        revops_capital::planner::cycle::DefibGate {
            observed_at: i64::MIN,
            ..Default::default()
        },
    );
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.defibrillations
            .iter()
            .all(|d| d.peer_id != "defib_i64min_ev"),
        "malformed timestamp evidence must NOT be treated as allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("defib_i64min_ev")));
}

#[test]
fn close_extreme_min_observed_at_denies_without_panic() {
    let mut ev = base_evidence();
    ev.loser_channels = vec![close_eligible_loser("close_i64min_ev")];
    ev.close_gates.insert(
        "close_i64min_ev".to_string(),
        revops_capital::planner::cycle::CloseGate {
            observed_at: i64::MIN,
            ..Default::default()
        },
    );
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.closes.iter().all(|c| c.peer_id != "close_i64min_ev"),
        "malformed timestamp evidence must NOT be treated as allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("close_i64min_ev")));
}

#[test]
fn open_extreme_min_observed_at_denies_without_panic() {
    let mut ev = base_evidence();
    ev.available_sats = 5_000_000;
    ev.exploration_budget_sats = 100_000;
    ev.discovery.graph_cached_source_channels = open_ready_graph_evidence("open_i64min_ev");
    let mut open_evidence = BTreeMap::new();
    open_evidence.insert("open_i64min_ev".to_string(), open_ready_evidence());
    ev.open_candidate_evidence = open_evidence;
    ev.open_guards.insert(
        "open_i64min_ev".to_string(),
        revops_capital::planner::cycle::OpenGuard {
            observed_at: i64::MIN,
            blocked: None,
        },
    );
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert!(
        plan.opens.iter().all(|o| o.peer_id != "open_i64min_ev"),
        "malformed timestamp evidence must NOT be treated as allowed"
    );
    assert!(plan
        .skipped_reasons
        .iter()
        .any(|r| r.contains("open_i64min_ev")));
}
