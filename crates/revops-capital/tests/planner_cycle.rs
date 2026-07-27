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
use std::collections::BTreeMap;

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
    let plan = revops_capital::planner::cycle::plan_cycle(&ev);
    assert_eq!(plan.loser_count, 1);
    assert_eq!(plan.losers[0].peer_id, "loser_peer");
    assert!(plan.loser_close_scids.contains("700000x1x0"));
    // The classified CLOSE loser reaches the close-selection step (no
    // close_gates entry -> allowed by default).
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
