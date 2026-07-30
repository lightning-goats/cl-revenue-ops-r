//! Task 67b slice 5: the planner's input gatherer is fail-closed and
//! produces a real, actionable plan end-to-end through the frozen kernel.

use std::collections::HashMap;

use revops::capital_inputs::{gather_capital_inputs, CapitalInputRefusal, CapitalReadSources};
use revops_db::analytics::ChannelFlowStateRow;
use revops_db::queries::{PerChannelCosts, PerChannelRevenue};

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;

fn healthy() -> CapitalReadSources {
    let mut revenue = HashMap::new();
    revenue.insert(
        "700x1x0".to_string(),
        PerChannelRevenue {
            fees_earned_msat: 10_000_000,
            volume_routed_msat: 400_000_000,
            forward_count: 80,
            sourced_volume_msat: 0,
            sourced_fee_contribution_msat: 0,
            sourced_forward_count: 0,
        },
    );
    let mut revenue_30d = HashMap::new();
    revenue_30d.insert(
        "700x1x0".to_string(),
        PerChannelRevenue {
            fees_earned_msat: 6_000_000, // 6000 sats
            volume_routed_msat: 200_000_000,
            forward_count: 40,
            sourced_volume_msat: 0,
            sourced_fee_contribution_msat: 0,
            sourced_forward_count: 0,
        },
    );
    let mut costs = HashMap::new();
    costs.insert(
        "700x1x0".to_string(),
        PerChannelCosts {
            peer_id: "02aa".into(),
            open_cost_sats: 2_000,
            capacity_sats: 5_000_000,
            opened_at: NOW - 100 * DAY,
            rebalance_cost_sats: 500,
            rebalance_cost_30d_sats: 100,
        },
    );
    let mut flow = HashMap::new();
    flow.insert(
        "700x1x0".to_string(),
        ChannelFlowStateRow {
            scid: "700x1x0".into(),
            peer_id: "02aa".into(),
            flow_state: "source".into(),
            balance_position: "depleted".into(),
            flow_ratio: 0.9,
            velocity: 0.5,
            confidence: 0.9,
            kalman_flow_ratio: 0.0,
            kalman_velocity: 0.0,
            kalman_uncertainty: 0.0,
            kalman_regime_change: false,
            forward_count: 80,
            updated_at: NOW,
            boot_id: "boot-a".into(),
        },
    );
    CapitalReadSources {
        revenue_all_time: Ok(revenue),
        revenue_30d: Ok(revenue_30d),
        costs: Ok(costs),
        flow_states: Ok(flow),
        planner_actions: Ok(HashMap::new()),
        rebalance_modes: Ok(HashMap::new()),
        close_protected_peers: Ok(Vec::new()),
        openers: HashMap::new(),
        daily_volume_sats: HashMap::from([("700x1x0".to_string(), 4_000_000.0)]),
        rebalance_success: HashMap::new(),
        now: NOW,
    }
}

/// EVERY source refuses typed. This matters more than usual here: the
/// frozen kernel is TOTAL over empty candidate sets, so a silently-empty
/// input yields a confident "planned nothing" indistinguishable from a
/// healthy quiet cycle.
#[test]
fn every_source_refuses_typed_rather_than_planning_nothing() {
    type Breaker = Box<dyn Fn(&mut CapitalReadSources)>;
    let cases: Vec<(&str, Breaker)> = vec![
        (
            "capital_inputs_revenue_unavailable",
            Box::new(|s: &mut CapitalReadSources| s.revenue_all_time = Err("read failed".into())),
        ),
        (
            "capital_inputs_costs_unavailable",
            Box::new(|s: &mut CapitalReadSources| s.costs = Err("read failed".into())),
        ),
        (
            "capital_inputs_flow_states_unavailable",
            Box::new(|s: &mut CapitalReadSources| s.flow_states = Err("read failed".into())),
        ),
        (
            "capital_inputs_planner_actions_unavailable",
            Box::new(|s: &mut CapitalReadSources| s.planner_actions = Err("read failed".into())),
        ),
        (
            "capital_inputs_policy_unavailable",
            Box::new(|s: &mut CapitalReadSources| s.rebalance_modes = Err("read failed".into())),
        ),
    ];
    for (code, break_it) in cases {
        let mut s = healthy();
        break_it(&mut s);
        let err = gather_capital_inputs(s).expect_err("must refuse");
        assert_eq!(err.code(), code);
    }
    // The policy refusal also covers close protection.
    let mut s = healthy();
    s.close_protected_peers = Err("read failed".into());
    let err = gather_capital_inputs(s).expect_err("must refuse");
    assert!(matches!(err, CapitalInputRefusal::PolicyUnavailable(_)));
}

/// END TO END: healthy inputs produce winner evidence AND a gate for the
/// peer, which is the combination the kernel needs to act. This is the
/// whole point of task 67b -- before it, both were empty.
#[test]
fn healthy_inputs_produce_actionable_evidence() {
    let inputs = gather_capital_inputs(healthy()).expect("gathers");
    assert_eq!(inputs.winner_channels.len(), 1, "winner evidence built");
    assert_eq!(inputs.loser_channels.len(), 1, "loser evidence built");
    assert_eq!(inputs.profitability.len(), 1);
    // A gate exists for the channel's peer -- without one the kernel skips
    // the action as unevaluable, however good the candidate is.
    assert!(inputs.defib_gates.contains_key("02aa"));
    assert!(inputs.close_gates.contains_key("02aa"));
    assert!(inputs.open_guards.contains_key("02aa"));
    assert!(inputs.defib_gates["02aa"].observed_at == NOW);

    // And the frozen kernel really classifies it as a winner.
    let winners = revops_capital::planner::winners::identify_winners(&inputs.winner_channels);
    assert_eq!(winners.len(), 1, "{winners:?}");
    assert_eq!(winners[0].peer_id, "02aa");
}

/// Channels that cannot be evaluated are reported WITH reasons, merged
/// from both the profitability and candidate stages -- never dropped.
#[test]
fn unevaluable_channels_are_reported_with_reasons() {
    let mut s = healthy();
    // A channel with costs but no flow state, and one with no opened_at.
    if let Ok(costs) = s.costs.as_mut() {
        costs.insert(
            "800x1x0".to_string(),
            PerChannelCosts {
                peer_id: "02bb".into(),
                open_cost_sats: 1_000,
                capacity_sats: 1_000_000,
                opened_at: NOW - 10 * DAY,
                rebalance_cost_sats: 0,
                rebalance_cost_30d_sats: 0,
            },
        );
        costs.insert(
            "900x1x0".to_string(),
            PerChannelCosts {
                peer_id: "02cc".into(),
                open_cost_sats: 1_000,
                capacity_sats: 1_000_000,
                opened_at: 0, // missing
                rebalance_cost_sats: 0,
                rebalance_cost_30d_sats: 0,
            },
        );
    }
    let inputs = gather_capital_inputs(s).expect("gathers");
    let reasons: HashMap<&str, &str> = inputs
        .skipped
        .iter()
        .map(|(s, r)| (s.as_str(), r.as_str()))
        .collect();
    assert!(reasons["900x1x0"].contains("opened_at"), "{reasons:?}");
    assert!(reasons["800x1x0"].contains("flow state"), "{reasons:?}");
}
