//! Task 67b slice 3: project profitability + flow state into the frozen
//! winner/loser kernels, and close Task 62's eleven evidence gaps.

use std::collections::HashMap;

use revops::capital_candidates::{build_candidate_evidence, CandidateSources};
use revops_analytics::profitability::{
    ChannelCosts, ChannelProfitability, ChannelRevenue, ProfitabilityClass,
};
use revops_db::analytics::ChannelFlowStateRow;

const NOW: i64 = 1_800_000_000;

fn profitability(scid: &str, marginal_profit: i64, rebal_30d: i64) -> ChannelProfitability {
    ChannelProfitability {
        channel_id: scid.into(),
        peer_id: "02aa".into(),
        capacity_sats: 5_000_000,
        costs: ChannelCosts {
            channel_id: scid.into(),
            peer_id: "02aa".into(),
            open_cost_sats: 2_000,
            rebalance_cost_sats: 500,
            effective_rebalance_cost_sats: 500,
        },
        revenue: ChannelRevenue {
            channel_id: scid.into(),
            fees_earned_msat: 10_000_000,
            volume_routed_msat: 100_000_000,
            forward_count: 50,
            sourced_volume_msat: 0,
            sourced_fee_contribution_msat: 3_000_000,
            sourced_forward_count: 10,
        },
        net_profit_sats: 7_500,
        roi_percent: 300.0,
        classification: ProfitabilityClass::Profitable,
        cost_per_sat_routed: 0.0,
        fee_per_sat_routed: 0.0,
        days_open: 100,
        last_routed: Some(NOW - 3_600),
        marginal_profit_30d_sats: marginal_profit,
        rebalance_cost_30d_sats: rebal_30d,
        opener: "local".into(),
        contribution_30d_msat: 0,
        fees_earned_30d_msat: 0,
        sourced_fee_30d_msat: 0,
        forward_count_30d: 0,
        sourced_forward_count_30d: 0,
        window_30d_available: true,
    }
}

/// A high-turnover, high-marginal-ROI, strongly-directional channel is a
/// WINNER through the frozen kernel. The whole point of this task: the
/// planner must actually identify candidates.
#[test]
fn a_real_winner_is_identified_through_the_frozen_kernel() {
    let mut prof = HashMap::new();
    // marginal 600 over 100 cost -> 600% marginal ROI, well over the 20% bar.
    prof.insert("700x1x0".to_string(), profitability("700x1x0", 600, 100));

    let mut flow = HashMap::new();
    flow.insert(
        "700x1x0".to_string(),
        ChannelFlowStateRow {
            scid: "700x1x0".into(),
            peer_id: "02aa".into(),
            flow_state: "source".into(),
            balance_position: "depleted".into(),
            // turnover = daily_volume/capacity must exceed 0.5, and the
            // kernel needs |flow_ratio| > 0.8.
            flow_ratio: 0.9,
            velocity: 0.5,
            confidence: 0.9,
            forward_count: 50,
            updated_at: NOW,
            boot_id: "boot-a".into(),
        },
    );

    let ev = build_candidate_evidence(CandidateSources {
        profitability: &prof,
        flow_states: &flow,
        daily_volume_sats: &HashMap::from([("700x1x0".to_string(), 4_000_000.0)]),
        rebalance_success: &HashMap::new(),
        now: NOW,
    });

    let (winners, _) = ev.classify();
    assert_eq!(winners.len(), 1, "a real winner must be found: {winners:?}");
    let w = &winners[0];
    assert_eq!(w.scid, "700x1x0");
    assert!(w.turnover > 0.5, "turnover {}", w.turnover);
    assert!(w.roi > 20.0, "roi {}", w.roi);
}

/// A channel that fails the thresholds is NOT a winner -- the kernel is
/// really being consulted, not bypassed.
#[test]
fn a_marginal_channel_is_not_promoted_to_winner() {
    let mut prof = HashMap::new();
    // marginal 10 over 100 -> 10% marginal ROI, under the 20% bar.
    prof.insert("700x1x0".to_string(), profitability("700x1x0", 10, 100));
    let mut flow = HashMap::new();
    flow.insert(
        "700x1x0".to_string(),
        ChannelFlowStateRow {
            scid: "700x1x0".into(),
            peer_id: "02aa".into(),
            flow_state: "balanced".into(),
            balance_position: "healthy".into(),
            flow_ratio: 0.1,
            velocity: 0.0,
            confidence: 0.5,
            forward_count: 5,
            updated_at: NOW,
            boot_id: "boot-a".into(),
        },
    );
    let ev = build_candidate_evidence(CandidateSources {
        profitability: &prof,
        flow_states: &flow,
        daily_volume_sats: &HashMap::from([("700x1x0".to_string(), 100.0)]),
        rebalance_success: &HashMap::new(),
        now: NOW,
    });
    let (winners, _) = ev.classify();
    assert!(winners.is_empty(), "{winners:?}");
}

/// A channel with NO flow state is skipped WITH A REASON, mirroring the
/// kernel's own `if not flow_metrics: continue` -- but surfaced rather
/// than silently dropped, so an operator can tell "no flow data yet" from
/// "evaluated and rejected".
#[test]
fn channels_without_flow_state_are_skipped_with_a_reason() {
    let mut prof = HashMap::new();
    prof.insert("700x1x0".to_string(), profitability("700x1x0", 600, 100));
    let ev = build_candidate_evidence(CandidateSources {
        profitability: &prof,
        flow_states: &HashMap::new(),
        daily_volume_sats: &HashMap::new(),
        rebalance_success: &HashMap::new(),
        now: NOW,
    });
    let (winners, _) = ev.classify();
    assert!(winners.is_empty());
    assert!(
        ev.winner_evidence.is_empty(),
        "no evidence built for it either"
    );
    assert_eq!(ev.skipped.len(), 1);
    assert_eq!(ev.skipped[0].0, "700x1x0");
    assert!(ev.skipped[0].1.contains("flow"), "{:?}", ev.skipped);
}

/// An underwater, idle channel reaches the loser kernel.
#[test]
fn a_real_loser_is_identified() {
    let mut prof = HashMap::new();
    let mut p = profitability("800x1x0", -5_000, 6_000);
    p.roi_percent = -80.0;
    p.classification = ProfitabilityClass::Underwater;
    p.last_routed = Some(NOW - 120 * 86_400);
    prof.insert("800x1x0".to_string(), p);
    let mut flow = HashMap::new();
    flow.insert(
        "800x1x0".to_string(),
        ChannelFlowStateRow {
            scid: "800x1x0".into(),
            peer_id: "02bb".into(),
            flow_state: "dormant".into(),
            balance_position: "saturated".into(),
            flow_ratio: 0.0,
            velocity: 0.0,
            confidence: 0.1,
            forward_count: 0,
            updated_at: NOW,
            boot_id: "boot-a".into(),
        },
    );
    let ev = build_candidate_evidence(CandidateSources {
        profitability: &prof,
        flow_states: &flow,
        daily_volume_sats: &HashMap::from([("800x1x0".to_string(), 0.0)]),
        rebalance_success: &HashMap::new(),
        now: NOW,
    });
    let (_, losers) = ev.classify();
    assert!(
        !losers.is_empty(),
        "an underwater idle channel must be a loser"
    );
    assert_eq!(losers[0].scid, "800x1x0");
}
