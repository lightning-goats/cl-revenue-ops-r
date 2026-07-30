//! Task 67b slice 3: project profitability + flow state into the FROZEN
//! winner/loser kernels.
//!
//! `identify_winners` / `identify_losers` are untouched. This module only
//! builds their evidence shapes from what the Rust side now owns:
//! `ChannelProfitability` (slice 2) and `rust_channel_flow_states`
//! (task 67).
//!
//! Both kernels skip a channel with no flow metrics (`if not
//! flow_metrics: continue`). That skip is preserved, but SURFACED with a
//! reason — an operator must be able to tell "no flow data yet" from
//! "evaluated and rejected", which a silent `continue` cannot express.

use std::collections::HashMap;

use revops_analytics::profitability::ChannelProfitability;
use revops_capital::planner::losers::{
    identify_losers, Loser, LoserChannelEvidence, LoserFlowEvidence,
};
use revops_capital::planner::winners::{
    identify_winners, RebalanceSuccessStats, Winner, WinnerCandidateEvidence, WinnerFlowEvidence,
};
use revops_db::analytics::ChannelFlowStateRow;

/// Everything the projection reads. Flow state and daily volume come from
/// task 67's owner; rebalance success from the rebalance ledger.
pub struct CandidateSources<'a> {
    pub profitability: &'a HashMap<String, ChannelProfitability>,
    pub flow_states: &'a HashMap<String, ChannelFlowStateRow>,
    /// py `flow_metrics.daily_volume`, in sats.
    pub daily_volume_sats: &'a HashMap<String, f64>,
    pub rebalance_success: &'a HashMap<String, RebalanceSuccessStats>,
    pub now: i64,
}

/// The kernels' INPUT evidence. `plan_cycle` runs `identify_winners` /
/// `identify_losers` itself (cycle.rs:528), so this module produces what
/// they consume rather than duplicating the classification.
#[derive(Debug, Default)]
pub struct CandidateEvidence {
    pub winner_evidence: Vec<WinnerCandidateEvidence>,
    pub loser_evidence: Vec<LoserChannelEvidence>,
    /// (scid, reason) for every channel the projection could not evaluate.
    pub skipped: Vec<(String, String)>,
}

impl CandidateEvidence {
    /// Run the frozen kernels over this evidence. Convenience for callers
    /// and tests that want the classified result directly; `plan_cycle`
    /// does the same thing internally.
    pub fn classify(&self) -> (Vec<Winner>, Vec<Loser>) {
        (
            identify_winners(&self.winner_evidence),
            identify_losers(&self.loser_evidence),
        )
    }
}

pub fn build_candidate_evidence(sources: CandidateSources<'_>) -> CandidateEvidence {
    let mut out = CandidateEvidence::default();
    let mut winner_inputs = Vec::new();
    let mut loser_inputs = Vec::new();

    let mut scids: Vec<&String> = sources.profitability.keys().collect();
    scids.sort();
    for scid in scids {
        let prof = &sources.profitability[scid];
        let Some(flow) = sources.flow_states.get(scid) else {
            out.skipped.push((
                scid.clone(),
                "no flow state persisted for this channel yet; the winner/loser kernels \
                 require flow metrics and skip such channels"
                    .to_string(),
            ));
            continue;
        };
        let daily_volume = sources.daily_volume_sats.get(scid).copied().unwrap_or(0.0);
        let rebalance_success = sources.rebalance_success.get(scid).copied();

        winner_inputs.push(WinnerCandidateEvidence {
            scid: scid.clone(),
            peer_id: prof.peer_id.clone(),
            capacity_sats: prof.capacity_sats,
            marginal_roi_percent: prof.marginal_roi_percent(),
            flow: Some(WinnerFlowEvidence {
                daily_volume,
                flow_ratio: flow.flow_ratio,
                kalman_velocity: flow.velocity,
                // The flow projection does not persist congestion; a
                // channel is not claimed congested without evidence.
                is_congested: false,
            }),
            rebalance_success,
            sourced_fee_contribution_sats: prof.revenue.sourced_fee_contribution_sats(),
            channel_role: None,
            dts_posterior_mean: None,
        });

        loser_inputs.push(LoserChannelEvidence {
            scid: scid.clone(),
            peer_id: prof.peer_id.clone(),
            capacity_sats: prof.capacity_sats,
            roi_percent: prof.roi_percent,
            marginal_roi_percent: prof.marginal_roi_percent(),
            marginal_profit_30d_sats: prof.marginal_profit_30d_sats,
            classification: prof.classification.as_value().to_string(),
            days_open: prof.days_open,
            opener: prof.opener.clone(),
            flow: Some(LoserFlowEvidence {
                flow_ratio: flow.flow_ratio,
                capacity: prof.capacity_sats,
                daily_volume,
                kalman_regime_change: false,
            }),
            rebalance_success,
            diagnostic_attempt_count: 0,
            is_hard_bleeder: false,
            defib_policy_blocked: false,
            close_protection_reason: None,
            uptime_pct: None,
            estimated_closure_cost_sats: 0,
            dead_capital: None,
        });
    }

    out.winner_evidence = winner_inputs;
    out.loser_evidence = loser_inputs;
    out
}
