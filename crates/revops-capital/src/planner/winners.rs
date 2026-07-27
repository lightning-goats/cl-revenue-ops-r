//! Port of `_identify_winners` (py `modules/capacity_planner.py` 805-899):
//! classifies channels as capacity-constrained high performers, enriched
//! with signals downstream discovery/sizing consumes.

use super::pyround::py_round;

/// A channel's `get_channel_rebalance_success_rate(scid, 30)` result (py
/// 833, 836-839: only consulted when `total >= 3`).
#[derive(Debug, Clone, Copy)]
pub struct RebalanceSuccessStats {
    pub success_rate: f64,
    pub total: i64,
}

/// The flow-analyzer fields `_identify_winners` reads (py 820-882);
/// `None` (no flow metrics for this channel) mirrors py's `if not
/// flow_metrics: continue` — the whole channel is skipped.
#[derive(Debug, Clone, Copy)]
pub struct WinnerFlowEvidence {
    pub daily_volume: f64,
    pub flow_ratio: f64,
    pub kalman_velocity: f64,
    pub is_congested: bool,
}

/// Every input `_identify_winners` needs for one channel, evidence hoisted
/// per the module doc comment on [`super`]. `dts_posterior_mean` replaces
/// py 862-878's inline `get_fee_strategy_state` JSON parse (`v2_state_json`
/// -> `thompson_state.posterior_mean`, nested-first with flat fallback) —
/// the caller performs that DB-blob decode and passes the already-resolved
/// value (or `None` on any parse failure, matching py's blanket `except
/// Exception: pass`).
#[derive(Debug, Clone)]
pub struct WinnerCandidateEvidence {
    /// Raw scid (colon form); display form (`x` separator) is derived
    /// internally (py 825).
    pub scid: String,
    pub peer_id: String,
    pub capacity_sats: i64,
    pub marginal_roi_percent: f64,
    pub flow: Option<WinnerFlowEvidence>,
    pub rebalance_success: Option<RebalanceSuccessStats>,
    pub sourced_fee_contribution_sats: i64,
    pub channel_role: Option<String>,
    pub dts_posterior_mean: Option<f64>,
}

/// A classified winner (py's dict, 884-897).
#[derive(Debug, Clone, PartialEq)]
pub struct Winner {
    pub scid: String,
    pub peer_id: String,
    pub roi: f64,
    pub flow_ratio: f64,
    pub turnover: f64,
    pub capacity: i64,
    pub rebal_difficulty: f64,
    pub velocity_urgency: bool,
    pub congestion_urgent: bool,
    pub sourced_fee_contribution_sats: i64,
    pub channel_role: Option<String>,
    pub dts_posterior_mean: Option<f64>,
}

/// Port of `_identify_winners` (py 805-899).
pub fn identify_winners(channels: &[WinnerCandidateEvidence]) -> Vec<Winner> {
    let mut winners = Vec::new();

    for ch in channels {
        let Some(flow) = &ch.flow else { continue };

        let scid_display = ch.scid.replace(':', "x");
        let capacity = ch.capacity_sats.max(0);
        let turnover = if capacity > 0 {
            flow.daily_volume / capacity as f64
        } else {
            0.0
        };

        let (rebal_penalty, rebal_difficulty) = match &ch.rebalance_success {
            Some(s) if s.total >= 3 => {
                let sr = s.success_rate;
                let penalty = if sr < 0.5 { (0.5 - sr) * 50.0 } else { 0.0 };
                (penalty, py_round(1.0 - sr, 2))
            }
            _ => (0.0, 0.0),
        };

        let effective_roi = ch.marginal_roi_percent - rebal_penalty;

        let velocity_urgency = flow.kalman_velocity > 0.1;
        let congestion_urgent = flow.is_congested;

        if effective_roi > 20.0
            && turnover > 0.5
            && (flow.flow_ratio > 0.8 || flow.flow_ratio < -0.8)
        {
            winners.push(Winner {
                scid: scid_display,
                peer_id: ch.peer_id.clone(),
                roi: py_round(effective_roi, 2),
                flow_ratio: py_round(flow.flow_ratio, 4),
                turnover: py_round(turnover, 4),
                capacity: ch.capacity_sats,
                rebal_difficulty,
                velocity_urgency,
                congestion_urgent,
                sourced_fee_contribution_sats: ch.sourced_fee_contribution_sats,
                channel_role: ch.channel_role.clone(),
                dts_posterior_mean: ch.dts_posterior_mean.map(|m| py_round(m, 1)),
            });
        }
    }

    winners
}
