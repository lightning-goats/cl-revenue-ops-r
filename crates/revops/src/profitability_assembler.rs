//! Task 67b slice 2: assemble `ChannelProfitability` per channel.
//!
//! The classifier and the profitability type are FROZEN
//! (`revops_analytics::profitability`). This module only assembles their
//! inputs from the Rust-owned reads, using Python's exact arithmetic
//! (modules/profitability_analyzer.py:795-880):
//!
//! - `net_profit = total_contribution - total_cost`, where contribution is
//!   direct + sourced (per-channel VALUATION legitimately counts both;
//!   only FLEET revenue must not — see `queries::PerChannelRevenue`).
//! - `roi = net_profit / total_cost` when a cost exists. With NO recorded
//!   cost Python does not divide-guard to zero: a channel earning with no
//!   cost gets a synthetic `1.0` ("free money"), and only a channel with
//!   no contribution at all falls back to return-on-capacity.
//! - Marginal ROI is the 30-day window over ONGOING rebalance cost with no
//!   sunk open cost. Conflating it with the all-time figure flips winners
//!   into losers.
//!
//! A missing `opened_at` REFUSES rather than defaulting `days_open` to 0 —
//! zero would make every staleness branch read as "too new to judge",
//! which is a silent misclassification rather than a visible gap.

use std::collections::HashMap;

use revops_analytics::profitability::{
    classify_channel, ChannelCosts, ChannelProfitability, ChannelRevenue, ClassifyEvidence,
    DiagStats,
};
use revops_db::queries::{PerChannelCosts, PerChannelRevenue};

/// One channel's assembled inputs.
#[derive(Debug, Clone)]
pub struct ChannelInput {
    pub scid: String,
    pub revenue_all_time: PerChannelRevenue,
    pub revenue_30d: PerChannelRevenue,
    pub costs: PerChannelCosts,
    /// `"local"` or `"remote"` (from the live channel snapshot).
    pub opener: String,
    pub last_routed: Option<i64>,
    pub diag_attempt_count: i64,
    pub diag_last_success_time: i64,
    pub posterior_variance: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfitabilityRefusal {
    OpenTimestampMissing(String),
}

impl ProfitabilityRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::OpenTimestampMissing(_) => "profitability_open_timestamp_missing",
        }
    }
}

fn to_revenue(scid: &str, r: &PerChannelRevenue) -> ChannelRevenue {
    ChannelRevenue {
        channel_id: scid.to_string(),
        fees_earned_msat: r.fees_earned_msat,
        volume_routed_msat: r.volume_routed_msat,
        forward_count: r.forward_count,
        sourced_volume_msat: r.sourced_volume_msat,
        sourced_fee_contribution_msat: r.sourced_fee_contribution_msat,
        sourced_forward_count: r.sourced_forward_count,
    }
}

/// Assemble one channel and run the frozen classifier.
pub fn assemble_channel_profitability(
    input: ChannelInput,
    now: i64,
) -> Result<ChannelProfitability, ProfitabilityRefusal> {
    if input.costs.opened_at <= 0 {
        return Err(ProfitabilityRefusal::OpenTimestampMissing(format!(
            "channel {} has no opened_at; days_open cannot be derived and defaulting it \
             to 0 would make every staleness branch read as 'too new to judge'",
            input.scid
        )));
    }

    let costs = ChannelCosts {
        channel_id: input.scid.clone(),
        peer_id: input.costs.peer_id.clone(),
        open_cost_sats: input.costs.open_cost_sats,
        rebalance_cost_sats: input.costs.rebalance_cost_sats,
        effective_rebalance_cost_sats: input.costs.rebalance_cost_sats,
    };
    let revenue = to_revenue(&input.scid, &input.revenue_all_time);
    let revenue_30d = to_revenue(&input.scid, &input.revenue_30d);

    let total_cost = costs.total_cost_sats();
    let total_contribution = revenue.total_contribution_sats();
    let net_profit_sats = total_contribution - total_cost;

    // py:803-816 -- NOT a divide guard.
    let roi = if total_cost > 0 {
        net_profit_sats as f64 / total_cost as f64
    } else if total_contribution > 0 {
        1.0
    } else {
        total_contribution as f64 / input.costs.capacity_sats.max(1) as f64
    };

    // py:818-824 -- physical throughput, not double-counted.
    let total_volume = revenue
        .volume_routed_sats()
        .max(revenue.sourced_volume_sats());
    let (cost_per_sat_routed, fee_per_sat_routed) = if total_volume > 0 {
        (
            total_cost as f64 / total_volume as f64,
            total_contribution as f64 / total_volume as f64,
        )
    } else {
        (0.0, 0.0)
    };

    let days_open = (now - input.costs.opened_at).div_euclid(86_400);
    let contribution_30d_msat = revenue_30d.total_contribution_msat();
    let marginal_profit_30d_sats =
        revenue_30d.total_contribution_sats() - input.costs.rebalance_cost_30d_sats;

    let diag = DiagStats {
        attempt_count: input.diag_attempt_count,
        last_success_time: input.diag_last_success_time,
    };
    let classification = classify_channel(
        roi,
        net_profit_sats,
        input.last_routed,
        days_open,
        revenue.total_forward_count(),
        &ClassifyEvidence {
            now,
            diag_stats: Some(&diag),
            posterior_variance: input.posterior_variance,
            contribution_30d_msat: Some(contribution_30d_msat),
        },
    );

    Ok(ChannelProfitability {
        channel_id: input.scid.clone(),
        peer_id: input.costs.peer_id.clone(),
        capacity_sats: input.costs.capacity_sats,
        costs,
        revenue,
        net_profit_sats,
        roi_percent: roi * 100.0,
        classification,
        cost_per_sat_routed,
        fee_per_sat_routed,
        days_open,
        last_routed: input.last_routed,
        marginal_profit_30d_sats,
        rebalance_cost_30d_sats: input.costs.rebalance_cost_30d_sats,
        opener: input.opener,
        contribution_30d_msat,
        fees_earned_30d_msat: revenue_30d.fees_earned_msat,
        sourced_fee_30d_msat: revenue_30d.sourced_fee_contribution_msat,
        forward_count_30d: revenue_30d.forward_count,
        sourced_forward_count_30d: revenue_30d.sourced_forward_count,
        window_30d_available: true,
    })
}

/// One fleet pass: assembled channels plus the ones skipped and WHY.
#[derive(Debug, Default)]
pub struct FleetProfitability {
    pub profitability: HashMap<String, ChannelProfitability>,
    /// (scid, reason) -- surfaced, never silently dropped.
    pub skipped: Vec<(String, String)>,
}

/// Assemble every channel that has costs. `openers` and `last_routed` come
/// from the live snapshot; absent entries fall back to Python's own
/// defaults (`"local"`, no routing time).
pub fn assemble_fleet(
    revenue_all_time: &HashMap<String, PerChannelRevenue>,
    revenue_30d: &HashMap<String, PerChannelRevenue>,
    costs: &HashMap<String, PerChannelCosts>,
    openers: &HashMap<String, String>,
    now: i64,
) -> FleetProfitability {
    let mut out = FleetProfitability::default();
    let mut scids: Vec<&String> = costs.keys().collect();
    scids.sort();
    for scid in scids {
        let input = ChannelInput {
            scid: scid.clone(),
            revenue_all_time: revenue_all_time.get(scid).cloned().unwrap_or_default(),
            revenue_30d: revenue_30d.get(scid).cloned().unwrap_or_default(),
            costs: costs.get(scid).cloned().unwrap_or_default(),
            opener: openers
                .get(scid)
                .cloned()
                .unwrap_or_else(|| "local".to_string()),
            last_routed: None,
            diag_attempt_count: 0,
            diag_last_success_time: 0,
            posterior_variance: None,
        };
        match assemble_channel_profitability(input, now) {
            Ok(p) => {
                out.profitability.insert(scid.clone(), p);
            }
            Err(refusal) => match refusal {
                ProfitabilityRefusal::OpenTimestampMissing(detail) => {
                    out.skipped.push((scid.clone(), detail));
                }
            },
        }
    }
    out
}
