//! Pure response builder for `revenue-r-history`. Fully DB-backed (no
//! `_phase1b_gaps`) -- see the plan's per-RPC gap table:
//! `profitability_analyzer.get_lifetime_report()` composes entirely from
//! `database.get_lifetime_stats()` and `.get_closed_channels_summary()`,
//! both plain SQL aggregates, no other module involved.

use revops_core::msat::{base_to_sats_ceil, py_round2};
use revops_db::queries::{ClosedChannelsSummary, LifetimeStats};
use serde_json::{json, Value};

/// Port of `ProfitabilityAnalyzer.get_lifetime_report`
/// (profitability_analyzer.py:1372-1430).
pub fn build_history(stats: &LifetimeStats, closed: &ClosedChannelsSummary) -> Value {
    // Ceiling matches every other revenue report in this module (Python's
    // own comment on this exact conversion), so sub-sat earnings stay
    // visible and lifetime/windowed figures agree.
    let lifetime_revenue_sats = base_to_sats_ceil(stats.total_revenue_msat.max(0) as u64) as i64;
    let lifetime_opening_costs_sats = stats.total_opening_cost_sats;
    let lifetime_closure_costs_sats = stats.total_closure_cost_sats;
    let lifetime_rebalance_costs_sats = stats.total_rebalance_cost_sats;
    let lifetime_total_costs_sats =
        lifetime_opening_costs_sats + lifetime_closure_costs_sats + lifetime_rebalance_costs_sats;
    let lifetime_net_profit_sats = lifetime_revenue_sats - lifetime_total_costs_sats;

    let lifetime_roi_percent = if lifetime_total_costs_sats > 0 {
        py_round2((lifetime_net_profit_sats as f64 / lifetime_total_costs_sats as f64) * 100.0)
    } else if lifetime_revenue_sats > 0 {
        100.0
    } else {
        0.0
    };

    json!({
        "lifetime_revenue_sats": lifetime_revenue_sats,
        "lifetime_opening_costs_sats": lifetime_opening_costs_sats,
        "lifetime_closure_costs_sats": lifetime_closure_costs_sats,
        "lifetime_rebalance_costs_sats": lifetime_rebalance_costs_sats,
        "lifetime_total_costs_sats": lifetime_total_costs_sats,
        "lifetime_net_profit_sats": lifetime_net_profit_sats,
        "lifetime_roi_percent": lifetime_roi_percent,
        "lifetime_forward_count": stats.total_forwards,
        "closed_channels_summary": {
            "channel_count": closed.channel_count,
            "total_capacity": closed.total_capacity,
            "total_open_costs": closed.total_open_costs,
            "total_closure_costs": closed.total_closure_costs,
            "total_revenue": closed.total_revenue,
            "total_rebalance_costs": closed.total_rebalance_costs,
            "total_forwards": closed.total_forwards,
            "total_net_pnl": closed.total_net_pnl,
            "avg_days_open": closed.avg_days_open,
        },
    })
}
