//! Port of `CapacityPlanner._check_portfolio_balance_gate` (py
//! `modules/capacity_planner.py` 321-361): the aggregate fleet
//! liquidity-balance gate that runs ahead of every discovery/execution
//! pass.

/// Fleet liquidity balance state (py docstring 324-328).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioBalanceState {
    /// `< 70%` local — all opens permitted.
    Healthy,
    /// `70-80%` local — opens permitted, caller should log a warning.
    Watch,
    /// `80-95%` local — only dual-funded or sink-targeting opens (F6:
    /// aligned with `receivable_ratio_floor` 0.20).
    Constrained,
    /// `>= 95%` local — only dual-funded or sink-targeting opens remain
    /// allowed (they are the REMEDY for being local-heavy, not the
    /// disease); candidate discovery still runs.
    Blocked,
}

impl PortfolioBalanceState {
    pub fn as_str(&self) -> &'static str {
        match self {
            PortfolioBalanceState::Healthy => "healthy",
            PortfolioBalanceState::Watch => "watch",
            PortfolioBalanceState::Constrained => "constrained",
            PortfolioBalanceState::Blocked => "blocked",
        }
    }
}

/// One `listpeerchannels` row's fields this gate reads (py 340-346:
/// `ch.get("state")`, `to_us_msat`, `total_msat`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelBalance {
    pub state: String,
    pub to_us_msat: i64,
    pub total_msat: i64,
}

/// Port of `_check_portfolio_balance_gate` (py 321-361). Only
/// `CHANNELD_NORMAL` channels count toward the local/total ratio (py 341-342).
pub fn check_portfolio_balance_gate(channels: &[ChannelBalance]) -> PortfolioBalanceState {
    let mut total_local: i64 = 0;
    let mut total_capacity: i64 = 0;
    for ch in channels {
        if ch.state != "CHANNELD_NORMAL" {
            continue;
        }
        total_local += ch.to_us_msat;
        total_capacity += ch.total_msat;
    }

    if total_capacity == 0 {
        return PortfolioBalanceState::Healthy;
    }

    let local_pct = (total_local as f64 / total_capacity as f64) * 100.0;

    if local_pct >= 95.0 {
        PortfolioBalanceState::Blocked
    } else if local_pct >= 80.0 {
        PortfolioBalanceState::Constrained
    } else if local_pct >= 70.0 {
        PortfolioBalanceState::Watch
    } else {
        PortfolioBalanceState::Healthy
    }
}
