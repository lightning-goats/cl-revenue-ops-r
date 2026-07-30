//! Task 71 slice A — the P&L / ROC / TLV evidence producers.
//!
//! Ports `get_pnl_summary` (py profitability_analyzer.py 1441-1499),
//! `calculate_roc` (1817-1866) and `get_tlv` (1867-1915). These three feed
//! profitability, econ-snapshot and the dashboard, so task 66 can wire
//! canonical responses against real evidence instead of `not_yet_ported`.
//!
//! **Disclosed divergence:** Python's `get_tlv` catches an RPC failure,
//! logs a warning, and returns ZEROS. This module refuses instead. A
//! zeroed TLV is indistinguishable from a node that genuinely holds
//! nothing, and TLV is the headline net-worth figure — reporting zero net
//! worth because an RPC timed out is a false statement about the
//! operator's money. Task 71 forbids fabricated zeros explicitly.

use revops_core::msat::{base_to_sats_ceil, base_to_sats_floor, parse_msat};
use revops_econ::pyfloat::py_round;
use serde_json::Value;

/// Revenue rounds UP at the msat boundary (`base_to_sats_ceil`), balances
/// round DOWN (`base_to_sats_floor`). Both come from `revops-core`, whose
/// rounding is fixture-verified against the real Python functions -- a
/// second local implementation would be a second thing to keep in parity.
/// The `u64` helpers are guarded here because these inputs can be negative
/// (a negative revenue sum, or a channel whose remote share computes below
/// zero on malformed evidence).
fn msat_to_sats_ceil(msat: i64) -> i64 {
    if msat <= 0 {
        return 0;
    }
    base_to_sats_ceil(msat as u64) as i64
}

fn msat_to_sats_floor(msat: i64) -> i64 {
    if msat <= 0 {
        return 0;
    }
    base_to_sats_floor(msat as u64) as i64
}

fn field_msat(v: Option<&Value>) -> i64 {
    v.map(parse_msat).unwrap_or(0)
}

/// Already-read P&L inputs, one window.
pub struct PnlSources {
    pub window_days: i64,
    /// `get_total_routing_revenue` returns MSAT; the conversion happens
    /// here, at the boundary, exactly as Python does.
    pub gross_revenue_msat: i64,
    pub rebalance_cost_sats: i64,
    pub closure_cost_sats: i64,
    pub volume_sats: i64,
    pub forward_count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PnlSummary {
    pub window_days: i64,
    pub gross_revenue_sats: i64,
    pub opex_sats: i64,
    pub rebalance_cost_sats: i64,
    pub closure_cost_sats: i64,
    pub net_profit_sats: i64,
    pub operating_margin_pct: f64,
    pub volume_sats: i64,
    pub forward_count: i64,
}

/// Port of `get_pnl_summary` (py 1441-1499).
pub fn pnl_summary(sources: PnlSources) -> PnlSummary {
    // py 1458: clamped, not rejected.
    let window_days = sources.window_days.max(1);

    let gross_revenue_sats = msat_to_sats_ceil(sources.gross_revenue_msat);
    let opex_sats = sources.rebalance_cost_sats + sources.closure_cost_sats;
    let net_profit_sats = gross_revenue_sats - opex_sats;

    // py 1479-1485. Zero revenue does NOT mean zero margin: an idle node
    // (no revenue, no costs) reads 0%, while one burning sats on
    // rebalances reads -100%. Collapsing both to 0.0 would make a bleeding
    // node look merely idle on the dashboard.
    let operating_margin_pct = if gross_revenue_sats > 0 {
        py_round(
            (net_profit_sats as f64 / gross_revenue_sats as f64) * 100.0,
            2,
        )
    } else if opex_sats == 0 {
        0.0
    } else {
        -100.0
    };

    PnlSummary {
        window_days,
        gross_revenue_sats,
        opex_sats,
        rebalance_cost_sats: sources.rebalance_cost_sats,
        closure_cost_sats: sources.closure_cost_sats,
        net_profit_sats,
        operating_margin_pct,
        volume_sats: sources.volume_sats,
        forward_count: sources.forward_count,
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RocSummary {
    pub window_days: i64,
    pub total_capacity_sats: i64,
    pub net_profit_sats: i64,
    pub roc_pct: f64,
    pub annualized_roc_pct: f64,
}

/// Port of `calculate_roc` (py 1817-1866): return on deployed capital,
/// annualized.
pub fn calculate_roc(pnl: PnlSummary, total_capacity_sats: i64, window_days: i64) -> RocSummary {
    // py 1830: clamped BEFORE annualizing, which is what prevents the
    // division by zero.
    let window_days = window_days.max(1);

    // Zero capacity is a genuine 0.0 -- a node with no channels really has
    // no return on deployed capital. This zero is measured, unlike TLV's
    // refusal above.
    let (roc_pct, annualized_roc_pct) = if total_capacity_sats > 0 {
        let roc = (pnl.net_profit_sats as f64 / total_capacity_sats as f64) * 100.0;
        (roc, roc * (365.0 / window_days as f64))
    } else {
        (0.0, 0.0)
    };

    RocSummary {
        window_days,
        total_capacity_sats,
        net_profit_sats: pnl.net_profit_sats,
        roc_pct: py_round(roc_pct, 4),
        annualized_roc_pct: py_round(annualized_roc_pct, 2),
    }
}

/// The `listfunds` reply, or the reason it could not be read.
pub struct TlvSources {
    pub listfunds: Result<Value, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlvSummary {
    pub onchain_sats: i64,
    pub local_balance_sats: i64,
    pub remote_balance_sats: i64,
    pub tlv_sats: i64,
    pub channel_count: i64,
}

/// Typed econ refusals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EconRefusal {
    ListfundsUnavailable(String),
}

impl EconRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ListfundsUnavailable(_) => "econ_listfunds_unavailable",
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Self::ListfundsUnavailable(d) => d,
        }
    }
}

/// Port of `get_tlv` (py 1867-1915) — the node's net worth if every
/// channel were cooperatively closed today.
///
/// Refuses on a failed read rather than returning Python's zeros; see the
/// module doc.
pub fn total_liquidating_value(sources: TlvSources) -> Result<TlvSummary, EconRefusal> {
    let funds = sources
        .listfunds
        .map_err(EconRefusal::ListfundsUnavailable)?;

    // Only CONFIRMED outputs: unconfirmed money is not yet ours to count.
    let onchain_sats = funds
        .get("outputs")
        .and_then(Value::as_array)
        .map(|outputs| {
            outputs
                .iter()
                .filter(|o| o.get("status").and_then(Value::as_str) == Some("confirmed"))
                .map(|o| msat_to_sats_floor(field_msat(o.get("amount_msat"))))
                .sum()
        })
        .unwrap_or(0);

    let mut local_balance_sats = 0i64;
    let mut remote_balance_sats = 0i64;
    let mut channel_count = 0i64;

    if let Some(channels) = funds.get("channels").and_then(Value::as_array) {
        for channel in channels {
            if channel.get("state").and_then(Value::as_str) != Some("CHANNELD_NORMAL") {
                continue;
            }
            let ours = field_msat(channel.get("our_amount_msat"));
            let total = field_msat(channel.get("amount_msat"));
            local_balance_sats += msat_to_sats_floor(ours);
            remote_balance_sats += msat_to_sats_floor(total - ours);
            channel_count += 1;
        }
    }

    Ok(TlvSummary {
        onchain_sats,
        local_balance_sats,
        remote_balance_sats,
        // onchain + LOCAL only. Remote balance is the counterparties'
        // money; including it would overstate net worth.
        tlv_sats: onchain_sats + local_balance_sats,
        channel_count,
    })
}
