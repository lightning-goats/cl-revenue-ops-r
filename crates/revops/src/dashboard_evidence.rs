//! C71-28: the four `_phase1b_gaps` the dashboard has carried since Phase
//! 1b, from their exact Python sources.
//!
//! - `financial_health.tlv_sats` -- py `get_tlv()` over `listfunds`
//!   (profitability_analyzer.py:1867).
//! - `financial_health.annualized_roc_pct` -- py
//!   `calculate_roc(window_days)` (:1817), over `listpeerchannels`
//!   capacity plus the P&L this port already reads.
//! - `warnings` / `bleeder_count` -- py `identify_bleeders(window_days)`
//!   (:1500) over `get_channel_full_pnl` (database.py:3252).
//!
//! **Why `warnings` was the dangerous one.** `tlv_sats: null` and
//! `bleeder_count: null` are visibly absent. `warnings: []` is not: an
//! empty array is a well-formed Python answer meaning "no channel is
//! bleeding", so a node quietly losing money on every channel reported
//! exactly what a healthy one reports. That is the only one of the four a
//! caller could act on while being wrong.
//!
//! **Disclosed divergence.** py's `get_tlv` catches `RpcError` and
//! "returns zeros" (:1904-1907), and `calculate_roc` logs and continues
//! with `total_capacity_sats = 0`. So an unreachable node reports a net
//! worth of 0 sats and a 0% return -- indistinguishable from a genuinely
//! empty node. This module refuses instead: TLV is the operator's net
//! worth, and zero is never a safe placeholder for "we could not look".

use std::collections::HashMap;

use revops_core::msat::parse_msat;
use revops_db::queries::{PerChannelCosts, PerChannelRevenue};
use serde_json::Value;

/// py `get_tlv`'s return shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Tlv {
    pub onchain_sats: i64,
    pub local_balance_sats: i64,
    pub remote_balance_sats: i64,
    pub tlv_sats: i64,
    pub channel_count: i64,
}

/// py `_scid_aliases`: the snapshot folds every key onto the `x` spelling,
/// so the live channel list has to be read the same way or a `:`-spelled
/// channel finds none of its own rows.
fn normalize_scid(scid: &str) -> String {
    scid.replace(':', "x")
}

/// py `base_to_sats_floor` -- a partial sat of BALANCE is not yours yet.
fn floor_sats(msat: i64) -> i64 {
    msat.div_euclid(1000)
}

/// py `base_to_sats_ceil` -- a partial sat of FEE or COST counts whole.
fn ceil_sats(msat: i64) -> i64 {
    msat.div_euclid(1000) + i64::from(msat.rem_euclid(1000) != 0)
}

/// py `base_delta_to_sats_toward_zero` -- a SIGNED delta rounds toward
/// zero, so a sub-sat loss is not yet a loss.
fn toward_zero_sats(msat: i64) -> i64 {
    msat / 1000
}

/// Total Liquidating Value: confirmed on-chain funds plus our side of every
/// live channel -- the node's net worth if it cooperatively closed today.
///
/// Refuses when `listfunds` carries neither array, rather than reporting
/// the zeros Python's exception handler produces.
pub fn total_liquidating_value(funds: &Value) -> Result<Tlv, String> {
    let outputs = funds.get("outputs").and_then(Value::as_array);
    let channels = funds.get("channels").and_then(Value::as_array);
    // Both absent means we are looking at something that is not a
    // listfunds reply at all (an error object, a null, a timeout's
    // remains). An EMPTY array is present, and is a real answer.
    if outputs.is_none() && channels.is_none() {
        return Err(
            "listfunds reply carries neither an outputs nor a channels array; \
             reporting a net worth of 0 sats would be indistinguishable from a \
             genuinely empty node"
                .to_string(),
        );
    }

    let mut tlv = Tlv::default();
    for output in outputs.into_iter().flatten() {
        if output.get("status").and_then(Value::as_str) != Some("confirmed") {
            continue;
        }
        tlv.onchain_sats += floor_sats(parse_msat(
            output.get("amount_msat").unwrap_or(&Value::Null),
        ));
    }
    for channel in channels.into_iter().flatten() {
        if channel.get("state").and_then(Value::as_str) != Some("CHANNELD_NORMAL") {
            continue;
        }
        let ours = parse_msat(channel.get("our_amount_msat").unwrap_or(&Value::Null));
        let total = parse_msat(channel.get("amount_msat").unwrap_or(&Value::Null));
        tlv.local_balance_sats += floor_sats(ours);
        tlv.remote_balance_sats += floor_sats(total - ours);
        tlv.channel_count += 1;
    }
    tlv.tlv_sats = tlv.onchain_sats + tlv.local_balance_sats;
    Ok(tlv)
}

/// py `_get_all_channels`'s capacity sum: `CHANNELD_NORMAL` channels with a
/// nameable SCID, `total_msat` floored, falling back to
/// `spendable + receivable` when that is zero.
pub fn total_capacity_sats(channels: &[Value]) -> i64 {
    let mut total = 0;
    for channel in channels {
        if channel.get("state").and_then(Value::as_str) != Some("CHANNELD_NORMAL") {
            continue;
        }
        // py skips a channel with no normalized SCID: it is not yet a
        // channel the analyzer can name, so it is not yet capacity it can
        // attribute.
        let named = channel
            .get("short_channel_id")
            .and_then(Value::as_str)
            .map(|scid| !scid.trim().is_empty())
            .unwrap_or(false);
        if !named {
            continue;
        }
        let capacity = floor_sats(parse_msat(
            channel.get("total_msat").unwrap_or(&Value::Null),
        ));
        total += if capacity == 0 {
            floor_sats(
                parse_msat(channel.get("spendable_msat").unwrap_or(&Value::Null))
                    + parse_msat(channel.get("receivable_msat").unwrap_or(&Value::Null)),
            )
        } else {
            capacity
        };
    }
    total
}

/// py `calculate_roc`: the window's return on deployed capital, annualized.
///
/// The zero-capacity branch is Python's explicit else, not a divide guard --
/// with nothing deployed there is no return to annualize. A LOSS still
/// reports negative; clamping it would hide exactly the state the dashboard
/// exists to surface.
pub fn annualized_roc_pct(net_profit_sats: i64, total_capacity_sats: i64, window_days: i64) -> f64 {
    // py `if window_days < 1: window_days = 1`, guarding the annualization.
    let window_days = window_days.max(1);
    if total_capacity_sats <= 0 {
        return 0.0;
    }
    let roc_pct = (net_profit_sats as f64 / total_capacity_sats as f64) * 100.0;
    revops_econ::pyfloat::py_round(roc_pct * (365.0 / window_days as f64), 2)
}

/// The bleeder half of the dashboard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BleederReport {
    /// Python's exact warning lines, worst bleeder first.
    pub warnings: Vec<String>,
    pub bleeder_count: usize,
}

/// py `identify_bleeders(window_days)`: every ACTIVE channel whose windowed
/// P&L is negative.
///
/// `revenue` and `costs` must both be the WINDOWED halves of one
/// profitability snapshot, so the numerator and denominator describe the
/// same instant and the same window.
pub fn bleeder_warnings(
    active_channels: &[Value],
    revenue: &HashMap<String, PerChannelRevenue>,
    costs: &HashMap<String, PerChannelCosts>,
) -> BleederReport {
    let mut bleeders: Vec<(i64, String)> = Vec::new();

    for channel in active_channels {
        if channel.get("state").and_then(Value::as_str) != Some("CHANNELD_NORMAL") {
            continue;
        }
        let Some(scid) = channel.get("short_channel_id").and_then(Value::as_str) else {
            continue;
        };
        let scid = normalize_scid(scid);

        let direct_msat = revenue.get(&scid).map(|r| r.fees_earned_msat).unwrap_or(0);
        let sourced_msat = revenue
            .get(&scid)
            .map(|r| r.sourced_fee_contribution_msat)
            .unwrap_or(0);
        // py `max(revenue_msat, sourced_fee_msat)` -- credit the channel
        // for its most valuable ROLE. NOT the per-channel valuation's
        // direct+sourced sum: summing would hide a bleeder whose two roles
        // each fall short of its rebalance spend.
        let contribution_msat = direct_msat.max(sourced_msat);
        let rebalance_msat = costs
            .get(&scid)
            .map(|c| c.rebalance_cost_30d_msat)
            .unwrap_or(0);

        let net_msat = contribution_msat - rebalance_msat;
        // Signed rounding TOWARD ZERO, then `< 0`. Flooring instead would
        // manufacture a warning for every channel with a sub-sat rounding
        // loss.
        let net_sats = toward_zero_sats(net_msat);
        if net_sats >= 0 {
            continue;
        }

        // py audit F7 removed the activity filter: a PURE bleeder -- paid
        // to fill, never routed -- is exactly the channel an operator must
        // see, so there is no forward-count gate here.
        //
        // `alias` is never set by `identify_bleeders`, so Python always
        // takes the no-alias branch. Emitting an alias form would be a
        // line no Python node ever produced.
        let spent = ceil_sats(rebalance_msat);
        let earned = ceil_sats(direct_msat);
        bleeders.push((
            net_sats,
            format!(
                "Channel {scid} is bleeding: Spent {spent} sats rebalancing, \
                 earned {earned} sats."
            ),
        ));
    }

    // py `bleeders.sort(key=lambda x: x['net_pnl_sats'])` -- worst first.
    // Ties broken by the message so the order is deterministic rather than
    // dependent on HashMap iteration.
    bleeders.sort();
    BleederReport {
        bleeder_count: bleeders.len(),
        warnings: bleeders.into_iter().map(|(_, line)| line).collect(),
    }
}
