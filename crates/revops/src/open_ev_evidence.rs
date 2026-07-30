//! Task 67c slice 3 — open-EV inputs, dual-fund and redeployment evidence.
//!
//! Feeds the FROZEN [`revops_capital::planner::ev::calculate_open_ev`]. Every
//! trap here biases the planner AGAINST opening, which is the dangerous
//! direction: the cycle still runs, still reports success, and simply never
//! opens a channel — indistinguishable from "nothing was worth doing".

use revops_capital::planner::ev::OpenEvInputs;
use serde_json::Value;

/// `ChainCostDefaults.CHANNEL_OPEN_COST_SATS` (py config.py 1445).
pub const LEGACY_OPEN_COST_SATS: i64 = 5_000;
/// `ChainCostDefaults.CHANNEL_CLOSE_COST_SATS` (py config.py 1446).
pub const LEGACY_CLOSE_COST_SATS: i64 = 3_000;

/// Funding tx size used to price an open (py 2976).
const OPEN_TX_VBYTES: f64 = 140.0;
/// Close tx size used to price a close (py 2995) — deliberately NOT the
/// open's 140.
const CLOSE_TX_VBYTES: f64 = 200.0;

/// One channel's realized-rate inputs, from the profitability assembler.
#[derive(Debug, Clone, Copy)]
pub struct ProfitabilitySample {
    pub capacity_sats: i64,
    pub days_open: i64,
    pub fees_earned_msat: i64,
}

/// Port of `_compute_observed_daily_ppm` (py 2832-2856): the MEDIAN realized
/// `(fees_earned/days_open)/capacity` across channels, in ppm/day.
///
/// `None` when no channel exposes usable data. That distinction carries the
/// whole weight of this function: the frozen kernel treats `None` as
/// "bootstrap, use the legacy ceiling" and would treat `Some(0.0)` as "this
/// node earns nothing", forecasting zero revenue for every candidate and
/// making every EV negative forever.
pub fn observed_node_daily_ppm(samples: &[ProfitabilitySample]) -> Option<f64> {
    let mut rates: Vec<f64> = samples
        .iter()
        // py 2852: a channel that cannot produce a rate is SKIPPED, not
        // counted as a zero-rate sample — skipping vs. zeroing moves the
        // median in opposite directions.
        .filter(|s| s.capacity_sats > 0 && s.days_open > 0 && s.fees_earned_msat >= 0)
        .map(|s| {
            let fees_sats = s.fees_earned_msat as f64 / 1_000.0;
            (fees_sats / s.days_open as f64) / s.capacity_sats as f64 * 1_000_000.0
        })
        .collect();
    if rates.is_empty() {
        return None;
    }
    rates.sort_by(|a, b| a.partial_cmp(b).expect("rates are finite"));
    let mid = rates.len() / 2;
    // Python's `statistics.median` averages the two middle values on an
    // even count (unlike the integer floor-division median used for
    // inbound fees — the two are genuinely different functions).
    Some(if rates.len().is_multiple_of(2) {
        (rates[mid - 1] + rates[mid]) / 2.0
    } else {
        rates[mid]
    })
}

/// The `feerates(style="perkb")` reply, or the reason it could not be read.
pub struct ChainCostSources {
    pub feerates: Result<Value, String>,
}

/// On-chain cost estimates for one open/close pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainCosts {
    pub open_cost_sats: i64,
    pub close_cost_sats: i64,
    /// True when either side fell back to a legacy default. Python's
    /// fallback is silent; making it visible costs nothing and keeps a
    /// stale-feerate cycle distinguishable from a priced one.
    pub used_fallback: bool,
}

fn perkb(feerates: &Value, key: &str) -> Option<f64> {
    feerates
        .get("perkb")
        .and_then(|p| p.get(key))
        .and_then(Value::as_f64)
        .filter(|v| *v > 0.0)
}

/// Port of `_estimate_open_cost` / `_estimate_close_cost` (py 2968-3006).
///
/// A feerate failure falls back to the legacy statics rather than refusing.
/// This is one of the port's few deliberate NON-refusals: Python catches the
/// exception, and refusing would abort the whole planner cycle over a
/// transient RPC hiccup.
pub fn chain_costs(sources: &ChainCostSources) -> ChainCosts {
    let Ok(feerates) = &sources.feerates else {
        return ChainCosts {
            open_cost_sats: LEGACY_OPEN_COST_SATS,
            close_cost_sats: LEGACY_CLOSE_COST_SATS,
            used_fallback: true,
        };
    };

    let mut used_fallback = false;

    let open_cost_sats = match perkb(feerates, "opening") {
        Some(rate) => (rate / 1_000.0 * OPEN_TX_VBYTES) as i64,
        None => {
            used_fallback = true;
            LEGACY_OPEN_COST_SATS
        }
    };

    // py E-4.6 (2983-2988): the planner executes MUTUAL closes, so
    // `mutual_close` is the target rate. `unilateral_close` is the
    // conservative fallback — it is the HIGHER commitment-tx rate, so it
    // overstates the cost and argues against opening. Reaching for the
    // OPENING rate here would resurrect the exact bug that audit fixed.
    let close_cost_sats =
        match perkb(feerates, "mutual_close").or_else(|| perkb(feerates, "unilateral_close")) {
            Some(rate) => (rate / 1_000.0 * CLOSE_TX_VBYTES) as i64,
            None => {
                used_fallback = true;
                LEGACY_CLOSE_COST_SATS
            }
        };

    ChainCosts {
        open_cost_sats,
        close_cost_sats,
        used_fallback,
    }
}

/// Assemble the frozen kernel's [`OpenEvInputs`] for one candidate.
///
/// `closed_channel_daily_net_est_sats` is the profit-inheritance signal
/// (py 2875): a peer we previously had a channel with carries its own
/// realized daily net, which overrides the node-wide forecast.
#[allow(clippy::too_many_arguments)]
pub fn open_ev_inputs(
    channel_size_sats: i64,
    closed_channel_daily_net_est_sats: Option<f64>,
    observed_node_daily_ppm: Option<f64>,
    costs: ChainCosts,
    inbound_median_fee_ppm: Option<f64>,
    min_annual_roi_pct: f64,
) -> OpenEvInputs {
    OpenEvInputs {
        channel_size_sats,
        closed_channel_daily_net_est_sats,
        observed_node_daily_ppm,
        open_cost_sats: costs.open_cost_sats,
        close_cost_sats: costs.close_cost_sats,
        inbound_median_fee_ppm,
        min_annual_roi_pct,
    }
}
