//! Port of the EV/ROI-gate math from `CapacityPlanner` (py
//! `modules/capacity_planner.py`): `_calculate_open_ev` (2859-2928),
//! `_calculate_redeployment_ev` (2930-2966), `_calculate_recycle_ev`
//! (1985-2012), and `_is_recycle_eligible` (1949-1983).
//!
//! Every RPC-backed lookup Python performs inline (closed-channel profit
//! inheritance, the observed node ppm anchor, feerate-derived on-chain
//! costs, historical inbound fee data, current block height) is hoisted to
//! the caller as a typed input — see each function's doc comment for the
//! exact py call site it replaces.

use std::collections::BTreeSet;

/// py `NEW_PEER_DISCOUNT = 0.5` (capacity_planner.py 73).
pub const NEW_PEER_DISCOUNT: f64 = 0.5;
/// py `LEGACY_FORECAST_DAILY_PPM_CEILING = 45.0` (capacity_planner.py 69).
pub const LEGACY_FORECAST_DAILY_PPM_CEILING: f64 = 45.0;
/// py `ASSUMED_AVG_FEE_PPM = 150.0` (capacity_planner.py 78).
pub const ASSUMED_AVG_FEE_PPM: f64 = 150.0;
/// py `lifetime_days = 180` (capacity_planner.py 2916, 2951, and the
/// recycle EV's matching 180-day horizon, 2001-2006).
pub const EV_LIFETIME_DAYS: f64 = 180.0;
/// py `RECYCLE_MIN_EV_SATS = 5000` (capacity_planner.py 2014).
pub const RECYCLE_MIN_EV_SATS: f64 = 5000.0;

/// Every input `_calculate_open_ev` needs (py 2859-2928), gathered up
/// front:
/// - `closed_channel_daily_net_est_sats`: py
///   `self.profitability.database.get_peer_closed_channel_profit_summary(peer_id)`
///   `.get('daily_net_est_sats', 0)` (2872-2878); `None`/`<=0` both take the
///   observed-ppm-anchor fallback, matching Python's `if daily_revenue <= 0`.
/// - `observed_node_daily_ppm`: py `self._observed_node_daily_ppm()`
///   (2882), the cached median realized ppm/day across the fleet; `None`
///   when the node has no routing history yet (bootstrap).
/// - `open_cost_sats` / `close_cost_sats`: py `self._estimate_open_cost()`
///   / `self._estimate_close_cost()` (2894-2895) — see
///   [`super::close_fee::estimate_open_cost_sats`] /
///   [`super::close_fee::estimate_close_cost_sats`].
/// - `inbound_median_fee_ppm`: py
///   `self.profitability.database.get_historical_inbound_fee_ppm(peer_id)`
///   `.get('median_fee_ppm', 0)` (2902-2904); `None`/`<=0` keeps the
///   default 10%-of-revenue rebalance-cost estimate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpenEvInputs {
    pub channel_size_sats: i64,
    pub closed_channel_daily_net_est_sats: Option<f64>,
    pub observed_node_daily_ppm: Option<f64>,
    pub open_cost_sats: i64,
    pub close_cost_sats: i64,
    pub inbound_median_fee_ppm: Option<f64>,
    /// py `getattr(cfg, "planner_min_annual_roi_pct", 1.0)` (2921), already
    /// resolved to a plain `f64` (the Python type-guard exists for
    /// dynamically-typed config attribute access, not a business rule).
    pub min_annual_roi_pct: f64,
}

/// Port of `_calculate_open_ev` (py capacity_planner.py 2859-2928):
/// expected 180-day profit in sats from opening a channel of
/// `channel_size_sats` to a peer, given the evidence in `inputs`. Only
/// open when the result is `> 0`.
pub fn calculate_open_ev(inputs: &OpenEvInputs) -> f64 {
    let mut daily_revenue = match inputs.closed_channel_daily_net_est_sats {
        Some(v) if v > 0.0 => v,
        _ => 0.0,
    };

    if daily_revenue <= 0.0 {
        let forecast_daily_ppm = match inputs.observed_node_daily_ppm {
            Some(observed) => (observed * NEW_PEER_DISCOUNT).min(LEGACY_FORECAST_DAILY_PPM_CEILING),
            None => LEGACY_FORECAST_DAILY_PPM_CEILING,
        };
        daily_revenue = inputs.channel_size_sats as f64 * forecast_daily_ppm / 1_000_000.0;
    }

    let on_chain_cost = (inputs.open_cost_sats + inputs.close_cost_sats) as f64;

    let mut rebal_cost_per_day = daily_revenue * 0.1;
    if let Some(median_inbound_ppm) = inputs.inbound_median_fee_ppm {
        if median_inbound_ppm > 0.0 {
            // The volume estimate is derived from the SAME forecast as the
            // revenue side (audit: previously assumed a fixed turnover
            // regardless of the revenue model).
            let daily_volume_est = daily_revenue * 1_000_000.0 / ASSUMED_AVG_FEE_PPM;
            rebal_cost_per_day = (daily_volume_est * median_inbound_ppm) / 1_000_000.0;
        }
    }

    let expected_revenue = daily_revenue * EV_LIFETIME_DAYS;
    let expected_rebal_cost = rebal_cost_per_day * EV_LIFETIME_DAYS;

    let min_annual_roi_pct = inputs.min_annual_roi_pct.max(0.0);
    let capital_hurdle =
        inputs.channel_size_sats as f64 * (min_annual_roi_pct / 100.0) * (EV_LIFETIME_DAYS / 365.0);

    expected_revenue - on_chain_cost - expected_rebal_cost - capital_hurdle
}

/// One winner candidate with its ALREADY-COMPUTED open EV (py's inline
/// `self._calculate_open_ev(winner["peer_id"], loser_capacity, cfg)` call
/// per winner, 2958 — hoisted to the caller since each winner needs its
/// own [`OpenEvInputs`]; call [`calculate_open_ev`] once per winner before
/// calling this function).
#[derive(Debug, Clone, Copy)]
pub struct RedeploymentCandidate<'a> {
    pub peer_id: &'a str,
    pub open_ev: f64,
}

/// Port of `_calculate_redeployment_ev` (py capacity_planner.py 2930-2966):
/// EV of closing a loser and redeploying capital to the best winner.
/// Returns `(redeployment_ev, best_peer_id, best_ev)`.
///
/// Subtle but load-bearing (verified against real-Python fixtures): `best_ev`
/// starts at `0.0`, not negative infinity (py 2954-2955) — a winner is only
/// selected if its own open EV is POSITIVE. If every winner's EV is `<= 0`,
/// `best_peer` stays `None` and `best_ev` stays `0.0`, so the residual/
/// closure-cost terms alone can still make `redeployment_ev` positive
/// (favoring close even with no attractive redeployment target).
///
/// P5-002: `residual_value = marginal_30d * 6` with NO max/negation — a
/// bleeding loser (`marginal_30d < 0`) has a NEGATIVE residual, so
/// subtracting it ADDS the avoided ongoing loss (favors closure).
pub fn calculate_redeployment_ev<'a>(
    loser_marginal_profit_30d_sats: i64,
    closure_cost_sats: i64,
    winners: &[RedeploymentCandidate<'a>],
) -> (f64, Option<&'a str>, f64) {
    let residual_value = (loser_marginal_profit_30d_sats * 6) as f64;

    let mut best_ev = 0.0f64;
    let mut best_peer: Option<&'a str> = None;
    for winner in winners {
        if winner.open_ev > best_ev {
            best_ev = winner.open_ev;
            best_peer = Some(winner.peer_id);
        }
    }

    let redeployment_ev = best_ev - residual_value - closure_cost_sats as f64;
    (redeployment_ev, best_peer, best_ev)
}

/// Port of `_calculate_recycle_ev` (py capacity_planner.py 1985-2012): EV
/// of closing `loser` and opening to `candidate`. `candidate_open_ev` is
/// `_calculate_open_ev(candidate["peer_id"], loser_capacity, cfg)` (py
/// 2002) — hoisted to the caller, same reasoning as
/// [`RedeploymentCandidate`]. F5 (audit): `candidate_open_ev` already nets
/// the new channel's open AND close costs internally, so only the loser's
/// closure cost is subtracted here (not the candidate's open cost again).
pub fn calculate_recycle_ev(
    candidate_open_ev: f64,
    loser_marginal_profit_30d_sats: i64,
    close_cost_sats: i64,
) -> f64 {
    let residual_value = (loser_marginal_profit_30d_sats * 6) as f64; // 180 days, matching candidate_open_ev
    candidate_open_ev - residual_value - close_cost_sats as f64
}

/// Evidence for [`is_recycle_eligible`] (py `loser` dict fields read at
/// 1956-1971).
#[derive(Debug, Clone, Copy)]
pub struct RecycleEligibilityInput<'a> {
    pub scid: &'a str,
    pub peer_id: &'a str,
    /// Already a PERCENTAGE (e.g. `-5.0` == -5%), matching the `loser`
    /// dict's `"marginal_roi"` field (py `round(prof.marginal_roi_percent, 2)`
    /// at the loser-building call sites).
    pub marginal_roi_percent: f64,
    /// py `self.data_service.get_block_height()` /
    /// `self.plugin.rpc.getinfo().get("blockheight", 0)` (1962).
    pub current_block_height: i64,
}

/// Port of `_is_recycle_eligible` (py capacity_planner.py 1949-1983).
/// `protected_peers: None` mirrors Python's `protected_peers is None` —
/// "policy source failed" — which fails CLOSED (every peer is protected).
/// Returns `(eligible, reason)`.
pub fn is_recycle_eligible(
    input: &RecycleEligibilityInput,
    protected_peers: Option<&BTreeSet<String>>,
    route_pair_scids: &BTreeSet<String>,
) -> (bool, String) {
    // Age gate: >60 days, derived from the SCID's embedded open block
    // height (py 1960-1967): `open_block = int(scid.split("x")[0])`.
    let open_block: Option<i64> = input.scid.split('x').next().and_then(|s| s.parse().ok());
    let Some(open_block) = open_block else {
        return (false, "Cannot determine channel age".to_string());
    };
    let age_days = (input.current_block_height - open_block) as f64 * 10.0 / 1440.0;
    if age_days < 60.0 {
        return (false, format!("Channel age {age_days:.0}d < 60d minimum"));
    }

    if input.marginal_roi_percent >= 0.0 {
        return (
            false,
            format!("Marginal ROI {:.1}% >= 0", input.marginal_roi_percent),
        );
    }

    match protected_peers {
        None => {
            return (
                false,
                "Policy protection unknown (source failed)".to_string(),
            )
        }
        Some(set) => {
            if set.contains(input.peer_id) {
                return (false, "Policy-protected peer".to_string());
            }
        }
    }

    if route_pair_scids.contains(input.scid) {
        return (false, "On revenue route pair".to_string());
    }

    (true, "Eligible for recycling".to_string())
}
