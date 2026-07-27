//! Pure response builder for `revenue-boltz-budget`.
//!
//! Port of `revenue_boltz_budget` (cl-revenue-ops.py:7996-8001) ->
//! `BoltzCliManager.budget()` (`modules/boltz_manager.py:2458-2459`) ->
//! `get_budget_status()` (`modules/boltz_manager.py:1540-1579`). The
//! numeric core (`spent_24h_sats_estimate`/`remaining_24h_sats_estimate`/
//! `reserved_24h_sats_estimate` and their `boltz_*` component breakdowns)
//! is `revops_boltz::budget::budget_status`, fully ported and reused here
//! verbatim. `external_liquidity_costs` and `budget_info` are themselves
//! provider callbacks in Python (`_get_external_liquidity_costs`/
//! `_get_global_budget_limit`, boltz_manager.py:1039-1100) that resolve
//! through the SAME unified total-cost-budget machinery this batch ports
//! for `revenue-total-cost-budget` -- they are taken here as already
//! -resolved JSON blobs (the caller's job to assemble, mirroring Python's
//! `**{k: v for k, v in data.items() ...}` passthrough-with-extra-keys
//! shape, which a fixed Rust struct cannot represent losslessly).

use revops_boltz::budget::{budget_status, CostComponents, ExternalLiquidityCosts};
use serde_json::{json, Value};

/// Every already-fetched/-computed input `get_budget_status` needs.
pub struct BoltzBudgetInputs {
    /// `_get_global_budget_limit()["budget_sats"]` (py 1542), already
    /// resolved by the caller (unified-budget provider or config
    /// fallback).
    pub daily_budget_sats: i64,
    /// `get_boltz_cost_components(window_hours=24, ...)`'s aggregate
    /// counts (py 1544-1550), fully ported (`revops_boltz::budget::
    /// boltz_cost_components`) -- the caller computes this from the
    /// manual-only, journal-augmented swap list.
    pub local: CostComponents,
    /// `_get_external_liquidity_costs()`'s numeric core (py 1552-1555),
    /// used by `budget_status` for the aggregate math.
    pub external: ExternalLiquidityCosts,
    /// `_get_external_liquidity_costs()`'s FULL dict (py 1039-1068),
    /// passed straight through to the `external_liquidity_costs` key
    /// (includes `source` plus any provider-supplied extra keys Python
    /// splats in) -- must be numerically consistent with `external` above
    /// (the caller's responsibility; this builder does not cross-check).
    pub external_liquidity_costs: Value,
    /// `_get_global_budget_limit()`'s FULL dict (py 1070-1100), passed
    /// straight through to the `budget_info` key.
    pub budget_info: Value,
    /// `bool(self.cfg.enforce_budget)` (py 1576).
    pub enforce_budget: bool,
}

/// Port of `get_budget_status` (boltz_manager.py:1540-1579).
///
/// `counted_details` (py 1549, 1578: the raw per-swap detail list
/// `local.get("counted_details", [])[:20]`) has NO Rust equivalent --
/// `revops_boltz::budget::boltz_cost_components` returns aggregate counts
/// only, never the per-swap detail rows -- so it is always `null` here and
/// declared in `_phase1b_gaps`.
pub fn build_boltz_budget(i: &BoltzBudgetInputs) -> Value {
    let status = budget_status(i.daily_budget_sats, &i.local, &i.external);
    let budget_source = i.budget_info.get("source").cloned().unwrap_or(Value::Null);

    json!({
        "daily_budget_sats": status.daily_budget_sats,
        "spent_24h_sats_estimate": status.spent_24h_sats_estimate,
        "remaining_24h_sats_estimate": status.remaining_24h_sats_estimate,
        "reserved_24h_sats_estimate": status.reserved_24h_sats_estimate,
        "boltz_spent_24h_sats_estimate": status.boltz_spent_24h_sats_estimate,
        "boltz_remaining_24h_sats_estimate": status.boltz_remaining_24h_sats_estimate,
        "boltz_reserved_24h_sats_estimate": status.boltz_reserved_24h_sats_estimate,
        "external_liquidity_costs": i.external_liquidity_costs.clone(),
        "budget_source": budget_source,
        "budget_info": i.budget_info.clone(),
        "counted_swaps": i.local.counted_swaps,
        "skipped_without_timestamp": i.local.skipped_without_timestamp,
        "enforce_budget": i.enforce_budget,
        "window_seconds": 86400,
        "counted_details": Value::Null,
        "_phase1b_gaps": ["counted_details"],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> BoltzBudgetInputs {
        BoltzBudgetInputs {
            daily_budget_sats: 10_000,
            local: CostComponents {
                spent_24h_sats: 2_000,
                reserved_24h_sats: 500,
                reserved_swaps: 1,
                counted_swaps: 4,
                skipped_without_timestamp: 1,
            },
            external: ExternalLiquidityCosts {
                spent_24h_sats: 1_000,
                reserved_24h_sats: 200,
            },
            external_liquidity_costs: json!({"source": "non_rebalance_total_costs", "spent_24h_sats": 1000, "reserved_24h_sats": 200}),
            budget_info: json!({"source": "unified", "budget_sats": 10000}),
            enforce_budget: true,
        }
    }

    #[test]
    fn numeric_core_matches_ported_budget_status() {
        let v = build_boltz_budget(&base());
        assert_eq!(v["daily_budget_sats"], 10_000);
        assert_eq!(v["spent_24h_sats_estimate"], 3_000);
        assert_eq!(v["reserved_24h_sats_estimate"], 700);
        assert_eq!(v["remaining_24h_sats_estimate"], 6_300);
        assert_eq!(v["boltz_spent_24h_sats_estimate"], 2_000);
        assert_eq!(v["boltz_reserved_24h_sats_estimate"], 500);
    }

    #[test]
    fn budget_source_is_extracted_from_budget_info() {
        let v = build_boltz_budget(&base());
        assert_eq!(v["budget_source"], "unified");
        assert_eq!(v["budget_info"]["budget_sats"], 10000);
    }

    #[test]
    fn budget_source_is_null_when_budget_info_lacks_source() {
        let mut i = base();
        i.budget_info = json!({"budget_sats": 500});
        let v = build_boltz_budget(&i);
        assert_eq!(v["budget_source"], Value::Null);
    }

    #[test]
    fn counted_details_is_declared_gap_not_fabricated() {
        let v = build_boltz_budget(&base());
        assert_eq!(v["counted_details"], Value::Null);
        assert_eq!(v["_phase1b_gaps"], json!(["counted_details"]));
        assert_eq!(v["window_seconds"], 86400);
    }
}
