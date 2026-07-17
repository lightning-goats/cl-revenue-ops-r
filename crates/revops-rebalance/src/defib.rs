//! Defibrillator diagnostic contracts (port of the module-head constants
//! and pure helpers of `modules/rebalancer.py`, `~/bin/cl_revenue_ops-port`,
//! branch `port`, v2.18.1 — THE production bugfix this port project started
//! from). The full shock sequence lives in
//! [`crate::facade::EvRebalancer::diagnostic_rebalance`]; this module owns
//! the pieces that are pure: the D4 fee envelope and the source-route
//! failure classifier that drives the ranked-source fallback loop.
//!
//! See the `modes::DIAGNOSTIC` spec-contradiction note — the diagnostic is
//! a BOUNDED spend (reserved on the unified rail, P4-020), not "no spend".
//!
//! Golden parity: `fixtures/rebalance/defib.json`
//! (`tools/port/gen_rebalance_fixtures.py defib`, port worktree branch
//! `phase5-t8-gen`).

use crate::errors::NO_ROUTE;

/// Operator ruling D4 (2026-07-01), `rebalancer.py:38`: static ceiling on
/// the diagnostic shock fee cap — whatever
/// `diagnostic_rebalance_max_fee_sats` is configured to, the effective
/// envelope never exceeds this (nor the daily budget); a typo cannot
/// authorize huge diagnostic spend.
pub const DIAGNOSTIC_FEE_CAP_CEILING_SATS: i64 = 10_000;

/// `rebalancer.py:44`: bound on how many shock sources the defibrillator
/// tries per run. Each failed pricing costs a getroutes call and a
/// `rebalance_history` row; a route-availability failure is
/// source-specific, so a small fallback list covers the
/// pathological-source case without spraying attempts.
pub const DIAGNOSTIC_MAX_SOURCE_ATTEMPTS: usize = 3;

/// `rebalancer.py:1771` (`shock_amount = 50_000`): small enough to be OpEx.
pub const SHOCK_AMOUNT_SATS: i64 = 50_000;

/// Substring markers of [`is_source_route_failure`]
/// (`rebalancer.py:1706-1712`). These are SUBSTRING matches on the whole
/// error text — deliberately NOT the `": "`-suffixed prefix constants in
/// [`crate::errors`] (`route_pricing_failed`/`native_route_invalid` match
/// with or without appended detail; [`NO_ROUTE`] is the bare contract
/// string and also matches inside e.g. `"no_routes"`, exactly as Python's
/// `in` does).
pub const SOURCE_ROUTE_FAILURE_MARKERS: [&str; 3] =
    ["route_pricing_failed", "native_route_invalid", NO_ROUTE];

/// Port of the D4 shock fee envelope (`rebalancer.py:1776-1787`):
/// `max_fee_sats = max(1, min(diagnostic_rebalance_max_fee_sats,
/// daily_budget_sats, DIAGNOSTIC_FEE_CAP_CEILING_SATS))`; the ppm ceiling
/// is DERIVED from the sat cap (`math.ceil(cap / 50_000 * 1e6)`) so the
/// sat cap is the single binding knob.
///
/// Python computes `int(getattr(cfg, 'daily_budget_sats', 0) or 0)` inside
/// the `min()`: a zero daily budget therefore clamps the whole envelope to
/// `max(1, 0) = 1` sat — pinned empirically by the fixture's
/// `daily_budget_sats=0` grid rows (the plan deliberately did not guess).
/// (`or 0` only coerces falsy `0` to `0`, so plain `i64::min` is exact.)
///
/// The ppm derivation goes through f64 exactly like Python
/// (`max_fee_sats / shock_amount * 1_000_000`, then `ceil`) — same IEEE-754
/// double ops, bit-identical results, fixture-pinned across the grid.
pub fn shock_fee_envelope(diag_max_fee_sats: i64, daily_budget_sats: i64) -> (i64, i64) {
    let max_fee_sats = diag_max_fee_sats
        .min(daily_budget_sats)
        .clamp(1, DIAGNOSTIC_FEE_CAP_CEILING_SATS);
    let max_fee_ppm = (max_fee_sats as f64 / SHOCK_AMOUNT_SATS as f64 * 1_000_000.0).ceil() as i64;
    (max_fee_sats, max_fee_ppm)
}

/// Port of `EVRebalancer._is_source_route_failure`
/// (`rebalancer.py:1700-1713`): true when an execution failure means no
/// route exists from the chosen source — pricing failed or the route never
/// validated, so no payment was attempted and retrying from another source
/// is safe. `None` maps to `""` (py `str(error or "")`), matching nothing.
pub fn is_source_route_failure(error: Option<&str>) -> bool {
    let text = error.unwrap_or("");
    SOURCE_ROUTE_FAILURE_MARKERS
        .iter()
        .any(|marker| text.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_default_config_is_400_sats_8000_ppm() {
        // Config defaults: diagnostic_rebalance_max_fee_sats=400,
        // daily_budget_sats=5000.
        assert_eq!(shock_fee_envelope(400, 5000), (400, 8000));
    }

    #[test]
    fn envelope_zero_daily_budget_clamps_to_one_sat() {
        assert_eq!(shock_fee_envelope(400, 0), (1, 20));
    }

    #[test]
    fn envelope_ceiling_binds_over_typo_config() {
        assert_eq!(
            shock_fee_envelope(1_000_000_000, 1_000_000),
            (DIAGNOSTIC_FEE_CAP_CEILING_SATS, 200_000)
        );
    }

    #[test]
    fn source_route_failure_matches_substrings_only() {
        assert!(is_source_route_failure(Some(
            "route_pricing_failed: x (market)"
        )));
        assert!(is_source_route_failure(Some(
            "native_route_invalid: first_hop_mismatch"
        )));
        assert!(is_source_route_failure(Some("no_route")));
        assert!(is_source_route_failure(Some("no_routes"))); // substring semantics
        assert!(!is_source_route_failure(Some("no route"))); // space, not underscore
        assert!(!is_source_route_failure(Some(
            "native_route_over_budget: 5 > 2"
        )));
        assert!(!is_source_route_failure(Some("local_budget_block")));
        assert!(!is_source_route_failure(None));
        assert!(!is_source_route_failure(Some("")));
    }
}
