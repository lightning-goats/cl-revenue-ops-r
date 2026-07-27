//! Pure response builder for `revenue-r-health`.
//!
//! Port of `revenue_health` (cl-revenue-ops.py:6181-6357). Section 1
//! (`financials.today`/`.week`) is DB-backed via
//! `profitability_analyzer.get_pnl_summary` (ported as
//! `revops_db::queries::pnl_summary`, already used by `rpc_dashboard`) and
//! is fully wired here. Section 1's `annualized_roc_pct`
//! (`calculate_roc`, profitability_analyzer.py:1817-1861) needs total
//! fleet channel capacity from a live `listpeerchannels` RPC call this
//! DB-only builder cannot perform itself -- the caller MAY supply an
//! already-fetched `total_capacity_sats`, else it is `null` and
//! gap-listed, exactly like `rpc_dashboard`'s `tlv_sats`.
//!
//! Sections 2-9 (channel classifications, fee-controller convergence,
//! rebalancer state, unified budget, Boltz auto-cycle, capacity planner,
//! top routing-fee route pairs, daemon-loop heartbeat liveness) all read
//! LIVE IN-PROCESS PYTHON STATE with no Rust-side equivalent running in
//! this plugin yet: no fee controller / rebalancer / Boltz manager /
//! capacity planner daemon loop executes here, and the `_loop_heartbeats`
//! registry (cl-revenue-ops.py:322-323) is a Python-only in-memory
//! structure this plugin has no counterpart for. Each is therefore
//! unconditionally `null` and gap-listed -- not a fabricated zero/empty
//! value.

use revops_db::queries::PnlSummary;
use revops_econ::pyfloat::py_round;
use serde_json::{json, Value};

/// Port of `ChannelProfitabilityAnalyzer.calculate_roc`'s annualization
/// step (profitability_analyzer.py:1846-1855), given an ALREADY-FETCHED
/// total fleet capacity (the live `listpeerchannels` sum this DB-only
/// builder cannot compute itself). Mirrors the exact zero-capacity
/// fallback (`0.0`, never a division).
fn annualized_roc_pct(net_profit_sats: i64, total_capacity_sats: i64, window_days: i64) -> f64 {
    if total_capacity_sats <= 0 {
        return 0.0;
    }
    let roc_pct = (net_profit_sats as f64 / total_capacity_sats as f64) * 100.0;
    let annualized = roc_pct * (365.0 / window_days.max(1) as f64);
    py_round(annualized, 2)
}

/// Port of `revenue_health`. `pnl_1d`/`pnl_7d` are
/// `profitability_analyzer.get_pnl_summary(1)`/`(7)`'s already-fetched
/// results; `total_capacity_sats` is the optional already-fetched live
/// capacity sum for `annualized_roc_pct` (see module doc comment).
pub fn build_health(
    generated_at: i64,
    pnl_1d: Option<&PnlSummary>,
    pnl_7d: Option<&PnlSummary>,
    total_capacity_sats: Option<i64>,
) -> Value {
    let mut gaps: Vec<&'static str> = Vec::new();

    let financials = match (pnl_1d, pnl_7d) {
        (Some(d1), Some(d7)) => {
            let roc = match total_capacity_sats {
                Some(cap) => json!(annualized_roc_pct(d7.net_profit_sats, cap, d7.window_days)),
                None => {
                    gaps.push("financials.week.annualized_roc_pct");
                    Value::Null
                }
            };
            json!({
                "today": {
                    "revenue_sats": d1.gross_revenue_sats,
                    "costs_sats": d1.opex_sats,
                    "net_profit_sats": d1.net_profit_sats,
                    "forward_count": d1.forward_count,
                    "volume_sats": d1.volume_sats,
                },
                "week": {
                    "revenue_sats": d7.gross_revenue_sats,
                    "costs_sats": d7.opex_sats,
                    "net_profit_sats": d7.net_profit_sats,
                    "forward_count": d7.forward_count,
                    "operating_margin_pct": py_round(d7.operating_margin_pct, 1),
                    "annualized_roc_pct": roc,
                },
            })
        }
        _ => {
            gaps.push("financials");
            Value::Null
        }
    };

    for g in [
        "channels",
        "fees",
        "rebalancer",
        "budget",
        "boltz",
        "planner",
        "top_routes",
        "loops",
    ] {
        gaps.push(g);
    }

    json!({
        "generated_at": generated_at,
        "financials": financials,
        "channels": Value::Null,
        "fees": Value::Null,
        "rebalancer": Value::Null,
        "budget": Value::Null,
        "boltz": Value::Null,
        "planner": Value::Null,
        "top_routes": Value::Null,
        "loops": Value::Null,
        "_gaps": gaps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pnl(
        window_days: i64,
        gross: i64,
        opex: i64,
        volume: i64,
        forwards: i64,
        margin: f64,
    ) -> PnlSummary {
        PnlSummary {
            window_days,
            gross_revenue_sats: gross,
            opex_sats: opex,
            rebalance_cost_sats: opex,
            closure_cost_sats: 0,
            net_profit_sats: gross - opex,
            operating_margin_pct: margin,
            volume_sats: volume,
            forward_count: forwards,
        }
    }

    #[test]
    fn missing_pnl_yields_null_financials_and_gap() {
        let v = build_health(1000, None, None, None);
        assert_eq!(v["financials"], Value::Null);
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert!(gaps.contains(&"financials"));
    }

    #[test]
    fn wired_pnl_populates_today_and_week() {
        let d1 = pnl(1, 1000, 200, 50_000, 12, 80.0);
        let d7 = pnl(7, 5000, 900, 300_000, 60, 75.451);
        let v = build_health(1000, Some(&d1), Some(&d7), None);
        assert_eq!(v["financials"]["today"]["revenue_sats"], 1000);
        assert_eq!(v["financials"]["today"]["net_profit_sats"], 800);
        assert_eq!(v["financials"]["week"]["revenue_sats"], 5000);
        // round(75.451, 1) == 75.5 (python3-verified golden) -- control on
        // py_round actually being applied to 1 decimal, not just passed
        // through unrounded.
        assert_eq!(v["financials"]["week"]["operating_margin_pct"], 75.5);
        assert_eq!(v["financials"]["week"]["annualized_roc_pct"], Value::Null);
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert!(gaps.contains(&"financials.week.annualized_roc_pct"));
        assert!(!gaps.contains(&"financials"));
    }

    #[test]
    fn annualized_roc_computed_when_capacity_supplied() {
        let d1 = pnl(1, 100, 0, 1000, 1, 100.0);
        let d7 = pnl(7, 700, 0, 7000, 7, 100.0);
        // net_profit_7d=700, capacity=10_000 -> roc_pct=7.0 -> annualized =
        // 7.0 * (365/7) = 365.0
        let v = build_health(0, Some(&d1), Some(&d7), Some(10_000));
        assert_eq!(v["financials"]["week"]["annualized_roc_pct"], 365.0);
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert!(!gaps.contains(&"financials.week.annualized_roc_pct"));
    }

    #[test]
    fn zero_capacity_falls_back_to_zero_not_division() {
        let d1 = pnl(1, 100, 0, 1000, 1, 100.0);
        let d7 = pnl(7, 700, 0, 7000, 7, 100.0);
        let v = build_health(0, Some(&d1), Some(&d7), Some(0));
        assert_eq!(v["financials"]["week"]["annualized_roc_pct"], 0.0);
    }

    #[test]
    fn always_present_static_gaps_and_never_fabricated_values() {
        let v = build_health(0, None, None, None);
        for field in [
            "channels",
            "fees",
            "rebalancer",
            "budget",
            "boltz",
            "planner",
            "top_routes",
            "loops",
        ] {
            assert_eq!(
                v[field],
                Value::Null,
                "{field} must be null, not fabricated"
            );
        }
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        for field in [
            "channels",
            "fees",
            "rebalancer",
            "budget",
            "boltz",
            "planner",
            "top_routes",
            "loops",
        ] {
            assert!(gaps.contains(&field), "{field} must be gap-listed");
        }
    }
}
