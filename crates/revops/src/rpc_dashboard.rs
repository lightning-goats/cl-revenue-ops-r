//! Pure response builder for `revenue-r-dashboard`.
//!
//! Per the plan's per-RPC gap table: `period.*` and
//! `financial_health.net_profit_sats`/`operating_margin_pct` are fully
//! DB-backed (`profitability_analyzer.get_pnl_summary()`, plain SQL).
//! `financial_health.tlv_sats`/`annualized_roc_pct` need a live
//! `listfunds`/`listpeerchannels` RPC call (`get_tlv`/`calculate_roc`) that
//! this DB-only task deliberately does not wire (that's Task 2's
//! hydration-only `cln-rpc` carve-out, not generalized here); `warnings`/
//! `bleeder_count` additionally need `profitability_analyzer`'s
//! sourced-fee-contribution attribution logic (Phase 3). All four are
//! returned as `null` and listed in `_phase1b_gaps`, per the plan's "no
//! silent stubs" contract.

use revops_db::queries::PnlSummary;
use serde_json::{json, Value};

/// Port of `revenue_dashboard`'s DB-backed half (cl-revenue-ops.py:5726-
/// 5825), minus the `tlv_sats`/`annualized_roc_pct`/`warnings`/
/// `bleeder_count` fields (see module doc comment).
pub fn build_dashboard(pnl: &PnlSummary, evidence: &DashboardEvidence) -> Value {
    json!({
        "financial_health": {
            "tlv_sats": evidence.tlv_sats,
            "net_profit_sats": pnl.net_profit_sats,
            "operating_margin_pct": pnl.operating_margin_pct,
            "annualized_roc_pct": evidence.annualized_roc_pct,
        },
        "period": {
            "window_days": pnl.window_days,
            "gross_revenue_sats": pnl.gross_revenue_sats,
            "opex_sats": pnl.opex_sats,
            "rebalance_cost_sats": pnl.rebalance_cost_sats,
            "closure_cost_sats": pnl.closure_cost_sats,
            "volume_sats": pnl.volume_sats,
            "forward_count": pnl.forward_count,
        },
        "warnings": evidence.warnings,
        "bleeder_count": evidence.bleeder_count,
    })
}

/// The four fields the dashboard used to gap-mark, once they have been
/// looked up. C71-28: there is no `_phase1b_gaps` key any more, because
/// there are no Phase-1b gaps left on this surface.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DashboardEvidence {
    pub tlv_sats: i64,
    /// Already `py_round(_, 2)`ed by `dashboard_evidence::annualized_roc_pct`.
    pub annualized_roc_pct: f64,
    pub warnings: Vec<String>,
    pub bleeder_count: usize,
}

/// A store or node this call could not consult. Never a zeroed dashboard:
/// `tlv_sats: 0` is a node worth nothing and `warnings: []` is a node with
/// nothing wrong, and both are answers Python emits for real.
pub fn build_dashboard_unavailable(code: &str, detail: &str) -> Value {
    json!({"error": code, "detail": detail})
}

/// Port of `revenue_dashboard`'s `window_days` parsing/clamp
/// (cl-revenue-ops.py, "L-23"/"P1-012" comments): coerce to `int`, then
/// clamp to `[1, 365]`. `Ok` carries the clamped value; `Err` carries the
/// exact error shape Python returns for a non-coercible input
/// (`{"error": "window_days must be an integer"}`) -- a clean error, never
/// a leaked exception.
///
/// Deliberately narrower than Python's `int(x)` for one edge case: a JSON
/// boolean is rejected here (`Err`), where Python's `int(True) == 1` would
/// succeed (`bool` is an `int` subclass). No real RPC caller passes a bool
/// for `window_days`, and rejecting it cleanly is preferable to silently
/// treating `true`/`false` as `1`/`0`.
pub fn parse_window_days(raw: Option<&Value>) -> Result<i64, Value> {
    let bad = || json!({"error": "window_days must be an integer"});
    let parsed: i64 = match raw {
        None | Some(Value::Null) => 30,
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                i
            } else if let Some(f) = n.as_f64() {
                f.trunc() as i64
            } else {
                return Err(bad());
            }
        }
        Some(Value::String(s)) => s.trim().parse::<i64>().map_err(|_| bad())?,
        Some(_) => return Err(bad()),
    };
    Ok(parsed.clamp(1, 365))
}
