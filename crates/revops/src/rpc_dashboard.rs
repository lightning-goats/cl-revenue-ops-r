//! Pure response builder for `revenue-r-dashboard`.
//!
//! Per the plan's per-RPC gap table: `period.*` and
//! `financial_health.net_profit_sats`/`operating_margin_pct` are fully
//! DB-backed (`profitability_analyzer.get_pnl_summary()`, plain SQL).
//! C71-28 wired the other four: `financial_health.tlv_sats` and
//! `annualized_roc_pct` from a live `listfunds`/`listpeerchannels` read
//! (py `get_tlv`/`calculate_roc`), and `warnings`/`bleeder_count` from
//! `identify_bleeders` over the windowed profitability snapshot. There is
//! no `_phase1b_gaps` key on this surface any more, and this note used to
//! say otherwise -- a stale contract is worse than a declared gap, because
//! it describes a response shape callers no longer receive.

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
/// (cl-revenue-ops.py, "L-23"/"P1-012" comments): coerce with Python's
/// `int()`, then clamp to `[1, 365]`. `Ok` carries the clamped value;
/// `Err` carries the exact error shape Python returns for a non-coercible
/// input (`{"error": "window_days must be an integer"}`) -- a clean error,
/// never a leaked exception.
///
/// C71-30, verified by EXECUTING the Python rather than reading it:
///
/// - OMITTED binds the signature default of 30. An EXPLICIT `null` does
///   not: it reaches `int(None)`, which raises `TypeError` and returns the
///   error dict. This function previously mapped both to 30, so a caller
///   that explicitly sent `null` silently got a 30-day window instead of
///   being told its argument was invalid.
/// - `bool` is an `int` SUBCLASS in Python, so `true` -> 1 and `false` ->
///   0, and the `max(1, ...)` clamp then makes BOTH of them 1. This
///   function previously rejected booleans on the stated grounds that "no
///   real RPC caller passes a bool" -- a deliberate divergence, and one
///   that returns an error where Python returns a one-day window.
pub fn parse_window_days(raw: Option<&Value>) -> Result<i64, Value> {
    let bad = || json!({"error": "window_days must be an integer"});
    // Absent key == the parameter was never bound, which is the only case
    // Python's signature default covers.
    let Some(value) = raw else {
        return Ok(30);
    };
    // `python_int` is this port's existing `int()` equivalent: bools as
    // 1/0, numeric strings trimmed, floats truncated toward zero, and
    // null/array/object refused with Python's own TypeError vocabulary.
    let parsed = crate::rpc_params::python_int(value).map_err(|_| bad())?;
    Ok(parsed.clamp(1, 365))
}
