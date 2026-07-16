//! Pure response builder for `revenue-r-report`.
//!
//! Per the plan's per-RPC gap table: `report_type="costs"` is fully
//! DB-backed (`database.get_closure_costs_since()` x3 windows +
//! `.get_total_closure_costs()`, plain SQL). `"summary"`/`"policies"`/
//! `"peer"` all route through `policy_manager` (`get_all_policies`/
//! `get_policy`) and/or `profitability_analyzer.get_profitability_by_peer`,
//! which are Phase 3 scope (governed econ/policy layer) -- Phase 1b returns
//! an explicit not-yet-ported shape for these, never a fabricated policy
//! count. Any other `report_type` preserves Python's exact "Unknown report
//! type" error string verbatim (cl-revenue-ops.py:5526).

use revops_db::queries::ClosureCostWindows;
use serde_json::{json, Value};

/// `ChainCostDefaults.CHANNEL_OPEN_COST_SATS` / `.CHANNEL_CLOSE_COST_SATS`
/// (modules/config.py:1423-1435) -- static estimate constants, not DB- or
/// live-RPC-backed, so safe to port as plain constants.
const CHANNEL_OPEN_COST_SATS: i64 = 5000;
const CHANNEL_CLOSE_COST_SATS: i64 = 3000;

/// `costs` is `None` when the caller has no DB actor to run the
/// `channel_closure_costs` queries against; `report_type` must still be
/// `"costs"` for that to matter (the gap-marked and unknown-type branches
/// never touch the DB, so they ignore `costs` entirely).
pub fn build_report(
    report_type: &str,
    costs: Option<&ClosureCostWindows>,
    generated_at: i64,
) -> Value {
    match report_type {
        "costs" => {
            let Some(c) = costs else {
                return json!({"error": "Database not initialized"});
            };
            json!({
                "type": "costs",
                "closure_costs": {
                    "last_24h_sats": c.last_24h_sats,
                    "last_7d_sats": c.last_7d_sats,
                    "last_30d_sats": c.last_30d_sats,
                    "total_sats": c.total_sats,
                },
                "estimated_defaults": {
                    "channel_open_sats": CHANNEL_OPEN_COST_SATS,
                    "channel_close_sats": CHANNEL_CLOSE_COST_SATS,
                },
                "generated_at": generated_at,
            })
        }
        "summary" | "policies" | "peer" => json!({
            "error": "not_yet_ported",
            "report_type": report_type,
            "reason": "requires policy_manager (Phase 3)",
        }),
        other => json!({
            "error": format!(
                "Unknown report type: {other}. Use 'summary', 'peer', 'policies', or 'costs'"
            ),
        }),
    }
}
