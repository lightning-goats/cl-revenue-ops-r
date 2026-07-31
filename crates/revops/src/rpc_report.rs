//! Pure response builders for `revenue-report` (Task 66 slice 8c: ALL
//! four report types are real now).
//!
//! Port of `revenue_report` (cl-revenue-ops.py:5578-5716): `costs` was
//! DB-backed since Phase 1b; `summary` and `policies` count the same
//! already-ported [`PeerPolicy`] rows `revenue-policy list` serves;
//! `peer` combines the peer's policy dict, the by-peer profitability
//! aggregate (`rpc_profitability::profitability_by_peer`), and the
//! peer's `channel_states` rows (singleton `flow_state` rule, py
//! 5640-5644). The former `not_yet_ported` arms are gone. Any other
//! `report_type` preserves Python's exact "Unknown report type" error
//! string verbatim (cl-revenue-ops.py:5712); every post-guard fetch
//! failure is Python's outer `except` arm ([`report_generation_failed`],
//! py 5715-5716).

use std::collections::BTreeMap;

use revops_analytics::policy::PeerPolicy;
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
        other => unknown_report_type(other),
    }
}

/// py 5712: the exact unknown-type error string.
pub fn unknown_report_type(report_type: &str) -> Value {
    json!({
        "error": format!(
            "Unknown report type: {report_type}. Use 'summary', 'peer', 'policies', or 'costs'"
        ),
    })
}

/// py 5715-5716: the outer `except Exception` arm every post-guard fetch
/// failure lands on.
pub fn report_generation_failed(error: &str) -> Value {
    json!({"status": "error", "error": format!("Report generation failed: {error}")})
}

fn count_into<'a>(counts: &mut BTreeMap<&'a str, i64>, key: &'a str) {
    *counts.entry(key).or_insert(0) += 1;
}

/// py 5601-5620: strategy/rebalance-mode histograms over
/// `get_all_policies`.
pub fn build_report_summary(policies: &[PeerPolicy], generated_at: i64) -> Value {
    let mut by_strategy: BTreeMap<&str, i64> = BTreeMap::new();
    let mut by_mode: BTreeMap<&str, i64> = BTreeMap::new();
    for policy in policies {
        count_into(&mut by_strategy, policy.strategy.as_value());
        count_into(&mut by_mode, policy.rebalance_mode.as_value());
    }
    json!({
        "type": "summary",
        "policies": {
            "total": policies.len(),
            "by_strategy": by_strategy,
            "by_rebalance_mode": by_mode,
        },
        "generated_at": generated_at,
    })
}

/// py 5648-5678: the flat variant plus the by_tag histogram (every tag
/// on every policy counts). No `generated_at` in Python's shape.
pub fn build_report_policies(policies: &[PeerPolicy]) -> Value {
    let mut by_strategy: BTreeMap<&str, i64> = BTreeMap::new();
    let mut by_mode: BTreeMap<&str, i64> = BTreeMap::new();
    let mut by_tag: BTreeMap<&str, i64> = BTreeMap::new();
    for policy in policies {
        count_into(&mut by_strategy, policy.strategy.as_value());
        count_into(&mut by_mode, policy.rebalance_mode.as_value());
        for tag in &policy.tags {
            count_into(&mut by_tag, tag);
        }
    }
    json!({
        "type": "policies",
        "total": policies.len(),
        "by_strategy": by_strategy,
        "by_rebalance_mode": by_mode,
        "by_tag": by_tag,
    })
}

/// py 5624-5646: `policy` is the peer's `to_dict()`
/// ([`crate::rpc_policy::peer_policy_to_json`]), `profitability` the
/// by-peer aggregate or null (py `prof_data = None` when no analyzer /
/// no channels), and `flow_state` the SINGLETON row only when the peer
/// has exactly one `channel_states` row (py 5643).
pub fn build_report_peer(
    peer_id: &str,
    policy: Value,
    profitability: Value,
    flow_states: Vec<Value>,
) -> Value {
    let flow_state = if flow_states.len() == 1 {
        flow_states[0].clone()
    } else {
        Value::Null
    };
    json!({
        "type": "peer",
        "peer_id": peer_id,
        "policy": policy,
        "profitability": profitability,
        "flow_state": flow_state,
        "flow_states": flow_states,
    })
}
