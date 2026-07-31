#![forbid(unsafe_code)]

//! Library surface for the `revops` plugin binary: the pieces worth
//! unit-testing in isolation from the cln-plugin stdio handshake (which the
//! `tests/manifest.rs` black-box test covers instead).

pub mod analytics_cadence;
pub mod analytics_passes;
pub mod boltz_boundaries;
pub mod boltz_config;
pub mod boltz_owner;
pub mod capital_adapters;
pub mod capital_boundaries;
pub mod capital_candidates;
pub mod capital_efficiency;
pub mod capital_evidence;
pub mod capital_gates;
pub mod capital_inputs;
pub mod capital_owner;
pub mod capital_producers;
pub mod config_resolve;
pub mod config_types;
pub mod cutover_arm;
pub mod dashboard_evidence;
pub mod discovery_evidence;
pub mod econ_evidence;
pub mod econ_producer;
pub mod enrichment_evidence;
pub mod fee_config;
pub mod fee_evidence;
pub mod fee_execution;
pub mod fee_governor;
pub mod fee_mode;
pub mod fee_scheduler;
pub mod fee_state;
pub mod fee_triggers;
pub mod financial_snapshot;
pub mod flow_config;
pub mod flow_evidence;
pub mod flow_owner;
pub mod hydration;
pub mod lnplus_adapters;
pub mod lnplus_runtime;
pub mod loop_health;
pub mod msat_evidence;
pub mod notify;
pub mod open_ev_evidence;
pub mod options_table;
pub mod profitability_assembler;
pub mod profitability_evidence;
pub mod python_authority;
pub mod rebalance_adapters;
pub mod rebalance_execution;
pub mod rebalance_owner;
pub mod recycle_evidence;
pub mod rpc_analyze;
pub mod rpc_boltz_budget;
pub mod rpc_boltz_history;
pub mod rpc_boltz_ops;
pub mod rpc_boltz_status;
pub mod rpc_capacity_report;
pub mod rpc_capex_status;
pub mod rpc_dashboard;
pub mod rpc_econ_reconcile;
pub mod rpc_econ_snapshot;
pub mod rpc_health;
pub mod rpc_history;
pub mod rpc_hot_channel_protection_peers;
pub mod rpc_list_banned;
pub mod rpc_list_ignored;
pub mod rpc_lnplus_status;
pub mod rpc_params;
pub mod rpc_planner_candidate_sources;
pub mod rpc_planner_candidates;
pub mod rpc_planner_execute;
pub mod rpc_planner_history;
pub mod rpc_planner_status;
pub mod rpc_policy;
pub mod rpc_profile_preview;
pub mod rpc_profitability;
pub mod rpc_rebalance;
pub mod rpc_rebalance_debug;
pub mod rpc_rebalance_ops;
pub mod rpc_report;
pub mod rpc_spend_ledger;
pub mod rpc_state_mutators;
pub mod rpc_status;
pub mod rpc_total_cost_budget;
pub mod runtime;
pub mod startup_snapshot;
pub mod state_writer;

#[cfg(test)]
mod analytics_cadence_tests;
#[cfg(test)]
mod analytics_passes_tests;
#[cfg(test)]
mod flow_evidence_tests;
#[cfg(test)]
mod profitability_evidence_tests;
#[cfg(test)]
mod runtime_tests;

/// Current Unix time in whole seconds, matching Python's
/// `int(time.time())` as used throughout `database.py`/
/// `profitability_analyzer.py`'s windowed queries. A thin wrapper so the
/// read-RPC handlers in `main.rs` don't each repeat the
/// `SystemTime`/`UNIX_EPOCH` dance; returns `0` on a pre-epoch clock rather
/// than panicking mid-request.
pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `serde_json::Value` -> `String`, for `opt_type == "string"` defaults.
pub fn as_string_default(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

/// `serde_json::Value` -> `i64`, for `opt_type == "int"` defaults. The
/// Python source stores every default as a string literal (even for the one
/// `opt_type="int"` option), so this accepts both a JSON number and a
/// numeric string.
pub fn as_int_default(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// `serde_json::Value` -> `bool`, for `opt_type == "bool"` defaults.
pub fn as_bool_default(v: &serde_json::Value) -> Option<bool> {
    match v {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::Number(n) => n.as_i64().map(|i| i != 0),
        serde_json::Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Some(true),
            "false" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}
