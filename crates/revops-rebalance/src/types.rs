//! Shared data types (port of `modules/rebalance_types_v2.py` and the
//! multi-source `RebalanceCandidate` in `modules/rebalancer.py`).
//!
//! `RebalanceCandidate` mirrors `rebalancer.py:82-129` (`class
//! RebalanceCandidate`), the explicit-execution shape with multi-source
//! support (`source_candidates`, best-first). Only the frozen-for-T1 field
//! subset is ported here (the Python dataclass carries additional optional
//! fields — `expected_fee_sats`, `ev_base_fee_ppm`, hot-channel-protection
//! metadata, `reason_code`, `bleeder_status`, `direction`,
//! `source_candidate_peer_ids` — with defaults; later tasks extend this
//! struct as they need those fields, per the plan's frozen-interfaces-only
//! scaffold).
//!
//! `SkipRecord` and `DrainDemand` mirror `rebalance_types_v2.py`'s
//! `SkipRecord` and `DrainDemandEntry` dataclasses respectively. Python's
//! outer `DrainDemand` dataclass (a `List[DrainDemandEntry]` plus
//! `total_excess_sats`/`over_local_count`/`paired_count` aggregate stats) is
//! intentionally flattened here: Task 2's `PlanOutput.drain_demand` is a
//! `Vec<DrainDemand>` of entries directly (aggregate stats are trivially
//! derivable by any consumer via `.iter()`, so no wrapper struct is needed
//! at this scaffold stage).
//!
//! NOTE on scope vs. the plan's "File Structure" overview: that section
//! also lists `RebalancePair` and `CycleResult` as T1-owned types in this
//! file. Neither name appears in any concrete Task interface block read for
//! this task (Task 2 uses `PlannedPair`, not `RebalancePair`; `CycleResult`
//! first appears as a return type in Task 7's `engine.rs`, which is a
//! serial/solo wave and can add it to this file without a parallel-wave
//! conflict). Rather than guess an unverified shape and risk a breaking
//! change later, both are deliberately deferred to their owning tasks. This
//! is a documented scope decision, not an oversight — flagged in the T1
//! completion report.

/// Port of `rebalancer.py:82-129` (`class RebalanceCandidate`), the frozen
/// field subset needed by later tasks. `source_candidates` is best-first;
/// `primary_source_peer_id` mirrors Python's `primary_source_peer_id`
/// (backing the `from_peer_id` backwards-compat property at
/// `rebalancer.py:137-140`).
#[derive(Debug, Clone, PartialEq)]
pub struct RebalanceCandidate {
    pub source_candidates: Vec<String>,
    pub to_channel: String,
    pub primary_source_peer_id: String,
    pub to_peer_id: String,
    pub amount_sats: i64,
    pub amount_msat: i64,
    pub outbound_fee_ppm: i64,
    pub inbound_fee_ppm: i64,
    pub source_fee_ppm: i64,
    pub weighted_opp_cost_ppm: i64,
    pub spread_ppm: i64,
    pub max_budget_sats: i64,
    pub max_budget_msat: i64,
    pub max_fee_ppm: i64,
    pub expected_profit_sats: i64,
    pub liquidity_ratio: f64,
    pub dest_flow_state: String,
    pub dest_turnover_rate: f64,
    pub source_turnover_rate: f64,
}

/// Port of `modules/rebalance_execution.py`'s `ExecutionResult` dataclass,
/// the frozen field subset needed by later tasks (Python also carries
/// `attempts`, `fee_sats`, `fee_ppm`, `hops`, `parts`, `failure_data` —
/// added by the owning task, T5, if/when the executor needs them).
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionResult {
    pub success: bool,
    pub error: Option<String>,
    pub amount_sats: i64,
    pub fee_msat: i64,
    pub payment_pending: bool,
    pub payment_hash: Option<String>,
    /// `"scid/dir"` formatted entries, e.g. `"123x456x0/1"`.
    pub excluded_channels: Vec<String>,
    pub route_type: &'static str,
}

/// Port of `rebalance_types_v2.py`'s `SkipRecord` dataclass (defaults:
/// `value_class="neutral"`, `remaining_budget_sats=0`, `detail=None`).
#[derive(Debug, Clone, PartialEq)]
pub struct SkipRecord {
    pub channel_id: String,
    pub reason: String,
    pub value_class: String,
    pub remaining_budget_sats: i64,
    pub detail: Option<String>,
}

impl SkipRecord {
    pub fn new(channel_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            channel_id: channel_id.into(),
            reason: reason.into(),
            value_class: "neutral".to_string(),
            remaining_budget_sats: 0,
            detail: None,
        }
    }
}

/// Port of `rebalance_types_v2.py`'s `DrainDemandEntry` dataclass (one
/// residual over-local channel the planner could not pair this cycle). See
/// the module doc comment for why the outer Python `DrainDemand` wrapper
/// (aggregate stats) is not ported as a separate type.
#[derive(Debug, Clone, PartialEq)]
pub struct DrainDemand {
    pub channel_id: String,
    pub peer_id: String,
    pub excess_sats: i64,
    pub drain_score: f64,
    pub value_class: String,
}

/// Port of `rebalance_execution.py:28-49` (`stable_failure_reason`): maps
/// executor-local error strings to stable coordination reasons via a
/// prefix/substring table. Checked in Python's exact order; case- and
/// whitespace-insensitive (`str(error or "").strip().lower()`).
pub fn stable_failure_reason(error: &str) -> String {
    let normalized = error.trim().to_lowercase();
    if normalized.is_empty() {
        return "local_execution_failed".to_string();
    }
    if normalized == "route_over_budget"
        || normalized.starts_with("route_over_budget:")
        || normalized.starts_with("native_route_over_budget:")
    {
        return "route_segment_exhausted".to_string();
    }
    if normalized.starts_with("native_route_invalid:") {
        return "local_policy_block".to_string();
    }
    if normalized.contains("temporary_channel_failure") || normalized.contains("fee_insufficient") {
        return "shared_conflict_changed".to_string();
    }
    if normalized.contains("incorrect_cltv_expiry") {
        return "shared_conflict_changed".to_string();
    }
    if normalized.contains("timeout") || normalized == "payment_pending_timeout" {
        return "executor_timeout".to_string();
    }
    if normalized.starts_with("retriable_failure:") {
        return "local_execution_failed".to_string();
    }
    "local_execution_failed".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Table transcribed from `rebalance_execution.py:28-49`, one row per
    // branch (order matters: earlier branches shadow later ones).
    #[test]
    fn stable_failure_reason_table() {
        let cases: &[(&str, &str)] = &[
            ("", "local_execution_failed"),
            ("   ", "local_execution_failed"),
            ("route_over_budget", "route_segment_exhausted"),
            ("route_over_budget: 500 > 200", "route_segment_exhausted"),
            (
                "native_route_over_budget: 500 > 200",
                "route_segment_exhausted",
            ),
            ("native_route_invalid: missing_route", "local_policy_block"),
            ("NATIVE_ROUTE_INVALID: missing_route", "local_policy_block"),
            (
                "temporary_channel_failure at hop 2",
                "shared_conflict_changed",
            ),
            ("fee_insufficient", "shared_conflict_changed"),
            ("incorrect_cltv_expiry", "shared_conflict_changed"),
            (
                "payment_pending_timeout: waitsendpay code 200",
                "executor_timeout",
            ),
            ("payment_pending_timeout", "executor_timeout"),
            ("some timeout occurred", "executor_timeout"),
            ("retriable_failure: peer offline", "local_execution_failed"),
            ("totally_unmapped_error", "local_execution_failed"),
        ];
        for (input, expected) in cases {
            assert_eq!(stable_failure_reason(input), *expected, "input={input:?}");
        }
    }

    #[test]
    fn skip_record_defaults_match_python_dataclass() {
        let s = SkipRecord::new("123x456x0", "no_route");
        assert_eq!(s.value_class, "neutral");
        assert_eq!(s.remaining_budget_sats, 0);
        assert_eq!(s.detail, None);
    }
}
