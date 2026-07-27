//! Pure response builder for `revenue-rebalance-debug`.
//!
//! Ports `revenue_rebalance_debug` (cl-revenue-ops.py:3896-4252). This is
//! the widest-gapped builder in this batch. Python's handler leans on:
//!
//! - `_total_cost_budget_status`/`listfunds`/`database.
//!   get_daily_rebalance_spend()` for `capital_controls` -- the unified
//!   budget aggregator this batch's `revenue-total-cost-budget` builder
//!   separately ports; NOT re-derived here (a real handler can wire the
//!   two together, but this builder does not assume that wiring exists).
//! - `database.get_channel_state(cid)` for per-channel `flow_state` --
//!   unported.
//! - `rebalancer._compute_hot_channel_protection` /
//!   `profitability_analyzer.analyze_channel` for the entire
//!   hot-channel-protection scoring pipeline (9 per-channel fields) --
//!   unported anywhere in this codebase.
//! - `rebalancer.job_manager.active_channels` for job-in-flight
//!   membership -- unported (no job manager exists in the Rust port; this
//!   read-only surface never launches jobs, so an empty set is the
//!   HONEST answer here, not a stub).
//! - `rebalancer.get_last_decision_summary()`,
//!   `RebalanceEngineV2.get_last_cycle_debug()`/`.get_drain_demand()` --
//!   none ported. `revops_rebalance::engine::RebalanceEngine` is a
//!   DIFFERENT engine than Python's `RebalanceEngineV2` and has no
//!   equivalent methods; `revops_rebalance::planner::plan()` computes an
//!   adjacent-but-not-identical drain demand from a fresh channel
//!   snapshot, not the live engine's cached one, so it is not substituted
//!   here either.
//!
//! What DOES have a direct, faithful port: `filters` (from RPC params),
//! `dry_run` (config passthrough), all three `thresholds` fields
//! (`low_liquidity_threshold`/`high_liquidity_threshold`/
//! `rebalance_hold_margin_sats` are registered `Config` fields --
//! `fixtures/config_types.json` -- resolvable via the same live
//! `revenue-r-config` machinery as any other key; NOT a gap), the
//! depleted/source/active_jobs channel bucketing (py 4108-4200, a direct
//! `local_pct` vs. threshold comparison over `listpeerchannels`-shaped
//! balances), the per-peer hot-channel-protection depletion-trigger
//! OVERRIDE table (`hot_channel_protection_overrides`, a plain DB read,
//! no scoring), and the two threshold-based `rejection_reasons` (py
//! 4202-4219).
//!
//! Every other Python field -- `capital_controls`, `channels.*[].
//! flow_state`, the 9 hot-channel-protection per-channel fields,
//! `last_decision`, `last_cycle`, `drain_demand`, and the two
//! capital-controls-derived rejection reasons -- is `null`/omitted and
//! declared in `_gaps`.

use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// RPC-param-derived filters (py 3926-3935). `max_candidates` parsing
/// mirrors py's defensive `try: max(0, int(max_candidates or 0)) except
/// (ValueError, TypeError): max_candidates = 0` -- a non-numeric input
/// degrades to "unlimited" rather than erroring a diagnostic command.
pub fn parse_max_candidates(raw: Option<&Value>) -> i64 {
    let parsed = match raw {
        None | Some(Value::Null) => Some(0),
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        Some(Value::Bool(_)) => None,
        Some(_) => None,
    };
    parsed.unwrap_or(0).max(0)
}

pub struct RebalanceDebugFilters {
    pub channel_id: Option<String>,
    pub peer_id: Option<String>,
    pub summary_only: bool,
    /// Already AND-ed with `!summary_only` (py 3929).
    pub include_hot_markers: bool,
    pub max_candidates: i64,
}

/// One channel's already-fetched balance snapshot (py
/// `rebalancer._get_channels_with_balances()`'s per-channel dict, the
/// subset this handler reads).
#[derive(Debug, Clone, PartialEq)]
pub struct RebalanceDebugChannel {
    pub channel_id: String,
    pub peer_id: String,
    pub capacity_sats: i64,
    pub spendable_sats: i64,
    pub fee_ppm: i64,
}

/// Registered `Config` fields `revenue-rebalance-debug` reads (py
/// `cfg = config.snapshot()`, 3974-3982) -- all three are resolvable via
/// this codebase's existing `revenue-r-config` machinery, so none of
/// these are gaps.
pub struct RebalanceDebugThresholds {
    pub low_liquidity_threshold: f64,
    pub high_liquidity_threshold: f64,
    pub rebalance_hold_margin_sats: f64,
}

/// Build the `revenue-rebalance-debug` response.
///
/// `active_channel_ids` should be the live job manager's in-flight set;
/// an empty set (the only value this Rust port can currently supply,
/// since no job manager is wired) is the honest answer, not an invented
/// one -- it means "no jobs known to be active", which is true.
/// `hot_override_depletion_thresholds` is `peer_id -> fraction` from
/// `hot_channel_protection_overrides` (a plain DB table, no scoring).
pub fn build_rebalance_debug(
    filters: &RebalanceDebugFilters,
    dry_run: bool,
    thresholds: &RebalanceDebugThresholds,
    channels: &[RebalanceDebugChannel],
    active_channel_ids: &BTreeSet<String>,
    hot_override_depletion_thresholds: &BTreeMap<String, f64>,
) -> Value {
    let filter_channel_id = filters.channel_id.as_deref().unwrap_or("").trim();
    let filter_peer_id = filters
        .peer_id
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();

    let mut depleted: Vec<Value> = Vec::new();
    let mut source: Vec<Value> = Vec::new();
    let mut active_jobs: Vec<Value> = Vec::new();
    let mut considered = 0i64;
    let mut depleted_count = 0i64;
    let mut source_count = 0i64;
    let mut active_jobs_count = 0i64;
    let mut trunc_depleted = 0i64;
    let mut trunc_source = 0i64;
    let mut trunc_active_jobs = 0i64;
    let mut effective_low_thresholds_seen_pct: BTreeSet<i64> = BTreeSet::new();

    let has_filter = !filter_channel_id.is_empty() || !filter_peer_id.is_empty();

    for ch in channels {
        if ch.capacity_sats == 0 {
            continue;
        }
        if !filter_channel_id.is_empty() && ch.channel_id != filter_channel_id {
            continue;
        }
        if !filter_peer_id.is_empty() && ch.peer_id.to_ascii_lowercase() != filter_peer_id {
            continue;
        }

        let ratio = ch.spendable_sats as f64 / ch.capacity_sats as f64;
        let peer_short: String = ch.peer_id.chars().take(16).collect();
        considered += 1;

        let effective_low_threshold = hot_override_depletion_thresholds
            .get(&ch.peer_id)
            .copied()
            .unwrap_or(thresholds.low_liquidity_threshold);
        // Round to 1dp-of-percent, matching py's `round(x*100, 1)`, then
        // key the seen-thresholds set on a 1dp-fixed integer (tenths of a
        // percent) so float equality doesn't fragment the set.
        let effective_low_pct = (effective_low_threshold * 1000.0).round() / 10.0;
        effective_low_thresholds_seen_pct.insert((effective_low_pct * 10.0).round() as i64);

        let scid_truncated: String = ch.channel_id.chars().take(20).collect();
        let mut channel_info = json!({
            "scid": scid_truncated,
            "peer": peer_short,
            "local_pct": py_round1(ratio * 100.0),
            "fee_ppm": ch.fee_ppm,
            // GAP: no ported DB-backed channel-state read.
            "flow_state": Value::Null,
            "effective_low_liquidity_threshold_pct": effective_low_pct,
        });
        if has_filter {
            channel_info["peer_id"] = json!(ch.peer_id);
        }
        if filters.include_hot_markers {
            channel_info["hot_channel_protection"] = Value::Null;
            channel_info["hot_channel_protection_enabled"] = Value::Null;
            channel_info["hot_channel_protection_reason"] = Value::Null;
            channel_info["hot_channel_protection_score"] = Value::Null;
            channel_info["hot_channel_protection_peer_override"] = Value::Null;
            channel_info["hot_channel_protection_peer_depletion_trigger_pct"] = Value::Null;
            channel_info["profit_budget_override_sats"] = Value::Null;
            channel_info["hot_recommended_cooldown_hours"] = Value::Null;
            channel_info["hot_chunk_multiplier"] = Value::Null;
        }

        if active_channel_ids.contains(&ch.channel_id) {
            active_jobs_count += 1;
            push_bucket(
                &mut active_jobs,
                channel_info,
                filters.summary_only,
                filters.max_candidates,
                &mut trunc_active_jobs,
            );
        } else if ratio < effective_low_threshold {
            depleted_count += 1;
            channel_info["reason"] = json!("low local balance");
            // py's `skip_reason` fires only when `flow_state == "sink"`;
            // `flow_state` is a gap here (always null), so that branch can
            // never fire honestly -- omitted rather than guessed.
            push_bucket(
                &mut depleted,
                channel_info,
                filters.summary_only,
                filters.max_candidates,
                &mut trunc_depleted,
            );
        } else if ratio > thresholds.high_liquidity_threshold {
            source_count += 1;
            channel_info["reason"] = json!("high local balance");
            push_bucket(
                &mut source,
                channel_info,
                filters.summary_only,
                filters.max_candidates,
                &mut trunc_source,
            );
        }
    }

    let mut rejection_reasons: Vec<String> = Vec::new();
    if depleted_count == 0 {
        if has_filter && !effective_low_thresholds_seen_pct.is_empty() {
            let sorted: Vec<f64> = effective_low_thresholds_seen_pct
                .iter()
                .map(|tenths| *tenths as f64 / 10.0)
                .collect();
            let threshold_txt = if sorted.len() == 1 {
                format!("{}%", sorted[0])
            } else {
                format!("{}%-{}%", sorted[0], sorted[sorted.len() - 1])
            };
            rejection_reasons.push(format!(
                "No depleted channels (none below effective filtered threshold {threshold_txt} local balance)"
            ));
        } else {
            rejection_reasons.push(format!(
                "No depleted channels (none below {}% local balance)",
                thresholds.low_liquidity_threshold * 100.0
            ));
        }
    }
    if source_count == 0 {
        rejection_reasons.push(format!(
            "No source channels (none above {}% local balance)",
            thresholds.high_liquidity_threshold * 100.0
        ));
    }

    json!({
        "executor_available": true,
        "dry_run": dry_run,
        "filters": {
            "channel_id": non_empty(filter_channel_id),
            "peer_id": non_empty(&filter_peer_id),
            "summary_only": filters.summary_only,
            "include_hot_markers": filters.include_hot_markers,
            "max_candidates": if filters.max_candidates > 0 { json!(filters.max_candidates) } else { Value::Null },
        },
        "capital_controls": Value::Null,
        "thresholds": {
            "low_liquidity_threshold": thresholds.low_liquidity_threshold,
            "high_liquidity_threshold": thresholds.high_liquidity_threshold,
            "rebalance_hold_margin_sats": thresholds.rebalance_hold_margin_sats,
        },
        "channels": {
            "depleted": depleted,
            "source": source,
            "active_jobs": active_jobs,
            "counts": {
                "considered": considered,
                "depleted": depleted_count,
                "source": source_count,
                "active_jobs": active_jobs_count,
            },
            "truncated": {
                "depleted": trunc_depleted,
                "source": trunc_source,
                "active_jobs": trunc_active_jobs,
            },
        },
        "rejection_reasons": rejection_reasons,
        "last_decision": Value::Null,
        "last_cycle": Value::Null,
        "drain_demand": Value::Null,
        "_phase1b_gaps": [
            "capital_controls",
            "channels.depleted[].flow_state",
            "channels.source[].flow_state",
            "channels.active_jobs[].flow_state",
            "channels.*[].hot_channel_protection*",
            "channels.*[].profit_budget_override_sats",
            "channels.*[].hot_recommended_cooldown_hours",
            "channels.*[].hot_chunk_multiplier",
            "last_decision",
            "last_cycle",
            "drain_demand",
        ],
    })
}

fn non_empty(s: &str) -> Value {
    if s.is_empty() {
        Value::Null
    } else {
        json!(s)
    }
}

fn py_round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

#[allow(clippy::too_many_arguments)]
fn push_bucket(
    bucket: &mut Vec<Value>,
    item: Value,
    summary_only: bool,
    max_candidates: i64,
    truncated: &mut i64,
) {
    if summary_only {
        return;
    }
    if max_candidates > 0 && bucket.len() as i64 >= max_candidates {
        *truncated += 1;
        return;
    }
    bucket.push(item);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filters() -> RebalanceDebugFilters {
        RebalanceDebugFilters {
            channel_id: None,
            peer_id: None,
            summary_only: false,
            include_hot_markers: true,
            max_candidates: 0,
        }
    }

    fn thresholds() -> RebalanceDebugThresholds {
        RebalanceDebugThresholds {
            low_liquidity_threshold: 0.2,
            high_liquidity_threshold: 0.8,
            rebalance_hold_margin_sats: 5.0,
        }
    }

    fn ch(id: &str, peer: &str, capacity: i64, spendable: i64) -> RebalanceDebugChannel {
        RebalanceDebugChannel {
            channel_id: id.to_string(),
            peer_id: peer.to_string(),
            capacity_sats: capacity,
            spendable_sats: spendable,
            fee_ppm: 100,
        }
    }

    #[test]
    fn a_low_ratio_channel_is_bucketed_depleted_and_a_high_one_source() {
        let channels = vec![
            ch("1x1x0", "03aa", 1_000_000, 100_000), // 10% < 20% -> depleted
            ch("2x2x0", "03bb", 1_000_000, 900_000), // 90% > 80% -> source
        ];
        let v = build_rebalance_debug(
            &filters(),
            true,
            &thresholds(),
            &channels,
            &BTreeSet::new(),
            &BTreeMap::new(),
        );
        assert_eq!(v["channels"]["counts"]["depleted"], 1);
        assert_eq!(v["channels"]["counts"]["source"], 1);
        assert_eq!(v["channels"]["depleted"][0]["scid"], "1x1x0");
        assert_eq!(v["channels"]["source"][0]["scid"], "2x2x0");

        // Control: a balanced channel (50%) lands in neither bucket, only
        // "considered".
        let balanced = vec![ch("3x3x0", "03cc", 1_000_000, 500_000)];
        let v2 = build_rebalance_debug(
            &filters(),
            true,
            &thresholds(),
            &balanced,
            &BTreeSet::new(),
            &BTreeMap::new(),
        );
        assert_eq!(v2["channels"]["counts"]["depleted"], 0);
        assert_eq!(v2["channels"]["counts"]["source"], 0);
        assert_eq!(v2["channels"]["counts"]["considered"], 1);
    }

    #[test]
    fn a_zero_capacity_channel_is_skipped_entirely() {
        let channels = vec![ch("1x1x0", "03aa", 0, 0)];
        let v = build_rebalance_debug(
            &filters(),
            false,
            &thresholds(),
            &channels,
            &BTreeSet::new(),
            &BTreeMap::new(),
        );
        assert_eq!(v["channels"]["counts"]["considered"], 0);
    }

    #[test]
    fn active_job_membership_overrides_depleted_or_source_classification() {
        // A deeply depleted channel that IS in the active-jobs set must be
        // bucketed there, not double-counted as depleted.
        let channels = vec![ch("1x1x0", "03aa", 1_000_000, 10_000)];
        let mut active = BTreeSet::new();
        active.insert("1x1x0".to_string());
        let v = build_rebalance_debug(
            &filters(),
            false,
            &thresholds(),
            &channels,
            &active,
            &BTreeMap::new(),
        );
        assert_eq!(v["channels"]["counts"]["active_jobs"], 1);
        assert_eq!(
            v["channels"]["counts"]["depleted"], 0,
            "an active-job channel must not ALSO be counted depleted"
        );
    }

    #[test]
    fn hot_channel_fields_are_null_when_requested_and_absent_when_not() {
        let channels = vec![ch("1x1x0", "03aa", 1_000_000, 100_000)];
        let with_markers = build_rebalance_debug(
            &filters(),
            false,
            &thresholds(),
            &channels,
            &BTreeSet::new(),
            &BTreeMap::new(),
        );
        assert!(with_markers["channels"]["depleted"][0]["hot_channel_protection"].is_null());

        let mut f = filters();
        f.include_hot_markers = false;
        let without_markers = build_rebalance_debug(
            &f,
            false,
            &thresholds(),
            &channels,
            &BTreeSet::new(),
            &BTreeMap::new(),
        );
        assert!(
            without_markers["channels"]["depleted"][0]
                .get("hot_channel_protection")
                .is_none(),
            "hot markers must be entirely absent when not requested, not just null"
        );
    }

    #[test]
    fn max_candidates_truncates_and_counts_the_overflow() {
        let channels = vec![
            ch("1x1x0", "03aa", 1_000_000, 100_000),
            ch("2x2x0", "03bb", 1_000_000, 100_000),
            ch("3x3x0", "03cc", 1_000_000, 100_000),
        ];
        let mut f = filters();
        f.max_candidates = 1;
        let v = build_rebalance_debug(
            &f,
            false,
            &thresholds(),
            &channels,
            &BTreeSet::new(),
            &BTreeMap::new(),
        );
        assert_eq!(v["channels"]["depleted"].as_array().unwrap().len(), 1);
        assert_eq!(
            v["channels"]["counts"]["depleted"], 3,
            "count reflects ALL matches, not just kept ones"
        );
        assert_eq!(v["channels"]["truncated"]["depleted"], 2);
    }

    #[test]
    fn summary_only_suppresses_all_channel_detail() {
        let channels = vec![ch("1x1x0", "03aa", 1_000_000, 100_000)];
        let mut f = filters();
        f.summary_only = true;
        let v = build_rebalance_debug(
            &f,
            false,
            &thresholds(),
            &channels,
            &BTreeSet::new(),
            &BTreeMap::new(),
        );
        assert!(v["channels"]["depleted"].as_array().unwrap().is_empty());
        assert_eq!(v["channels"]["counts"]["depleted"], 1);
    }

    #[test]
    fn the_response_declares_its_gaps_honestly() {
        let v = build_rebalance_debug(
            &filters(),
            false,
            &thresholds(),
            &[],
            &BTreeSet::new(),
            &BTreeMap::new(),
        );
        assert!(v["capital_controls"].is_null());
        assert!(v["last_decision"].is_null());
        assert!(v["last_cycle"].is_null());
        assert!(v["drain_demand"].is_null());
        let gaps = v["_phase1b_gaps"].as_array().unwrap();
        assert!(gaps
            .iter()
            .any(|g| g.as_str().unwrap() == "capital_controls"));
        assert!(gaps.iter().any(|g| g.as_str().unwrap() == "last_decision"));
    }

    #[test]
    fn hot_override_threshold_replaces_the_default_for_that_peer_only() {
        let channels = vec![
            ch("1x1x0", "03aa", 1_000_000, 250_000), // 25%
            ch("2x2x0", "03bb", 1_000_000, 250_000), // 25%
        ];
        let mut overrides = BTreeMap::new();
        overrides.insert("03aa".to_string(), 0.30); // 30% > 25% -> depleted
        let v = build_rebalance_debug(
            &filters(),
            false,
            &thresholds(), // default low threshold 20% -- 25% is NOT depleted by default
            &channels,
            &BTreeSet::new(),
            &overrides,
        );
        assert_eq!(
            v["channels"]["counts"]["depleted"], 1,
            "only the overridden peer's channel should flip to depleted"
        );
        assert_eq!(v["channels"]["depleted"][0]["scid"], "1x1x0");
    }

    #[test]
    fn max_candidates_param_parses_defensively() {
        assert_eq!(parse_max_candidates(None), 0);
        assert_eq!(parse_max_candidates(Some(&json!(5))), 5);
        assert_eq!(
            parse_max_candidates(Some(&json!(-3))),
            0,
            "negative clamps to 0, never sent to a bucket cap"
        );
        assert_eq!(
            parse_max_candidates(Some(&json!("not a number"))),
            0,
            "a non-numeric input degrades to unlimited rather than erroring"
        );
    }
}
