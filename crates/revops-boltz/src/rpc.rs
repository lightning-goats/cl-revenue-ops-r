//! Pure JSON response builders for the Boltz operator RPCs (`status`,
//! `budget`, `history`, `quote`).
//!
//! Ports the response-SHAPING half of py `swap_status` (boltz_manager.py:
//! 2394-2433), `get_budget_status` (1540-1579), `swap_history`
//! (2435-2456), and `quote` (1887-1894) — MINUS every I/O call each of
//! those functions makes (the `swapinfo`/`listswaps` subprocess calls, the
//! swap-journal / ignored-external-swaps file reads, the unified-budget and
//! external-liquidity-cost providers). Each builder here takes the
//! already-acquired/-computed pieces as plain parameters and returns the
//! exact `serde_json::Value` shape the Python RPC returns, so a live
//! adapter only has to plug in real I/O around a builder call, not
//! re-derive the response shape by hand.

use crate::budget::{BudgetStatus, CostComponents};
use crate::fee::estimate_swap_fee_sats;
use crate::state::is_completed_swap;
use serde_json::{json, Map, Value};

fn opt_map_to_value(m: Option<&Map<String, Value>>) -> Value {
    match m {
        Some(map) => Value::Object(map.clone()),
        None => Value::Null,
    }
}

/// py `swap_status` (boltz_manager.py:2425-2433). `swapinfo_entry`/
/// `listswaps_entry` are already annotated with journal/ignored metadata by
/// the live adapter (the annotation itself is I/O-touching — file reads —
/// per `ENTRYPOINTS.md`'s item 5).
#[allow(clippy::too_many_arguments)]
pub fn build_status_response(
    swap_id: &str,
    swapinfo_raw: &str,
    swapinfo_entry: Option<&Map<String, Value>>,
    listswaps_entry: Option<&Map<String, Value>>,
    ignored_external_swap: bool,
    ignored_external_swap_meta: Option<&Map<String, Value>>,
    journal_meta: Option<&Map<String, Value>>,
) -> Value {
    json!({
        "swap_id": swap_id,
        "swapinfo_raw": swapinfo_raw,
        "swapinfo_entry": opt_map_to_value(swapinfo_entry),
        "listswaps_entry": opt_map_to_value(listswaps_entry),
        "ignored_external_swap": ignored_external_swap,
        "ignored_external_swap_meta": opt_map_to_value(ignored_external_swap_meta),
        "journal_meta": opt_map_to_value(journal_meta),
    })
}

/// py `get_budget_status` (boltz_manager.py:1561-1579). `external` /
/// `budget_info` are the raw provider-returned dicts (py
/// `_get_external_liquidity_costs`/`_get_global_budget_limit` — both call
/// out to injected providers, I/O by this crate's definition).
/// `counted_details` is truncated to the first 20 entries, matching py's
/// `counted[:20]` — pass the FULL list, this function does the slicing.
#[allow(clippy::too_many_arguments)]
pub fn build_budget_response(
    local: &CostComponents,
    status: &BudgetStatus,
    external: &Value,
    budget_source: &str,
    budget_info: &Value,
    enforce_budget: bool,
    counted_details: &[Value],
) -> Value {
    let truncated: Vec<Value> = counted_details.iter().take(20).cloned().collect();
    json!({
        "daily_budget_sats": status.daily_budget_sats,
        "spent_24h_sats_estimate": status.spent_24h_sats_estimate,
        "remaining_24h_sats_estimate": status.remaining_24h_sats_estimate,
        "reserved_24h_sats_estimate": status.reserved_24h_sats_estimate,
        "boltz_spent_24h_sats_estimate": status.boltz_spent_24h_sats_estimate,
        "boltz_remaining_24h_sats_estimate": status.boltz_remaining_24h_sats_estimate,
        "boltz_reserved_24h_sats_estimate": status.boltz_reserved_24h_sats_estimate,
        "external_liquidity_costs": external,
        "budget_source": budget_source,
        "budget_info": budget_info,
        "counted_swaps": local.counted_swaps,
        "skipped_without_timestamp": local.skipped_without_timestamp,
        "enforce_budget": enforce_budget,
        "window_seconds": 86400,
        "counted_details": truncated,
    })
}

/// py `swap_history` (boltz_manager.py:2435-2456): sort newest-created
/// first, optionally limit, and compute the cost summary. `swaps` must
/// already be extracted/augmented/annotated (I/O — `ENTRYPOINTS.md` item
/// 5); this function performs only the pure sort/limit/summarize tail.
pub fn build_swap_history_response(
    swaps: &[Map<String, Value>],
    limit: Option<usize>,
    created_ts_of: impl Fn(&Map<String, Value>) -> i64,
) -> Value {
    let mut sorted: Vec<Map<String, Value>> = swaps.to_vec();
    sorted.sort_by_key(|s| std::cmp::Reverse(created_ts_of(s)));
    if let Some(lim) = limit {
        sorted.truncate(lim);
    }
    let swap_count = sorted.len();
    let estimated_total_fee_sats: i64 = sorted
        .iter()
        .map(|s| estimate_swap_fee_sats(&Value::Object(s.clone())))
        .sum();
    let completed_count = sorted.iter().filter(|s| is_completed_swap(s)).count();
    json!({
        "swaps": sorted,
        "cost_summary": {
            "swap_count": swap_count,
            "estimated_total_fee_sats": estimated_total_fee_sats,
            "completed_count": completed_count,
        }
    })
}

/// py `quote` (boltz_manager.py:1887-1894). `swap_type_label` is the
/// (already lowercased, NOT further normalized) string py echoes back
/// verbatim as `st` — e.g. an input of `"normal"` stays `"normal"` in the
/// response even though it is classified as [`crate::argv::SwapType::Submarine`]
/// for argv purposes; `currency_label` is the caller's already-computed
/// `argv::normalize_currency(currency, default)` result;
/// `estimated_routing_fee_sats` is `argv`'s reverse-only routing estimate
/// (0 for submarine/chain, matching py's `if st == "reverse" else 0`).
pub fn build_quote_response(
    swap_type_label: &str,
    amount_sats: i64,
    currency_label: &str,
    quote_data: &Value,
    estimated_routing_fee_sats: i64,
) -> Value {
    let estimated_total_fee_sats =
        estimate_swap_fee_sats(quote_data) + estimated_routing_fee_sats.max(0);
    json!({
        "swap_type": swap_type_label,
        "amount_sats": amount_sats,
        "currency": currency_label,
        "quote": quote_data,
        "estimated_routing_fee_sats": estimated_routing_fee_sats,
        "estimated_total_fee_sats": estimated_total_fee_sats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::{budget_status, ExternalLiquidityCosts};
    use serde_json::json;

    fn map(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    // --- build_status_response ---

    #[test]
    fn status_response_includes_all_fields_when_present() {
        let entry = map(json!({"id": "s1", "state": "swap.created"}));
        let journal = map(json!({"source": "loop_out"}));
        let v = build_status_response(
            "s1",
            "{\"id\":\"s1\"}",
            Some(&entry),
            Some(&entry),
            true,
            Some(&journal),
            Some(&journal),
        );
        assert_eq!(v["swap_id"], "s1");
        assert_eq!(v["swapinfo_entry"]["state"], "swap.created");
        assert_eq!(v["ignored_external_swap"], true);
        assert_eq!(v["journal_meta"]["source"], "loop_out");
    }

    #[test]
    fn status_response_uses_null_for_absent_optionals() {
        // Control: an unknown swap_id (nothing found anywhere) must
        // produce nulls, not an error or a missing key.
        let v = build_status_response("unknown", "", None, None, false, None, None);
        assert!(v["swapinfo_entry"].is_null());
        assert!(v["listswaps_entry"].is_null());
        assert!(v["journal_meta"].is_null());
        assert_eq!(v["ignored_external_swap"], false);
    }

    // --- build_budget_response ---

    #[test]
    fn budget_response_combines_status_and_local_fields() {
        let local = CostComponents {
            spent_24h_sats: 100,
            reserved_24h_sats: 50,
            reserved_swaps: 1,
            counted_swaps: 3,
            skipped_without_timestamp: 2,
        };
        let status = budget_status(1000, &local, &ExternalLiquidityCosts::default());
        let external = json!({"source": "none", "spent_24h_sats": 0, "reserved_24h_sats": 0});
        let budget_info = json!({"budget_sats": 1000, "source": "boltz_cfg"});
        let v = build_budget_response(
            &local,
            &status,
            &external,
            "boltz_cfg",
            &budget_info,
            true,
            &[],
        );
        assert_eq!(v["daily_budget_sats"], 1000);
        assert_eq!(v["counted_swaps"], 3);
        assert_eq!(v["skipped_without_timestamp"], 2);
        assert_eq!(v["enforce_budget"], true);
        assert_eq!(v["window_seconds"], 86400);
        assert_eq!(v["budget_source"], "boltz_cfg");
    }

    #[test]
    fn budget_response_truncates_counted_details_to_20() {
        let local = CostComponents::default();
        let status = budget_status(1000, &local, &ExternalLiquidityCosts::default());
        let details: Vec<Value> = (0..30).map(|i| json!({"id": format!("s{i}")})).collect();
        let v = build_budget_response(
            &local,
            &status,
            &json!({}),
            "boltz_cfg",
            &json!({}),
            false,
            &details,
        );
        assert_eq!(v["counted_details"].as_array().unwrap().len(), 20);
        assert_eq!(v["counted_details"][0]["id"], "s0");
    }

    // --- build_swap_history_response ---

    #[test]
    fn history_response_sorts_newest_first() {
        let swaps = vec![
            map(json!({"id": "old", "createdAt": 100})),
            map(json!({"id": "new", "createdAt": 500})),
        ];
        let v = build_swap_history_response(&swaps, None, |s| {
            s.get("createdAt").and_then(|x| x.as_i64()).unwrap_or(0)
        });
        let ids: Vec<String> = v["swaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, vec!["new".to_string(), "old".to_string()]);
    }

    #[test]
    fn history_response_applies_limit() {
        let swaps = vec![
            map(json!({"id": "a", "createdAt": 1})),
            map(json!({"id": "b", "createdAt": 2})),
            map(json!({"id": "c", "createdAt": 3})),
        ];
        let v = build_swap_history_response(&swaps, Some(2), |s| {
            s.get("createdAt").and_then(|x| x.as_i64()).unwrap_or(0)
        });
        assert_eq!(v["swaps"].as_array().unwrap().len(), 2);
        assert_eq!(v["cost_summary"]["swap_count"], 2);
    }

    #[test]
    fn history_response_computes_cost_summary() {
        let swaps = vec![
            map(json!({"id": "a", "state": "swap.completed", "boltzFee": 10, "createdAt": 1})),
            map(json!({"id": "b", "state": "swap.created", "boltzFee": 5, "createdAt": 2})),
        ];
        let v = build_swap_history_response(&swaps, None, |s| {
            s.get("createdAt").and_then(|x| x.as_i64()).unwrap_or(0)
        });
        assert_eq!(v["cost_summary"]["estimated_total_fee_sats"], 15);
        assert_eq!(v["cost_summary"]["completed_count"], 1);
    }

    // --- build_quote_response ---

    #[test]
    fn quote_response_reverse_includes_routing_fee() {
        let quote_data = json!({"boltzFee": 20});
        let v = build_quote_response("reverse", 50_000, "BTC", &quote_data, 7);
        assert_eq!(v["swap_type"], "reverse");
        assert_eq!(v["estimated_routing_fee_sats"], 7);
        assert_eq!(v["estimated_total_fee_sats"], 27);
    }

    #[test]
    fn quote_response_non_reverse_has_zero_routing_fee() {
        // Control: submarine/chain quotes pass 0 for the routing component.
        let quote_data = json!({"boltzFee": 20});
        let v = build_quote_response("submarine", 1000, "LBTC", &quote_data, 0);
        assert_eq!(v["estimated_routing_fee_sats"], 0);
        assert_eq!(v["estimated_total_fee_sats"], 20);
    }

    #[test]
    fn quote_response_echoes_raw_swap_type_label_verbatim() {
        // "normal" is classified as Submarine for argv purposes but py
        // echoes the original string back in the response.
        let quote_data = json!({});
        let v = build_quote_response("normal", 1000, "LBTC", &quote_data, 0);
        assert_eq!(v["swap_type"], "normal");
    }
}
