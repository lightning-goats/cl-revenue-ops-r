//! Pure response builder for `revenue-boltz-history`.
//!
//! Port of `revenue_boltz_history` (cl-revenue-ops.py:7980-7985) ->
//! `BoltzCliManager.swap_history` (`modules/boltz_manager.py:2435-2456`).
//! `swaps` must already be the fully extracted + journal-augmented +
//! journal-/ignore-annotated list (`_listswaps_json` -> `_extract_swap_list`
//! -> `_augment_with_swap_journal` -> per-entry `_annotate_journal_swap`/
//! `_annotate_ignored_swap`, boltz_manager.py:2436-2442) -- all I/O
//! (subprocess + journal file reads), the caller's job per
//! `crates/revops-boltz/ENTRYPOINTS.md` ("swap_history's full read-path
//! composition ... belongs with a live adapter"). This builder does the
//! PURE remainder: sort newest-first, apply `limit`, and compute
//! `cost_summary` using the fully-ported `revops_boltz::fee`/`::state`
//! kernels.

use revops_boltz::fee::estimate_swap_fee_sats;
use revops_boltz::parsing::parse_timestamp;
use revops_boltz::state::is_completed_swap;
use serde_json::{json, Map, Value};

/// py `_swap_created_ts` (boltz_manager.py:1010-1015): `createdAt`/
/// `updatedAt`/`created_at`/`updated_at`, first match wins. Duplicated
/// here (rather than imported) because `revops_boltz::budget`'s copy is
/// private to that module -- see `crates/revops-boltz/src/budget.rs:71-79`
/// for the byte-identical upstream definition this must stay in sync with.
fn swap_created_ts(swap: &Map<String, Value>) -> Option<i64> {
    for key in ["createdAt", "updatedAt", "created_at", "updated_at"] {
        if let Some(v) = swap.get(key) {
            if let Some(ts) = parse_timestamp(v) {
                return Some(ts);
            }
        }
    }
    None
}

/// Port of `swap_history`'s success path (boltz_manager.py:2435-2456).
/// `swaps` order does not matter -- this re-sorts by created-ts
/// descending, matching py 2444's `swaps.sort(..., reverse=True)`.
pub fn build_boltz_history(mut swaps: Vec<Map<String, Value>>, limit: Option<i64>) -> Value {
    swaps.sort_by_key(|s| std::cmp::Reverse(swap_created_ts(s).unwrap_or(0)));

    if let Some(limit) = limit {
        // py 2446-2450: `max(0, int(limit))`; a non-coercible limit is a
        // no-op in Python (`except: pass`) -- not reachable here since
        // `limit` is already a typed `Option<i64>` at this boundary.
        let lim = limit.max(0) as usize;
        swaps.truncate(lim);
    }

    let swap_count = swaps.len();
    let estimated_total_fee_sats: i64 = swaps
        .iter()
        .map(|s| estimate_swap_fee_sats(&Value::Object(s.clone())))
        .sum();
    let completed_count = swaps.iter().filter(|s| is_completed_swap(s)).count();

    json!({
        "swaps": swaps.into_iter().map(Value::Object).collect::<Vec<_>>(),
        "cost_summary": {
            "swap_count": swap_count,
            "estimated_total_fee_sats": estimated_total_fee_sats,
            "completed_count": completed_count,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // `created` is added to a real 2024-ish base so `parse_timestamp`'s
    // pre-2001 sentinel guard (`revops_boltz::parsing::parse_timestamp`)
    // never rejects these as boltzd zero-timestamp sentinels -- a raw small
    // int like `100` would be silently treated as "no timestamp", making
    // every fixture below sort-order-equal and defeating the test.
    const BASE_TS: i64 = 1_700_000_000;

    fn swap(id: &str, created_offset: i64, status: &str, fee: i64) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("id".into(), json!(id));
        m.insert("createdAt".into(), json!(BASE_TS + created_offset));
        m.insert("status".into(), json!(status));
        m.insert("boltzFee".into(), json!(fee));
        m
    }

    #[test]
    fn sorts_newest_first() {
        let swaps = vec![
            swap("old", 100, "swap.expired", 0),
            swap("new", 200, "swap.expired", 0),
        ];
        let v = build_boltz_history(swaps, None);
        assert_eq!(v["swaps"][0]["id"], "new");
        assert_eq!(v["swaps"][1]["id"], "old");
    }

    #[test]
    fn limit_truncates_after_sorting() {
        let swaps = vec![
            swap("a", 100, "swap.expired", 0),
            swap("b", 300, "swap.expired", 0),
            swap("c", 200, "swap.expired", 0),
        ];
        let v = build_boltz_history(swaps, Some(2));
        let ids: Vec<&str> = v["swaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["b", "c"]);
        assert_eq!(v["cost_summary"]["swap_count"], 2);
    }

    #[test]
    fn cost_summary_uses_ported_fee_and_completion_kernels() {
        let swaps = vec![
            swap("done", 100, "transaction.claimed", 10),
            swap("pending", 200, "invoice.set", 5),
        ];
        let v = build_boltz_history(swaps, None);
        assert_eq!(v["cost_summary"]["estimated_total_fee_sats"], 15);
        assert_eq!(
            v["cost_summary"]["completed_count"], 1,
            "only the claimed swap counts as completed"
        );
    }

    #[test]
    fn negative_limit_clamps_to_zero_not_underflow() {
        let swaps = vec![swap("a", 100, "swap.expired", 0)];
        let v = build_boltz_history(swaps, Some(-5));
        assert_eq!(v["swaps"], json!([]));
        assert_eq!(v["cost_summary"]["swap_count"], 0);
    }
}
