//! Swap journal: pure pruning, indexing, and result-merge logic.
//!
//! Ports py `_prune_swap_journal_entries` (boltz_manager.py:1241-1260),
//! `_load_swap_journal_index` (boltz_manager.py:1321-1330), and the
//! id-indexed merge core of `_record_swap_result_locked`
//! (boltz_manager.py:1438-1504) — everything except the actual file I/O
//! (py `_load_swap_journal`/`_save_swap_journal`, boltz_manager.py:
//! 1224-1270) and the capex-engine spend recording side effect, both of
//! which are the live adapter's job (see `ENTRYPOINTS.md`).
//!
//! Journal entries are modeled as `serde_json::Map` rather than a fixed
//! struct because the Python journal is a schemaless dict merged from
//! whatever `metadata` a caller passes plus boltzcli's own swap fields —
//! a fixed struct would either drop fields silently or need to be kept in
//! lockstep with every future metadata key by hand.

use crate::fee::estimate_swap_fee_sats;
use crate::parsing::parse_int;
use crate::state::is_error_swap;
use serde_json::{Map, Value};

/// py `_SWAP_JOURNAL_MAX_AGE_SECONDS` (boltz_manager.py:1238): 180 days.
pub const SWAP_JOURNAL_MAX_AGE_SECONDS: i64 = 180 * 24 * 60 * 60;
/// py `_SWAP_JOURNAL_MAX_ENTRIES` (boltz_manager.py:1239).
pub const SWAP_JOURNAL_MAX_ENTRIES: usize = 200;

/// py `_SPEND_RECORD_SOURCES` (boltz_manager.py:1431): only these sources
/// correspond to actually executing a swap (creation paths); status
/// probes and journal lookups must not double-spend the budget.
pub const SPEND_RECORD_SOURCES: &[&str] = &[
    "loop_in",
    "loop_out",
    "loop_out_external_create",
    "chainswap",
];

/// py `_prune_swap_journal_entries` (boltz_manager.py:1241-1260): drop
/// entries older than the retention window (measured by the newest of
/// `recorded_at`/`created_at`; entries with no usable timestamp are kept —
/// age unknown, not assumed stale), then cap the count at the last N.
pub fn prune_swap_journal_entries(
    entries: &[Map<String, Value>],
    now: i64,
) -> Vec<Map<String, Value>> {
    let cutoff = now - SWAP_JOURNAL_MAX_AGE_SECONDS;
    let mut kept: Vec<Map<String, Value>> = Vec::new();
    for rec in entries {
        let recorded_at = rec.get("recorded_at").map(|v| parse_int(v, 0)).unwrap_or(0);
        let created_at = rec.get("created_at").map(|v| parse_int(v, 0)).unwrap_or(0);
        let newest = recorded_at.max(created_at);
        if newest != 0 && newest < cutoff {
            continue; // older than the retention window
        }
        kept.push(rec.clone());
    }
    if kept.len() > SWAP_JOURNAL_MAX_ENTRIES {
        let drop = kept.len() - SWAP_JOURNAL_MAX_ENTRIES;
        kept.drain(0..drop);
    }
    kept
}

/// py `_load_swap_journal_index` (boltz_manager.py:1321-1330): index
/// journal entries by their `id` field (last write wins, matching a dict
/// comprehension over a list).
pub fn index_by_id(
    entries: &[Map<String, Value>],
) -> std::collections::HashMap<String, Map<String, Value>> {
    let mut out = std::collections::HashMap::new();
    for rec in entries {
        let sid = rec
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if sid.is_empty() {
            continue;
        }
        out.insert(sid, rec.clone());
    }
    out
}

/// Whether recording this swap's fee should also deplete the capex
/// "boltz" spend category. Ports the condition guard at
/// boltz_manager.py:1470-1475 (`source in _SPEND_RECORD_SOURCES and fee >
/// 0 and not self._is_error_swap(s)`), minus the `self._capex_engine is
/// not None` half (a live-wiring concern, not a decision this pure
/// function can see).
pub fn should_record_spend(source: &str, fee_sats: i64, swap: &Map<String, Value>) -> bool {
    SPEND_RECORD_SOURCES.contains(&source) && fee_sats > 0 && !is_error_swap(swap)
}

/// One journal-merge outcome for a single swap entry: the updated record
/// to persist, plus whether a capex spend event should be recorded for it
/// (the live adapter performs that side effect; this function only
/// decides whether it applies).
#[derive(Debug, Clone, PartialEq)]
pub struct MergedSwapRecord {
    pub id: String,
    pub record: Map<String, Value>,
    pub fee_sats: i64,
    pub should_record_spend: bool,
}

/// Core of py `_record_swap_result_locked` (boltz_manager.py:1438-1504),
/// minus capex-engine cost attribution (that lives in `budget.rs`/the
/// live adapter) and file I/O. Given the existing journal entries, the
/// swaps extracted from a boltzcli result payload, the recording
/// `source`, optional `metadata` to merge in, and the current time,
/// returns one [`MergedSwapRecord`] per swap with an `id`.
///
/// Swaps with no `id` are skipped (py: `if not sid: continue`).
pub fn merge_swap_results(
    existing: &[Map<String, Value>],
    swaps: &[Map<String, Value>],
    source: &str,
    metadata: Option<&Map<String, Value>>,
    now: i64,
) -> Vec<MergedSwapRecord> {
    let by_id = index_by_id(existing);
    let mut out = Vec::new();
    for s in swaps {
        let sid = match s.get("id").and_then(|v| v.as_str()) {
            Some(id) if !id.trim().is_empty() => id.trim().to_string(),
            _ => continue,
        };
        let mut rec = by_id.get(&sid).cloned().unwrap_or_default();
        rec.insert("id".to_string(), Value::String(sid.clone()));
        rec.insert("recorded_at".to_string(), Value::from(now));
        rec.insert("source".to_string(), Value::String(source.to_string()));
        if let Some(created) = s
            .get("createdAt")
            .or_else(|| s.get("created_at"))
            .and_then(crate::parsing::parse_timestamp)
        {
            rec.insert("created_at".to_string(), Value::from(created));
        }
        if let Some(meta) = metadata {
            for (k, v) in meta {
                if !v.is_null() {
                    rec.insert(k.clone(), v.clone());
                }
            }
        }
        let fee = estimate_swap_fee_sats(&Value::Object(s.clone()));
        out.push(MergedSwapRecord {
            id: sid,
            should_record_spend: should_record_spend(source, fee, s),
            fee_sats: fee,
            record: rec,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn prune_drops_entries_older_than_180_days() {
        let now = 1_700_000_000i64;
        let old = map(json!({"id": "old", "recorded_at": now - SWAP_JOURNAL_MAX_AGE_SECONDS - 10}));
        let fresh = map(json!({"id": "fresh", "recorded_at": now - 10}));
        let kept = prune_swap_journal_entries(&[old, fresh.clone()], now);
        assert_eq!(kept, vec![fresh]);
    }

    #[test]
    fn prune_keeps_entries_with_unknown_timestamp() {
        let now = 1_700_000_000i64;
        let unknown = map(json!({"id": "unknown"}));
        let kept = prune_swap_journal_entries(std::slice::from_ref(&unknown), now);
        assert_eq!(kept, vec![unknown]);
    }

    #[test]
    fn prune_caps_at_max_entries_keeping_newest() {
        let now = 1_700_000_000i64;
        let entries: Vec<Map<String, Value>> = (0..(SWAP_JOURNAL_MAX_ENTRIES + 5))
            .map(|i| map(json!({"id": format!("s{i}"), "recorded_at": now})))
            .collect();
        let kept = prune_swap_journal_entries(&entries, now);
        assert_eq!(kept.len(), SWAP_JOURNAL_MAX_ENTRIES);
        // The oldest-by-insertion-order (first 5) are the ones dropped.
        assert_eq!(kept.first().unwrap().get("id").unwrap(), "s5");
    }

    #[test]
    fn should_record_spend_true_for_executed_create_source() {
        let s = map(json!({"id": "a", "state": "pending"}));
        assert!(should_record_spend("loop_out", 100, &s));
    }

    #[test]
    fn should_record_spend_false_for_status_probe_source() {
        // Control: a status-lookup source must never deplete the budget,
        // even with a positive fee and no error.
        let s = map(json!({"id": "a", "state": "pending"}));
        assert!(!should_record_spend("swap_status_lookup", 100, &s));
    }

    #[test]
    fn should_record_spend_false_for_zero_fee() {
        let s = map(json!({"id": "a", "state": "pending"}));
        assert!(!should_record_spend("loop_out", 0, &s));
    }

    #[test]
    fn should_record_spend_false_for_error_swap() {
        let s = map(json!({"id": "a", "error": "failed"}));
        assert!(!should_record_spend("loop_out", 100, &s));
    }

    #[test]
    fn merge_swap_results_skips_entries_without_id() {
        let swaps = vec![map(json!({"state": "pending"}))];
        let merged = merge_swap_results(&[], &swaps, "loop_out", None, 1000);
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_swap_results_preserves_existing_fields_and_updates_recorded_at() {
        let existing = vec![map(json!({"id": "x", "note": "keep-me", "recorded_at": 1}))];
        let swaps = vec![map(json!({"id": "x", "boltzFee": 42}))];
        let merged = merge_swap_results(&existing, &swaps, "loop_out", None, 2000);
        assert_eq!(merged.len(), 1);
        let rec = &merged[0].record;
        assert_eq!(rec.get("note").unwrap(), "keep-me");
        assert_eq!(rec.get("recorded_at").unwrap(), 2000);
        assert_eq!(merged[0].fee_sats, 42);
        assert!(merged[0].should_record_spend);
    }

    #[test]
    fn merge_swap_results_applies_metadata_overrides() {
        let mut meta = Map::new();
        meta.insert(
            "trigger_channel_id".to_string(),
            Value::String("123x1x0".to_string()),
        );
        let swaps = vec![map(json!({"id": "y"}))];
        let merged = merge_swap_results(&[], &swaps, "loop_in", Some(&meta), 5000);
        assert_eq!(
            merged[0].record.get("trigger_channel_id").unwrap(),
            "123x1x0"
        );
    }
}
