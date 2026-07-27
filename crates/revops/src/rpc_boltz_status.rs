//! Pure response builder for `revenue-boltz-status`.
//!
//! Port of `revenue_boltz_status` (cl-revenue-ops.py:7969-7975) ->
//! `BoltzCliManager.swap_status` (`modules/boltz_manager.py:2394-2433`).
//! Per `crates/revops-boltz/ENTRYPOINTS.md`, `swap_status`'s composition is
//! "I/O-heavy ... with very little decision logic of its own": it shells
//! out to `boltzcli swapinfo`/`listswaps` and merges journal/ignore-list
//! annotations read from local files. Every field this builder emits is
//! therefore a pass-through of an already-fetched/-annotated value; the
//! only logic ported here is Python's own `bool(ignore_meta)` derivation
//! (py 2430) and the `swap_id` usage-error branch (py 7971-7973), which is
//! genuinely pure.

use serde_json::{json, Value};

/// Every already-fetched/-annotated input `swap_status` needs. `raw` is
/// the literal `boltzcli swapinfo -- <id>` stdout (py 2398, 2427) --
/// opaque text, passed through verbatim.
pub struct BoltzSwapStatusInputs {
    pub swap_id: String,
    pub swapinfo_raw: String,
    /// `_annotate_journal_swap(_annotate_ignored_swap(swapinfo_entry, ...), ...)`
    /// (py 2421), already computed by the caller. `None` when boltzcli's
    /// JSON reply had no matching entry (py 2409-2418's `swapinfo_entry =
    /// None` path).
    pub swapinfo_entry: Option<Value>,
    /// `_annotate_journal_swap(_annotate_ignored_swap(list_match, ...), ...)`
    /// (py 2422), from the separate `listswaps --json` lookup (py
    /// 2401-2406). `None` when not found in the list.
    pub listswaps_entry: Option<Value>,
    /// `ignores.get(swap_id)` (py 2423): the ignore-list metadata entry,
    /// if this swap is on the manual external-pay ignore list.
    pub ignore_meta: Option<Value>,
    /// `journal_index.get(swap_id)` (py 2424): the local swap journal
    /// entry, if one exists.
    pub journal_meta: Option<Value>,
}

/// Port of `swap_status`'s success path (boltz_manager.py:2425-2433).
pub fn build_boltz_status(i: &BoltzSwapStatusInputs) -> Value {
    json!({
        "swap_id": i.swap_id,
        "swapinfo_raw": i.swapinfo_raw,
        "swapinfo_entry": i.swapinfo_entry,
        "listswaps_entry": i.listswaps_entry,
        "ignored_external_swap": i.ignore_meta.is_some(),
        "ignored_external_swap_meta": i.ignore_meta,
        "journal_meta": i.journal_meta,
    })
}

/// Port of `revenue_boltz_status`'s usage-error branch (cl-revenue-ops.py:
/// 7971-7973): no `swap_id` supplied. Returns the exact Python error
/// string verbatim.
pub fn build_boltz_status_usage_error() -> Value {
    json!({
        "error": "usage: revenue-boltz-status swap_id (per-swap status; \
    see revenue-boltz-wallet/-budget/-history for global state)"
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_flag_is_true_only_when_meta_present() {
        let i = BoltzSwapStatusInputs {
            swap_id: "abc".to_string(),
            swapinfo_raw: "{}".to_string(),
            swapinfo_entry: None,
            listswaps_entry: None,
            ignore_meta: Some(json!({"note": "manual pay"})),
            journal_meta: None,
        };
        let v = build_boltz_status(&i);
        assert_eq!(v["ignored_external_swap"], true);
        assert_eq!(v["ignored_external_swap_meta"]["note"], "manual pay");
    }

    #[test]
    fn ignored_flag_is_false_when_meta_absent() {
        let i = BoltzSwapStatusInputs {
            swap_id: "abc".to_string(),
            swapinfo_raw: "{}".to_string(),
            swapinfo_entry: None,
            listswaps_entry: None,
            ignore_meta: None,
            journal_meta: None,
        };
        let v = build_boltz_status(&i);
        assert_eq!(v["ignored_external_swap"], false);
        assert_eq!(v["ignored_external_swap_meta"], Value::Null);
    }

    #[test]
    fn usage_error_matches_python_string_verbatim() {
        let v = build_boltz_status_usage_error();
        assert_eq!(
            v["error"],
            "usage: revenue-boltz-status swap_id (per-swap status; see revenue-boltz-wallet/-budget/-history for global state)"
        );
    }

    #[test]
    fn raw_and_entries_pass_through_unmodified() {
        let i = BoltzSwapStatusInputs {
            swap_id: "xyz".to_string(),
            swapinfo_raw: "{\"id\":\"xyz\"}".to_string(),
            swapinfo_entry: Some(json!({"id": "xyz", "status": "invoice.set"})),
            listswaps_entry: Some(json!({"id": "xyz"})),
            ignore_meta: None,
            journal_meta: Some(json!({"recorded_at": 1700000000})),
        };
        let v = build_boltz_status(&i);
        assert_eq!(v["swapinfo_raw"], "{\"id\":\"xyz\"}");
        assert_eq!(v["swapinfo_entry"]["status"], "invoice.set");
        assert_eq!(v["journal_meta"]["recorded_at"], 1700000000);
    }
}
