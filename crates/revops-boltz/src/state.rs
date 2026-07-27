//! Swap status text classification.
//!
//! Ports py `_swap_status_text`/`_is_completed_swap`/`_swap_entry_error_text`/
//! `_is_error_swap`/`_is_terminal_swap` (boltz_manager.py:1028-1204).
//!
//! The load-bearing subtlety (documented in the Python and repeated in
//! `docs/port/port-map.json`'s `rust_considerations` for `BoltzCliManager`):
//! `"abandoned"` CONTAINS the substring `"done"`, but an abandoned swap is
//! terminal and NOT completed — its estimated fee must never count as
//! spend. The abandoned check MUST run before the completed-token check.

use serde_json::{Map, Value};

fn get_str_lower(swap: &Map<String, Value>, key: &str) -> Option<String> {
    match swap.get(key) {
        Some(Value::String(s)) => Some(s.to_lowercase()),
        _ => None,
    }
}

/// py `_swap_status_text` (boltz_manager.py:1028-1029): `state` field,
/// falling back to `status`, lowercased; empty string if neither present.
pub fn swap_status_text(swap: &Map<String, Value>) -> String {
    get_str_lower(swap, "state")
        .or_else(|| get_str_lower(swap, "status"))
        .unwrap_or_default()
}

/// py `_is_completed_swap` (boltz_manager.py:1031-1037).
pub fn is_completed_swap(swap: &Map<String, Value>) -> bool {
    let st = swap_status_text(swap);
    if st.contains("abandoned") {
        // "abandoned" contains "done" — terminal but NOT completed.
        return false;
    }
    ["success", "completed", "claimed", "done"]
        .iter()
        .any(|token| st.contains(token))
}

/// py `_swap_entry_error_text` (boltz_manager.py:1183-1186).
pub fn swap_entry_error_text(swap: &Map<String, Value>) -> String {
    match swap.get("error") {
        Some(Value::String(s)) => s.trim().to_string(),
        _ => String::new(),
    }
}

/// py `_is_error_swap` (boltz_manager.py:1188-1194).
pub fn is_error_swap(swap: &Map<String, Value>) -> bool {
    if !swap_entry_error_text(swap).is_empty() {
        return true;
    }
    let st = swap_status_text(swap);
    ["error", "failed"].iter().any(|token| st.contains(token))
}

/// py `_is_terminal_swap` (boltz_manager.py:1196-1203): completed, error,
/// refunded, or abandoned.
pub fn is_terminal_swap(swap: &Map<String, Value>) -> bool {
    if is_completed_swap(swap) || is_error_swap(swap) {
        return true;
    }
    let st = swap_status_text(swap);
    ["refund", "abandon"].iter().any(|token| st.contains(token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn swap(fields: Value) -> Map<String, Value> {
        fields.as_object().unwrap().clone()
    }

    #[test]
    fn abandoned_is_not_completed_despite_containing_done() {
        let s = swap(json!({"state": "swap.abandoned"}));
        assert!(
            !is_completed_swap(&s),
            "abandoned must never count as completed spend"
        );
        // Control: abandoned IS terminal.
        assert!(is_terminal_swap(&s));
    }

    #[test]
    fn plain_done_is_completed() {
        let s = swap(json!({"state": "transaction.done"}));
        assert!(is_completed_swap(&s));
    }

    #[test]
    fn success_completed_claimed_are_completed() {
        for token in ["success", "swap.completed", "invoice.claimed"] {
            let s = swap(json!({"status": token}));
            assert!(is_completed_swap(&s), "{token} should be completed");
        }
    }

    #[test]
    fn pending_is_not_completed_or_terminal() {
        let s = swap(json!({"state": "swap.created"}));
        assert!(!is_completed_swap(&s));
        assert!(!is_terminal_swap(&s));
    }

    #[test]
    fn explicit_error_field_marks_error_and_terminal() {
        let s = swap(json!({"state": "pending", "error": "boom"}));
        assert!(is_error_swap(&s));
        assert!(is_terminal_swap(&s));
    }

    #[test]
    fn failed_status_marks_error() {
        let s = swap(json!({"status": "transaction.failed"}));
        assert!(is_error_swap(&s));
    }

    #[test]
    fn refund_and_abandon_are_terminal_not_completed() {
        for token in ["transaction.refunded", "swap.abandoned"] {
            let s = swap(json!({"state": token}));
            assert!(is_terminal_swap(&s));
            assert!(!is_completed_swap(&s));
        }
    }

    #[test]
    fn state_field_takes_precedence_over_status() {
        let s = swap(json!({"state": "swap.created", "status": "swap.completed"}));
        assert_eq!(swap_status_text(&s), "swap.created");
        assert!(!is_completed_swap(&s));
    }
}
