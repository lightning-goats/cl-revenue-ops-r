//! `LNPlusError` (py `lnplus_swaps.py:61-69`) — the one error type every
//! [`crate::ports::LnPlusApi`] method returns on failure.
//!
//! Python carries `http_status: Optional[int]` and, since the 2026-07-08
//! audit (C-4), a best-effort parsed `errors: Optional[Dict]` — the `Dict`
//! from a 422 body's `"errors"` key (e.g. `{"id": ["already applied"]}`).
//! Both fields exist so callers can do a STRUCTURAL check instead of the
//! substring-over-everything anti-pattern this module is infamous for (see
//! [`crate::rating::rating_already_filed`] for defect #2 and
//! [`crate::withdrawal::classify_delete_application_error`] for defect #3).

use std::collections::BTreeMap;
use std::fmt;

/// Parsed field -> messages map from a 422 body's `"errors"` key. Kept as
/// `BTreeMap<String, Vec<String>>` rather than raw `serde_json::Value` so
/// every consumer works with the same normalized shape Python's
/// `isinstance(parsed.get("errors"), dict)` guard produces (values coerced
/// to a message list — LN+ error values are either a single string or a
/// list of strings; either shape flattens to `Vec<String>` here).
pub type ErrorsMap = BTreeMap<String, Vec<String>>;

/// `LNPlusError` (py 61-69). `message` is the human-readable summary Python
/// would pass to `Exception.__init__`; `http_status`/`errors` are the two
/// structured fields callers key gate decisions off of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LnPlusError {
    pub message: String,
    pub http_status: Option<u16>,
    pub errors: Option<ErrorsMap>,
    /// Task 61 4B: `true` iff the failure happened AFTER the request may
    /// have reached LN+ (response timeout / connection reset mid-exchange)
    /// — the submit's outcome is UNKNOWN, not cleanly failed. Producers:
    /// the transport mapping (4C) and test fakes. Consumers must
    /// quarantine, never treat as a clean not-submitted failure.
    pub outcome_unknown: bool,
}

impl LnPlusError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            http_status: None,
            errors: None,
            outcome_unknown: false,
        }
    }

    /// A post-submit-ambiguous failure (see the `outcome_unknown` field).
    pub fn unknown_outcome(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            http_status: None,
            errors: None,
            outcome_unknown: true,
        }
    }

    pub fn is_outcome_unknown(&self) -> bool {
        self.outcome_unknown
    }

    pub fn with_status(message: impl Into<String>, http_status: u16) -> Self {
        Self {
            message: message.into(),
            http_status: Some(http_status),
            errors: None,
            outcome_unknown: false,
        }
    }

    pub fn with_errors(message: impl Into<String>, http_status: u16, errors: ErrorsMap) -> Self {
        Self {
            message: message.into(),
            http_status: Some(http_status),
            errors: Some(errors),
            outcome_unknown: false,
        }
    }

    /// Every message string across every field in `errors`, structurally
    /// — i.e. only the values LN+ itself put in the parsed `errors` dict,
    /// never the exception's free-text `message` or any other field. This
    /// is the building block both `rating.rs` and `withdrawal.rs` use
    /// instead of Python's `str(errors or e).lower()` whole-blob scan.
    pub fn error_messages(&self) -> Vec<&str> {
        match &self.errors {
            Some(map) => map.values().flatten().map(String::as_str).collect(),
            None => Vec::new(),
        }
    }

    /// True when `http_status == expected` AND at least one message across
    /// every `errors` field contains `needle` (case-insensitive). This is
    /// the STRUCTURAL match primitive: status-gated, and scoped to the
    /// parsed per-field message list rather than the raw response body or
    /// the exception's stringification.
    pub fn structural_contains(&self, expected: u16, needle: &str) -> bool {
        if self.http_status != Some(expected) {
            return false;
        }
        let needle = needle.to_ascii_lowercase();
        self.error_messages()
            .iter()
            .any(|m| m.to_ascii_lowercase().contains(&needle))
    }
}

impl fmt::Display for LnPlusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for LnPlusError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn errors(pairs: &[(&str, &[&str])]) -> ErrorsMap {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    #[test]
    fn structural_contains_requires_matching_status() {
        let e = LnPlusError::with_errors("boom", 422, errors(&[("id", &["already applied"])]));
        assert!(e.structural_contains(422, "already"));
        assert!(!e.structural_contains(400, "already"));
    }

    #[test]
    fn structural_contains_ignores_free_text_message() {
        // The free-text `message` says "already" but there is no parsed
        // errors dict at all (e.g. a non-JSON or non-dict body) -- this
        // must NOT match, unlike a `str(e).lower()` scan would.
        let e = LnPlusError::with_status("LN+ already told us to go away", 422);
        assert!(!e.structural_contains(422, "already"));
    }
}
