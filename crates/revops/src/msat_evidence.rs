//! Shared msat-shape validation for money-evidence boundaries.
//!
//! `revops_core::msat::parse_msat` is deliberately PERMISSIVE — it returns
//! 0 for null, bools, arrays, objects and unparseable strings. That is the
//! right global contract for a tolerant reader, and it is not changed here.
//!
//! But at a boundary where the number becomes a claim about the operator's
//! money — a balance, a net worth, an exposure figure — permissiveness
//! fabricates evidence: `amount_msat: "garbage"` silently becomes a
//! confident zero. So required fields are shape-validated FIRST and only
//! then handed to the canonical parser, which stays the sole
//! accepted-format authority.
//!
//! Extracted from `econ_evidence` (F71-R6/R7) so `capital_evidence`
//! (F71-R9) reuses it rather than growing a third copy — the same
//! duplication F71-R2 caught in the P&L path.

use revops_core::msat::parse_msat;
use serde_json::Value;

/// Is this a valid, non-negative, in-range CLN msat representation?
///
/// Accepted, matching what CLN actually emits: a JSON integer, or a string
/// of digits with an optional `msat` suffix.
///
/// Rejected: null, bools, arrays, objects, unparseable strings, NEGATIVE
/// values (balances are non-negative, and a negative would be silently
/// clamped to zero by the floor conversion), and values above `i64::MAX`
/// (lossy through the permissive parser). The `-` sign is rejected
/// outright rather than parsed then tested, which is what stops `"-1msat"`
/// slipping through the string branch.
fn is_valid_msat(v: &Value) -> bool {
    match v {
        Value::Number(n) => n.as_u64().is_some_and(|u| i64::try_from(u).is_ok()),
        Value::String(s) => {
            let body = s.trim().strip_suffix("msat").unwrap_or(s.trim());
            !body.is_empty()
                && body.bytes().all(|b| b.is_ascii_digit())
                && body.parse::<i64>().is_ok()
        }
        _ => false,
    }
}

/// Validate then parse. `Err` carries a human-readable reason naming the
/// field, for the caller to wrap in its own typed refusal.
///
/// A VALID zero stays a measured zero: the refusal keys on the value being
/// unusable, never on it being zero. An empty channel and a corrupt one are
/// different facts.
pub fn validated_msat(v: &Value, what: &str) -> Result<i64, String> {
    if !is_valid_msat(v) {
        return Err(format!(
            "{what} is not a valid non-negative msat value: {v}"
        ));
    }
    Ok(parse_msat(v))
}

/// Fetch a REQUIRED msat field from a row, then validate and parse it.
pub fn required_msat(row: &Value, field: &str, context: &str) -> Result<i64, String> {
    let raw = row
        .get(field)
        .ok_or_else(|| format!("{context} has no {field}"))?;
    validated_msat(raw, &format!("{context} {field}"))
}

/// Fetch a REQUIRED non-empty string field from a row.
///
/// Defaulting these to `""` is how a malformed row becomes a plausible one:
/// an empty `peer_id` still counts toward exposure, and an empty `state`
/// silently fails every state comparison rather than announcing itself.
pub fn required_str(row: &Value, field: &str, context: &str) -> Result<String, String> {
    match row.get(field) {
        None => Err(format!("{context} has no {field}")),
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        Some(other) => Err(format!(
            "{context} {field} is not a non-empty string: {other}"
        )),
    }
}
