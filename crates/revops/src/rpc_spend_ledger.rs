//! Pure response builder for `revenue-r-spend-ledger`.
//!
//! Port of `revenue_spend_ledger` (cl-revenue-ops.py:7780-7796), which is
//! a thin wrapper over `Database.get_spend_ledger_summary`
//! (modules/database.py:4483-4581): plain `SUM`/`COUNT`/`GROUP BY`
//! aggregates over the `spend_events`/`spend_reservations` tables (the
//! same tables `revops_db::budget::BudgetDb` models, though that module is
//! a WRITE rail over the plugin's own shadow DB, not a read query against
//! the production DB handle). The typed read path lives in
//! `revops_db::queries`; see `crates/revops/RPC_BATCH_A.md` for wiring.
//!
//! `coverage_hours`/`coverage_status` are measured by the DB-owned query
//! from the oldest positive spend-event or reservation timestamp. No evidence
//! is the explicit `null`/`unknown` result; it is not a declared gap and is
//! never promoted to fabricated full coverage.

use crate::rpc_params::{is_truthy_py, python_int};
use serde_json::{json, Value};

/// Shared DB-owned ledger contracts. Re-exported to preserve this builder's
/// public API while plugin wiring fetches the values from `revops_db::queries`.
pub use revops_db::queries::{ActiveReservation, SpendLedgerAggregates};

/// Task 50 correction round, F7: `revenue_spend_ledger`'s
/// `window_hours: int = 24` (cl-revenue-ops.py:7780-7796) then unconditional
/// `int(window_hours)` inside a try/except -- an ABSENT key binds the
/// literal default `24` directly (never coerced, can't error); an EXPLICIT
/// value (any JSON type) always goes through `int()`. `Database
/// .get_spend_ledger_summary` then applies `max(1, int(window_hours))` --
/// floored at 1, deliberately NO upper clamp (contrast
/// `rpc_total_cost_budget::parse_window_hours`'s `[1,168]` for a DIFFERENT
/// RPC). The OLD wiring was `.and_then(as_i64).unwrap_or(24).max(1)`,
/// which silently substitutes the default for ANY non-JSON-number
/// `window_hours` (a numeric STRING like `"48"`, a garbage string, `null`)
/// instead of coercing or erroring -- "a confident ledger for the wrong
/// window", per the audit.
///
/// `Ok(n)` (already floored at 1, no ceiling) on success; `Err(message)`
/// (Python's own `int()` exception text) on garbage, for the caller to
/// wrap as `{"error": message}` matching Python's in-band style.
pub fn parse_window_hours(raw: Option<&Value>) -> Result<i64, String> {
    let n = match raw {
        None => 24,
        Some(v) => python_int(v)?,
    };
    Ok(n.max(1))
}

/// Same convention as [`parse_window_hours`] for `reservation_limit: int =
/// 50` (also always `int()`-coerced, then `max(1, ...)` inside
/// `get_spend_ledger_summary`, only when `include_reservations` is true --
/// but coerced unconditionally by the outer wrapper regardless).
pub fn parse_reservation_limit(raw: Option<&Value>) -> Result<i64, String> {
    let n = match raw {
        None => 50,
        Some(v) => python_int(v)?,
    };
    Ok(n.max(1))
}

/// `include_reservations: bool = False` (cl-revenue-ops.py:7783), then
/// `bool(include_reservations)` -- Python TRUTHINESS, not a strict JSON
/// bool parse: `bool("false")` is `True` (a non-empty string is always
/// truthy, whatever it says). The OLD wiring's `.and_then(as_bool)`
/// silently dropped anything that wasn't a literal JSON `true`/`false`
/// (including the string `"true"`) to the `unwrap_or(false)` default --
/// this instead matches Python's actual (admittedly surprising)
/// truthiness rule byte-for-byte via [`is_truthy_py`].
pub fn parse_include_reservations(raw: Option<&Value>) -> bool {
    match raw {
        None => false,
        Some(v) => is_truthy_py(v),
    }
}

/// Port of `Database.get_spend_ledger_summary`. `window_hours` is the
/// (already-clamped, `max(1, int(window_hours))`) request window;
/// `generated_at` stands in for Python's `int(time.time())`;
/// `reservations: Some(rows)` mirrors `include_reservations=true`.
pub fn build_spend_ledger(
    window_hours: i64,
    generated_at: i64,
    aggregates: &SpendLedgerAggregates,
    reservations: Option<&[ActiveReservation]>,
) -> Value {
    let covered_hours = match (
        aggregates.coverage_status.as_str(),
        aggregates.covered_hours,
    ) {
        ("complete", Some(_)) => json!(window_hours),
        (_, Some(hours)) => json!(hours),
        (_, None) => Value::Null,
    };
    let mut out = json!({
        "timestamp": generated_at,
        "generated_at": generated_at,
        "ttl_seconds": 1800,
        "window_hours": window_hours,
        "coverage_hours": covered_hours,
        "covered_hours": covered_hours,
        "coverage_status": aggregates.coverage_status,
        "spent_24h_sats": aggregates.spent_24h_sats,
        "reserved_24h_sats": aggregates.reserved_24h_sats,
        "spent_by_category": aggregates.spent_by_category,
        "reserved_by_category": aggregates.reserved_by_category,
        "event_count_by_category": aggregates.event_count_by_category,
        "active_reservation_count_by_category": aggregates.active_reservation_count_by_category,
        "_gaps": [],
    });

    if let Some(rows) = reservations {
        let now = generated_at;
        let active: Vec<Value> = rows
            .iter()
            .map(|r| {
                json!({
                    "reservation_id": r.reservation_id,
                    "category": r.category,
                    "subcategory": r.subcategory,
                    "reserved_sats": r.reserved_sats,
                    "reserved_at": r.reserved_at,
                    "age_seconds": (now - r.reserved_at).max(0),
                    "reference_id": r.reference_id,
                    "channel_id": r.channel_id,
                    "status": r.status,
                    "metadata_json": r.metadata_json,
                })
            })
            .collect();
        out["active_reservations"] = Value::Array(active);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn parse_window_hours_absent_defaults_to_24() {
        assert_eq!(parse_window_hours(None), Ok(24));
    }

    #[test]
    fn parse_window_hours_numeric_string_coerces_like_python_int() {
        assert_eq!(parse_window_hours(Some(&json!("48"))), Ok(48));
    }

    #[test]
    fn parse_window_hours_float_truncates() {
        assert_eq!(parse_window_hours(Some(&json!(48.9))), Ok(48));
    }

    #[test]
    fn parse_window_hours_floors_at_one_never_clamps_the_top() {
        assert_eq!(parse_window_hours(Some(&json!(0))), Ok(1));
        assert_eq!(parse_window_hours(Some(&json!(-5))), Ok(1));
        // Deliberately NO upper clamp -- unlike
        // rpc_total_cost_budget::parse_window_hours's [1,168].
        assert_eq!(parse_window_hours(Some(&json!(999))), Ok(999));
    }

    #[test]
    fn parse_window_hours_garbage_is_an_error_not_a_silent_default() {
        let err = parse_window_hours(Some(&json!("abc"))).unwrap_err();
        assert!(err.contains("invalid literal for int()"));
    }

    #[test]
    fn parse_window_hours_explicit_null_errors_like_python_int_none() {
        // Explicit `null` is NOT the same as an absent key: Python's
        // `int(None)` raises; only an absent key binds the signature
        // default directly.
        assert!(parse_window_hours(Some(&Value::Null)).is_err());
    }

    #[test]
    fn parse_reservation_limit_absent_defaults_to_50_floors_at_one() {
        assert_eq!(parse_reservation_limit(None), Ok(50));
        assert_eq!(parse_reservation_limit(Some(&json!(0))), Ok(1));
    }

    #[test]
    fn parse_include_reservations_matches_python_truthiness() {
        assert!(!parse_include_reservations(None));
        assert!(!parse_include_reservations(Some(&json!(false))));
        assert!(!parse_include_reservations(Some(&json!(""))));
        assert!(parse_include_reservations(Some(&json!(true))));
        // The exact quirk the audit calls out.
        assert!(parse_include_reservations(Some(&json!("false"))));
    }

    fn sample_aggregates() -> SpendLedgerAggregates {
        let mut spent_by_category = BTreeMap::new();
        spent_by_category.insert("rebalance".to_string(), 1500);
        spent_by_category.insert("channel_open".to_string(), 5000);
        let mut event_count_by_category = BTreeMap::new();
        event_count_by_category.insert("rebalance".to_string(), 3);
        SpendLedgerAggregates {
            spent_24h_sats: 6500,
            reserved_24h_sats: 200,
            spent_by_category,
            reserved_by_category: BTreeMap::new(),
            event_count_by_category,
            active_reservation_count_by_category: BTreeMap::new(),
            covered_hours: Some(24.0),
            coverage_status: "complete".to_string(),
        }
    }

    #[test]
    fn wires_aggregates_and_measured_coverage() {
        let agg = sample_aggregates();
        let v = build_spend_ledger(24, 1_700_000_000, &agg, None);
        assert_eq!(v["spent_24h_sats"], 6500);
        assert_eq!(v["reserved_24h_sats"], 200);
        assert_eq!(v["spent_by_category"]["rebalance"], 1500);
        assert_eq!(v["event_count_by_category"]["rebalance"], 3);
        assert_eq!(v["coverage_hours"], 24);
        assert_eq!(v["covered_hours"], 24);
        assert_eq!(v["coverage_status"], "complete");
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert!(gaps.is_empty(), "measured coverage has no declared gap");
        assert!(v.get("active_reservations").is_none());
    }

    #[test]
    fn unknown_coverage_is_explicit_null_not_a_declared_gap() {
        let agg = SpendLedgerAggregates {
            coverage_status: "unknown".to_string(),
            ..SpendLedgerAggregates::default()
        };
        let v = build_spend_ledger(24, 1_700_000_000, &agg, None);
        assert_eq!(v["coverage_hours"], Value::Null);
        assert_eq!(v["covered_hours"], Value::Null);
        assert_eq!(v["coverage_status"], "unknown");
        assert_eq!(v["_gaps"], json!([]));
    }

    #[test]
    fn include_reservations_computes_age_from_generated_at() {
        let agg = sample_aggregates();
        let rows = vec![ActiveReservation {
            reservation_id: "r1".to_string(),
            category: "rebalance".to_string(),
            subcategory: None,
            reserved_sats: 500,
            reserved_at: 1_700_000_000 - 90,
            reference_id: None,
            channel_id: Some("123x1x0".to_string()),
            status: "active".to_string(),
            metadata_json: None,
        }];
        let v = build_spend_ledger(24, 1_700_000_000, &agg, Some(&rows));
        let reservations = v["active_reservations"].as_array().unwrap();
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0]["age_seconds"], 90);
        assert_eq!(reservations[0]["channel_id"], "123x1x0");
    }

    #[test]
    fn age_seconds_never_goes_negative() {
        let agg = sample_aggregates();
        let rows = vec![ActiveReservation {
            reservation_id: "r1".to_string(),
            category: "rebalance".to_string(),
            subcategory: None,
            reserved_sats: 500,
            // reserved_at AFTER generated_at (clock skew) -- Python's
            // `max(0, now - reserved_at)` floors this at 0.
            reserved_at: 1_700_000_100,
            reference_id: None,
            channel_id: None,
            status: "active".to_string(),
            metadata_json: None,
        }];
        let v = build_spend_ledger(24, 1_700_000_000, &agg, Some(&rows));
        assert_eq!(v["active_reservations"][0]["age_seconds"], 0);
    }

    #[test]
    fn ttl_and_timestamp_fields_match_python_constants() {
        let agg = sample_aggregates();
        let v = build_spend_ledger(1, 42, &agg, None);
        assert_eq!(v["ttl_seconds"], 1800);
        assert_eq!(v["timestamp"], 42);
        assert_eq!(v["generated_at"], 42);
        assert_eq!(v["window_hours"], 1);
    }
}
