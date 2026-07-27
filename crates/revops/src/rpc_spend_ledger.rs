//! Pure response builder for `revenue-r-spend-ledger`.
//!
//! Port of `revenue_spend_ledger` (cl-revenue-ops.py:7780-7796), which is
//! a thin wrapper over `Database.get_spend_ledger_summary`
//! (modules/database.py:4483-4581): plain `SUM`/`COUNT`/`GROUP BY`
//! aggregates over the `spend_events`/`spend_reservations` tables (the
//! same tables `revops_db::budget::BudgetDb` models, though that module is
//! a WRITE rail over the plugin's own shadow DB, not a read query against
//! the production DB handle -- see `crates/revops/RPC_BATCH_A.md` for the
//! new read-only `revops-db` query this needs).
//!
//! `coverage_hours`/`coverage_status` (`Database._coverage_from_earliest`,
//! called via `_earliest_evidence_timestamp`) are NOT ported: that helper
//! measures how much of the requested window is actually backed by
//! ledger evidence, a separate small algorithm this batch does not carry
//! forward. They are therefore always `null`, gap-listed -- never a fake
//! "full coverage" claim.

use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Already-fetched `spend_events`/`spend_reservations` aggregates for one
/// window (`Database.get_spend_ledger_summary`'s SQL results,
/// modules/database.py:4493-4527).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SpendLedgerAggregates {
    pub spent_24h_sats: i64,
    pub reserved_24h_sats: i64,
    pub spent_by_category: BTreeMap<String, i64>,
    pub reserved_by_category: BTreeMap<String, i64>,
    pub event_count_by_category: BTreeMap<String, i64>,
    pub active_reservation_count_by_category: BTreeMap<String, i64>,
}

/// One row of `spend_reservations` (only included when
/// `include_reservations=true`, modules/database.py:4557-4580).
#[derive(Debug, Clone, PartialEq)]
pub struct ActiveReservation {
    pub reservation_id: String,
    pub category: String,
    pub subcategory: Option<String>,
    pub reserved_sats: i64,
    pub reserved_at: i64,
    pub reference_id: Option<String>,
    pub channel_id: Option<String>,
    pub status: String,
    pub metadata_json: Option<String>,
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
    let mut out = json!({
        "timestamp": generated_at,
        "generated_at": generated_at,
        "ttl_seconds": 1800,
        "window_hours": window_hours,
        "coverage_hours": Value::Null,
        "covered_hours": Value::Null,
        "coverage_status": Value::Null,
        "spent_24h_sats": aggregates.spent_24h_sats,
        "reserved_24h_sats": aggregates.reserved_24h_sats,
        "spent_by_category": aggregates.spent_by_category,
        "reserved_by_category": aggregates.reserved_by_category,
        "event_count_by_category": aggregates.event_count_by_category,
        "active_reservation_count_by_category": aggregates.active_reservation_count_by_category,
        "_gaps": ["coverage_hours", "covered_hours", "coverage_status"],
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
        }
    }

    #[test]
    fn wires_aggregates_and_gaps_coverage_fields() {
        let agg = sample_aggregates();
        let v = build_spend_ledger(24, 1_700_000_000, &agg, None);
        assert_eq!(v["spent_24h_sats"], 6500);
        assert_eq!(v["reserved_24h_sats"], 200);
        assert_eq!(v["spent_by_category"]["rebalance"], 1500);
        assert_eq!(v["event_count_by_category"]["rebalance"], 3);
        assert_eq!(v["coverage_hours"], Value::Null);
        assert_eq!(v["coverage_status"], Value::Null);
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert_eq!(
            gaps,
            vec!["coverage_hours", "covered_hours", "coverage_status"]
        );
        assert!(v.get("active_reservations").is_none());
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
