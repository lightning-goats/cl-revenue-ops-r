//! Integration tests for `revops_econ::reconcile`, porting
//! `cl_revenue_ops-port/tests/test_econ_reconcile.py` plus the conformance
//! corpus scenario named in the Task 6 brief:
//!
//! - scenario 27 (`27-boltz-timeout-after-acceptance/case.json`): a stale
//!   `execution_started`-without-terminal key must quarantine as
//!   `unknown_outcome` (never auto-resolved), NOT surface as `db_missing`.
//!
//! Corpus VALUES are transcribed here per repo convention (copy the values
//! into tests; the corpus itself is not vendored).

use std::collections::BTreeMap;

use revops_econ::ledger::EconLedger;
use revops_econ::reconcile::{apply, fee_intent_completeness, reconcile, DbReservationState};
use serde_json::{json, Value};
use tempfile::TempDir;

const KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // "a" * 64
const KEY_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; // "b" * 64
const KEY_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"; // "c" * 64
const KEY_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"; // "d" * 64
const NOW: i64 = 1_752_400_000;

fn new_ledger(dir: &TempDir) -> EconLedger {
    EconLedger::open(dir.path().join("econ_ledger.db")).expect("open ledger")
}

fn amounts_of(pairs: &[(&str, i64)]) -> BTreeMap<String, i64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

/// Mirrors the Python test helper `_append(ledger, event_type, key=KEY,
/// at=NOW, amounts=None, details=None)`.
fn append(
    ledger: &EconLedger,
    event_type: &str,
    key: &str,
    at: i64,
    amounts: &[(&str, i64)],
) -> i64 {
    let take = key.chars().take(16).collect::<String>();
    ledger
        .append(
            event_type,
            &take,
            key,
            "spend-test",
            at,
            &amounts_of(amounts),
            &json!({}),
        )
        .expect("append should succeed")
}

fn active(reserved_sats: i64) -> DbReservationState {
    DbReservationState {
        status: "active".to_string(),
        reserved_sats,
    }
}

fn terminal(status: &str, reserved_sats: i64) -> DbReservationState {
    DbReservationState {
        status: status.to_string(),
        reserved_sats,
    }
}

#[test]
fn matched_state_reports_clean() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW,
        &[("reserved_msat", 3_000)],
    );
    let mut db_states = BTreeMap::new();
    db_states.insert(KEY.to_string(), active(3));
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    assert_eq!(report.matched, 1);
    assert!(report.divergences.is_empty());
}

#[test]
fn ledger_stale_reservation() {
    // DB settled/released but ledger still shows outstanding (the
    // mid-stream-disable gap).
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW,
        &[("reserved_msat", 3_000)],
    );
    let mut db_states = BTreeMap::new();
    db_states.insert(KEY.to_string(), terminal("released", 3));
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    let kinds: Vec<&str> = report.divergences.iter().map(|d| d.kind.as_str()).collect();
    assert_eq!(kinds, vec!["ledger_stale_reservation"]);
    assert!(report.divergences[0].resolution.is_some());
}

#[test]
fn ledger_missing_reservation() {
    // DB has an active reservation the ledger never saw.
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    let mut db_states = BTreeMap::new();
    db_states.insert(KEY.to_string(), active(4));
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    let kinds: Vec<&str> = report.divergences.iter().map(|d| d.kind.as_str()).collect();
    assert_eq!(kinds, vec!["ledger_missing_reservation"]);
}

#[test]
fn db_missing() {
    // Ledger shows outstanding; DB has no such row.
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW,
        &[("reserved_msat", 3_000)],
    );
    let db_states = BTreeMap::new();
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    let kinds: Vec<&str> = report.divergences.iter().map(|d| d.kind.as_str()).collect();
    assert_eq!(kinds, vec!["db_missing"]);
}

#[test]
fn amount_mismatch() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW,
        &[("reserved_msat", 3_000)],
    );
    let mut db_states = BTreeMap::new();
    db_states.insert(KEY.to_string(), active(5));
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    let kinds: Vec<&str> = report.divergences.iter().map(|d| d.kind.as_str()).collect();
    assert_eq!(kinds, vec!["amount_mismatch"]);
    assert_eq!(
        report.divergences[0]
            .resolution
            .as_ref()
            .unwrap()
            .get("reserved_msat")
            .unwrap(),
        &json!(5_000)
    );
}

#[test]
fn unknown_outcome_quarantined_not_resolved() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW - 7200,
        &[("reserved_msat", 3_000)],
    );
    append(&ledger, "execution_started", KEY, NOW - 7200, &[]);
    let mut db_states = BTreeMap::new();
    db_states.insert(KEY.to_string(), active(3));
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    let kinds: Vec<&str> = report.divergences.iter().map(|d| d.kind.as_str()).collect();
    assert!(kinds.contains(&"unknown_outcome"));
    let unknown = report
        .divergences
        .iter()
        .find(|d| d.kind == "unknown_outcome")
        .unwrap();
    assert!(unknown.resolution.is_none()); // quarantine: never auto-resolved
}

/// Corpus scenario 27 (`27-boltz-timeout-after-acceptance/case.json`):
/// ```json
/// {
///   "inputs": {"age_seconds": 7200, "lifecycle": ["execution_started"],
///              "stale_after_seconds": 3600},
///   "expected": {"quarantine_when_stale": true,
///                "resolvable_as_db_missing": false}
/// }
/// ```
/// An in-flight (started-without-terminal) key that has gone stale must
/// quarantine as `unknown_outcome` — it must NEVER be auto-zeroed as
/// `db_missing`, even with an empty db_states map (no DB row at all).
#[test]
fn corpus_scenario_27_stale_quarantine_not_db_missing() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    let age_seconds = 7200;
    let stale_after_seconds = 3600;
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW - age_seconds,
        &[("reserved_msat", 3_000)],
    );
    append(&ledger, "execution_started", KEY, NOW - age_seconds, &[]);
    let db_states = BTreeMap::new(); // empty: no spend_reservations row at all
    let report = reconcile(&ledger, &db_states, NOW, stale_after_seconds).unwrap();
    let kinds: Vec<&str> = report.divergences.iter().map(|d| d.kind.as_str()).collect();
    assert_eq!(kinds, vec!["unknown_outcome"]); // quarantine_when_stale: true
    assert!(!kinds.contains(&"db_missing")); // resolvable_as_db_missing: false
    assert!(report.divergences[0].resolution.is_none());
}

#[test]
fn recent_execution_started_not_flagged() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW,
        &[("reserved_msat", 3_000)],
    );
    append(&ledger, "execution_started", KEY, NOW - 60, &[]);
    let mut db_states = BTreeMap::new();
    db_states.insert(KEY.to_string(), active(3));
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    assert!(report.divergences.is_empty());
}

#[test]
fn intent_only_keys_ignored() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(&ledger, "intent_proposed", KEY, NOW, &[]); // fee-cycle shadow intent
    let db_states = BTreeMap::new();
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    assert_eq!(report.checked, 0);
    assert!(report.divergences.is_empty());
}

#[test]
fn apply_then_reconcile_is_clean() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY_B,
        NOW,
        &[("reserved_msat", 3_000)],
    ); // stale (db released)
    append(
        &ledger,
        "budget_reserved",
        KEY_C,
        NOW,
        &[("reserved_msat", 2_000)],
    ); // mismatch (db 5)
    let mut db_states = BTreeMap::new();
    db_states.insert(KEY_B.to_string(), terminal("released", 3));
    db_states.insert(KEY_C.to_string(), active(5));
    db_states.insert(KEY_D.to_string(), active(7)); // missing

    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    assert_eq!(report.divergences.len(), 3);
    let applied = apply(&ledger, &report, NOW).unwrap();
    assert_eq!(applied, 3);

    let report2 = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    assert!(report2.divergences.is_empty());
    assert_eq!(report2.matched, 2); // c and d now match; b terminal-cleared

    let state = ledger.replay().unwrap();
    let mut expected = BTreeMap::new();
    expected.insert(KEY_C.to_string(), 5_000);
    expected.insert(KEY_D.to_string(), 7_000);
    assert_eq!(state.reserved_msat, expected);
}

#[test]
fn in_flight_reservation_retained_even_without_db_row() {
    // Spec: ambiguous outcomes RETAIN the reservation. A started execution
    // with no DB row must NOT be auto-zeroed as db_missing — fresh
    // in-flight is silent; stale surfaces only as quarantine.
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW - 60,
        &[("reserved_msat", 3_000)],
    );
    append(&ledger, "execution_started", KEY, NOW - 60, &[]);
    let db_states = BTreeMap::new();

    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap(); // fresh: nothing reported
    assert!(report.divergences.is_empty());

    let report_stale = reconcile(&ledger, &db_states, NOW + 7200, 3600).unwrap();
    let kinds: Vec<&str> = report_stale
        .divergences
        .iter()
        .map(|d| d.kind.as_str())
        .collect();
    assert_eq!(kinds, vec!["unknown_outcome"]);
    assert!(report_stale.divergences[0].resolution.is_none());
    assert_eq!(apply(&ledger, &report_stale, NOW + 7200).unwrap(), 0);
}

#[test]
fn apply_skips_quarantined() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append(
        &ledger,
        "budget_reserved",
        KEY,
        NOW - 7200,
        &[("reserved_msat", 3_000)],
    );
    append(&ledger, "execution_started", KEY, NOW - 7200, &[]);
    let mut db_states = BTreeMap::new();
    db_states.insert(KEY.to_string(), active(3));
    let report = reconcile(&ledger, &db_states, NOW, 3600).unwrap();
    assert_eq!(apply(&ledger, &report, NOW).unwrap(), 0);
}

mod fee_intent_completeness_tests {
    use super::*;

    fn intent(ledger: &EconLedger, cycle_ts: i64, n: usize) {
        for i in 0..n {
            ledger
                .append(
                    "intent_proposed",
                    &format!("int-{cycle_ts}-{i}"),
                    &format!("{cycle_ts:032}{i:032}"),
                    &format!("fee-cycle-{cycle_ts}"),
                    cycle_ts,
                    &BTreeMap::new(),
                    &json!({}),
                )
                .unwrap();
        }
    }

    fn changes(rows: &[i64]) -> Vec<Value> {
        rows.iter().map(|ts| json!({"timestamp": ts})).collect()
    }

    #[test]
    fn complete_capture() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        intent(&ledger, NOW - 1800, 4);
        intent(&ledger, NOW, 2);
        let mut rows = vec![NOW - 1800; 4];
        rows.extend(vec![NOW + 3; 2]); // rows a moment later
        let result = fee_intent_completeness(&ledger, &changes(&rows), NOW, 86400, 120).unwrap();
        assert_eq!(result["complete"], json!(true));
        assert_eq!(result["cycles_checked"], json!(2));
    }

    #[test]
    fn missing_cycle_flagged() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        intent(&ledger, NOW - 3600, 4);
        let mut rows = vec![NOW - 3600; 4];
        rows.extend(vec![NOW; 3]); // cycle never journaled
        let result = fee_intent_completeness(&ledger, &changes(&rows), NOW, 86400, 120).unwrap();
        assert_eq!(result["complete"], json!(false));
        assert_eq!(
            result["mismatched_cycles"][NOW.to_string()],
            json!({"fee_changes": 3, "intents": 0})
        );
    }

    #[test]
    fn pre_shadow_history_out_of_scope() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        intent(&ledger, NOW, 2);
        let mut rows = vec![NOW - 50_000; 5]; // pre-shadow
        rows.extend(vec![NOW; 2]);
        let result = fee_intent_completeness(&ledger, &changes(&rows), NOW, 86400, 120).unwrap();
        assert_eq!(result["complete"], json!(true));
        assert_eq!(result["cycles_checked"], json!(1));
    }

    #[test]
    fn no_intent_data() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        let result = fee_intent_completeness(&ledger, &changes(&[NOW]), NOW, 86400, 120).unwrap();
        assert_eq!(result["status"], json!("no_intent_data"));
    }

    #[test]
    fn governed_per_broadcast_intents_count() {
        // Phase 2H: governed mode records one fee-broadcast-<ts> intent
        // per setchannel; the detector must count them like cycle batches.
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        for i in 0..3i64 {
            ledger
                .append(
                    "intent_proposed",
                    &format!("int-b{i}"),
                    &format!("{i:064}"),
                    &format!("fee-broadcast-{}", NOW + i),
                    NOW + i,
                    &BTreeMap::new(),
                    &json!({}),
                )
                .unwrap();
        }
        let rows = vec![NOW, NOW + 1, NOW + 2];
        let result = fee_intent_completeness(&ledger, &changes(&rows), NOW, 86400, 120).unwrap();
        assert_eq!(result["complete"], json!(true));
    }

    /// Live false-positive 2026-07-12: one cycle's 8 fee_changes rows
    /// landed as 3 rows at :41 + 5 rows at :42 against 8 ledgered intents
    /// — clustering within `tolerance_seconds` must treat this as ONE
    /// cycle, not two, or it false-positives as incomplete.
    #[test]
    fn changes_straddling_seconds_are_one_cycle() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        intent(&ledger, NOW - 2, 8);
        let mut rows = vec![NOW; 3]; // 3 rows at :41
        rows.extend(vec![NOW + 1; 5]); // 5 rows at :42
        let result = fee_intent_completeness(&ledger, &changes(&rows), NOW, 86400, 120).unwrap();
        assert_eq!(result["complete"], json!(true));
        assert_eq!(result["cycles_checked"], json!(1));
    }
}
