//! Integration tests for `revops_econ::ledger`, porting
//! `cl_revenue_ops-port/tests/test_econ_ledger.py` plus the conformance
//! corpus scenarios named in the Task 4 brief:
//!
//! - scenario 25 (`25-missing-execution-cost/case.json`): orphan cost
//!   anomaly.
//! - scenario 26 (`26-unknown-execution-outcome/case.json`): unknown-outcome
//!   terminal with reservation preserved.
//! - scenario 40 (`40-sanitized-production-decisions/`): replaying a real,
//!   sanitized production lifecycle reproduces the reference projection
//!   exactly (`expected-ledger-events.json` -> `expected-projections.json`).
//!
//! Corpus VALUES are transcribed here per the task brief ("copy the VALUES
//! into your tests (do not vendor the corpus; that's Task 9's job)") from
//! `cl_revenue_ops-port/tests/conformance/scenarios/{25,26,40}-*/*.json`.

use std::collections::BTreeMap;

use revops_econ::ledger::{EconLedger, EVENT_TYPES};
use serde_json::{json, Value};
use tempfile::TempDir;

const KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // "a" * 64
const KEY2: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"; // "b" * 64
const DEFAULT_AT: i64 = 1_752_400_000;

fn new_ledger(dir: &TempDir) -> EconLedger {
    EconLedger::open(dir.path().join("econ_ledger.db")).expect("open ledger")
}

fn amounts_of(pairs: &[(&str, i64)]) -> BTreeMap<String, i64> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

/// Mirrors the Python test helper `_append(ledger, event_type, key=KEY,
/// at=1_752_400_000, amounts=None, details=None)`.
fn append(
    ledger: &EconLedger,
    event_type: &str,
    key: &str,
    at: i64,
    amounts: &BTreeMap<String, i64>,
    details: &Value,
) -> i64 {
    let take = key.len().min(16);
    let intent_id = format!("int-{}", &key[..take]);
    ledger
        .append(
            event_type,
            &intent_id,
            key,
            "cycle-000001",
            at,
            amounts,
            details,
        )
        .expect("append should succeed")
}

fn append_simple(ledger: &EconLedger, event_type: &str, key: &str) -> i64 {
    append(
        ledger,
        event_type,
        key,
        DEFAULT_AT,
        &BTreeMap::new(),
        &json!({}),
    )
}

fn append_with_amounts(
    ledger: &EconLedger,
    event_type: &str,
    key: &str,
    amounts: &[(&str, i64)],
) -> i64 {
    append(
        ledger,
        event_type,
        key,
        DEFAULT_AT,
        &amounts_of(amounts),
        &json!({}),
    )
}

fn append_with_amounts_and_details(
    ledger: &EconLedger,
    event_type: &str,
    key: &str,
    amounts: &[(&str, i64)],
    details: Value,
) -> i64 {
    append(
        ledger,
        event_type,
        key,
        DEFAULT_AT,
        &amounts_of(amounts),
        &details,
    )
}

#[test]
fn vocabulary_is_exactly_the_spec() {
    let expected: std::collections::BTreeSet<&str> = [
        "intent_proposed",
        "intent_rejected",
        "intent_deferred",
        "intent_authorized",
        "budget_reserved",
        "execution_started",
        "execution_succeeded",
        "execution_failed",
        "execution_outcome_unknown",
        "cost_recorded",
        "reservation_released",
        "reconciliation_completed",
        "snapshot_created",
    ]
    .into_iter()
    .collect();
    let got: std::collections::BTreeSet<&str> = EVENT_TYPES.into_iter().collect();
    assert_eq!(got, expected);
}

#[test]
fn append_and_ordered_readback() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    let id1 = append_simple(&ledger, "intent_proposed", KEY);
    let id2 = append_simple(&ledger, "intent_authorized", KEY);
    assert!(id2 > id1);
    let events = ledger.events(0).unwrap();
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(types, vec!["intent_proposed", "intent_authorized"]);
    assert_eq!(events[0].idempotency_key, KEY);
}

#[test]
fn invalid_event_type_rejected() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    let result = ledger.append(
        "made_up_event",
        "int-aaaaaaaaaaaaaaaa",
        KEY,
        "cycle-000001",
        DEFAULT_AT,
        &BTreeMap::new(),
        &json!({}),
    );
    assert!(result.is_err());
}

#[test]
fn empty_ids_rejected() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    let result = ledger.append(
        "intent_proposed",
        "",
        KEY,
        "cycle-000001",
        DEFAULT_AT,
        &BTreeMap::new(),
        &json!({}),
    );
    assert!(result.is_err());
}

#[test]
fn negative_at_rejected() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    let result = ledger.append(
        "intent_proposed",
        "int-aaaaaaaaaaaaaaaa",
        KEY,
        "cycle-000001",
        -1,
        &BTreeMap::new(),
        &json!({}),
    );
    assert!(result.is_err());
}

#[test]
fn existing_rows_immutable_across_appends() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append_simple(&ledger, "intent_proposed", KEY);
    let before = ledger.events(0).unwrap();
    append_simple(&ledger, "intent_authorized", KEY);
    let after = ledger.events(0).unwrap();
    assert_eq!(&after[..before.len()], &before[..]);
    // No update/delete surface: `EconLedger` exposes only
    // open/append/events/count_events/replay (a compile-time fact for the
    // Rust port, unlike Python's runtime `hasattr` check).
}

#[test]
fn opening_twice_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("econ_ledger.db");
    let ledger1 = EconLedger::open(&path).unwrap();
    append_simple(&ledger1, "intent_proposed", KEY);
    let ledger2 = EconLedger::open(&path).unwrap();
    assert_eq!(ledger2.events(0).unwrap().len(), 1);
}

#[test]
fn count_events_total_and_by_type() {
    let dir = TempDir::new().unwrap();
    let ledger = new_ledger(&dir);
    append_simple(&ledger, "intent_proposed", KEY);
    append_simple(&ledger, "intent_authorized", KEY);
    append_simple(&ledger, "intent_authorized", KEY2);
    assert_eq!(ledger.count_events(None).unwrap(), 3);
    assert_eq!(ledger.count_events(Some("intent_authorized")).unwrap(), 2);
    assert_eq!(ledger.count_events(Some("intent_proposed")).unwrap(), 1);
    assert_eq!(ledger.count_events(Some("cost_recorded")).unwrap(), 0);
}

#[test]
fn ledger_usable_across_threads() {
    // Regression for the 2026-07-12 production finding: the fee loop, RPC
    // handlers, and spend hooks all touch the ledger from different
    // threads; a fresh connection per operation must not drop events.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("econ_ledger.db");
    let ledger = EconLedger::open(&path).unwrap();

    std::thread::scope(|scope| {
        for i in 0..8 {
            let ledger_ref = &ledger;
            scope.spawn(move || {
                let key = format!("{i:064}");
                append_simple(ledger_ref, "intent_proposed", &key);
            });
        }
    });

    assert_eq!(ledger.events(0).unwrap().len(), 8);
}

mod replay {
    use super::*;

    #[test]
    fn full_lifecycle() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_simple(&ledger, "intent_proposed", KEY);
        append_simple(&ledger, "intent_authorized", KEY);
        append_with_amounts(&ledger, "budget_reserved", KEY, &[("reserved_msat", 5_000)]);
        append_simple(&ledger, "execution_started", KEY);
        append_simple(&ledger, "execution_succeeded", KEY);
        append_with_amounts(&ledger, "cost_recorded", KEY, &[("cost_msat", 3_000)]);
        append_simple(&ledger, "reservation_released", KEY);

        let state = ledger.replay().unwrap();
        assert_eq!(state.reserved_msat.get(KEY).copied().unwrap_or(0), 0);
        assert_eq!(state.spent_msat[KEY], 3_000);
        assert_eq!(state.total_spent_msat, 3_000);
        assert_eq!(state.terminal[KEY], "execution_succeeded");
        assert!(state.anomalies.is_empty());
    }

    #[test]
    fn reservation_outstanding() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(&ledger, "budget_reserved", KEY, &[("reserved_msat", 5_000)]);
        let state = ledger.replay().unwrap();
        assert_eq!(state.reserved_msat[KEY], 5_000);
        assert_eq!(state.total_spent_msat, 0);
    }

    #[test]
    fn duplicate_terminal_events_harmless() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(&ledger, "budget_reserved", KEY, &[("reserved_msat", 5_000)]);
        append_simple(&ledger, "execution_succeeded", KEY);
        append_simple(&ledger, "execution_succeeded", KEY); // duplicate callback
        append_with_amounts(&ledger, "cost_recorded", KEY, &[("cost_msat", 2_000)]);
        append_with_amounts(&ledger, "cost_recorded", KEY, &[("cost_msat", 2_000)]);

        let state = ledger.replay().unwrap();
        // Duplicate cost records ARE two records (corrections are new
        // events); duplicate terminal transitions are ignored.
        assert_eq!(state.terminal[KEY], "execution_succeeded");
        assert_eq!(state.spent_msat[KEY], 4_000);
    }

    #[test]
    fn duplicate_reservation_idempotent() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(&ledger, "budget_reserved", KEY, &[("reserved_msat", 5_000)]);
        append_with_amounts(&ledger, "budget_reserved", KEY, &[("reserved_msat", 5_000)]);
        let state = ledger.replay().unwrap();
        assert_eq!(state.reserved_msat[KEY], 5_000);
    }

    #[test]
    fn cost_without_reservation_is_anomalous_not_free() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(&ledger, "cost_recorded", KEY, &[("cost_msat", 7_000)]);
        let state = ledger.replay().unwrap();
        assert_eq!(state.spent_msat[KEY], 7_000); // never free budget
        assert_eq!(state.reserved_msat.get(KEY).copied().unwrap_or(0), 0); // never negative
        assert!(state.anomalies.iter().any(|a| a.contains(KEY)));
    }

    #[test]
    fn two_intents_tracked_independently() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(&ledger, "budget_reserved", KEY, &[("reserved_msat", 1_000)]);
        append_with_amounts(
            &ledger,
            "budget_reserved",
            KEY2,
            &[("reserved_msat", 2_000)],
        );
        append_simple(&ledger, "intent_rejected", KEY2);

        let state = ledger.replay().unwrap();
        assert_eq!(state.reserved_msat[KEY], 1_000);
        assert_eq!(
            state.terminal.get(KEY2).map(String::as_str),
            Some("intent_rejected")
        );
        assert!(!state.terminal.contains_key(KEY));
    }

    #[test]
    fn empty_ledger() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        let state = ledger.replay().unwrap();
        assert_eq!(state.total_spent_msat, 0);
        assert!(state.reserved_msat.is_empty());
        assert!(state.terminal.is_empty());
    }
}

/// Phase 2 pilot B: `reconciliation_completed` corrects replay state
/// (corrections are new events — the append-only rule).
mod reconciliation_replay {
    use super::*;

    #[test]
    fn reconciliation_zeroes_stale_reservation() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(&ledger, "budget_reserved", KEY, &[("reserved_msat", 5_000)]);
        append_with_amounts_and_details(
            &ledger,
            "reconciliation_completed",
            KEY,
            &[("reserved_msat", 0)],
            json!({"kind": "ledger_stale_reservation"}),
        );
        let state = ledger.replay().unwrap();
        assert!(state.reserved_msat.is_empty());
    }

    #[test]
    fn reconciliation_sets_missing_reservation() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts_and_details(
            &ledger,
            "reconciliation_completed",
            KEY,
            &[("reserved_msat", 4_000)],
            json!({"kind": "ledger_missing_reservation"}),
        );
        let state = ledger.replay().unwrap();
        let expected: BTreeMap<String, i64> = [(KEY.to_string(), 4_000)].into_iter().collect();
        assert_eq!(state.reserved_msat, expected);
    }

    #[test]
    fn reconciliation_cost_adds_spend_once() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(&ledger, "budget_reserved", KEY, &[("reserved_msat", 5_000)]);
        append_with_amounts_and_details(
            &ledger,
            "reconciliation_completed",
            KEY,
            &[("reserved_msat", 0), ("cost_msat", 3_000)],
            json!({"kind": "ledger_stale_reservation", "terminal": true}),
        );
        let state = ledger.replay().unwrap();
        let expected_spent: BTreeMap<String, i64> =
            [(KEY.to_string(), 3_000)].into_iter().collect();
        assert_eq!(state.spent_msat, expected_spent);
        assert!(state.reserved_msat.is_empty());
        let expected_terminal: BTreeMap<String, String> =
            [(KEY.to_string(), "reconciliation_completed".to_string())]
                .into_iter()
                .collect();
        assert_eq!(state.terminal, expected_terminal);
    }

    #[test]
    fn reconciliation_never_overwrites_terminal() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_simple(&ledger, "execution_succeeded", KEY);
        append_with_amounts_and_details(
            &ledger,
            "reconciliation_completed",
            KEY,
            &[("reserved_msat", 0)],
            json!({"terminal": true}),
        );
        let state = ledger.replay().unwrap();
        let expected: BTreeMap<String, String> =
            [(KEY.to_string(), "execution_succeeded".to_string())]
                .into_iter()
                .collect();
        assert_eq!(state.terminal, expected);
    }
}

/// Conformance corpus scenarios (values transcribed from
/// `cl_revenue_ops-port/tests/conformance/scenarios/{25,26,40}-*`).
mod corpus {
    use super::*;

    /// `25-missing-execution-cost/case.json`: DoD 6 — cost without
    /// reservation context is an ANOMALY, never silently absorbed.
    /// `expected.anomalies == ["cost_recorded without reservation: k-25"]`,
    /// `expected.spent_msat == {"k-25": 7000}`.
    #[test]
    fn scenario_25_missing_execution_cost() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(&ledger, "cost_recorded", "k-25", &[("cost_msat", 7_000)]);

        let state = ledger.replay().unwrap();
        assert_eq!(
            state.anomalies,
            vec!["cost_recorded without reservation: k-25".to_string()]
        );
        assert_eq!(state.spent_msat.get("k-25"), Some(&7_000));
    }

    /// `26-unknown-execution-outcome/case.json`: Workstream E — unknown
    /// outcome is a TERMINAL state pending reconciliation; reservation
    /// state preserved for the sweep. `inputs.lifecycle ==
    /// [budget_reserved, execution_started, execution_outcome_unknown]`;
    /// `expected.reserved_msat == {"k-26": 5000}`,
    /// `expected.terminal == {"k-26": "execution_outcome_unknown"}`.
    #[test]
    fn scenario_26_unknown_execution_outcome() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);
        append_with_amounts(
            &ledger,
            "budget_reserved",
            "k-26",
            &[("reserved_msat", 5_000)],
        );
        append_simple(&ledger, "execution_started", "k-26");
        append_simple(&ledger, "execution_outcome_unknown", "k-26");

        let state = ledger.replay().unwrap();
        assert_eq!(state.reserved_msat.get("k-26"), Some(&5_000));
        assert_eq!(
            state.terminal.get("k-26").map(String::as_str),
            Some("execution_outcome_unknown")
        );
    }

    /// `40-sanitized-production-decisions/`: DoD 17 — replaying real
    /// production lifecycles reproduces the reference ledger state.
    /// `expected-ledger-events.json` transcribed verbatim as three
    /// `append` calls (real `intent_id`/`cycle_id`/`at`/`idempotency_key`
    /// values, sanitized); `expected-projections.json` pins the replay
    /// output to all-empty/zero.
    #[test]
    fn scenario_40_sanitized_production_decisions() {
        let dir = TempDir::new().unwrap();
        let ledger = new_ledger(&dir);

        ledger
            .append(
                "budget_reserved",
                "sanitized",
                "504",
                "spend-rebalance",
                1_783_946_902,
                &amounts_of(&[("reserved_msat", 400_000)]),
                &json!({}),
            )
            .unwrap();
        ledger
            .append(
                "reservation_released",
                "sanitized",
                "504",
                "spend-generic",
                1_783_946_902,
                &BTreeMap::new(),
                &json!({"reason": "released"}),
            )
            .unwrap();
        ledger
            .append(
                "snapshot_created",
                "preview-1783962518",
                "preview-1783962518",
                "preview-1783962518",
                1_783_962_518,
                &BTreeMap::new(),
                &json!({"observed_at": 1_783_962_518}),
            )
            .unwrap();

        let state = ledger.replay().unwrap();
        // expected-projections.json: reserved_msat={}, spent_msat={},
        // total_spent_msat=0, terminal={}, anomalies=[].
        assert!(state.reserved_msat.is_empty());
        assert!(state.spent_msat.is_empty());
        assert_eq!(state.total_spent_msat, 0);
        assert!(state.terminal.is_empty());
        assert!(state.anomalies.is_empty());
    }
}
