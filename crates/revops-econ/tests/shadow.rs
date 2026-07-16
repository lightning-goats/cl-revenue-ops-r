//! Integration tests for `revops_econ::shadow` — the `EconShadow` hub
//! itself (snapshot-ref TTL cache, spend journal, live arbitration
//! registry, reconciliation throttle). Pure-function unit tests (flag
//! tolerance table, `default_ledger_path`) live in `src/shadow.rs`,
//! mirroring the module/integration split already established by
//! `crate::arbiter`/`tests/arbiter.rs` and `crate::governor`/
//! `tests/governor.rs`.
//!
//! Ported from `cl_revenue_ops-port/tests/test_econ_shadow.py`'s
//! `TestSpendJournal` class, `tests/test_rebalance_snapshot_adoption.py`'s
//! `TestSnapshotRef`, `tests/test_live_arbitration.py`'s
//! `TestShadowAccessor`, and `tests/test_reconcile_automation.py` — the
//! Python test suites for the six methods Task 10 ports (see the scope
//! note in `src/shadow.rs`'s module doc comment for what's excluded).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use revops_econ::arbiter::ActiveIntentRegistry;
use revops_econ::intents::{make_intent, Explanation, IntentFields};
use revops_econ::reconcile::DbReservationState;
use revops_econ::shadow::{
    ClockFn, ConfigFlag, ConfigGetter, EconShadow, LogFn, ReconciliationInputs,
    ShadowConfigSnapshot, SnapshotRef,
};
use revops_econ::types::{EconError, EconResult, Micro, Msat, SignedMsat, UnixTime};
use serde_json::{json, Value};

const NOW: i64 = 1_752_400_000;

fn shadow(dir: &tempfile::TempDir, enabled: bool) -> EconShadow {
    shadow_with_arbiter(dir, enabled, false, false)
}

fn shadow_with_arbiter(
    dir: &tempfile::TempDir,
    enabled: bool,
    arbiter_enabled: bool,
    extended: bool,
) -> EconShadow {
    let config: ConfigGetter = Arc::new(move || ShadowConfigSnapshot {
        econ_shadow_enabled: ConfigFlag::Bool(enabled),
        econ_arbiter_enabled: ConfigFlag::Bool(arbiter_enabled),
        econ_conflict_rules_extended: ConfigFlag::Bool(extended),
    });
    let clock: ClockFn = Arc::new(|| NOW);
    let log: LogFn = Arc::new(|_msg: &str, _level: &str| {});
    EconShadow::new(config, clock, log, dir.path().join("econ_ledger.db"))
}

/// A shadow whose logs are observable, for tests that assert on specific
/// `(message, level)` pairs.
struct Harness {
    shadow: EconShadow,
    logs: Arc<Mutex<Vec<(String, String)>>>,
}

fn harness(dir: &tempfile::TempDir, enabled: bool) -> Harness {
    harness_with_clock(dir, enabled, NOW)
}

fn harness_with_clock(dir: &tempfile::TempDir, enabled: bool, clock_value: i64) -> Harness {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let logs_for_closure = Arc::clone(&logs);
    let config: ConfigGetter = Arc::new(move || ShadowConfigSnapshot {
        econ_shadow_enabled: ConfigFlag::Bool(enabled),
        econ_arbiter_enabled: ConfigFlag::Bool(false),
        econ_conflict_rules_extended: ConfigFlag::Bool(false),
    });
    let clock: ClockFn = Arc::new(move || clock_value);
    let log: LogFn = Arc::new(move |msg: &str, level: &str| {
        logs_for_closure
            .lock()
            .unwrap()
            .push((msg.to_string(), level.to_string()));
    });
    let shadow = EconShadow::new(config, clock, log, dir.path().join("econ_ledger.db"));
    Harness { shadow, logs }
}

// --- snapshot_ref TTL cache ---

#[test]
fn snapshot_ref_returns_ref_from_provider() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    s.set_snapshot_provider(|| {
        Some((json!({"snapshot_id": "snap-7", "observed_at": 123}), vec![]))
    });
    let r = s.snapshot_ref(NOW);
    assert_eq!(
        r,
        Some(SnapshotRef {
            snapshot_id: "snap-7".to_string(),
            observed_at: 123
        })
    );
}

#[test]
fn snapshot_ref_cached_within_max_age_299s() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    let calls = Arc::new(Mutex::new(0));
    let calls_c = Arc::clone(&calls);
    s.set_snapshot_provider(move || {
        *calls_c.lock().unwrap() += 1;
        Some((json!({"snapshot_id": "snap-1", "observed_at": NOW}), vec![]))
    });
    let first = s.snapshot_ref(NOW);
    let second = s.snapshot_ref(NOW + 299);
    assert_eq!(first, second);
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[test]
fn snapshot_ref_rebuilds_at_max_age_boundary_300s() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    let calls = Arc::new(Mutex::new(0));
    let calls_c = Arc::clone(&calls);
    s.set_snapshot_provider(move || {
        let n = {
            let mut c = calls_c.lock().unwrap();
            *c += 1;
            *c
        };
        Some((
            json!({"snapshot_id": format!("snap-{n}"), "observed_at": NOW}),
            vec![],
        ))
    });
    let first = s.snapshot_ref_with_max_age(NOW, 300).unwrap();
    let second = s.snapshot_ref_with_max_age(NOW + 300, 300).unwrap();
    assert_eq!(first.snapshot_id, "snap-1");
    assert_eq!(second.snapshot_id, "snap-2");
    assert_eq!(*calls.lock().unwrap(), 2);
}

#[test]
fn snapshot_ref_provider_error_fails_open() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    s.set_snapshot_provider(|| None);
    assert_eq!(s.snapshot_ref(NOW), None);
}

#[test]
fn snapshot_ref_no_provider_or_disabled_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    assert_eq!(s.snapshot_ref(NOW), None);

    let s2 = shadow(&dir, false);
    s2.set_snapshot_provider(|| Some((json!({"snapshot_id": "x"}), vec![])));
    assert_eq!(s2.snapshot_ref(NOW), None);
}

#[test]
fn snapshot_ref_empty_snapshot_id_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    s.set_snapshot_provider(|| Some((json!({"snapshot_id": ""}), vec![])));
    assert_eq!(s.snapshot_ref(NOW), None);
}

#[test]
fn snapshot_ref_fresh_build_ledgers_snapshot_created_once() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    s.set_snapshot_provider(|| {
        Some((json!({"snapshot_id": "snap-9", "observed_at": NOW}), vec![]))
    });
    s.snapshot_ref(NOW);
    s.snapshot_ref(NOW + 1); // cached -> no second event
    let ledger = s.ledger_for_reconciliation().unwrap();
    let events: Vec<_> = ledger
        .events(0)
        .unwrap()
        .into_iter()
        .filter(|e| e.event_type == "snapshot_created")
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].idempotency_key, "snap-9");
}

// --- spend journal ---

#[test]
fn spend_journal_disabled_records_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, false);
    s.note_spend_reserved("res-1", 3, "rebalance");
    s.note_spend_settled("res-1", 2, "rebalance");
    s.note_spend_released("res-1", "released");
    assert!(!dir.path().join("econ_ledger.db").exists());
}

#[test]
fn spend_journal_full_lifecycle_events_in_order_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    s.note_spend_reserved("res-1", 3, "rebalance");
    s.note_spend_settled("res-1", 2, "rebalance");

    let ledger = s.ledger_for_reconciliation().unwrap();
    let events = ledger.events(0).unwrap();
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        types,
        vec![
            "budget_reserved",
            "cost_recorded",
            "execution_succeeded",
            "reservation_released",
        ]
    );
    assert_eq!(events[0].cycle_id, "spend-rebalance");
    assert!(events.iter().all(|e| e.intent_id == "res-1"));
    assert!(events.iter().all(|e| e.idempotency_key == "res-1"));

    let state = ledger.replay().unwrap();
    assert!(state.reserved_msat.is_empty());
    assert_eq!(state.spent_msat.get("res-1"), Some(&2000));
    assert_eq!(
        state.terminal.get("res-1").map(String::as_str),
        Some("execution_succeeded")
    );
}

#[test]
fn spend_journal_release_only_replays_to_zero() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    s.note_spend_reserved("res-2", 5, "planner");
    s.note_spend_released("res-2", "stale");

    let ledger = s.ledger_for_reconciliation().unwrap();
    let events = ledger.events(0).unwrap();
    let last = events.last().unwrap();
    assert_eq!(last.event_type, "reservation_released");
    assert_eq!(
        last.details.get("reason").and_then(Value::as_str),
        Some("stale")
    );

    let state = ledger.replay().unwrap();
    assert!(state.reserved_msat.is_empty());
    assert!(state.spent_msat.is_empty());
}

#[test]
fn spend_journal_empty_reservation_id_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    s.note_spend_reserved("", 3, "planner");
    s.note_spend_reserved("   ", 3, "planner");
    assert!(!dir.path().join("econ_ledger.db").exists());
}

#[test]
fn spend_journal_overflowing_amount_skips_without_recording() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    s.note_spend_reserved("res-3", i64::MAX, "planner");
    // Nothing should ever have been appended for res-3 (and the ledger
    // file may not even exist, since the overflow is caught before any
    // ledger access is attempted).
    if let Some(ledger) = s.ledger_for_reconciliation() {
        let events = ledger.events(0).unwrap();
        assert!(events.iter().all(|e| e.intent_id != "res-3"));
    }
}

#[test]
fn spend_journal_uses_injected_clock_not_caller_now() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness_with_clock(&dir, true, 999_000_000);
    h.shadow.note_spend_reserved("res-4", 1, "planner");
    let ledger = h.shadow.ledger_for_reconciliation().unwrap();
    let events = ledger.events(0).unwrap();
    assert_eq!(events[0].at, 999_000_000);
}

// --- unwritable ledger path: warn logged once, not per-call ---

#[test]
fn unwritable_ledger_path_fails_open_and_warns_once() {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let logs_for_closure = Arc::clone(&logs);
    let config: ConfigGetter = Arc::new(|| ShadowConfigSnapshot {
        econ_shadow_enabled: ConfigFlag::Bool(true),
        econ_arbiter_enabled: ConfigFlag::Bool(false),
        econ_conflict_rules_extended: ConfigFlag::Bool(false),
    });
    let clock: ClockFn = Arc::new(|| NOW);
    let log: LogFn = Arc::new(move |msg: &str, level: &str| {
        logs_for_closure
            .lock()
            .unwrap()
            .push((msg.to_string(), level.to_string()));
    });
    let shadow = EconShadow::new(config, clock, log, "/nonexistent-dir/econ.db");

    shadow.note_spend_reserved("res-1", 1, "planner");
    shadow.note_spend_reserved("res-1", 1, "planner");
    let warns = logs
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, level)| level == "warn")
        .count();
    assert_eq!(warns, 1);
}

// --- arbitration_registry ---

#[test]
fn arbitration_registry_gated_off_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow_with_arbiter(&dir, true, false, false);
    assert!(s.arbitration_registry().is_none());
}

#[test]
fn arbitration_registry_strict_string_does_not_enable() {
    let dir = tempfile::tempdir().unwrap();
    let config: ConfigGetter = Arc::new(|| ShadowConfigSnapshot {
        econ_shadow_enabled: ConfigFlag::Bool(true),
        econ_arbiter_enabled: ConfigFlag::from("true"),
        econ_conflict_rules_extended: ConfigFlag::Bool(false),
    });
    let clock: ClockFn = Arc::new(|| NOW);
    let log: LogFn = Arc::new(|_: &str, _: &str| {});
    let s = EconShadow::new(config, clock, log, dir.path().join("econ_ledger.db"));
    assert!(s.arbitration_registry().is_none());
}

#[test]
fn arbitration_registry_is_singleton_across_calls() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow_with_arbiter(&dir, true, true, false);
    let a = s.arbitration_registry().unwrap() as *const ActiveIntentRegistry;
    let b = s.arbitration_registry().unwrap() as *const ActiveIntentRegistry;
    assert_eq!(a, b);
}

#[test]
fn arbitration_registry_reads_extended_rules_live() {
    let dir = tempfile::tempdir().unwrap();
    let extended = Arc::new(AtomicBool::new(false));
    let extended_c = Arc::clone(&extended);
    let config: ConfigGetter = Arc::new(move || ShadowConfigSnapshot {
        econ_shadow_enabled: ConfigFlag::Bool(true),
        econ_arbiter_enabled: ConfigFlag::Bool(true),
        econ_conflict_rules_extended: ConfigFlag::Bool(extended_c.load(Ordering::SeqCst)),
    });
    let clock: ClockFn = Arc::new(|| NOW);
    let log: LogFn = Arc::new(|_: &str, _: &str| {});
    let s = EconShadow::new(config, clock, log, dir.path().join("econ_ledger.db"));

    let make = |target: &str, kind: &str, snapshot_id: &str| {
        let created_at = UnixTime::new(NOW).unwrap();
        make_intent(IntentFields {
            intent_type: kind.to_string(),
            snapshot_id: snapshot_id.to_string(),
            created_at,
            expires_at: created_at.plus_seconds(600).unwrap(),
            target: target.to_string(),
            amount_msat: None,
            expected_benefit_msat: SignedMsat(0),
            max_cost_msat: Msat::new(0).unwrap(),
            capital_committed_msat: Msat::new(0).unwrap(),
            confidence_micro: Micro::new(0).unwrap(),
            reason_codes: vec![],
            explanation: Explanation {
                kind: "test".to_string(),
                components: vec![],
            },
            preconditions: vec![],
            priority: 50,
            budget_bucket: "b".to_string(),
            origin_policy: "test".to_string(),
            reversible: false,
        })
        .unwrap()
    };

    let registry = s.arbitration_registry().unwrap();
    // Different snapshot_ids -> different idempotency keys, so these two
    // probe the "duplicate OPEN_CHANNEL on the same target" rule
    // specifically, not the (always-active) duplicate-idempotency-key
    // rule.
    let open_a = make("100x1x0", "OPEN_CHANNEL", "snap-a");
    let open_b = make("100x1x0", "OPEN_CHANNEL", "snap-b");
    assert!(registry.check_and_register(&open_a, NOW).is_none());
    // Extended rules off: a second OPEN_CHANNEL on the same target is NOT
    // blocked by the legacy-only ruleset.
    assert!(registry.check_and_register(&open_b, NOW).is_none());
    registry.release(&open_b.idempotency_key);

    extended.store(true, Ordering::SeqCst);
    let open_c = make("100x1x0", "OPEN_CHANNEL", "snap-c");
    // Extended rules now on, read live (no restart): duplicate open is
    // blocked.
    assert!(registry.check_and_register(&open_c, NOW).is_some());
}

// --- maybe_run_reconciliation ---

fn ok_states() -> EconResult<BTreeMap<String, DbReservationState>> {
    Ok(BTreeMap::new())
}

fn ok_fee_changes(_limit: usize) -> EconResult<Vec<Value>> {
    Ok(vec![])
}

fn empty_inputs() -> ReconciliationInputs<'static> {
    ReconciliationInputs {
        spend_reservation_states: &ok_states,
        recent_fee_changes: &ok_fee_changes,
    }
}

#[test]
fn reconciliation_disabled_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, false);
    assert!(s
        .maybe_run_reconciliation(Some(&empty_inputs()), NOW, 3600)
        .is_none());
}

#[test]
fn reconciliation_missing_inputs_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    assert!(s.maybe_run_reconciliation(None, NOW, 3600).is_none());
}

#[test]
fn reconciliation_clean_sweep_and_throttle() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    let result = s
        .maybe_run_reconciliation(Some(&empty_inputs()), NOW, 3600)
        .unwrap();
    assert_eq!(result.divergences, 0);
    assert_eq!(result.applied, 0);

    // Throttled: within the hour returns None without re-running.
    assert!(s
        .maybe_run_reconciliation(Some(&empty_inputs()), NOW + 600, 3600)
        .is_none());
    // After the interval it runs again.
    assert!(s
        .maybe_run_reconciliation(Some(&empty_inputs()), NOW + 3601, 3600)
        .is_some());
}

#[test]
fn reconciliation_auto_applies_resolvable_divergence() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    // Ledger shows an outstanding reservation the "DB" has no record of at
    // all -> db_missing, auto-resolved to zero.
    s.note_spend_reserved("op-1", 3, "planner");

    let result = s
        .maybe_run_reconciliation(Some(&empty_inputs()), NOW, 3600)
        .unwrap();
    assert_eq!(result.divergences, 1);
    assert_eq!(result.applied, 1);

    let result2 = s
        .maybe_run_reconciliation(Some(&empty_inputs()), NOW + 3601, 3600)
        .unwrap();
    assert_eq!(result2.divergences, 0);
}

#[test]
fn reconciliation_quarantined_unknowns_warn_every_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(&dir, true);
    let ledger = h.shadow.ledger_for_reconciliation().unwrap();
    let key = "q".repeat(64);
    ledger
        .append(
            "budget_reserved",
            "x",
            &key,
            "spend-test",
            NOW - 7200,
            &BTreeMap::from([("reserved_msat".to_string(), 3000)]),
            &json!({}),
        )
        .unwrap();
    ledger
        .append(
            "execution_started",
            "x",
            &key,
            "spend-test",
            NOW - 7200,
            &BTreeMap::new(),
            &json!({}),
        )
        .unwrap();

    let result = h
        .shadow
        .maybe_run_reconciliation(Some(&empty_inputs()), NOW, 3600)
        .unwrap();
    assert_eq!(result.quarantined, 1);
    assert_eq!(result.applied, 0); // never auto-resolved

    let warns = h
        .logs
        .lock()
        .unwrap()
        .iter()
        .filter(|(msg, level)| level == "warn" && msg.contains("EXTERNAL_OUTCOME_UNKNOWN"))
        .count();
    assert_eq!(warns, 1);
}

#[test]
fn reconciliation_completeness_gap_warns() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness(&dir, true);
    let ledger = h.shadow.ledger_for_reconciliation().unwrap();
    ledger
        .append(
            "intent_proposed",
            "f1",
            &"f".repeat(64),
            &format!("fee-cycle-{}", NOW - 3600),
            NOW - 3600,
            &BTreeMap::new(),
            &json!({}),
        )
        .unwrap();

    let recent_fee_changes = |_limit: usize| -> EconResult<Vec<Value>> {
        Ok(vec![
            json!({"timestamp": NOW - 3600}),
            json!({"timestamp": NOW - 60}),
        ])
    };
    let inputs = ReconciliationInputs {
        spend_reservation_states: &ok_states,
        recent_fee_changes: &recent_fee_changes,
    };
    let result = h
        .shadow
        .maybe_run_reconciliation(Some(&inputs), NOW, 3600)
        .unwrap();
    assert_eq!(result.completeness_ok, Some(false));

    let warns = h
        .logs
        .lock()
        .unwrap()
        .iter()
        .filter(|(msg, level)| level == "warn" && msg.to_lowercase().contains("completeness"))
        .count();
    assert_eq!(warns, 1);
}

#[test]
fn reconciliation_fail_open_on_database_error_does_not_consume_throttle() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    let broken_states = || -> EconResult<BTreeMap<String, DbReservationState>> {
        Err(EconError {
            msg: "simulated database error".to_string(),
        })
    };
    let broken_inputs = ReconciliationInputs {
        spend_reservation_states: &broken_states,
        recent_fee_changes: &ok_fee_changes,
    };

    assert!(s
        .maybe_run_reconciliation(Some(&broken_inputs), NOW, 3600)
        .is_none());
    // Throttle must not have been consumed by the failed attempt: a normal
    // sweep right after (same `now`) still runs.
    let result = s.maybe_run_reconciliation(Some(&empty_inputs()), NOW, 3600);
    assert!(result.is_some());
}

#[test]
fn reconciliation_completeness_failure_does_not_abort_sweep_and_advances_throttle() {
    let dir = tempfile::tempdir().unwrap();
    let s = shadow(&dir, true);
    let broken_fee_changes = |_limit: usize| -> EconResult<Vec<Value>> {
        Err(EconError {
            msg: "simulated fee-changes read error".to_string(),
        })
    };
    let inputs = ReconciliationInputs {
        spend_reservation_states: &ok_states,
        recent_fee_changes: &broken_fee_changes,
    };
    let result = s
        .maybe_run_reconciliation(Some(&inputs), NOW, 3600)
        .unwrap();
    assert_eq!(result.completeness_ok, None);
    assert_eq!(result.divergences, 0);

    // Throttle DID advance despite the completeness-check failure.
    assert!(s
        .maybe_run_reconciliation(Some(&empty_inputs()), NOW + 600, 3600)
        .is_none());
}
