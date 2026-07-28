use revops_db::loop_health::{
    self, LoopId, RuntimeStatus, TerminalStatus, WiringStatus, MAX_ERROR_BYTES, REQUIRED_LOOPS,
};
use revops_db::owner::spawn_read_write;
use rusqlite::Connection;

#[test]
fn schema_registration_cas_history_and_restart_reconciliation_are_durable() {
    let conn = Connection::open_in_memory().unwrap();
    loop_health::init_schema(&conn).unwrap();

    for id in REQUIRED_LOOPS {
        let wiring = if id == LoopId::Fee {
            WiringStatus::Ready
        } else {
            WiringStatus::NotWired
        };
        loop_health::register_loop(&conn, id, wiring, 10).unwrap();
    }
    assert_eq!(loop_health::list_loop_health(&conn).unwrap().len(), 5);
    assert!(loop_health::begin_loop_pass(&conn, LoopId::Planner, 11).is_err());

    let first = loop_health::begin_loop_pass(&conn, LoopId::Fee, 20).unwrap();
    assert_eq!(first, 1);
    loop_health::fail_loop_pass(
        &conn,
        LoopId::Fee,
        first,
        21,
        &"x".repeat(MAX_ERROR_BYTES + 99),
    )
    .unwrap();
    let failed = loop_health::list_loop_health(&conn).unwrap().remove(0);
    assert_eq!(failed.last_error_at, Some(21));
    assert_eq!(failed.last_error.as_ref().unwrap().len(), MAX_ERROR_BYTES);

    let second = loop_health::begin_loop_pass(&conn, LoopId::Fee, 30).unwrap();
    assert_eq!(second, 2);
    assert!(loop_health::finish_loop_pass(&conn, LoopId::Fee, first, 31).is_err());
    loop_health::finish_loop_pass(&conn, LoopId::Fee, second, 32).unwrap();
    let passed = loop_health::list_loop_health(&conn).unwrap().remove(0);
    assert_eq!(passed.last_passed_at, Some(32));
    assert_eq!(
        passed.last_error_at,
        Some(21),
        "success preserves error history"
    );
    assert!(passed.last_error.is_some());

    let same_second = loop_health::begin_loop_pass(&conn, LoopId::Fee, 32).unwrap();
    assert_eq!(same_second, 3);
    assert_eq!(
        loop_health::reconcile_incomplete_on_restart(&conn, 33).unwrap(),
        1,
        "generation, not second ordering, detects incomplete restart"
    );

    let third = loop_health::begin_loop_pass(&conn, LoopId::Fee, 40).unwrap();
    assert_eq!(third, 4);
    assert_eq!(
        loop_health::reconcile_incomplete_on_restart(&conn, 41).unwrap(),
        1
    );
    let reconciled = loop_health::list_loop_health(&conn).unwrap().remove(0);
    assert_eq!(reconciled.last_error_at, Some(41));
    assert_eq!(
        reconciled.last_error.as_deref(),
        Some("previous_generation_incomplete_on_restart")
    );
}

#[test]
fn canonical_schema_rejects_unsupported_partial_table_without_fabricating_evidence() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE rust_loop_health (loop_name TEXT PRIMARY KEY, wiring_status TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0, last_started_at INTEGER, last_passed_at INTEGER, last_error_at INTEGER, last_error TEXT, coalesced_total INTEGER NOT NULL DEFAULT 0, dropped_total INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL);").unwrap();
    conn.execute(
        "INSERT INTO rust_loop_health (loop_name,wiring_status,generation,last_passed_at,updated_at) VALUES (?1,?2,?3,?4,?5)",
        rusqlite::params!["fee", "ready", 7, 99, 99],
    )
    .unwrap();
    let error = loop_health::init_schema(&conn).unwrap_err();
    assert!(
        format!("{error:#}").contains("noncanonical rust_loop_health schema"),
        "partial schema must be rejected explicitly: {error:#}"
    );
    let columns: Vec<String> = {
        let mut statement = conn.prepare("PRAGMA table_info(rust_loop_health)").unwrap();
        statement
            .query_map([], |row| row.get(1))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    assert!(!columns.contains(&"terminal_generation".to_string()));
    assert!(!columns.contains(&"terminal_status".to_string()));
    assert!(!columns.contains(&"runtime_status".to_string()));
}

#[test]
fn suspension_is_durable_reactivated_only_by_ready_registration_and_never_masked_by_finish() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    {
        let conn = Connection::open(&path).unwrap();
        loop_health::init_schema(&conn).unwrap();
        loop_health::register_loop(&conn, LoopId::Fee, WiringStatus::Ready, 10).unwrap();
        let generation = loop_health::begin_loop_pass(&conn, LoopId::Fee, 11).unwrap();
        loop_health::suspend_loop(&conn, LoopId::Fee, 12, &"z".repeat(MAX_ERROR_BYTES + 50))
            .unwrap();
        loop_health::finish_loop_pass(&conn, LoopId::Fee, generation, 13).unwrap();
        let suspended = loop_health::list_loop_health(&conn).unwrap().remove(0);
        assert_eq!(suspended.runtime_status, RuntimeStatus::Suspended);
        assert_eq!(suspended.last_suspended_at, Some(12));
        assert_eq!(
            suspended.last_suspension_reason.as_ref().unwrap().len(),
            MAX_ERROR_BYTES
        );
        assert_eq!(suspended.terminal_status, TerminalStatus::Passed);
        assert!(loop_health::begin_loop_pass(&conn, LoopId::Fee, 14).is_err());
    }
    {
        let conn = Connection::open(&path).unwrap();
        loop_health::init_schema(&conn).unwrap();
        let reopened = loop_health::list_loop_health(&conn).unwrap().remove(0);
        assert_eq!(reopened.runtime_status, RuntimeStatus::Suspended);
        loop_health::register_loop(&conn, LoopId::Fee, WiringStatus::Ready, 20).unwrap();
        let reactivated = loop_health::list_loop_health(&conn).unwrap().remove(0);
        assert_eq!(reactivated.runtime_status, RuntimeStatus::Active);
        assert_eq!(reactivated.last_suspended_at, Some(12));
        assert!(reactivated.last_suspension_reason.is_some());
        assert_eq!(
            loop_health::begin_loop_pass(&conn, LoopId::Fee, 21).unwrap(),
            2
        );
    }
}

#[test]
fn one_generation_accepts_exactly_one_terminal_write_in_either_direction() {
    let conn = Connection::open_in_memory().unwrap();
    loop_health::init_schema(&conn).unwrap();
    loop_health::register_loop(&conn, LoopId::Fee, WiringStatus::Ready, 1).unwrap();

    let passed_generation = loop_health::begin_loop_pass(&conn, LoopId::Fee, 2).unwrap();
    loop_health::finish_loop_pass(&conn, LoopId::Fee, passed_generation, 3).unwrap();
    assert!(loop_health::fail_loop_pass(
        &conn,
        LoopId::Fee,
        passed_generation,
        4,
        "must not overwrite pass"
    )
    .is_err());
    let passed = loop_health::list_loop_health(&conn).unwrap().remove(0);
    assert_eq!(passed.terminal_status, TerminalStatus::Passed);
    assert_eq!(passed.last_error_at, None);

    let failed_generation = loop_health::begin_loop_pass(&conn, LoopId::Fee, 5).unwrap();
    loop_health::fail_loop_pass(&conn, LoopId::Fee, failed_generation, 6, "original error")
        .unwrap();
    assert!(loop_health::finish_loop_pass(&conn, LoopId::Fee, failed_generation, 7).is_err());
    let failed = loop_health::list_loop_health(&conn).unwrap().remove(0);
    assert_eq!(failed.terminal_status, TerminalStatus::Error);
    assert_eq!(failed.last_error.as_deref(), Some("original error"));
}

#[tokio::test]
async fn actor_round_trips_every_health_write_and_current_boot_can_mark_not_wired() {
    let dir = tempfile::tempdir().unwrap();
    let handle = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    handle
        .register_loop(LoopId::Fee, WiringStatus::Ready, 100)
        .await
        .unwrap();
    handle
        .register_loop(LoopId::Fee, WiringStatus::NotWired, 101)
        .await
        .unwrap();
    let after_downgrade = handle.list_loop_health().await.unwrap();
    assert_eq!(after_downgrade[0].wiring_status, WiringStatus::NotWired);
    handle
        .register_loop(LoopId::Fee, WiringStatus::Ready, 102)
        .await
        .unwrap();
    let generation = handle.begin_loop_pass(LoopId::Fee, 103).await.unwrap();
    handle
        .increment_loop_backpressure(LoopId::Fee, 2, 3, 104)
        .await
        .unwrap();
    handle
        .finish_loop_pass(LoopId::Fee, generation, 105)
        .await
        .unwrap();
    handle
        .suspend_loop(LoopId::Fee, 106, "actor suspension".to_string())
        .await
        .unwrap();
    let rows = handle.list_loop_health().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].wiring_status, WiringStatus::Ready);
    assert_eq!(rows[0].generation, 1);
    assert_eq!(rows[0].coalesced_total, 2);
    assert_eq!(rows[0].dropped_total, 3);
    assert_eq!(rows[0].last_passed_at, Some(105));
    assert_eq!(rows[0].runtime_status, RuntimeStatus::Suspended);
    assert_eq!(rows[0].last_suspended_at, Some(106));
    assert_eq!(
        rows[0].last_suspension_reason.as_deref(),
        Some("actor suspension")
    );
}
