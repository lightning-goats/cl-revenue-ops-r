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
    // Task 67: the registry is the eight Python business/startup loops.
    assert_eq!(loop_health::list_loop_health(&conn).unwrap().len(), 8);
    assert!(loop_health::begin_loop_pass(&conn, LoopId::Planner, "boot-test", 11).is_err());

    let first = loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 20).unwrap();
    assert_eq!(first, 1);
    loop_health::fail_loop_pass(
        &conn,
        LoopId::Fee,
        first,
        "boot-test",
        21,
        &"x".repeat(MAX_ERROR_BYTES + 99),
    )
    .unwrap();
    let failed = loop_health::load_loop(&conn, LoopId::Fee).unwrap().unwrap();
    assert_eq!(failed.last_error_at, Some(21));
    assert_eq!(failed.last_error.as_ref().unwrap().len(), MAX_ERROR_BYTES);

    let second = loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 30).unwrap();
    assert_eq!(second, 2);
    assert!(loop_health::finish_loop_pass(&conn, LoopId::Fee, first, "boot-test", 31).is_err());
    loop_health::finish_loop_pass(&conn, LoopId::Fee, second, "boot-test", 32).unwrap();
    let passed = loop_health::load_loop(&conn, LoopId::Fee).unwrap().unwrap();
    assert_eq!(passed.last_passed_at, Some(32));
    assert_eq!(
        passed.last_error_at,
        Some(21),
        "success preserves error history"
    );
    assert!(passed.last_error.is_some());

    let same_second = loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 32).unwrap();
    assert_eq!(same_second, 3);
    assert_eq!(
        loop_health::reconcile_incomplete_on_restart(&conn, 33).unwrap(),
        1,
        "generation, not second ordering, detects incomplete restart"
    );

    let third = loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 40).unwrap();
    assert_eq!(third, 4);
    assert_eq!(
        loop_health::reconcile_incomplete_on_restart(&conn, 41).unwrap(),
        1
    );
    let reconciled = loop_health::load_loop(&conn, LoopId::Fee).unwrap().unwrap();
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
        let generation = loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 11).unwrap();
        loop_health::suspend_loop(&conn, LoopId::Fee, 12, &"z".repeat(MAX_ERROR_BYTES + 50))
            .unwrap();
        loop_health::finish_loop_pass(&conn, LoopId::Fee, generation, "boot-test", 13).unwrap();
        let suspended = loop_health::list_loop_health(&conn).unwrap().remove(0);
        assert_eq!(suspended.runtime_status, RuntimeStatus::Suspended);
        assert_eq!(suspended.last_suspended_at, Some(12));
        assert_eq!(
            suspended.last_suspension_reason.as_ref().unwrap().len(),
            MAX_ERROR_BYTES
        );
        assert_eq!(suspended.terminal_status, TerminalStatus::Passed);
        assert!(loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 14).is_err());
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
            loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 21).unwrap(),
            2
        );
    }
}

#[test]
fn one_generation_accepts_exactly_one_terminal_write_in_either_direction() {
    let conn = Connection::open_in_memory().unwrap();
    loop_health::init_schema(&conn).unwrap();
    loop_health::register_loop(&conn, LoopId::Fee, WiringStatus::Ready, 1).unwrap();

    let passed_generation =
        loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 2).unwrap();
    loop_health::finish_loop_pass(&conn, LoopId::Fee, passed_generation, "boot-test", 3).unwrap();
    assert!(loop_health::fail_loop_pass(
        &conn,
        LoopId::Fee,
        passed_generation,
        "boot-test",
        4,
        "must not overwrite pass"
    )
    .is_err());
    let passed = loop_health::load_loop(&conn, LoopId::Fee).unwrap().unwrap();
    assert_eq!(passed.terminal_status, TerminalStatus::Passed);
    assert_eq!(passed.last_error_at, None);

    let failed_generation =
        loop_health::begin_loop_pass(&conn, LoopId::Fee, "boot-test", 5).unwrap();
    loop_health::fail_loop_pass(
        &conn,
        LoopId::Fee,
        failed_generation,
        "boot-test",
        6,
        "original error",
    )
    .unwrap();
    assert!(
        loop_health::finish_loop_pass(&conn, LoopId::Fee, failed_generation, "boot-test", 7)
            .is_err()
    );
    let failed = loop_health::load_loop(&conn, LoopId::Fee).unwrap().unwrap();
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
    let generation = handle
        .begin_loop_pass(LoopId::Fee, "boot-test", 103)
        .await
        .unwrap();
    handle
        .increment_loop_backpressure(LoopId::Fee, 2, 3, 104)
        .await
        .unwrap();
    handle
        .finish_loop_pass(LoopId::Fee, generation, "boot-test", 105)
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

// -- Task 67: current-boot health binding + the eight-loop registry --

/// THE AUDIT DEFECT: a pass that completed cleanly in a PRIOR boot must
/// never read as `passed` on a fresh process that has run nothing. The
/// row survives (it is real history), but its status this boot is
/// `never_run_this_boot` -- a distinct honest state, never `passed` and
/// never `error`.
#[test]
fn prior_boot_pass_is_not_inherited_by_a_fresh_process() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    loop_health::init_schema(&conn).unwrap();

    let boot_a = "boot-aaaa";
    let boot_b = "boot-bbbb";
    loop_health::register_loop(&conn, LoopId::Fee, WiringStatus::Ready, 10).unwrap();

    // Boot A runs a complete, clean pass.
    let generation = loop_health::begin_loop_pass(&conn, LoopId::Fee, boot_a, 20).unwrap();
    loop_health::finish_loop_pass(&conn, LoopId::Fee, generation, boot_a, 30).unwrap();
    let row = loop_health::load_loop(&conn, LoopId::Fee).unwrap().unwrap();
    assert_eq!(
        loop_health::current_boot_status(&row, boot_a),
        loop_health::BootStatus::Passed,
        "the boot that ran the pass sees it as passed"
    );

    // Boot B is a fresh process. The prior terminal evidence is still on
    // disk, but this process has produced NOTHING.
    assert_eq!(
        loop_health::current_boot_status(&row, boot_b),
        loop_health::BootStatus::NeverRunThisBoot,
        "a prior-boot pass must NOT be inherited"
    );

    // An in-flight generation this boot is `incomplete`, and a completed
    // one flips to passed only for THIS boot.
    let generation = loop_health::begin_loop_pass(&conn, LoopId::Fee, boot_b, 40).unwrap();
    let row = loop_health::load_loop(&conn, LoopId::Fee).unwrap().unwrap();
    assert_eq!(
        loop_health::current_boot_status(&row, boot_b),
        loop_health::BootStatus::Incomplete
    );
    loop_health::finish_loop_pass(&conn, LoopId::Fee, generation, boot_b, 50).unwrap();
    let row = loop_health::load_loop(&conn, LoopId::Fee).unwrap().unwrap();
    assert_eq!(
        loop_health::current_boot_status(&row, boot_b),
        loop_health::BootStatus::Passed
    );
    // ...and boot A's view of boot B's pass is equally not-inherited.
    assert_eq!(
        loop_health::current_boot_status(&row, boot_a),
        loop_health::BootStatus::NeverRunThisBoot
    );
}

/// A failure in THIS boot is an error; a prior boot's failure is not
/// inherited either (it is history, not a current verdict).
#[test]
fn prior_boot_failure_is_also_not_inherited() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    loop_health::init_schema(&conn).unwrap();
    loop_health::register_loop(&conn, LoopId::Planner, WiringStatus::Ready, 10).unwrap();

    let generation = loop_health::begin_loop_pass(&conn, LoopId::Planner, "boot-a", 20).unwrap();
    loop_health::fail_loop_pass(&conn, LoopId::Planner, generation, "boot-a", 30, "boom").unwrap();
    let row = loop_health::load_loop(&conn, LoopId::Planner)
        .unwrap()
        .unwrap();
    assert_eq!(
        loop_health::current_boot_status(&row, "boot-a"),
        loop_health::BootStatus::Error
    );
    assert_eq!(
        loop_health::current_boot_status(&row, "boot-b"),
        loop_health::BootStatus::NeverRunThisBoot
    );
}

/// The registry is exactly Python's eight business/startup loops, in
/// Python's own label vocabulary (cl-revenue-ops.py:3588-3600).
#[test]
fn registry_is_exactly_the_eight_python_loops() {
    let names: Vec<&str> = REQUIRED_LOOPS.iter().map(|id| id.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "flow-analysis",
            "fee-adjustment",
            "rebalance-check",
            "startup-snapshot",
            "financial-snapshot",
            "boltz-auto-cycle",
            "capacity-planner",
            "lnplus-watcher",
        ],
        "the eight loops must match Python's thread labels exactly"
    );
    // Every name round-trips through the parser.
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    loop_health::init_schema(&conn).unwrap();
    for id in REQUIRED_LOOPS {
        loop_health::register_loop(&conn, id, WiringStatus::Ready, 1).unwrap();
    }
    let rows = loop_health::list_loop_health(&conn).unwrap();
    assert_eq!(rows.len(), 8);
}

/// One boot-identity record per process, shared by every loop, so fee
/// restart markers and loop health cannot drift apart.
#[test]
fn boot_session_records_process_identity_once() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    loop_health::init_schema(&conn).unwrap();
    let identity = loop_health::BootIdentity {
        boot_id: "boot-aaaa".into(),
        process_id: 4242,
        source_commit: Some("deadbeef".into()),
        binary_sha256: Some("cafe".into()),
        started_at: 1_800_000_000,
    };
    loop_health::record_boot_session(&conn, &identity).unwrap();
    // Idempotent: recording the same boot twice is not a second session.
    loop_health::record_boot_session(&conn, &identity).unwrap();
    let sessions = loop_health::recent_boot_sessions(&conn, 10).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].boot_id, "boot-aaaa");
    assert_eq!(sessions[0].process_id, 4242);
    assert_eq!(sessions[0].source_commit.as_deref(), Some("deadbeef"));
}
