//! `SqliteLnPlusDb` — every test opens a fresh `tempfile::TempDir`-backed
//! sqlite file. HARD RULE: no test in this crate touches a real database;
//! a throwaway temp file is not the production `revenue_ops.db` and is
//! discarded when the `TempDir` drops.

mod common;

use common::FakeLogger;
use revops_lnplus::breaker::{BreakerCause, BreakerState};
use revops_lnplus::db_types::{SwapPatch, SwapRow};
use revops_lnplus::ports::{LnPlusDb, PlannerActionRequest, ReserveSpendRequest};
use revops_lnplus::sqlite_db::{ensure_schema, SqliteLnPlusDb};
use revops_lnplus::types::Rating;
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

fn open_db() -> (tempfile::TempDir, SqliteLnPlusDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lnplus.db");
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).expect("open db");
    (dir, db)
}

// ------------------------------------------------------------------ schema

#[test]
fn schema_init_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    // Opening twice must not error (CREATE TABLE/INDEX IF NOT EXISTS).
    let _first = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let _second = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
}

#[test]
fn schema_creates_the_exact_python_table_and_column_set() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    ensure_schema(&conn).unwrap();

    let mut stmt = conn
        .prepare("SELECT name FROM pragma_table_info('lnplus_swaps') ORDER BY cid")
        .unwrap();
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        cols,
        vec![
            "swap_id",
            "status",
            "capacity_sats",
            "duration_months",
            "ends_at",
            "outbound_peer",
            "incoming_peer",
            "our_identifier",
            "applied_at",
            "opened_at",
            "completed_at",
            "channel_funding_txid",
            "deadline_at",
            "planner_action_id",
            "outcome",
            "metadata_json",
            "tag_added",
            "incoming_tag_added",
        ],
        "must match database.py:1359-1409 exactly, including the unused completed_at column"
    );

    let mut stmt2 = conn
        .prepare("SELECT name FROM pragma_table_info('lnplus_peers') ORDER BY cid")
        .unwrap();
    let peer_cols: Vec<String> = stmt2
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        peer_cols,
        vec![
            "pubkey",
            "swaps_count",
            "ratings_given_positive",
            "ratings_given_negative",
            "defections",
            "last_swap_at",
        ]
    );
}

// ------------------------------------------------------------- swap ledger

#[test]
fn record_and_get_swap_roundtrips() {
    let (_dir, db) = open_db();
    let mut row = SwapRow::new("s1", "applied", 500_000, 6, 1000)
        .with_outbound_peer("02aa")
        .with_incoming_peer("02bb")
        .with_our_identifier("A")
        .with_planner_action_id(7);
    let mut meta = BTreeMap::new();
    meta.insert("swap_id".to_string(), "s1".to_string());
    row.metadata = Some(meta);

    db.insert_swap_new(&row).unwrap();
    let fetched = db.get_swap("s1").expect("row present");

    assert_eq!(fetched.status, "applied");
    assert_eq!(fetched.capacity_sats, 500_000);
    assert_eq!(fetched.outbound_peer.as_deref(), Some("02aa"));
    assert_eq!(fetched.incoming_peer.as_deref(), Some("02bb"));
    assert_eq!(fetched.our_identifier.as_deref(), Some("A"));
    assert_eq!(fetched.planner_action_id, Some(7));
    assert_eq!(fetched.applied_at, 1000);
    assert_eq!(
        fetched.metadata.unwrap().get("swap_id").cloned(),
        Some("s1".to_string())
    );
}

#[test]
fn get_swap_missing_returns_none() {
    // Control for the roundtrip test: a swap that was never recorded must
    // come back `None`, not a zeroed/default row.
    let (_dir, db) = open_db();
    assert!(db.get_swap("does-not-exist").is_none());
}

// The old `record_swap` INSERT OR REPLACE idempotency test is gone by
// design (Task 61 4A): a second insert of the same swap_id is now a typed
// `AlreadyExists` that never clobbers — see `tests/store_acks.rs`.

#[test]
fn cas_swap_only_touches_patched_columns() {
    let (_dir, db) = open_db();
    let row = SwapRow::new("s1", "applied", 500_000, 6, 1000).with_outbound_peer("02aa");
    db.insert_swap_new(&row).unwrap();

    db.cas_swap(
        "s1",
        &["applied"],
        &SwapPatch::default().status("opening").opened_at(2000),
    )
    .unwrap();

    let fetched = db.get_swap("s1").unwrap();
    assert_eq!(fetched.status, "opening");
    assert_eq!(fetched.opened_at, Some(2000));
    assert_eq!(
        fetched.outbound_peer.as_deref(),
        Some("02aa"),
        "untouched column must survive the patch"
    );
}

#[test]
fn cas_swap_with_no_fields_set_is_a_guarded_no_op() {
    let (_dir, db) = open_db();
    let row = SwapRow::new("s1", "applied", 500_000, 6, 1000);
    db.insert_swap_new(&row).unwrap();
    assert_eq!(
        db.cas_swap("s1", &["applied"], &SwapPatch::default())
            .unwrap(),
        revops_lnplus::ports::CasOutcome::Applied
    );
    assert_eq!(
        db.cas_swap("s1", &["opening"], &SwapPatch::default())
            .unwrap(),
        revops_lnplus::ports::CasOutcome::Conflict {
            actual: Some("applied".to_string())
        },
        "an empty patch still honors the status guard"
    );
    assert_eq!(db.get_swap("s1").unwrap().status, "applied");
}

#[test]
fn cas_swap_persists_tag_added_booleans() {
    let (_dir, db) = open_db();
    db.insert_swap_new(&SwapRow::new("s1", "active", 1, 1, 1000))
        .unwrap();
    db.cas_swap(
        "s1",
        &["active"],
        &SwapPatch::default()
            .tag_added(true)
            .incoming_tag_added(false),
    )
    .unwrap();
    let fetched = db.get_swap("s1").unwrap();
    assert_eq!(fetched.tag_added, Some(true));
    assert_eq!(fetched.incoming_tag_added, Some(false));
}

#[test]
fn get_swaps_by_status_filters_and_orders_by_applied_at() {
    let (_dir, db) = open_db();
    db.insert_swap_new(&SwapRow::new("s1", "applied", 1, 1, 300))
        .unwrap();
    db.insert_swap_new(&SwapRow::new("s2", "applied", 1, 1, 100))
        .unwrap();
    db.insert_swap_new(&SwapRow::new("s3", "opened", 1, 1, 200))
        .unwrap();

    let applied = db.get_swaps_by_status(&["applied"]);
    assert_eq!(
        applied
            .iter()
            .map(|r| r.swap_id.as_str())
            .collect::<Vec<_>>(),
        vec!["s2", "s1"],
        "ordered by applied_at ascending"
    );
}

#[test]
fn inflight_swaps_uses_the_default_trait_status_set() {
    let (_dir, db) = open_db();
    db.insert_swap_new(&SwapRow::new("s1", "applied", 1, 1, 1))
        .unwrap();
    db.insert_swap_new(&SwapRow::new("s2", "ended", 1, 1, 1))
        .unwrap();
    let inflight = db.inflight_swaps();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].swap_id, "s1");
}

#[test]
fn prune_terminal_deletes_old_terminal_rows_but_keeps_recent_ones() {
    let (_dir, db) = open_db();
    let now = 1_000_000i64;
    let old_cutoff = now - 200 * 86_400;
    db.insert_swap_new(&SwapRow::new("old", "ended", 1, 1, old_cutoff - 10))
        .unwrap();
    db.insert_swap_new(&SwapRow::new("recent", "ended", 1, 1, now - 10))
        .unwrap();
    db.insert_swap_new(&SwapRow::new("inflight", "applied", 1, 1, old_cutoff - 10))
        .unwrap();

    let pruned = db.prune_terminal(180, now).unwrap();

    assert_eq!(pruned, 1);
    assert!(db.get_swap("old").is_none());
    assert!(
        db.get_swap("recent").is_some(),
        "a recent terminal row must survive pruning"
    );
    assert!(
        db.get_swap("inflight").is_some(),
        "a non-terminal row must never be pruned regardless of age"
    );
}

// ---------------------------------------------------------- peer reputation

#[test]
fn bump_peer_creates_then_accumulates() {
    let (_dir, db) = open_db();
    assert!(db.get_peer("02aa").is_none());

    db.bump_peer("02aa", false, Some(Rating::Positive)).unwrap();
    let p1 = db.get_peer("02aa").unwrap();
    assert_eq!(p1.swaps_count, 1);
    assert_eq!(p1.ratings_given_positive, 1);
    assert_eq!(p1.defections, 0);

    db.bump_peer("02aa", true, Some(Rating::Negative)).unwrap();
    let p2 = db.get_peer("02aa").unwrap();
    assert_eq!(p2.swaps_count, 2, "count accumulates across calls");
    assert_eq!(
        p2.ratings_given_positive, 1,
        "prior positive rating preserved"
    );
    assert_eq!(p2.ratings_given_negative, 1);
    assert_eq!(p2.defections, 1);
}

// ---------------------------------------------------------- config overrides

#[test]
fn config_override_set_get_delete_roundtrip() {
    let (_dir, db) = open_db();
    assert_eq!(db.get_config_override("k"), None);
    db.set_config_override("k", "v1").unwrap();
    assert_eq!(db.get_config_override("k").as_deref(), Some("v1"));
    db.set_config_override("k", "v2").unwrap();
    assert_eq!(
        db.get_config_override("k").as_deref(),
        Some("v2"),
        "second set overwrites"
    );
    db.delete_config_override("k").unwrap();
    assert_eq!(db.get_config_override("k"), None);
}

// ------------------------------------------------------------------ breaker

#[test]
fn breaker_set_get_clear_roundtrips_the_structured_cause() {
    let (_dir, db) = open_db();
    assert!(db.get_breaker().unwrap().is_none());

    let state = BreakerState {
        tripped_at: 555,
        cause: BreakerCause::MissedOpenDeadline {
            swap_id: "s1".to_string(),
        },
    };
    db.set_breaker(&state).unwrap();
    let fetched = db.get_breaker().unwrap().expect("breaker present");
    assert_eq!(fetched, state);

    db.clear_breaker().unwrap();
    assert!(db.get_breaker().unwrap().is_none());
}

#[test]
fn breaker_roundtrips_every_cause_variant() {
    let (_dir, db) = open_db();
    let causes = vec![
        BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: "1".into(),
        },
        BreakerCause::PendingGhostNoLocalRecord {
            swap_id: "2".into(),
        },
        BreakerCause::LocalRowDivergentFromRemote {
            swap_id: "3".into(),
            detail: "d".into(),
        },
        BreakerCause::AmbiguousFundedChannelDivergence {
            swap_id: "4".into(),
            detail: "d2".into(),
        },
        BreakerCause::LnPlusOutage {
            detail: "timeout".into(),
        },
    ];
    for cause in causes {
        let state = BreakerState {
            tripped_at: 1,
            cause: cause.clone(),
        };
        db.set_breaker(&state).unwrap();
        assert_eq!(db.get_breaker().unwrap().unwrap().cause, cause);
        db.clear_breaker().unwrap();
    }
}

#[test]
fn breaker_read_fails_closed_on_foreign_or_malformed_value() {
    // Task 61 4A inverted this from the original wiring-layer behavior: a
    // value this crate cannot decode is corruption evidence in a
    // Rust-only store and must be an ERROR, never silently "untripped"
    // (see tests/store_acks.rs for the full fail-closed matrix).
    let (_dir, db) = open_db();
    db.set_config_override(
        revops_lnplus::breaker::BREAKER_KEY,
        "circuit breaker tripped: swap 42 ghost",
    )
    .unwrap();
    assert!(
        db.get_breaker().is_err(),
        "a foreign-format value must fail closed"
    );
}

// ------------------------------------------------------------ planner actions

#[test]
fn record_and_update_planner_action_persists_status_and_completed_at() {
    let (dir, db) = open_db();
    let id = db
        .record_planner_action(&PlannerActionRequest {
            action_type: "swap_apply",
            peer_id: "02aa".to_string(),
            amount_sats: Some(500_000),
            estimated_cost_sats: Some(2500),
            reason: "test".to_string(),
            metadata: None,
        })
        .unwrap();
    assert!(id > 0);

    db.update_planner_action(id, "completed").unwrap();

    let conn = rusqlite::Connection::open(dir.path().join("lnplus.db")).unwrap();
    let (status, completed_at): (String, Option<i64>) = conn
        .query_row(
            "SELECT status, completed_at FROM planner_actions WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "completed");
    assert!(
        completed_at.is_some(),
        "a terminal status must stamp completed_at"
    );
}

#[test]
fn update_planner_action_to_a_non_terminal_status_leaves_completed_at_null() {
    // Control for the test above: only completed/failed stamp completed_at.
    let (dir, db) = open_db();
    let id = db
        .record_planner_action(&PlannerActionRequest {
            action_type: "swap_apply",
            peer_id: "02aa".to_string(),
            amount_sats: None,
            estimated_cost_sats: None,
            reason: "test".to_string(),
            metadata: None,
        })
        .unwrap();
    db.update_planner_action(id, "recommended").unwrap();

    let conn = rusqlite::Connection::open(dir.path().join("lnplus.db")).unwrap();
    let completed_at: Option<i64> = conn
        .query_row(
            "SELECT completed_at FROM planner_actions WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(completed_at.is_none());
}

// -------------------------------------------------------------- budget rail

#[test]
fn reserve_spend_release_and_mark_spent_roundtrip_via_composed_budgetdb() {
    let (_dir, db) = open_db();
    let req = ReserveSpendRequest {
        reservation_id: "resv-1".to_string(),
        amount_sats: 2500,
        category: "channel_open",
        subcategory: "lnplus_swap",
        metadata: BTreeMap::new(),
        effective_budget_sats: None,
        since_timestamp: None,
    };
    let granted = db.reserve_spend(&req).expect("reserve_spend call succeeds");
    assert!(
        granted,
        "best-effort (no budget cap) reservation must grant"
    );

    let settled = db
        .mark_spend_reservation_spent("resv-1", 2500, "lnplus_swaps")
        .expect("settle call succeeds");
    assert!(settled);
}

#[test]
fn reserve_spend_refuses_when_over_the_effective_budget() {
    let (_dir, db) = open_db();
    let req = ReserveSpendRequest {
        reservation_id: "resv-2".to_string(),
        amount_sats: 10_000,
        category: "channel_open",
        subcategory: "lnplus_swap",
        metadata: BTreeMap::new(),
        effective_budget_sats: Some(1_000), // cap far below the request
        since_timestamp: None,
    };
    let granted = db.reserve_spend(&req).unwrap();
    assert!(
        !granted,
        "a request over the effective budget must be refused"
    );
}

#[test]
fn release_spend_reservation_smoke() {
    let (_dir, db) = open_db();
    let req = ReserveSpendRequest {
        reservation_id: "resv-3".to_string(),
        amount_sats: 100,
        category: "channel_open",
        subcategory: "lnplus_swap",
        metadata: BTreeMap::new(),
        effective_budget_sats: None,
        since_timestamp: None,
    };
    assert!(db.reserve_spend(&req).unwrap());
    assert!(db.release_spend_reservation("resv-3").is_ok());
}

// ------------------------------------------------ ownership / concurrency

#[test]
fn boundary_independent_instances_reopen_committed_swap_planner_and_budget_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    let first = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let second = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();

    let swap = SwapRow::new("boundary-swap", "applied", 600_000, 6, 1_000)
        .with_outbound_peer("02aa")
        .with_planner_action_id(41);
    first.insert_swap_new(&swap).unwrap();
    assert_eq!(
        second.get_swap("boundary-swap").unwrap().capacity_sats,
        600_000,
        "the independently opened LN+ connection must see the committed swap"
    );

    let action_id = second
        .record_planner_action(&PlannerActionRequest {
            action_type: "swap_apply",
            peer_id: "02aa".to_string(),
            amount_sats: Some(600_000),
            estimated_cost_sats: Some(2_500),
            reason: "boundary reopen".to_string(),
            metadata: None,
        })
        .unwrap();
    assert!(action_id > 0);

    assert!(first
        .reserve_spend(&ReserveSpendRequest {
            reservation_id: "boundary-resv-1".to_string(),
            amount_sats: 600,
            category: "channel_open",
            subcategory: "lnplus_swap",
            metadata: BTreeMap::new(),
            effective_budget_sats: Some(1_000),
            since_timestamp: Some(0),
        })
        .unwrap());

    drop(second);
    drop(first);

    let reopened_first = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let reopened_second = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let reopened_swap = reopened_first.get_swap("boundary-swap").unwrap();
    assert_eq!(reopened_swap.planner_action_id, Some(41));
    assert_eq!(reopened_swap.outbound_peer.as_deref(), Some("02aa"));

    let second_reservation_granted = reopened_second
        .reserve_spend(&ReserveSpendRequest {
            reservation_id: "boundary-resv-2".to_string(),
            amount_sats: 500,
            category: "channel_open",
            subcategory: "lnplus_swap",
            metadata: BTreeMap::new(),
            effective_budget_sats: Some(1_000),
            since_timestamp: Some(0),
        })
        .unwrap();
    assert!(
        !second_reservation_granted,
        "the reopened composed BudgetDb connection must count the persisted 600-sat hold"
    );

    let raw = rusqlite::Connection::open(&path).unwrap();
    let planner_reason: String = raw
        .query_row(
            "SELECT reason FROM planner_actions WHERE id = ?1",
            [action_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(planner_reason, "boundary reopen");
    let reservations: i64 = raw
        .query_row(
            "SELECT COUNT(*) FROM spend_reservations WHERE status = 'active'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservations, 1, "the refused retry must not add a row");
}

#[test]
fn boundary_rolled_back_raw_swap_insert_is_absent_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    drop(SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap());

    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch("BEGIN IMMEDIATE").unwrap();
    raw.execute(
        "INSERT INTO lnplus_swaps \
         (swap_id, status, capacity_sats, duration_months, applied_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params!["rolled-back", "applied", 500_000, 6, 1_000],
    )
    .unwrap();
    raw.execute_batch("ROLLBACK").unwrap();
    drop(raw);

    let reopened = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    assert!(reopened.get_swap("rolled-back").is_none());
}

#[test]
fn boundary_write_lock_waits_for_busy_timeout_and_leaves_no_partial_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    let row =
        SwapRow::new("busy-timeout", "applied", 700_000, 12, 2_000).with_outbound_peer("02bb");
    let swap_started = Instant::now();
    let swap_result = db.insert_swap_new(&row);
    let swap_elapsed = swap_started.elapsed();

    let action_started = Instant::now();
    let action_result = db.record_planner_action(&PlannerActionRequest {
        action_type: "swap_apply",
        peer_id: "02busy".to_string(),
        amount_sats: Some(700_000),
        estimated_cost_sats: Some(2_500),
        reason: "boundary busy-timeout planner action".to_string(),
        metadata: None,
    });
    let action_elapsed = action_started.elapsed();

    assert!(
        swap_elapsed >= Duration::from_millis(revops_lnplus::sqlite_db::BUSY_TIMEOUT_MS - 500),
        "swap write returned before the configured busy period elapsed: {swap_elapsed:?}"
    );
    assert!(
        swap_elapsed <= Duration::from_millis(revops_lnplus::sqlite_db::BUSY_TIMEOUT_MS + 3_000),
        "swap write exceeded the generous bounded-failure window: {swap_elapsed:?}"
    );
    assert!(
        action_elapsed >= Duration::from_millis(revops_lnplus::sqlite_db::BUSY_TIMEOUT_MS - 500),
        "planner write returned before the configured busy period elapsed: {action_elapsed:?}"
    );
    assert!(
        action_elapsed <= Duration::from_millis(revops_lnplus::sqlite_db::BUSY_TIMEOUT_MS + 3_000),
        "planner write exceeded the generous bounded-failure window: {action_elapsed:?}"
    );
    assert!(
        swap_result.is_err(),
        "the timed-out swap insert must be acknowledged as an Err (Task 61 4A), not swallowed"
    );
    assert!(
        action_result.is_err(),
        "the timed-out planner write must be acknowledged as an Err (Task 61 4A)"
    );

    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    drop(db);

    let reopened = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    assert!(
        reopened.get_swap("busy-timeout").is_none(),
        "the timed-out insert must not leave a partial swap row"
    );
    let raw = rusqlite::Connection::open(&path).unwrap();
    let actions: i64 = raw
        .query_row(
            "SELECT COUNT(*) FROM planner_actions \
             WHERE action_type = ?1 AND peer_id = ?2 AND reason = ?3",
            rusqlite::params![
                "swap_apply",
                "02busy",
                "boundary busy-timeout planner action"
            ],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        actions, 0,
        "the timed-out identifiable planner write must not persist any row"
    );
}

#[test]
fn boundary_write_waits_then_succeeds_when_raw_lock_is_released() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let blocker = rusqlite::Connection::open(&path).unwrap();
    let (locked_tx, locked_rx) = mpsc::channel();
    let blocker_thread = thread::spawn(move || {
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(250));
        blocker.execute_batch("ROLLBACK").unwrap();
    });
    locked_rx.recv().unwrap();

    let row = SwapRow::new("released-lock", "applied", 800_000, 12, 3_000)
        .with_outbound_peer("02cc")
        .with_incoming_peer("02dd")
        .with_our_identifier("A")
        .with_planner_action_id(88);
    let started = Instant::now();
    db.insert_swap_new(&row).unwrap();
    let elapsed = started.elapsed();
    blocker_thread.join().unwrap();

    assert!(
        elapsed >= Duration::from_millis(100),
        "the queued write did not wait for the held lock: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(revops_lnplus::sqlite_db::BUSY_TIMEOUT_MS),
        "the released lock should permit success before timeout: {elapsed:?}"
    );
    let fetched = db.get_swap("released-lock").unwrap();
    assert_eq!(fetched.status, "applied");
    assert_eq!(fetched.capacity_sats, 800_000);
    assert_eq!(fetched.duration_months, 12);
    assert_eq!(fetched.outbound_peer.as_deref(), Some("02cc"));
    assert_eq!(fetched.incoming_peer.as_deref(), Some("02dd"));
    assert_eq!(fetched.our_identifier.as_deref(), Some("A"));
    assert_eq!(fetched.planner_action_id, Some(88));

    let raw = rusqlite::Connection::open(&path).unwrap();
    let count: i64 = raw
        .query_row(
            "SELECT COUNT(*) FROM lnplus_swaps WHERE swap_id = 'released-lock'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn boundary_reopen_preserves_structured_breaker_first_cause_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    let first_cause = BreakerState {
        tripped_at: 1_234_567,
        cause: BreakerCause::LocalRowDivergentFromRemote {
            swap_id: "first-swap".to_string(),
            detail: "remote completed while local remained applied".to_string(),
        },
    };
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    db.set_breaker(&first_cause).unwrap();
    drop(db);

    let reopened = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    assert_eq!(reopened.get_breaker().unwrap(), Some(first_cause));
}

#[test]
fn boundary_foreign_breaker_encodings_are_read_only_and_panic_free() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    drop(SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap());

    let foreign_values = [
        "circuit breaker tripped: swap 42 ghost",
        r#"{"tripped_at":123,"cause":{"kind":"MissedOpenDeadline""#,
    ];
    for (version, raw_value) in foreign_values.into_iter().enumerate() {
        let raw = rusqlite::Connection::open(&path).unwrap();
        raw.execute(
            "INSERT OR REPLACE INTO config_overrides (key, value, version, updated_at) \
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                revops_lnplus::breaker::BREAKER_KEY,
                raw_value,
                version as i64 + 1,
                10_000 + version as i64
            ],
        )
        .unwrap();
        drop(raw);

        let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
        assert!(
            db.get_breaker().is_err(),
            "foreign encoding must fail closed (Task 61 4A), never panic or be \
             interpreted as Rust state"
        );
        drop(db);

        let check = rusqlite::Connection::open(&path).unwrap();
        let persisted: String = check
            .query_row(
                "SELECT value FROM config_overrides WHERE key = ?1",
                [revops_lnplus::breaker::BREAKER_KEY],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            persisted, raw_value,
            "reading a foreign breaker encoding must not rewrite it"
        );
    }
}
