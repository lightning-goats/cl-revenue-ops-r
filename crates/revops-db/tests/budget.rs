//! Budget rail lifecycle tests (Phase 3 Task 6).
//!
//! PRODUCTION-WRITE CONSTRAINT: every test here builds its database inside a
//! `tempfile::TempDir` using `BudgetDb::open`'s own DDL (the exact Python
//! schema shape). No test takes a path from the environment; nothing here can
//! reach lnnode's production `revenue_ops.db`.

use revops_db::budget::{BudgetDb, BudgetError, ReserveRequest, SpendEvent, MAX_AMOUNT_SATS};
use rusqlite::Connection;
use serde_json::json;
use std::time::Duration;
use tempfile::TempDir;

/// Frozen clock, matching the golden drivers' FROZEN_NOW.
const NOW: i64 = 1_752_400_000;
const DAY: i64 = 86_400;
/// Daily budget window start used throughout (24h ago).
const SINCE: i64 = NOW - DAY;

fn fresh() -> (TempDir, BudgetDb) {
    let dir = TempDir::new().expect("tempdir");
    let db = BudgetDb::open(&dir.path().join("budget.db")).expect("open budget db");
    (dir, db)
}

/// Second, independent connection to the same tempdir DB (seeding /
/// inspection / lock-contention). Short busy timeout so a lingering write
/// lock from the code under test fails the test fast instead of hanging.
fn side(dir: &TempDir) -> Connection {
    let conn = Connection::open(dir.path().join("budget.db")).expect("side conn");
    conn.busy_timeout(Duration::from_millis(250)).unwrap();
    conn
}

fn req(rid: &str, amount: i64, cat: &str) -> ReserveRequest {
    ReserveRequest {
        reservation_id: rid.to_string(),
        amount_sats: amount,
        category: cat.to_string(),
        ..Default::default()
    }
}

fn budget_req(rid: &str, amount: i64, budget: i64) -> ReserveRequest {
    ReserveRequest {
        effective_budget_sats: Some(budget),
        since_timestamp: Some(SINCE),
        ..req(rid, amount, "misc")
    }
}

fn seed_spend_resv(conn: &Connection, rid: &str, cat: &str, sats: i64, at: i64, status: &str) {
    conn.execute(
        "INSERT INTO spend_reservations \
         (reservation_id, category, reserved_sats, reserved_at, status) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![rid, cat, sats, at, status],
    )
    .unwrap();
}

fn seed_budget_resv(conn: &Connection, rid: &str, sats: i64, at: i64, status: &str) {
    conn.execute(
        "INSERT INTO budget_reservations \
         (reservation_id, reserved_sats, reserved_at, job_channel_id, status) \
         VALUES (?1, ?2, ?3, 'chan', ?4)",
        rusqlite::params![rid, sats, at, status],
    )
    .unwrap();
}

fn seed_cost(conn: &Connection, sats: i64, ts: i64) {
    conn.execute(
        "INSERT INTO rebalance_costs (cost_sats, timestamp) VALUES (?1, ?2)",
        rusqlite::params![sats, ts],
    )
    .unwrap();
}

fn seed_event(conn: &Connection, eid: &str, cat: &str, sats: i64, ts: i64) {
    conn.execute(
        "INSERT INTO spend_events (event_id, category, amount_sats, timestamp) \
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![eid, cat, sats, ts],
    )
    .unwrap();
}

fn resv_row(conn: &Connection, rid: &str) -> Option<(String, i64, i64)> {
    conn.query_row(
        "SELECT status, reserved_sats, reserved_at FROM spend_reservations \
         WHERE reservation_id = ?1",
        [rid],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .ok()
}

fn legacy_status(conn: &Connection, rid: &str) -> Option<String> {
    conn.query_row(
        "SELECT status FROM budget_reservations WHERE reservation_id = ?1",
        [rid],
        |r| r.get(0),
    )
    .ok()
}

fn event_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM spend_events", [], |r| r.get(0))
        .unwrap()
}

// ---------------------------------------------------------------------------
// open()
// ---------------------------------------------------------------------------

#[test]
fn open_creates_schema_and_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    let _db = BudgetDb::open(&path).unwrap();
    // Reopen: CREATE IF NOT EXISTS everywhere, no error.
    let _db2 = BudgetDb::open(&path).unwrap();
    let conn = side(&dir);
    for table in [
        "budget_reservations",
        "spend_reservations",
        "spend_events",
        "rebalance_costs",
    ] {
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "missing table {table}");
    }
}

// ---------------------------------------------------------------------------
// reserve_spend: sanitize rejects (before any transaction)
// ---------------------------------------------------------------------------

#[test]
fn reserve_rejects_zero_negative_amount_and_empty_ids() {
    let (dir, mut db) = fresh();
    assert_eq!(
        db.reserve_spend(req("r", 0, "misc"), NOW).unwrap(),
        (false, 0)
    );
    assert_eq!(
        db.reserve_spend(req("r", -5, "misc"), NOW).unwrap(),
        (false, 0)
    );
    assert_eq!(
        db.reserve_spend(req("", 10, "misc"), NOW).unwrap(),
        (false, 0)
    );
    assert_eq!(
        db.reserve_spend(req("  ", 10, "misc"), NOW).unwrap(),
        (false, 0)
    );
    assert_eq!(db.reserve_spend(req("r", 10, ""), NOW).unwrap(), (false, 0));
    assert_eq!(
        db.reserve_spend(req("r", 10, "   "), NOW).unwrap(),
        (false, 0)
    );
    let conn = side(&dir);
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM spend_reservations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn reserve_clamps_excessive_amount_to_max() {
    let (dir, mut db) = fresh();
    // 20 BTC-billions clamps to the 10 BTC MAX_AMOUNT_SATS ceiling.
    let (ok, rem) = db
        .reserve_spend(req("big", 2 * MAX_AMOUNT_SATS, "misc"), NOW)
        .unwrap();
    assert!(ok);
    assert_eq!(rem, 0); // best-effort (no budget) always returns 0 remaining
    let conn = side(&dir);
    assert_eq!(resv_row(&conn, "big").unwrap().1, MAX_AMOUNT_SATS);
}

#[test]
fn reserve_normalizes_category_and_trims_rid() {
    let (dir, mut db) = fresh();
    let (ok, _) = db
        .reserve_spend(req("  padded  ", 10, " Channel_Open "), NOW)
        .unwrap();
    assert!(ok);
    let conn = side(&dir);
    let (cat, rid): (String, String) = conn
        .query_row(
            "SELECT category, reservation_id FROM spend_reservations",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(cat, "channel_open");
    assert_eq!(rid, "padded");
}

// ---------------------------------------------------------------------------
// committed-total arithmetic (P4-017 shape)
// ---------------------------------------------------------------------------

/// Seeds all four committed-total sources. The two active holds are OLDER
/// than every window (30d / 10d) and must count IN FULL (P4-017); the
/// out-of-window cost/event rows must NOT count.
fn seed_committed_sources(conn: &Connection) {
    seed_spend_resv(conn, "g1", "boltz", 1000, NOW - 30 * DAY, "active");
    seed_budget_resv(conn, "b1", 2000, NOW - 10 * DAY, "active");
    // Terminal rows never count.
    seed_spend_resv(conn, "gdone", "boltz", 4444, NOW - 100, "spent");
    seed_budget_resv(conn, "bdone", 3333, NOW - 100, "released");
    seed_cost(conn, 500, NOW - 3600); // in daily window
    seed_cost(conn, 9999, SINCE - 1); // out of daily window
    seed_event(conn, "e-in", "misc", 700, NOW - 100); // in window
    seed_event(conn, "e-out", "misc", 8888, SINCE - 10); // out of window
}

#[test]
fn committed_total_grants_exact_remaining_including_aged_holds() {
    let (dir, mut db) = fresh();
    seed_committed_sources(&side(&dir));
    // already = 1000 (gen held, 30d old) + 2000 (reb held, 10d old)
    //         + 500 (windowed cost) + 700 (windowed event) = 4200
    let (ok, rem) = db
        .reserve_spend(budget_req("r1", 5800, 10_000), NOW)
        .unwrap();
    assert!(ok);
    assert_eq!(rem, 0);
    // Now fully committed: one more sat is refused with remaining 0.
    let (ok2, rem2) = db.reserve_spend(budget_req("r2", 1, 10_000), NOW).unwrap();
    assert!(!ok2);
    assert_eq!(rem2, 0);
    let conn = side(&dir);
    assert!(resv_row(&conn, "r1").is_some());
    assert!(resv_row(&conn, "r2").is_none());
}

#[test]
fn committed_total_refusal_reports_exact_remaining() {
    let (dir, mut db) = fresh();
    seed_committed_sources(&side(&dir));
    let (ok, rem) = db
        .reserve_spend(budget_req("r1", 5801, 10_000), NOW)
        .unwrap();
    assert!(!ok);
    assert_eq!(rem, 5800);
    assert!(resv_row(&side(&dir), "r1").is_none());
}

#[test]
fn committed_total_remaining_can_be_negative() {
    let (dir, mut db) = fresh();
    seed_event(&side(&dir), "e", "misc", 150, NOW - 10);
    let (ok, rem) = db.reserve_spend(budget_req("r", 10, 100), NOW).unwrap();
    assert!(!ok);
    assert_eq!(rem, -50);
}

#[test]
fn reserve_without_budget_is_best_effort_grant() {
    let (dir, mut db) = fresh();
    // Way past any budget, but effective_budget_sats is None: grant, 0.
    seed_event(&side(&dir), "e", "misc", 1_000_000, NOW - 10);
    assert_eq!(
        db.reserve_spend(req("r", 500, "misc"), NOW).unwrap(),
        (true, 0)
    );
}

// ---------------------------------------------------------------------------
// re-reserve (active rid) and terminal-rid refusal
// ---------------------------------------------------------------------------

#[test]
fn re_reserve_active_rid_replaces_amount_and_gates_only_delta() {
    let (dir, mut db) = fresh();
    let (ok, rem) = db
        .reserve_spend(budget_req("rA", 4000, 10_000), NOW)
        .unwrap();
    assert!(ok);
    assert_eq!(rem, 6000);
    // Re-reserve: existing 4000 is excluded, so already = 0 again.
    let (ok, rem) = db
        .reserve_spend(budget_req("rA", 4500, 10_000), NOW + 50)
        .unwrap();
    assert!(ok);
    assert_eq!(rem, 5500);
    let conn = side(&dir);
    let (status, sats, at) = resv_row(&conn, "rA").unwrap();
    assert_eq!((status.as_str(), sats, at), ("active", 4500, NOW + 50));
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM spend_reservations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1);
    // Another rid still sees the replaced hold in full.
    let (ok, rem) = db
        .reserve_spend(budget_req("rB", 9000, 10_000), NOW + 60)
        .unwrap();
    assert!(!ok);
    assert_eq!(rem, 5500);
    // But rA itself can grow to the full budget (delta gating).
    let (ok, rem) = db
        .reserve_spend(budget_req("rA", 10_000, 10_000), NOW + 70)
        .unwrap();
    assert!(ok);
    assert_eq!(rem, 0);
}

#[test]
fn terminal_rid_refuses_resurrection_and_commits_not_rollbacks() {
    let (dir, mut db) = fresh();
    for (rid, terminal) in [("rRel", "released"), ("rSp", "spent")] {
        assert_eq!(
            db.reserve_spend(req(rid, 100, "misc"), NOW).unwrap(),
            (true, 0)
        );
        if terminal == "released" {
            assert!(db.release_spend_reservation(rid).unwrap());
        } else {
            assert!(db
                .mark_spend_reservation_spent(rid, None, None, false, NOW)
                .unwrap());
        }
        // Refused, remaining 0, status untouched.
        assert_eq!(
            db.reserve_spend(req(rid, 200, "misc"), NOW + 1).unwrap(),
            (false, 0)
        );
        let conn = side(&dir);
        let (status, sats, _) = resv_row(&conn, rid).unwrap();
        assert_eq!(status, terminal);
        assert_eq!(sats, 100);
        // The guard path COMMITs (not ROLLBACK): no lingering write lock, so a
        // second connection can immediately take the write lock.
        conn.execute(
            "INSERT INTO spend_events (event_id, category, amount_sats, timestamp) \
             VALUES (?1, 'probe', 1, ?2)",
            rusqlite::params![format!("probe:{rid}"), NOW],
        )
        .expect("write lock must be free after terminal-guard COMMIT");
    }
}

// ---------------------------------------------------------------------------
// weekly cap
// ---------------------------------------------------------------------------

#[test]
fn weekly_cap_uses_windowed_spends_and_unfiltered_holds() {
    let (dir, mut db) = fresh();
    let conn = side(&dir);
    // Holds far older than BOTH windows: still count in full (P4-017).
    seed_spend_resv(&conn, "g1", "misc", 500, NOW - 30 * DAY, "active");
    seed_budget_resv(&conn, "b1", 500, NOW - 10 * DAY, "active");
    // Spends inside the weekly window but outside the daily window.
    seed_cost(&conn, 3000, NOW - 2 * DAY);
    seed_event(&conn, "e-week", "misc", 1000, NOW - 3 * DAY);
    let weekly = |rid: &str, amount: i64| ReserveRequest {
        weekly_budget_limit: Some(6000),
        weekly_since_timestamp: Some(NOW - 7 * DAY),
        ..budget_req(rid, amount, 10_000)
    };
    // daily: already = 500 + 500 = 1000 -> remaining 9000 (spends out of window)
    // weekly: already = 3000 + 500 + 500 + 1000 = 5000 -> remaining 1000
    let (ok, rem) = db.reserve_spend(weekly("rw1", 1500), NOW).unwrap();
    assert!(!ok);
    assert_eq!(rem, 1000); // weekly remaining is what's reported on a weekly reject
    assert!(resv_row(&side(&dir), "rw1").is_none());
    // Grant: remaining = min(daily_after, weekly_after) = min(8200, 200) = 200.
    let (ok, rem) = db.reserve_spend(weekly("rw2", 800), NOW).unwrap();
    assert!(ok);
    assert_eq!(rem, 200);
}

// ---------------------------------------------------------------------------
// metadata_json byte-compat
// ---------------------------------------------------------------------------

#[test]
fn metadata_json_is_python_dumps_sort_keys_default_separators() {
    let (dir, mut db) = fresh();
    let r = ReserveRequest {
        metadata: Some(json!({"b": 2, "a": 1, "nested": {"z": true, "y": [1, 2]}})),
        ..req("rm", 10, "misc")
    };
    assert_eq!(db.reserve_spend(r, NOW).unwrap(), (true, 0));
    let conn = side(&dir);
    let meta: Option<String> = conn
        .query_row(
            "SELECT metadata_json FROM spend_reservations WHERE reservation_id='rm'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        meta.as_deref(),
        Some(r#"{"a": 1, "b": 2, "nested": {"y": [1, 2], "z": true}}"#)
    );
    // Empty metadata (falsy dict in Python) -> NULL.
    let r = ReserveRequest {
        metadata: Some(json!({})),
        ..req("rm2", 10, "misc")
    };
    assert_eq!(db.reserve_spend(r, NOW).unwrap(), (true, 0));
    let meta: Option<String> = conn
        .query_row(
            "SELECT metadata_json FROM spend_reservations WHERE reservation_id='rm2'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(meta, None);
}

#[test]
fn metadata_with_floats_is_rejected_before_any_write() {
    let (dir, mut db) = fresh();
    let r = ReserveRequest {
        metadata: Some(json!({"f": 1.5})),
        ..req("rf", 10, "misc")
    };
    assert!(matches!(
        db.reserve_spend(r, NOW),
        Err(BudgetError::Metadata(_))
    ));
    let conn = side(&dir);
    assert!(resv_row(&conn, "rf").is_none());
    // Rejected BEFORE BEGIN IMMEDIATE: write lock is free.
    conn.execute(
        "INSERT INTO spend_events (event_id, category, amount_sats, timestamp) \
         VALUES ('probe', 'probe', 1, 1)",
        [],
    )
    .expect("no transaction may be left open by a metadata reject");
}

// ---------------------------------------------------------------------------
// release / settle (P2-003)
// ---------------------------------------------------------------------------

#[test]
fn release_flips_active_to_released_once() {
    let (dir, mut db) = fresh();
    assert_eq!(
        db.reserve_spend(req("r", 10, "misc"), NOW).unwrap(),
        (true, 0)
    );
    assert!(db.release_spend_reservation("r").unwrap());
    assert!(!db.release_spend_reservation("r").unwrap()); // already terminal
    assert!(!db.release_spend_reservation("ghost").unwrap());
    assert_eq!(resv_row(&side(&dir), "r").unwrap().0, "released");
}

#[test]
fn mark_spent_records_settlement_event_atomically() {
    let (dir, mut db) = fresh();
    let r = ReserveRequest {
        subcategory: Some("swap".into()),
        reference_id: Some("ref-1".into()),
        channel_id: Some("chan-1".into()),
        ..req("rM", 250, "Boltz")
    };
    assert_eq!(db.reserve_spend(r, NOW).unwrap(), (true, 0));
    assert!(db
        .mark_spend_reservation_spent("rM", Some(240), None, true, NOW + 10)
        .unwrap());
    let conn = side(&dir);
    assert_eq!(resv_row(&conn, "rM").unwrap().0, "spent");
    struct EventRow {
        category: String,
        amount_sats: i64,
        timestamp: i64,
        subcategory: Option<String>,
        reference_id: Option<String>,
        channel_id: Option<String>,
        source: Option<String>,
        metadata_json: Option<String>,
    }
    let row = conn
        .query_row(
            "SELECT category, amount_sats, timestamp, subcategory, reference_id, \
             channel_id, source, metadata_json FROM spend_events WHERE event_id='resv:rM'",
            [],
            |row| {
                Ok(EventRow {
                    category: row.get(0)?,
                    amount_sats: row.get(1)?,
                    timestamp: row.get(2)?,
                    subcategory: row.get(3)?,
                    reference_id: row.get(4)?,
                    channel_id: row.get(5)?,
                    source: row.get(6)?,
                    metadata_json: row.get(7)?,
                })
            },
        )
        .unwrap();
    assert_eq!(row.category, "boltz");
    assert_eq!(row.amount_sats, 240);
    assert_eq!(row.timestamp, NOW + 10);
    assert_eq!(row.subcategory.as_deref(), Some("swap"));
    assert_eq!(row.reference_id.as_deref(), Some("ref-1"));
    assert_eq!(row.channel_id.as_deref(), Some("chan-1"));
    assert_eq!(row.source.as_deref(), Some("reservation_settlement"));
    assert_eq!(
        row.metadata_json.as_deref(),
        Some(r#"{"reservation_id": "rM"}"#)
    );
    // Second settle: row exists but is no longer 'active' -> false, no 2nd event.
    assert!(!db
        .mark_spend_reservation_spent("rM", Some(240), None, true, NOW + 20)
        .unwrap());
    assert_eq!(event_count(&conn), 1);
}

#[test]
fn mark_spent_defaults_amount_to_reserved_and_source_override() {
    let (dir, mut db) = fresh();
    assert_eq!(
        db.reserve_spend(req("rD", 77, "misc"), NOW).unwrap(),
        (true, 0)
    );
    assert!(db
        .mark_spend_reservation_spent("rD", None, Some("custom_src"), true, NOW)
        .unwrap());
    let conn = side(&dir);
    let (amt, src): (i64, String) = conn
        .query_row(
            "SELECT amount_sats, source FROM spend_events WHERE event_id='resv:rD'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(amt, 77);
    assert_eq!(src, "custom_src");
}

#[test]
fn mark_spent_missing_or_empty_rid_returns_false() {
    let (_dir, mut db) = fresh();
    assert!(!db
        .mark_spend_reservation_spent("ghost", None, None, true, NOW)
        .unwrap());
    assert!(!db
        .mark_spend_reservation_spent("", None, None, true, NOW)
        .unwrap());
    assert!(!db
        .mark_spend_reservation_spent("   ", None, None, true, NOW)
        .unwrap());
}

/// THE money-safety property: a failed settle-event write rolls the 'spent'
/// flip back. The reservation must never be 'spent' without its event —
/// failing toward HOLDING budget.
#[test]
fn spent_without_event_is_impossible() {
    let (dir, mut db) = fresh();
    assert_eq!(
        db.reserve_spend(req("rX", 300, "misc"), NOW).unwrap(),
        (true, 0)
    );
    let conn = side(&dir);
    conn.execute_batch(
        "CREATE TRIGGER budget_test_fail_events BEFORE INSERT ON spend_events \
         BEGIN SELECT RAISE(ABORT, 'injected event failure'); END;",
    )
    .unwrap();
    let err = db
        .mark_spend_reservation_spent("rX", Some(300), None, true, NOW)
        .unwrap_err();
    assert!(matches!(err, BudgetError::Sqlite(_)));
    // The 'spent' flip was rolled back and no event exists.
    assert_eq!(resv_row(&conn, "rX").unwrap().0, "active");
    assert_eq!(event_count(&conn), 0);
    // And the failure left no open transaction: settle succeeds once fixed.
    conn.execute_batch("DROP TRIGGER budget_test_fail_events")
        .unwrap();
    assert!(db
        .mark_spend_reservation_spent("rX", Some(300), None, true, NOW)
        .unwrap());
    assert_eq!(resv_row(&conn, "rX").unwrap().0, "spent");
    assert_eq!(event_count(&conn), 1);
}

// ---------------------------------------------------------------------------
// record_spend_event (P2-008)
// ---------------------------------------------------------------------------

fn event(eid: &str, cat: &str, amount: i64) -> SpendEvent {
    SpendEvent {
        event_id: eid.to_string(),
        category: cat.to_string(),
        amount_sats: amount,
        timestamp: NOW,
        ..Default::default()
    }
}

#[test]
fn record_spend_event_normalizes_and_replaces() {
    let (dir, mut db) = fresh();
    db.record_spend_event(event(" e1 ", " Misc ", 40)).unwrap();
    db.record_spend_event(event("e1", "misc", 55)).unwrap(); // INSERT OR REPLACE
    let conn = side(&dir);
    let (eid, cat, amt): (String, String, i64) = conn
        .query_row(
            "SELECT event_id, category, amount_sats FROM spend_events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!((eid.as_str(), cat.as_str(), amt), ("e1", "misc", 55));
}

#[test]
fn record_spend_event_rejects_nonpositive_and_empty() {
    let (dir, mut db) = fresh();
    assert!(db.record_spend_event(event("e", "c", 0)).is_err());
    assert!(db.record_spend_event(event("e", "c", -10)).is_err());
    assert!(db.record_spend_event(event("", "c", 5)).is_err());
    assert!(db.record_spend_event(event("e", "  ", 5)).is_err());
    assert_eq!(event_count(&side(&dir)), 0);
}

#[test]
fn record_spend_event_retries_then_errs_on_held_write_lock() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    // Short busy timeout so 3 exhausted attempts stay fast (test seam only;
    // production open() pins 5000ms).
    let mut db = BudgetDb::open_with_busy_timeout(&path, 25).unwrap();
    let blocker = side(&dir);
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let err = db
        .record_spend_event(event("e-busy", "misc", 9))
        .unwrap_err();
    assert!(matches!(err, BudgetError::Sqlite(_)));
    blocker.execute_batch("ROLLBACK").unwrap();
    // Never a silent lost write: after the lock clears, the same event lands.
    db.record_spend_event(event("e-busy", "misc", 9)).unwrap();
    assert_eq!(event_count(&side(&dir)), 1);
}

// ---------------------------------------------------------------------------
// reads: states + category sums
// ---------------------------------------------------------------------------

#[test]
fn spend_reservation_states_filters_and_orders() {
    let (_dir, mut db) = fresh();
    assert_eq!(
        db.reserve_spend(req("b", 20, "misc"), NOW).unwrap(),
        (true, 0)
    );
    assert_eq!(
        db.reserve_spend(req("a", 10, "misc"), NOW).unwrap(),
        (true, 0)
    );
    assert!(db.release_spend_reservation("b").unwrap());
    let all = db.get_spend_reservation_states(None).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all["a"].status, "active");
    assert_eq!(all["a"].reserved_sats, 10);
    assert_eq!(all["b"].status, "released");
    assert_eq!(all["b"].reserved_sats, 20);
    let some = db
        .get_spend_reservation_states(Some(&["a".into(), "zzz".into(), "   ".into()]))
        .unwrap();
    assert_eq!(some.len(), 1);
    assert!(some.contains_key("a"));
    assert!(db
        .get_spend_reservation_states(Some(&[]))
        .unwrap()
        .is_empty());
    assert!(db
        .get_spend_reservation_states(Some(&["  ".into()]))
        .unwrap()
        .is_empty());
}

#[test]
fn category_spend_sums_normalize_and_window() {
    let (_dir, mut db) = fresh();
    db.record_spend_event(event("e1", "boltz", 100)).unwrap();
    db.record_spend_event(SpendEvent {
        subcategory: Some("swap".into()),
        ..event("e2", "boltz", 50)
    })
    .unwrap();
    db.record_spend_event(event("e3", "misc", 70)).unwrap();
    assert_eq!(db.get_category_spend_sats(" Boltz ", None, 0).unwrap(), 150);
    assert_eq!(
        db.get_category_spend_sats("boltz", Some("swap"), 0)
            .unwrap(),
        50
    );
    assert_eq!(
        db.get_category_spend_sats("boltz", None, NOW + 1).unwrap(),
        0
    );
    assert_eq!(db.get_category_spend_sats("ghost", None, 0).unwrap(), 0);
}

// ---------------------------------------------------------------------------
// sweeps: P4-015 / P4-021 / clear-all / count
// ---------------------------------------------------------------------------

#[test]
fn cleanup_stale_reservations_skips_pending_settlement_in_both_tables() {
    let (dir, mut db) = fresh();
    let conn = side(&dir);
    conn.execute_batch("CREATE TABLE rebalance_history (id INTEGER PRIMARY KEY, status TEXT);")
        .unwrap();
    conn.execute_batch(
        "INSERT INTO rebalance_history (id, status) VALUES \
         (42, 'pending_settlement'), (43, 'complete'), (46, 'pending_settlement');",
    )
    .unwrap();
    let old = NOW - 8 * 3600;
    // Legacy table.
    seed_budget_resv(&conn, "42", 100, old, "active"); // pending -> hold
    seed_budget_resv(&conn, "43", 100, old, "active"); // resolved -> release
    seed_budget_resv(&conn, "44", 100, NOW - 100, "active"); // fresh -> keep
                                                             // Unified table (category='rebalance' only).
    seed_spend_resv(&conn, "46", "rebalance", 100, old, "active"); // pending -> hold
    seed_spend_resv(&conn, "47", "rebalance", 100, old, "active"); // release
    seed_spend_resv(&conn, "48", "rebalance", 100, NOW - 100, "active"); // fresh
    seed_spend_resv(&conn, "49", "channel_open", 100, old, "active"); // other category
    let released = db.cleanup_stale_reservations(14_400, NOW).unwrap();
    assert_eq!(released, 2);
    assert_eq!(legacy_status(&conn, "42").unwrap(), "active");
    assert_eq!(legacy_status(&conn, "43").unwrap(), "released");
    assert_eq!(legacy_status(&conn, "44").unwrap(), "active");
    assert_eq!(resv_row(&conn, "46").unwrap().0, "active");
    assert_eq!(resv_row(&conn, "47").unwrap().0, "released");
    assert_eq!(resv_row(&conn, "48").unwrap().0, "active");
    assert_eq!(resv_row(&conn, "49").unwrap().0, "active");
}

#[test]
fn cleanup_stale_spend_reservations_blind_vs_explicit() {
    let (dir, mut db) = fresh();
    let conn = side(&dir);
    let old = NOW - 2 * DAY;
    for (rid, cat) in [
        ("c1", "channel_open"),
        ("c2", "channel_close"),
        ("c3", "boltz"),
        ("r1", "rebalance"),
        ("m1", "misc"),
    ] {
        seed_spend_resv(&conn, rid, cat, 10, old, "active");
    }
    // Blind sweep (P4-021): committed on-chain categories are untouchable.
    assert_eq!(
        db.cleanup_stale_spend_reservations(DAY, None, NOW).unwrap(),
        2
    );
    for rid in ["c1", "c2", "c3"] {
        assert_eq!(resv_row(&conn, rid).unwrap().0, "active");
    }
    for rid in ["r1", "m1"] {
        assert_eq!(resv_row(&conn, rid).unwrap().0, "released");
    }
    // Explicit category sweep reaches everything (normalized like reserve).
    assert_eq!(
        db.cleanup_stale_spend_reservations(DAY, Some(" Channel_Open "), NOW)
            .unwrap(),
        1
    );
    assert_eq!(resv_row(&conn, "c1").unwrap().0, "released");
    assert_eq!(
        db.cleanup_stale_spend_reservations(DAY, Some("boltz"), NOW)
            .unwrap(),
        1
    );
    assert_eq!(resv_row(&conn, "c3").unwrap().0, "released");
}

#[test]
fn count_and_clear_all_reservations_are_legacy_table_scoped() {
    let (dir, mut db) = fresh();
    let conn = side(&dir);
    seed_budget_resv(&conn, "L1", 100, NOW - 8 * 3600, "active");
    seed_budget_resv(&conn, "L2", 250, NOW - 100, "active");
    seed_budget_resv(&conn, "L3", 999, NOW - 8 * 3600, "released");
    seed_spend_resv(&conn, "U1", "misc", 400, NOW - 8 * 3600, "active");
    assert_eq!(db.count_stale_reservations(14_400, NOW).unwrap(), 1);
    let stats = db.clear_all_reservations().unwrap();
    assert_eq!(stats.cleared_count, 2);
    assert_eq!(stats.released_sats, 350);
    assert_eq!(legacy_status(&conn, "L1").unwrap(), "released");
    assert_eq!(legacy_status(&conn, "L2").unwrap(), "released");
    // I-1 mirrors Python: clear_all touches ONLY budget_reservations.
    assert_eq!(resv_row(&conn, "U1").unwrap().0, "active");
    // Idempotent: nothing left to clear.
    let stats = db.clear_all_reservations().unwrap();
    assert_eq!((stats.cleared_count, stats.released_sats), (0, 0));
}

// ---------------------------------------------------------------------------
// Phase 2J compatibility wrappers
// ---------------------------------------------------------------------------

#[test]
fn reserve_budget_wrapper_lands_in_unified_ledger() {
    let (dir, mut db) = fresh();
    let (ok, rem) = db
        .reserve_budget("w1", 500, "chan-9", 10_000, SINCE, None, None, NOW)
        .unwrap();
    assert!(ok);
    assert_eq!(rem, 9500);
    let conn = side(&dir);
    let (cat, chan): (String, String) = conn
        .query_row(
            "SELECT category, channel_id FROM spend_reservations WHERE reservation_id='w1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(cat, "rebalance");
    assert_eq!(chan, "chan-9");
    // Legacy table stays transition-read-only: no new rows.
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM budget_reservations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
    // Over-budget refusal flows through with the unified remaining.
    let (ok, rem) = db
        .reserve_budget("w2", 9501, "chan-9", 10_000, SINCE, None, None, NOW)
        .unwrap();
    assert!(!ok);
    assert_eq!(rem, 9500);
}

#[test]
fn release_budget_reservation_dual_path() {
    let (dir, mut db) = fresh();
    let conn = side(&dir);
    // Unified row first.
    assert_eq!(
        db.reserve_spend(req("U", 10, "rebalance"), NOW).unwrap(),
        (true, 0)
    );
    assert!(db.release_budget_reservation("U").unwrap());
    assert_eq!(resv_row(&conn, "U").unwrap().0, "released");
    // Legacy fallback: a pre-unification row created by hand.
    seed_budget_resv(&conn, "L", 20, NOW - 100, "active");
    assert!(db.release_budget_reservation("L").unwrap());
    assert_eq!(legacy_status(&conn, "L").unwrap(), "released");
    assert!(!db.release_budget_reservation("L").unwrap()); // terminal now
    assert!(!db.release_budget_reservation("ghost").unwrap());
}

#[test]
fn mark_budget_spent_dual_path_records_no_event() {
    let (dir, mut db) = fresh();
    let conn = side(&dir);
    assert_eq!(
        db.reserve_spend(req("U", 10, "rebalance"), NOW).unwrap(),
        (true, 0)
    );
    assert!(db.mark_budget_spent("U", 9).unwrap());
    assert_eq!(resv_row(&conn, "U").unwrap().0, "spent");
    // record_event=false: actual rebalance costs live in rebalance_costs.
    assert_eq!(event_count(&conn), 0);
    seed_budget_resv(&conn, "L", 20, NOW - 100, "active");
    assert!(db.mark_budget_spent("L", 20).unwrap());
    assert_eq!(legacy_status(&conn, "L").unwrap(), "spent");
    assert!(!db.mark_budget_spent("ghost", 1).unwrap());
}
