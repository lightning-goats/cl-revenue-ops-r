//! Notification-ingestion parity + dedup tests for `revops_db::notifications`
//! -- the Rust plugin's OWN writable sqlite file (never production). See
//! `docs/superpowers/plans/2026-07-17-phase1b-observer.md` Task 2.

use revops_db::notifications::{
    compute_forward_hydration_start, init_schema, insert_channel_closure_event,
    insert_forward_ignore_dup, insert_peer_connection_event, last_forward_ts, ForwardRow,
};
use rusqlite::Connection;

fn sample() -> ForwardRow {
    ForwardRow {
        in_channel: "1x1x0".into(),
        out_channel: "2x2x0".into(),
        in_msat: 100_000,
        out_msat: 99_000,
        fee_msat: 1_000,
        timestamp: 1_800_000_000,
        resolved_time: 1_800_000_005,
    }
}

#[test]
fn hydration_start_matches_python() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/hydration.json"))
            .expect("fixtures/hydration.json must parse");
    for c in cases.as_array().unwrap() {
        let last = c["last_forward_ts"].as_i64();
        let flow_window_days = c["flow_window_days"].as_i64().unwrap();
        let now = c["now"].as_i64().unwrap();
        let expected = c["result"].as_i64();
        assert_eq!(
            compute_forward_hydration_start(last, flow_window_days, now),
            expected,
            "case={c:?}"
        );
    }
}

#[test]
fn dedup_ignores_exact_duplicate_insert() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    assert!(
        insert_forward_ignore_dup(&conn, &sample()).unwrap(),
        "first insert"
    );
    assert!(
        !insert_forward_ignore_dup(&conn, &sample()).unwrap(),
        "dup must be ignored"
    );
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ingested_forwards", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn hydration_and_live_insert_race_safely() {
    // Simulates the exact scenario the design doc calls out: startup
    // hydration and a live forward_event for the SAME forward can both
    // attempt an insert. Both must succeed at the DB layer (no error),
    // and the row count must still be 1.
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    let row = sample();
    insert_forward_ignore_dup(&conn, &row).unwrap();
    insert_forward_ignore_dup(&conn, &row).unwrap(); // "hydration" reinserting what "live" already wrote
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ingested_forwards", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(last_forward_ts(&conn).unwrap(), Some(1_800_000_000));
}

#[test]
fn distinct_forwards_both_inserted() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    let a = sample();
    let mut b = sample();
    b.timestamp += 1;
    assert!(insert_forward_ignore_dup(&conn, &a).unwrap());
    assert!(insert_forward_ignore_dup(&conn, &b).unwrap());
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM ingested_forwards", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(last_forward_ts(&conn).unwrap(), Some(1_800_000_001));
}

#[test]
fn last_forward_ts_is_none_on_empty_table() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    assert_eq!(last_forward_ts(&conn).unwrap(), None);
}

#[test]
fn init_schema_is_idempotent() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    init_schema(&conn).unwrap(); // must not error on a second call
}

#[test]
fn peer_connection_event_insert_and_count() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    insert_peer_connection_event(&conn, "03deadbeef", "connected", 1_800_000_000).unwrap();
    insert_peer_connection_event(&conn, "03deadbeef", "disconnected", 1_800_000_010).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM peer_connection_events", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn channel_closure_event_insert_and_count() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    insert_channel_closure_event(&conn, "1x1x0", "remote", 1_800_000_000).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM channel_closure_events", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
}
