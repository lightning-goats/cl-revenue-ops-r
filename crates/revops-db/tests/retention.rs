use revops_db::fee_runway::{self, FeeCycleCommit, FeeStateRow, FeeTriggerEventRow};
use revops_db::notifications;
use revops_db::retention::{
    RetentionCursor, CURRENT_STATE_TABLES, DEFERRED_TABLES, EXCLUDED_TABLES, RETENTION_BATCH_ROWS,
    RETENTION_MAX_BATCHES_PER_SWEEP, RUNWAY_EVIDENCE_RETENTION_SECONDS, SNAPSHOT_KEEP_LAST,
    SQLITE_INTERNAL, WINDOWED_TABLES,
};
use rusqlite::{params, Connection};
use std::collections::{BTreeMap, BTreeSet};

fn schema() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    notifications::init_schema(&conn).unwrap();
    conn
}

#[test]
fn retention_classifies_every_table_including_sqlite_internal() {
    let conn = schema();
    let actual: BTreeSet<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();

    let mut counts = BTreeMap::<&str, usize>::new();
    for table in EXCLUDED_TABLES
        .iter()
        .chain(CURRENT_STATE_TABLES)
        .chain(DEFERRED_TABLES)
        .chain(WINDOWED_TABLES)
        .chain(SQLITE_INTERNAL)
    {
        *counts.entry(table).or_default() += 1;
    }
    assert!(counts.values().all(|count| *count == 1));
    assert_eq!(
        actual,
        counts.keys().map(|name| (*name).to_string()).collect()
    );
}

#[test]
fn sweep_batches_bounded_globally_and_fair_across_tables() {
    let conn = schema();
    let now = 2_000_000_000_i64;
    let old = now - RUNWAY_EVIDENCE_RETENTION_SECONDS - 1;
    let backlog = RETENTION_BATCH_ROWS as i64 * 3;
    for i in 0..backlog {
        conn.execute(
            "INSERT INTO rust_fee_trigger_events
                 (trigger_type, received_at, coalesced, detail)
             VALUES ('test', ?1, 0, ?2)",
            params![old, format!("trigger-{i}")],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO rust_mempool_ma_comparison
                 (at, cycle_ts, rust_ma, python_ma, delta)
             VALUES (?1, ?1, 1.0, 1.0, 0.0)",
            params![old],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO rust_runway_snapshots
                 (snapshot_at, report_schema_version, source_commit, binary_sha256, summary_json)
             VALUES (?1, 'v1', 'test', 'hash', '{}')",
            params![old],
        )
        .unwrap();
    }

    // Shadow outcomes carry a foreign key, so seed them through old cycles.
    for i in 0..backlog {
        let cycle_id = format!("cycle-{i}");
        fee_runway::commit_fee_cycle(
            &conn,
            &FeeCycleCommit {
                cycle_id: cycle_id.clone(),
                started_at: old,
                completed_at: old,
                state_rows: vec![FeeStateRow {
                    channel_id: format!("state-{i}"),
                    v2_state_json: "{}".into(),
                    last_update: old,
                }],
                outcomes: vec![fee_runway::ShadowCycleOutcomeRow {
                    cycle_ts: old,
                    channel_id: format!("channel-{i}"),
                    would_broadcast: false,
                    has_algorithm_values: false,
                    disposition: None,
                    skip_gate_comparable: true,
                }],
                ..FeeCycleCommit::default()
            },
        )
        .unwrap();
    }

    let mut cursor = RetentionCursor::default();
    let first = fee_runway::run_retention_sweep(&conn, now, cursor).unwrap();
    assert_eq!(first.batches, RETENTION_MAX_BATCHES_PER_SWEEP);
    assert!(first.truncated);
    assert!(first.deleted.values().all(|rows| *rows > 0));
    cursor = first.next_cursor;

    for _ in 0..8 {
        let report = fee_runway::run_retention_sweep(&conn, now, cursor).unwrap();
        cursor = report.next_cursor;
        if !report.truncated {
            break;
        }
    }

    for table in [
        "rust_fee_shadow_outcomes",
        "rust_fee_trigger_events",
        "rust_mempool_ma_comparison",
    ] {
        let count: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "{table} must eventually drain");
    }
    let snapshots: i64 = conn
        .query_row("SELECT COUNT(*) FROM rust_runway_snapshots", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(snapshots, SNAPSHOT_KEEP_LAST as i64);
}

#[test]
fn sweep_preserves_never_prune_and_current_state_rows() {
    let conn = schema();
    let now = 2_000_000_000_i64;
    let old = now - RUNWAY_EVIDENCE_RETENTION_SECONDS - 1;
    fee_runway::commit_fee_cycle(
        &conn,
        &FeeCycleCommit {
            cycle_id: "a3-old-identity".into(),
            started_at: old,
            completed_at: old,
            state_rows: vec![FeeStateRow {
                channel_id: "1x1x0".into(),
                v2_state_json: "{}".into(),
                last_update: old,
            }],
            ..FeeCycleCommit::default()
        },
    )
    .unwrap();
    conn.execute(
        "INSERT INTO rust_execution_quarantine
             (entered_at, reason, cycle_id, channel_id, request_id, cleared_at)
         VALUES (?1, 'test', 'a3-old-identity', '1x1x0', 'request-1', NULL)",
        params![old],
    )
    .unwrap();
    fee_runway::insert_consumed_nonce(&conn, "nonce-1", old, "commit-1", "sha-1", old).unwrap();

    let mut cursor = RetentionCursor::default();
    for _ in 0..3 {
        let report = fee_runway::run_retention_sweep(&conn, now, cursor).unwrap();
        cursor = report.next_cursor;
    }
    assert!(fee_runway::cycle_exists(&conn, "a3-old-identity").unwrap());
    assert_eq!(fee_runway::current_state_generation(&conn).unwrap(), 1);
    let quarantine: i64 = conn
        .query_row("SELECT COUNT(*) FROM rust_execution_quarantine", [], |r| {
            r.get(0)
        })
        .unwrap();
    let nonces: i64 = conn
        .query_row("SELECT COUNT(*) FROM rust_consumed_arm_nonces", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!((quarantine, nonces), (1, 1));
}

#[tokio::test]
async fn owner_dispatches_bounded_sweep_and_advances_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = revops_db::owner::spawn_read_write(&path).await.unwrap();

    let old = 1_700_000_000_i64;
    let now = old + RUNWAY_EVIDENCE_RETENTION_SECONDS + 86_400;
    for i in 0..3 {
        handle
            .record_fee_trigger_event(FeeTriggerEventRow {
                trigger_type: "forward".into(),
                channel_id: None,
                cycle_id: None,
                cycle_ts: None,
                received_at: old + i,
                coalesced: false,
                detail: None,
            })
            .await
            .unwrap();
    }

    let report = handle
        .run_retention_sweep(now, RetentionCursor::default())
        .await
        .unwrap();
    assert_eq!(report.deleted.get("rust_fee_trigger_events"), Some(&3));
    assert!(!report.truncated);
    assert_ne!(
        report.next_cursor,
        RetentionCursor::default(),
        "cursor must advance so the next sweep starts at a different table"
    );
}
