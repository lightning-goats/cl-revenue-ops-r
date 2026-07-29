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

/// R0 structural pin: every DELETE statement in `fee_runway.rs` names a
/// Class-W table (or the sweep's private-enum interpolation, whose own
/// `table: "..."` literals are scanned too). A DELETE against any other
/// class is a classification violation, not a tuning choice.
#[test]
fn sweep_statements_touch_only_windowed_tables() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/fee_runway.rs")).unwrap();
    let windowed: BTreeSet<&str> = WINDOWED_TABLES.iter().copied().collect();

    // Task 63 deliberate widening: two OWNER-EXPLICIT deletes that are
    // not retention sweeps -- an operator un-ignoring an external swap,
    // and the journal prune to the exact keep-set the
    // `revops_boltz::journal` kernel computed (180d/200 entries). Both
    // tables stay Class E for the SWEEP (the sweep must never touch
    // them); these are the sanctioned non-sweep call sites. Widening
    // this list further is a deliberate decision.
    let owner_explicit: BTreeSet<&str> = ["rust_boltz_ignores", "rust_boltz_journal"]
        .into_iter()
        .collect();

    let mut checked = 0usize;
    for (idx, _) in source.match_indices("DELETE FROM") {
        let after = source[idx + "DELETE FROM".len()..].trim_start();
        let name: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '{' || *c == '}')
            .collect();
        checked += 1;
        if name == "{table}" {
            continue; // covered by the `table: "..."` literal scan below
        }
        if owner_explicit.contains(name.as_str()) {
            continue;
        }
        assert!(
            windowed.contains(name.as_str()),
            "DELETE targets non-Class-W table `{name}`"
        );
    }
    assert!(
        checked >= 3,
        "expected the known DELETE statements, found {checked}"
    );

    let mut literals = 0usize;
    for (idx, _) in source.match_indices("table: \"") {
        let after = &source[idx + "table: \"".len()..];
        let name = &after[..after.find('"').unwrap()];
        literals += 1;
        assert!(
            windowed.contains(name),
            "sweep target names non-Class-W table `{name}`"
        );
    }
    assert!(
        literals >= 3,
        "expected the sweep target literals, found {literals}"
    );
}

/// R2: request/ledger child rows of an OLD cycle survive repeated sweeps
/// as exact row SETS -- append-only evidence is never collateral of
/// Class-W pruning.
#[test]
fn sweep_preserves_actual_request_and_ledger_child_rows() {
    let conn = schema();
    let now = 2_000_000_000_i64;
    let old = now - RUNWAY_EVIDENCE_RETENTION_SECONDS - 1;
    fee_runway::commit_fee_cycle(
        &conn,
        &FeeCycleCommit {
            cycle_id: "old-cycle-with-children".into(),
            started_at: old,
            completed_at: old,
            state_rows: vec![FeeStateRow {
                channel_id: "1x1x0".into(),
                v2_state_json: "{}".into(),
                last_update: old,
            }],
            requests: vec![fee_runway::PreparedFeeActionRow {
                channel_id: "1x1x0".into(),
                idempotency_key: Some("idem-1".into()),
                old_fee_ppm: 100,
                new_fee_ppm: 120,
                feebase_msat: 0,
                htlcmin_msat: None,
                htlcmax_msat: None,
                message: "old request".into(),
                at: old,
            }],
            ledger: vec![fee_runway::LedgerAuditRow {
                channel_id: "1x1x0".into(),
                event_type: "test".into(),
                intent_id: "intent-1".into(),
                idempotency_key: "idem-1".into(),
                snapshot_id: "snap-1".into(),
                at: old,
                details_json: "{}".into(),
            }],
            ..FeeCycleCommit::default()
        },
    )
    .unwrap();

    let rows = |table: &str| -> Vec<String> {
        conn.prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
            .unwrap()
            .query_map([], |row| {
                let mut cells = Vec::new();
                let mut i = 0;
                while let Ok(value) = row.get::<_, rusqlite::types::Value>(i) {
                    cells.push(format!("{value:?}"));
                    i += 1;
                }
                Ok(cells.join("|"))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    let requests_before = rows("rust_fee_requests");
    let ledger_before = rows("rust_fee_ledger");
    assert!(!requests_before.is_empty() && !ledger_before.is_empty());

    let mut cursor = RetentionCursor::default();
    for _ in 0..5 {
        cursor = fee_runway::run_retention_sweep(&conn, now, cursor)
            .unwrap()
            .next_cursor;
    }

    assert_eq!(rows("rust_fee_requests"), requests_before);
    assert_eq!(rows("rust_fee_ledger"), ledger_before);
    assert!(fee_runway::cycle_exists(&conn, "old-cycle-with-children").unwrap());
}

/// Task 60 slice 1 + task-59 review follow-up (facts:1720): the
/// never-prune list is pinned by EXACT membership, not just partition
/// completeness -- silently reclassifying an append-only evidence table
/// out of Class E must red here even though the fixed SweepTarget enum
/// makes it inert against the sweep itself.
#[test]
fn never_prune_membership_is_pinned_exactly() {
    let expected: BTreeSet<&str> = [
        "rust_fee_cycles",
        "rust_fee_requests",
        "rust_fee_ledger",
        "rust_broadcast_attempts",
        "rust_execution_quarantine",
        "rust_fee_seed_events",
        "rust_fee_restart_markers",
        "rust_consumed_arm_nonces",
        "rust_rebalance_attempts",
        "rust_rebalance_reservations",
        "rust_capital_intents",
        "rust_capital_reservations",
        "rust_boltz_attempts",
        "rust_boltz_reservations",
        "rust_boltz_ignores",
        "rust_boltz_cooldowns",
        "rust_boltz_journal",
        "rust_boot_sessions",
        "rust_financial_snapshots",
    ]
    .into_iter()
    .collect();
    let actual: BTreeSet<&str> = EXCLUDED_TABLES.iter().copied().collect();
    assert_eq!(
        actual, expected,
        "EXCLUDED_TABLES (the never-prune list) changed membership -- adding a table \
         here requires a deliberate edit to BOTH the classification and this pin"
    );
}
