//! Notification-ingestion parity + dedup tests for `revops_db::notifications`
//! -- the Rust plugin's OWN writable sqlite file (never production). See
//! `docs/superpowers/plans/2026-07-17-phase1b-observer.md` Task 2.

use revops_db::fee_runway::{
    active_quarantine, commit_fee_cycle, current_state_generation, insert_quarantine,
    latest_runway_snapshot, load_latest_state, mempool_sample_stats, mutation_count,
    query_mempool_ma_comparisons_since, query_mempool_samples_since, record_mempool_ma_comparison,
    record_mempool_sample, record_mempool_sample_pruned, record_runway_snapshot,
    record_trigger_event, FeeCycleCommit, FeeStateRow, FeeTriggerEventRow, GovernorAuditRow,
    LedgerAuditRow, MempoolMaComparisonRow, PreparedFeeActionRow, QuarantineEntry,
    RunwaySnapshotRow, ShadowCycleOutcomeRow,
};
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

// ---------------------------------------------------------------------------
// Task 4 (stateful-shadow plan): transactional Rust-owned fee state +
// audit schema (`revops_db::fee_runway`).
// ---------------------------------------------------------------------------

const RUST_FEE_TABLES: &[&str] = &[
    "rust_fee_state",
    "rust_fee_state_generation",
    "rust_fee_cycles",
    "rust_fee_requests",
    "rust_fee_shadow_outcomes",
    "rust_mempool_fee_history",
    "rust_mempool_ma_comparison",
    "rust_fee_trigger_events",
    "rust_fee_ledger",
    "rust_execution_quarantine",
    "rust_runway_snapshots",
];

fn table_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap()
        == 1
}

#[test]
fn rust_fee_schema_creates_all_tables_idempotently() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    for table in RUST_FEE_TABLES {
        assert!(table_exists(&conn, table), "missing table {table}");
    }
    // A second init call over the same connection must not error (the
    // brief's "repeated initialization is safe" expectation).
    init_schema(&conn).unwrap();
    for table in RUST_FEE_TABLES {
        assert!(table_exists(&conn, table), "table {table} lost on re-init");
    }
}

#[test]
fn rust_fee_schema_foreign_key_rejects_orphan_request() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    // No `rust_fee_cycles` row for "no-such-cycle" exists -- the FK must
    // reject this insert (PRAGMA foreign_keys=ON, set by init_schema).
    let result = conn.execute(
        "INSERT INTO rust_fee_requests
             (cycle_id, channel_id, idempotency_key, old_fee_ppm, new_fee_ppm,
              feebase_msat, htlcmin_msat, htlcmax_msat, message, at)
         VALUES ('no-such-cycle', '1x1x0', NULL, 100, 150, 0, NULL, NULL, 'Fee set to 150 PPM', 1)",
        [],
    );
    assert!(result.is_err(), "orphan cycle_id must be rejected by FK");
}

fn sample_commit(cycle_id: &str, at: i64) -> FeeCycleCommit {
    FeeCycleCommit {
        cycle_id: cycle_id.to_string(),
        started_at: at,
        completed_at: at + 1,
        source_commit: "f7ccc24".to_string(),
        binary_sha256: "deadbeef".repeat(8),
        state_rows: vec![FeeStateRow {
            channel_id: "1x1x0".to_string(),
            v2_state_json: r#"{"algorithm_version": "dts_pid_v1"}"#.to_string(),
            last_update: at,
        }],
        requests: vec![PreparedFeeActionRow {
            channel_id: "1x1x0".to_string(),
            idempotency_key: Some("idem-1".to_string()),
            old_fee_ppm: 100,
            new_fee_ppm: 150,
            feebase_msat: 0,
            htlcmin_msat: Some(1000),
            htlcmax_msat: None,
            message: "Fee set to 150 PPM".to_string(),
            at,
        }],
        governor: vec![GovernorAuditRow {
            channel_id: "1x1x0".to_string(),
            authorized: true,
            reason_code: "authorized".to_string(),
            intent_id: "intent-1".to_string(),
            idempotency_key: "idem-1".to_string(),
            at,
        }],
        ledger: vec![LedgerAuditRow {
            channel_id: "1x1x0".to_string(),
            event_type: "intent_proposed".to_string(),
            intent_id: "intent-1".to_string(),
            idempotency_key: "idem-1".to_string(),
            snapshot_id: format!("fee-broadcast-{at}"),
            at,
            details_json: r#"{"target": "1x1x0"}"#.to_string(),
        }],
        outcomes: vec![ShadowCycleOutcomeRow {
            cycle_ts: at,
            channel_id: "1x1x0".to_string(),
            would_broadcast: true,
            has_algorithm_values: true,
            disposition: Some("broadcast".to_string()),
            skip_gate_comparable: true,
        }],
        pending_seed: None,
        trigger_receipt: None,
    }
}

#[test]
fn rust_fee_schema_commit_and_load_state_round_trip() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();

    let generation = commit_fee_cycle(&conn, &sample_commit("cycle-1", 1_800_000_000)).unwrap();
    assert_eq!(generation, 1);

    let snapshot = load_latest_state(&conn).unwrap();
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(snapshot.rows[0].channel_id, "1x1x0");
    assert_eq!(
        snapshot.rows[0].v2_state_json,
        r#"{"algorithm_version": "dts_pid_v1"}"#
    );

    let request_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rust_fee_requests", [], |r| r.get(0))
        .unwrap();
    assert_eq!(request_count, 1);
    let ledger_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rust_fee_ledger", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ledger_count, 2, "one governor row + one ledger row");
    let outcome_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rust_fee_shadow_outcomes", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(outcome_count, 1);

    // A second cycle bumps the generation and REPLACES the channel's state
    // row (not a new row -- `rust_fee_state` holds only the latest).
    let mut second = sample_commit("cycle-2", 1_800_000_100);
    second.state_rows[0].v2_state_json = r#"{"algorithm_version": "dts_pid_v2"}"#.to_string();
    let generation2 = commit_fee_cycle(&conn, &second).unwrap();
    assert_eq!(generation2, 2);
    let snapshot2 = load_latest_state(&conn).unwrap();
    assert_eq!(snapshot2.generation, 2);
    assert_eq!(snapshot2.rows.len(), 1, "still one row per channel");
    assert_eq!(
        snapshot2.rows[0].v2_state_json,
        r#"{"algorithm_version": "dts_pid_v2"}"#
    );
}

#[test]
fn rust_fee_schema_rollback_on_injected_request_failure() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();

    // Establish a known-good baseline generation and request count.
    commit_fee_cycle(&conn, &sample_commit("cycle-1", 1_800_000_000)).unwrap();
    let baseline_generation = load_latest_state(&conn).unwrap().generation;
    let baseline_request_count = mutation_count(&conn).unwrap();
    assert_eq!(baseline_generation, 1);
    assert_eq!(baseline_request_count, 1);

    // Inject a request-row failure: two requests in the SAME cycle with
    // the same channel_id collide on the UNIQUE(cycle_id, channel_id)
    // constraint.
    let mut broken = sample_commit("cycle-2", 1_800_000_100);
    let duplicate = broken.requests[0].clone();
    broken.requests.push(duplicate);

    let result = commit_fee_cycle(&conn, &broken);
    assert!(result.is_err(), "duplicate request identity must fail");

    // State, cycle, ledger, and requests must all be rolled back together
    // -- the previous generation and request count are UNCHANGED.
    let snapshot = load_latest_state(&conn).unwrap();
    assert_eq!(snapshot.generation, baseline_generation);
    assert_eq!(mutation_count(&conn).unwrap(), baseline_request_count);
    let cycle_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM rust_fee_cycles WHERE cycle_id = 'cycle-2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cycle_count, 0, "the failed cycle row must not exist");
    let ledger_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM rust_fee_ledger WHERE cycle_id = 'cycle-2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        ledger_count, 0,
        "the failed cycle's ledger rows must not exist"
    );
    let outcome_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM rust_fee_shadow_outcomes WHERE cycle_id = 'cycle-2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        outcome_count, 0,
        "the failed cycle's outcome rows must not exist"
    );
}

#[test]
fn rust_fee_schema_shadow_outcome_columns_match_engagement_gate_contract() {
    // 2026-07-26 revision plan Task R8 amendment #1: exact column names
    // and Python-side truthiness (0/1 INTEGER) contract.
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    let mut commit = sample_commit("cycle-1", 1_800_000_000);
    commit.outcomes = vec![
        ShadowCycleOutcomeRow {
            cycle_ts: 1_800_000_000,
            channel_id: "1x1x0".to_string(),
            would_broadcast: true,
            has_algorithm_values: true,
            disposition: Some("broadcast".to_string()),
            skip_gate_comparable: true,
        },
        ShadowCycleOutcomeRow {
            cycle_ts: 1_800_000_000,
            channel_id: "2x2x0".to_string(),
            would_broadcast: false,
            has_algorithm_values: false,
            disposition: Some("waiting_window".to_string()),
            skip_gate_comparable: false,
        },
    ];
    commit.requests.clear(); // avoid an unrelated FK/unique collision
    commit_fee_cycle(&conn, &commit).unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT cycle_ts, channel_id, would_broadcast, has_algorithm_values,
                    disposition, skip_gate_comparable
             FROM rust_fee_shadow_outcomes ORDER BY channel_id",
        )
        .unwrap();
    let rows: Vec<(i64, String, i64, i64, Option<String>, i64)> = stmt
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        (
            1_800_000_000,
            "1x1x0".to_string(),
            1,
            1,
            Some("broadcast".to_string()),
            1
        )
    );
    assert_eq!(
        rows[1],
        (
            1_800_000_000,
            "2x2x0".to_string(),
            0,
            0,
            Some("waiting_window".to_string()),
            0
        )
    );
}

/// Fix round 1 (I-5): `current_state_generation` must agree with
/// `load_latest_state(&conn).generation` at every point -- cold start (0),
/// after one commit (1), and after a second (2) -- without ever reading
/// the `rust_fee_state` rows table.
#[test]
fn current_state_generation_agrees_with_load_latest_state() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    assert_eq!(current_state_generation(&conn).unwrap(), 0);
    assert_eq!(load_latest_state(&conn).unwrap().generation, 0);

    commit_fee_cycle(&conn, &sample_commit("cycle-1", 1_800_000_000)).unwrap();
    assert_eq!(current_state_generation(&conn).unwrap(), 1);
    assert_eq!(load_latest_state(&conn).unwrap().generation, 1);

    commit_fee_cycle(&conn, &sample_commit("cycle-2", 1_800_000_100)).unwrap();
    assert_eq!(current_state_generation(&conn).unwrap(), 2);
    assert_eq!(load_latest_state(&conn).unwrap().generation, 2);
}

#[test]
fn rust_fee_schema_mempool_samples_record_and_query() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    record_mempool_sample(&conn, 1_800_000_000, 12.5).unwrap();
    record_mempool_sample(&conn, 1_800_003_600, 15.0).unwrap();
    record_mempool_sample(&conn, 1_799_990_000, 9.0).unwrap(); // before the window

    let samples = query_mempool_samples_since(&conn, 1_800_000_000).unwrap();
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].sampled_at, 1_800_000_000);
    assert_eq!(samples[0].sat_per_vbyte, 12.5);
    assert_eq!(samples[1].sampled_at, 1_800_003_600);
}

/// Fix round 1 (I-5): `mempool_sample_stats` must agree with
/// `query_mempool_samples_since(..).len()`/`.last()` without ever fetching
/// the rows themselves.
#[test]
fn mempool_sample_stats_agrees_with_query_mempool_samples_since() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    record_mempool_sample(&conn, 1_800_000_000, 12.5).unwrap();
    record_mempool_sample(&conn, 1_800_003_600, 15.0).unwrap();
    record_mempool_sample(&conn, 1_799_990_000, 9.0).unwrap(); // before the window

    let stats = mempool_sample_stats(&conn, 1_800_000_000).unwrap();
    let rows = query_mempool_samples_since(&conn, 1_800_000_000).unwrap();
    assert_eq!(stats.count, rows.len() as i64);
    assert_eq!(stats.latest_sampled_at, rows.last().map(|r| r.sampled_at));
    assert_eq!(stats.count, 2);
    assert_eq!(stats.latest_sampled_at, Some(1_800_003_600));
}

/// Companion: an empty window reports `count: 0` and `latest_sampled_at:
/// None`, matching an empty `Vec` from `query_mempool_samples_since`.
#[test]
fn mempool_sample_stats_reports_zero_for_an_empty_window() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    let stats = mempool_sample_stats(&conn, 0).unwrap();
    assert_eq!(stats.count, 0);
    assert_eq!(stats.latest_sampled_at, None);
}

#[test]
fn rust_fee_schema_mempool_sample_pruned_is_transactional() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    record_mempool_sample(&conn, 1_799_990_000, 9.0).unwrap(); // stale, will be pruned
    record_mempool_sample(&conn, 1_800_000_000, 12.5).unwrap(); // fresh, retained

    // Insert one new sample AND prune everything before the 24h window,
    // atomically -- Task 6 step 1's "old rows are pruned transactionally".
    record_mempool_sample_pruned(&conn, 1_800_003_600, 15.0, 1_800_000_000).unwrap();

    let samples = query_mempool_samples_since(&conn, 0).unwrap();
    assert_eq!(
        samples.len(),
        2,
        "the stale pre-window row must be gone, the retained + new rows must remain"
    );
    assert_eq!(samples[0].sampled_at, 1_800_000_000);
    assert_eq!(samples[1].sampled_at, 1_800_003_600);
    assert_eq!(samples[1].sat_per_vbyte, 15.0);
}

/// Review finding 1 (fix round 1): the shadow-window mempool 24h-MA
/// comparison (R8 binding constraint) must be a persisted row, not only a
/// log line -- `revops`'s daily rollup consumes DB evidence, never logs.
#[test]
fn rust_fee_schema_mempool_ma_comparison_record_and_query() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();

    let id = record_mempool_ma_comparison(
        &conn,
        &MempoolMaComparisonRow {
            at: 1_800_000_000,
            cycle_ts: 1_800_000_000,
            rust_ma: 12.5,
            python_ma: Some(11.75),
            delta: Some(0.75),
        },
    )
    .unwrap();
    assert!(id > 0);

    let rows = query_mempool_ma_comparisons_since(&conn, 0).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cycle_ts, 1_800_000_000);
    assert_eq!(rows[0].rust_ma, 12.5);
    assert_eq!(rows[0].python_ma, Some(11.75));
    assert_eq!(rows[0].delta, Some(0.75));
}

/// Python's MA can be genuinely unavailable this cycle (a fresh-state
/// `DecisionInputError`) -- the row must still be recorded, with
/// `python_ma`/`delta` NULL, so absence is itself evidence rather than a
/// skipped row.
#[test]
fn rust_fee_schema_mempool_ma_comparison_records_null_python_ma_as_evidence() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();

    record_mempool_ma_comparison(
        &conn,
        &MempoolMaComparisonRow {
            at: 1_800_000_100,
            cycle_ts: 1_800_000_100,
            rust_ma: 9.0,
            python_ma: None,
            delta: None,
        },
    )
    .unwrap();

    let rows = query_mempool_ma_comparisons_since(&conn, 0).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].python_ma, None);
    assert_eq!(rows[0].delta, None);
    assert_eq!(rows[0].rust_ma, 9.0);
}

#[test]
fn rust_fee_schema_trigger_event_and_mutation_count() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    assert_eq!(mutation_count(&conn).unwrap(), 0);

    record_trigger_event(
        &conn,
        &FeeTriggerEventRow {
            trigger_type: "forward_event".to_string(),
            channel_id: Some("1x1x0".to_string()),
            cycle_id: None,
            cycle_ts: None,
            received_at: 1_800_000_000,
            coalesced: false,
            detail: Some("first forward".to_string()),
        },
    )
    .unwrap();

    commit_fee_cycle(&conn, &sample_commit("cycle-1", 1_800_000_000)).unwrap();
    assert_eq!(mutation_count(&conn).unwrap(), 1);

    let trigger_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rust_fee_trigger_events", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(trigger_count, 1);
}

#[test]
fn rust_fee_schema_quarantine_and_runway_snapshot() {
    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();
    assert!(active_quarantine(&conn).unwrap().is_none());

    let id = insert_quarantine(
        &conn,
        &QuarantineEntry {
            reason: "ambiguous post-submission transport outcome".to_string(),
            cycle_id: None,
            channel_id: Some("1x1x0".to_string()),
            request_id: Some("req-1".to_string()),
            entered_at: 1_800_000_000,
        },
    )
    .unwrap();
    assert!(id > 0);

    let active = active_quarantine(&conn).unwrap().expect("quarantine set");
    assert_eq!(active.reason, "ambiguous post-submission transport outcome");
    assert_eq!(active.channel_id.as_deref(), Some("1x1x0"));

    assert!(latest_runway_snapshot(&conn).unwrap().is_none());
    record_runway_snapshot(
        &conn,
        &RunwaySnapshotRow {
            snapshot_at: 1_800_000_000,
            report_schema_version: "1".to_string(),
            source_commit: "f7ccc24".to_string(),
            binary_sha256: "deadbeef".repeat(8),
            summary_json: r#"{"cycles": 0}"#.to_string(),
        },
    )
    .unwrap();
    let snap = latest_runway_snapshot(&conn)
        .unwrap()
        .expect("snapshot set");
    assert_eq!(snap.report_schema_version, "1");
}

// ---------------------------------------------------------------------------
// Task 5 (stateful-shadow plan): SeedOnce seed-provenance events + restart
// markers (schema-direct; the actor plumbing is covered in tests/owner.rs).
// ---------------------------------------------------------------------------

#[test]
fn rust_fee_schema_seed_event_records_provenance_and_refusal() {
    use revops_db::fee_runway::{
        commit_fee_cycle, latest_seed_event, record_seed_refusal, FeeCycleCommit, FeeSeedEventRow,
    };

    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();

    assert!(latest_seed_event(&conn).unwrap().is_none());

    // Task 42: SUCCESS provenance has no standalone write path -- it
    // commits atomically with generation 1.
    let seeded = FeeSeedEventRow {
        seeded_at: 1_800_000_000,
        outcome: "seeded".to_string(),
        source_db_path: "/prod/revenue_ops.db".to_string(),
        source_max_last_update: 1_799_999_000,
        row_count: 47,
        payload_sha256: "ab".repeat(32),
        source_commit: "649c320".to_string(),
        refused_channel: None,
        refused_field: None,
        detail: None,
    };
    assert!(
        record_seed_refusal(&conn, &seeded).is_err(),
        "the standalone path must refuse a success row outright"
    );
    assert!(latest_seed_event(&conn).unwrap().is_none());
    let generation = commit_fee_cycle(
        &conn,
        &FeeCycleCommit {
            cycle_id: "seed-cycle-1".to_string(),
            started_at: 1_800_000_000,
            completed_at: 1_800_000_000,
            source_commit: "649c320".to_string(),
            binary_sha256: "0".repeat(64),
            pending_seed: Some(seeded.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(generation, 1);
    let read = latest_seed_event(&conn).unwrap().expect("seed event");
    assert_eq!(read, seeded);

    // A later refusal supersedes as the LATEST event and carries the
    // offending channel + field (fail-closed seed import, Task R6) --
    // refusal remains a STANDALONE durable event (Task 42).
    let refused = FeeSeedEventRow {
        seeded_at: 1_800_000_100,
        outcome: "seed_refused".to_string(),
        source_db_path: "/prod/revenue_ops.db".to_string(),
        source_max_last_update: 1_799_999_500,
        row_count: 47,
        payload_sha256: "cd".repeat(32),
        source_commit: "649c320".to_string(),
        refused_channel: Some("700x1x0".to_string()),
        refused_field: Some("thompson_state._last_fee_min".to_string()),
        detail: Some("non-numeric value where Python float() raises".to_string()),
    };
    record_seed_refusal(&conn, &refused).unwrap();
    let read = latest_seed_event(&conn).unwrap().expect("refusal event");
    assert_eq!(read, refused);

    // Outcome vocabulary is closed: anything else is rejected before the
    // insert (and the pending-seed path equally refuses non-'seeded').
    let bogus = FeeSeedEventRow {
        outcome: "partial".to_string(),
        ..seeded
    };
    assert!(
        record_seed_refusal(&conn, &bogus).is_err(),
        "outcome must be 'seeded' or 'seed_refused' -- no partial seeds exist"
    );
}

#[test]
fn refresh_mempool_window_inserts_prunes_and_aggregates_in_one_transaction() {
    use revops_db::fee_runway::{record_mempool_sample, refresh_mempool_window, MempoolWindow};

    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();

    // Pre-existing window: one fresh (kept) + one stale (pruned) sample.
    record_mempool_sample(&conn, 1_800_000_000 - 1_000, 10.0).unwrap();
    record_mempool_sample(&conn, 1_800_000_000 - 90_000, 500.0).unwrap();

    let window =
        refresh_mempool_window(&conn, 1_800_000_000, 30.0, 1_800_000_000 - 86_400).unwrap();
    assert_eq!(
        window,
        MempoolWindow {
            count: 2,
            latest_sampled_at: Some(1_800_000_000),
            average: Some(20.0),
        },
        "the aggregate covers the post-prune window INCLUDING the just-inserted \
         current sample (virgin first-cycle evidence), excluding the pruned row"
    );
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rust_mempool_fee_history", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 2, "stale row pruned, fresh + current kept");

    // A virgin table: the FIRST refresh already returns its own sample.
    let virgin = Connection::open_in_memory().unwrap();
    init_schema(&virgin).unwrap();
    let window =
        refresh_mempool_window(&virgin, 1_800_000_000, 3.0, 1_800_000_000 - 86_400).unwrap();
    assert_eq!(
        window,
        MempoolWindow {
            count: 1,
            latest_sampled_at: Some(1_800_000_000),
            average: Some(3.0),
        }
    );
}

/// Task 42: the DB-level virgin-store gate under the pending-seed
/// contract. A commit carrying success provenance against an ALREADY
/// ADVANCED store must roll back ENTIRELY — generation, cycle, and seed
/// table all unchanged.
#[test]
fn pending_seed_against_advanced_store_rolls_back_the_whole_commit() {
    use revops_db::fee_runway::{
        commit_fee_cycle, current_state_generation, latest_seed_event, FeeCycleCommit,
        FeeSeedEventRow,
    };

    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();

    let seed = FeeSeedEventRow {
        seeded_at: 1_800_000_000,
        outcome: "seeded".to_string(),
        source_db_path: "/prod/revenue_ops.db".to_string(),
        source_max_last_update: 1_799_999_000,
        row_count: 1,
        payload_sha256: "ab".repeat(32),
        source_commit: "649c320".to_string(),
        refused_channel: None,
        refused_field: None,
        detail: None,
    };

    // Advance the store to generation 1 WITHOUT a seed (e.g. an A3-first
    // virgin store — a consistent no-seed-claim state).
    commit_fee_cycle(
        &conn,
        &FeeCycleCommit {
            cycle_id: "cycle-1".to_string(),
            started_at: 1_800_000_000,
            completed_at: 1_800_000_000,
            source_commit: "649c320".to_string(),
            binary_sha256: "0".repeat(64),
            ..Default::default()
        },
    )
    .unwrap();

    let err = commit_fee_cycle(
        &conn,
        &FeeCycleCommit {
            cycle_id: "cycle-2".to_string(),
            started_at: 1_800_000_100,
            completed_at: 1_800_000_100,
            source_commit: "649c320".to_string(),
            binary_sha256: "0".repeat(64),
            pending_seed: Some(seed.clone()),
            ..Default::default()
        },
    )
    .expect_err("pending seed provenance requires a virgin store");
    assert!(
        err.to_string().contains("virgin store"),
        "typed reason names the gate: {err:#}"
    );

    assert_eq!(current_state_generation(&conn).unwrap(), 1);
    assert!(latest_seed_event(&conn).unwrap().is_none());
    let cycle2: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM rust_fee_cycles WHERE cycle_id = 'cycle-2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        cycle2, 0,
        "the ENTIRE commit rolled back, not only the seed row"
    );

    // And a pending row claiming refusal is refused as a commit rider.
    let refused_rider = FeeSeedEventRow {
        outcome: "seed_refused".to_string(),
        ..seed
    };
    let fresh = Connection::open_in_memory().unwrap();
    init_schema(&fresh).unwrap();
    assert!(
        commit_fee_cycle(
            &fresh,
            &FeeCycleCommit {
                cycle_id: "cycle-r".to_string(),
                started_at: 1_800_000_000,
                completed_at: 1_800_000_000,
                source_commit: "649c320".to_string(),
                binary_sha256: "0".repeat(64),
                pending_seed: Some(refused_rider),
                ..Default::default()
            },
        )
        .is_err(),
        "refusals never ride a commit — they are standalone terminal facts"
    );
}

#[test]
fn rust_fee_schema_restart_marker_round_trip() {
    use revops_db::fee_runway::{
        latest_restart_marker, record_restart_marker, FeeRestartMarkerRow,
    };

    let conn = Connection::open_in_memory().unwrap();
    init_schema(&conn).unwrap();

    assert!(latest_restart_marker(&conn).unwrap().is_none());

    let first = FeeRestartMarkerRow {
        started_at: 1_800_000_000,
        process_id: 4242,
        prior_generation: 0,
        hydration_source: "python_seed".to_string(),
        source_commit: "649c320".to_string(),
    };
    record_restart_marker(&conn, &first).unwrap();

    let second = FeeRestartMarkerRow {
        started_at: 1_800_100_000,
        process_id: 4300,
        prior_generation: 12,
        hydration_source: "rust_generation:12".to_string(),
        source_commit: "649c320".to_string(),
    };
    record_restart_marker(&conn, &second).unwrap();

    let read = latest_restart_marker(&conn).unwrap().expect("marker");
    assert_eq!(read, second, "latest marker wins (newest restart)");
}
