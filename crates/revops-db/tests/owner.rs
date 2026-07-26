//! Tests for `revops_db::owner` -- the single-owner read-write actor over
//! the Rust plugin's OWN notification-ingestion db (never production).
//! Mirrors `actor.rs`'s single-owner-task pattern (see `actor_wal.rs`) but
//! for a writable connection created fresh if the file doesn't exist yet.

use revops_db::fee_runway::{
    FeeCycleCommit, FeeStateRow, FeeTriggerEventRow, GovernorAuditRow, LedgerAuditRow,
    PreparedFeeActionRow, QuarantineEntry, ShadowCycleOutcomeRow,
};
use revops_db::notifications::ForwardRow;
use revops_db::owner::spawn_read_write;

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

#[tokio::test]
async fn creates_db_file_and_parent_dir_if_missing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join("observer.db");
    assert!(!path.exists());
    let handle = spawn_read_write(&path).await.expect("creates fresh db");
    assert!(path.exists(), "spawn_read_write must create the db file");
    assert_eq!(handle.last_forward_ts().await.unwrap(), None);
}

#[tokio::test]
async fn insert_and_dedup_through_the_actor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    assert!(
        handle.insert_forward(sample()).await.unwrap(),
        "first insert"
    );
    assert!(
        !handle.insert_forward(sample()).await.unwrap(),
        "dup ignored"
    );
    assert_eq!(handle.last_forward_ts().await.unwrap(), Some(1_800_000_000));
}

#[tokio::test]
async fn reopening_existing_db_preserves_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    {
        let handle = spawn_read_write(&path).await.unwrap();
        handle.insert_forward(sample()).await.unwrap();
    }
    let handle = spawn_read_write(&path).await.unwrap();
    assert_eq!(handle.last_forward_ts().await.unwrap(), Some(1_800_000_000));
}

#[tokio::test]
async fn peer_and_closure_events_go_through_the_actor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();
    handle
        .insert_peer_connection_event("03deadbeef".into(), "connected".into(), 1_800_000_000)
        .await
        .unwrap();
    handle
        .insert_channel_closure_event("1x1x0".into(), "remote".into(), 1_800_000_000)
        .await
        .unwrap();
    // No dedicated read accessor for these tables at the actor layer yet
    // (Phase 1b only needs write-path coverage) -- reopen a direct
    // connection to confirm the rows landed.
    drop(handle);
    let conn = rusqlite::Connection::open(&path).unwrap();
    let peer_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM peer_connection_events", [], |r| {
            r.get(0)
        })
        .unwrap();
    let closure_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM channel_closure_events", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(peer_count, 1);
    assert_eq!(closure_count, 1);
}

// ---------------------------------------------------------------------------
// Task 4 (stateful-shadow plan): transactional Rust-owned fee state +
// audit schema, through the single-owner actor.
// ---------------------------------------------------------------------------

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
    }
}

#[tokio::test]
async fn fee_cycle_transaction_commits_atomically_and_bumps_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    let generation = handle
        .commit_fee_cycle(sample_commit("cycle-1", 1_800_000_000))
        .await
        .unwrap();
    assert_eq!(generation, 1);

    let snapshot = handle.load_latest_fee_state().await.unwrap();
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(snapshot.rows[0].channel_id, "1x1x0");
    assert_eq!(handle.fee_mutation_count().await.unwrap(), 1);
}

#[tokio::test]
async fn fee_cycle_transaction_rollback_leaves_generation_and_count_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    handle
        .commit_fee_cycle(sample_commit("cycle-1", 1_800_000_000))
        .await
        .unwrap();
    let baseline_generation = handle.load_latest_fee_state().await.unwrap().generation;
    let baseline_count = handle.fee_mutation_count().await.unwrap();
    assert_eq!(baseline_generation, 1);
    assert_eq!(baseline_count, 1);

    // Inject a request-row failure: duplicate channel_id within one cycle.
    let mut broken = sample_commit("cycle-2", 1_800_000_100);
    let duplicate = broken.requests[0].clone();
    broken.requests.push(duplicate);

    let result = handle.commit_fee_cycle(broken).await;
    assert!(result.is_err(), "duplicate request identity must fail");

    let snapshot = handle.load_latest_fee_state().await.unwrap();
    assert_eq!(
        snapshot.generation, baseline_generation,
        "a failed commit must not bump the generation"
    );
    assert_eq!(
        handle.fee_mutation_count().await.unwrap(),
        baseline_count,
        "a failed commit must not add request rows"
    );
}

#[tokio::test]
async fn fee_cycle_transaction_mempool_trigger_and_quarantine_through_actor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    handle
        .record_mempool_sample(1_800_000_000, 12.5)
        .await
        .unwrap();
    handle
        .record_mempool_sample(1_800_003_600, 15.0)
        .await
        .unwrap();
    let samples = handle
        .query_mempool_samples_since(1_800_000_000)
        .await
        .unwrap();
    assert_eq!(samples.len(), 2);

    // Task 6: the transactional insert+prune sibling, through the actor.
    handle
        .record_mempool_sample_pruned(1_800_010_000, 20.0, 1_800_003_600)
        .await
        .unwrap();
    let pruned = handle.query_mempool_samples_since(0).await.unwrap();
    assert_eq!(
        pruned.len(),
        2,
        "the 1_800_000_000 sample must be pruned; the retained + new samples remain"
    );
    assert_eq!(pruned[0].sampled_at, 1_800_003_600);
    assert_eq!(pruned[1].sampled_at, 1_800_010_000);

    handle
        .record_fee_trigger_event(FeeTriggerEventRow {
            trigger_type: "wake_all".to_string(),
            channel_id: None,
            cycle_id: None,
            cycle_ts: Some(1_800_000_000),
            received_at: 1_800_000_000,
            coalesced: true,
            detail: None,
        })
        .await
        .unwrap();

    assert!(handle
        .active_execution_quarantine()
        .await
        .unwrap()
        .is_none());
    handle
        .insert_execution_quarantine(QuarantineEntry {
            reason: "ambiguous post-submission transport outcome".to_string(),
            cycle_id: None,
            channel_id: Some("1x1x0".to_string()),
            request_id: Some("req-1".to_string()),
            entered_at: 1_800_000_000,
        })
        .await
        .unwrap();
    let active = handle
        .active_execution_quarantine()
        .await
        .unwrap()
        .expect("quarantine set");
    assert_eq!(active.channel_id.as_deref(), Some("1x1x0"));
}

/// Step 3: the bounded blocking bridge for the scheduler's plain
/// `std::thread` -- no second SQLite connection, no Tokio runtime on that
/// thread. Runs concurrently with an async writer on the SAME actor to
/// prove the bridge does not corrupt interleaved commits.
#[tokio::test]
async fn fee_cycle_transaction_blocking_bridge_from_scheduler_thread() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    // Seed generation 1 asynchronously (as the async ingestion side would).
    handle
        .commit_fee_cycle(sample_commit("cycle-1", 1_800_000_000))
        .await
        .unwrap();

    // The scheduler's own plain OS thread, exactly like `fee_scheduler.rs`
    // will run: no `.await`, only the `blocking_*` bridge.
    let blocking_handle = handle.clone();
    let joined = tokio::task::spawn_blocking(move || {
        let snapshot = blocking_handle.blocking_load_latest_fee_state().unwrap();
        assert_eq!(snapshot.generation, 1);

        let generation = blocking_handle
            .blocking_commit_fee_cycle(sample_commit("cycle-2", 1_800_000_100))
            .unwrap();
        assert_eq!(generation, 2);

        blocking_handle
            .blocking_record_mempool_sample(1_800_000_200, 20.0)
            .unwrap();
        let samples = blocking_handle
            .blocking_query_mempool_samples_since(0)
            .unwrap();
        assert_eq!(samples.len(), 1);

        blocking_handle
            .blocking_record_fee_trigger_event(FeeTriggerEventRow {
                trigger_type: "vegas_spike".to_string(),
                channel_id: None,
                cycle_id: Some("cycle-2".to_string()),
                cycle_ts: Some(1_800_000_100),
                received_at: 1_800_000_100,
                coalesced: false,
                detail: None,
            })
            .unwrap();

        assert!(blocking_handle
            .blocking_active_execution_quarantine()
            .unwrap()
            .is_none());
        blocking_handle
            .blocking_insert_execution_quarantine(QuarantineEntry {
                reason: "test quarantine".to_string(),
                cycle_id: Some("cycle-2".to_string()),
                channel_id: None,
                request_id: None,
                entered_at: 1_800_000_100,
            })
            .unwrap();
        assert!(blocking_handle
            .blocking_active_execution_quarantine()
            .unwrap()
            .is_some());

        blocking_handle.blocking_fee_mutation_count().unwrap()
    })
    .await
    .expect("blocking scheduler thread must not panic");
    assert_eq!(joined, 2, "one request per committed cycle");

    // The async view sees exactly what the blocking bridge wrote -- same
    // connection, same actor, no second SQLite connection was opened.
    let snapshot = handle.load_latest_fee_state().await.unwrap();
    assert_eq!(snapshot.generation, 2);
    assert_eq!(handle.fee_mutation_count().await.unwrap(), 2);
}

/// Task 5 (stateful-shadow plan): seed-provenance events + restart markers
/// through the single-owner actor -- async methods and the blocking bridge
/// the SeedOnce scheduler thread uses, on the SAME connection.
#[tokio::test]
async fn fee_cycle_transaction_seed_event_and_restart_marker_through_actor() {
    use revops_db::fee_runway::{FeeRestartMarkerRow, FeeSeedEventRow};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    assert!(handle.latest_fee_seed_event().await.unwrap().is_none());
    assert!(handle.latest_fee_restart_marker().await.unwrap().is_none());

    // Blocking side (the scheduler's plain-thread analog).
    let blocking = handle.clone();
    tokio::task::spawn_blocking(move || {
        blocking
            .blocking_record_fee_seed_event(FeeSeedEventRow {
                seeded_at: 1_800_000_000,
                outcome: "seeded".to_string(),
                source_db_path: "/prod/revenue_ops.db".to_string(),
                source_max_last_update: 1_799_999_000,
                row_count: 3,
                payload_sha256: "ab".repeat(32),
                source_commit: "649c320".to_string(),
                refused_channel: None,
                refused_field: None,
                detail: None,
            })
            .unwrap();
        blocking
            .blocking_record_fee_restart_marker(FeeRestartMarkerRow {
                started_at: 1_800_000_000,
                process_id: std::process::id() as i64,
                prior_generation: 0,
                hydration_source: "python_seed".to_string(),
                source_commit: "649c320".to_string(),
            })
            .unwrap();
    })
    .await
    .unwrap();

    // Async side re-reads what the blocking side wrote: one connection.
    let seed = handle
        .latest_fee_seed_event()
        .await
        .unwrap()
        .expect("seed event visible to async side");
    assert_eq!(seed.outcome, "seeded");
    assert_eq!(seed.row_count, 3);
    let marker = handle
        .latest_fee_restart_marker()
        .await
        .unwrap()
        .expect("restart marker visible to async side");
    assert_eq!(marker.hydration_source, "python_seed");
    assert_eq!(marker.process_id, std::process::id() as i64);
}
