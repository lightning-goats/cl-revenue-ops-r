//! Tests for `revops_db::owner` -- the single-owner read-write actor over
//! the Rust plugin's OWN notification-ingestion db (never production).
//! Mirrors `actor.rs`'s single-owner-task pattern (see `actor_wal.rs`) but
//! for a writable connection created fresh if the file doesn't exist yet.

use revops_db::fee_runway::{
    BroadcastAttemptIntent, BroadcastAttemptOutcome, FeeCycleCommit, FeeStateRow,
    FeeTriggerEventRow, GovernorAuditRow, LedgerAuditRow, MempoolMaComparisonRow,
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
        pending_seed: None,
        trigger_receipt: None,
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

/// Fix round 1 (I-5): the scalar-only `ObserverHandle` siblings agree with
/// their full-row counterparts through the actor, both async and blocking.
#[tokio::test]
async fn scalar_generation_and_mempool_stats_agree_with_full_row_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    assert_eq!(handle.current_state_generation().await.unwrap(), 0);
    handle
        .commit_fee_cycle(sample_commit("cycle-1", 1_800_000_000))
        .await
        .unwrap();
    let generation = handle.current_state_generation().await.unwrap();
    assert_eq!(
        generation,
        handle.load_latest_fee_state().await.unwrap().generation
    );
    assert_eq!(generation, 1);

    handle
        .record_mempool_sample(1_800_000_000, 12.5)
        .await
        .unwrap();
    handle
        .record_mempool_sample(1_800_003_600, 15.0)
        .await
        .unwrap();
    let stats = handle.mempool_sample_stats(1_800_000_000).await.unwrap();
    let rows = handle
        .query_mempool_samples_since(1_800_000_000)
        .await
        .unwrap();
    assert_eq!(stats.count, rows.len() as i64);
    assert_eq!(stats.latest_sampled_at, rows.last().map(|r| r.sampled_at));

    // The blocking siblings must be driven from a plain OS thread (never
    // from inside the tokio runtime itself) -- the exact same bridge
    // `fee_cycle_transaction_blocking_bridge_from_scheduler_thread` above
    // exercises.
    let blocking_handle = handle.clone();
    tokio::task::spawn_blocking(move || {
        assert_eq!(
            blocking_handle.blocking_current_state_generation().unwrap(),
            generation
        );
        let blocking_stats = blocking_handle
            .blocking_mempool_sample_stats(1_800_000_000)
            .unwrap();
        assert_eq!(blocking_stats, stats);
    })
    .await
    .unwrap();
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

/// Fix round 1 (review finding 1): the mempool 24h-MA comparison round
/// trips through the actor, both async and blocking (the scheduler's
/// plain OS thread never `.await`s).
#[tokio::test]
async fn mempool_ma_comparison_round_trips_through_actor_async_and_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    let id = handle
        .record_mempool_ma_comparison(MempoolMaComparisonRow {
            at: 1_800_000_000,
            cycle_ts: 1_800_000_000,
            rust_ma: 10.0,
            python_ma: Some(9.5),
            delta: Some(0.5),
        })
        .await
        .unwrap();
    assert!(id > 0);

    let blocking_handle = handle.clone();
    let blocking_id = tokio::task::spawn_blocking(move || {
        blocking_handle
            .blocking_record_mempool_ma_comparison(MempoolMaComparisonRow {
                at: 1_800_000_100,
                cycle_ts: 1_800_000_100,
                rust_ma: 11.0,
                python_ma: None,
                delta: None,
            })
            .unwrap()
    })
    .await
    .unwrap();
    assert!(blocking_id > id);
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
            .blocking_record_seed_refusal(FeeSeedEventRow {
                seeded_at: 1_800_000_000,
                outcome: "seed_refused".to_string(),
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
    assert_eq!(seed.outcome, "seed_refused");
    assert_eq!(seed.row_count, 3);
    let marker = handle
        .latest_fee_restart_marker()
        .await
        .unwrap()
        .expect("restart marker visible to async side");
    assert_eq!(marker.hydration_source, "python_seed");
    assert_eq!(marker.process_id, std::process::id() as i64);
}

// ---------------------------------------------------------------------------
// Task 9 (stateful-shadow revision plan): the guarded broadcaster's
// intent/result ledger + restart quarantine reconciliation, through the
// single-owner actor.
// ---------------------------------------------------------------------------

fn sample_intent(request_id: &str, at: i64) -> BroadcastAttemptIntent {
    BroadcastAttemptIntent {
        cycle_id: Some("live-cycle-1".to_string()),
        channel_id: "1x1x0".to_string(),
        request_id: request_id.to_string(),
        method: "setchannel".to_string(),
        params_json: r#"{"id":"1x1x0","feebase":0,"feeppm":150}"#.to_string(),
        submitted_at: at,
    }
}

/// Intent-then-result round trip: the intent is written with no outcome
/// (readable as unresolved), and recording a result clears that.
#[tokio::test]
async fn broadcast_attempt_intent_then_result_round_trips_through_actor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    let id = handle
        .insert_broadcast_attempt(sample_intent("req-1", 1_800_000_000))
        .await
        .unwrap();
    assert!(id > 0);

    // Before any result is recorded, restart reconciliation must treat
    // this exact row as unresolved -- quarantine every subsequent batch.
    assert!(handle
        .active_execution_quarantine()
        .await
        .unwrap()
        .is_none());
    let reconciled = handle
        .reconcile_quarantine_on_restart(1_800_000_100)
        .await
        .unwrap();
    assert_eq!(reconciled, 1, "the unresolved intent must be reconciled");
    let active = handle
        .active_execution_quarantine()
        .await
        .unwrap()
        .expect("reconciliation must quarantine an unresolved intent");
    assert_eq!(active.request_id.as_deref(), Some("req-1"));

    // A SECOND reconciliation pass over the SAME (now-resolved) row must
    // be a no-op -- it was marked ambiguous by the first pass and must
    // never be re-scanned as unresolved again.
    let reconciled_again = handle
        .reconcile_quarantine_on_restart(1_800_000_200)
        .await
        .unwrap();
    assert_eq!(
        reconciled_again, 0,
        "a row already resolved (even to Ambiguous) must not be reconciled twice"
    );

    // A fresh attempt that DOES record a definite result before any
    // restart happens must never be reconciled at all.
    let id2 = handle
        .insert_broadcast_attempt(sample_intent("req-2", 1_800_000_300))
        .await
        .unwrap();
    handle
        .record_broadcast_attempt_result(id2, BroadcastAttemptOutcome::Success, None, 1_800_000_301)
        .await
        .unwrap();
    let reconciled_third = handle
        .reconcile_quarantine_on_restart(1_800_000_400)
        .await
        .unwrap();
    assert_eq!(
        reconciled_third, 0,
        "a resolved (Success) attempt must never be treated as unresolved"
    );
}

/// No unresolved attempts and no active quarantine -> reconciliation is a
/// true no-op (does not fabricate a quarantine entry out of nothing).
#[tokio::test]
async fn reconcile_quarantine_on_restart_is_noop_with_nothing_unresolved() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    let reconciled = handle
        .reconcile_quarantine_on_restart(1_800_000_000)
        .await
        .unwrap();
    assert_eq!(reconciled, 0);
    assert!(handle
        .active_execution_quarantine()
        .await
        .unwrap()
        .is_none());
}

/// An explicit rejection (or any other definite result) is never
/// reconciled into a quarantine -- only a MISSING result is ambiguous.
#[tokio::test]
async fn reconcile_quarantine_on_restart_ignores_definite_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    let id = handle
        .insert_broadcast_attempt(sample_intent("req-rejected", 1_800_000_000))
        .await
        .unwrap();
    handle
        .record_broadcast_attempt_result(
            id,
            BroadcastAttemptOutcome::Rejected,
            Some("explicit CLN rejection".to_string()),
            1_800_000_001,
        )
        .await
        .unwrap();

    let reconciled = handle
        .reconcile_quarantine_on_restart(1_800_000_100)
        .await
        .unwrap();
    assert_eq!(reconciled, 0);
    assert!(handle
        .active_execution_quarantine()
        .await
        .unwrap()
        .is_none());
}

/// If a quarantine is ALREADY active (e.g. the live broadcaster inserted
/// one directly after observing an ambiguous transport outcome, before
/// any restart happened), reconciliation must never insert a SECOND
/// entry -- restoration only, never a duplicate.
#[tokio::test]
async fn reconcile_quarantine_on_restart_does_not_duplicate_an_active_quarantine() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    handle
        .insert_execution_quarantine(QuarantineEntry {
            reason: "ambiguous post-submission transport outcome".to_string(),
            cycle_id: None,
            channel_id: Some("1x1x0".to_string()),
            request_id: Some("req-live".to_string()),
            entered_at: 1_800_000_000,
        })
        .await
        .unwrap();
    let before = handle
        .active_execution_quarantine()
        .await
        .unwrap()
        .expect("quarantine set directly");

    // An unrelated unresolved intent from the SAME crash.
    handle
        .insert_broadcast_attempt(sample_intent("req-live", 1_800_000_000))
        .await
        .unwrap();

    let reconciled = handle
        .reconcile_quarantine_on_restart(1_800_000_100)
        .await
        .unwrap();
    assert_eq!(
        reconciled, 1,
        "the unresolved intent is still reconciled..."
    );
    let after = handle
        .active_execution_quarantine()
        .await
        .unwrap()
        .expect("quarantine still active");
    assert_eq!(
        after.id, before.id,
        "...but must not insert a SECOND quarantine row on top of the active one"
    );
}

// ---------------------------------------------------------------------------
// Final-review finding I3 (2026-07-26): the observer db was the only open in
// the repo with NO busy_timeout (SQLite's default is 0 -> immediate
// SQLITE_BUSY), and its `PRAGMA journal_mode=WAL` went through
// `execute_batch`, which DISCARDS the returned mode -- so a silent fallback
// to rollback-journal was undetectable. In rollback mode the engagement
// gate's `mode=ro` reader blocks the writer, and with no busy_timeout every
// `commit_fee_cycle` fails instantly and forever, visible only in logs.
// ---------------------------------------------------------------------------

#[test]
fn observer_db_open_sets_the_repo_busy_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested").join("observer.db");
    let conn = revops_db::owner::open_observer_db(&path).unwrap();
    let timeout_ms: i64 = conn
        .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        timeout_ms,
        revops_db::BUSY_TIMEOUT_MS as i64,
        "the observer db must use the same busy_timeout as every other open \
         in this repo (open_read_only, the econ ledger)"
    );
}

#[test]
fn observer_db_open_verifies_wal_actually_took_effect() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let conn = revops_db::owner::open_observer_db(&path).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        mode.to_lowercase(),
        "wal",
        "a silent fallback to rollback-journal must never survive the open"
    );
}

/// Task-40 verifier finding (2026-07-26), remediated under task 30: the
/// `require_wal_mode(&mode, path)?` line in `open_observer_db` was DELETABLE
/// with the entire 1442-test suite still green. Neither test above can catch
/// that — `observer_db_open_verifies_wal_actually_took_effect` opens a normal
/// tempdir file, which is WAL whether or not the answer is checked, and
/// `observer_db_open_fails_loudly_when_wal_was_not_applied` calls the pure
/// verifier directly and never reaches the call site. A guard no test defends
/// is one a later commit removes silently, which is exactly how this branch
/// acquired its other five false-clean defects.
///
/// This drives the CALL SITE. Note that the remediation as originally
/// specified — a database file pre-set to `journal_mode=DELETE` — does NOT
/// work: `PRAGMA journal_mode=WAL` simply converts such a file (verified
/// against sqlite 3.45.1). The targets that genuinely answer something other
/// than `wal` WITHOUT erroring are the two the `require_wal_mode` doc comment
/// already names, and both are reachable by mis-setting `observer-db-path`:
///
/// * `:memory:` answers `memory`. This is the discriminating case for THIS
///   guard: `notifications::init_schema` deliberately accepts `memory` (its
///   test doubles are in-memory), so nothing downstream would catch it.
/// * `""` — an anonymous temporary database — answers `delete`. Caught here
///   first, and by `init_schema` as a second layer.
#[test]
fn open_observer_db_refuses_a_target_that_cannot_enter_wal() {
    for (target, expected_mode) in [(":memory:", "memory"), ("", "delete")] {
        let err = revops_db::owner::open_observer_db(std::path::Path::new(target)).expect_err(
            "a target that cannot enter WAL must fail the open, never degrade to a rollback \
             journal in silence",
        );
        let msg = format!("{err:#}");
        assert!(
            msg.contains(expected_mode),
            "the failure must name the mode actually reached ({expected_mode}), got: {msg}"
        );
        assert!(
            msg.to_lowercase().contains("wal"),
            "the failure must name WAL as the requirement, got: {msg}"
        );
    }
}

#[test]
fn observer_db_open_fails_loudly_when_wal_was_not_applied() {
    // The pure verifier behind the open: any mode other than WAL is a hard
    // error naming the file, never a shrug.
    let path = std::path::Path::new("/tmp/observer.db");
    assert!(revops_db::owner::require_wal_mode("wal", path).is_ok());
    assert!(revops_db::owner::require_wal_mode("WAL", path).is_ok());
    for fallback in ["delete", "truncate", "persist", "memory", "off"] {
        let err = revops_db::owner::require_wal_mode(fallback, path)
            .expect_err("a non-WAL journal mode must fail the open");
        let msg = format!("{err:#}");
        assert!(msg.contains(fallback), "{msg}");
        assert!(msg.contains("observer.db"), "{msg}");
    }
}

// -- Task 59 §3.1: two-phase admission/receipt for live-path writes --

fn intent(request_id: &str) -> BroadcastAttemptIntent {
    BroadcastAttemptIntent {
        cycle_id: None,
        channel_id: "1x1x0".into(),
        request_id: request_id.into(),
        method: "setchannel".into(),
        params_json: "{}".into(),
        submitted_at: 1_800_000_000,
    }
}

/// §3.1.2 "not admitted": a full owner queue refuses admission
/// IMMEDIATELY and typed -- provably nothing was enqueued, so the caller
/// may report a clean non-write (`store_admission_refused`).
#[tokio::test]
async fn full_owner_queue_refuses_intent_admission_typed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    // Wedge the actor: a second connection holds a write transaction, so
    // the FIRST queued command blocks on the busy timeout while the rest
    // pile up in the bounded queue.
    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker
        .busy_timeout(std::time::Duration::from_secs(60))
        .unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    // Fill the bounded queue (capacity 64) with fire-and-forget writes.
    let mut pending = Vec::new();
    for i in 0..70 {
        let handle = handle.clone();
        let mut row = sample();
        row.timestamp += i;
        pending.push(tokio::spawn(async move {
            let _ = handle.insert_forward(row).await;
        }));
        tokio::task::yield_now().await;
    }

    // Wait until admission is actually refused (the queue is provably
    // full) rather than sleeping a fixed time.
    let mut refused = None;
    for _ in 0..200 {
        match handle.try_insert_broadcast_attempt(intent("full-queue")) {
            Err(refusal) => {
                refused = Some(refusal);
                break;
            }
            Ok(_receipt) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    }
    let refusal = refused.expect("a full bounded queue must refuse admission");
    assert!(
        matches!(refusal, revops_db::owner::StoreAdmissionRefused::QueueFull),
        "{refusal:?}"
    );

    blocker.execute_batch("ROLLBACK").unwrap();
    for task in pending {
        let _ = task.await;
    }
}

/// §3.1.2 "admitted, budget expired": once admitted, an expired receipt
/// is OUTCOME UNKNOWN -- the command is queued and uncancellable and may
/// still execute. It must never read as "no write happened".
#[tokio::test]
async fn admitted_receipt_expiry_is_outcome_unknown_and_may_still_land() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker
        .busy_timeout(std::time::Duration::from_secs(60))
        .unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    let receipt = handle
        .try_insert_broadcast_attempt(intent("expired-receipt"))
        .expect("an idle queue admits");
    let wait = receipt.within(std::time::Duration::from_millis(100)).await;
    assert!(
        matches!(wait, revops_db::owner::StoreReceiptWait::OutcomeUnknown),
        "expiry after admission must be UNKNOWN, not a clean failure"
    );

    // Release the wedge: the queued, uncancellable command now executes,
    // proving UNKNOWN was the only honest classification.
    blocker.execute_batch("ROLLBACK").unwrap();
    let mut landed = false;
    for _ in 0..200 {
        if handle.fee_broadcast_attempt_count().await.unwrap() == 1 {
            landed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert!(
        landed,
        "the admitted command must still land after the wedge lifts"
    );
}

/// §3.1.2 "admitted, reply within budget": the receipt carries the
/// actor's actual result both ways -- success and a clean actor-reported
/// error (here: a duplicate request_id violating the UNIQUE constraint).
#[tokio::test]
async fn admitted_receipt_within_budget_carries_the_actual_result() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");
    let handle = spawn_read_write(&path).await.unwrap();

    let receipt = handle
        .try_insert_broadcast_attempt(intent("carried"))
        .expect("admits");
    match receipt.within(std::time::Duration::from_secs(7)).await {
        revops_db::owner::StoreReceiptWait::Replied(Ok(id)) => assert!(id > 0),
        other => panic!("expected a replied success, got {other:?}"),
    }

    let receipt = handle
        .try_insert_broadcast_attempt(intent("carried"))
        .expect("admits");
    match receipt.within(std::time::Duration::from_secs(7)).await {
        revops_db::owner::StoreReceiptWait::Replied(Err(e)) => {
            assert!(format!("{e:#}").to_lowercase().contains("unique"), "{e:#}")
        }
        other => panic!("a duplicate request_id is a CLEAN actor-reported error, got {other:?}"),
    }
}
