//! Task 65 slice 1: the writable production-schema actor. Every test
//! runs on a TEMP database carrying the exact Python DDL
//! (cl_revenue_ops/modules/database.py) -- the production file is never
//! involved anywhere in this workspace.

use revops_db::budget::ReserveRequest;
use revops_db::state_writer::{
    spawn_state_writer, BatchAck, BudgetTransition, ClosedChannelArchive, ConfigDelete,
    PeerPolicyWrite, PolicyDelete, StateWriterOpenError,
};
use rusqlite::Connection;
use std::path::PathBuf;

/// The Python-owned schema, verbatim from database.py:842-990 and the
/// closed-channel purge targets (:6592-6665).
fn python_schema(path: &PathBuf) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE peer_policies (
            peer_id TEXT PRIMARY KEY,
            strategy TEXT NOT NULL DEFAULT 'dynamic',
            rebalance_mode TEXT NOT NULL DEFAULT 'enabled',
            fee_ppm_target INTEGER,
            tags TEXT,
            updated_at INTEGER NOT NULL,
            fee_multiplier_min REAL,
            fee_multiplier_max REAL,
            expires_at INTEGER
        );
        CREATE TABLE hot_channel_protection_overrides (
            peer_id TEXT PRIMARY KEY,
            added_at INTEGER NOT NULL,
            note TEXT,
            min_depletion_trigger_pct REAL
        );
        CREATE TABLE config_overrides (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE budget_reservations (
            reservation_id TEXT PRIMARY KEY,
            reserved_sats INTEGER NOT NULL,
            reserved_at INTEGER NOT NULL,
            job_channel_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active'
        );
        CREATE TABLE spend_reservations (
            reservation_id TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            subcategory TEXT,
            reserved_sats INTEGER NOT NULL,
            reserved_at INTEGER NOT NULL,
            reference_id TEXT,
            channel_id TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            metadata_json TEXT
        );
        CREATE TABLE spend_events (
            event_id TEXT PRIMARY KEY,
            category TEXT NOT NULL,
            subcategory TEXT,
            amount_sats INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            reference_id TEXT,
            channel_id TEXT,
            source TEXT,
            metadata_json TEXT
        );
        CREATE TABLE rebalance_costs (cost_sats INTEGER, timestamp INTEGER);
        CREATE TABLE channel_states (channel_id TEXT PRIMARY KEY, peer_id TEXT);
        CREATE TABLE channel_failures (channel_id TEXT, at INTEGER);
        CREATE TABLE channel_probes (channel_id TEXT, at INTEGER);
        CREATE TABLE kalman_state (channel_id TEXT PRIMARY KEY, state TEXT);
        CREATE TABLE pair_rebalance_failures (
            source_channel_id TEXT, dest_channel_id TEXT, at INTEGER
        );
        CREATE TABLE fee_strategy_state (channel_id TEXT PRIMARY KEY, v2_state_json TEXT);
        CREATE TABLE closed_channels (
            channel_id TEXT PRIMARY KEY,
            peer_id TEXT NOT NULL,
            capacity_sats INTEGER NOT NULL,
            opened_at INTEGER,
            closed_at INTEGER NOT NULL,
            close_type TEXT NOT NULL,
            open_cost_sats INTEGER NOT NULL DEFAULT 0,
            closure_cost_sats INTEGER NOT NULL DEFAULT 0,
            total_revenue_sats INTEGER NOT NULL DEFAULT 0,
            total_rebalance_cost_sats INTEGER NOT NULL DEFAULT 0,
            forward_count INTEGER NOT NULL DEFAULT 0,
            net_pnl_sats INTEGER NOT NULL DEFAULT 0,
            days_open INTEGER NOT NULL DEFAULT 0,
            funding_txid TEXT,
            closing_txid TEXT,
            closer TEXT DEFAULT 'unknown'
        );
        "#,
    )
    .unwrap();
}

fn fixture() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("revenue_ops.db");
    python_schema(&path);
    (dir, path)
}

// ---------------------------------------------------------------------------
// Open refusals: fail-closed identity checks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn open_refuses_missing_file_and_never_creates_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nope.db");
    let err = spawn_state_writer(&path)
        .await
        .expect_err("a missing production db must refuse");
    assert!(
        matches!(err, StateWriterOpenError::MissingFile(_)),
        "{err:?}"
    );
    assert!(!path.exists(), "the writer must NEVER create the file");
}

#[tokio::test]
async fn open_refuses_a_schema_missing_a_required_table_or_column() {
    // Missing table.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("revenue_ops.db");
    python_schema(&path);
    Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE peer_policies;")
        .unwrap();
    let err = spawn_state_writer(&path).await.expect_err("missing table");
    match &err {
        StateWriterOpenError::SchemaMismatch { table, .. } => {
            assert_eq!(table, "peer_policies")
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }

    // Missing column.
    let dir2 = tempfile::tempdir().unwrap();
    let path2 = dir2.path().join("revenue_ops.db");
    python_schema(&path2);
    Connection::open(&path2)
        .unwrap()
        .execute_batch("ALTER TABLE peer_policies DROP COLUMN tags;")
        .unwrap();
    let err = spawn_state_writer(&path2)
        .await
        .expect_err("missing column");
    match &err {
        StateWriterOpenError::SchemaMismatch { table, detail } => {
            assert_eq!(table, "peer_policies");
            assert!(detail.contains("tags"), "{detail}");
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Config overrides: M-13 v2 version discipline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn config_versions_are_computed_inside_the_transaction_and_never_regress() {
    let (_d, path) = fixture();
    let writer = spawn_state_writer(&path).await.unwrap();

    assert_eq!(
        writer
            .set_config_override("a".into(), "1".into())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        writer
            .set_config_override("b".into(), "2".into())
            .await
            .unwrap(),
        2
    );
    // Overwrite the CURRENT max-version key: INSERT OR REPLACE deletes
    // the v2 row first -- a post-hoc MAX would regress. The version must
    // still advance (the M-13 v2 trap).
    assert_eq!(
        writer
            .set_config_override("b".into(), "3".into())
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        writer
            .set_config_override("a".into(), "4".into())
            .await
            .unwrap(),
        4
    );

    let conn = Connection::open(&path).unwrap();
    let (val, ver): (String, i64) = conn
        .query_row(
            "SELECT value, version FROM config_overrides WHERE key = 'a'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((val.as_str(), ver), ("4", 4));
}

#[tokio::test]
async fn config_delete_is_durable_and_distinguishes_absent() {
    let (_d, path) = fixture();
    let writer = spawn_state_writer(&path).await.unwrap();
    writer
        .set_config_override("k".into(), "v".into())
        .await
        .unwrap();

    assert_eq!(
        writer.delete_config_override("k".into()).await.unwrap(),
        ConfigDelete::Deleted
    );
    assert_eq!(
        writer.delete_config_override("k".into()).await.unwrap(),
        ConfigDelete::AlreadyAbsent
    );
    let conn = Connection::open(&path).unwrap();
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM config_overrides", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0);
}

// ---------------------------------------------------------------------------
// Budget transitions: guarded, terminal states never resurrect
// ---------------------------------------------------------------------------

#[tokio::test]
async fn budget_transitions_are_guarded_and_terminal() {
    let (_d, path) = fixture();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "INSERT INTO budget_reservations VALUES
         ('r1', 1000, 1000000, '100x1x0', 'active'),
         ('r2', 2000, 1000000, '200x2x0', 'active'),
         ('r-old', 3000, 1, '300x3x0', 'active');",
    )
    .unwrap();
    drop(conn);
    let writer = spawn_state_writer(&path).await.unwrap();

    assert_eq!(
        writer
            .release_budget_reservation("r1".into())
            .await
            .unwrap(),
        BudgetTransition::Applied
    );
    assert_eq!(
        writer.mark_budget_spent("r1".into(), 900).await.unwrap(),
        BudgetTransition::AlreadyTerminal,
        "released never becomes spent"
    );
    assert_eq!(
        writer.mark_budget_spent("r2".into(), 1500).await.unwrap(),
        BudgetTransition::Applied
    );
    assert_eq!(
        writer
            .release_budget_reservation("r2".into())
            .await
            .unwrap(),
        BudgetTransition::AlreadyTerminal
    );
    assert_eq!(
        writer
            .release_budget_reservation("ghost".into())
            .await
            .unwrap(),
        BudgetTransition::NotFound
    );

    // Stale cleanup touches only aged actives (r-old), not terminals.
    let released = writer
        .cleanup_stale_reservations(3600, 2_000_000)
        .await
        .unwrap();
    assert_eq!(released, 1);
    let conn = Connection::open(&path).unwrap();
    let status: String = conn
        .query_row(
            "SELECT status FROM budget_reservations WHERE reservation_id = 'r-old'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(status, "released");
}

#[tokio::test]
async fn generic_spend_commands_share_the_writer_and_preserve_atomic_results() {
    let (_d, path) = fixture();
    let writer = spawn_state_writer(&path).await.unwrap();

    let request = |id: &str, amount: i64, category: &str| ReserveRequest {
        reservation_id: id.to_string(),
        amount_sats: amount,
        category: category.to_string(),
        effective_budget_sats: Some(1_000),
        since_timestamp: Some(0),
        ..ReserveRequest::default()
    };
    assert_eq!(
        writer
            .reserve_spend(request("r1", 400, " Rebalance "), 100)
            .await
            .unwrap(),
        (true, 600)
    );
    assert_eq!(
        writer
            .reserve_spend(request("r2", 700, "misc"), 101)
            .await
            .unwrap(),
        (false, 600),
        "the actor transaction must enforce the live cross-category total"
    );
    assert!(writer.release_spend_reservation("r1".into()).await.unwrap());
    assert!(!writer.release_spend_reservation("r1".into()).await.unwrap());

    for (id, amount, category, at) in [
        ("old-a", 100, "foo", 1),
        ("old-b", 200, "foo", 2),
        ("old-c", 300, "bar", 3),
        ("fresh", 50, "foo", 950),
    ] {
        assert!(
            writer
                .reserve_spend(request(id, amount, category), at)
                .await
                .unwrap()
                .0
        );
    }
    let released = writer
        .release_spend_reservations(Some("foo".into()), 100, 1, 1_000)
        .await
        .unwrap();
    assert_eq!(released.released_count, 1);
    assert_eq!(released.released_sats, 100);
    assert_eq!(released.reservation_ids, vec!["old-a"]);

    assert!(writer
        .settle_spend_reservation(
            "old-b".into(),
            Some(150),
            Some("operator".into()),
            true,
            2_000,
        )
        .await
        .unwrap());
    assert!(!writer
        .settle_spend_reservation("old-b".into(), None, None, false, 2_001)
        .await
        .unwrap());

    assert!(
        writer
            .reserve_spend(request("bad-event", 40, "foo"), 1_500)
            .await
            .unwrap()
            .0
    );
    assert!(!writer
        .settle_spend_reservation("bad-event".into(), Some(-1), None, true, 2_100)
        .await
        .unwrap());

    let conn = Connection::open(&path).unwrap();
    let bad_status: String = conn
        .query_row(
            "SELECT status FROM spend_reservations WHERE reservation_id = 'bad-event'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let bad_events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM spend_events WHERE event_id = 'resv:bad-event'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bad_status, "active", "rejected event must roll settle back");
    assert_eq!(bad_events, 0);

    let row: (String, i64, String) = conn
        .query_row(
            "SELECT r.status, e.amount_sats, e.source
             FROM spend_reservations r JOIN spend_events e
               ON e.event_id = 'resv:' || r.reservation_id
             WHERE r.reservation_id = 'old-b'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(row, ("spent".into(), 150, "operator".into()));
}

// ---------------------------------------------------------------------------
// Batches: bounded to 100, all-or-nothing
// ---------------------------------------------------------------------------

fn policy(peer: &str) -> PeerPolicyWrite {
    PeerPolicyWrite {
        peer_id: peer.to_string(),
        strategy: "dynamic".to_string(),
        rebalance_mode: "enabled".to_string(),
        fee_ppm_target: Some(150),
        tags: None,
        fee_multiplier_min: None,
        fee_multiplier_max: None,
        expires_at: None,
    }
}

type StoredPolicyRow = (
    String,
    String,
    Option<i64>,
    Option<String>,
    Option<f64>,
    Option<f64>,
    Option<i64>,
);

#[tokio::test]
async fn policy_upsert_preserves_all_python_policy_columns_and_delete_is_explicit() {
    let (_d, path) = fixture();
    let writer = spawn_state_writer(&path).await.unwrap();
    let write = PeerPolicyWrite {
        peer_id: "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        strategy: "passive".into(),
        rebalance_mode: "disabled".into(),
        fee_ppm_target: Some(321),
        tags: Some(r#"["no_close","banned"]"#.into()),
        fee_multiplier_min: Some(0.75),
        fee_multiplier_max: Some(2.25),
        expires_at: Some(1_900_000_000),
    };

    writer
        .upsert_peer_policy(write.clone(), 1_800_000_000)
        .await
        .unwrap();
    let conn = Connection::open(&path).unwrap();
    let stored: StoredPolicyRow = conn.query_row(
            "SELECT strategy, rebalance_mode, fee_ppm_target, tags, fee_multiplier_min, fee_multiplier_max, expires_at FROM peer_policies WHERE peer_id = ?1",
            [&write.peer_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
        )
        .unwrap();
    assert_eq!(
        stored,
        (
            "passive".into(),
            "disabled".into(),
            Some(321),
            Some(r#"["no_close","banned"]"#.into()),
            Some(0.75),
            Some(2.25),
            Some(1_900_000_000),
        )
    );
    drop(conn);

    assert_eq!(
        writer
            .delete_peer_policy(write.peer_id.clone())
            .await
            .unwrap(),
        PolicyDelete::Deleted
    );
    assert_eq!(
        writer.delete_peer_policy(write.peer_id).await.unwrap(),
        PolicyDelete::AlreadyAbsent
    );
}

#[tokio::test]
async fn policy_batches_are_bounded_and_atomic() {
    let (_d, path) = fixture();
    let writer = spawn_state_writer(&path).await.unwrap();

    // 101 refused WHOLE.
    let oversized: Vec<PeerPolicyWrite> = (0..101).map(|i| policy(&format!("peer{i}"))).collect();
    match writer
        .apply_policy_batch(oversized, 1_800_000_000)
        .await
        .unwrap()
    {
        BatchAck::DeniedOverBound { len } => assert_eq!(len, 101),
        other => panic!("101 must refuse whole, got {other:?}"),
    }
    let conn = Connection::open(&path).unwrap();
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM peer_policies", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 0, "a refused batch writes NOTHING");
    drop(conn);

    // 100 applies atomically.
    let full: Vec<PeerPolicyWrite> = (0..100).map(|i| policy(&format!("peer{i}"))).collect();
    match writer
        .apply_policy_batch(full, 1_800_000_000)
        .await
        .unwrap()
    {
        BatchAck::Applied { count } => assert_eq!(count, 100),
        other => panic!("{other:?}"),
    }

    // A mid-batch failure rolls back ALL rows: a NOT NULL violation via
    // an empty strategy is not expressible through the typed row, so
    // sabotage with a trigger.
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TRIGGER poison_one BEFORE INSERT ON peer_policies
         WHEN NEW.peer_id = 'boom' BEGIN SELECT RAISE(ABORT, 'injected'); END;",
    )
    .unwrap();
    drop(conn);
    let mixed = vec![policy("fresh1"), policy("boom"), policy("fresh2")];
    assert!(writer
        .apply_policy_batch(mixed, 1_800_000_001)
        .await
        .is_err());
    let conn = Connection::open(&path).unwrap();
    let fresh: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM peer_policies WHERE peer_id LIKE 'fresh%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(fresh, 0, "mid-batch failure must roll back the whole batch");
}

// ---------------------------------------------------------------------------
// Closed-channel purge: py remove_closed_channel_data parity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn closed_channel_purge_hits_the_python_tables_atomically() {
    let (_d, path) = fixture();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "INSERT INTO channel_states VALUES ('700x1x0', 'peerA'), ('800x1x0', 'peerB');
         INSERT INTO channel_failures VALUES ('700x1x0', 1), ('800x1x0', 2);
         INSERT INTO channel_probes VALUES ('700x1x0', 1);
         INSERT INTO kalman_state VALUES ('700x1x0', '{}');
         INSERT INTO pair_rebalance_failures VALUES ('700x1x0', '900x1x0', 1),
                                                   ('900x1x0', '700x1x0', 2),
                                                   ('800x1x0', '900x1x0', 3);
         INSERT INTO fee_strategy_state VALUES ('700x1x0', '{}');",
    )
    .unwrap();
    drop(conn);
    let writer = spawn_state_writer(&path).await.unwrap();

    match writer
        .cleanup_closed_channels(vec!["700x1x0".to_string()])
        .await
        .unwrap()
    {
        BatchAck::Applied { count } => assert_eq!(count, 1),
        other => panic!("{other:?}"),
    }

    let conn = Connection::open(&path).unwrap();
    let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
    assert_eq!(count("SELECT COUNT(*) FROM channel_states"), 1);
    assert_eq!(count("SELECT COUNT(*) FROM channel_failures"), 1);
    assert_eq!(count("SELECT COUNT(*) FROM channel_probes"), 0);
    assert_eq!(count("SELECT COUNT(*) FROM kalman_state"), 0);
    assert_eq!(
        count("SELECT COUNT(*) FROM pair_rebalance_failures"),
        1,
        "both directions involving the closed channel purge (py: source OR dest)"
    );
    assert_eq!(count("SELECT COUNT(*) FROM fee_strategy_state"), 0);

    // Bound applies here too.
    let oversized: Vec<String> = (0..101).map(|i| format!("{i}x0x0")).collect();
    match writer.cleanup_closed_channels(oversized).await.unwrap() {
        BatchAck::DeniedOverBound { len } => assert_eq!(len, 101),
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Closed-channel archive: py record_closed_channel_history +
// remove_closed_channel_data, one atomic unit per channel
// ---------------------------------------------------------------------------

fn archive(channel_id: &str) -> ClosedChannelArchive {
    ClosedChannelArchive {
        channel_id: channel_id.to_string(),
        peer_id: "peerA".to_string(),
        capacity_sats: 1_000_000,
        opened_at: Some(1_800_000_000 - 10 * 86400),
        closed_at: 1_800_000_000,
        close_type: "mutual".to_string(),
        open_cost_sats: 500,
        closure_cost_sats: 300,
        total_revenue_sats: 900,
        total_rebalance_cost_sats: 50,
        forward_count: 20,
        funding_txid: None,
        closing_txid: Some("txid-close".to_string()),
        closer: "mutual".to_string(),
    }
}

/// `closed_channels` is a write target as of the archive op, so its
/// absence must refuse at OPEN (fail-closed), not at first archive.
#[tokio::test]
async fn open_refuses_a_schema_missing_closed_channels() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("revenue_ops.db");
    python_schema(&path);
    Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE closed_channels;")
        .unwrap();
    let err = spawn_state_writer(&path).await.expect_err("missing table");
    match &err {
        StateWriterOpenError::SchemaMismatch { table, .. } => {
            assert_eq!(table, "closed_channels")
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }
}

/// The archive records the permanent P&L row (derived net_pnl/days_open,
/// py database.py:6434-6446) and purges the live tracking rows in ONE
/// immediate transaction. Python runs record + purge as two separate
/// calls; fusing them is deliberate hardening — an archive that lands
/// without its purge would double-count the channel as both open and
/// closed, a partial state Python merely tolerates.
#[tokio::test]
async fn archive_closed_channel_records_pnl_row_and_purges_tracking() {
    let (_d, path) = fixture();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "INSERT INTO channel_states VALUES ('700x1x0', 'peerA');
         INSERT INTO kalman_state VALUES ('700x1x0', '{}');",
    )
    .unwrap();
    drop(conn);
    let writer = spawn_state_writer(&path).await.unwrap();

    writer
        .archive_closed_channel(archive("700x1x0"))
        .await
        .unwrap();

    let conn = Connection::open(&path).unwrap();
    let row: (String, i64, i64, i64, String, String) = conn
        .query_row(
            "SELECT peer_id, capacity_sats, net_pnl_sats, days_open, close_type, closer \
             FROM closed_channels WHERE channel_id = '700x1x0'",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .unwrap();
    // net_pnl = 900 - (500 + 300 + 50) = 50; days_open = 10.
    assert_eq!(
        row,
        (
            "peerA".to_string(),
            1_000_000,
            50,
            10,
            "mutual".to_string(),
            "mutual".to_string()
        )
    );
    let tracked: i64 = conn
        .query_row("SELECT COUNT(*) FROM channel_states", [], |r| r.get(0))
        .unwrap();
    let kalman: i64 = conn
        .query_row("SELECT COUNT(*) FROM kalman_state", [], |r| r.get(0))
        .unwrap();
    assert_eq!((tracked, kalman), (0, 0), "purge rides the same commit");
}

/// Python's input validation (database.py:6416-6433): amounts clamp to
/// |10 BTC|, forward_count floors at 0, close_type/closer outside their
/// valid sets become 'unknown', a missing/future opened_at yields
/// days_open 0 (M15 clock-skew clamp), and re-archiving REPLACES the row
/// (INSERT OR REPLACE).
#[tokio::test]
async fn archive_closed_channel_sanitizes_like_python_and_replaces() {
    let (_d, path) = fixture();
    let writer = spawn_state_writer(&path).await.unwrap();

    let mut wild = archive("900x1x0");
    wild.capacity_sats = 20_000_000_000;
    wild.forward_count = -5;
    wild.close_type = "weird".to_string();
    wild.closer = "banana".to_string();
    wild.opened_at = Some(1_800_000_000 + 999); // closed_at < opened_at
    writer.archive_closed_channel(wild).await.unwrap();

    let conn = Connection::open(&path).unwrap();
    let row: (i64, i64, String, String, i64) = conn
        .query_row(
            "SELECT capacity_sats, forward_count, close_type, closer, days_open \
             FROM closed_channels WHERE channel_id = '900x1x0'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        (
            10_000_000_000,
            0,
            "unknown".to_string(),
            "unknown".to_string(),
            0
        )
    );
    drop(conn);

    // Second archive of the same channel replaces, not errors.
    let mut second = archive("900x1x0");
    second.total_revenue_sats = 1;
    writer.archive_closed_channel(second).await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let (count, revenue): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*), MAX(total_revenue_sats) FROM closed_channels \
             WHERE channel_id = '900x1x0'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((count, revenue), (1, 1));
}

// ---------------------------------------------------------------------------
// Hot-channel overrides
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hot_channel_overrides_round_trip() {
    let (_d, path) = fixture();
    let writer = spawn_state_writer(&path).await.unwrap();
    writer
        .set_hot_channel_override(
            "peerX".into(),
            Some("drain guard".into()),
            Some(0.25),
            1_800_000_000,
        )
        .await
        .unwrap();
    assert!(writer
        .remove_hot_channel_override("peerX".into())
        .await
        .unwrap());
    assert!(!writer
        .remove_hot_channel_override("peerX".into())
        .await
        .unwrap());
}
