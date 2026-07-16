//! Tests for `revops_db::owner` -- the single-owner read-write actor over
//! the Rust plugin's OWN notification-ingestion db (never production).
//! Mirrors `actor.rs`'s single-owner-task pattern (see `actor_wal.rs`) but
//! for a writable connection created fresh if the file doesn't exist yet.

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
