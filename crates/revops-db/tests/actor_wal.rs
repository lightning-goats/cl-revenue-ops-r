//! WAL-concurrency and cold-start integration tests for the persistent
//! read-only DB actor (`revops_db::actor`). These pin the two obligations
//! Phase 1a's probe-and-drop couldn't exercise (there was no persistent
//! connection to hold open across a concurrent writer, and no async actor
//! spawn path to fail gracefully on a cold-start path):
//!
//! 1. `reader_sees_only_committed_data_while_writer_holds_open_transaction`
//!    — the actor's read connection must attach to a WAL db while another
//!    connection holds an open, uncommitted `BEGIN IMMEDIATE` transaction,
//!    see none of the uncommitted row, then see it once committed.
//! 2. `cold_start_before_writer_fails_gracefully` — pointing the actor at a
//!    path with no `-wal`/`-shm` (indeed no file at all) must fail with a
//!    clean `Err`, never hang or panic.
//! 3. `table_listing_failure_at_spawn_returns_err` — a path that opens
//!    fine (SQLite's open is lazy) but fails the `table_names` probe
//!    (not a valid database) must fail `spawn_read_only` itself, not
//!    silently succeed and defer the failure to first request -- pins
//!    the table-listing-leniency-regression fix (option (a) from review).

use revops_db::actor::spawn_read_only;
use rusqlite::Connection;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

#[tokio::test]
async fn cold_start_before_writer_fails_gracefully() {
    // No file has ever been created at this path -- mirrors an operator
    // pointing revops-r-db-path at a DB the Python plugin hasn't
    // initialized yet (or a typo'd path). Must be a clean Err, never a
    // panic that would crash plugin init.
    //
    // Wrapped in a timeout so a future regression that makes this path
    // hang (rather than fail fast) fails CI loudly instead of wedging it.
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        spawn_read_only(Path::new("/nonexistent/cl-revops-phase1b/nope.db")),
    )
    .await;
    let err = result
        .expect("spawn_read_only must fail fast on a cold-start path, not hang")
        .unwrap_err();
    assert!(
        err.to_string().contains("not found") || err.to_string().contains("database"),
        "unexpected error message: {err}"
    );
}

#[tokio::test]
async fn table_listing_failure_at_spawn_returns_err() {
    // A file that exists (so `open_read_only`'s `path.exists()` check
    // passes) and that SQLite's lazy `Connection::open_with_flags` opens
    // without complaint, but that is not actually a valid SQLite database
    // -- the failure only surfaces once something tries to read the
    // header, which is exactly what the `table_names` probe does. Before
    // the fix, `spawn_read_only` returned `Ok` here (the failure was
    // deferred to the first `table_count()`/`query_i64()` call and then
    // swallowed by the `.ok()` at the `revenue-r-status` call site); the
    // fix makes this fail synchronously at spawn time instead.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not-a-database.db");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"this is not a sqlite file, just garbage bytes\0\0\0\0")
            .unwrap();
    }

    let err = spawn_read_only(&path)
        .await
        .expect_err("table_names probe over a non-database file must fail spawn_read_only");
    assert!(
        err.to_string().contains("table_names probe"),
        "expected the probe context to be present: {err}"
    );
}

fn init_wal_db(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=WAL; \
         CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);",
    )
    .unwrap();
}

#[tokio::test]
async fn reader_sees_only_committed_data_while_writer_holds_open_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.db");
    init_wal_db(&path);

    // Writer opens its own connection (simulating the Python plugin's
    // writer thread) and holds an uncommitted BEGIN IMMEDIATE across the
    // whole test body.
    let mut writer = Connection::open(&path).unwrap();
    writer.busy_timeout(Duration::from_millis(5000)).unwrap();
    let tx = writer
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    tx.execute("INSERT INTO t (v) VALUES ('uncommitted')", [])
        .unwrap();

    // Reader (our actor) attaches WHILE the writer transaction is still
    // open and uncommitted.
    let handle = spawn_read_only(&path)
        .await
        .expect("reader attaches under WAL");
    let count_before: i64 = handle
        .query_i64("SELECT COUNT(*) FROM t", vec![])
        .await
        .unwrap();
    // WAL snapshot isolation: the reader must NOT see the writer's
    // uncommitted row -- this is the property that makes read-only
    // coexistence with Python's writer safe.
    assert_eq!(count_before, 0, "reader saw an uncommitted write");

    tx.commit().unwrap();
    drop(writer);

    let count_after: i64 = handle
        .query_i64("SELECT COUNT(*) FROM t", vec![])
        .await
        .unwrap();
    assert_eq!(count_after, 1, "reader didn't pick up the committed write");
}
