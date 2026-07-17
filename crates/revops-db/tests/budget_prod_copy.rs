//! Production-database-copy smoke test (Phase 3 closure fix).
//!
//! PRODUCTION-WRITE CONSTRAINT: this file NEVER opens lnnode's live
//! `revenue_ops.db`. The `#[ignore]`d variant below only runs against a COPY
//! an operator has already made (env var `REVOPS_PROD_DB_COPY`), and even
//! then this test copies THAT copy again into a fresh `tempfile::TempDir`
//! before opening it read-write via `BudgetDb` — so nothing here can ever
//! mutate the path named by the environment variable, let alone the real
//! production file.
//!
//! # Running the ignored (production-shape) variant
//!
//! This requires an OPERATOR-AUTHORIZED copy of the production
//! `revenue_ops.db` (e.g. `scp`'d off `lnnode` under explicit operator
//! approval — never taken automatically by any agent or CI job). Point
//! `REVOPS_PROD_DB_COPY` at that copy and run:
//!
//! ```text
//! REVOPS_PROD_DB_COPY=/path/to/an/authorized/revenue_ops.db.copy \
//!     cargo test -p revops-db --test budget_prod_copy -- --ignored
//! ```
//!
//! The non-ignored `runs_against_fixture_db` test exercises the exact same
//! routine against the checked-in `fixtures/fixture.db`, so the code path
//! itself stays covered by ordinary `cargo test --workspace` / CI runs even
//! when no production copy is available.

use revops_db::budget::{BudgetDb, ReserveRequest};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs() as i64
}

/// Copies `src` (and any WAL/SHM sidecars) into a fresh tempdir, mirroring
/// `budget_concurrency.rs`'s cold-start-from-snapshot helper. Guarantees the
/// routine below only ever writes to a throwaway copy — never `src` itself,
/// whether `src` is the checked-in fixture or an operator-supplied
/// production copy.
fn copy_into_tempdir(src: &Path) -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let dst = dir.path().join("copy.db");
    std::fs::copy(src, &dst).expect("copy db file");
    for suffix in ["-wal", "-shm"] {
        let s = PathBuf::from(format!("{}{suffix}", src.display()));
        if s.exists() {
            let d = PathBuf::from(format!("{}{suffix}", dst.display()));
            std::fs::copy(&s, &d).expect("copy wal/shm sidecar");
        }
    }
    (dir, dst)
}

/// P4-017 committed-total shape (mirrors `budget_concurrency.rs`'s helper of
/// the same name, extended to also sum the legacy `budget_reservations`
/// table since a real production copy has both legacy and unified rows):
/// unfiltered active holds across BOTH reservation tables, plus committed
/// costs/events windowed on `since`.
fn committed_total(conn: &Connection, since: i64) -> i64 {
    let legacy_held: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM budget_reservations \
             WHERE status = 'active'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let unified_held: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM spend_reservations \
             WHERE status = 'active'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let costs: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(cost_sats), 0) FROM rebalance_costs WHERE timestamp >= ?1",
            [since],
            |r| r.get(0),
        )
        .unwrap();
    let events: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(amount_sats), 0) FROM spend_events WHERE timestamp >= ?1",
            [since],
            |r| r.get(0),
        )
        .unwrap();
    legacy_held + unified_held + costs + events
}

/// The routine both variants below run: `committed_total` +
/// `get_spend_reservation_states` + one reserve/release cycle. Every
/// assertion is a DELTA against whatever the copy already contains, so the
/// invariants hold whether the copy is empty (the fixture) or carries real,
/// non-trivial production history in both the legacy and unified tables.
fn run_smoke_routine(db_path: &Path) {
    // BudgetDb::open runs `CREATE TABLE/INDEX IF NOT EXISTS` DDL and sets
    // WAL mode; harmless (and idempotent) against an already-initialized
    // production schema shape.
    let mut db = BudgetDb::open(db_path).expect("open db copy read-write");
    let side = Connection::open(db_path).expect("side connection");
    side.busy_timeout(Duration::from_millis(5000)).unwrap();

    let now = now();
    let since = now - 24 * 3600;
    let before_total = committed_total(&side, since);

    // get_spend_reservation_states must succeed and its row count must
    // match a direct SQL count (capped at 10000), regardless of how many
    // rows a real production copy already has.
    let states = db
        .get_spend_reservation_states(None)
        .expect("get_spend_reservation_states");
    let row_count: i64 = side
        .query_row("SELECT COUNT(*) FROM spend_reservations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        states.len() as i64,
        row_count.min(10_000),
        "reservation-state count must match the table (capped at 10000)"
    );

    // One reserve/release cycle with a probe id vanishingly unlikely to
    // collide with any real reservation_id, and a 1-sat amount with no
    // budget cap so the grant never depends on the copy's actual budget
    // state.
    let rid = "phase3_closure_prod_copy_smoke_probe__do_not_reuse";
    let req = ReserveRequest {
        reservation_id: rid.to_string(),
        amount_sats: 1,
        category: "misc".to_string(),
        ..Default::default()
    };
    let (granted, _) = db.reserve_spend(req, now).expect("reserve_spend");
    assert!(granted, "best-effort (no budget) reserve must always grant");

    let after_reserve_total = committed_total(&side, since);
    assert_eq!(
        after_reserve_total,
        before_total + 1,
        "committed_total must move by exactly the reserved amount"
    );

    let states = db
        .get_spend_reservation_states(Some(&[rid.to_string()]))
        .expect("get_spend_reservation_states after reserve");
    assert_eq!(states.get(rid).map(|s| s.status.as_str()), Some("active"));
    assert_eq!(states.get(rid).map(|s| s.reserved_sats), Some(1));

    assert!(
        db.release_spend_reservation(rid).expect("release"),
        "release must flip the fresh active hold"
    );
    assert!(
        !db.release_spend_reservation(rid).expect("release again"),
        "a terminal rid must not be releasable twice"
    );

    let after_release_total = committed_total(&side, since);
    assert_eq!(
        after_release_total, before_total,
        "released holds must drop back out of committed_total"
    );
}

/// CI-covered code path: same routine, against the checked-in fixture db.
/// Runs in ordinary `cargo test --workspace`.
#[test]
fn runs_against_fixture_db() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db");
    let (_dir, copy_path) = copy_into_tempdir(&fixture);
    run_smoke_routine(&copy_path);
}

/// Production-shape smoke test. Requires an OPERATOR-AUTHORIZED copy of
/// lnnode's `revenue_ops.db` — see the module doc comment for how to run
/// it. Never runs in ordinary `cargo test` / CI (requires `--ignored`).
#[test]
#[ignore]
fn runs_against_prod_db_copy() {
    let path = std::env::var("REVOPS_PROD_DB_COPY")
        .expect("set REVOPS_PROD_DB_COPY=/path/to/an/operator-authorized copy of revenue_ops.db");
    let (_dir, copy_path) = copy_into_tempdir(Path::new(&path));
    run_smoke_routine(&copy_path);
}
