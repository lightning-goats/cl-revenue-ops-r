//! Budget rail concurrency + restart durability tests (Phase 3 Task 9).
//!
//! PRODUCTION-WRITE CONSTRAINT: every test here builds its own database
//! inside a `tempfile::TempDir` using `BudgetDb::open`'s own DDL. Nothing in
//! this file ever takes a path from the environment or from configuration;
//! there is no way for any test to reach the operator's live node database.
//!
//! # Method: TDD foils
//!
//! For the two properties where a naive (non-atomic) implementation is the
//! textbook failure mode — check-then-act reservation and non-atomic
//! settle — this file first proves the invariant-checking logic used later
//! is actually capable of catching the bug. Each such test builds a
//! deliberately-weakened oracle out of RAW SQL (never `budget.rs` production
//! code) and asserts it DOES violate the invariant under real thread
//! contention. Only after that foil is shown failing does the matching real
//! test exercise `BudgetDb` and assert the invariant HOLDS. This mirrors
//! "watch it fail first" from the TDD discipline, at the level of the
//! invariant itself rather than the production code (which is already
//! merged): if the foil ever stopped overshooting, the corresponding
//! "real" test would not be trustworthy evidence of anything.
//!
//! Foil tests (do NOT touch `crates/revops-db/src/budget.rs`):
//! - `naive_toctou_reserve_overshoots_budget_under_contention`
//! - `naive_per_category_budget_check_overshoots_jointly`
//! - `naive_non_atomic_settle_allows_budget_dip_window`

use revops_db::budget::{BudgetDb, BudgetError, ReserveRequest};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

/// Frozen clock, matching the golden drivers' FROZEN_NOW (also used by the
/// Task 6 lifecycle suite).
const NOW: i64 = 1_752_400_000;
const DAY: i64 = 86_400;
const SINCE: i64 = NOW - DAY;

fn req(rid: &str, amount: i64, cat: &str) -> ReserveRequest {
    ReserveRequest {
        reservation_id: rid.to_string(),
        amount_sats: amount,
        category: cat.to_string(),
        ..Default::default()
    }
}

fn budget_req(rid: &str, amount: i64, cat: &str, budget: i64) -> ReserveRequest {
    ReserveRequest {
        effective_budget_sats: Some(budget),
        since_timestamp: Some(SINCE),
        ..req(rid, amount, cat)
    }
}

/// P4-017 committed-total shape, restricted to what these tests ever seed
/// (`spend_reservations` + `spend_events`; no `budget_reservations` /
/// `rebalance_costs` rows are created in this file): unfiltered active
/// holds plus windowed committed spend.
fn committed_total(conn: &Connection, since: i64) -> i64 {
    let held: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM spend_reservations WHERE status = 'active'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let spent: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(amount_sats), 0) FROM spend_events WHERE timestamp >= ?1",
            [since],
            |r| r.get(0),
        )
        .unwrap();
    held + spent
}

fn side_conn(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(Duration::from_millis(5000)).unwrap();
    conn
}

/// Copies the main db file AND its WAL/SHM sidecars, simulating a cold
/// start from a filesystem snapshot taken while WAL pages had not yet been
/// checkpointed into the main file.
fn copy_wal_files(src: &Path, dst: &Path) {
    std::fs::copy(src, dst).expect("copy main db file");
    for suffix in ["-wal", "-shm"] {
        let s = PathBuf::from(format!("{}{suffix}", src.display()));
        if s.exists() {
            let d = PathBuf::from(format!("{}{suffix}", dst.display()));
            std::fs::copy(&s, &d).expect("copy wal/shm sidecar");
        }
    }
}

// ---------------------------------------------------------------------------
// 8-thread contention (+ TDD foil)
// ---------------------------------------------------------------------------

/// Deliberately weakened oracle: SELECT the current held total, sleep
/// (widening the race window so the bug is not merely theoretical), THEN
/// insert unconditionally. This is the check-then-act anti-pattern
/// `budget.rs`'s single `BEGIN IMMEDIATE` transaction exists to prevent —
/// reproduced here in raw SQL only, never via `BudgetDb`.
fn naive_toctou_reserve(conn: &Connection, rid: &str, amount: i64, budget: i64) -> bool {
    let held: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM spend_reservations WHERE status = 'active'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let granted = held + amount <= budget;
    thread::sleep(Duration::from_millis(20));
    if granted {
        conn.execute(
            "INSERT INTO spend_reservations \
             (reservation_id, category, reserved_sats, reserved_at, status) \
             VALUES (?1, 'misc', ?2, ?3, 'active')",
            rusqlite::params![rid, amount, NOW],
        )
        .unwrap();
    }
    granted
}

#[test]
fn naive_toctou_reserve_overshoots_budget_under_contention() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    drop(BudgetDb::open(&path).unwrap()); // schema only, then close

    const THREADS: usize = 8;
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            thread::spawn(move || {
                let conn = side_conn(&path);
                barrier.wait();
                naive_toctou_reserve(&conn, &format!("naive-{t}"), 300, 1000)
            })
        })
        .collect();
    let grants: usize = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|&ok| ok)
        .count();

    let conn = side_conn(&path);
    let held = committed_total(&conn, SINCE);
    assert!(
        held > 1000,
        "the naive check-then-act path is expected to overshoot the 1000-sat \
         budget under contention (held={held}, grants={grants}); if this ever \
         stops overshooting, widen the sleep in naive_toctou_reserve rather \
         than trusting the invariant check below"
    );
}

#[test]
fn eight_thread_contention_never_overshoots_budget() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    drop(BudgetDb::open(&path).unwrap()); // schema only, then close

    const BUDGET: i64 = 1000;
    const AMOUNT: i64 = 300;
    const THREADS: usize = 8;
    const ATTEMPTS: usize = 10;

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            thread::spawn(move || {
                let mut db = BudgetDb::open(&path).expect("thread-local connection");
                barrier.wait();
                let mut grants = 0usize;
                for a in 0..ATTEMPTS {
                    let rid = format!("t{t}-{a}");
                    let (ok, _) = db
                        .reserve_spend(budget_req(&rid, AMOUNT, "misc", BUDGET), NOW)
                        .expect("reserve_spend must not error under contention");
                    if ok {
                        grants += 1;
                    }
                }
                grants
            })
        })
        .collect();
    let total_grants: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

    assert_eq!(
        total_grants,
        (BUDGET / AMOUNT) as usize,
        "with no releases, exactly floor(1000/300)=3 reservations may EVER be granted"
    );

    let conn = side_conn(&path);
    let held = committed_total(&conn, SINCE);
    assert!(
        held <= BUDGET,
        "committed total {held} must never exceed budget {BUDGET}"
    );
    assert_eq!(held, total_grants as i64 * AMOUNT);
}

// ---------------------------------------------------------------------------
// Cross-category contention (+ TDD foil)
// ---------------------------------------------------------------------------

/// Deliberately weakened oracle: sums held amounts FILTERED BY CATEGORY,
/// so each category believes it alone owns the full budget. This is the
/// structural bug the P4-017 shape (an unfiltered sum across ALL
/// categories) exists to prevent.
fn naive_per_category_reserve(
    conn: &Connection,
    rid: &str,
    amount: i64,
    category: &str,
    budget: i64,
) -> bool {
    let held: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM spend_reservations \
             WHERE status = 'active' AND category = ?1",
            [category],
            |r| r.get(0),
        )
        .unwrap();
    if held + amount > budget {
        return false;
    }
    conn.execute(
        "INSERT INTO spend_reservations \
         (reservation_id, category, reserved_sats, reserved_at, status) \
         VALUES (?1, ?2, ?3, ?4, 'active')",
        rusqlite::params![rid, category, amount, NOW],
    )
    .unwrap();
    true
}

#[test]
fn naive_per_category_budget_check_overshoots_jointly() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    drop(BudgetDb::open(&path).unwrap());

    let barrier = Arc::new(Barrier::new(2));
    let cats = ["rebalance", "boltz"];
    let handles: Vec<_> = cats
        .iter()
        .map(|&cat| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            thread::spawn(move || {
                let conn = side_conn(&path);
                barrier.wait();
                naive_per_category_reserve(&conn, &format!("naive-{cat}"), 1000, cat, 1000)
            })
        })
        .collect();
    let all_granted = handles.into_iter().all(|h| h.join().unwrap());
    assert!(
        all_granted,
        "each per-category check independently believes it has the full 1000-sat budget"
    );

    let conn = side_conn(&path);
    let held = committed_total(&conn, SINCE);
    assert_eq!(
        held, 2000,
        "per-category budget checking jointly overshoots a single shared 1000-sat budget"
    );
}

#[test]
fn cross_category_contention_never_overshoots_shared_budget() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    drop(BudgetDb::open(&path).unwrap());

    const BUDGET: i64 = 1000;
    const AMOUNT: i64 = 300;
    const THREADS: usize = 8; // even threads: rebalance via reserve_budget; odd: boltz via reserve_spend
    const ATTEMPTS: usize = 10;

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            thread::spawn(move || {
                let mut db = BudgetDb::open(&path).expect("thread-local connection");
                barrier.wait();
                let mut grants = 0usize;
                for a in 0..ATTEMPTS {
                    let rid = format!("x{t}-{a}");
                    let ok = if t % 2 == 0 {
                        db.reserve_budget(&rid, AMOUNT, "chan", BUDGET, SINCE, None, None, NOW)
                            .unwrap()
                            .0
                    } else {
                        db.reserve_spend(budget_req(&rid, AMOUNT, "boltz", BUDGET), NOW)
                            .unwrap()
                            .0
                    };
                    if ok {
                        grants += 1;
                    }
                }
                grants
            })
        })
        .collect();
    let total_grants: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

    assert_eq!(total_grants, (BUDGET / AMOUNT) as usize);
    let conn = side_conn(&path);
    let held = committed_total(&conn, SINCE);
    assert!(
        held <= BUDGET,
        "rebalance and boltz reservations must never jointly overshoot the shared budget (held={held})"
    );
    let rebalance_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM spend_reservations WHERE category = 'rebalance' AND status = 'active'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let boltz_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM spend_reservations WHERE category = 'boltz' AND status = 'active'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rebalance_count + boltz_count, total_grants as i64);
}

// ---------------------------------------------------------------------------
// Reserve -> settle race (+ TDD foil)
// ---------------------------------------------------------------------------

/// Deliberately weakened oracle: flips the reservation to `'spent'` as its
/// own autocommit statement, sleeps (the dip window), THEN inserts the
/// settlement event as a second, separate autocommit statement. During the
/// sleep the reservation is neither an active hold NOR a committed event —
/// exactly the "spent without its event" state `budget.rs`'s single
/// `BEGIN IMMEDIATE` (UPDATE + INSERT in one transaction) exists to make
/// impossible.
fn naive_non_atomic_settle(conn: &Connection, rid: &str, amount: i64) {
    conn.execute(
        "UPDATE spend_reservations SET status = 'spent' WHERE reservation_id = ?1 AND status = 'active'",
        [rid],
    )
    .unwrap();
    thread::sleep(Duration::from_millis(50));
    conn.execute(
        "INSERT INTO spend_events (event_id, category, amount_sats, timestamp) \
         VALUES (?1, 'misc', ?2, ?3)",
        rusqlite::params![format!("resv:{rid}"), amount, NOW],
    )
    .unwrap();
}

#[test]
fn naive_non_atomic_settle_allows_budget_dip_window() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    let mut seed_db = BudgetDb::open(&path).unwrap();
    let (ok, _) = seed_db
        .reserve_spend(budget_req("pre-1", 700, "misc", 1000), NOW)
        .unwrap();
    assert!(ok);
    drop(seed_db);

    let settle_path = path.clone();
    let settle = thread::spawn(move || {
        let conn = side_conn(&settle_path);
        naive_non_atomic_settle(&conn, "pre-1", 700);
    });
    // Give the settle thread a head start into its dip window before the
    // racers start attempting reservations against the (briefly) emptied
    // budget.
    thread::sleep(Duration::from_millis(10));

    const RACERS: usize = 4;
    let barrier = Arc::new(Barrier::new(RACERS));
    let racers: Vec<_> = (0..RACERS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            thread::spawn(move || {
                let mut db = BudgetDb::open(&path).unwrap();
                barrier.wait();
                db.reserve_spend(budget_req(&format!("race-{t}"), 250, "misc", 1000), NOW)
                    .unwrap()
                    .0
            })
        })
        .collect();

    settle.join().unwrap();
    let grants: usize = racers
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|&ok| ok)
        .count();

    let conn = side_conn(&path);
    let total = committed_total(&conn, SINCE);
    assert!(
        total > 1000,
        "a non-atomic settle should let concurrent reservations overshoot the budget \
         during its dip window (total={total}, race_grants={grants}); if this ever \
         stops overshooting, widen the sleep in naive_non_atomic_settle"
    );
}

#[test]
fn reserve_settle_race_never_dips_committed_total() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    let mut seed_db = BudgetDb::open(&path).unwrap();
    let (ok, _) = seed_db
        .reserve_spend(budget_req("pre-1", 700, "misc", 1000), NOW)
        .unwrap();
    assert!(ok);
    drop(seed_db);

    const RACERS: usize = 4;
    const THREADS: usize = RACERS + 1; // + the settler
    let barrier = Arc::new(Barrier::new(THREADS));

    let settle_barrier = Arc::clone(&barrier);
    let settle_path = path.clone();
    let settle = thread::spawn(move || {
        let mut db = BudgetDb::open(&settle_path).unwrap();
        settle_barrier.wait();
        db.mark_spend_reservation_spent("pre-1", Some(700), None, true, NOW + 1)
            .unwrap()
    });
    let racers: Vec<_> = (0..RACERS)
        .map(|t| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            thread::spawn(move || {
                let mut db = BudgetDb::open(&path).unwrap();
                barrier.wait();
                db.reserve_spend(budget_req(&format!("race-{t}"), 250, "misc", 1000), NOW)
                    .unwrap()
                    .0
            })
        })
        .collect();

    let settled = settle.join().unwrap();
    assert!(settled);
    racers.into_iter().for_each(|h| {
        h.join().unwrap();
    });

    let conn = side_conn(&path);
    let total = committed_total(&conn, SINCE);
    assert!(
        total <= 1000,
        "committed total {total} must never exceed the 1000-sat budget across the settle race"
    );

    // No window where 'spent' exists without its event: every settled
    // reservation has its resv:{rid} event landed atomically with the flip.
    let mut stmt = conn
        .prepare("SELECT reservation_id FROM spend_reservations WHERE status = 'spent'")
        .unwrap();
    let spent_rids: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();
    assert!(!spent_rids.is_empty(), "the settle must have completed");
    for rid in spent_rids {
        let event_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM spend_events WHERE event_id = ?1",
                [format!("resv:{rid}")],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            event_exists, 1,
            "reservation {rid} is 'spent' but has no settlement event"
        );
    }
}

// ---------------------------------------------------------------------------
// Simulated restart (scenario 24 shape)
// ---------------------------------------------------------------------------

#[test]
fn simulated_restart_holds_survive_cold_start_and_still_gate() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    {
        let mut db = BudgetDb::open(&path).unwrap();
        let (ok, _) = db
            .reserve_spend(budget_req("restart-1", 500, "misc", 800), NOW)
            .unwrap();
        assert!(ok);
    } // all handles dropped here: simulates the process exiting

    // Cold-start simulation: copy db + wal + shm to a fresh path (WAL may
    // still hold the committed page that never made it into the main
    // file) and reopen there, standing in for a fresh process starting
    // against a filesystem snapshot.
    let copy_path = dir.path().join("budget-cold-start-copy.db");
    copy_wal_files(&path, &copy_path);

    let copy_db = BudgetDb::open(&copy_path).unwrap();
    let copy_states = copy_db
        .get_spend_reservation_states(Some(&["restart-1".to_string()]))
        .unwrap();
    assert_eq!(copy_states["restart-1"].status, "active");
    assert_eq!(copy_states["restart-1"].reserved_sats, 500);

    // AND the original path, reopened fresh (same process restarting
    // against its own file).
    let mut orig_db = BudgetDb::open(&path).unwrap();
    let orig_states = orig_db
        .get_spend_reservation_states(Some(&["restart-1".to_string()]))
        .unwrap();
    assert_eq!(orig_states["restart-1"].status, "active");
    assert_eq!(orig_states["restart-1"].reserved_sats, 500);

    // The outstanding hold survives restart and still gates: 800 - 500 =
    // 300 remaining, refuses a fresh 400-sat ask.
    let (ok, rem) = orig_db
        .reserve_spend(budget_req("restart-2", 400, "misc", 800), NOW + 10)
        .unwrap();
    assert!(!ok);
    assert_eq!(rem, 300);
}

// ---------------------------------------------------------------------------
// Crash-window restart
// ---------------------------------------------------------------------------

#[test]
fn crash_window_retry_exhaustion_leaves_status_active_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    // Short busy timeout: a test seam only, so the exhausted-retry path
    // below stays fast (production always opens with the house 5000ms).
    let mut db = BudgetDb::open_with_busy_timeout(&path, 50).unwrap();
    let (ok, _) = db.reserve_spend(req("cw-1", 400, "misc"), NOW).unwrap();
    assert!(ok);

    // A second connection holds the write lock mid-transaction: this
    // stands in for the process being killed between the UPDATE and the
    // event write, which is impossible by construction (both statements
    // are one `BEGIN IMMEDIATE`) — so the only way to observe a "stuck"
    // in-flight settle is to have a DIFFERENT writer hold the lock and
    // force the primary's attempt to exhaust its busy timeout entirely,
    // never even starting its own transaction.
    let blocker = side_conn(&path);
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

    let err = db
        .mark_spend_reservation_spent("cw-1", Some(400), None, true, NOW + 1)
        .unwrap_err();
    assert!(matches!(err, BudgetError::Sqlite(_)));

    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    drop(db);

    // Reopen (simulated restart) and confirm the single-transaction
    // construction held: the settle never partially applied. Status is
    // still 'active' — never 'spent' without its event, never stuck
    // mid-flip.
    let reopened = BudgetDb::open(&path).unwrap();
    let states = reopened
        .get_spend_reservation_states(Some(&["cw-1".to_string()]))
        .unwrap();
    assert_eq!(states["cw-1"].status, "active");
    assert_eq!(states["cw-1"].reserved_sats, 400);

    let conn = side_conn(&path);
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM spend_events WHERE event_id = 'resv:cw-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 0);
}

// ---------------------------------------------------------------------------
// WAL dual-writer
// ---------------------------------------------------------------------------

/// A raw-SQL writer shaped exactly like `record_spend_event_on`'s
/// `INSERT OR REPLACE INTO spend_events` statement (the same statement the
/// ported-from Python side issues), running as an independent WAL writer
/// alongside `BudgetDb::reserve_spend`. Retries on a transient busy error,
/// mirroring the retry discipline the rail itself uses — the obligation
/// under test is that BOTH writers' commits land with no lost writes.
fn python_shaped_insert_event(conn: &Connection, event_id: &str, amount: i64, ts: i64) {
    let mut last_err = None;
    for attempt in 0..10u32 {
        match conn.execute(
            "INSERT OR REPLACE INTO spend_events \
             (event_id, category, subcategory, amount_sats, timestamp, \
              reference_id, channel_id, source, metadata_json) \
             VALUES (?1, 'misc', NULL, ?2, ?3, NULL, NULL, 'python_writer', NULL)",
            rusqlite::params![event_id, amount, ts],
        ) {
            Ok(_) => return,
            Err(e) => {
                last_err = Some(e);
                thread::sleep(Duration::from_millis(10 * u64::from(attempt + 1)));
            }
        }
    }
    panic!("python-shaped writer lost a write ({event_id}): {last_err:?}");
}

#[test]
fn wal_dual_writer_no_lost_writes_and_sums_agree() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("budget.db");
    drop(BudgetDb::open(&path).unwrap());

    const EVENTS: usize = 20;
    const RESERVES: usize = 20;
    let barrier = Arc::new(Barrier::new(2));

    let writer_barrier = Arc::clone(&barrier);
    let writer_path = path.clone();
    let writer = thread::spawn(move || {
        let conn = side_conn(&writer_path);
        writer_barrier.wait();
        for i in 0..EVENTS {
            python_shaped_insert_event(&conn, &format!("py-{i}"), 10, NOW - i as i64);
        }
    });
    let reserver_barrier = Arc::clone(&barrier);
    let reserver_path = path.clone();
    let reserver = thread::spawn(move || {
        let mut db = BudgetDb::open(&reserver_path).unwrap();
        reserver_barrier.wait();
        let mut grants = 0usize;
        for i in 0..RESERVES {
            // Best-effort (no budget cap): isolates "do writes get lost"
            // from "does the budget check refuse", which the other tests
            // already cover.
            let (ok, _) = db
                .reserve_spend(req(&format!("rust-{i}"), 5, "misc"), NOW)
                .unwrap();
            if ok {
                grants += 1;
            }
        }
        grants
    });

    writer.join().unwrap();
    let grants = reserver.join().unwrap();
    assert_eq!(grants, RESERVES);

    let conn = side_conn(&path);
    let event_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM spend_events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        event_count, EVENTS as i64,
        "no lost writes from the python-shaped writer"
    );
    let resv_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM spend_reservations", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        resv_count, RESERVES as i64,
        "no lost writes from the rust reserver"
    );
    let event_sum: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(amount_sats), 0) FROM spend_events",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(event_sum, EVENTS as i64 * 10);
    let resv_sum: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(reserved_sats), 0) FROM spend_reservations WHERE status = 'active'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(resv_sum, RESERVES as i64 * 5);
}

// ---------------------------------------------------------------------------
// Hygiene: no absolute operator-filesystem path, no production db filename
// ---------------------------------------------------------------------------

#[test]
fn file_contains_no_absolute_home_or_production_path() {
    let src = include_str!("budget_concurrency.rs");
    // Built by concatenation so the banned literal itself is never a
    // substring of THIS file's own source (which would trivially self-fail).
    let banned_home_prefix = format!("{}{}", "/ho", "me");
    assert!(
        !src.contains(&banned_home_prefix),
        "test file must never reference an absolute operator-filesystem path"
    );
    let banned_prod_db = format!("{}{}", "revenue_op", "s.db");
    assert!(
        !src.contains(&banned_prod_db),
        "test file must never reference the production db filename"
    );
}
