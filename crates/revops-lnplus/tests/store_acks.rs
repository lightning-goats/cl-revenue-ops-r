//! Task 61 stage 4A — the fallible, acknowledged, CAS-transitioned LN+
//! store contract, proven against the REAL `SqliteLnPlusDb` (throwaway
//! temp-file databases only; HARD RULE: never a production db).
//!
//! What this file pins, per the Task 4 contract:
//!  - every lifecycle write is FALLIBLE and ACKNOWLEDGED — a persistence
//!    failure surfaces as `Err` to the caller, never a warn-log + silent
//!    success return;
//!  - row creation is a typed INSERT (`insert_swap_new`), not an
//!    overwrite-prone `INSERT OR REPLACE`: a second insert of the same
//!    swap_id is `AlreadyExists` and leaves the existing row untouched;
//!  - row mutation is a CAS transition (`cas_swap`): the patch applies
//!    only from an expected status, and a conflict is a typed outcome
//!    with the actual status, never a blind UPDATE;
//!  - the compound terminal+breaker change (`terminalize_and_trip`) is
//!    ATOMIC: either the row terminalizes AND the breaker state advances
//!    (preserving the B10 first cause) in one transaction, or neither
//!    happens — proven with a real mid-compound persistence fault;
//!  - breaker reads fail CLOSED: a malformed persisted breaker value is
//!    an `Err`, never silently "untripped".

mod common;

use common::FakeLogger;
use revops_lnplus::breaker::{BreakerCause, BreakerState};
use revops_lnplus::db_types::{SwapPatch, SwapRow};
use revops_lnplus::ports::{
    CasOutcome, CompoundOutcome, InsertOutcome, LnPlusDb, TerminalizeSpec, TripAck,
};
use revops_lnplus::sqlite_db::SqliteLnPlusDb;

fn open_db() -> (tempfile::TempDir, SqliteLnPlusDb, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lnplus.db");
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).expect("open db");
    (dir, db, path)
}

/// A second, independent connection to the same file — the fault seam:
/// dropping a table out from under the store makes the NEXT write to that
/// table a real sqlite error, exactly the failure class the acked-write
/// contract exists to surface.
fn sabotage(path: &std::path::Path, sql: &str) {
    let conn = rusqlite::Connection::open(path).expect("sabotage connection");
    conn.execute_batch(sql).expect("sabotage sql");
}

fn seeded(db: &SqliteLnPlusDb, sid: &str, status: &str) {
    let outcome = db
        .insert_swap_new(&SwapRow::new(sid, status, 2_000_000, 6, 1_000))
        .expect("seed insert");
    assert_eq!(outcome, InsertOutcome::Inserted);
}

// ------------------------------------------------------ typed insert

#[test]
fn insert_swap_new_second_insert_is_already_exists_and_never_clobbers() {
    let (_dir, db, _path) = open_db();
    seeded(&db, "s1", "applied");
    let before = db.get_swap("s1").expect("row present");

    // Same id, different terms — the overwrite-prone INSERT OR REPLACE
    // shape this API replaces would silently clobber the row.
    let outcome = db
        .insert_swap_new(&SwapRow::new("s1", "opening", 999, 1, 2_000))
        .expect("second insert acked");
    assert_eq!(outcome, InsertOutcome::AlreadyExists);
    assert_eq!(
        db.get_swap("s1").expect("row still present"),
        before,
        "an AlreadyExists insert must leave the existing row byte-identical"
    );
}

#[test]
fn insert_swap_new_persists_every_row_field_in_one_write() {
    // Backfill previously needed an insert + patch pair because the old
    // INSERT wrote only a column subset; the typed insert persists the
    // whole row so no second write can be lost.
    let (_dir, db, _path) = open_db();
    let row = SwapRow::new("s2", "opening", 2_000_000, 6, 1_000)
        .with_outbound_peer("02aa")
        .with_incoming_peer("02bb")
        .with_our_identifier("A")
        .with_deadline_at(9_000)
        .with_ends_at(99_000)
        .with_opened_at(1_500)
        .with_channel_funding_txid("txid-abc")
        .with_planner_action_id(41);
    assert_eq!(
        db.insert_swap_new(&row).expect("insert acked"),
        InsertOutcome::Inserted
    );
    assert_eq!(db.get_swap("s2").expect("row present"), row);
}

#[test]
fn insert_swap_new_surfaces_persistence_failure_as_err() {
    let (_dir, db, path) = open_db();
    sabotage(&path, "DROP TABLE lnplus_swaps;");
    let result = db.insert_swap_new(&SwapRow::new("s1", "applied", 1, 1, 1));
    assert!(
        result.is_err(),
        "a failed insert must be an Err acknowledgement, not a swallowed warn-log"
    );
}

// ------------------------------------------------------ CAS transitions

#[test]
fn cas_swap_applies_only_from_expected_status() {
    let (_dir, db, _path) = open_db();
    seeded(&db, "s1", "applied");

    let applied = db
        .cas_swap(
            "s1",
            &["applied"],
            &SwapPatch::default().status("opening").outcome("intent"),
        )
        .expect("cas acked");
    assert_eq!(applied, CasOutcome::Applied);
    let row = db.get_swap("s1").expect("row");
    assert_eq!(row.status, "opening");
    assert_eq!(row.outcome.as_deref(), Some("intent"));

    // Same expectation again — the row has moved on; the patch must NOT
    // apply and the conflict must name the actual status.
    let conflict = db
        .cas_swap(
            "s1",
            &["applied"],
            &SwapPatch::default().status("failed").outcome("late writer"),
        )
        .expect("cas acked");
    assert_eq!(
        conflict,
        CasOutcome::Conflict {
            actual: Some("opening".to_string())
        }
    );
    let row = db.get_swap("s1").expect("row");
    assert_eq!(row.status, "opening", "conflicting CAS must not write");
    assert_eq!(
        row.outcome.as_deref(),
        Some("intent"),
        "conflicting CAS must not write any patch field"
    );
}

#[test]
fn cas_swap_accepts_any_of_the_expected_statuses() {
    let (_dir, db, _path) = open_db();
    seeded(&db, "s1", "opening");
    let outcome = db
        .cas_swap(
            "s1",
            &["applied", "opening"],
            &SwapPatch::default().status("failed"),
        )
        .expect("cas acked");
    assert_eq!(outcome, CasOutcome::Applied);
    assert_eq!(db.get_swap("s1").unwrap().status, "failed");
}

#[test]
fn cas_swap_on_missing_row_is_conflict_with_no_actual() {
    let (_dir, db, _path) = open_db();
    let outcome = db
        .cas_swap(
            "ghost",
            &["applied"],
            &SwapPatch::default().status("failed"),
        )
        .expect("cas acked");
    assert_eq!(outcome, CasOutcome::Conflict { actual: None });
}

#[test]
fn cas_swap_surfaces_persistence_failure_as_err() {
    let (_dir, db, path) = open_db();
    seeded(&db, "s1", "applied");
    sabotage(&path, "DROP TABLE lnplus_swaps;");
    assert!(db
        .cas_swap("s1", &["applied"], &SwapPatch::default().status("opening"))
        .is_err());
}

// ------------------------------------- atomic compound terminal + breaker

fn deadline_spec<'a>(sid: &'a str) -> TerminalizeSpec<'a> {
    TerminalizeSpec {
        swap_id: sid,
        expected_statuses: &["applied", "opening"],
        require_null_funding_txid: true,
    }
}

fn failed_patch() -> SwapPatch {
    SwapPatch::default()
        .status("failed")
        .outcome("missed open deadline")
}

fn miss_cause(sid: &str) -> BreakerCause {
    BreakerCause::MissedOpenDeadline {
        swap_id: sid.to_string(),
    }
}

#[test]
fn terminalize_and_trip_applies_row_and_breaker_together() {
    let (_dir, db, _path) = open_db();
    seeded(&db, "s1", "opening");

    let outcome = db
        .terminalize_and_trip(&deadline_spec("s1"), &failed_patch(), miss_cause("s1"), 500)
        .expect("compound acked");
    assert_eq!(
        outcome,
        CompoundOutcome::Terminalized {
            breaker: TripAck::NewTrip
        }
    );
    assert_eq!(db.get_swap("s1").unwrap().status, "failed");
    let state = db
        .get_breaker()
        .expect("breaker readable")
        .expect("breaker tripped");
    assert_eq!(state.tripped_at, 500);
    assert_eq!(state.cause, miss_cause("s1"));
}

#[test]
fn terminalize_and_trip_preserves_first_cause_b10() {
    let (_dir, db, _path) = open_db();
    seeded(&db, "s1", "opening");
    let first = BreakerState {
        tripped_at: 100,
        cause: BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: "other".to_string(),
        },
    };
    db.set_breaker(&first).expect("pre-trip");

    let outcome = db
        .terminalize_and_trip(&deadline_spec("s1"), &failed_patch(), miss_cause("s1"), 500)
        .expect("compound acked");
    assert_eq!(
        outcome,
        CompoundOutcome::Terminalized {
            breaker: TripAck::AlreadyTripped
        }
    );
    // Row still terminalizes (defect #4 fix) …
    assert_eq!(db.get_swap("s1").unwrap().status, "failed");
    // … but the FIRST cause is preserved untouched.
    assert_eq!(db.get_breaker().unwrap().unwrap(), first);
}

#[test]
fn terminalize_and_trip_conflicts_on_unexpected_status_without_tripping() {
    let (_dir, db, _path) = open_db();
    seeded(&db, "s1", "opened");

    let outcome = db
        .terminalize_and_trip(&deadline_spec("s1"), &failed_patch(), miss_cause("s1"), 500)
        .expect("compound acked");
    assert_eq!(
        outcome,
        CompoundOutcome::Conflict {
            actual: Some("opened".to_string())
        }
    );
    assert_eq!(db.get_swap("s1").unwrap().status, "opened");
    assert_eq!(
        db.get_breaker().unwrap(),
        None,
        "a conflicted compound must not trip the breaker"
    );
}

#[test]
fn terminalize_and_trip_conflicts_when_funding_txid_present() {
    // The deadline-miss guard: a row that DID get funded (txid recorded)
    // must be left alone even if its status still says opening.
    let (_dir, db, _path) = open_db();
    let row =
        SwapRow::new("s1", "opening", 2_000_000, 6, 1_000).with_channel_funding_txid("txid-funded");
    assert_eq!(db.insert_swap_new(&row).unwrap(), InsertOutcome::Inserted);

    let outcome = db
        .terminalize_and_trip(&deadline_spec("s1"), &failed_patch(), miss_cause("s1"), 500)
        .expect("compound acked");
    assert_eq!(
        outcome,
        CompoundOutcome::Conflict {
            actual: Some("opening".to_string())
        }
    );
    assert_eq!(db.get_swap("s1").unwrap().status, "opening");
    assert_eq!(db.get_breaker().unwrap(), None);
}

#[test]
fn terminalize_and_trip_rolls_back_the_row_when_the_breaker_write_fails() {
    // THE atomicity proof: a real persistence fault on EXACTLY the breaker
    // WRITE (reads unaffected — a RAISE trigger on INSERT) must roll the
    // row's terminalization back too — all-or-nothing, acknowledged as
    // Err. A whole-table fault would trip the compound's breaker READ
    // first and prove nothing about the write half.
    let (_dir, db, path) = open_db();
    seeded(&db, "s1", "opening");
    sabotage(
        &path,
        "CREATE TRIGGER sabotage_breaker_write BEFORE INSERT ON config_overrides \
         BEGIN SELECT RAISE(ABORT, 'sabotaged breaker write'); END;",
    );

    let result =
        db.terminalize_and_trip(&deadline_spec("s1"), &failed_patch(), miss_cause("s1"), 500);
    assert!(result.is_err(), "mid-compound fault must be acknowledged");
    assert_eq!(
        db.get_swap("s1").unwrap().status,
        "opening",
        "the row terminalization must have rolled back with the failed breaker write"
    );
}

#[test]
fn terminalize_and_trip_rolls_back_when_the_whole_store_half_is_gone() {
    // Companion coverage: the coarser whole-table fault (read AND write
    // gone) is also acknowledged with the row rolled back.
    let (_dir, db, path) = open_db();
    seeded(&db, "s1", "opening");
    sabotage(&path, "DROP TABLE config_overrides;");

    let result =
        db.terminalize_and_trip(&deadline_spec("s1"), &failed_patch(), miss_cause("s1"), 500);
    assert!(result.is_err());
    assert_eq!(db.get_swap("s1").unwrap().status, "opening");
}

// --------------------------------------------- fallible acked plain writes

#[test]
fn set_breaker_and_clear_breaker_surface_failures() {
    let (_dir, db, path) = open_db();
    sabotage(&path, "DROP TABLE config_overrides;");
    let state = BreakerState {
        tripped_at: 1,
        cause: miss_cause("s1"),
    };
    assert!(db.set_breaker(&state).is_err());
    assert!(db.clear_breaker().is_err());
}

#[test]
fn bump_peer_and_planner_action_writes_surface_failures() {
    let (_dir, db, path) = open_db();
    sabotage(
        &path,
        "DROP TABLE lnplus_peers; DROP TABLE planner_actions;",
    );
    assert!(db.bump_peer("02aa", false, None).is_err());
    assert!(db
        .record_planner_action(&revops_lnplus::ports::PlannerActionRequest {
            action_type: "swap_apply",
            peer_id: "02aa".to_string(),
            amount_sats: None,
            estimated_cost_sats: None,
            reason: "r".to_string(),
            metadata: None,
        })
        .is_err());
    assert!(db.update_planner_action(1, "completed").is_err());
}

#[test]
fn config_override_writes_surface_failures() {
    let (_dir, db, path) = open_db();
    sabotage(&path, "DROP TABLE config_overrides;");
    assert!(db.set_config_override("k", "v").is_err());
    assert!(db.delete_config_override("k").is_err());
}

#[test]
fn prune_terminal_surfaces_failures() {
    let (_dir, db, path) = open_db();
    sabotage(&path, "DROP TABLE lnplus_swaps;");
    assert!(db.prune_terminal(180, 1_000_000).is_err());
}

// --------------------------------------------------- fail-closed breaker read

#[test]
fn get_breaker_malformed_value_is_an_error_not_untripped() {
    // Fail CLOSED: a persisted breaker value this crate cannot decode is
    // evidence of corruption (this store is Rust-only), and treating it
    // as "no breaker" would let execution proceed past a latched trip.
    let (_dir, db, _path) = open_db();
    db.set_config_override(revops_lnplus::breaker::BREAKER_KEY, "not json at all")
        .expect("write garbage");
    assert!(db.get_breaker().is_err());

    // Python's legacy plain-string format is equally undecodable here and
    // equally corruption from this store's perspective.
    db.set_config_override(
        revops_lnplus::breaker::BREAKER_KEY,
        "1700000000: opening ghost swap-42",
    )
    .expect("write legacy format");
    assert!(db.get_breaker().is_err());
}

// ---------------------------------------- atomic breaker trip/clear CAS

#[test]
fn trip_if_untripped_first_wins_and_second_preserves_first_cause() {
    // B10 at the STORE level: the trip is insert-if-absent in one
    // transaction, so a second trip with a different cause can never
    // clobber the first no matter how callers interleave.
    let (_dir, db, _path) = open_db();
    let first = BreakerState {
        tripped_at: 100,
        cause: miss_cause("s1"),
    };
    assert_eq!(
        db.trip_breaker_if_untripped(&first).expect("trip acked"),
        TripAck::NewTrip
    );
    let second = BreakerState {
        tripped_at: 200,
        cause: BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: "s2".to_string(),
        },
    };
    assert_eq!(
        db.trip_breaker_if_untripped(&second).expect("trip acked"),
        TripAck::AlreadyTripped
    );
    assert_eq!(
        db.get_breaker().unwrap().unwrap(),
        first,
        "the first cause must be byte-identical after a suppressed second trip"
    );
}

#[test]
fn trip_if_untripped_over_malformed_value_fails_closed() {
    let (_dir, db, _path) = open_db();
    db.set_config_override(revops_lnplus::breaker::BREAKER_KEY, "garbage")
        .expect("write garbage");
    let state = BreakerState {
        tripped_at: 1,
        cause: miss_cause("s1"),
    };
    assert!(
        db.trip_breaker_if_untripped(&state).is_err(),
        "an undecodable latched value must be an error, not silently overwritten or ignored"
    );
}

#[test]
fn clear_if_cause_removes_only_the_exact_verified_cause() {
    let (_dir, db, _path) = open_db();
    let latched = BreakerState {
        tripped_at: 100,
        cause: BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: "s7".to_string(),
        },
    };
    db.set_breaker(&latched).expect("latch");

    // A DIFFERENT cause (what a stale auto-clear might present) must not
    // clear anything.
    assert!(!db
        .clear_breaker_if_cause(&BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: "other".to_string(),
        })
        .expect("clear acked"));
    assert_eq!(db.get_breaker().unwrap().unwrap(), latched);

    // The exact cause clears.
    assert!(db
        .clear_breaker_if_cause(&latched.cause)
        .expect("clear acked"));
    assert_eq!(db.get_breaker().unwrap(), None);

    // Idempotent second clear: nothing latched, nothing cleared.
    assert!(!db.clear_breaker_if_cause(&latched.cause).expect("acked"));
}

#[test]
fn clear_if_cause_over_malformed_value_fails_closed() {
    let (_dir, db, _path) = open_db();
    db.set_config_override(revops_lnplus::breaker::BREAKER_KEY, "garbage")
        .expect("write garbage");
    assert!(db.clear_breaker_if_cause(&miss_cause("s1")).is_err());
}

// -------------------------------- single-connection budget-rail boundary

#[test]
fn budget_rail_runs_on_the_stores_single_connection() {
    // Task 61 4A architecture gate: reserve → settle roundtrips through
    // the SAME store (no second BudgetDb connection), and a rail-table
    // fault surfaces as an acknowledged Err on that store.
    let (_dir, db, path) = open_db();
    let req = revops_lnplus::ports::ReserveSpendRequest {
        reservation_id: "resv-arch-1".to_string(),
        amount_sats: 500,
        category: "channel_open",
        subcategory: "lnplus_swap",
        metadata: Default::default(),
        effective_budget_sats: Some(1_000),
        since_timestamp: Some(0),
    };
    assert!(db.reserve_spend(&req).expect("reserve acked"));
    assert!(db
        .mark_spend_reservation_spent("resv-arch-1", 400, "lnplus_swaps")
        .expect("settle acked"));

    sabotage(&path, "DROP TABLE spend_reservations;");
    assert!(db.reserve_spend(&req).is_err());
    assert!(db
        .mark_spend_reservation_spent("resv-arch-1", 400, "lnplus_swaps")
        .is_err());
}

#[test]
fn get_breaker_roundtrips_a_healthy_state() {
    // Control: the fail-closed read still decodes what set_breaker wrote.
    let (_dir, db, _path) = open_db();
    assert_eq!(db.get_breaker().expect("readable"), None);
    let state = BreakerState {
        tripped_at: 77,
        cause: BreakerCause::LnPlusOutage {
            detail: "timeout".to_string(),
        },
    };
    db.set_breaker(&state).expect("write");
    assert_eq!(db.get_breaker().expect("readable"), Some(state));
    db.clear_breaker().expect("clear");
    assert_eq!(db.get_breaker().expect("readable"), None);
}
