//! `reconcile.rs` end-to-end (through the DB/API fakes), including
//! **defect #5**'s integration-level proof: a ghost trip that clears once
//! backfill adopts the row, contrasted with a divergence class that never
//! auto-clears no matter how many passes run clean afterward.

mod common;

use common::*;
use revops_lnplus::breaker::{BreakerCause, BreakerState};
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::ports::LnPlusDb;
use revops_lnplus::reconcile::{reconcile, RECONCILE_GRACE_SECONDS};
use revops_lnplus::types::{MySwapEntry, MySwaps};

fn entry(id: &str) -> MySwapEntry {
    MySwapEntry {
        id: id.to_string(),
        ..Default::default()
    }
}

// ------------------------------------------------------------------- B4

#[test]
fn b4_vanished_applied_row_becomes_cancelled_remote_not_a_trip() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    db.insert(SwapRow::new("1", "applied", 100_000, 6, 0));
    let now = 10_000; // far past the grace window
    let my = MySwaps::default();
    let ok = reconcile(&my, &db, &api, &logger, now);
    assert!(ok, "B4 must not trip the breaker");
    assert_eq!(db.get_swap("1").unwrap().status, "cancelled_remote");
    assert!(db.get_breaker().is_none());
}

#[test]
fn control_within_grace_window_applied_row_is_not_touched_b9() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    db.insert(SwapRow::new("1", "applied", 100_000, 6, 0));
    let now = RECONCILE_GRACE_SECONDS - 1; // still inside the grace window
    let my = MySwaps::default();
    let ok = reconcile(&my, &db, &api, &logger, now);
    assert!(ok);
    assert_eq!(
        db.get_swap("1").unwrap().status,
        "applied",
        "grace window must skip this row entirely"
    );
}

#[test]
fn applied_but_ln_plus_completed_trips_not_cancels() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    db.insert(SwapRow::new("1", "applied", 100_000, 6, 0));
    let now = 10_000;
    let my = MySwaps {
        completed: vec![entry("1")],
        ..Default::default()
    };
    let ok = reconcile(&my, &db, &api, &logger, now);
    assert!(!ok);
    // Must stay a LIVE contract row (still "applied"), not be
    // mismarked cancelled_remote (which would skip activation entirely).
    assert_eq!(db.get_swap("1").unwrap().status, "applied");
    assert!(db.get_breaker().is_some());
}

// ---------------------------------------------------------- defect #5 (I1)

#[test]
fn opening_ghost_with_no_local_record_trips_breaker() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        opening: vec![entry("42")],
        ..Default::default()
    };
    let ok = reconcile(&my, &db, &api, &logger, 1000);
    assert!(!ok);
    let state = db.get_breaker().expect("breaker must be tripped");
    assert_eq!(
        state.cause,
        BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: "42".to_string()
        }
    );
}

#[test]
fn defect5_ghost_trip_auto_clears_once_backfill_adopts_the_row() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        opening: vec![entry("42")],
        ..Default::default()
    };
    // Pass 1: trips (matches the 8-day incident's trigger exactly).
    assert!(!reconcile(&my, &db, &api, &logger, 1000));
    assert!(db.get_breaker().is_some());

    // Simulate backfill adopting the row (what `revenue-lnplus-backfill`
    // or the automatic choke point does).
    db.insert(
        SwapRow::new("42", "opening", 500_000, 6, 1000)
            .with_outbound_peer(pubkey(1))
            .with_deadline_at(2000),
    );

    // Pass 2: the SAME `my` (LN+ still lists it under opening -- that's
    // expected, LN+ doesn't know about our local adoption) but now there
    // IS a local row -- the ghost condition no longer reproduces.
    let ok = reconcile(&my, &db, &api, &logger, 1500);
    assert!(ok, "the resolved ghost must not keep failing reconcile");
    assert!(
        db.get_breaker().is_none(),
        "defect #5 fix: reverifiable cause must auto-clear once resolved"
    );
    assert!(logger.contains("auto-cleared"));
}

#[test]
fn control_ghost_trip_stays_latched_while_still_a_ghost() {
    // CONTROL: same starting shape as the defect #5 test above, but NO
    // backfill/adoption happens between passes -- the breaker MUST stay
    // tripped. Proves the auto-clear test isn't vacuously true (i.e. it
    // isn't clearing unconditionally on the second call).
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        opening: vec![entry("42")],
        ..Default::default()
    };
    assert!(!reconcile(&my, &db, &api, &logger, 1000));
    assert!(db.get_breaker().is_some());
    let ok = reconcile(&my, &db, &api, &logger, 2000);
    assert!(!ok);
    assert!(
        db.get_breaker().is_some(),
        "ghost never resolved -- must stay latched"
    );
}

#[test]
fn missed_deadline_cause_never_auto_clears_even_after_reconcile_runs_clean() {
    // NEVER-auto-clear class: manually trip with a MissedOpenDeadline
    // cause (this is what open.rs does), then run several clean
    // reconcile passes with nothing divergent -- the breaker must stay
    // tripped until an operator clears it.
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    db.set_breaker(&BreakerState {
        tripped_at: 500,
        cause: BreakerCause::MissedOpenDeadline {
            swap_id: "9".to_string(),
        },
    });
    let my = MySwaps::default();
    for now in [1000, 2000, 3000] {
        reconcile(&my, &db, &api, &logger, now);
    }
    assert!(
        db.get_breaker().is_some(),
        "MissedOpenDeadline must never auto-clear"
    );
}

// ------------------------------------------------------------------- B5(b)

#[test]
fn b5b_stale_pending_ghost_matching_terminal_local_row_is_cleaned_up_not_tripped() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    db.insert(SwapRow::new("7", "withdrawn", 100_000, 6, 0));
    let my = MySwaps {
        pending: vec![entry("7")],
        ..Default::default()
    };
    let ok = reconcile(&my, &db, &api, &logger, 1000);
    assert!(ok, "B5(b) cleanup must not trip the breaker");
    assert_eq!(
        api.delete_application_calls.borrow().as_slice(),
        &["7".to_string()]
    );
    assert!(db.get_breaker().is_none());
}

#[test]
fn control_pending_ghost_with_no_local_row_at_all_still_trips() {
    // CONTROL for B5(b): a pending ghost with NO local row (not even a
    // terminal one) is a genuine untracked commitment and must trip.
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        pending: vec![entry("8")],
        ..Default::default()
    };
    let ok = reconcile(&my, &db, &api, &logger, 1000);
    assert!(!ok);
    assert!(api.delete_application_calls.borrow().is_empty());
    let state = db.get_breaker().unwrap();
    assert_eq!(
        state.cause,
        BreakerCause::PendingGhostNoLocalRecord {
            swap_id: "8".to_string()
        }
    );
}

#[test]
fn opening_opened_row_divergent_from_remote_trips() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    db.insert(SwapRow::new("3", "opened", 100_000, 6, 0).with_outbound_peer(pubkey(1)));
    let my = MySwaps::default(); // LN+ shows neither opening nor completed for "3"
    let ok = reconcile(&my, &db, &api, &logger, 1000);
    assert!(!ok);
    assert!(db.get_breaker().is_some());
}

#[test]
fn b10_first_cause_preserved_across_multiple_new_divergences() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        opening: vec![entry("a")],
        pending: vec![entry("b")],
        ..Default::default()
    };
    reconcile(&my, &db, &api, &logger, 1000);
    let state = db.get_breaker().unwrap();
    // Whichever ran first in iteration order, the SECOND divergence must
    // not overwrite it.
    assert!(matches!(
        state.cause,
        BreakerCause::OpeningGhostNoLocalRecord { .. }
    ));
}
