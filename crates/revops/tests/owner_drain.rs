//! R68-4 (RED): owner drain and join acknowledgements.
//!
//! R68-1 pinned the shutdown ORDER. Each step there answers
//! `Result<(), String>`, and `Ok(())` is taken as proof the owners
//! drained -- which is the `.ok()`-to-null shape Task 68 exists to
//! remove. Sending a stop message into a channel returns `Ok` whether or
//! not anyone was listening.
//!
//! Four states a single `Ok(())` cannot tell apart:
//!
//!   * the owner does not run in this process at all (the action owners
//!     are `Option` in `State`, and a passive-observer boot legitimately
//!     has none of them),
//!   * the owner acknowledged and had nothing left queued,
//!   * the owner was ALREADY GONE, so the stop was never delivered and
//!     whatever it had queued was never processed,
//!   * the owner took the stop and never answered.
//!
//! Only the second is clean. And draining is not joining: an owner can
//! acknowledge the stop and then never end, so a join that never happened
//! must not be reported as one -- nor may an owner be joined it never
//! drained, which means the task ended with work still queued.

use std::time::Duration;

use revops::lifecycle::{
    drain_is_clean, join_is_clean, shutdown_acks_are_consistent, DrainAck, DrainLedger,
    DrainRefusal, JoinAck, JoinRefusal, Owner, OwnerClass,
};

/// Every owner acknowledged cleanly, nothing left queued.
fn all_drained() -> DrainLedger {
    let ledger = DrainLedger::default();
    for owner in Owner::ALL {
        ledger.record(owner, DrainAck::Drained { pending_at_stop: 0 });
    }
    ledger
}

fn all_joined() -> DrainLedger {
    let ledger = DrainLedger::default();
    for owner in Owner::ALL {
        ledger.record_join(owner, JoinAck::Joined);
    }
    ledger
}

// =====================================================================
// the roster
// =====================================================================

/// Derived from `main.rs`' `State`: the serialized owners that hold their
/// own retained task or thread. A missing retained entry is an owner
/// shutdown never asks about.
#[test]
fn the_roster_is_exactly_the_owners_the_plugin_spawns() {
    assert_eq!(
        Owner::ALL,
        [
            Owner::ProductionDb,
            Owner::ObserverStore,
            Owner::FeeScheduler,
            Owner::Rebalance,
        ]
    );
    for owner in Owner::ALL {
        assert!(!owner.as_str().is_empty());
    }
}

/// The split R68-1's phase order rests on: action owners drain BEFORE
/// observer owners, because an action still in flight can produce
/// observations the observer must still record.
#[test]
fn every_owner_is_classified_as_action_or_observer() {
    assert_eq!(Owner::ProductionDb.class(), OwnerClass::Observer);
    assert_eq!(Owner::ObserverStore.class(), OwnerClass::Observer);
    assert_eq!(Owner::FeeScheduler.class(), OwnerClass::Action);
    assert_eq!(Owner::Rebalance.class(), OwnerClass::Action);
}

#[test]
fn the_roster_has_no_duplicates() {
    let unique: std::collections::BTreeSet<_> = Owner::ALL.iter().collect();
    assert_eq!(unique.len(), Owner::ALL.len());
}

/// Two owners sharing a ledger slot would silently overwrite each other:
/// one owner's fault would be reported under another's name, and the
/// overwritten owner would read as never having reported at all. Every
/// owner gets a DISTINCT value and must read back its own.
#[test]
fn each_owner_records_into_its_own_slot() {
    let ledger = DrainLedger::default();
    for (index, owner) in Owner::ALL.into_iter().enumerate() {
        ledger.record(
            owner,
            DrainAck::Drained {
                pending_at_stop: index as u64,
            },
        );
    }
    for (index, owner) in Owner::ALL.into_iter().enumerate() {
        assert_eq!(
            ledger.drain_ack(owner),
            Some(DrainAck::Drained {
                pending_at_stop: index as u64
            }),
            "{owner:?} must not share a slot"
        );
    }
}

// =====================================================================
// drain acknowledgements
// =====================================================================

#[test]
fn a_fully_acknowledged_drain_is_clean() {
    assert!(drain_is_clean(&all_drained()).is_ok());
}

/// The whole point. An owner nobody reported on is an owner shutdown
/// never asked about, and an empty ledger must never read as success.
#[test]
fn an_owner_that_reported_nothing_refuses() {
    let ledger = DrainLedger::default();
    let refusal = drain_is_clean(&ledger).expect_err("silence is not an acknowledgement");
    assert!(matches!(refusal, DrainRefusal::Unreported { .. }));

    // ...and one MISSING entry among six good ones refuses just the same.
    let ledger = all_drained();
    ledger.clear_for_tests(Owner::Rebalance);
    match drain_is_clean(&ledger).expect_err("one unreported owner is enough") {
        DrainRefusal::Unreported { owner } => assert_eq!(owner, Owner::Rebalance),
        other => panic!("expected Unreported, got {other:?}"),
    }
}

/// A passive-observer boot has no action owners at all. That absence is
/// legitimate -- but it has to be DECLARED, not inferred from silence.
#[test]
fn an_owner_that_does_not_run_in_this_process_is_declared_not_assumed() {
    let ledger = DrainLedger::default();
    for owner in Owner::ALL {
        match owner.class() {
            OwnerClass::Action => ledger.record(owner, DrainAck::NotSpawned),
            OwnerClass::Observer => ledger.record(owner, DrainAck::Drained { pending_at_stop: 0 }),
        }
    }
    assert!(drain_is_clean(&ledger).is_ok());
}

/// An owner that was already gone never received the stop, so whatever it
/// had queued was never processed. Reporting that as drained is how a
/// shutdown says "clean" over lost work.
#[test]
fn an_unreachable_owner_is_not_a_drained_one() {
    let ledger = all_drained();
    ledger.record(
        Owner::Rebalance,
        DrainAck::Unreachable {
            detail: "owner task already ended".to_string(),
        },
    );
    match drain_is_clean(&ledger).expect_err("an owner that never got the stop is not drained") {
        DrainRefusal::Unreachable { owner, detail } => {
            assert_eq!(owner, Owner::Rebalance);
            assert!(detail.contains("already ended"), "{detail}");
        }
        other => panic!("expected Unreachable, got {other:?}"),
    }
}

#[test]
fn an_owner_that_took_the_stop_and_never_answered_refuses() {
    let ledger = all_drained();
    ledger.record(
        Owner::FeeScheduler,
        DrainAck::NoAck {
            waited: Duration::from_secs(3),
        },
    );
    match drain_is_clean(&ledger).expect_err("no answer is not an answer") {
        DrainRefusal::NoAck { owner, waited } => {
            assert_eq!(owner, Owner::FeeScheduler);
            assert_eq!(waited, Duration::from_secs(3));
        }
        other => panic!("expected NoAck, got {other:?}"),
    }
}

/// "I stopped" is not "I finished". An owner that acknowledges the stop
/// with work still queued dropped that work on the floor.
#[test]
fn an_owner_that_stopped_with_work_still_queued_refuses() {
    let ledger = all_drained();
    ledger.record(
        Owner::FeeScheduler,
        DrainAck::Drained { pending_at_stop: 7 },
    );
    match drain_is_clean(&ledger).expect_err("stopping is not finishing") {
        DrainRefusal::LeftWork { owner, pending } => {
            assert_eq!(owner, Owner::FeeScheduler);
            assert_eq!(pending, 7);
        }
        other => panic!("expected LeftWork, got {other:?}"),
    }
}

/// Every owner is checked, not just the first -- otherwise the verdict
/// depends on iteration order and a fault on the last owner passes.
#[test]
fn a_fault_on_any_owner_refuses_including_the_last() {
    for owner in Owner::ALL {
        let ledger = all_drained();
        ledger.record(
            owner,
            DrainAck::Unreachable {
                detail: "gone".to_string(),
            },
        );
        assert!(
            drain_is_clean(&ledger).is_err(),
            "{owner:?} must refuse when unreachable"
        );
    }
}

// =====================================================================
// join acknowledgements
// =====================================================================

#[test]
fn a_fully_acknowledged_join_is_clean() {
    assert!(join_is_clean(&all_joined()).is_ok());
}

#[test]
fn an_owner_that_never_reported_a_join_refuses() {
    let ledger = all_joined();
    ledger.clear_join_for_tests(Owner::ObserverStore);
    match join_is_clean(&ledger).expect_err("silence is not a join") {
        JoinRefusal::Unreported { owner } => assert_eq!(owner, Owner::ObserverStore),
        other => panic!("expected Unreported, got {other:?}"),
    }
}

#[test]
fn an_owner_that_timed_out_joining_refuses() {
    let ledger = all_joined();
    ledger.record_join(
        Owner::Rebalance,
        JoinAck::Timeout {
            waited: Duration::from_secs(10),
        },
    );
    match join_is_clean(&ledger).expect_err("a wedged owner is not a joined one") {
        JoinRefusal::Timeout { owner, waited } => {
            assert_eq!(owner, Owner::Rebalance);
            assert_eq!(waited, Duration::from_secs(10));
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
}

/// R68-1 established that a panicking owner is a LOST owner, not a clean
/// one. The join ledger keeps that distinction rather than folding a
/// panic into "the task is no longer running, so it ended".
#[test]
fn a_panicking_owner_is_not_a_joined_one() {
    let ledger = all_joined();
    ledger.record_join(
        Owner::FeeScheduler,
        JoinAck::Panicked {
            detail: "panicked at 'unwrap on None'".to_string(),
        },
    );
    match join_is_clean(&ledger).expect_err("a panic is a lost owner") {
        JoinRefusal::Panicked { owner, detail } => {
            assert_eq!(owner, Owner::FeeScheduler);
            assert!(detail.contains("unwrap"), "{detail}");
        }
        other => panic!("expected Panicked, got {other:?}"),
    }
}

#[test]
fn an_owner_that_does_not_run_never_needs_joining() {
    let ledger = DrainLedger::default();
    for owner in Owner::ALL {
        ledger.record_join(owner, JoinAck::NotSpawned);
    }
    assert!(join_is_clean(&ledger).is_ok());
}

// =====================================================================
// drain and join must agree
// =====================================================================

/// The cross-check neither ledger can make alone: an owner reported
/// JOINED that was never reported DRAINED ended while work was still
/// queued. Both halves look clean in isolation.
#[test]
fn an_owner_joined_without_draining_refuses() {
    let ledger = all_drained();
    for owner in Owner::ALL {
        ledger.record_join(owner, JoinAck::Joined);
    }
    assert!(shutdown_acks_are_consistent(&ledger).is_ok());

    ledger.clear_for_tests(Owner::ObserverStore);
    assert!(
        shutdown_acks_are_consistent(&ledger).is_err(),
        "an owner cannot be joined it never drained"
    );
}

/// The inverse is legitimate and must NOT refuse: an owner declared
/// absent is absent in both ledgers.
#[test]
fn an_absent_owner_is_consistent_across_both_ledgers() {
    let ledger = DrainLedger::default();
    for owner in Owner::ALL {
        ledger.record(owner, DrainAck::NotSpawned);
        ledger.record_join(owner, JoinAck::NotSpawned);
    }
    assert!(shutdown_acks_are_consistent(&ledger).is_ok());
}

/// The mirror image, and the sharper one: an owner declared ABSENT at
/// drain time that then reports a join plainly did run. Declaring absence
/// is how a passive-observer boot passes the drain gate, so an absence
/// that can still be joined is a way to pass it while an owner is live.
#[test]
fn an_owner_declared_absent_cannot_then_report_a_join() {
    let ledger = DrainLedger::default();
    for owner in Owner::ALL {
        ledger.record(owner, DrainAck::NotSpawned);
        ledger.record_join(owner, JoinAck::NotSpawned);
    }
    assert!(shutdown_acks_are_consistent(&ledger).is_ok());

    ledger.record_join(Owner::Rebalance, JoinAck::Joined);
    match shutdown_acks_are_consistent(&ledger)
        .expect_err("an owner declared absent cannot have been joined")
    {
        JoinRefusal::JoinedWithoutDraining { owner } => assert_eq!(owner, Owner::Rebalance),
        other => panic!("expected JoinedWithoutDraining, got {other:?}"),
    }
}

/// An owner that drained but is declared absent at join time is a
/// bookkeeping lie -- it plainly existed a moment earlier.
#[test]
fn an_owner_that_drained_cannot_be_absent_at_join_time() {
    let ledger = all_drained();
    for owner in Owner::ALL {
        ledger.record_join(owner, JoinAck::Joined);
    }
    ledger.record_join(Owner::Rebalance, JoinAck::NotSpawned);
    assert!(
        shutdown_acks_are_consistent(&ledger).is_err(),
        "an owner that acknowledged a drain existed"
    );
}

#[test]
fn every_refusal_carries_a_distinct_actionable_code() {
    let drain_codes = [
        DrainRefusal::Unreported {
            owner: Owner::Rebalance,
        }
        .code(),
        DrainRefusal::Unreachable {
            owner: Owner::Rebalance,
            detail: String::new(),
        }
        .code(),
        DrainRefusal::NoAck {
            owner: Owner::Rebalance,
            waited: Duration::ZERO,
        }
        .code(),
        DrainRefusal::LeftWork {
            owner: Owner::Rebalance,
            pending: 1,
        }
        .code(),
    ];
    let join_codes = [
        JoinRefusal::Unreported {
            owner: Owner::Rebalance,
        }
        .code(),
        JoinRefusal::Timeout {
            owner: Owner::Rebalance,
            waited: Duration::ZERO,
        }
        .code(),
        JoinRefusal::Panicked {
            owner: Owner::Rebalance,
            detail: String::new(),
        }
        .code(),
        JoinRefusal::JoinedWithoutDraining {
            owner: Owner::Rebalance,
        }
        .code(),
    ];
    let all: Vec<&str> = drain_codes
        .iter()
        .chain(join_codes.iter())
        .copied()
        .collect();
    let unique: std::collections::BTreeSet<_> = all.iter().collect();
    assert_eq!(unique.len(), all.len(), "codes must not collide: {all:?}");
}
