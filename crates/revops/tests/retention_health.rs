//! R68-3 (RED): retention health.
//!
//! Task 59 gave this node a bounded Class-W retention sweep and one red
//! counter, `retention_failures`. That counter answers "did a sweep that
//! ran fail?" -- and nothing answers the two questions that actually
//! precede disk exhaustion:
//!
//!   1. Are sweeps running AT ALL? A sweep is scheduled only off a
//!      SUCCESSFUL SCHEDULED CYCLE COMMIT (`fee_scheduler.rs`, "retention
//!      rides only successful scheduled commits"). If cycles stop
//!      committing, retention silently stops with `failures == 0` -- which
//!      reads as healthy.
//!   2. Are sweeps KEEPING UP? Each sweep is capped at
//!      `RETENTION_MAX_BATCHES_PER_SWEEP` and reports `truncated` when it
//!      hits that cap with work still pending. The scheduler currently
//!      keeps only `next_cursor` from the report and discards `truncated`,
//!      so retention that is permanently behind is indistinguishable from
//!      retention that finished the job.
//!
//! There is no Python parity contract here: Python has no automated
//! retention at all (its only mention is an operator-run `DELETE FROM`
//! hint at cl-revenue-ops.py:7447). The obligation is this node's own --
//! it has already run itself out of disk once.

use revops::lifecycle::{
    retention_is_healthy, RetentionObservation, RetentionRefusal, RETENTION_MAX_SILENCE_SECONDS,
    RETENTION_TRUNCATION_TOLERANCE_SECONDS,
};

const BOOT: i64 = 1_800_000_000;

/// A node that has swept recently and cleanly.
fn healthy(now: i64) -> RetentionObservation {
    RetentionObservation {
        failures: 0,
        sweeps_completed: 12,
        last_sweep_at: Some(now - 60),
        truncated_since: None,
    }
}

// =====================================================================
// silence: is retention running at all?
// =====================================================================

#[test]
fn a_fresh_node_that_has_not_swept_yet_is_healthy() {
    // Startup does not sweep; the first sweep waits for the first
    // committing cycle. Refusing immediately would make every boot red.
    let now = BOOT + 60;
    assert!(retention_is_healthy(&RetentionObservation::default(), BOOT, now).is_ok());
}

#[test]
fn a_node_that_has_never_swept_refuses_once_the_grace_window_passes() {
    let now = BOOT + RETENTION_MAX_SILENCE_SECONDS + 1;
    let refusal = retention_is_healthy(&RetentionObservation::default(), BOOT, now)
        .expect_err("silence past the window is not health");
    match refusal {
        RetentionRefusal::NeverSwept { silent_seconds } => {
            assert_eq!(silent_seconds, RETENTION_MAX_SILENCE_SECONDS + 1);
        }
        other => panic!("expected NeverSwept, got {other:?}"),
    }
}

#[test]
fn the_silence_window_boundary_is_inclusive() {
    // Exactly at the bound is still healthy; one second past is not. A
    // gate whose boundary is undefined refuses at random on a busy node.
    let at = BOOT + RETENTION_MAX_SILENCE_SECONDS;
    assert!(retention_is_healthy(&RetentionObservation::default(), BOOT, at).is_ok());
    assert!(retention_is_healthy(&RetentionObservation::default(), BOOT, at + 1).is_err());
}

/// The failure the counter cannot see. Retention worked one hundred times
/// and then stopped -- `failures` is still 0, `sweeps_completed` is still
/// large, and the store has been growing ever since.
#[test]
fn retention_that_worked_and_then_stopped_refuses() {
    let now = BOOT + 30 * 86_400;
    let observation = RetentionObservation {
        failures: 0,
        sweeps_completed: 100,
        last_sweep_at: Some(now - RETENTION_MAX_SILENCE_SECONDS - 1),
        truncated_since: None,
    };
    let refusal = retention_is_healthy(&observation, BOOT, now)
        .expect_err("a long-dead sweep loop is not healthy");
    match refusal {
        RetentionRefusal::Stalled { silent_seconds } => {
            assert_eq!(silent_seconds, RETENTION_MAX_SILENCE_SECONDS + 1);
        }
        other => panic!("expected Stalled, got {other:?}"),
    }
}

/// Silence is measured from the LAST SWEEP, not from boot. A long-running
/// process that sweeps every hour must not go red simply for being old.
#[test]
fn silence_is_measured_from_the_last_sweep_not_from_boot() {
    let now = BOOT + 365 * 86_400;
    assert!(retention_is_healthy(&healthy(now), BOOT, now).is_ok());
}

/// The count is reported, never gated on: "swept 100 times" says nothing
/// about WHEN. Only the timestamp can distinguish a live sweep loop from
/// one that died an hour ago.
#[test]
fn a_large_sweep_count_does_not_excuse_silence() {
    let now = BOOT + 30 * 86_400;
    let observation = RetentionObservation {
        sweeps_completed: u64::MAX,
        last_sweep_at: Some(now - RETENTION_MAX_SILENCE_SECONDS - 1),
        ..RetentionObservation::default()
    };
    assert!(retention_is_healthy(&observation, BOOT, now).is_err());
}

// =====================================================================
// failures
// =====================================================================

#[test]
fn a_counted_failure_refuses_even_with_a_fresh_sweep() {
    let now = BOOT + 3600;
    let observation = RetentionObservation {
        failures: 1,
        ..healthy(now)
    };
    let refusal =
        retention_is_healthy(&observation, BOOT, now).expect_err("a failure is not health");
    match refusal {
        RetentionRefusal::Failing { failures } => assert_eq!(failures, 1),
        other => panic!("expected Failing, got {other:?}"),
    }
}

/// When retention has both failed and gone quiet, the failure is the
/// actionable cause -- reporting the silence would send the operator
/// looking for a stopped cycle loop instead of reading the error.
#[test]
fn a_failure_is_reported_ahead_of_the_silence_it_caused() {
    let now = BOOT + 30 * 86_400;
    let observation = RetentionObservation {
        failures: 4,
        sweeps_completed: 9,
        last_sweep_at: Some(now - RETENTION_MAX_SILENCE_SECONDS - 1),
        truncated_since: Some(now - RETENTION_TRUNCATION_TOLERANCE_SECONDS - 1),
    };
    assert!(matches!(
        retention_is_healthy(&observation, BOOT, now),
        Err(RetentionRefusal::Failing { failures: 4 })
    ));
}

// =====================================================================
// truncation: is retention keeping up?
// =====================================================================

#[test]
fn one_truncated_sweep_is_not_a_refusal() {
    // Truncation is BY DESIGN -- the sweep is globally batch-capped so it
    // can never monopolise the owner. A backlog draining over several
    // sweeps is the bound working, not a fault.
    let now = BOOT + 3600;
    let observation = RetentionObservation {
        truncated_since: Some(now),
        ..healthy(now)
    };
    assert!(retention_is_healthy(&observation, BOOT, now).is_ok());
}

#[test]
fn truncation_inside_the_tolerance_is_not_a_refusal() {
    let now = BOOT + 30 * 86_400;
    let observation = RetentionObservation {
        truncated_since: Some(now - RETENTION_TRUNCATION_TOLERANCE_SECONDS),
        ..healthy(now)
    };
    assert!(retention_is_healthy(&observation, BOOT, now).is_ok());
}

/// Sustained truncation means arrivals are outrunning deletions: the store
/// is growing DESPITE retention running perfectly. `failures` stays 0 the
/// whole way to a full disk.
#[test]
fn truncation_that_persists_past_the_tolerance_refuses() {
    let now = BOOT + 30 * 86_400;
    let observation = RetentionObservation {
        truncated_since: Some(now - RETENTION_TRUNCATION_TOLERANCE_SECONDS - 1),
        ..healthy(now)
    };
    let refusal = retention_is_healthy(&observation, BOOT, now)
        .expect_err("retention permanently behind is not healthy");
    match refusal {
        RetentionRefusal::PersistentlyTruncated { truncated_seconds } => {
            assert_eq!(
                truncated_seconds,
                RETENTION_TRUNCATION_TOLERANCE_SECONDS + 1
            );
        }
        other => panic!("expected PersistentlyTruncated, got {other:?}"),
    }
}

/// The run is measured from where it BEGAN, so a sweep that catches up
/// clears it. Otherwise a single truncation would eventually refuse
/// forever on a node that recovered hours ago.
#[test]
fn a_caught_up_sweep_clears_the_truncation_run() {
    let now = BOOT + 30 * 86_400;
    assert!(retention_is_healthy(&healthy(now), BOOT, now).is_ok());
}

// =====================================================================
// clocks and codes
// =====================================================================

#[test]
fn a_backwards_clock_step_does_not_manufacture_a_refusal() {
    // A clock that stepped backwards puts the last sweep in the future.
    // That must read as "swept very recently", never as a fault.
    let observation = RetentionObservation {
        last_sweep_at: Some(BOOT + 10_000),
        ..healthy(BOOT)
    };
    assert!(retention_is_healthy(&observation, BOOT, BOOT).is_ok());
    assert!(retention_is_healthy(&RetentionObservation::default(), BOOT, BOOT - 10_000).is_ok());
}

/// Both operands are wall clocks, so a corrupt row or a bad config can put
/// them arbitrarily far apart. A health check that PANICS on a strange
/// timestamp is worse than one that reports a strange age -- and in a
/// debug build plain subtraction here panics rather than wrapping.
#[test]
fn an_impossible_clock_spread_reports_rather_than_panics() {
    let observation = RetentionObservation {
        last_sweep_at: Some(i64::MIN),
        ..RetentionObservation::default()
    };
    assert!(retention_is_healthy(&observation, i64::MIN, i64::MAX).is_err());
    assert!(retention_is_healthy(&RetentionObservation::default(), i64::MIN, i64::MAX).is_err());

    let future = RetentionObservation {
        truncated_since: Some(i64::MAX),
        last_sweep_at: Some(i64::MAX),
        ..RetentionObservation::default()
    };
    assert!(retention_is_healthy(&future, i64::MAX, i64::MIN).is_ok());
}

#[test]
fn every_refusal_carries_a_distinct_actionable_code() {
    let codes = [
        RetentionRefusal::Failing { failures: 1 }.code(),
        RetentionRefusal::NeverSwept { silent_seconds: 1 }.code(),
        RetentionRefusal::Stalled { silent_seconds: 1 }.code(),
        RetentionRefusal::PersistentlyTruncated {
            truncated_seconds: 1,
        }
        .code(),
    ];
    let unique: std::collections::BTreeSet<_> = codes.iter().collect();
    assert_eq!(
        unique.len(),
        codes.len(),
        "codes must not collide: {codes:?}"
    );
    for code in codes {
        assert!(code.starts_with("retention_"), "{code}");
    }
}

#[test]
fn the_thresholds_are_wider_than_the_default_cycle_cadence() {
    // py `fee_interval` defaults to 1800s (cl-revenue-ops.py:4369) and a
    // sweep rides only a COMMITTING cycle, so the silence window has to
    // tolerate several non-committing intervals in a row.
    // Const-asserted, so narrowing either threshold fails the BUILD rather
    // than waiting for a test run.
    const { assert!(RETENTION_MAX_SILENCE_SECONDS >= 4 * 1800) };
    // A truncation run is only evidence of falling behind once it has
    // outlasted any plausible catch-up.
    const { assert!(RETENTION_TRUNCATION_TOLERANCE_SECONDS > RETENTION_MAX_SILENCE_SECONDS) };
}
