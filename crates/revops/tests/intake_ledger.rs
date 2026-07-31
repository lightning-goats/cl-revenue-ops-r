//! R68-2 (RED): the four subscriptions' intake outcomes, and the cursor
//! they move.
//!
//! The four `@plugin.subscribe` bindings -- `forward_event`, `connect`,
//! `disconnect`, `channel_state_changed` -- currently end in
//! `if let Err(e) = ... { eprintln!(...) }`. That mirrors Python's
//! `except Exception: plugin.log(...)`, and keeping CLN event processing
//! alive is right: a handler that propagates would take down the
//! subscription.
//!
//! What is NOT right is that the failure then leaves no trace a gate can
//! read. Task 68's rule is that required writes return typed outcomes and
//! "no `.ok()`-to-null conversion may satisfy preflight" -- so a dropped
//! notification must be COUNTED, and a cutover readiness check must be
//! able to refuse on it.

use revops::lifecycle::{
    intake_is_clean, IntakeLedger, IntakeOutcome, IntakeRefusal, Subscription,
};
use revops_db::notifications::ForwardRow;
use revops_db::owner::spawn_read_write;

const NOW: i64 = 1_800_000_000;

// =====================================================================
// the ledger: every subscription's outcome is countable
// =====================================================================

#[test]
fn the_ledger_covers_exactly_the_four_python_subscriptions() {
    // Derived from `@plugin.subscribe` in cl-revenue-ops.py. A fifth would
    // be a Rust-only intake path; a missing one is an unbound notification.
    assert_eq!(
        Subscription::ALL,
        [
            Subscription::ForwardEvent,
            Subscription::Connect,
            Subscription::Disconnect,
            Subscription::ChannelStateChanged,
        ]
    );
    for subscription in Subscription::ALL {
        assert!(!subscription.as_str().is_empty());
    }
}

#[test]
fn a_fresh_ledger_is_clean() {
    assert!(intake_is_clean(&IntakeLedger::default()).is_ok());
}

#[test]
fn recorded_and_skipped_intake_stays_clean() {
    let ledger = IntakeLedger::default();
    ledger.record(Subscription::ForwardEvent, IntakeOutcome::Recorded);
    // py returns early for non-settled forwards: a real decision, not a
    // failure.
    ledger.record(
        Subscription::ForwardEvent,
        IntakeOutcome::Skipped("status is not settled"),
    );
    assert!(intake_is_clean(&ledger).is_ok());
    assert_eq!(ledger.recorded(Subscription::ForwardEvent), 1);
    assert_eq!(ledger.dropped(Subscription::ForwardEvent), 0);
}

/// The whole point. A dropped notification is data this node saw and did
/// not keep; a readiness gate must not pass over it.
#[test]
fn a_single_dropped_notification_refuses_readiness() {
    let ledger = IntakeLedger::default();
    ledger.record(Subscription::Connect, IntakeOutcome::Recorded);
    ledger.record(
        Subscription::Disconnect,
        IntakeOutcome::Dropped("observer store unavailable".to_string()),
    );

    let refusal = intake_is_clean(&ledger).expect_err("a dropped notification is not clean");
    match refusal {
        IntakeRefusal::Dropped {
            subscription,
            count,
            ..
        } => {
            assert_eq!(subscription, Subscription::Disconnect);
            assert_eq!(count, 1);
        }
    }
}

/// Every subscription is checked, not just the first. A drop on the LAST
/// one must still refuse -- otherwise the gate's verdict depends on
/// iteration order.
#[test]
fn a_drop_on_any_subscription_refuses_including_the_last() {
    for subscription in Subscription::ALL {
        let ledger = IntakeLedger::default();
        ledger.record(subscription, IntakeOutcome::Dropped("boom".to_string()));
        assert!(
            intake_is_clean(&ledger).is_err(),
            "{subscription:?} drop must refuse"
        );
    }
}

#[test]
fn the_refusal_names_the_reason_so_it_can_be_acted_on() {
    let ledger = IntakeLedger::default();
    ledger.record(
        Subscription::ForwardEvent,
        IntakeOutcome::Dropped("observer actor gone".to_string()),
    );
    let refusal = intake_is_clean(&ledger).expect_err("refuses");
    let IntakeRefusal::Dropped { last_reason, .. } = refusal;
    assert!(
        last_reason.contains("observer actor gone"),
        "the reason must survive to the gate: {last_reason}"
    );
}

// =====================================================================
// the cursor a dropped forward silently skips past
// =====================================================================

fn forward(ts: i64) -> ForwardRow {
    ForwardRow {
        in_channel: "900x1x0".to_string(),
        out_channel: "700x1x0".to_string(),
        in_msat: 1_000,
        out_msat: 900,
        fee_msat: 100,
        timestamp: ts,
        resolved_time: ts,
    }
}

/// The forward cursor is DERIVED from persisted rows (`MAX(timestamp)`),
/// which is what makes it restart-safe: a process that dies mid-hydration
/// re-derives the same cursor from what actually landed.
#[tokio::test]
async fn the_forward_cursor_is_derived_from_persisted_rows_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("observer.db");

    {
        let store = spawn_read_write(&path).await.unwrap();
        assert_eq!(store.last_forward_ts().await.unwrap(), None);
        store.insert_forward(forward(NOW - 100)).await.unwrap();
        assert_eq!(store.last_forward_ts().await.unwrap(), Some(NOW - 100));
    }

    // A fresh process over the same file re-derives the same cursor.
    let restarted = spawn_read_write(&path).await.unwrap();
    assert_eq!(
        restarted.last_forward_ts().await.unwrap(),
        Some(NOW - 100),
        "the cursor is the data, so a restart cannot lose or invent it"
    );
}

/// The failure a counted drop exists to expose.
///
/// A forward whose write is DROPPED leaves no row. A later forward that
/// succeeds advances the derived cursor PAST the hole, and startup
/// hydration only re-fetches a bounded overlap -- so once the hole falls
/// outside that window it is lost with nothing in the data to show it.
/// The ledger is the only place that remembers.
#[tokio::test]
async fn a_dropped_forward_leaves_a_hole_the_cursor_advances_past() {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let ledger = IntakeLedger::default();

    // The first forward's write fails (simulated at the intake boundary,
    // which is where main.rs's `None => log_and_drop` branch also lands).
    ledger.record(
        Subscription::ForwardEvent,
        IntakeOutcome::Dropped("observer store unavailable".to_string()),
    );

    // A later one succeeds.
    store.insert_forward(forward(NOW)).await.unwrap();
    ledger.record(Subscription::ForwardEvent, IntakeOutcome::Recorded);

    assert_eq!(
        store.last_forward_ts().await.unwrap(),
        Some(NOW),
        "the cursor advanced past the dropped forward"
    );
    assert!(
        intake_is_clean(&ledger).is_err(),
        "and the DATA cannot show the hole -- only the ledger can, which is \
         why a gate must consult it rather than the cursor"
    );
}

/// Control: a clean intake over the same store leaves a passable gate, so
/// the refusal above is about the drop and not about the store.
#[tokio::test]
async fn a_clean_forward_intake_leaves_the_gate_passable() {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let ledger = IntakeLedger::default();

    store.insert_forward(forward(NOW)).await.unwrap();
    ledger.record(Subscription::ForwardEvent, IntakeOutcome::Recorded);

    assert_eq!(store.last_forward_ts().await.unwrap(), Some(NOW));
    assert!(intake_is_clean(&ledger).is_ok());
}
