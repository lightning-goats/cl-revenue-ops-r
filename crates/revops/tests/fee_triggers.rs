//! Unit tests for `revops::fee_triggers` -- the bounded, coalescing
//! trigger queue (stateful-shadow plan Task 6, steps 3-4).

use revops::fee_triggers::{build_receipt, FeeTrigger, TriggerOutcome, TriggerQueue};

const NOW: i64 = 1_800_000_000;

// ---------------------------------------------------------------------------
// One offer per trigger kind
// ---------------------------------------------------------------------------

#[test]
fn fixed_interval_trigger_is_enqueued() {
    let mut q = TriggerQueue::new(8);
    assert_eq!(
        q.offer(FeeTrigger::FixedInterval, NOW),
        TriggerOutcome::Enqueued
    );
    assert_eq!(q.pending_len(), 1);
}

#[test]
fn failed_forward_nudge_trigger_is_enqueued_and_keyed_by_channel() {
    let mut q = TriggerQueue::new(8);
    let outcome = q.offer(
        FeeTrigger::FailedForward {
            channel_id: "100x1x0".to_string(),
        },
        NOW,
    );
    assert_eq!(outcome, TriggerOutcome::Enqueued);
    assert_eq!(q.pending_len(), 1);

    // A DIFFERENT channel's failed-forward nudge is a distinct pending
    // entry, not a coalesce.
    let outcome2 = q.offer(
        FeeTrigger::FailedForward {
            channel_id: "200x2x0".to_string(),
        },
        NOW,
    );
    assert_eq!(outcome2, TriggerOutcome::Enqueued);
    assert_eq!(q.pending_len(), 2);
}

#[test]
fn policy_changed_trigger_is_enqueued() {
    let mut q = TriggerQueue::new(8);
    let peer = format!("02{}", "aa".repeat(32));
    assert_eq!(
        q.offer(
            FeeTrigger::PolicyChanged {
                channel_id: peer.clone()
            },
            NOW
        ),
        TriggerOutcome::Enqueued
    );
    assert_eq!(q.pending_len(), 1);
}

#[test]
fn wake_all_trigger_is_enqueued() {
    let mut q = TriggerQueue::new(8);
    assert_eq!(q.offer(FeeTrigger::WakeAll, NOW), TriggerOutcome::Enqueued);
}

#[test]
fn vegas_spike_trigger_is_enqueued() {
    let mut q = TriggerQueue::new(8);
    assert_eq!(
        q.offer(FeeTrigger::VegasSpike, NOW),
        TriggerOutcome::Enqueued
    );
}

/// Fix round 1 (review finding 2): CLN's own `forward_event` notification
/// is a sixth trigger kind, keyed by channel like `FailedForward`.
#[test]
fn forward_event_trigger_is_enqueued_and_keyed_by_channel() {
    let mut q = TriggerQueue::new(8);
    let outcome = q.offer(
        FeeTrigger::ForwardEvent {
            channel_id: "100x1x0".to_string(),
        },
        NOW,
    );
    assert_eq!(outcome, TriggerOutcome::Enqueued);
    assert_eq!(q.pending_len(), 1);

    // A DIFFERENT channel's forward event is a distinct pending entry.
    let outcome2 = q.offer(
        FeeTrigger::ForwardEvent {
            channel_id: "200x2x0".to_string(),
        },
        NOW,
    );
    assert_eq!(outcome2, TriggerOutcome::Enqueued);
    assert_eq!(q.pending_len(), 2);
}

#[test]
fn forward_event_trigger_type_is_stable_text_identity() {
    let row = build_receipt(
        &FeeTrigger::ForwardEvent {
            channel_id: "100x1x0".to_string(),
        },
        NOW,
        false,
        None,
        "forward_event received",
    );
    assert_eq!(row.trigger_type, "forward_event");
    assert_eq!(row.channel_id.as_deref(), Some("100x1x0"));
}

// ---------------------------------------------------------------------------
// Coalescing keys
// ---------------------------------------------------------------------------

#[test]
fn repeated_same_key_trigger_coalesces_instead_of_growing() {
    let mut q = TriggerQueue::new(8);
    assert_eq!(q.offer(FeeTrigger::WakeAll, NOW), TriggerOutcome::Enqueued);
    assert_eq!(
        q.offer(FeeTrigger::WakeAll, NOW + 1),
        TriggerOutcome::Coalesced
    );
    assert_eq!(
        q.offer(FeeTrigger::WakeAll, NOW + 2),
        TriggerOutcome::Coalesced
    );
    assert_eq!(
        q.pending_len(),
        1,
        "coalesced triggers must not grow the queue"
    );

    let drained = q.drain_all();
    assert_eq!(drained.len(), 1);
    assert_eq!(
        drained[0].coalesced_count, 2,
        "two later occurrences coalesced in"
    );
    assert_eq!(
        drained[0].first_seen_at, NOW,
        "the FIRST occurrence's timestamp is kept"
    );
}

#[test]
fn different_channel_scoped_triggers_of_the_same_kind_do_not_coalesce() {
    let mut q = TriggerQueue::new(8);
    q.offer(
        FeeTrigger::PolicyChanged {
            channel_id: "peerA".to_string(),
        },
        NOW,
    );
    let outcome = q.offer(
        FeeTrigger::PolicyChanged {
            channel_id: "peerB".to_string(),
        },
        NOW,
    );
    assert_eq!(outcome, TriggerOutcome::Enqueued);
    assert_eq!(q.pending_len(), 2);
}

#[test]
fn different_trigger_kinds_never_coalesce_with_each_other() {
    let mut q = TriggerQueue::new(8);
    q.offer(FeeTrigger::WakeAll, NOW);
    let outcome = q.offer(FeeTrigger::VegasSpike, NOW);
    assert_eq!(outcome, TriggerOutcome::Enqueued);
    assert_eq!(q.pending_len(), 2);
}

// ---------------------------------------------------------------------------
// Bounded queue saturation / explicit drop counters
// ---------------------------------------------------------------------------

#[test]
fn bounded_queue_drops_a_new_key_once_at_capacity() {
    let mut q = TriggerQueue::new(2);
    assert_eq!(
        q.offer(
            FeeTrigger::FailedForward {
                channel_id: "a".to_string()
            },
            NOW
        ),
        TriggerOutcome::Enqueued
    );
    assert_eq!(
        q.offer(
            FeeTrigger::FailedForward {
                channel_id: "b".to_string()
            },
            NOW
        ),
        TriggerOutcome::Enqueued
    );
    // Capacity 2 is full with two DISTINCT keys; a third distinct key is
    // dropped, never silently grows the queue.
    assert_eq!(
        q.offer(
            FeeTrigger::FailedForward {
                channel_id: "c".to_string()
            },
            NOW
        ),
        TriggerOutcome::Dropped
    );
    assert_eq!(q.pending_len(), 2, "a dropped trigger must not be added");
    assert_eq!(q.dropped_total(), 1);
}

#[test]
fn dropped_total_accumulates_across_multiple_drops_and_never_resets() {
    let mut q = TriggerQueue::new(1);
    q.offer(FeeTrigger::WakeAll, NOW);
    q.offer(FeeTrigger::VegasSpike, NOW); // dropped: capacity 1 already full
    q.offer(
        FeeTrigger::PolicyChanged {
            channel_id: "peerA".to_string(),
        },
        NOW,
    ); // dropped too
    assert_eq!(q.dropped_total(), 2);

    // Draining and re-offering does not reset the counter.
    q.drain_all();
    q.offer(FeeTrigger::WakeAll, NOW);
    assert_eq!(
        q.dropped_total(),
        2,
        "dropped_total is cumulative, never resets"
    );
}

#[test]
fn a_coalesced_offer_at_full_capacity_is_still_coalesced_not_dropped() {
    // Coalescing is checked BEFORE the capacity gate: an already-pending
    // key never counts against capacity for its own repeats.
    let mut q = TriggerQueue::new(1);
    assert_eq!(q.offer(FeeTrigger::WakeAll, NOW), TriggerOutcome::Enqueued);
    assert_eq!(
        q.offer(FeeTrigger::WakeAll, NOW + 5),
        TriggerOutcome::Coalesced,
        "a repeat of the ALREADY-pending key must coalesce, not drop, even at capacity"
    );
    assert_eq!(q.dropped_total(), 0);
}

// ---------------------------------------------------------------------------
// drain_all
// ---------------------------------------------------------------------------

#[test]
fn drain_all_empties_the_queue_and_returns_every_pending_entry_oldest_first() {
    let mut q = TriggerQueue::new(8);
    q.offer(FeeTrigger::WakeAll, NOW);
    q.offer(FeeTrigger::VegasSpike, NOW + 1);
    let drained = q.drain_all();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[0].trigger, FeeTrigger::WakeAll);
    assert_eq!(drained[0].first_seen_at, NOW);
    assert_eq!(drained[1].trigger, FeeTrigger::VegasSpike);
    assert_eq!(drained[1].first_seen_at, NOW + 1);
    assert_eq!(q.pending_len(), 0);
    assert!(
        q.drain_all().is_empty(),
        "draining an empty queue is a no-op"
    );
}

// ---------------------------------------------------------------------------
// build_receipt / receipt timestamps
// ---------------------------------------------------------------------------

#[test]
fn build_receipt_carries_trigger_type_channel_and_coalesced_flag() {
    let row = build_receipt(
        &FeeTrigger::PolicyChanged {
            channel_id: "peerA".to_string(),
        },
        NOW,
        true,
        None,
        "applied in-memory wake",
    );
    assert_eq!(row.trigger_type, "policy_changed");
    assert_eq!(row.channel_id.as_deref(), Some("peerA"));
    assert!(row.coalesced);
    assert_eq!(row.received_at, NOW);
    assert_eq!(row.cycle_id, None);
    assert_eq!(row.cycle_ts, None);
    assert_eq!(row.detail.as_deref(), Some("applied in-memory wake"));
}

#[test]
fn build_receipt_with_no_channel_scope_leaves_channel_id_none() {
    let row = build_receipt(&FeeTrigger::WakeAll, NOW, false, None, "ok");
    assert_eq!(row.channel_id, None);
}

/// R8 amendment item 3: a receipt that DID produce a cycle carries the
/// cycle's `cycle_ts` -- the SAME value a `rust_fee_shadow_outcomes` row
/// for that cycle would carry, so the two tables join on `cycle_ts`.
#[test]
fn build_receipt_with_a_cycle_shares_the_cycle_ts_key() {
    let row = build_receipt(
        &FeeTrigger::FixedInterval,
        NOW,
        false,
        Some(("rust-fee-1800000000-123-0", NOW)),
        "ran: 3 decisions",
    );
    assert_eq!(row.cycle_id.as_deref(), Some("rust-fee-1800000000-123-0"));
    assert_eq!(
        row.cycle_ts,
        Some(NOW),
        "cycle_ts must equal the cycle's own clock read"
    );
    assert_eq!(row.trigger_type, "fixed_interval");
}
