//! Bounded, coalescing trigger queue (stateful-shadow plan Task 6, steps
//! 3-4): every out-of-cycle scheduler trigger (the mechanism that actually
//! dispatches a cycle, a failed-forward nudge, a policy change, a manual
//! wake-all, or an edge-triggered Vegas spike) is offered to ONE bounded
//! [`TriggerQueue`] before the owner thread acts on it. Two invariants the
//! design holds on purpose:
//!
//! - **Coalescing, not deduplication-by-drop**: a SECOND occurrence of a
//!   trigger whose (kind, scope) key is already pending collapses into the
//!   existing pending entry (`TriggerOutcome::Coalesced`) instead of
//!   growing the queue -- repeated wakes for the same channel/peer before
//!   the next cycle observes them cost nothing extra.
//! - **Backpressure is loud, never silent**: once `capacity` distinct
//!   pending keys are already queued, a trigger with a NEW key is dropped
//!   (`TriggerOutcome::Dropped`) rather than growing unbounded. Every
//!   caller that receives `Dropped` MUST persist a red trigger-event
//!   receipt (`fee_scheduler.rs`'s `CycleOwner::record_trigger_receipt`
//!   does this) -- this module only counts drops
//!   ([`TriggerQueue::dropped_total`]); it does not persist anything
//!   itself (no DB dependency here on purpose: constructing a
//!   `revops_db::fee_runway::FeeTriggerEventRow` is [`build_receipt`]'s
//!   job, kept separate so this queue stays pure and independently
//!   testable).
//!
//! Subscriber handlers (`fee_scheduler.rs`'s `CycleOwner::handle_*`
//! methods) enqueue and record ONLY -- per the module contract, they never
//! run a cycle inline; the actual cycle dispatch stays the async trigger
//! loop's `dispatch_cycle`/`CycleMsg::RunPrepared` path, entirely
//! unchanged by this module (Step 4: "preserves the existing fixed-
//! interval cadence").
//!
//! ## R8 amendment item 3 (cycle-ts keying)
//!
//! [`build_receipt`]'s `cycle` parameter, when present, carries
//! `(cycle_id, cycle_ts)` for the `FixedInterval` trigger whose dispatch
//! produced a `SeedOnce` cycle commit. `cycle_ts` is always the EXACT same
//! `i64` clock value that cycle's `rust_fee_shadow_outcomes.cycle_ts` rows
//! carry (both come from the SAME single per-cycle clock read,
//! `CycleOwner::run_cycle`'s `now`) -- so a report/comparison tool can join
//! `rust_fee_trigger_events` and `rust_fee_shadow_outcomes` on `cycle_ts`
//! directly, not only on the (nullable, `RehydratePerCycle`-absent)
//! `cycle_id`.

use revops_db::fee_runway::FeeTriggerEventRow;

/// One trigger source. `FixedInterval` is the ONE variant that itself
/// causes (or accompanies) a cycle dispatch -- every other variant only
/// mutates in-memory scheduler bookkeeping and waits for the next
/// `FixedInterval`-classified cycle to observe it (mirrors Python's own
/// wake functions: they clear `is_sleeping`/backdate `last_update` and
/// never themselves run `adjust_all_fees`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeeTrigger {
    /// The scheduler's own cycle-dispatch mechanism: a wall-clock tick
    /// (`TriggerMode::FixedInterval`), an observed Python flush
    /// (`TriggerMode::FlushTriggered`'s `PollOutcome::RunCycle`), or a
    /// manual `RunCycleNow` wake -- all three are, from the trigger
    /// queue's perspective, "the mechanism that runs a cycle now."
    FixedInterval,
    /// `record_failed_forward`'s scheduler-facing hook (py
    /// fee_controller.py:8527-8608): a fee-relevant failed forward on
    /// `channel_id`. Recording-only in this task (the queue/receipt
    /// machinery) -- the per-channel Thompson posterior nudge itself is
    /// unported scheduler-side work; same "handler is real, caller is
    /// future work" posture `PolicyChanged` already carries in
    /// `fee_scheduler.rs`.
    FailedForward { channel_id: String },
    /// `_handle_policy_change` (py 7356-7400): a peer's policy changed.
    /// `channel_id` carries the affected PEER's id -- a policy change
    /// wakes every channel with that peer, not one channel, so there is
    /// no single-channel scope for this trigger; the peer id occupies the
    /// same free-form string slot `rust_fee_trigger_events.channel_id`
    /// already provides for scope-keyed triggers.
    PolicyChanged { channel_id: String },
    /// `wake_all_sleeping_channels` (py 4295-4384): the manual
    /// `revenue-r-fee-wake` RPC.
    WakeAll,
    /// `_maybe_wake_for_vegas_spike` (py 4386-4411): the edge-triggered
    /// Vegas-spike wake, offered between full cycles.
    VegasSpike,
}

impl FeeTrigger {
    /// Stable TEXT identity for `rust_fee_trigger_events.trigger_type`.
    pub fn trigger_type(&self) -> &'static str {
        match self {
            FeeTrigger::FixedInterval => "fixed_interval",
            FeeTrigger::FailedForward { .. } => "failed_forward",
            FeeTrigger::PolicyChanged { .. } => "policy_changed",
            FeeTrigger::WakeAll => "wake_all",
            FeeTrigger::VegasSpike => "vegas_spike",
        }
    }

    /// The scope-keyed channel/peer id this trigger carries, if any.
    pub fn channel_id(&self) -> Option<&str> {
        match self {
            FeeTrigger::FailedForward { channel_id } | FeeTrigger::PolicyChanged { channel_id } => {
                Some(channel_id.as_str())
            }
            FeeTrigger::FixedInterval | FeeTrigger::WakeAll | FeeTrigger::VegasSpike => None,
        }
    }

    /// The coalescing key: two triggers with the same key collapse into
    /// ONE pending queue entry.
    fn coalesce_key(&self) -> (&'static str, Option<&str>) {
        (self.trigger_type(), self.channel_id())
    }
}

/// What [`TriggerQueue::offer`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerOutcome {
    /// A new pending entry was accepted.
    Enqueued,
    /// Collapsed into an already-pending entry sharing the same key.
    Coalesced,
    /// The queue was at capacity and this key was not already pending:
    /// dropped. ALWAYS an explicit, recorded red event -- never silent
    /// (see the module doc).
    Dropped,
}

/// One pending (possibly coalesced) trigger, tracked until
/// [`TriggerQueue::drain_all`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTrigger {
    pub trigger: FeeTrigger,
    pub first_seen_at: i64,
    /// How many additional occurrences coalesced into this entry after
    /// the first (0 = only ever offered once).
    pub coalesced_count: u32,
}

/// The bounded, coalescing queue every trigger source offers to before the
/// owner thread acts. Deliberately synchronous (mirrors `FlushWatcher`):
/// production feeds it real events one at a time from the single owner
/// thread; tests script timelines.
#[derive(Debug)]
pub struct TriggerQueue {
    capacity: usize,
    pending: Vec<PendingTrigger>,
    dropped_total: u64,
}

impl TriggerQueue {
    pub fn new(capacity: usize) -> TriggerQueue {
        TriggerQueue {
            capacity,
            pending: Vec::new(),
            dropped_total: 0,
        }
    }

    /// Offer one trigger at `now`.
    pub fn offer(&mut self, trigger: FeeTrigger, now: i64) -> TriggerOutcome {
        let key = trigger.coalesce_key();
        if let Some(existing) = self
            .pending
            .iter_mut()
            .find(|p| p.trigger.coalesce_key() == key)
        {
            existing.coalesced_count += 1;
            return TriggerOutcome::Coalesced;
        }
        if self.pending.len() >= self.capacity {
            self.dropped_total += 1;
            return TriggerOutcome::Dropped;
        }
        self.pending.push(PendingTrigger {
            trigger,
            first_seen_at: now,
            coalesced_count: 0,
        });
        TriggerOutcome::Enqueued
    }

    /// Remove and return every currently pending entry (oldest first),
    /// freeing the whole queue -- the owner thread calls this once a
    /// `FixedInterval`-classified cycle has run (or attempted to run),
    /// since every pending wake-only trigger is now covered by that
    /// cycle's own evaluation.
    pub fn drain_all(&mut self) -> Vec<PendingTrigger> {
        std::mem::take(&mut self.pending)
    }

    /// Total triggers ever dropped for backpressure over this queue's
    /// lifetime. Never resets -- the red counter callers expose alongside
    /// `CycleOwner::persistence_failures`.
    pub fn dropped_total(&self) -> u64 {
        self.dropped_total
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
}

/// Build the persisted receipt for one trigger occurrence.
///
/// `cycle` carries `(cycle_id, cycle_ts)` ONLY when this occurrence is
/// itself the `FixedInterval` trigger whose dispatch produced (or
/// attempted) a `SeedOnce` cycle commit -- see the module doc's "R8
/// amendment item 3" section. Every other trigger (and every
/// `RehydratePerCycle` cycle, which never writes
/// `rust_fee_shadow_outcomes` at all) passes `None`: `cycle_id` is
/// nullable (`ON DELETE SET NULL`), so a receipt with no committed cycle
/// to point at simply carries no cycle identity, never a dangling one.
pub fn build_receipt(
    trigger: &FeeTrigger,
    received_at: i64,
    coalesced: bool,
    cycle: Option<(&str, i64)>,
    detail: impl Into<String>,
) -> FeeTriggerEventRow {
    FeeTriggerEventRow {
        trigger_type: trigger.trigger_type().to_string(),
        channel_id: trigger.channel_id().map(str::to_string),
        cycle_id: cycle.map(|(id, _)| id.to_string()),
        cycle_ts: cycle.map(|(_, ts)| ts),
        received_at,
        coalesced,
        detail: Some(detail.into()),
    }
}
