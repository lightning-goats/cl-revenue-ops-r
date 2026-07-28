//! Single-owner read-write actor for the Rust plugin's OWN
//! notification-ingestion database (never the production DB -- see the
//! phase1b plan's Global Constraints). Mirrors `actor.rs`'s
//! single-owner-task pattern (the `rusqlite::Connection` never crosses a
//! task boundary, only request/reply messages do) but for a writable
//! connection that this plugin creates itself if it doesn't exist yet,
//! rather than `actor::spawn_read_only`'s "never creates the file"
//! contract over the production db.

use crate::fee_runway::{
    self, BroadcastAttemptIntent, BroadcastAttemptOutcome, FeeCycleCommit, FeeRestartMarkerRow,
    FeeSeedEventRow, FeeStateSnapshot, FeeTriggerEventRow, MempoolMaComparisonRow,
    MempoolSampleRow, QuarantineEntry, QuarantineRow, RunwaySnapshotRow,
};
use crate::notifications::{self, ForwardRow};
use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

enum Command {
    InsertForward {
        row: ForwardRow,
        reply: oneshot::Sender<Result<bool>>,
    },
    LastForwardTs(oneshot::Sender<Result<Option<i64>>>),
    InsertPeerConnectionEvent {
        peer_id: String,
        event_type: String,
        ts: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    InsertChannelClosureEvent {
        scid: String,
        cause: String,
        ts: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    // -- Task 4: transactional Rust-owned fee state + audit schema --
    CommitFeeCycle {
        commit: FeeCycleCommit,
        reply: oneshot::Sender<Result<u64>>,
    },
    LoadLatestFeeState(oneshot::Sender<Result<FeeStateSnapshot>>),
    // -- Task 10 fix round 1 (I-5): scalar-only siblings that avoid
    // materialising full-row results just to answer a single number --
    CurrentStateGeneration(oneshot::Sender<Result<u64>>),
    // -- Task 44 / A3, live-review finding F3: cross-restart event
    // idempotency check --
    CycleExists {
        cycle_id: String,
        reply: oneshot::Sender<Result<bool>>,
    },
    // -- Task 44 / A3, live-review finding F7: generation-bound A3 commit --
    CycleExistsWithGeneration {
        cycle_id: String,
        reply: oneshot::Sender<Result<(bool, u64)>>,
    },
    CommitFeeCycleGuarded {
        commit: FeeCycleCommit,
        expected_prior_generation: u64,
        reply: oneshot::Sender<Result<fee_runway::GuardedCommitOutcome>>,
    },
    MempoolSampleStats {
        since: i64,
        reply: oneshot::Sender<Result<fee_runway::MempoolSampleStats>>,
    },
    RecordMempoolSample {
        sampled_at: i64,
        sat_per_vbyte: f64,
        reply: oneshot::Sender<Result<()>>,
    },
    // -- Task 6: transactional insert+prune for Rust's own recorder --
    RecordMempoolSamplePruned {
        sampled_at: i64,
        sat_per_vbyte: f64,
        retain_since: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    QueryMempoolSamplesSince {
        since: i64,
        reply: oneshot::Sender<Result<Vec<MempoolSampleRow>>>,
    },
    // -- Fix round 1 (review finding 1): persisted mempool 24h-MA
    // comparison --
    RecordMempoolMaComparison {
        row: MempoolMaComparisonRow,
        reply: oneshot::Sender<Result<i64>>,
    },
    RecordFeeTriggerEvent {
        event: FeeTriggerEventRow,
        reply: oneshot::Sender<Result<()>>,
    },
    FeeMutationCount(oneshot::Sender<Result<i64>>),
    // -- Task 10: distinct live-broadcast-attempt counter (see
    // `fee_runway::broadcast_attempt_count`'s doc comment for why this is
    // NOT the same number as `FeeMutationCount`) --
    FeeBroadcastAttemptCount(oneshot::Sender<Result<i64>>),
    InsertExecutionQuarantine {
        entry: QuarantineEntry,
        reply: oneshot::Sender<Result<i64>>,
    },
    ActiveExecutionQuarantine(oneshot::Sender<Result<Option<QuarantineRow>>>),
    RecordRunwaySnapshot {
        snapshot: RunwaySnapshotRow,
        reply: oneshot::Sender<Result<i64>>,
    },
    LatestRunwaySnapshot(oneshot::Sender<Result<Option<RunwaySnapshotRow>>>),
    // -- Task 5: SeedOnce seed events + restart markers --
    RecordFeeSeedEvent {
        event: FeeSeedEventRow,
        reply: oneshot::Sender<Result<i64>>,
    },
    LatestFeeSeedEvent(oneshot::Sender<Result<Option<FeeSeedEventRow>>>),
    RecordFeeRestartMarker {
        marker: FeeRestartMarkerRow,
        reply: oneshot::Sender<Result<i64>>,
    },
    LatestFeeRestartMarker(oneshot::Sender<Result<Option<FeeRestartMarkerRow>>>),
    // -- Task 9: guarded live broadcaster intent/result ledger + restart
    // quarantine reconciliation --
    InsertBroadcastAttempt {
        intent: BroadcastAttemptIntent,
        reply: oneshot::Sender<Result<i64>>,
    },
    RecordBroadcastAttemptResult {
        id: i64,
        outcome: BroadcastAttemptOutcome,
        outcome_detail: Option<String>,
        completed_at: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    ReconcileQuarantineOnRestart {
        now: i64,
        reply: oneshot::Sender<Result<usize>>,
    },
    RegisterLoop {
        id: crate::loop_health::LoopId,
        wiring: crate::loop_health::WiringStatus,
        now: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    ReconcileIncompleteLoops {
        now: i64,
        reply: oneshot::Sender<Result<usize>>,
    },
    BeginLoopPass {
        id: crate::loop_health::LoopId,
        now: i64,
        reply: oneshot::Sender<Result<u64>>,
    },
    FinishLoopPass {
        id: crate::loop_health::LoopId,
        generation: u64,
        now: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    FailLoopPass {
        id: crate::loop_health::LoopId,
        generation: u64,
        now: i64,
        error: String,
        reply: oneshot::Sender<Result<()>>,
    },
    IncrementLoopBackpressure {
        id: crate::loop_health::LoopId,
        coalesced: u64,
        dropped: u64,
        now: i64,
        reply: oneshot::Sender<Result<()>>,
    },
    ListLoopHealth(oneshot::Sender<Result<Vec<crate::loop_health::LoopHealthRow>>>),
}

/// Cheap, `Clone`-able handle to the observer-db owner task.
#[derive(Clone, Debug)]
pub struct ObserverHandle {
    tx: mpsc::Sender<Command>,
}

impl ObserverHandle {
    /// `INSERT OR IGNORE` a settled forward. Returns whether a new row was
    /// actually inserted (`false` on an exact-duplicate dedup no-op).
    pub async fn insert_forward(&self, row: ForwardRow) -> Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InsertForward { row, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// `SELECT MAX(timestamp) FROM ingested_forwards`.
    pub async fn last_forward_ts(&self) -> Result<Option<i64>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::LastForwardTs(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Record a peer connect/disconnect event.
    pub async fn insert_peer_connection_event(
        &self,
        peer_id: String,
        event_type: String,
        ts: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InsertPeerConnectionEvent {
                peer_id,
                event_type,
                ts,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Record a channel-closure event.
    pub async fn insert_channel_closure_event(
        &self,
        scid: String,
        cause: String,
        ts: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InsertChannelClosureEvent {
                scid,
                cause,
                ts,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    // -----------------------------------------------------------------
    // Task 4: transactional Rust-owned fee state + audit schema.
    //
    // Each async method below has a `blocking_*` sibling for the fee
    // scheduler, which runs on a plain `std::thread` (not a Tokio task)
    // and therefore cannot `.await`. The blocking siblings use
    // `mpsc::Sender::blocking_send` + `oneshot::Receiver::blocking_recv`
    // -- the SAME channel and the SAME single-owner connection, never a
    // second SQLite connection. Neither primitive takes a timeout
    // argument; a caller that needs a deadline enforces it itself (e.g.
    // by running the blocking call on a helper thread and racing it
    // against a `recv_timeout` on a `std::sync::mpsc` channel) -- this
    // API only provides the bounded bridge, not a policy for how long to
    // wait.
    // -----------------------------------------------------------------

    /// Atomically persist one complete fee cycle. See
    /// [`fee_runway::commit_fee_cycle`] for the transactional contract.
    pub async fn commit_fee_cycle(&self, commit: FeeCycleCommit) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::CommitFeeCycle { commit, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::commit_fee_cycle`] for the
    /// scheduler's `std::thread`.
    pub fn blocking_commit_fee_cycle(&self, commit: FeeCycleCommit) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::CommitFeeCycle { commit, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Load the current state generation and every channel's state row.
    pub async fn load_latest_fee_state(&self) -> Result<FeeStateSnapshot> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::LoadLatestFeeState(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::load_latest_fee_state`].
    pub fn blocking_load_latest_fee_state(&self) -> Result<FeeStateSnapshot> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::LoadLatestFeeState(reply))
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Task 44 / A3, live-review finding F3: has `cycle_id` already been
    /// durably committed? See [`fee_runway::cycle_exists`]'s doc comment.
    pub async fn cycle_exists(&self, cycle_id: String) -> Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::CycleExists { cycle_id, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::cycle_exists`] for the
    /// scheduler's `std::thread`.
    pub fn blocking_cycle_exists(&self, cycle_id: String) -> Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::CycleExists { cycle_id, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Task 44 / A3, live-review F7: [`fee_runway::cycle_exists_with_generation`]
    /// (idempotency answer + the current state generation, one consistent
    /// read).
    pub fn blocking_cycle_exists_with_generation(&self, cycle_id: String) -> Result<(bool, u64)> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::CycleExistsWithGeneration { cycle_id, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Task 44 / A3, live-review F7: [`fee_runway::commit_fee_cycle_guarded`]
    /// (the generation compare-and-set commit the A3 path uses).
    pub fn blocking_commit_fee_cycle_guarded(
        &self,
        commit: FeeCycleCommit,
        expected_prior_generation: u64,
    ) -> Result<fee_runway::GuardedCommitOutcome> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::CommitFeeCycleGuarded {
                commit,
                expected_prior_generation,
                reply,
            })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Scalar-only sibling of [`ObserverHandle::load_latest_fee_state`]
    /// (fix round 1, I-5): the current state generation WITHOUT
    /// materialising every channel's state row -- see
    /// [`fee_runway::current_state_generation`]'s doc comment.
    pub async fn current_state_generation(&self) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::CurrentStateGeneration(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::current_state_generation`].
    pub fn blocking_current_state_generation(&self) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::CurrentStateGeneration(reply))
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Record one mempool fee-rate sample.
    pub async fn record_mempool_sample(&self, sampled_at: i64, sat_per_vbyte: f64) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RecordMempoolSample {
                sampled_at,
                sat_per_vbyte,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::record_mempool_sample`].
    pub fn blocking_record_mempool_sample(
        &self,
        sampled_at: i64,
        sat_per_vbyte: f64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::RecordMempoolSample {
                sampled_at,
                sat_per_vbyte,
                reply,
            })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Record one mempool fee-rate sample AND prune everything strictly
    /// before `retain_since`, atomically. See
    /// [`fee_runway::record_mempool_sample_pruned`] for the transactional
    /// contract (Task 6 step 1).
    pub async fn record_mempool_sample_pruned(
        &self,
        sampled_at: i64,
        sat_per_vbyte: f64,
        retain_since: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RecordMempoolSamplePruned {
                sampled_at,
                sat_per_vbyte,
                retain_since,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of
    /// [`ObserverHandle::record_mempool_sample_pruned`].
    pub fn blocking_record_mempool_sample_pruned(
        &self,
        sampled_at: i64,
        sat_per_vbyte: f64,
        retain_since: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::RecordMempoolSamplePruned {
                sampled_at,
                sat_per_vbyte,
                retain_since,
                reply,
            })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Every mempool sample at or after `since`, oldest first.
    pub async fn query_mempool_samples_since(&self, since: i64) -> Result<Vec<MempoolSampleRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::QueryMempoolSamplesSince { since, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::query_mempool_samples_since`].
    pub fn blocking_query_mempool_samples_since(
        &self,
        since: i64,
    ) -> Result<Vec<MempoolSampleRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::QueryMempoolSamplesSince { since, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Scalar-only sibling of [`ObserverHandle::query_mempool_samples_since`]
    /// (fix round 1, I-5): sample count + latest timestamp WITHOUT
    /// fetching the rows themselves -- see
    /// [`fee_runway::mempool_sample_stats`]'s doc comment.
    pub async fn mempool_sample_stats(&self, since: i64) -> Result<fee_runway::MempoolSampleStats> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::MempoolSampleStats { since, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::mempool_sample_stats`].
    pub fn blocking_mempool_sample_stats(
        &self,
        since: i64,
    ) -> Result<fee_runway::MempoolSampleStats> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::MempoolSampleStats { since, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Fix round 1 (review finding 1): persist one shadow-window mempool
    /// 24h-MA comparison row (`NULL` `python_ma`/`delta` included -- the
    /// caller decides absence, this never substitutes a value).
    pub async fn record_mempool_ma_comparison(&self, row: MempoolMaComparisonRow) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RecordMempoolMaComparison { row, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::record_mempool_ma_comparison`]
    /// -- the scheduler's plain `std::thread` never `.await`s.
    pub fn blocking_record_mempool_ma_comparison(
        &self,
        row: MempoolMaComparisonRow,
    ) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::RecordMempoolMaComparison { row, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Record one trigger receipt / coalescing decision.
    pub async fn record_fee_trigger_event(&self, event: FeeTriggerEventRow) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RecordFeeTriggerEvent { event, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::record_fee_trigger_event`].
    pub fn blocking_record_fee_trigger_event(&self, event: FeeTriggerEventRow) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::RecordFeeTriggerEvent { event, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Total number of would-broadcast prepared fee actions ever
    /// committed (see [`fee_runway::mutation_count`]).
    pub async fn fee_mutation_count(&self) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::FeeMutationCount(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::fee_mutation_count`].
    pub fn blocking_fee_mutation_count(&self) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::FeeMutationCount(reply))
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Total number of live broadcast attempts ever recorded (see
    /// [`fee_runway::broadcast_attempt_count`]) -- Task 10's runway status
    /// RPC's "mutation-call count", distinct from
    /// [`ObserverHandle::fee_mutation_count`]'s "prepared-request count".
    pub async fn fee_broadcast_attempt_count(&self) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::FeeBroadcastAttemptCount(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::fee_broadcast_attempt_count`].
    pub fn blocking_fee_broadcast_attempt_count(&self) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::FeeBroadcastAttemptCount(reply))
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Record a new execution quarantine entry. Returns the new row id.
    pub async fn insert_execution_quarantine(&self, entry: QuarantineEntry) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InsertExecutionQuarantine { entry, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::insert_execution_quarantine`].
    pub fn blocking_insert_execution_quarantine(&self, entry: QuarantineEntry) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::InsertExecutionQuarantine { entry, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// The most recent still-active quarantine entry, if any. `Some(_)`
    /// means execution must stay denied.
    pub async fn active_execution_quarantine(&self) -> Result<Option<QuarantineRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::ActiveExecutionQuarantine(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::active_execution_quarantine`].
    pub fn blocking_active_execution_quarantine(&self) -> Result<Option<QuarantineRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::ActiveExecutionQuarantine(reply))
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Record one runway timer snapshot / report identity row.
    pub async fn record_runway_snapshot(&self, snapshot: RunwaySnapshotRow) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RecordRunwaySnapshot { snapshot, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::record_runway_snapshot`].
    pub fn blocking_record_runway_snapshot(&self, snapshot: RunwaySnapshotRow) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::RecordRunwaySnapshot { snapshot, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// The most recently recorded runway snapshot, if any.
    pub async fn latest_runway_snapshot(&self) -> Result<Option<RunwaySnapshotRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::LatestRunwaySnapshot(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::latest_runway_snapshot`].
    pub fn blocking_latest_runway_snapshot(&self) -> Result<Option<RunwaySnapshotRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::LatestRunwaySnapshot(reply))
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Record one SeedOnce seed event (import or fail-closed refusal).
    pub async fn record_fee_seed_event(&self, event: FeeSeedEventRow) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RecordFeeSeedEvent { event, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::record_fee_seed_event`].
    pub fn blocking_record_fee_seed_event(&self, event: FeeSeedEventRow) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::RecordFeeSeedEvent { event, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// The most recently recorded seed event, if any.
    pub async fn latest_fee_seed_event(&self) -> Result<Option<FeeSeedEventRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::LatestFeeSeedEvent(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::latest_fee_seed_event`].
    pub fn blocking_latest_fee_seed_event(&self) -> Result<Option<FeeSeedEventRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::LatestFeeSeedEvent(reply))
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// Record one scheduler restart marker.
    pub async fn record_fee_restart_marker(&self, marker: FeeRestartMarkerRow) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RecordFeeRestartMarker { marker, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::record_fee_restart_marker`].
    pub fn blocking_record_fee_restart_marker(&self, marker: FeeRestartMarkerRow) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::RecordFeeRestartMarker { marker, reply })
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    /// The most recently recorded restart marker, if any.
    pub async fn latest_fee_restart_marker(&self) -> Result<Option<FeeRestartMarkerRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::LatestFeeRestartMarker(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Blocking sibling of [`ObserverHandle::latest_fee_restart_marker`].
    pub fn blocking_latest_fee_restart_marker(&self) -> Result<Option<FeeRestartMarkerRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .blocking_send(Command::LatestFeeRestartMarker(reply))
            .context("observer actor gone (blocking)")?;
        rx.blocking_recv()
            .context("observer actor dropped reply (blocking)")?
    }

    // -----------------------------------------------------------------
    // Task 9: the guarded live broadcaster's intent/result ledger +
    // restart quarantine reconciliation. Async only -- the guarded
    // broadcaster (the only caller) is already async (it awaits the CLN
    // RPC call), unlike the fee scheduler's plain `std::thread` owner.
    // -----------------------------------------------------------------

    /// Persist one broadcast attempt's intent BEFORE any socket write.
    /// Returns the new row's id.
    pub async fn insert_broadcast_attempt(&self, intent: BroadcastAttemptIntent) -> Result<i64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::InsertBroadcastAttempt { intent, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Record one broadcast attempt's terminal transport outcome, AFTER
    /// the socket call returned (or was conclusively refused before any
    /// write).
    pub async fn record_broadcast_attempt_result(
        &self,
        id: i64,
        outcome: BroadcastAttemptOutcome,
        outcome_detail: Option<String>,
        completed_at: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RecordBroadcastAttemptResult {
                id,
                outcome,
                outcome_detail,
                completed_at,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    /// Restart reconciliation (Task 9 brief Step 2): restore quarantine
    /// for any broadcast attempt that never recorded a result, BEFORE any
    /// cutover arm is accepted. See
    /// [`fee_runway::reconcile_quarantine_on_restart`] for the full
    /// contract. Returns the number of attempts reconciled.
    pub async fn reconcile_quarantine_on_restart(&self, now: i64) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::ReconcileQuarantineOnRestart { now, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }

    pub async fn register_loop(
        &self,
        id: crate::loop_health::LoopId,
        wiring: crate::loop_health::WiringStatus,
        now: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::RegisterLoop {
                id,
                wiring,
                now,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }
    pub async fn reconcile_incomplete_loops(&self, now: i64) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::ReconcileIncompleteLoops { now, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }
    pub async fn begin_loop_pass(&self, id: crate::loop_health::LoopId, now: i64) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::BeginLoopPass { id, now, reply })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }
    pub async fn finish_loop_pass(
        &self,
        id: crate::loop_health::LoopId,
        generation: u64,
        now: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::FinishLoopPass {
                id,
                generation,
                now,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }
    pub async fn fail_loop_pass(
        &self,
        id: crate::loop_health::LoopId,
        generation: u64,
        now: i64,
        error: String,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::FailLoopPass {
                id,
                generation,
                now,
                error,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }
    pub async fn increment_loop_backpressure(
        &self,
        id: crate::loop_health::LoopId,
        coalesced: u64,
        dropped: u64,
        now: i64,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::IncrementLoopBackpressure {
                id,
                coalesced,
                dropped,
                now,
                reply,
            })
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }
    pub async fn list_loop_health(&self) -> Result<Vec<crate::loop_health::LoopHealthRow>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Command::ListLoopHealth(reply))
            .await
            .context("observer actor gone")?;
        rx.await.context("observer actor dropped reply")?
    }
}

/// Open (creating the file and any missing parent directories if needed)
/// the plugin's own read-write sqlite file with the two settings the
/// single-owner actor's correctness depends on, then initialize its schema.
///
/// Final-review finding I3 (2026-07-26) -- this was a bare
/// `Connection::open`, the only db open in the repo without a
/// `busy_timeout`, and its WAL pragma's result was discarded:
///
/// * `busy_timeout` -- SQLite defaults to 0, i.e. an immediate
///   `SQLITE_BUSY` rather than any lock wait. During a soak an operator
///   WILL open `sqlite3` on this file, and the engagement gate reads it
///   concurrently; without a wait budget every `commit_fee_cycle` fails
///   instantly, forever, visible only in the log. Same
///   [`crate::BUSY_TIMEOUT_MS`] as every other open here.
/// * WAL -- `PRAGMA journal_mode=WAL` is a *request*: SQLite answers with
///   the mode it actually ended up in, and `execute_batch` throws that
///   answer away. A silent fallback to a rollback journal is exactly the
///   regime where a concurrent reader BLOCKS the writer, so it is verified
///   here and fails the open loudly ([`require_wal_mode`]) instead of
///   degrading invisibly.
pub fn open_observer_db(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create observer db directory {}", parent.display()))?;
        }
    }
    let conn =
        Connection::open(path).with_context(|| format!("open observer db {}", path.display()))?;
    conn.busy_timeout(std::time::Duration::from_millis(crate::BUSY_TIMEOUT_MS))
        .with_context(|| format!("set busy_timeout on observer db {}", path.display()))?;
    let mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .with_context(|| format!("set journal_mode=WAL on observer db {}", path.display()))?;
    require_wal_mode(&mode, path)?;
    notifications::init_schema(&conn).context("init observer db schema")?;
    Ok(conn)
}

/// The verifier behind [`open_observer_db`]: `PRAGMA journal_mode=WAL`
/// REPORTS the mode the database actually ended up in, which is not
/// necessarily WAL (a read-only directory, an unsupported filesystem, or a
/// `:memory:`/temp database all answer something else). Anything but WAL
/// is a hard error naming the file -- the observer db is written by one
/// actor while the engagement gate reads it, and only WAL keeps those from
/// blocking each other.
pub fn require_wal_mode(mode: &str, path: &Path) -> Result<()> {
    anyhow::ensure!(
        mode.eq_ignore_ascii_case("wal"),
        "observer db {} did not enter WAL mode (journal_mode={mode}); a rollback \
         journal lets a concurrent reader block the fee-cycle writer",
        path.display(),
    );
    Ok(())
}

/// [`open_observer_db`] plus the single-owner actor task that owns the
/// resulting `Connection` for the rest of the plugin's lifetime.
///
/// Unlike `actor::spawn_read_only` (which never creates the production
/// db), this is a fresh, Rust-only file with no production analog -- an
/// operator pointing `observer-db-path` at a path that doesn't exist yet
/// is the expected first-run case, not a misconfiguration.
pub async fn spawn_read_write(path: &Path) -> Result<ObserverHandle> {
    let conn = open_observer_db(path)?;

    let (tx, mut rx) = mpsc::channel::<Command>(64);
    tokio::task::spawn_blocking(move || {
        // Single-owner: this `Connection` (which is `!Sync`) never leaves
        // this blocking task; only command/reply messages cross the
        // boundary. WAL + a single writer task means no cross-task lock
        // contention on our own db file.
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                Command::InsertForward { row, reply } => {
                    let result = notifications::insert_forward_ignore_dup(&conn, &row);
                    let _ = reply.send(result);
                }
                Command::LastForwardTs(reply) => {
                    let result = notifications::last_forward_ts(&conn);
                    let _ = reply.send(result);
                }
                Command::InsertPeerConnectionEvent {
                    peer_id,
                    event_type,
                    ts,
                    reply,
                } => {
                    let result = notifications::insert_peer_connection_event(
                        &conn,
                        &peer_id,
                        &event_type,
                        ts,
                    );
                    let _ = reply.send(result);
                }
                Command::InsertChannelClosureEvent {
                    scid,
                    cause,
                    ts,
                    reply,
                } => {
                    let result =
                        notifications::insert_channel_closure_event(&conn, &scid, &cause, ts);
                    let _ = reply.send(result);
                }
                Command::CommitFeeCycle { commit, reply } => {
                    let result = fee_runway::commit_fee_cycle(&conn, &commit);
                    let _ = reply.send(result);
                }
                Command::LoadLatestFeeState(reply) => {
                    let result = fee_runway::load_latest_state(&conn);
                    let _ = reply.send(result);
                }
                Command::CurrentStateGeneration(reply) => {
                    let result = fee_runway::current_state_generation(&conn);
                    let _ = reply.send(result);
                }
                Command::CycleExists { cycle_id, reply } => {
                    let result = fee_runway::cycle_exists(&conn, &cycle_id);
                    let _ = reply.send(result);
                }
                Command::CycleExistsWithGeneration { cycle_id, reply } => {
                    let result = fee_runway::cycle_exists_with_generation(&conn, &cycle_id);
                    let _ = reply.send(result);
                }
                Command::CommitFeeCycleGuarded {
                    commit,
                    expected_prior_generation,
                    reply,
                } => {
                    let result = fee_runway::commit_fee_cycle_guarded(
                        &conn,
                        &commit,
                        expected_prior_generation,
                    );
                    let _ = reply.send(result);
                }
                Command::MempoolSampleStats { since, reply } => {
                    let result = fee_runway::mempool_sample_stats(&conn, since);
                    let _ = reply.send(result);
                }
                Command::RecordMempoolSample {
                    sampled_at,
                    sat_per_vbyte,
                    reply,
                } => {
                    let result =
                        fee_runway::record_mempool_sample(&conn, sampled_at, sat_per_vbyte);
                    let _ = reply.send(result);
                }
                Command::RecordMempoolSamplePruned {
                    sampled_at,
                    sat_per_vbyte,
                    retain_since,
                    reply,
                } => {
                    let result = fee_runway::record_mempool_sample_pruned(
                        &conn,
                        sampled_at,
                        sat_per_vbyte,
                        retain_since,
                    );
                    let _ = reply.send(result);
                }
                Command::QueryMempoolSamplesSince { since, reply } => {
                    let result = fee_runway::query_mempool_samples_since(&conn, since);
                    let _ = reply.send(result);
                }
                Command::RecordMempoolMaComparison { row, reply } => {
                    let result = fee_runway::record_mempool_ma_comparison(&conn, &row);
                    let _ = reply.send(result);
                }
                Command::RecordFeeTriggerEvent { event, reply } => {
                    let result = fee_runway::record_trigger_event(&conn, &event);
                    let _ = reply.send(result);
                }
                Command::FeeMutationCount(reply) => {
                    let result = fee_runway::mutation_count(&conn);
                    let _ = reply.send(result);
                }
                Command::FeeBroadcastAttemptCount(reply) => {
                    let result = fee_runway::broadcast_attempt_count(&conn);
                    let _ = reply.send(result);
                }
                Command::InsertExecutionQuarantine { entry, reply } => {
                    let result = fee_runway::insert_quarantine(&conn, &entry);
                    let _ = reply.send(result);
                }
                Command::ActiveExecutionQuarantine(reply) => {
                    let result = fee_runway::active_quarantine(&conn);
                    let _ = reply.send(result);
                }
                Command::RecordRunwaySnapshot { snapshot, reply } => {
                    let result = fee_runway::record_runway_snapshot(&conn, &snapshot);
                    let _ = reply.send(result);
                }
                Command::LatestRunwaySnapshot(reply) => {
                    let result = fee_runway::latest_runway_snapshot(&conn);
                    let _ = reply.send(result);
                }
                Command::RecordFeeSeedEvent { event, reply } => {
                    let result = fee_runway::record_seed_event(&conn, &event);
                    let _ = reply.send(result);
                }
                Command::LatestFeeSeedEvent(reply) => {
                    let result = fee_runway::latest_seed_event(&conn);
                    let _ = reply.send(result);
                }
                Command::RecordFeeRestartMarker { marker, reply } => {
                    let result = fee_runway::record_restart_marker(&conn, &marker);
                    let _ = reply.send(result);
                }
                Command::LatestFeeRestartMarker(reply) => {
                    let result = fee_runway::latest_restart_marker(&conn);
                    let _ = reply.send(result);
                }
                Command::InsertBroadcastAttempt { intent, reply } => {
                    let result = fee_runway::insert_broadcast_attempt(&conn, &intent);
                    let _ = reply.send(result);
                }
                Command::RecordBroadcastAttemptResult {
                    id,
                    outcome,
                    outcome_detail,
                    completed_at,
                    reply,
                } => {
                    let result = fee_runway::record_broadcast_attempt_result(
                        &conn,
                        id,
                        outcome,
                        outcome_detail.as_deref(),
                        completed_at,
                    );
                    let _ = reply.send(result);
                }
                Command::ReconcileQuarantineOnRestart { now, reply } => {
                    let result = fee_runway::reconcile_quarantine_on_restart(&conn, now);
                    let _ = reply.send(result);
                }
                Command::RegisterLoop {
                    id,
                    wiring,
                    now,
                    reply,
                } => {
                    let _ = reply.send(crate::loop_health::register_loop(&conn, id, wiring, now));
                }
                Command::ReconcileIncompleteLoops { now, reply } => {
                    let _ = reply.send(crate::loop_health::reconcile_incomplete_on_restart(
                        &conn, now,
                    ));
                }
                Command::BeginLoopPass { id, now, reply } => {
                    let _ = reply.send(crate::loop_health::begin_loop_pass(&conn, id, now));
                }
                Command::FinishLoopPass {
                    id,
                    generation,
                    now,
                    reply,
                } => {
                    let _ = reply.send(crate::loop_health::finish_loop_pass(
                        &conn, id, generation, now,
                    ));
                }
                Command::FailLoopPass {
                    id,
                    generation,
                    now,
                    error,
                    reply,
                } => {
                    let _ = reply.send(crate::loop_health::fail_loop_pass(
                        &conn, id, generation, now, &error,
                    ));
                }
                Command::IncrementLoopBackpressure {
                    id,
                    coalesced,
                    dropped,
                    now,
                    reply,
                } => {
                    let _ = reply.send(crate::loop_health::increment_loop_backpressure(
                        &conn, id, coalesced, dropped, now,
                    ));
                }
                Command::ListLoopHealth(reply) => {
                    let _ = reply.send(crate::loop_health::list_loop_health(&conn));
                }
            }
        }
    });
    Ok(ObserverHandle { tx })
}
