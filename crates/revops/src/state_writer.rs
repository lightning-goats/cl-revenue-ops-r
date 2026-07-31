//! Task 65 slice 2: the canonical live state-writer CAPABILITY.
//!
//! `ProductionStateWriter` fronts the revops-db writer actor with the
//! task's six-way acknowledgement vocabulary and the ordering rails:
//!
//! - **Acks**: `Applied` / `AlreadyTerminal` / `Denied` / `NotAdmitted`
//!   (try_send refused -- provably nothing enqueued) /
//!   `AdmittedOutcomeUnknown` (receipt expired -- the write MAY land;
//!   callers re-read, never blind-retry) / `StorageFailure` (the actor
//!   definitively failed the write). Every arm has a stable code.
//! - **Publication strictly follows commit**: the publish seam observes
//!   exactly the committed config version and never fires on any
//!   non-Applied ack; the wake seam fires strictly after a committed
//!   policy write (py parity: version-inside-txn, commit-before-wake).
//!
//! ## Capability boundary (pre-Task-69, DISCLOSED)
//!
//! `assemble` is public so temp-DB tests can build the capability, but
//! it has ZERO production call sites: `runtime.rs`, `lnplus_runtime.rs`,
//! and `main.rs` never name this type (source-scan pinned in
//! `tests/state_writer.rs`), `ObserverRuntime` has no field for it, and
//! the type is `!Clone` with no other constructor. The REAL
//! authority-gated construction -- consuming Task 69's
//! `WholePluginLiveCapability` -- replaces this seam at cutover; until
//! then no code path in the binary can reach a production database with
//! it, because nothing constructs it and the underlying actor refuses
//! non-existent/mis-shapen databases anyway.

use std::time::Duration;

use revops_db::owner::{StoreAdmissionRefused, StoreReceipt, StoreReceiptWait};
use revops_db::state_writer::{
    BatchAck, BudgetTransition, ConfigDelete, PeerPolicyWrite, PolicyDelete, StateWriterHandle,
};

/// Receipt budget default: the Task 59 floor (one legitimate SQLite lock
/// wait on an idle actor is never cut short).
const DEFAULT_RECEIPT_BUDGET: Duration = Duration::from_millis(revops_db::BUSY_TIMEOUT_MS + 2_000);

/// One state write's acknowledgement, in the Task 65 contract's exact
/// vocabulary. Stable codes via [`StateWriteAck::code`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateWriteAck<T> {
    Applied(T),
    /// The row exists but is already in a terminal state -- nothing was
    /// re-applied (terminal non-resurrection).
    AlreadyTerminal,
    /// Refused on validation grounds (over-bound batch, unknown id):
    /// provably nothing written.
    Denied(String),
    /// Admission refused (queue full / actor gone): provably nothing
    /// enqueued.
    NotAdmitted(String),
    /// Admitted but no reply within the receipt budget: the write MAY
    /// land later. Callers must re-read state, never blind-retry.
    AdmittedOutcomeUnknown(String),
    /// The actor definitively failed the write (transaction rolled
    /// back).
    StorageFailure(String),
}

impl<T> StateWriteAck<T> {
    /// Stable machine-matchable code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Applied(_) => "applied",
            Self::AlreadyTerminal => "already_terminal",
            Self::Denied(_) => "denied",
            Self::NotAdmitted(_) => "not_admitted",
            Self::AdmittedOutcomeUnknown(_) => "admitted_outcome_unknown",
            Self::StorageFailure(_) => "storage_failure",
        }
    }
}

fn admission_detail(refused: StoreAdmissionRefused) -> String {
    match refused {
        StoreAdmissionRefused::QueueFull => {
            "state-writer queue at capacity; nothing was enqueued".to_string()
        }
        StoreAdmissionRefused::ActorGone => {
            "state-writer actor gone; nothing was enqueued".to_string()
        }
    }
}

/// Resolve a two-phase call into the ack vocabulary.
async fn resolve<T>(
    admitted: Result<StoreReceipt<T>, StoreAdmissionRefused>,
    budget: Duration,
) -> StateWriteAck<T> {
    match admitted {
        Err(refused) => StateWriteAck::NotAdmitted(admission_detail(refused)),
        Ok(receipt) => match receipt.within(budget).await {
            StoreReceiptWait::Replied(Ok(value)) => StateWriteAck::Applied(value),
            StoreReceiptWait::Replied(Err(e)) => StateWriteAck::StorageFailure(format!("{e:#}")),
            StoreReceiptWait::OutcomeUnknown => StateWriteAck::AdmittedOutcomeUnknown(
                "admitted but no reply within the receipt budget; the write may still land \
                 -- re-read state, never blind-retry"
                    .to_string(),
            ),
        },
    }
}

/// The canonical live state-writer capability. `!Clone`; see the module
/// doc's capability-boundary note.
pub struct ProductionStateWriter {
    handle: StateWriterHandle,
    receipt_budget: Duration,
}

/// Sealed proof that the whole-plugin live handoff authorized core state
/// mutations. No production constructor exists before Task69 composes the
/// single WholePluginLiveCapability; unit tests receive a cfg(test) token.
pub struct CoreStateLiveCapability {
    _seal: (),
}

impl CoreStateLiveCapability {
    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self { _seal: () }
    }
}

/// Non-cloneable live core-state mutation bundle. RPC handlers can hold this
/// bundle only after consuming the sealed live capability above.
pub struct CoreMutators {
    writer: ProductionStateWriter,
    _live: CoreStateLiveCapability,
}

impl CoreMutators {
    pub fn assemble(writer: ProductionStateWriter, live: CoreStateLiveCapability) -> Self {
        Self {
            writer,
            _live: live,
        }
    }

    pub(crate) async fn upsert_peer_policy(
        &self,
        write: PeerPolicyWrite,
        now: i64,
    ) -> StateWriteAck<()> {
        self.writer.upsert_peer_policy(write, now).await
    }

    pub(crate) async fn delete_peer_policy(&self, peer_id: String) -> StateWriteAck<PolicyDelete> {
        self.writer.delete_peer_policy(peer_id).await
    }
}

impl ProductionStateWriter {
    /// Build the capability over a spawned writer actor. ZERO production
    /// call sites exist (source-scan pinned) until Task 69's
    /// authority-gated construction replaces this seam.
    pub fn assemble(handle: StateWriterHandle) -> Self {
        Self {
            handle,
            receipt_budget: DEFAULT_RECEIPT_BUDGET,
        }
    }

    /// A copy of this capability with a different receipt budget --
    /// test/diagnostic seam for the unknown arm; the capability itself
    /// stays `!Clone` (this consumes nothing but shares the same actor).
    pub fn with_receipt_budget(&self, budget: Duration) -> Self {
        Self {
            handle: self.handle.clone(),
            receipt_budget: budget,
        }
    }

    pub async fn set_config_override(&self, key: String, value: String) -> StateWriteAck<i64> {
        resolve(
            self.handle.try_set_config_override(key, value),
            self.receipt_budget,
        )
        .await
    }

    /// Version-then-publish: `publish` observes EXACTLY the committed
    /// version and runs strictly after the commit; any non-Applied ack
    /// publishes nothing.
    pub async fn set_config_override_and_publish(
        &self,
        key: String,
        value: String,
        publish: impl FnOnce(i64),
    ) -> StateWriteAck<i64> {
        let ack = self.set_config_override(key, value).await;
        if let StateWriteAck::Applied(version) = &ack {
            publish(*version);
        }
        ack
    }

    pub async fn delete_config_override(&self, key: String) -> StateWriteAck<ConfigDelete> {
        resolve(
            self.handle.try_delete_config_override(key),
            self.receipt_budget,
        )
        .await
    }

    pub async fn upsert_peer_policy(&self, write: PeerPolicyWrite, now: i64) -> StateWriteAck<()> {
        resolve(
            self.handle.try_upsert_peer_policy(write, now),
            self.receipt_budget,
        )
        .await
    }

    pub async fn delete_peer_policy(&self, peer_id: String) -> StateWriteAck<PolicyDelete> {
        resolve(
            self.handle.try_delete_peer_policy(peer_id),
            self.receipt_budget,
        )
        .await
    }

    /// Commit-then-wake: `wake` runs strictly after a committed policy
    /// write; any non-Applied ack wakes nothing.
    pub async fn upsert_peer_policy_then_wake(
        &self,
        write: PeerPolicyWrite,
        now: i64,
        wake: impl FnOnce(),
    ) -> StateWriteAck<()> {
        let ack = self.upsert_peer_policy(write, now).await;
        if matches!(ack, StateWriteAck::Applied(())) {
            wake();
        }
        ack
    }

    pub async fn apply_policy_batch(
        &self,
        writes: Vec<PeerPolicyWrite>,
        now: i64,
    ) -> StateWriteAck<usize> {
        match resolve(
            self.handle.try_apply_policy_batch(writes, now),
            self.receipt_budget,
        )
        .await
        {
            StateWriteAck::Applied(BatchAck::Applied { count }) => StateWriteAck::Applied(count),
            StateWriteAck::Applied(BatchAck::DeniedOverBound { len }) => StateWriteAck::Denied(
                format!("batch of {len} exceeds the 100-row bound; refused whole"),
            ),
            StateWriteAck::AlreadyTerminal => StateWriteAck::AlreadyTerminal,
            StateWriteAck::Denied(d) => StateWriteAck::Denied(d),
            StateWriteAck::NotAdmitted(d) => StateWriteAck::NotAdmitted(d),
            StateWriteAck::AdmittedOutcomeUnknown(d) => StateWriteAck::AdmittedOutcomeUnknown(d),
            StateWriteAck::StorageFailure(d) => StateWriteAck::StorageFailure(d),
        }
    }

    pub async fn set_hot_channel_override(
        &self,
        peer_id: String,
        note: Option<String>,
        min_depletion_trigger_pct: Option<f64>,
        now: i64,
    ) -> StateWriteAck<()> {
        resolve(
            self.handle
                .try_set_hot_channel_override(peer_id, note, min_depletion_trigger_pct, now),
            self.receipt_budget,
        )
        .await
    }

    pub async fn remove_hot_channel_override(&self, peer_id: String) -> StateWriteAck<bool> {
        resolve(
            self.handle.try_remove_hot_channel_override(peer_id),
            self.receipt_budget,
        )
        .await
    }

    pub async fn release_budget_reservation(&self, reservation_id: String) -> StateWriteAck<()> {
        map_transition(
            resolve(
                self.handle.try_release_budget_reservation(reservation_id),
                self.receipt_budget,
            )
            .await,
        )
    }

    pub async fn mark_budget_spent(
        &self,
        reservation_id: String,
        actual_spent: i64,
    ) -> StateWriteAck<()> {
        map_transition(
            resolve(
                self.handle
                    .try_mark_budget_spent(reservation_id, actual_spent),
                self.receipt_budget,
            )
            .await,
        )
    }

    pub async fn cleanup_stale_reservations(
        &self,
        max_age_seconds: i64,
        now: i64,
    ) -> StateWriteAck<i64> {
        resolve(
            self.handle
                .try_cleanup_stale_reservations(max_age_seconds, now),
            self.receipt_budget,
        )
        .await
    }

    pub async fn cleanup_closed_channels(&self, channel_ids: Vec<String>) -> StateWriteAck<usize> {
        match resolve(
            self.handle.try_cleanup_closed_channels(channel_ids),
            self.receipt_budget,
        )
        .await
        {
            StateWriteAck::Applied(BatchAck::Applied { count }) => StateWriteAck::Applied(count),
            StateWriteAck::Applied(BatchAck::DeniedOverBound { len }) => StateWriteAck::Denied(
                format!("batch of {len} exceeds the 100-row bound; refused whole"),
            ),
            StateWriteAck::AlreadyTerminal => StateWriteAck::AlreadyTerminal,
            StateWriteAck::Denied(d) => StateWriteAck::Denied(d),
            StateWriteAck::NotAdmitted(d) => StateWriteAck::NotAdmitted(d),
            StateWriteAck::AdmittedOutcomeUnknown(d) => StateWriteAck::AdmittedOutcomeUnknown(d),
            StateWriteAck::StorageFailure(d) => StateWriteAck::StorageFailure(d),
        }
    }
}

/// Guarded-transition mapping: Applied stays Applied, an already-terminal
/// row is its own ack arm, an unknown id is a Denied validation fact.
fn map_transition(ack: StateWriteAck<BudgetTransition>) -> StateWriteAck<()> {
    match ack {
        StateWriteAck::Applied(BudgetTransition::Applied) => StateWriteAck::Applied(()),
        StateWriteAck::Applied(BudgetTransition::AlreadyTerminal) => StateWriteAck::AlreadyTerminal,
        StateWriteAck::Applied(BudgetTransition::NotFound) => {
            StateWriteAck::Denied("no reservation with that id exists".to_string())
        }
        StateWriteAck::AlreadyTerminal => StateWriteAck::AlreadyTerminal,
        StateWriteAck::Denied(d) => StateWriteAck::Denied(d),
        StateWriteAck::NotAdmitted(d) => StateWriteAck::NotAdmitted(d),
        StateWriteAck::AdmittedOutcomeUnknown(d) => StateWriteAck::AdmittedOutcomeUnknown(d),
        StateWriteAck::StorageFailure(d) => StateWriteAck::StorageFailure(d),
    }
}
