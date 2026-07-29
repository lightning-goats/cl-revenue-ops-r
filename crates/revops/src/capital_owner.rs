//! Task 62 slice 5: the serialized capital owner.
//!
//! ONE dedicated OS thread owns every capital submission (open / close /
//! defibrillation), exactly the task-60 rebalance-owner discipline:
//! bounded ingress, per-message reply channels, blocking store siblings,
//! suspension on settle-persistence failure.
//!
//! The submission rail, in order, every time:
//!
//! 1. **Suspension gate** -- a prior settle-persistence failure refuses
//!    everything until restart.
//! 2. **Adapters gate** -- no [`CapitalActionAdapters`] (pre-cutover
//!    production, until Task 69's authority assembly) is the typed
//!    `capital_adapters_not_assembled` refusal; the RPC layer maps it to
//!    Python's verbatim "Capacity planner not initialized" arm.
//! 3. **Governor consult** -- absent or denying refuses typed.
//! 4. **Fresh positive budget evidence** (opens and defibrillations;
//!    closes RECOVER capital so they skip only the exhaustion arm --
//!    they never consume the window).
//! 5. **Registry begin** -- one in-flight-or-quarantined intent per
//!    (kind, peer); duplicates refuse with the holder's request id.
//! 6. **Durable intent + ACTIVE reservation**, two-phase admission: a
//!    clean refusal releases the registry slot; an admitted receipt that
//!    expires keeps it held and NEVER proceeds (reconciliation owns any
//!    orphan row).
//! 7. **Fresh evidence revalidation** (AFTER the durable intent, per the
//!    task contract): the live `listpeerchannels` snapshot must answer
//!    and the target must still make sense -- an open's peer must have
//!    no live channel, a close/defib's channel must exist. Failure is a
//!    SETTLED `clean_refusal` (released), zero wire calls.
//! 8. **Execute** through the adapters; a defibrillation is a bounded
//!    diagnostic REBALANCE (py `_execute_defibrillation:3666`) routed
//!    through the [`DefibExecutor`] seam (its production impl wraps the
//!    task-60 rebalance owner at Task 69), never an on-chain transport.
//! 9. **Classify + settle exactly once**; `outcome_unknown` QUARANTINES:
//!    the reservation keeps holding the budget window and the (kind,
//!    peer) pair stays blocked -- in-process via the registry, across
//!    restarts via the quarantined-intents seed. A settle-persistence
//!    failure suspends the owner.

use std::sync::Arc;

use revops_db::fee_runway::{CapitalIntent, CapitalSettle, UnresolvedCapitalIntent};
use revops_db::owner::{ObserverHandle, StoreAdmissionRefused, StoreReceiptWait};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::capital_adapters::{classify_capital_submit, CloseRpc, FundchannelRpc};
use crate::capital_boundaries::{
    check_budget_evidence, settlement_for_capital, ActiveIntentRegistry, BudgetDb, BudgetRefusal,
    CapitalSubmitOutcome, GovernorFacade, GovernorVerdict, RegistryVerdict,
};

/// Store budget for the owner's blocking receipt waits (Task 59 floor).
const OWNER_STORE_BUDGET: std::time::Duration =
    std::time::Duration::from_millis(revops_db::BUSY_TIMEOUT_MS + 2_000);

/// Bounded ingress: operator RPC + reconciliation only.
const OWNER_QUEUE_CAPACITY: usize = 16;

/// Python's defibrillation constants (`_execute_defibrillation:3666`):
/// a bounded 50k-sat diagnostic rebalance.
pub const DEFIB_AMOUNT_SATS: i64 = 50_000;

/// The capital execution capability: everything that can move funds.
/// ZERO production construction sites until Task 69's authority assembly
/// (source-scan pinned in `tests/capital_owner.rs`).
pub struct CapitalActionAdapters {
    pub fundchannel: Arc<dyn FundchannelRpc>,
    pub close: Arc<dyn CloseRpc>,
    pub defib: Arc<dyn DefibExecutor>,
}

/// The defibrillation seam: a bounded diagnostic rebalance through the
/// task-60 rebalance owner, already classified into the capital
/// vocabulary by the implementation.
pub trait DefibExecutor: Send + Sync {
    fn diagnostic_rebalance(
        &self,
        peer_id: &str,
        scid: &str,
        amount_sats: i64,
    ) -> CapitalSubmitOutcome;
}

/// The fresh-snapshot seam for step-7 revalidation.
pub trait CapitalEvidenceRpc: Send + Sync {
    fn get_peer_channels(&self) -> Result<Value, String>;
}

/// Restart-reconciliation lookups (read-only).
pub trait CapitalReconcileLookup: Send + Sync {
    fn listfunds(&self) -> Result<Value, String>;
    fn listclosedchannels(&self) -> Result<Value, String>;
}

/// Everything the owner thread needs. `adapters: None` is the production
/// pre-cutover state: submissions refuse typed while reconciliation
/// (store + read-only lookups) works.
pub struct CapitalOwnerDeps {
    pub adapters: Option<Arc<CapitalActionAdapters>>,
    pub governor: Option<Arc<dyn GovernorFacade>>,
    pub budget: Arc<dyn BudgetDb>,
    pub evidence: Arc<dyn CapitalEvidenceRpc>,
    pub store: ObserverHandle,
    pub reconcile: Arc<dyn CapitalReconcileLookup>,
    /// Injected wall clock (unix seconds) -- tests pin it.
    pub clock: Box<dyn Fn() -> i64 + Send>,
}

/// One planned capital action, in the kernel's vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapitalAction {
    Open {
        peer_id: String,
        amount_sats: i64,
        reason: String,
    },
    Close {
        peer_id: String,
        scid: String,
        reason: String,
    },
    Defibrillate {
        peer_id: String,
        scid: String,
        reason: String,
    },
}

impl CapitalAction {
    fn kind(&self) -> &'static str {
        match self {
            Self::Open { .. } => "open",
            Self::Close { .. } => "close",
            Self::Defibrillate { .. } => "defib",
        }
    }
    fn peer_id(&self) -> &str {
        match self {
            Self::Open { peer_id, .. }
            | Self::Close { peer_id, .. }
            | Self::Defibrillate { peer_id, .. } => peer_id,
        }
    }
    /// Sats the reservation holds: opens their amount, defibs the bounded
    /// diagnostic amount, closes nothing (they recover capital).
    fn reserved_sats(&self) -> i64 {
        match self {
            Self::Open { amount_sats, .. } => *amount_sats,
            Self::Defibrillate { .. } => DEFIB_AMOUNT_SATS,
            Self::Close { .. } => 0,
        }
    }
    fn scid(&self) -> Option<&str> {
        match self {
            Self::Open { .. } => None,
            Self::Close { scid, .. } | Self::Defibrillate { scid, .. } => Some(scid),
        }
    }
    fn reason(&self) -> &str {
        match self {
            Self::Open { reason, .. }
            | Self::Close { reason, .. }
            | Self::Defibrillate { reason, .. } => reason,
        }
    }
}

/// Every typed way the owner refuses or fails a request. Stable codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapitalRefusal {
    Suspended,
    AdaptersNotAssembled,
    GovernorNotAssembled,
    GovernorDenied {
        reason_code: String,
    },
    Budget(BudgetRefusal),
    /// An identical (kind, peer) intent is in flight, unresolved, or
    /// quarantined.
    IntentBusy {
        existing: String,
    },
    StoreAdmissionRefused(String),
    StoreIntentOutcomeUnknown(String),
    StoreFailed(String),
    SettlePersistenceUnknown {
        request_id: String,
        detail: String,
    },
}

impl CapitalRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Suspended => "capital_owner_suspended",
            Self::AdaptersNotAssembled => "capital_adapters_not_assembled",
            Self::GovernorNotAssembled => "capital_governor_not_assembled",
            Self::GovernorDenied { .. } => "capital_governor_denied",
            Self::Budget(refusal) => refusal.code(),
            Self::IntentBusy { .. } => "capital_intent_busy",
            Self::StoreAdmissionRefused(_) => "store_admission_refused",
            Self::StoreIntentOutcomeUnknown(_) => "store_intent_outcome_unknown",
            Self::StoreFailed(_) => "capital_store_failed",
            Self::SettlePersistenceUnknown { .. } => "capital_settle_persistence_unknown",
        }
    }
}

/// One completed submission (a settled terminal, including clean
/// refusals and quarantined unknowns).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapitalActionOutcome {
    pub request_id: String,
    pub outcome: CapitalSubmitOutcome,
}

/// One reconciliation pass's counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CapitalReconcileSummary {
    pub settled_success: usize,
    pub quarantined: usize,
}

enum OwnerMsg {
    Execute {
        action: CapitalAction,
        reply: oneshot::Sender<Result<CapitalActionOutcome, CapitalRefusal>>,
    },
    Debug {
        reply: oneshot::Sender<Value>,
    },
    ReconcileOnStart {
        reply: oneshot::Sender<Result<CapitalReconcileSummary, CapitalRefusal>>,
    },
}

/// Cheap cloneable handle to the owner thread.
#[derive(Clone)]
pub struct CapitalOwnerHandle {
    tx: mpsc::Sender<OwnerMsg>,
}

impl CapitalOwnerHandle {
    pub async fn execute(
        &self,
        action: CapitalAction,
    ) -> Result<CapitalActionOutcome, CapitalRefusal> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OwnerMsg::Execute { action, reply })
            .await
            .map_err(|_| CapitalRefusal::Suspended)?;
        rx.await.map_err(|_| CapitalRefusal::Suspended)?
    }

    pub async fn debug(&self) -> Option<Value> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(OwnerMsg::Debug { reply }).await.ok()?;
        rx.await.ok()
    }

    pub async fn reconcile_on_start(&self) -> Result<CapitalReconcileSummary, CapitalRefusal> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OwnerMsg::ReconcileOnStart { reply })
            .await
            .map_err(|_| CapitalRefusal::Suspended)?;
        rx.await.map_err(|_| CapitalRefusal::Suspended)?
    }
}

/// Spawn the owner thread and return its handle.
pub fn spawn_capital_owner(deps: CapitalOwnerDeps) -> CapitalOwnerHandle {
    let (tx, mut rx) = mpsc::channel::<OwnerMsg>(OWNER_QUEUE_CAPACITY);
    std::thread::Builder::new()
        .name("revops-capital-owner".to_string())
        .spawn(move || {
            let mut owner = OwnerState {
                deps,
                suspended: false,
                registry: ActiveIntentRegistry::default(),
                submissions: 0,
                refusals: 0,
            };
            while let Some(msg) = rx.blocking_recv() {
                match msg {
                    OwnerMsg::Execute { action, reply } => {
                        let result = owner.execute(action);
                        if result.is_err() {
                            owner.refusals += 1;
                        }
                        let _ = reply.send(result);
                    }
                    OwnerMsg::Debug { reply } => {
                        let _ = reply.send(owner.debug());
                    }
                    OwnerMsg::ReconcileOnStart { reply } => {
                        let _ = reply.send(owner.reconcile_on_start());
                    }
                }
            }
        })
        .expect("spawn capital owner thread");
    CapitalOwnerHandle { tx }
}

struct OwnerState {
    deps: CapitalOwnerDeps,
    suspended: bool,
    registry: ActiveIntentRegistry,
    submissions: u64,
    refusals: u64,
}

impl OwnerState {
    fn execute(&mut self, action: CapitalAction) -> Result<CapitalActionOutcome, CapitalRefusal> {
        if self.suspended {
            return Err(CapitalRefusal::Suspended);
        }
        let Some(adapters) = self.deps.adapters.clone() else {
            return Err(CapitalRefusal::AdaptersNotAssembled);
        };
        let Some(governor) = self.deps.governor.clone() else {
            return Err(CapitalRefusal::GovernorNotAssembled);
        };
        let now = (self.deps.clock)();

        // Governor consult.
        match governor.authorize(action.kind(), action.reserved_sats()) {
            GovernorVerdict::Authorized { .. } => {}
            GovernorVerdict::Denied { reason_code } => {
                return Err(CapitalRefusal::GovernorDenied { reason_code });
            }
        }

        // Fresh positive budget evidence. A close recovers capital: it
        // still demands a fresh readable observation, but the exhaustion
        // arm cannot block it.
        match check_budget_evidence(self.deps.budget.as_ref(), now) {
            Ok(_) => {}
            Err(BudgetRefusal::Exhausted { .. })
                if matches!(action, CapitalAction::Close { .. }) => {}
            Err(refusal) => return Err(CapitalRefusal::Budget(refusal)),
        }

        // Registry: one intent per (kind, peer).
        self.submissions += 1;
        let request_id = format!(
            "{}-{now}-{}-{}",
            action.kind(),
            self.submissions,
            action.peer_id()
        );
        match self
            .registry
            .begin(&request_id, action.kind(), action.peer_id())
        {
            RegistryVerdict::Admitted => {}
            RegistryVerdict::Busy { existing } => {
                return Err(CapitalRefusal::IntentBusy { existing });
            }
        }

        // Durable intent + ACTIVE reservation, two-phase.
        let receipt = match self.deps.store.try_insert_capital_intent(CapitalIntent {
            request_id: request_id.clone(),
            kind: action.kind().to_string(),
            peer_id: action.peer_id().to_string(),
            channel_id: action.scid().map(String::from),
            amount_sats: action.reserved_sats(),
            reason: Some(action.reason().to_string()),
            submitted_at: now,
        }) {
            Ok(receipt) => receipt,
            Err(refused) => {
                self.registry.resolve(&request_id);
                return Err(CapitalRefusal::StoreAdmissionRefused(match refused {
                    StoreAdmissionRefused::QueueFull => {
                        "owner store queue at capacity; nothing was enqueued".to_string()
                    }
                    StoreAdmissionRefused::ActorGone => {
                        "observer store actor gone; nothing was enqueued".to_string()
                    }
                }));
            }
        };
        match receipt.blocking_within(OWNER_STORE_BUDGET) {
            StoreReceiptWait::Replied(Ok(_id)) => {}
            StoreReceiptWait::Replied(Err(e)) => {
                self.registry.resolve(&request_id);
                return Err(CapitalRefusal::StoreFailed(format!("{e:#}")));
            }
            StoreReceiptWait::OutcomeUnknown => {
                // The intent row may exist: keep the registry slot held
                // and let restart reconciliation own it.
                return Err(CapitalRefusal::StoreIntentOutcomeUnknown(
                    "the intent write was admitted but produced no reply within the store \
                     budget; the submission does NOT proceed and restart reconciliation owns \
                     any orphan intent"
                        .to_string(),
                ));
            }
        }

        // Fresh evidence revalidation (post-intent): failure settles a
        // clean refusal -- provably nothing reached the wire.
        if let Err(detail) = self.revalidate(&action) {
            let outcome = CapitalSubmitOutcome::CleanRefusal { detail };
            return self.settle_and_resolve(&request_id, &action, outcome);
        }

        // Execute + classify.
        let outcome = match &action {
            CapitalAction::Open {
                peer_id,
                amount_sats,
                ..
            } => classify_capital_submit(&adapters.fundchannel.fundchannel(
                peer_id,
                *amount_sats,
                None,
                None,
            )),
            CapitalAction::Close { scid, .. } => {
                classify_capital_submit(&adapters.close.close(scid, None))
            }
            CapitalAction::Defibrillate { peer_id, scid, .. } => adapters
                .defib
                .diagnostic_rebalance(peer_id, scid, DEFIB_AMOUNT_SATS),
        };
        self.settle_and_resolve(&request_id, &action, outcome)
    }

    /// Settle exactly once, then release the registry slot -- UNLESS the
    /// outcome is unknown: a quarantined pair stays blocked.
    fn settle_and_resolve(
        &mut self,
        request_id: &str,
        action: &CapitalAction,
        outcome: CapitalSubmitOutcome,
    ) -> Result<CapitalActionOutcome, CapitalRefusal> {
        let settle = settlement_for_capital(
            &outcome,
            request_id,
            action.reserved_sats(),
            (self.deps.clock)(),
        );
        if let Err(e) = self.settle(settle) {
            self.suspended = true;
            eprintln!(
                "revops: CAPITAL SETTLE PERSISTENCE FAILED for {request_id} ({e:#}); the owner \
                 is SUSPENDED until restart (observed outcome: {outcome:?})"
            );
            return Err(CapitalRefusal::SettlePersistenceUnknown {
                request_id: request_id.to_string(),
                detail: format!("{e:#}"),
            });
        }
        if !matches!(outcome, CapitalSubmitOutcome::OutcomeUnknown { .. }) {
            self.registry.resolve(request_id);
        }
        Ok(CapitalActionOutcome {
            request_id: request_id.to_string(),
            outcome,
        })
    }

    fn settle(&self, settle: CapitalSettle) -> anyhow::Result<()> {
        self.deps.store.blocking_settle_capital_intent(settle)
    }

    /// Step-7 revalidation over a LIVE `listpeerchannels` snapshot.
    fn revalidate(&self, action: &CapitalAction) -> Result<(), String> {
        let raw = self
            .deps
            .evidence
            .get_peer_channels()
            .map_err(|e| format!("revalidation snapshot unavailable: {e}"))?;
        let channels = raw
            .get("channels")
            .and_then(Value::as_array)
            .ok_or_else(|| "revalidation reply carries no channels array".to_string())?;
        match action {
            CapitalAction::Open { peer_id, .. } => {
                let live = channels.iter().any(|c| {
                    c.get("peer_id").and_then(Value::as_str) == Some(peer_id)
                        && !matches!(
                            c.get("state").and_then(Value::as_str),
                            Some("ONCHAIN") | Some("CLOSED")
                        )
                });
                if live {
                    return Err(format!(
                        "peer {peer_id} already has a live channel; the open target changed"
                    ));
                }
            }
            CapitalAction::Close { scid, .. } | CapitalAction::Defibrillate { scid, .. } => {
                let present = channels
                    .iter()
                    .any(|c| c.get("short_channel_id").and_then(Value::as_str) == Some(scid));
                if !present {
                    return Err(format!(
                        "channel {scid} is not in the live snapshot; the target vanished"
                    ));
                }
            }
        }
        Ok(())
    }

    fn reconcile_on_start(&mut self) -> Result<CapitalReconcileSummary, CapitalRefusal> {
        let unresolved = self
            .deps
            .store
            .blocking_unresolved_capital_intents()
            .map_err(|e| CapitalRefusal::StoreFailed(format!("{e:#}")))?;
        let mut summary = CapitalReconcileSummary::default();
        for intent in unresolved {
            let now = (self.deps.clock)();
            let (outcome, detail, status, txid, settled_sats) = self.reconcile_verdict(&intent);
            self.settle(CapitalSettle {
                request_id: intent.request_id.clone(),
                outcome: outcome.to_string(),
                outcome_detail: Some(detail),
                txid,
                reservation_status: status.to_string(),
                settled_sats,
                resolved_at: now,
            })
            .map_err(|e| CapitalRefusal::StoreFailed(format!("{e:#}")))?;
            match status {
                "settled" => summary.settled_success += 1,
                _ => summary.quarantined += 1,
            }
        }
        // Seed the duplicate registry from every quarantined pair: funds
        // that MAY be committed on-chain block their (kind, peer) slot
        // across restarts.
        let quarantined = self
            .deps
            .store
            .blocking_quarantined_capital_intents()
            .map_err(|e| CapitalRefusal::StoreFailed(format!("{e:#}")))?;
        self.registry = ActiveIntentRegistry::seeded_from(&quarantined);
        Ok(summary)
    }

    /// The definite/quarantine split for one orphan intent. Only
    /// positive visibility settles: an open whose peer appears in
    /// `listfunds`, a close whose channel appears in
    /// `listclosedchannels`. Everything else -- absence, defibs, lookup
    /// failures -- quarantines.
    fn reconcile_verdict(
        &self,
        intent: &UnresolvedCapitalIntent,
    ) -> (
        &'static str,
        String,
        &'static str,
        Option<String>,
        Option<i64>,
    ) {
        match intent.kind.as_str() {
            "open" => match self.deps.reconcile.listfunds() {
                Err(e) => (
                    "outcome_unknown",
                    format!("reconciliation lookup failed: {e}"),
                    "quarantined",
                    None,
                    None,
                ),
                Ok(funds) => {
                    let hit = funds["channels"].as_array().and_then(|channels| {
                        channels
                            .iter()
                            .find(|c| c["peer_id"].as_str() == Some(intent.peer_id.as_str()))
                            .cloned()
                    });
                    match hit {
                        Some(channel) => (
                            "success",
                            "reconciled visible in listfunds on restart".to_string(),
                            "settled",
                            channel["funding_txid"].as_str().map(String::from),
                            Some(intent.amount_sats),
                        ),
                        None => (
                            "outcome_unknown",
                            "open not visible in listfunds; may still be in mempool".to_string(),
                            "quarantined",
                            None,
                            None,
                        ),
                    }
                }
            },
            "close" => match self.deps.reconcile.listclosedchannels() {
                Err(e) => (
                    "outcome_unknown",
                    format!("reconciliation lookup failed: {e}"),
                    "quarantined",
                    None,
                    None,
                ),
                Ok(closed) => {
                    let seen = closed["closedchannels"].as_array().is_some_and(|entries| {
                        entries
                            .iter()
                            .any(|c| c["short_channel_id"].as_str() == intent.channel_id.as_deref())
                    });
                    if seen {
                        (
                            "success",
                            "reconciled visible in listclosedchannels on restart".to_string(),
                            "settled",
                            None,
                            Some(intent.amount_sats),
                        )
                    } else {
                        (
                            "outcome_unknown",
                            "close not visible in listclosedchannels; may still be in flight"
                                .to_string(),
                            "quarantined",
                            None,
                            None,
                        )
                    }
                }
            },
            other => (
                "outcome_unknown",
                format!("no reconciliation rail for kind {other}; quarantined"),
                "quarantined",
                None,
                None,
            ),
        }
    }

    fn debug(&self) -> Value {
        let reserved = self
            .deps
            .store
            .blocking_active_capital_reserved_sats(0)
            .ok();
        json!({
            "suspended": self.suspended,
            "adapters_assembled": self.deps.adapters.is_some(),
            "governor_assembled": self.deps.governor.is_some(),
            "submissions": self.submissions,
            "refusals": self.refusals,
            "reserved_sats": reserved,
        })
    }
}

/// Pre-cutover production stand-ins: every read fails typed, so even a
/// hypothetical gate bypass could never see healthy evidence.
pub struct UnassembledCapitalBudget;
impl BudgetDb for UnassembledCapitalBudget {
    fn positive_budget_evidence(
        &self,
        _now: i64,
    ) -> Result<crate::capital_boundaries::BudgetEvidence, String> {
        Err("capital budget evidence not assembled (pre-cutover)".to_string())
    }
}

pub struct UnassembledCapitalEvidence;
impl CapitalEvidenceRpc for UnassembledCapitalEvidence {
    fn get_peer_channels(&self) -> Result<Value, String> {
        Err("capital evidence not assembled (pre-cutover)".to_string())
    }
}
