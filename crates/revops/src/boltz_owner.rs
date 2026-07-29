//! Task 63 slices 4+5: the serialized Boltz owner.
//!
//! ONE dedicated OS thread owns every Boltz submission (manual actions,
//! cycle arms, reconciliation) -- the task-60/62 owner discipline:
//! bounded ingress, blocking store siblings, suspension on
//! settle-persistence failure.
//!
//! The submission rail, in order, every time:
//!
//! 1. **Suspension gate.**
//! 2. **Capability gate** -- no [`BoltzActionCapability`] (pre-cutover
//!    production, until Task 69) is the typed
//!    `boltz_capability_not_assembled` refusal; the RPC layer maps it to
//!    Python's verbatim "Boltz CLI integration not initialized" arm.
//! 3. **Governor consult** -- absent or denying refuses typed.
//! 4. **Pending-swap gate** (py `_boltz_pending_swap_count`, the
//!    strongest overlap guard): durable unresolved + QUARANTINED
//!    attempts must be zero AND the live `listswaps` snapshot (query
//!    transport; unreadable refuses fail-closed) must show zero
//!    non-terminal, non-ignored swaps -- unless `allow_concurrent_swaps`.
//!    A quarantined attempt therefore blocks every later submission:
//!    structurally no resubmit past an unknown outcome.
//! 5. **Fee budget** -- estimated fee against the window budget net of
//!    ACTIVE + QUARANTINED holds; structural loop-outs additionally pass
//!    the structural envelope (unreadable spend evidence refuses).
//! 6. **Durable cooldown** check + pre-claim (survives restarts -- the
//!    Python in-memory gap).
//! 7. **Durable attempt + ACTIVE fee reservation**, two-phase, BEFORE
//!    any spawn (test-pinned: the transport itself asserts the row).
//! 8. **Execute** through the capability's armed transport.
//! 9. **Classify + settle exactly once**; unknown QUARANTINES (fee held,
//!    pending gate blocks); committed journals the swap durably;
//!    cooldown kept for committed/unknown, restored otherwise
//!    (`autocycle::cooldown_after_attempt`). Settle failure suspends.
//!
//! Reconcile-on-start: an unresolved attempt means the process died
//! between spawn and settle. Its swap id was never recorded (create
//! replies carry the id), so NOTHING can prove the swap's absence -- it
//! quarantines, keeping its fee hold and its pending-gate block. (The
//! plan's "positive visibility settles" arm is unreachable for creates
//! and is deliberately not pretended; disclosed.)

use std::sync::Arc;

use revops_boltz::autocycle::{
    cooldown_after_attempt, cooldown_check, select_boltz_auto_cycle_mode, BoltzMode,
    SwapAttemptOutcome,
};
use revops_boltz::cli::{run_json, BoltzCli};
use revops_boltz::commands::{self, ActionOutcome};
use revops_boltz::execution::ExecutionMode;
use revops_boltz::parsing::extract_swap_list;
use revops_boltz::state::is_terminal_swap;
use revops_db::fee_runway::{BoltzAttempt, BoltzSettle};
use revops_db::owner::{ObserverHandle, StoreAdmissionRefused, StoreReceiptWait};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};

use crate::boltz_boundaries::{
    classify_boltz_create, classify_boltz_manual, settlement_for_boltz, BoltzActionCapability,
    BoltzSubmitOutcome,
};
use crate::capital_boundaries::{GovernorFacade, GovernorVerdict};

/// Store budget for blocking receipt waits (Task 59 floor).
const OWNER_STORE_BUDGET: std::time::Duration =
    std::time::Duration::from_millis(revops_db::BUSY_TIMEOUT_MS + 2_000);

const OWNER_QUEUE_CAPACITY: usize = 16;

/// py create timeout floor: `max(cfg.timeout_seconds, 120)`
/// (boltz_manager.py:1959). Production uses this; the config field lets
/// the e2e proof drive the ambiguity path without a 2-minute test.
pub const CREATE_TIMEOUT_FLOOR_SECS: u64 = 120;

/// Manual-action timeout (py cfg default).
const MANUAL_TIMEOUT_SECS: u64 = 60;

/// The structural-spend evidence producer (py
/// `database.get_category_spend_sats("boltz", subcategory="structural")`).
/// `Err` is a failed read, never "no structural spend".
pub trait StructuralSpendDb: Send + Sync {
    fn structural_spend_sats_24h(&self) -> Result<i64, String>;
}

/// Pre-cutover production stand-in: fails typed.
pub struct UnassembledStructuralSpend;
impl StructuralSpendDb for UnassembledStructuralSpend {
    fn structural_spend_sats_24h(&self) -> Result<i64, String> {
        Err("boltz structural spend evidence not assembled (pre-cutover)".to_string())
    }
}

/// Owner configuration (the py config fields the rail reads).
#[derive(Debug, Clone)]
pub struct BoltzOwnerConfig {
    /// py `boltz_daily_budget_sats` (default 3000).
    pub daily_budget_sats: i64,
    pub budget_window_hours: i64,
    /// py structural envelope (0 = structural credits disabled).
    pub structural_envelope_sats: i64,
    /// py `allow_concurrent_swaps` (default false).
    pub allow_concurrent_swaps: bool,
    pub default_cooldown_seconds: i64,
    /// py `boltz_auto_cycle_enabled`.
    pub auto_cycle_enabled: bool,
    /// Create-call timeout. Production passes
    /// [`CREATE_TIMEOUT_FLOOR_SECS`] (py's `max(cfg, 120)`); tests lower
    /// it to exercise the ambiguity path.
    pub create_timeout_secs: u64,
}

/// Everything the owner thread needs. `capability: None` is the
/// production pre-cutover state.
pub struct BoltzOwnerDeps {
    pub capability: Option<Arc<BoltzActionCapability>>,
    pub governor: Option<Arc<dyn GovernorFacade>>,
    /// The QUERY transport (read-only allowlist; production-constructible).
    pub query: Arc<dyn BoltzCli + Send + Sync>,
    pub structural: Arc<dyn StructuralSpendDb>,
    pub store: ObserverHandle,
    pub config: BoltzOwnerConfig,
    pub clock: Box<dyn Fn() -> i64 + Send>,
}

/// One manual Boltz action, rail-shaped. `estimated_fee_sats` comes from
/// the caller's quote through the query transport (the RPC layer's job).
#[derive(Debug, Clone, PartialEq)]
pub enum BoltzAction {
    LoopIn {
        wallet_name: String,
        currency: Option<String>,
        amount_sats: i64,
        channel_id: Option<String>,
        estimated_fee_sats: i64,
    },
    LoopOut {
        amount_sats: i64,
        currency: String,
        address: Option<String>,
        wallet_name: Option<String>,
        chan_ids: Vec<String>,
        routing_fee_limit_ppm: i64,
        channel_id: Option<String>,
        estimated_fee_sats: i64,
        structural: bool,
    },
    ChainSwap {
        amount_sats: i64,
        from_currency: String,
        to_currency: String,
        from_wallet_name: String,
        to_address: Option<String>,
        to_wallet_name: Option<String>,
        estimated_fee_sats: i64,
    },
    Refund {
        swap_id: String,
        destination: Option<String>,
    },
    Claim {
        swap_ids: Vec<String>,
        destination: Option<String>,
    },
    Withdraw {
        wallet_name: String,
        destination: String,
        currency: Option<String>,
        amount_sats: i64,
        sat_per_vbyte: Option<i64>,
        sweep: bool,
        confirm_sweep: bool,
    },
}

impl BoltzAction {
    fn kind(&self) -> &'static str {
        match self {
            Self::LoopIn { .. } => "loop_in",
            Self::LoopOut { .. } => "loop_out",
            Self::ChainSwap { .. } => "chainswap",
            Self::Refund { .. } => "refund",
            Self::Claim { .. } => "claim",
            Self::Withdraw { .. } => "withdraw",
        }
    }
    fn amount_sats(&self) -> i64 {
        match self {
            Self::LoopIn { amount_sats, .. }
            | Self::LoopOut { amount_sats, .. }
            | Self::ChainSwap { amount_sats, .. }
            | Self::Withdraw { amount_sats, .. } => *amount_sats,
            Self::Refund { .. } | Self::Claim { .. } => 0,
        }
    }
    /// The fee the reservation holds (swap-creating actions only).
    fn estimated_fee_sats(&self) -> i64 {
        match self {
            Self::LoopIn {
                estimated_fee_sats, ..
            }
            | Self::LoopOut {
                estimated_fee_sats, ..
            }
            | Self::ChainSwap {
                estimated_fee_sats, ..
            } => *estimated_fee_sats,
            Self::Refund { .. } | Self::Claim { .. } | Self::Withdraw { .. } => 0,
        }
    }
    fn channel_id(&self) -> Option<&str> {
        match self {
            Self::LoopIn { channel_id, .. } | Self::LoopOut { channel_id, .. } => {
                channel_id.as_deref()
            }
            _ => None,
        }
    }
    fn is_structural(&self) -> bool {
        matches!(
            self,
            Self::LoopOut {
                structural: true,
                ..
            }
        )
    }
    /// Swap-creating actions consume the fee budget; manual recovery and
    /// withdrawals do not.
    fn consumes_fee_budget(&self) -> bool {
        matches!(
            self,
            Self::LoopIn { .. } | Self::LoopOut { .. } | Self::ChainSwap { .. }
        )
    }
}

/// Every typed way the owner refuses or fails a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoltzRefusal {
    Suspended,
    CapabilityNotAssembled,
    GovernorNotAssembled,
    GovernorDenied {
        reason_code: String,
    },
    PendingSwapsBlocked {
        count: usize,
    },
    PendingEvidenceUnavailable(String),
    BudgetExhausted {
        reserved_sats: i64,
        requested_fee_sats: i64,
        budget_sats: i64,
    },
    StructuralEvidenceUnavailable(String),
    StructuralEnvelopeExhausted {
        spent_sats: i64,
        envelope_sats: i64,
    },
    CooldownActive {
        remaining_seconds: i64,
    },
    InvalidAction(String),
    StoreAdmissionRefused(String),
    StoreIntentOutcomeUnknown(String),
    StoreFailed(String),
    SettlePersistenceUnknown {
        request_id: String,
        detail: String,
    },
}

impl BoltzRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Suspended => "boltz_owner_suspended",
            Self::CapabilityNotAssembled => "boltz_capability_not_assembled",
            Self::GovernorNotAssembled => "boltz_governor_not_assembled",
            Self::GovernorDenied { .. } => "boltz_governor_denied",
            Self::PendingSwapsBlocked { .. } => "boltz_pending_swaps_blocked",
            Self::PendingEvidenceUnavailable(_) => "boltz_pending_evidence_unavailable",
            Self::BudgetExhausted { .. } => "boltz_budget_exhausted",
            Self::StructuralEvidenceUnavailable(_) => "boltz_structural_evidence_unavailable",
            Self::StructuralEnvelopeExhausted { .. } => "boltz_structural_envelope_exhausted",
            Self::CooldownActive { .. } => "boltz_cooldown_active",
            Self::InvalidAction(_) => "boltz_invalid_action",
            Self::StoreAdmissionRefused(_) => "store_admission_refused",
            Self::StoreIntentOutcomeUnknown(_) => "store_intent_outcome_unknown",
            Self::StoreFailed(_) => "boltz_store_failed",
            Self::SettlePersistenceUnknown { .. } => "boltz_settle_persistence_unknown",
        }
    }
}

/// One completed submission (a settled terminal, including quarantined
/// unknowns).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoltzActionResult {
    pub request_id: String,
    pub outcome: BoltzSubmitOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BoltzReconcileSummary {
    pub quarantined: usize,
}

enum OwnerMsg {
    Execute {
        action: BoltzAction,
        reply: oneshot::Sender<Result<BoltzActionResult, BoltzRefusal>>,
    },
    AutoCycleRunNow {
        force: bool,
        dry_run: bool,
        reply: oneshot::Sender<Value>,
    },
    Debug {
        reply: oneshot::Sender<Value>,
    },
    ReconcileOnStart {
        reply: oneshot::Sender<Result<BoltzReconcileSummary, BoltzRefusal>>,
    },
}

/// Cheap cloneable handle to the owner thread.
#[derive(Clone)]
pub struct BoltzOwnerHandle {
    tx: mpsc::Sender<OwnerMsg>,
}

impl BoltzOwnerHandle {
    pub async fn execute(&self, action: BoltzAction) -> Result<BoltzActionResult, BoltzRefusal> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OwnerMsg::Execute { action, reply })
            .await
            .map_err(|_| BoltzRefusal::Suspended)?;
        rx.await.map_err(|_| BoltzRefusal::Suspended)?
    }

    pub async fn auto_cycle_run_now(&self, force: bool, dry_run: bool) -> Value {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(OwnerMsg::AutoCycleRunNow {
                force,
                dry_run,
                reply,
            })
            .await
            .is_err()
        {
            return json!({"status": "error", "error": "boltz owner gone"});
        }
        rx.await
            .unwrap_or_else(|_| json!({"status": "error", "error": "boltz owner gone"}))
    }

    pub async fn debug(&self) -> Option<Value> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(OwnerMsg::Debug { reply }).await.ok()?;
        rx.await.ok()
    }

    pub async fn reconcile_on_start(&self) -> Result<BoltzReconcileSummary, BoltzRefusal> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OwnerMsg::ReconcileOnStart { reply })
            .await
            .map_err(|_| BoltzRefusal::Suspended)?;
        rx.await.map_err(|_| BoltzRefusal::Suspended)?
    }
}

/// Spawn the owner thread and return its handle.
pub fn spawn_boltz_owner(deps: BoltzOwnerDeps) -> BoltzOwnerHandle {
    let (tx, mut rx) = mpsc::channel::<OwnerMsg>(OWNER_QUEUE_CAPACITY);
    std::thread::Builder::new()
        .name("revops-boltz-owner".to_string())
        .spawn(move || {
            let mut owner = OwnerState {
                deps,
                suspended: false,
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
                    OwnerMsg::AutoCycleRunNow {
                        force,
                        dry_run,
                        reply,
                    } => {
                        let _ = reply.send(owner.auto_cycle_run_now(force, dry_run));
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
        .expect("spawn boltz owner thread");
    BoltzOwnerHandle { tx }
}

struct OwnerState {
    deps: BoltzOwnerDeps,
    suspended: bool,
    submissions: u64,
    refusals: u64,
}

impl OwnerState {
    fn execute(&mut self, action: BoltzAction) -> Result<BoltzActionResult, BoltzRefusal> {
        if self.suspended {
            return Err(BoltzRefusal::Suspended);
        }
        let Some(capability) = self.deps.capability.clone() else {
            return Err(BoltzRefusal::CapabilityNotAssembled);
        };
        let Some(governor) = self.deps.governor.clone() else {
            return Err(BoltzRefusal::GovernorNotAssembled);
        };
        let now = (self.deps.clock)();

        match governor.authorize(action.kind(), action.amount_sats()) {
            GovernorVerdict::Authorized { .. } => {}
            GovernorVerdict::Denied { reason_code } => {
                return Err(BoltzRefusal::GovernorDenied { reason_code });
            }
        }

        // Pending-swap gate (durable + live).
        if !self.deps.config.allow_concurrent_swaps {
            let pending = self.pending_swap_count()?;
            if pending > 0 {
                return Err(BoltzRefusal::PendingSwapsBlocked { count: pending });
            }
        }

        // Fee budget (swap-creating actions).
        if action.consumes_fee_budget() {
            let window = self.deps.config.budget_window_hours.max(1) * 3_600;
            let reserved = self
                .deps
                .store
                .blocking_active_boltz_reserved_sats(now - window)
                .map_err(|e| BoltzRefusal::StoreFailed(format!("{e:#}")))?;
            let fee = action.estimated_fee_sats();
            if reserved + fee > self.deps.config.daily_budget_sats {
                return Err(BoltzRefusal::BudgetExhausted {
                    reserved_sats: reserved,
                    requested_fee_sats: fee,
                    budget_sats: self.deps.config.daily_budget_sats,
                });
            }
            // Structural envelope, fail-closed (structural loop-outs).
            if action.is_structural() {
                let envelope = self.deps.config.structural_envelope_sats;
                let spent = self
                    .deps
                    .structural
                    .structural_spend_sats_24h()
                    .map_err(BoltzRefusal::StructuralEvidenceUnavailable)?;
                if envelope <= 0 || spent >= envelope {
                    return Err(BoltzRefusal::StructuralEnvelopeExhausted {
                        spent_sats: spent,
                        envelope_sats: envelope,
                    });
                }
            }
        }

        // Durable cooldown check + pre-claim.
        let cooldowns = self
            .deps
            .store
            .blocking_boltz_cooldowns()
            .map_err(|e| BoltzRefusal::StoreFailed(format!("{e:#}")))?;
        let prior_ts = action.channel_id().and_then(|channel| {
            cooldowns
                .iter()
                .find(|(id, _)| id == channel)
                .map(|(_, ts)| *ts)
        });
        if let (Some(channel), Some(last)) = (action.channel_id(), prior_ts) {
            let decision = cooldown_check(now, last, self.deps.config.default_cooldown_seconds);
            if !decision.allowed {
                return Err(BoltzRefusal::CooldownActive {
                    remaining_seconds: decision.remaining_sec,
                });
            }
            let _ = channel;
        }
        if let Some(channel) = action.channel_id() {
            self.deps
                .store
                .blocking_set_boltz_cooldown(channel, now)
                .map_err(|e| BoltzRefusal::StoreFailed(format!("{e:#}")))?;
        }

        // Durable attempt + ACTIVE fee reservation, two-phase, BEFORE
        // any spawn.
        self.submissions += 1;
        let request_id = format!("{}-{now}-{}", action.kind(), self.submissions);
        let digest = {
            let mut hasher = Sha256::new();
            hasher.update(action.kind().as_bytes());
            hasher.update(action.amount_sats().to_le_bytes());
            format!("{:x}", hasher.finalize())
        };
        let receipt = match self.deps.store.try_insert_boltz_attempt(BoltzAttempt {
            request_id: request_id.clone(),
            kind: action.kind().to_string(),
            channel_id: action.channel_id().map(String::from),
            amount_sats: action.amount_sats(),
            estimated_fee_sats: action.estimated_fee_sats(),
            argv_digest: digest,
            submitted_at: now,
        }) {
            Ok(receipt) => receipt,
            Err(refused) => {
                self.restore_cooldown(&action, prior_ts);
                return Err(BoltzRefusal::StoreAdmissionRefused(match refused {
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
                self.restore_cooldown(&action, prior_ts);
                return Err(BoltzRefusal::StoreFailed(format!("{e:#}")));
            }
            StoreReceiptWait::OutcomeUnknown => {
                // The attempt row may exist; reconciliation owns it. The
                // cooldown stays burned (fail-closed).
                return Err(BoltzRefusal::StoreIntentOutcomeUnknown(
                    "the attempt write was admitted but produced no reply within the store \
                     budget; the submission does NOT proceed and restart reconciliation owns \
                     any orphan attempt"
                        .to_string(),
                ));
            }
        }

        // Execute through the capability's armed transport.
        let outcome = self.run_action(capability.as_ref(), &action);

        // Cooldown discipline: committed/unknown keep the burn; clean
        // failures restore the prior slot (autocycle kernel semantics).
        let attempt_outcome = match &outcome {
            BoltzSubmitOutcome::Committed { .. } => SwapAttemptOutcome::Accepted,
            BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { .. } => SwapAttemptOutcome::Unknown,
            _ => SwapAttemptOutcome::RejectedOrError,
        };
        if let Some(channel) = action.channel_id() {
            let restored = cooldown_after_attempt(prior_ts.unwrap_or(0), now, attempt_outcome);
            if restored != now {
                let _ = self
                    .deps
                    .store
                    .blocking_set_boltz_cooldown(channel, restored);
            }
        }

        // Settle exactly once; journal committed swaps durably.
        let settle = settlement_for_boltz(
            &outcome,
            &request_id,
            action.estimated_fee_sats(),
            (self.deps.clock)(),
        );
        let committed_swap_id = match &outcome {
            BoltzSubmitOutcome::Committed { swap_id } => swap_id.clone(),
            _ => None,
        };
        if let Err(e) = self.deps.store.blocking_settle_boltz_attempt(settle) {
            self.suspended = true;
            eprintln!(
                "revops: BOLTZ SETTLE PERSISTENCE FAILED for {request_id} ({e:#}); the owner \
                 is SUSPENDED until restart (observed outcome: {outcome:?})"
            );
            return Err(BoltzRefusal::SettlePersistenceUnknown {
                request_id,
                detail: format!("{e:#}"),
            });
        }
        if let Some(swap_id) = committed_swap_id {
            let record = json!({
                "id": swap_id,
                "source": action.kind(),
                "recorded_at": (self.deps.clock)(),
            });
            if let Err(e) = self.deps.store.blocking_upsert_boltz_journal(
                &swap_id,
                record,
                action.kind(),
                (self.deps.clock)(),
            ) {
                eprintln!(
                    "revops: boltz journal write failed for {swap_id} ({e:#}); loud, not fatal"
                );
            }
        }
        Ok(BoltzActionResult {
            request_id,
            outcome,
        })
    }

    fn restore_cooldown(&self, action: &BoltzAction, prior_ts: Option<i64>) {
        if let Some(channel) = action.channel_id() {
            let restored = prior_ts.unwrap_or(0);
            let _ = self
                .deps
                .store
                .blocking_set_boltz_cooldown(channel, restored);
        }
    }

    /// The live half of the pending-swap gate plus the durable half.
    fn pending_swap_count(&self) -> Result<usize, BoltzRefusal> {
        let unresolved = self
            .deps
            .store
            .blocking_unresolved_boltz_attempts()
            .map_err(|e| BoltzRefusal::StoreFailed(format!("{e:#}")))?;
        let quarantined = self
            .deps
            .store
            .blocking_quarantined_boltz_attempts()
            .map_err(|e| BoltzRefusal::StoreFailed(format!("{e:#}")))?;
        let ignores: std::collections::BTreeSet<String> = self
            .deps
            .store
            .blocking_boltz_ignores()
            .map_err(|e| BoltzRefusal::StoreFailed(format!("{e:#}")))?
            .into_iter()
            .map(|(id, _, _)| id)
            .collect();
        let live = run_json(
            self.deps.query.as_ref(),
            &["listswaps", "--json"],
            MANUAL_TIMEOUT_SECS,
        )
        .map_err(|e| BoltzRefusal::PendingEvidenceUnavailable(e.to_string()))?;
        let live_pending = extract_swap_list(&live)
            .into_iter()
            .filter(|swap| {
                let id = swap
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                !ignores.contains(&id) && !is_terminal_swap(swap)
            })
            .count();
        Ok(unresolved.len() + quarantined.len() + live_pending)
    }

    fn run_action(
        &self,
        capability: &BoltzActionCapability,
        action: &BoltzAction,
    ) -> BoltzSubmitOutcome {
        let cli = capability.armed();
        match action {
            BoltzAction::LoopIn {
                wallet_name,
                currency,
                amount_sats,
                ..
            } => match commands::execute_loop_in(
                cli,
                ExecutionMode::Armed,
                wallet_name,
                currency.as_deref(),
                *amount_sats,
                self.deps.config.create_timeout_secs,
            ) {
                Ok(ActionOutcome::Executed(create)) => classify_boltz_create(&create),
                Ok(ActionOutcome::Preview { .. }) => BoltzSubmitOutcome::NotSubmitted {
                    detail: "unarmed preview (unreachable through the owner)".to_string(),
                },
                Err(argv_error) => BoltzSubmitOutcome::NotSubmitted {
                    detail: format!("argv validation refused: {argv_error:?}"),
                },
            },
            BoltzAction::LoopOut {
                amount_sats,
                currency,
                address,
                wallet_name,
                chan_ids,
                routing_fee_limit_ppm,
                ..
            } => match commands::execute_loop_out(
                cli,
                ExecutionMode::Armed,
                *amount_sats,
                Some(currency.as_str()),
                address.as_deref(),
                wallet_name.as_deref(),
                chan_ids,
                *routing_fee_limit_ppm,
                self.deps.config.create_timeout_secs,
            ) {
                Ok(ActionOutcome::Executed(create)) => classify_boltz_create(&create),
                Ok(ActionOutcome::Preview { .. }) => BoltzSubmitOutcome::NotSubmitted {
                    detail: "unarmed preview (unreachable through the owner)".to_string(),
                },
                Err(argv_error) => BoltzSubmitOutcome::NotSubmitted {
                    detail: format!("argv validation refused: {argv_error:?}"),
                },
            },
            BoltzAction::ChainSwap {
                amount_sats,
                from_currency,
                to_currency,
                from_wallet_name,
                to_address,
                to_wallet_name,
                ..
            } => match commands::execute_chain_swap(
                cli,
                ExecutionMode::Armed,
                *amount_sats,
                Some(from_currency.as_str()),
                Some(to_currency.as_str()),
                from_wallet_name,
                to_address.as_deref(),
                to_wallet_name.as_deref(),
                self.deps.config.create_timeout_secs,
            ) {
                Ok(ActionOutcome::Executed(create)) => classify_boltz_create(&create),
                Ok(ActionOutcome::Preview { .. }) => BoltzSubmitOutcome::NotSubmitted {
                    detail: "unarmed preview (unreachable through the owner)".to_string(),
                },
                Err(argv_error) => BoltzSubmitOutcome::NotSubmitted {
                    detail: format!("argv validation refused: {argv_error:?}"),
                },
            },
            BoltzAction::Refund {
                swap_id,
                destination,
            } => match commands::execute_refund(
                cli,
                ExecutionMode::Armed,
                swap_id,
                destination.as_deref(),
                MANUAL_TIMEOUT_SECS,
            ) {
                Ok(ActionOutcome::Executed(manual)) => classify_boltz_manual(&manual),
                Ok(ActionOutcome::Preview { .. }) => BoltzSubmitOutcome::NotSubmitted {
                    detail: "unarmed preview (unreachable through the owner)".to_string(),
                },
                Err(argv_error) => BoltzSubmitOutcome::NotSubmitted {
                    detail: format!("argv validation refused: {argv_error:?}"),
                },
            },
            BoltzAction::Claim {
                swap_ids,
                destination,
            } => match commands::execute_claim(
                cli,
                ExecutionMode::Armed,
                swap_ids,
                destination.as_deref(),
                MANUAL_TIMEOUT_SECS,
            ) {
                Ok(ActionOutcome::Executed(manual)) => classify_boltz_manual(&manual),
                Ok(ActionOutcome::Preview { .. }) => BoltzSubmitOutcome::NotSubmitted {
                    detail: "unarmed preview (unreachable through the owner)".to_string(),
                },
                Err(argv_error) => BoltzSubmitOutcome::NotSubmitted {
                    detail: format!("argv validation refused: {argv_error:?}"),
                },
            },
            BoltzAction::Withdraw {
                wallet_name,
                destination,
                currency,
                amount_sats,
                sat_per_vbyte,
                sweep,
                confirm_sweep,
            } => match commands::execute_withdraw(
                cli,
                ExecutionMode::Armed,
                wallet_name,
                destination,
                currency.as_deref().unwrap_or("BTC"),
                *amount_sats,
                *sat_per_vbyte,
                *sweep,
                *confirm_sweep,
                // The hard cap lives INSIDE the capability.
                capability.max_withdraw_sats(),
                MANUAL_TIMEOUT_SECS,
            ) {
                Ok(ActionOutcome::Executed(manual)) => classify_boltz_manual(&manual),
                Ok(ActionOutcome::Preview { .. }) => BoltzSubmitOutcome::NotSubmitted {
                    detail: "unarmed preview (unreachable through the owner)".to_string(),
                },
                Err(gate_error) => BoltzSubmitOutcome::NotSubmitted {
                    detail: format!("withdraw gate refused: {gate_error:?}"),
                },
            },
        }
    }

    fn auto_cycle_run_now(&mut self, force: bool, dry_run: bool) -> Value {
        if self.deps.capability.is_none() {
            // py: boltz manager absent/disabled.
            return json!({
                "status": "disabled",
                "reason": "boltz integration disabled",
                "trigger": "manual",
            });
        }
        if !self.deps.config.auto_cycle_enabled && !force {
            return json!({
                "status": "disabled",
                "reason": "boltz auto-cycle disabled by config",
                "trigger": "manual",
            });
        }
        if !self.deps.config.allow_concurrent_swaps {
            match self.pending_swap_count() {
                Ok(0) => {}
                Ok(count) => {
                    return json!({
                        "status": "blocked",
                        "reason": format!("{count} pending Boltz swap(s) detected"),
                        "trigger": "manual",
                    });
                }
                Err(refusal) => {
                    return json!({
                        "status": "error",
                        "code": refusal.code(),
                        "error": format!("{refusal:?}"),
                        "trigger": "manual",
                    });
                }
            }
        }
        // Candidate analytics (treasury status, balance recommendations)
        // are Task 67 owners; until they land the mode selector sees no
        // executable candidates and lands Idle -- surfaced, never
        // fabricated.
        let selection = select_boltz_auto_cycle_mode(false, 0, 0, 0);
        match selection.mode {
            BoltzMode::Idle => json!({
                "status": "idle",
                "reason": selection.reason,
                "dry_run": dry_run,
                "trigger": "manual",
                "candidate_evidence": "treasury/balance analytics land with Task 67",
            }),
            // Unreachable until Task 67 supplies candidates; kept total.
            BoltzMode::Treasury | BoltzMode::Balance => json!({
                "status": "skipped",
                "reason": "candidate execution requires Task 67 analytics evidence",
                "dry_run": dry_run,
                "trigger": "manual",
            }),
        }
    }

    fn reconcile_on_start(&mut self) -> Result<BoltzReconcileSummary, BoltzRefusal> {
        let unresolved = self
            .deps
            .store
            .blocking_unresolved_boltz_attempts()
            .map_err(|e| BoltzRefusal::StoreFailed(format!("{e:#}")))?;
        let mut summary = BoltzReconcileSummary::default();
        for attempt in unresolved {
            let now = (self.deps.clock)();
            // A create's swap id arrives only in the reply, so an
            // unresolved attempt has none recorded -- NOTHING can prove
            // the swap's absence. Quarantine, keep the fee hold, keep
            // the pending gate blocked.
            self.deps
                .store
                .blocking_settle_boltz_attempt(BoltzSettle {
                    request_id: attempt.request_id.clone(),
                    outcome: "outcome_unknown".to_string(),
                    outcome_detail: Some(
                        "process died between spawn and settle; no swap id was recorded, so \
                         absence is unprovable"
                            .to_string(),
                    ),
                    swap_id: None,
                    reservation_status: "quarantined".to_string(),
                    settled_sats: None,
                    resolved_at: now,
                })
                .map_err(|e| BoltzRefusal::StoreFailed(format!("{e:#}")))?;
            summary.quarantined += 1;
        }
        Ok(summary)
    }

    fn debug(&self) -> Value {
        let reserved = self.deps.store.blocking_active_boltz_reserved_sats(0).ok();
        json!({
            "suspended": self.suspended,
            "capability_assembled": self.deps.capability.is_some(),
            "governor_assembled": self.deps.governor.is_some(),
            "submissions": self.submissions,
            "refusals": self.refusals,
            "reserved_fee_sats": reserved,
            "auto_cycle_enabled": self.deps.config.auto_cycle_enabled,
        })
    }
}
