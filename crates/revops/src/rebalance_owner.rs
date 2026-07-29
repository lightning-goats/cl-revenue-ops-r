//! Task 60 slice 4: the serialized rebalance owner.
//!
//! ONE dedicated OS thread owns every rebalance submission (the engine's
//! executor seam is synchronous, and the store calls from this thread use
//! the blocking siblings -- the same single-owner discipline as the fee
//! scheduler's cycle owner). Callers reach it through a bounded ingress
//! with per-message reply channels; there is no other path to a
//! submission.
//!
//! The submission rail, in order, every time:
//!
//! 1. **Suspension gate** -- a prior settle-persistence failure suspends
//!    every further submission until restart (the Task 59 F4 posture: a
//!    process that could not durably record a terminal result must not
//!    take on more).
//! 2. **Hard amount cap** -- refused regardless of `force` (defense in
//!    depth; the RPC layer enforces it too, py `cl-revenue-ops.py`
//!    DD2/P1-004).
//! 3. **Soft gates** (SKIPPED under `force`, py parity): per-pair
//!    cooldown, budget-window advisory (active + quarantined holds count
//!    against the window budget).
//! 4. **Fresh evidence revalidation** -- `listfunds` + `listpeerchannels`
//!    must answer NOW; any failure refuses fail-closed before anything
//!    durable happens.
//! 5. **Durable intent + ACTIVE reservation**, two-phase: admission
//!    refusal is a clean typed non-write; an admitted receipt that
//!    expires is `store_intent_outcome_unknown` and the submission NEVER
//!    proceeds (an intent row may exist; restart reconciliation owns it).
//! 6. **Execute** through the engine seam (dry-run gated today -- the
//!    live payment mode is constructible only at cutover).
//! 7. **Classify + settle exactly once** (`rebalance_execution`); a
//!    settle failure suspends the owner and surfaces typed.
//!
//! The engine-internal cycle path (`run_cycle`) executes inside the
//! frozen kernel, so its per-candidate submissions cannot be intercepted
//! for pre-submit intent rows without breaking Python parity; the owner
//! records each cycle execution as an atomic post-hoc attempt+terminal
//! row instead, and the cutover task that injects live payment capability
//! into the engine MUST also move the intent write ahead of the wire (it
//! re-wires that seam anyway). Today every cycle execution is dry-run
//! gated: zero payment RPCs.

use std::collections::HashMap;
use std::sync::Arc;

use revops_db::fee_runway::{RebalanceAttemptIntent, RebalanceSettle};
use revops_db::owner::{ObserverHandle, StoreAdmissionRefused, StoreReceiptWait};
use revops_rebalance::engine::CycleResult;
use revops_rebalance::facade::{CandidateExecutor, FacadeRpc};
use revops_rebalance::modes::engine_kwargs;
use revops_rebalance::types::RebalanceCandidate;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::rebalance_adapters::ReconcileLookup;
use crate::rebalance_execution::{classify_execution, settlement_for, RebalanceSubmitOutcome};

/// Store budget for the owner's blocking receipt waits -- the Task 59
/// floor (a single legitimate SQLite lock wait on an idle actor must
/// never be cut short).
const OWNER_STORE_BUDGET: std::time::Duration =
    std::time::Duration::from_millis(revops_db::BUSY_TIMEOUT_MS + 2_000);

/// Bounded ingress capacity: operator RPCs + reconciliation only -- there
/// is no high-frequency producer.
const OWNER_QUEUE_CAPACITY: usize = 16;

/// One manual submission's parameters (validated at the RPC layer; the
/// owner re-enforces the hard rails regardless).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualRebalanceParams {
    pub from_channel: String,
    pub to_channel: String,
    pub amount_sats: i64,
    pub max_fee_sats: Option<i64>,
    pub force: bool,
}

/// Owner configuration (the py config fields the owner-level gates read).
#[derive(Debug, Clone)]
pub struct RebalanceOwnerConfig {
    /// py `daily_budget_sats` (soft, force-bypassable).
    pub daily_budget_sats: i64,
    /// py `total_cost_budget_window_hours` (min 1 at use).
    pub budget_window_hours: i64,
    /// py `rebalance_max_amount` (HARD, never bypassed; 0 = unset).
    pub rebalance_max_amount: i64,
    /// Owner-level per-(source,dest) cooldown (soft, force-bypassable).
    pub pair_cooldown_seconds: i64,
}

/// Everything the owner thread needs. `engine: None` is the production
/// pre-cutover state: submissions refuse with the Python-parity
/// uninitialized arm while reconciliation (store + lookup only) works.
pub struct RebalanceOwnerDeps {
    pub engine: Option<Arc<dyn CandidateExecutor>>,
    pub evidence: Arc<dyn FacadeRpc>,
    pub store: ObserverHandle,
    pub reconcile: Arc<dyn ReconcileLookup>,
    pub config: RebalanceOwnerConfig,
    /// Injected wall clock (unix seconds) -- tests step it.
    pub clock: Box<dyn Fn() -> i64 + Send>,
}

/// Every typed way the owner refuses or fails a request. Stable codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceRefusal {
    /// A prior settle-persistence failure: no submissions until restart.
    Suspended,
    /// No assembled engine (pre-cutover production) -- the Python-parity
    /// "Rebalancer not initialized" arm.
    EngineNotAssembled,
    HardCapExceeded {
        requested_sats: i64,
        max_amount_sats: i64,
    },
    CooldownActive {
        remaining_seconds: i64,
    },
    BudgetExhausted {
        reserved_sats: i64,
        requested_sats: i64,
        budget_sats: i64,
    },
    EvidenceUnavailable(String),
    /// try_send refused: provably nothing enqueued, clean non-write.
    StoreAdmissionRefused(String),
    /// Admitted intent write with no reply in budget: the intent may
    /// exist, the submission must NOT proceed, reconciliation owns it.
    StoreIntentOutcomeUnknown(String),
    /// The intent write definitively failed (e.g. duplicate request id).
    StoreFailed(String),
    /// The terminal settle could not be recorded: the owner is now
    /// SUSPENDED; the observed outcome is carried for the operator.
    SettlePersistenceUnknown {
        request_id: String,
        detail: String,
    },
}

impl RebalanceRefusal {
    /// Stable machine-matchable code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Suspended => "rebalance_owner_suspended",
            Self::EngineNotAssembled => "rebalance_engine_not_assembled",
            Self::HardCapExceeded { .. } => "rebalance_hard_cap_exceeded",
            Self::CooldownActive { .. } => "rebalance_pair_cooldown",
            Self::BudgetExhausted { .. } => "rebalance_budget_exhausted",
            Self::EvidenceUnavailable(_) => "rebalance_evidence_unavailable",
            Self::StoreAdmissionRefused(_) => "store_admission_refused",
            Self::StoreIntentOutcomeUnknown(_) => "store_intent_outcome_unknown",
            Self::StoreFailed(_) => "rebalance_store_failed",
            Self::SettlePersistenceUnknown { .. } => "rebalance_settle_persistence_unknown",
        }
    }
}

/// One completed manual submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualOutcome {
    pub request_id: String,
    pub outcome: RebalanceSubmitOutcome,
}

/// One reconciliation pass's counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReconcileSummary {
    pub settled_success: usize,
    pub settled_failed: usize,
    pub quarantined: usize,
}

/// One owner cycle's summary (engine-internal executions recorded
/// post-hoc; see the module doc's cycle-path note).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CycleSummary {
    pub executions: usize,
    pub recorded: usize,
    pub skips: usize,
}

enum OwnerMsg {
    Manual {
        params: ManualRebalanceParams,
        reply: oneshot::Sender<Result<ManualOutcome, RebalanceRefusal>>,
    },
    RunCycle {
        reply: oneshot::Sender<Result<CycleSummary, RebalanceRefusal>>,
    },
    Debug {
        reply: oneshot::Sender<Value>,
    },
    ReconcileOnStart {
        reply: oneshot::Sender<Result<ReconcileSummary, RebalanceRefusal>>,
    },
}

/// Cheap cloneable handle to the owner thread.
#[derive(Clone)]
pub struct RebalanceOwnerHandle {
    tx: mpsc::Sender<OwnerMsg>,
}

impl RebalanceOwnerHandle {
    pub async fn manual(
        &self,
        params: ManualRebalanceParams,
    ) -> Result<ManualOutcome, RebalanceRefusal> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OwnerMsg::Manual { params, reply })
            .await
            .map_err(|_| RebalanceRefusal::Suspended)?;
        rx.await.map_err(|_| RebalanceRefusal::Suspended)?
    }

    pub async fn run_cycle(&self) -> Result<CycleSummary, RebalanceRefusal> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OwnerMsg::RunCycle { reply })
            .await
            .map_err(|_| RebalanceRefusal::Suspended)?;
        rx.await.map_err(|_| RebalanceRefusal::Suspended)?
    }

    pub async fn debug(&self) -> Option<Value> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(OwnerMsg::Debug { reply }).await.ok()?;
        rx.await.ok()
    }

    pub async fn reconcile_on_start(&self) -> Result<ReconcileSummary, RebalanceRefusal> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(OwnerMsg::ReconcileOnStart { reply })
            .await
            .map_err(|_| RebalanceRefusal::Suspended)?;
        rx.await.map_err(|_| RebalanceRefusal::Suspended)?
    }
}

/// Spawn the owner thread and return its handle.
pub fn spawn_rebalance_owner(deps: RebalanceOwnerDeps) -> RebalanceOwnerHandle {
    let (tx, mut rx) = mpsc::channel::<OwnerMsg>(OWNER_QUEUE_CAPACITY);
    std::thread::Builder::new()
        .name("revops-rebalance-owner".to_string())
        .spawn(move || {
            let mut owner = OwnerState {
                deps,
                suspended: false,
                cooldowns: HashMap::new(),
                submissions: 0,
                refusals: 0,
            };
            while let Some(msg) = rx.blocking_recv() {
                match msg {
                    OwnerMsg::Manual { params, reply } => {
                        let result = owner.manual(params);
                        if result.is_err() {
                            owner.refusals += 1;
                        }
                        let _ = reply.send(result);
                    }
                    OwnerMsg::RunCycle { reply } => {
                        let _ = reply.send(owner.run_cycle());
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
        .expect("spawn rebalance owner thread");
    RebalanceOwnerHandle { tx }
}

struct OwnerState {
    deps: RebalanceOwnerDeps,
    suspended: bool,
    cooldowns: HashMap<(String, String), i64>,
    submissions: u64,
    refusals: u64,
}

impl OwnerState {
    fn manual(&mut self, params: ManualRebalanceParams) -> Result<ManualOutcome, RebalanceRefusal> {
        if self.suspended {
            return Err(RebalanceRefusal::Suspended);
        }
        let Some(engine) = self.deps.engine.clone() else {
            return Err(RebalanceRefusal::EngineNotAssembled);
        };
        let now = (self.deps.clock)();

        // Hard cap: NEVER bypassed.
        let cap = self.deps.config.rebalance_max_amount;
        if cap > 0 && params.amount_sats > cap {
            return Err(RebalanceRefusal::HardCapExceeded {
                requested_sats: params.amount_sats,
                max_amount_sats: cap,
            });
        }

        // Soft gates, force-bypassable.
        if !params.force {
            let pair = (params.from_channel.clone(), params.to_channel.clone());
            if let Some(last) = self.cooldowns.get(&pair) {
                let elapsed = now - last;
                if elapsed < self.deps.config.pair_cooldown_seconds {
                    return Err(RebalanceRefusal::CooldownActive {
                        remaining_seconds: self.deps.config.pair_cooldown_seconds - elapsed,
                    });
                }
            }
            let window = self.deps.config.budget_window_hours.max(1) * 3_600;
            let reserved = self
                .deps
                .store
                .blocking_active_rebalance_reserved_sats(now - window)
                .map_err(|e| RebalanceRefusal::StoreFailed(format!("{e:#}")))?;
            if reserved + params.amount_sats > self.deps.config.daily_budget_sats {
                return Err(RebalanceRefusal::BudgetExhausted {
                    reserved_sats: reserved,
                    requested_sats: params.amount_sats,
                    budget_sats: self.deps.config.daily_budget_sats,
                });
            }
        }

        // Fresh evidence, fail-closed.
        self.deps
            .evidence
            .get_funds()
            .map_err(|e| RebalanceRefusal::EvidenceUnavailable(format!("listfunds: {e}")))?;
        self.deps
            .evidence
            .get_peer_channels()
            .map_err(|e| RebalanceRefusal::EvidenceUnavailable(format!("listpeerchannels: {e}")))?;

        // Durable intent + reservation, two-phase.
        self.submissions += 1;
        let request_id = format!(
            "manual-{now}-{}-{}-{}",
            self.submissions, params.from_channel, params.to_channel
        );
        let trigger = if params.force {
            "manual_force"
        } else {
            "manual"
        };
        let receipt = self
            .deps
            .store
            .try_insert_rebalance_attempt(RebalanceAttemptIntent {
                request_id: request_id.clone(),
                source_channel: params.from_channel.clone(),
                dest_channel: params.to_channel.clone(),
                amount_sats: params.amount_sats,
                max_fee_sats: params.max_fee_sats.unwrap_or(0),
                trigger: trigger.to_string(),
                submitted_at: now,
            })
            .map_err(|refused| match refused {
                StoreAdmissionRefused::QueueFull => RebalanceRefusal::StoreAdmissionRefused(
                    "owner store queue at capacity; nothing was enqueued".to_string(),
                ),
                StoreAdmissionRefused::ActorGone => RebalanceRefusal::StoreAdmissionRefused(
                    "observer store actor gone; nothing was enqueued".to_string(),
                ),
            })?;
        match receipt.blocking_within(OWNER_STORE_BUDGET) {
            StoreReceiptWait::Replied(Ok(_id)) => {}
            StoreReceiptWait::Replied(Err(e)) => {
                return Err(RebalanceRefusal::StoreFailed(format!("{e:#}")));
            }
            StoreReceiptWait::OutcomeUnknown => {
                return Err(RebalanceRefusal::StoreIntentOutcomeUnknown(
                    "the intent write was admitted but produced no reply within the store \
                     budget; the submission does NOT proceed and restart reconciliation owns \
                     any orphan intent"
                        .to_string(),
                ));
            }
        }

        // Execute through the engine seam (dry-run gated today).
        self.cooldowns.insert(
            (params.from_channel.clone(), params.to_channel.clone()),
            now,
        );
        let candidate = manual_candidate(&params);
        let kw = engine_kwargs("manual");
        let result = engine.execute_candidate(&candidate, -1, kw);

        // Classify + settle exactly once.
        let outcome = classify_execution(&result);
        let settle = settlement_for(&outcome, &request_id, (self.deps.clock)());
        if let Err(e) = self.settle(settle) {
            self.suspended = true;
            eprintln!(
                "revops: REBALANCE SETTLE PERSISTENCE FAILED for {request_id} ({e:#}); the \
                 owner is SUSPENDED until restart (observed outcome: {outcome:?})"
            );
            return Err(RebalanceRefusal::SettlePersistenceUnknown {
                request_id,
                detail: format!("{e:#}"),
            });
        }
        Ok(ManualOutcome {
            request_id,
            outcome,
        })
    }

    fn settle(&self, settle: RebalanceSettle) -> anyhow::Result<()> {
        self.deps.store.blocking_settle_rebalance_attempt(settle)
    }

    fn run_cycle(&mut self) -> Result<CycleSummary, RebalanceRefusal> {
        if self.suspended {
            return Err(RebalanceRefusal::Suspended);
        }
        let Some(engine) = self.deps.engine.clone() else {
            return Err(RebalanceRefusal::EngineNotAssembled);
        };
        let CycleResult {
            candidates: _,
            executions,
            audit_records,
        } = engine.run_cycle();
        let mut summary = CycleSummary {
            executions: executions.len(),
            recorded: 0,
            skips: audit_records.len(),
        };
        for (index, execution) in executions.iter().enumerate() {
            let now = (self.deps.clock)();
            let request_id = format!("cycle-{now}-{index}");
            // Post-hoc atomic record: intent + terminal in immediate
            // succession (see the module doc's cycle-path note).
            let inserted = self
                .deps
                .store
                .try_insert_rebalance_attempt(RebalanceAttemptIntent {
                    request_id: request_id.clone(),
                    source_channel: execution
                        .failure_data
                        .get("route_summary")
                        .map(|v| v.to_string())
                        .unwrap_or_default(),
                    dest_channel: String::new(),
                    amount_sats: execution.amount_sats,
                    max_fee_sats: 0,
                    trigger: "cycle".to_string(),
                    submitted_at: now,
                })
                .ok()
                .map(|receipt| receipt.blocking_within(OWNER_STORE_BUDGET));
            let outcome = classify_execution(execution);
            let settle = settlement_for(&outcome, &request_id, now);
            match (inserted, self.settle(settle)) {
                (Some(StoreReceiptWait::Replied(Ok(_))), Ok(())) => summary.recorded += 1,
                (_, Err(e)) => {
                    self.suspended = true;
                    eprintln!(
                        "revops: REBALANCE CYCLE SETTLE FAILED for {request_id} ({e:#}); \
                         owner SUSPENDED"
                    );
                    return Err(RebalanceRefusal::SettlePersistenceUnknown {
                        request_id,
                        detail: format!("{e:#}"),
                    });
                }
                _ => {
                    eprintln!(
                        "revops: rebalance cycle attempt {request_id} could not be recorded; \
                         cycle continues (dry-run evidence loss, loud)"
                    );
                }
            }
        }
        Ok(summary)
    }

    fn reconcile_on_start(&mut self) -> Result<ReconcileSummary, RebalanceRefusal> {
        let unresolved = self
            .deps
            .store
            .blocking_unresolved_rebalance_attempts()
            .map_err(|e| RebalanceRefusal::StoreFailed(format!("{e:#}")))?;
        let mut summary = ReconcileSummary::default();
        for attempt in unresolved {
            let now = (self.deps.clock)();
            let (outcome, detail, reservation_status, fee) = match &attempt.payment_hash {
                None => (
                    "outcome_unknown",
                    "no payment hash recorded; nothing to look up".to_string(),
                    "quarantined",
                    None,
                ),
                Some(hash) => match self.deps.reconcile.listsendpays(hash) {
                    Err(e) => (
                        "outcome_unknown",
                        format!("reconciliation lookup failed: {e}"),
                        "quarantined",
                        None,
                    ),
                    Ok(value) => {
                        let statuses: Vec<String> = value["payments"]
                            .as_array()
                            .map(|payments| {
                                payments
                                    .iter()
                                    .map(|p| p["status"].as_str().unwrap_or_default().to_string())
                                    .collect()
                            })
                            .unwrap_or_default();
                        if statuses.iter().any(|s| s == "complete") {
                            let fee = value["payments"]
                                .as_array()
                                .and_then(|payments| {
                                    payments.iter().find(|p| p["status"] == "complete")
                                })
                                .and_then(|p| {
                                    let sent = p["amount_sent_msat"].as_i64()?;
                                    let amount = p["amount_msat"].as_i64()?;
                                    Some((sent - amount).max(0) / 1000)
                                });
                            (
                                "success",
                                "reconciled complete on restart".to_string(),
                                "settled",
                                fee,
                            )
                        } else if !statuses.is_empty() && statuses.iter().all(|s| s == "failed") {
                            (
                                "rejected",
                                "reconciled failed on restart".to_string(),
                                "released",
                                None,
                            )
                        } else {
                            (
                                "outcome_unknown",
                                format!("reconciled non-terminal on restart: {statuses:?}"),
                                "quarantined",
                                None,
                            )
                        }
                    }
                },
            };
            self.settle(RebalanceSettle {
                request_id: attempt.request_id.clone(),
                outcome: outcome.to_string(),
                outcome_detail: Some(detail),
                fee_paid_sats: fee,
                payment_hash: attempt.payment_hash.clone(),
                reservation_status: reservation_status.to_string(),
                resolved_at: now,
            })
            .map_err(|e| RebalanceRefusal::StoreFailed(format!("{e:#}")))?;
            match reservation_status {
                "settled" => summary.settled_success += 1,
                "released" => summary.settled_failed += 1,
                _ => summary.quarantined += 1,
            }
        }
        Ok(summary)
    }

    fn debug(&self) -> Value {
        let now = (self.deps.clock)();
        let window = self.deps.config.budget_window_hours.max(1) * 3_600;
        let reserved = self
            .deps
            .store
            .blocking_active_rebalance_reserved_sats(now - window)
            .ok();
        json!({
            "suspended": self.suspended,
            "engine_assembled": self.deps.engine.is_some(),
            "submissions": self.submissions,
            "refusals": self.refusals,
            "reserved_sats_window": reserved,
            "budget_sats": self.deps.config.daily_budget_sats,
            "hard_cap_sats": self.deps.config.rebalance_max_amount,
            "pair_cooldowns": self.cooldowns.len(),
        })
    }
}

/// The minimal candidate a manual submission prices through the engine
/// (py `manual_rebalance` builds the same minimal shape; the engine
/// derives routing from live channel data, not from these hints).
fn manual_candidate(params: &ManualRebalanceParams) -> RebalanceCandidate {
    let max_fee = params.max_fee_sats.unwrap_or(0);
    RebalanceCandidate {
        source_candidates: vec![params.from_channel.clone()],
        to_channel: params.to_channel.clone(),
        primary_source_peer_id: String::new(),
        to_peer_id: String::new(),
        amount_sats: params.amount_sats,
        amount_msat: params.amount_sats * 1000,
        outbound_fee_ppm: 0,
        inbound_fee_ppm: 0,
        source_fee_ppm: 0,
        weighted_opp_cost_ppm: 0,
        spread_ppm: 0,
        max_budget_sats: max_fee,
        max_budget_msat: max_fee * 1000,
        max_fee_ppm: if params.amount_sats > 0 {
            (max_fee * 1_000_000) / params.amount_sats
        } else {
            0
        },
        expected_profit_sats: 0,
        liquidity_ratio: 0.0,
        dest_flow_state: "manual".to_string(),
        dest_turnover_rate: 0.0,
        source_turnover_rate: 0.0,
    }
}

/// Task 60: the pre-cutover evidence stand-in -- every read fails typed,
/// so a submission could never pass evidence revalidation even if the
/// engine gate were somehow bypassed. Replaced by a real facade impl at
/// engine assembly.
pub struct UnassembledEvidence;

impl FacadeRpc for UnassembledEvidence {
    fn get_funds(&self) -> Result<Value, revops_rebalance::router::RpcFailure> {
        Err(revops_rebalance::router::RpcFailure {
            message: "facade evidence not assembled (pre-cutover)".to_string(),
        })
    }
    fn get_peer_channels(&self) -> Result<Value, revops_rebalance::router::RpcFailure> {
        Err(revops_rebalance::router::RpcFailure {
            message: "facade evidence not assembled (pre-cutover)".to_string(),
        })
    }
    fn get_channels_source(
        &self,
        _source: &str,
    ) -> Result<Value, revops_rebalance::router::RpcFailure> {
        Err(revops_rebalance::router::RpcFailure {
            message: "facade evidence not assembled (pre-cutover)".to_string(),
        })
    }
    fn get_node_id(&self) -> Result<String, revops_rebalance::router::RpcFailure> {
        Err(revops_rebalance::router::RpcFailure {
            message: "facade evidence not assembled (pre-cutover)".to_string(),
        })
    }
}
