//! Task 62 slice 2: the capital-execution boundaries.
//!
//! Three gates every capital submission passes BEFORE anything durable
//! or external happens, plus the four-way outcome vocabulary and its
//! settlement mapping. This module is deliberately EXECUTION-FREE: it
//! can express verdicts and settlements but names no transport, so
//! "retry on unknown" is not writable here (source-scan pinned).

use std::collections::HashMap;

use revops_db::fee_runway::{CapitalSettle, UnresolvedCapitalIntent};

/// Budget evidence freshness bound: older observations are STALE and
/// refuse (the audit's nullable/stale-evidence complaint).
pub const BUDGET_EVIDENCE_MAX_AGE_SECONDS: i64 = 60;

/// One positive-budget observation. `observed_at` is when the underlying
/// reads happened -- the caller enforces freshness, not the producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetEvidence {
    pub available_sats: i64,
    pub window_reserved_sats: i64,
    pub observed_at: i64,
}

/// The budget evidence producer. Implementations read the
/// production-schema budget/spend tables through the READ-ONLY actor
/// plus the observer-side capital reserve sum; `Err` is a failed read
/// (never coerced to "no budget pressure").
pub trait BudgetDb: Send + Sync {
    fn positive_budget_evidence(&self, now: i64) -> Result<BudgetEvidence, String>;
}

/// Typed budget refusals -- each fail-closed, each with a stable code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetRefusal {
    Unavailable(String),
    Stale { age_seconds: i64 },
    Exhausted { available_sats: i64 },
}

impl BudgetRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unavailable(_) => "capital_budget_evidence_unavailable",
            Self::Stale { .. } => "capital_budget_evidence_stale",
            Self::Exhausted { .. } => "capital_budget_exhausted",
        }
    }
}

/// The mandatory positive-evidence gate: read failure, staleness, and
/// non-positive availability all refuse typed; only fresh positive
/// evidence passes (and is returned for the caller's sizing checks).
pub fn check_budget_evidence(db: &dyn BudgetDb, now: i64) -> Result<BudgetEvidence, BudgetRefusal> {
    let evidence = db
        .positive_budget_evidence(now)
        .map_err(BudgetRefusal::Unavailable)?;
    let age = now - evidence.observed_at;
    if age > BUDGET_EVIDENCE_MAX_AGE_SECONDS {
        return Err(BudgetRefusal::Stale { age_seconds: age });
    }
    if evidence.available_sats <= 0 {
        return Err(BudgetRefusal::Exhausted {
            available_sats: evidence.available_sats,
        });
    }
    Ok(evidence)
}

/// Governor verdict for one capital action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GovernorVerdict {
    Authorized { reason_code: String },
    Denied { reason_code: String },
}

/// The governor consult before any capital submit. The production impl
/// arrives with authority assembly (Task 69); the owner refuses typed
/// without one.
pub trait GovernorFacade: Send + Sync {
    fn authorize(&self, kind: &str, amount_sats: i64) -> GovernorVerdict;
}

/// Registry admission verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryVerdict {
    Admitted,
    /// An identical (kind, peer) intent is in flight or unresolved.
    Busy {
        existing: String,
    },
}

/// In-process duplicate guard, seeded from the durable unresolved list
/// at startup. The DB's UNIQUE request_id is the durable rail; this is
/// the pre-submit fast path that also covers cross-restart unresolved
/// intents (which must reconcile before the pair is eligible again).
#[derive(Debug, Default)]
pub struct ActiveIntentRegistry {
    /// (kind, peer_id) -> request_id currently holding the slot.
    in_flight: HashMap<(String, String), String>,
}

impl ActiveIntentRegistry {
    pub fn seeded_from(unresolved: &[UnresolvedCapitalIntent]) -> Self {
        let mut registry = Self::default();
        for intent in unresolved {
            registry.in_flight.insert(
                (intent.kind.clone(), intent.peer_id.clone()),
                intent.request_id.clone(),
            );
        }
        registry
    }

    /// Claim the (kind, peer) slot for `request_id`, or report who holds
    /// it.
    pub fn begin(&mut self, request_id: &str, kind: &str, peer_id: &str) -> RegistryVerdict {
        let slot = (kind.to_string(), peer_id.to_string());
        if let Some(existing) = self.in_flight.get(&slot) {
            return RegistryVerdict::Busy {
                existing: existing.clone(),
            };
        }
        self.in_flight.insert(slot, request_id.to_string());
        RegistryVerdict::Admitted
    }

    /// Release whatever slot `request_id` holds (terminal settle or
    /// reconciliation).
    pub fn resolve(&mut self, request_id: &str) {
        self.in_flight.retain(|_, held| held != request_id);
    }
}

/// One capital submission's classified terminal, in the Task 62
/// contract's vocabulary. `OutcomeUnknown` is the fail-closed default
/// everywhere ambiguity exists: an on-chain submit whose reply was lost
/// MAY have broadcast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapitalSubmitOutcome {
    /// Provably nothing reached the wire (validation refusal, connect
    /// failure before any write).
    CleanRefusal {
        detail: String,
    },
    /// Explicit terminal refusal with proof (a definite CLN error).
    Rejected {
        detail: String,
    },
    Success {
        txid: Option<String>,
    },
    /// The reply was lost or ambiguous: quarantine, never retry.
    OutcomeUnknown {
        detail: String,
    },
}

/// Map one classified outcome onto its durable settlement: success
/// SETTLES (with sats + txid), rejection and clean refusal RELEASE,
/// unknown QUARANTINES (the funds may be committed on-chain).
pub fn settlement_for_capital(
    outcome: &CapitalSubmitOutcome,
    request_id: &str,
    amount_sats: i64,
    resolved_at: i64,
) -> CapitalSettle {
    match outcome {
        CapitalSubmitOutcome::Success { txid } => CapitalSettle {
            request_id: request_id.to_string(),
            outcome: "success".to_string(),
            outcome_detail: None,
            txid: txid.clone(),
            reservation_status: "settled".to_string(),
            settled_sats: Some(amount_sats),
            resolved_at,
        },
        CapitalSubmitOutcome::Rejected { detail } => CapitalSettle {
            request_id: request_id.to_string(),
            outcome: "rejected".to_string(),
            outcome_detail: Some(detail.clone()),
            txid: None,
            reservation_status: "released".to_string(),
            settled_sats: None,
            resolved_at,
        },
        CapitalSubmitOutcome::CleanRefusal { detail } => CapitalSettle {
            request_id: request_id.to_string(),
            outcome: "clean_refusal".to_string(),
            outcome_detail: Some(detail.clone()),
            txid: None,
            reservation_status: "released".to_string(),
            settled_sats: None,
            resolved_at,
        },
        CapitalSubmitOutcome::OutcomeUnknown { detail } => CapitalSettle {
            request_id: request_id.to_string(),
            outcome: "outcome_unknown".to_string(),
            outcome_detail: Some(detail.clone()),
            txid: None,
            reservation_status: "quarantined".to_string(),
            settled_sats: None,
            resolved_at,
        },
    }
}
