//! Task 60 slice 2: the typed rebalance submission-outcome rail.
//!
//! Maps the executor's `ExecutionResult` encoding onto the four-way
//! submission vocabulary the Task 60 contract pins, and each outcome onto
//! its durable settlement. Two structural properties this module carries:
//!
//! - **Fail-closed classification.** Only PROVEN shapes classify away
//!   from unknown: `success=true`, `payment_pending=true`, an error
//!   string carrying one of the executor's pre-write prefixes, or the
//!   terminal sendpay-failure prefix. Anything else — including error
//!   shapes a future executor version might add — defaults to
//!   [`RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit`].
//! - **Execution-free.** This module can express settlements but not
//!   submissions: it names no executor entry point, so "resubmit on
//!   unknown" is not writable here (pinned by a source scan in
//!   `tests/rebalance_execution.rs`).

use revops_db::fee_runway::RebalanceSettle;
use revops_rebalance::errors::{
    NATIVE_ROUTE_INVALID_PREFIX, NATIVE_ROUTE_OVER_BUDGET_PREFIX, NATIVE_SENDPAY_ERROR_PREFIX,
};
use revops_rebalance::executor::{DRYRUN_GATE_SENDPAY_DISABLED, NATIVE_INVOICE_ERROR_PREFIX};
use revops_rebalance::types::ExecutionResult;

/// One submission's classified terminal, in the Task 60 contract's exact
/// vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceSubmitOutcome {
    /// Provably nothing reached the wire: route validation, the dry-run
    /// gate, or a malformed invoice response — all before any HTLC.
    CleanFailureBeforeWrite { detail: String },
    /// The payment attempt failed with terminal proof (a definite
    /// sendpay/waitsendpay failure): no funds moved, reservation
    /// releasable.
    Rejected { detail: String },
    Success {
        fee_sats: i64,
        fee_msat: i64,
        hops: i64,
        parts: i64,
    },
    /// The HTLC may still settle (waitsendpay timeout / pending, or any
    /// unrecognized shape — fail-closed). The reservation must be
    /// QUARANTINED, never released, and never resubmitted.
    OutcomeUnknownAfterSubmit {
        payment_hash: Option<String>,
        detail: String,
    },
}

/// The executor's provably-pre-write error prefixes (see
/// `revops_rebalance::executor`'s construction sites: `validate_route`,
/// the DryRun gate, and the malformed-invoice path all return before any
/// payment write).
const PRE_WRITE_PREFIXES: &[&str] = &[
    NATIVE_ROUTE_INVALID_PREFIX,
    NATIVE_ROUTE_OVER_BUDGET_PREFIX,
    NATIVE_INVOICE_ERROR_PREFIX,
];

/// Classify one execution result. Fail-closed: unknown is the default,
/// not a residual error branch.
pub fn classify_execution(result: &ExecutionResult) -> RebalanceSubmitOutcome {
    if result.success {
        return RebalanceSubmitOutcome::Success {
            fee_sats: result.fee_sats,
            fee_msat: result.fee_msat,
            hops: result.hops,
            parts: result.parts,
        };
    }
    if result.payment_pending {
        return RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit {
            payment_hash: result.payment_hash.clone(),
            detail: result
                .error
                .clone()
                .unwrap_or_else(|| "payment pending with no error detail".to_string()),
        };
    }
    if let Some(error) = &result.error {
        if error == DRYRUN_GATE_SENDPAY_DISABLED
            || PRE_WRITE_PREFIXES
                .iter()
                .any(|prefix| error.starts_with(prefix))
        {
            return RebalanceSubmitOutcome::CleanFailureBeforeWrite {
                detail: error.clone(),
            };
        }
        if error.starts_with(NATIVE_SENDPAY_ERROR_PREFIX) {
            return RebalanceSubmitOutcome::Rejected {
                detail: error.clone(),
            };
        }
        return RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit {
            payment_hash: result.payment_hash.clone(),
            detail: format!("unrecognized failure shape (fail-closed): {error}"),
        };
    }
    RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit {
        payment_hash: result.payment_hash.clone(),
        detail: "no success, no pending, no error: unclassifiable result (fail-closed)".to_string(),
    }
}

/// Map one classified outcome onto its durable settlement row. The
/// reservation policy is the contract: success SETTLES with the paid fee,
/// rejection and clean failure RELEASE, unknown QUARANTINES (the money
/// may be gone — releasing would let the budget over-spend).
pub fn settlement_for(
    outcome: &RebalanceSubmitOutcome,
    request_id: &str,
    resolved_at: i64,
) -> RebalanceSettle {
    match outcome {
        RebalanceSubmitOutcome::Success {
            fee_sats,
            fee_msat: _,
            hops: _,
            parts: _,
        } => RebalanceSettle {
            request_id: request_id.to_string(),
            outcome: "success".to_string(),
            outcome_detail: None,
            fee_paid_sats: Some(*fee_sats),
            payment_hash: None,
            reservation_status: "settled".to_string(),
            resolved_at,
        },
        RebalanceSubmitOutcome::Rejected { detail } => RebalanceSettle {
            request_id: request_id.to_string(),
            outcome: "rejected".to_string(),
            outcome_detail: Some(detail.clone()),
            fee_paid_sats: None,
            payment_hash: None,
            reservation_status: "released".to_string(),
            resolved_at,
        },
        RebalanceSubmitOutcome::CleanFailureBeforeWrite { detail } => RebalanceSettle {
            request_id: request_id.to_string(),
            outcome: "clean_failure_before_write".to_string(),
            outcome_detail: Some(detail.clone()),
            fee_paid_sats: None,
            payment_hash: None,
            reservation_status: "released".to_string(),
            resolved_at,
        },
        RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit {
            payment_hash,
            detail,
        } => RebalanceSettle {
            request_id: request_id.to_string(),
            outcome: "outcome_unknown".to_string(),
            outcome_detail: Some(detail.clone()),
            fee_paid_sats: None,
            payment_hash: payment_hash.clone(),
            reservation_status: "quarantined".to_string(),
            resolved_at,
        },
    }
}
