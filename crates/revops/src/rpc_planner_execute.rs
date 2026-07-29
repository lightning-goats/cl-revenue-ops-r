//! Task 62 slice 5: `revenue-planner-execute` (py cl-revenue-ops.py:4696)
//! -- the ONLY write-shaped planner RPC.
//!
//! Parity notes:
//! - Python's uninitialized arm `{"error": "Capacity planner not
//!   initialized"}` is returned VERBATIM both when no owner exists and
//!   when the owner's adapters are unassembled (pre-cutover production
//!   is permanently in that arm, exactly like the rebalance RPCs'
//!   "Rebalancer not initialized"). Evidence assembly never runs there.
//! - The SUCCESS-arm response is Rust-typed (owner outcomes + honest
//!   evidence gaps), NOT byte-parity: Python composes its cycle summary
//!   from analyzer subsystems that do not exist before Task 67, and a
//!   fabricated lookalike would lie. Reachable only with an assembled
//!   owner (tests today; Task 69 at cutover).
//! - Execution order matches Python's `execute_cycle`: defibrillations,
//!   then closes, then opens -- capital preservation before deployment.

use serde_json::{json, Value};

use crate::capital_evidence::{AssembledEvidence, EvidenceRefusal};
use crate::capital_owner::{CapitalAction, CapitalOwnerHandle, CapitalRefusal};
use revops_capital::planner::cycle::{plan_cycle, CyclePlan};

/// Map one frozen-kernel plan onto owner actions, in Python's execution
/// order (defib -> close -> open).
pub fn planned_actions(plan: &CyclePlan) -> Vec<CapitalAction> {
    let mut actions = Vec::new();
    for defib in &plan.defibrillations {
        actions.push(CapitalAction::Defibrillate {
            peer_id: defib.peer_id.clone(),
            scid: defib.scid.clone(),
            reason: defib.reason.clone(),
        });
    }
    for close in &plan.closes {
        actions.push(CapitalAction::Close {
            peer_id: close.peer_id.clone(),
            scid: close.scid.clone(),
            reason: close.reason.clone(),
        });
    }
    for open in &plan.opens {
        actions.push(CapitalAction::Open {
            peer_id: open.peer_id.clone(),
            amount_sats: open.amount_sats,
            reason: format!("planned open ev={}", open.ev),
        });
    }
    actions
}

const UNINITIALIZED: &str = "Capacity planner not initialized";

/// `revenue-planner-execute`. `assemble` is called at most once, and
/// only after the owner proves its adapters are assembled -- production
/// pre-cutover never assembles evidence.
pub async fn handle_planner_execute(
    owner: Option<&CapitalOwnerHandle>,
    assemble: impl FnOnce() -> Result<AssembledEvidence, EvidenceRefusal>,
) -> Value {
    let Some(owner) = owner else {
        return json!({"error": UNINITIALIZED});
    };
    let Some(debug) = owner.debug().await else {
        return json!({"error": UNINITIALIZED});
    };
    if debug["adapters_assembled"] != json!(true) {
        return json!({"error": UNINITIALIZED});
    }

    let assembled = match assemble() {
        Ok(assembled) => assembled,
        Err(refusal) => {
            return json!({
                "status": "error",
                "code": refusal.code(),
                "error": format!("{refusal:?}"),
            });
        }
    };

    let plan = plan_cycle(&assembled.evidence);
    if plan.skipped {
        return json!({
            "status": "skipped",
            "skip_reason": plan.skip_reason,
            "skipped_reasons": plan.skipped_reasons,
        });
    }

    let actions = planned_actions(&plan);
    let planned = actions.len();
    let mut results = Vec::with_capacity(planned);
    for action in actions {
        let (kind, peer_id) = match &action {
            CapitalAction::Open { peer_id, .. } => ("open", peer_id.clone()),
            CapitalAction::Close { peer_id, .. } => ("close", peer_id.clone()),
            CapitalAction::Defibrillate { peer_id, .. } => ("defib", peer_id.clone()),
        };
        match owner.execute(action).await {
            Err(CapitalRefusal::AdaptersNotAssembled) => {
                // The capability vanished mid-cycle (should be
                // impossible; fail exactly like Python's arm).
                return json!({"error": UNINITIALIZED});
            }
            Err(refusal) => results.push(json!({
                "kind": kind,
                "peer_id": peer_id,
                "refused": refusal.code(),
                "detail": format!("{refusal:?}"),
            })),
            Ok(outcome) => results.push(json!({
                "kind": kind,
                "peer_id": peer_id,
                "request_id": outcome.request_id,
                "outcome": format!("{:?}", outcome.outcome),
            })),
        }
    }

    json!({
        "status": "success",
        "planned": planned,
        "results": results,
        "skipped_reasons": plan.skipped_reasons,
        "evidence_gaps": assembled
            .gaps
            .iter()
            .map(|gap| json!({"field": gap.field, "reason": gap.reason}))
            .collect::<Vec<_>>(),
    })
}
