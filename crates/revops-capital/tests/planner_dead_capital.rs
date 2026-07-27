//! `planner::dead_capital` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kind `"dead_capital_stage"`,
//! generated from the REAL `CapacityPlanner._build_dead_capital_loser`
//! stage machine with `_close_protection_reason` /
//! `_dead_capital_defib_attempted` monkeypatched to injected values —
//! isolating the pure state-transition logic this module ports).

use revops_capital::planner::dead_capital::{
    advance_dead_capital_stage, DeadCapitalInput, DeadCapitalStage, DeadCapitalStageRow,
};
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/capital/planner/kernels.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn scenarios() -> Vec<Value> {
    let fx = fixture();
    fx["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == "dead_capital_stage")
        .cloned()
        .collect()
}

#[test]
fn all_scenarios_present() {
    assert_eq!(
        scenarios().len(),
        12,
        "expected 12 dead_capital_stage scenarios"
    );
}

#[test]
fn matches_python_across_all_scenarios() {
    for case in scenarios() {
        let name = case["name"].as_str().unwrap();
        let stage_row_in = &case["input"]["stage_row"];
        let stage = stage_row_in
            .get("stage")
            .and_then(|v| v.as_str())
            .unwrap_or("none");
        let entered_at = stage_row_in
            .get("entered_at")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let opener = case["input"]["opener"].as_str().unwrap();
        let close_protection = case["input"]["close_protection"].as_str();
        let defib_attempted = case["input"]["defib_attempted"].as_bool().unwrap();
        let now = case["input"]["now"].as_i64().unwrap();

        let input = DeadCapitalInput {
            current: DeadCapitalStageRow {
                stage: DeadCapitalStage::parse(stage),
                entered_at,
            },
            opener,
            close_protection,
            defib_attempted,
            // The fixture's real-Python driver always stubs
            // `_record_fee_reduce_delegation` to succeed (StubDatabase
            // records unconditionally) — so the `none -> fee_reduction`
            // transition's `persist` gate was exercised as "recorded".
            fee_reduce_delegation_recorded: true,
            now,
        };

        let decision = advance_dead_capital_stage(&input);
        let expected = &case["output"];

        assert_eq!(
            decision.action.as_str(),
            expected["action"].as_str().unwrap(),
            "{name}: action"
        );
        assert_eq!(
            decision.stage.as_str(),
            expected["stage"].as_str().unwrap(),
            "{name}: stage"
        );
        match expected["close_protection_out"].as_str() {
            Some(reason) => {
                let actual_reason = if decision.close_blocked {
                    close_protection
                } else {
                    None
                };
                assert_eq!(actual_reason, Some(reason), "{name}: close_protection_out");
            }
            None => {
                assert!(
                    !decision.close_blocked,
                    "{name}: expected close_protection_out None"
                );
            }
        }
    }
}

/// Control: the SAME stage row produces a DIFFERENT decision depending on
/// whether the channel is protected — proves the protection gate is
/// actually wired into the transition, not a no-op.
#[test]
fn protection_changes_the_outcome_at_the_same_timeout() {
    let base = DeadCapitalInput {
        current: DeadCapitalStageRow {
            stage: DeadCapitalStage::Defibrillation,
            entered_at: 1_000_000,
        },
        opener: "local",
        close_protection: None,
        defib_attempted: true,
        fee_reduce_delegation_recorded: true,
        now: 1_000_000 + 25 * 3600,
    };
    let unprotected = advance_dead_capital_stage(&base);
    assert_eq!(unprotected.stage, DeadCapitalStage::Close);

    let mut protected = base;
    protected.close_protection = Some("inbound_gateway_protected");
    let protected_decision = advance_dead_capital_stage(&protected);
    assert_eq!(protected_decision.stage, DeadCapitalStage::Defibrillation);
    assert!(protected_decision.close_blocked);
}

/// Control: without a fee-reduce delegation recorded, `persist` must be
/// `false` even though the action/stage are still reported (py 1285-1286:
/// the ACTION is `FEE_REDUCE` either way, only the DB write is gated).
#[test]
fn unstaged_without_delegation_recorded_does_not_persist() {
    let input = DeadCapitalInput {
        current: DeadCapitalStageRow::default(),
        opener: "local",
        close_protection: None,
        defib_attempted: false,
        fee_reduce_delegation_recorded: false,
        now: 1_000_000,
    };
    let decision = advance_dead_capital_stage(&input);
    assert_eq!(decision.stage, DeadCapitalStage::FeeReduction);
    assert!(!decision.persist);

    let mut recorded = input;
    recorded.fee_reduce_delegation_recorded = true;
    let decision2 = advance_dead_capital_stage(&recorded);
    assert_eq!(decision2.stage, DeadCapitalStage::FeeReduction);
    assert!(decision2.persist);
}
