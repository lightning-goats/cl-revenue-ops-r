//! Task 67b slice 4: per-peer defib/close gate evidence. The frozen
//! kernel is FAIL-CLOSED on these -- a missing or stale gate skips the
//! action with a reason -- so supplying them is what lets the planner
//! actually defibrillate and close.

use std::collections::HashMap;

use revops::capital_gates::{build_gates, GateSources, PlannerActionRecord};

const NOW: i64 = 1_800_000_000;
const HOUR: i64 = 3_600;

fn sources<'a>(
    actions: &'a HashMap<String, Vec<PlannerActionRecord>>,
    modes: &'a HashMap<String, String>,
) -> GateSources<'a> {
    GateSources {
        recent_planner_actions: actions,
        rebalance_modes: modes,
        close_protected_peers: &[],
        now: NOW,
    }
}

/// Every gate is stamped with `observed_at = now`, because the kernel
/// treats stale evidence as a denial. A gate that forgot its timestamp
/// would be silently discarded as stale.
#[test]
fn gates_are_stamped_with_the_observation_time() {
    let peers = ["02aa".to_string()];
    let g = build_gates(&peers, sources(&HashMap::new(), &HashMap::new()));
    assert_eq!(g.defib_gates["02aa"].observed_at, NOW);
    assert_eq!(g.close_gates["02aa"].observed_at, NOW);
    assert_eq!(g.open_guards["02aa"].observed_at, NOW);
}

/// py `_check_cooldown`: any planner action for the peer inside 24h
/// blocks -- EXCEPT dry_run and failed ones, which are not real actions.
#[test]
fn cooldown_counts_real_actions_only() {
    let peers = ["02aa".to_string(), "02bb".to_string(), "02cc".to_string()];
    let mut actions = HashMap::new();
    actions.insert(
        "02aa".to_string(),
        vec![PlannerActionRecord {
            status: "executed".into(),
            created_at: NOW - 2 * HOUR,
        }],
    );
    // dry_run and failed must NOT block.
    actions.insert(
        "02bb".to_string(),
        vec![
            PlannerActionRecord {
                status: "dry_run".into(),
                created_at: NOW - HOUR,
            },
            PlannerActionRecord {
                status: "failed".into(),
                created_at: NOW - HOUR,
            },
        ],
    );
    // Outside the 24h window.
    actions.insert(
        "02cc".to_string(),
        vec![PlannerActionRecord {
            status: "executed".into(),
            created_at: NOW - 30 * HOUR,
        }],
    );

    let g = build_gates(&peers, sources(&actions, &HashMap::new()));
    assert!(
        g.defib_gates["02aa"].cooldown_blocked.is_some(),
        "a real action inside 24h blocks"
    );
    assert!(
        g.defib_gates["02bb"].cooldown_blocked.is_none(),
        "dry_run/failed are not real actions: {:?}",
        g.defib_gates["02bb"]
    );
    assert!(
        g.defib_gates["02cc"].cooldown_blocked.is_none(),
        "outside the window"
    );
}

/// py `_check_defib_allowed`: a defibrillation FILLS the channel, so
/// rebalance_mode disabled/source_only forbids it. no_close/protect tags
/// deliberately do NOT block defib (operator policy 2026-07-09: LN+
/// contract channels stay diagnosable).
#[test]
fn defib_policy_blocks_only_on_fill_forbidding_modes() {
    let peers = ["02aa".to_string(), "02bb".to_string(), "02cc".to_string()];
    let modes = HashMap::from([
        ("02aa".to_string(), "disabled".to_string()),
        ("02bb".to_string(), "source_only".to_string()),
        ("02cc".to_string(), "enabled".to_string()),
    ]);
    let g = build_gates(&peers, sources(&HashMap::new(), &modes));
    for blocked in ["02aa", "02bb"] {
        let reason = g.defib_gates[blocked]
            .policy_blocked
            .as_ref()
            .unwrap_or_else(|| panic!("{blocked} must be policy-blocked"));
        assert!(reason.contains("rebalance_mode"), "{reason}");
    }
    assert!(g.defib_gates["02cc"].policy_blocked.is_none());
}

/// A peer with NO policy entry defaults to `enabled` (py's
/// `str(mode or "enabled")`), so an absent policy does not silently
/// disable defibrillation for a healthy peer.
#[test]
fn absent_policy_defaults_to_enabled() {
    let peers = ["02zz".to_string()];
    let g = build_gates(&peers, sources(&HashMap::new(), &HashMap::new()));
    assert!(g.defib_gates["02zz"].policy_blocked.is_none());
}

/// Close protection blocks CLOSES but not defibrillation -- the same
/// operator policy that keeps contract channels diagnosable.
#[test]
fn close_protection_blocks_closes_but_not_defibs() {
    let peers = ["02aa".to_string()];
    let protected = ["02aa".to_string()];
    let g = build_gates(
        &peers,
        GateSources {
            recent_planner_actions: &HashMap::new(),
            rebalance_modes: &HashMap::new(),
            close_protected_peers: &protected,
            now: NOW,
        },
    );
    assert!(
        g.close_gates["02aa"].close_allowed_blocked.is_some(),
        "a protected peer must not be closed"
    );
    assert!(
        g.defib_gates["02aa"].policy_blocked.is_none(),
        "protection must NOT block defibrillation -- contract channels stay diagnosable"
    );
}

/// The cooldown applies to closes too, and open guards share it.
#[test]
fn cooldown_applies_across_gate_kinds() {
    let peers = ["02aa".to_string()];
    let actions = HashMap::from([(
        "02aa".to_string(),
        vec![PlannerActionRecord {
            status: "executed".into(),
            created_at: NOW - HOUR,
        }],
    )]);
    let g = build_gates(&peers, sources(&actions, &HashMap::new()));
    assert!(g.close_gates["02aa"].cooldown_blocked.is_some());
    assert!(g.open_guards["02aa"].blocked.is_some());
}
