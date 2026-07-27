//! Pure response builder for `revenue-planner-status`.
//!
//! Port of `CapacityPlanner.get_status` (`modules/capacity_planner.py:
//! 190-211`), which is entirely config-echo + two DB reads
//! (`db.get_planner_candidates()` sized for the pool count,
//! `db.get_planner_actions(limit=5)`) -- no scoring or candidate-discovery
//! logic. Both DB reads are the caller's job; this builder just formats
//! already-fetched values.

use serde_json::{json, Value};

/// Config subset `get_status` reads off `self.config` (py 194, 198-200),
/// plus the already-computed `candidate_pool_size`
/// (`len(db.get_planner_candidates())`, py 209) and `recent_actions`
/// (`db.get_planner_actions(limit=5)`, py 210) evidence.
pub struct PlannerStatusInputs {
    pub enabled: bool,
    pub dry_run: bool,
    pub execute_closes: bool,
    /// `getattr(cfg, 'planner_max_closes_per_cycle', 0)`, already coerced
    /// to a non-negative-safe int by the caller (py 194-196: a non-numeric
    /// or bool config value degrades to `0`).
    pub max_closes_per_cycle: i64,
    /// `len(db.get_planner_candidates())` (unfiltered pool size; py calls
    /// `get_planner_candidates()` with its `min_score=-999.0`/`limit=32`
    /// defaults, so this is capped at 32 by the same default the DB query
    /// uses -- not a true unbounded pool count).
    pub candidate_pool_size: i64,
    /// `db.get_planner_actions(limit=5)` rows, already JSON-shaped.
    pub recent_actions: Vec<Value>,
}

/// Port of `CapacityPlanner.get_status` (capacity_planner.py:190-211).
pub fn build_planner_status(i: &PlannerStatusInputs) -> Value {
    let close_execution_effective = i.execute_closes && i.max_closes_per_cycle > 0;
    json!({
        "enabled": i.enabled,
        "dry_run": i.dry_run,
        "execute_closes": i.execute_closes,
        "max_closes_per_cycle": i.max_closes_per_cycle,
        "close_execution_effective": close_execution_effective,
        "candidate_pool_size": i.candidate_pool_size,
        "recent_actions": i.recent_actions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> PlannerStatusInputs {
        PlannerStatusInputs {
            enabled: true,
            dry_run: false,
            execute_closes: true,
            max_closes_per_cycle: 2,
            candidate_pool_size: 7,
            recent_actions: vec![json!({"id": 1, "action_type": "open"})],
        }
    }

    #[test]
    fn passthrough_fields_match_python_shape() {
        let v = build_planner_status(&base());
        assert_eq!(v["enabled"], true);
        assert_eq!(v["dry_run"], false);
        assert_eq!(v["execute_closes"], true);
        assert_eq!(v["max_closes_per_cycle"], 2);
        assert_eq!(v["candidate_pool_size"], 7);
        assert_eq!(v["recent_actions"][0]["action_type"], "open");
    }

    #[test]
    fn close_execution_effective_requires_both_flag_and_nonzero_max() {
        let mut i = base();
        i.execute_closes = true;
        i.max_closes_per_cycle = 0;
        assert_eq!(
            build_planner_status(&i)["close_execution_effective"],
            false,
            "execute_closes=true is inert while max_closes_per_cycle=0"
        );

        i.max_closes_per_cycle = 3;
        assert_eq!(build_planner_status(&i)["close_execution_effective"], true);

        i.execute_closes = false;
        assert_eq!(
            build_planner_status(&i)["close_execution_effective"],
            false,
            "max_closes_per_cycle>0 alone does not enable close execution"
        );
    }
}
