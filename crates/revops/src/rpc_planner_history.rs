//! Pure response builder for `revenue-planner-history`.
//!
//! Port of `revenue_planner_history` (cl-revenue-ops.py:4709-4719): a thin
//! wrapper over `database.get_planner_actions(limit=limit)`
//! (`modules/database.py:7502-7515`, `SELECT * FROM planner_actions ORDER
//! BY created_at DESC LIMIT ?`) -- an audit log read, no planning logic.
//! The DB read is the caller's job; the `_clamp_query_limit` validation is
//! ported as `crate::rpc_planner_candidates::parse_query_limit` (shared
//! with `revenue-planner-candidates`, which needs the identical
//! `default=20, lo=1, hi=1000` clamp).

use serde_json::{json, Value};

/// Port of `revenue_planner_history`'s success path. `actions` must
/// already be `database.get_planner_actions(limit=limit)`'s rows, in the
/// DB's `ORDER BY created_at DESC` order.
pub fn build_planner_history(actions: Vec<Value>) -> Value {
    let count = actions.len();
    json!({
        "actions": actions,
        "count": count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_matches_action_list_length() {
        let rows = vec![
            json!({"id": 1, "action_type": "open", "status": "completed"}),
            json!({"id": 2, "action_type": "close", "status": "planned"}),
            json!({"id": 3, "action_type": "open", "status": "failed"}),
        ];
        let v = build_planner_history(rows);
        assert_eq!(v["count"], 3);
        assert_eq!(v["actions"][2]["status"], "failed");
    }

    #[test]
    fn empty_history_is_zero_not_omitted() {
        let v = build_planner_history(vec![]);
        assert_eq!(v["count"], 0);
        assert_eq!(v["actions"], json!([]));
    }
}
