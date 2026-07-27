//! Pure response builder for `revenue-r-capacity-report`.
//!
//! Port of `CapacityPlanner.generate_report`
//! (modules/capacity_planner.py:213-238), called by `revenue_capacity_report`
//! (cl-revenue-ops.py:4578-4593).
//!
//! Every field but `timestamp` is an HONEST GAP: `generate_report` builds
//! its `winners`/`losers` via `_identify_winners`/`_identify_losers`
//! (each hundreds of lines of live-channel-graph traversal, node-info RPC
//! calls, and portfolio-gate checks over `analyze_all_channels()` +
//! `flow.analyze_all_channels()`), then `_generate_recommendations`, then
//! `_get_mempool_recommendation()` (a live `feerates` RPC call). NONE of
//! that identification/recommendation engine is ported to
//! `revops-capital` yet -- that crate has the EV/scoring/gating PRIMITIVES
//! (`planner::ev`, `planner::scoring`, `planner::gates`,
//! `planner::dead_capital`) the engine would be built FROM, not the
//! engine's winner/loser assembly itself. Fabricating `winners`/`losers`
//! from those primitives without the real assembly logic would produce a
//! response that looks complete but silently disagrees with Python --
//! exactly what this project's honesty convention forbids. So every field
//! is `null` here, always gap-listed, until that assembly is ported.

use serde_json::{json, Value};

/// Port of `generate_report`'s response shape. `timestamp` is the only
/// live-computable field (`int(time.time())`, cl-revenue-ops.py doesn't
/// even need a DB/RPC call for it) -- injected by the caller, never read
/// from a clock here.
pub fn build_capacity_report(timestamp: i64) -> Value {
    json!({
        "timestamp": timestamp,
        "mempool_recommendation": Value::Null,
        "summary": Value::Null,
        "winners": Value::Null,
        "losers": Value::Null,
        "recommendations": Value::Null,
        "_gaps": [
            "mempool_recommendation",
            "summary",
            "winners",
            "losers",
            "recommendations",
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_wired_everything_else_is_an_honest_gap() {
        let v = build_capacity_report(1_700_000_000);
        assert_eq!(v["timestamp"], 1_700_000_000);
        for field in [
            "mempool_recommendation",
            "summary",
            "winners",
            "losers",
            "recommendations",
        ] {
            assert_eq!(
                v[field],
                Value::Null,
                "{field} must be null, not fabricated"
            );
        }
        let gaps: Vec<&str> = v["_gaps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g.as_str().unwrap())
            .collect();
        assert_eq!(
            gaps,
            vec![
                "mempool_recommendation",
                "summary",
                "winners",
                "losers",
                "recommendations"
            ]
        );
    }

    #[test]
    fn does_not_silently_omit_any_python_top_level_key() {
        // Control: every key `generate_report` returns must be present
        // here (even if null) -- an omitted key would be the "looks
        // complete but isn't" failure this convention forbids.
        let v = build_capacity_report(0);
        let obj = v.as_object().unwrap();
        for key in [
            "timestamp",
            "mempool_recommendation",
            "summary",
            "winners",
            "losers",
            "recommendations",
        ] {
            assert!(obj.contains_key(key), "missing key: {key}");
        }
    }
}
