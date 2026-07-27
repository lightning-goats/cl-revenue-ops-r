//! Pure response builder for `revenue-planner-candidates`.
//!
//! Port of `revenue_planner_candidates` (cl-revenue-ops.py:4685-4698): a
//! thin wrapper over `database.get_planner_candidates(limit=limit)`
//! (`modules/database.py:7423-7439`, `SELECT * FROM planner_candidates
//! WHERE score >= ? ORDER BY score DESC LIMIT ?`, default `min_score=
//! -999.0`) -- no scoring or candidate-discovery logic of its own (that
//! lives, unported, in the rest of `CapacityPlanner`; see
//! `crates/revops-capital/ENTRYPOINTS.md`). The DB read and the
//! `_clamp_query_limit` validation (cl-revenue-ops.py:407-419, raises
//! `_ParamError`) are the caller's job; this builder only wraps the
//! already-fetched, already-`ORDER BY score DESC` rows with a `count`.

use serde_json::{json, Value};

/// Port of `_clamp_query_limit` (cl-revenue-ops.py:407-419): coerce to an
/// int and clamp into `[lo, hi]`; a non-coercible value is the `_ParamError`
/// branch (`{"error": "limit must be an integer, got <repr>"}` in Python --
/// approximated here as a stable, not byte-identical, message since Rust
/// has no `repr()` equivalent for an arbitrary JSON value). Shared by
/// `revenue-planner-candidates` and `revenue-planner-history`, both of
/// which call it with Python's `default=20, lo=1, hi=1000` defaults.
///
/// Deliberately narrower than Python's `int(x)` for one edge case (same
/// documented tradeoff as `crate::rpc_dashboard::parse_window_days`): a
/// JSON boolean is rejected here, where Python's `int(True) == 1` would
/// succeed (`bool` is an `int` subclass). No real RPC caller passes a bool
/// for `limit`.
pub fn parse_query_limit(
    raw: Option<&Value>,
    default: i64,
    lo: i64,
    hi: i64,
) -> Result<i64, Value> {
    let ivalue: i64 = match raw {
        None | Some(Value::Null) => default,
        Some(Value::Number(n)) => match n.as_i64() {
            Some(i) => i,
            None => match n.as_f64() {
                Some(f) => f.trunc() as i64,
                None => return Err(json!({"error": "limit must be an integer"})),
            },
        },
        Some(Value::String(s)) => s
            .trim()
            .parse::<i64>()
            .map_err(|_| json!({"error": format!("limit must be an integer, got {s:?}")}))?,
        Some(other) => {
            return Err(json!({"error": format!("limit must be an integer, got {other}")}))
        }
    };
    Ok(ivalue.clamp(lo, hi))
}

/// Port of `revenue_planner_candidates`'s success path. `candidates` must
/// already be `database.get_planner_candidates(limit=limit)`'s rows, in
/// the DB's `ORDER BY score DESC` order -- this builder does not re-sort.
pub fn build_planner_candidates(candidates: Vec<Value>) -> Value {
    let count = candidates.len();
    json!({
        "candidates": candidates,
        "count": count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_matches_candidate_list_length() {
        let rows = vec![
            json!({"peer_id": "aa", "score": 3.5, "source": "gossip"}),
            json!({"peer_id": "bb", "score": 1.0, "source": "manual"}),
        ];
        let v = build_planner_candidates(rows);
        assert_eq!(v["count"], 2);
        assert_eq!(v["candidates"][0]["peer_id"], "aa");
        assert_eq!(v["candidates"][1]["peer_id"], "bb");
    }

    #[test]
    fn empty_pool_is_zero_not_omitted() {
        let v = build_planner_candidates(vec![]);
        assert_eq!(v["count"], 0);
        assert_eq!(v["candidates"], json!([]));
    }

    #[test]
    fn query_limit_defaults_and_clamps() {
        assert_eq!(parse_query_limit(None, 20, 1, 1000).unwrap(), 20);
        assert_eq!(
            parse_query_limit(Some(&json!(5000)), 20, 1, 1000).unwrap(),
            1000
        );
        assert_eq!(
            parse_query_limit(Some(&json!(-5)), 20, 1, 1000).unwrap(),
            1,
            "a negative limit clamps to lo, never reaches SQLite as unbounded"
        );
        assert_eq!(
            parse_query_limit(Some(&json!(50)), 20, 1, 1000).unwrap(),
            50
        );
    }

    #[test]
    fn query_limit_rejects_non_integer_input() {
        assert!(parse_query_limit(Some(&json!("nope")), 20, 1, 1000).is_err());
        assert!(parse_query_limit(Some(&json!(true)), 20, 1, 1000).is_err());
    }
}
