//! Pure response builder for `revenue-planner-candidate-sources`.
//!
//! Port of `CapacityPlanner.get_candidate_sources`
//! (`modules/capacity_planner.py:2306-2328`): groups the candidate pool
//! (`database.get_planner_candidates(min_score=-999.0, limit=100)`, py
//! 2312) by `source`, and returns a `peer_id`/`score`/`source` view sorted
//! by score descending. Pure grouping/sorting/rounding -- no scoring or
//! candidate-discovery logic (that lives, unported, in the rest of
//! `CapacityPlanner`). The DB read is the caller's job.

use revops_econ::pyfloat::py_round;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// One `planner_candidates` row's fields that `get_candidate_sources`
/// reads (py 2317-2319, 2325).
#[derive(Debug, Clone, PartialEq)]
pub struct CandidateRow {
    pub peer_id: String,
    pub score: f64,
    /// `c.get("source", "unknown")` (py 2318): the DB schema declares
    /// `source TEXT NOT NULL` (`modules/database.py:1320`), so a missing
    /// source should not occur in practice -- callers pass the DB value
    /// verbatim; this builder does not itself default a missing string
    /// (there is no `Option` here to default).
    pub source: String,
}

/// Port of `CapacityPlanner.get_candidate_sources`'s success path.
/// `candidates` must already be
/// `database.get_planner_candidates(min_score=-999.0, limit=100)`'s rows
/// (order does not matter -- this builder re-sorts by score itself, as
/// Python does at py 2326).
pub fn build_planner_candidate_sources(candidates: &[CandidateRow]) -> Value {
    let mut by_source: BTreeMap<&str, i64> = BTreeMap::new();
    for c in candidates {
        *by_source.entry(c.source.as_str()).or_insert(0) += 1;
    }

    let mut sorted: Vec<&CandidateRow> = candidates.iter().collect();
    // Python's `sorted(..., key=lambda x: x["score"], reverse=True)` is
    // stable; `sort_by` here is likewise stable, preserving input order
    // among equal scores.
    sorted.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let candidates_view: Vec<Value> = sorted
        .into_iter()
        .map(|c| {
            json!({
                "peer_id": c.peer_id,
                "score": py_round(c.score, 4),
                "source": c.source,
            })
        })
        .collect();

    json!({
        "total": candidates.len(),
        "by_source": Value::Object(by_source.into_iter().map(|(k, v)| (k.to_string(), json!(v))).collect()),
        "candidates": candidates_view,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(peer_id: &str, score: f64, source: &str) -> CandidateRow {
        CandidateRow {
            peer_id: peer_id.to_string(),
            score,
            source: source.to_string(),
        }
    }

    #[test]
    fn by_source_counts_group_correctly() {
        let rows = vec![
            row("aa", 1.0, "gossip"),
            row("bb", 2.0, "gossip"),
            row("cc", 3.0, "manual"),
        ];
        let v = build_planner_candidate_sources(&rows);
        assert_eq!(v["total"], 3);
        assert_eq!(v["by_source"]["gossip"], 2);
        assert_eq!(v["by_source"]["manual"], 1);
    }

    #[test]
    fn candidates_are_sorted_by_score_descending() {
        let rows = vec![
            row("low", 0.5, "a"),
            row("high", 9.0, "a"),
            row("mid", 3.0, "a"),
        ];
        let v = build_planner_candidate_sources(&rows);
        let ids: Vec<&str> = v["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["peer_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["high", "mid", "low"]);
    }

    #[test]
    fn score_is_rounded_to_four_decimals() {
        let rows = vec![row("aa", 1.0 / 3.0, "a")];
        let v = build_planner_candidate_sources(&rows);
        assert_eq!(v["candidates"][0]["score"], 0.3333);
    }
}
