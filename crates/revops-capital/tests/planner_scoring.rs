//! `planner::scoring` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kinds
//! `"normalize_candidate_scores"` and `"apply_pool_quotas"`, generated
//! from the REAL `CapacityPlanner` scoring methods).

use revops_capital::planner::scoring::{apply_pool_quotas, normalize_candidate_scores, Candidate};
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/capital/planner/kernels.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn scenarios(kind: &str) -> Vec<Value> {
    let fx = fixture();
    fx["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == kind)
        .cloned()
        .collect()
}

fn parse_score(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap(),
        // The fixture generator encodes a NaN INPUT as the string "NaN"
        // because raw JSON has no NaN literal; Rust's f64::from_str also
        // parses "NaN", matching Python's `float("NaN")`.
        Value::String(s) => s.parse::<f64>().expect("parseable float string"),
        other => panic!("unexpected score shape {other:?}"),
    }
}

fn parse_candidates(v: &Value) -> Vec<Candidate> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|c| Candidate {
            peer_id: c["peer_id"].as_str().unwrap().to_string(),
            source: c["source"].as_str().unwrap().to_string(),
            score: parse_score(&c["score"]),
        })
        .collect()
}

#[test]
fn normalize_candidate_scores_matches_python() {
    let cases = scenarios("normalize_candidate_scores");
    assert_eq!(
        cases.len(),
        9,
        "expected 9 normalize_candidate_scores scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let candidates = parse_candidates(&case["input"]["candidates"]);
        let expected = parse_candidates(&case["output"]);

        let actual = normalize_candidate_scores(candidates);

        assert_eq!(actual.len(), expected.len(), "{name}: result count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.peer_id, e.peer_id, "{name}: peer_id");
            assert_eq!(a.source, e.source, "{name}: source");
            assert_eq!(a.score, e.score, "{name}: score (bit-exact)");
        }
    }
}

/// Control: a NaN raw score must be DROPPED via the `winner` floor (0.20 >
/// 0.0, and NaN is coerced to 0.0 first) — not silently propagated as NaN
/// through the pipeline where it would poison every downstream comparison.
#[test]
fn nan_score_is_dropped_not_propagated() {
    let candidates = vec![Candidate {
        peer_id: "p1".to_string(),
        source: "winner".to_string(),
        score: f64::NAN,
    }];
    let result = normalize_candidate_scores(candidates);
    assert!(
        result.is_empty(),
        "NaN winner score must be dropped, got {result:?}"
    );
}

/// Control: the SAME raw score value normalizes to a DIFFERENT common
/// score depending on strategy (graph's 50x anchor vs winner's 1x anchor)
/// — proves per-strategy anchors are actually applied, not a shared
/// constant.
#[test]
fn anchors_differ_by_strategy() {
    let winner = normalize_candidate_scores(vec![Candidate {
        peer_id: "p1".to_string(),
        source: "winner".to_string(),
        score: 0.4,
    }]);
    let graph = normalize_candidate_scores(vec![Candidate {
        peer_id: "p2".to_string(),
        source: "graph".to_string(),
        score: 0.4,
    }]);
    assert_ne!(winner[0].score, graph[0].score);
}

#[test]
fn apply_pool_quotas_matches_python() {
    let cases = scenarios("apply_pool_quotas");
    assert_eq!(cases.len(), 4, "expected 4 apply_pool_quotas scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let candidates = parse_candidates(&case["input"]["candidates"]);
        let max_pool = case["input"]["max_pool"].as_i64().unwrap();

        let result = apply_pool_quotas(&candidates, max_pool);
        let actual_ids: Vec<String> = result.iter().map(|c| c.peer_id.clone()).collect();
        let expected_ids: Vec<String> = case["output"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        assert_eq!(actual_ids, expected_ids, "{name}");
    }
}

/// Control: the `graph` and `route_pair` reserved quotas (4 each) must
/// actually reserve slots even when `winner` candidates would otherwise
/// dominate purely by score — this is the exact regression the reserved
/// quotas exist to prevent (see the port-map's F2 audit note on
/// per-group normalization).
#[test]
fn reserved_quota_slots_are_not_starved_by_higher_scoring_winners() {
    let mut candidates = vec![Candidate {
        peer_id: "g1".to_string(),
        source: "graph".to_string(),
        score: 0.01, // deliberately low score
    }];
    for i in 0..20 {
        candidates.push(Candidate {
            peer_id: format!("w{i}"),
            source: "winner".to_string(),
            score: 0.99, // deliberately high score
        });
    }
    let result = apply_pool_quotas(&candidates, 32);
    assert!(
        result.iter().any(|c| c.peer_id == "g1"),
        "the sole graph candidate must occupy its reserved slot regardless of score"
    );
}
