//! Port of the candidate-pool score normalization and quota allocation
//! from `CapacityPlanner` (py `modules/capacity_planner.py`):
//! `_normalize_candidate_scores` (2232-2265) and `_apply_pool_quotas`
//! (2267-2304).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

/// py `STRATEGY_WEIGHTS` (capacity_planner.py 81-87): higher = more
/// trusted signal.
fn strategy_weight(source: &str) -> f64 {
    match source {
        "winner" => 1.0,
        "demand_flow" => 1.0,
        "route_pair" => 0.85,
        "graph" => 0.8,
        "neighbor" => 0.9,
        _ => 1.0, // py `.get(source, 1.0)`
    }
}

/// py `SCALE_ANCHORS` (capacity_planner.py 106-112): fixed-reference
/// normalization anchors — the raw-score scale of each strategy.
fn scale_anchor(source: &str) -> f64 {
    match source {
        "winner" => 1.0,
        "neighbor" => 1.0,
        "demand_flow" => 0.8,
        "route_pair" => 0.4,
        "graph" => 50.0,
        _ => 1.0, // py `.get(source, 1.0)`
    }
}

/// py `RAW_SCORE_FLOORS` (capacity_planner.py 117-119): minimum raw
/// evidence to participate in the candidate pool at all. Only `"winner"`
/// has a floor today.
fn raw_score_floor(source: &str) -> Option<f64> {
    match source {
        "winner" => Some(0.20),
        _ => None,
    }
}

/// One open-candidate entry (py's candidate `dict`, the subset of fields
/// score normalization and pool quotas touch).
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub peer_id: String,
    pub source: String,
    pub score: f64,
}

fn score_desc(a: &Candidate, b: &Candidate) -> Ordering {
    b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal)
}

/// Port of `_normalize_candidate_scores` (py capacity_planner.py 2232-2265):
/// no group-max rescale — a candidate's final score depends only on its own
/// raw evidence. Raw scores are clamped to `[0, 2 x anchor]`, converted onto
/// the common advisory scale, then multiplied by the strategy weight.
/// Candidates below their strategy's raw-evidence floor are dropped
/// (non-finite raw scores, e.g. from a `NaN`-poisoned upstream calculation,
/// are treated as `0.0` first — py `math.isfinite` guard).
pub fn normalize_candidate_scores(candidates: Vec<Candidate>) -> Vec<Candidate> {
    let mut normalized = Vec::with_capacity(candidates.len());
    for mut c in candidates {
        let anchor = scale_anchor(&c.source);
        let weight = strategy_weight(&c.source);

        let mut raw = c.score;
        if !raw.is_finite() {
            raw = 0.0;
        }

        if let Some(floor) = raw_score_floor(&c.source) {
            if raw < floor {
                continue; // weak evidence must not be normalized upward
            }
        }

        let clamped = raw.clamp(0.0, 2.0 * anchor);
        let common = clamped / anchor.max(1.0);
        c.score = common * weight;
        normalized.push(c);
    }
    normalized
}

/// Port of `_apply_pool_quotas` (py capacity_planner.py 2267-2304): apply
/// reserved slot quotas per strategy (`graph`: 4, `route_pair`: 4), then
/// fill remaining slots by score. Unfilled reserved slots flow back into
/// the open pool (py 2294-2296). Mirrors Python's negative-slice behavior
/// verbatim if `max_pool` is smaller than the reserved total (an
/// out-of-range configuration, not expected in practice, but kept exact
/// rather than silently clamped).
pub fn apply_pool_quotas(candidates: &[Candidate], max_pool: i64) -> Vec<Candidate> {
    const RESERVED: [(&str, i64); 2] = [("graph", 4), ("route_pair", 4)];
    let reserved_total: i64 = RESERVED.iter().map(|(_, q)| *q).sum();
    let mut open_slots = max_pool - reserved_total;

    let mut by_source: BTreeMap<&str, Vec<&Candidate>> = BTreeMap::new();
    for c in candidates {
        by_source.entry(c.source.as_str()).or_default().push(c);
    }
    for group in by_source.values_mut() {
        group.sort_by(|a, b| score_desc(a, b));
    }

    let mut result: Vec<Candidate> = Vec::new();
    let mut used_ids: BTreeSet<&str> = BTreeSet::new();
    let mut unfilled_slots: i64 = 0;

    for (source, quota) in RESERVED {
        let empty: Vec<&Candidate> = Vec::new();
        let group = by_source.get(source).unwrap_or(&empty);
        let mut filled: i64 = 0;
        for &c in group {
            if filled >= quota {
                break;
            }
            if !used_ids.contains(c.peer_id.as_str()) {
                result.push(c.clone());
                used_ids.insert(c.peer_id.as_str());
                filled += 1;
            }
        }
        unfilled_slots += quota - filled;
    }

    open_slots += unfilled_slots;

    let mut all_remaining: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| !used_ids.contains(c.peer_id.as_str()))
        .collect();
    all_remaining.sort_by(|a, b| score_desc(a, b));

    let take_n: usize = if open_slots >= 0 {
        open_slots as usize
    } else {
        all_remaining.len().saturating_sub((-open_slots) as usize)
    };

    for &c in all_remaining.iter().take(take_n) {
        if !used_ids.contains(c.peer_id.as_str()) {
            used_ids.insert(c.peer_id.as_str());
            result.push(c.clone());
        }
    }

    result
}
