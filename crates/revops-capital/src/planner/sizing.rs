//! Port of `_size_channel` (py `modules/capacity_planner.py` 2757-2806):
//! ROI-proportional channel sizing with a competitive-floor bump against
//! the peer's own median destination-channel size.

/// The subset of a candidate's fields `_size_channel` reads (py 2769-2770,
/// 2782).
#[derive(Debug, Clone, Copy)]
pub struct SizingCandidate<'a> {
    pub peer_id: &'a str,
    pub score: f64,
}

/// Config fields `_size_channel` reads (py `cfg.planner_min_channel_sats` /
/// `planner_max_channel_sats`).
#[derive(Debug, Clone, Copy)]
pub struct SizingConfig {
    pub planner_min_channel_sats: i64,
    pub planner_max_channel_sats: i64,
}

/// Port of `_size_channel` (py 2757-2806). `all_candidates` is the sizing
/// pool (py's `sizing_pool_for(candidate)` — the caller builds this;
/// see `crates/revops-capital/ENTRYPOINTS.md`). `peer_dest_channel_capacities_sats`
/// is `candidate.peer_id`'s cached `listchannels(destination=peer_id)`
/// ACTIVE-channel capacities (py 2785-2793) — the SAME evidence
/// [`super::candidate_score::CandidateEnrichmentEvidence::dest_channel_capacities_sats`]
/// uses, so callers can share one fetch.
pub fn size_channel(
    candidate: &SizingCandidate,
    all_candidates: &[SizingCandidate],
    available_sats: i64,
    cfg: &SizingConfig,
    peer_dest_channel_capacities_sats: &[i64],
) -> i64 {
    if all_candidates.is_empty() {
        return cfg.planner_min_channel_sats;
    }

    let total_score: f64 = all_candidates.iter().map(|c| c.score.max(0.01)).sum();
    let candidate_score = candidate.score.max(0.01);
    let roi_weight = candidate_score / total_score;

    let mut raw_size = (available_sats as f64 * roi_weight) as i64;
    raw_size = raw_size.min(available_sats.div_euclid(2));

    if !candidate.peer_id.is_empty() && !peer_dest_channel_capacities_sats.is_empty() {
        let mut caps: Vec<i64> = peer_dest_channel_capacities_sats.to_vec();
        caps.sort_unstable();
        let median_cap = caps[caps.len() / 2];
        let competitive_floor = median_cap.div_euclid(2);
        if competitive_floor > raw_size && competitive_floor <= available_sats.div_euclid(2) {
            raw_size = competitive_floor.min(cfg.planner_max_channel_sats);
        }
    }

    raw_size
        .max(cfg.planner_min_channel_sats)
        .min(cfg.planner_max_channel_sats)
}
