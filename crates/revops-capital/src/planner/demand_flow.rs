//! Port of `DemandFlowClassifier` (py `modules/demand_flow.py`, 233 LOC):
//! `classify_peers` (56-95) and `find_sink_adjacent_candidates` (193-233).
//! Both are pure functions of already-fetched flow/channel data — no RPC or
//! DB calls in the Python source. `classify_candidate` (97-191, gossip
//! alias/structure heuristics for a SINGLE unclassified candidate node) is
//! not used by [`super::discovery::discover_from_demand_flow`] (capacity
//! planner's Strategy 6 only calls `classify_peers` +
//! `find_sink_adjacent_candidates`, py `capacity_planner.py` 1917-1947) and
//! is not ported here.

use super::pyround::py_round;

/// One channel's flow evidence contributing to a peer's aggregate flow
/// profile (py `classify_peers`, 60-65: `flow.peer_id`, `flow.sats_in`,
/// `flow.sats_out`, summed per peer across all that peer's channels).
#[derive(Debug, Clone, Copy)]
pub struct PeerFlowContribution<'a> {
    pub peer_id: &'a str,
    pub sats_in: i64,
    pub sats_out: i64,
}

/// Aggregate per-peer payment-flow role (py `NodeFlowProfile`, demand_flow.py
/// 43-50 — only the fields `classify_peers`/`find_sink_adjacent_candidates`
/// populate; `gossip_signals`/`has_liquidity_ads` are `classify_candidate`
/// output, not reachable from this pass, and omitted).
#[derive(Debug, Clone, PartialEq)]
pub struct NodeFlowProfile {
    pub node_id: String,
    pub role: FlowRole,
    pub confidence: f64,
    pub net_flow_ratio: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowRole {
    Unknown,
    Source,
    Sink,
    Router,
}

impl FlowRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            FlowRole::Unknown => "unknown",
            FlowRole::Source => "source",
            FlowRole::Sink => "sink",
            FlowRole::Router => "router",
        }
    }
}

/// Port of `DemandFlowClassifier.classify_peers` (py demand_flow.py 56-95):
/// aggregate per-channel in/out flow into a role/confidence profile per
/// peer. Peers with zero total flow keep the default `Unknown` profile with
/// `confidence = 0.0` and `net_flow_ratio = None` (py 73-75).
pub fn classify_peers(flows: &[PeerFlowContribution]) -> Vec<NodeFlowProfile> {
    use std::collections::BTreeMap;

    let mut peer_in: BTreeMap<&str, i64> = BTreeMap::new();
    let mut peer_out: BTreeMap<&str, i64> = BTreeMap::new();
    for f in flows {
        *peer_in.entry(f.peer_id).or_insert(0) += f.sats_in;
        *peer_out.entry(f.peer_id).or_insert(0) += f.sats_out;
    }

    let mut all_peers: Vec<&str> = peer_in.keys().chain(peer_out.keys()).copied().collect();
    all_peers.sort_unstable();
    all_peers.dedup();

    let mut profiles = Vec::with_capacity(all_peers.len());
    for pid in all_peers {
        let total_in = *peer_in.get(pid).unwrap_or(&0);
        let total_out = *peer_out.get(pid).unwrap_or(&0);
        let total = total_in + total_out;

        if total == 0 {
            profiles.push(NodeFlowProfile {
                node_id: pid.to_string(),
                role: FlowRole::Unknown,
                confidence: 0.0,
                net_flow_ratio: None,
            });
            continue;
        }

        let ratio = (total_in - total_out) as f64 / total as f64;
        let role = if ratio > 0.3 {
            FlowRole::Source
        } else if ratio < -0.3 {
            FlowRole::Sink
        } else {
            FlowRole::Router
        };

        // py 85-86: `min(0.9, 0.3 * log10(max(total,1)) / log10(1_000_000))`,
        // then `max(0.1, confidence)`.
        let confidence = (0.3 * (total.max(1) as f64).log10() / 1_000_000f64.log10()).min(0.9);
        let confidence = confidence.max(0.1);

        profiles.push(NodeFlowProfile {
            node_id: pid.to_string(),
            role,
            confidence: py_round(confidence, 3),
            net_flow_ratio: Some(py_round(ratio, 4)),
        });
    }

    profiles
}

/// One channel edge from a sink peer, as `_get_cached_channels(pid,
/// "source")` would return it (py `find_sink_adjacent_candidates`,
/// 213-219: `destination`, `active`).
#[derive(Debug, Clone)]
pub struct SinkChannelEdge {
    pub destination: String,
    pub active: bool,
}

/// A demand-flow-sourced candidate (py's dict, 222-229: `peer_id`,
/// `source: "demand_flow"`, `score`, `reason`, `sink_peer_id`,
/// `is_sink_adjacent: True` — the last is always `true` for every element
/// this function returns, so it is not modeled as a field).
#[derive(Debug, Clone, PartialEq)]
pub struct SinkAdjacentCandidate {
    pub peer_id: String,
    pub score: f64,
    pub sink_peer_id: String,
    pub sink_confidence: f64,
}

/// Port of `DemandFlowClassifier.find_sink_adjacent_candidates` (py
/// demand_flow.py 193-233). `sink_channels` must contain an entry for every
/// `sink.node_id` present in `sink_profiles` with `role == Sink` (py's
/// `sink_channels.get(sink.node_id, [])` — an absent key here is
/// equivalent to an empty `Vec`, so callers may omit peers with no cached
/// channels rather than inserting an empty entry).
pub fn find_sink_adjacent_candidates(
    sink_profiles: &[NodeFlowProfile],
    sink_channels: &std::collections::BTreeMap<String, Vec<SinkChannelEdge>>,
    existing_peers: &std::collections::BTreeSet<String>,
) -> Vec<SinkAdjacentCandidate> {
    if sink_profiles.is_empty() {
        return Vec::new();
    }

    // py 203-207: sort by |net_flow_ratio| descending, take top 5. Ties
    // keep encounter order (Python's `sorted` is stable); mirror that with
    // a stable sort over the caller-provided slice order.
    let mut ranked: Vec<&NodeFlowProfile> = sink_profiles.iter().collect();
    ranked.sort_by(|a, b| {
        let ra = a.net_flow_ratio.unwrap_or(0.0).abs();
        let rb = b.net_flow_ratio.unwrap_or(0.0).abs();
        rb.partial_cmp(&ra).unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(5);
    let n = ranked.len();

    let mut candidates: Vec<SinkAdjacentCandidate> = Vec::new();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for (rank, sink) in ranked.iter().enumerate() {
        let empty: Vec<SinkChannelEdge> = Vec::new();
        let channels = sink_channels.get(&sink.node_id).unwrap_or(&empty);
        for ch in channels {
            let dest = &ch.destination;
            if dest.is_empty() || existing_peers.contains(dest) || seen.contains(dest) {
                continue;
            }
            if !ch.active {
                continue;
            }

            // py 221: `0.4 * confidence * (1 + (len(ranked) - rank) / len(ranked))`
            let score = 0.4 * sink.confidence * (1.0 + (n - rank) as f64 / n as f64);
            candidates.push(SinkAdjacentCandidate {
                peer_id: dest.clone(),
                score: py_round(score, 4),
                sink_peer_id: sink.node_id.clone(),
                sink_confidence: sink.confidence,
            });
            seen.insert(dest.clone());
        }
    }

    // py 232: sort by score descending, take top 10. `sort_by` is stable so
    // ties preserve insertion (rank, then per-sink channel) order.
    candidates.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    candidates.truncate(10);
    candidates
}
