//! Port of four of `CapacityPlanner`'s five open-candidate discovery
//! strategies (py `modules/capacity_planner.py`): `_discover_from_winners`
//! (1485-1497), `_discover_from_neighbors`'s no-capital-efficiency fallback
//! path (1499-1566), `_discover_from_graph` (1762-1806),
//! `_discover_from_route_pairs` (1808-1904). The fifth,
//! `_discover_from_demand_flow` (1917-1947), is [`discover_from_demand_flow`]
//! below, built on [`super::demand_flow`].
//!
//! # What is intentionally NOT ported: the capital-efficiency-aware
//! neighbor-discovery branch
//!
//! `_discover_from_neighbors` (py 1499-1622) has TWO code paths selected by
//! `self._capital_efficiency is None`. [`discover_from_neighbors`] here
//! ports only the `is None` fallback branch (py 1516-1566) — patron
//! selection by top-3 marginal ROI, single-hop neighbor scoring. The
//! `is not None` branch (py 1568-1663 `_build_neighbor_patron_pool`,
//! 1665-1707 `_build_neighbor_candidate`, 1709-1760
//! `_discover_second_degree_neighbors`) additionally weighs patrons by a
//! capital-efficiency `efficiency_rank` and volume-routed ranking, then
//! explores a second hop from the best first-degree results. This is a
//! substantial additional state machine (multi-criteria patron selection +
//! degree-2 traversal) that was not ported in this pass — see
//! `crates/revops-capital/TASK47-REPORT.md` for the full gap list. Every
//! production node that injects a capital-efficiency analyzer (the common
//! case; `set_capital_efficiency` is called by the main plugin at init)
//! takes the UNPORTED branch in Python — callers of this crate must be
//! aware [`discover_from_neighbors`] is not full parity with that
//! configuration.

use std::collections::{BTreeMap, BTreeSet};

use revops_core::msat::{base_to_sats_ceil, base_to_sats_floor};

/// One discovered open candidate (py's candidate `dict`'s common fields:
/// `peer_id`, `source`, `score`, `reason`). Strategy-specific bookkeeping
/// fields (`patron_peer_id`, `route_peer_id`, `channel_count`, ...) are
/// intentionally not modeled — nothing downstream of discovery
/// ([`super::scoring::normalize_candidate_scores`],
/// [`super::scoring::apply_pool_quotas`], EV sizing) reads them.
#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredCandidate {
    pub peer_id: String,
    pub source: &'static str,
    pub score: f64,
    pub reason: String,
}

impl DiscoveredCandidate {
    /// Project onto [`super::scoring::Candidate`], the shape the
    /// normalize/pool-quota pipeline consumes.
    pub fn to_scoring_candidate(&self) -> super::scoring::Candidate {
        super::scoring::Candidate {
            peer_id: self.peer_id.clone(),
            source: self.source.to_string(),
            score: self.score,
        }
    }
}

/// First 12 characters of an identifier, matching Python's `s[:12]` on a
/// `str` (character count, not byte count — pubkeys/peer ids here are
/// ASCII hex so the two coincide in practice).
fn short(id: &str) -> String {
    id.chars().take(12).collect()
}

/// Thousands-grouped decimal, matching Python's `f"{n:,}"` (py 1800's
/// `total_capacity_sats:,` in the graph-centrality reason string).
fn format_commas(n: i64) -> String {
    let neg = n < 0;
    let digits = n.unsigned_abs().to_string();
    let mut grouped = String::new();
    for (i, c) in digits.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    let mut result: String = grouped.chars().rev().collect();
    if neg {
        result.insert(0, '-');
    }
    result
}

// ---------------------------------------------------------------------
// Strategy 1: existing winners (py 1485-1497)
// ---------------------------------------------------------------------

/// A winner's fields `_discover_from_winners` reads (py 1489-1496).
#[derive(Debug, Clone, Copy)]
pub struct WinnerForDiscovery<'a> {
    pub peer_id: &'a str,
    pub roi: f64,
}

/// Port of `_discover_from_winners` (py 1485-1497): only very strong
/// winners (>30% effective ROI) become open candidates.
pub fn discover_from_winners(winners: &[WinnerForDiscovery]) -> Vec<DiscoveredCandidate> {
    winners
        .iter()
        .filter(|w| w.roi > 30.0)
        .map(|w| DiscoveredCandidate {
            peer_id: w.peer_id.to_string(),
            source: "winner",
            score: w.roi / 100.0,
            reason: format!("Existing winner with {:.1}% ROI", w.roi),
        })
        .collect()
}

// ---------------------------------------------------------------------
// Strategy 2 (fallback path only — see module doc): neighbors of top
// earners (py 1516-1566)
// ---------------------------------------------------------------------

/// A channel's peer + marginal ROI, one per `all_profitability` entry (py
/// 1517-1521's sort key and 1524's `patron.peer_id`).
#[derive(Debug, Clone)]
pub struct PatronCandidate {
    pub peer_id: String,
    pub marginal_roi_percent: f64,
}

/// One channel from a patron's `listchannels(source=patron_peer_id)` (py
/// 1533-1550's fields: `destination`, `amount_msat`, `fee_per_millionth`).
#[derive(Debug, Clone, Copy)]
pub struct NeighborChannelEdge<'a> {
    pub destination: &'a str,
    pub amount_msat: i64,
    pub fee_per_millionth: i64,
}

/// Port of `_discover_from_neighbors`'s no-capital-efficiency fallback (py
/// 1516-1566): the top-3-by-marginal-ROI channels' peers become "patrons";
/// each patron's outbound neighbors are scored and the top 5 per patron
/// (max 10 overall, in patron-then-rank order — NOT globally re-sorted by
/// score, matching py's plain `candidates[:10]`) become candidates.
///
/// `patron_source_channels` should hold every peer's cached
/// `listchannels(source=peer_id)` result the caller has available; a
/// missing entry is treated as "no neighbors from this patron" (Python's
/// `except Exception: continue` on the whole patron has the same
/// zero-candidates-from-this-patron effect).
pub fn discover_from_neighbors(
    all_profitability: &[PatronCandidate],
    patron_source_channels: &BTreeMap<String, Vec<NeighborChannelEdge>>,
    existing_peers: &BTreeSet<String>,
    our_node_id: &str,
) -> Vec<DiscoveredCandidate> {
    let mut sorted_channels: Vec<&PatronCandidate> = all_profitability.iter().collect();
    sorted_channels.sort_by(|a, b| {
        b.marginal_roi_percent
            .partial_cmp(&a.marginal_roi_percent)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    sorted_channels.truncate(3);

    let mut candidates: Vec<DiscoveredCandidate> = Vec::new();

    for patron in sorted_channels {
        if patron.peer_id.is_empty() {
            continue;
        }
        let empty: Vec<NeighborChannelEdge> = Vec::new();
        let channels = patron_source_channels
            .get(&patron.peer_id)
            .unwrap_or(&empty);
        let patron_roi = patron.marginal_roi_percent;

        let mut scored: Vec<(&str, f64)> = Vec::new();
        for ch in channels {
            let dest = ch.destination;
            if dest.is_empty() || dest == our_node_id || existing_peers.contains(dest) {
                continue;
            }
            let cap = base_to_sats_floor(ch.amount_msat.max(0) as u64) as i64;
            let fee_ppm = ch.fee_per_millionth;
            if fee_ppm > 1500 {
                continue;
            }
            if cap > 0 && cap < 200_000 {
                continue;
            }
            let mut base = (patron_roi / 200.0).max(0.1);
            if cap > 5_000_000 {
                base *= 1.15;
            }
            if fee_ppm > 0 && fee_ppm < 100 {
                base *= 1.10;
            }
            scored.push((dest, base));
        }
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        for (neighbor_id, neighbor_score) in scored.into_iter().take(5) {
            candidates.push(DiscoveredCandidate {
                peer_id: neighbor_id.to_string(),
                source: "neighbor",
                score: neighbor_score,
                reason: format!("Neighbor of top earner {}...", short(&patron.peer_id)),
            });
        }
    }

    candidates.truncate(10);
    candidates
}

// ---------------------------------------------------------------------
// Strategy 3: graph centrality over already-cached channel data (py
// 1762-1806)
// ---------------------------------------------------------------------

/// One channel from a cached node's channel list (py 1783, 1789: `active`,
/// `amount_msat`).
#[derive(Debug, Clone, Copy)]
pub struct GraphChannelEdge {
    pub active: bool,
    pub amount_msat: i64,
}

/// Port of `_discover_from_graph` (py 1762-1806): scores nodes already
/// present in the per-cycle channel cache (`_cycle_channels_source`,
/// populated by earlier strategies) — no additional gossip fetch. Nodes
/// with fewer than 5 active channels are excluded.
pub fn discover_from_graph(
    cached_source_channels: &BTreeMap<String, Vec<GraphChannelEdge>>,
    our_node_id: &str,
    existing_peer_ids: &BTreeSet<String>,
) -> Vec<DiscoveredCandidate> {
    let mut scored: Vec<DiscoveredCandidate> = Vec::new();

    for (node_id, channels) in cached_source_channels {
        if node_id == our_node_id || existing_peer_ids.contains(node_id) {
            continue;
        }

        let active_channels: Vec<&GraphChannelEdge> =
            channels.iter().filter(|ch| ch.active).collect();
        let channel_count = active_channels.len();
        if channel_count < 5 {
            continue;
        }

        let total_capacity_sats: i64 = active_channels
            .iter()
            .map(|ch| base_to_sats_floor(ch.amount_msat.max(0) as u64) as i64)
            .sum();

        let capacity_btc = if total_capacity_sats > 0 {
            total_capacity_sats as f64 / 100_000_000.0
        } else {
            0.001
        };
        let score = channel_count as f64 * capacity_btc.sqrt();

        scored.push(DiscoveredCandidate {
            peer_id: node_id.clone(),
            source: "graph",
            score,
            reason: format!(
                "Graph centrality: {channel_count} channels, {} sat",
                format_commas(total_capacity_sats)
            ),
        });
    }

    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored.truncate(10);
    scored
}

// ---------------------------------------------------------------------
// Strategy 5 (py's own label): route-pair peers' neighbors (py 1808-1904)
// ---------------------------------------------------------------------

/// One `get_top_route_pairs` row (py 1850-1852): `in_channel`,
/// `out_channel` are display-form SCIDs (`x` separator, as
/// `channel_to_peer` is keyed); `total_fee_msat` the pair's aggregate fee
/// revenue.
#[derive(Debug, Clone)]
pub struct RoutePairRow {
    pub in_channel: String,
    pub out_channel: String,
    pub total_fee_msat: i64,
}

/// Port of `_discover_from_route_pairs` (py 1808-1904). `channel_to_peer`
/// maps a display-form SCID to the peer on the other end (py 1828-1835).
/// `route_peer_source_channels` should hold `listchannels(source=peer)` for
/// every peer that could appear in `route_peer_scores`; a missing entry is
/// treated as "no neighbors from this route peer" (matching Python's
/// `except Exception: continue`, which drops just that route peer).
pub fn discover_from_route_pairs(
    rows: &[RoutePairRow],
    channel_to_peer: &BTreeMap<String, String>,
    route_peer_source_channels: &BTreeMap<String, Vec<NeighborChannelEdge>>,
    existing_peers: &BTreeSet<String>,
    our_node_id: &str,
) -> Vec<DiscoveredCandidate> {
    if rows.is_empty() {
        return Vec::new();
    }

    // py 1848-1859: accumulate fee-weighted peer scores, first-seen order
    // preserved for score ties (Python `dict` insertion order).
    let mut order: Vec<String> = Vec::new();
    let mut route_peer_scores: BTreeMap<String, i64> = BTreeMap::new();
    let bump =
        |peer: &str, fee_sats: i64, order: &mut Vec<String>, scores: &mut BTreeMap<String, i64>| {
            if !scores.contains_key(peer) {
                order.push(peer.to_string());
            }
            *scores.entry(peer.to_string()).or_insert(0) += fee_sats;
        };
    for row in rows {
        let total_fee_sats = base_to_sats_ceil(row.total_fee_msat.max(0) as u64) as i64;
        if let Some(peer) = channel_to_peer.get(&row.in_channel) {
            bump(peer, total_fee_sats, &mut order, &mut route_peer_scores);
        }
        if let Some(peer) = channel_to_peer.get(&row.out_channel) {
            bump(peer, total_fee_sats, &mut order, &mut route_peer_scores);
        }
    }

    let mut ranked_route_peers = order.clone();
    ranked_route_peers.sort_by(|a, b| route_peer_scores[b].cmp(&route_peer_scores[a]));

    let mut candidates: Vec<DiscoveredCandidate> = Vec::new();
    for route_peer in &ranked_route_peers {
        let empty: Vec<NeighborChannelEdge> = Vec::new();
        let channels = route_peer_source_channels.get(route_peer).unwrap_or(&empty);
        for ch in channels {
            let dest = ch.destination;
            if dest.is_empty() || dest == our_node_id || existing_peers.contains(dest) {
                continue;
            }
            let capacity = base_to_sats_floor(ch.amount_msat.max(0) as u64) as i64;
            let fee_ppm = ch.fee_per_millionth;
            if fee_ppm > 1000 || capacity < 500_000 {
                continue;
            }
            let mut score = 0.3;
            if capacity > 5_000_000 {
                score *= 1.2;
            }
            if fee_ppm < 200 {
                score *= 1.1;
            }
            candidates.push(DiscoveredCandidate {
                peer_id: dest.to_string(),
                source: "route_pair",
                score,
                reason: format!("Neighbor of route-pair peer {}...", short(route_peer)),
            });
        }
    }

    // py 1897-1903: dedup keeping max score, then sort desc, cap 10.
    let mut best: BTreeMap<String, DiscoveredCandidate> = BTreeMap::new();
    for c in candidates {
        match best.get(&c.peer_id) {
            Some(existing) if existing.score >= c.score => {}
            _ => {
                best.insert(c.peer_id.clone(), c);
            }
        }
    }
    let mut ranked: Vec<DiscoveredCandidate> = best.into_values().collect();
    ranked.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(10);
    ranked
}

// ---------------------------------------------------------------------
// Strategy 6 (py's own label): demand-flow sink adjacency (py 1917-1947)
// ---------------------------------------------------------------------

/// Result of [`discover_from_demand_flow`]: the candidates themselves, plus
/// the two per-cycle caches Python stashes on `self` for later reuse —
/// `_demand_flow_sink_adjacent` (py's portfolio-state sink-adjacency check,
/// 705) and `_demand_flow_profiles` (py's `_score_candidate` role boost,
/// 2192-2199; see [`super::candidate_score`]).
#[derive(Debug, Clone)]
pub struct DemandFlowDiscovery {
    pub candidates: Vec<DiscoveredCandidate>,
    pub sink_adjacent_peer_ids: BTreeSet<String>,
    pub profiles: Vec<super::demand_flow::NodeFlowProfile>,
}

/// Port of `_discover_from_demand_flow` (py 1917-1947): classify all peers'
/// flow roles, then find candidates adjacent to `sink`-role peers.
pub fn discover_from_demand_flow(
    flows: &[super::demand_flow::PeerFlowContribution],
    sink_channels: &BTreeMap<String, Vec<super::demand_flow::SinkChannelEdge>>,
    existing_peers: &BTreeSet<String>,
) -> DemandFlowDiscovery {
    let profiles = super::demand_flow::classify_peers(flows);
    let sink_profiles: Vec<super::demand_flow::NodeFlowProfile> = profiles
        .iter()
        .filter(|p| p.role == super::demand_flow::FlowRole::Sink)
        .cloned()
        .collect();

    if sink_profiles.is_empty() {
        return DemandFlowDiscovery {
            candidates: Vec::new(),
            sink_adjacent_peer_ids: BTreeSet::new(),
            profiles,
        };
    }

    let sink_candidates = super::demand_flow::find_sink_adjacent_candidates(
        &sink_profiles,
        sink_channels,
        existing_peers,
    );

    let sink_adjacent_peer_ids: BTreeSet<String> =
        sink_candidates.iter().map(|c| c.peer_id.clone()).collect();

    let candidates = sink_candidates
        .into_iter()
        .map(|c| DiscoveredCandidate {
            reason: format!(
                "Adjacent to sink {}... (conf={})",
                short(&c.sink_peer_id),
                c.sink_confidence
            ),
            peer_id: c.peer_id,
            source: "demand_flow",
            score: c.score,
        })
        .collect();

    DemandFlowDiscovery {
        candidates,
        sink_adjacent_peer_ids,
        profiles,
    }
}
