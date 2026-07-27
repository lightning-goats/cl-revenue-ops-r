//! Port of all five of `CapacityPlanner`'s open-candidate discovery
//! strategies (py `modules/capacity_planner.py`): `_discover_from_winners`
//! (1485-1497), `_discover_from_neighbors`'s no-capital-efficiency fallback
//! path (1499-1566) AND its capital-efficiency-aware path
//! ([`discover_from_neighbors_capital_efficiency`], py 1568-1760 — Task 47
//! correction round 1, finding 2), `_discover_from_graph` (1762-1806),
//! `_discover_from_route_pairs` (1808-1904). The fifth,
//! `_discover_from_demand_flow` (1917-1947), is [`discover_from_demand_flow`]
//! below, built on [`super::demand_flow`].
//!
//! # The capital-efficiency-aware neighbor-discovery branch
//!
//! `_discover_from_neighbors` (py 1499-1622) has TWO code paths selected by
//! `self._capital_efficiency is None`. [`discover_from_neighbors`] ports
//! the `is None` fallback branch (py 1516-1566) — patron selection by
//! top-3 marginal ROI, single-hop neighbor scoring.
//! [`discover_from_neighbors_capital_efficiency`] ports the `is not None`
//! branch (py 1568-1663 `_build_neighbor_patron_pool`, 1665-1707
//! `_build_neighbor_candidate`, 1709-1760
//! `_discover_second_degree_neighbors`): patrons are additionally weighed
//! by a capital-efficiency `efficiency_rank` and volume-routed ranking,
//! then a second hop explores neighbors of the best first-degree results.
//! Every production node that injects a capital-efficiency analyzer (the
//! common case; `set_capital_efficiency` is called by the main plugin at
//! init) takes THIS branch in Python — callers assembling
//! [`super::cycle::DiscoveryEvidence`] select which of the two functions
//! applies via [`super::cycle::DiscoveryEvidence::neighbor_capital_efficiency`]
//! (present -> capital-efficiency-aware; absent -> fallback), mirroring
//! Python's `self._capital_efficiency is None` branch.

use std::collections::{BTreeMap, BTreeSet, HashMap};

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

    // py 1897-1903: dedup keeping max score (first-discovery order
    // preserved — see `super::dedup`'s doc comment; Task 47 finding 4), then
    // stable sort desc, cap 10.
    let mut order: Vec<String> = Vec::new();
    let mut best: std::collections::HashMap<String, DiscoveredCandidate> =
        std::collections::HashMap::new();
    for c in candidates {
        super::dedup::upsert_best(&mut order, &mut best, c.peer_id.clone(), c, |c| c.score);
    }
    let mut ranked: Vec<DiscoveredCandidate> = super::dedup::into_ordered_vec(order, best);
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

// ---------------------------------------------------------------------
// Strategy 2, capital-efficiency-aware branch (py 1568-1760): patron-pool
// selection by efficiency/volume/ROI, first- and second-degree traversal.
// Task 47 correction round 1, finding 2.
// ---------------------------------------------------------------------

/// One profitability channel's contribution to the capital-efficiency-aware
/// patron ranking (py `_build_neighbor_patron_pool`'s `entries` list,
/// 1632-1647): `efficiency_rank` is `None` when the channel has no
/// capital-efficiency snapshot (py `channel_efficiencies.get(scid)` misses)
/// -- both a missing snapshot AND an explicit `0.0` fall back to Python's
/// `0.1` default (py's `... or 0.1`, a falsy-value fallback, not just a
/// missing-key one).
#[derive(Debug, Clone)]
pub struct PatronPoolInput {
    pub peer_id: String,
    pub efficiency_rank: Option<f64>,
    pub volume_routed_sats: i64,
    pub marginal_roi_percent: f64,
}

/// A selected patron (py's post-`_build_neighbor_patron_pool` dict subset:
/// `peer_id`, `patron_score`).
#[derive(Debug, Clone, PartialEq)]
pub struct Patron {
    pub peer_id: String,
    pub patron_score: f64,
}

/// Port of `_build_neighbor_patron_pool` (py 1624-1663): select up to 10
/// patrons — top 5 by `patron_score` (efficiency rank), top 5 by
/// `volume_routed_sats`, top 3 by `marginal_roi_percent` — concatenated in
/// that order, then deduped by peer_id keeping the higher `patron_score`
/// (first-discovery order preserved: py's `deduped = {}` dict, see
/// `super::dedup`). The final `list(deduped.values())[:10]` is NOT
/// re-sorted by score — it is exactly first-discovery order, truncated.
pub fn build_neighbor_patron_pool(entries: &[PatronPoolInput]) -> Vec<Patron> {
    fn patron_score_of(efficiency_rank: Option<f64>) -> f64 {
        // py: `float(getattr(channel_eff, "efficiency_rank", 0.1) or 0.1)`
        // -- a MISSING attribute AND an explicit falsy (0/0.0) value both
        // fall back to 0.1.
        match efficiency_rank {
            Some(v) if v != 0.0 => v,
            _ => 0.1,
        }
    }

    let scored: Vec<(String, f64, i64, f64)> = entries
        .iter()
        .map(|e| {
            (
                e.peer_id.clone(),
                patron_score_of(e.efficiency_rank),
                e.volume_routed_sats,
                e.marginal_roi_percent,
            )
        })
        .collect();

    let mut selected: Vec<(String, f64)> = Vec::new();

    // Correction round 2 (sort-reuse P1): Python ranks each criterion with
    // an INDEPENDENT stable `sorted(entries, ...)` over the ORIGINAL
    // insertion order (py capacity_planner.py:1650-1652), so ties in the
    // volume and ROI rankings resolve to insertion order. Re-sorting one
    // Vec in place made later rankings inherit the previous sort's order
    // instead: with tied volumes/ROI the pool collapsed to the efficiency
    // top-5 (Rust 5 patrons vs Python 7 in the reviewer's counterexample),
    // changing candidate composition under the top-5/top-3 caps. Each
    // ranking now sorts its own clone of the original-order list.
    let mut by_eff = scored.clone();
    by_eff.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    selected.extend(by_eff.iter().take(5).map(|(p, s, _, _)| (p.clone(), *s)));

    let mut by_vol = scored.clone();
    by_vol.sort_by_key(|(_, _, vol, _)| std::cmp::Reverse(*vol));
    selected.extend(by_vol.iter().take(5).map(|(p, s, _, _)| (p.clone(), *s)));

    let mut by_roi = scored;
    by_roi.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));
    selected.extend(by_roi.iter().take(3).map(|(p, s, _, _)| (p.clone(), *s)));

    let mut order: Vec<String> = Vec::new();
    let mut deduped: HashMap<String, f64> = HashMap::new();
    for (peer_id, patron_score) in selected {
        if peer_id.is_empty() {
            continue;
        }
        super::dedup::upsert_best(&mut order, &mut deduped, peer_id, patron_score, |s| *s);
    }
    order
        .into_iter()
        .take(10)
        .map(|peer_id| {
            let patron_score = deduped[&peer_id];
            Patron {
                peer_id,
                patron_score,
            }
        })
        .collect()
}

/// Port of `_build_neighbor_candidate` (py 1665-1707): score a first- or
/// second-degree neighbor candidate, or `None` when it's filtered
/// (missing/self/existing-peer destination, fee too high, capacity too
/// small).
fn build_neighbor_candidate(
    ch: &NeighborChannelEdge,
    patron_score: f64,
    patron_peer_id: &str,
    existing_peers: &BTreeSet<String>,
    our_node_id: &str,
    degree: u8,
) -> Option<(String, f64)> {
    let peer_id = ch.destination;
    if peer_id.is_empty() || peer_id == our_node_id || existing_peers.contains(peer_id) {
        return None;
    }

    let capacity_sats = base_to_sats_floor(ch.amount_msat.max(0) as u64) as i64;
    let fee_ppm = ch.fee_per_millionth;
    if fee_ppm > 1500 {
        return None;
    }
    if capacity_sats > 0 && capacity_sats < 200_000 {
        return None;
    }

    // py: `float(patron.get("patron_score", 0.1) or 0.1)` -- same
    // falsy-value fallback as the patron-pool's own `patron_score`.
    let mut base_score = if patron_score != 0.0 {
        patron_score
    } else {
        0.1
    };
    if degree == 2 {
        base_score *= 0.5;
    }
    let capacity_bonus = if capacity_sats > 0 {
        (capacity_sats as f64 / 5_000_000.0).min(1.0) * 0.4
    } else {
        0.0
    };
    let fee_bonus = if fee_ppm < 200 {
        0.2
    } else if fee_ppm < 500 {
        0.1
    } else {
        0.0
    };

    let mut score = base_score + capacity_bonus + fee_bonus;
    if degree == 2 {
        score *= 0.5;
    }

    let _ = patron_peer_id; // reason text is assembled by the caller (py's f-string).
    Some((peer_id.to_string(), score))
}

fn median_of(values: &[i64]) -> f64 {
    let mut v = values.to_vec();
    v.sort_unstable();
    let n = v.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        v[n / 2] as f64
    } else {
        (v[n / 2 - 1] + v[n / 2]) as f64 / 2.0
    }
}

/// One first-degree candidate accumulator (py's per-candidate dict inside
/// the first-degree loop, 1591-1602: `score`, `reason`, `patron_peer_id`,
/// plus the popped `_patron_ids` set used only for the patron-count score
/// bonus, 1605-1607).
struct FirstDegreeCandidate {
    peer_id: String,
    score: f64,
    patron_peer_id: String,
    patron_ids: BTreeSet<String>,
}

/// Port of `_discover_second_degree_neighbors` (py 1709-1760). Mutates
/// `first_degree_top3` IN PLACE — py's `candidate["score"] += ...` mutates
/// the SAME dict objects referenced by `first_degree_list[:3]` (a shallow
/// list slice), so the boosted score is visible both to the caller's later
/// merge AND to this function's own `patron = {"patron_score":
/// candidate["score"]}` (using the ALREADY-boosted value).
fn discover_second_degree_neighbors(
    first_degree_top3: &mut [FirstDegreeCandidate],
    patron_source_channels: &BTreeMap<String, Vec<NeighborChannelEdge>>,
    existing_peers: &BTreeSet<String>,
    our_node_id: &str,
) -> Vec<(String, f64, String)> {
    let mut second_degree: Vec<(String, f64, String)> = Vec::new();
    let empty: Vec<NeighborChannelEdge> = Vec::new();

    for candidate in first_degree_top3.iter_mut() {
        let channels = patron_source_channels
            .get(&candidate.peer_id)
            .unwrap_or(&empty);
        let active_channels: Vec<&NeighborChannelEdge> = channels
            .iter()
            .filter(|ch| !ch.destination.is_empty())
            .collect();

        if !active_channels.is_empty() {
            let capacities: Vec<i64> = active_channels
                .iter()
                .map(|ch| base_to_sats_floor(ch.amount_msat.max(0) as u64) as i64)
                .filter(|&c| c > 0)
                .collect();
            let channel_count_bonus = (active_channels.len() as f64 / 20.0).min(1.0) * 0.3;
            let median_size_bonus = (median_of(&capacities) / 5_000_000.0).min(1.0) * 0.4;
            let fees: Vec<i64> = active_channels
                .iter()
                .map(|ch| ch.fee_per_millionth)
                .collect();
            let avg_fee_ppm = if fees.is_empty() {
                0.0
            } else {
                fees.iter().sum::<i64>() as f64 / fees.len() as f64
            };
            let fee_bonus = if avg_fee_ppm < 200.0 {
                0.2
            } else if avg_fee_ppm < 500.0 {
                0.1
            } else {
                0.0
            };
            candidate.score += channel_count_bonus + median_size_bonus + fee_bonus;
        }

        let mut scored: Vec<(String, f64)> = Vec::new();
        for ch in &active_channels {
            if let Some((peer_id, score)) = build_neighbor_candidate(
                ch,
                candidate.score,
                &candidate.peer_id,
                existing_peers,
                our_node_id,
                2,
            ) {
                scored.push((peer_id, score));
            }
        }
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(3);
        second_degree.extend(
            scored
                .into_iter()
                .map(|(peer_id, score)| (peer_id, score, candidate.peer_id.clone())),
        );
    }

    second_degree
}

/// Port of `_discover_from_neighbors`'s capital-efficiency-aware branch (py
/// 1568-1622, `self._capital_efficiency is not None`): patron pool
/// selection by efficiency/volume/ROI via [`build_neighbor_patron_pool`],
/// first-degree traversal with a per-patron-count score bonus, second-degree
/// traversal from the top-3 first-degree candidates via
/// [`discover_second_degree_neighbors`], then a final merge of first-degree
/// plus second-degree candidates, deduped by peer_id keeping the higher
/// score with first-discovery order preserved, sorted desc by score and
/// capped at 10.
///
/// `patron_source_channels` serves BOTH the first-hop patron lookup and any
/// second-hop lookup (mirroring Python's single `_get_cached_channels`
/// cache) — a missing entry is treated as "no neighbors from this peer"
/// (Python's `except Exception: continue`).
pub fn discover_from_neighbors_capital_efficiency(
    patron_pool_inputs: &[PatronPoolInput],
    patron_source_channels: &BTreeMap<String, Vec<NeighborChannelEdge>>,
    existing_peers: &BTreeSet<String>,
    our_node_id: &str,
) -> Vec<DiscoveredCandidate> {
    let patron_pool = build_neighbor_patron_pool(patron_pool_inputs);

    let empty: Vec<NeighborChannelEdge> = Vec::new();
    let mut fd_order: Vec<String> = Vec::new();
    let mut fd_map: HashMap<String, FirstDegreeCandidate> = HashMap::new();

    for patron in &patron_pool {
        let channels = patron_source_channels
            .get(&patron.peer_id)
            .unwrap_or(&empty);
        for ch in channels {
            let Some((peer_id, score)) = build_neighbor_candidate(
                ch,
                patron.patron_score,
                &patron.peer_id,
                existing_peers,
                our_node_id,
                1,
            ) else {
                continue;
            };
            match fd_map.get_mut(&peer_id) {
                None => {
                    let mut patron_ids = BTreeSet::new();
                    patron_ids.insert(patron.peer_id.clone());
                    fd_order.push(peer_id.clone());
                    fd_map.insert(
                        peer_id.clone(),
                        FirstDegreeCandidate {
                            peer_id,
                            score,
                            patron_peer_id: patron.peer_id.clone(),
                            patron_ids,
                        },
                    );
                }
                Some(current) => {
                    current.patron_ids.insert(patron.peer_id.clone());
                    if score > current.score {
                        current.score = score;
                        current.patron_peer_id = patron.peer_id.clone();
                    }
                }
            }
        }
    }

    let mut first_degree_list: Vec<FirstDegreeCandidate> = fd_order
        .into_iter()
        .map(|k| fd_map.remove(&k).expect("built together above"))
        .collect();
    for c in first_degree_list.iter_mut() {
        let patron_count = c.patron_ids.len() as f64;
        c.score += 0.15 * patron_count;
    }
    // py 1609: stable sort desc by score — ties preserve first-degree
    // discovery order (the order patrons were iterated above).
    first_degree_list.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let top3_len = first_degree_list.len().min(3);
    let second_degree_raw = discover_second_degree_neighbors(
        &mut first_degree_list[..top3_len],
        patron_source_channels,
        existing_peers,
        our_node_id,
    );

    // py 1616-1622: merge first_degree_list (now with the top-3's scores
    // boosted in place by the second-degree pass above) + second_degree,
    // dedup by peer_id keeping the higher score, first-discovery order
    // preserved, THEN sort desc by score and cap at 10.
    let mut order: Vec<String> = Vec::new();
    let mut merged: HashMap<String, DiscoveredCandidate> = HashMap::new();
    for c in first_degree_list {
        let candidate = DiscoveredCandidate {
            peer_id: c.peer_id.clone(),
            source: "neighbor",
            score: c.score,
            reason: format!(
                "Neighbor of capital-efficient patron {}...",
                short(&c.patron_peer_id)
            ),
        };
        super::dedup::upsert_best(&mut order, &mut merged, c.peer_id, candidate, |c| c.score);
    }
    for (peer_id, score, patron_peer_id) in second_degree_raw {
        let candidate = DiscoveredCandidate {
            peer_id: peer_id.clone(),
            source: "neighbor",
            score,
            reason: format!(
                "Neighbor of capital-efficient patron {}...",
                short(&patron_peer_id)
            ),
        };
        super::dedup::upsert_best(&mut order, &mut merged, peer_id, candidate, |c| c.score);
    }

    let mut result: Vec<DiscoveredCandidate> = super::dedup::into_ordered_vec(order, merged);
    result.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    result.truncate(10);
    result
}
