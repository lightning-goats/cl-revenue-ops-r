//! Task 67c slice 1: assemble `DiscoveryEvidence` from the ONE gossip
//! prefetch the fee loop already performs (`fee_evidence.rs:162`).
//!
//! The five discovery strategies are FROZEN. This module only groups and
//! projects their inputs:
//!
//! - Edges group **by source**, because every patron lookup is Python's
//!   `listchannels(source=peer_id)`. Grouping by destination would
//!   silently answer a different question and hand the strategies the
//!   wrong neighbourhood.
//! - `channel_to_peer` is built from **both endpoints**: a one-sided map
//!   loses half the route-pair lookups without erroring.
//! - The `active` flag survives the projection, because
//!   `discover_from_graph` counts ACTIVE channels and excludes nodes with
//!   fewer than five.
//! - A peer with no profitability entry is **absent** from the patron
//!   list, never invented as a 0% patron — zero is a real ROI and would
//!   rank it against genuine data.
//!
//! Every source is `Result`-shaped and a failure REFUSES. The strategies
//! are total over empty inputs, so a silently-empty graph yields a
//! confident "no candidates anywhere" that is indistinguishable from a
//! genuinely sparse network.

use std::collections::{BTreeMap, HashMap};

use revops_capital::planner::cycle::{DiscoveryEvidence, NeighborEdge};
use revops_capital::planner::discovery::{GraphChannelEdge, PatronCandidate, RoutePairRow};
use serde_json::Value;

/// py `_apply_pool_quotas`'s default `max_pool` (capacity_planner.py:2267).
pub const DEFAULT_MAX_CANDIDATE_POOL: i64 = 32;

pub struct GraphSources {
    /// The single `listchannels` `channels` array (REQUIRED).
    pub gossip_channels: Result<Vec<Value>, String>,
    /// Route-pair rows from the production DB (REQUIRED).
    pub route_pair_rows: Result<Vec<RoutePairRow>, String>,
    /// Marginal ROI per peer, from task 67b's profitability assembler.
    pub marginal_roi_by_peer: HashMap<String, f64>,
    /// Demand-flow contributions from the analytics owner (task 67's flow
    /// kernels). Empty is a valid observation; the strategy simply finds
    /// no sink-adjacent candidates.
    pub demand_flows: Vec<revops_capital::planner::cycle::FlowContribution>,
    pub demand_flow_sink_channels:
        BTreeMap<String, Vec<revops_capital::planner::demand_flow::SinkChannelEdge>>,
    pub our_node_id: String,
    /// py's `max_pool` (default 32); evidence rather than a literal so a
    /// non-default config needs no crate edit.
    pub max_candidate_pool: i64,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryRefusal {
    GossipUnavailable(String),
    RoutePairsUnavailable(String),
}

impl DiscoveryRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::GossipUnavailable(_) => "discovery_gossip_unavailable",
            Self::RoutePairsUnavailable(_) => "discovery_route_pairs_unavailable",
        }
    }
}

fn msat(v: Option<&Value>) -> i64 {
    match v {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::String(s)) => s.trim().trim_end_matches("msat").parse().unwrap_or(0),
        _ => 0,
    }
}

pub fn build_discovery_evidence(
    sources: GraphSources,
) -> Result<DiscoveryEvidence, DiscoveryRefusal> {
    let channels = sources
        .gossip_channels
        .map_err(DiscoveryRefusal::GossipUnavailable)?;
    let route_pair_rows = sources
        .route_pair_rows
        .map_err(DiscoveryRefusal::RoutePairsUnavailable)?;

    let mut neighbor_source: BTreeMap<String, Vec<NeighborEdge>> = BTreeMap::new();
    let mut graph_source: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
    let mut channel_to_peer: BTreeMap<String, String> = BTreeMap::new();

    for ch in &channels {
        let Some(source) = ch.get("source").and_then(Value::as_str) else {
            continue;
        };
        let destination = ch
            .get("destination")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let amount_msat = msat(ch.get("amount_msat"));
        let fee_per_millionth = ch
            .get("fee_per_millionth")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let active = ch.get("active").and_then(Value::as_bool).unwrap_or(false);

        neighbor_source
            .entry(source.to_string())
            .or_default()
            .push(NeighborEdge {
                destination: destination.to_string(),
                amount_msat,
                fee_per_millionth,
            });
        graph_source
            .entry(source.to_string())
            .or_default()
            .push(GraphChannelEdge {
                active,
                amount_msat,
            });

        // Both endpoints, so a route-pair lookup from either side resolves.
        if let Some(scid) = ch.get("short_channel_id").and_then(Value::as_str) {
            channel_to_peer
                .entry(scid.to_string())
                .or_insert_with(|| destination.to_string());
        }
    }

    // Patrons are the peers we actually have profitability for. A peer
    // without one is ABSENT rather than a fabricated 0% patron.
    let mut all_channels: Vec<PatronCandidate> = sources
        .marginal_roi_by_peer
        .iter()
        .map(|(peer_id, roi)| PatronCandidate {
            peer_id: peer_id.clone(),
            marginal_roi_percent: *roi,
        })
        .collect();
    all_channels.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));

    Ok(DiscoveryEvidence {
        all_channels,
        // Python's ONE cache serves every caller; the same grouped map is
        // shared by the patron, graph and route-pair strategies.
        neighbor_patron_source_channels: neighbor_source.clone(),
        neighbor_capital_efficiency: None,
        graph_cached_source_channels: graph_source,
        route_pair_rows,
        channel_to_peer,
        route_peer_source_channels: neighbor_source,
        demand_flows: sources.demand_flows,
        demand_flow_sink_channels: sources.demand_flow_sink_channels,
        our_node_id: sources.our_node_id,
        max_candidate_pool: sources.max_candidate_pool,
    })
}
