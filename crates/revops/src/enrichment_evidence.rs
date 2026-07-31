//! Task 67c slice 2 — per-candidate enrichment assembly.
//!
//! Builds [`CandidateEnrichmentEvidence`] for each discovered candidate so
//! the FROZEN `score_candidate` kernel can weigh it. Nothing here decides
//! anything: it projects already-read sources into the kernel's input
//! shape, keeping ABSENT distinct from zero, because `None` and
//! `Some(0.0)` score differently.
//!
//! Two Python behaviours are deliberately preserved even though the
//! natural Rust answer differs:
//!
//!  * **Cold-start uptime is 100%** (py `get_peer_uptime_percent` 7235). A
//!    peer we have never seen connect is assumed up. `None` would drop the
//!    multiplier and `Some(0.0)` would zero out every newly-discovered
//!    peer — precisely the set discovery exists to surface.
//!  * **Inbound fees need three samples** (py 4877). Below that there is no
//!    signal at all, rather than a fee inferred from one or two rebalances.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use revops_capital::planner::candidate_score::{
    CandidateEnrichmentEvidence, ClosedChannelProfitSummary, DemandFlowRole, PeerReputation,
};
use revops_capital::planner::demand_flow::FlowRole;
use revops_core::msat::{base_to_sats_floor, parse_msat};
use serde_json::Value;

/// `get_peer_uptime_percent`'s window (py 2137: `duration_seconds=604800`).
pub const UPTIME_WINDOW_SECONDS: i64 = 604_800;

/// Below this many successful rebalances there is no inbound-fee signal
/// (py 4812: `min_samples: int = 3`).
pub const INBOUND_FEE_MIN_SAMPLES: usize = 3;

/// A `peer_reputation` row (columns `success_count` / `failure_count`).
#[derive(Debug, Clone, Copy)]
pub struct ReputationRow {
    pub success_count: i64,
    pub failure_count: i64,
}

/// A `peer_connection_history` row.
#[derive(Debug, Clone)]
pub struct ConnectionEvent {
    pub event_type: String,
    pub timestamp: i64,
}

/// One successful rebalance TO the peer, from `rebalance_history`.
#[derive(Debug, Clone, Copy)]
pub struct InboundFeeSample {
    pub amount_sats: i64,
    pub fee_msat: i64,
}

/// Everything the assembler reads. Each fallible source is `Result`-shaped
/// so a read failure refuses rather than silently enriching with blanks —
/// blanks would score every candidate as unmeasured, which reads exactly
/// like a graph full of bad peers.
pub struct EnrichmentSources {
    pub reputation: Result<HashMap<String, ReputationRow>, String>,
    pub connection_events: Result<HashMap<String, Vec<ConnectionEvent>>, String>,
    pub inbound_fee_samples: Result<HashMap<String, Vec<InboundFeeSample>>, String>,
    /// `marginal_roi_proxy` from the closed-channel profit summary. Absent
    /// peers simply have no closed channels — not a failure.
    pub closed_channel_roi_proxy: Result<HashMap<String, f64>, String>,
    /// The same single `listchannels` array slice 1 groups; here it is read
    /// in the DESTINATION direction.
    pub gossip_channels: Result<Vec<Value>, String>,
    /// Peers whose `listnodes` entry carries at least one `ipv4`/`ipv6`
    /// address (py 2143-2151).
    pub clearnet_peers: Result<BTreeSet<String>, String>,
    pub sink_adjacent_peers: BTreeSet<String>,
    pub demand_flow_roles: BTreeMap<String, FlowRole>,
    pub now: i64,
}

/// A required source could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnrichmentRefusal {
    ReputationUnavailable(String),
    ConnectionHistoryUnavailable(String),
    InboundFeesUnavailable(String),
    ClosedChannelProfitUnavailable(String),
    GossipUnavailable(String),
    NodeAddressesUnavailable(String),
}

impl EnrichmentRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ReputationUnavailable(_) => "enrichment_reputation_unavailable",
            Self::ConnectionHistoryUnavailable(_) => "enrichment_connection_history_unavailable",
            Self::InboundFeesUnavailable(_) => "enrichment_inbound_fees_unavailable",
            Self::ClosedChannelProfitUnavailable(_) => {
                "enrichment_closed_channel_profit_unavailable"
            }
            Self::GossipUnavailable(_) => "enrichment_gossip_unavailable",
            Self::NodeAddressesUnavailable(_) => "enrichment_node_addresses_unavailable",
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Self::ReputationUnavailable(d)
            | Self::ConnectionHistoryUnavailable(d)
            | Self::InboundFeesUnavailable(d)
            | Self::ClosedChannelProfitUnavailable(d)
            | Self::GossipUnavailable(d)
            | Self::NodeAddressesUnavailable(d) => d,
        }
    }
}

/// Port of `get_peer_uptime_percent` (py 7196-7274).
///
/// `events` must be every `peer_connection_history` row for the peer at or
/// after `now - window_seconds`, ascending, plus — if one exists — the
/// single most recent row BEFORE the window, which establishes the state at
/// window start.
pub fn uptime_percent(events: &[ConnectionEvent], now: i64, window_seconds: i64) -> f64 {
    let window_start = now - window_seconds;
    let (prior, in_window): (Vec<&ConnectionEvent>, Vec<&ConnectionEvent>) =
        events.iter().partition(|e| e.timestamp < window_start);
    let prior = prior.last();

    // COLD START: no history at all means fully up (py 7235).
    if prior.is_none() && in_window.is_empty() {
        return 100.0;
    }

    // With history predating the window the denominator is the whole
    // window; otherwise it is only the span we have actually observed, so a
    // peer first seen yesterday is not judged over a week it did not exist
    // for (py 7241-7244).
    let effective_start = match prior {
        Some(_) => window_start,
        None => in_window[0].timestamp,
    };
    let actual_duration = now - effective_start;

    // Too short a span is noise (py 7248).
    if actual_duration < 60 {
        return 100.0;
    }

    let mut is_connected = prior.is_some_and(|e| is_connected_event(&e.event_type));
    let mut last_interval_start = effective_start;
    let mut total_connected = 0i64;

    for e in &in_window {
        if is_connected {
            total_connected += e.timestamp - last_interval_start;
        }
        is_connected = is_connected_event(&e.event_type);
        last_interval_start = e.timestamp;
    }
    if is_connected {
        total_connected += now - last_interval_start;
    }

    let pct = (total_connected as f64 / actual_duration as f64) * 100.0;
    pct.clamp(0.0, 100.0)
}

/// `snapshot` counts as connected (py 7224/7263) — the startup-snapshot
/// owner writes those rows for every already-connected peer, so excluding
/// them would read the whole fleet as down after a restart.
fn is_connected_event(event_type: &str) -> bool {
    matches!(event_type, "connected" | "snapshot")
}

/// Port of `get_historical_inbound_fee_ppm`'s `median_fee_ppm` (py
/// 4811-4910). `None` below [`INBOUND_FEE_MIN_SAMPLES`].
pub fn inbound_fee_ppm(samples: &[InboundFeeSample]) -> Option<f64> {
    let usable: Vec<&InboundFeeSample> = samples
        .iter()
        .filter(|s| s.amount_sats > 0 && s.fee_msat > 0)
        .collect();
    if usable.len() < INBOUND_FEE_MIN_SAMPLES {
        return None;
    }
    let total_volume: i64 = usable.iter().map(|s| s.amount_sats).sum();
    if total_volume == 0 {
        return None;
    }
    let mut ppms: Vec<i64> = usable
        .iter()
        .map(|s| (s.fee_msat * 1_000) / s.amount_sats)
        .collect();
    ppms.sort_unstable();
    let mid = ppms.len() / 2;
    // Python floor-divides the two middle samples on an even count.
    let median = if ppms.len().is_multiple_of(2) {
        (ppms[mid - 1] + ppms[mid]) / 2
    } else {
        ppms[mid]
    };
    Some(median as f64)
}

/// Destination-direction capacities, filtered exactly as py 2171-2179:
/// ACTIVE channels with a positive amount.
fn dest_capacities_sats(gossip: &[Value], peer_id: &str) -> Vec<i64> {
    gossip
        .iter()
        .filter(|c| c.get("destination").and_then(Value::as_str) == Some(peer_id))
        .filter(|c| c.get("active").and_then(Value::as_bool).unwrap_or(false))
        // Review finding F71-R1: `amount_msat` arrives as EITHER a JSON
        // integer or a string such as "2000000000msat" -- the shared
        // prefetch preserves the raw reply, and Python reads it with
        // `parse_msat`. `as_i64` silently drops every string form, which
        // erases the 5M/10M large-channel bonus (py 2181-2185) and
        // reorders open candidates with no error and no refusal.
        .map(|c| parse_msat(c.get("amount_msat").unwrap_or(&Value::Null)))
        .filter(|msat| *msat > 0)
        .map(|msat| base_to_sats_floor(msat as u64) as i64)
        .collect()
}

/// `Router` maps to `Other`, NOT to `Unknown`: the frozen scorer penalises
/// unknown and leaves other roles alone, so collapsing the two would
/// penalise every well-classified router on the graph.
fn project_role(role: FlowRole) -> DemandFlowRole {
    match role {
        FlowRole::Sink => DemandFlowRole::Sink,
        FlowRole::Source => DemandFlowRole::Source,
        FlowRole::Unknown => DemandFlowRole::Unknown,
        FlowRole::Router => DemandFlowRole::Other,
    }
}

/// Build enrichment evidence for each candidate peer.
pub fn build_enrichment(
    peer_ids: &[String],
    sources: EnrichmentSources,
) -> Result<BTreeMap<String, CandidateEnrichmentEvidence>, EnrichmentRefusal> {
    let reputation = sources
        .reputation
        .map_err(EnrichmentRefusal::ReputationUnavailable)?;
    let connection_events = sources
        .connection_events
        .map_err(EnrichmentRefusal::ConnectionHistoryUnavailable)?;
    let inbound_fee_samples = sources
        .inbound_fee_samples
        .map_err(EnrichmentRefusal::InboundFeesUnavailable)?;
    let closed_roi = sources
        .closed_channel_roi_proxy
        .map_err(EnrichmentRefusal::ClosedChannelProfitUnavailable)?;
    let gossip = sources
        .gossip_channels
        .map_err(EnrichmentRefusal::GossipUnavailable)?;
    let clearnet = sources
        .clearnet_peers
        .map_err(EnrichmentRefusal::NodeAddressesUnavailable)?;

    let mut out = BTreeMap::new();
    for peer_id in peer_ids {
        let is_sink_adjacent = sources.sink_adjacent_peers.contains(peer_id);
        out.insert(
            peer_id.clone(),
            CandidateEnrichmentEvidence {
                reputation: reputation.get(peer_id).map(|r| PeerReputation {
                    successes: r.success_count,
                    failures: r.failure_count,
                }),
                closed_channel_profit: closed_roi.get(peer_id).map(|roi| {
                    ClosedChannelProfitSummary {
                        marginal_roi_proxy: *roi,
                    }
                }),
                uptime_pct: Some(uptime_percent(
                    connection_events.get(peer_id).map_or(&[], Vec::as_slice),
                    sources.now,
                    UPTIME_WINDOW_SECONDS,
                )),
                has_clearnet_address: clearnet.contains(peer_id),
                inbound_median_fee_ppm: inbound_fee_samples
                    .get(peer_id)
                    .and_then(|s| inbound_fee_ppm(s)),
                dest_channel_capacities_sats: dest_capacities_sats(&gossip, peer_id),
                is_sink_adjacent,
                // Consulted only when NOT sink-adjacent (py 2192-2199's
                // `elif`); carrying it regardless is harmless but the
                // adjacency flag is what the kernel branches on first.
                demand_flow_role: sources
                    .demand_flow_roles
                    .get(peer_id)
                    .copied()
                    .map(project_role),
            },
        );
    }
    Ok(out)
}
