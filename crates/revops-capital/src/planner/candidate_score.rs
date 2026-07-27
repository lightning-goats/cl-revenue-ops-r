//! Port of `_score_candidate` (py `modules/capacity_planner.py` 2110-2201):
//! candidate enrichment via six independent evidence signals plus the
//! demand-flow role boost, applied as successive multipliers to a base
//! score. Every `try/except: pass` in Python (a lookup that silently no-ops
//! on failure) is modeled as an `Option` that, when `None`, skips that
//! multiplier — exactly Python's fail-open-on-missing-signal behavior for
//! this SCORING (not gating) path.

/// Peer reputation counts (py 2116-2121: `get_peer_reputation`).
#[derive(Debug, Clone, Copy)]
pub struct PeerReputation {
    pub successes: i64,
    pub failures: i64,
}

/// `get_peer_closed_channel_profit_summary` result fields this function
/// reads (py 2127-2129).
#[derive(Debug, Clone, Copy)]
pub struct ClosedChannelProfitSummary {
    pub marginal_roi_proxy: f64,
}

/// A destination-direction channel capacity sample (py 2171-2179: `active`,
/// `amount_msat > 0`).
#[derive(Debug, Clone, Copy)]
pub struct PeerChannelCapacitySample {
    pub capacity_sats: i64,
}

/// The demand-flow role signal (py 2189-2199); mirrors
/// [`super::demand_flow::FlowRole`] but as a plain enum here to avoid a
/// hard dependency direction requirement on callers that did not run
/// demand-flow discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemandFlowRole {
    Sink,
    Source,
    Unknown,
    /// Router, or any other classified role — no boost/penalty applies (py:
    /// only `"sink"`/`"source"`/`"unknown"` are branched on; anything else,
    /// e.g. `"router"`, falls through the `elif` chain unchanged).
    Other,
}

/// Every input `_score_candidate` needs (py 2110-2201). All fields are
/// `Option`/default-empty because each corresponding Python lookup is
/// wrapped in its own `try/except: pass` — a missing signal simply skips
/// that multiplier, it never fails the whole score to zero or an error.
#[derive(Debug, Clone, Default)]
pub struct CandidateEnrichmentEvidence {
    pub reputation: Option<PeerReputation>,
    pub closed_channel_profit: Option<ClosedChannelProfitSummary>,
    /// `get_peer_uptime_percent(peer_id, duration_seconds=604800)`.
    pub uptime_pct: Option<f64>,
    /// `true` iff `_get_cached_node(peer_id)` returned a node with at least
    /// one `ipv4`/`ipv6` address (py 2143-2151).
    pub has_clearnet_address: bool,
    /// `get_historical_inbound_fee_ppm(peer_id)["median_fee_ppm"]` (py
    /// 2157-2163); `None` mirrors a missing/non-dict result.
    pub inbound_median_fee_ppm: Option<f64>,
    /// `listchannels(destination=peer_id)` capacities of ACTIVE channels
    /// with positive amount (py 2171-2179) — the caller pre-filters exactly
    /// as Python's list comprehension does.
    pub dest_channel_capacities_sats: Vec<i64>,
    /// `true` iff `peer_id in self._demand_flow_sink_adjacent` (py 2190).
    pub is_sink_adjacent: bool,
    /// `self._demand_flow_profiles[peer_id].role`, consulted only when
    /// `is_sink_adjacent` is `false` (py 2192-2199's `elif`); `None`
    /// mirrors "peer not in the profile map" (no boost/penalty).
    pub demand_flow_role: Option<DemandFlowRole>,
}

/// Port of `_score_candidate` (py 2110-2201).
pub fn score_candidate(base_score: f64, evidence: &CandidateEnrichmentEvidence) -> f64 {
    let mut score = base_score;

    if let Some(rep) = evidence.reputation {
        let rep_score =
            (rep.successes as f64 + 1.0) / (rep.successes as f64 + rep.failures as f64 + 2.0);
        score *= rep_score;
    }

    if let Some(closed) = &evidence.closed_channel_profit {
        if closed.marginal_roi_proxy > 0.0 {
            score *= 1.5;
        }
    }

    if let Some(uptime) = evidence.uptime_pct {
        if uptime < 90.0 {
            score *= uptime / 100.0;
        }
    }

    if evidence.has_clearnet_address {
        score *= 1.25;
    }

    if let Some(median_inbound) = evidence.inbound_median_fee_ppm {
        if median_inbound > 200.0 {
            let penalty = ((median_inbound - 200.0) / 1000.0).min(0.4);
            score *= 1.0 - penalty;
        }
    }

    if !evidence.dest_channel_capacities_sats.is_empty() {
        let mut caps = evidence.dest_channel_capacities_sats.clone();
        caps.sort_unstable();
        // py 2181: `sorted(capacities)[len(capacities) // 2]` — the
        // Python "median" here is the plain middle-index element (no
        // even-length averaging).
        let median_cap = caps[caps.len() / 2];
        if median_cap >= 10_000_000 {
            score *= 1.2;
        } else if median_cap >= 5_000_000 {
            score *= 1.1;
        }
    }

    if evidence.is_sink_adjacent {
        score *= 1.4;
    } else if let Some(role) = evidence.demand_flow_role {
        match role {
            DemandFlowRole::Sink => score *= 1.3,
            DemandFlowRole::Source => score *= 1.2,
            DemandFlowRole::Unknown => score *= 0.9,
            DemandFlowRole::Other => {}
        }
    }

    score
}
