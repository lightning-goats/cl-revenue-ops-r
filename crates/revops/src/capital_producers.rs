//! Task 71 / F71-R5 — REAL producers for the four open-side evidence
//! fields: `open_candidate_evidence`, `dual_fund_peers`,
//! `redeployment_winner_evs` and `recycle_candidates`.
//!
//! The finding this closes is structural. Task 67c slice 5 wired those
//! fields as `EvidenceDeps` parameters and closed the gap LIST, but nothing
//! in production produced values for them — only tests did. The planner
//! therefore reported no gaps while still planning nothing, which is a
//! worse failure than the honest gap list it replaced: an empty plan became
//! unattributable.
//!
//! [`OpenSideEvidence`] answers that by construction. Its fields are
//! PRIVATE and [`build_open_side`] is the only way to make one, so a caller
//! cannot hand-assemble an empty value. "No gaps" now implies a producer
//! actually ran, and an empty result is a measurement ("discovery found
//! nobody") rather than an absence of evidence.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use revops_capital::planner::candidate_score::CandidateEnrichmentEvidence;
use revops_capital::planner::cycle::{
    discover_peers, DiscoveryEvidence, OpenCandidateEvidence, RecycleCandidateOwned,
};
use revops_capital::planner::ev::OpenEvInputs;
use revops_capital::planner::winners::{identify_winners, WinnerCandidateEvidence};
use serde_json::Value;

use crate::open_ev_evidence::ChainCosts;

/// Everything the producers read, already fetched by the caller.
pub struct OpenSideSources {
    /// F71-R11: the candidate universe is DERIVED here by running the
    /// frozen `discover_peers` over this evidence -- it is never supplied
    /// by the caller. A caller-supplied `Vec<String>` (which is what this
    /// used to be) let an empty list masquerade as "discovery ran and found
    /// nobody", preserving the exact failure F71-R5 set out to close.
    pub discovery: DiscoveryEvidence,
    /// Winner CHANNELS, not peer ids: `identify_winners` runs here too, so
    /// an empty winner set also cannot be asserted by a caller.
    pub winner_channels: Vec<WinnerCandidateEvidence>,
    /// Slice 2's enrichment, keyed by peer.
    pub enrichment: BTreeMap<String, CandidateEnrichmentEvidence>,
    /// The raw `listnodes` reply (REQUIRED) — dual-fund support is read
    /// from it.
    pub listnodes: Result<Value, String>,
    /// Per-peer profit inheritance from closed channels (py 2875).
    pub closed_channel_daily_net_est: HashMap<String, f64>,
    pub observed_node_daily_ppm: Option<f64>,
    pub chain_costs: ChainCosts,
    /// The channel size the planner intends to open, used for candidate
    /// templates. Redeployment templates deliberately do NOT commit a size.
    pub planned_channel_size_sats: i64,
    pub min_annual_roi_pct: f64,
}

/// Typed producer refusals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProducerRefusal {
    ListnodesUnavailable(String),
    MalformedListnodes(String),
    EnrichmentMissing(String),
}

impl ProducerRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ListnodesUnavailable(_) => "producer_listnodes_unavailable",
            Self::MalformedListnodes(_) => "producer_listnodes_malformed",
            Self::EnrichmentMissing(_) => "producer_enrichment_missing",
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Self::ListnodesUnavailable(d)
            | Self::MalformedListnodes(d)
            | Self::EnrichmentMissing(d) => d,
        }
    }
}

/// The produced open-side evidence.
///
/// Fields are private ON PURPOSE (F71-R5): only [`build_open_side`] can
/// construct this, so an empty value always means a producer ran and
/// measured nothing — never that no producer ran. Accessors are read-only.
#[derive(Debug, Clone)]
pub struct OpenSideEvidence {
    /// F71-R13: the SAME instances that fed `discover_peers` are carried
    /// through to `CycleEvidence`. They used to be separate `EvidenceDeps`
    /// fields, which let a caller produce from snapshot A and then assemble
    /// with snapshot B -- reintroducing stale or missing per-peer evidence
    /// while the gap list stayed empty.
    discovery: DiscoveryEvidence,
    candidate_enrichment: BTreeMap<String, CandidateEnrichmentEvidence>,
    open_candidate_evidence: BTreeMap<String, OpenCandidateEvidence>,
    dual_fund_peers: BTreeSet<String>,
    redeployment_winner_evs: Vec<(String, OpenEvInputs)>,
    recycle_candidates: Vec<RecycleCandidateOwned>,
}

impl OpenSideEvidence {
    pub fn discovery(&self) -> &DiscoveryEvidence {
        &self.discovery
    }

    pub fn candidate_enrichment(&self) -> &BTreeMap<String, CandidateEnrichmentEvidence> {
        &self.candidate_enrichment
    }

    pub fn open_candidate_evidence(&self) -> &BTreeMap<String, OpenCandidateEvidence> {
        &self.open_candidate_evidence
    }

    pub fn dual_fund_peers(&self) -> &BTreeSet<String> {
        &self.dual_fund_peers
    }

    pub fn redeployment_winner_evs(&self) -> &[(String, OpenEvInputs)] {
        &self.redeployment_winner_evs
    }

    pub fn recycle_candidates(&self) -> &[RecycleCandidateOwned] {
        &self.recycle_candidates
    }

    /// Move the produced fields into the kernel's evidence shape. Consuming
    /// keeps this the single hand-off point.
    pub fn into_parts(self) -> OpenSideParts {
        OpenSideParts {
            discovery: self.discovery,
            candidate_enrichment: self.candidate_enrichment,
            open_candidate_evidence: self.open_candidate_evidence,
            dual_fund_peers: self.dual_fund_peers,
            redeployment_winner_evs: self.redeployment_winner_evs,
            recycle_candidates: self.recycle_candidates,
        }
    }
}

/// The produced fields, moved out for assembly.
pub struct OpenSideParts {
    pub discovery: DiscoveryEvidence,
    pub candidate_enrichment: BTreeMap<String, CandidateEnrichmentEvidence>,
    pub open_candidate_evidence: BTreeMap<String, OpenCandidateEvidence>,
    pub dual_fund_peers: BTreeSet<String>,
    pub redeployment_winner_evs: Vec<(String, OpenEvInputs)>,
    pub recycle_candidates: Vec<RecycleCandidateOwned>,
}

/// Peers whose `listnodes` entry advertises dual-fund (liquidity ads).
///
/// py 707: `bool(node_info and node_info.get("option_will_fund"))` — a
/// TRUTHY check. An explicitly null or empty `option_will_fund` is NOT
/// support; treating mere key presence as support would let a blocked
/// portfolio open to peers that cannot actually lease, which is exactly
/// what the blocked state exists to restrict.
fn dual_fund_peers_from_listnodes(listnodes: &Value) -> Result<BTreeSet<String>, ProducerRefusal> {
    // F71-R12(b): a successful reply whose `nodes` is missing or wrongly
    // typed is NOT an empty measurement. Reading it as "nobody supports
    // dual-fund" silently blocks every constrained-portfolio open, which
    // looks exactly like a healthy cycle that found nothing worth doing.
    // A real `nodes: []` stays a measured empty.
    let nodes = listnodes
        .get("nodes")
        .unwrap_or(&Value::Null)
        .as_array()
        .ok_or_else(|| ProducerRefusal::MalformedListnodes("nodes is not an array".into()))?;
    for n in nodes {
        if !n.get("nodeid").is_some_and(Value::is_string) {
            return Err(ProducerRefusal::MalformedListnodes(format!(
                "node entry has no string nodeid: {n}"
            )));
        }
    }
    Ok(nodes
        .iter()
        .filter(|n| match n.get("option_will_fund") {
            None | Some(Value::Null) => false,
            Some(Value::Object(o)) => !o.is_empty(),
            Some(Value::Array(a)) => !a.is_empty(),
            Some(Value::Bool(b)) => *b,
            Some(Value::String(s)) => !s.is_empty(),
            Some(Value::Number(n)) => n.as_f64().is_some_and(|v| v != 0.0),
        })
        .filter_map(|n| n.get("nodeid").and_then(Value::as_str))
        .map(str::to_string)
        .collect())
}

/// Build the open-EV template for one peer. `channel_size_sats` is supplied
/// by the caller because the two consumers differ: an open candidate
/// commits to the planned size, while a redeployment template leaves it as
/// a placeholder for F71-R10's per-loser substitution.
fn ev_template(sources: &OpenSideSources, peer_id: &str, channel_size_sats: i64) -> OpenEvInputs {
    OpenEvInputs {
        channel_size_sats,
        closed_channel_daily_net_est_sats: sources
            .closed_channel_daily_net_est
            .get(peer_id)
            .copied(),
        observed_node_daily_ppm: sources.observed_node_daily_ppm,
        open_cost_sats: sources.chain_costs.open_cost_sats,
        close_cost_sats: sources.chain_costs.close_cost_sats,
        inbound_median_fee_ppm: sources
            .enrichment
            .get(peer_id)
            .and_then(|e| e.inbound_median_fee_ppm),
        min_annual_roi_pct: sources.min_annual_roi_pct,
    }
}

/// Produce all four open-side fields.
pub fn build_open_side(sources: OpenSideSources) -> Result<OpenSideEvidence, ProducerRefusal> {
    let listnodes = sources
        .listnodes
        .as_ref()
        .map_err(|e| ProducerRefusal::ListnodesUnavailable(e.clone()))?;
    let dual_fund_peers = dual_fund_peers_from_listnodes(listnodes)?;

    // F71-R11: both sets are DERIVED from the frozen kernels here. Neither
    // the candidate universe nor the winner set can be asserted by a
    // caller, so an empty result always means the real producer ran.
    let winners = identify_winners(&sources.winner_channels);
    let candidates = discover_peers(&winners, &sources.discovery, &sources.enrichment);
    let candidates: Vec<(String, f64)> = candidates
        .into_iter()
        .map(|c| (c.peer_id, c.score))
        .collect();

    let mut open_candidate_evidence = BTreeMap::new();
    let mut recycle_candidates = Vec::new();

    for (peer_id, score) in &candidates {
        // A candidate with no enrichment REFUSES rather than being scored
        // on blanks: enrichment is what the frozen scorer multiplies by, so
        // a blank-scored or silently-dropped candidate changes the ranking
        // with no signal at all.
        let enrichment = sources.enrichment.get(peer_id).cloned().ok_or_else(|| {
            ProducerRefusal::EnrichmentMissing(format!("no enrichment for candidate {peer_id}"))
        })?;

        // F71-R12(c): the score comes from the frozen discovery output, so
        // there is no missing-score case to paper over with 0.0. A
        // non-finite score would still corrupt the recycle top-5 ranking
        // silently, so it refuses.
        if !score.is_finite() {
            return Err(ProducerRefusal::EnrichmentMissing(format!(
                "candidate {peer_id} has a non-finite discovery score"
            )));
        }

        open_candidate_evidence.insert(
            peer_id.clone(),
            OpenCandidateEvidence {
                peer_dest_channel_capacities_sats: enrichment.dest_channel_capacities_sats.clone(),
                open_ev_template: ev_template(&sources, peer_id, sources.planned_channel_size_sats),
                enrichment,
            },
        );

        recycle_candidates.push(RecycleCandidateOwned {
            peer_id: peer_id.clone(),
            score: *score,
            // The loser's capacity is substituted per pairing, so no size
            // is committed here either.
            open_ev_template: ev_template(&sources, peer_id, 0),
        });
    }

    // F71-R10: templates, not scalars. `channel_size_sats` stays a
    // placeholder -- each loser's capacity is substituted at pricing time.
    let redeployment_winner_evs = winners
        .iter()
        .map(|w| (w.peer_id.clone(), ev_template(&sources, &w.peer_id, 0)))
        .collect();

    Ok(OpenSideEvidence {
        // The same instances that fed discover_peers above.
        discovery: sources.discovery,
        candidate_enrichment: sources.enrichment,
        open_candidate_evidence,
        dual_fund_peers,
        redeployment_winner_evs,
        recycle_candidates,
    })
}
