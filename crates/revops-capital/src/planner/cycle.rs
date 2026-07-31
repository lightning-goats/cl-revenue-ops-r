//! Port of `CapacityPlanner.execute_cycle` (py `modules/capacity_planner.py`
//! 363-786) and `_discover_peers` (2714-2755) — the top-level per-cycle
//! orchestration — WITHOUT any live-mutation capability (hard rule #1: this
//! crate is structurally incapable of constructing an RPC mutation; see the
//! crate root doc comment). [`plan_cycle`] takes a single, fully-populated
//! [`CycleEvidence`] (every RPC/DB read Python performs inline, already
//! fetched and typed) and returns a [`CyclePlan`]: what the cycle WOULD do
//! (opens/closes/defibrillations, with reasons), never what it did.
//!
//! # Composition, matching Python's call order exactly
//!
//! 1. `planner_enabled` gate (py 368-369) -> early return.
//! 2. Fee gate result (py `_check_fee_gate`, hoisted — budget/capex
//!    dependent, not ported).
//! 3. [`super::winners::identify_winners`] / [`super::losers::identify_losers`]
//!    (py 394-400).
//! 4. [`super::recycle::apply_redeployment_ev_demotion`] (py 406,
//!    `_apply_redeployment_ev_demotion`).
//! 5. Defibrillation selection: worst-marginal-ROI-first, bounded by
//!    `defibrillation_limit`, gated by cooldown/recently-attempted/policy
//!    evidence (py 415-455).
//! 6. Close selection: worst-marginal-ROI-first, bounded by
//!    `close_limit`, gated by close-allowed/safety-guard/cooldown evidence
//!    (py 457-512). Python's `_arbitrate_close_list` batch-dedup/conflict-arming
//!    (DB-registry-backed) is NOT re-implemented — see the module's
//!    "Declared gaps" section in `crates/revops-capital/TASK47-REPORT.md`;
//!    the input order (worst-ROI-first) is used directly.
//! 7. [`super::portfolio_gate::check_portfolio_balance_gate`] (py 514-525).
//! 8. [`discover_peers`] — all five discovery strategies, normalize, dedup,
//!    [`super::candidate_score::score_candidate`] enrichment, pool quotas
//!    (py 527-539, `_discover_peers` 2714-2755) — only when the fee gate is
//!    open (py 534, F6c: discovery still runs even when portfolio-blocked).
//! 9. Funding-deficit detection (py 564-575).
//! 10. Per-candidate evaluation: [`super::gates::failed_open_backoff_reason`]
//!     / [`super::gates::peer_exposure_cap_reason`], [`super::sizing::size_channel`],
//!     [`super::ev::calculate_open_ev`] (py 608-646).
//! 11. Opens selection: portfolio-state remedy gate, safety guards,
//!     exploration budget, up to `max_opens_per_cycle` (py 674-745).
//! 12. [`super::recycle::find_best_recycle_pair`] over already-eligible
//!     losers (py 750-766).

use std::collections::{BTreeMap, BTreeSet};

use super::candidate_score::CandidateEnrichmentEvidence;
use super::discovery::{self, DiscoveredCandidate};
use super::ev::RedeploymentCandidate;
use super::ev::{calculate_open_ev, OpenEvInputs};
use super::gates::{
    failed_open_backoff_reason, peer_exposure_cap_reason, PeerChannelCapacity, PlannerActionRecord,
};
use super::losers::{identify_losers, Loser, LoserAction, LoserChannelEvidence};
use super::portfolio_gate::{check_portfolio_balance_gate, ChannelBalance, PortfolioBalanceState};
use super::recycle::{
    apply_redeployment_ev_demotion, find_best_recycle_pair, EligibleLoser, RecycleCandidate,
    RecyclePlan,
};
use super::scoring::{apply_pool_quotas, normalize_candidate_scores, Candidate};
use super::sizing::{size_channel, SizingCandidate, SizingConfig};
use super::winners::{identify_winners, Winner, WinnerCandidateEvidence};

/// Task 47 correction round 1, finding 1: the maximum age (inclusive,
/// seconds) a defib/close/open gate evidence entry's `observed_at` may be
/// relative to [`CycleEvidence::now`] before it is treated as stale and the
/// action DENIED — mirroring the fail-closed freshness contract already
/// established by `crates/revops/src/python_authority.rs`'s
/// `validate_status` (`age_seconds < 0 || age_seconds > max_age_seconds` ->
/// deny). Present-but-stale safety-gate evidence must be indistinguishable
/// from missing evidence, never treated as "still allowed".
///
/// 900s (15 minutes) is sized to comfortably cover one planner cycle's
/// evidence-gathering pass (which fans out per-peer over potentially dozens
/// of peers' RPC/DB reads) while still catching evidence that was cached or
/// reused across cycles instead of freshly fetched for the cycle actually
/// being planned — the concrete risk the review flagged. Callers assembling
/// [`CycleEvidence`] must stamp each gate's `observed_at` with the
/// wall-clock time its underlying DB/RPC read actually happened, not a
/// cycle-start constant reused for every peer.
pub const GATE_EVIDENCE_MAX_AGE_SECS: i64 = 900;

/// Per-peer gate evidence for the defibrillation-selection loop (py's
/// `_check_cooldown` / `_defib_recently_attempted` / `_check_defib_allowed`,
/// each hoisted as `Some(reason)` when the gate BLOCKS, `None` when it
/// allows). Finding 1 (Task 47 correction round 1): a peer with NO entry in
/// [`CycleEvidence::defib_gates`], or an entry whose `observed_at` is more
/// than [`GATE_EVIDENCE_MAX_AGE_SECS`] from [`CycleEvidence::now`] (or in
/// the future), is DENIED with an actionable reason — never defaulted to
/// "no reason on file, therefore allowed".
#[derive(Debug, Clone, Default)]
pub struct DefibGate {
    pub observed_at: i64,
    pub cooldown_blocked: Option<String>,
    pub recently_attempted_blocked: Option<String>,
    pub policy_blocked: Option<String>,
}

/// Per-peer gate evidence for the close-selection loop (py's
/// `_check_close_allowed` / `_check_safety_guards` / `_check_cooldown`).
/// Same missing/stale-deny contract as [`DefibGate`] (finding 1).
#[derive(Debug, Clone, Default)]
pub struct CloseGate {
    pub observed_at: i64,
    pub close_allowed_blocked: Option<String>,
    pub safety_guard_blocked: Option<String>,
    pub cooldown_blocked: Option<String>,
}

/// Per-peer gate evidence for the open-execution loop (py's
/// `_check_safety_guards(cfg, "open", ...)`). Same missing/stale-deny
/// contract as [`DefibGate`] (finding 1).
#[derive(Debug, Clone, Default)]
pub struct OpenGuard {
    pub observed_at: i64,
    pub blocked: Option<String>,
}

/// `true` when `observed_at` is within `[now - GATE_EVIDENCE_MAX_AGE_SECS,
/// now]` inclusive — i.e. neither stale nor from-the-future (clock skew is
/// exactly as untrusted as staleness: neither is proof the evidence
/// reflects the peer's CURRENT state).
fn gate_evidence_is_fresh(observed_at: i64, now: i64) -> bool {
    matches!(gate_evidence_age(observed_at, now),
             Some(age) if (0..=GATE_EVIDENCE_MAX_AGE_SECS).contains(&age))
}

/// Overflow-safe age (correction round 2): `now.checked_sub(observed_at)`.
/// `None` means the subtraction itself overflowed — evidence so malformed
/// that its age cannot even be computed. That is DENIED like staleness,
/// never defaulted: unchecked `now - observed_at` panicked in debug on
/// `observed_at = i64::MIN`, and under wrapping arithmetic
/// `(now = i64::MIN + 900, observed_at = i64::MAX)` wrapped to 899 —
/// INSIDE the accepted window — turning untrusted evidence into fresh
/// evidence. The skip-reason paths format from THIS value so the
/// subtraction is never repeated unchecked.
fn gate_evidence_age(observed_at: i64, now: i64) -> Option<i64> {
    now.checked_sub(observed_at)
}

/// Human text for a denied gate age (correction round 2): shared by all
/// three action families so no reason branch repeats raw subtraction.
fn gate_age_denial_text(observed_at: i64, now: i64) -> String {
    match gate_evidence_age(observed_at, now) {
        Some(age) => format!("observed {age}s ago (max {GATE_EVIDENCE_MAX_AGE_SECS}s)"),
        None => "timestamp arithmetic overflow (malformed evidence)".to_string(),
    }
}

/// Evidence for one open-candidate's EV evaluation (py 608-646, 674-698):
/// the sizing pool's OTHER candidates' scores (for [`size_channel`]'s
/// ROI-weighting), the peer's destination-channel capacities (shared by
/// sizing's competitive floor AND [`CandidateEnrichmentEvidence`]'s
/// large-channel bonus), and an [`OpenEvInputs`] TEMPLATE whose
/// `channel_size_sats` this module overwrites with the sized amount.
#[derive(Debug, Clone)]
pub struct OpenCandidateEvidence {
    pub peer_dest_channel_capacities_sats: Vec<i64>,
    pub open_ev_template: OpenEvInputs,
    pub enrichment: CandidateEnrichmentEvidence,
}

/// Evidence for the five discovery strategies (py `_discover_peers`
/// 2714-2755). See each strategy's module (`super::discovery`,
/// `super::demand_flow`) for field-level provenance.
#[derive(Debug, Clone, Default)]
pub struct DiscoveryEvidence {
    pub all_channels: Vec<discovery::PatronCandidate>,
    /// Shared "cached `listchannels(source=peer_id)`" evidence, keyed by
    /// peer_id — used by [`discovery::discover_from_neighbors`]'s patrons
    /// AND, when [`Self::neighbor_capital_efficiency`] is present, by
    /// [`discovery::discover_from_neighbors_capital_efficiency`]'s patron
    /// pool AND its second-hop lookups (py's single `_get_cached_channels`
    /// cache serves every caller).
    pub neighbor_patron_source_channels: BTreeMap<String, Vec<NeighborEdge>>,
    /// Present <=> py's `self._capital_efficiency is not None` (Task 47
    /// correction round 1, finding 2): when supplied, neighbor discovery
    /// (Strategy 2) runs [`discovery::discover_from_neighbors_capital_efficiency`]
    /// instead of the no-capital-efficiency fallback
    /// [`discovery::discover_from_neighbors`] — mirroring Python's branch
    /// selection, decided by whoever assembles this evidence (they know
    /// whether a capital-efficiency analyzer was injected).
    pub neighbor_capital_efficiency: Option<Vec<discovery::PatronPoolInput>>,
    pub graph_cached_source_channels: BTreeMap<String, Vec<discovery::GraphChannelEdge>>,
    pub route_pair_rows: Vec<discovery::RoutePairRow>,
    pub channel_to_peer: BTreeMap<String, String>,
    pub route_peer_source_channels: BTreeMap<String, Vec<NeighborEdge>>,
    pub demand_flows: Vec<FlowContribution>,
    pub demand_flow_sink_channels: BTreeMap<String, Vec<super::demand_flow::SinkChannelEdge>>,
    pub our_node_id: String,
    /// py `_apply_pool_quotas`'s default `max_pool=32` (2267); expose it as
    /// evidence rather than a hardcoded literal so callers can match a
    /// non-default config without editing this crate.
    pub max_candidate_pool: i64,
}

/// A channel/edge for both neighbor-discovery evidence maps (py's cached
/// `listchannels(source=peer)` rows: `destination`, `amount_msat`,
/// `fee_per_millionth`) — an owned equivalent of
/// [`discovery::NeighborChannelEdge`] since [`DiscoveryEvidence`] must own
/// its data.
#[derive(Debug, Clone)]
pub struct NeighborEdge {
    pub destination: String,
    pub amount_msat: i64,
    pub fee_per_millionth: i64,
}

/// Every input [`plan_cycle`] needs — the complete, already-fetched
/// evidence for one cycle. See the module doc comment for the exact
/// Python call order this replicates.
#[derive(Debug, Clone, Default)]
pub struct CycleEvidence {
    pub planner_enabled: bool,
    pub fee_gate_ok: bool,
    pub fee_gate_reason: Option<String>,

    pub winner_channels: Vec<WinnerCandidateEvidence>,
    pub loser_channels: Vec<LoserChannelEvidence>,
    /// Per-winner open-EV TEMPLATES. Each carries every `OpenEvInputs`
    /// field except `channel_size_sats`; the loser's capacity is
    /// substituted when that loser is priced.
    ///
    /// F71-R10: this was `Vec<(String, f64)>` — one EV per winner, computed
    /// once and reused for every loser. Python recomputes
    /// `_calculate_open_ev(winner["peer_id"], loser_capacity, cfg)` inside
    /// each per-loser call (py 2957), and `calculate_open_ev` scales with
    /// `channel_size_sats`, so the scalar shape could not reproduce
    /// unequal-capacity losers: winner EV, chosen peer, and
    /// close-vs-defibrillate could all diverge.
    pub redeployment_winner_evs: Vec<(String, super::ev::OpenEvInputs)>,

    pub defibrillation_limit: i64,
    pub defib_gates: BTreeMap<String, DefibGate>,

    pub close_execution_enabled: bool,
    /// `None` mirrors py's `configured_close_limit <= 0` -> "no execution
    /// cap" (recommendation-only mode still logs).
    pub close_limit: Option<i64>,
    pub close_gates: BTreeMap<String, CloseGate>,

    pub peer_channels: Vec<ChannelBalance>,

    pub discovery: DiscoveryEvidence,
    pub candidate_enrichment: BTreeMap<String, CandidateEnrichmentEvidence>,

    pub now: i64,
    pub backoff_actions: BTreeMap<String, Vec<StoredPlannerAction>>,
    pub exposure_channels: Vec<ExposureChannel>,
    pub max_channel_sats: i64,
    pub min_channel_sats: i64,
    pub open_candidate_evidence: BTreeMap<String, OpenCandidateEvidence>,

    pub available_sats: i64,
    pub max_opens_per_cycle: i64,
    pub exploration_budget_sats: i64,
    pub estimated_open_cost_sats: i64,
    /// Peers with a dual-fund (`option_will_fund`) offer OR sink-adjacency
    /// (py 705-708) — sink-adjacency from discovery is merged in
    /// automatically; this field supplies the dual-fund half only.
    pub dual_fund_peers: BTreeSet<String>,
    pub open_guards: BTreeMap<String, OpenGuard>,

    pub recycle_block_height: i64,
    pub recycle_protected_peers: Option<BTreeSet<String>>,
    pub recycle_route_pair_scids: BTreeSet<String>,
    /// `_close_protection_reason` result per loser scid, for the recycle
    /// nomination gate (py 2070-2079) — same evidence shape as
    /// [`LoserChannelEvidence::close_protection_reason`], keyed by the
    /// loser's scid since recycle evaluates a DIFFERENT loser subset.
    pub recycle_close_protection: BTreeMap<String, Option<String>>,
    pub recycle_candidates: Vec<RecycleCandidateOwned>,
    pub recycle_close_cost_sats: i64,
}

/// Owned equivalent of [`RecycleCandidate`] ([`CycleEvidence`] must own its
/// data; [`plan_cycle`] borrows into a `RecycleCandidate` at call time).
#[derive(Debug, Clone)]
pub struct RecycleCandidateOwned {
    pub peer_id: String,
    pub score: f64,
    pub open_ev_template: OpenEvInputs,
}

/// Owned equivalent of [`PlannerActionRecord`].
#[derive(Debug, Clone)]
pub struct StoredPlannerAction {
    pub action_type: String,
    pub status: String,
    pub created_at: i64,
}

/// Owned equivalent of [`PeerChannelCapacity`].
#[derive(Debug, Clone)]
pub struct ExposureChannel {
    pub peer_id: String,
    pub state: String,
    pub total_msat: i64,
}

/// Owned equivalent of [`super::demand_flow::PeerFlowContribution`].
#[derive(Debug, Clone)]
pub struct FlowContribution {
    pub peer_id: String,
    pub sats_in: i64,
    pub sats_out: i64,
}

/// A defibrillation the cycle WOULD execute (py's `summary["defibrillations"]`
/// entry, 447-454 — minus `action_id`/RPC `status`, which only exist after
/// real execution).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedDefibrillation {
    pub scid: String,
    pub peer_id: String,
    pub reason: String,
}

/// A close the cycle WOULD execute (py's `summary["closes"]` entry,
/// 505-511, minus post-execution fields).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedClose {
    pub scid: String,
    pub peer_id: String,
    pub reason: String,
}

/// An open the cycle WOULD execute (py's `summary["opens"]` entry, 730-736,
/// minus post-execution fields).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedOpen {
    pub peer_id: String,
    pub amount_sats: i64,
    pub ev: f64,
}

/// One evaluated-but-possibly-rejected open candidate (py
/// `summary["evaluated_open_candidates"]`, 625-631).
#[derive(Debug, Clone, PartialEq)]
pub struct EvaluatedOpenCandidate {
    pub peer_id: String,
    pub source: String,
    pub score: f64,
    pub amount_sats: i64,
    pub ev: f64,
}

/// The full per-cycle plan: everything `execute_cycle` WOULD do, with
/// reasons — never anything it did (hard rule #1).
#[derive(Debug, Clone, Default)]
pub struct CyclePlan {
    pub skipped: bool,
    pub skip_reason: Option<String>,

    pub winners: Vec<Winner>,
    pub losers: Vec<Loser>,
    pub loser_close_scids: BTreeSet<String>,

    pub defibrillations: Vec<PlannedDefibrillation>,
    pub closes: Vec<PlannedClose>,
    pub opens: Vec<PlannedOpen>,
    pub evaluated_open_candidates: Vec<EvaluatedOpenCandidate>,

    pub portfolio_state: Option<PortfolioBalanceState>,
    pub candidate_pool: Vec<Candidate>,
    pub funding_deficit_sats: i64,
    pub best_candidate_peer_id: Option<String>,

    pub recycle_opportunity: Option<RecyclePlan>,

    pub skipped_reasons: Vec<String>,
    pub winner_count: usize,
    pub loser_count: usize,
    pub candidate_count: usize,
}

fn short(id: &str) -> String {
    id.chars().take(16).collect()
}

fn borrow_flows(flows: &[FlowContribution]) -> Vec<super::demand_flow::PeerFlowContribution<'_>> {
    flows
        .iter()
        .map(|f| super::demand_flow::PeerFlowContribution {
            peer_id: &f.peer_id,
            sats_in: f.sats_in,
            sats_out: f.sats_out,
        })
        .collect()
}

/// Port of `_discover_peers` (py 2714-2755): run all five strategies,
/// normalize, dedup by peer_id (keep highest score), enrich via
/// [`super::candidate_score::score_candidate`], apply pool quotas.
pub fn discover_peers(
    winners: &[Winner],
    evidence: &DiscoveryEvidence,
    enrichment: &BTreeMap<String, CandidateEnrichmentEvidence>,
) -> Vec<Candidate> {
    let mut raw: Vec<DiscoveredCandidate> = Vec::new();

    let winner_refs: Vec<discovery::WinnerForDiscovery> = winners
        .iter()
        .map(|w| discovery::WinnerForDiscovery {
            peer_id: &w.peer_id,
            roi: w.roi,
        })
        .collect();
    raw.extend(discovery::discover_from_winners(&winner_refs));

    let existing_peers: BTreeSet<String> = evidence
        .all_channels
        .iter()
        .map(|c| c.peer_id.clone())
        .collect();

    let patron_map: BTreeMap<String, Vec<discovery::NeighborChannelEdge>> = evidence
        .neighbor_patron_source_channels
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.iter()
                    .map(|e| discovery::NeighborChannelEdge {
                        destination: &e.destination,
                        amount_msat: e.amount_msat,
                        fee_per_millionth: e.fee_per_millionth,
                    })
                    .collect(),
            )
        })
        .collect();
    // Finding 2: evidence-driven branch selection, mirroring Python's
    // `self._capital_efficiency is None` check — the caller assembling
    // `DiscoveryEvidence` knows whether a capital-efficiency analyzer was
    // injected and supplies (or omits) `neighbor_capital_efficiency`.
    match &evidence.neighbor_capital_efficiency {
        Some(patron_pool_inputs) => {
            raw.extend(discovery::discover_from_neighbors_capital_efficiency(
                patron_pool_inputs,
                &patron_map,
                &existing_peers,
                &evidence.our_node_id,
            ));
        }
        None => {
            raw.extend(discovery::discover_from_neighbors(
                &evidence.all_channels,
                &patron_map,
                &existing_peers,
                &evidence.our_node_id,
            ));
        }
    }

    raw.extend(discovery::discover_from_graph(
        &evidence.graph_cached_source_channels,
        &evidence.our_node_id,
        &existing_peers,
    ));

    let route_peer_map: BTreeMap<String, Vec<discovery::NeighborChannelEdge>> = evidence
        .route_peer_source_channels
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                v.iter()
                    .map(|e| discovery::NeighborChannelEdge {
                        destination: &e.destination,
                        amount_msat: e.amount_msat,
                        fee_per_millionth: e.fee_per_millionth,
                    })
                    .collect(),
            )
        })
        .collect();
    raw.extend(discovery::discover_from_route_pairs(
        &evidence.route_pair_rows,
        &evidence.channel_to_peer,
        &route_peer_map,
        &existing_peers,
        &evidence.our_node_id,
    ));

    let borrowed_flows = borrow_flows(&evidence.demand_flows);
    let demand_flow_result = discovery::discover_from_demand_flow(
        &borrowed_flows,
        &evidence.demand_flow_sink_channels,
        &existing_peers,
    );
    raw.extend(demand_flow_result.candidates);

    let normalized =
        normalize_candidate_scores(raw.iter().map(|c| c.to_scoring_candidate()).collect());

    // py 2738-2743: dedup by peer_id, keep highest score, first-discovery
    // order preserved (see `super::dedup`'s doc comment; Task 47 finding 4
    // -- a `BTreeMap`-keyed dedup would reorder every candidate into
    // peer-id sort order, not just colliding ones).
    let mut order: Vec<String> = Vec::new();
    let mut best: std::collections::HashMap<String, Candidate> = std::collections::HashMap::new();
    for c in normalized {
        super::dedup::upsert_best(&mut order, &mut best, c.peer_id.clone(), c, |c| c.score);
    }
    let mut merged: Vec<Candidate> = super::dedup::into_ordered_vec(order, best);

    for c in merged.iter_mut() {
        let empty = CandidateEnrichmentEvidence::default();
        let ev = enrichment.get(&c.peer_id).unwrap_or(&empty);
        c.score = super::candidate_score::score_candidate(c.score, ev);
    }

    apply_pool_quotas(&merged, evidence.max_candidate_pool)
}

/// Port of `execute_cycle` (py 363-786), evidence-in/plan-out.
pub fn plan_cycle(evidence: &CycleEvidence) -> CyclePlan {
    if !evidence.planner_enabled {
        return CyclePlan {
            skipped: true,
            skip_reason: Some("planner disabled".to_string()),
            ..Default::default()
        };
    }

    let mut plan = CyclePlan {
        skipped: false,
        ..Default::default()
    };

    if !evidence.fee_gate_ok {
        if let Some(r) = &evidence.fee_gate_reason {
            plan.skipped_reasons.push(r.clone());
        }
    }

    let winners = identify_winners(&evidence.winner_channels);
    let mut losers = identify_losers(&evidence.loser_channels);

    let redeployment_winners: Vec<RedeploymentCandidate> = evidence
        .redeployment_winner_evs
        .iter()
        .map(|(peer_id, template)| RedeploymentCandidate {
            peer_id: peer_id.as_str(),
            open_ev_template: *template,
        })
        .collect();
    apply_redeployment_ev_demotion(&mut losers, &redeployment_winners);

    plan.loser_close_scids = losers
        .iter()
        .filter(|l| l.action == LoserAction::Close)
        .map(|l| l.scid.clone())
        .collect();

    // --- 4. Defibrillations -------------------------------------------
    let mut defibrillate: Vec<&Loser> = losers
        .iter()
        .filter(|l| l.action == LoserAction::Defibrillate)
        .collect();
    defibrillate.sort_by(|a, b| {
        a.marginal_roi
            .partial_cmp(&b.marginal_roi)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut defibrillations_this_cycle: i64 = 0;
    for loser in defibrillate {
        if defibrillations_this_cycle >= evidence.defibrillation_limit {
            break;
        }
        let Some(gate) = evidence.defib_gates.get(&loser.peer_id) else {
            plan.skipped_reasons.push(format!(
                "Defibrillation evidence missing for {}...: cannot evaluate safety gates (fail-closed)",
                short(&loser.peer_id)
            ));
            continue;
        };
        if !gate_evidence_is_fresh(gate.observed_at, evidence.now) {
            plan.skipped_reasons.push(format!(
                "Defibrillation evidence stale for {}...: {} — fail-closed",
                short(&loser.peer_id),
                gate_age_denial_text(gate.observed_at, evidence.now)
            ));
            continue;
        }
        if let Some(reason) = &gate.cooldown_blocked {
            plan.skipped_reasons.push(format!(
                "Defibrillation cooldown for {}: {reason}",
                loser.scid
            ));
            continue;
        }
        if let Some(reason) = &gate.recently_attempted_blocked {
            plan.skipped_reasons.push(format!(
                "Defibrillation skipped for {}: {reason}",
                loser.scid
            ));
            continue;
        }
        if let Some(reason) = &gate.policy_blocked {
            plan.skipped_reasons.push(format!(
                "Defibrillation policy-blocked for {}: {reason}",
                loser.scid
            ));
            continue;
        }
        plan.defibrillations.push(PlannedDefibrillation {
            scid: loser.scid.clone(),
            peer_id: loser.peer_id.clone(),
            reason: loser.reason.clone(),
        });
        defibrillations_this_cycle += 1;
    }

    // --- 5. Closes -------------------------------------------------------
    let mut closeable: Vec<&Loser> = losers
        .iter()
        .filter(|l| l.action == LoserAction::Close)
        .collect();
    closeable.sort_by(|a, b| {
        a.marginal_roi
            .partial_cmp(&b.marginal_roi)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut closes_this_cycle: i64 = 0;
    for loser in closeable {
        if let Some(limit) = evidence.close_limit {
            if closes_this_cycle >= limit {
                break;
            }
        }
        let Some(gate) = evidence.close_gates.get(&loser.peer_id) else {
            plan.skipped_reasons.push(format!(
                "Close evidence missing for {}...: cannot evaluate safety gates (fail-closed)",
                short(&loser.peer_id)
            ));
            continue;
        };
        if !gate_evidence_is_fresh(gate.observed_at, evidence.now) {
            plan.skipped_reasons.push(format!(
                "Close evidence stale for {}...: {} — fail-closed",
                short(&loser.peer_id),
                gate_age_denial_text(gate.observed_at, evidence.now)
            ));
            continue;
        }
        if let Some(reason) = &gate.close_allowed_blocked {
            plan.skipped_reasons
                .push(format!("Close blocked for {}: {reason}", loser.scid));
            continue;
        }
        if evidence.close_execution_enabled {
            if let Some(reason) = &gate.safety_guard_blocked {
                plan.skipped_reasons
                    .push(format!("Close guard failed for {}: {reason}", loser.scid));
                continue;
            }
        } else if let Some(reason) = &gate.cooldown_blocked {
            plan.skipped_reasons
                .push(format!("Close cooldown for {}: {reason}", loser.scid));
            continue;
        }
        plan.closes.push(PlannedClose {
            scid: loser.scid.clone(),
            peer_id: loser.peer_id.clone(),
            reason: loser.reason.clone(),
        });
        closes_this_cycle += 1;
    }

    // --- 6. Portfolio balance gate ---------------------------------------
    let portfolio_state = check_portfolio_balance_gate(&evidence.peer_channels);
    plan.portfolio_state = Some(portfolio_state);

    // --- 7-11. Discovery, scoring, sizing, EV evaluation, opens ----------
    let mut candidate_pool: Vec<Candidate> = Vec::new();
    if evidence.fee_gate_ok {
        candidate_pool = discover_peers(
            &winners,
            &evidence.discovery,
            &evidence.candidate_enrichment,
        );
        plan.candidate_pool = candidate_pool.clone();

        if let Some(top) = candidate_pool.iter().max_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            if evidence.available_sats < evidence.min_channel_sats {
                plan.funding_deficit_sats =
                    (evidence.min_channel_sats - evidence.available_sats).max(0);
                plan.best_candidate_peer_id = Some(top.peer_id.clone());
            }
        }

        let mut score_ranked: Vec<Candidate> = candidate_pool.clone();
        score_ranked.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let sizing_slots = evidence
            .max_opens_per_cycle
            .max(1)
            .min(score_ranked.len() as i64) as usize;

        let sizing_pool_for = |peer_id: &str| -> Vec<SizingCandidate> {
            let mut pool = Vec::with_capacity(sizing_slots);
            if let Some(c) = score_ranked.iter().find(|c| c.peer_id == peer_id) {
                pool.push(SizingCandidate {
                    peer_id: &c.peer_id,
                    score: c.score,
                });
            }
            for other in &score_ranked {
                if other.peer_id == peer_id {
                    continue;
                }
                pool.push(SizingCandidate {
                    peer_id: &other.peer_id,
                    score: other.score,
                });
                if pool.len() >= sizing_slots {
                    break;
                }
            }
            pool
        };

        let sizing_cfg = SizingConfig {
            planner_min_channel_sats: evidence.min_channel_sats,
            planner_max_channel_sats: evidence.max_channel_sats,
        };

        let mut evaluated: Vec<(Candidate, i64, f64)> = Vec::new();
        for candidate in score_ranked.iter() {
            let peer_id = candidate.peer_id.clone();

            let empty_actions: Vec<StoredPlannerAction> = Vec::new();
            let actions = evidence
                .backoff_actions
                .get(&peer_id)
                .unwrap_or(&empty_actions);
            let action_records: Vec<PlannerActionRecord> = actions
                .iter()
                .map(|a| PlannerActionRecord {
                    action_type: &a.action_type,
                    status: &a.status,
                    created_at: a.created_at,
                })
                .collect();
            if let Some(reason) =
                failed_open_backoff_reason(&peer_id, &action_records, evidence.now)
            {
                plan.skipped_reasons.push(reason);
                continue;
            }

            let exposure_channels: Vec<PeerChannelCapacity> = evidence
                .exposure_channels
                .iter()
                .map(|c| PeerChannelCapacity {
                    peer_id: &c.peer_id,
                    state: &c.state,
                    total_msat: c.total_msat,
                })
                .collect();
            if let Some(reason) =
                peer_exposure_cap_reason(&peer_id, evidence.max_channel_sats, &exposure_channels)
            {
                plan.skipped_reasons.push(reason);
                continue;
            }

            let Some(oc_evidence) = evidence.open_candidate_evidence.get(&peer_id) else {
                // Fail closed: no sizing/EV evidence for this peer means it
                // cannot be evaluated — never default to opening anyway.
                plan.skipped_reasons.push(format!(
                    "No sizing/EV evidence for {}...: cannot evaluate",
                    short(&peer_id)
                ));
                continue;
            };

            let sizing_pool = sizing_pool_for(&peer_id);
            let sizing_candidate = SizingCandidate {
                peer_id: &peer_id,
                score: candidate.score,
            };
            let channel_size = size_channel(
                &sizing_candidate,
                &sizing_pool,
                evidence.available_sats,
                &sizing_cfg,
                &oc_evidence.peer_dest_channel_capacities_sats,
            );

            let mut inputs = oc_evidence.open_ev_template;
            inputs.channel_size_sats = channel_size;
            let ev = calculate_open_ev(&inputs);

            plan.evaluated_open_candidates.push(EvaluatedOpenCandidate {
                peer_id: peer_id.clone(),
                source: candidate.source.clone(),
                score: candidate.score,
                amount_sats: channel_size,
                ev,
            });

            if ev <= 0.0 {
                plan.skipped_reasons
                    .push(format!("Negative EV ({ev:.0}) for {}...", short(&peer_id)));
                continue;
            }

            evaluated.push((candidate.clone(), channel_size, ev));
        }

        evaluated.sort_by(|a, b| {
            b.2.partial_cmp(&a.2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(
                    b.0.score
                        .partial_cmp(&a.0.score)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )
        });

        let mut opens_this_cycle: i64 = 0;
        let mut remaining_budget = evidence.exploration_budget_sats;
        // Task 47 correction round 1, finding 3: the CAPITAL balance opens
        // are sized/EV'd against, tracked separately from the exploration
        // FEE budget (`remaining_budget`, unchanged). Python recomputes
        // later candidates' size/EV against the balance remaining after
        // each prior accepted open's debit (capacity_planner.py 687-693)
        // and debits the channel amount only at the successful-open commit
        // point (737-744) — never at evaluation time and never conflated
        // with the exploration-fee accounting.
        let mut running_available_sats = evidence.available_sats;
        for (candidate, precomputed_channel_size, precomputed_ev) in evaluated {
            if opens_this_cycle >= evidence.max_opens_per_cycle {
                break;
            }
            if remaining_budget < evidence.estimated_open_cost_sats {
                plan.skipped_reasons.push(format!(
                    "Exploration budget: {remaining_budget} < open cost {}",
                    evidence.estimated_open_cost_sats
                ));
                break;
            }

            // py 687-690: the first accepted open reuses the size/EV
            // computed in the earlier evaluation pass (against the
            // ORIGINAL available balance); every open after it is
            // recomputed against `running_available_sats`, which already
            // reflects every prior ACCEPTED open's debit.
            let (channel_size, ev) = if opens_this_cycle > 0 {
                let Some(oc_evidence) = evidence.open_candidate_evidence.get(&candidate.peer_id)
                else {
                    // Evaluated once already (it had evidence then — see the
                    // first pass above); fail closed defensively rather than
                    // panic if this invariant is ever violated.
                    plan.skipped_reasons.push(format!(
                        "No sizing/EV evidence for {}...: cannot re-evaluate after prior open",
                        short(&candidate.peer_id)
                    ));
                    continue;
                };
                let sizing_pool = sizing_pool_for(&candidate.peer_id);
                let sizing_candidate = SizingCandidate {
                    peer_id: &candidate.peer_id,
                    score: candidate.score,
                };
                let recomputed_size = size_channel(
                    &sizing_candidate,
                    &sizing_pool,
                    running_available_sats,
                    &sizing_cfg,
                    &oc_evidence.peer_dest_channel_capacities_sats,
                );
                let mut inputs = oc_evidence.open_ev_template;
                inputs.channel_size_sats = recomputed_size;
                let recomputed_ev = calculate_open_ev(&inputs);
                (recomputed_size, recomputed_ev)
            } else {
                (precomputed_channel_size, precomputed_ev)
            };

            if ev <= 0.0 {
                plan.skipped_reasons.push(format!(
                    "Negative EV ({ev:.0}) for {}...",
                    short(&candidate.peer_id)
                ));
                continue;
            }

            if matches!(
                portfolio_state,
                PortfolioBalanceState::Constrained | PortfolioBalanceState::Blocked
            ) {
                let is_sink_adjacent = discover_peers_sink_adjacent(evidence, &candidate.peer_id);
                let has_dual_fund = evidence.dual_fund_peers.contains(&candidate.peer_id);
                if !is_sink_adjacent && !has_dual_fund {
                    let label = if portfolio_state == PortfolioBalanceState::Constrained {
                        "Constrained"
                    } else {
                        "Blocked"
                    };
                    plan.skipped_reasons.push(format!(
                        "{label}: {}... not sink-adjacent or dual-fund",
                        short(&candidate.peer_id)
                    ));
                    continue;
                }
            }

            let Some(guard) = evidence.open_guards.get(&candidate.peer_id) else {
                plan.skipped_reasons.push(format!(
                    "Open guard evidence missing for {}...: cannot evaluate safety gates (fail-closed)",
                    short(&candidate.peer_id)
                ));
                continue;
            };
            if !gate_evidence_is_fresh(guard.observed_at, evidence.now) {
                plan.skipped_reasons.push(format!(
                    "Open guard evidence stale for {}...: {} — fail-closed",
                    short(&candidate.peer_id),
                    gate_age_denial_text(guard.observed_at, evidence.now)
                ));
                continue;
            }
            if let Some(reason) = &guard.blocked {
                plan.skipped_reasons.push(format!(
                    "Guard failed for {}...: {reason}",
                    short(&candidate.peer_id)
                ));
                continue;
            }

            plan.opens.push(PlannedOpen {
                peer_id: candidate.peer_id.clone(),
                amount_sats: channel_size,
                ev,
            });
            opens_this_cycle += 1;
            // py 737-744: debit the planned CHANNEL AMOUNT from the capital
            // balance at the commit point (finding 3) — kept separate from
            // the exploration FEE budget debited just below.
            running_available_sats = (running_available_sats - channel_size).max(0);
            remaining_budget = (remaining_budget - evidence.estimated_open_cost_sats).max(0);
        }
    }

    // --- 12. Recycle opportunity ------------------------------------------
    if !candidate_pool.is_empty() && !losers.is_empty() {
        let eligible: Vec<EligibleLoser> = losers
            .iter()
            .filter(|l| {
                let input = super::ev::RecycleEligibilityInput {
                    scid: &l.scid,
                    peer_id: &l.peer_id,
                    marginal_roi_percent: l.marginal_roi,
                    current_block_height: evidence.recycle_block_height,
                };
                let (ok, _reason) = super::ev::is_recycle_eligible(
                    &input,
                    evidence.recycle_protected_peers.as_ref(),
                    &evidence.recycle_route_pair_scids,
                );
                if !ok {
                    return false;
                }
                !matches!(
                    evidence.recycle_close_protection.get(&l.scid),
                    Some(Some(_))
                )
            })
            .map(|l| EligibleLoser {
                scid: &l.scid,
                peer_id: &l.peer_id,
                capacity_sats: l.capacity,
                marginal_profit_30d_sats: l.marginal_profit_30d_sats,
            })
            .collect();

        let recycle_candidates: Vec<RecycleCandidate> = evidence
            .recycle_candidates
            .iter()
            .map(|c| RecycleCandidate {
                peer_id: &c.peer_id,
                score: c.score,
                open_ev_template: c.open_ev_template,
            })
            .collect();

        plan.recycle_opportunity = find_best_recycle_pair(
            &eligible,
            &recycle_candidates,
            evidence.recycle_close_cost_sats,
        );
    }

    plan.winner_count = winners.len();
    plan.loser_count = losers.len();
    plan.candidate_count = candidate_pool.len();
    plan.winners = winners;
    plan.losers = losers;

    plan
}

/// Whether `peer_id` was returned by demand-flow discovery as sink-adjacent
/// (py's `peer_id in self._demand_flow_sink_adjacent`, 705). Recomputing
/// discovery here (rather than threading the set through) keeps
/// [`plan_cycle`]'s open-selection loop from needing a second mutable
/// pass over `discover_peers`' internals; demand-flow discovery is cheap
/// (pure, already-fetched evidence).
fn discover_peers_sink_adjacent(evidence: &CycleEvidence, peer_id: &str) -> bool {
    let existing_peers: BTreeSet<String> = evidence
        .discovery
        .all_channels
        .iter()
        .map(|c| c.peer_id.clone())
        .collect();
    let borrowed_flows = borrow_flows(&evidence.discovery.demand_flows);
    let result = discovery::discover_from_demand_flow(
        &borrowed_flows,
        &evidence.discovery.demand_flow_sink_channels,
        &existing_peers,
    );
    result.sink_adjacent_peer_ids.contains(peer_id)
}

#[cfg(test)]
mod freshness_overflow_tests {
    use super::*;

    /// Correction round 2: the wrap-into-fresh pair. True age of
    /// `(now = i64::MIN + 900, observed_at = i64::MAX)` is ~-2^64+899 —
    /// future evidence — but two's-complement WRAPPING subtraction yields
    /// 899, inside the accepted 0..=900 window. Checked/debug arithmetic
    /// panics instead. Either behaviour turns malformed evidence into a
    /// planned action; the predicate must simply deny.
    #[test]
    fn wrap_pair_is_denied_not_accepted_as_fresh() {
        assert!(!gate_evidence_is_fresh(i64::MAX, i64::MIN + 900));
    }

    /// Debug-panic pair straight at the predicate.
    #[test]
    fn extreme_min_observed_at_is_denied_not_panicking() {
        assert!(!gate_evidence_is_fresh(i64::MIN, 0));
        assert!(!gate_evidence_is_fresh(i64::MIN, i64::MAX));
    }

    /// The round-1 contract survives: 900s inclusive stays fresh, 901s is
    /// stale, 1s future is denied.
    #[test]
    fn inclusive_boundary_and_future_denial_preserved() {
        assert!(gate_evidence_is_fresh(
            1_000,
            1_000 + GATE_EVIDENCE_MAX_AGE_SECS
        ));
        assert!(!gate_evidence_is_fresh(
            1_000,
            1_001 + GATE_EVIDENCE_MAX_AGE_SECS
        ));
        assert!(!gate_evidence_is_fresh(1_001, 1_000));
    }
}
