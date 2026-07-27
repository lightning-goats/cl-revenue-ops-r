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

/// Per-peer gate evidence for the defibrillation-selection loop (py's
/// `_check_cooldown` / `_defib_recently_attempted` / `_check_defib_allowed`,
/// each hoisted as `Some(reason)` when the gate BLOCKS, `None` when it
/// allows — matching this crate's fail-closed contract, missing evidence
/// for a peer with no entry is treated as "no reason on file", i.e.
/// allowed, since Python's default path is "check passes unless a reason
/// is returned").
#[derive(Debug, Clone, Default)]
pub struct DefibGate {
    pub cooldown_blocked: Option<String>,
    pub recently_attempted_blocked: Option<String>,
    pub policy_blocked: Option<String>,
}

/// Per-peer gate evidence for the close-selection loop (py's
/// `_check_close_allowed` / `_check_safety_guards` / `_check_cooldown`).
#[derive(Debug, Clone, Default)]
pub struct CloseGate {
    pub close_allowed_blocked: Option<String>,
    pub safety_guard_blocked: Option<String>,
    pub cooldown_blocked: Option<String>,
}

/// Per-peer gate evidence for the open-execution loop (py's
/// `_check_safety_guards(cfg, "open", ...)`).
#[derive(Debug, Clone, Default)]
pub struct OpenGuard {
    pub blocked: Option<String>,
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
    pub neighbor_patron_source_channels: BTreeMap<String, Vec<NeighborEdge>>,
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
    /// Winners' open-EV, precomputed per winner (py's inline
    /// `self._calculate_open_ev(winner["peer_id"], loser_capacity, cfg)`
    /// call inside `_calculate_redeployment_ev` — hoisted the same way
    /// [`super::ev::RedeploymentCandidate`] already documents).
    pub redeployment_winner_evs: Vec<(String, f64)>,

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
    raw.extend(discovery::discover_from_neighbors(
        &evidence.all_channels,
        &patron_map,
        &existing_peers,
        &evidence.our_node_id,
    ));

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

    // py 2738-2743: dedup by peer_id, keep highest score.
    let mut best: BTreeMap<String, Candidate> = BTreeMap::new();
    for c in normalized {
        match best.get(&c.peer_id) {
            Some(existing) if existing.score >= c.score => {}
            _ => {
                best.insert(c.peer_id.clone(), c);
            }
        }
    }
    let mut merged: Vec<Candidate> = best.into_values().collect();

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
        .map(|(peer_id, ev)| RedeploymentCandidate {
            peer_id: peer_id.as_str(),
            open_ev: *ev,
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
        let gate = evidence
            .defib_gates
            .get(&loser.peer_id)
            .cloned()
            .unwrap_or_default();
        if let Some(reason) = gate.cooldown_blocked {
            plan.skipped_reasons.push(format!(
                "Defibrillation cooldown for {}: {reason}",
                loser.scid
            ));
            continue;
        }
        if let Some(reason) = gate.recently_attempted_blocked {
            plan.skipped_reasons.push(format!(
                "Defibrillation skipped for {}: {reason}",
                loser.scid
            ));
            continue;
        }
        if let Some(reason) = gate.policy_blocked {
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
        let gate = evidence
            .close_gates
            .get(&loser.peer_id)
            .cloned()
            .unwrap_or_default();
        if let Some(reason) = gate.close_allowed_blocked {
            plan.skipped_reasons
                .push(format!("Close blocked for {}: {reason}", loser.scid));
            continue;
        }
        if evidence.close_execution_enabled {
            if let Some(reason) = gate.safety_guard_blocked {
                plan.skipped_reasons
                    .push(format!("Close guard failed for {}: {reason}", loser.scid));
                continue;
            }
        } else if let Some(reason) = gate.cooldown_blocked {
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
        for (candidate, channel_size, ev) in evaluated {
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

            let guard = evidence.open_guards.get(&candidate.peer_id);
            if let Some(OpenGuard {
                blocked: Some(reason),
            }) = guard
            {
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
