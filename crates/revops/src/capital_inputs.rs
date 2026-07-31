//! Task 67b slice 5: gather every Rust-owned capital-planner input in one
//! place, so `main.rs` composes `EvidenceDeps` from a tested function
//! rather than an inline read pipeline.
//!
//! Every source is `Result`-shaped and a failure REFUSES. That matters
//! more here than usual: the frozen kernel is total over empty candidate
//! sets, so a silently-empty input does not error — it produces a
//! confident "planned nothing" that is indistinguishable from a healthy
//! quiet cycle. Refusing is the only way an unreadable source stays
//! visible.

use std::collections::{BTreeMap, HashMap};

use revops_analytics::profitability::ChannelProfitability;
use revops_capital::planner::cycle::{CloseGate, DefibGate, OpenGuard};
use revops_capital::planner::losers::LoserChannelEvidence;
use revops_capital::planner::winners::{RebalanceSuccessStats, WinnerCandidateEvidence};
use revops_db::analytics::ChannelFlowStateRow;
use revops_db::queries::{PerChannelCosts, PerChannelRevenue};

use crate::capital_candidates::{build_candidate_evidence, CandidateSources};
use crate::capital_gates::{build_gates, GateSources, PlannerActionRecord};
use crate::profitability_assembler::assemble_fleet;

/// The raw reads, each independently fallible.
pub struct CapitalReadSources {
    pub revenue_all_time: Result<HashMap<String, PerChannelRevenue>, String>,
    pub revenue_30d: Result<HashMap<String, PerChannelRevenue>, String>,
    pub costs: Result<HashMap<String, PerChannelCosts>, String>,
    pub flow_states: Result<HashMap<String, ChannelFlowStateRow>, String>,
    pub planner_actions: Result<HashMap<String, Vec<PlannerActionRecord>>, String>,
    pub rebalance_modes: Result<HashMap<String, String>, String>,
    pub close_protected_peers: Result<Vec<String>, String>,
    /// C71-25: per-channel profitability evidence that was actually looked
    /// up (routing time, diagnostics, fee posterior, opener). Replaces the
    /// bare `openers` map, whose absent entries used to become a fabricated
    /// `"local"` alongside three other invented classifier inputs. A
    /// channel missing here is skipped with a reason, never defaulted.
    pub evidence: HashMap<String, crate::profitability_evidence::ChannelEvidence>,
    pub daily_volume_sats: HashMap<String, f64>,
    pub rebalance_success: HashMap<String, RebalanceSuccessStats>,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapitalInputRefusal {
    RevenueUnavailable(String),
    CostsUnavailable(String),
    FlowStatesUnavailable(String),
    PlannerActionsUnavailable(String),
    PolicyUnavailable(String),
}

impl CapitalInputRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::RevenueUnavailable(_) => "capital_inputs_revenue_unavailable",
            Self::CostsUnavailable(_) => "capital_inputs_costs_unavailable",
            Self::FlowStatesUnavailable(_) => "capital_inputs_flow_states_unavailable",
            Self::PlannerActionsUnavailable(_) => "capital_inputs_planner_actions_unavailable",
            Self::PolicyUnavailable(_) => "capital_inputs_policy_unavailable",
        }
    }
}

/// Everything the planner needs that this port now owns.
#[derive(Debug, Default)]
pub struct CapitalInputs {
    pub winner_channels: Vec<WinnerCandidateEvidence>,
    pub loser_channels: Vec<LoserChannelEvidence>,
    pub defib_gates: BTreeMap<String, DefibGate>,
    pub close_gates: BTreeMap<String, CloseGate>,
    pub open_guards: BTreeMap<String, OpenGuard>,
    /// Per-channel profitability, kept for the surfaces that report it.
    pub profitability: HashMap<String, ChannelProfitability>,
    /// Channels that could not be evaluated, WITH reasons.
    pub skipped: Vec<(String, String)>,
}

pub fn gather_capital_inputs(
    sources: CapitalReadSources,
) -> Result<CapitalInputs, CapitalInputRefusal> {
    let revenue_all_time = sources
        .revenue_all_time
        .map_err(CapitalInputRefusal::RevenueUnavailable)?;
    let revenue_30d = sources
        .revenue_30d
        .map_err(CapitalInputRefusal::RevenueUnavailable)?;
    let costs = sources
        .costs
        .map_err(CapitalInputRefusal::CostsUnavailable)?;
    let flow_states = sources
        .flow_states
        .map_err(CapitalInputRefusal::FlowStatesUnavailable)?;
    let planner_actions = sources
        .planner_actions
        .map_err(CapitalInputRefusal::PlannerActionsUnavailable)?;
    let rebalance_modes = sources
        .rebalance_modes
        .map_err(CapitalInputRefusal::PolicyUnavailable)?;
    let close_protected_peers = sources
        .close_protected_peers
        .map_err(CapitalInputRefusal::PolicyUnavailable)?;

    let fleet = assemble_fleet(
        &revenue_all_time,
        &revenue_30d,
        &costs,
        &sources.evidence,
        sources.now,
    );
    let candidates = build_candidate_evidence(CandidateSources {
        profitability: &fleet.profitability,
        flow_states: &flow_states,
        daily_volume_sats: &sources.daily_volume_sats,
        rebalance_success: &sources.rebalance_success,
        now: sources.now,
    });

    // Gate every peer the planner might act on -- a peer with no gate is
    // skipped by the kernel as unevaluable.
    let mut peer_ids: Vec<String> = fleet
        .profitability
        .values()
        .map(|p| p.peer_id.clone())
        .collect();
    peer_ids.sort();
    peer_ids.dedup();
    let gates = build_gates(
        &peer_ids,
        GateSources {
            recent_planner_actions: &planner_actions,
            rebalance_modes: &rebalance_modes,
            close_protected_peers: &close_protected_peers,
            now: sources.now,
        },
    );

    let mut skipped = fleet.skipped;
    skipped.extend(candidates.skipped);

    Ok(CapitalInputs {
        winner_channels: candidates.winner_evidence,
        loser_channels: candidates.loser_evidence,
        defib_gates: gates.defib_gates,
        close_gates: gates.close_gates,
        open_guards: gates.open_guards,
        profitability: fleet.profitability,
        skipped,
    })
}
