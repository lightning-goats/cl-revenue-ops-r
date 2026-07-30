//! Task 62 slice 3: the fail-closed evidence assembler feeding the
//! FROZEN `plan_cycle` kernel.
//!
//! Two classes of `CycleEvidence` inputs, both handled honestly:
//!
//! - **Runtime-derivable, REQUIRED**: config limits, the fee gate, the
//!   live `listpeerchannels` snapshot (balances + exposure), positive
//!   budget evidence, and the planner backoff history. Every one is
//!   `Result`-shaped: a failed read is a typed [`EvidenceRefusal`]
//!   naming the source -- never a default (the Task 8/11 audit's
//!   nullable-evidence complaint).
//! - **Analytics-owner-derived** (winners, losers, defib/close gates,
//!   discovery, enrichment, open candidates, recycle inputs): their
//!   Python sources are the profitability/flow analyzer subsystems.
//!   Tasks 67b and 67c ported those owners, so every one of these is now
//!   SUPPLIED and the gap list is empty by construction. [`EvidenceGap`]
//!   remains as the mechanism for declaring a future gap honestly --
//!   `revenue-r-planner-status` surfaces the list, so an empty plan stays
//!   attributable rather than a silent "nothing to do".

use std::collections::{BTreeMap, BTreeSet};

use revops_capital::planner::candidate_score::CandidateEnrichmentEvidence;
use revops_capital::planner::cycle::{
    CycleEvidence, DiscoveryEvidence, OpenCandidateEvidence, RecycleCandidateOwned,
    StoredPlannerAction,
};
use revops_capital::planner::portfolio_gate::ChannelBalance;
use serde_json::Value;

use crate::capital_boundaries::{check_budget_evidence, BudgetDb, BudgetRefusal};

/// One honestly-empty field and why it is empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceGap {
    pub field: &'static str,
    pub reason: &'static str,
}

/// Typed assembly refusals -- each names its failed source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceRefusal {
    PeerChannelsUnavailable(String),
    Budget(BudgetRefusal),
    BackoffUnavailable(String),
}

impl EvidenceRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::PeerChannelsUnavailable(_) => "capital_evidence_peer_channels_unavailable",
            Self::Budget(refusal) => refusal.code(),
            Self::BackoffUnavailable(_) => "capital_evidence_backoff_unavailable",
        }
    }
}

/// Everything the assembler consumes. Each fallible source arrives as a
/// `Result` produced by the caller's actual read (RPC prefetch,
/// read-only production actor) so this function stays pure and every
/// failure path is drivable in tests.
pub struct EvidenceDeps<'a> {
    pub planner_enabled: bool,
    /// `Ok(())` when the fee subsystem is healthy; `Err(reason)` feeds
    /// the kernel's fee gate verbatim (the kernel skips the cycle).
    pub fee_gate: Result<(), String>,
    /// The raw `listpeerchannels` result (REQUIRED).
    pub peer_channels_raw: Result<Value, String>,
    pub budget: &'a dyn BudgetDb,
    /// Peer-keyed planner action history for backoff (REQUIRED; from the
    /// read-only production actor).
    pub backoff_actions: Result<BTreeMap<String, Vec<StoredPlannerAction>>, String>,
    pub defibrillation_limit: i64,
    pub close_execution_enabled: bool,
    pub close_limit: Option<i64>,
    pub max_channel_sats: i64,
    pub min_channel_sats: i64,
    pub max_opens_per_cycle: i64,
    pub exploration_budget_sats: i64,
    pub estimated_open_cost_sats: i64,
    pub recycle_block_height: i64,
    pub recycle_close_cost_sats: i64,
    pub now: i64,
    /// Task 67b: winners/losers from the frozen kernels, now that the
    /// profitability and flow assemblers exist. Passing them CLOSES the
    /// two largest of Task 62's eleven analytics gaps.
    pub winner_channels: Vec<revops_capital::planner::winners::WinnerCandidateEvidence>,
    pub loser_channels: Vec<revops_capital::planner::losers::LoserChannelEvidence>,
    /// Task 67b: per-peer gate evidence. The kernel is FAIL-CLOSED on
    /// these -- supplying them is what lets defibrillate and close
    /// actually happen instead of being skipped as unevaluable.
    pub defib_gates: BTreeMap<String, revops_capital::planner::cycle::DefibGate>,
    pub close_gates: BTreeMap<String, revops_capital::planner::cycle::CloseGate>,
    pub open_guards: BTreeMap<String, revops_capital::planner::cycle::OpenGuard>,

    /// Task 67c: the open side. Built by the slice 1-4 assemblers
    /// (`discovery_evidence`, `enrichment_evidence`, `open_ev_evidence`,
    /// `recycle_evidence`) and passed in, keeping this function pure and
    /// every failure path drivable from tests.
    pub discovery: DiscoveryEvidence,
    pub candidate_enrichment: BTreeMap<String, CandidateEnrichmentEvidence>,
    pub open_candidate_evidence: BTreeMap<String, OpenCandidateEvidence>,
    pub dual_fund_peers: BTreeSet<String>,
    pub redeployment_winner_evs: Vec<(String, f64)>,
    pub recycle_candidates: Vec<RecycleCandidateOwned>,
    /// Three-way, from [`crate::recycle_evidence::recycle_protected_peers`]:
    /// `None` = source failed, everything protected; `Some(empty)` = nothing
    /// protected. The frozen kernel branches on exactly this distinction.
    pub recycle_protected_peers: Option<BTreeSet<String>>,
    pub recycle_route_pair_scids: BTreeSet<String>,
    pub recycle_close_protection: BTreeMap<String, Option<String>>,
}

/// The assembly product: kernel-ready evidence plus the honest gap list.
#[derive(Debug)]
pub struct AssembledEvidence {
    pub evidence: CycleEvidence,
    pub gaps: Vec<EvidenceGap>,
}

fn parse_msat(v: &Value) -> i64 {
    match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().map(|f| f.trunc() as i64))
            .unwrap_or(0),
        Value::String(s) => s
            .trim()
            .trim_end_matches("msat")
            .parse::<i64>()
            .unwrap_or(0),
        _ => 0,
    }
}

/// Assemble kernel evidence, fail-closed on every required source.
pub fn assemble_cycle_evidence(
    deps: EvidenceDeps<'_>,
) -> Result<AssembledEvidence, EvidenceRefusal> {
    // REQUIRED: live channel snapshot.
    let raw = deps
        .peer_channels_raw
        .map_err(EvidenceRefusal::PeerChannelsUnavailable)?;
    let channels = raw
        .get("channels")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            EvidenceRefusal::PeerChannelsUnavailable(
                "listpeerchannels reply carries no channels array".to_string(),
            )
        })?;
    let mut peer_channels = Vec::with_capacity(channels.len());
    let mut exposure_channels = Vec::with_capacity(channels.len());
    for channel in channels {
        let state = channel
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let total_msat = channel.get("total_msat").map(parse_msat).unwrap_or(0);
        peer_channels.push(ChannelBalance {
            state: state.clone(),
            to_us_msat: channel.get("to_us_msat").map(parse_msat).unwrap_or(0),
            total_msat,
        });
        exposure_channels.push(revops_capital::planner::cycle::ExposureChannel {
            peer_id: channel
                .get("peer_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            state,
            total_msat,
        });
    }

    // REQUIRED: fresh positive budget evidence.
    let budget = check_budget_evidence(deps.budget, deps.now).map_err(EvidenceRefusal::Budget)?;

    // REQUIRED: backoff history.
    let backoff_actions = deps
        .backoff_actions
        .map_err(EvidenceRefusal::BackoffUnavailable)?;

    let (fee_gate_ok, fee_gate_reason) = match deps.fee_gate {
        Ok(()) => (true, None),
        Err(reason) => (false, Some(reason)),
    };

    // Task 67c closed the last six. The planner's evidence is now
    // complete, so this list is empty BY CONSTRUCTION rather than by
    // omission -- `EvidenceGap` stays as the mechanism for declaring a
    // future gap honestly.
    let gaps: Vec<EvidenceGap> = Vec::new();

    let evidence = CycleEvidence {
        planner_enabled: deps.planner_enabled,
        fee_gate_ok,
        fee_gate_reason,
        winner_channels: deps.winner_channels,
        loser_channels: deps.loser_channels,
        redeployment_winner_evs: deps.redeployment_winner_evs,
        defibrillation_limit: deps.defibrillation_limit,
        defib_gates: deps.defib_gates,
        close_execution_enabled: deps.close_execution_enabled,
        close_limit: deps.close_limit,
        close_gates: deps.close_gates,
        peer_channels,
        discovery: deps.discovery,
        candidate_enrichment: deps.candidate_enrichment,
        now: deps.now,
        backoff_actions,
        exposure_channels,
        max_channel_sats: deps.max_channel_sats,
        min_channel_sats: deps.min_channel_sats,
        open_candidate_evidence: deps.open_candidate_evidence,
        available_sats: budget.available_sats,
        max_opens_per_cycle: deps.max_opens_per_cycle,
        exploration_budget_sats: deps.exploration_budget_sats,
        estimated_open_cost_sats: deps.estimated_open_cost_sats,
        dual_fund_peers: deps.dual_fund_peers,
        open_guards: deps.open_guards,
        recycle_block_height: deps.recycle_block_height,
        recycle_protected_peers: deps.recycle_protected_peers,
        recycle_route_pair_scids: deps.recycle_route_pair_scids,
        recycle_close_protection: deps.recycle_close_protection,
        recycle_candidates: deps.recycle_candidates,
        recycle_close_cost_sats: deps.recycle_close_cost_sats,
    };

    Ok(AssembledEvidence { evidence, gaps })
}
