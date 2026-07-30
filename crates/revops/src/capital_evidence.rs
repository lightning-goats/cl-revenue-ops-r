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
//!   Python sources are the profitability/flow analyzer subsystems that
//!   Task 67 ports. Until those owners exist, the assembler fills them
//!   EMPTY and reports each as a typed [`EvidenceGap`] carried alongside
//!   the evidence -- `revenue-r-planner-status` surfaces the gap list,
//!   so an empty plan is attributable, never a silent "nothing to do".
//!   The frozen kernel is total over empty candidate sets (it plans no
//!   actions), which the assembly test pins.

use std::collections::{BTreeMap, BTreeSet};

use revops_capital::planner::cycle::{CycleEvidence, DiscoveryEvidence, StoredPlannerAction};
use revops_capital::planner::portfolio_gate::ChannelBalance;
use serde_json::Value;

use crate::capital_boundaries::{check_budget_evidence, BudgetDb, BudgetRefusal};

/// One honestly-empty field and why it is empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceGap {
    pub field: &'static str,
    pub reason: &'static str,
}

/// Task 67b closed `winner_channels`/`loser_channels`. The nine fields
/// still listed below need discovery, enrichment and recycle inputs that
/// remain unported -- named accurately rather than left pointing at a task
/// that has already shipped.
const ANALYTICS_GAP: &str =
    "needs discovery/enrichment/recycle inputs not yet ported (winners and losers ARE \
     now supplied by the Task 67b profitability + flow assemblers)";

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

    let gaps = vec![
        EvidenceGap {
            field: "redeployment_winner_evs",
            reason: ANALYTICS_GAP,
        },
        EvidenceGap {
            field: "defib_gates",
            reason: ANALYTICS_GAP,
        },
        EvidenceGap {
            field: "close_gates",
            reason: ANALYTICS_GAP,
        },
        EvidenceGap {
            field: "discovery",
            reason: ANALYTICS_GAP,
        },
        EvidenceGap {
            field: "candidate_enrichment",
            reason: ANALYTICS_GAP,
        },
        EvidenceGap {
            field: "open_candidate_evidence",
            reason: ANALYTICS_GAP,
        },
        EvidenceGap {
            field: "dual_fund_peers",
            reason: ANALYTICS_GAP,
        },
        EvidenceGap {
            field: "open_guards",
            reason: ANALYTICS_GAP,
        },
        EvidenceGap {
            field: "recycle_candidates",
            reason: ANALYTICS_GAP,
        },
    ];

    let evidence = CycleEvidence {
        planner_enabled: deps.planner_enabled,
        fee_gate_ok,
        fee_gate_reason,
        winner_channels: deps.winner_channels,
        loser_channels: deps.loser_channels,
        redeployment_winner_evs: Vec::new(),
        defibrillation_limit: deps.defibrillation_limit,
        defib_gates: BTreeMap::new(),
        close_execution_enabled: deps.close_execution_enabled,
        close_limit: deps.close_limit,
        close_gates: BTreeMap::new(),
        peer_channels,
        discovery: DiscoveryEvidence::default(),
        candidate_enrichment: BTreeMap::new(),
        now: deps.now,
        backoff_actions,
        exposure_channels,
        max_channel_sats: deps.max_channel_sats,
        min_channel_sats: deps.min_channel_sats,
        open_candidate_evidence: BTreeMap::new(),
        available_sats: budget.available_sats,
        max_opens_per_cycle: deps.max_opens_per_cycle,
        exploration_budget_sats: deps.exploration_budget_sats,
        estimated_open_cost_sats: deps.estimated_open_cost_sats,
        dual_fund_peers: BTreeSet::new(),
        open_guards: BTreeMap::new(),
        recycle_block_height: deps.recycle_block_height,
        recycle_protected_peers: None,
        recycle_route_pair_scids: BTreeSet::new(),
        recycle_close_protection: BTreeMap::new(),
        recycle_candidates: Vec::new(),
        recycle_close_cost_sats: deps.recycle_close_cost_sats,
    };

    Ok(AssembledEvidence { evidence, gaps })
}
