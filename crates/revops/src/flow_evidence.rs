//! F71-R23 slice 1: the current-boot gate in front of the flow store.
//!
//! `revenue-r-analyze` reads `rust_channel_flow_states`, which the flow
//! pass upserts. Those rows are DURABLE: they outlive the process that
//! wrote them. So "the store has a row for this channel" is not the same
//! claim as "this process analysed this channel", and the gap between the
//! two is exactly one restart wide.
//!
//! Two facts have to line up before a row is evidence:
//!
//! 1. **The flow loop completed a pass in THIS boot.** Between process
//!    start and that first pass (30s -- F71-R26) the store still holds
//!    every row the previous boot left. Without this check, `analyze`
//!    would spend its first half-minute confidently serving another
//!    process's numbers.
//! 2. **This channel's row was written by THIS boot.** A completed pass
//!    does not imply full coverage: a channel the live snapshot still
//!    lists but the pass did not update keeps its old row (closed-channel
//!    reconciliation, F71-R21a, only removes channels that are GONE).
//!
//! `updated_at` cannot substitute for either check. A prior boot that ran
//! minutes ago carries a larger timestamp than a current boot that started
//! seconds ago, so freshness-by-clock prefers precisely the stale row.
//!
//! What is deliberately NOT a refusal: a completed current-boot pass that
//! produced no row for the requested channel. That is Python's own
//! `{"channel": ..., "analysis": null}` -- a real answer about a channel
//! the flow analyzer does not track. Refusing it would invent an error
//! Python never returns.

use revops_db::analytics::ChannelFlowStateRow;
use revops_db::loop_health::{current_boot_status, BootStatus, LoopHealthRow};
use revops_db::owner::ObserverHandle;

/// Flow evidence this process is entitled to serve.
#[derive(Debug, Clone, PartialEq)]
pub enum FlowEvidence {
    /// A row this boot's completed pass wrote.
    Current(ChannelFlowStateRow),
    /// The pass ran and does not track this channel. Python's own answer.
    NoSuchChannel,
}

/// Why no evidence can be served. Each variant is a DIFFERENT node state;
/// collapsing any two of them would put the caller back where this module
/// started.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowEvidenceRefusal {
    /// No observer store is configured in this process at all, so no flow
    /// pass can ever have run. Permanent for this process's lifetime --
    /// distinct from a read that failed once.
    StoreNotConfigured,
    /// The store is configured but could not be read.
    StoreUnavailable(String),
    /// The flow loop has no health row: it was never registered in this
    /// process. A wiring defect, not a loop that is merely warming up.
    LoopUnregistered,
    /// The loop exists but has produced no completed pass in this boot.
    /// The carried status says which not-ready state it is, because
    /// "started 20s ago" and "errored" call for different operator action.
    NoPassThisBoot(BootStatus),
    /// A completed pass this boot did not cover this channel; the row on
    /// hand belongs to an earlier process.
    StalePriorBootRow { scid: String, row_boot_id: String },
}

impl FlowEvidenceRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::StoreNotConfigured => "flow_evidence_store_not_configured",
            Self::StoreUnavailable(_) => "flow_evidence_store_unavailable",
            Self::LoopUnregistered => "flow_evidence_loop_unregistered",
            Self::NoPassThisBoot(_) => "flow_evidence_no_pass_this_boot",
            Self::StalePriorBootRow { .. } => "flow_evidence_stale_prior_boot",
        }
    }

    /// Operator-facing explanation. Says what was checked and why the
    /// answer is a refusal rather than a number.
    pub fn detail(&self) -> String {
        match self {
            Self::StoreNotConfigured => {
                "no observer store is configured in this process, so the flow pass has \
                 never run and no flow evidence exists"
                    .to_string()
            }
            Self::StoreUnavailable(error) => {
                format!("flow state store could not be read: {error}")
            }
            Self::LoopUnregistered => {
                "the flow-analysis loop has no health row in this process; it was never \
                 registered, so nothing has produced flow evidence"
                    .to_string()
            }
            Self::NoPassThisBoot(status) => format!(
                "the flow-analysis loop has completed no pass in this boot (status: {}); \
                 any rows in the store belong to an earlier process",
                status.as_str()
            ),
            Self::StalePriorBootRow { scid, row_boot_id } => format!(
                "channel {scid} was last written by boot {row_boot_id}, not this one; \
                 this boot's completed pass did not cover it"
            ),
        }
    }

    /// The `BootStatus` behind a [`Self::NoPassThisBoot`], for surfaces
    /// that report which not-ready state the loop is in.
    pub fn boot_status(&self) -> Option<BootStatus> {
        match self {
            Self::NoPassThisBoot(status) => Some(*status),
            _ => None,
        }
    }
}

/// Decide whether a stored row is this boot's evidence.
///
/// Pure: the two reads happen in [`current_boot_flow_evidence`]. Keeping
/// the decision separate is what lets every branch be tested without a
/// database, including the ones a live node reaches only in the seconds
/// after a restart.
pub fn classify_flow_evidence(
    loop_row: Option<&LoopHealthRow>,
    row: Option<ChannelFlowStateRow>,
    boot_id: &str,
) -> Result<FlowEvidence, FlowEvidenceRefusal> {
    let Some(loop_row) = loop_row else {
        return Err(FlowEvidenceRefusal::LoopUnregistered);
    };

    // `current_boot_status` is the same judgement `revenue-r-health`
    // applies (Task 67): a prior boot's terminal is history, an in-flight
    // generation counts only if this boot began it.
    let status = current_boot_status(loop_row, boot_id);
    if status != BootStatus::Passed {
        return Err(FlowEvidenceRefusal::NoPassThisBoot(status));
    }

    let Some(row) = row else {
        return Ok(FlowEvidence::NoSuchChannel);
    };

    if row.boot_id != boot_id {
        return Err(FlowEvidenceRefusal::StalePriorBootRow {
            scid: row.scid,
            row_boot_id: row.boot_id,
        });
    }

    Ok(FlowEvidence::Current(row))
}

/// Read both facts from the observer store, as ONE observation, and
/// classify them.
///
/// C71-14: this deliberately issues a single store command. An earlier
/// draft read loop health and the channel row with two separate awaits,
/// which meant the conjunction it claims to check -- "a completed pass
/// this boot" AND "this boot wrote this row" -- was never true at any
/// single instant. The flow pass mutates both halves, so it only had to
/// start between the two awaits to make the pair disagree; see
/// [`revops_db::analytics::flow_evidence_snapshot`] for the two concrete
/// tears. The store has no single-row read command precisely so this
/// cannot be reassembled from parts.
///
/// There is no short-circuit on loop health here either: skipping the row
/// read when the loop has not passed would save one query and reintroduce
/// two round trips.
pub async fn current_boot_flow_evidence(
    observer: Option<&ObserverHandle>,
    scid: &str,
    boot_id: &str,
) -> Result<FlowEvidence, FlowEvidenceRefusal> {
    let Some(observer) = observer else {
        return Err(FlowEvidenceRefusal::StoreNotConfigured);
    };

    let snapshot = observer
        .flow_evidence_snapshot(scid)
        .await
        .map_err(|error| FlowEvidenceRefusal::StoreUnavailable(format!("{error:#}")))?;

    classify_flow_evidence(snapshot.flow_loop.as_ref(), snapshot.row, boot_id)
}
