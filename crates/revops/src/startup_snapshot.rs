//! Task 67 slice 4: the one-shot startup-snapshot owner.
//!
//! Ports py `_snapshot_peers_once` (cl-revenue-ops.py:422-457), driven by
//! `snapshot_peers_delayed` (:3462-3489): after a startup delay, record a
//! connection event for every CONNECTED peer that has no connection
//! history inside the last hour, then exit. One-shot by design.
//!
//! Two deliberate divergences from Python, both disclosed:
//!
//! 1. **The heartbeat-before-work bug is not ported.** Python records this
//!    loop's heartbeat BEFORE doing the work and never marks it failed
//!    (:3481), so a crashed snapshot still reports alive. Here the planner
//!    is a pure function that cannot report a loop pass at all -- the
//!    caller reports only from a plan that already succeeded, and a
//!    refusal fails the pass. A source scan pins that the planner names no
//!    pass-reporting function.
//! 2. **Every required source is `Result`-shaped.** An unreadable peer
//!    list is a typed refusal, never an empty list. An empty-but-READABLE
//!    peer list is a legitimate zero-work success. Collapsing those two
//!    into "recorded nothing" is precisely the nullable-evidence failure
//!    the Task 8/11 audit flagged.

use std::collections::BTreeSet;

use serde_json::Value;

/// py `has_recent_connection_history(peer_id, 3600)`.
pub const RECENT_HISTORY_WINDOW_SECONDS: i64 = 3_600;

/// py `record_connection_event(peer_id, "snapshot")`.
pub const SNAPSHOT_EVENT_TYPE: &str = "snapshot";

/// Everything the one-shot pass consumes. Each fallible source arrives as
/// a `Result` produced by the caller's real read.
pub struct SnapshotDeps<'a> {
    /// The live `listpeers` reply (REQUIRED).
    pub peers_raw: Result<Value, String>,
    /// Peers that already have a connection event inside
    /// [`RECENT_HISTORY_WINDOW_SECONDS`] (REQUIRED).
    pub peers_with_recent_history: Result<&'a BTreeSet<String>, String>,
    pub now: i64,
}

/// Typed refusals; neither source defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotRefusal {
    PeersUnavailable(String),
    HistoryUnavailable(String),
}

impl SnapshotRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::PeersUnavailable(_) => "startup_snapshot_peers_unavailable",
            Self::HistoryUnavailable(_) => "startup_snapshot_history_unavailable",
        }
    }
}

/// What the caller should persist, plus what was skipped and why.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SnapshotPlan {
    pub record_peer_ids: Vec<String>,
    pub skipped_disconnected: usize,
    pub skipped_recent: usize,
    pub recorded_at: i64,
}

/// Plan the one-shot snapshot. Pure: it decides, the caller persists.
pub fn plan_startup_snapshot(deps: SnapshotDeps<'_>) -> Result<SnapshotPlan, SnapshotRefusal> {
    let raw = deps.peers_raw.map_err(SnapshotRefusal::PeersUnavailable)?;
    let peers = raw.get("peers").and_then(Value::as_array).ok_or_else(|| {
        SnapshotRefusal::PeersUnavailable("listpeers reply carries no peers array".to_string())
    })?;
    let recent = deps
        .peers_with_recent_history
        .map_err(SnapshotRefusal::HistoryUnavailable)?;

    let mut plan = SnapshotPlan {
        recorded_at: deps.now,
        ..Default::default()
    };
    for peer in peers {
        let Some(id) = peer.get("id").and_then(Value::as_str) else {
            continue;
        };
        if !peer
            .get("connected")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            plan.skipped_disconnected += 1;
            continue;
        }
        if recent.contains(id) {
            plan.skipped_recent += 1;
            continue;
        }
        plan.record_peer_ids.push(id.to_string());
    }
    Ok(plan)
}
