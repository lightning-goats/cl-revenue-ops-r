//! Task 67b slice 4: per-peer gate evidence for the capital planner.
//!
//! The frozen kernel is FAIL-CLOSED on these: a missing gate makes it skip
//! the action with *"cannot evaluate safety gates (fail-closed)"*, and a
//! stale one is denied by `gate_evidence_is_fresh`. Supplying fresh,
//! correct gates is therefore what lets the planner actually defibrillate
//! and close — the last step of functional parity on the loser side.
//!
//! Ported semantics, each with a trap worth stating:
//!
//! - **Cooldown** (py `_check_cooldown`, capacity_planner.py:2393): any
//!   planner action for the peer inside 24h blocks — EXCEPT `dry_run` and
//!   `failed`, which are not real actions. Counting them would make a
//!   dry-run cycle suppress the next real one.
//! - **Defib policy** (py `_check_defib_allowed`, :3345): a
//!   defibrillation FILLS the channel, so `rebalance_mode` of `disabled`
//!   or `source_only` forbids it. An ABSENT policy defaults to `enabled`
//!   (py's `str(mode or "enabled")`) — a peer with no policy row must not
//!   be silently undiagnosable.
//! - **Close protection does NOT block defib.** Operator policy
//!   2026-07-09: `no_close`/`protect` keep LN+ contract channels
//!   diagnosable. Blocking defib on protection would quietly strand
//!   exactly the channels most in need of diagnosis.

use std::collections::{BTreeMap, HashMap};

use revops_capital::planner::cycle::{CloseGate, DefibGate, OpenGuard};

/// py's planner-action row, reduced to what the cooldown reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerActionRecord {
    pub status: String,
    pub created_at: i64,
}

/// py `get_recent_planner_actions(peer_id, hours=24)`.
pub const COOLDOWN_WINDOW_SECONDS: i64 = 24 * 3_600;

pub struct GateSources<'a> {
    /// Planner actions keyed by peer id.
    pub recent_planner_actions: &'a HashMap<String, Vec<PlannerActionRecord>>,
    /// `rebalance_mode` per peer; absent means `enabled`.
    pub rebalance_modes: &'a HashMap<String, String>,
    /// Peers whose channels must not be closed (no_close / protect).
    pub close_protected_peers: &'a [String],
    pub now: i64,
}

#[derive(Debug, Default)]
pub struct CapitalGates {
    pub defib_gates: BTreeMap<String, DefibGate>,
    pub close_gates: BTreeMap<String, CloseGate>,
    pub open_guards: BTreeMap<String, OpenGuard>,
}

/// A `dry_run` or `failed` action never happened, so it must not start a
/// cooldown.
fn is_real_action(status: &str) -> bool {
    !matches!(status.to_ascii_lowercase().as_str(), "dry_run" | "failed")
}

fn cooldown_reason(peer_id: &str, sources: &GateSources<'_>) -> Option<String> {
    let actions = sources.recent_planner_actions.get(peer_id)?;
    let recent = actions
        .iter()
        .filter(|a| {
            is_real_action(&a.status) && sources.now - a.created_at < COOLDOWN_WINDOW_SECONDS
        })
        .count();
    (recent > 0).then(|| format!("Cooldown: {recent} action(s) for peer in last 24h"))
}

/// Build gate evidence for every peer under consideration.
pub fn build_gates(peer_ids: &[String], sources: GateSources<'_>) -> CapitalGates {
    let mut out = CapitalGates::default();
    for peer_id in peer_ids {
        let cooldown = cooldown_reason(peer_id, &sources);

        // Absent policy => "enabled" (py's `str(mode or "enabled")`).
        let mode = sources
            .rebalance_modes
            .get(peer_id)
            .map(String::as_str)
            .unwrap_or("enabled");
        let fill_forbidden = matches!(mode, "disabled" | "source_only");
        let policy_blocked = fill_forbidden
            .then(|| format!("rebalance_mode={mode} forbids filling — defib blocked"));

        out.defib_gates.insert(
            peer_id.clone(),
            DefibGate {
                observed_at: sources.now,
                cooldown_blocked: cooldown.clone(),
                // A separate signal from the cooldown; the caller supplies
                // it once per-attempt history is tracked per peer.
                recently_attempted_blocked: None,
                policy_blocked,
            },
        );

        let protected = sources.close_protected_peers.iter().any(|p| p == peer_id);
        out.close_gates.insert(
            peer_id.clone(),
            CloseGate {
                observed_at: sources.now,
                close_allowed_blocked: protected
                    .then(|| "peer is close-protected (no_close/protect)".to_string()),
                safety_guard_blocked: None,
                cooldown_blocked: cooldown.clone(),
            },
        );

        out.open_guards.insert(
            peer_id.clone(),
            OpenGuard {
                observed_at: sources.now,
                blocked: cooldown,
            },
        );
    }
    out
}
