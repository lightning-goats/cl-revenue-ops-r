//! `SwapEvaluator` — pre-application gate chain, spec gates 0-9
//! (`lnplus_swaps.py:241-661`). At most one application per cycle
//! (`run_cycle`/[`run_cycle`] returns after the first `applied`/
//! `recommended` swap, exactly like py 564-661 `_select_and_apply`).
//!
//! Deliberate simplification vs. Python: `SwapEvaluator.__init__` takes
//! `lifecycle` as a live collaborator and calls
//! `lifecycle.breaker_tripped()` / `.has_inflight()` / `.reconcile_ok()`
//! inline (py 294-305). Here those three preflight results are INPUTS to
//! [`run_cycle`] (already computed by the caller via `crate::breaker` /
//! `crate::reconcile` in that exact order) rather than an injected
//! `dyn LifecyclePreflight`. This keeps `evaluator.rs` decoupled from
//! `reconcile.rs`'s own port dependencies; see `ENTRYPOINTS.md` for the
//! required call order at the plugin layer.

use std::collections::BTreeSet;

use crate::config::LnPlusConfig;
use crate::db_types::SwapPatch;
use crate::error::LnPlusError;
use crate::ports::{
    AttemptIntent, AttemptKind, AttemptResolution, BeginAttemptAck, ChainPort, InsertOutcome,
    LnPlusApi, LnPlusDb, LogLevel, Logger, PlannerActionRequest, PlannerPort, PolicyPort,
    PortResult,
};
use crate::types::{Metadata, Participant, SwapListing};

pub const NEG_RATIO_MAX: f64 = 0.10;
pub const TOR_RELIABILITY: f64 = 0.8;
pub const RELIABILITY_FLOOR: f64 = 0.6;
pub const P_UNDERPERFORM: f64 = 0.3;
pub const BOLTZ_REPLACEMENT_RATE: f64 = 0.005;
pub const SCORE_FLOOR: f64 = 0.5;
const IDENTIFIERS: [&str; 5] = ["A", "B", "C", "D", "E"];

/// A rejected swap, gate + human reason (py `_reject`, 508-512, which
/// partitions a combined `"gate:reason"` string — modeled directly as two
/// fields here instead).
#[derive(Debug, Clone, PartialEq)]
pub struct Rejection {
    pub swap_id: String,
    pub gate: String,
    pub reason: String,
}

fn reject(gate_reason: &str) -> String {
    gate_reason.to_string()
}

fn split_gate_reason(gate_reason: &str) -> (String, String) {
    match gate_reason.split_once(':') {
        Some((gate, reason)) => (gate.to_string(), reason.to_string()),
        None => (gate_reason.to_string(), String::new()),
    }
}

/// py `_infer_assignment` (476-506). LN+ convention: each participant
/// opens to the next letter; last wraps to A. B12: cancelled/banned
/// participants are excluded from the identifier map entirely. B11: no
/// free identifier -> every field `None` instead of panicking.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Assignment {
    pub our_identifier: Option<String>,
    pub outbound_peer: Option<String>,
    pub incoming_peer: Option<String>,
}

pub fn infer_assignment(swap: &SwapListing) -> Assignment {
    use std::collections::BTreeMap;
    let participants: BTreeMap<&str, &Participant> = swap
        .participants
        .iter()
        .filter(|p| !(p.cancelled || p.banned))
        .filter_map(|p| p.participant_identifier.as_deref().map(|id| (id, p)))
        .collect();

    let ours = IDENTIFIERS
        .iter()
        .find(|id| !participants.contains_key(**id));
    let ours = match ours {
        Some(id) => *id,
        None => return Assignment::default(),
    };

    let count = if swap.participant_max_count > 0 {
        swap.participant_max_count as usize
    } else {
        participants.len() + 1
    };
    let letters: Vec<&str> = IDENTIFIERS.iter().take(count).copied().collect();
    let Some(idx) = letters.iter().position(|l| *l == ours) else {
        return Assignment::default();
    };
    let outbound_id = letters[(idx + 1) % letters.len()];
    let incoming_id = letters[(idx + letters.len() - 1) % letters.len()];

    Assignment {
        our_identifier: Some(ours.to_string()),
        outbound_peer: participants.get(outbound_id).and_then(|p| p.pubkey.clone()),
        incoming_peer: participants.get(incoming_id).and_then(|p| p.pubkey.clone()),
    }
}

/// Gate 3 (fill state) + gate 4 (terms). py `_filter_swap` (348-378).
pub fn filter_swap(swap: &SwapListing, cfg: &LnPlusConfig) -> Option<String> {
    if swap.status != "pending" {
        return Some(reject("fill_state:not pending"));
    }
    if swap.participant_waiting_for_count != 1 {
        return Some(reject("fill_state:not the last open slot"));
    }
    let capacity = swap.capacity_sats;
    if capacity < cfg.planner_min_channel_sats {
        return Some(reject(&format!(
            "terms:capacity {capacity} below planner min {}",
            cfg.planner_min_channel_sats
        )));
    }
    // I2(a): an upper capacity bound, absent in the original.
    if cfg.planner_max_channel_sats > 0 && capacity > cfg.planner_max_channel_sats {
        return Some(reject(&format!(
            "terms:capacity {capacity} above planner max {}",
            cfg.planner_max_channel_sats
        )));
    }
    if swap.duration_months > cfg.lnplus_max_duration_months {
        return Some(reject("terms:duration exceeds cap"));
    }
    if swap.participant_max_count > cfg.lnplus_max_participants {
        return Some(reject("terms:too many participants"));
    }
    // D-3 (2026-07-08 Revision 2): dual (2-party) swaps rejected.
    if swap.participant_max_count < cfg.lnplus_min_participants {
        return Some(reject(&format!(
            "terms:fewer than {} participants",
            cfg.lnplus_min_participants
        )));
    }
    if swap
        .platform
        .as_deref()
        .unwrap_or("any")
        .eq_ignore_ascii_case("lnd")
    {
        return Some(reject("terms:LND/BOS swap — we are CLN"));
    }
    None
}

/// Gate 5 (peer quality, one bad peer vetoes) + own-node check.
/// py `_check_participants` (380-436).
#[allow(clippy::too_many_arguments)]
pub fn check_participants(
    swap: &SwapListing,
    cfg: &LnPlusConfig,
    our_id: Option<&str>,
    db: &dyn LnPlusDb,
    policy: Option<&dyn PolicyPort>,
    planner: &dyn PlannerPort,
    logger: &dyn Logger,
) -> Option<String> {
    if swap.participants.is_empty() {
        return Some(reject("peer_quality:no visible participants"));
    }
    for p in &swap.participants {
        // B12: a cancelled/banned participant's slot is effectively free.
        if p.cancelled || p.banned {
            continue;
        }
        let pk = match &p.pubkey {
            Some(pk) if crate::validation::valid_pubkey(pk) => pk.clone(),
            _ => return Some(reject("peer_quality:invalid participant pubkey")),
        };
        if let Some(our_id) = our_id {
            if pk == our_id {
                return Some(reject("own_node:we are already in this swap"));
            }
        }
        if let Some(policy) = policy {
            match policy.is_peer_banned(&pk) {
                Ok(true) => {
                    return Some(reject(&format!(
                        "peer_quality:{} operator-banned",
                        short(&pk)
                    )));
                }
                Ok(false) => {}
                Err(e) => {
                    logger.log(
                        LogLevel::Warn,
                        &format!(
                            "LNPLUS: ban lookup failed for {} ({e}) — rejecting swap (fail closed)",
                            short(&pk)
                        ),
                    );
                    return Some(reject(&format!(
                        "peer_quality:{} ban lookup failed (fail closed)",
                        short(&pk)
                    )));
                }
            }
        }
        if p.address_1.is_none() && p.address_2.is_none() {
            return Some(reject(&format!(
                "peer_quality:{} publishes no address",
                short(&pk)
            )));
        }
        let pos = p.positive_ratings_count;
        let neg = p.negative_ratings_count;
        if pos < cfg.lnplus_min_peer_positive_ratings {
            return Some(reject(&format!(
                "peer_quality:{} has {pos} positive ratings (< floor)",
                short(&pk)
            )));
        }
        if pos + neg > 0 && (neg as f64) / ((pos + neg) as f64) > NEG_RATIO_MAX {
            return Some(reject(&format!(
                "peer_quality:{} negative ratio too high",
                short(&pk)
            )));
        }
        // D-2 (Revision 2): gold-or-better rank floor, fail-closed on
        // missing/zero rank.
        if p.lnplus_rank_number < cfg.lnplus_min_peer_rank {
            return Some(reject(&format!(
                "peer_quality:{} rank {} below floor {}",
                short(&pk),
                p.lnplus_rank_number,
                cfg.lnplus_min_peer_rank
            )));
        }
        if let Some(local) = db.get_peer(&pk) {
            if local.defections > 0 {
                return Some(reject(&format!(
                    "peer_quality:{} defected on us before",
                    short(&pk)
                )));
            }
        }
        let enriched = planner.score_candidate(&pk, 1.0);
        if enriched < SCORE_FLOOR {
            return Some(reject(&format!(
                "peer_quality:{} planner score {enriched:.2} below floor",
                short(&pk)
            )));
        }
    }
    None
}

fn short(pk: &str) -> String {
    pk.chars().take(16).collect()
}

/// B11: a full identifier set leaves `infer_assignment` with no free slot.
/// py `_check_assignment_available` (438-445).
pub fn check_assignment_available(swap: &SwapListing) -> Option<String> {
    if infer_assignment(swap).our_identifier.is_none() {
        return Some(reject("terms:no free participant slot"));
    }
    None
}

/// I5(a): reject a swap whose inferred outbound peer we already have a
/// channel to. py `_check_existing_channel` (447-474). Only the
/// pass-frozen peer set is consulted here (`frozen: None` == the capture
/// itself failed, matching Python's fail-open live-RPC fallback by simply
/// not rejecting — the real state is re-checked at open time regardless).
pub fn check_existing_channel(
    swap: &SwapListing,
    frozen: Option<&BTreeSet<String>>,
) -> Option<String> {
    let outbound_peer = infer_assignment(swap).outbound_peer?;
    let frozen = frozen?;
    if frozen.contains(&outbound_peer) {
        Some(reject("terms:existing channel to assigned outbound peer"))
    } else {
        None
    }
}

fn tor_only(p: &Participant) -> bool {
    let addrs: Vec<&String> = [&p.address_1, &p.address_2].into_iter().flatten().collect();
    !addrs.is_empty() && addrs.iter().all(|a| a.contains(".onion"))
}

/// EV of applying to `swap`. py `_swap_ev` (514-557). I4: `outbound_ev`
/// already nets out open+close on-chain cost inside `calculate_open_ev` —
/// this function must NOT subtract `open_cost` again.
pub fn swap_ev(
    swap: &SwapListing,
    cfg: &LnPlusConfig,
    best_regular_ev: f64,
    planner: &dyn PlannerPort,
) -> (f64, Assignment) {
    let assignment = infer_assignment(swap);
    let capacity = swap.capacity_sats;
    let duration = swap.duration_months;
    let outbound_ev = planner.calculate_open_ev(assignment.outbound_peer.as_deref(), capacity);
    let inbound_corridor = planner.calculate_open_ev(assignment.incoming_peer.as_deref(), capacity);
    let replacement = capacity as f64 * BOLTZ_REPLACEMENT_RATE;
    let inbound_credit = inbound_corridor.min(replacement);

    let counterparties: Vec<&Participant> = swap
        .participants
        .iter()
        .filter(|p| !(p.cancelled || p.banned))
        .collect();
    let min_pos = counterparties
        .iter()
        .map(|p| p.positive_ratings_count)
        .min()
        .unwrap_or(0);
    let mut reliability = (RELIABILITY_FLOOR + 0.4 * (min_pos as f64 / 50.0).min(1.0)).min(1.0);
    if !counterparties.is_empty() && counterparties.iter().all(|p| tor_only(p)) {
        reliability *= TOR_RELIABILITY;
    }

    let lockup_haircut = P_UNDERPERFORM * best_regular_ev.max(0.0) * (duration as f64 / 12.0);
    let value = outbound_ev + inbound_credit * reliability * cfg.lnplus_inbound_credit_factor
        - lockup_haircut;
    (value, assignment)
}

/// Result of one evaluator pass. Mirrors Python's `summary` dict (py
/// 288-290) plus `rejections` as typed [`Rejection`]s.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CycleOutcome {
    pub applied: bool,
    pub recommended: bool,
    pub swap_id: Option<String>,
    pub swap_ev: f64,
    pub best_regular_ev: f64,
    pub rejections: Vec<Rejection>,
}

impl CycleOutcome {
    fn reject(&mut self, swap_id: &str, gate_reason: &str) {
        let (gate, reason) = split_gate_reason(gate_reason);
        self.rejections.push(Rejection {
            swap_id: swap_id.to_string(),
            gate,
            reason,
        });
    }
}

/// Preflight results the caller must compute (in this order) before
/// calling [`run_cycle`] — py 292-305.
#[derive(Debug, Clone)]
pub struct CyclePreflight {
    pub breaker_tripped: Option<String>,
    pub has_inflight: bool,
    pub reconcile_ok: bool,
}

/// Everything [`run_cycle`] needs that Python fetches inline
/// (`self.rpc.feerates`, `self._client.get_applicable_swaps`,
/// `self.rpc.listfunds`, `self._planner._capex_engine`) — assembled by
/// the caller so the gate chain itself stays a single, testable function.
pub struct CycleInputs {
    pub cfg: LnPlusConfig,
    pub preflight: CyclePreflight,
    pub opening_feerate_perkw: Option<i64>,
    pub swaps: Vec<SwapListing>,
    pub our_id: Option<String>,
    pub frozen_peers_with_channels: Option<BTreeSet<String>>,
    pub best_regular_ev: f64,
    pub confirmed_unreserved_sats: i64,
    pub capex_budget_sats: Option<i64>,
    pub now: i64,
}

/// py `run_cycle` (287-332) + `_select_and_apply` (564-661). One pass:
/// gate every candidate, pick at most one, apply or recommend it.
///
/// Task 61 4A: a ledger persistence failure anywhere in the pass aborts
/// with `Err` (fail closed) — in particular, an unacknowledged intent
/// write can never be followed by the live `create_application` call.
#[allow(clippy::too_many_arguments)]
pub fn run_cycle(
    inputs: CycleInputs,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    policy: Option<&dyn PolicyPort>,
    planner: &dyn PlannerPort,
    logger: &dyn Logger,
) -> PortResult<CycleOutcome> {
    let mut summary = CycleOutcome {
        best_regular_ev: inputs.best_regular_ev,
        ..Default::default()
    };

    if !inputs.cfg.lnplus_swaps_enabled {
        return Ok(summary);
    }
    if let Some(reason) = &inputs.preflight.breaker_tripped {
        logger.log(
            LogLevel::Warn,
            &format!("LNPLUS: breaker tripped ({reason}) — no applications"),
        );
        return Ok(summary);
    }
    if inputs.preflight.has_inflight {
        logger.log(
            LogLevel::Debug,
            "LNPLUS: swap in flight — serialization gate holds",
        );
        return Ok(summary);
    }
    match inputs.opening_feerate_perkw {
        None => {
            logger.log(
                LogLevel::Warn,
                "LNPLUS: feerates unavailable — skipping cycle",
            );
            return Ok(summary);
        }
        Some(perkw) if perkw > inputs.cfg.lnplus_apply_feerate_ceiling => {
            logger.log(
                LogLevel::Info,
                &format!(
                    "LNPLUS: opening feerate {perkw} perkw above ceiling {} — no applications",
                    inputs.cfg.lnplus_apply_feerate_ceiling
                ),
            );
            return Ok(summary);
        }
        _ => {}
    }
    if !inputs.preflight.reconcile_ok {
        logger.log(
            LogLevel::Warn,
            "LNPLUS: reconciliation preflight failed — no applications",
        );
        return Ok(summary);
    }

    let mut qualifying = Vec::new();
    for swap in &inputs.swaps {
        let reason = filter_swap(swap, &inputs.cfg)
            .or_else(|| {
                check_participants(
                    swap,
                    &inputs.cfg,
                    inputs.our_id.as_deref(),
                    db,
                    policy,
                    planner,
                    logger,
                )
            })
            .or_else(|| check_assignment_available(swap))
            .or_else(|| check_existing_channel(swap, inputs.frozen_peers_with_channels.as_ref()));
        match reason {
            Some(r) => summary.reject(&swap.id, &r),
            None => qualifying.push(swap.clone()),
        }
    }

    select_and_apply(
        qualifying,
        &inputs.cfg,
        inputs.best_regular_ev,
        inputs.confirmed_unreserved_sats,
        inputs.capex_budget_sats,
        inputs.now,
        db,
        api,
        planner,
        logger,
        &mut summary,
    )?;
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn select_and_apply(
    qualifying: Vec<SwapListing>,
    cfg: &LnPlusConfig,
    best_regular_ev: f64,
    confirmed_unreserved_sats: i64,
    capex_budget_sats: Option<i64>,
    now: i64,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    planner: &dyn PlannerPort,
    logger: &dyn Logger,
    summary: &mut CycleOutcome,
) -> PortResult<()> {
    let mut scored: Vec<(f64, SwapListing, Assignment)> = Vec::new();
    for swap in qualifying {
        let (value, assignment) = swap_ev(&swap, cfg, best_regular_ev, planner);
        if value <= 0.0 {
            summary.reject(&swap.id, &format!("economics:non-positive EV ({value:.0})"));
            continue;
        }
        scored.push((value, swap, assignment));
    }
    if scored.is_empty() {
        return Ok(());
    }
    // D-3: among equal-EV qualifiers, prefer fewer participants.
    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.participant_max_count.cmp(&b.1.participant_max_count))
    });
    let (value, swap, assignment) = scored.into_iter().next().unwrap();
    summary.swap_ev = value;
    summary.swap_id = Some(swap.id.clone());

    let margin = cfg.lnplus_swap_preference_margin;
    if best_regular_ev > value * (1.0 + margin) {
        summary.reject(
            &swap.id,
            &format!(
                "preference_margin:regular open EV {best_regular_ev:.0} beats swap EV {value:.0} beyond {:.0}% margin",
                margin * 100.0
            ),
        );
        summary.swap_id = None;
        return Ok(());
    }

    let open_cost = planner.estimate_open_cost();
    let capacity = swap.capacity_sats;
    // Gate 7: confirmed unreserved on-chain funds must cover capacity +
    // fee buffer + the planner's own wallet reserve floor (I2(b)).
    let required = capacity + open_cost + cfg.min_wallet_reserve;
    if confirmed_unreserved_sats < required {
        summary.reject(
            &swap.id,
            &format!("economics:confirmed funds {confirmed_unreserved_sats} below capacity+fees+reserve {required}"),
        );
        summary.swap_id = None;
        return Ok(());
    }
    if let Some(budget) = capex_budget_sats {
        if budget < open_cost {
            summary.reject(
                &swap.id,
                &format!("economics:capex budget {budget} below open cost {open_cost}"),
            );
            summary.swap_id = None;
            return Ok(());
        }
    }

    let reason = format!(
        "LN+ swap {}: EV {value:.0} vs regular {best_regular_ev:.0}, {capacity} sats for {}mo",
        swap.id, swap.duration_months
    );
    let mut metadata = Metadata::new();
    metadata.insert("swap_id".to_string(), swap.id.clone());
    if let Some(incoming) = &assignment.incoming_peer {
        metadata.insert("incoming_peer".to_string(), incoming.clone());
    }
    let action_id = db.record_planner_action(&PlannerActionRequest {
        action_type: "swap_apply",
        peer_id: assignment
            .outbound_peer
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        amount_sats: Some(capacity),
        estimated_cost_sats: Some(open_cost),
        reason: reason.clone(),
        metadata: Some(metadata),
    })?;

    if !cfg.lnplus_execute_applications || cfg.planner_dry_run {
        db.update_planner_action(action_id, "recommended")?;
        logger.log(
            LogLevel::Info,
            &format!(
                "LNPLUS: [RECOMMEND] would apply to swap {} — {reason}",
                swap.id
            ),
        );
        summary.recommended = true;
        return Ok(());
    }

    // Intent-first: ledger row before the external call, as a typed
    // insert whose acknowledgement gates the live call (Task 61 4A). An
    // AlreadyExists here is an invariant breach — a row for a swap we
    // are about to apply to — and must veto the application, not be
    // clobbered over.
    let mut row = crate::db_types::SwapRow::new(
        swap.id.clone(),
        "applied",
        capacity,
        swap.duration_months,
        now,
    )
    .with_planner_action_id(action_id);
    if let Some(p) = &assignment.outbound_peer {
        row = row.with_outbound_peer(p.clone());
    }
    if let Some(p) = &assignment.incoming_peer {
        row = row.with_incoming_peer(p.clone());
    }
    if let Some(id) = &assignment.our_identifier {
        row = row.with_our_identifier(id.clone());
    }
    match db.insert_swap_new(&row)? {
        InsertOutcome::Inserted => {}
        InsertOutcome::AlreadyExists => {
            db.update_planner_action(action_id, "failed")?;
            logger.log(
                LogLevel::Error,
                &format!(
                    "LNPLUS: a ledger row for swap {} already exists — refusing to apply over it",
                    swap.id
                ),
            );
            summary.reject(&swap.id, "ledger:row already exists — not applying");
            summary.swap_id = None;
            return Ok(());
        }
    }

    // Task 61 4B: durable attempt identity BEFORE the irreversible LN+
    // commitment. Blocked = a prior apply attempt is quarantined — no
    // auto-resubmit.
    let attempt_id = format!("apply:{}:{now}", swap.id);
    match db.begin_attempt(&AttemptIntent {
        attempt_id: attempt_id.clone(),
        swap_id: swap.id.clone(),
        kind: AttemptKind::Apply,
        reservation_id: None,
        peer_id: assignment.outbound_peer.clone(),
        amount_sats: Some(capacity),
        created_at: now,
    })? {
        BeginAttemptAck::Started => {}
        BeginAttemptAck::Blocked {
            existing_attempt_id,
            state,
        } => {
            db.update_planner_action(action_id, "failed")?;
            logger.log(
                LogLevel::Error,
                &format!(
                    "LNPLUS: swap {} has apply attempt {existing_attempt_id} in state {state:?} \
                     — NOT applying again (no auto-resubmit)",
                    swap.id
                ),
            );
            summary.reject(&swap.id, "ledger:apply attempt quarantined — not applying");
            summary.swap_id = None;
            return Ok(());
        }
    }

    if let Err(e) = api.create_application(&swap.id) {
        if e.is_outcome_unknown() {
            // The application MAY be live on LN+ — quarantine; the row
            // stays 'applied' (never falsified to 'failed'), and
            // reconciliation resolves from get_my_swaps evidence.
            db.resolve_attempt(
                &attempt_id,
                &AttemptResolution::OutcomeUnknown {
                    detail: format!("create_application outcome unknown: {e}"),
                },
                now,
            )?;
            db.cas_swap(
                &swap.id,
                &["applied"],
                &SwapPatch::default().outcome(format!("apply outcome unknown — quarantined ({e})")),
            )?;
            db.update_planner_action(action_id, "outcome_unknown")?;
            logger.log(
                LogLevel::Error,
                &format!(
                    "LNPLUS: application to {} has UNKNOWN outcome ({e}) — quarantined; \
                     reconciliation will resolve from LN+ evidence",
                    swap.id
                ),
            );
            return Ok(());
        }
        db.resolve_attempt(
            &attempt_id,
            &AttemptResolution::NotSubmitted {
                detail: format!("apply clean failure: {e}"),
            },
            now,
        )?;
        db.cas_swap(
            &swap.id,
            &["applied"],
            &SwapPatch::default()
                .status("failed")
                .outcome(format!("apply failed: {e}")),
        )?;
        db.update_planner_action(action_id, "failed")?;
        logger.log(
            LogLevel::Warn,
            &format!("LNPLUS: application to {} failed: {e}", swap.id),
        );
        return Ok(());
    }
    db.resolve_attempt(&attempt_id, &AttemptResolution::CommittedApply, now)?;
    db.update_planner_action(action_id, "completed")?;
    logger.log(
        LogLevel::Info,
        &format!("LNPLUS: applied to swap {} — {reason}", swap.id),
    );
    summary.applied = true;
    Ok(())
}

/// PR 3d: freeze one peer-channel observation at pass entry. py
/// `_capture_peers_with_channels` (272-285). `None` on failure -> per-swap
/// live fallback (not modeled; see [`check_existing_channel`]'s doc
/// comment).
pub fn capture_peers_with_channels(chain: &dyn ChainPort) -> Option<BTreeSet<String>> {
    chain
        .list_peer_channels(None)
        .ok()
        .map(|channels| channels.into_iter().map(|c| c.peer_id).collect())
}

/// PR 3d: our node id is a process constant, cached on first success by
/// the caller. py `_our_id` (259-270).
pub fn fetch_our_id(chain: &dyn ChainPort) -> Option<String> {
    chain.our_node_id().ok()
}

/// Ban-lookup / RPC failures surface as [`LnPlusError`]-free strings in
/// several places; re-exported so downstream modules format consistently.
pub fn describe(e: &LnPlusError) -> String {
    e.to_string()
}
