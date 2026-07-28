//! Obligations watcher — `run_watcher_once` (py 1299-1491), phases 1-7.
//! Ties `reconcile`, `open`, `activate`, `finalize`, and `withdrawal`
//! together in Python's exact phase order. **This is where defect #1's
//! fix is consumed**: phase 5 only counts a swap as `finalized` when
//! [`crate::finalize::FinalizeOutcome::is_finalized`] says so.
//!
//! Deliberate simplification vs. Python: `run_watcher_once`'s
//! `threading.Lock` (py 1300, "watcher already running" skip) is a
//! concurrency guard for a single long-lived process re-entering this
//! function from two OS threads. That is wiring, not decision logic — see
//! `ENTRYPOINTS.md`. This module's [`run_watcher_once`] assumes it is
//! called non-reentrantly, exactly like every other pure-decision module
//! in this crate assumes its caller sequences calls correctly.

use std::collections::{BTreeMap, BTreeSet};

use crate::activate::{self, TagColumn};
use crate::breaker;
use crate::db_types::{SwapPatch, TERMINAL_PENDING_GHOST_STATUSES};
use crate::finalize;
use crate::open::{self, OpenExecParams};
use crate::ports::{
    CasOutcome, ChainPort, IgnorePeerPort, LnPlusApi, LnPlusDb, LogLevel, Logger, PolicyPort,
    PortResult,
};
use crate::reconcile;
use crate::types::NotificationEntry;
use crate::validation::{parse_ts, valid_pubkey};
use crate::withdrawal;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct WatcherSummary {
    pub opened: Vec<String>,
    pub activated: Vec<String>,
    /// Defect #1 fix consumed here: only ever pushed when `finalize`
    /// returned `FinalizeOutcome::Finalized`.
    pub finalized: Vec<String>,
    pub withdrawn: Vec<String>,
    /// Reserved for genuinely unexpected failures; see this module's doc
    /// comment for why Python's per-entry `try/except Exception` guards
    /// mostly have no Rust analogue (this crate's port functions return
    /// outcomes, not exceptions).
    pub errors: Vec<String>,
    pub skipped: Option<String>,
}

/// Task 61 4A: a ledger persistence failure anywhere in the pass aborts
/// with `Err` (fail closed) — the owner records the pass as failed and
/// the next pass retries from durable state.
#[allow(clippy::too_many_arguments)]
pub fn run_watcher_once(
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    policy: &dyn PolicyPort,
    ignore_peer: Option<&dyn IgnorePeerPort>,
    logger: &dyn Logger,
    open_exec: &OpenExecParams,
    pending_timeout_days: i64,
    now: i64,
) -> PortResult<WatcherSummary> {
    let mut summary = WatcherSummary::default();

    // Phase 1: fetch live state. An outage must not stall a funded
    // deadline and must NOT trip the breaker.
    let my = match api.get_my_swaps() {
        Ok(my) => my,
        Err(e) => {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: get_my_swaps unreachable: {e}"),
            );
            summary.skipped = Some("lnplus unreachable".to_string());
            // Task 61 4B: quarantined attempts reconcile from CHAIN
            // evidence even when LN+ is down (apply attempts stay
            // quarantined without LN+ evidence — my=None).
            reconcile::reconcile_unknown_attempts(None, db, chain, logger, now)?;
            // B1: outage path -- drive obligations off the local ledger
            // unconditionally (live_ids=None disables the dead-swap filter).
            phase_3b(
                &mut summary,
                &BTreeSet::new(),
                None,
                open_exec,
                db,
                api,
                chain,
                logger,
                now,
            )?;
            prune_terminal(db, logger, now)?;
            finalize::retry_pending_ratings(db, api, logger, now)?;
            return Ok(summary);
        }
    };

    // Phase 1.5 (Task 61 4B): resolve quarantined attempts from the fresh
    // authoritative evidence BEFORE divergence checks, so an adopted
    // outcome is visible to them.
    reconcile::reconcile_unknown_attempts(Some(&my), db, chain, logger, now)?;

    // Phase 2: reconcile (choke point + divergence checks; breaker does
    // not block the phases below).
    reconcile::maybe_run_backfill_once(&my, db, api, chain, logger, now)?;
    reconcile::reconcile(&my, db, api, logger, now)?;

    // Phase 3: entries LN+ shows as filled/opening.
    let mut processed_opening_ids = BTreeSet::new();
    for entry in &my.opening {
        let sid = entry.id.clone();
        if sid.is_empty() {
            continue;
        }
        let Some(row) = db.get_swap(&sid) else {
            continue;
        };
        match row.status.as_str() {
            "opened" => continue, // already funded; phase 4 owns activation
            "applied" | "opening" => {}
            other => {
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: swap {sid} is terminal locally (status {other:?}) but LN+ still lists it under 'opening' — ignoring, not resurrecting"),
                );
                continue;
            }
        }
        let peer = entry.outgoing_peer_pubkey.clone();
        if let Some(p) = &peer {
            if !valid_pubkey(p) {
                logger.log(
                    LogLevel::Error,
                    &format!("LNPLUS: swap {sid} — LN+ returned an invalid outgoing_peer_pubkey ({p:?}); refusing to write or open this pass"),
                );
                continue;
            }
        }
        let mut patch = SwapPatch::default().status("opening");
        if let Some(p) = &peer {
            if let Some(existing) = &row.outbound_peer {
                if existing != p {
                    logger.log(
                        LogLevel::Warn,
                        &format!(
                            "LNPLUS: swap {sid} — LN+-assigned outbound peer {}... differs from our apply-time inference {}...; LN+'s value is authoritative",
                            &p[..p.len().min(16)],
                            &existing[..existing.len().min(16)]
                        ),
                    );
                }
            }
            patch = patch.outbound_peer(p.clone());
        }
        match entry.deadline.as_ref().and_then(parse_ts) {
            Some(ts) => patch = patch.deadline_at(ts),
            None if row.deadline_at.is_none() => {
                let fallback = now + 48 * 3600;
                patch = patch.deadline_at(fallback);
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: swap {sid} — LN+ supplied no parseable deadline; local 48h estimate ({fallback}) in force"),
                );
            }
            None => {}
        }
        if let CasOutcome::Conflict { actual } =
            db.cas_swap(&sid, &["applied", "opening"], &patch)?
        {
            logger.log(
                LogLevel::Warn,
                &format!(
                    "LNPLUS: swap {sid} moved (now {actual:?}) before its opening patch — \
                     skipping this pass"
                ),
            );
            continue;
        }
        let row = db.get_swap(&sid).unwrap_or(row);
        let opened_now =
            open::execute_swap_open(&row, Some(entry), open_exec, db, api, chain, logger, now)?;
        processed_opening_ids.insert(sid.clone());
        if opened_now {
            summary.opened.push(sid);
        }
    }

    // Phase 3b: local opening rows not touched above this pass.
    let opening_ids: BTreeSet<String> = my
        .opening
        .iter()
        .filter(|e| !e.id.is_empty())
        .map(|e| e.id.clone())
        .collect();
    let completed_ids: BTreeSet<String> = my
        .completed
        .iter()
        .filter(|e| !e.id.is_empty())
        .map(|e| e.id.clone())
        .collect();
    let live_ids: BTreeSet<String> = opening_ids.union(&completed_ids).cloned().collect();
    phase_3b(
        &mut summary,
        &processed_opening_ids,
        Some(&live_ids),
        open_exec,
        db,
        api,
        chain,
        logger,
        now,
    )?;

    // Phase 4: swaps LN+ reports completed -> activate protection.
    let completed_by_id: BTreeMap<&str, _> = my
        .completed
        .iter()
        .filter(|e| !e.id.is_empty())
        .map(|e| (e.id.as_str(), e))
        .collect();
    for row in db.get_swaps_by_status(&["opened"]) {
        let sid = row.swap_id.clone();
        let Some(entry) = completed_by_id.get(sid.as_str()) else {
            continue;
        };
        let ends_ts = entry.ends.as_ref().and_then(parse_ts);
        activate::activate(
            &row,
            ends_ts,
            entry.incoming_peer_pubkey.as_deref(),
            db,
            policy,
            logger,
        )?;
        summary.activated.push(sid);
    }

    // Phase 5: active contracts -- mid-contract vanish watch + finalize.
    for row in db.get_swaps_by_status(&["active"]) {
        let sid = row.swap_id.clone();
        let due = row.ends_at.map(|e| now >= e).unwrap_or(false);
        if due {
            // Defect #1 fix consumed here: only push when actually finalized.
            let outcome = finalize::finalize(&row, db, api, policy, ignore_peer, chain, logger)?;
            if outcome.is_finalized() {
                summary.finalized.push(sid);
            }
            continue;
        }
        // Self-heal: contracts activated before a side's protection
        // existed get tagged now (idempotent -- stamps 0/1 on first eval).
        if row.incoming_tag_added.is_none() {
            if let Some(incoming_peer) = &row.incoming_peer {
                activate::protect_peer_no_close(
                    &sid,
                    incoming_peer,
                    TagColumn::Incoming,
                    &["active"],
                    db,
                    policy,
                    logger,
                )?;
            }
        }
        if row.tag_added.is_none() {
            if let Some(outbound_peer) = &row.outbound_peer {
                activate::protect_peer_no_close(
                    &sid,
                    outbound_peer,
                    TagColumn::Outbound,
                    &["active"],
                    db,
                    policy,
                    logger,
                )?;
            }
        }
        activate::check_mid_contract_vanish(&row, chain, logger);
    }

    // Phase 6: applications that timed out still pending.
    summary.withdrawn =
        withdrawal::handle_pending_timeouts(&my, db, api, logger, pending_timeout_days, now)?;

    // Phase 7: cheap terminal-row pruning + rating retries.
    prune_terminal(db, logger, now)?;
    finalize::retry_pending_ratings(db, api, logger, now)?;

    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
fn phase_3b(
    summary: &mut WatcherSummary,
    processed_opening_ids: &BTreeSet<String>,
    live_ids: Option<&BTreeSet<String>>,
    open_exec: &OpenExecParams,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
    now: i64,
) -> PortResult<()> {
    for row in db.get_swaps_by_status(&["opening"]) {
        let sid = row.swap_id.clone();
        if processed_opening_ids.contains(&sid) {
            continue;
        }
        if row.outbound_peer.is_none() || row.deadline_at.is_none() {
            continue;
        }
        if let Some(live) = live_ids {
            if !live.contains(&sid) {
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: swap {sid} is 'opening' locally but LN+ no longer recognizes it (missing from opening/completed) — not funding a channel for a dead swap"),
                );
                continue;
            }
        }
        if open::execute_swap_open(&row, None, open_exec, db, api, chain, logger, now)? {
            summary.opened.push(sid);
        }
    }
    Ok(())
}

fn prune_terminal(db: &dyn LnPlusDb, logger: &dyn Logger, now: i64) -> PortResult<()> {
    let pruned = db.prune_terminal(180, now)?;
    if pruned > 0 {
        logger.log(
            LogLevel::Debug,
            &format!("LNPLUS: pruned {pruned} terminal swap row(s)"),
        );
    }
    Ok(())
}

/// py `_poll_notifications` (2089-2112). Observability only: no
/// autonomous action is ever taken from notification content. Returns the
/// (at most 10) entries to keep for `get_status()`; marks read best-effort.
pub fn poll_notifications(api: &dyn LnPlusApi, logger: &dyn Logger) -> Vec<NotificationEntry> {
    let notes = match api.get_notifications() {
        Ok(notes) => notes,
        Err(e) => {
            logger.log(
                LogLevel::Debug,
                &format!("LNPLUS: notification poll failed: {e}"),
            );
            return Vec::new();
        }
    };
    if notes.is_empty() {
        return Vec::new();
    }
    for n in notes.iter().take(20) {
        logger.log(
            LogLevel::Info,
            &format!(
                "LNPLUS: notification [{}] {}",
                n.kind.as_deref().unwrap_or("?"),
                n.body
                    .as_deref()
                    .unwrap_or("")
                    .chars()
                    .take(200)
                    .collect::<String>()
            ),
        );
    }
    let keep: Vec<NotificationEntry> = notes.into_iter().take(10).collect();
    if let Err(e) = api.mark_read_notifications() {
        logger.log(
            LogLevel::Debug,
            &format!("LNPLUS: mark_read_notifications failed: {e}"),
        );
    }
    keep
}

/// py `get_status` (2114-2131), as a pure aggregation over already-fetched
/// state. `last_watcher_pass`/`recent_notifications` are in-memory-only
/// state Python keeps on the `SwapLifecycle` instance (py 859, 2106) — an
/// orchestrator-owned concern passed in here rather than tracked by this
/// crate (see `ENTRYPOINTS.md`).
#[derive(Debug, Clone, PartialEq)]
pub struct StatusSnapshot {
    pub breaker: Option<String>,
    pub inflight: Vec<crate::db_types::SwapRow>,
    pub active: Vec<crate::db_types::SwapRow>,
    pub recent_ended: Vec<crate::db_types::SwapRow>,
    pub recent_failed: Vec<crate::db_types::SwapRow>,
    pub backfill_done: bool,
}

pub fn get_status(db: &dyn LnPlusDb) -> PortResult<StatusSnapshot> {
    let mut recent_ended = db.get_swaps_by_status(&["ended"]);
    if recent_ended.len() > 10 {
        recent_ended = recent_ended.split_off(recent_ended.len() - 10);
    }
    let mut recent_failed = db.get_swaps_by_status(&TERMINAL_PENDING_GHOST_STATUSES);
    if recent_failed.len() > 5 {
        recent_failed = recent_failed.split_off(recent_failed.len() - 5);
    }
    Ok(StatusSnapshot {
        breaker: breaker::tripped_message(db)?,
        inflight: db.inflight_swaps(),
        active: db.get_swaps_by_status(&["active"]),
        recent_ended,
        recent_failed,
        backfill_done: db.get_config_override(reconcile::BACKFILL_FLAG).is_some(),
    })
}
