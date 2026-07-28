//! Live `get_my_swaps` vs. local in-flight rows. Ports `_reconcile`
//! (py 900-1027) plus the backfill choke point (py 901-924) and
//! `reconcile_ok` (887-898). Divergence trips the [`crate::breaker`];
//! before evaluating any NEW divergence this also attempts the defect #5
//! auto-clear (see `breaker.rs`'s module doc).

use std::collections::BTreeSet;

use crate::backfill;
use crate::breaker::{self, BreakerCause};
use crate::db_types::{SwapPatch, TERMINAL_PENDING_GHOST_STATUSES};
use crate::error::LnPlusError;
use crate::ports::{
    AttemptKind, AttemptResolution, ChainPort, LnPlusApi, LnPlusDb, LogLevel, Logger, PortResult,
};
use crate::types::MySwaps;

/// py 676 `_RECONCILE_GRACE_SECONDS`: local 'applied' rows younger than
/// this are exempt from divergence checks (B9).
pub const RECONCILE_GRACE_SECONDS: i64 = 600;

/// py 665 `_BACKFILL_FLAG` config_overrides key.
pub const BACKFILL_FLAG: &str = "_lnplus_backfill_done";

/// py 887-898 `reconcile_ok` — the evaluator's preflight gate. Fetches
/// live state itself and returns `false` (without tripping) on an LN+
/// outage; otherwise delegates to [`reconcile`] (after the backfill choke
/// point).
pub fn reconcile_ok(
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
    now: i64,
) -> PortResult<bool> {
    let my = match api.get_my_swaps() {
        Ok(my) => my,
        Err(e) => {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: reconcile fetch failed: {e}"),
            );
            return Ok(false);
        }
    };
    if !maybe_run_backfill_once(&my, db, api, chain, logger, now)? {
        return Ok(false);
    }
    reconcile(&my, db, api, logger, now)
}

/// py 914-924 choke point. Runs backfill exactly once (flag-gated).
/// C-7's concurrency guard (a dedicated lock so two threads calling this
/// concurrently cannot double-run backfill) is a wiring-layer concern for
/// whatever concrete `LnPlusDb` this is called against — see
/// `ENTRYPOINTS.md`. Returns `false` iff backfill was attempted and
/// failed this pass (caller must retry next pass, matching py's `return
/// False`).
pub fn maybe_run_backfill_once(
    my: &MySwaps,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
    now: i64,
) -> PortResult<bool> {
    if db.get_config_override(BACKFILL_FLAG).is_some() {
        return Ok(true);
    }
    // Task 61 4A: a backfill persistence failure propagates (fail closed)
    // — and, critically, the done-flag below is then NEVER written, so
    // the choke point retries the backfill next pass instead of latching
    // "done" over a half-imported ledger.
    backfill::backfill_from_lnplus(my, db, api, chain, logger, now)?;
    db.set_config_override(BACKFILL_FLAG, &now.to_string())?;
    Ok(true)
}

/// py `_reconcile` (900-1027), excluding the choke point (hoisted to
/// [`maybe_run_backfill_once`], called separately by [`reconcile_ok`] and
/// the watcher — py 1339-1342 calls `_reconcile` directly, relying on
/// `reconcile_ok`'s earlier choke-point run in the SAME pass; this port
/// makes that ordering an explicit two-call contract instead of an
/// implicit one, see `ENTRYPOINTS.md`).
pub fn reconcile(
    my: &MySwaps,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    logger: &dyn Logger,
    now: i64,
) -> PortResult<bool> {
    let pending_ids = my.pending_ids();
    let opening_ids = my.opening_ids();
    let completed_ids = my.completed_ids();
    let local_inflight = db.inflight_swaps();
    let local_ids: BTreeSet<String> = local_inflight.iter().map(|r| r.swap_id.clone()).collect();

    // Defect #5 fix: re-verify a currently-tripped REVERIFIABLE cause
    // before evaluating any new divergence below, so a resolved ghost
    // trip cannot mask a genuinely new one forever. Task 61 4A: a
    // breaker read failure propagates — fail closed, never "clear".
    if let Some(state) = db.get_breaker()? {
        let still_reproducible = match &state.cause {
            BreakerCause::OpeningGhostNoLocalRecord { swap_id } => {
                opening_ids.contains(swap_id) && !local_ids.contains(swap_id)
            }
            BreakerCause::PendingGhostNoLocalRecord { swap_id } => {
                pending_ids.contains(swap_id) && !local_ids.contains(swap_id)
            }
            // Never-auto-clear causes: `still_reproducible = true` is a
            // no-op here (`auto_clear_if_resolved`/`maybe_auto_clear` are
            // already gated on `is_reverifiable()`), kept explicit for
            // readability.
            _ => true,
        };
        breaker::auto_clear_if_resolved(db, logger, still_reproducible)?;
    }

    let mut ok = true;

    for row in &local_inflight {
        let sid = row.swap_id.clone();
        match row.status.as_str() {
            "applied" => {
                // B9: grace window for a swap the evaluator applied to
                // milliseconds after this pass's `get_my_swaps` fetch ran.
                if now - row.applied_at < RECONCILE_GRACE_SECONDS {
                    continue;
                }
                if pending_ids.contains(&sid) || opening_ids.contains(&sid) {
                    continue;
                }
                if completed_ids.contains(&sid) {
                    // Live contract, not a remote cancellation -- trip for
                    // operator review (activation must not be skipped).
                    breaker::trip_and_persist(
                        db,
                        logger,
                        BreakerCause::LocalRowDivergentFromRemote {
                            swap_id: sid.clone(),
                            detail: "still 'applied' but LN+ lists it completed — resolve manually (backfill skips swaps that already have a local row)".to_string(),
                        },
                        now,
                    )?;
                    ok = false;
                    continue;
                }
                // B4: an 'applied' row absent from pending/opening/completed
                // on a successful fetch is a REMOTE cancellation, not our
                // defection. Terminal, frees the serialization slot and the
                // capacity reservation automatically -- NOT a breaker trip.
                db.cas_swap(
                    &sid,
                    &["applied"],
                    &SwapPatch::default()
                        .status("cancelled_remote")
                        .outcome("swap disappeared from LN+ (creator cancelled/deleted)"),
                )?;
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: applied swap {sid} vanished from LN+ (not in pending/opening/completed) — treating as remote cancellation, not tripping the breaker"),
                );
            }
            "opening" | "opened" => {
                let compatible = opening_ids.contains(&sid) || completed_ids.contains(&sid);
                if !compatible {
                    breaker::trip_and_persist(
                        db,
                        logger,
                        BreakerCause::LocalRowDivergentFromRemote {
                            swap_id: sid.clone(),
                            detail: format!("(status {}) missing/divergent on LN+", row.status),
                        },
                        now,
                    )?;
                    ok = false;
                }
            }
            _ => {}
        }
    }

    // I1: pending/opening ghosts LN+ knows about with no local row.
    for sid in &opening_ids {
        if sid.is_empty() || local_ids.contains(sid) {
            continue;
        }
        breaker::trip_and_persist(
            db,
            logger,
            BreakerCause::OpeningGhostNoLocalRecord {
                swap_id: sid.clone(),
            },
            now,
        )?;
        ok = false;
    }

    for sid in &pending_ids {
        if sid.is_empty() || local_ids.contains(sid) {
            continue;
        }
        // B5(b): a pending entry matching a local row we already knowingly
        // walked away from is stale-application cleanup, not a ghost.
        let local_row = db.get_swap(sid);
        if let Some(local_row) = &local_row {
            if TERMINAL_PENDING_GHOST_STATUSES.contains(&local_row.status.as_str()) {
                if let Err(e) = api.delete_application(sid) {
                    logger.log(
                        LogLevel::Warn,
                        &format!("LNPLUS: delete_application({sid}) failed for a stale pending application (local status {:?}): {e}", local_row.status),
                    );
                }
                continue;
            }
        }
        breaker::trip_and_persist(
            db,
            logger,
            BreakerCause::PendingGhostNoLocalRecord {
                swap_id: sid.clone(),
            },
            now,
        )?;
        ok = false;
    }

    Ok(ok)
}

/// Task 61 4B — restart reconciliation for quarantined attempts.
///
/// First promotes stale in-flight `Intent` rows (a crashed process died
/// mid-submit — unknown by definition; quarantine must survive restart).
/// Then, for every `OutcomeUnknown` attempt, resolves EXACTLY ONCE from
/// authoritative evidence:
///  - Fund: a genuine `listpeerchannels` answer. A channel to the
///    attempt's peer matching the committed capacity (the same I5(b)/B7
///    total_msat-or-to_us_msat rule as the open path) → `CommittedFund`
///    (row txid + settle-at-reserved + receipt, one transaction);
///    a genuine empty answer → `NotSubmitted` (release). An RPC failure
///    leaves the attempt quarantined — never resolved on a guess.
///  - Apply: LN+'s own `get_my_swaps` listing (`my`). Listed in any
///    bucket → `CommittedApply`; genuinely absent → `NotSubmitted` plus
///    the row failed. `my: None` (outage pass) leaves apply attempts
///    quarantined.
pub fn reconcile_unknown_attempts(
    my: Option<&MySwaps>,
    db: &dyn LnPlusDb,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
    now: i64,
) -> PortResult<()> {
    let promoted = db.quarantine_stale_intents("process restarted with attempt in flight", now)?;
    if promoted > 0 {
        logger.log(
            LogLevel::Warn,
            &format!(
                "LNPLUS: promoted {promoted} stale in-flight attempt(s) to quarantine — a \
                 previous process died mid-submit; resolving from evidence"
            ),
        );
    }

    for attempt in db.unknown_attempts()? {
        let aid = attempt.attempt_id.clone();
        match attempt.kind {
            AttemptKind::Fund => {
                let Some(peer) = attempt.peer_id.clone() else {
                    logger.log(
                        LogLevel::Error,
                        &format!(
                            "LNPLUS: quarantined fund attempt {aid} has no peer recorded — \
                             cannot verify from chain; operator review needed"
                        ),
                    );
                    continue;
                };
                let channels = match chain.list_peer_channels(Some(&peer)) {
                    Ok(channels) => channels,
                    Err(e) => {
                        logger.log(
                            LogLevel::Warn,
                            &format!(
                                "LNPLUS: cannot verify quarantined attempt {aid} \
                                 (listpeerchannels failed: {e}) — staying quarantined"
                            ),
                        );
                        continue;
                    }
                };
                let capacity = attempt.amount_sats.unwrap_or(-1);
                let matched = channels.iter().find(|ch| {
                    ch.peer_id == peer
                        && crate::open::OPEN_STATES.contains(&ch.state.as_str())
                        && (ch.total_msat / 1000 == capacity || ch.to_us_msat / 1000 == capacity)
                });
                let resolution = match matched {
                    Some(ch) => {
                        let txid = ch.funding_txid.clone().unwrap_or_else(|| {
                            "unknown (reconciled from channel evidence)".to_string()
                        });
                        logger.log(
                            LogLevel::Warn,
                            &format!(
                                "LNPLUS: quarantined attempt {aid} RESOLVED committed — chain \
                                 shows the matching channel (txid {txid})"
                            ),
                        );
                        AttemptResolution::CommittedFund {
                            txid,
                            actual_cost_sats: None,
                        }
                    }
                    None => {
                        logger.log(
                            LogLevel::Warn,
                            &format!(
                                "LNPLUS: quarantined attempt {aid} RESOLVED not-submitted — a \
                                 genuine chain answer shows no matching channel; releasing its \
                                 reservation"
                            ),
                        );
                        AttemptResolution::NotSubmitted {
                            detail: "chain shows no matching channel after restart".to_string(),
                        }
                    }
                };
                db.resolve_attempt(&aid, &resolution, now)?;
            }
            AttemptKind::Apply => {
                let Some(my) = my else { continue };
                let sid = attempt.swap_id.clone();
                let listed = my.pending_ids().contains(&sid)
                    || my.opening_ids().contains(&sid)
                    || my.completed_ids().contains(&sid);
                if listed {
                    logger.log(
                        LogLevel::Warn,
                        &format!(
                            "LNPLUS: quarantined apply attempt {aid} RESOLVED committed — LN+ \
                             lists swap {sid}"
                        ),
                    );
                    db.resolve_attempt(&aid, &AttemptResolution::CommittedApply, now)?;
                } else {
                    logger.log(
                        LogLevel::Warn,
                        &format!(
                            "LNPLUS: quarantined apply attempt {aid} RESOLVED not-submitted — \
                             LN+ does not list swap {sid}; marking the row failed"
                        ),
                    );
                    db.resolve_attempt(
                        &aid,
                        &AttemptResolution::NotSubmitted {
                            detail: "LN+ does not list the swap after restart".to_string(),
                        },
                        now,
                    )?;
                    db.cas_swap(
                        &sid,
                        &["applied"],
                        &SwapPatch::default()
                            .status("failed")
                            .outcome("apply never landed (reconciled after restart)"),
                    )?;
                }
            }
        }
    }
    Ok(())
}

/// Re-exported so callers only need one `use` for reconcile-adjacent
/// error handling.
pub type ReconcileError = LnPlusError;
