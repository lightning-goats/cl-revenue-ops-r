//! Gates 10-11: channel-open execution. Ports `_execute_swap_open`
//! (py 1532-1699), `_already_past_opening` (1701-1717),
//! `_complete_and_mark_opened` (1719-1734),
//! `_release_swap_open_reservation` / `_settle_swap_open_reservation`
//! (1736-1783), and `_maybe_trip_deadline_miss` (1785-1793) — **the
//! defect #4 fix lives in [`maybe_trip_deadline_miss`]**, see
//! `telemetry.rs`'s module doc for the full incident writeup; the short
//! version: Python trips the breaker on a missed deadline but never
//! terminalizes the row, so it keeps consuming reserved capex budget
//! forever. This port also patches the row to `"failed"` in the same
//! call.

use crate::breaker::BreakerCause;
use crate::db_types::{SwapPatch, SwapRow};
use crate::error::LnPlusError;
use crate::ports::{
    AttemptIntent, AttemptKind, AttemptResolution, BeginAttemptAck, CasOutcome, ChainPort,
    CompoundOutcome, Feerate, FundChannelOutcome, LnPlusApi, LnPlusDb, LogLevel, Logger,
    PlannerActionRequest, PortResult, ReserveSpendRequest, ResolveAck, TerminalizeSpec,
};
use crate::types::{Metadata, MySwapEntry};
use crate::validation::valid_connect_addr;

/// py 672 `_DEFAULT_OPEN_COST_SATS`.
pub const DEFAULT_OPEN_COST_SATS: i64 = 2500;
/// py 666-667 `_OPEN_STATES`.
pub const OPEN_STATES: [&str; 5] = [
    "OPENINGD",
    "CHANNELD_AWAITING_LOCKIN",
    "CHANNELD_NORMAL",
    "DUALOPEND_OPEN_INIT",
    "DUALOPEND_AWAITING_LOCKIN",
];

/// Everything `_execute_swap_open` reads from injected sources that isn't
/// a trait call — the "would-be `_estimate_open_cost_fn`/
/// `_budget_params_fn`" values, resolved by the caller (mirrors Python's
/// `self._estimate_open_cost_fn() if ... else _DEFAULT_OPEN_COST_SATS`
/// fallback happening one layer up, at construction time, rather than
/// inside this function).
#[derive(Debug, Clone)]
pub struct OpenExecParams {
    pub estimated_cost_sats: i64,
    pub effective_budget_sats: Option<i64>,
    pub budget_since_timestamp: Option<i64>,
}

/// py `_execute_swap_open` (1532-1699). Returns `Ok(true)` only when the
/// row actually reached `"opened"` — callers use this to decide whether
/// the pass may report the swap as opened.
///
/// Task 61 4A: every ledger write is a CAS from the in-flight statuses
/// this row was selected by, and a persistence failure aborts with `Err`
/// (fail closed) instead of continuing on unacknowledged state.
#[allow(clippy::too_many_arguments)]
pub fn execute_swap_open(
    row: &SwapRow,
    entry: Option<&MySwapEntry>,
    params: &OpenExecParams,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
    now: i64,
) -> PortResult<bool> {
    let sid = row.swap_id.clone();
    let deadline = row.deadline_at;

    if row.channel_funding_txid.is_some() {
        // Already funded on a prior pass -- only complete_application left.
        return complete_and_mark_opened(&sid, db, api, logger);
    }

    let Some(peer) = row.outbound_peer.clone() else {
        // Rust-side hardening: Python would let `peer=None` reach string
        // interpolation / RPC calls below and rely on the outer
        // try/except in `run_watcher_once` to catch the resulting
        // exception and log+retry next pass (py 1424-1426). Same
        // observable outcome (retry next pass), reached without relying
        // on an exception escaping mid-mutation.
        logger.log(
            LogLevel::Error,
            &format!(
                "LNPLUS: swap {sid} has no outbound_peer yet — cannot open, retrying next pass"
            ),
        );
        return Ok(false);
    };

    // Task 61 4C correction F4C-2: an unreadable peer-channel state must
    // FAIL CLOSED. Proceeding on an empty default would skip the
    // I5(b)/B7 existing-channel check and could double-fund a channel we
    // simply could not see. No connect, no fundchannel; retry next pass.
    let channels = match chain.list_peer_channels(None) {
        Ok(channels) => channels,
        Err(e) => {
            logger.log(
                LogLevel::Error,
                &format!(
                    "LNPLUS: listpeerchannels failed for swap {sid}: {e} — refusing to open \
                     against unreadable channel state; retrying next pass"
                ),
            );
            return Ok(false);
        }
    };
    let capacity_sats = row.capacity_sats;
    // I5(b) / B7: only trust a match by capacity (this row's committed
    // swap terms, via total_msat OR to_us_msat for dual-fund) or an
    // already-recorded funding txid -- never "any open channel to peer".
    let existing = channels.iter().find(|ch| {
        ch.peer_id == peer
            && OPEN_STATES.contains(&ch.state.as_str())
            && (ch.total_msat / 1000 == capacity_sats || ch.to_us_msat / 1000 == capacity_sats)
    });

    let mut first_fund = false;
    if let Some(existing) = existing {
        if let Some(txid) = &existing.funding_txid {
            if let CasOutcome::Conflict { actual } = db.cas_swap(
                &sid,
                &["applied", "opening"],
                &SwapPatch::default()
                    .channel_funding_txid(txid.clone())
                    .opened_at(now),
            )? {
                logger.log(
                    LogLevel::Warn,
                    &format!(
                        "LNPLUS: swap {sid} moved (now {actual:?}) before its funding txid \
                         could be recorded — retrying next pass"
                    ),
                );
                return Ok(false);
            }
        }
    } else {
        let hours_left = deadline.map(|d| (d - now) as f64 / 3600.0).unwrap_or(-1.0);
        let feerate = if hours_left > 24.0 {
            Feerate::Slow
        } else if hours_left > 12.0 {
            Feerate::Normal
        } else {
            Feerate::Urgent
        };

        let raw_addr = entry.and_then(|e| {
            e.outgoing_peer_clearnet_address
                .clone()
                .or_else(|| e.outgoing_peer_tor_address.clone())
        });
        let addr = match &raw_addr {
            Some(a) if valid_connect_addr(a) => Some(a.clone()),
            Some(a) => {
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: swap {sid} — LN+ returned a malformed connect address ({a:?}); falling back to bare-pubkey connect"),
                );
                None
            }
            None => None,
        };
        let target = match &addr {
            Some(a) => format!("{peer}@{a}"),
            None => peer.clone(),
        };

        if let Err(e) = chain.connect(&target) {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: connect to {peer} failed for swap {sid}: {e}"),
            );
            maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger)?;
            return Ok(false);
        }

        // Write intent before the irreversible external call — CAS from
        // the in-flight statuses; an unacknowledged intent write must
        // never be followed by the live call (Task 61 4A).
        if let CasOutcome::Conflict { actual } = db.cas_swap(
            &sid,
            &["applied", "opening"],
            &SwapPatch::default()
                .status("opening")
                .outcome("fundchannel attempt"),
        )? {
            logger.log(
                LogLevel::Warn,
                &format!(
                    "LNPLUS: swap {sid} moved (now {actual:?}) before its fundchannel intent \
                     could be recorded — not funding this pass"
                ),
            );
            return Ok(false);
        }

        let reservation_id = format!("lnplus-open-{sid}-{now}");
        let mut metadata = Metadata::new();
        metadata.insert("swap_id".to_string(), sid.clone());
        metadata.insert("peer_id".to_string(), peer.clone());
        let reserve_req = ReserveSpendRequest {
            reservation_id: reservation_id.clone(),
            amount_sats: params.estimated_cost_sats,
            category: "channel_open",
            subcategory: "lnplus_swap",
            metadata,
            effective_budget_sats: params.effective_budget_sats,
            since_timestamp: params.budget_since_timestamp,
        };
        let reservation_active = match db.reserve_spend(&reserve_req) {
            Ok(active) => active,
            Err(e) => {
                logger.log(LogLevel::Warn, &format!("LNPLUS: budget reservation failed for swap {sid} — retrying next pass ({e})"));
                maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger)?;
                return Ok(false);
            }
        };
        if !reservation_active {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: budget reservation failed for swap {sid} — retrying next pass"),
            );
            maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger)?;
            return Ok(false);
        }

        // Task 61 4B: durable attempt identity BEFORE the irreversible
        // submit, binding the reservation. Blocked = an earlier attempt is
        // quarantined (or somehow still in flight) — the no-auto-resubmit
        // rail: release this pass's fresh reservation and stand down.
        let attempt_id = format!("fund:{sid}:{now}");
        match db.begin_attempt(&AttemptIntent {
            attempt_id: attempt_id.clone(),
            swap_id: sid.clone(),
            kind: AttemptKind::Fund,
            reservation_id: Some(reservation_id.clone()),
            peer_id: Some(peer.clone()),
            amount_sats: Some(capacity_sats),
            created_at: now,
        })? {
            BeginAttemptAck::Started => {}
            BeginAttemptAck::Blocked {
                existing_attempt_id,
                state,
            } => {
                logger.log(
                    LogLevel::Error,
                    &format!(
                        "LNPLUS: swap {sid} has attempt {existing_attempt_id} in state {state:?} \
                         — NOT funding again (no auto-resubmit); reconciliation owns it"
                    ),
                );
                release_swap_open_reservation(&reservation_id, &sid, db, logger);
                return Ok(false);
            }
        }

        match chain.fund_channel(&peer, capacity_sats, feerate) {
            Err(e) => {
                // Typed CLEAN pre-submit failure: nothing reached the
                // node. Resolve not_submitted — the release rides the
                // same transaction as the attempt transition.
                logger.log(
                    LogLevel::Error,
                    &format!("LNPLUS: fundchannel to {peer} failed for swap {sid}: {e}"),
                );
                db.resolve_attempt(
                    &attempt_id,
                    &AttemptResolution::NotSubmitted {
                        detail: format!("fundchannel clean failure: {e}"),
                    },
                    now,
                )?;
                maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger)?;
                return Ok(false);
            }
            Ok(FundChannelOutcome::OutcomeUnknown { detail }) => {
                // Post-submit ambiguity: funds MAY be committed on chain.
                // Quarantine (reservation RETAINED), never terminalize the
                // row, never trip the deadline miss, never retry.
                logger.log(
                    LogLevel::Error,
                    &format!(
                        "LNPLUS: fundchannel outcome UNKNOWN for swap {sid} ({detail}) — \
                         quarantining attempt {attempt_id}; reservation held; \
                         reconciliation will resolve from chain evidence"
                    ),
                );
                db.resolve_attempt(
                    &attempt_id,
                    &AttemptResolution::OutcomeUnknown {
                        detail: detail.clone(),
                    },
                    now,
                )?;
                db.cas_swap(
                    &sid,
                    &["applied", "opening"],
                    &SwapPatch::default().outcome(format!(
                        "fundchannel outcome unknown — quarantined ({detail})"
                    )),
                )?;
                return Ok(false);
            }
            Ok(FundChannelOutcome::Funded(result)) => match result.txid {
                None => {
                    // The node reported success without a txid: the open
                    // very likely HAPPENED — this is ambiguity, not a
                    // clean failure. Quarantine; never release.
                    logger.log(
                        LogLevel::Error,
                        &format!(
                            "LNPLUS: fundchannel for swap {sid} returned success with no txid — \
                             treating as OUTCOME UNKNOWN; quarantining attempt {attempt_id}"
                        ),
                    );
                    db.resolve_attempt(
                        &attempt_id,
                        &AttemptResolution::OutcomeUnknown {
                            detail: "fundchannel success response carried no txid".to_string(),
                        },
                        now,
                    )?;
                    return Ok(false);
                }
                Some(txid) => {
                    // Task 61 4B compound: row txid/opened_at + reservation
                    // settle + receipt + attempt state, ONE transaction.
                    match db.resolve_attempt(
                        &attempt_id,
                        &AttemptResolution::CommittedFund {
                            txid,
                            actual_cost_sats: Some(params.estimated_cost_sats),
                        },
                        now,
                    )? {
                        ResolveAck::Resolved => {}
                        other => {
                            logger.log(
                                LogLevel::Error,
                                &format!(
                                    "LNPLUS: committed-fund resolution for swap {sid} did not \
                                     apply ({other:?}) — not reporting opened"
                                ),
                            );
                            return Ok(false);
                        }
                    }
                    first_fund = true;
                }
            },
        }
    }

    let marked_opened = complete_and_mark_opened(&sid, db, api, logger)?;
    if first_fund {
        record_swap_open_planner_action(
            row,
            "completed",
            &format!("LN+ swap {sid}: channel opened to {peer}"),
            db,
        )?;
    }
    Ok(marked_opened)
}

/// py `_already_past_opening` (1701-1717): true when LN+ rejects
/// `complete_application` because the application already left the
/// opening state — a success signal wearing a 422, not a retryable
/// failure.
pub fn already_past_opening(e: &LnPlusError) -> bool {
    e.structural_contains(422, "not in opening state")
}

/// py `_complete_and_mark_opened` (1719-1734).
///
/// Task 61 4A: the "opened" transition is a CAS from the in-flight
/// statuses. A conflict whose actual status is already `"opened"` is a
/// success (an earlier pass got there); any other conflict is not.
pub fn complete_and_mark_opened(
    sid: &str,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    logger: &dyn Logger,
) -> PortResult<bool> {
    if let Err(e) = api.complete_application(sid) {
        if !already_past_opening(&e) {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: complete_application failed for {sid} (will retry): {e}"),
            );
            return Ok(false);
        }
        logger.log(
            LogLevel::Info,
            &format!("LNPLUS: complete_application for {sid} reports it already left 'opening' — treating as already complete and marking opened"),
        );
    }
    match db.cas_swap(
        sid,
        &["applied", "opening"],
        &SwapPatch::default().status("opened"),
    )? {
        CasOutcome::Applied => Ok(true),
        CasOutcome::Conflict { actual } if actual.as_deref() == Some("opened") => Ok(true),
        CasOutcome::Conflict { actual } => {
            logger.log(
                LogLevel::Warn,
                &format!(
                    "LNPLUS: swap {sid} moved (now {actual:?}) before its opened transition — \
                     not reporting it opened this pass"
                ),
            );
            Ok(false)
        }
    }
}

fn release_swap_open_reservation(
    reservation_id: &str,
    sid: &str,
    db: &dyn LnPlusDb,
    logger: &dyn Logger,
) {
    if let Err(e) = db.release_spend_reservation(reservation_id) {
        logger.log(
            LogLevel::Warn,
            &format!(
                "LNPLUS: release_spend_reservation failed for swap {sid} ({reservation_id}): {e}"
            ),
        );
    }
}

// (The pre-4B `settle_swap_open_reservation` retry loop is gone: the
// settle + receipt now ride the SAME transaction as the row's funding
// txid and the attempt's committed state — see
// `LnPlusDb::resolve_attempt` / `AttemptResolution::CommittedFund`. A
// settle failure rolls the whole resolution back and the attempt stays
// reconcilable, instead of a best-effort loop leaving a settled-or-not
// reservation beside an already-updated row.)

/// py `_maybe_trip_deadline_miss` (1785-1793) — **defect #4 fix here**.
/// Python trips the breaker and records a `failed` planner-action
/// breadcrumb but never touches `lnplus_swaps.status`, so the row stays
/// in `INFLIGHT_STATUSES` (`"opening"`) forever and keeps consuming
/// reserved capex budget (see `telemetry.rs`). This port additionally
/// patches the row's `status` to `"failed"` in the SAME call, atomically
/// with the breaker trip — the breaker still latches (never
/// auto-clearable, per `breaker.rs`) but the row now correctly falls out
/// of `reserved_sats()`/`inflight_swaps()`.
/// Since Task 61 4A the row-terminalize + breaker-trip pairing is
/// GENUINELY atomic: one [`LnPlusDb::terminalize_and_trip`] transaction
/// covers the row CAS (guarded on in-flight status + no recorded funding
/// txid) and the B10 first-cause-preserving trip; either both land or
/// neither does, and a persistence failure surfaces as `Err`.
pub fn maybe_trip_deadline_miss(
    row: &SwapRow,
    sid: &str,
    deadline: Option<i64>,
    now: i64,
    db: &dyn LnPlusDb,
    logger: &dyn Logger,
) -> PortResult<()> {
    let Some(deadline) = deadline else {
        return Ok(());
    };
    if now <= deadline {
        return Ok(());
    }
    let outcome = db.terminalize_and_trip(
        &TerminalizeSpec {
            swap_id: sid,
            expected_statuses: &["applied", "opening"],
            require_null_funding_txid: true,
        },
        &SwapPatch::default()
            .status("failed")
            .outcome("missed open deadline"),
        BreakerCause::MissedOpenDeadline {
            swap_id: sid.to_string(),
        },
        now,
    )?;
    match outcome {
        CompoundOutcome::Terminalized { breaker } => {
            logger.log(
                LogLevel::Error,
                &format!(
                    "LNPLUS: CIRCUIT BREAKER — missed 48h deadline for swap {sid}; row \
                     terminalized{}",
                    match breaker {
                        crate::ports::TripAck::NewTrip => "",
                        crate::ports::TripAck::AlreadyTripped =>
                            " (breaker already tripped; first cause preserved)",
                    }
                ),
            );
            record_swap_open_planner_action(
                row,
                "failed",
                &format!("LN+ swap {sid}: missed open deadline"),
                db,
            )?;
        }
        CompoundOutcome::Conflict { .. } => {
            // Guard held inside the transaction: the row was funded, or
            // already moved out of the in-flight statuses — exactly the
            // cases the old code's racy pre-checks tried to skip.
        }
    }
    Ok(())
}

fn record_swap_open_planner_action(
    row: &SwapRow,
    status: &str,
    reason: &str,
    db: &dyn LnPlusDb,
) -> PortResult<()> {
    let action_id = db.record_planner_action(&PlannerActionRequest {
        action_type: "swap_open",
        peer_id: row
            .outbound_peer
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        amount_sats: Some(row.capacity_sats),
        estimated_cost_sats: Some(0),
        reason: reason.to_string(),
        metadata: None,
    })?;
    db.update_planner_action(action_id, status)
}
