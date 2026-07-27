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

use crate::breaker::{self, BreakerCause};
use crate::db_types::{SwapPatch, SwapRow};
use crate::error::LnPlusError;
use crate::ports::{
    ChainPort, Feerate, LnPlusApi, LnPlusDb, LogLevel, Logger, PlannerActionRequest,
    ReserveSpendRequest,
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

/// py `_execute_swap_open` (1532-1699). Returns `true` only when the row
/// actually reached `"opened"` — callers use this to decide whether the
/// pass may report the swap as opened.
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
) -> bool {
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
        return false;
    };

    let channels = chain.list_peer_channels(None).unwrap_or_else(|e| {
        logger.log(
            LogLevel::Error,
            &format!("LNPLUS: listpeerchannels failed for swap {sid}: {e}"),
        );
        Vec::new()
    });
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
            db.update_swap(
                &sid,
                &SwapPatch::default()
                    .channel_funding_txid(txid.clone())
                    .opened_at(now),
            );
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
            maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger);
            return false;
        }

        // Write intent before the irreversible external call.
        db.update_swap(
            &sid,
            &SwapPatch::default()
                .status("opening")
                .outcome("fundchannel attempt"),
        );

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
                maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger);
                return false;
            }
        };
        if !reservation_active {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: budget reservation failed for swap {sid} — retrying next pass"),
            );
            maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger);
            return false;
        }

        match chain.fund_channel(&peer, capacity_sats, feerate) {
            Err(e) => {
                logger.log(
                    LogLevel::Error,
                    &format!("LNPLUS: fundchannel to {peer} failed for swap {sid}: {e}"),
                );
                release_swap_open_reservation(&reservation_id, &sid, db, logger);
                maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger);
                return false;
            }
            Ok(result) => match result.txid {
                None => {
                    logger.log(
                        LogLevel::Error,
                        &format!("LNPLUS: fundchannel for swap {sid} returned no txid"),
                    );
                    release_swap_open_reservation(&reservation_id, &sid, db, logger);
                    maybe_trip_deadline_miss(row, &sid, deadline, now, db, logger);
                    return false;
                }
                Some(txid) => {
                    db.update_swap(
                        &sid,
                        &SwapPatch::default()
                            .channel_funding_txid(txid)
                            .opened_at(now),
                    );
                    first_fund = true;
                    settle_swap_open_reservation(
                        &reservation_id,
                        params.estimated_cost_sats,
                        &sid,
                        db,
                        logger,
                    );
                }
            },
        }
    }

    let marked_opened = complete_and_mark_opened(&sid, db, api, logger);
    if first_fund {
        record_swap_open_planner_action(
            row,
            "completed",
            &format!("LN+ swap {sid}: channel opened to {peer}"),
            db,
        );
    }
    marked_opened
}

/// py `_already_past_opening` (1701-1717): true when LN+ rejects
/// `complete_application` because the application already left the
/// opening state — a success signal wearing a 422, not a retryable
/// failure.
pub fn already_past_opening(e: &LnPlusError) -> bool {
    e.structural_contains(422, "not in opening state")
}

/// py `_complete_and_mark_opened` (1719-1734).
pub fn complete_and_mark_opened(
    sid: &str,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    logger: &dyn Logger,
) -> bool {
    if let Err(e) = api.complete_application(sid) {
        if !already_past_opening(&e) {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: complete_application failed for {sid} (will retry): {e}"),
            );
            return false;
        }
        logger.log(
            LogLevel::Info,
            &format!("LNPLUS: complete_application for {sid} reports it already left 'opening' — treating as already complete and marking opened"),
        );
    }
    db.update_swap(sid, &SwapPatch::default().status("opened"));
    true
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

fn settle_swap_open_reservation(
    reservation_id: &str,
    estimated_cost: i64,
    sid: &str,
    db: &dyn LnPlusDb,
    logger: &dyn Logger,
) {
    for attempt in 1..=3 {
        match db.mark_spend_reservation_spent(reservation_id, estimated_cost, "lnplus_swaps") {
            Ok(true) => return,
            Ok(false) => {
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: swap-open settle write returned failure for swap {sid} (attempt {attempt}/3); retrying"),
                );
            }
            Err(e) => {
                logger.log(LogLevel::Warn, &format!("LNPLUS: swap-open settle write failed for swap {sid} (attempt {attempt}/3): {e}"));
            }
        }
    }
    logger.log(
        LogLevel::Error,
        &format!(
            "LNPLUS: swap-open settle write PERSISTENTLY FAILED for swap {sid}: committed fee kept as an active reservation ({estimated_cost} sats) so it stays counted against the unified budget; investigate spend_events."
        ),
    );
}

/// py `_maybe_trip_deadline_miss` (1785-1793) — **defect #4 fix here**.
/// Python trips the breaker and records a `failed` planner-action
/// breadcrumb but never touches `lnplus_swaps.status`, so the row stays
/// in `INFLIGHT_STATUSES` (`"opening"`) forever and keeps consuming
/// reserved capex budget (see `telemetry.rs`). This port additionally
/// patches the row's `status` to `"failed"` in the SAME call, atomically
/// with the breaker trip — the breaker still latches (never
/// auto-clearable, per `breaker.rs`) but the row now correctly falls out
/// of `reserved_sats()`/`inflight_swaps()`.
pub fn maybe_trip_deadline_miss(
    row: &SwapRow,
    sid: &str,
    deadline: Option<i64>,
    now: i64,
    db: &dyn LnPlusDb,
    logger: &dyn Logger,
) {
    let Some(deadline) = deadline else { return };
    if now <= deadline {
        return;
    }
    let current = db.get_swap(sid).unwrap_or_else(|| row.clone());
    if current.channel_funding_txid.is_some() {
        return;
    }
    breaker::trip_and_persist(
        db,
        logger,
        BreakerCause::MissedOpenDeadline {
            swap_id: sid.to_string(),
        },
        now,
    );
    // Defect #4 fix: terminalize the row so reserved-budget/inflight
    // telemetry stops counting a swap that will never open.
    db.update_swap(
        sid,
        &SwapPatch::default()
            .status("failed")
            .outcome("missed open deadline"),
    );
    record_swap_open_planner_action(
        &current,
        "failed",
        &format!("LN+ swap {sid}: missed open deadline"),
        db,
    );
}

fn record_swap_open_planner_action(row: &SwapRow, status: &str, reason: &str, db: &dyn LnPlusDb) {
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
    });
    db.update_planner_action(action_id, status);
}
