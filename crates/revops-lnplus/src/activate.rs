//! Gates 12-13: activation. Ports `_activate` (py 1808-1850),
//! `_check_mid_contract_vanish` (1852-1865), `_protect_peer_no_close`
//! (1985-2022), `_release_no_close_if_ours` (2023-2049), and
//! `_incoming_channel_open` (2050-2064).

use crate::db_types::{SwapPatch, SwapRow};
use crate::open::OPEN_STATES;
use crate::ports::{
    CasOutcome, ChainPort, LnPlusDb, LogLevel, Logger, PlannerActionRequest, PolicyPort, PortResult,
};

/// Which `lnplus_swaps` boolean column a no_close operation targets —
/// `"tag_added"` for the outbound side, `"incoming_tag_added"` for the
/// counterparty's channel to us (py's `flag_column` string parameter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagColumn {
    Outbound,
    Incoming,
}

fn patch_for(column: TagColumn, value: bool) -> SwapPatch {
    match column {
        TagColumn::Outbound => SwapPatch::default().tag_added(value),
        TagColumn::Incoming => SwapPatch::default().incoming_tag_added(value),
    }
}

fn flag_of(row: &SwapRow, column: TagColumn) -> Option<bool> {
    match column {
        TagColumn::Outbound => row.tag_added,
        TagColumn::Incoming => row.incoming_tag_added,
    }
}

/// Task 61 4A: the ownership-flag write is a CAS from the status the
/// caller just observed. A conflict (the row moved concurrently) skips
/// the stamp with a log line — the protection tag itself already landed;
/// a persistence FAILURE propagates as `Err` (fail closed).
fn stamp_flag(
    sid: &str,
    column: TagColumn,
    value: bool,
    expected_statuses: &[&str],
    db: &dyn LnPlusDb,
    logger: &dyn Logger,
) -> PortResult<()> {
    match db.cas_swap(sid, expected_statuses, &patch_for(column, value))? {
        CasOutcome::Applied => Ok(()),
        CasOutcome::Conflict { actual } => {
            logger.log(
                LogLevel::Warn,
                &format!(
                    "LNPLUS: swap {sid} moved (now {actual:?}) before its no_close ownership \
                     flag could be stamped — tag left in place, flag not recorded"
                ),
            );
            Ok(())
        }
    }
}

/// py `_protect_peer_no_close` (1985-2022). C-3: a pre-existing tag
/// (operator-set or another contract's) is recorded as "not ours" (0) so
/// release never clobbers it. Lazy-eval audit F3b: on a lookup FAILURE,
/// protect anyway but never claim ownership (stamp 0) — worst case is a
/// stale tag needing manual cleanup, not an unprotected channel.
///
/// `expected_statuses`: the status(es) the caller just read the row in —
/// the CAS guard for the ownership-flag stamp (Task 61 4A).
pub fn protect_peer_no_close(
    sid: &str,
    peer: &str,
    column: TagColumn,
    expected_statuses: &[&str],
    db: &dyn LnPlusDb,
    policy: &dyn PolicyPort,
    logger: &dyn Logger,
) -> PortResult<()> {
    let lookup = policy.get_policy(peer);
    let (already_tagged, lookup_failed) = match &lookup {
        Ok(Some(p)) => (p.has_tag("no_close"), false),
        Ok(None) => (false, false),
        Err(e) => {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: get_policy failed for {peer} while protecting swap {sid}: {e}"),
            );
            (false, true)
        }
    };

    if lookup_failed {
        if let Err(e) = policy.add_tag(peer, "no_close") {
            logger.log(
                LogLevel::Warn,
                &format!("LNPLUS: add_tag(no_close) failed for {peer}: {e}"),
            );
        }
        return stamp_flag(sid, column, false, expected_statuses, db, logger);
    }
    if already_tagged {
        stamp_flag(sid, column, false, expected_statuses, db, logger)
    } else {
        match policy.add_tag(peer, "no_close") {
            Ok(()) => stamp_flag(sid, column, true, expected_statuses, db, logger),
            Err(e) => {
                logger.log(
                    LogLevel::Warn,
                    &format!("LNPLUS: add_tag(no_close) failed for {peer}: {e}"),
                );
                Ok(())
            }
        }
    }
}

/// py `_release_no_close_if_ours` (2023-2049). C-3: only remove the tag
/// when THIS row is the one that added it AND no OTHER `active` row still
/// references the same peer in either contract role.
pub fn release_no_close_if_ours(
    sid: &str,
    row: &SwapRow,
    peer: Option<&str>,
    column: TagColumn,
    db: &dyn LnPlusDb,
    policy: &dyn PolicyPort,
    logger: &dyn Logger,
) {
    let Some(peer) = peer else { return };
    if flag_of(row, column) != Some(true) {
        return;
    }
    let other_active: Vec<SwapRow> = db
        .get_swaps_by_status(&["active"])
        .into_iter()
        .filter(|r| {
            r.swap_id != sid
                && (r.outbound_peer.as_deref() == Some(peer)
                    || r.incoming_peer.as_deref() == Some(peer))
        })
        .collect();
    if !other_active.is_empty() {
        logger.log(
            LogLevel::Info,
            &format!("LNPLUS: swap {sid} ended but no_close on {peer} is still held by {} other active contract(s) — not removing", other_active.len()),
        );
        return;
    }
    if let Err(e) = policy.remove_tag(peer, "no_close") {
        logger.log(
            LogLevel::Warn,
            &format!("LNPLUS: remove_tag(no_close) failed for {peer}: {e}"),
        );
    }
}

/// py `_activate` (1808-1850). Both sides of the swap ring are protected:
/// the channel WE opened outbound, and the counterparty's channel TO us
/// (the LN+ agreement binds both — closing either mid-contract is a
/// defection).
///
/// Task 61 4A: every ledger write is a CAS from the "opened" status the
/// caller selected this row by, and any persistence failure aborts the
/// activation with `Err` (fail closed — the next pass retries from the
/// still-"opened" row).
pub fn activate(
    row: &SwapRow,
    entry_ends_at: Option<i64>,
    entry_incoming_peer: Option<&str>,
    db: &dyn LnPlusDb,
    policy: &dyn PolicyPort,
    logger: &dyn Logger,
) -> PortResult<()> {
    let sid = row.swap_id.clone();

    let mut pre = SwapPatch::default();
    if let Some(ends) = entry_ends_at {
        pre = pre.ends_at(ends);
    }
    let incoming = entry_incoming_peer
        .map(str::to_string)
        .or_else(|| row.incoming_peer.clone());
    if let Some(inc) = &incoming {
        pre = pre.incoming_peer(inc.clone());
    }
    if pre != SwapPatch::default() {
        if let CasOutcome::Conflict { actual } = db.cas_swap(&sid, &["opened"], &pre)? {
            logger.log(
                LogLevel::Warn,
                &format!(
                    "LNPLUS: swap {sid} moved (now {actual:?}) before activation could record \
                     its contract terms — skipping activation this pass"
                ),
            );
            return Ok(());
        }
    }

    if let Some(outbound_peer) = &row.outbound_peer {
        protect_peer_no_close(
            &sid,
            outbound_peer,
            TagColumn::Outbound,
            &["opened"],
            db,
            policy,
            logger,
        )?;
    }
    let incoming_peer = incoming.or_else(|| db.get_swap(&sid).and_then(|r| r.incoming_peer));
    if let Some(incoming_peer) = &incoming_peer {
        protect_peer_no_close(
            &sid,
            incoming_peer,
            TagColumn::Incoming,
            &["opened"],
            db,
            policy,
            logger,
        )?;
    }

    if let CasOutcome::Conflict { actual } =
        db.cas_swap(&sid, &["opened"], &SwapPatch::default().status("active"))?
    {
        logger.log(
            LogLevel::Warn,
            &format!(
                "LNPLUS: swap {sid} moved (now {actual:?}) before its active transition — \
                 leaving as-is"
            ),
        );
        return Ok(());
    }

    let current = db.get_swap(&sid).unwrap_or_else(|| row.clone());
    let action_id = db.record_planner_action(&PlannerActionRequest {
        action_type: "swap_complete",
        peer_id: row
            .outbound_peer
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        amount_sats: Some(current.capacity_sats),
        estimated_cost_sats: None,
        reason: format!("LN+ swap {sid} contract active until {:?}", current.ends_at),
        metadata: None,
    })?;
    db.update_planner_action(action_id, "completed")?;
    Ok(())
}

/// py `_check_mid_contract_vanish` (1852-1865). Fail-open on an RPC
/// hiccup (no channels list -> no alert this pass; matches Python's bare
/// `except: return`).
pub fn check_mid_contract_vanish(row: &SwapRow, chain: &dyn ChainPort, logger: &dyn Logger) {
    let Some(peer) = &row.outbound_peer else {
        return;
    };
    let Ok(channels) = chain.list_peer_channels(None) else {
        return;
    };
    if !channels.iter().any(|c| &c.peer_id == peer) {
        logger.log(
            LogLevel::Error,
            &format!("LNPLUS: swap channel to {peer} closed mid-contract — operator review needed"),
        );
    }
}

/// py `_incoming_channel_open` (2050-2064). `Some(true/false)` is a
/// genuine RPC answer (a missing/empty `incoming_peer` is itself an
/// authoritative "no channel" -> `Some(false)`, matching Python's
/// `if not incoming_peer: return False`); `None` means the RPC itself
/// FAILED — B2: distinct from an authoritative "no channel" answer, and
/// must not be treated as a negative-rating signal by the caller
/// ([`crate::finalize::finalize`]).
pub fn incoming_channel_open(incoming_peer: Option<&str>, chain: &dyn ChainPort) -> Option<bool> {
    let Some(peer) = incoming_peer.filter(|p| !p.is_empty()) else {
        return Some(false);
    };
    match chain.list_peer_channels(None) {
        Ok(channels) => Some(
            channels
                .iter()
                .any(|c| c.peer_id == peer && OPEN_STATES.contains(&c.state.as_str())),
        ),
        Err(_) => None,
    }
}
