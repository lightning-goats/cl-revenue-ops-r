//! Adopt LN+ account state accumulated before this automation existed
//! (manual applications/opens/running-or-ended contracts made on the LN+
//! website) into the local ledger. Ports `backfill_from_lnplus` and its
//! four `_backfill_*` rule helpers (py 1030-1296).
//!
//! Common rule across every category: skip unconditionally if a local row
//! for that `swap_id` already exists — `record_swap` is INSERT OR REPLACE
//! and would otherwise clobber automation-owned state or resurrect a
//! terminal row. This makes the whole function idempotent and safe to call
//! any number of times.

use crate::db_types::SwapPatch;
use crate::error::LnPlusError;
use crate::ports::{ChainPort, LnPlusApi, LnPlusDb, LogLevel, Logger};
use crate::types::{MySwapEntry, MySwaps, Rating, SwapDetail};
use crate::validation::{parse_ts, valid_pubkey};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ImportCounts {
    pub pending: u32,
    pub opening: u32,
    pub active: u32,
    pub ended: u32,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct BackfillResult {
    pub imported: ImportCounts,
    pub skipped: Vec<String>,
    pub warnings: Vec<String>,
}

/// py `backfill_from_lnplus` (1030-1058). `my` is required here (unlike
/// Python's `my=None -> fetch`) — the caller (`reconcile.rs`'s choke
/// point) always already has a fresh fetch in hand; a standalone
/// `revenue-lnplus-backfill` RPC entry point fetches via
/// [`LnPlusApi::get_my_swaps`] itself before calling this (see
/// `ENTRYPOINTS.md`).
pub fn backfill_from_lnplus(
    my: &MySwaps,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
    now: i64,
) -> BackfillResult {
    let mut result = BackfillResult::default();
    for entry in &my.pending {
        backfill_pending(entry, now, db, api, logger, &mut result);
    }
    for entry in &my.opening {
        backfill_opening(entry, now, db, api, logger, &mut result);
    }
    for entry in &my.completed {
        backfill_completed(entry, now, db, api, chain, logger, &mut result);
    }
    result
}

fn resolve_capacity_duration(
    entry: &MySwapEntry,
    sid: &str,
    api: &dyn LnPlusApi,
    warnings: &mut Vec<String>,
) -> (i64, i64) {
    let mut capacity = entry.capacity_sats;
    let mut duration = entry.duration_months;
    if capacity.is_none() || duration.is_none() {
        if let Ok(detail) = api.get_swap(sid) {
            if capacity.is_none() {
                capacity = detail.capacity_sats;
            }
            if duration.is_none() {
                duration = detail.duration_months;
            }
        }
    }
    let capacity = capacity.unwrap_or_else(|| {
        warnings.push(format!(
            "swap {sid}: capacity_sats unknown on import (defaulted to 0)"
        ));
        0
    });
    let duration = duration.unwrap_or_else(|| {
        warnings.push(format!(
            "swap {sid}: duration_months unknown on import (defaulted to 0)"
        ));
        0
    });
    (capacity, duration)
}

/// Rule 1 (py 1060-1080): applied row, no peer/identifier assignment yet.
fn backfill_pending(
    entry: &MySwapEntry,
    now: i64,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    logger: &dyn Logger,
    result: &mut BackfillResult,
) {
    let sid = &entry.id;
    if sid.is_empty() {
        return;
    }
    if db.get_swap(sid).is_some() {
        result.skipped.push(sid.clone());
        return;
    }
    let (capacity, duration) = resolve_capacity_duration(entry, sid, api, &mut result.warnings);
    db.record_swap(&crate::db_types::SwapRow::new(
        sid.clone(),
        "applied",
        capacity,
        duration,
        now,
    ));
    logger.log(
        LogLevel::Info,
        &format!("LNPLUS: backfill — imported pending swap {sid} ({capacity} sats, {duration}mo); pending-timeout clock starts now"),
    );
    result.imported.pending += 1;
}

/// Rule 2 (py 1082-1118): opening row.
fn backfill_opening(
    entry: &MySwapEntry,
    now: i64,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    logger: &dyn Logger,
    result: &mut BackfillResult,
) {
    let sid = &entry.id;
    if sid.is_empty() {
        return;
    }
    if db.get_swap(sid).is_some() {
        result.skipped.push(sid.clone());
        return;
    }
    let (capacity, duration) = resolve_capacity_duration(entry, sid, api, &mut result.warnings);
    let outbound_peer = entry
        .outgoing_peer_pubkey
        .as_deref()
        .filter(|p| valid_pubkey(p));
    if entry.outgoing_peer_pubkey.is_some() && outbound_peer.is_none() {
        result.warnings.push(format!(
            "swap {sid}: invalid outgoing_peer_pubkey on import — outbound_peer left NULL"
        ));
    }
    let deadline_ts = entry
        .deadline
        .as_ref()
        .and_then(parse_ts)
        .unwrap_or_else(|| {
            result.warnings.push(format!(
                "swap {sid}: no parseable deadline on import — using 48h fallback"
            ));
            now + 48 * 3600
        });
    let mut row = crate::db_types::SwapRow::new(sid.clone(), "opening", capacity, duration, now);
    if let Some(p) = outbound_peer {
        row = row.with_outbound_peer(p);
    }
    db.record_swap(&row);
    db.update_swap(sid, &SwapPatch::default().deadline_at(deadline_ts));
    logger.log(
        LogLevel::Info,
        &format!("LNPLUS: backfill — imported opening swap {sid} ({capacity} sats)"),
    );
    result.imported.opening += 1;
}

fn backfill_completed(
    entry: &MySwapEntry,
    now: i64,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
    result: &mut BackfillResult,
) {
    let sid = &entry.id;
    if sid.is_empty() {
        return;
    }
    if db.get_swap(sid).is_some() {
        result.skipped.push(sid.clone());
        return;
    }
    let ends_ts = entry.ends.as_ref().and_then(parse_ts);
    let incoming_peer = entry
        .incoming_peer_pubkey
        .as_deref()
        .filter(|p| valid_pubkey(p));
    if entry.incoming_peer_pubkey.is_some() && incoming_peer.is_none() {
        result.warnings.push(format!(
            "swap {sid}: invalid incoming_peer_pubkey on import — incoming_peer left NULL"
        ));
    }

    if let Some(ends_ts) = ends_ts.filter(|t| *t > now) {
        backfill_running_contract(
            sid,
            entry,
            ends_ts,
            incoming_peer,
            now,
            db,
            api,
            chain,
            logger,
            result,
        );
    } else {
        if ends_ts.is_none() {
            result.warnings.push(format!(
                "swap {sid}: completed entry has no parseable 'ends' — importing as ended anyway"
            ));
        }
        backfill_ended_contract(sid, entry, ends_ts, incoming_peer, now, db, logger, result);
    }
}

/// Rule 3 (py 1148-1176): a still-running manual contract is NOT protected
/// by no_close until this import runs.
#[allow(clippy::too_many_arguments)]
fn backfill_running_contract(
    sid: &str,
    entry: &MySwapEntry,
    ends_ts: i64,
    incoming_peer: Option<&str>,
    now: i64,
    db: &dyn LnPlusDb,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
    result: &mut BackfillResult,
) {
    let (outbound_peer, detail) = derive_outbound_for_import(sid, api, chain, logger);
    let capacity = entry
        .capacity_sats
        .or_else(|| detail.as_ref().and_then(|d| d.capacity_sats))
        .unwrap_or_else(|| {
            result.warnings.push(format!(
                "swap {sid}: capacity_sats unknown on import (defaulted to 0)"
            ));
            0
        });
    let duration = entry.duration_months.unwrap_or(0);
    let mut row = crate::db_types::SwapRow::new(sid.to_string(), "opened", capacity, duration, now);
    if let Some(p) = &outbound_peer {
        row = row.with_outbound_peer(p.clone());
    }
    if let Some(p) = incoming_peer {
        row = row.with_incoming_peer(p);
    }
    db.record_swap(&row);
    db.update_swap(sid, &SwapPatch::default().ends_at(ends_ts));
    logger.log(
        LogLevel::Info,
        &format!(
            "LNPLUS: backfill — imported running contract {sid} (outbound_peer={}); phase 4 this pass will activate no_close protection",
            if outbound_peer.is_some() { "set" } else { "NULL" }
        ),
    );
    result.imported.active += 1;
}

/// py `_derive_outbound_for_import` (1177-1232). Fetch `get_swap(sid)`,
/// locate our own participant by pubkey, derive outbound as the next
/// `participant_identifier` cyclically — exact (not an inference), since
/// a completed entry's participant list is final. Never panics: any
/// failure returns `(None, ..)` and the caller imports with
/// `outbound_peer` NULL, logging at ERROR that the operator must protect
/// the channel manually.
fn derive_outbound_for_import(
    sid: &str,
    api: &dyn LnPlusApi,
    chain: &dyn ChainPort,
    logger: &dyn Logger,
) -> (Option<String>, Option<SwapDetail>) {
    // Note: unlike the rest of this module, failures here are ERROR-logged
    // only — matches py (no `warnings.append` calls in `_derive_outbound_
    // for_import`, 1177-1232).
    let detail = match api.get_swap(sid) {
        Ok(d) => d,
        Err(e) => {
            logger.log(
                LogLevel::Error,
                &format!("LNPLUS: backfill — get_swap({sid}) failed while importing a running contract: {e}; importing with outbound_peer NULL — operator must protect this channel manually"),
            );
            return (None, None);
        }
    };
    if detail.participants.is_empty() {
        logger.log(
            LogLevel::Error,
            &format!("LNPLUS: backfill — get_swap({sid}) returned no usable participants while importing a running contract; importing with outbound_peer NULL — operator must protect this channel manually"),
        );
        return (None, Some(detail));
    }
    let our_id = chain.our_node_id().ok();
    let mut by_ident: std::collections::BTreeMap<String, Option<String>> =
        std::collections::BTreeMap::new();
    let mut our_ident: Option<String> = None;
    for p in &detail.participants {
        let Some(ident) = &p.participant_identifier else {
            continue;
        };
        by_ident.insert(ident.clone(), p.pubkey.clone());
        if let (Some(our_id), Some(pk)) = (&our_id, &p.pubkey) {
            if pk == our_id {
                our_ident = Some(ident.clone());
            }
        }
    }
    let Some(our_ident) = our_ident.filter(|_| !by_ident.is_empty()) else {
        logger.log(
            LogLevel::Error,
            &format!("LNPLUS: backfill — could not locate our own pubkey among swap {sid}'s participants; importing with outbound_peer NULL — operator must protect this channel manually"),
        );
        return (None, Some(detail));
    };
    let letters: Vec<&String> = by_ident.keys().collect();
    let idx = letters.iter().position(|l| **l == our_ident).unwrap();
    let outbound_ident = letters[(idx + 1) % letters.len()];
    let outbound_pk = by_ident.get(outbound_ident).cloned().flatten();
    match outbound_pk {
        Some(pk) if valid_pubkey(&pk) => (Some(pk), Some(detail)),
        _ => {
            logger.log(
                LogLevel::Error,
                &format!("LNPLUS: backfill — derived outbound pubkey for swap {sid} is invalid; importing with outbound_peer NULL — operator must protect this channel manually"),
            );
            (None, Some(detail))
        }
    }
}

/// Rule 4 (py 1234-1268): a contract that already ended before this
/// automation existed. Never rate it — the still-open heuristic used by
/// `finalize.rs` is only valid to check AT contract end.
#[allow(clippy::too_many_arguments)]
fn backfill_ended_contract(
    sid: &str,
    entry: &MySwapEntry,
    ends_ts: Option<i64>,
    incoming_peer: Option<&str>,
    now: i64,
    db: &dyn LnPlusDb,
    logger: &dyn Logger,
    result: &mut BackfillResult,
) {
    let capacity = entry.capacity_sats.unwrap_or_else(|| {
        result.warnings.push(format!(
            "swap {sid}: capacity_sats unknown on import (defaulted to 0)"
        ));
        0
    });
    let duration = entry.duration_months.unwrap_or(0);
    let mut row = crate::db_types::SwapRow::new(sid.to_string(), "ended", capacity, duration, now);
    if let Some(p) = incoming_peer {
        row = row.with_incoming_peer(p);
    }
    db.record_swap(&row);
    let mut patch = SwapPatch::default().outcome("imported_pre_automation");
    if let Some(ends_ts) = ends_ts {
        patch = patch.ends_at(ends_ts);
    }
    db.update_swap(sid, &patch);
    logger.log(
        LogLevel::Info,
        &format!("LNPLUS: backfill — imported ended contract {sid} (no rating filed — still-open heuristic is only valid at contract end)"),
    );
    if let Some(peer) = incoming_peer {
        db.bump_peer(peer, false, None::<Rating>);
    }
    result.imported.ended += 1;
}

/// Re-exported so `reconcile.rs` can name the specific error type without
/// importing `error::LnPlusError` redundantly.
pub type BackfillError = LnPlusError;
