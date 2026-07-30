//! F71-R23 slice 2: the production-store history one channel's
//! profitability verdict depends on.
//!
//! Two facts live in Python's production database and are read-only here:
//!
//! - `last_routed` -- py `get_last_forward_time_any_direction`
//!   (database.py:3453): MAX over `forwards` in BOTH roles, unioned with
//!   BOTH daily rollups that survive retention pruning. There are two
//!   rollup tables, not one: `daily_forwarding_stats` is exit-side and
//!   `daily_forwarding_stats_inbound` is entry-side (C71-20).
//! - the diagnostic-rebalance stats -- py
//!   `get_diagnostic_rebalance_stats` (database.py:2787).
//!
//! **Why one command.** These are two halves of one channel's verdict,
//! and the production database is concurrently written by the Python
//! plugin under WAL. Two independent round trips could each land on a
//! different WAL snapshot and yield a combination Python's own reads could
//! never have produced -- the C71-14 shape, and the same reasoning
//! `DbHandle::query_row` already documents for multi-column aggregates.
//! One `unchecked_transaction` gives both SELECTs a single snapshot.
//!
//! **Why the production DB and not a Rust store.** The Rust rebalance
//! owner's `rust_rebalance_attempts` constrains `trigger` to
//! `('cycle','manual','manual_force')` -- it records no diagnostic
//! rebalances at all, because the diagnostic path is Python's. Python is
//! still the actor here, so its history is the authority. This is a
//! deliberate cross-store routing decision, not a fallback.

use crate::queries::{PerChannelCosts, PerChannelRevenue};
use anyhow::{Context, Result};
use revops_analytics::profitability::DiagStats;
use rusqlite::Connection;
use std::collections::HashMap;

/// One channel's production-side history, read as a single observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelHistory {
    /// Most recent routing activity in EITHER direction, or `None` when
    /// the channel genuinely has no history. `None` here is
    /// consulted-and-empty; a failed read is an `Err` on the outer
    /// `Result` and never reaches this field.
    pub last_routed: Option<i64>,
    /// Diagnostic-rebalance attempts in the trailing window. Zero
    /// attempts is a real answer; an unreadable history is an `Err`.
    pub diag: DiagStats,
}

/// py `_scid_aliases`: the same short-channel-id spelled both ways. A row
/// written in the other spelling must not read as no history, because "no
/// history" is the dead-capital verdict.
fn scid_aliases(scid: &str) -> [String; 2] {
    [scid.replace(':', "x"), scid.replace('x', ":")]
}

/// Read both halves of one channel's production history under a single
/// snapshot. Runs inside the read actor's own turn (see
/// [`crate::actor::DbHandle::channel_history`]) -- callers never receive a
/// connection.
pub(crate) fn read_channel_history(
    conn: &Connection,
    scid: &str,
    now: i64,
    diag_days: i64,
) -> Result<ChannelHistory> {
    let [x_form, colon_form] = scid_aliases(scid);
    let diag_since = now - diag_days * 86_400;

    // One snapshot for both SELECTs.
    let tx = conn
        .unchecked_transaction()
        .context("open profitability history read transaction")?;

    // py unions the raw forwards (both roles) with BOTH daily rollups,
    // approximating each rollup's time-of-day as midnight + 86399
    // (database.py:3468-3483).
    //
    // C71-20: `daily_forwarding_stats` is EXIT-side only. The inbound
    // rollup is a separate table, and omitting it makes a channel whose
    // retained history is inbound-only read as never-routed -- the
    // dead-capital verdict on exactly the channels the rollup preserves.
    // The alias pair must reach all three arms for the same reason.
    let last_routed: Option<i64> = tx
        .query_row(
            "SELECT MAX(ts) FROM (
                 SELECT MAX(timestamp) AS ts FROM forwards
                  WHERE out_channel IN (?1, ?2) OR in_channel IN (?1, ?2)
                 UNION ALL
                 SELECT MAX(date) + 86399 AS ts FROM daily_forwarding_stats
                  WHERE channel_id IN (?1, ?2) AND forward_count > 0
                 UNION ALL
                 SELECT MAX(date) + 86399 AS ts FROM daily_forwarding_stats_inbound
                  WHERE channel_id IN (?1, ?2) AND forward_count > 0
             )",
            rusqlite::params![&x_form, &colon_form],
            |row| row.get::<_, Option<i64>>(0),
        )
        .context("read last routing time")?;

    let (attempt_count, last_success_time) = tx
        .query_row(
            "SELECT COUNT(*),
                    COALESCE(MAX(CASE WHEN status = 'success' THEN timestamp ELSE 0 END), 0)
               FROM rebalance_history
              WHERE to_channel IN (?1, ?2)
                AND rebalance_type = 'diagnostic'
                AND timestamp >= ?3",
            rusqlite::params![&x_form, &colon_form, diag_since],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .context("read diagnostic rebalance stats")?;

    tx.finish().context("finish profitability history read")?;

    Ok(ChannelHistory {
        // A stored `0` timestamp means never routed, same as no row --
        // py's `if last_routed:` truthiness treats them identically, and
        // the frozen classifier ports that trap.
        last_routed: last_routed.filter(|&ts| ts > 0),
        diag: DiagStats {
            attempt_count,
            last_success_time,
        },
    })
}

// ---------------------------------------------------------------------
// C71-21: the whole fleet verdict, as one observation.
// ---------------------------------------------------------------------

/// Every production-database input the profitability classifier consumes,
/// for the whole fleet, read under a single snapshot.
///
/// Revenue, costs and history are not independent facts that happen to be
/// fetched together -- they are the numerator, the denominator and the
/// liveness gate of ONE verdict. Fetching them in separate actor turns
/// lets a Python write commit in between, and the assembled verdict then
/// describes a node state that existed at no instant: a channel with one
/// forward all-time whose last routing time was created by the second.
/// Both halves are individually true, which is exactly what makes the pair
/// undetectable downstream.
#[derive(Debug, Clone, Default)]
pub struct ProfitabilitySnapshot {
    /// All-time per-channel revenue (`since = 0`).
    pub revenue_all_time: HashMap<String, PerChannelRevenue>,
    /// Revenue over the trailing window, for marginal ROI.
    pub revenue_30d: HashMap<String, PerChannelRevenue>,
    /// Open and rebalance costs, all-time and windowed.
    pub costs: HashMap<String, PerChannelCosts>,
    /// Liveness: last routing time and diagnostic-rebalance stats.
    pub history: HashMap<String, ChannelHistory>,
}

/// Every key in a [`ProfitabilitySnapshot`] is the `x` spelling.
///
/// The per-channel reads take the alias pair as bind parameters; a
/// fleet-wide `GROUP BY` cannot, so the spellings are folded here instead.
/// Without this a `:`-spelled row becomes a *separate channel* in the
/// fleet pass -- one entry with revenue and no history (dead capital) and
/// one with history and no revenue.
fn normalize_scid(scid: &str) -> String {
    scid.replace(':', "x")
}

fn read_revenue(
    tx: &rusqlite::Transaction<'_>,
    since: i64,
) -> Result<HashMap<String, PerChannelRevenue>> {
    let mut out: HashMap<String, PerChannelRevenue> = HashMap::new();

    // EXIT side: this channel earned the fee.
    {
        let mut stmt = tx
            .prepare(
                "SELECT out_channel, COALESCE(SUM(fee_msat),0),
                        COALESCE(SUM(out_msat),0), COUNT(*)
                   FROM forwards
                  WHERE out_channel IS NOT NULL AND timestamp >= ?1
                  GROUP BY out_channel",
            )
            .context("prepare snapshot earned revenue")?;
        let rows = stmt
            .query_map([since], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .context("run snapshot earned revenue")?;
        for row in rows {
            let (scid, fees, volume, count) = row.context("decode snapshot earned revenue")?;
            let e = out.entry(normalize_scid(&scid)).or_default();
            // `+=`, not `=`: two spellings of one channel are one channel,
            // and their forwards are disjoint events.
            e.fees_earned_msat += fees;
            e.volume_routed_msat += volume;
            e.forward_count += count;
        }
    }

    // ENTRY side: attribution only, NEVER summed into fleet revenue.
    {
        let mut stmt = tx
            .prepare(
                "SELECT in_channel, COALESCE(SUM(fee_msat),0),
                        COALESCE(SUM(in_msat),0), COUNT(*)
                   FROM forwards
                  WHERE in_channel IS NOT NULL AND timestamp >= ?1
                  GROUP BY in_channel",
            )
            .context("prepare snapshot sourced revenue")?;
        let rows = stmt
            .query_map([since], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .context("run snapshot sourced revenue")?;
        for row in rows {
            let (scid, fees, volume, count) = row.context("decode snapshot sourced revenue")?;
            let e = out.entry(normalize_scid(&scid)).or_default();
            e.sourced_fee_contribution_msat += fees;
            e.sourced_volume_msat += volume;
            e.sourced_forward_count += count;
        }
    }

    Ok(out)
}

fn read_costs(
    tx: &rusqlite::Transaction<'_>,
    window_since: i64,
) -> Result<HashMap<String, PerChannelCosts>> {
    let mut out: HashMap<String, PerChannelCosts> = HashMap::new();

    {
        let mut stmt = tx
            .prepare(
                "SELECT channel_id, COALESCE(peer_id,''), COALESCE(open_cost_sats,0),
                        COALESCE(capacity_sats,0), COALESCE(opened_at,0)
                   FROM channel_costs
                  WHERE channel_id IS NOT NULL",
            )
            .context("prepare snapshot open costs")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .context("run snapshot open costs")?;
        for row in rows {
            let (scid, peer_id, open_cost, capacity, opened_at) =
                row.context("decode snapshot open costs")?;
            let e = out.entry(normalize_scid(&scid)).or_default();
            if e.peer_id.is_empty() {
                e.peer_id = peer_id;
            }
            // `max`, not `+=`: an open cost and a capacity describe the
            // channel itself, so two spellings are two descriptions of one
            // fact, not two facts. Summing would double the denominator
            // and manufacture an underwater verdict. `max` is also
            // order-independent, unlike last-write-wins over a HashMap.
            e.open_cost_sats = e.open_cost_sats.max(open_cost);
            e.capacity_sats = e.capacity_sats.max(capacity);
            e.opened_at = e.opened_at.max(opened_at);
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "SELECT channel_id, COALESCE(peer_id,''), COALESCE(SUM(cost_sats),0),
                        COALESCE(SUM(CASE WHEN timestamp >= ?1 THEN cost_sats ELSE 0 END),0)
                   FROM rebalance_costs
                  WHERE channel_id IS NOT NULL
                  GROUP BY channel_id",
            )
            .context("prepare snapshot rebalance costs")?;
        let rows = stmt
            .query_map([window_since], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .context("run snapshot rebalance costs")?;
        for row in rows {
            let (scid, peer_id, total, windowed) =
                row.context("decode snapshot rebalance costs")?;
            let e = out.entry(normalize_scid(&scid)).or_default();
            if e.peer_id.is_empty() {
                e.peer_id = peer_id;
            }
            // `+=` here: rebalance costs ARE disjoint spend events.
            e.rebalance_cost_sats += total;
            e.rebalance_cost_30d_sats += windowed;
        }
    }

    Ok(out)
}

/// The empty history: consulted, nothing found. `DiagStats` lives in the
/// frozen analytics kernel and derives no `Default`, so the zeros are
/// spelled out here rather than added there -- and spelling them out is
/// the honest form anyway, since `0`/`None` are Python's own answers for a
/// channel with no rows, not placeholders.
const NO_HISTORY: ChannelHistory = ChannelHistory {
    last_routed: None,
    diag: DiagStats {
        attempt_count: 0,
        last_success_time: 0,
    },
};

fn read_history(
    tx: &rusqlite::Transaction<'_>,
    diag_since: i64,
) -> Result<HashMap<String, ChannelHistory>> {
    // The same three sources `read_channel_history` unions, but grouped
    // per channel rather than filtered to one (C71-20: BOTH rollups).
    const LAST_ROUTED_SOURCES: [&str; 4] = [
        "SELECT out_channel, MAX(timestamp) FROM forwards
          WHERE out_channel IS NOT NULL GROUP BY out_channel",
        "SELECT in_channel, MAX(timestamp) FROM forwards
          WHERE in_channel IS NOT NULL GROUP BY in_channel",
        "SELECT channel_id, MAX(date) + 86399 FROM daily_forwarding_stats
          WHERE channel_id IS NOT NULL AND forward_count > 0 GROUP BY channel_id",
        "SELECT channel_id, MAX(date) + 86399 FROM daily_forwarding_stats_inbound
          WHERE channel_id IS NOT NULL AND forward_count > 0 GROUP BY channel_id",
    ];

    let mut out: HashMap<String, ChannelHistory> = HashMap::new();

    for sql in LAST_ROUTED_SOURCES {
        let mut stmt = tx.prepare(sql).context("prepare snapshot last routed")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
            })
            .context("run snapshot last routed")?;
        for row in rows {
            let (scid, ts) = row.context("decode snapshot last routed")?;
            // A stored `0` is never-routed, same as no row -- py's
            // `if last_routed:` truthiness treats them identically.
            let Some(ts) = ts.filter(|&ts| ts > 0) else {
                continue;
            };
            let e = out.entry(normalize_scid(&scid)).or_insert(NO_HISTORY);
            e.last_routed = Some(e.last_routed.unwrap_or(0).max(ts));
        }
    }

    {
        let mut stmt = tx
            .prepare(
                "SELECT to_channel, COUNT(*),
                        COALESCE(MAX(CASE WHEN status = 'success' THEN timestamp ELSE 0 END), 0)
                   FROM rebalance_history
                  WHERE to_channel IS NOT NULL
                    AND rebalance_type = 'diagnostic'
                    AND timestamp >= ?1
                  GROUP BY to_channel",
            )
            .context("prepare snapshot diagnostic stats")?;
        let rows = stmt
            .query_map([diag_since], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .context("run snapshot diagnostic stats")?;
        for row in rows {
            let (scid, attempts, last_success) = row.context("decode snapshot diagnostic stats")?;
            let e = out.entry(normalize_scid(&scid)).or_insert(NO_HISTORY);
            e.diag.attempt_count += attempts;
            e.diag.last_success_time = e.diag.last_success_time.max(last_success);
        }
    }

    Ok(out)
}

/// Read every production-side profitability input for the whole fleet
/// under one snapshot. Runs inside the read actor's own turn -- callers
/// never receive a connection.
pub(crate) fn read_profitability_snapshot(
    conn: &Connection,
    now: i64,
    window_days: i64,
    diag_days: i64,
) -> Result<ProfitabilitySnapshot> {
    let window_since = now - window_days * 86_400;
    let diag_since = now - diag_days * 86_400;

    // One snapshot for every SELECT below. This transaction is the entire
    // point of the command: it is what makes the returned figures a
    // description of one instant rather than a plausible-looking splice.
    let tx = conn
        .unchecked_transaction()
        .context("open profitability snapshot read transaction")?;

    let revenue_all_time = read_revenue(&tx, 0).context("snapshot: all-time revenue")?;
    let revenue_30d = read_revenue(&tx, window_since).context("snapshot: windowed revenue")?;
    let costs = read_costs(&tx, window_since).context("snapshot: costs")?;
    let history = read_history(&tx, diag_since).context("snapshot: history")?;

    tx.finish().context("finish profitability snapshot read")?;

    Ok(ProfitabilitySnapshot {
        revenue_all_time,
        revenue_30d,
        costs,
        history,
    })
}

/// Typed handle-level read. Thin by design: the SQL and the transaction
/// live above, and this is only the actor round trip, so the whole
/// observation is one `.await`.
pub async fn channel_history(
    handle: &crate::actor::DbHandle,
    scid: &str,
    now: i64,
    diag_days: i64,
) -> Result<ChannelHistory> {
    handle.channel_history(scid, now, diag_days).await
}

/// Fleet-wide production inputs as ONE observation. Thin for the same
/// reason [`channel_history`] is: one actor turn, one transaction, one
/// `.await`.
pub async fn profitability_snapshot(
    handle: &crate::actor::DbHandle,
    now: i64,
    window_days: i64,
    diag_days: i64,
) -> Result<ProfitabilitySnapshot> {
    handle
        .profitability_snapshot(now, window_days, diag_days)
        .await
}
