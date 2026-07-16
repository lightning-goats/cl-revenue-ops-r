//! DB-backed read-query functions for Phase 1b's read-RPC subset
//! (`revenue-r-history`, `-report`, `-dashboard`). Each function is a
//! statement-for-statement port of its `database.py`/
//! `profitability_analyzer.py` namesake in `~/bin/cl_revenue_ops-port`
//! (branch `port`) -- see each function's doc comment for the exact source
//! lines. All queries run read-only, through the persistent actor
//! (`crate::actor::DbHandle`), never crossing a task boundary.
//!
//! These are pure DB aggregates -- no live `listfunds`/`listpeerchannels`
//! RPC calls, no `policy_manager`/`fee_controller` state. That is this
//! phase's own scope boundary (see the plan's per-RPC gap table): fields
//! that need something beyond plain SQL are built by the `rpc_*` response
//! builders in `crates/revops`, not here, as an explicit `_phase1b_gaps`
//! entry.

use crate::actor::DbHandle;
use anyhow::Result;
use revops_core::msat::{base_to_sats_ceil, base_to_sats_floor, py_round2};
use rusqlite::types::Value as SqlValue;

/// Port of `Database.get_config_override` (modules/database.py:7316-7322):
/// `SELECT value FROM config_overrides WHERE key = ?`. `key` is the Python
/// `Config` dataclass field name (snake_case, e.g. `min_fee_ppm`), NOT the
/// CLN option suffix (`min-fee-ppm`) -- `config_overrides.key` is written
/// by `Database.set_config_override` keyed exactly the same way
/// `Config.load_overrides` reads it back (`hasattr(self, key)`,
/// modules/config.py:912). Returns `None` when no override row exists for
/// `key` -- the common case, never an error (see
/// [`crate::actor::DbHandle::query_optional_string`]).
pub async fn config_override(handle: &DbHandle, key: &str) -> Result<Option<String>> {
    handle
        .query_optional_string(
            "SELECT value FROM config_overrides WHERE key = ?1",
            vec![SqlValue::Text(key.to_string())],
        )
        .await
}

/// Port of `Database.get_lifetime_stats` (modules/database.py:6018-6087).
///
/// Deliberately EIGHT separate single-column queries, mirroring Python's
/// own non-atomic composition (the Python method itself never wraps these
/// in one transaction either) -- not a combined statement. See
/// [`DbHandle::query_row`] for the contrasting case
/// (`closed_channels_summary`) where Python's source genuinely is one
/// atomic `SELECT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifetimeStats {
    pub total_revenue_msat: i64,
    pub total_rebalance_cost_sats: i64,
    pub total_opening_cost_sats: i64,
    pub total_closure_cost_sats: i64,
    pub total_forwards: i64,
}

/// `now` is the caller's current Unix time (seconds) -- passed in rather
/// than read internally so the "exclude today" boundary-day fix
/// (`today_start = (now // 86400) * 86400`, matching Python's own comment
/// at database.py:6035-6036) is deterministic and unit-testable.
pub async fn lifetime_stats(handle: &DbHandle, now: i64) -> Result<LifetimeStats> {
    let today_start = (now / 86400) * 86400;

    let pruned_revenue_msat = handle
        .query_i64(
            "SELECT COALESCE((SELECT pruned_revenue_msat FROM lifetime_aggregates WHERE id = 1), 0)",
            vec![],
        )
        .await?;
    let pruned_forward_count = handle
        .query_i64(
            "SELECT COALESCE((SELECT pruned_forward_count FROM lifetime_aggregates WHERE id = 1), 0)",
            vec![],
        )
        .await?;
    // Current revenue from forwards table (msat) -- unconditional sum, no
    // date filter (matches Python: this table only retains recent raw rows
    // by construction of the pruning job, not by this query).
    let current_revenue_msat = handle
        .query_i64("SELECT COALESCE(SUM(fee_msat), 0) FROM forwards", vec![])
        .await?;
    // Rolled-up revenue, excluding today to avoid double-counting with
    // `forwards` on the boundary day.
    let rollup_revenue_msat = handle
        .query_i64(
            "SELECT COALESCE(SUM(total_fee_msat), 0) FROM daily_forwarding_stats WHERE date < ?1",
            vec![SqlValue::Integer(today_start)],
        )
        .await?;
    let total_revenue_msat = pruned_revenue_msat + rollup_revenue_msat + current_revenue_msat;

    let total_rebalance_cost_sats = handle
        .query_i64(
            "SELECT COALESCE(SUM(cost_sats), 0) FROM rebalance_costs",
            vec![],
        )
        .await?;
    let total_opening_cost_sats = handle
        .query_i64(
            "SELECT COALESCE(SUM(open_cost_sats), 0) FROM channel_costs",
            vec![],
        )
        .await?;
    let total_closure_cost_sats = handle
        .query_i64(
            "SELECT COALESCE(SUM(total_closure_cost_sats), 0) FROM channel_closure_costs",
            vec![],
        )
        .await?;

    let current_forwards = handle
        .query_i64("SELECT COUNT(*) FROM forwards", vec![])
        .await?;
    let rollup_forwards = handle
        .query_i64(
            "SELECT COALESCE(SUM(forward_count), 0) FROM daily_forwarding_stats WHERE date < ?1",
            vec![SqlValue::Integer(today_start)],
        )
        .await?;
    let total_forwards = pruned_forward_count + rollup_forwards + current_forwards;

    Ok(LifetimeStats {
        total_revenue_msat,
        total_rebalance_cost_sats,
        total_opening_cost_sats,
        total_closure_cost_sats,
        total_forwards,
    })
}

/// Port of `Database.get_closed_channels_summary` (database.py:6495-6526):
/// one 9-column atomic `SELECT`, ported as ONE `query_row` call (not nine
/// `query_i64` calls) to preserve that atomicity under a concurrently
/// written production DB -- see [`DbHandle::query_row`]'s doc comment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClosedChannelsSummary {
    pub channel_count: i64,
    pub total_capacity: i64,
    pub total_open_costs: i64,
    pub total_closure_costs: i64,
    pub total_revenue: i64,
    pub total_rebalance_costs: i64,
    pub total_forwards: i64,
    pub total_net_pnl: i64,
    pub avg_days_open: f64,
}

const CLOSED_CHANNELS_SUMMARY_SQL: &str = "SELECT
        COUNT(*) as channel_count,
        COALESCE(SUM(capacity_sats), 0) as total_capacity,
        COALESCE(SUM(open_cost_sats), 0) as total_open_costs,
        COALESCE(SUM(closure_cost_sats), 0) as total_closure_costs,
        COALESCE(SUM(total_revenue_sats), 0) as total_revenue,
        COALESCE(SUM(total_rebalance_cost_sats), 0) as total_rebalance_costs,
        COALESCE(SUM(forward_count), 0) as total_forwards,
        COALESCE(SUM(net_pnl_sats), 0) as total_net_pnl,
        COALESCE(AVG(days_open), 0) as avg_days_open
    FROM closed_channels";

pub async fn closed_channels_summary(handle: &DbHandle) -> Result<ClosedChannelsSummary> {
    handle
        .query_row(CLOSED_CHANNELS_SUMMARY_SQL, vec![], |r| {
            Ok(ClosedChannelsSummary {
                channel_count: r.get(0)?,
                total_capacity: r.get(1)?,
                total_open_costs: r.get(2)?,
                total_closure_costs: r.get(3)?,
                total_revenue: r.get(4)?,
                total_rebalance_costs: r.get(5)?,
                total_forwards: r.get(6)?,
                total_net_pnl: r.get(7)?,
                avg_days_open: r.get(8)?,
            })
        })
        .await
}

/// Port of `Database.get_closure_costs_since` (database.py:6353-6369).
pub async fn closure_costs_since(handle: &DbHandle, since_timestamp: i64) -> Result<i64> {
    handle
        .query_i64(
            "SELECT COALESCE(SUM(total_closure_cost_sats), 0) FROM channel_closure_costs WHERE closed_at >= ?1",
            vec![SqlValue::Integer(since_timestamp)],
        )
        .await
}

/// Port of `Database.get_total_closure_costs` (database.py:6319-6332).
pub async fn total_closure_costs(handle: &DbHandle) -> Result<i64> {
    handle
        .query_i64(
            "SELECT COALESCE(SUM(total_closure_cost_sats), 0) FROM channel_closure_costs",
            vec![],
        )
        .await
}

/// 24h/7d/30d/total closure-cost windows, port of `revenue-report costs`'s
/// composition in `cl-revenue-ops.py` (`get_closure_costs_since` x3 +
/// `get_total_closure_costs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosureCostWindows {
    pub last_24h_sats: i64,
    pub last_7d_sats: i64,
    pub last_30d_sats: i64,
    pub total_sats: i64,
}

pub async fn closure_costs_windows(handle: &DbHandle, now: i64) -> Result<ClosureCostWindows> {
    let last_24h_sats = closure_costs_since(handle, now - 86400).await?;
    let last_7d_sats = closure_costs_since(handle, now - 7 * 86400).await?;
    let last_30d_sats = closure_costs_since(handle, now - 30 * 86400).await?;
    let total_sats = total_closure_costs(handle).await?;
    Ok(ClosureCostWindows {
        last_24h_sats,
        last_7d_sats,
        last_30d_sats,
        total_sats,
    })
}

/// Port of `Database.get_total_routing_revenue` (database.py:2921-2960).
/// Returns msat (conversion to sats happens at the reporting boundary, same
/// as Python).
pub async fn total_routing_revenue_msat(
    handle: &DbHandle,
    since_timestamp: i64,
    now: i64,
) -> Result<i64> {
    let since_day = (since_timestamp / 86400) * 86400;
    let today_start = (now / 86400) * 86400;
    handle
        .query_i64(
            "SELECT
                (SELECT COALESCE(SUM(fee_msat), 0) FROM forwards WHERE timestamp >= ?1) +
                (SELECT COALESCE(SUM(total_fee_msat), 0) FROM daily_forwarding_stats WHERE date >= ?2 AND date < ?3)",
            vec![
                SqlValue::Integer(since_timestamp),
                SqlValue::Integer(since_day),
                SqlValue::Integer(today_start),
            ],
        )
        .await
}

/// Port of `Database.get_total_volume_since` (database.py:5601-5626).
/// Converts msat -> sats via floor (never overstate spendable volume),
/// matching Python's own `base_to_sats_floor` call at the DB layer.
pub async fn total_volume_sats_since(
    handle: &DbHandle,
    since_timestamp: i64,
    now: i64,
) -> Result<i64> {
    let since_day = (since_timestamp / 86400) * 86400;
    let today_start = (now / 86400) * 86400;
    let total_volume_msat = handle
        .query_i64(
            "SELECT
                (SELECT COALESCE(SUM(out_msat), 0) FROM forwards WHERE timestamp >= ?1) +
                (SELECT COALESCE(SUM(total_out_msat), 0) FROM daily_forwarding_stats WHERE date >= ?2 AND date < ?3)",
            vec![
                SqlValue::Integer(since_timestamp),
                SqlValue::Integer(since_day),
                SqlValue::Integer(today_start),
            ],
        )
        .await?;
    Ok(base_to_sats_floor(total_volume_msat.max(0) as u64) as i64)
}

/// Port of `Database.get_total_forward_count_since` (database.py:5652-5671).
pub async fn total_forward_count_since(
    handle: &DbHandle,
    since_timestamp: i64,
    now: i64,
) -> Result<i64> {
    let since_day = (since_timestamp / 86400) * 86400;
    let today_start = (now / 86400) * 86400;
    handle
        .query_i64(
            "SELECT
                (SELECT COUNT(*) FROM forwards WHERE timestamp >= ?1) +
                (SELECT COALESCE(SUM(forward_count), 0) FROM daily_forwarding_stats WHERE date >= ?2 AND date < ?3)",
            vec![
                SqlValue::Integer(since_timestamp),
                SqlValue::Integer(since_day),
                SqlValue::Integer(today_start),
            ],
        )
        .await
}

/// Port of `Database.get_total_rebalance_fees` (database.py:2814-2839).
/// Schema here already carries `rebalance_costs.cost_msat` (see
/// `fixtures/schema.sql`), so Python's `sqlite3.OperationalError` legacy
/// fallback (for a pre-migration schema without that column) has no
/// equivalent path in this port -- there's nothing to fall back from.
/// Converts msat -> sats via ceil, matching Python's own
/// `base_to_sats_ceil` call at the DB layer.
pub async fn total_rebalance_fees_since(handle: &DbHandle, since_timestamp: i64) -> Result<i64> {
    let total_fees_msat = handle
        .query_i64(
            "SELECT COALESCE(SUM(COALESCE(cost_msat, cost_sats * 1000)), 0) FROM rebalance_costs WHERE timestamp >= ?1",
            vec![SqlValue::Integer(since_timestamp)],
        )
        .await?;
    Ok(base_to_sats_ceil(total_fees_msat.max(0) as u64) as i64)
}

/// Port of `ProfitabilityAnalyzer.get_pnl_summary`
/// (profitability_analyzer.py:1441-1498), composed entirely from the
/// `database.py`-ported functions above -- no live RPC, no
/// `policy_manager`. `window_days` is clamped to a minimum of 1 here
/// (Python's own internal clamp: "BUG FIX: Validate window_days..."); the
/// separate upper clamp (`min(window_days, 365)`) lives in the
/// `revenue-dashboard` RPC handler itself in Python (cl-revenue-ops.py),
/// not in `get_pnl_summary` -- ported the same way, in the RPC-layer
/// `parse_window_days` (`crates/revops/src/rpc_dashboard.rs`), not here.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PnlSummary {
    pub window_days: i64,
    pub gross_revenue_sats: i64,
    pub opex_sats: i64,
    pub rebalance_cost_sats: i64,
    pub closure_cost_sats: i64,
    pub net_profit_sats: i64,
    pub operating_margin_pct: f64,
    pub volume_sats: i64,
    pub forward_count: i64,
}

pub async fn pnl_summary(handle: &DbHandle, window_days_in: i64, now: i64) -> Result<PnlSummary> {
    let window_days = window_days_in.max(1);
    let since_timestamp = now - window_days * 86400;

    let gross_revenue_msat = total_routing_revenue_msat(handle, since_timestamp, now).await?;
    let gross_revenue_sats = if gross_revenue_msat > 0 {
        base_to_sats_ceil(gross_revenue_msat as u64) as i64
    } else {
        0
    };
    let volume_sats = total_volume_sats_since(handle, since_timestamp, now).await?;
    let forward_count = total_forward_count_since(handle, since_timestamp, now).await?;
    let rebalance_cost_sats = total_rebalance_fees_since(handle, since_timestamp).await?;
    let closure_cost_sats = closure_costs_since(handle, since_timestamp).await?;
    let opex_sats = rebalance_cost_sats + closure_cost_sats;
    let net_profit_sats = gross_revenue_sats - opex_sats;
    let operating_margin_pct = if gross_revenue_sats > 0 {
        py_round2((net_profit_sats as f64 / gross_revenue_sats as f64) * 100.0)
    } else if opex_sats == 0 {
        0.0
    } else {
        -100.0
    };

    Ok(PnlSummary {
        window_days,
        gross_revenue_sats,
        opex_sats,
        rebalance_cost_sats,
        closure_cost_sats,
        net_profit_sats,
        operating_margin_pct,
        volume_sats,
        forward_count,
    })
}
