//! Task 67 slice 5: the financial-snapshot owner.
//!
//! Ports py `_take_financial_snapshot` (cl-revenue-ops.py:3553-3585) and
//! its loop `financial_snapshot_loop` (:3491-3551): a 300s startup delay,
//! one immediate snapshot, then daily.
//!
//! **Disclosed divergence.** Python opens with
//! `if database is None or profitability_analyzer is None: log(warn);
//! return` — it records no snapshot and reports no failure, so a missing
//! component is indistinguishable from "no snapshot was due". That is the
//! nullable-evidence pattern the Task 8/11 audit flagged, and it is NOT
//! ported: both required sources are `Result`-shaped and a failure is a
//! typed refusal that FAILS the loop pass.
//!
//! Note the arithmetic parity details, both easy to get subtly wrong:
//! `capacity_sats` is `local + remote` COMPUTED (not the reply's
//! `tlv_sats`), and revenue is msat FLOOR-divided by 1000.
//!
//! During the first [`STARTUP_DELAY_SECONDS`] of every boot this loop has
//! honestly not run, which Task 67's `BootStatus::NeverRunThisBoot`
//! reports as its own state rather than as `error`.

use revops_db::analytics::FinancialSnapshotRow;
use serde_json::Value;

/// py `financial_snapshot_loop`'s startup delay.
pub const STARTUP_DELAY_SECONDS: i64 = 300;

/// py `SNAPSHOT_INTERVAL` (daily).
pub const SNAPSHOT_INTERVAL_SECONDS: i64 = 86_400;

/// py `database.get_lifetime_stats()`'s two fields this snapshot uses.
/// Revenue arrives in MSAT (Python's own comment notes the unit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifetimeStats {
    pub total_revenue_msat: i64,
    pub total_rebalance_cost_sats: i64,
}

/// Everything one snapshot consumes, each fallible source `Result`-shaped.
pub struct FinancialDeps {
    /// py `profitability_analyzer.get_tlv()` (REQUIRED).
    pub tlv_raw: Result<Value, String>,
    /// py `database.get_lifetime_stats()` (REQUIRED).
    pub lifetime: Result<LifetimeStats, String>,
    pub now: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FinancialRefusal {
    TlvUnavailable(String),
    LifetimeUnavailable(String),
}

impl FinancialRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::TlvUnavailable(_) => "financial_snapshot_tlv_unavailable",
            Self::LifetimeUnavailable(_) => "financial_snapshot_lifetime_unavailable",
        }
    }
}

/// An ABSENT field is unusable evidence; an explicit zero is a real
/// observation. Collapsing the two would let a broken TLV read record a
/// zero-balance snapshot that looks like a genuinely empty node.
fn required_sats(tlv: &Value, field: &str) -> Result<i64, FinancialRefusal> {
    tlv.get(field).and_then(Value::as_i64).ok_or_else(|| {
        FinancialRefusal::TlvUnavailable(format!(
            "tlv reply is missing the required `{field}` field"
        ))
    })
}

/// Assemble one snapshot row. Pure: it decides, the caller persists.
pub fn plan_financial_snapshot(
    deps: FinancialDeps,
) -> Result<FinancialSnapshotRow, FinancialRefusal> {
    let tlv = deps.tlv_raw.map_err(FinancialRefusal::TlvUnavailable)?;
    let lifetime = deps
        .lifetime
        .map_err(FinancialRefusal::LifetimeUnavailable)?;

    let local_balance_sats = required_sats(&tlv, "local_balance_sats")?;
    let remote_balance_sats = required_sats(&tlv, "remote_balance_sats")?;
    let onchain_sats = required_sats(&tlv, "onchain_sats")?;
    let channel_count = required_sats(&tlv, "channel_count")?;

    Ok(FinancialSnapshotRow {
        taken_at: deps.now,
        local_balance_sats,
        remote_balance_sats,
        onchain_sats,
        // py: capacity_sats=local_bal + remote_bal -- COMPUTED, not the
        // reply's own tlv_sats (which also includes onchain).
        capacity_sats: local_balance_sats + remote_balance_sats,
        // py: revenue_sats = revenue_msat // 1000 (floor).
        revenue_accumulated_sats: lifetime.total_revenue_msat.div_euclid(1000),
        rebalance_cost_accumulated_sats: lifetime.total_rebalance_cost_sats,
        channel_count,
    })
}
