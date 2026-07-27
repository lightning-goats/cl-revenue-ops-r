//! Port of the Boltz swap-budget reservation lifecycle from
//! `CapexBudgetEngine` (py `modules/capex_budget.py` 420-511): `reserve` ->
//! `settle` -> `release`, plus `record_boltz_spend` (py 375-418).
//!
//! # Why this wraps `revops-db`, not a hand-rolled trait
//!
//! Python's methods here are thin orchestration over `self._database`
//! (`reserve_spend` / `record_spend_event` / `release_spend_reservation`).
//! Rather than inventing a parallel trait + fake for "the DB", this module
//! takes `&mut revops_db::budget::BudgetDb` directly — the SAME
//! already-audited unified budget rail every other subsystem (fees,
//! rebalance) reserves against (DD1: a Boltz swap must reserve against the
//! SAME cross-category budget, inside the SAME `BEGIN IMMEDIATE`, so a
//! concurrent rebalance reservation both sees and is counted by the Boltz
//! commitment). This is "inject the IO dependency" done by reusing a
//! vetted component, not by duplicating money-safety logic.
//!
//! [`BudgetDb::open`] never touches a production path (see that module's
//! doc comment) — callers own opening it against a plugin-private DB file
//! or a test tempfile.
//!
//! The pure `channel`/`tactical` cost SPLIT (py `attribute_boltz_cost`,
//! 356-373) has no DB dependency and lives in
//! [`crate::capex::attribute_boltz_cost`] instead.

use revops_db::budget::{BudgetDb, ReserveRequest, SpendEvent};

/// Normalize a channel id the way every Python call site here does:
/// `str(channel_id).replace(':', 'x')`.
fn normalize_channel_id(channel_id: Option<&str>) -> Option<String> {
    channel_id.map(|c| c.replace(':', "x"))
}

/// Port of `CapexBudgetEngine.record_boltz_spend` (py 375-418): record an
/// executed Boltz swap fee in the generic spend ledger. Writes a
/// `category="boltz"` spend event keyed `boltz:{swap_id}` (idempotent —
/// `BudgetDb::record_spend_event` is `INSERT OR REPLACE`, so a repeated
/// journal update for the same swap re-settles rather than double-counting).
/// `now` is the injected clock (Python's `timestamp or time.time()` has no
/// ambient-clock analog here).
///
/// Returns `false` for a blank swap id or non-positive fee (py 397-405),
/// or if the underlying write failed (py 417-418 catches ANY exception).
pub fn record_boltz_spend(
    db: &mut BudgetDb,
    swap_id: &str,
    fee_sats: i64,
    channel_id: Option<&str>,
    source: Option<&str>,
    subcategory: &str,
    now: i64,
) -> bool {
    let sid = swap_id.trim();
    if sid.is_empty() {
        return false;
    }
    if fee_sats <= 0 {
        return false;
    }
    let ev = SpendEvent {
        event_id: format!("boltz:{sid}"),
        category: "boltz".to_string(),
        amount_sats: fee_sats,
        subcategory: Some(if subcategory.is_empty() {
            "swap_fee".to_string()
        } else {
            subcategory.to_string()
        }),
        timestamp: now,
        reference_id: Some(sid.to_string()),
        channel_id: normalize_channel_id(channel_id),
        source: Some(source.unwrap_or("boltz_manager").to_string()),
        metadata: None,
    };
    db.record_spend_event(ev).is_ok()
}

/// Port of `CapexBudgetEngine.reserve_boltz_swap_budget` (py 429-457):
/// reserve a swap's estimated committed cost against the unified budget
/// BEFORE the swap is created. `false` = budget would be exceeded (or the
/// fee is non-positive) — caller must not create the swap. `since_timestamp`
/// mirrors Python's `int(time.time()) - 24 * 3600` using the injected `now`.
pub fn reserve_boltz_swap_budget(
    db: &mut BudgetDb,
    reservation_id: &str,
    estimated_fee_sats: i64,
    channel_id: Option<&str>,
    effective_budget_sats: i64,
    subcategory: &str,
    now: i64,
) -> bool {
    if estimated_fee_sats <= 0 {
        return false;
    }
    let req = ReserveRequest {
        reservation_id: reservation_id.to_string(),
        amount_sats: estimated_fee_sats,
        category: "boltz".to_string(),
        subcategory: Some(if subcategory.is_empty() {
            "swap_fee".to_string()
        } else {
            subcategory.to_string()
        }),
        reference_id: None,
        channel_id: normalize_channel_id(channel_id),
        metadata: None,
        effective_budget_sats: Some(effective_budget_sats),
        since_timestamp: Some(now - 24 * 3600),
        weekly_budget_limit: None,
        weekly_since_timestamp: None,
    };
    matches!(db.reserve_spend(req, now), Ok((true, _)))
}

/// Port of `CapexBudgetEngine.release_boltz_swap_reservation` (py 504-510):
/// swap failed/never created (or superseded by its spend event) — release
/// the reservation so the budget is not held.
pub fn release_boltz_swap_reservation(db: &mut BudgetDb, reservation_id: &str) -> bool {
    matches!(db.release_spend_reservation(reservation_id), Ok(true))
}

/// Port of `CapexBudgetEngine.settle_boltz_swap_reservation` (py 459-502).
///
/// P4-019 (mirrors P2-008's loud-write requirement): the reservation must
/// NOT be released unconditionally. If the spend-event write FAILS,
/// releasing the reservation would leave the created swap with NO
/// reservation AND NO spend event — its committed cost would vanish from
/// the unified budget in the overspend-permitting direction. So the
/// reservation is released ONLY once the event has persisted; on write
/// failure it stays active (the cost keeps being counted) and this returns
/// `false` so the caller logs/retries.
pub fn settle_boltz_swap_reservation(
    db: &mut BudgetDb,
    reservation_id: &str,
    swap_id: &str,
    estimated_fee_sats: i64,
    channel_id: Option<&str>,
    subcategory: &str,
    now: i64,
) -> bool {
    if estimated_fee_sats <= 0 {
        // Nothing to commit — release the (zero-cost) reservation.
        release_boltz_swap_reservation(db, reservation_id);
        return true;
    }
    let recorded = record_boltz_spend(
        db,
        swap_id,
        estimated_fee_sats,
        channel_id,
        Some("boltz_swap_estimate"),
        subcategory,
        now,
    );
    if !recorded {
        // Loud write failed: keep the reservation so the committed cost
        // stays visible to the unified budget. Do NOT release.
        return false;
    }
    release_boltz_swap_reservation(db, reservation_id);
    true
}
