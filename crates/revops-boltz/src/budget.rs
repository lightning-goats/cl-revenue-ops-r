//! Budget gates: cost aggregation, unified-budget status, and the
//! atomic pre-create reservation kernel.
//!
//! Ports py `get_boltz_cost_components` (boltz_manager.py:1102-1181),
//! `get_budget_status` (boltz_manager.py:1540-1579),
//! `_enforce_budget_for_quote` (boltz_manager.py:1581-1602), and the
//! decision core of `_open_swap_budget_reservation`/
//! `_finalize_swap_budget_reservation` (boltz_manager.py:1743-1843), minus
//! the actual capex-engine RPC/DB calls (`reserve_boltz_swap_budget`,
//! `release_boltz_swap_reservation`, `settle_boltz_swap_reservation`) —
//! those are injected effects the live adapter performs; see
//! `ENTRYPOINTS.md`.
//!
//! CRITICAL (per `docs/port/port-map.json` `rust_considerations`):
//! `get_boltz_cost_components`-equivalent logic must NEVER itself call a
//! global/unified budget provider — the unified budget status aggregates
//! Boltz costs through this exact computation, so calling the provider
//! from inside it would mutually recurse. This module enforces that by
//! construction: [`boltz_cost_components`] takes `global_budget_cap_sats`
//! as a plain `Option<i64>` parameter, never a callback.

use crate::fee::estimate_swap_fee_sats;
use crate::parsing::parse_timestamp;
use crate::state::{is_completed_swap, is_terminal_swap};
use serde_json::{Map, Value};

/// py `check_channel_capex_budget`/`_open_swap_budget_reservation`'s
/// `subcategory` (`"swap_fee"` vs `"structural"`, boltz_manager.py:1777).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subcategory {
    SwapFee,
    Structural,
}

impl Subcategory {
    pub fn as_str(self) -> &'static str {
        match self {
            Subcategory::SwapFee => "swap_fee",
            Subcategory::Structural => "structural",
        }
    }
}

// ---------------------------------------------------------------------
// Cost aggregation (py get_boltz_cost_components, boltz_manager.py:1102-1181)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CostComponents {
    pub spent_24h_sats: i64,
    pub reserved_24h_sats: i64,
    pub reserved_swaps: usize,
    pub counted_swaps: usize,
    pub skipped_without_timestamp: usize,
}

/// A swap's completion timestamp for budget windowing (py
/// `_swap_completed_ts`, boltz_manager.py:1017-1026): prefer
/// `updatedAt`/`updated_at`, then `createdAt`/`created_at`.
fn swap_completed_ts(swap: &Map<String, Value>) -> Option<i64> {
    for key in ["updatedAt", "updated_at", "createdAt", "created_at"] {
        if let Some(v) = swap.get(key) {
            if let Some(ts) = parse_timestamp(v) {
                return Some(ts);
            }
        }
    }
    None
}

/// py `_swap_created_ts` (boltz_manager.py:1010-1015): `createdAt`/
/// `created_at` preferred over `updatedAt`/`updated_at` (opposite key
/// order from `swap_completed_ts` — this one is used for the *reserved*
/// (pending) side, where creation time is the relevant clock).
fn swap_created_ts(swap: &Map<String, Value>) -> Option<i64> {
    for key in ["createdAt", "updatedAt", "created_at", "updated_at"] {
        if let Some(v) = swap.get(key) {
            if let Some(ts) = parse_timestamp(v) {
                return Some(ts);
            }
        }
    }
    None
}

/// py `get_boltz_cost_components` (boltz_manager.py:1102-1181). `swaps`
/// must already be the manual-only, journal-augmented swap list (the
/// `_listswaps_json`/`_augment_with_swap_journal` calls are I/O and stay
/// in the live adapter).
pub fn boltz_cost_components(
    swaps: &[Map<String, Value>],
    now: i64,
    window_hours: i64,
    boltz_daily_budget_sats: i64,
    global_budget_cap_sats: Option<i64>,
) -> CostComponents {
    let window_hours = window_hours.clamp(1, 168);
    let cutoff = now - (window_hours * 3600);

    let mut boltz_spent = 0i64;
    let mut counted = 0usize;
    let mut unknown_ts = 0usize;
    for s in swaps {
        let ts = match swap_completed_ts(s) {
            Some(t) => t,
            None => {
                unknown_ts += 1;
                continue;
            }
        };
        if ts < cutoff {
            continue;
        }
        if !is_completed_swap(s) {
            continue;
        }
        let fee = estimate_swap_fee_sats(&Value::Object(s.clone())).max(0);
        boltz_spent += fee;
        counted += 1;
    }

    // C2 FIX (py comment, boltz_manager.py:1145): count pending
    // (non-terminal) swaps as reserved budget.
    let mut reserved = 0i64;
    let mut reserved_count = 0usize;
    for s in swaps {
        if is_terminal_swap(s) {
            continue;
        }
        match swap_created_ts(s) {
            Some(t) if t >= cutoff => {}
            _ => continue,
        }
        let fee_est = estimate_swap_fee_sats(&Value::Object(s.clone()));
        if fee_est > 0 {
            reserved += fee_est;
            reserved_count += 1;
        }
    }

    // Cap reserved at remaining budget so an over-estimating fee fallback
    // cannot block the unified capital control. Use the tighter of
    // Boltz-specific and global (unified) budget.
    let boltz_budget = boltz_daily_budget_sats.max(0);
    let mut cap_budget = boltz_budget;
    if let Some(global_budget) = global_budget_cap_sats {
        let global_budget = global_budget.max(0);
        if global_budget > 0 {
            cap_budget = if cap_budget > 0 {
                cap_budget.min(global_budget)
            } else {
                global_budget
            };
        }
    }
    if cap_budget > 0 {
        let max_reservable = (cap_budget - boltz_spent).max(0);
        reserved = reserved.min(max_reservable);
    }

    CostComponents {
        spent_24h_sats: boltz_spent,
        reserved_24h_sats: reserved,
        reserved_swaps: reserved_count,
        counted_swaps: counted,
        skipped_without_timestamp: unknown_ts,
    }
}

// ---------------------------------------------------------------------
// Unified budget status (py get_budget_status, boltz_manager.py:1540-1579)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExternalLiquidityCosts {
    pub spent_24h_sats: i64,
    pub reserved_24h_sats: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BudgetStatus {
    pub daily_budget_sats: i64,
    pub spent_24h_sats_estimate: i64,
    pub remaining_24h_sats_estimate: i64,
    pub reserved_24h_sats_estimate: i64,
    pub boltz_spent_24h_sats_estimate: i64,
    pub boltz_remaining_24h_sats_estimate: i64,
    pub boltz_reserved_24h_sats_estimate: i64,
}

/// py `get_budget_status` (boltz_manager.py:1540-1579), given the already
/// -computed `local` Boltz-only components and `external` liquidity costs.
pub fn budget_status(
    budget_sats: i64,
    local: &CostComponents,
    external: &ExternalLiquidityCosts,
) -> BudgetStatus {
    let budget = budget_sats.max(0);
    let boltz_spent = local.spent_24h_sats.max(0);
    let local_reserved = local.reserved_24h_sats.max(0);
    let external_spent = external.spent_24h_sats.max(0);
    let external_reserved = external.reserved_24h_sats.max(0);
    let total_spent = boltz_spent + external_spent;
    let total_reserved = local_reserved + external_reserved;

    let boltz_remaining = (budget - boltz_spent - local_reserved).max(0);
    let remaining = (budget - total_spent - total_reserved).max(0);

    BudgetStatus {
        daily_budget_sats: budget,
        spent_24h_sats_estimate: total_spent,
        remaining_24h_sats_estimate: remaining,
        reserved_24h_sats_estimate: total_reserved,
        boltz_spent_24h_sats_estimate: boltz_spent,
        boltz_remaining_24h_sats_estimate: boltz_remaining,
        boltz_reserved_24h_sats_estimate: local_reserved,
    }
}

// ---------------------------------------------------------------------
// Quote-time advisory gate (py _enforce_budget_for_quote, boltz_manager.py:1581-1602)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetEnforcement {
    pub allowed: bool,
    pub estimated_fee_sats: i64,
    pub reason: Option<String>,
}

/// `quote_fee_sats`: `_estimate_swap_fee_sats(quote)`. `extra_fee_sats`
/// (E-4.4): plan-time components not in the raw boltzcli quote payload
/// (the reverse-swap LN routing estimate).
pub fn enforce_budget_for_quote(
    quote_fee_sats: i64,
    extra_fee_sats: i64,
    enforce_budget: bool,
    remaining_24h_sats_estimate: i64,
) -> BudgetEnforcement {
    let fee_sats = quote_fee_sats + extra_fee_sats.max(0);
    if enforce_budget && fee_sats > remaining_24h_sats_estimate {
        return BudgetEnforcement {
            allowed: false,
            estimated_fee_sats: fee_sats,
            reason: Some(format!(
                "Estimated swap fee {fee_sats} sats exceeds remaining Boltz daily budget {remaining_24h_sats_estimate} sats"
            )),
        };
    }
    BudgetEnforcement {
        allowed: true,
        estimated_fee_sats: fee_sats,
        reason: None,
    }
}

// ---------------------------------------------------------------------
// Atomic pre-create reservation (py _open_swap_budget_reservation /
// _finalize_swap_budget_reservation, boltz_manager.py:1743-1843)
// ---------------------------------------------------------------------

/// Whether — and with what parameters — a pre-create reservation attempt
/// should even be made. Mirrors the early-return ladder of py
/// `_open_swap_budget_reservation` up to (not including) the actual
/// `reserve_boltz_swap_budget` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservationGate {
    /// No reservation applies — legacy no-op, behaviour unchanged. Covers:
    /// no capex engine, `enforce_budget` off, fee <= 0, or (fail-open) an
    /// unreadable/absent unified budget.
    NotApplicable,
    /// A reservation attempt should be made with this fee and budget.
    Attempt {
        fee_sats: i64,
        effective_budget_sats: i64,
    },
}

/// py `_open_swap_budget_reservation` (boltz_manager.py:1759-1776) up to
/// the reservation call. `effective_budget_sats` is `None` when the
/// unified budget provider raised/was unreadable (py's bare `except:
/// return None`) OR when no unified budget is wired (`budget_sats <= 0`)
/// — both fail open identically in Python, so both collapse to
/// `NotApplicable` here.
pub fn reservation_gate(
    capex_engine_present: bool,
    enforce_budget: bool,
    estimated_fee_sats: i64,
    effective_budget_sats: Option<i64>,
) -> ReservationGate {
    if !capex_engine_present || !enforce_budget {
        return ReservationGate::NotApplicable;
    }
    let fee = estimated_fee_sats.max(0);
    if fee <= 0 {
        return ReservationGate::NotApplicable;
    }
    match effective_budget_sats {
        Some(b) if b > 0 => ReservationGate::Attempt {
            fee_sats: fee,
            effective_budget_sats: b,
        },
        _ => ReservationGate::NotApplicable,
    }
}

/// Outcome of an attempted reservation. Three-valued per the Python
/// contract (boltz_manager.py:1750-1757): a reservation id when placed,
/// "not applicable" when enforcement does not apply, or "rejected" when
/// the unified budget WOULD be exceeded — the caller must reject the swap
/// without creating it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservationOutcome {
    Reserved(String),
    NotApplicable,
    Rejected,
}

/// py `_open_swap_budget_reservation`'s tail (boltz_manager.py:1786-1800):
/// given the [`ReservationGate::Attempt`] fired and the underlying
/// `reserve_boltz_swap_budget(...)` call was made, fold its outcome.
/// `engine_result`: `Some(true)` = reserved, `Some(false)` = budget would
/// be exceeded, `None` = the call raised (infra error) — Python's bare
/// `except Exception: return None` (fail OPEN on infra error; this is a
/// deliberate legacy behaviour, NOT the same direction as the fail-CLOSED
/// capex-lookup gates elsewhere in this subsystem).
pub fn finalize_reservation_attempt(
    reservation_id: &str,
    engine_result: Option<bool>,
) -> ReservationOutcome {
    match engine_result {
        Some(true) => ReservationOutcome::Reserved(reservation_id.to_string()),
        Some(false) => ReservationOutcome::Rejected,
        None => ReservationOutcome::NotApplicable,
    }
}

/// py `_finalize_swap_budget_reservation` (boltz_manager.py:1802-1843):
/// settle (swap created) or release (swap failed/errored) the pre-create
/// reservation on every exit. `created` mirrors py's `created = bool(sid)
/// and not self._is_error_swap(primary)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeAction {
    /// Settle: `settle_boltz_swap_reservation(reservation_id, swap_id, ...)`.
    Settle,
    /// Release: `release_boltz_swap_reservation(reservation_id)`.
    Release,
}

pub fn finalize_action(primary_swap_has_id: bool, primary_swap_is_error: bool) -> FinalizeAction {
    if primary_swap_has_id && !primary_swap_is_error {
        FinalizeAction::Settle
    } else {
        FinalizeAction::Release
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn swap(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    // --- boltz_cost_components ---

    #[test]
    fn completed_swap_within_window_counts_as_spent() {
        let now = 1_700_000_000i64;
        let swaps = vec![swap(json!({
            "id": "a", "state": "swap.completed",
            "updatedAt": now - 3600, "boltzFee": 50,
        }))];
        let c = boltz_cost_components(&swaps, now, 24, 1_000_000, None);
        assert_eq!(c.spent_24h_sats, 50);
        assert_eq!(c.counted_swaps, 1);
    }

    #[test]
    fn completed_swap_outside_window_not_counted() {
        let now = 1_700_000_000i64;
        let swaps = vec![swap(json!({
            "id": "a", "state": "swap.completed",
            "updatedAt": now - 100_000, "boltzFee": 50,
        }))];
        let c = boltz_cost_components(&swaps, now, 24, 1_000_000, None);
        assert_eq!(c.spent_24h_sats, 0);
    }

    #[test]
    fn abandoned_swap_never_counted_as_spent() {
        let now = 1_700_000_000i64;
        let swaps = vec![swap(json!({
            "id": "a", "state": "swap.abandoned",
            "updatedAt": now - 100, "boltzFee": 999,
        }))];
        let c = boltz_cost_components(&swaps, now, 24, 1_000_000, None);
        assert_eq!(c.spent_24h_sats, 0, "abandoned must never count as spend");
    }

    #[test]
    fn swap_with_no_timestamp_is_skipped_and_counted() {
        let now = 1_700_000_000i64;
        let swaps = vec![swap(
            json!({"id": "a", "state": "swap.completed", "boltzFee": 10}),
        )];
        let c = boltz_cost_components(&swaps, now, 24, 1_000_000, None);
        assert_eq!(c.spent_24h_sats, 0);
        assert_eq!(c.skipped_without_timestamp, 1);
    }

    #[test]
    fn pending_swap_counts_as_reserved_not_spent() {
        let now = 1_700_000_000i64;
        let swaps = vec![swap(json!({
            "id": "a", "state": "swap.created",
            "createdAt": now - 60, "boltzFee": 30,
        }))];
        let c = boltz_cost_components(&swaps, now, 24, 1_000_000, None);
        assert_eq!(c.spent_24h_sats, 0);
        assert_eq!(c.reserved_24h_sats, 30);
        assert_eq!(c.reserved_swaps, 1);
    }

    #[test]
    fn reserved_capped_at_remaining_budget() {
        let now = 1_700_000_000i64;
        let swaps = vec![
            swap(
                json!({"id": "a", "state": "swap.completed", "updatedAt": now - 10, "boltzFee": 400}),
            ),
            swap(
                json!({"id": "b", "state": "swap.created", "createdAt": now - 10, "boltzFee": 700}),
            ),
        ];
        // budget 1000, already spent 400 -> only 600 reservable, even
        // though the pending swap's own estimate is 700.
        let c = boltz_cost_components(&swaps, now, 24, 1000, None);
        assert_eq!(c.spent_24h_sats, 400);
        assert_eq!(c.reserved_24h_sats, 600);
    }

    #[test]
    fn global_cap_tighter_than_boltz_budget_wins() {
        let now = 1_700_000_000i64;
        let swaps = vec![swap(
            json!({"id": "a", "state": "swap.created", "createdAt": now - 10, "boltzFee": 500}),
        )];
        let c = boltz_cost_components(&swaps, now, 24, 10_000, Some(200));
        assert_eq!(c.reserved_24h_sats, 200);
    }

    // --- budget_status ---

    #[test]
    fn budget_status_combines_local_and_external() {
        let local = CostComponents {
            spent_24h_sats: 100,
            reserved_24h_sats: 50,
            ..Default::default()
        };
        let external = ExternalLiquidityCosts {
            spent_24h_sats: 200,
            reserved_24h_sats: 0,
        };
        let s = budget_status(1000, &local, &external);
        assert_eq!(s.spent_24h_sats_estimate, 300);
        assert_eq!(s.remaining_24h_sats_estimate, 650); // 1000-300-50
        assert_eq!(s.boltz_remaining_24h_sats_estimate, 850); // 1000-100-50
    }

    #[test]
    fn budget_status_remaining_floors_at_zero() {
        let local = CostComponents {
            spent_24h_sats: 5000,
            ..Default::default()
        };
        let s = budget_status(1000, &local, &ExternalLiquidityCosts::default());
        assert_eq!(s.remaining_24h_sats_estimate, 0);
    }

    // --- enforce_budget_for_quote ---

    #[test]
    fn quote_within_budget_is_allowed() {
        let e = enforce_budget_for_quote(100, 10, true, 200);
        assert!(e.allowed);
        assert_eq!(e.estimated_fee_sats, 110);
    }

    #[test]
    fn quote_exceeding_budget_is_rejected_with_reason() {
        let e = enforce_budget_for_quote(150, 10, true, 100);
        assert!(!e.allowed);
        assert!(e.reason.is_some());
    }

    #[test]
    fn quote_exceeding_budget_allowed_when_enforcement_off() {
        // Control: same numbers as the rejection case above, but
        // enforce_budget=false must allow it.
        let e = enforce_budget_for_quote(150, 10, false, 100);
        assert!(e.allowed);
    }

    // --- reservation_gate / finalize_reservation_attempt ---

    #[test]
    fn gate_not_applicable_without_capex_engine() {
        assert_eq!(
            reservation_gate(false, true, 500, Some(1000)),
            ReservationGate::NotApplicable
        );
    }

    #[test]
    fn gate_not_applicable_with_enforce_budget_off() {
        assert_eq!(
            reservation_gate(true, false, 500, Some(1000)),
            ReservationGate::NotApplicable
        );
    }

    #[test]
    fn gate_not_applicable_for_zero_fee() {
        assert_eq!(
            reservation_gate(true, true, 0, Some(1000)),
            ReservationGate::NotApplicable
        );
    }

    #[test]
    fn gate_not_applicable_when_budget_unreadable() {
        assert_eq!(
            reservation_gate(true, true, 500, None),
            ReservationGate::NotApplicable
        );
    }

    #[test]
    fn gate_attempts_with_valid_inputs() {
        assert_eq!(
            reservation_gate(true, true, 500, Some(1000)),
            ReservationGate::Attempt {
                fee_sats: 500,
                effective_budget_sats: 1000
            }
        );
    }

    #[test]
    fn finalize_reserved_on_engine_true() {
        assert_eq!(
            finalize_reservation_attempt("boltz-swap:1", Some(true)),
            ReservationOutcome::Reserved("boltz-swap:1".to_string())
        );
    }

    #[test]
    fn finalize_rejected_on_engine_false() {
        assert_eq!(
            finalize_reservation_attempt("boltz-swap:1", Some(false)),
            ReservationOutcome::Rejected
        );
    }

    #[test]
    fn finalize_fails_open_on_engine_error() {
        assert_eq!(
            finalize_reservation_attempt("boltz-swap:1", None),
            ReservationOutcome::NotApplicable
        );
    }

    // --- finalize_action ---

    #[test]
    fn finalize_action_settles_on_success() {
        assert_eq!(finalize_action(true, false), FinalizeAction::Settle);
    }

    #[test]
    fn finalize_action_releases_on_error_swap() {
        assert_eq!(finalize_action(true, true), FinalizeAction::Release);
    }

    #[test]
    fn finalize_action_releases_when_no_swap_id() {
        assert_eq!(finalize_action(false, false), FinalizeAction::Release);
    }
}
