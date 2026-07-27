//! Swap fee estimation.
//!
//! Ports py `_estimate_swap_fee_sats` (boltz_manager.py:951-989) and
//! `_estimate_reverse_routing_fee_sats` (boltz_manager.py:991-1008).

use crate::parsing::parse_int;
use serde_json::{Map, Value};

const NAMED_FEE_FIELDS: &[&str] = &[
    "boltzFee",
    "networkFee",
    "serviceFee",
    "routingFee",
    "onchainFee",
];

/// py `_estimate_swap_fee_sats` (boltz_manager.py:951-989).
///
/// Prefers the named top-level fields (summed) when ANY are present.
/// Otherwise falls back to a recursive sum of `*fee*`-named numeric
/// fields, EXCLUDING rate-like keys (`percent`/`percentage`/`ppm`/`rate`).
///
/// E-4.4 anti-double-count (2026-07 econ audit): once a dict level has a
/// positive numeric fee-field total, that level is authoritative and its
/// subtree is NOT also descended — a payload with a parent total plus a
/// nested per-component breakdown must not sum both.
pub fn estimate_swap_fee_sats(swap: &Value) -> i64 {
    if let Some(obj) = swap.as_object() {
        let mut total = 0i64;
        let mut seen_named = false;
        for key in NAMED_FEE_FIELDS {
            if let Some(v) = obj.get(*key) {
                total += parse_int(v, 0).max(0);
                seen_named = true;
            }
        }
        if seen_named {
            return total;
        }
    }
    recursive_fee_sum(swap)
}

fn recursive_fee_sum(obj: &Value) -> i64 {
    match obj {
        Value::Object(map) => recursive_fee_sum_map(map),
        Value::Array(arr) => arr.iter().map(recursive_fee_sum).sum(),
        _ => 0,
    }
}

fn recursive_fee_sum_map(obj: &Map<String, Value>) -> i64 {
    let mut level_total = 0i64;
    for (k, v) in obj {
        let lk = k.to_lowercase();
        let is_rate_like = ["percent", "percentage", "ppm", "rate"]
            .iter()
            .any(|x| lk.contains(x));
        if lk.contains("fee") && !is_rate_like && is_numeric_like(v) {
            level_total += parse_int(v, 0).max(0);
        }
    }
    if level_total > 0 {
        return level_total;
    }
    obj.values()
        .filter(|v| v.is_object() || v.is_array())
        .map(recursive_fee_sum)
        .sum()
}

/// Python's `isinstance(v, (int, float, str))` gate before treating a
/// `*fee*`-named field as summable — a nested dict/list under a key like
/// `feeBreakdown` must not be coerced by `_parse_int` (which would just
/// silently return 0 for it and, worse, mask the intended recursive
/// descent).
fn is_numeric_like(v: &Value) -> bool {
    matches!(v, Value::Number(_) | Value::String(_))
}

/// py `_estimate_reverse_routing_fee_sats` (boltz_manager.py:991-1008).
///
/// A reverse swap PAYS a lightning invoice to Boltz; the quote covers
/// Boltz service + on-chain fees but NOT our routing cost, bounded by
/// `routing_fee_limit_ppm`. `limit_ppm <= 0` or `amount_sats <= 0` -> 0
/// (there is no honest bound to plan with when the cap is unset).
/// Rounds UP (`ceil`), matching the Python integer-ceiling-division idiom
/// `(amount * limit_ppm + 999_999) // 1_000_000`.
pub fn estimate_reverse_routing_fee_sats(amount_sats: i64, routing_fee_limit_ppm: i64) -> i64 {
    let limit_ppm = routing_fee_limit_ppm.max(0);
    if limit_ppm <= 0 || amount_sats <= 0 {
        return 0;
    }
    (amount_sats * limit_ppm + 999_999) / 1_000_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn named_fields_sum_when_present() {
        let swap = json!({"boltzFee": 10, "networkFee": 5, "unrelated": 999});
        assert_eq!(estimate_swap_fee_sats(&swap), 15);
    }

    #[test]
    fn named_fields_negative_clamped_to_zero() {
        let swap = json!({"boltzFee": -5, "networkFee": 5});
        assert_eq!(estimate_swap_fee_sats(&swap), 5);
    }

    #[test]
    fn falls_back_to_recursive_sum_when_no_named_fields() {
        let swap = json!({"details": {"minerFee": 3, "swapFee": 4}});
        assert_eq!(estimate_swap_fee_sats(&swap), 7);
    }

    #[test]
    fn recursive_sum_excludes_rate_like_keys() {
        let swap = json!({"details": {"feePercentage": 200, "swapFee": 4, "feePpm": 500}});
        // Only swapFee (4) counts; feePercentage/feePpm are rate-like, not spend.
        assert_eq!(estimate_swap_fee_sats(&swap), 4);
    }

    #[test]
    fn anti_double_count_stops_descent_at_authoritative_level() {
        // A parent total (swapFee: 10) alongside a nested per-component
        // breakdown (minerFee/serviceFee under "breakdown") must not sum
        // both — the level WITH a positive fee total wins outright.
        let swap = json!({
            "swapFee": 10,
            "breakdown": {"minerFee": 3, "serviceFee": 2},
        });
        assert_eq!(
            estimate_swap_fee_sats(&swap),
            10,
            "must not also sum breakdown (would give 15)"
        );
    }

    #[test]
    fn zero_total_level_still_descends() {
        // A level with fee-like keys summing to 0 is NOT authoritative
        // (Python: `if level_total > 0`) — descent must continue.
        let swap = json!({"feeInfo": {"note": "n/a"}, "nested": {"minerFee": 6}});
        assert_eq!(estimate_swap_fee_sats(&swap), 6);
    }

    #[test]
    fn no_fee_fields_anywhere_is_zero() {
        let swap = json!({"id": "abc", "amount": 1000});
        assert_eq!(estimate_swap_fee_sats(&swap), 0);
    }

    #[test]
    fn reverse_routing_fee_zero_when_limit_unset() {
        assert_eq!(estimate_reverse_routing_fee_sats(1_000_000, 0), 0);
    }

    #[test]
    fn reverse_routing_fee_zero_when_amount_non_positive() {
        assert_eq!(estimate_reverse_routing_fee_sats(0, 500), 0);
        assert_eq!(estimate_reverse_routing_fee_sats(-100, 500), 0);
    }

    #[test]
    fn reverse_routing_fee_rounds_up() {
        // 1_000_000 sats * 1 ppm = 1 sat exactly -> ceil(1) == 1.
        assert_eq!(estimate_reverse_routing_fee_sats(1_000_000, 1), 1);
        // 1 sat * 1 ppm = 0.000001 sat -> ceil rounds up to 1, never 0.
        assert_eq!(estimate_reverse_routing_fee_sats(1, 1), 1);
    }

    #[test]
    fn reverse_routing_fee_scales_with_amount_and_ppm() {
        // 500_000 sats @ 100 ppm = 50 sats exactly.
        assert_eq!(estimate_reverse_routing_fee_sats(500_000, 100), 50);
    }
}
