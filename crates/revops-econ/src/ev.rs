//! Expected-value contract (port of `modules/econ_ev.py`).
//!
//! The spec's contract:
//!
//! ```text
//! expected_value = expected_incremental_revenue
//!                - expected_execution_cost
//!                - expected_capital_cost
//!                - risk_premium
//! ```
//!
//! in checked integer msat (wire-contract numeric rules). Missing data
//! fails CONSERVATIVELY: absent or non-finite benefit/confidence resolves
//! to zero — never to an optimistic estimate.
//!
//! Float ingress here uses **banker's rounding** (`f64::round_ties_even`,
//! matching Python's `round()`), which is deliberately different from
//! `types::Micro::from_float_clamped`'s half-up-by-truncation rule (see
//! that function's doc comment). The two must not be unified — see
//! `confidence_micro_and_micro_from_float_clamped_diverge_on_a_tie` below
//! for a pinned input where they disagree.

use crate::types::{EconError, EconResult, Micro, SignedMsat};

/// Mirrors Python's module-level `MICRO_ONE` constant.
pub const MICRO_ONE: i64 = 1_000_000;

/// The common EV contract, checked. Costs and the risk premium SUBTRACT —
/// a higher cost or premium can never raise the result. Uses a checked
/// `i128` accumulator so an out-of-`i64`-range result is rejected
/// (`Err`) rather than silently wrapping — authorization arithmetic fails
/// closed, never wraps, never coerces to zero (see Global Constraints).
pub fn expected_value_msat(
    revenue_msat: i64,
    execution_cost_msat: i64,
    capital_cost_msat: i64,
    risk_premium_msat: i64,
) -> EconResult<SignedMsat> {
    let total = revenue_msat as i128
        - execution_cost_msat as i128
        - capital_cost_msat as i128
        - risk_premium_msat as i128;
    if total < i64::MIN as i128 || total > i64::MAX as i128 {
        return Err(EconError {
            msg: format!("expected_value_msat overflow: {total}"),
        });
    }
    Ok(SignedMsat(total as i64))
}

/// Sats (as an optional float) -> `SignedMsat` msat. `None`/non-finite ->
/// 0 (conservative missing-data rule, mirrors Python's
/// `TypeError`/`ValueError`/non-finite catch-all). Finite -> banker's
/// rounding: `(value_sats * 1000.0).round_ties_even()`, matching Python's
/// `int(round(value * 1000))`.
pub fn benefit_msat_from_sats(value_sats: Option<f64>) -> EconResult<SignedMsat> {
    let value = match value_sats {
        Some(v) if v.is_finite() => v,
        _ => return Ok(SignedMsat(0)),
    };
    let scaled = value * 1000.0;
    if !scaled.is_finite() {
        return Err(EconError {
            msg: format!("benefit_msat_from_sats: {value} * 1000 overflowed f64 range"),
        });
    }
    let rounded = scaled.round_ties_even();
    let widened = rounded as i128;
    if widened < i64::MIN as i128 || widened > i64::MAX as i128 {
        return Err(EconError {
            msg: format!("benefit_msat_from_sats out of SignedMsat range: {rounded}"),
        });
    }
    Ok(SignedMsat(widened as i64))
}

/// Unit-interval fraction (as an optional float) -> `Micro` fixed-point,
/// clamped to `[0, 1_000_000]`. `None`/non-finite -> 0 (conservative).
/// Finite -> clamp to `[0.0, 1.0]` then banker's round:
/// `(clamped * MICRO_ONE).round_ties_even()`, matching Python's
/// `int(round(max(0.0, min(1.0, value)) * MICRO_ONE))`.
///
/// Infallible outward (returns `Micro` directly, not `EconResult<Micro>`):
/// clamping to `[0.0, 1.0]` before scaling guarantees the banker's-rounded
/// result always lands in `Micro`'s `[0, 1_000_000]` range.
pub fn confidence_micro(fraction: Option<f64>) -> Micro {
    let value = match fraction {
        Some(v) if v.is_finite() => v,
        _ => return Micro::new(0).expect("0 is always a valid Micro"),
    };
    let clamped = value.clamp(0.0, 1.0);
    let scaled = (clamped * MICRO_ONE as f64).round_ties_even();
    Micro::new(scaled as i64)
        .expect("clamped fraction scaled by MICRO_ONE always fits Micro's [0, 1_000_000] range")
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- expected_value_msat ---

    #[test]
    fn expected_value_msat_subtracts_all_three_costs() {
        let ev = expected_value_msat(10_000, 1_000, 2_000, 500).unwrap();
        assert_eq!(ev.0, 6_500);
    }

    #[test]
    fn expected_value_msat_can_go_negative() {
        let ev = expected_value_msat(100, 1_000, 0, 0).unwrap();
        assert_eq!(ev.0, -900);
    }

    #[test]
    fn expected_value_msat_higher_cost_never_raises_result() {
        let low_cost = expected_value_msat(1_000, 100, 0, 0).unwrap();
        let high_cost = expected_value_msat(1_000, 200, 0, 0).unwrap();
        assert!(high_cost.0 <= low_cost.0);
    }

    #[test]
    fn expected_value_msat_rejects_i64_overflow() {
        assert!(expected_value_msat(i64::MAX, -1, 0, 0).is_err());
        assert!(expected_value_msat(i64::MIN, 1, 0, 0).is_err());
    }

    // --- benefit_msat_from_sats ---

    #[test]
    fn benefit_msat_from_sats_none_is_zero() {
        assert_eq!(benefit_msat_from_sats(None).unwrap().0, 0);
    }

    #[test]
    fn benefit_msat_from_sats_nan_and_inf_are_zero() {
        assert_eq!(benefit_msat_from_sats(Some(f64::NAN)).unwrap().0, 0);
        assert_eq!(benefit_msat_from_sats(Some(f64::INFINITY)).unwrap().0, 0);
        assert_eq!(
            benefit_msat_from_sats(Some(f64::NEG_INFINITY)).unwrap().0,
            0
        );
    }

    #[test]
    fn benefit_msat_from_sats_scales_by_1000() {
        assert_eq!(
            benefit_msat_from_sats(Some(400_000.0)).unwrap().0,
            400_000_000
        );
        assert_eq!(benefit_msat_from_sats(Some(-5.0)).unwrap().0, -5_000);
    }

    /// Banker's rounding on a genuine f64 tie: `0.1234565 * 1e6 ==
    /// 123456.5` exactly (verified `python3 -c "print(repr(0.1234565*1e6))"`
    /// => `123456.5`, a clean tie, not IEEE-754 noise). Scaled by 1000 sats
    /// here (123.4565 sats -> 123456.5 msat), `round_ties_even` rounds to
    /// the even neighbor, 123456 — matching Python's `round()` (verified
    /// `python3 -c "from modules.econ_ev import benefit_msat_from_sats; \
    /// print(benefit_msat_from_sats(123.4565).value)"` => `123456`).
    #[test]
    fn benefit_msat_from_sats_rounds_ties_to_even() {
        assert_eq!(benefit_msat_from_sats(Some(123.4565)).unwrap().0, 123_456);
    }

    // --- confidence_micro ---

    #[test]
    fn confidence_micro_none_and_non_finite_are_zero() {
        assert_eq!(confidence_micro(None).value(), 0);
        assert_eq!(confidence_micro(Some(f64::NAN)).value(), 0);
        assert_eq!(confidence_micro(Some(f64::INFINITY)).value(), 0);
        assert_eq!(confidence_micro(Some(f64::NEG_INFINITY)).value(), 0);
    }

    #[test]
    fn confidence_micro_clamps_to_unit_interval() {
        assert_eq!(confidence_micro(Some(-5.0)).value(), 0);
        assert_eq!(confidence_micro(Some(5.0)).value(), 1_000_000);
    }

    #[test]
    fn confidence_micro_scales_ordinary_fraction() {
        assert_eq!(confidence_micro(Some(0.5)).value(), 500_000);
    }

    /// Pinned golden values, re-verified against real Python (not the
    /// idealized decimal expansion — see the analogous note in
    /// `types.rs::micro_from_float_clamped_half_up`):
    /// `python3 -c "from modules.econ_ev import confidence_micro; \
    ///   print([confidence_micro(f).value for f in \
    ///   [0.1234565, 0.5000005, 0.123456500001, 0.1234575]])"`
    /// => `[123456, 500000, 123457, 123458]`.
    #[test]
    fn confidence_micro_matches_python_golden_values() {
        assert_eq!(confidence_micro(Some(0.1234565)).value(), 123_456);
        assert_eq!(confidence_micro(Some(0.5000005)).value(), 500_000);
        assert_eq!(confidence_micro(Some(0.123456500001)).value(), 123_457);
        assert_eq!(confidence_micro(Some(0.1234575)).value(), 123_458);
    }

    /// THE pinned banker's-vs-half-up divergence: `0.1234565 * 1e6 ==
    /// 123456.5` exactly (a genuine f64 tie). `confidence_micro` (banker's,
    /// this module) rounds it to the even neighbor 123456; `Micro::
    /// from_float_clamped` (half-up-by-truncation, `types.rs`) rounds the
    /// same input to 123457 — the exact case that module's own test
    /// (`micro_from_float_clamped_half_up`) already pins. This test proves
    /// the two rounding rules are NOT interchangeable for this input.
    #[test]
    fn confidence_micro_and_micro_from_float_clamped_diverge_on_a_tie() {
        use crate::types::Micro as TypesMicro;
        let input = 0.1234565;
        let banker = confidence_micro(Some(input)).value();
        let half_up = TypesMicro::from_float_clamped(input).unwrap().value();
        assert_eq!(banker, 123_456);
        assert_eq!(half_up, 123_457);
        assert_ne!(banker, half_up);
    }
}
