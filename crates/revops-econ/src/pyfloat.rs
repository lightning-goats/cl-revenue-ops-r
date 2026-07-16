//! CPython-`repr(float)`-compatible float formatting, needed only where a
//! float legitimately reaches the wire (shadow-cycle explanations) — see the
//! "Float-in-explanation hazard" note on Task 8 in
//! `docs/superpowers/plans/2026-07-16-phase2-econ-core.md`.
//!
//! `revops_core::canonical::canonical_json` rejects every float-typed
//! `serde_json::Value::Number` fail-closed, by design (see that function's
//! doc comment): idempotency keys and snapshot hashing must never depend on
//! a float formatter that could silently diverge from Python's. The ONE
//! legitimate exception in the whole governed core is the `"score"`
//! component of a `cycle_rebalance` `Explanation` (`modules/econ_cycle.py`
//! puts `round(float(score), 6)` there) — a shadow-publishing diagnostic
//! field, never an idempotency input. `cycle.rs`'s `CycleResult::canonical`
//! uses a *local* canonical writer (not `revops_core::canonical_json`) that
//! formats exactly that kind of float-typed leaf through [`py_repr`].
//!
//! ## The algorithm
//!
//! CPython's `repr(float)` is "shortest decimal string that round-trips to
//! the same IEEE-754 double" (unique up to a ties-to-even tie-break — see
//! Steele & White 1990), formatted as fixed decimal for `-4 <= decexp < 16`
//! (`1e16` and above, or values with magnitude `< 1e-4`, use scientific
//! notation), always keeping at least one fractional digit for a fixed-form
//! integral value (`5.0`, not `5`).
//!
//! Empirically, `{:?}` (`Debug`) on `f64` in Rust already computes exactly
//! this — the same shortest-round-trip digit generation (`core::fmt::float`
//! uses the Grisu3/Dragon4 family, the same well-defined algorithm class CPython
//! uses via David Gay's dtoa) and the same fixed-vs-scientific decision
//! boundary (verified against CPython across integral/half/1e15..1e17/
//! subnormal/`f64::MAX`/negative-zero cases — see `tests/cycle.rs` and the
//! `fixtures/pyfloat.json` corpus). The only remaining differences are
//! purely cosmetic, so `py_repr` post-processes Rust's `{:?}` output rather
//! than reimplementing digit generation:
//! - Rust omits the `+` on positive exponents (`"1e16"`); Python always
//!   shows a sign (`"1e+16"`).
//! - Rust doesn't zero-pad the exponent; Python zero-pads to at least 2
//!   digits (`"1e-5"` -> `"1e-05"`, but `"1e-300"` is left alone).
//! - Rust's `NaN`/`inf`/`-inf` are capitalized differently than Python's
//!   `nan`/`inf`/`-inf` (not reachable from a real economic score, but
//!   handled for completeness rather than left to panic).
pub fn py_repr(f: f64) -> String {
    if f.is_nan() {
        return "nan".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    let s = format!("{f:?}");
    match s.find(['e', 'E']) {
        Some(idx) => {
            let (mantissa, exp_part) = s.split_at(idx);
            let exp_digits = &exp_part[1..]; // skip the 'e'/'E'
            let exp: i32 = exp_digits
                .parse()
                .expect("Rust float Debug exponent is always a valid signed integer");
            let sign = if exp < 0 { '-' } else { '+' };
            format!("{mantissa}e{sign}{:02}", exp.abs())
        }
        None => s,
    }
}

/// Best-effort port of Python's built-in `round(value, ndigits)` for
/// `f64`. Not a from-scratch decimal-rounding implementation: it formats
/// `value` to `ndigits` fractional digits (Rust's fixed-precision float
/// formatter is, like Python's `round()`, a *correctly rounded*
/// binary-to-decimal conversion of the exact `f64` value — ties-to-even on
/// the true binary value, not on the naive decimal literal), then parses
/// the result back to `f64`. Cross-checked against live CPython for a set
/// of representative "economic score" shapes, including inputs that look
/// like exact `.5` ties in decimal but are not exact ties in the underlying
/// binary value (`round(0.1234565, 6) == 0.123456`, not `0.123457`) — see
/// `tests/cycle.rs::py_round_matches_python_reference_values`.
///
/// Used only to reproduce `modules/econ_cycle.py`'s
/// `round(float(score), 6)` for the shadow-cycle explanation field; never
/// used on a money path (those use the checked, integer-only rounding
/// rules in `crate::types` / `crate::ev`, which do not go through floats
/// this way at all).
pub fn py_round(value: f64, ndigits: i32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let digits = ndigits.max(0) as usize;
    format!("{value:.digits$}").parse::<f64>().unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn fixtures() -> Value {
        let raw = include_str!("../../../fixtures/pyfloat.json");
        serde_json::from_str(raw).unwrap()
    }

    #[test]
    fn py_repr_matches_python_fixture_corpus() {
        let data = fixtures();
        let cases = data["repr_cases"].as_array().unwrap();
        assert!(!cases.is_empty(), "fixture must be non-empty");
        for case in cases {
            let value = case["value"].as_f64().unwrap();
            let expected = case["repr"].as_str().unwrap();
            assert_eq!(py_repr(value), expected, "value={value:?}");
        }
    }

    #[test]
    fn py_repr_handles_integrals() {
        assert_eq!(py_repr(5.0), "5.0");
        assert_eq!(py_repr(-5.0), "-5.0");
        assert_eq!(py_repr(0.0), "0.0");
        assert_eq!(py_repr(-0.0), "-0.0");
    }

    #[test]
    fn py_repr_handles_shortest_roundtrip_cases() {
        assert_eq!(py_repr(0.1), "0.1");
        assert_eq!(py_repr(0.3), "0.3");
        assert_eq!(py_repr(1.0 / 3.0), "0.3333333333333333");
    }

    #[test]
    fn py_repr_handles_exponent_boundaries() {
        assert_eq!(py_repr(1e15), "1000000000000000.0");
        assert_eq!(py_repr(1e16), "1e+16");
        assert_eq!(py_repr(9.999e15), "9999000000000000.0");
        assert_eq!(py_repr(1e-4), "0.0001");
        assert_eq!(py_repr(9.99e-5), "9.99e-05");
    }

    #[test]
    fn py_repr_pads_and_signs_exponent() {
        assert_eq!(py_repr(1e-5), "1e-05");
        assert_eq!(py_repr(1e21), "1e+21");
        assert_eq!(py_repr(1e100), "1e+100");
        assert_eq!(py_repr(1e-300), "1e-300");
    }

    #[test]
    fn py_repr_handles_non_finite() {
        assert_eq!(py_repr(f64::NAN), "nan");
        assert_eq!(py_repr(f64::INFINITY), "inf");
        assert_eq!(py_repr(f64::NEG_INFINITY), "-inf");
    }

    #[test]
    fn py_round_matches_python_reference_values() {
        // Golden values re-verified against live CPython:
        // python3 -c "print([round(v, 6) for v in [...]])"
        assert_eq!(py_round(0.1234565, 6), 0.123456);
        assert_eq!(py_round(0.29812345, 6), 0.298123);
        assert_eq!(py_round(1.0 / 3.0, 6), 0.333333);
        assert_eq!(py_round(2.6755555, 6), 2.675556);
        assert_eq!(py_round(100.0000005, 6), 100.0);
        assert_eq!(py_round(-0.123456789, 6), -0.123457);
        assert_eq!(py_round(0.5, 6), 0.5);
        assert_eq!(py_round(2.675, 6), 2.675);
    }
}
