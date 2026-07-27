//! Python `round(x, ndigits)` parity helper (ties-to-even), generalizing
//! `revops_core::msat::py_round2` to an arbitrary digit count. Used
//! throughout the orchestration modules ported in this pass wherever
//! Python's `round()` builtin appears on a `float` (winners/losers
//! enrichment fields, demand-flow scores).
//!
//! Uses the same "format then reparse" technique as `py_round2`: Rust's
//! `{:.N}` float formatting is a correctly-rounded (ties-to-even) decimal
//! conversion of the underlying IEEE-754 double, matching CPython's
//! `round()` on a `float` argument.

/// Round `value` to `ndigits` decimal places, Python `round(value, ndigits)`
/// semantics. Non-finite values (`NaN`/`inf`) pass through unchanged,
/// matching Python's `round(nan, n) is nan`.
pub fn py_round(value: f64, ndigits: i32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    let ndigits = ndigits.max(0) as usize;
    format!("{value:.ndigits$}").parse::<f64>().unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ties_to_even() {
        // Python: round(2.675, 2) == 2.67 (binary repr of 2.675 is
        // slightly below the exact decimal value).
        assert_eq!(py_round(2.675, 2), 2.67);
        // Python: round(0.5, 0) == 0.0 (even), round(1.5, 0) == 2.0 (even)
        assert_eq!(py_round(0.5, 0), 0.0);
        assert_eq!(py_round(1.5, 0), 2.0);
    }

    #[test]
    fn non_finite_passthrough() {
        assert!(py_round(f64::NAN, 2).is_nan());
        assert_eq!(py_round(f64::INFINITY, 2), f64::INFINITY);
    }
}
