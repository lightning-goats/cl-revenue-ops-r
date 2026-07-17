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

/// Guards `f64::powf` against LLVM's `-O2`+ constant-operand rewrites so
/// this workspace's `pow()` ports match CPython's `**` (`float.__pow__`,
/// which always dispatches to the platform's `pow()`) bit-for-bit in
/// *both* the `dev` and `--release` profiles.
///
/// ## The story
///
/// This is the workspace-wide fix mandated by the "HARD PRE-DRY-RUN GATE"
/// entry in `.superpowers/sdd/progress.md` (T6 discovery) and adjudicated
/// by the T6 review. Treat the following as measured facts, not something
/// to re-derive:
///
/// Under `-O2`+, LLVM recognizes several `pow(x, y)` call shapes where one
/// operand is a compile-time constant and rewrites the call to a cheaper
/// intrinsic that is **not** bit-identical to glibc's `pow()`:
/// - `x.powf(0.5)` -> `x.sqrt()` — diverges from `pow(x, 0.5)` in the
///   last bit for ~0.090% of inputs.
/// - `x.powf(2.0)` -> `x * x` — diverges from `pow(x, 2.0)` for ~0.084%.
/// - a compile-time-constant *base* with a variable exponent, e.g.
///   `0.5f64.powf(y)` -> an `exp2`-based form — diverges for ~0.112%.
///
/// CPython's `**` operator always calls `pow()` (`floatobject.c`
/// `float_pow`), never `sqrt()`/a multiply/`exp2()`, so any of these three
/// rewrites is a silent parity bug: invisible in `dev` (unoptimized, calls
/// the real `pow()` symbol), surfacing only in a `--release` binary — the
/// profile actually deployed to lnnode.
///
/// Rewrite-*shaped* constant bases other than `0.5` were checked against
/// this toolchain and found **not** to be rewritten this way — e.g.
/// `1.5_f64.powf(y)` (`pid.rs`'s controller gain, the one such call site
/// that actually exists in this workspace) and the hypothetical
/// `0.85_f64.powf(y)` shape. Real call sites matching a *safe* (non-
/// rewritten) shape are deliberately left calling `.powf()` directly (see
/// the comment at each call site) rather than migrated for uniformity —
/// churning a site that isn't actually at risk
/// only adds a chance of re-baking a fixture for zero parity benefit.
///
/// Two alternative fixes were considered and rejected:
/// - The `libm` crate (musl-derived): it reproduces the *rewritten*
///   (wrong, sqrt-identical) bits, not glibc's — it would "fix" the LLVM
///   rewrite problem by hard-coding the same wrong answer at every call
///   site, unconditionally.
/// - An `extern "C"` FFI shim straight to libc's `pow()`: LLVM still
///   rewrites `powf(2.0)` across the FFI boundary once it can see the
///   callee is `pow`, and the shim requires `unsafe`, which this workspace
///   forbids (`#![forbid(unsafe_code)]`).
///
/// The fix that survives verification: [`std::hint::black_box`] on *both*
/// operands. `black_box` is an optimization barrier that prevents LLVM
/// from treating either operand as a compile-time constant, removing the
/// precondition every rewrite above depends on and forcing a genuine
/// runtime call into the real `pow()` symbol. Verified empirically against
/// CPython on divergent inputs (see `tests::py_pow_matches_cpython_on_*`
/// below) in both the `dev` and `--release` profiles — every case matches
/// CPython's bits exactly in both.
///
/// Every `powf` call in a fixture-compared path in this workspace that
/// matches one of the three rewritten shapes above (constant `0.5` or
/// `2.0` exponent, or a constant `0.5` base) must go through this function
/// instead of calling `.powf()` directly.
pub fn py_pow(x: f64, y: f64) -> f64 {
    std::hint::black_box(x).powf(std::hint::black_box(y))
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

    /// Canary for the LLVM `powf(0.5)` -> `sqrt()` rewrite (T6 review;
    /// see `py_pow`'s doc comment). Inputs were found by random search
    /// (`x = random.uniform(0.0001, 100.0)`, seed 42) for
    /// `x**0.5 != math.sqrt(x)` under live CPython, i.e. cases where the
    /// LLVM-rewritten form would diverge. `py_pow` must reproduce
    /// CPython's `x**0.5` bits exactly; a bare `x.powf(0.5)` reproduces
    /// them only in `dev` (see `py_pow_bare_powf_diverges_from_cpython_in_release_only`,
    /// gated to `--release`, for the contrast).
    #[test]
    fn py_pow_matches_cpython_on_sqrt_rewrite_divergent_inputs() {
        let cases: &[(f64, u64)] = &[
            (18.582577515576222, 0x40113e359c878ce5),
            (53.055730687448715, 0x401d22c03ed1689d),
            (30.1946537745775, 0x4015fad86cd68ad9),
            (22.585802677016954, 0x40130282d9bfa39a),
            (67.16334608320724, 0x40206401979d0037),
        ];
        for &(x, expected_bits) in cases {
            assert_eq!(
                py_pow(x, 0.5).to_bits(),
                expected_bits,
                "py_pow({x}, 0.5) must match CPython's x**0.5"
            );
        }
    }

    /// Canary for the LLVM `powf(2.0)` -> `x * x` rewrite (T6 review). Inputs
    /// found by random search (seed 42) for `x**2 != x*x` under live
    /// CPython.
    #[test]
    fn py_pow_matches_cpython_on_square_rewrite_divergent_inputs() {
        let cases: &[(f64, u64)] = &[
            (37.057150564718, 0x409574edfc5b4415),
            (79.29330751994493, 0x40b88f6db9df9538),
            (92.05210293762607, 0x40c08ccb79d2a85f),
            (85.12954896918512, 0x40bc4f0a447f7e73),
            (9.760396620964551, 0x4057d0fb5dd83d8f),
        ];
        for &(x, expected_bits) in cases {
            assert_eq!(
                py_pow(x, 2.0).to_bits(),
                expected_bits,
                "py_pow({x}, 2.0) must match CPython's x**2"
            );
        }
    }

    /// Canary for the LLVM constant-base `0.5f64.powf(y)` -> `exp2` rewrite
    /// (T6 review). Inputs found by random search (`y =
    /// random.uniform(0.001, 50.0)`, seed 7) for `0.5**y != math.exp2(-y)`
    /// under live CPython.
    #[test]
    fn py_pow_matches_cpython_on_constant_base_rewrite_divergent_inputs() {
        let cases: &[(f64, u64)] = &[
            (37.31567578975353, 0x3d99b6103bc8215b),
            (18.401847912794327, 0x3ec8386d90d72995),
            (32.901101957487896, 0x3de122a1d9e00253),
            (23.090029634084964, 0x3e7e10698a80416d),
            (28.094147381518685, 0x3e2dfa79f8aa8d29),
        ];
        for &(y, expected_bits) in cases {
            assert_eq!(
                py_pow(0.5, y).to_bits(),
                expected_bits,
                "py_pow(0.5, {y}) must match CPython's 0.5**{y}"
            );
        }
    }

    /// Contrast test, `--release`-only: demonstrates the bug `py_pow`
    /// fixes. A bare `x.powf(0.5)`/`x.powf(2.0)` call on these same
    /// divergent inputs is expected to diverge from CPython once LLVM's
    /// `-O2`+ rewrite kicks in — this test asserts the divergence exists,
    /// so it would fail (loudly) if a future toolchain ever stopped
    /// rewriting these shapes, which would be a signal to revisit whether
    /// `py_pow`'s `black_box` guard is still load-bearing. Gated to
    /// `release` because `dev` is unoptimized and never triggers the
    /// rewrite (bare `.powf()` matches CPython there too).
    #[test]
    #[cfg(not(debug_assertions))]
    fn py_pow_bare_powf_diverges_from_cpython_in_release_only() {
        let sqrt_cases: &[(f64, u64)] = &[
            (18.582577515576222, 0x40113e359c878ce5),
            (53.055730687448715, 0x401d22c03ed1689d),
        ];
        for &(x, cpython_bits) in sqrt_cases {
            assert_ne!(
                x.powf(0.5).to_bits(),
                cpython_bits,
                "expected the unguarded sqrt-rewrite divergence for x={x} to still reproduce; \
                 if this now matches CPython, the toolchain may have changed and py_pow's \
                 black_box guard should be re-verified rather than assumed obsolete"
            );
        }

        let square_cases: &[(f64, u64)] = &[
            (37.057150564718, 0x409574edfc5b4415),
            (79.29330751994493, 0x40b88f6db9df9538),
        ];
        for &(x, cpython_bits) in square_cases {
            assert_ne!(
                x.powf(2.0).to_bits(),
                cpython_bits,
                "expected the unguarded x*x-rewrite divergence for x={x} to still reproduce; \
                 if this now matches CPython, the toolchain may have changed and py_pow's \
                 black_box guard should be re-verified rather than assumed obsolete"
            );
        }
    }
}
