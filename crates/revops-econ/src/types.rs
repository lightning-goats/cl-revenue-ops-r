//! Checked economic domain types (Rust port of `modules/econ_types.py`).
//!
//! Normative rules mirrored from the Python module:
//! - Money is integer millisatoshi. Unsigned quantities live in
//!   `[0, 2**63 - 1]` (checked `i64`, "u63"); signed P&L/deltas in the full
//!   `i64` range.
//! - Every bound violation, overflow, or invalid unit operation returns
//!   `Err(EconError)` — authorization-relevant arithmetic fails closed,
//!   never wraps or coerces to zero.
//! - Explicit methods only (`add`/`sub`/`diff`/...): incompatible units
//!   cannot be mixed silently with plain integers or each other (enforced
//!   by the Rust type system rather than Python's runtime `isinstance`
//!   checks).
//! - msat->sat conversions are direction-specific: CEIL for fees, budgets,
//!   costs, and revenue reporting; FLOOR for capacity and balances;
//!   TOWARD-ZERO for signed deltas (mirrors `modules/utils.py` rules, also
//!   ported in `revops_core::msat`).

use std::fmt;

/// Inclusive upper bound for unsigned ("u63") money and time quantities:
/// `2**63 - 1`, i.e. `i64::MAX`.
pub const U63_MAX: i64 = i64::MAX;

/// A checked domain-type operation failed. Mirrors Python's
/// `EconArithmeticError`: callers must treat this as a hard stop for the
/// affected decision (reason code `ARITHMETIC_OVERFLOW` / `SCHEMA_INVALID`),
/// never as zero.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{msg}")]
pub struct EconError {
    pub msg: String,
}

pub type EconResult<T> = Result<T, EconError>;

/// Checks `value` is within `[low, high]` and narrows to `i64`. Takes an
/// `i128` probe so callers can test out-of-`i64`-range values (e.g. `2**63`)
/// without the probe itself overflowing.
fn check_int(value: i128, low: i128, high: i128, kind: &str) -> EconResult<i64> {
    if value < low || value > high {
        return Err(EconError {
            msg: format!("{kind} out of range [{low}, {high}]: {value}"),
        });
    }
    Ok(value as i64)
}

/// Unsigned millisatoshi amount, `[0, 2**63 - 1]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Msat(i64);

impl Msat {
    pub fn new(value: i64) -> EconResult<Self> {
        Self::from_checked(value as i128)
    }

    /// `i128` entry point so conformance probes can pass values unrepresentable
    /// in `i64` (e.g. `2**63`) and still get a checked rejection rather than a
    /// silent truncation before the bounds check runs.
    pub fn from_checked(value: i128) -> EconResult<Self> {
        Ok(Msat(check_int(value, 0, U63_MAX as i128, "Msat")?))
    }

    pub const fn value(self) -> i64 {
        self.0
    }

    // Named `add`/`sub` (not `std::ops::Add`/`Sub`) to mirror the Python
    // `Msat.add`/`Msat.sub` API exactly (explicit-methods-only contract:
    // no operator-overload mixing with plain integers or other unit types).
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Msat) -> EconResult<Msat> {
        Msat::from_checked(self.0 as i128 + other.0 as i128)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, other: Msat) -> EconResult<Msat> {
        if self.0 < other.0 {
            return Err(EconError {
                msg: format!("Msat.sub underflow: {} - {}", self.0, other.0),
            });
        }
        Ok(Msat(self.0 - other.0))
    }

    /// Both operands are u63, so the difference always fits in `i64`.
    pub fn diff(self, other: Msat) -> SignedMsat {
        SignedMsat(self.0 - other.0)
    }

    /// Fees, budgets, costs, revenue reporting: round UP.
    pub fn to_sats_ceil(self) -> Sat {
        Sat((self.0 as u64).div_ceil(1000) as i64)
    }

    /// Capacity and balances: round DOWN.
    pub fn to_sats_floor(self) -> Sat {
        Sat(self.0 / 1000)
    }

    pub fn from_sats(sats: i64) -> EconResult<Msat> {
        check_int(sats as i128, 0, U63_MAX as i128, "Msat.from_sats")?;
        Msat::from_checked(sats as i128 * 1000)
    }
}

/// Signed millisatoshi delta / P&L. Range is exactly `i64`, so construction
/// from an `i64` is infallible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignedMsat(pub i64);

impl SignedMsat {
    /// Signed deltas: round toward zero. Rust's `/` on signed integers
    /// already truncates toward zero, matching Python's explicit
    /// `-((-value) // 1000)` branch for negatives.
    pub fn to_sats_toward_zero(self) -> i64 {
        self.0 / 1000
    }
}

/// Unsigned satoshi amount, `[0, 2**63 - 1]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Sat(i64);

impl Sat {
    pub fn new(value: i64) -> EconResult<Self> {
        Ok(Sat(check_int(value as i128, 0, U63_MAX as i128, "Sat")?))
    }

    pub const fn value(self) -> i64 {
        self.0
    }

    pub fn to_msat(self) -> EconResult<Msat> {
        Msat::from_checked(self.0 as i128 * 1000)
    }
}

/// Parts-per-million rate (fee rates, budget shares), `[0, 10_000_000]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ppm(i64);

impl Ppm {
    pub fn new(value: i64) -> EconResult<Self> {
        Ok(Ppm(check_int(value as i128, 0, 10_000_000, "Ppm")?))
    }

    pub const fn value(self) -> i64 {
        self.0
    }

    /// `-(-(amount*ppm) // 1_000_000)` in Python == ceiling division here,
    /// since both operands are non-negative. Uses `u128::div_ceil` (stable)
    /// rather than the signed `i128::div_ceil` (still library-unstable on
    /// this toolchain) — safe because `amount` and `ppm` are both u63/Ppm
    /// range, i.e. non-negative.
    pub fn fee_ceil(self, amount: Msat) -> EconResult<Msat> {
        let product = amount.0 as u128 * self.0 as u128;
        Msat::from_checked(product.div_ceil(1_000_000) as i128)
    }

    pub fn fee_floor(self, amount: Msat) -> EconResult<Msat> {
        let product = amount.0 as i128 * self.0 as i128;
        Msat::from_checked(product / 1_000_000)
    }
}

/// Fixed-point ratio in `[0, 1]` scaled by `1_000_000` (confidence).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Micro(i64);

impl Micro {
    pub fn new(value: i64) -> EconResult<Self> {
        Ok(Micro(check_int(value as i128, 0, 1_000_000, "Micro")?))
    }

    /// THE only float ingress in `econ_types`: clamp to `[0, 1]` then round
    /// HALF-UP by truncation — Python `int(clamped * 1e6 + 0.5)`.
    /// (`econ_ev` uses banker's rounding instead for its own float ingress
    /// points — do not unify the two.)
    pub fn from_float_clamped(f: f64) -> EconResult<Self> {
        if !f.is_finite() {
            return Err(EconError {
                msg: format!("Micro.from_float_clamped: {f:?}"),
            });
        }
        let clamped = f.clamp(0.0, 1.0);
        Ok(Micro((clamped * 1_000_000.0 + 0.5) as i64))
    }

    pub const fn value(self) -> i64 {
        self.0
    }
}

/// Integer unix seconds, UTC, `[0, 2**63 - 1]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnixTime(i64);

impl UnixTime {
    pub fn new(value: i64) -> EconResult<Self> {
        Ok(UnixTime(check_int(
            value as i128,
            0,
            U63_MAX as i128,
            "UnixTime",
        )?))
    }

    pub const fn value(self) -> i64 {
        self.0
    }

    pub fn plus_seconds(self, seconds: i64) -> EconResult<UnixTime> {
        check_int(seconds as i128, 0, U63_MAX as i128, "UnixTime.plus_seconds")?;
        Ok(UnixTime(check_int(
            self.0 as i128 + seconds as i128,
            0,
            U63_MAX as i128,
            "UnixTime",
        )?))
    }
}

fn is_lower_hex(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn is_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Short channel id, `NNNxNNNxN` — three non-empty decimal-digit runs
/// separated by literal `x`, mirroring `^[0-9]+x[0-9]+x[0-9]+$`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChannelId(String);

impl ChannelId {
    pub fn new(value: impl Into<String>) -> EconResult<Self> {
        let value = value.into();
        let parts: Vec<&str> = value.split('x').collect();
        if parts.len() == 3 && parts.iter().all(|p| is_digits(p)) {
            Ok(ChannelId(value))
        } else {
            Err(EconError {
                msg: format!("ChannelId invalid: {value:?}"),
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Compressed node pubkey: 66 lowercase-hex chars, starting `02` or `03`,
/// mirroring `^0[23][0-9a-f]{64}$`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(String);

impl PeerId {
    pub fn new(value: impl Into<String>) -> EconResult<Self> {
        let value = value.into();
        let valid = value.len() == 66
            && (value.starts_with("02") || value.starts_with("03"))
            && is_lower_hex(&value);
        if valid {
            Ok(PeerId(value))
        } else {
            Err(EconError {
                msg: format!("PeerId invalid: {value:?}"),
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Deterministic intent identifier: 1-64 chars of lowercase letters, digits,
/// or dashes, mirroring `^[a-z0-9-]{1,64}$`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IntentId(String);

impl IntentId {
    pub fn new(value: impl Into<String>) -> EconResult<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase() || b == b'-');
        if valid {
            Ok(IntentId(value))
        } else {
            Err(EconError {
                msg: format!("IntentId invalid: {value:?}"),
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IntentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Msat bounds (corpus s32: numeric-overflow-underflow) ---

    #[test]
    fn msat_rejects_2_pow_63() {
        assert!(Msat::from_checked(1i128 << 63).is_err());
    }

    #[test]
    fn msat_rejects_negative_one() {
        assert!(Msat::from_checked(-1).is_err());
        assert!(Msat::new(-1).is_err());
    }

    #[test]
    fn msat_accepts_zero_and_u63_max() {
        assert!(Msat::new(0).is_ok());
        assert!(Msat::new(U63_MAX).is_ok());
    }

    #[test]
    fn msat_sub_underflow_rejected() {
        let a = Msat::new(5).unwrap();
        let b = Msat::new(10).unwrap();
        assert!(a.sub(b).is_err());
    }

    #[test]
    fn msat_add_overflow_rejected() {
        let a = Msat::new(U63_MAX).unwrap();
        let b = Msat::new(1).unwrap();
        assert!(a.add(b).is_err());
    }

    // --- msat/sat rounding boundaries (corpus s33) ---

    #[test]
    fn to_sats_floor_and_ceil_boundaries() {
        let inputs = [999i64, 1000, 1001, 1999];
        let floor: Vec<i64> = inputs
            .iter()
            .map(|&m| Msat::new(m).unwrap().to_sats_floor().value())
            .collect();
        let ceil: Vec<i64> = inputs
            .iter()
            .map(|&m| Msat::new(m).unwrap().to_sats_ceil().value())
            .collect();
        assert_eq!(floor, vec![0, 1, 1, 1]);
        assert_eq!(ceil, vec![1, 1, 2, 2]);
    }

    #[test]
    fn signed_msat_to_sats_toward_zero() {
        assert_eq!(SignedMsat(-1999).to_sats_toward_zero(), -1);
        assert_eq!(SignedMsat(1999).to_sats_toward_zero(), 1);
        assert_eq!(SignedMsat(-1000).to_sats_toward_zero(), -1);
        assert_eq!(SignedMsat(0).to_sats_toward_zero(), 0);
    }

    // --- Ppm ---

    #[test]
    fn ppm_fee_ceil_and_floor() {
        let ppm = Ppm::new(1500).unwrap(); // 0.15%
        let amount = Msat::new(1_000_000).unwrap();
        // 1_000_000 * 1500 / 1_000_000 = 1500 exactly.
        assert_eq!(ppm.fee_ceil(amount).unwrap().value(), 1500);
        assert_eq!(ppm.fee_floor(amount).unwrap().value(), 1500);

        let odd = Msat::new(7).unwrap();
        let ppm2 = Ppm::new(3).unwrap();
        // 7*3 = 21; 21/1_000_000 floors to 0, ceils to 1.
        assert_eq!(ppm2.fee_floor(odd).unwrap().value(), 0);
        assert_eq!(ppm2.fee_ceil(odd).unwrap().value(), 1);
    }

    #[test]
    fn ppm_out_of_range_rejected() {
        assert!(Ppm::new(-1).is_err());
        assert!(Ppm::new(10_000_001).is_err());
    }

    // --- Micro ---

    #[test]
    fn micro_from_float_clamped_half_up() {
        // Golden values re-verified against the actual Python computation
        // (IEEE-754 double arithmetic, not the exact decimal): the brief's
        // illustrative "0.5000005 -> 500001" does not hold once you compute
        // `0.5000005 * 1e6` in real f64 (it lands at 500000.4999...9994, not
        // 500000.5), so both Python and Rust truncate that one to 500000 —
        // confirmed identical via `python3 -c "print(int(0.5000005*1e6+0.5))"`
        // => 500000. These two pairs instead pin exact `.5` boundaries where
        // half-up must round away from zero (500000.5 -> 500001, and
        // 123456.5 -> 123457 — the latter would come out 123456 under
        // banker's rounding, the deliberate contrast with `econ_ev`'s
        // rounding rule).
        assert_eq!(Micro::from_float_clamped(0.0000005).unwrap().value(), 1);
        assert_eq!(
            Micro::from_float_clamped(0.1234565).unwrap().value(),
            123457
        );
        // Re-confirm the brief's literal example against real f64 semantics
        // rather than silently dropping it.
        assert_eq!(
            Micro::from_float_clamped(0.5000005).unwrap().value(),
            500000
        );
    }

    #[test]
    fn micro_from_float_clamped_clamps_range() {
        assert_eq!(Micro::from_float_clamped(-5.0).unwrap().value(), 0);
        assert_eq!(Micro::from_float_clamped(5.0).unwrap().value(), 1_000_000);
    }

    #[test]
    fn micro_from_float_clamped_rejects_non_finite() {
        assert!(Micro::from_float_clamped(f64::NAN).is_err());
        assert!(Micro::from_float_clamped(f64::INFINITY).is_err());
        assert!(Micro::from_float_clamped(f64::NEG_INFINITY).is_err());
    }

    // --- UnixTime ---

    #[test]
    fn unix_time_plus_seconds_checked() {
        let t = UnixTime::new(1_752_400_000).unwrap();
        assert_eq!(t.plus_seconds(600).unwrap().value(), 1_752_400_600);
        assert!(t.plus_seconds(-1).is_err());
        assert!(UnixTime::new(U63_MAX).unwrap().plus_seconds(1).is_err());
    }

    // --- ID validators ---

    #[test]
    fn channel_id_accepts_valid() {
        assert!(ChannelId::new("111x222x0").is_ok());
    }

    #[test]
    fn channel_id_rejects_invalid() {
        assert!(ChannelId::new("111x222").is_err());
        assert!(ChannelId::new("111Xx222x0").is_err());
        assert!(ChannelId::new("").is_err());
        assert!(ChannelId::new("x1x1").is_err());
    }

    #[test]
    fn peer_id_accepts_valid() {
        let pk = format!("02{}", "b".repeat(64));
        assert!(PeerId::new(pk).is_ok());
        let pk3 = format!("03{}", "a".repeat(64));
        assert!(PeerId::new(pk3).is_ok());
    }

    #[test]
    fn peer_id_rejects_uppercase_hex() {
        let pk = format!("02{}", "B".repeat(64));
        assert!(PeerId::new(pk).is_err());
    }

    #[test]
    fn peer_id_rejects_bad_prefix() {
        let pk = format!("04{}", "b".repeat(64));
        assert!(PeerId::new(pk).is_err());
    }

    #[test]
    fn peer_id_rejects_wrong_length() {
        let pk = format!("02{}", "b".repeat(63));
        assert!(PeerId::new(pk).is_err());
    }

    #[test]
    fn intent_id_accepts_valid() {
        assert!(IntentId::new("int-0123abc").is_ok());
        assert!(IntentId::new("a".repeat(64)).is_ok());
    }

    #[test]
    fn intent_id_rejects_empty() {
        assert!(IntentId::new("").is_err());
    }

    #[test]
    fn intent_id_rejects_uppercase_and_too_long() {
        assert!(IntentId::new("ABC").is_err());
        assert!(IntentId::new("a".repeat(65)).is_err());
    }
}
