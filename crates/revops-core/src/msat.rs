//! msat parsing and directional rounding, mirroring Python
//! `modules/utils.py` (parse_msat / base_to_sats_ceil / base_to_sats_floor
//! / sats_to_base) exactly, including its rounding direction contract:
//! ceil for fees/costs/revenue, floor for balances, exact for sats->msat.
//!
//! Parity with the Python implementation is enforced by
//! `tests/rounding_parity.rs` against fixtures generated from the real
//! Python functions (see `tools/port/gen_rounding_fixtures.py` in the
//! `cl_revenue_ops-port` worktree).

use serde_json::Value;

/// Convert base units (msat) to sats, rounding UP.
///
/// Use for: fees, budgets, costs, revenue — never undercharge, underbudget,
/// or hide sub-sat earnings by truncating to zero.
pub fn base_to_sats_ceil(msat: u64) -> u64 {
    msat.div_ceil(1000)
}

/// Convert base units (msat) to sats, rounding DOWN.
///
/// Use for: capacity and balances — never overstate what is spendable.
pub fn base_to_sats_floor(msat: u64) -> u64 {
    msat / 1000
}

/// Convert sats to base units (msat today). Exact, no rounding involved.
pub fn sats_to_base(sats: u64) -> u64 {
    sats * 1000
}

/// Mirrors Python `utils.parse_msat`: never errors, returns 0 on anything
/// unparseable.
///
/// - `null` -> 0
/// - bool -> 0 (Python's U-1 fix: `True`/`False` are never valid msat values)
/// - number -> for integers, passthrough (including negative); for floats,
///   truncate toward zero (Python's `int(x)` semantics)
/// - string -> trimmed, an optional trailing "msat" suffix stripped, then
///   parsed as a base-10 integer; 0 if that fails (Python does not accept
///   float-looking strings here, matching `int(s)`)
/// - anything else (arrays, objects, etc.) -> 0
///
/// Note: Python's `parse_msat` also duck-types pyln `Millisatoshi`-like
/// objects via a `.millisatoshis` attribute. That form has no JSON
/// representation and is out of scope here.
pub fn parse_msat(v: &Value) -> i64 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 0,
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i
            } else if let Some(f) = n.as_f64() {
                f.trunc() as i64
            } else {
                0
            }
        }
        Value::String(s) => {
            let t = s.trim();
            let t = t.strip_suffix("msat").unwrap_or(t);
            t.parse::<i64>().unwrap_or(0)
        }
        _ => 0,
    }
}
