//! htlcmax admission valve (port of `modules/admission_policy.py`).
//!
//! Owns the dynamic `htlc_max` valve: how large an HTLC each channel
//! advertises it will accept, based on flow role, spendable liquidity, and
//! the churn deadband. Pure functions of (cfg, capacity, spendable, flow
//! state): no RPC, no DB, no clock.
//!
//! Golden parity target: `fixtures/golden/htlcmax/*.json` — byte-identical
//! to `~/bin/cl_revenue_ops-port/tests/golden/fixtures/htlcmax/`.

use serde_json::Value;

/// E-1 valve constant: live-depletion cap fraction of spendable outbound
/// (`admission_policy.py` line 21).
pub const DEPLETION_SPENDABLE_FRACTION: f64 = 0.85;
/// E-1 valve constant: never advertise below 10k sats (`admission_policy.py`
/// line 22).
pub const FLOOR_MSAT: i64 = 10_000_000;
/// E-1 churn guard: an htlcmax delta alone forces a broadcast only when it
/// moves more than this fraction of the currently advertised value
/// (`fee_controller.py` `HTLCMAX_UPDATE_DEADBAND_FRAC`, `admission_policy.py`
/// line 23).
pub const UPDATE_DEADBAND_FRAC: f64 = 0.10;

/// Config slice for the htlcmax valve. `enable_dynamic_htlcmax` carries the
/// raw config value because Python's enable check has a specific, narrow
/// truthiness rule (see [`is_enabled`]) that does not collapse to a plain
/// bool at the config layer.
pub struct HtlcmaxCfg {
    pub enable_dynamic_htlcmax: Value,
    pub htlcmax_source_pct: f64,
    pub htlcmax_sink_pct: f64,
    pub htlcmax_balanced_pct: f64,
}

/// Port of the enable check in `admission_policy.compute_htlcmax_msat`
/// (lines 33-38):
///
/// ```python
/// enabled = getattr(cfg, 'enable_dynamic_htlcmax', False)
/// if isinstance(enabled, str):
///     enabled = enabled.lower() in ("true", "1", "yes")
/// else:
///     enabled = enabled is True
/// ```
///
/// A string is enabling iff its lowercased form is "true"/"1"/"yes". Any
/// non-string value is enabling iff it is the actual Python bool `True` —
/// Python's `is True` identity check means truthy ints (e.g. `1`) do NOT
/// enable the valve, unlike a plain `bool(x)` coercion would. JSON has no
/// int/bool identity distinction the way Python does (`1 is True` is
/// `False` in CPython even though `1 == True`), so we model it directly:
/// only `Value::Bool(true)` or a matching string enables; everything else
/// (numbers including `1`, null, arrays, objects, `Bool(false)`) disables.
fn is_enabled(v: &Value) -> bool {
    match v {
        Value::String(s) => matches!(s.to_lowercase().as_str(), "true" | "1" | "yes"),
        Value::Bool(b) => *b,
        _ => false,
    }
}

/// Port of `admission_policy.compute_htlcmax_msat` (lines 26-57).
///
/// `capacity_sats` is the channel's on-chain capacity in sats (as reported
/// by `channel_info["capacity"]`); `spendable_msat` is already in msat
/// (`channel_info["spendable_msat"]`, pre-parsed — see `parse_msat`).
///
/// Returns `None` when disabled or capacity <= 0. Otherwise: `target =
/// int(capacity_msat * pct)` — an f64 multiply then trunc-toward-zero cast,
/// NOT integer math, to match Python's `int()` on a float product. Then
/// `min(target, int(spendable_msat * 0.85))`, then `max(FLOOR_MSAT,
/// min(target, capacity_msat))`.
pub fn compute_htlcmax_msat(
    cfg: &HtlcmaxCfg,
    capacity_sats: i64,
    spendable_msat: i64,
    flow_state: &str,
) -> Option<i64> {
    if !is_enabled(&cfg.enable_dynamic_htlcmax) {
        return None;
    }

    // sats_to_base: capacity (sats) -> msat.
    let capacity_msat = capacity_sats * 1000;
    if capacity_msat <= 0 {
        return None;
    }

    let pct = match flow_state {
        "source" => cfg.htlcmax_source_pct,
        "sink" => cfg.htlcmax_sink_pct,
        _ => cfg.htlcmax_balanced_pct,
    };
    // Python: int(capacity_msat * pct) — trunc toward zero on the f64
    // product, exactly as `as i64` does for finite values here.
    let mut target_msat = ((capacity_msat as f64) * pct) as i64;

    // E-1: live-depletion cap — spendable outbound is what can actually
    // forward; advertising more invites doomed HTLCs.
    let depletion_cap_msat = ((spendable_msat as f64) * DEPLETION_SPENDABLE_FRACTION) as i64;
    target_msat = target_msat.min(depletion_cap_msat);

    // Safety bounds: never below 10,000 sats or above capacity.
    Some(FLOOR_MSAT.max(target_msat.min(capacity_msat)))
}

/// Port of `admission_policy.delta_exceeds_deadband` (lines 60-69). True
/// when the htlcmax move is big enough to justify a broadcast on its own
/// (E-1 churn guard).
///
/// Equal -> false; current <= 0 -> true (unset/zero on chain: always
/// advertise the valve); else `|new - current| > current * 0.10` — the
/// comparison promotes `current` (int) to f64 exactly as Python's
/// `int > int * float` does.
pub fn delta_exceeds_deadband(new_msat: i64, current_msat: i64) -> bool {
    if new_msat == current_msat {
        return false;
    }
    if current_msat <= 0 {
        return true;
    }
    ((new_msat - current_msat).unsigned_abs() as f64) > (current_msat as f64) * UPDATE_DEADBAND_FRAC
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> HtlcmaxCfg {
        HtlcmaxCfg {
            enable_dynamic_htlcmax: Value::Bool(true),
            htlcmax_source_pct: 0.85,
            htlcmax_sink_pct: 0.25,
            htlcmax_balanced_pct: 0.50,
        }
    }

    #[test]
    fn is_enabled_rejects_truthy_int() {
        assert!(!is_enabled(&Value::from(1)));
        assert!(!is_enabled(&Value::from(1.0)));
    }

    #[test]
    fn is_enabled_accepts_bool_true_only() {
        assert!(is_enabled(&Value::Bool(true)));
        assert!(!is_enabled(&Value::Bool(false)));
    }

    #[test]
    fn is_enabled_string_variants() {
        for s in ["true", "TRUE", "1", "yes", "YES"] {
            assert!(is_enabled(&Value::String(s.to_string())), "{s}");
        }
        for s in ["false", "0", "no", "2", ""] {
            assert!(!is_enabled(&Value::String(s.to_string())), "{s}");
        }
    }

    #[test]
    fn disabled_via_null_or_object() {
        assert!(!is_enabled(&Value::Null));
        assert!(!is_enabled(&serde_json::json!({})));
    }

    #[test]
    fn smoke_source_ample_spendable() {
        let got = compute_htlcmax_msat(&cfg(), 2_000_000, 1_900_000_000, "source");
        assert_eq!(got, Some(1_615_000_000));
    }
}
