//! Port of the close-fee-gating subsystem from `CapacityPlanner` (py
//! `modules/capacity_planner.py`): feerate extraction (2999-3037), on-chain
//! cost estimation (2968-2997), and the close-fee reserve/cap/feerange plan
//! (2506-2597) — "a shared close-fee-gating subsystem rather than each
//! close path deriving its own cap" (module docstring, py 20-22).

use revops_core::msat::parse_msat;
use serde_json::Value;
use std::collections::BTreeMap;

/// py `_CLOSE_TX_VBYTES = 200` (capacity_planner.py 48).
const CLOSE_TX_VBYTES: i64 = 200;
/// py `_DEFAULT_CLOSE_FEE_RESERVE_MULTIPLIER = 2.0` (capacity_planner.py 49).
pub const DEFAULT_CLOSE_FEE_RESERVE_MULTIPLIER: f64 = 2.0;
/// py `ChainCostDefaults.CHANNEL_OPEN_COST_SATS` (config.py 1445).
pub const CHANNEL_OPEN_COST_SATS_DEFAULT: i64 = 5000;
/// py `ChainCostDefaults.CHANNEL_CLOSE_COST_SATS` (config.py 1446).
pub const CHANNEL_CLOSE_COST_SATS_DEFAULT: i64 = 3000;

// ---------------------------------------------------------------------
// Feerate extraction (py 2999-3037, both `@staticmethod`)
// ---------------------------------------------------------------------

/// CLN `feerates(style="perkb")`'s `result["perkb"]` map. Modeled as
/// already-well-typed (the Python `isinstance(feerates, dict)` /
/// `isinstance(perkb, dict)` guards exist for malformed RPC JSON, which a
/// typed Rust caller resolves at the RPC boundary, not here).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Feerates {
    pub perkb: BTreeMap<String, f64>,
}

/// Port of `_extract_close_feerate_perkb` (py 2999-3022). The planner
/// executes MUTUAL (cooperative) closes, so `mutual_close` is preferred;
/// `unilateral_close` (the higher, conservative commitment-tx rate) is the
/// fallback; `opening` is the last resort. Unlike
/// [`extract_opening_feerate_perkb`], a MISSING key is skipped (no
/// default) — only a present, positive value counts.
pub fn extract_close_feerate_perkb(feerates: Option<&Feerates>) -> Option<f64> {
    let feerates = feerates?;
    for key in ["mutual_close", "unilateral_close", "opening"] {
        if let Some(&value) = feerates.perkb.get(key) {
            if value > 0.0 {
                return Some(value);
            }
        }
    }
    None
}

/// Port of `_extract_opening_feerate_perkb` (py 3024-3037). `"opening"`
/// defaults to `1000` when the key is absent (py `perkb.get("opening",
/// 1000)`) — the ONLY extractor here with a default-if-missing value.
pub fn extract_opening_feerate_perkb(feerates: Option<&Feerates>) -> Option<f64> {
    let feerates = feerates?;
    let value = feerates.perkb.get("opening").copied().unwrap_or(1000.0);
    if value <= 0.0 {
        return None;
    }
    Some(value)
}

/// Port of `_estimate_open_cost` (py 2968-2978), with the RPC call
/// (`data_service.get_feerates` / `plugin.rpc.feerates`) replaced by an
/// injected `feerates` snapshot — `None` mirrors the `except Exception`
/// fallback to `ChainCostDefaults.CHANNEL_OPEN_COST_SATS`.
pub fn estimate_open_cost_sats(feerates: Option<&Feerates>) -> i64 {
    match extract_opening_feerate_perkb(feerates) {
        None => CHANNEL_OPEN_COST_SATS_DEFAULT,
        Some(perkb) => {
            let sat_per_vb = perkb / 1000.0;
            (sat_per_vb * 140.0) as i64 // ~140 vbytes for funding tx
        }
    }
}

/// Port of `_estimate_close_cost` (py 2980-2997): uses the CLOSE-appropriate
/// feerate (E-4.6), 200 vbytes matching the inline closure size elsewhere
/// in the Python module.
pub fn estimate_close_cost_sats(feerates: Option<&Feerates>) -> i64 {
    match extract_close_feerate_perkb(feerates) {
        None => CHANNEL_CLOSE_COST_SATS_DEFAULT,
        Some(perkb) => {
            let sat_per_vb = perkb / 1000.0;
            (sat_per_vb * 200.0) as i64 // ~200 vbytes for close tx
        }
    }
}

// ---------------------------------------------------------------------
// Close-fee plan (py 2506-2597)
// ---------------------------------------------------------------------

/// The config fields `_close_fee_plan` and friends read (py
/// `planner_close_fee_reserve_multiplier` default 2.0,
/// `planner_close_fee_cap_sats` default 0, `planner_close_feerange_enabled`
/// default `False` — config.py 799-801).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CloseFeeConfig {
    pub planner_close_fee_reserve_multiplier: f64,
    pub planner_close_fee_cap_sats: i64,
    pub planner_close_feerange_enabled: bool,
}

impl Default for CloseFeeConfig {
    fn default() -> Self {
        CloseFeeConfig {
            planner_close_fee_reserve_multiplier: DEFAULT_CLOSE_FEE_RESERVE_MULTIPLIER,
            planner_close_fee_cap_sats: 0,
            planner_close_feerange_enabled: false,
        }
    }
}

/// Port of `_close_fee_reserve_multiplier` (py 2506-2517): non-finite
/// (NaN/Inf) values fall back to the default, then the result is clamped
/// to a floor of `1.0` (a reserve can never be LESS than the estimate).
pub fn close_fee_reserve_multiplier(cfg: &CloseFeeConfig) -> f64 {
    let value = cfg.planner_close_fee_reserve_multiplier;
    let value = if value.is_finite() {
        value
    } else {
        DEFAULT_CLOSE_FEE_RESERVE_MULTIPLIER
    };
    value.max(1.0)
}

/// Port of `_configured_close_fee_cap_sats` (py 2519-2527): `0` means "no
/// fixed cap configured" (falls through to the multiplier path).
pub fn configured_close_fee_cap_sats(cfg: &CloseFeeConfig) -> i64 {
    cfg.planner_close_fee_cap_sats.max(0)
}

/// Port of `_close_feerange` (py 2560-2576): an optional CLN `feerange`
/// hint `["slow", "{max_perkb}perkb"]` derived from the fee cap, only when
/// `planner_close_feerange_enabled`.
pub fn close_feerange(cfg: &CloseFeeConfig, fee_cap_sats: i64) -> Option<(String, String)> {
    if !cfg.planner_close_feerange_enabled {
        return None;
    }
    let cap = fee_cap_sats.max(0);
    if cap <= 0 {
        return None;
    }
    // `int(math.floor((cap * 1000) / _CLOSE_TX_VBYTES))`: both operands are
    // positive here, so integer division equals the Python float-floor.
    let max_perkb = ((cap * 1000) / CLOSE_TX_VBYTES).max(1);
    Some(("slow".to_string(), format!("{max_perkb}perkb")))
}

/// Which branch of [`close_fee_plan`] produced the reserve amount (py's
/// `source: "fixed_cap" | "multiplier"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseFeeSource {
    FixedCap,
    Multiplier,
}

impl CloseFeeSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            CloseFeeSource::FixedCap => "fixed_cap",
            CloseFeeSource::Multiplier => "multiplier",
        }
    }
}

/// Port of the `_close_fee_plan` return dict (py 2529-2558).
#[derive(Debug, Clone, PartialEq)]
pub struct CloseFeePlan {
    pub ok: bool,
    pub estimated_cost_sats: i64,
    pub reserve_sats: i64,
    pub fee_cap_sats: i64,
    pub source: CloseFeeSource,
    pub feerange: Option<(String, String)>,
    /// Only set when `ok == false` (a configured cap below the estimate).
    pub error: Option<String>,
}

/// Port of `_close_fee_plan` (py 2529-2558). `estimated_close_cost_sats` is
/// the output of [`estimate_close_cost_sats`] (Python calls
/// `self._estimate_close_cost()` inline; injected here to keep this
/// function pure).
pub fn close_fee_plan(cfg: &CloseFeeConfig, estimated_close_cost_sats: i64) -> CloseFeePlan {
    let estimated = estimated_close_cost_sats.max(1);
    let configured_cap = configured_close_fee_cap_sats(cfg);

    if configured_cap > 0 {
        if configured_cap < estimated {
            return CloseFeePlan {
                ok: false,
                estimated_cost_sats: estimated,
                reserve_sats: configured_cap,
                fee_cap_sats: configured_cap,
                source: CloseFeeSource::FixedCap,
                feerange: None,
                error: Some(format!(
                    "Configured close fee cap {configured_cap} sats is below \
                     estimated close cost {estimated} sats"
                )),
            };
        }
        let reserve_sats = configured_cap;
        return CloseFeePlan {
            ok: true,
            estimated_cost_sats: estimated,
            reserve_sats,
            fee_cap_sats: reserve_sats,
            source: CloseFeeSource::FixedCap,
            feerange: close_feerange(cfg, reserve_sats),
            error: None,
        };
    }

    let multiplier = close_fee_reserve_multiplier(cfg);
    let reserve_sats = estimated.max((estimated as f64 * multiplier).ceil() as i64);
    CloseFeePlan {
        ok: true,
        estimated_cost_sats: estimated,
        reserve_sats,
        fee_cap_sats: reserve_sats,
        source: CloseFeeSource::Multiplier,
        feerange: close_feerange(cfg, reserve_sats),
        error: None,
    }
}

// ---------------------------------------------------------------------
// Extract actual close fee from a `close` RPC result (py 2578-2597)
// ---------------------------------------------------------------------

/// Port of `_extract_actual_close_fee_sats` (py 2578-2597): defensively
/// parses a CLN `close` RPC result across several possible field-name/unit
/// spellings.
///
/// # A Python quirk this must reproduce exactly
///
/// The second (msat) loop calls `parse_msat(result.get(key))` (py 2594)
/// unconditionally — `dict.get` returns `None` for an absent key, and
/// `parse_msat(None)` is `0` (never an error), and `0 >= 0` is always true.
/// So the loop returns `Some(0)` on its VERY FIRST key
/// (`"actual_fee_msat"`) whenever that key is merely absent, regardless of
/// whether a later key in the same loop would have matched a real value —
/// the trailing `return None` (py 2597) is **dead code for any dict
/// input**; it is only reached when `result` itself is not a JSON object.
/// Verified against the real Python function: a `{"foo": "bar"}` result
/// (no matching field at all) returns `0`, not `None`.
pub fn extract_actual_close_fee_sats(result: &Value) -> Option<i64> {
    let obj = result.as_object()?;

    for key in [
        "actual_fee_sats",
        "close_fee_sats",
        "closing_fee_sats",
        "fee_sats",
    ] {
        let Some(value) = obj.get(key) else { continue };
        if value.is_boolean() {
            continue;
        }
        if let Some(n) = value.as_i64() {
            if n >= 0 {
                return Some(n);
            }
            continue;
        }
        if let Some(f) = value.as_f64() {
            if f >= 0.0 {
                return Some(f.trunc() as i64);
            }
            continue;
        }
        if let Some(s) = value.as_str() {
            let lower = s.trim().to_lowercase();
            let stripped = lower.strip_suffix("sat").unwrap_or(&lower).trim();
            if !stripped.is_empty() && stripped.chars().all(|c| c.is_ascii_digit()) {
                if let Ok(n) = stripped.parse::<i64>() {
                    return Some(n);
                }
            }
        }
    }

    for key in [
        "actual_fee_msat",
        "close_fee_msat",
        "closing_fee_msat",
        "fee_msat",
        "fees_msat",
    ] {
        // `.get(key)` defaults to `Value::Null` for an absent key — matching
        // Python's `dict.get(key)` returning `None` — NOT skipped, per the
        // doc comment above.
        let value = obj.get(key).cloned().unwrap_or(Value::Null);
        let parsed = parse_msat(&value);
        if parsed >= 0 {
            return Some((parsed as f64 / 1000.0).ceil() as i64);
        }
    }

    None
}
