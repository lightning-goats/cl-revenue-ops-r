//! Task 71 slice A — the P&L / ROC / TLV evidence producers.
//!
//! Ports `get_pnl_summary` (py profitability_analyzer.py 1441-1499),
//! `calculate_roc` (1817-1866) and `get_tlv` (1867-1915). These three feed
//! profitability, econ-snapshot and the dashboard, so task 66 can wire
//! canonical responses against real evidence instead of `not_yet_ported`.
//!
//! **Disclosed divergence:** Python's `get_tlv` catches an RPC failure,
//! logs a warning, and returns ZEROS. This module refuses instead. A
//! zeroed TLV is indistinguishable from a node that genuinely holds
//! nothing, and TLV is the headline net-worth figure — reporting zero net
//! worth because an RPC timed out is a false statement about the
//! operator's money. Task 71 forbids fabricated zeros explicitly.

use revops_core::msat::{base_to_sats_floor, parse_msat};
use revops_db::queries::PnlSummary;
use revops_econ::pyfloat::py_round;
use serde_json::Value;

/// Review finding F71-R6: presence is not validity.
///
/// `revops_core::msat::parse_msat` is deliberately permissive -- it returns
/// 0 for null, bools, arrays, objects and unparseable strings, which is the
/// right global contract for a tolerant reader. At THIS boundary that
/// permissiveness fabricates money evidence: `amount_msat: "garbage"` would
/// become a confident zero. So required amounts are shape-validated first
/// and only then handed to the canonical parser; `parse_msat` itself is
/// unchanged and remains the sole accepted-format authority.
///
/// Accepted, matching what CLN actually emits: a JSON integer, or a string
/// of optional sign + digits with an optional `msat` suffix. A VALID zero
/// stays a measured zero -- an empty channel and a corrupt one are
/// different facts.
fn validated_msat(v: &Value, what: &str) -> Result<i64, EconRefusal> {
    // Shape check ONLY -- the value itself still comes from the canonical
    // `parse_msat` below, which stays the sole accepted-format authority
    // (F71-R6's instruction: validate here, do not fork the parser).
    let acceptable = match v {
        // F71-R7: `is_u64()` alone would admit values above i64::MAX,
        // which the permissive parser then truncates -- fabricating a
        // number instead of reporting unusable evidence.
        Value::Number(n) => n.as_u64().is_some_and(|u| i64::try_from(u).is_ok()),
        Value::String(s) => {
            let body = s.trim().strip_suffix("msat").unwrap_or(s.trim());
            // F71-R7: no sign is accepted at all. listfunds amounts are
            // non-negative, and a negative would otherwise be silently
            // clamped to zero by the floor conversion. Rejecting the '-'
            // outright also stops "-1msat" slipping through.
            !body.is_empty()
                && body.bytes().all(|b| b.is_ascii_digit())
                && body.parse::<i64>().is_ok()
        }
        _ => false,
    };
    if !acceptable {
        return Err(EconRefusal::MalformedListfunds(format!(
            "listfunds {what} is not a valid non-negative msat value: {v}"
        )));
    }
    Ok(parse_msat(v))
}

/// Balances round DOWN (`base_to_sats_floor`), from `revops-core`, whose
/// rounding is fixture-verified against the real Python functions. Guarded
/// for negatives because a malformed remote share can compute below zero
/// before the structural checks reject it.
fn msat_to_sats_floor(msat: i64) -> i64 {
    if msat <= 0 {
        return 0;
    }
    base_to_sats_floor(msat as u64) as i64
}

// P&L lives in `revops_db::queries::pnl_summary` -- the canonical,
// Python-parity, DB-backed authority already consumed by `rpc_dashboard`
// and `rpc_health`. Review finding F71-R2: this module briefly carried a
// second implementation of the same financial contract. Two authorities
// for one contract drift, and the duplicate also bypassed the intended
// store routing. `calculate_roc` consumes the canonical type directly;
// `econ_evidence.rs` deliberately contains NO P&L arithmetic, which
// `tests/econ_evidence.rs::pnl_arithmetic_has_exactly_one_authority`
// pins structurally.

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RocSummary {
    pub window_days: i64,
    pub total_capacity_sats: i64,
    pub net_profit_sats: i64,
    pub roc_pct: f64,
    pub annualized_roc_pct: f64,
}

/// Port of `calculate_roc` (py 1817-1866): return on deployed capital,
/// annualized.
pub fn calculate_roc(pnl: PnlSummary, total_capacity_sats: i64, window_days: i64) -> RocSummary {
    // py 1830: clamped BEFORE annualizing, which is what prevents the
    // division by zero.
    let window_days = window_days.max(1);

    // Zero capacity is a genuine 0.0 -- a node with no channels really has
    // no return on deployed capital. This zero is measured, unlike TLV's
    // refusal above.
    let (roc_pct, annualized_roc_pct) = if total_capacity_sats > 0 {
        let roc = (pnl.net_profit_sats as f64 / total_capacity_sats as f64) * 100.0;
        (roc, roc * (365.0 / window_days as f64))
    } else {
        (0.0, 0.0)
    };

    RocSummary {
        window_days,
        total_capacity_sats,
        net_profit_sats: pnl.net_profit_sats,
        roc_pct: py_round(roc_pct, 4),
        annualized_roc_pct: py_round(annualized_roc_pct, 2),
    }
}

/// The `listfunds` reply, or the reason it could not be read.
pub struct TlvSources {
    pub listfunds: Result<Value, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TlvSummary {
    pub onchain_sats: i64,
    pub local_balance_sats: i64,
    pub remote_balance_sats: i64,
    pub tlv_sats: i64,
    pub channel_count: i64,
}

/// Typed econ refusals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EconRefusal {
    ListfundsUnavailable(String),
    /// Review finding F71-R3: the read SUCCEEDED but the payload cannot
    /// support the claim. Optional traversal plus parse-to-zero defaults
    /// would turn each of these into a success-shaped zero -- the same
    /// false statement about the operator's money the failure case makes,
    /// only harder to notice, because nothing failed.
    MalformedListfunds(String),
}

impl EconRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ListfundsUnavailable(_) => "econ_listfunds_unavailable",
            Self::MalformedListfunds(_) => "econ_listfunds_malformed",
        }
    }

    pub fn detail(&self) -> &str {
        match self {
            Self::ListfundsUnavailable(d) | Self::MalformedListfunds(d) => d,
        }
    }
}

/// Port of `get_tlv` (py 1867-1915) — the node's net worth if every
/// channel were cooperatively closed today.
///
/// Refuses on a failed read rather than returning Python's zeros; see the
/// module doc.
pub fn total_liquidating_value(sources: TlvSources) -> Result<TlvSummary, EconRefusal> {
    let funds = sources
        .listfunds
        .map_err(EconRefusal::ListfundsUnavailable)?;

    let malformed = |what: &str| EconRefusal::MalformedListfunds(format!("listfunds {what}"));

    // Both arrays are REQUIRED. An EMPTY array is real evidence (this node
    // holds nothing); an absent or wrong-typed one is not evidence at all,
    // and silently reading it as empty is how a malformed reply becomes a
    // confident report of zero net worth.
    let outputs = funds
        .get("outputs")
        .ok_or_else(|| malformed("reply has no outputs array"))?
        .as_array()
        .ok_or_else(|| malformed("outputs is not an array"))?;
    let channels = funds
        .get("channels")
        .ok_or_else(|| malformed("reply has no channels array"))?
        .as_array()
        .ok_or_else(|| malformed("channels is not an array"))?;

    let mut onchain_sats = 0i64;
    for output in outputs {
        // Only CONFIRMED outputs count: unconfirmed money is not yet ours.
        // The status filter runs FIRST, so an ignored row's malformed
        // amount can never poison the total or trigger a refusal -- only
        // fields actually depended on are required.
        if output.get("status").and_then(Value::as_str) != Some("confirmed") {
            continue;
        }
        let amount = output
            .get("amount_msat")
            .ok_or_else(|| malformed("confirmed output has no amount_msat"))?;
        onchain_sats += msat_to_sats_floor(validated_msat(amount, "confirmed output amount_msat")?);
    }

    let mut local_balance_sats = 0i64;
    let mut remote_balance_sats = 0i64;
    let mut channel_count = 0i64;

    for channel in channels {
        if channel.get("state").and_then(Value::as_str) != Some("CHANNELD_NORMAL") {
            continue;
        }
        let ours = validated_msat(
            channel
                .get("our_amount_msat")
                .ok_or_else(|| malformed("live channel has no our_amount_msat"))?,
            "live channel our_amount_msat",
        )?;
        let total = validated_msat(
            channel
                .get("amount_msat")
                .ok_or_else(|| malformed("live channel has no amount_msat"))?,
            "live channel amount_msat",
        )?;
        // An impossible split means the evidence is wrong, not that the
        // remote side holds negative sats.
        if ours > total {
            return Err(malformed(&format!(
                "live channel our_amount_msat {ours} exceeds amount_msat {total}"
            )));
        }
        local_balance_sats += msat_to_sats_floor(ours);
        remote_balance_sats += msat_to_sats_floor(total - ours);
        channel_count += 1;
    }

    Ok(TlvSummary {
        onchain_sats,
        local_balance_sats,
        remote_balance_sats,
        // onchain + LOCAL only. Remote balance is the counterparties'
        // money; including it would overstate net worth.
        tlv_sats: onchain_sats + local_balance_sats,
        channel_count,
    })
}
