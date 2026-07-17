//! Evidence-backed floors: chain-cost floor, rebalance floor, flow
//! ceiling, congestion detection, class-aware min fee (port of the floor
//! stack in `modules/fee_controller.py`; clamp ORDER is load-bearing).
//!
//! Every function here is pure over explicit evidence inputs — the cycle
//! orchestrator (Wave 3) is responsible for gathering the underlying
//! RPC/DB evidence (peer latency stats, cost history, flow windows, live
//! HTLC counts) and threading it in; nothing in this module calls out.
//!
//! Mirrors (line numbers from `cl_revenue_ops` v2.18.1
//! `modules/fee_controller.py` unless noted): `_calculate_floor`
//! (8130-8251), `ChainCostDefaults` (`modules/config.py:1423-1468`,
//! transcribed here per the Task 6 brief rather than waiting on a config
//! crate), `_get_dynamic_chain_costs_live` (8253-8311),
//! `_get_rebalance_cost_floor` (4069-4159), `_get_flow_adjusted_ceiling`
//! (4161-4223), `_detect_congestion` (5459-5502), `_effective_min_fee_ppm`
//! (2811-2877), `_is_flow_balanced_router`/`_get_flow_window_map`
//! (2777-2810).

use crate::rails::{
    ZERO_FLOW_DAYS_MODERATE, ZERO_FLOW_DAYS_SEVERE, ZERO_FLOW_FEE_THRESHOLD,
    ZERO_FLOW_REDUCTION_MODERATE, ZERO_FLOW_REDUCTION_SEVERE,
};

// ---------------------------------------------------------------------------
// ChainCostDefaults (modules/config.py:1423-1468) — transcribed verbatim.
// Every constant here is load-bearing (Phase 4 Global Constraints): these
// are the static fallback assumptions used when live feerates are
// unavailable, and the base term the dynamic (live) floor is `max()`-ed
// against.
// ---------------------------------------------------------------------------

/// `ChainCostDefaults.CHANNEL_OPEN_COST_SATS`.
pub const CHANNEL_OPEN_COST_SATS: i64 = 5000;
/// `ChainCostDefaults.CHANNEL_CLOSE_COST_SATS`.
pub const CHANNEL_CLOSE_COST_SATS: i64 = 3000;
/// `ChainCostDefaults.CHANNEL_LIFETIME_DAYS`.
pub const CHANNEL_LIFETIME_DAYS: i64 = 365;
/// `ChainCostDefaults.DAILY_VOLUME_SATS`.
pub const DAILY_VOLUME_SATS: i64 = 1_000_000;

/// `ChainCostDefaults.calculate_floor_ppm` (config.py:1444-1468) verbatim.
///
/// Note: `capacity_sats` is accepted (and forwarded from
/// [`calculate_floor`]) purely for call-site/API parity with the Python
/// classmethod signature — the Python body never reads it either (a
/// vestigial parameter kept for fidelity, not re-added functionality).
pub fn calculate_floor_ppm(_capacity_sats: i64, opener: &str) -> i64 {
    let total_chain_cost = if opener == "remote" {
        CHANNEL_CLOSE_COST_SATS
    } else {
        CHANNEL_OPEN_COST_SATS + CHANNEL_CLOSE_COST_SATS
    };
    let estimated_lifetime_volume = DAILY_VOLUME_SATS * CHANNEL_LIFETIME_DAYS;
    if estimated_lifetime_volume > 0 {
        let floor_ppm = (total_chain_cost as f64 / estimated_lifetime_volume as f64) * 1_000_000.0;
        1.max(floor_ppm as i64)
    } else {
        1
    }
}

/// Live/pre-fetched chain costs (py `chain_costs: Optional[Dict[str, int]]`
/// with keys `open_cost_sats`/`close_cost_sats`/`sat_per_vbyte`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChainCosts {
    pub open_cost_sats: i64,
    pub close_cost_sats: i64,
    pub sat_per_vbyte: f64,
}

/// Per-peer HTLC-hold latency stats (py `database.get_peer_latency_stats`
/// result dict, keys `avg`/`std`, seconds).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PeerLatency {
    pub avg: f64,
    pub std: f64,
}

/// `_calculate_floor` (py 8130-8251) verbatim.
///
/// ADR-001 stage 1 (rails: floor). Conformance scenario 08 replays this
/// (capacity 2_000_000, chain_costs None, opener "local" -> 21).
///
/// Formula: `floor = max(base_floor, risk_premium) * stall_multiplier`,
/// with the stall markup captured early (from peer latency) but applied
/// AFTER the risk-premium `max()` (P8-002 fix, py 8189-8249): the prior
/// code multiplied the base floor before the risk-premium max(), silently
/// dropping the stall markup whenever the risk premium won. Order here is
/// exactly: (1) base floor from `ChainCostDefaults` + live-cost base_floor
/// max, (2) stall_multiplier computed from peer_latency but NOT yet
/// applied, (3) risk premium from `sat_per_vbyte` folded in via max(), (4)
/// stall_multiplier applied to whichever term won, (5) `max(1, int(...))`.
pub fn calculate_floor(
    capacity_sats: i64,
    chain_costs: Option<&ChainCosts>,
    peer_latency: Option<&PeerLatency>,
    opener: &str,
) -> i64 {
    let mut floor_ppm = calculate_floor_ppm(capacity_sats, opener);

    // 1. Base Floor (Cost Recovery) using REPLACEMENT COST (py 8163-8187).
    if let Some(costs) = chain_costs {
        let total_chain_cost = if opener == "remote" {
            costs.close_cost_sats
        } else {
            costs.open_cost_sats + costs.close_cost_sats
        };
        let estimated_lifetime_volume = DAILY_VOLUME_SATS * CHANNEL_LIFETIME_DAYS;
        if estimated_lifetime_volume > 0 {
            let base_floor =
                (total_chain_cost as f64 / estimated_lifetime_volume as f64) * 1_000_000.0;
            floor_ppm = floor_ppm.max(base_floor as i64);
        }
    }

    // 3. HTLC Hold Risk Premium (Stall Defense, py 8189-8217) — captured
    // now, applied later (P8-002).
    let mut stall_multiplier: f64 = 1.0;
    if let Some(latency) = peer_latency {
        if latency.avg > 10.0 || latency.std > 5.0 {
            stall_multiplier = 1.2;
        }
    }

    // 2. Risk Premium (Congestion Defense, py 8219-8243).
    if let Some(costs) = chain_costs {
        let sat_per_vbyte = costs.sat_per_vbyte;
        if sat_per_vbyte > 0.0 {
            const COMMITMENT_TX_VBYTES: f64 = 150.0;
            const AVG_HTLC_SIZE_SATS: f64 = 50_000.0;
            const FORCE_CLOSE_PROBABILITY: f64 = 0.001;
            let expected_enforcement_cost =
                sat_per_vbyte * COMMITMENT_TX_VBYTES * FORCE_CLOSE_PROBABILITY;
            if AVG_HTLC_SIZE_SATS > 0.0 {
                let risk_premium_ppm =
                    (expected_enforcement_cost / AVG_HTLC_SIZE_SATS) * 1_000_000.0;
                floor_ppm = floor_ppm.max(risk_premium_ppm as i64);
            }
        }
    }

    // P8-002: apply the stall markup to whichever term won (py 8245-8249).
    if stall_multiplier != 1.0 {
        floor_ppm = (floor_ppm as f64 * stall_multiplier) as i64;
    }

    floor_ppm.max(1)
}

/// `_get_dynamic_chain_costs_live` (py 8253-8311), made pure over an
/// already-fetched `perkb` feerates map (the RPC call itself is the
/// cycle orchestrator's job).
///
/// Preference chain `opening | mutual_close | unilateral_close | floor |
/// 1000` mirrors Python's `or`-chain dict lookups: a present-but-falsy
/// value (JSON `0`/`null`) is skipped exactly like Python's falsy `0`/
/// `None`, falling through to the next candidate. The final `1000`
/// fallback means this always resolves to a positive `sat_per_kvb`, so
/// the `Option` return is `Some` in every reachable case (kept as `Option`
/// for interface/API symmetry with the fallible live variant).
pub fn dynamic_chain_costs(perkb: &serde_json::Value) -> Option<ChainCosts> {
    fn truthy_f64(v: &serde_json::Value, key: &str) -> Option<f64> {
        v.get(key).and_then(|x| x.as_f64()).filter(|x| *x != 0.0)
    }

    let sat_per_kvb = truthy_f64(perkb, "opening")
        .or_else(|| truthy_f64(perkb, "mutual_close"))
        .or_else(|| truthy_f64(perkb, "unilateral_close"))
        .or_else(|| truthy_f64(perkb, "floor"))
        .unwrap_or(1000.0);

    let sat_per_vbyte = sat_per_kvb / 1000.0;

    const FUNDING_TX_VBYTES: f64 = 140.0;
    const CLOSE_TX_VBYTES: f64 = 200.0;

    let open_cost_sats = ((sat_per_vbyte * FUNDING_TX_VBYTES) as i64).clamp(500, 50_000);
    let close_cost_sats = ((sat_per_vbyte * CLOSE_TX_VBYTES) as i64).clamp(300, 50_000);

    Some(ChainCosts {
        open_cost_sats,
        close_cost_sats,
        sat_per_vbyte,
    })
}

// ---------------------------------------------------------------------------
// Rebalance cost floor (Issue #32) — py 4069-4159.
// ---------------------------------------------------------------------------

/// `FeeController.REBALANCE_FLOOR_WINDOW_DAYS` (py 2620).
pub const REBALANCE_FLOOR_WINDOW_DAYS: i64 = 30;
/// `FeeController.REBALANCE_FLOOR_MIN_SAMPLES` (py 2619).
pub const REBALANCE_FLOOR_MIN_SAMPLES: usize = 4;
/// `FeeController.REBALANCE_FLOOR_MARGIN` (py 2613).
pub const REBALANCE_FLOOR_MARGIN: f64 = 1.20;

/// One row of `database.get_channel_cost_history` (py dict keys
/// `cost_sats`/`amount_sats`/`timestamp`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RebalanceCostSample {
    pub cost_sats: i64,
    pub amount_sats: i64,
    pub timestamp: i64,
}

/// `_get_rebalance_cost_floor` (py 4069-4159) verbatim.
///
/// `recent_costs` is the caller-filtered-or-not history; this function
/// re-applies the `timestamp >= cutoff` filter itself exactly as Python
/// does (py 4116), so passing the full history is safe. `peer_fallback`
/// is `(confidence, avg_fee_ppm)` from `get_historical_inbound_fee_ppm`
/// (py 4142-4146) when available.
///
/// No cap at 5000 PPM here: that cap belongs to a DIFFERENT, sibling
/// function (`_get_channel_rebalance_cost_ppm`, py 3554-3594, `return
/// min(5000, cost_ppm)` at 3592) used for the *soft nudge target*
/// elsewhere in `_adjust_channel_fee` — verified by reading both call
/// sites; `_get_rebalance_cost_floor` (the HARD floor this function
/// ports) has no such cap in its own body.
pub fn rebalance_cost_floor(
    flow_state: &str,
    recent_costs: &[RebalanceCostSample],
    peer_fallback: Option<(&str, i64)>,
    now: i64,
) -> Option<i64> {
    // Sinks fill from inbound; dormant channels have no flow to amortize
    // costs against (py 4102-4103).
    if flow_state == "sink" || flow_state == "dormant" {
        return None;
    }

    let cutoff = now - REBALANCE_FLOOR_WINDOW_DAYS * 86400;
    let filtered: Vec<&RebalanceCostSample> = recent_costs
        .iter()
        .filter(|c| c.timestamp >= cutoff)
        .collect();

    // Strategy 1: per-channel cost history.
    if filtered.len() >= REBALANCE_FLOOR_MIN_SAMPLES {
        let total_cost: i64 = filtered.iter().map(|c| c.cost_sats).sum();
        let total_volume: i64 = filtered.iter().map(|c| c.amount_sats).sum();

        if total_volume > 0 {
            // Integer floor division of positives (py `//`, py 4123):
            // Rust `/` on non-negative i64 truncates toward zero, which
            // equals floor division for non-negative operands.
            let cost_ppm = (total_cost * 1_000_000) / total_volume;
            let floor_ppm = (cost_ppm as f64 * REBALANCE_FLOOR_MARGIN) as i64;
            return Some(floor_ppm);
        }
    }

    // Strategy 2: per-peer fallback (cold-start), py 4141-4157.
    if let Some((confidence, avg_fee_ppm)) = peer_fallback {
        if confidence == "medium" || confidence == "high" {
            let cost_ppm = avg_fee_ppm;
            if cost_ppm > 0 {
                let floor_ppm = (cost_ppm as f64 * REBALANCE_FLOOR_MARGIN) as i64;
                return Some(floor_ppm);
            }
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Flow-adjusted ceiling (Issue #20) — py 4161-4223. Reuses the zero-flow
// day-boundary/reduction constants already pinned in `crate::rails` (same
// `FeeController` class body, `ZERO_FLOW_*` section) rather than
// re-declaring them here.
// ---------------------------------------------------------------------------

/// `_get_flow_adjusted_ceiling` (py 4161-4223) verbatim.
///
/// `last_forward_ts` is `None` for "no forwards recorded" (py
/// `last_forward_ts is None`) and `Some(0)` for the sentinel "never
/// forwarded" value Python also treats as absent (py `... or
/// last_forward_ts == 0`) — both return `base_ceiling` unchanged
/// (conservative: don't penalize new channels).
pub fn flow_adjusted_ceiling(
    current_fee: i64,
    base_ceiling: i64,
    last_forward_ts: Option<i64>,
    now: i64,
) -> i64 {
    if current_fee < ZERO_FLOW_FEE_THRESHOLD {
        return base_ceiling;
    }

    let last_forward_ts = match last_forward_ts {
        None => return base_ceiling,
        Some(0) => return base_ceiling,
        Some(ts) => ts,
    };

    let days_since_forward = (now - last_forward_ts) as f64 / 86400.0;

    if days_since_forward >= ZERO_FLOW_DAYS_SEVERE as f64 {
        1.max((base_ceiling as f64 * ZERO_FLOW_REDUCTION_SEVERE) as i64)
    } else if days_since_forward >= ZERO_FLOW_DAYS_MODERATE as f64 {
        1.max((base_ceiling as f64 * ZERO_FLOW_REDUCTION_MODERATE) as i64)
    } else {
        base_ceiling
    }
}

// ---------------------------------------------------------------------------
// Congestion detection (F4, 2026-06 audit) — py 5459-5502.
// ---------------------------------------------------------------------------

/// Live channel-info HTLC utilization evidence (py `channel_info` dict
/// subset: `has_htlc_data`/`max_accepted_htlcs`/`our_htlcs_in_flight`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LiveHtlc {
    pub has_htlc_data: bool,
    pub max_accepted_htlcs: i64,
    pub our_htlcs_in_flight: i64,
}

/// Hourly flow-snapshot congestion label (py `state` dict subset:
/// `state`/`updated_at`).
#[derive(Debug, Clone, PartialEq)]
pub struct FlowStateRow {
    pub state: Option<String>,
    pub updated_at: Option<i64>,
}

/// `_detect_congestion` (py 5459-5502) verbatim.
///
/// Resolution order: (a) live HTLC data is authoritative when present and
/// usable (`has_htlc_data` AND `max_accepted_htlcs > 0`) — recompute
/// utilization now, counting only OUR-direction in-flight HTLCs; (b)
/// otherwise fall back to the hourly snapshot label, ignoring it once
/// stale (`updated_at` older than `2 * flow_interval`); a row without a
/// usable timestamp (`updated_at` `None` or `<= 0`) is treated as FRESH
/// (py comment: the production schema makes `updated_at` NOT NULL, only
/// synthetic rows lack it).
pub fn detect_congestion(
    flow_state_row: Option<&FlowStateRow>,
    live_htlc: Option<&LiveHtlc>,
    htlc_congestion_threshold: f64,
    flow_interval: i64,
    now: i64,
) -> bool {
    // (a) live HTLC utilization from the channel info already in hand.
    if let Some(live) = live_htlc {
        if live.has_htlc_data && live.max_accepted_htlcs > 0 {
            return (live.our_htlcs_in_flight as f64 / live.max_accepted_htlcs as f64)
                > htlc_congestion_threshold;
        }
    }

    // (b) snapshot fallback with staleness TTL.
    let row = match flow_state_row {
        Some(r) if r.state.as_deref() == Some("congested") => r,
        _ => return false,
    };

    if let Some(updated_at) = row.updated_at {
        if updated_at > 0 {
            let max_age = 2 * flow_interval;
            if (now - updated_at) > max_age {
                return false; // stale label — flow analysis stopped updating
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Class-aware min fee (E-2) — py 2777-2877.
// ---------------------------------------------------------------------------

/// `FeeController.SATURATED_OUTBOUND_RATIO` (py 2761).
pub const SATURATED_OUTBOUND_RATIO: f64 = 0.85;
/// `FeeController.FLOW_BALANCED_WINDOW_SECONDS` (py 2772, `7 * 86400`).
pub const FLOW_BALANCED_WINDOW_SECONDS: i64 = 7 * 86400;
/// `FeeController.FLOW_BALANCED_MAX_NET_RATIO` (py 2773).
pub const FLOW_BALANCED_MAX_NET_RATIO: f64 = 0.33;
/// `FeeController.FLOW_BALANCED_MIN_WEEKLY_TURNOVER` (py 2774).
pub const FLOW_BALANCED_MIN_WEEKLY_TURNOVER: f64 = 0.25;

/// Class-aware config min-fee floor inputs (py `cfg.min_fee_ppm` /
/// `cfg.min_fee_ppm_saturated`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MinFeeCfg {
    pub min_fee_ppm: i64,
    pub min_fee_ppm_saturated: i64,
}

/// Pre-resolved 7d directional flow for one channel (py
/// `_get_flow_window_map()[channel_id]` 3-tuple, count dropped — unused
/// by `_is_flow_balanced_router`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlowWindow {
    pub out_sats: i64,
    pub in_sats: i64,
}

/// `_is_flow_balanced_router` (py 2795-2809), pure over an already-resolved
/// flow window (the batch-fetch + cycle-cache in `_get_flow_window_map`,
/// py 2777-2793, is DB/RPC-bound cycle-orchestrator plumbing, out of scope
/// here).
pub fn is_flow_balanced_router(capacity_sats: i64, window: Option<&FlowWindow>) -> bool {
    if capacity_sats <= 0 {
        return false;
    }
    let window = match window {
        Some(w) => w,
        None => return false,
    };
    let gross = window.out_sats + window.in_sats;
    if gross <= 0 {
        return false;
    }
    if (gross as f64) < capacity_sats as f64 * FLOW_BALANCED_MIN_WEEKLY_TURNOVER {
        return false;
    }
    let net_ratio = (window.out_sats - window.in_sats).abs() as f64 / gross as f64;
    net_ratio <= FLOW_BALANCED_MAX_NET_RATIO
}

/// `_effective_min_fee_ppm` (py 2811-2877) verbatim.
///
/// Class-aware config min-fee floor (E-2, operator-approved): saturated
/// (`outbound_ratio >= SATURATED_OUTBOUND_RATIO`) or source-classified
/// channels take `min(min_fee_ppm, min_fee_ppm_saturated)` — UNLESS recent
/// flow shows the channel is a self-refilling, flow-balanced router (py
/// 2849-2854), in which case the normal floor is kept. Values >=
/// `min_fee_ppm` (or negative) for the saturated term are ignored (py
/// 2839: `if sat_floor < 0 or sat_floor >= base: return base`).
pub fn effective_min_fee_ppm(
    cfg: &MinFeeCfg,
    flow_state: Option<&str>,
    outbound_ratio: Option<f64>,
    capacity_sats: i64,
    flow_window: Option<&FlowWindow>,
) -> i64 {
    let base = cfg.min_fee_ppm;
    let sat_floor = cfg.min_fee_ppm_saturated;
    if sat_floor < 0 || sat_floor >= base {
        return base;
    }
    let is_source = flow_state == Some("source");
    let is_saturated = outbound_ratio
        .map(|r| r >= SATURATED_OUTBOUND_RATIO)
        .unwrap_or(false);
    if is_source || is_saturated {
        if is_flow_balanced_router(capacity_sats, flow_window) {
            return base;
        }
        return sat_floor;
    }
    base
}
