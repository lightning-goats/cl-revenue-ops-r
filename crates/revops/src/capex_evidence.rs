//! Evidence assembly for `revenue-capex-status` (Task 66 slice 6).
//!
//! Port of the *evidence-gathering half* of
//! `CapexBudgetEngine.compute_allocations` (py capex_budget.py:142-230,
//! 690-773) — the half `revops_capital::capex`'s module doc declared out
//! of scope for the pure kernel. Every source follows Python's own
//! fail-soft/fail-closed split EXACTLY:
//!
//! - `analyze_all_channels()` raising -> `all_prof = {}` (py 151-155):
//!   allocations are still computed over an empty fleet.
//! - `get_total_capex_by_channel` / `get_spend_ledger_summary` failing ->
//!   `None` -> CB-4 fail-closed, ALL budgets zeroed this cycle (py
//!   744-773; the kernel's `db_degraded` arm).
//! - `get_confirmed_onchain_sats` failing -> 0 (py 704-709).
//! - the per-channel success-rate read failing -> no adjustment (py
//!   565-575).
//! - `capital_efficiency.analyze()` raising -> `fleet_efficiency = None`
//!   (py 164-169). In Python, `analyze()` re-runs
//!   `profitability.analyze_all_channels()` itself; this port reuses the
//!   ONE profitability snapshot the same call already gathered (same
//!   instant, no second read), so a failed profitability read fails both
//!   consumers — exactly the Python outcome, without the double fetch.
//!
//! The bleeder half ports `get_bleeder_status`'s verdict machinery
//! (py profitability_analyzer.py:1795-1815): verdicts are computed over
//! the ACTIVE channel set with `revops_analytics::bleeder`'s pure ladder,
//! fed by the same windowed per-channel revenue/cost reads the dashboard's
//! bleeder report uses (the declared full-P&L equivalence — see
//! `dashboard_evidence`), and cached by the caller with Python's 300s TTL
//! + hysteresis (`prev` verdicts feed the F4a hold).
//!
//! DECLARED DELTA (parity authority record, docs/port/PARITY-CHECKLIST.md
//! "Python-side read-RPC mutations"): Python's handler pushes a summary to
//! the CLN datastore (`["revenue","capex-summary"]`, cl-revenue-ops.py:
//! 7752-7771). That is a shared-state write Python owns until cutover —
//! this port's read RPCs stay read-only, so NO datastore push happens
//! here, matching the record's `revenue-health`/`econ-snapshot` rulings.

use std::collections::{BTreeMap, HashMap};

use revops_analytics::bleeder::{classify_bleeder, BleederPnl, BleederSuccess};
use revops_analytics::profitability::ChannelProfitability;
use revops_capital::capex::{
    compute_allocations, CapexConfig, CapexEvidence, ChannelProfile, FleetEfficiency, SpendSummary,
    Window30d,
};
use revops_core::msat::base_to_sats_ceil;
use revops_db::actor::DbHandle;
use revops_db::analytics::ChannelFlowStateRow;
use revops_db::queries::{
    PerChannelCosts, PerChannelRevenue, RebalanceSuccessRateRow, SpendLedgerAggregates,
};
use serde_json::Value;
use std::sync::atomic::AtomicU64;

use crate::capital_efficiency::{capex_fleet_efficiency, CapexEfficiencyChannel};
use crate::fee_config::{resolve_float, resolve_int};
use crate::rpc_capex_status::build_capex_status;

/// py `Database.get_confirmed_onchain_sats`'s parse half (database.py:
/// 471-487): sum the FLOOR-converted `amount_msat` of every output with
/// `status == "confirmed"`. A malformed amount aborts the whole sum to 0
/// — Python's exception propagates to `_get_confirmed_onchain_sats`'s
/// `except -> 0` (capex_budget.py:704-709), taking the healthy outputs
/// with it.
pub fn confirmed_onchain_sats(listfunds: &Value) -> i64 {
    let Some(outputs) = listfunds.get("outputs").and_then(Value::as_array) else {
        return 0;
    };
    let mut total = 0i64;
    for output in outputs {
        if output.get("status").and_then(Value::as_str) != Some("confirmed") {
            continue;
        }
        let Some(amount_msat) = output.get("amount_msat").and_then(Value::as_i64) else {
            return 0;
        };
        total += amount_msat.div_euclid(1000);
    }
    total
}

/// py `base_delta_to_sats_toward_zero` — signed msat delta to sats,
/// toward zero (a sub-sat loss is not yet a loss).
fn toward_zero_sats(msat: i64) -> i64 {
    msat / 1000
}

/// One channel's `get_channel_full_pnl` subset for the bleeder ladder
/// (py database.py:3252-3292): contribution is `max(direct, sourced)`
/// msat, revenue ceils, net rounds toward zero. Absent map entries are
/// zero rows (py's batch `.get` miss falls back to a query over the same
/// empty window).
fn bleeder_pnl(
    scid: &str,
    revenue_30d: &HashMap<String, PerChannelRevenue>,
    costs_30d: &HashMap<String, PerChannelCosts>,
    revenue_7d: &HashMap<String, PerChannelRevenue>,
    costs_7d: &HashMap<String, PerChannelCosts>,
) -> BleederPnl {
    let contribution = |rev: Option<&PerChannelRevenue>| -> i64 {
        rev.map(|r| r.fees_earned_msat.max(r.sourced_fee_contribution_msat))
            .unwrap_or(0)
    };
    let ceil_sats = |msat: i64| -> i64 {
        if msat <= 0 {
            0
        } else {
            base_to_sats_ceil(msat as u64) as i64
        }
    };
    let contribution_30d = contribution(revenue_30d.get(scid));
    let contribution_7d = contribution(revenue_7d.get(scid));
    let cost_30d_msat = costs_30d
        .get(scid)
        .map(|c| c.rebalance_cost_30d_msat)
        .unwrap_or(0);
    let cost_7d_msat = costs_7d
        .get(scid)
        .map(|c| c.rebalance_cost_30d_msat)
        .unwrap_or(0);
    BleederPnl {
        rebalance_cost_30d_sats: costs_30d
            .get(scid)
            .map(|c| c.rebalance_cost_30d_sats)
            .unwrap_or(0),
        revenue_30d_sats: ceil_sats(contribution_30d),
        net_profit_30d_sats: toward_zero_sats(contribution_30d - cost_30d_msat),
        net_profit_7d_sats: toward_zero_sats(contribution_7d - cost_7d_msat),
    }
}

/// The v2 scan over the active channel set (py identify_bleeders_v2,
/// 1596-1793): one verdict string per ACTIVE channel, with the previous
/// verdicts feeding the F4a hysteresis hold.
pub fn bleeder_verdicts(
    active_scids: &[String],
    revenue_30d: &HashMap<String, PerChannelRevenue>,
    costs_30d: &HashMap<String, PerChannelCosts>,
    revenue_7d: &HashMap<String, PerChannelRevenue>,
    costs_7d: &HashMap<String, PerChannelCosts>,
    success_rates: &BTreeMap<String, RebalanceSuccessRateRow>,
    prev: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for scid in active_scids {
        let pnl = bleeder_pnl(scid, revenue_30d, costs_30d, revenue_7d, costs_7d);
        let success = success_rates.get(scid).map(|row| BleederSuccess {
            total: row.total,
            success_rate: row.success_rate,
        });
        let prev_hard = prev.get(scid).map(String::as_str) == Some("hard");
        out.insert(
            scid.clone(),
            classify_bleeder(&pnl, success, prev_hard).to_string(),
        );
    }
    out
}

/// py `prof` object -> the kernel's [`ChannelProfile`] (the exact fields
/// `compute_allocations`/`_compute_channel_budget` read, py 179-234,
/// 514-634).
pub fn channel_profile(prof: &ChannelProfitability) -> ChannelProfile {
    ChannelProfile {
        classification: prof.classification.as_value().to_string(),
        capacity_sats: prof.capacity_sats,
        days_open: prof.days_open,
        marginal_roi: prof.marginal_roi(),
        marginal_roi_reliable: prof.marginal_roi_reliable(),
        channel_role: Some(prof.channel_role().as_value().to_string()),
        total_contribution_msat: prof.revenue.total_contribution_msat(),
        total_forward_count: prof.revenue.total_forward_count(),
        fees_earned_msat: prof.revenue.fees_earned_msat,
        sourced_fee_contribution_msat: prof.revenue.sourced_fee_contribution_msat,
        window_30d: prof.window_30d_available.then_some(Window30d {
            contribution_30d_msat: prof.contribution_30d_msat,
            fees_earned_30d_msat: prof.fees_earned_30d_msat,
        }),
    }
}

/// The bleeder-cache read decision, extracted so the runtime's cache
/// handling is TESTABLE (main.rs is a binary no test can import).
/// Returns `(cached_verdicts, prev_verdicts, needs_write_back)`.
///
/// py `get_bleeder_status` (profitability_analyzer.py:1806-1813)
/// recomputes only when the entry is older than 300s and re-stamps the
/// timestamp only on that recompute. Re-stamping on a HIT would slide the
/// deadline forward on every call, so under sub-300s polling the verdicts
/// would never refresh (self-review 2026-07-31).
pub fn bleeder_cache_decision(
    cache: Option<&(i64, BTreeMap<String, String>)>,
    now: i64,
) -> (
    Option<BTreeMap<String, String>>,
    BTreeMap<String, String>,
    bool,
) {
    match cache {
        Some((at, map)) if now - at <= 300 => (Some(map.clone()), map.clone(), false),
        // A STALE map still feeds the F4a hysteresis hold (py 1634:
        // `prev_verdicts = self._bleeder_cache or {}`).
        Some((_, map)) => (None, map.clone(), true),
        None => (None, BTreeMap::new(), true),
    }
}

/// Every prefetched input `capex_status_response` shapes into a
/// [`CapexEvidence`]. Each `Result` carries its source's failure so the
/// response function applies Python's arm for THAT source (module doc).
pub struct CapexStatusSources {
    pub profitability: Result<HashMap<String, ChannelProfitability>, String>,
    /// Normalized scids of CHANNELD_NORMAL channels — the bleeder scan
    /// set (py `_get_all_channels`, profitability_analyzer.py:1921-1935).
    pub active_scids: Vec<String>,
    pub revenue_30d: Result<HashMap<String, PerChannelRevenue>, String>,
    pub costs_30d: Result<HashMap<String, PerChannelCosts>, String>,
    pub revenue_7d: Result<HashMap<String, PerChannelRevenue>, String>,
    pub costs_7d: Result<HashMap<String, PerChannelCosts>, String>,
    pub success_rates: Result<BTreeMap<String, RebalanceSuccessRateRow>, String>,
    pub capex_by_channel: Result<BTreeMap<String, i64>, String>,
    pub spend_aggregates: Result<SpendLedgerAggregates, String>,
    pub listfunds: Result<Value, String>,
    pub flow_states: Result<HashMap<String, ChannelFlowStateRow>, String>,
    /// `Some` = the caller's verdict cache is FRESH (py's 300s TTL,
    /// profitability_analyzer.py:1808-1813) — reused verbatim, no rescan.
    pub cached_bleeder: Option<BTreeMap<String, String>>,
    /// The PREVIOUS verdicts (fresh or stale) feeding the F4a hysteresis
    /// hold on a rescan (py 1634: `prev_verdicts = self._bleeder_cache or
    /// {}` — a stale cache still feeds hysteresis).
    pub prev_bleeder: BTreeMap<String, String>,
    pub cfg: CapexConfig,
    pub now: i64,
}

/// Assemble evidence with Python's per-source arms, run the frozen
/// kernel, and shape `build_capex_status`. Returns the bleeder verdicts
/// that were used so the caller can refresh its cache (py's refresh
/// assigns the new cache unconditionally, even when empty —
/// profitability_analyzer.py:1810-1813).
pub fn capex_status_response(s: CapexStatusSources) -> (Value, BTreeMap<String, String>) {
    let verdicts = match s.cached_bleeder {
        Some(cached) => cached,
        None => match (&s.revenue_30d, &s.costs_30d, &s.revenue_7d, &s.costs_7d) {
            // py's scan needs the success-rate evidence too: a failed
            // batch read falls back to per-channel queries, and a failure
            // THERE drops the channel from the classification entirely
            // (profitability_analyzer.py:1680-1687, 1768-1771). Scanning
            // with the adjustment silently omitted would invent verdicts
            // from partial evidence -- the same class of fabrication the
            // CB-4 arms exist to prevent (self-review 2026-07-31).
            (Ok(revenue_30d), Ok(costs_30d), Ok(revenue_7d), Ok(costs_7d))
                if s.success_rates.is_ok() =>
            {
                bleeder_verdicts(
                    &s.active_scids,
                    revenue_30d,
                    costs_30d,
                    revenue_7d,
                    costs_7d,
                    s.success_rates.as_ref().expect("guarded by is_ok"),
                    &s.prev_bleeder,
                )
            }
            // py: any exception inside identify_bleeders_v2's outer try
            // returns [] classifications (1786-1789) -> every
            // get_bleeder_status lookup misses -> "none" everywhere.
            _ => BTreeMap::new(),
        },
    };

    let profitability = match &s.profitability {
        Ok(map) => map.clone(),
        // py 151-155: analyze_all_channels raising -> all_prof = {}.
        Err(_) => HashMap::new(),
    };

    let channels: BTreeMap<String, ChannelProfile> = profitability
        .iter()
        .map(|(scid, prof)| (scid.clone(), channel_profile(prof)))
        .collect();

    // py 195-203: bleeder_status recorded per profiled channel; a scan
    // miss (or a failed scan) is "none", which the kernel treats as
    // no-bleeder. Entries that ARE "none" are equivalent to absent for
    // the kernel — only "hard" changes behavior — so only real verdicts
    // are inserted.
    let bleeder_status: BTreeMap<String, String> = channels
        .keys()
        .filter_map(|scid| {
            verdicts
                .get(scid)
                .filter(|v| v.as_str() != "none")
                .map(|v| (scid.clone(), v.clone()))
        })
        .collect();

    // py 567-575 (inside its own try): only >= 3 samples count.
    let success_rates: BTreeMap<String, f64> = s
        .success_rates
        .as_ref()
        .map(|rates| {
            rates
                .iter()
                .filter(|(_, row)| row.total >= 3)
                .map(|(scid, row)| (scid.clone(), row.success_rate))
                .collect()
        })
        .unwrap_or_default();

    // py 164-169: analyze() raising -> None. analyze() itself needs BOTH
    // the profitability snapshot and the flow metrics; a failure of
    // either fails it (module doc).
    let fleet_efficiency: Option<FleetEfficiency> = match (&s.profitability, &s.flow_states) {
        (Ok(prof), Ok(flow)) => {
            let inputs: Vec<CapexEfficiencyChannel> = prof
                .iter()
                .map(|(scid, p)| CapexEfficiencyChannel {
                    scid: scid.clone(),
                    capacity_sats: p.capacity_sats,
                    fees_earned_msat: p.revenue.fees_earned_msat,
                    days_open: p.days_open,
                    flow_forward_count: flow.get(scid).map(|row| row.forward_count),
                })
                .collect();
            // py `int(getattr(cfg, "capex_grace_days", 14) or 14)`
            // (capital_efficiency.py:161): a configured 0 is FALSY and
            // becomes 14 -- it does not mean "no grace period".
            Some(capex_fleet_efficiency(
                &inputs,
                if s.cfg.capex_grace_days == 0 {
                    14
                } else {
                    s.cfg.capex_grace_days
                },
            ))
        }
        _ => None,
    };

    let evidence = CapexEvidence {
        channels,
        bleeder_status,
        // py 744-757 CB-4: a failed read is None (deny spend), NEVER {}.
        capex_by_channel_sats: s.capex_by_channel.ok(),
        // py 759-773 CB-4: same fail-closed rule.
        spend_summary: s.spend_aggregates.ok().map(|aggregates| SpendSummary {
            spent_by_category: aggregates.spent_by_category,
            reserved_by_category: aggregates.reserved_by_category,
        }),
        // py 704-709: exceptions become 0.
        onchain_sats: s
            .listfunds
            .as_ref()
            .map(confirmed_onchain_sats)
            .unwrap_or(0),
        success_rates,
        fleet_efficiency,
    };

    let alloc = compute_allocations(&evidence, &s.cfg);
    (build_capex_status(&alloc, s.now), verdicts)
}

/// Resolve the [`CapexConfig`] snapshot through the same layered
/// resolution every fee-config field uses (`fee_config::resolve_int`/
/// `resolve_float`: DB override > live listconfigs value where a CLN
/// option exists > the py `config.py` default). Only three of these
/// fields are CLN options (daily/weekly budget, min-wallet-reserve); the
/// capex_* fields are pure `revenue-config` keys, so their option layer
/// is always empty and they resolve override-or-default — exactly the
/// layers Python's in-memory Config carries for them.
pub async fn resolve_capex_config(
    db: Option<&DbHandle>,
    python_option_values: &HashMap<String, cln_plugin::options::Value>,
) -> CapexConfig {
    let failures = AtomicU64::new(0);
    let d = CapexConfig::default();
    CapexConfig {
        capex_reinvestment_rate: resolve_float(
            db,
            python_option_values,
            &failures,
            "capex-reinvestment-rate",
            d.capex_reinvestment_rate,
        )
        .await,
        capex_bootstrap_bps: resolve_int(
            db,
            python_option_values,
            &failures,
            "capex-bootstrap-bps",
            d.capex_bootstrap_bps,
        )
        .await,
        capex_bootstrap_max_sats: resolve_int(
            db,
            python_option_values,
            &failures,
            "capex-bootstrap-max-sats",
            d.capex_bootstrap_max_sats,
        )
        .await,
        capex_grace_days: resolve_int(
            db,
            python_option_values,
            &failures,
            "capex-grace-days",
            d.capex_grace_days,
        )
        .await,
        capex_exploration_rate: resolve_float(
            db,
            python_option_values,
            &failures,
            "capex-exploration-rate",
            d.capex_exploration_rate,
        )
        .await,
        capex_tactical_rate: resolve_float(
            db,
            python_option_values,
            &failures,
            "capex-tactical-rate",
            d.capex_tactical_rate,
        )
        .await,
        capex_global_envelope_sats: resolve_int(
            db,
            python_option_values,
            &failures,
            "capex-global-envelope-sats",
            d.capex_global_envelope_sats,
        )
        .await,
        daily_budget_sats: resolve_int(
            db,
            python_option_values,
            &failures,
            "daily-budget-sats",
            d.daily_budget_sats,
        )
        .await,
        weekly_budget_sats: resolve_int(
            db,
            python_option_values,
            &failures,
            "weekly-budget-sats",
            d.weekly_budget_sats,
        )
        .await,
        min_wallet_reserve: resolve_int(
            db,
            python_option_values,
            &failures,
            "min-wallet-reserve",
            d.min_wallet_reserve,
        )
        .await,
        estimated_open_cost_sats: resolve_int(
            db,
            python_option_values,
            &failures,
            "estimated-open-cost-sats",
            d.estimated_open_cost_sats,
        )
        .await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_analytics::profitability::{ChannelCosts, ChannelRevenue, ProfitabilityClass};
    use serde_json::json;

    /// The adapter must carry the EXACT fields the kernel reads (py
    /// 179-234, 514-634) — including the `window_30d_available` gate on
    /// the windowed pair and the derived marginal-ROI trio.
    #[test]
    fn channel_profile_adapter_maps_every_kernel_field() {
        let mut prof = ChannelProfitability {
            channel_id: "700x1x0".to_string(),
            peer_id: "02".repeat(33),
            capacity_sats: 3_000_000,
            costs: ChannelCosts {
                channel_id: "700x1x0".to_string(),
                peer_id: "02".repeat(33),
                open_cost_sats: 500,
                rebalance_cost_sats: 900,
                effective_rebalance_cost_sats: 900,
            },
            revenue: ChannelRevenue {
                channel_id: "700x1x0".to_string(),
                fees_earned_msat: 5_000_000,
                volume_routed_msat: 900_000_000,
                forward_count: 40,
                sourced_volume_msat: 100_000_000,
                sourced_fee_contribution_msat: 7_000_000,
                sourced_forward_count: 4,
            },
            net_profit_sats: 1_000,
            roi_percent: 10.0,
            classification: ProfitabilityClass::StagnantCandidate,
            cost_per_sat_routed: 0.0,
            fee_per_sat_routed: 0.0,
            days_open: 42,
            last_routed: None,
            marginal_profit_30d_sats: 150,
            rebalance_cost_30d_sats: 300,
            opener: "local".to_string(),
            contribution_30d_msat: 2_500_000,
            fees_earned_30d_msat: 2_000_000,
            sourced_fee_30d_msat: 0,
            forward_count_30d: 9,
            sourced_forward_count_30d: 1,
            window_30d_available: true,
        };
        let profile = channel_profile(&prof);
        assert_eq!(profile.classification, "stagnant_candidate");
        assert_eq!(profile.capacity_sats, 3_000_000);
        assert_eq!(profile.days_open, 42);
        assert_eq!(profile.marginal_roi, 0.5, "150/300");
        assert!(profile.marginal_roi_reliable, ">= 100 sats of 30d spend");
        // 40 of 44 forwards are exit-side (> 70% one direction) -> the
        // outbound gateway role, py `.value` lowercase.
        assert_eq!(profile.channel_role.as_deref(), Some("outbound_gateway"));
        assert_eq!(
            profile.total_contribution_msat, 7_000_000,
            "max(direct, sourced)"
        );
        assert_eq!(profile.total_forward_count, 44);
        assert_eq!(profile.fees_earned_msat, 5_000_000);
        assert_eq!(profile.sourced_fee_contribution_msat, 7_000_000);
        let window = profile.window_30d.expect("window available");
        assert_eq!(window.contribution_30d_msat, 2_500_000);
        assert_eq!(window.fees_earned_30d_msat, 2_000_000);

        prof.window_30d_available = false;
        assert!(
            channel_profile(&prof).window_30d.is_none(),
            "py window_30d_available=False falls back to lifetime aggregates"
        );
    }

    #[test]
    fn confirmed_onchain_sums_only_confirmed_and_floors() {
        let lf = json!({"outputs": [
            {"status": "confirmed", "amount_msat": 1_500_999},
            {"status": "unconfirmed", "amount_msat": 9_000_000},
            {"status": "confirmed", "amount_msat": 2_000_000},
        ]});
        // floor(1500.999) + floor(2000) = 1500 + 2000.
        assert_eq!(confirmed_onchain_sats(&lf), 3500);
        assert_eq!(confirmed_onchain_sats(&json!({})), 0);
    }

    /// py: a malformed amount raises out of the loop; the capex caller's
    /// `except -> 0` loses the healthy outputs too (capex_budget.py:
    /// 704-709). Partial sums would overstate the wallet.
    #[test]
    fn malformed_amount_aborts_the_whole_sum() {
        let lf = json!({"outputs": [
            {"status": "confirmed", "amount_msat": 2_000_000},
            {"status": "confirmed", "amount_msat": "junk"},
        ]});
        assert_eq!(confirmed_onchain_sats(&lf), 0);
    }
}
