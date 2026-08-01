//! Integration tests for `revenue-capex-status`'s production assembly
//! (`capex_evidence::capex_status_response`, the exact function `main.rs`
//! registers) driven with hand-built evidence sources. The frozen
//! allocation kernel is already fixture-verified against the Python
//! engine (`crates/revops-capital/tests/capex.rs`); these tests pin the
//! WIRING — that each source reaches the evidence through Python's exact
//! fail-soft/fail-closed arm, and that the bleeder verdict machinery
//! (fresh scan, cache reuse, hysteresis) feeds the kernel.

use std::collections::{BTreeMap, HashMap};

use revops::capex_evidence::{
    bleeder_verdicts, capex_status_response, resolve_capex_config, CapexStatusSources,
};
use revops_analytics::profitability::{
    ChannelCosts, ChannelProfitability, ChannelRevenue, ProfitabilityClass,
};
use revops_capital::capex::CapexConfig;
use revops_db::analytics::ChannelFlowStateRow;
use revops_db::queries::{PerChannelCosts, PerChannelRevenue, RebalanceSuccessRateRow};
use serde_json::{json, Value};

const NOW: i64 = 1_800_000_000;

fn prof(scid: &str, capacity: i64, fees_msat: i64) -> ChannelProfitability {
    ChannelProfitability {
        channel_id: scid.to_string(),
        peer_id: "02".repeat(33),
        capacity_sats: capacity,
        costs: ChannelCosts {
            channel_id: scid.to_string(),
            peer_id: "02".repeat(33),
            open_cost_sats: 500,
            rebalance_cost_sats: 0,
            effective_rebalance_cost_sats: 0,
        },
        revenue: ChannelRevenue {
            channel_id: scid.to_string(),
            fees_earned_msat: fees_msat,
            volume_routed_msat: fees_msat * 100,
            forward_count: 50,
            sourced_volume_msat: 0,
            sourced_fee_contribution_msat: 0,
            sourced_forward_count: 0,
        },
        net_profit_sats: 1000,
        roi_percent: 10.0,
        classification: ProfitabilityClass::Profitable,
        cost_per_sat_routed: 0.0,
        fee_per_sat_routed: 0.0,
        days_open: 100,
        last_routed: Some(NOW - 3600),
        marginal_profit_30d_sats: 500,
        rebalance_cost_30d_sats: 200,
        opener: "local".to_string(),
        contribution_30d_msat: 3_000_000,
        fees_earned_30d_msat: 3_000_000,
        sourced_fee_30d_msat: 0,
        forward_count_30d: 20,
        sourced_forward_count_30d: 0,
        window_30d_available: true,
    }
}

fn flow_row(scid: &str, forward_count: i64) -> ChannelFlowStateRow {
    ChannelFlowStateRow {
        scid: scid.to_string(),
        peer_id: "02".repeat(33),
        flow_state: "balanced".to_string(),
        balance_position: "balanced".to_string(),
        flow_ratio: 0.0,
        velocity: 0.0,
        confidence: 0.5,
        kalman_flow_ratio: 0.0,
        kalman_velocity: 0.0,
        kalman_uncertainty: 1.0,
        kalman_regime_change: false,
        forward_count,
        updated_at: NOW - 60,
        boot_id: "boot-1".to_string(),
    }
}

fn revenue(fees_msat: i64) -> PerChannelRevenue {
    PerChannelRevenue {
        fees_earned_msat: fees_msat,
        volume_routed_msat: fees_msat * 100,
        forward_count: 5,
        sourced_volume_msat: 0,
        sourced_fee_contribution_msat: 0,
        sourced_forward_count: 0,
    }
}

fn costs(rebalance_30d_sats: i64) -> PerChannelCosts {
    PerChannelCosts {
        peer_id: "02".repeat(33),
        open_cost_sats: 500,
        capacity_sats: 2_000_000,
        opened_at: NOW - 100 * 86400,
        rebalance_cost_sats: rebalance_30d_sats,
        rebalance_cost_30d_sats: rebalance_30d_sats,
        rebalance_cost_msat: rebalance_30d_sats * 1000,
        rebalance_cost_30d_msat: rebalance_30d_sats * 1000,
    }
}

fn rich_listfunds() -> Value {
    json!({"outputs": [{"status": "confirmed", "amount_msat": 10_000_000_000i64}]})
}

/// Two-channel fleet: "aaa" healthy, "bbb" a textbook hard bleeder
/// (2M sats of 30d rebalance spend, zero contribution). The scan must
/// classify "bbb" hard, the evidence must carry it, and the kernel's
/// visible outputs must show it: tier "blocked" for "bbb" and fleet
/// priority "defensive" (py `_get_priority_class`: hard bleeders win).
fn healthy_and_bleeder_sources() -> CapexStatusSources {
    let mut profitability = HashMap::new();
    profitability.insert("aaa".to_string(), prof("aaa", 2_000_000, 5_000_000));
    profitability.insert("bbb".to_string(), prof("bbb", 2_000_000, 0));

    let mut revenue_30d = HashMap::new();
    revenue_30d.insert("aaa".to_string(), revenue(3_000_000));
    let mut costs_30d = HashMap::new();
    costs_30d.insert("bbb".to_string(), costs(2_000_000));
    let mut costs_7d = HashMap::new();
    costs_7d.insert("bbb".to_string(), costs(400_000));

    let mut flow_states = HashMap::new();
    flow_states.insert("aaa".to_string(), flow_row("aaa", 10));
    flow_states.insert("bbb".to_string(), flow_row("bbb", 3));

    CapexStatusSources {
        profitability: Ok(profitability),
        active_scids: vec!["aaa".to_string(), "bbb".to_string()],
        revenue_30d: Ok(revenue_30d),
        costs_30d: Ok(costs_30d),
        revenue_7d: Ok(HashMap::new()),
        costs_7d: Ok(costs_7d),
        success_rates: Ok(BTreeMap::new()),
        capex_by_channel: Ok(BTreeMap::new()),
        spend_aggregates: Ok(Default::default()),
        listfunds: Ok(rich_listfunds()),
        flow_states: Ok(flow_states),
        cached_bleeder: None,
        prev_bleeder: BTreeMap::new(),
        cfg: CapexConfig::default(),
        now: NOW,
    }
}

#[test]
fn hard_bleeder_blocks_its_channel_and_sets_defensive_priority() {
    let (v, verdicts) = capex_status_response(healthy_and_bleeder_sources());
    assert_eq!(v["status"], "ok");
    assert_eq!(v["timestamp"], NOW);
    assert_eq!(v["generated_at"], NOW);
    assert_eq!(v["ttl_seconds"], 1800);
    assert_eq!(v["channel_count"], 2);
    // Scan verdicts: bbb enters hard (net30 -2_000_000 < -1000, effective
    // cost 2M > 2x0 revenue); aaa's 30d net is +3000 sats -> none.
    assert_eq!(verdicts.get("bbb").map(String::as_str), Some("hard"));
    assert_eq!(verdicts.get("aaa").map(String::as_str), Some("none"));
    // The verdict must REACH the kernel: hard bleeder -> blocked tier,
    // and fleet priority defensive (py capex_budget.py:690-697).
    assert_eq!(v["channels"]["bbb"]["tier"], "blocked");
    assert_eq!(v["priority_class"], "defensive");
    // The healthy channel is not blocked by someone else's bleed.
    assert_ne!(v["channels"]["aaa"]["tier"], "blocked");
}

/// CB-4 (py 744-757): a FAILED capex-by-channel read is `None` — never
/// `{}` — and the kernel zeroes every budget this cycle.
#[test]
fn failed_capex_read_fails_closed_to_zero_budgets() {
    let mut s = healthy_and_bleeder_sources();
    s.capex_by_channel = Err("db read failed".to_string());
    let (v, _) = capex_status_response(s);
    let channels = v["channels"].as_object().unwrap();
    assert_eq!(channels.len(), 2);
    for (scid, channel) in channels {
        assert_eq!(
            channel["budget_sats"], 0,
            "db_degraded must zero {scid}: {channel:?}"
        );
    }
}

/// py get_bleeder_status's 300s cache: fresh verdicts are reused verbatim
/// — no rescan — and returned unchanged for re-caching. The cached "hard"
/// wins even though a fresh scan of these windows would say "none".
#[test]
fn fresh_cached_verdicts_skip_the_rescan() {
    let mut s = healthy_and_bleeder_sources();
    s.revenue_30d = Ok(HashMap::new());
    s.costs_30d = Ok(HashMap::new());
    s.costs_7d = Ok(HashMap::new());
    let mut cached = BTreeMap::new();
    cached.insert("aaa".to_string(), "hard".to_string());
    s.cached_bleeder = Some(cached.clone());
    let (v, verdicts) = capex_status_response(s);
    assert_eq!(verdicts, cached);
    assert_eq!(v["channels"]["aaa"]["tier"], "blocked");
    assert_eq!(v["priority_class"], "defensive");
}

/// py identify_bleeders_v2's outer try (1786-1789): any window read
/// failing yields NO classifications — every channel reads as unbleeding
/// ("none" via lookup miss), never a fabricated verdict.
#[test]
fn scan_window_read_failure_yields_no_verdicts() {
    let mut s = healthy_and_bleeder_sources();
    s.revenue_7d = Err("window read failed".to_string());
    let (v, verdicts) = capex_status_response(s);
    assert!(verdicts.is_empty());
    assert_ne!(v["channels"]["bbb"]["tier"], "blocked");
    assert_ne!(v["priority_class"], "defensive");
}

/// py F4a hysteresis through the production scan fn: a previously-hard
/// channel at 30d net -600 holds hard; without the prior verdict the
/// same windows classify soft (sustained-minor).
#[test]
fn hysteresis_holds_previously_hard_verdicts() {
    let scids = vec!["ccc".to_string()];
    let revenue_30d = HashMap::new();
    let mut costs_30d = HashMap::new();
    costs_30d.insert("ccc".to_string(), costs(600));
    let revenue_7d = HashMap::new();
    let mut costs_7d = HashMap::new();
    costs_7d.insert("ccc".to_string(), costs(100));
    let rates = BTreeMap::new();

    let fresh = bleeder_verdicts(
        &scids,
        &revenue_30d,
        &costs_30d,
        &revenue_7d,
        &costs_7d,
        &rates,
        &BTreeMap::new(),
    );
    assert_eq!(fresh.get("ccc").map(String::as_str), Some("soft"));

    let mut prev = BTreeMap::new();
    prev.insert("ccc".to_string(), "hard".to_string());
    let held = bleeder_verdicts(
        &scids,
        &revenue_30d,
        &costs_30d,
        &revenue_7d,
        &costs_7d,
        &rates,
        &prev,
    );
    assert_eq!(held.get("ccc").map(String::as_str), Some("hard"));
}

/// The success-rate adjustment reaches the scan: 3+ samples at 50%
/// double the effective cost (py 1680-1687). Same windows, same channel:
/// hard only WITH the rate row. (revenue 1000 sats, cost 1900, net30
/// -900... adjusted below to cross the -1000 entry.)
#[test]
fn success_rate_row_reaches_the_scan() {
    // Hand-derived discriminator: contribution 1500 sats, 30d cost 2900,
    // 7d net +100 (recovery blip). net30 = -1400 < -1000, but the RAW
    // cost 2900 <= 2x1500 = 3000 misses the hard entry -> R1 soft. At a
    // 50% success rate the EFFECTIVE cost is 5800 > 3000 -> hard.
    let scids = vec!["ddd".to_string()];
    let mut revenue_30d = HashMap::new();
    revenue_30d.insert("ddd".to_string(), revenue(1_500_000));
    let mut costs_30d = HashMap::new();
    costs_30d.insert("ddd".to_string(), costs(2_900));
    let mut revenue_7d = HashMap::new();
    revenue_7d.insert("ddd".to_string(), revenue(100_000));
    let costs_7d = HashMap::new();

    let no_rates = BTreeMap::new();
    let fresh = bleeder_verdicts(
        &scids,
        &revenue_30d,
        &costs_30d,
        &revenue_7d,
        &costs_7d,
        &no_rates,
        &BTreeMap::new(),
    );
    assert_eq!(
        fresh.get("ddd").map(String::as_str),
        Some("soft"),
        "R1 arm without the rate"
    );

    let mut rates = BTreeMap::new();
    rates.insert(
        "ddd".to_string(),
        RebalanceSuccessRateRow {
            total: 5,
            successes: 2,
            success_rate: 0.5,
        },
    );
    let adjusted = bleeder_verdicts(
        &scids,
        &revenue_30d,
        &costs_30d,
        &revenue_7d,
        &costs_7d,
        &rates,
        &BTreeMap::new(),
    );
    assert_eq!(adjusted.get("ddd").map(String::as_str), Some("hard"));
}

/// Onchain wiring: a failed listfunds is 0 sats (py 704-709), and with
/// `min_wallet_reserve` at its 1M default that is a reserve deficit ->
/// priority "operational". A rich confirmed wallet -> "growth".
#[test]
fn listfunds_failure_zeroes_onchain_and_flips_priority() {
    let mut s = healthy_and_bleeder_sources();
    // Remove the hard bleeder so priority derives from the reserve.
    s.costs_30d = Ok(HashMap::new());
    s.costs_7d = Ok(HashMap::new());
    let (v, _) = capex_status_response(s);
    assert_eq!(v["priority_class"], "growth");

    let mut s = healthy_and_bleeder_sources();
    s.costs_30d = Ok(HashMap::new());
    s.costs_7d = Ok(HashMap::new());
    s.listfunds = Err("rpc failed".to_string());
    let (v, _) = capex_status_response(s);
    assert_eq!(v["priority_class"], "operational");
}

/// SELF-REVIEW 2026-07-31 (mutation-verified): the runtime's cache
/// read/write-back decision. A HIT must NOT ask for a write-back --
/// re-stamping the timestamp on every read slides the TTL forward
/// forever, so under sub-300s polling the verdicts never refresh and a
/// recovered channel stays "hard" permanently.
#[test]
fn bleeder_cache_hit_does_not_request_a_write_back() {
    use revops::capex_evidence::bleeder_cache_decision;
    let mut map = BTreeMap::new();
    map.insert("aaa".to_string(), "hard".to_string());

    // Fresh (age 250 <= 300): serve from cache, NO write-back.
    let (cached, prev, write_back) = bleeder_cache_decision(Some(&(NOW - 250, map.clone())), NOW);
    assert_eq!(cached, Some(map.clone()));
    assert_eq!(prev, map);
    assert!(!write_back, "a hit must not re-stamp the TTL");

    // Boundary: exactly 300 is still fresh (py `> 300` refreshes).
    let (cached, _, write_back) = bleeder_cache_decision(Some(&(NOW - 300, map.clone())), NOW);
    assert!(cached.is_some());
    assert!(!write_back);

    // Stale (301): rescan, write back, and the STALE map still feeds
    // hysteresis.
    let (cached, prev, write_back) = bleeder_cache_decision(Some(&(NOW - 301, map.clone())), NOW);
    assert!(cached.is_none(), "stale must rescan");
    assert_eq!(prev, map, "a stale map still feeds the F4a hold");
    assert!(write_back);

    // Empty cache: rescan, write back, no prior verdicts.
    let (cached, prev, write_back) = bleeder_cache_decision(None, NOW);
    assert!(cached.is_none());
    assert!(prev.is_empty());
    assert!(write_back);
}

/// SELF-REVIEW: a FAILED success-rate read must not produce verdicts at
/// all -- py falls back to per-channel queries and drops the channel
/// when those fail too (profitability_analyzer.py:1680-1687, 1768-1771).
/// Scanning with the adjustment silently omitted invents verdicts from
/// partial evidence.
#[test]
fn failed_success_rate_read_yields_no_verdicts() {
    let mut s = healthy_and_bleeder_sources();
    s.success_rates = Err("success-rate read failed".to_string());
    let (v, verdicts) = capex_status_response(s);
    assert!(
        verdicts.is_empty(),
        "no verdicts may be invented without the adjustment evidence: {verdicts:?}"
    );
    assert_ne!(
        v["channels"]["bbb"]["tier"], "blocked",
        "the bleeder must not be blocked on partial evidence: {v:?}"
    );
}

/// SELF-REVIEW 2026-07-31: py re-stamps the bleeder cache timestamp ONLY
/// on a recompute (profitability_analyzer.py:1806-1813). The caller must
/// therefore be able to tell a cache HIT from a rescan; this test pins
/// the contract `capex_status_response` gives it -- a hit returns the
/// SAME map it was handed (so the caller can skip the write-back and let
/// the TTL keep measuring time since the last recompute), while a rescan
/// returns freshly-computed verdicts.
#[test]
fn cache_hit_returns_the_cached_map_unchanged_so_the_ttl_can_stay_fixed() {
    let mut s = healthy_and_bleeder_sources();
    let mut cached = BTreeMap::new();
    cached.insert("aaa".to_string(), "hard".to_string());
    cached.insert("bbb".to_string(), "none".to_string());
    s.cached_bleeder = Some(cached.clone());
    let (_, returned) = capex_status_response(s);
    assert_eq!(returned, cached, "a hit must round-trip the cached map");

    // A rescan over the same sources computes DIFFERENT verdicts (bbb is
    // the real hard bleeder here) -- so the two paths are distinguishable
    // and the caller's write-back decision is observable.
    let (_, rescanned) = capex_status_response(healthy_and_bleeder_sources());
    assert_ne!(rescanned, cached);
    assert_eq!(rescanned.get("bbb").map(String::as_str), Some("hard"));
}

/// Layered config resolution: no DB, no live option values -> the
/// py config.py defaults, byte-for-byte the kernel's Default.
#[tokio::test]
async fn capex_config_defaults_without_db_or_options() {
    let cfg = resolve_capex_config(None, &HashMap::new()).await;
    assert_eq!(cfg, CapexConfig::default());
}
