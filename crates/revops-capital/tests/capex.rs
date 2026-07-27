//! `capex::compute_allocations` parity, pinned by
//! `fixtures/capital/capex/allocations.json` (generated from the REAL
//! `CapexBudgetEngine.compute_allocations` — `modules/capex_budget.py` —
//! by `tools/port/gen_capex_fixtures.py`, run against
//! `/home/sat/bin/cl_revenue_ops` with constructed stub profitability/
//! database/capital-efficiency evidence). Each scenario is a control: it
//! asserts on Python-computed values that vary across tiers, clamps, and
//! fail-closed paths, so a reverted or wrong port fails these tests rather
//! than passing vacuously.

use revops_capital::capex::{
    compute_allocations, CapexConfig, CapexEvidence, ChannelEfficiency, ChannelProfile,
    FleetEfficiency, PriorityClass, SpendSummary, Tier, Window30d,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/capital/capex/allocations.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn tier_from_str(s: &str) -> Tier {
    match s {
        "proven" => Tier::Proven,
        "active" => Tier::Active,
        "bootstrap" => Tier::Bootstrap,
        "blocked" => Tier::Blocked,
        other => panic!("unknown tier {other}"),
    }
}

fn priority_from_str(s: &str) -> PriorityClass {
    match s {
        "defensive" => PriorityClass::Defensive,
        "preservation" => PriorityClass::Preservation,
        "operational" => PriorityClass::Operational,
        "growth" => PriorityClass::Growth,
        other => panic!("unknown priority class {other}"),
    }
}

fn parse_profile(v: &Value) -> ChannelProfile {
    let revenue = &v["revenue"];
    let window_30d = if v["window_30d_available"].as_bool().unwrap_or(false) {
        Some(Window30d {
            contribution_30d_msat: v["contribution_30d_msat"].as_i64().unwrap(),
            fees_earned_30d_msat: v["fees_earned_30d_msat"].as_i64().unwrap(),
        })
    } else {
        None
    };
    ChannelProfile {
        classification: v["classification"].as_str().unwrap().to_string(),
        capacity_sats: v["capacity_sats"].as_i64().unwrap(),
        days_open: v["days_open"].as_i64().unwrap(),
        marginal_roi: v["marginal_roi"].as_f64().unwrap(),
        marginal_roi_reliable: v["marginal_roi_reliable"].as_bool().unwrap(),
        channel_role: v["channel_role"].as_str().map(|s| s.to_string()),
        total_contribution_msat: revenue["total_contribution_msat"].as_i64().unwrap(),
        total_forward_count: revenue["total_forward_count"].as_i64().unwrap(),
        fees_earned_msat: revenue["fees_earned_msat"].as_i64().unwrap(),
        sourced_fee_contribution_msat: revenue["sourced_fee_contribution_msat"].as_i64().unwrap(),
        window_30d,
    }
}

fn parse_config(v: &Value) -> CapexConfig {
    CapexConfig {
        capex_reinvestment_rate: v["capex_reinvestment_rate"].as_f64().unwrap(),
        capex_bootstrap_bps: v["capex_bootstrap_bps"].as_i64().unwrap(),
        capex_bootstrap_max_sats: v["capex_bootstrap_max_sats"].as_i64().unwrap(),
        capex_grace_days: v["capex_grace_days"].as_i64().unwrap(),
        capex_exploration_rate: v["capex_exploration_rate"].as_f64().unwrap(),
        capex_tactical_rate: v["capex_tactical_rate"].as_f64().unwrap(),
        capex_global_envelope_sats: v["capex_global_envelope_sats"].as_i64().unwrap(),
        daily_budget_sats: v["daily_budget_sats"].as_i64().unwrap(),
        weekly_budget_sats: v["weekly_budget_sats"].as_i64().unwrap(),
        min_wallet_reserve: v["min_wallet_reserve"].as_i64().unwrap(),
        estimated_open_cost_sats: v["estimated_open_cost_sats"].as_i64().unwrap(),
    }
}

fn parse_evidence(input: &Value) -> CapexEvidence {
    let mut channels = BTreeMap::new();
    for (ch, v) in input["channels"].as_object().unwrap() {
        channels.insert(ch.clone(), parse_profile(v));
    }

    let mut bleeder_status = BTreeMap::new();
    for (ch, v) in input["bleeders"].as_object().unwrap() {
        bleeder_status.insert(ch.clone(), v.as_str().unwrap().to_string());
    }

    let capex_by_channel_sats = if input["capex_by_channel_sats"].is_null() {
        None
    } else {
        let mut m = BTreeMap::new();
        for (ch, v) in input["capex_by_channel_sats"].as_object().unwrap() {
            m.insert(ch.clone(), v.as_i64().unwrap());
        }
        Some(m)
    };

    let spend_summary = if input["spend_summary"].is_null() {
        None
    } else {
        let s = &input["spend_summary"];
        let mut spent_by_category = BTreeMap::new();
        for (k, v) in s["spent_by_category"].as_object().unwrap() {
            spent_by_category.insert(k.clone(), v.as_i64().unwrap());
        }
        let mut reserved_by_category = BTreeMap::new();
        for (k, v) in s["reserved_by_category"].as_object().unwrap() {
            reserved_by_category.insert(k.clone(), v.as_i64().unwrap());
        }
        Some(SpendSummary {
            spent_by_category,
            reserved_by_category,
        })
    };

    let mut success_rates = BTreeMap::new();
    for (ch, v) in input["success_rates"].as_object().unwrap() {
        success_rates.insert(ch.clone(), v.as_f64().unwrap());
    }

    let fleet_efficiency = if input["fleet_efficiency"].is_null() {
        None
    } else {
        let fe = &input["fleet_efficiency"];
        let mut channel_efficiencies = BTreeMap::new();
        for (ch, v) in fe["channel_efficiencies"].as_object().unwrap() {
            channel_efficiencies.insert(
                ch.clone(),
                ChannelEfficiency {
                    is_dead_capital: v["is_dead_capital"].as_bool().unwrap(),
                    rpsd: v["rpsd"].as_f64().unwrap(),
                },
            );
        }
        Some(FleetEfficiency {
            channel_efficiencies,
            median_rpsd: fe["median_rpsd"].as_f64().unwrap(),
        })
    };

    CapexEvidence {
        channels,
        bleeder_status,
        capex_by_channel_sats,
        spend_summary,
        onchain_sats: input["onchain_sats"].as_i64().unwrap(),
        success_rates,
        fleet_efficiency,
    }
}

#[test]
fn all_scenarios_present() {
    let fx = fixture();
    let scenarios = fx["scenarios"].as_array().expect("scenarios array");
    // 24 compute_allocations scenarios + boltz_lifecycle + settle_write_failure.
    assert_eq!(scenarios.len(), 26, "expected fixture scenario count");
}

#[test]
fn compute_allocations_matches_python() {
    let fx = fixture();
    let scenarios = fx["scenarios"].as_array().expect("scenarios array");

    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        if !scenario
            .get("input")
            .map(|i| i.is_object())
            .unwrap_or(false)
        {
            continue; // boltz_lifecycle / settle_write_failure: separate test.
        }

        let evidence = parse_evidence(&scenario["input"]);
        let cfg = parse_config(&scenario["input"]["config"]);
        let alloc = compute_allocations(&evidence, &cfg);
        let expected = &scenario["output"];

        assert_eq!(
            alloc.priority_class.as_str(),
            expected["priority_class"].as_str().unwrap(),
            "{name}: priority_class"
        );
        assert_eq!(
            alloc.global_envelope_msat,
            expected["global_envelope_msat"].as_i64().unwrap(),
            "{name}: global_envelope_msat"
        );
        assert_eq!(
            alloc.global_envelope_sats(),
            expected["global_envelope_sats"].as_i64().unwrap(),
            "{name}: global_envelope_sats"
        );
        assert_eq!(
            alloc.fleet_exploration_budget_msat,
            expected["fleet_exploration_budget_msat"].as_i64().unwrap(),
            "{name}: fleet_exploration_budget_msat"
        );
        assert_eq!(
            alloc.tactical_budget_msat,
            expected["tactical_budget_msat"].as_i64().unwrap(),
            "{name}: tactical_budget_msat"
        );
        assert_eq!(
            alloc.total_fleet_contribution_msat,
            expected["total_fleet_contribution_msat"].as_i64().unwrap(),
            "{name}: total_fleet_contribution_msat"
        );
        assert_eq!(
            alloc.db_degraded,
            expected["db_degraded"].as_bool().unwrap(),
            "{name}: db_degraded"
        );

        let expected_priority_msat = expected["allocated_by_priority_msat"].as_object().unwrap();
        for (k, v) in expected_priority_msat {
            let pc = priority_from_str(k);
            assert_eq!(
                alloc
                    .allocated_by_priority_msat
                    .get(&pc)
                    .copied()
                    .unwrap_or(0),
                v.as_i64().unwrap(),
                "{name}: allocated_by_priority_msat[{k}]"
            );
        }

        let expected_budgets = expected["channel_budgets"].as_object().unwrap();
        assert_eq!(
            alloc.channel_budgets.len(),
            expected_budgets.len(),
            "{name}: channel_budgets count"
        );
        for (ch_id, eb) in expected_budgets {
            let b = alloc
                .channel_budgets
                .get(ch_id)
                .unwrap_or_else(|| panic!("{name}: missing channel budget for {ch_id}"));
            assert_eq!(
                b.budget_msat,
                eb["budget_msat"].as_i64().unwrap(),
                "{name}/{ch_id}: budget_msat"
            );
            assert_eq!(
                b.budget_sats(),
                eb["budget_sats"].as_i64().unwrap(),
                "{name}/{ch_id}: budget_sats"
            );
            assert_eq!(
                b.tier,
                tier_from_str(eb["tier"].as_str().unwrap()),
                "{name}/{ch_id}: tier"
            );
            assert_eq!(
                b.tier_ppm,
                eb["tier_ppm"].as_i64().unwrap(),
                "{name}/{ch_id}: tier_ppm"
            );
            assert_eq!(
                b.priority_class.as_str(),
                eb["priority_class"].as_str().unwrap(),
                "{name}/{ch_id}: priority_class"
            );
            assert_eq!(
                b.roi_multiplier,
                eb["roi_multiplier"].as_f64().unwrap(),
                "{name}/{ch_id}: roi_multiplier"
            );
            match eb["success_rate_30d"].as_f64() {
                Some(expected_sr) => {
                    assert_eq!(
                        b.success_rate_30d,
                        Some(expected_sr),
                        "{name}/{ch_id}: success_rate_30d"
                    );
                }
                None => assert_eq!(
                    b.success_rate_30d, None,
                    "{name}/{ch_id}: success_rate_30d should be None"
                ),
            }
        }
    }
}

/// Control: a channel above the proven-tier gate must be strictly better
/// funded than one below it, all else equal — this would fail if tier
/// classification were reverted to a single flat rate.
#[test]
fn proven_tier_gets_higher_ppm_cap_than_bootstrap() {
    let fx = fixture();
    let scenarios = fx["scenarios"].as_array().unwrap();
    let proven = scenarios
        .iter()
        .find(|s| s["name"] == "proven_tier_basic")
        .unwrap();
    let bootstrap = scenarios
        .iter()
        .find(|s| s["name"] == "bootstrap_tier")
        .unwrap();

    let proven_evidence = parse_evidence(&proven["input"]);
    let proven_cfg = parse_config(&proven["input"]["config"]);
    let proven_alloc = compute_allocations(&proven_evidence, &proven_cfg);
    let proven_budget = proven_alloc.channel_budgets.get("100x1x0").unwrap();

    let bootstrap_evidence = parse_evidence(&bootstrap["input"]);
    let bootstrap_cfg = parse_config(&bootstrap["input"]["config"]);
    let bootstrap_alloc = compute_allocations(&bootstrap_evidence, &bootstrap_cfg);
    let bootstrap_budget = bootstrap_alloc.channel_budgets.get("102x1x0").unwrap();

    assert!(proven_budget.tier_ppm > bootstrap_budget.tier_ppm);
    assert_eq!(proven_budget.tier, Tier::Proven);
    assert_eq!(bootstrap_budget.tier, Tier::Bootstrap);
}

/// Control: CB-4 fail-closed must actually zero budgets, not just flag
/// `db_degraded` — this is the exact failure mode the port map's risk
/// register calls out (silent re-grant on DB read failure).
#[test]
fn db_degraded_zeroes_every_budget() {
    let fx = fixture();
    let scenarios = fx["scenarios"].as_array().unwrap();
    let scenario = scenarios
        .iter()
        .find(|s| s["name"] == "db_degraded_capex_read_fails")
        .unwrap();

    let evidence = parse_evidence(&scenario["input"]);
    let cfg = parse_config(&scenario["input"]["config"]);
    let alloc = compute_allocations(&evidence, &cfg);

    assert!(alloc.db_degraded);
    assert_eq!(alloc.fleet_exploration_budget_msat, 0);
    assert_eq!(alloc.tactical_budget_msat, 0);
    for b in alloc.channel_budgets.values() {
        assert_eq!(
            b.budget_msat, 0,
            "channel {} should be zeroed",
            b.channel_id
        );
    }
}

#[test]
fn attribute_boltz_cost_matches_python_split() {
    let fx = fixture();
    let scenarios = fx["scenarios"].as_array().unwrap();
    let boltz = scenarios
        .iter()
        .find(|s| s["name"] == "boltz_lifecycle")
        .unwrap();
    let out = &boltz["output"];

    let channel_split = revops_capital::capex::attribute_boltz_cost(1001, Some("200x1x0"));
    assert_eq!(
        channel_split.channel,
        out["split_channel"]["channel"].as_i64().unwrap()
    );
    assert_eq!(
        channel_split.tactical,
        out["split_channel"]["tactical"].as_i64().unwrap()
    );

    let treasury_split = revops_capital::capex::attribute_boltz_cost(1000, None);
    assert_eq!(
        treasury_split.channel,
        out["split_treasury"]["channel"].as_i64().unwrap()
    );
    assert_eq!(
        treasury_split.tactical,
        out["split_treasury"]["tactical"].as_i64().unwrap()
    );
}
