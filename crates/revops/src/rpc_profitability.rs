//! Pure response builder for `revenue-r-profitability`.
//!
//! Port of `revenue_profitability` (cl-revenue-ops.py:4906-5099). Both
//! response shapes -- single-channel (`channel_id` provided) and the
//! fleet-wide summary -- are built purely from already-computed
//! [`ChannelProfitability`] values (the P&L core ported in
//! `revops_analytics::profitability`, itself a port of
//! `modules/profitability_analyzer.py`'s `ChannelCosts`/`ChannelRevenue`/
//! `ChannelProfitability`/`classify_channel`). Assembling those values
//! from live channel + forward-history data is the wiring layer's job
//! (see `crates/revops/RPC_BATCH_A.md`), not this builder's -- this file
//! takes them as already-fetched inputs, same convention as
//! `rpc_dashboard::build_dashboard` taking a `&PnlSummary`.
//!
//! `fee_multiplier` (`profitability_analyzer.get_fee_multiplier`,
//! cl-revenue-ops.py:4968) reads the fee controller's live in-memory DTS
//! posterior state, which has no Rust-side equivalent wired to this
//! read-only batch -- always `null`, gap-listed.

use revops_analytics::profitability::{ChannelProfitability, ProfitabilityClass};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Python `-(-x // 1000)`: ceiling integer division by 1000, sign-correct
/// for the (unlikely but not impossible) negative-sum case
/// `analyze_all_channels`'s totals cannot rule out.
fn ceil_div_1000(x: i64) -> i64 {
    -(-x).div_euclid(1000)
}

/// Single-channel branch's flow-profile/ratio pair (cl-revenue-ops.py:
/// 4936-4949): `inbound_outbound_ratio` is `round(inbound/outbound, 2)`,
/// or the string `"infinite"` when `outbound_count == 0` (and there IS
/// inbound flow).
fn flow_profile_and_ratio(outbound_count: i64, inbound_count: i64) -> (&'static str, Value) {
    let total = outbound_count + inbound_count;
    if total == 0 {
        ("inactive", json!(0.0))
    } else if outbound_count == 0 {
        ("inbound_only", json!("infinite"))
    } else if inbound_count == 0 {
        ("outbound_only", json!(0.0))
    } else {
        let ratio = revops_econ::pyfloat::py_round(inbound_count as f64 / outbound_count as f64, 2);
        let profile = if ratio > 3.0 {
            "inbound_dominant"
        } else if ratio < 0.33 {
            "outbound_dominant"
        } else {
            "balanced"
        };
        (profile, json!(ratio))
    }
}

/// Aggregate-loop branch's flow-profile bucket (cl-revenue-ops.py:
/// 5006-5017): computes only the bucket, from the UNROUNDED ratio (that
/// copy of the branch never calls `round()` before comparing).
fn flow_profile_only(outbound_count: i64, inbound_count: i64) -> &'static str {
    let total = outbound_count + inbound_count;
    if total == 0 {
        "inactive"
    } else if outbound_count == 0 {
        "inbound_only"
    } else if inbound_count == 0 {
        "outbound_only"
    } else {
        let ratio = inbound_count as f64 / outbound_count as f64;
        if ratio > 3.0 {
            "inbound_dominant"
        } else if ratio < 0.33 {
            "outbound_dominant"
        } else {
            "balanced"
        }
    }
}

/// `channel_id` provided: the single-channel branch (cl-revenue-ops.py:
/// 4927-4980). `result: None` mirrors `profitability_analyzer
/// .analyze_channel()` returning falsy (channel unknown / no data).
pub fn build_profitability_channel(
    channel_id: &str,
    result: Option<&ChannelProfitability>,
) -> Value {
    let Some(r) = result else {
        return json!({"channel_id": channel_id, "error": "No data available"});
    };
    let outbound_count = r.revenue.forward_count;
    let inbound_count = r.revenue.sourced_forward_count;
    let (flow_profile, ratio) = flow_profile_and_ratio(outbound_count, inbound_count);
    json!({
        "channel_id": channel_id,
        "profitability": {
            "total_costs_sats": r.costs.total_cost_sats(),
            "total_contribution_sats": r.revenue.total_contribution_sats(),
            "net_profit_sats": r.net_profit_sats,
            "roi_percentage": revops_econ::pyfloat::py_round(r.roi_percent, 2),
            "profitability_class": r.classification.as_value(),
            "days_active": r.days_open,
            "fee_multiplier": Value::Null,
            "outbound_flow": {
                "payment_count": outbound_count,
                "volume_sats": r.revenue.volume_routed_sats(),
                "revenue_earned_sats": r.revenue.fees_earned_sats(),
            },
            "inbound_flow": {
                "payment_count": inbound_count,
                "volume_sats": r.revenue.sourced_volume_sats(),
                "contribution_to_other_channels_sats": r.revenue.sourced_fee_contribution_sats(),
            },
            "flow_profile": flow_profile,
            "inbound_outbound_ratio": ratio,
            "total_revenue_sats": r.revenue.fees_earned_sats(),
            "volume_routed_sats": r.revenue.volume_routed_sats(),
            "forward_count": outbound_count,
        },
        "_gaps": ["profitability.fee_multiplier"],
    })
}

/// No `channel_id`: the fleet-wide summary branch (cl-revenue-ops.py:
/// 4982-5075). `results` mirrors `all_results.values()` -- each entry's
/// own `channel_id` field supplies the dict key Python iterates.
pub fn build_profitability_summary(results: &[ChannelProfitability]) -> Value {
    let mut by_class: BTreeMap<&'static str, Vec<Value>> = [
        ProfitabilityClass::Profitable,
        ProfitabilityClass::BreakEven,
        ProfitabilityClass::Underwater,
        ProfitabilityClass::StagnantCandidate,
        ProfitabilityClass::Zombie,
    ]
    .iter()
    .map(|c| (c.as_value(), Vec::new()))
    .collect();

    let mut flow_counts: BTreeMap<&'static str, i64> = BTreeMap::new();
    for key in [
        "inbound_dominant",
        "outbound_dominant",
        "balanced",
        "inactive",
    ] {
        flow_counts.insert(key, 0);
    }

    let mut total_revenue_msat: i64 = 0;
    let mut total_contribution_msat: i64 = 0;
    let mut total_costs: i64 = 0;

    for r in results {
        let outbound_count = r.revenue.forward_count;
        let inbound_count = r.revenue.sourced_forward_count;
        let flow_profile = flow_profile_only(outbound_count, inbound_count);
        if let Some(c) = flow_counts.get_mut(flow_profile) {
            *c += 1;
        }

        let entry = json!({
            "channel_id": r.channel_id,
            "net_profit_sats": r.net_profit_sats,
            "roi_percentage": revops_econ::pyfloat::py_round(r.roi_percent, 2),
            "days_active": r.days_open,
            "flow_profile": flow_profile,
            "forward_count": outbound_count,
            "sourced_forward_count": inbound_count,
            "total_forward_count": r.revenue.total_forward_count(),
            "fees_earned_sats": r.revenue.fees_earned_sats(),
            "volume_routed_sats": r.revenue.volume_routed_sats(),
            "sourced_fee_contribution_sats": r.revenue.sourced_fee_contribution_sats(),
            "sourced_volume_sats": r.revenue.sourced_volume_sats(),
            "total_contribution_sats": r.revenue.total_contribution_sats(),
        });
        by_class
            .get_mut(r.classification.as_value())
            .expect("all ProfitabilityClass::as_value() keys are pre-seeded above")
            .push(entry);

        total_revenue_msat += r.revenue.fees_earned_msat;
        total_contribution_msat += r.revenue.total_contribution_msat();
        total_costs += r.costs.total_cost_sats();
    }

    let total_revenue = ceil_div_1000(total_revenue_msat);
    let total_contribution = ceil_div_1000(total_contribution_msat);
    let total_profit = total_revenue - total_costs;
    let overall_roi_pct = if total_costs > 0 {
        revops_econ::pyfloat::py_round((total_profit as f64 / total_costs as f64) * 100.0, 2)
    } else {
        0.0
    };

    json!({
        "summary": {
            "total_channels": results.len(),
            "profitable_count": by_class[ProfitabilityClass::Profitable.as_value()].len(),
            "break_even_count": by_class[ProfitabilityClass::BreakEven.as_value()].len(),
            "underwater_count": by_class[ProfitabilityClass::Underwater.as_value()].len(),
            "stagnant_candidate_count": by_class[ProfitabilityClass::StagnantCandidate.as_value()].len(),
            "zombie_count": by_class[ProfitabilityClass::Zombie.as_value()].len(),
            "total_profit_sats": total_profit,
            "total_revenue_sats": total_revenue,
            "total_contribution_sats": total_contribution,
            "total_costs_sats": total_costs,
            "overall_roi_pct": overall_roi_pct,
            "flow_profiles": {
                "inbound_dominant_count": flow_counts["inbound_dominant"],
                "outbound_dominant_count": flow_counts["outbound_dominant"],
                "balanced_count": flow_counts["balanced"],
                "inactive_count": flow_counts["inactive"],
            },
        },
        "channels_by_class": by_class,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_analytics::profitability::{ChannelCosts, ChannelRevenue};

    fn channel(
        id: &str,
        fees_msat: i64,
        forward_count: i64,
        sourced_forward_count: i64,
    ) -> ChannelProfitability {
        ChannelProfitability {
            channel_id: id.to_string(),
            peer_id: "02".to_string() + &"a".repeat(64),
            capacity_sats: 1_000_000,
            costs: ChannelCosts {
                channel_id: id.to_string(),
                peer_id: String::new(),
                open_cost_sats: 1000,
                rebalance_cost_sats: 500,
                effective_rebalance_cost_sats: 500,
            },
            revenue: ChannelRevenue {
                channel_id: id.to_string(),
                fees_earned_msat: fees_msat,
                volume_routed_msat: 10_000_000,
                forward_count,
                sourced_volume_msat: 5_000_000,
                sourced_fee_contribution_msat: 2000,
                sourced_forward_count,
            },
            net_profit_sats: fees_msat / 1000 - 1500,
            roi_percent: 12.345,
            classification: ProfitabilityClass::Profitable,
            cost_per_sat_routed: 0.0,
            fee_per_sat_routed: 0.0,
            days_open: 30,
            last_routed: Some(1_700_000_000),
            marginal_profit_30d_sats: 100,
            rebalance_cost_30d_sats: 50,
            opener: "local".to_string(),
            contribution_30d_msat: 1000,
            fees_earned_30d_msat: 1000,
            sourced_fee_30d_msat: 0,
            forward_count_30d: 5,
            sourced_forward_count_30d: 1,
            window_30d_available: true,
        }
    }

    #[test]
    fn channel_not_found_matches_python_error_shape() {
        let v = build_profitability_channel("123x1x0", None);
        assert_eq!(v["channel_id"], "123x1x0");
        assert_eq!(v["error"], "No data available");
        assert!(v.get("profitability").is_none());
    }

    #[test]
    fn single_channel_flow_profile_and_ratio_balanced() {
        // 10 outbound, 5 inbound -> ratio 0.5 -> "balanced" (not >3.0, not <0.33).
        let ch = channel("111x1x0", 50_000_000, 10, 5);
        let v = build_profitability_channel("111x1x0", Some(&ch));
        let p = &v["profitability"];
        assert_eq!(p["flow_profile"], "balanced");
        assert_eq!(p["inbound_outbound_ratio"], 0.5);
        assert_eq!(p["forward_count"], 10);
        assert_eq!(p["outbound_flow"]["payment_count"], 10);
        assert_eq!(p["inbound_flow"]["payment_count"], 5);
        // fee_multiplier is an honest, always-declared gap.
        assert_eq!(p["fee_multiplier"], Value::Null);
        assert_eq!(v["_gaps"][0], "profitability.fee_multiplier");
    }

    #[test]
    fn single_channel_zero_outbound_is_infinite_ratio() {
        let ch = channel("222x1x0", 0, 0, 7);
        let v = build_profitability_channel("222x1x0", Some(&ch));
        let p = &v["profitability"];
        assert_eq!(p["flow_profile"], "inbound_only");
        assert_eq!(p["inbound_outbound_ratio"], "infinite");
    }

    #[test]
    fn aggregate_summary_totals_and_class_grouping() {
        let mut a = channel("aaa1x1x0", 3000, 10, 1); // profitable, msat->3sats ceil
        a.classification = ProfitabilityClass::Profitable;
        let mut b = channel("bbb2x1x0", 0, 0, 0);
        b.classification = ProfitabilityClass::Zombie;
        b.revenue.fees_earned_msat = 0;
        b.costs.open_cost_sats = 200;
        b.costs.rebalance_cost_sats = 0;

        let results = vec![a, b];
        let v = build_profitability_summary(&results);
        let s = &v["summary"];
        assert_eq!(s["total_channels"], 2);
        assert_eq!(s["profitable_count"], 1);
        assert_eq!(s["zombie_count"], 1);
        assert_eq!(s["break_even_count"], 0);
        // total_costs_sats = (1000+500) + (200+0) = 1700
        assert_eq!(s["total_costs_sats"], 1700);
        // total_revenue_sats = ceil(3000/1000) + ceil(0/1000) = 3 + 0 = 3
        assert_eq!(s["total_revenue_sats"], 3);
        let by_class = &v["channels_by_class"];
        assert_eq!(by_class["profitable"].as_array().unwrap().len(), 1);
        assert_eq!(by_class["zombie"].as_array().unwrap().len(), 1);
        assert_eq!(by_class["break_even"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn overall_roi_pct_is_zero_when_no_costs() {
        let mut a = channel("z1x1x0", 0, 0, 0);
        a.costs.open_cost_sats = 0;
        a.costs.rebalance_cost_sats = 0;
        let results = vec![a];
        let v = build_profitability_summary(&results);
        assert_eq!(v["summary"]["overall_roi_pct"], 0.0);
        assert_eq!(v["summary"]["total_costs_sats"], 0);
    }
}
