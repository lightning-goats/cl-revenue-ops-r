//! Capital-efficiency evidence used only by reporting and capex allocation.

use std::collections::BTreeMap;

// Task 66 slice 6: the capex engine's FleetEfficiency subset
// ---------------------------------------------------------------------------

/// The exact per-channel fields `CapitalEfficiencyAnalyzer.analyze()`
/// reads for the two outputs the capex engine consumes (py
/// capital_efficiency.py:59-147 -> `capex::FleetEfficiency`):
/// per-channel `rpsd` + `is_dead_capital`, and the fleet `median_rpsd`.
/// Ranks/velocity/stages are analyze()'s OTHER outputs — no capex
/// consumer, not built here.
#[derive(Debug, Clone)]
pub struct CapexEfficiencyChannel {
    pub scid: String,
    pub capacity_sats: i64,
    /// Lifetime gross fees earned, msat (`prof.revenue.fees_earned_msat`).
    pub fees_earned_msat: i64,
    pub days_open: i64,
    /// `Some(count)` when flow metrics exist for this channel; `None`
    /// mirrors py's `flow_metrics is None` -> never dead capital.
    pub flow_forward_count: Option<i64>,
}

/// py `statistics.median`: middle value, or the mean of the two middle
/// values on an even count.
fn py_median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).expect("rpsd values are finite"));
    let n = values.len();
    if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    }
}

/// py `_is_dead_capital` (capital_efficiency.py:207-218): conservative —
/// requires flow metrics to exist, ZERO forwards in the flow window, and
/// an age strictly past the grace period.
fn is_dead_capital(channel: &CapexEfficiencyChannel, grace_days: i64) -> bool {
    let Some(forward_count) = channel.flow_forward_count else {
        return false;
    };
    if forward_count != 0 {
        return false;
    }
    channel.days_open > grace_days
}

/// Build the capex engine's [`revops_capital::capex::FleetEfficiency`]
/// snapshot — the `analyze()` subset it reads (py capex_budget.py:164-169
/// consumes `channel_efficiencies[..].is_dead_capital`/`.rpsd` and
/// `median_rpsd` only).
pub fn capex_fleet_efficiency(
    channels: &[CapexEfficiencyChannel],
    grace_days: i64,
) -> revops_capital::capex::FleetEfficiency {
    let mut efficiencies = BTreeMap::new();
    let mut rpsd_values = Vec::with_capacity(channels.len());
    for channel in channels {
        let capacity = channel.capacity_sats.max(0);
        let value = if capacity <= 0 {
            0.0
        } else {
            let fees_sats = channel.fees_earned_msat as f64 / 1_000.0;
            fees_sats * 1_000_000.0 / capacity as f64
        };
        rpsd_values.push(value);
        efficiencies.insert(
            channel.scid.clone(),
            revops_capital::capex::ChannelEfficiency {
                is_dead_capital: is_dead_capital(channel, grace_days),
                rpsd: value,
            },
        );
    }
    revops_capital::capex::FleetEfficiency {
        channel_efficiencies: efficiencies,
        median_rpsd: py_median(&mut rpsd_values),
    }
}

#[cfg(test)]
mod capex_efficiency_tests {
    use super::*;

    fn ch(
        scid: &str,
        capacity: i64,
        fees_msat: i64,
        days: i64,
        fwd: Option<i64>,
    ) -> CapexEfficiencyChannel {
        CapexEfficiencyChannel {
            scid: scid.to_string(),
            capacity_sats: capacity,
            fees_earned_msat: fees_msat,
            days_open: days,
            flow_forward_count: fwd,
        }
    }

    /// rpsd is ppm of lifetime gross over capacity (py 149-162):
    /// 2_000_000 msat = 2000 sats over 1_000_000 sats = 2000 ppm.
    /// Odd-count median picks the middle value.
    #[test]
    fn rpsd_and_odd_median_hand_derived() {
        let fleet = capex_fleet_efficiency(
            &[
                ch("a", 1_000_000, 2_000_000, 30, Some(5)),
                ch("b", 1_000_000, 500_000, 30, Some(5)),
                ch("c", 2_000_000, 2_000_000, 30, Some(5)),
            ],
            14,
        );
        assert_eq!(fleet.channel_efficiencies["a"].rpsd, 2000.0);
        assert_eq!(fleet.channel_efficiencies["b"].rpsd, 500.0);
        assert_eq!(fleet.channel_efficiencies["c"].rpsd, 1000.0);
        assert_eq!(fleet.median_rpsd, 1000.0, "median of [500, 1000, 2000]");
    }

    /// Even count -> mean of the two middles (py statistics.median);
    /// zero capacity -> 0.0 rpsd, not a division (py 152).
    #[test]
    fn even_median_and_zero_capacity() {
        let fleet = capex_fleet_efficiency(
            &[
                ch("a", 1_000_000, 2_000_000, 30, Some(5)),
                ch("z", 0, 9_000_000, 30, Some(5)),
            ],
            14,
        );
        assert_eq!(fleet.channel_efficiencies["z"].rpsd, 0.0);
        assert_eq!(fleet.median_rpsd, 1000.0, "mean of [0, 2000]");
    }

    /// py `_is_dead_capital`: needs flow metrics present, zero forwards,
    /// and age STRICTLY past grace. Missing flow is never dead; exactly
    /// at the grace boundary is never dead.
    #[test]
    fn dead_capital_requires_flow_zero_forwards_and_age_past_grace() {
        let fleet = capex_fleet_efficiency(
            &[
                ch("dead", 1_000_000, 0, 15, Some(0)),
                ch("routing", 1_000_000, 0, 15, Some(2)),
                ch("young", 1_000_000, 0, 14, Some(0)),
                ch("no-flow", 1_000_000, 0, 400, None),
            ],
            14,
        );
        assert!(fleet.channel_efficiencies["dead"].is_dead_capital);
        assert!(!fleet.channel_efficiencies["routing"].is_dead_capital);
        assert!(
            !fleet.channel_efficiencies["young"].is_dead_capital,
            "15th day is inside grace"
        );
        assert!(!fleet.channel_efficiencies["no-flow"].is_dead_capital);
    }

    /// Empty fleet: py `median(...) if rpsd_by_channel else 0.0`.
    #[test]
    fn empty_fleet_is_zero_median() {
        let fleet = capex_fleet_efficiency(&[], 14);
        assert_eq!(fleet.median_rpsd, 0.0);
        assert!(fleet.channel_efficiencies.is_empty());
    }
}
