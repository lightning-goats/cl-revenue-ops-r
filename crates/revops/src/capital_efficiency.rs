//! Task 71 / F71-R4 — the capital-efficiency ranking producer.
//!
//! Port of `modules/capital_efficiency.py`'s ranking core: per-channel
//! `rpsd` (lifetime gross revenue per sat deployed, in ppm), the
//! `windowed_net_rpsd` (30-day marginal profit per sat deployed), and the
//! `efficiency_rank` that blends their percentile ranks 50/50.
//!
//! This exists because `build_discovery_evidence` hardcoded
//! `neighbor_capital_efficiency: None`, which forced the frozen kernel down
//! `discover_from_neighbors` — the FALLBACK — while Python production
//! injects the analyzer and takes
//! `discover_from_neighbors_capital_efficiency`, the common path. Two
//! different discovery strategies produce two different candidate sets.

use std::collections::BTreeMap;

use revops_capital::planner::discovery::PatronPoolInput;

/// One channel's already-read efficiency inputs.
#[derive(Debug, Clone)]
pub struct EfficiencyInput {
    pub scid: String,
    pub capacity_sats: i64,
    /// Lifetime gross fees earned, msat.
    pub fees_earned_msat: i64,
    /// 30-day marginal profit. `None` means the profitability snapshot does
    /// not expose it for this channel — which DISABLES the blend fleet-wide
    /// (py 89-95), it is never synthesized.
    pub marginal_profit_30d_sats: Option<i64>,
}

/// Port of `_calculate_rpsd` (py 149-162): revenue per sat deployed, ppm.
fn rpsd(input: &EfficiencyInput) -> f64 {
    let capacity = input.capacity_sats.max(0);
    if capacity <= 0 {
        // py 152: zero capacity is 0.0, not a divide.
        return 0.0;
    }
    let fees_sats = input.fees_earned_msat as f64 / 1_000.0;
    fees_sats * 1_000_000.0 / capacity as f64
}

/// Port of `_calculate_windowed_net_rpsd` (py 164-180). `None` propagates
/// the "no windowed signal" case; a zero-capacity channel is a real 0.0.
fn windowed_net_rpsd(input: &EfficiencyInput) -> Option<f64> {
    let raw = input.marginal_profit_30d_sats?;
    let capacity = input.capacity_sats.max(0);
    if capacity <= 0 {
        return Some(0.0);
    }
    Some(raw as f64 * 1_000_000.0 / capacity as f64)
}

/// Port of `_calculate_percentile_ranks` (py 182-205): each key mapped to a
/// 0..1 percentile rank, with TIES sharing the average of their positions.
///
/// The tie rule matters: assigning tied channels distinct ranks by sort
/// order would make the patron pool depend on scid string ordering rather
/// than on measured efficiency.
pub fn percentile_ranks(values: &BTreeMap<String, f64>) -> BTreeMap<String, f64> {
    if values.is_empty() {
        return BTreeMap::new();
    }

    // py sorts by (value, key) -- the key breaks value ties deterministically.
    let mut sorted: Vec<(&String, &f64)> = values.iter().collect();
    sorted.sort_by(|a, b| {
        a.1.partial_cmp(b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });

    // py 187-189: a lone channel ranks 1.0. Ranking it 0.0 would make a
    // one-channel node's only patron look worthless.
    if sorted.len() == 1 {
        return BTreeMap::from([(sorted[0].0.clone(), 1.0)]);
    }

    let denominator = (sorted.len() - 1) as f64;
    let mut ranks = BTreeMap::new();
    let mut index = 0usize;
    while index < sorted.len() {
        let mut end = index;
        let value = *sorted[index].1;
        while end + 1 < sorted.len() && *sorted[end + 1].1 == value {
            end += 1;
        }
        let avg_rank = ((index + end) as f64 / 2.0) / denominator;
        for tied in &sorted[index..=end] {
            ranks.insert(tied.0.clone(), avg_rank);
        }
        index = end + 1;
    }
    ranks
}

/// Port of `analyze`'s ranking section (py 78-101): lifetime-gross
/// percentile ranks, blended 50/50 with windowed-net percentile ranks when
/// — and only when — EVERY channel exposes the windowed signal.
///
/// The all-or-nothing rule is py's own (89-95 breaks and clears the map on
/// the first `None`). A partial blend would rank channels against a metric
/// half of them lack, silently, and this ranking decides which peers become
/// patrons and therefore which peers get discovered at all.
///
/// Audit F7 is why the blend exists: lifetime gross alone is sticky, so a
/// channel that earned well a year ago and has bled since keeps a top rank
/// forever.
pub fn efficiency_ranks(inputs: &[EfficiencyInput]) -> BTreeMap<String, f64> {
    let lifetime: BTreeMap<String, f64> =
        inputs.iter().map(|i| (i.scid.clone(), rpsd(i))).collect();
    let lifetime_ranks = percentile_ranks(&lifetime);

    let mut windowed: BTreeMap<String, f64> = BTreeMap::new();
    for input in inputs {
        match windowed_net_rpsd(input) {
            Some(v) => {
                windowed.insert(input.scid.clone(), v);
            }
            None => {
                windowed.clear();
                break;
            }
        }
    }

    if windowed.is_empty() {
        return lifetime_ranks;
    }

    let windowed_ranks = percentile_ranks(&windowed);
    lifetime_ranks
        .iter()
        .map(|(scid, lifetime_rank)| {
            let windowed_rank = windowed_ranks.get(scid).copied().unwrap_or(0.0);
            (scid.clone(), 0.5 * lifetime_rank + 0.5 * windowed_rank)
        })
        .collect()
}

/// One patron's already-read pool inputs, keyed by PEER (the pool is
/// per-peer, while efficiency ranks are per-channel).
#[derive(Debug, Clone)]
pub struct PatronInput {
    pub peer_id: String,
    pub scid: String,
    pub volume_routed_sats: i64,
    pub marginal_roi_percent: f64,
}

/// Build the frozen strategy's `PatronPoolInput` list, attaching each
/// peer's channel efficiency rank.
///
/// Note the deliberate NON-application of this port's usual absent-vs-zero
/// rule: `build_neighbor_patron_pool` falls back to `0.1` for BOTH a
/// missing rank and an explicit falsy `0.0` (py 1644:
/// `float(getattr(channel_eff, "efficiency_rank", 0.1) or 0.1)`). Absent
/// and zero genuinely collapse here, so passing `Some(0.0)` and `None` is
/// equivalent by design — the frozen kernel already encodes that.
pub fn patron_pool_inputs(
    patrons: &[PatronInput],
    ranks: &BTreeMap<String, f64>,
) -> Vec<PatronPoolInput> {
    patrons
        .iter()
        .map(|p| PatronPoolInput {
            peer_id: p.peer_id.clone(),
            efficiency_rank: ranks.get(&p.scid).copied(),
            volume_routed_sats: p.volume_routed_sats,
            marginal_roi_percent: p.marginal_roi_percent,
        })
        .collect()
}

// ---------------------------------------------------------------------------
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
        let value = rpsd(&EfficiencyInput {
            scid: channel.scid.clone(),
            capacity_sats: channel.capacity_sats,
            fees_earned_msat: channel.fees_earned_msat,
            marginal_profit_30d_sats: None,
        });
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
