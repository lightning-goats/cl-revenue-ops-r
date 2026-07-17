//! Pure pair selection (port of `modules/rebalance_planner_v2.py`,
//! `RebalancePlanner.plan`).
//!
//! Golden parity target: `fixtures/rebalance/planner.json` — pair lists
//! order-sensitive, scores via `revops_econ::pyfloat::py_repr` string
//! equality (Global Constraints: byte-parity discipline).
//!
//! ## Scope: a reduced `ChannelState`
//!
//! [`PlannerChannel`] is a REDUCED subset of the real `ChannelState` (py
//! `rebalance_state_v2.py`): it carries the numbers a live snapshot builder
//! (`build_state_snapshot`, ported later at plugin-wiring time — see the
//! plan's "Explicitly Deferred" section) would already have computed —
//! `urgency`/`drain` in place of `dest_urgency`/`source_drain_score`,
//! `capex_remaining_sats` in place of `remaining_budget_sats` — rather than
//! raw RPC fields. It does NOT carry `source_eligible`/`dest_eligible`/
//! cooldown (that gating lives in the deferred state builder): every
//! channel this module classifies is implicitly eligible once it falls
//! outside its own target band. This mirrors the fixture generator, which
//! always constructs the real Python `ChannelState` with
//! `source_eligible=dest_eligible=True` — see
//! `tools/port/gen_rebalance_fixtures.py`'s `_channel_state_from_case`.
//! Consequently the `source_ineligible`/`dest_ineligible` skip reasons the
//! Python planner *can* emit (only when fed an ineligible `ChannelState`)
//! are unreachable here — a deliberate, documented scope reduction, not an
//! oversight.
//!
//! `local_ratio` is derived from `spendable_sats / capacity_sats`, clamped
//! to `[0, 1]` and rounded to 6 decimals — this exactly mirrors
//! `build_state_snapshot`'s `round(local_ratio, 6)`
//! (`rebalance_state_v2.py` ~380), which matters for exact band-edge and
//! round-half-even parity.

use crate::types::{DrainDemand, SkipRecord};
use revops_econ::pyfloat::py_round;

/// Reduced per-channel input the planner reads (see module doc comment for
/// the scope note vs. the full Python `ChannelState`).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannerChannel {
    pub channel_id: String,
    pub peer_id: String,
    pub capacity_sats: i64,
    pub spendable_sats: i64,
    pub receivable_sats: i64,
    pub band_low: f64,
    pub band_high: f64,
    pub inbound_ppm: i64,
    pub value_class: String,
    pub urgency: f64,
    pub drain: f64,
    pub capex_remaining_sats: i64,
}

/// One selected rebalance pair (py `PairCandidate`, reduced field subset).
#[derive(Debug, Clone, PartialEq)]
pub struct PlannedPair {
    pub source: String,
    pub dest: String,
    pub amount_sats: i64,
    pub pair_budget_sats: i64,
    pub pair_fee_cap_ppm: i64,
    pub score: f64,
}

/// Output of one planning cycle (py `PlanResult`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PlanOutput {
    pub pairs: Vec<PlannedPair>,
    pub skips: Vec<SkipRecord>,
    pub drain_demand: Vec<DrainDemand>,
}

/// Value-class score table (py `_VALUE_SCORES`): unmapped classes score 0,
/// same as `"neutral"` (py `dict.get(value_class, 0)`).
fn value_score(value_class: &str) -> i64 {
    match value_class {
        "profitable" => 2,
        "active" | "funded" => 1,
        _ => 0,
    }
}

/// Py `build_state_snapshot`'s `local_ratio` derivation: `local_sats /
/// capacity_sats`, clamped to `[0, 1]`, `capacity_sats <= 0` guarded to
/// `0.0`, then `round(local_ratio, 6)`.
fn local_ratio(spendable_sats: i64, capacity_sats: i64) -> f64 {
    if capacity_sats <= 0 {
        return 0.0;
    }
    let raw = spendable_sats.max(0) as f64 / capacity_sats as f64;
    py_round(raw.clamp(0.0, 1.0), 6)
}

/// Py `_sats_from_ratio_delta`: `max(0, int(round(ratio_delta *
/// max(0, capacity_sats))))`. `round()` is Python's round-half-to-even,
/// reproduced here via `py_round(_, 0)` (NOT `f64::round()`, which is
/// round-half-away-from-zero and would diverge on exact `.5` ties — see
/// `tests/planner.rs::half_even_rounding_case`).
fn sats_from_ratio_delta(ratio_delta: f64, capacity_sats: i64) -> i64 {
    let capacity = capacity_sats.max(0) as f64;
    let raw = ratio_delta * capacity;
    let rounded = py_round(raw, 0) as i64;
    rounded.max(0)
}

/// A channel already classified into `over_local` or `over_remote`,
/// carrying its computed `local_ratio` alongside the borrowed input.
struct Classified<'a> {
    ch: &'a PlannerChannel,
    ratio: f64,
}

/// A scored candidate pair before greedy selection (py `_generate_pairs`
/// output, pre-sort).
struct Candidate<'a, 'b> {
    src: &'b Classified<'a>,
    dest: &'b Classified<'a>,
    amount_sats: i64,
    pair_budget_sats: i64,
    score: f64,
}

/// Port of `RebalancePlanner.plan` (py `rebalance_planner_v2.py`): classify
/// channels by per-channel target band, generate cross pairs (skipping
/// same-peer), score additively, greedily select one pair per channel up to
/// `max_pairs`, and explain every unselected channel with a [`SkipRecord`].
/// Residual over-local excess that could not be paired is published as
/// [`DrainDemand`].
pub fn plan(
    channels: &[PlannerChannel],
    max_chunk_sats: i64,
    max_pairs: usize,
    pair_fee_cap_ppm: i64,
) -> PlanOutput {
    let mut over_local: Vec<Classified> = Vec::new();
    let mut over_remote: Vec<Classified> = Vec::new();
    let mut skipped: Vec<SkipRecord> = Vec::new();

    // Phase 1: classify by each channel's OWN target band.
    for ch in channels {
        let ratio = local_ratio(ch.spendable_sats, ch.capacity_sats);
        if ratio > ch.band_high {
            over_local.push(Classified { ch, ratio });
        } else if ratio < ch.band_low {
            over_remote.push(Classified { ch, ratio });
        } else {
            skipped.push(SkipRecord {
                channel_id: ch.channel_id.clone(),
                reason: "inside_band".to_string(),
                value_class: ch.value_class.clone(),
                remaining_budget_sats: ch.capex_remaining_sats,
                detail: None,
            });
        }
    }

    // Phase 2: generate candidate pairs (skip same-peer), score, sort
    // stable-descending by score.
    let mut candidates: Vec<Candidate> = Vec::new();
    for src in &over_local {
        for dest in &over_remote {
            if src.ch.peer_id == dest.ch.peer_id {
                continue;
            }

            // FIX 2(a): size against each channel's OWN band -- source
            // drains toward its own band_high, dest refills toward its own
            // band_low.
            let source_excess =
                sats_from_ratio_delta(src.ratio - src.ch.band_high, src.ch.capacity_sats);
            let dest_need =
                sats_from_ratio_delta(dest.ch.band_low - dest.ratio, dest.ch.capacity_sats);
            let amount = source_excess
                .max(0)
                .min(dest_need.max(0))
                .min(max_chunk_sats);
            if amount <= 0 {
                continue;
            }

            // Destination authorizes spend: pair_budget = max(dest capex
            // remaining, ceil(amount * pair_fee_cap_ppm / 1e6)).
            let mut pair_budget = dest.ch.capex_remaining_sats;
            if pair_fee_cap_ppm > 0 {
                let fee_cap_from_amount = (amount * pair_fee_cap_ppm + 999_999) / 1_000_000;
                pair_budget = pair_budget.max(fee_cap_from_amount);
            }

            // Additive role-aware score: 0.30*dest_urgency +
            // 0.20*source_drain + 0.20*dest_value_class + cheap_return.
            let dest_value_term = value_score(&dest.ch.value_class) as f64 * 0.20;
            let dest_urgency_term = dest.ch.urgency * 0.30;
            let source_drain_term = src.ch.drain * 0.20;
            let inbound_ppm = src.ch.inbound_ppm.clamp(0, 5_000);
            let cheap_return_term = ((5_000 - inbound_ppm) as f64 / 50_000.0).max(0.0);
            // Left-to-right summation order matches the Python source
            // exactly (floating-point addition is not associative).
            let score = dest_urgency_term + source_drain_term + dest_value_term + cheap_return_term;

            candidates.push(Candidate {
                src,
                dest,
                amount_sats: amount,
                pair_budget_sats: pair_budget,
                score,
            });
        }
    }
    // Stable sort: score ties preserve original (source-major, dest-minor)
    // generation order (Global Constraints: sort on scores must be stable).
    candidates.sort_by(|a, b| b.score.total_cmp(&a.score));

    let mut paired_sources: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut paired_dests: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut selected: Vec<PlannedPair> = Vec::new();

    for cand in &candidates {
        if selected.len() >= max_pairs {
            break;
        }
        if paired_sources.contains(cand.src.ch.channel_id.as_str())
            || paired_dests.contains(cand.dest.ch.channel_id.as_str())
        {
            continue;
        }
        paired_sources.insert(cand.src.ch.channel_id.as_str());
        paired_dests.insert(cand.dest.ch.channel_id.as_str());
        selected.push(PlannedPair {
            source: cand.src.ch.channel_id.clone(),
            dest: cand.dest.ch.channel_id.clone(),
            amount_sats: cand.amount_sats,
            pair_budget_sats: cand.pair_budget_sats,
            pair_fee_cap_ppm,
            score: cand.score,
        });
    }

    // Phase 3: explain every unselected channel.
    let at_capacity = selected.len() >= max_pairs;

    for c in &over_local {
        if !paired_sources.contains(c.ch.channel_id.as_str()) {
            let (reason, detail): (&str, Option<String>) = if over_remote.is_empty() {
                (
                    "no_partner",
                    Some("no over-remote channels available".to_string()),
                )
            } else if at_capacity {
                ("max_pairs_reached", Some(format!("limit={max_pairs}")))
            } else {
                (
                    "outcompeted",
                    Some("lower-scoring pairs selected".to_string()),
                )
            };
            skipped.push(SkipRecord {
                channel_id: c.ch.channel_id.clone(),
                reason: reason.to_string(),
                value_class: c.ch.value_class.clone(),
                remaining_budget_sats: c.ch.capex_remaining_sats,
                detail,
            });
        }
    }
    for c in &over_remote {
        if !paired_dests.contains(c.ch.channel_id.as_str()) {
            let (reason, detail): (&str, Option<String>) = if over_local.is_empty() {
                (
                    "no_partner",
                    Some("no over-local channels available".to_string()),
                )
            } else if at_capacity {
                ("max_pairs_reached", Some(format!("limit={max_pairs}")))
            } else {
                (
                    "outcompeted",
                    Some("lower-scoring pairs selected".to_string()),
                )
            };
            skipped.push(SkipRecord {
                channel_id: c.ch.channel_id.clone(),
                reason: reason.to_string(),
                value_class: c.ch.value_class.clone(),
                remaining_budget_sats: c.ch.capex_remaining_sats,
                detail,
            });
        }
    }

    // Phase 4: publish the residual the circular path cannot place.
    let mut drain_demand: Vec<DrainDemand> = over_local
        .iter()
        .filter(|c| !paired_sources.contains(c.ch.channel_id.as_str()))
        .map(|c| DrainDemand {
            channel_id: c.ch.channel_id.clone(),
            peer_id: c.ch.peer_id.clone(),
            excess_sats: sats_from_ratio_delta(c.ratio - c.ch.band_high, c.ch.capacity_sats),
            drain_score: c.ch.drain,
            value_class: c.ch.value_class.clone(),
        })
        .collect();
    drain_demand.sort_by(|a, b| b.drain_score.total_cmp(&a.drain_score));

    PlanOutput {
        pairs: selected,
        skips: skipped,
        drain_demand,
    }
}
