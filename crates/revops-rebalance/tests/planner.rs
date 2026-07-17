//! Golden parity for `revops_rebalance::planner::plan`, pinned by
//! `fixtures/rebalance/planner.json` (generated from the REAL
//! `modules/rebalance_planner_v2.RebalancePlanner` by
//! `tools/port/gen_rebalance_fixtures.py planner` in the port worktree,
//! branch `port`).
//!
//! Comparisons are ORDER-SENSITIVE for `pairs`, `skips`, and `drain_demand`
//! (the Python source appends/sorts deterministically; a stable-sort or
//! iteration-order bug would still pass a set-equality check but fail here).
//! Scores are compared via `revops_econ::pyfloat::py_repr` string equality
//! (Global Constraints: byte-parity discipline).

use revops_econ::pyfloat::py_repr;
use revops_rebalance::planner::{plan, PlannerChannel};
use serde_json::Value;
use std::path::PathBuf;

fn fixture() -> Value {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/rebalance/planner.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn channel_from_json(v: &Value) -> PlannerChannel {
    PlannerChannel {
        channel_id: v["channel_id"].as_str().unwrap().to_string(),
        peer_id: v["peer_id"].as_str().unwrap().to_string(),
        capacity_sats: v["capacity_sats"].as_i64().unwrap(),
        spendable_sats: v["spendable_sats"].as_i64().unwrap(),
        receivable_sats: v["receivable_sats"].as_i64().unwrap(),
        band_low: v["band_low"].as_f64().unwrap(),
        band_high: v["band_high"].as_f64().unwrap(),
        inbound_ppm: v["inbound_ppm"].as_i64().unwrap(),
        value_class: v["value_class"].as_str().unwrap().to_string(),
        urgency: v["urgency"].as_f64().unwrap(),
        drain: v["drain"].as_f64().unwrap(),
        capex_remaining_sats: v["capex_remaining_sats"].as_i64().unwrap(),
    }
}

/// Runs one fixture case through `plan()` and asserts the full `PlanOutput`
/// (pairs/skips/drain_demand, each order-sensitive) matches the fixture's
/// `expected` block exactly.
fn assert_case_matches(case: &Value) {
    let case_id = case["case_id"].as_str().unwrap();
    let channels: Vec<PlannerChannel> = case["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(channel_from_json)
        .collect();
    let params = &case["params"];
    let max_chunk_sats = params["max_chunk_sats"].as_i64().unwrap();
    let max_pairs = params["max_pairs"].as_u64().unwrap() as usize;
    let pair_fee_cap_ppm = params["pair_fee_cap_ppm"].as_i64().unwrap();

    let output = plan(&channels, max_chunk_sats, max_pairs, pair_fee_cap_ppm);
    let expected = &case["expected"];

    let expected_pairs = expected["pairs"].as_array().unwrap();
    assert_eq!(
        output.pairs.len(),
        expected_pairs.len(),
        "{case_id}: pairs length"
    );
    for (i, (got, want)) in output.pairs.iter().zip(expected_pairs.iter()).enumerate() {
        assert_eq!(
            got.source,
            want["source"].as_str().unwrap(),
            "{case_id}: pair[{i}].source"
        );
        assert_eq!(
            got.dest,
            want["dest"].as_str().unwrap(),
            "{case_id}: pair[{i}].dest"
        );
        assert_eq!(
            got.amount_sats,
            want["amount_sats"].as_i64().unwrap(),
            "{case_id}: pair[{i}].amount_sats"
        );
        assert_eq!(
            got.pair_budget_sats,
            want["pair_budget_sats"].as_i64().unwrap(),
            "{case_id}: pair[{i}].pair_budget_sats"
        );
        assert_eq!(
            got.pair_fee_cap_ppm,
            want["pair_fee_cap_ppm"].as_i64().unwrap(),
            "{case_id}: pair[{i}].pair_fee_cap_ppm"
        );
        assert_eq!(
            py_repr(got.score),
            py_repr(want["score"].as_f64().unwrap()),
            "{case_id}: pair[{i}].score"
        );
    }

    let expected_skips = expected["skips"].as_array().unwrap();
    assert_eq!(
        output.skips.len(),
        expected_skips.len(),
        "{case_id}: skips length"
    );
    for (i, (got, want)) in output.skips.iter().zip(expected_skips.iter()).enumerate() {
        assert_eq!(
            got.channel_id,
            want["channel_id"].as_str().unwrap(),
            "{case_id}: skip[{i}].channel_id"
        );
        assert_eq!(
            got.reason,
            want["reason"].as_str().unwrap(),
            "{case_id}: skip[{i}].reason"
        );
        assert_eq!(
            got.value_class,
            want["value_class"].as_str().unwrap(),
            "{case_id}: skip[{i}].value_class"
        );
        assert_eq!(
            got.remaining_budget_sats,
            want["remaining_budget_sats"].as_i64().unwrap(),
            "{case_id}: skip[{i}].remaining_budget_sats"
        );
        assert_eq!(
            got.detail.as_deref(),
            want["detail"].as_str(),
            "{case_id}: skip[{i}].detail"
        );
    }

    let expected_drain = expected["drain_demand"].as_array().unwrap();
    assert_eq!(
        output.drain_demand.len(),
        expected_drain.len(),
        "{case_id}: drain_demand length"
    );
    for (i, (got, want)) in output
        .drain_demand
        .iter()
        .zip(expected_drain.iter())
        .enumerate()
    {
        assert_eq!(
            got.channel_id,
            want["channel_id"].as_str().unwrap(),
            "{case_id}: drain_demand[{i}].channel_id"
        );
        assert_eq!(
            got.peer_id,
            want["peer_id"].as_str().unwrap(),
            "{case_id}: drain_demand[{i}].peer_id"
        );
        assert_eq!(
            got.excess_sats,
            want["excess_sats"].as_i64().unwrap(),
            "{case_id}: drain_demand[{i}].excess_sats"
        );
        assert_eq!(
            py_repr(got.drain_score),
            py_repr(want["drain_score"].as_f64().unwrap()),
            "{case_id}: drain_demand[{i}].drain_score"
        );
        assert_eq!(
            got.value_class,
            want["value_class"].as_str().unwrap(),
            "{case_id}: drain_demand[{i}].value_class"
        );
    }
}

#[test]
fn planner_replays_python_fixture_byte_identically() {
    let fx = fixture();
    let cases = fx["cases"].as_array().expect("cases array");
    assert!(cases.len() >= 30, "expected at least 30 fixture cases");
    for case in cases {
        assert_case_matches(case);
    }
}

/// `int(round(ratio_delta * capacity_sats))` must be Python's round-half-to-
/// even, not Rust's `f64::round()` (round-half-away-from-zero). The fixture
/// case `half_even_rounding_case` has three unpaired over-local channels
/// whose (local_ratio - band_high) * capacity lands EXACTLY on 0.5 / 2.5 /
/// 4.5 — Python rounds these to 0 / 2 / 4 (nearest even); a naive
/// `.round()` port would produce 1 / 3 / 5.
#[test]
fn half_even_rounding_case() {
    let fx = fixture();
    let case = fx["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["case_id"] == "half_even_rounding_case")
        .expect("half_even_rounding_case present in fixture");

    let channels: Vec<PlannerChannel> = case["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(channel_from_json)
        .collect();
    let output = plan(&channels, 2_000_000, 10, 0);

    assert_eq!(
        output.pairs.len(),
        0,
        "no pairs: all three channels are unpaired sources"
    );
    assert_eq!(output.drain_demand.len(), 3);

    let by_id = |id: &str| {
        output
            .drain_demand
            .iter()
            .find(|d| d.channel_id == id)
            .unwrap_or_else(|| panic!("drain_demand entry {id} missing"))
    };
    assert_eq!(
        by_id("tie-2").excess_sats,
        0,
        "round(0.5) must be 0 (even), not 1"
    );
    assert_eq!(
        by_id("tie-10").excess_sats,
        2,
        "round(2.5) must be 2 (even), not 3"
    );
    assert_eq!(
        by_id("tie-18").excess_sats,
        4,
        "round(4.5) must be 4 (even), not 5"
    );

    // Cross-check against the fixture's own oracle values too.
    assert_case_matches(case);
}

/// Score-sort ties must be broken by a STABLE sort so the greedy
/// one-pair-per-channel selection reproduces Python's exact result. The
/// fixture case `stable_tie_case` has 2 sources x 2 dests, all four
/// candidate pairs scoring bit-identically; only a stable sort selects
/// (src0,dest0) then (src1,dest1) in original nested-loop generation order
/// — an unstable sort could legally pick a different (still "valid" by
/// score) pairing.
#[test]
fn stable_tie_case() {
    let fx = fixture();
    let case = fx["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["case_id"] == "stable_tie_case")
        .expect("stable_tie_case present in fixture");

    let channels: Vec<PlannerChannel> = case["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(channel_from_json)
        .collect();
    let output = plan(&channels, 2_000_000, 10, 0);

    assert_eq!(output.pairs.len(), 2);
    assert_eq!(output.pairs[0].source, "stable-src-0");
    assert_eq!(output.pairs[0].dest, "stable-dest-0");
    assert_eq!(output.pairs[1].source, "stable-src-1");
    assert_eq!(output.pairs[1].dest, "stable-dest-1");

    assert_case_matches(case);
}
