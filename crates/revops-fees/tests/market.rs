//! Neighbor-gossip market intelligence parity, pinned by
//! `fixtures/fees/market/*.json` (generated from the REAL
//! `FeeController._get_neighbor_fee_median_live` /
//! `_get_neighbor_fee_percentile_live` / `_is_cln_default_fee` /
//! `_get_competitive_undercut_pct` / `_get_market_boundary_fee` /
//! `_get_network_fee_prior_live` / `_select_best_fee_prior` /
//! `_gossip_cache_ttl_seconds` — `fee_controller.py:3134-3596,7924-7948` —
//! by `tools/port/gen_fees_fixtures.py market` in the port worktree). Each
//! case monkeypatched only the instance's gossip-fetch seam so the live
//! method body ran unmodified; nothing in the generator reimplements the
//! algorithm.
//!
//! Median/percentile/CLN-default/boundary/TTL results are plain integers,
//! booleans, or `null` — compared directly (no float parity concern).
//! `competitive_undercut_pct` and the network-prior `std`/`mean` floor
//! arithmetic involve floats: undercut expectations are `py_repr` strings,
//! compared via `revops_econ::pyfloat::py_repr` bit-for-bit (Phase 4
//! Global Constraints — no epsilon comparisons).

use revops_econ::pyfloat::py_repr;
use revops_fees::market::{
    competitive_undercut_pct, gossip_cache_ttl_seconds, is_cln_default_fee, market_boundary_fee,
    neighbor_fee_median, neighbor_fee_percentile, network_fee_prior, select_best_fee_prior,
    FeePrior, GossipCache, GossipChannel, GOSSIP_CACHE_DEFAULT_TTL_SECONDS,
};
use serde_json::Value;
use std::path::PathBuf;

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../fixtures/fees/market/{name}.json"));
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn parse_f(v: &Value) -> f64 {
    v.as_str().expect("repr string").parse().expect("parse f64")
}

fn channel_from_json(v: &Value) -> GossipChannel {
    GossipChannel {
        source: v["source"].as_str().expect("source").to_string(),
        destination: v["destination"].as_str().expect("destination").to_string(),
        fee_ppm: v["fee_ppm"].as_i64().expect("fee_ppm"),
        base_fee_msat: v["base_fee_msat"].as_i64().expect("base_fee_msat"),
        capacity_sats: v["capacity_sats"].as_i64().expect("capacity_sats"),
        last_update_ts: v["last_update_ts"].as_i64().expect("last_update_ts"),
    }
}

fn channels_from_json(v: &Value) -> Vec<GossipChannel> {
    v.as_array()
        .expect("peer_channels array")
        .iter()
        .map(channel_from_json)
        .collect()
}

#[test]
fn neighbor_fee_median_matches_python() {
    let fx = fixture("median");
    let cases = fx["cases"].as_array().expect("cases array");
    assert!(cases.len() >= 10, "expected the full median scenario set");

    for case in cases {
        let name = case["name"].as_str().expect("name");
        let our_id = case["our_id"].as_str().expect("our_id");
        let now = case["now"].as_i64().expect("now");
        let channels = channels_from_json(&case["peer_channels"]);
        let expected = case["expected"].as_i64();

        let actual = neighbor_fee_median(&channels, our_id, now);
        assert_eq!(actual, expected, "case {name}");
    }
}

#[test]
fn neighbor_fee_percentile_matches_python() {
    let fx = fixture("percentile");
    let cases = fx["cases"].as_array().expect("cases array");
    assert!(
        cases.len() >= 8,
        "expected the full percentile scenario set"
    );

    for case in cases {
        let name = case["name"].as_str().expect("name");
        let our_id = case["our_id"].as_str().expect("our_id");
        let now = case["now"].as_i64().expect("now");
        let pct = parse_f(&case["pct"]);
        let channels = channels_from_json(&case["peer_channels"]);
        let expected = case["expected"].as_i64();

        let actual = neighbor_fee_percentile(&channels, our_id, pct, now);
        assert_eq!(actual, expected, "case {name}");
    }
}

#[test]
fn is_cln_default_fee_matches_python() {
    let fx = fixture("cln_default");
    let cases = fx["cases"].as_array().expect("cases array");

    for case in cases {
        let label = case["label"].as_str().expect("label");
        let ch = &case["channel"];
        let fee_ppm = ch["fee_per_millionth"].as_i64().expect("fee_per_millionth");
        // Prefer the pre-24.x spelling, fall back to the post-24.x one,
        // default 0 when neither is present (Python's `None` case — any
        // concrete non-1000 stand-in reproduces the same "not default"
        // outcome as the real missing-field short circuit).
        let base_fee_msat = ch
            .get("base_fee_millisatoshi")
            .and_then(Value::as_i64)
            .or_else(|| ch.get("fee_base_msat").and_then(Value::as_i64))
            .unwrap_or(0);
        let expected = case["expected"].as_bool().expect("expected bool");

        let channel = GossipChannel {
            source: "peer".to_string(),
            destination: "peer".to_string(),
            fee_ppm,
            base_fee_msat,
            capacity_sats: 1,
            last_update_ts: 1,
        };
        assert_eq!(is_cln_default_fee(&channel), expected, "case {label}");
    }
}

#[test]
fn competitive_undercut_pct_matches_python() {
    let fx = fixture("undercut");
    let cases = fx["cases"].as_array().expect("cases array");
    assert!(
        cases.len() >= 20,
        "expected the full rank x median x invert grid"
    );

    for case in cases {
        let label = case["label"].as_str().expect("label");
        let capacity_rank = case["capacity_rank"].as_u64().expect("capacity_rank") as usize;
        let rank_count = case["rank_count"].as_u64().expect("rank_count") as usize;
        let neighbor_median = case["neighbor_median"].as_i64().expect("neighbor_median");
        let invert_rank = case["invert_rank"].as_bool().expect("invert_rank");
        let expected = case["expected"].as_str().expect("expected repr string");

        let actual =
            competitive_undercut_pct(capacity_rank, rank_count, neighbor_median, invert_rank);
        assert_eq!(py_repr(actual), expected, "case {label}");
    }
}

#[test]
fn competitive_undercut_pct_clamps_to_unit_interval() {
    let fx = fixture("undercut");
    for case in fx["cases"].as_array().expect("cases array") {
        let expected: f64 = case["expected"]
            .as_str()
            .expect("expected repr string")
            .parse()
            .expect("parse f64");
        assert!((0.03..=0.20).contains(&expected), "case {}", case["label"]);
    }
}

#[test]
fn market_boundary_fee_always_none_regardless_of_cfg() {
    let fx = fixture("boundary");
    let cases = fx["cases"].as_array().expect("cases array");
    assert_eq!(cases.len(), 2);

    for case in cases {
        let cfg_enabled = case["cfg_enabled"].as_bool().expect("cfg_enabled");
        assert!(case["expected"].is_null());
        assert_eq!(market_boundary_fee(cfg_enabled), None);
    }
}

#[test]
fn network_fee_prior_and_selection_match_python() {
    let fx = fixture("prior");
    let cases = fx["cases"].as_array().expect("cases array");
    assert!(cases.len() >= 5);

    for case in cases {
        let name = case["name"].as_str().expect("name");
        let allow_rpc = case["allow_rpc"].as_bool().expect("allow_rpc");
        let channels: Vec<GossipChannel> = case["channels_response"]["channels"]
            .as_array()
            .expect("channels array")
            .iter()
            .map(|c| GossipChannel {
                source: "peer".to_string(),
                destination: "us".to_string(),
                fee_ppm: c["fee_per_millionth"].as_i64().expect("fee_per_millionth"),
                base_fee_msat: 0,
                capacity_sats: c["satoshis"].as_i64().expect("satoshis"),
                last_update_ts: 1,
            })
            .collect();

        let prior = network_fee_prior(&channels);

        match &case["expected_network_prior"] {
            Value::Null => assert!(prior.is_none(), "case {name}: expected no network prior"),
            v => {
                let expected_mean = v["mean"].as_i64().expect("mean");
                let expected_std = v["std"].as_i64().expect("std");
                let got = prior
                    .clone()
                    .unwrap_or_else(|| panic!("case {name}: expected a network prior"));
                assert_eq!(got.mean, expected_mean, "case {name} mean");
                assert_eq!(got.std, expected_std, "case {name} std");
                assert_eq!(got.source, "network");
            }
        }

        // `_select_best_fee_prior(allow_rpc=False)` never consults the
        // network source at all (py 7930-7938) — an out-of-cycle caller
        // (network prior available) will still see no candidates from a
        // per-cycle caller.
        let candidates: Vec<FeePrior> = if allow_rpc {
            prior.into_iter().collect()
        } else {
            Vec::new()
        };
        let selected = select_best_fee_prior(&candidates);

        match &case["expected_selected"] {
            Value::Null => assert!(selected.is_none(), "case {name}: expected no selection"),
            v => {
                let expected = FeePrior {
                    mean: v["mean"].as_i64().expect("mean"),
                    std: v["std"].as_i64().expect("std"),
                    source: v["source"].as_str().expect("source").to_string(),
                };
                assert_eq!(selected, Some(expected), "case {name}");
            }
        }
    }
}

#[test]
fn gossip_cache_ttl_seconds_matches_python() {
    let fx = fixture("ttl");
    let cases = fx["cases"].as_array().expect("cases array");
    assert!(cases.len() >= 6);

    for case in cases {
        let fee_interval = case["fee_interval"].as_i64().expect("fee_interval");
        let expected = case["expected"].as_i64().expect("expected");
        assert_eq!(
            gossip_cache_ttl_seconds(fee_interval),
            expected,
            "fee_interval {fee_interval}"
        );
    }
}

// ---------------------------------------------------------------------------
// GossipCache: TTL freshness + 500-entry eviction, no wall clock (injected
// `now` throughout — Global Constraints).
// ---------------------------------------------------------------------------

#[test]
fn gossip_cache_default_ttl_matches_peer_inbound_channels_default() {
    assert_eq!(GOSSIP_CACHE_DEFAULT_TTL_SECONDS, 1800);
}

#[test]
fn gossip_cache_fresh_within_ttl_stale_after() {
    let mut cache = GossipCache::default();
    let t0 = 1_752_400_000_i64;
    cache.insert("k".to_string(), serde_json::json!({"v": 1}), t0);

    assert!(cache.get("k", t0, None).is_some());
    assert!(cache
        .get("k", t0 + GOSSIP_CACHE_DEFAULT_TTL_SECONDS - 1, None)
        .is_some());
    assert!(cache
        .get("k", t0 + GOSSIP_CACHE_DEFAULT_TTL_SECONDS, None)
        .is_none());
}

#[test]
fn gossip_cache_explicit_ttl_overrides_default() {
    let mut cache = GossipCache::default();
    let t0 = 1_752_400_000_i64;
    cache.insert("k".to_string(), serde_json::json!(1), t0);
    assert!(cache.get("k", t0 + 100, Some(50)).is_none());
    assert!(cache.get("k", t0 + 49, Some(50)).is_some());
}

#[test]
fn gossip_cache_evicts_stale_entries_past_500_threshold() {
    let mut cache = GossipCache::default();
    let now = 1_752_400_000_i64;
    // 501 entries: one stale (>3600s old), the rest fresh.
    cache.insert("stale".to_string(), serde_json::json!(0), now - 4000);
    for i in 0..500 {
        cache.insert(format!("k{i}"), serde_json::json!(i), now);
    }
    assert_eq!(cache.len(), 501);

    // Force a maintenance pass (an eviction-triggering insert).
    cache.insert("k500".to_string(), serde_json::json!(500), now);

    assert!(cache.get("stale", now, None).is_none());
    assert!(cache.get("k0", now, None).is_some());
}

#[test]
fn gossip_cache_no_eviction_under_threshold() {
    let mut cache = GossipCache::default();
    let now = 1_752_400_000_i64;
    cache.insert("stale".to_string(), serde_json::json!(0), now - 10_000);
    for i in 0..10 {
        cache.insert(format!("k{i}"), serde_json::json!(i), now);
    }
    assert_eq!(cache.len(), 11);
    // Still present: threshold (500) never crossed, no sweep ran. A large
    // TTL override isolates "was it removed" from "is it merely stale".
    assert!(cache.get("stale", now, Some(20_000)).is_some());
}
