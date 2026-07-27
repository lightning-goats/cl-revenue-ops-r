//! `planner::discovery` and `planner::demand_flow` parity.
//!
//! Task 47 correction round 1, finding 5: `discover_from_winners`,
//! `discover_from_graph`, `discover_from_neighbors`,
//! `discover_from_route_pairs`, `classify_peers`, and
//! `find_sink_adjacent_candidates` are now ALL pinned by
//! `fixtures/capital/planner/kernels.json` (generated from the REAL,
//! unmodified `modules/capacity_planner.py` — see
//! `tools/port/gen_capital_planner_fixtures.py`'s `discover_neighbors_case`
//! / `discover_route_pairs_case`). Finding 2:
//! `discover_from_neighbors_capital_efficiency` (the previously-unported
//! capital-efficiency-aware branch) is likewise fixture-verified
//! (`discover_neighbors_capital_efficiency_case`), covering the fallback,
//! efficiency-aware first-degree, and second-degree cases the review
//! required. The hand-derived tests further below remain as additional
//! boundary/tripwire coverage alongside the fixtures, not a substitute for
//! them.

use revops_capital::planner::demand_flow::{
    classify_peers, find_sink_adjacent_candidates, FlowRole, NodeFlowProfile, PeerFlowContribution,
    SinkChannelEdge,
};
use revops_capital::planner::discovery::{
    discover_from_graph, discover_from_neighbors, discover_from_neighbors_capital_efficiency,
    discover_from_route_pairs, discover_from_winners, GraphChannelEdge, NeighborChannelEdge,
    PatronCandidate, PatronPoolInput, RoutePairRow, WinnerForDiscovery,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/capital/planner/kernels.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn scenarios(kind: &str) -> Vec<Value> {
    fixture()["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == kind)
        .cloned()
        .collect()
}

// --- discover_from_winners --------------------------------------------------

#[test]
fn discover_from_winners_matches_python() {
    let cases = scenarios("discover_from_winners");
    assert_eq!(cases.len(), 4, "expected 4 discover_from_winners scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let winners: Vec<Value> = case["input"]["winners"].as_array().unwrap().clone();
        let refs: Vec<WinnerForDiscovery> = winners
            .iter()
            .map(|w| WinnerForDiscovery {
                peer_id: w["peer_id"].as_str().unwrap(),
                roi: w["roi"].as_f64().unwrap(),
            })
            .collect();
        let actual = discover_from_winners(&refs);
        let expected = case["output"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len(), "{name}: count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.peer_id, e["peer_id"].as_str().unwrap(), "{name}: peer_id");
            assert_eq!(a.source, "winner");
            assert_eq!(a.score, e["score"].as_f64().unwrap(), "{name}: score");
            assert_eq!(a.reason, e["reason"].as_str().unwrap(), "{name}: reason");
        }
    }
}

/// Revert tripwire: the `roi > 30.0` threshold must actually gate — a
/// removed/inverted filter would let a 25%-ROI winner through.
#[test]
fn discover_from_winners_filters_weak_winners() {
    let winners = vec![WinnerForDiscovery {
        peer_id: "p1",
        roi: 25.0,
    }];
    assert!(discover_from_winners(&winners).is_empty());
}

// --- discover_from_graph -----------------------------------------------------

#[test]
fn discover_from_graph_matches_python() {
    let cases = scenarios("discover_from_graph");
    assert_eq!(cases.len(), 7, "expected 7 discover_from_graph scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let our_node_id = case["input"]["our_node_id"].as_str().unwrap();
        let existing: BTreeSet<String> = case["input"]["existing_peer_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        let cache_val = &case["input"]["cached_source_channels"];
        let mut cache: BTreeMap<String, Vec<GraphChannelEdge>> = BTreeMap::new();
        for (node, chans) in cache_val.as_object().unwrap() {
            let edges = chans
                .as_array()
                .unwrap()
                .iter()
                .map(|c| GraphChannelEdge {
                    active: c["active"].as_bool().unwrap(),
                    amount_msat: c["amount_msat"].as_i64().unwrap(),
                })
                .collect();
            cache.insert(node.clone(), edges);
        }

        let actual = discover_from_graph(&cache, our_node_id, &existing);
        let expected = case["output"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len(), "{name}: count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.peer_id, e["peer_id"].as_str().unwrap(), "{name}: peer_id");
            assert_eq!(a.score, e["score"].as_f64().unwrap(), "{name}: score");
            assert_eq!(
                a.reason,
                e["reason"].as_str().unwrap(),
                "{name}: reason (comma-grouped capacity)"
            );
        }
    }
}

// --- discover_from_neighbors (Python-oracle fixture; Task 47 finding 5) ---

fn edges_from_channels(channels: &Value) -> Vec<(String, i64, i64)> {
    channels
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["destination"].as_str().unwrap().to_string(),
                c["amount_msat"].as_i64().unwrap(),
                c["fee_per_millionth"].as_i64().unwrap(),
            )
        })
        .collect()
}

#[test]
fn discover_from_neighbors_matches_python() {
    let cases = scenarios("discover_from_neighbors");
    assert_eq!(
        cases.len(),
        8,
        "expected 8 discover_from_neighbors scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let our_node_id = case["input"]["our_node_id"].as_str().unwrap();
        let rows: Vec<(String, String, f64)> = case["input"]["profitability_rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let arr = r.as_array().unwrap();
                (
                    arr[0].as_str().unwrap().to_string(),
                    arr[1].as_str().unwrap().to_string(),
                    arr[2].as_f64().unwrap(),
                )
            })
            .collect();
        let all_profitability: Vec<PatronCandidate> = rows
            .iter()
            .map(|(_, peer_id, roi)| PatronCandidate {
                peer_id: peer_id.clone(),
                marginal_roi_percent: *roi,
            })
            .collect();
        // py derives `existing_peers` from `all_profitability.values()`'s
        // peer_ids internally — no separate input field.
        let existing: BTreeSet<String> = rows.iter().map(|(_, p, _)| p.clone()).collect();

        let owned_channels: BTreeMap<String, Vec<(String, i64, i64)>> = case["input"]
            ["patron_channels"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(peer, chans)| (peer.clone(), edges_from_channels(chans)))
            .collect();
        let patron_channels: BTreeMap<String, Vec<NeighborChannelEdge>> = owned_channels
            .iter()
            .map(|(peer, edges)| {
                (
                    peer.clone(),
                    edges
                        .iter()
                        .map(|(dest, amt, fee)| NeighborChannelEdge {
                            destination: dest,
                            amount_msat: *amt,
                            fee_per_millionth: *fee,
                        })
                        .collect(),
                )
            })
            .collect();

        let actual =
            discover_from_neighbors(&all_profitability, &patron_channels, &existing, our_node_id);
        let expected = case["output"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len(), "{name}: count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.peer_id, e["peer_id"].as_str().unwrap(), "{name}: peer_id");
            assert_eq!(a.source, "neighbor");
            assert_eq!(a.score, e["score"].as_f64().unwrap(), "{name}: score");
            assert_eq!(a.reason, e["reason"].as_str().unwrap(), "{name}: reason");
        }
    }
}

// --- discover_from_neighbors_capital_efficiency (Python-oracle fixture;
// Task 47 finding 2 -- the previously-unported branch) ---------------------

#[test]
fn discover_from_neighbors_capital_efficiency_matches_python() {
    let cases = scenarios("discover_from_neighbors_capital_efficiency");
    assert_eq!(
        cases.len(),
        9,
        "expected 9 discover_from_neighbors_capital_efficiency scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let our_node_id = case["input"]["our_node_id"].as_str().unwrap();

        let rows: Vec<(String, String, f64, i64)> = case["input"]["profitability_rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let arr = r.as_array().unwrap();
                (
                    arr[0].as_str().unwrap().to_string(),
                    arr[1].as_str().unwrap().to_string(),
                    arr[2].as_f64().unwrap(),
                    arr[3].as_i64().unwrap(),
                )
            })
            .collect();
        let channel_efficiencies: BTreeMap<String, Option<f64>> = case["input"]
            ["channel_efficiencies"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(scid, rank)| (scid.clone(), rank.as_f64()))
            .collect();
        let existing: BTreeSet<String> = rows.iter().map(|(_, p, _, _)| p.clone()).collect();

        let patron_pool_inputs: Vec<PatronPoolInput> = rows
            .iter()
            .map(|(scid, peer_id, roi, vol)| PatronPoolInput {
                peer_id: peer_id.clone(),
                efficiency_rank: channel_efficiencies.get(scid).copied().flatten(),
                volume_routed_sats: *vol,
                marginal_roi_percent: *roi,
            })
            .collect();

        let owned_channels: BTreeMap<String, Vec<(String, i64, i64)>> = case["input"]
            ["source_channels"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(peer, chans)| (peer.clone(), edges_from_channels(chans)))
            .collect();
        let source_channels: BTreeMap<String, Vec<NeighborChannelEdge>> = owned_channels
            .iter()
            .map(|(peer, edges)| {
                (
                    peer.clone(),
                    edges
                        .iter()
                        .map(|(dest, amt, fee)| NeighborChannelEdge {
                            destination: dest,
                            amount_msat: *amt,
                            fee_per_millionth: *fee,
                        })
                        .collect(),
                )
            })
            .collect();

        let actual = discover_from_neighbors_capital_efficiency(
            &patron_pool_inputs,
            &source_channels,
            &existing,
            our_node_id,
        );
        let expected = case["output"].as_array().unwrap();
        assert_eq!(
            actual.len(),
            expected.len(),
            "{name}: count. actual={actual:?}"
        );
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.peer_id, e["peer_id"].as_str().unwrap(), "{name}: peer_id");
            assert_eq!(a.source, "neighbor");
            assert!(
                (a.score - e["score"].as_f64().unwrap()).abs() < 1e-9,
                "{name}: score for {}: got {} want {}",
                a.peer_id,
                a.score,
                e["score"].as_f64().unwrap()
            );
        }
    }
}

// --- discover_from_neighbors (hand-derived; see module doc comment) --------

#[test]
fn discover_from_neighbors_picks_top_3_patrons_by_marginal_roi() {
    let all: Vec<PatronCandidate> = vec![
        PatronCandidate {
            peer_id: "low".to_string(),
            marginal_roi_percent: 1.0,
        },
        PatronCandidate {
            peer_id: "mid".to_string(),
            marginal_roi_percent: 50.0,
        },
        PatronCandidate {
            peer_id: "high".to_string(),
            marginal_roi_percent: 90.0,
        },
        PatronCandidate {
            peer_id: "excluded".to_string(),
            marginal_roi_percent: 0.5,
        },
    ];
    let mut patron_channels: BTreeMap<String, Vec<NeighborChannelEdge>> = BTreeMap::new();
    patron_channels.insert(
        "excluded".to_string(),
        vec![NeighborChannelEdge {
            destination: "should_not_appear",
            amount_msat: 1_000_000_000,
            fee_per_millionth: 10,
        }],
    );
    patron_channels.insert(
        "high".to_string(),
        vec![NeighborChannelEdge {
            destination: "neighbor1",
            amount_msat: 1_000_000_000,
            fee_per_millionth: 10,
        }],
    );
    let existing = BTreeSet::new();
    let out = discover_from_neighbors(&all, &patron_channels, &existing, "us");
    assert!(
        out.iter().all(|c| c.peer_id != "should_not_appear"),
        "4th-ranked patron must not contribute"
    );
    assert!(out.iter().any(|c| c.peer_id == "neighbor1"));
}

#[test]
fn discover_from_neighbors_filters_expensive_and_tiny_channels() {
    let all = vec![PatronCandidate {
        peer_id: "patron".to_string(),
        marginal_roi_percent: 50.0,
    }];
    let mut patron_channels: BTreeMap<String, Vec<NeighborChannelEdge>> = BTreeMap::new();
    patron_channels.insert(
        "patron".to_string(),
        vec![
            NeighborChannelEdge {
                destination: "expensive",
                amount_msat: 1_000_000_000,
                fee_per_millionth: 2000,
            },
            NeighborChannelEdge {
                destination: "tiny",
                amount_msat: 100_000_000,
                fee_per_millionth: 10,
            },
            NeighborChannelEdge {
                destination: "good",
                amount_msat: 1_000_000_000,
                fee_per_millionth: 10,
            },
        ],
    );
    let existing = BTreeSet::new();
    let out = discover_from_neighbors(&all, &patron_channels, &existing, "us");
    let ids: Vec<&str> = out.iter().map(|c| c.peer_id.as_str()).collect();
    assert_eq!(ids, vec!["good"]);
}

// --- discover_from_route_pairs (Python-oracle fixture; Task 47 finding 5) -

#[test]
fn discover_from_route_pairs_matches_python() {
    let cases = scenarios("discover_from_route_pairs");
    assert_eq!(
        cases.len(),
        8,
        "expected 8 discover_from_route_pairs scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let our_node_id = case["input"]["our_node_id"].as_str().unwrap();

        // py's `_discover_from_route_pairs` converts each row's ':' SCID
        // form to 'x' form internally; the Rust port takes already-'x'-form
        // rows (matching `channel_to_peer`'s keys) -- convert here, at the
        // fixture boundary, exactly where Python's own conversion happens.
        let rows: Vec<RoutePairRow> = case["input"]["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| RoutePairRow {
                in_channel: r["in_channel"].as_str().unwrap().replace(':', "x"),
                out_channel: r["out_channel"].as_str().unwrap().replace(':', "x"),
                total_fee_msat: r["total_fee_msat"].as_i64().unwrap(),
            })
            .collect();

        let profitability_rows: Vec<(String, String)> = case["input"]["profitability_rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let arr = r.as_array().unwrap();
                (
                    arr[0].as_str().unwrap().to_string(),
                    arr[1].as_str().unwrap().to_string(),
                )
            })
            .collect();
        let mut channel_to_peer: BTreeMap<String, String> = BTreeMap::new();
        let mut existing: BTreeSet<String> = BTreeSet::new();
        for (scid, peer_id) in &profitability_rows {
            channel_to_peer.insert(scid.clone(), peer_id.clone());
            existing.insert(peer_id.clone());
        }

        let owned_channels: BTreeMap<String, Vec<(String, i64, i64)>> = case["input"]
            ["route_pairs_channels"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(peer, chans)| (peer.clone(), edges_from_channels(chans)))
            .collect();
        let route_peer_channels: BTreeMap<String, Vec<NeighborChannelEdge>> = owned_channels
            .iter()
            .map(|(peer, edges)| {
                (
                    peer.clone(),
                    edges
                        .iter()
                        .map(|(dest, amt, fee)| NeighborChannelEdge {
                            destination: dest,
                            amount_msat: *amt,
                            fee_per_millionth: *fee,
                        })
                        .collect(),
                )
            })
            .collect();

        let actual = discover_from_route_pairs(
            &rows,
            &channel_to_peer,
            &route_peer_channels,
            &existing,
            our_node_id,
        );
        let expected = case["output"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len(), "{name}: count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.peer_id, e["peer_id"].as_str().unwrap(), "{name}: peer_id");
            assert_eq!(a.source, "route_pair");
            assert_eq!(a.score, e["score"].as_f64().unwrap(), "{name}: score");
            assert_eq!(a.reason, e["reason"].as_str().unwrap(), "{name}: reason");
        }
    }
}

// --- discover_from_route_pairs (hand-derived; see module doc comment) ------

#[test]
fn discover_from_route_pairs_ranks_by_fee_weighted_score() {
    let rows = vec![RoutePairRow {
        in_channel: "700000x1x0".to_string(),
        out_channel: "700000x2x0".to_string(),
        total_fee_msat: 5_000_000,
    }];
    let mut channel_to_peer = BTreeMap::new();
    channel_to_peer.insert("700000x1x0".to_string(), "peerA".to_string());
    channel_to_peer.insert("700000x2x0".to_string(), "peerB".to_string());

    let mut route_peer_channels: BTreeMap<String, Vec<NeighborChannelEdge>> = BTreeMap::new();
    route_peer_channels.insert(
        "peerA".to_string(),
        vec![NeighborChannelEdge {
            destination: "cand1",
            amount_msat: 6_000_000_000,
            fee_per_millionth: 100,
        }],
    );
    let existing = BTreeSet::new();
    let out = discover_from_route_pairs(
        &rows,
        &channel_to_peer,
        &route_peer_channels,
        &existing,
        "us",
    );
    assert!(out.iter().any(|c| c.peer_id == "cand1"));
}

/// Revert tripwire: the `fee_ppm > 1000 || capacity < 500_000` filter must
/// actually exclude weak candidates.
#[test]
fn discover_from_route_pairs_filters_expensive_or_tiny() {
    let rows = vec![RoutePairRow {
        in_channel: "700000x1x0".to_string(),
        out_channel: "700000x2x0".to_string(),
        total_fee_msat: 1_000_000,
    }];
    let mut channel_to_peer = BTreeMap::new();
    channel_to_peer.insert("700000x1x0".to_string(), "peerA".to_string());

    let mut route_peer_channels: BTreeMap<String, Vec<NeighborChannelEdge>> = BTreeMap::new();
    route_peer_channels.insert(
        "peerA".to_string(),
        vec![NeighborChannelEdge {
            destination: "tiny",
            amount_msat: 100_000_000,
            fee_per_millionth: 10,
        }],
    );
    let existing = BTreeSet::new();
    let out = discover_from_route_pairs(
        &rows,
        &channel_to_peer,
        &route_peer_channels,
        &existing,
        "us",
    );
    assert!(out.is_empty());
}

// --- demand_flow: classify_peers / find_sink_adjacent_candidates -----------

#[test]
fn classify_peers_matches_python() {
    let cases = scenarios("classify_peers");
    assert_eq!(cases.len(), 7, "expected 7 classify_peers scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let flows: Vec<(String, String, i64, i64)> = case["input"]["flows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| {
                let arr = f.as_array().unwrap();
                (
                    arr[0].as_str().unwrap().to_string(),
                    arr[1].as_str().unwrap().to_string(),
                    arr[2].as_i64().unwrap(),
                    arr[3].as_i64().unwrap(),
                )
            })
            .collect();
        let contributions: Vec<PeerFlowContribution> = flows
            .iter()
            .map(|(_, peer_id, sats_in, sats_out)| PeerFlowContribution {
                peer_id,
                sats_in: *sats_in,
                sats_out: *sats_out,
            })
            .collect();
        let profiles = classify_peers(&contributions);
        let expected = case["output"].as_object().unwrap();
        assert_eq!(profiles.len(), expected.len(), "{name}: profile count");
        for p in &profiles {
            let e = &expected[&p.node_id];
            let role_str = match p.role {
                FlowRole::Unknown => "unknown",
                FlowRole::Source => "source",
                FlowRole::Sink => "sink",
                FlowRole::Router => "router",
            };
            assert_eq!(
                role_str,
                e["role"].as_str().unwrap(),
                "{name}: role for {}",
                p.node_id
            );
            assert_eq!(
                p.confidence,
                e["confidence"].as_f64().unwrap(),
                "{name}: confidence"
            );
            assert_eq!(
                p.net_flow_ratio,
                e["net_flow_ratio"].as_f64(),
                "{name}: net_flow_ratio"
            );
        }
    }
}

#[test]
fn find_sink_adjacent_candidates_matches_python() {
    let cases = scenarios("find_sink_adjacent_candidates");
    assert_eq!(
        cases.len(),
        6,
        "expected 6 find_sink_adjacent_candidates scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let sink_profiles: Vec<NodeFlowProfile> = case["input"]["sink_profiles"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                let arr = s.as_array().unwrap();
                NodeFlowProfile {
                    node_id: arr[0].as_str().unwrap().to_string(),
                    role: FlowRole::Sink,
                    confidence: arr[1].as_f64().unwrap(),
                    net_flow_ratio: arr[2].as_f64(),
                }
            })
            .collect();
        let mut sink_channels: BTreeMap<String, Vec<SinkChannelEdge>> = BTreeMap::new();
        for (node, chans) in case["input"]["sink_channels"].as_object().unwrap() {
            let edges = chans
                .as_array()
                .unwrap()
                .iter()
                .map(|c| SinkChannelEdge {
                    destination: c["destination"].as_str().unwrap().to_string(),
                    active: c["active"].as_bool().unwrap(),
                })
                .collect();
            sink_channels.insert(node.clone(), edges);
        }
        let existing: BTreeSet<String> = case["input"]["existing_peers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        let actual = find_sink_adjacent_candidates(&sink_profiles, &sink_channels, &existing);
        let expected = case["output"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len(), "{name}: count");
        for (a, e) in actual.iter().zip(expected.iter()) {
            assert_eq!(a.peer_id, e["peer_id"].as_str().unwrap(), "{name}: peer_id");
            assert_eq!(a.score, e["score"].as_f64().unwrap(), "{name}: score");
        }
    }
}

// --- Task 47 correction round 1, finding 4: tie ordering -------------------

/// Two equal-scoring, non-colliding candidates whose channel-list
/// (discovery) order differs from their peer-id sort order. Python's
/// `_discover_from_route_pairs` dedups via a `dict` (insertion order
/// preserved) then a STABLE score-sort — ties keep first-discovery order.
#[test]
fn discover_from_route_pairs_preserves_first_discovery_order_for_equal_score_ties() {
    let rows = vec![RoutePairRow {
        in_channel: "700000x1x0".to_string(),
        out_channel: "700000x2x0".to_string(),
        total_fee_msat: 5_000_000,
    }];
    let mut channel_to_peer = BTreeMap::new();
    channel_to_peer.insert("700000x1x0".to_string(), "routePeer".to_string());

    let mut route_peer_channels: BTreeMap<String, Vec<NeighborChannelEdge>> = BTreeMap::new();
    route_peer_channels.insert(
        "routePeer".to_string(),
        vec![
            // Discovery order: "zzz_dest" first, "aaa_dest" second — the
            // OPPOSITE of peer-id sort order. Identical amount/fee -> equal
            // score, so this is a genuine tie.
            NeighborChannelEdge {
                destination: "zzz_dest",
                amount_msat: 6_000_000_000,
                fee_per_millionth: 100,
            },
            NeighborChannelEdge {
                destination: "aaa_dest",
                amount_msat: 6_000_000_000,
                fee_per_millionth: 100,
            },
        ],
    );
    let existing = BTreeSet::new();
    let out = discover_from_route_pairs(
        &rows,
        &channel_to_peer,
        &route_peer_channels,
        &existing,
        "us",
    );
    let zzz_idx = out
        .iter()
        .position(|c| c.peer_id == "zzz_dest")
        .expect("zzz_dest present");
    let aaa_idx = out
        .iter()
        .position(|c| c.peer_id == "aaa_dest")
        .expect("aaa_dest present");
    assert!(
        zzz_idx < aaa_idx,
        "equal-score ties must preserve discovery order (zzz_dest first), not peer-id sort \
         order; got {:?}",
        out.iter().map(|c| &c.peer_id).collect::<Vec<_>>()
    );
}
