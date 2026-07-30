//! Task 67c slice 1: assemble `DiscoveryEvidence` from the ONE gossip
//! prefetch the fee loop already performs.

use std::collections::HashMap;

use revops::discovery_evidence::{build_discovery_evidence, DiscoveryRefusal, GraphSources};
use serde_json::json;

const NOW: i64 = 1_800_000_000;

fn graph() -> serde_json::Value {
    json!([
        {"source":"02aa","destination":"02bb","short_channel_id":"700x1x0",
         "amount_msat":5_000_000_000i64,"fee_per_millionth":100,"active":true},
        {"source":"02bb","destination":"02aa","short_channel_id":"700x1x0",
         "amount_msat":5_000_000_000i64,"fee_per_millionth":150,"active":true},
        {"source":"02aa","destination":"02cc","short_channel_id":"800x1x0",
         "amount_msat":2_000_000_000i64,"fee_per_millionth":50,"active":false}
    ])
}

fn sources(marginal: HashMap<String, f64>) -> GraphSources {
    GraphSources {
        gossip_channels: Ok(graph().as_array().unwrap().clone()),
        route_pair_rows: Ok(Vec::new()),
        marginal_roi_by_peer: marginal,
        demand_flows: Vec::new(),
        demand_flow_sink_channels: Default::default(),
        our_node_id: "02us".into(),
        max_candidate_pool: revops::discovery_evidence::DEFAULT_MAX_CANDIDATE_POOL,
        now: NOW,
    }
}

/// Edges are grouped BY SOURCE, because every patron lookup is
/// `listchannels(source=peer_id)`. Grouping by destination would silently
/// answer a different question and hand the frozen strategies the wrong
/// neighbourhood.
#[test]
fn edges_are_grouped_by_source() {
    let ev = build_discovery_evidence(sources(HashMap::new())).expect("assembles");
    let from_aa = ev
        .neighbor_patron_source_channels
        .get("02aa")
        .expect("02aa has outgoing edges");
    assert_eq!(from_aa.len(), 2, "both of 02aa's outgoing edges");
    assert!(from_aa.iter().any(|e| e.destination == "02bb"));
    assert!(from_aa.iter().any(|e| e.destination == "02cc"));
    // 02bb sources exactly one edge, back to 02aa.
    let from_bb = ev.neighbor_patron_source_channels.get("02bb").unwrap();
    assert_eq!(from_bb.len(), 1);
    assert_eq!(from_bb[0].destination, "02aa");
    assert_eq!(from_bb[0].fee_per_millionth, 150);
    // 02cc sources nothing -- it is a destination only.
    assert!(!ev.neighbor_patron_source_channels.contains_key("02cc"));
}

/// The graph strategy counts ACTIVE channels, so the active flag must
/// survive the projection rather than defaulting to true.
#[test]
fn graph_edges_preserve_the_active_flag() {
    let ev = build_discovery_evidence(sources(HashMap::new())).expect("assembles");
    let edges = ev.graph_cached_source_channels.get("02aa").unwrap();
    assert_eq!(edges.len(), 2);
    assert_eq!(edges.iter().filter(|e| e.active).count(), 1);
    assert_eq!(
        edges.iter().filter(|e| !e.active).count(),
        1,
        "an inactive channel must NOT be counted as active: {edges:?}"
    );
}

/// `channel_to_peer` maps an scid to the peer on the other end, so it must
/// be built from BOTH endpoints -- a one-sided map silently loses half the
/// route-pair lookups.
#[test]
fn channel_to_peer_covers_both_endpoints() {
    let ev = build_discovery_evidence(sources(HashMap::new())).expect("assembles");
    let mapped = ev.channel_to_peer.get("700x1x0").expect("scid mapped");
    assert!(
        mapped == "02aa" || mapped == "02bb",
        "must map to one of the endpoints, got {mapped}"
    );
    assert!(ev.channel_to_peer.contains_key("800x1x0"));
}

/// Patrons carry the marginal ROI that task 67b's assembler produced.
#[test]
fn patrons_carry_marginal_roi_from_profitability() {
    let marginal = HashMap::from([("02aa".to_string(), 250.0), ("02bb".to_string(), -30.0)]);
    let ev = build_discovery_evidence(sources(marginal)).expect("assembles");
    let aa = ev
        .all_channels
        .iter()
        .find(|p| p.peer_id == "02aa")
        .expect("02aa is a patron");
    assert!((aa.marginal_roi_percent - 250.0).abs() < 1e-9);
    // A peer with no profitability entry is NOT invented as a 0% patron --
    // zero is a real ROI and would rank it against genuine data.
    assert!(
        !ev.all_channels.iter().any(|p| p.peer_id == "02cc"),
        "peers without profitability must be absent, not zeroed: {:?}",
        ev.all_channels
    );
}

/// An unreadable graph REFUSES. The frozen strategies are total over empty
/// inputs, so an empty graph would produce a confident "no candidates
/// anywhere" indistinguishable from a genuinely sparse network.
#[test]
fn an_unreadable_graph_refuses_rather_than_yielding_no_candidates() {
    let mut s = sources(HashMap::new());
    s.gossip_channels = Err("listchannels rpc timeout".into());
    let err = build_discovery_evidence(s).expect_err("must refuse");
    assert_eq!(err.code(), "discovery_gossip_unavailable");
    assert!(matches!(err, DiscoveryRefusal::GossipUnavailable(_)));

    let mut s = sources(HashMap::new());
    s.route_pair_rows = Err("route pair read failed".into());
    let err = build_discovery_evidence(s).expect_err("must refuse");
    assert_eq!(err.code(), "discovery_route_pairs_unavailable");
}

/// A genuinely EMPTY graph is a valid observation, distinct from an
/// unreadable one.
#[test]
fn an_empty_but_readable_graph_is_a_valid_observation() {
    let s = GraphSources {
        gossip_channels: Ok(Vec::new()),
        route_pair_rows: Ok(Vec::new()),
        marginal_roi_by_peer: HashMap::new(),
        demand_flows: Vec::new(),
        demand_flow_sink_channels: Default::default(),
        our_node_id: "02us".into(),
        max_candidate_pool: revops::discovery_evidence::DEFAULT_MAX_CANDIDATE_POOL,
        now: NOW,
    };
    let ev = build_discovery_evidence(s).expect("empty is readable");
    assert!(ev.all_channels.is_empty());
    assert!(ev.neighbor_patron_source_channels.is_empty());
}
