//! Task 67c slice 1: assemble `DiscoveryEvidence` from the ONE gossip
//! prefetch the fee loop already performs.

use std::collections::{BTreeMap, HashMap};

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
        our_channel_scid_to_peer: BTreeMap::from([
            // Our channel with 02aa. Note the CLN ':' form -- Python
            // normalizes it to 'x' before every lookup.
            ("900:1:0".to_string(), "02aa".to_string()),
        ]),
        neighbor_capital_efficiency: None,
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

/// `channel_to_peer` maps OUR channel scids to the PEER on the far end,
/// built from our own profitability set -- NOT from the gossip
/// `destination` field (py 1828-1835).
///
/// Gossip carries both directions of every channel, so reading
/// `destination` resolves our own channel to whichever direction happened
/// to be listed first: half the time, to OUR OWN node id. Route-pair
/// discovery then scores our own node as a candidate peer and hunts for
/// neighbours of ourselves. Nothing errors; the strategy just quietly
/// searches the wrong neighbourhood.
#[test]
fn channel_to_peer_maps_our_channels_to_the_far_peer() {
    let ev = build_discovery_evidence(sources(HashMap::new())).expect("assembles");
    assert_eq!(
        ev.channel_to_peer.get("900x1x0").map(String::as_str),
        Some("02aa"),
        "must resolve to the PEER, never to our own node id: {:?}",
        ev.channel_to_peer
    );
    assert!(
        !ev.channel_to_peer.values().any(|p| p == "02us"),
        "our own node id must never be a mapped peer: {:?}",
        ev.channel_to_peer
    );
}

/// Scids are normalized from CLN's `:` form to the `x` form before being
/// keyed, because route-pair rows are looked up in the `x` form (py 1835:
/// `str(scid).replace(':', 'x')`). An unnormalized key never matches and
/// the route-pair strategy silently finds nothing.
#[test]
fn scids_are_normalized_to_the_x_form() {
    let ev = build_discovery_evidence(sources(HashMap::new())).expect("assembles");
    assert!(
        ev.channel_to_peer.contains_key("900x1x0"),
        "':' must be normalized to 'x': {:?}",
        ev.channel_to_peer
    );
    assert!(!ev.channel_to_peer.contains_key("900:1:0"));
}

/// Gossip-only channels -- ones we are not party to -- are NOT in the map.
/// Python builds it exclusively from our own profitability set, so
/// including the whole graph would resolve route pairs to strangers.
#[test]
fn foreign_channels_are_not_mapped() {
    let ev = build_discovery_evidence(sources(HashMap::new())).expect("assembles");
    assert!(
        !ev.channel_to_peer.contains_key("700x1x0"),
        "700x1x0 is a foreign gossip channel, not ours: {:?}",
        ev.channel_to_peer
    );
}

/// Mutation C10 SURVIVED the first matrix: discovery read `amount_msat`
/// through a hand-rolled helper with no test covering the non-integer
/// forms CLN actually emits. Same class as review finding F71-R1 in the
/// sibling enrichment site -- a dropped capacity silently changes which
/// neighbours look worth opening to.
#[test]
fn edge_amounts_accept_every_cln_msat_form() {
    let mut s = sources(HashMap::new());
    s.gossip_channels = Ok(json!([
        {"source":"02aa","destination":"02s","short_channel_id":"701x1x0",
         "amount_msat":"5000000000msat","fee_per_millionth":100,"active":true},
        {"source":"02aa","destination":"02b","short_channel_id":"702x1x0",
         "amount_msat":"4000000000","fee_per_millionth":100,"active":true},
        {"source":"02aa","destination":"02f","short_channel_id":"703x1x0",
         "amount_msat":3_000_000_000.0,"fee_per_millionth":100,"active":true},
    ])
    .as_array()
    .unwrap()
    .clone());
    let ev = build_discovery_evidence(s).expect("assembles");
    let mut amounts: Vec<i64> = ev.neighbor_patron_source_channels["02aa"]
        .iter()
        .map(|e| e.amount_msat)
        .collect();
    amounts.sort_unstable();
    assert_eq!(
        amounts,
        vec![3_000_000_000, 4_000_000_000, 5_000_000_000],
        "suffixed string, bare string and float forms must all parse"
    );
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
        our_channel_scid_to_peer: BTreeMap::from([
            // Our channel with 02aa. Note the CLN ':' form -- Python
            // normalizes it to 'x' before every lookup.
            ("900:1:0".to_string(), "02aa".to_string()),
        ]),
        neighbor_capital_efficiency: None,
        our_node_id: "02us".into(),
        max_candidate_pool: revops::discovery_evidence::DEFAULT_MAX_CANDIDATE_POOL,
        now: NOW,
    };
    let ev = build_discovery_evidence(s).expect("empty is readable");
    assert!(ev.all_channels.is_empty());
    assert!(ev.neighbor_patron_source_channels.is_empty());
}

/// F71-R4: when a capital-efficiency snapshot exists it must reach the
/// evidence, so the frozen kernel takes Python's COMMON path
/// (`discover_from_neighbors_capital_efficiency`) rather than the fallback
/// (`discover_from_neighbors`). Hardcoding `None` here silently ran a
/// different discovery strategy and produced a different candidate set --
/// with no error and no gap.
#[test]
fn capital_efficiency_reaches_the_evidence() {
    use revops::capital_efficiency::{
        efficiency_ranks, patron_pool_inputs, EfficiencyInput, PatronInput,
    };

    let ranks = efficiency_ranks(&[
        EfficiencyInput {
            scid: "700x1x0".into(),
            capacity_sats: 1_000_000,
            fees_earned_msat: 800_000_000,
            marginal_profit_30d_sats: Some(5_000),
        },
        EfficiencyInput {
            scid: "800x1x0".into(),
            capacity_sats: 1_000_000,
            fees_earned_msat: 10_000_000,
            marginal_profit_30d_sats: Some(100),
        },
    ]);
    let pool = patron_pool_inputs(
        &[PatronInput {
            peer_id: "02aa".into(),
            scid: "700x1x0".into(),
            volume_routed_sats: 5_000_000,
            marginal_roi_percent: 250.0,
        }],
        &ranks,
    );

    let mut s = sources(HashMap::new());
    s.neighbor_capital_efficiency = Some(pool);
    let ev = build_discovery_evidence(s).expect("assembles");

    let carried = ev
        .neighbor_capital_efficiency
        .as_ref()
        .expect("Some selects the common strategy; None forces the fallback");
    assert_eq!(carried.len(), 1);
    assert_eq!(carried[0].peer_id, "02aa");
    assert_eq!(
        carried[0].efficiency_rank,
        Some(1.0),
        "the top-ranked channel's rank must reach the kernel"
    );

    // And absence stays representable: a node with no snapshot.
    let ev = build_discovery_evidence(sources(HashMap::new())).expect("assembles");
    assert!(ev.neighbor_capital_efficiency.is_none());
}
