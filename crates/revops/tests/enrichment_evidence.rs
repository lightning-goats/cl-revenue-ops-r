//! Task 67c slice 2: per-candidate enrichment.
//!
//! Two Python behaviours here are counter-intuitive enough that the obvious
//! Rust implementation would silently diverge, so both are pinned:
//!
//!  * a peer with NO connection history is 100% up, not 0% and not unknown
//!    (py `get_peer_uptime_percent` 7235: the COLD START branch);
//!  * fewer than three successful rebalances yields NO inbound-fee signal,
//!    not a fee computed from one or two samples (py 4877).

use std::collections::{BTreeMap, HashMap};

use revops::enrichment_evidence::{
    build_enrichment, inbound_fee_ppm, uptime_percent, ConnectionEvent, EnrichmentSources,
    InboundFeeSample, ReputationRow,
};
use revops_capital::planner::demand_flow::FlowRole;
use serde_json::json;

const NOW: i64 = 1_800_000_000;
const WEEK: i64 = 604_800;

fn sources() -> EnrichmentSources {
    EnrichmentSources {
        reputation: Ok(HashMap::from([(
            "02aa".to_string(),
            ReputationRow {
                success_count: 8,
                failure_count: 2,
            },
        )])),
        connection_events: Ok(HashMap::new()),
        inbound_fee_samples: Ok(HashMap::new()),
        closed_channel_roi_proxy: Ok(HashMap::from([("02aa".to_string(), 45.0)])),
        gossip_channels: Ok(json!([
            {"source":"02zz","destination":"02aa","amount_msat":3_000_000_000i64,"active":true},
            {"source":"02yy","destination":"02aa","amount_msat":0i64,"active":true},
            {"source":"02xx","destination":"02aa","amount_msat":9_000_000_000i64,"active":false},
        ])
        .as_array()
        .unwrap()
        .clone()),
        clearnet_peers: Ok(["02aa".to_string()].into_iter().collect()),
        sink_adjacent_peers: Default::default(),
        demand_flow_roles: BTreeMap::new(),
        now: NOW,
    }
}

/// A peer with full data gets every field populated, and the reputation
/// counts survive intact -- the frozen scorer computes a Laplace ratio from
/// them, so swapping successes and failures inverts the score.
#[test]
fn a_fully_known_peer_is_enriched() {
    let mut s = sources();
    s.inbound_fee_samples = Ok(HashMap::from([(
        "02aa".to_string(),
        vec![
            InboundFeeSample {
                amount_sats: 100_000,
                fee_msat: 10_000,
            },
            InboundFeeSample {
                amount_sats: 100_000,
                fee_msat: 20_000,
            },
            InboundFeeSample {
                amount_sats: 100_000,
                fee_msat: 30_000,
            },
        ],
    )]));
    let e = build_enrichment(&["02aa".to_string()], s).expect("assembles");
    let a = &e["02aa"];
    let rep = a.reputation.expect("reputation present");
    assert_eq!(rep.successes, 8);
    assert_eq!(rep.failures, 2);
    assert!(a.has_clearnet_address);
    assert_eq!(
        a.closed_channel_profit.map(|c| c.marginal_roi_proxy),
        Some(45.0)
    );
    // median of 100, 200, 300 ppm
    assert_eq!(a.inbound_median_fee_ppm, Some(200.0));
}

/// ABSENT is not zero. A peer with no reputation row must score as
/// "unknown", not as a peer with 0 successes and 0 failures -- the frozen
/// scorer multiplies by a Laplace ratio, so a fabricated (0,0) silently
/// halves the score of every unmeasured peer.
#[test]
fn absent_enrichment_is_none_not_zero() {
    let e = build_enrichment(&["02unknown".to_string()], sources()).expect("assembles");
    let u = &e["02unknown"];
    assert!(u.reputation.is_none(), "no row must be None, not (0,0)");
    assert!(
        u.inbound_median_fee_ppm.is_none(),
        "no fee history must be None, not 0.0"
    );
    assert!(u.closed_channel_profit.is_none());
    assert!(!u.has_clearnet_address, "unknown peers are not clearnet");
}

/// The COLD START rule (py 7235). A peer we have never observed connecting
/// is assumed 100% up. Neither of the two natural Rust answers is right:
/// `None` would drop the multiplier and `Some(0.0)` would zero out every
/// newly-discovered peer -- which is EXACTLY the peer set discovery exists
/// to surface, so the bug would be invisible until no channel ever opened.
#[test]
fn a_peer_with_no_connection_history_is_fully_up() {
    assert_eq!(uptime_percent(&[], NOW, WEEK), 100.0);
    let e = build_enrichment(&["02unknown".to_string()], sources()).expect("assembles");
    assert_eq!(e["02unknown"].uptime_pct, Some(100.0));
}

/// Uptime walks the event sequence and sums connected intervals. With
/// history that begins INSIDE the window, the denominator is time since
/// that first event, not the whole window (py 7244) -- otherwise a peer
/// first seen yesterday reads as 14% up over a week and never qualifies.
#[test]
fn uptime_sums_connected_intervals_over_the_observed_span() {
    // First seen 1000s ago, connected the whole time.
    let events = vec![ConnectionEvent {
        event_type: "connected".into(),
        timestamp: NOW - 1_000,
    }];
    assert_eq!(uptime_percent(&events, NOW, WEEK), 100.0);

    // Connected 1000s ago, disconnected 500s ago: half of the observed span.
    let events = vec![
        ConnectionEvent {
            event_type: "connected".into(),
            timestamp: NOW - 1_000,
        },
        ConnectionEvent {
            event_type: "disconnected".into(),
            timestamp: NOW - 500,
        },
    ];
    assert_eq!(uptime_percent(&events, NOW, WEEK), 50.0);
}

/// `snapshot` counts as CONNECTED (py 7224/7263). The startup-snapshot
/// owner writes `snapshot` rows for every connected peer, so treating them
/// as anything else would read the entire fleet as down after a restart.
#[test]
fn a_snapshot_event_means_connected() {
    let events = vec![ConnectionEvent {
        event_type: "snapshot".into(),
        timestamp: NOW - 1_000,
    }];
    assert_eq!(uptime_percent(&events, NOW, WEEK), 100.0);
}

/// Fewer than three successful rebalances is NO signal (py 4877). Two
/// expensive rebalances must not brand a peer as high-fee forever.
#[test]
fn inbound_fee_needs_three_samples() {
    let two = vec![
        InboundFeeSample {
            amount_sats: 100_000,
            fee_msat: 10_000,
        },
        InboundFeeSample {
            amount_sats: 100_000,
            fee_msat: 20_000,
        },
    ];
    assert_eq!(
        inbound_fee_ppm(&two),
        None,
        "2 samples is below min_samples"
    );
    let three = vec![
        InboundFeeSample {
            amount_sats: 100_000,
            fee_msat: 10_000,
        },
        InboundFeeSample {
            amount_sats: 100_000,
            fee_msat: 20_000,
        },
        InboundFeeSample {
            amount_sats: 100_000,
            fee_msat: 30_000,
        },
    ];
    assert_eq!(inbound_fee_ppm(&three), Some(200.0));
}

/// Destination capacities are pre-filtered exactly as Python's list
/// comprehension does: ACTIVE and amount_msat > 0.
#[test]
fn destination_capacities_filter_inactive_and_zero() {
    let e = build_enrichment(&["02aa".to_string()], sources()).expect("assembles");
    let caps = &e["02aa"].dest_channel_capacities_sats;
    assert_eq!(
        caps.len(),
        1,
        "only the active, positive-amount channel counts: {caps:?}"
    );
    assert_eq!(caps[0], 3_000_000, "msat converted to sats");
}

/// Every required source refuses typed rather than enriching with blanks,
/// which would silently score every candidate as unknown.
#[test]
fn required_sources_refuse_typed() {
    type BreakOneSource = fn(&mut EnrichmentSources);
    let cases: [(&str, BreakOneSource); 4] = [
        ("enrichment_reputation_unavailable", |s| {
            s.reputation = Err("read failed".into())
        }),
        ("enrichment_connection_history_unavailable", |s| {
            s.connection_events = Err("read failed".into())
        }),
        ("enrichment_gossip_unavailable", |s| {
            s.gossip_channels = Err("read failed".into())
        }),
        ("enrichment_node_addresses_unavailable", |s| {
            s.clearnet_peers = Err("read failed".into())
        }),
    ];
    for (code, break_it) in cases {
        let mut s = sources();
        break_it(&mut s);
        let err = build_enrichment(&["02aa".to_string()], s).expect_err("must refuse");
        assert_eq!(err.code(), code);
    }
}

/// Sink adjacency and the demand-flow role are carried through. `Router`
/// maps to `Other`, NOT to `Unknown`: the frozen scorer PENALISES unknown
/// and leaves other roles alone, so collapsing them would penalise every
/// well-classified router on the graph.
#[test]
fn sink_adjacency_and_role_are_carried() {
    use revops_capital::planner::candidate_score::DemandFlowRole;

    let mut s = sources();
    s.sink_adjacent_peers = ["02aa".to_string()].into_iter().collect();
    let e = build_enrichment(&["02aa".to_string()], s).expect("assembles");
    assert!(e["02aa"].is_sink_adjacent);

    for (from, want) in [
        (FlowRole::Sink, DemandFlowRole::Sink),
        (FlowRole::Source, DemandFlowRole::Source),
        (FlowRole::Unknown, DemandFlowRole::Unknown),
        (FlowRole::Router, DemandFlowRole::Other),
    ] {
        let mut s = sources();
        s.demand_flow_roles.insert("02aa".to_string(), from);
        let e = build_enrichment(&["02aa".to_string()], s).expect("assembles");
        assert!(!e["02aa"].is_sink_adjacent);
        assert_eq!(e["02aa"].demand_flow_role, Some(want), "{from:?}");
    }
}
