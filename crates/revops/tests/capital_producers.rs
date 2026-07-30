//! Task 71 / F71-R5: REAL producers for the four open-side fields.
//!
//! The finding's core point is structural, not cosmetic: `EvidenceDeps`
//! parameters populated only by tests are not producers. Slice 5 closed the
//! gap LIST while production still passed empty collections, so the planner
//! reported no gaps and still planned nothing -- arguably worse than the
//! honest gap list it replaced.
//!
//! The answer here is that `OpenSideEvidence` has PRIVATE fields and only
//! `build_open_side` can construct it. A caller cannot hand-assemble an
//! empty one, so "no gaps" now implies a producer actually ran.

use std::collections::{BTreeMap, HashMap};

use revops::capital_producers::{build_open_side, OpenSideSources, ProducerRefusal};
use revops::open_ev_evidence::ChainCosts;
use revops_capital::planner::candidate_score::CandidateEnrichmentEvidence;
use revops_capital::planner::cycle::{DiscoveryEvidence, NeighborEdge};
use revops_capital::planner::discovery::PatronCandidate;
use serde_json::json;

/// Discovery INPUT evidence that makes the frozen `discover_peers` surface
/// `02cand` as a patron. The candidate universe is derived from this by the
/// producer -- never handed to it.
fn discovery_with_candidate() -> DiscoveryEvidence {
    DiscoveryEvidence {
        // `02patron` is a high-ROI channel of OURS; the frozen neighbour
        // strategy surfaces ITS neighbours as candidates. `02cand` is
        // therefore discovered, not declared.
        all_channels: vec![PatronCandidate {
            peer_id: "02patron".to_string(),
            marginal_roi_percent: 250.0,
        }],
        neighbor_patron_source_channels: BTreeMap::from([(
            "02patron".to_string(),
            vec![NeighborEdge {
                destination: "02cand".to_string(),
                amount_msat: 5_000_000_000,
                fee_per_millionth: 100,
            }],
        )]),
        our_node_id: "02us".to_string(),
        // Without this the pool quota truncates every discovered candidate
        // to nothing -- a silent zero, not an error.
        max_candidate_pool: revops::discovery_evidence::DEFAULT_MAX_CANDIDATE_POOL,
        ..Default::default()
    }
}

fn enrichment(caps: Vec<i64>) -> CandidateEnrichmentEvidence {
    CandidateEnrichmentEvidence {
        reputation: None,
        closed_channel_profit: None,
        uptime_pct: Some(100.0),
        has_clearnet_address: true,
        inbound_median_fee_ppm: None,
        dest_channel_capacities_sats: caps,
        is_sink_adjacent: false,
        demand_flow_role: None,
    }
}

fn sources() -> OpenSideSources {
    OpenSideSources {
        discovery: discovery_with_candidate(),
        winner_channels: Vec::new(),
        enrichment: BTreeMap::from([("02cand".to_string(), enrichment(vec![3_000_000]))]),
        listnodes: Ok(json!({"nodes": [
            {"nodeid": "02cand", "option_will_fund": {"lease_fee_base_msat": 0}},
            {"nodeid": "02plain"},
        ]})),
        closed_channel_daily_net_est: HashMap::new(),
        observed_node_daily_ppm: Some(45.0),
        chain_costs: ChainCosts {
            open_cost_sats: 1_400,
            close_cost_sats: 400,
            used_fallback: false,
        },
        planned_channel_size_sats: 2_000_000,
        min_annual_roi_pct: 1.0,
    }
}

/// Each of the four fields is genuinely produced from its own source.
#[test]
fn the_four_fields_are_produced() {
    let e = build_open_side(sources()).expect("produces");

    let cand = e
        .open_candidate_evidence()
        .get("02cand")
        .expect("candidate produced");
    assert_eq!(cand.open_ev_template.channel_size_sats, 2_000_000);
    assert_eq!(cand.open_ev_template.open_cost_sats, 1_400);
    assert_eq!(cand.peer_dest_channel_capacities_sats, vec![3_000_000]);

    assert!(
        e.dual_fund_peers().contains("02cand"),
        "option_will_fund present => dual-fund capable"
    );
    assert!(
        !e.dual_fund_peers().contains("02plain"),
        "no option_will_fund => not dual-fund"
    );

    // Winners derive from `identify_winners` over winner_channels; this
    // fixture supplies none, so the produced set is a measured empty.
    assert!(e.redeployment_winner_evs().is_empty());

    assert_eq!(e.recycle_candidates().len(), 1);
    assert_eq!(e.recycle_candidates()[0].peer_id, "02cand");
    // F71-R12(c): the score comes from the FROZEN discovery pipeline, not
    // from a caller-supplied map. It is whatever normalisation produced --
    // the point is that no 0.0 was fabricated for a missing entry.
    assert!(
        e.recycle_candidates()[0].score.is_finite() && e.recycle_candidates()[0].score > 0.0,
        "score must come from discovery: {}",
        e.recycle_candidates()[0].score
    );
}

/// `option_will_fund` is a TRUTHY check in Python (py 707:
/// `bool(node_info.get("option_will_fund"))`), so an explicitly null or
/// empty value is NOT dual-fund support. Treating mere key presence as
/// support would let a blocked portfolio open to peers that cannot
/// actually lease -- exactly the opens the blocked state exists to
/// restrict.
#[test]
fn dual_fund_requires_a_truthy_option_will_fund() {
    let mut s = sources();
    s.listnodes = Ok(json!({"nodes": [
        {"nodeid": "02a", "option_will_fund": null},
        {"nodeid": "02b", "option_will_fund": {}},
        {"nodeid": "02c", "option_will_fund": {"lease_fee_base_msat": 0}},
    ]}));
    let e = build_open_side(s).expect("produces");
    assert!(!e.dual_fund_peers().contains("02a"), "null is not support");
    assert!(!e.dual_fund_peers().contains("02b"), "empty is not support");
    assert!(e.dual_fund_peers().contains("02c"));
}

/// The redeployment template's `channel_size_sats` is a PLACEHOLDER --
/// F71-R10 substitutes each loser's capacity at pricing time. Producing a
/// baked-in size here would reintroduce the scalar defect one layer up.
#[test]
fn redeployment_templates_carry_no_committed_channel_size() {
    let e = build_open_side(sources()).expect("produces");
    // Candidate templates commit the planned size; the recycle template for
    // the same peer must NOT, since the loser's capacity is substituted.
    assert_eq!(
        e.recycle_candidates()[0].open_ev_template.channel_size_sats,
        0,
        "must be a placeholder; the loser's capacity is substituted per pricing"
    );
    assert_eq!(
        e.recycle_candidates()[0]
            .open_ev_template
            .observed_node_daily_ppm,
        Some(45.0)
    );
}

/// A failed listnodes read REFUSES. Defaulting to "no dual-fund peers"
/// would silently block every constrained-portfolio open, which looks
/// exactly like a healthy cycle that found nothing worth doing.
#[test]
fn a_failed_listnodes_read_refuses() {
    let mut s = sources();
    s.listnodes = Err("listnodes rpc timeout".into());
    let err = build_open_side(s).expect_err("must refuse");
    assert_eq!(err.code(), "producer_listnodes_unavailable");
    assert!(matches!(err, ProducerRefusal::ListnodesUnavailable(_)));
}

/// A candidate with no enrichment REFUSES rather than being scored on
/// blanks. Enrichment is what the frozen scorer multiplies by; a candidate
/// silently dropped or blank-scored changes the ranking with no signal.
#[test]
fn a_candidate_without_enrichment_refuses() {
    let mut s = sources();
    s.enrichment = BTreeMap::new();
    let err = build_open_side(s).expect_err("must refuse");
    assert_eq!(err.code(), "producer_enrichment_missing");
}

/// F71-R11: an empty result is valid ONLY because the real producer ran
/// and measured nothing. This drives EMPTY DISCOVERY EVIDENCE through the
/// frozen `discover_peers`, which is a genuine measurement -- as opposed to
/// the previous version of this test, which passed `Vec::new()` as the
/// candidate list and thereby blessed exactly the hand-asserted empty that
/// F71-R5 existed to forbid.
#[test]
fn empty_discovery_evidence_produces_a_measured_empty() {
    let mut s = sources();
    s.discovery = DiscoveryEvidence::default();
    s.enrichment = BTreeMap::new();
    let e = build_open_side(s).expect("produces a measured empty");
    assert!(e.open_candidate_evidence().is_empty());
    assert!(e.recycle_candidates().is_empty());
}

/// F71-R12(b): a successful listnodes reply with a missing or wrongly
/// typed `nodes` array is NOT an empty measurement -- reading it that way
/// silently blocks every constrained-portfolio open, which looks like a
/// healthy quiet cycle. A real `nodes: []` stays a measured empty.
#[test]
fn malformed_listnodes_refuses_but_empty_nodes_is_measured() {
    for bad in [
        json!({}),
        json!({"nodes": {}}),
        json!({"nodes": [{"alias": "x"}]}),
    ] {
        let mut s = sources();
        s.listnodes = Ok(bad.clone());
        let err = build_open_side(s).expect_err("must refuse");
        assert_eq!(err.code(), "producer_listnodes_malformed", "{bad:?}");
    }

    let mut s = sources();
    s.listnodes = Ok(json!({"nodes": []}));
    let e = build_open_side(s).expect("real empty is measured");
    assert!(e.dual_fund_peers().is_empty());
}

/// Structural guard for F71-R5: `OpenSideEvidence` cannot be built by hand.
/// If its fields become publicly constructible, a caller can once again
/// pass empties and clear the gap list while planning stays inert.
#[test]
fn open_side_evidence_is_not_hand_constructible() {
    let src = include_str!("../src/capital_producers.rs");
    let decl = src
        .split("pub struct OpenSideEvidence {")
        .nth(1)
        .expect("struct exists");
    let body = decl.split('}').next().expect("struct body");
    assert!(
        !body.contains("pub "),
        "OpenSideEvidence fields must stay private so only build_open_side \
         can construct it: {body}"
    );
}

/// The only public path into `BTreeSet<String>`-shaped dual-fund evidence
/// is the producer, so an empty set always means "listnodes was read and
/// nobody supports it", never "nobody asked".
#[test]
fn dual_fund_absence_is_measured() {
    let mut s = sources();
    s.listnodes = Ok(json!({"nodes": []}));
    let e = build_open_side(s).expect("produces");
    assert!(e.dual_fund_peers().is_empty());
}

/// F71-R14: a POSITIVE winner fixture. The previous revision passed
/// `winner_channels: Vec::new()` and asserted only that the result was
/// empty, so the redeployment-template producer could have been deleted
/// outright without failing a single test -- despite being one of the four
/// fields F71-R5 exists to implement.
///
/// This drives a channel the frozen `identify_winners` really classifies
/// (effective ROI > 20, turnover > 0.5, |flow_ratio| > 0.8) and asserts the
/// produced template field by field.
#[test]
fn a_real_winner_produces_a_redeployment_template() {
    use revops_capital::planner::winners::{WinnerCandidateEvidence, WinnerFlowEvidence};

    let mut s = sources();
    s.closed_channel_daily_net_est = HashMap::from([("02win".to_string(), 1_234.0)]);
    // Winners feed discovery strategy 1, so a winner can itself surface as
    // a candidate and therefore needs enrichment. Production must enrich
    // the UNION of discovered peers, not just graph neighbours -- the
    // producer refuses rather than scoring a candidate on blanks.
    s.enrichment
        .insert("02win".to_string(), enrichment(vec![1_000_000]));
    s.winner_channels = vec![WinnerCandidateEvidence {
        scid: "700000:1:0".to_string(),
        peer_id: "02win".to_string(),
        capacity_sats: 1_000_000,
        marginal_roi_percent: 250.0,
        flow: Some(WinnerFlowEvidence {
            // turnover = daily_volume / capacity = 1.0 > 0.5
            daily_volume: 1_000_000.0,
            flow_ratio: 0.95,
            kalman_velocity: 0.2,
            is_congested: false,
        }),
        rebalance_success: None,
        sourced_fee_contribution_sats: 0,
        channel_role: None,
        dts_posterior_mean: None,
    }];

    let e = build_open_side(s).expect("produces");
    let winners = e.redeployment_winner_evs();
    assert_eq!(winners.len(), 1, "the winner must produce a template");
    let (peer, template) = &winners[0];
    assert_eq!(peer, "02win");
    assert_eq!(
        template.channel_size_sats, 0,
        "F71-R10: size stays a placeholder for per-loser substitution"
    );
    assert_eq!(template.open_cost_sats, 1_400);
    assert_eq!(template.close_cost_sats, 400);
    assert_eq!(template.observed_node_daily_ppm, Some(45.0));
    assert_eq!(
        template.closed_channel_daily_net_est_sats,
        Some(1_234.0),
        "profit inheritance must reach the template"
    );
    assert_eq!(template.min_annual_roi_pct, 1.0);
}

/// F71-R12(a): `OpenSideEvidence` must not derive `Default`, or any caller
/// can produce an empty one and bypass `build_open_side` entirely --
/// private fields alone do not stop `OpenSideEvidence::default()`.
#[test]
fn open_side_evidence_has_no_public_empty_constructor() {
    let src = include_str!("../src/capital_producers.rs");
    let decl = src
        .split("pub struct OpenSideEvidence")
        .next()
        .expect("struct exists");
    let derive_line = decl.lines().last().unwrap_or_default();
    assert!(
        !derive_line.contains("Default"),
        "OpenSideEvidence must not derive Default: {derive_line}"
    );
    assert!(
        !src.contains("impl Default for OpenSideEvidence"),
        "no hand-written Default either"
    );
}

/// F71-R13, and the killing test for mutation C19, which SURVIVED the first
/// clean-tree matrix: the bundle must carry the SAME discovery and
/// enrichment instances that fed `discover_peers`. Swapping them for a
/// default afterwards is silent -- candidates were derived from snapshot A
/// while the kernel plans against snapshot B, and the gap list stays empty
/// either way.
#[test]
fn the_bundle_carries_the_instances_that_fed_discovery() {
    let e = build_open_side(sources()).expect("produces");

    assert_eq!(
        e.discovery().all_channels.len(),
        1,
        "the produced bundle must carry the discovery evidence it used"
    );
    assert_eq!(e.discovery().all_channels[0].peer_id, "02patron");
    assert_eq!(e.discovery().our_node_id, "02us");
    assert!(
        e.discovery()
            .neighbor_patron_source_channels
            .contains_key("02patron"),
        "the neighbour edges that produced 02cand must survive"
    );

    assert!(
        e.candidate_enrichment().contains_key("02cand"),
        "the enrichment that fed scoring must survive: {:?}",
        e.candidate_enrichment().keys().collect::<Vec<_>>()
    );
}
