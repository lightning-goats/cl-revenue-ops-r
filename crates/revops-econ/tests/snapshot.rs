//! Conformance/unit tests for `revops_econ::snapshot`, transcribing
//! `modules/econ_snapshot.py` behavior from the Python port
//! (`~/bin/cl_revenue_ops-port`).

use revops_econ::snapshot::{
    build_channel_snapshot, canonical_json, BudgetState, EconomicSnapshot, NodeState, ProfEvidence,
    Protection,
};
use revops_econ::types::{Msat, UnixTime};
use serde_json::json;

fn chan1() -> serde_json::Value {
    json!({
        "short_channel_id": "111x222x0",
        "peer_id": format!("02{}", "a".repeat(64)),
        "total_msat": 2_000_000_000i64,
        "to_us_msat": 1_200_000_000i64,
        "spendable_msat": 1_180_000_000i64,
        "receivable_msat": 780_000_000i64,
    })
}

fn chan2() -> serde_json::Value {
    json!({
        "short_channel_id": "50x60x1",
        "peer_id": format!("03{}", "b".repeat(64)),
        "total_msat": 500_000_000i64,
        "to_us_msat": 100_000_000i64,
    })
}

fn prof1() -> ProfEvidence {
    ProfEvidence {
        fees_earned_msat: 2_000_000,
        sourced_fee_contribution_msat: 1_500_000,
        rebalance_cost_sats: 800,
        open_cost_sats: 400,
        net_profit_sats: 2300,
        volume_routed_msat: 900_000_000,
        forward_count: 142,
        sourced_forward_count_30d: 96,
    }
}

fn node() -> NodeState {
    NodeState {
        total_local_msat: Msat::new(500_000_000_000).unwrap(),
        total_remote_msat: Msat::new(300_000_000_000).unwrap(),
        receivable_objective_msat: Msat::new(400_000_000_000).unwrap(),
        onchain_confirmed_msat: Msat::new(100_000_000_000).unwrap(),
        reserved_msat: Msat::new(5_000_000_000).unwrap(),
        daily_budget: BudgetState {
            cap_msat: Msat::new(10_000_000_000).unwrap(),
            reserved_msat: Msat::new(2_000_000_000).unwrap(),
            spent_msat: Msat::new(1_000_000_000).unwrap(),
        },
        pending_operations: vec![],
        external_obligations: vec![],
    }
}

// --- role / lifecycle enum rejection ---

#[test]
fn build_channel_snapshot_rejects_unknown_role() {
    let err = build_channel_snapshot(&chan1(), None, None, "NOT_A_ROLE", "PRODUCTIVE", vec![])
        .unwrap_err();
    assert!(err.msg.contains("role"));
}

#[test]
fn build_channel_snapshot_rejects_unknown_lifecycle() {
    let err = build_channel_snapshot(&chan1(), None, None, "ROUTER", "NOT_A_LIFECYCLE", vec![])
        .unwrap_err();
    assert!(err.msg.contains("lifecycle"));
}

#[test]
fn build_channel_snapshot_accepts_every_role_and_lifecycle() {
    for role in revops_econ::snapshot::ROLES {
        for lifecycle in revops_econ::snapshot::LIFECYCLES {
            build_channel_snapshot(&chan1(), None, None, role, lifecycle, vec![])
                .unwrap_or_else(|e| panic!("role={role} lifecycle={lifecycle}: {e}"));
        }
    }
}

// --- negative forward_count rejection ---

#[test]
fn build_channel_snapshot_rejects_negative_forward_count() {
    let mut prof = prof1();
    prof.forward_count = -1;
    let err = build_channel_snapshot(&chan1(), Some(&prof), None, "ROUTER", "PRODUCTIVE", vec![])
        .unwrap_err();
    assert!(err.msg.contains("forward"));
}

#[test]
fn build_channel_snapshot_rejects_negative_sourced_forward_count() {
    let mut prof = prof1();
    prof.sourced_forward_count_30d = -1;
    let err = build_channel_snapshot(&chan1(), Some(&prof), None, "ROUTER", "PRODUCTIVE", vec![])
        .unwrap_err();
    assert!(err.msg.contains("forward"));
}

// --- prof=None => all-zero economics + Micro(0) confidence (invariant 7) ---

#[test]
fn build_channel_snapshot_with_no_prof_is_all_zero_and_zero_confidence() {
    let cs =
        build_channel_snapshot(&chan2(), None, None, "DORMANT", "UNDERPERFORMING", vec![]).unwrap();
    assert_eq!(cs.exit_revenue_msat.value(), 0);
    assert_eq!(cs.sourced_value_msat.value(), 0);
    assert_eq!(cs.rebalance_cost_msat.value(), 0);
    assert_eq!(cs.capital_cost_msat.value(), 0);
    assert_eq!(cs.net_value_msat.0, 0);
    assert_eq!(cs.exit_volume_msat.value(), 0);
    assert_eq!(cs.sourced_volume_msat.value(), 0);
    assert_eq!(cs.forward_count, 0);
    assert_eq!(cs.sourced_forward_count, 0);
    assert_eq!(cs.confidence_micro.value(), 0);
}

#[test]
fn build_channel_snapshot_flow_confidence_none_is_zero_even_with_prof() {
    let prof = prof1();
    let cs = build_channel_snapshot(&chan1(), Some(&prof), None, "ROUTER", "PRODUCTIVE", vec![])
        .unwrap();
    assert_eq!(cs.confidence_micro.value(), 0);
    // Economics ARE populated when prof is Some, even though confidence is 0.
    assert_eq!(cs.exit_revenue_msat.value(), 2_000_000);
}

// --- channel auto-sort by channel_id (J3) ---

#[test]
fn economic_snapshot_sorts_channels_by_channel_id() {
    let cs1 = build_channel_snapshot(
        &chan1(),
        Some(&prof1()),
        Some(0.85),
        "ROUTER",
        "PRODUCTIVE",
        vec![],
    )
    .unwrap();
    let cs2 =
        build_channel_snapshot(&chan2(), None, None, "DORMANT", "UNDERPERFORMING", vec![]).unwrap();
    // Insert in reverse lexicographic order; construction must sort them.
    let snap = EconomicSnapshot::new(
        "cycle-000001",
        UnixTime::new(1_752_300_000).unwrap(),
        2_592_000,
        node(),
        vec![cs2, cs1],
    )
    .unwrap();
    let ids: Vec<&str> = snap
        .channels
        .iter()
        .map(|c| c.channel_id.as_str())
        .collect();
    assert_eq!(ids, vec!["111x222x0", "50x60x1"]);
}

// --- snapshot_id / evidence_window_seconds validation ---

#[test]
fn economic_snapshot_rejects_empty_snapshot_id() {
    let err = EconomicSnapshot::new("", UnixTime::new(1_752_300_000).unwrap(), 0, node(), vec![])
        .unwrap_err();
    assert!(err.msg.contains("snapshot_id"));
}

#[test]
fn economic_snapshot_rejects_negative_evidence_window() {
    let err = EconomicSnapshot::new(
        "cycle-1",
        UnixTime::new(1_752_300_000).unwrap(),
        -1,
        node(),
        vec![],
    )
    .unwrap_err();
    assert!(err.msg.contains("evidence_window_seconds"));
}

// --- to_wire key set matches economic_snapshot.v0.schema.json ---

#[test]
fn to_wire_key_set_matches_schema() {
    let cs1 = build_channel_snapshot(
        &chan1(),
        Some(&prof1()),
        Some(0.85),
        "ROUTER",
        "PRODUCTIVE",
        vec![Protection::new(
            "lnplus_contract",
            "lnplus",
            Some(UnixTime::new(1_755_000_000).unwrap()),
        )
        .unwrap()],
    )
    .unwrap();
    let snap = EconomicSnapshot::new(
        "cycle-000001",
        UnixTime::new(1_752_300_000).unwrap(),
        2_592_000,
        node(),
        vec![cs1],
    )
    .unwrap();
    let wire = snap.to_wire();
    let obj = wire.as_object().unwrap();
    let mut top_keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    top_keys.sort_unstable();
    assert_eq!(
        top_keys,
        vec![
            "channels",
            "evidence_window_seconds",
            "node",
            "observed_at",
            "schema_name",
            "schema_version",
            "snapshot_id",
        ]
    );

    let node_obj = obj["node"].as_object().unwrap();
    let mut node_keys: Vec<&str> = node_obj.keys().map(String::as_str).collect();
    node_keys.sort_unstable();
    assert_eq!(
        node_keys,
        vec![
            "daily_budget",
            "external_obligations",
            "onchain_confirmed_msat",
            "pending_operations",
            "receivable_objective_msat",
            "reserved_msat",
            "total_local_msat",
            "total_remote_msat",
        ]
    );

    let budget_obj = node_obj["daily_budget"].as_object().unwrap();
    let mut budget_keys: Vec<&str> = budget_obj.keys().map(String::as_str).collect();
    budget_keys.sort_unstable();
    assert_eq!(budget_keys, vec!["cap_msat", "reserved_msat", "spent_msat"]);

    let channel_obj = obj["channels"][0].as_object().unwrap();
    let mut channel_keys: Vec<&str> = channel_obj.keys().map(String::as_str).collect();
    channel_keys.sort_unstable();
    assert_eq!(
        channel_keys,
        vec![
            "capacity_msat",
            "capital_cost_msat",
            "channel_id",
            "confidence_micro",
            "exit_revenue_msat",
            "exit_volume_msat",
            "forward_count",
            "lifecycle",
            "local_msat",
            "net_value_msat",
            "peer_id",
            "protections",
            "rebalance_cost_msat",
            "receivable_msat",
            "remote_msat",
            "role",
            "sourced_forward_count",
            "sourced_value_msat",
            "sourced_volume_msat",
            "spendable_msat",
        ]
    );

    let protection_obj = channel_obj["protections"][0].as_object().unwrap();
    let mut protection_keys: Vec<&str> = protection_obj.keys().map(String::as_str).collect();
    protection_keys.sort_unstable();
    assert_eq!(protection_keys, vec!["expires_at", "owner", "reason"]);

    assert_eq!(obj["schema_name"], json!("economic_snapshot"));
    assert_eq!(obj["schema_version"], json!(0));
}

// --- golden canonical string, generated once from Python ---
//
// Generated with (in ~/bin/cl_revenue_ops-port):
//
//   python3 - <<'EOF'
//   import sys
//   from types import SimpleNamespace
//   sys.path.insert(0, '.')
//   from modules.econ_snapshot import (
//       EconomicSnapshot, NodeState, BudgetState, Protection,
//       build_channel_snapshot, to_wire, canonical_json,
//   )
//   from modules.econ_types import Msat, UnixTime
//
//   def make_prof(fees_earned_msat, sourced_fee_contribution_msat,
//                 rebalance_cost_sats, open_cost_sats, net_profit_sats,
//                 volume_routed_msat, forward_count, sourced_forward_count_30d):
//       revenue = SimpleNamespace(
//           fees_earned_msat=fees_earned_msat,
//           sourced_fee_contribution_msat=sourced_fee_contribution_msat,
//           volume_routed_msat=volume_routed_msat,
//           forward_count=forward_count,
//       )
//       costs = SimpleNamespace(
//           rebalance_cost_sats=rebalance_cost_sats,
//           open_cost_sats=open_cost_sats,
//       )
//       return SimpleNamespace(
//           revenue=revenue, costs=costs, net_profit_sats=net_profit_sats,
//           sourced_forward_count_30d=sourced_forward_count_30d,
//       )
//
//   chan1 = {
//       "short_channel_id": "111x222x0",
//       "peer_id": "02" + "a" * 64,
//       "total_msat": 2_000_000_000,
//       "to_us_msat": 1_200_000_000,
//       "spendable_msat": 1_180_000_000,
//       "receivable_msat": 780_000_000,
//   }
//   prof1 = make_prof(2_000_000, 1_500_000, 800, 400, 2300, 900_000_000, 142, 96)
//   cs1 = build_channel_snapshot(
//       channel=chan1, prof=prof1, flow_confidence=0.85, role="ROUTER",
//       lifecycle="PRODUCTIVE",
//       protections=(Protection("lnplus_contract", "lnplus", UnixTime(1755000000)),),
//   )
//
//   chan2 = {
//       "short_channel_id": "50x60x1",
//       "peer_id": "03" + "b" * 64,
//       "total_msat": 500_000_000,
//       "to_us_msat": 100_000_000,
//   }
//   cs2 = build_channel_snapshot(
//       channel=chan2, prof=None, flow_confidence=None, role="DORMANT",
//       lifecycle="UNDERPERFORMING", protections=(),
//   )
//
//   node = NodeState(
//       total_local_msat=Msat(500_000_000_000),
//       total_remote_msat=Msat(300_000_000_000),
//       receivable_objective_msat=Msat(400_000_000_000),
//       onchain_confirmed_msat=Msat(100_000_000_000),
//       reserved_msat=Msat(5_000_000_000),
//       daily_budget=BudgetState(
//           Msat(10_000_000_000), Msat(2_000_000_000), Msat(1_000_000_000)),
//   )
//
//   snap = EconomicSnapshot(
//       snapshot_id="cycle-000001",
//       observed_at=UnixTime(1752300000),
//       evidence_window_seconds=2592000,
//       node=node,
//       channels=(cs2, cs1),
//   )
//
//   print(canonical_json(to_wire(snap)))
//   EOF
//
// Output (hardcoded below):
const GOLDEN: &str = "{\"channels\":[{\"capacity_msat\":2000000000,\"capital_cost_msat\":400000,\"channel_id\":\"111x222x0\",\"confidence_micro\":850000,\"exit_revenue_msat\":2000000,\"exit_volume_msat\":900000000,\"forward_count\":142,\"lifecycle\":\"PRODUCTIVE\",\"local_msat\":1200000000,\"net_value_msat\":2300000,\"peer_id\":\"02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"protections\":[{\"expires_at\":1755000000,\"owner\":\"lnplus\",\"reason\":\"lnplus_contract\"}],\"rebalance_cost_msat\":800000,\"receivable_msat\":780000000,\"remote_msat\":800000000,\"role\":\"ROUTER\",\"sourced_forward_count\":96,\"sourced_value_msat\":1500000,\"sourced_volume_msat\":0,\"spendable_msat\":1180000000},{\"capacity_msat\":500000000,\"capital_cost_msat\":0,\"channel_id\":\"50x60x1\",\"confidence_micro\":0,\"exit_revenue_msat\":0,\"exit_volume_msat\":0,\"forward_count\":0,\"lifecycle\":\"UNDERPERFORMING\",\"local_msat\":100000000,\"net_value_msat\":0,\"peer_id\":\"03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"protections\":[],\"rebalance_cost_msat\":0,\"receivable_msat\":0,\"remote_msat\":400000000,\"role\":\"DORMANT\",\"sourced_forward_count\":0,\"sourced_value_msat\":0,\"sourced_volume_msat\":0,\"spendable_msat\":0}],\"evidence_window_seconds\":2592000,\"node\":{\"daily_budget\":{\"cap_msat\":10000000000,\"reserved_msat\":2000000000,\"spent_msat\":1000000000},\"external_obligations\":[],\"onchain_confirmed_msat\":100000000000,\"pending_operations\":[],\"receivable_objective_msat\":400000000000,\"reserved_msat\":5000000000,\"total_local_msat\":500000000000,\"total_remote_msat\":300000000000},\"observed_at\":1752300000,\"schema_name\":\"economic_snapshot\",\"schema_version\":0,\"snapshot_id\":\"cycle-000001\"}";

#[test]
fn to_wire_canonical_json_matches_python_golden() {
    let cs1 = build_channel_snapshot(
        &chan1(),
        Some(&prof1()),
        Some(0.85),
        "ROUTER",
        "PRODUCTIVE",
        vec![Protection::new(
            "lnplus_contract",
            "lnplus",
            Some(UnixTime::new(1_755_000_000).unwrap()),
        )
        .unwrap()],
    )
    .unwrap();
    let cs2 =
        build_channel_snapshot(&chan2(), None, None, "DORMANT", "UNDERPERFORMING", vec![]).unwrap();

    // Constructed in reverse order deliberately; EconomicSnapshot::new must
    // sort by channel_id (J3) before canonicalization.
    let snap = EconomicSnapshot::new(
        "cycle-000001",
        UnixTime::new(1_752_300_000).unwrap(),
        2_592_000,
        node(),
        vec![cs2, cs1],
    )
    .unwrap();

    let got = canonical_json(&snap.to_wire()).unwrap();
    assert_eq!(got, GOLDEN);
}

// --- Protection validation ---

#[test]
fn protection_rejects_empty_reason_or_owner() {
    assert!(Protection::new("", "owner", None).is_err());
    assert!(Protection::new("reason", "", None).is_err());
}

#[test]
fn protection_allows_no_expiry() {
    let p = Protection::new("reason", "owner", None).unwrap();
    assert_eq!(p.expires_at, None);
}

// --- net_value_msat checked overflow ---

#[test]
fn build_channel_snapshot_rejects_net_value_overflow() {
    let mut prof = prof1();
    prof.net_profit_sats = i64::MAX; // * 1000 overflows i64
    let err = build_channel_snapshot(&chan1(), Some(&prof), None, "ROUTER", "PRODUCTIVE", vec![])
        .unwrap_err();
    assert!(err.msg.to_lowercase().contains("overflow"));
}
