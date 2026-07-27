//! `planner::ev` parity, pinned by
//! `fixtures/capital/planner/kernels.json` (kinds `"calculate_open_ev"`,
//! `"calculate_redeployment_ev"`, `"calculate_recycle_ev"`,
//! `"is_recycle_eligible"`, generated from the REAL `CapacityPlanner` EV
//! methods).

use revops_capital::planner::ev::{
    calculate_open_ev, calculate_recycle_ev, calculate_redeployment_ev, is_recycle_eligible,
    OpenEvInputs, RecycleEligibilityInput, RedeploymentCandidate,
};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::PathBuf;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/capital/planner/kernels.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn scenarios(kind: &str) -> Vec<Value> {
    let fx = fixture();
    fx["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["kind"] == kind)
        .cloned()
        .collect()
}

/// Default feerates: `{"perkb": {"opening": 1000, "mutual_close": 1000}}`
/// -> open_cost=140, close_cost=200 (see `close_fee` module tests for the
/// extraction math).
const DEFAULT_OPEN_COST_SATS: i64 = 140;
const DEFAULT_CLOSE_COST_SATS: i64 = 200;

fn open_cost_close_cost_for(feerates: Option<&Value>) -> (i64, i64) {
    use revops_capital::planner::close_fee::{
        estimate_close_cost_sats, estimate_open_cost_sats, Feerates,
    };
    use std::collections::BTreeMap;

    match feerates {
        None => (DEFAULT_OPEN_COST_SATS, DEFAULT_CLOSE_COST_SATS),
        Some(v) => {
            let mut perkb = BTreeMap::new();
            if let Some(obj) = v["perkb"].as_object() {
                for (k, val) in obj {
                    perkb.insert(k.clone(), val.as_f64().unwrap());
                }
            }
            let fr = Feerates { perkb };
            (
                estimate_open_cost_sats(Some(&fr)),
                estimate_close_cost_sats(Some(&fr)),
            )
        }
    }
}

#[test]
fn calculate_open_ev_matches_python() {
    let cases = scenarios("calculate_open_ev");
    assert_eq!(cases.len(), 8, "expected 8 calculate_open_ev scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let input = &case["input"];

        let closed_summary = &input["closed_summary"];
        let closed_channel_daily_net_est_sats = if closed_summary.is_null() {
            None
        } else {
            closed_summary["daily_net_est_sats"].as_f64()
        };

        let inbound = &input["inbound_fee_data"];
        let inbound_median_fee_ppm = if inbound.is_null() {
            None
        } else {
            inbound["median_fee_ppm"].as_f64()
        };

        let observed_node_daily_ppm = input["observed_ppm_cache"].as_f64();
        let channel_size_sats = input["channel_size_sats"].as_i64().unwrap();
        let min_annual_roi_pct = input["min_annual_roi_pct"].as_f64().unwrap();

        let feerates_val = if input["feerates"].is_null() {
            None
        } else {
            Some(&input["feerates"])
        };
        let (open_cost_sats, close_cost_sats) = open_cost_close_cost_for(feerates_val);

        let ev_inputs = OpenEvInputs {
            channel_size_sats,
            closed_channel_daily_net_est_sats,
            observed_node_daily_ppm,
            open_cost_sats,
            close_cost_sats,
            inbound_median_fee_ppm,
            min_annual_roi_pct,
        };

        let actual = calculate_open_ev(&ev_inputs);
        let expected = case["output"].as_f64().unwrap();
        assert!(
            (actual - expected).abs() < 1e-6,
            "{name}: expected {expected}, got {actual}"
        );
    }
}

/// Control: a channel that inherits a healthy closed-channel profit
/// estimate must score MEASURABLY higher than the bootstrap fallback for
/// otherwise-identical inputs — this is the whole point of "profit
/// inheritance" (py 2872-2878).
#[test]
fn closed_channel_profit_inheritance_beats_bootstrap() {
    let cases = scenarios("calculate_open_ev");
    let inherited = cases
        .iter()
        .find(|c| c["name"] == "closed_channel_profit_inheritance")
        .unwrap()["output"]
        .as_f64()
        .unwrap();
    let bootstrap = cases
        .iter()
        .find(|c| c["name"] == "bootstrap_no_history")
        .unwrap()["output"]
        .as_f64()
        .unwrap();
    assert!(inherited > bootstrap);
}

#[test]
fn calculate_redeployment_ev_matches_python() {
    let cases = scenarios("calculate_redeployment_ev");
    assert_eq!(
        cases.len(),
        4,
        "expected 4 calculate_redeployment_ev scenarios"
    );

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let input = &case["input"];
        let loser = &input["loser"];
        let loser_marginal_profit_30d_sats = loser["marginal_profit_30d_sats"].as_i64().unwrap();
        let loser_capacity = loser["capacity"].as_i64().unwrap();
        let observed_ppm = input["observed_ppm_cache"].as_f64();

        let (open_cost_sats, close_cost_sats) = open_cost_close_cost_for(None);
        let closure_cost_sats = close_cost_sats;

        let winners: Vec<Value> = input["winners"].as_array().unwrap().clone();
        let winner_evs: Vec<f64> = winners
            .iter()
            .map(|_w| {
                let ev_inputs = OpenEvInputs {
                    channel_size_sats: loser_capacity,
                    closed_channel_daily_net_est_sats: None,
                    observed_node_daily_ppm: observed_ppm,
                    open_cost_sats,
                    close_cost_sats,
                    inbound_median_fee_ppm: None,
                    min_annual_roi_pct: 1.0,
                };
                calculate_open_ev(&ev_inputs)
            })
            .collect();
        let candidates: Vec<RedeploymentCandidate> = winners
            .iter()
            .zip(winner_evs.iter())
            .map(|(w, ev)| RedeploymentCandidate {
                peer_id: w["peer_id"].as_str().unwrap(),
                open_ev: *ev,
            })
            .collect();

        let (redeployment_ev, best_peer, best_ev) = calculate_redeployment_ev(
            loser_marginal_profit_30d_sats,
            closure_cost_sats,
            &candidates,
        );

        let expected = &case["output"];
        assert!(
            (redeployment_ev - expected["redeployment_ev"].as_f64().unwrap()).abs() < 1e-6,
            "{name}: redeployment_ev"
        );
        assert_eq!(
            best_ev,
            expected["best_ev"].as_f64().unwrap(),
            "{name}: best_ev"
        );
        match expected["best_peer"].as_str() {
            Some(p) => assert_eq!(best_peer, Some(p), "{name}: best_peer"),
            None => assert_eq!(best_peer, None, "{name}: best_peer"),
        }
    }
}

/// Control proving `best_ev` starts at `0.0`, not negative infinity: every
/// fixture winner in these scenarios has a negative open EV, so
/// `best_peer` must stay `None` even though winners were supplied. A
/// `f64::NEG_INFINITY` starting point would instead pick the "least
/// negative" winner — a different, wrong answer that would still look
/// plausible.
#[test]
fn best_ev_floor_is_zero_not_negative_infinity() {
    let candidates = [
        RedeploymentCandidate {
            peer_id: "peerA",
            open_ev: -500.0,
        },
        RedeploymentCandidate {
            peer_id: "peerB",
            open_ev: -100.0,
        },
    ];
    let (_ev, best_peer, best_ev) = calculate_redeployment_ev(0, 200, &candidates);
    assert_eq!(best_peer, None);
    assert_eq!(best_ev, 0.0);
}

#[test]
fn calculate_recycle_ev_matches_python() {
    let cases = scenarios("calculate_recycle_ev");
    assert_eq!(cases.len(), 2, "expected 2 calculate_recycle_ev scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let input = &case["input"];
        let loser = &input["loser"];
        let loser_capacity = loser["capacity"].as_i64().unwrap();
        let loser_marginal_profit_30d_sats = loser["marginal_profit_30d_sats"].as_i64().unwrap();
        let observed_ppm = input["observed_ppm_cache"].as_f64();

        let (open_cost_sats, close_cost_sats) = open_cost_close_cost_for(None);

        let candidate_ev_inputs = OpenEvInputs {
            channel_size_sats: loser_capacity,
            closed_channel_daily_net_est_sats: None,
            observed_node_daily_ppm: observed_ppm,
            open_cost_sats,
            close_cost_sats,
            inbound_median_fee_ppm: None,
            min_annual_roi_pct: 1.0,
        };
        let candidate_ev = calculate_open_ev(&candidate_ev_inputs);

        let actual = calculate_recycle_ev(
            candidate_ev,
            loser_marginal_profit_30d_sats,
            close_cost_sats,
        );
        let expected = case["output"].as_f64().unwrap();
        assert!(
            (actual - expected).abs() < 1e-6,
            "{name}: expected {expected}, got {actual}"
        );
    }
}

#[test]
fn is_recycle_eligible_matches_python() {
    let cases = scenarios("is_recycle_eligible");
    assert_eq!(cases.len(), 6, "expected 6 is_recycle_eligible scenarios");

    for case in &cases {
        let name = case["name"].as_str().unwrap();
        let input = &case["input"];
        let loser = &input["loser"];
        let recycle_input = RecycleEligibilityInput {
            scid: loser["scid"].as_str().unwrap(),
            peer_id: loser["peer_id"].as_str().unwrap(),
            marginal_roi_percent: loser["marginal_roi"].as_f64().unwrap(),
            current_block_height: input["block_height"].as_i64().unwrap(),
        };

        let protected_peers: Option<BTreeSet<String>> = match &input["protected_peers"] {
            Value::Null => None,
            Value::Array(arr) => Some(
                arr.iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect(),
            ),
            other => panic!("unexpected protected_peers shape {other:?}"),
        };
        let route_pair_scids: BTreeSet<String> = input["route_pair_scids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();

        let (ok, reason) =
            is_recycle_eligible(&recycle_input, protected_peers.as_ref(), &route_pair_scids);
        let expected = &case["output"];
        assert_eq!(ok, expected["ok"].as_bool().unwrap(), "{name}: ok");
        assert_eq!(
            reason,
            expected["reason"].as_str().unwrap(),
            "{name}: reason"
        );
    }
}

/// Control: `protected_peers: None` ("policy source failed") must fail
/// CLOSED — every peer becomes ineligible, not just the ones in an (empty)
/// set. This is the opposite of the natural "no protections configured"
/// reading of `None`, so it is worth a dedicated assertion.
#[test]
fn unknown_policy_source_fails_closed_for_every_peer() {
    let input = RecycleEligibilityInput {
        scid: "700000x1x0",
        peer_id: "any_peer_at_all",
        marginal_roi_percent: -10.0,
        current_block_height: 800_000,
    };
    let (ok, reason) = is_recycle_eligible(&input, None, &BTreeSet::new());
    assert!(!ok);
    assert_eq!(reason, "Policy protection unknown (source failed)");
}
