//! Task 61 4A — kernel-level fail-closed enforcement: a store persistence
//! failure ABORTS the pass with `Err`; it is never swallowed into a
//! warn-log continuation, and an unacknowledged intent write is never
//! followed by a live external call.

mod common;

use std::collections::BTreeSet;

use common::*;
use revops_lnplus::config::LnPlusConfig;
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::evaluator::{run_cycle, CycleInputs, CyclePreflight};
use revops_lnplus::exec_mode::ExecutionMode;
use revops_lnplus::loop_drivers::{evaluator_pass, EvaluatorPassParams};
use revops_lnplus::open::OpenExecParams;
use revops_lnplus::ports::LnPlusDb;
use revops_lnplus::types::{MySwapEntry, MySwaps};
use revops_lnplus::watcher::run_watcher_once;

fn cfg() -> LnPlusConfig {
    LnPlusConfig {
        lnplus_swaps_enabled: true,
        lnplus_apply_feerate_ceiling: 5000,
        planner_min_channel_sats: 100_000,
        planner_max_channel_sats: 10_000_000,
        lnplus_max_duration_months: 12,
        lnplus_max_participants: 5,
        lnplus_min_participants: 3,
        lnplus_min_peer_positive_ratings: 3,
        lnplus_min_peer_rank: 1,
        lnplus_inbound_credit_factor: 1.0,
        lnplus_swap_preference_margin: 0.1,
        min_wallet_reserve: 0,
        lnplus_execute_applications: true,
        planner_dry_run: false,
        lnplus_pending_timeout_days: 7,
    }
}

fn open_params() -> OpenExecParams {
    OpenExecParams {
        estimated_cost_sats: 2000,
        effective_budget_sats: None,
        budget_since_timestamp: None,
    }
}

/// A swap listing that passes every pre-application gate with the
/// standard fixture planner.
fn qualifying_swap_inputs(planner: &FakePlanner) -> CycleInputs {
    let a = pubkey(1);
    let b = pubkey(2);
    let mut pa = participant("A", &a);
    pa.positive_ratings_count = 50;
    let mut pb = participant("B", &b);
    pb.positive_ratings_count = 50;
    planner.set_ev(&a, 50_000.0);
    planner.set_ev(&b, 50_000.0);
    CycleInputs {
        cfg: cfg(),
        preflight: CyclePreflight {
            breaker_tripped: None,
            has_inflight: false,
            reconcile_ok: true,
        },
        opening_feerate_perkw: Some(100),
        swaps: vec![listing("1", vec![pa, pb])],
        our_id: None,
        frozen_peers_with_channels: Some(BTreeSet::new()),
        best_regular_ev: 0.0,
        confirmed_unreserved_sats: 100_000_000,
        capex_budget_sats: None,
        now: 1000,
    }
}

#[test]
fn evaluator_intent_write_failure_blocks_the_live_application_call() {
    // THE ordering invariant: if the intent row's insert is not
    // acknowledged, the irreversible create_application must never fire.
    let db = FakeDb::new();
    *db.fail_insert_swap.borrow_mut() = true;
    let api = FakeApi::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let result = run_cycle(
        qualifying_swap_inputs(&planner),
        &db,
        &api,
        None,
        &planner,
        &logger,
    );
    assert!(result.is_err(), "an unacknowledged intent write must abort");
    assert!(
        api.create_application_calls.borrow().is_empty(),
        "create_application must NOT be reachable past a failed intent write"
    );
}

#[test]
fn control_evaluator_applies_when_the_intent_write_is_acknowledged() {
    // CONTROL: identical fixture, healthy store — the application fires.
    // Proves the assertion above fails for the right reason.
    let db = FakeDb::new();
    let api = FakeApi::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let out = run_cycle(
        qualifying_swap_inputs(&planner),
        &db,
        &api,
        None,
        &planner,
        &logger,
    )
    .expect("healthy cycle");
    assert!(out.applied);
    assert_eq!(api.create_application_calls.borrow().len(), 1);
}

#[test]
fn evaluator_planner_breadcrumb_failure_aborts_before_any_live_call() {
    let db = FakeDb::new();
    *db.fail_planner_actions.borrow_mut() = true;
    let api = FakeApi::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let result = run_cycle(
        qualifying_swap_inputs(&planner),
        &db,
        &api,
        None,
        &planner,
        &logger,
    );
    assert!(result.is_err());
    assert!(api.create_application_calls.borrow().is_empty());
}

#[test]
fn watcher_pass_aborts_when_a_lifecycle_write_fails() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    // An applied row that LN+ now lists as opening — phase 3 must CAS the
    // opening patch; the injected failure must abort the whole pass.
    let peer = pubkey(1);
    db.insert(SwapRow::new("s1", "applied", 1_000_000, 6, 500).with_outbound_peer(peer.clone()));
    *api.my_swaps.borrow_mut() = Ok(MySwaps {
        opening: vec![MySwapEntry {
            id: "s1".to_string(),
            outgoing_peer_pubkey: Some(peer),
            ..Default::default()
        }],
        ..Default::default()
    });
    // Skip the backfill choke point so the failure seam is phase 3's CAS.
    db.set_config_override(revops_lnplus::reconcile::BACKFILL_FLAG, "1")
        .unwrap();
    // Fail ONLY the next cas call (phase 3's opening patch) then heal —
    // a blanket failure would be caught by any later write and prove
    // nothing about this specific seam's fail-closed handling.
    *db.fail_cas_swap_times.borrow_mut() = 1;

    let result = run_watcher_once(
        &db,
        &api,
        &chain,
        &policy,
        None,
        &logger,
        &open_params(),
        7,
        1000,
    );
    assert!(
        result.is_err(),
        "a failed lifecycle write mid-pass must abort the pass, not continue on ghost state"
    );
}

#[test]
fn breaker_read_failure_gates_the_evaluator_pass_closed() {
    // Fail closed: "could not check the breaker" must never behave as
    // "breaker is clear" — the pass errors out BEFORE any live, signed
    // API call is issued.
    let db = FakeDb::new();
    *db.fail_get_breaker.borrow_mut() = true;
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let result = evaluator_pass(
        &cfg(),
        ExecutionMode::DryRun,
        &db,
        &api,
        &chain,
        None,
        &planner,
        &logger,
        EvaluatorPassParams {
            best_regular_ev: 0.0,
            cached_our_id: None,
            now: 1000,
        },
    );
    assert!(result.is_err());
    assert_eq!(
        *api.get_my_swaps_calls.borrow(),
        0,
        "no live signed call may fire when the breaker state is unknowable"
    );
    assert_eq!(*api.get_applicable_swaps_calls.borrow(), 0);
}

#[test]
fn watcher_terminal_prune_failure_aborts_the_pass() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();
    db.set_config_override(revops_lnplus::reconcile::BACKFILL_FLAG, "1")
        .unwrap();
    *db.fail_prune.borrow_mut() = true;

    let result = run_watcher_once(
        &db,
        &api,
        &chain,
        &policy,
        None,
        &logger,
        &open_params(),
        7,
        1000,
    );
    assert!(result.is_err());
}
