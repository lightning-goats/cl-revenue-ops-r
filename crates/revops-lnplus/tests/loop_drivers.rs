//! `evaluator_pass` / `watcher_pass` / `WatcherLoop` — the loop drivers.
//! Every test here is either (a) a short-circuit-ordering assertion (a
//! blocked pass must not issue the LIVE calls a proceeding pass would —
//! verified via the call counters added to `FakeApi`/`FakeChain` in
//! `tests/common/mod.rs`) or (b) an `ExecutionMode` safety assertion (a
//! `DryRun` pass must never reach a real mutating port call, with an
//! `Armed` control proving the scenario CAN succeed).

mod common;

use common::*;
use revops_lnplus::breaker::{BreakerCause, BreakerState};
use revops_lnplus::config::LnPlusConfig;
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::exec_mode::ExecutionMode;
use revops_lnplus::loop_drivers::{evaluator_pass, watcher_pass, EvaluatorPassParams, WatcherLoop};
use revops_lnplus::open::OpenExecParams;
use revops_lnplus::ports::LnPlusDb;
use revops_lnplus::reconcile::BACKFILL_FLAG;
use revops_lnplus::types::MySwapEntry;
use revops_lnplus::validation::TsValue;

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

fn eval_params() -> EvaluatorPassParams {
    EvaluatorPassParams {
        best_regular_ev: 0.0,
        cached_our_id: None,
        now: 1_000_000,
    }
}

// ------------------------------------------------------ short-circuit order

#[test]
fn breaker_tripped_short_circuits_before_any_live_call() {
    let db = FakeDb::new();
    *db.breaker.borrow_mut() = Some(BreakerState {
        tripped_at: 1,
        cause: BreakerCause::MissedOpenDeadline {
            swap_id: "x".into(),
        },
    });
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let result = evaluator_pass(
        &cfg(),
        ExecutionMode::Armed,
        &db,
        &api,
        &chain,
        Some(&policy),
        &planner,
        &logger,
        eval_params(),
    );

    assert!(!result.outcome.applied && !result.outcome.recommended);
    assert_eq!(
        *api.get_my_swaps_calls.borrow(),
        0,
        "reconcile's signed get_my_swaps must not fire while breaker is tripped"
    );
    assert_eq!(*api.get_applicable_swaps_calls.borrow(), 0);
}

#[test]
fn has_inflight_short_circuits_before_any_live_call() {
    let db = FakeDb::new();
    db.insert(SwapRow::new("inflight-1", "applied", 100_000, 6, 1));
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let result = evaluator_pass(
        &cfg(),
        ExecutionMode::Armed,
        &db,
        &api,
        &chain,
        Some(&policy),
        &planner,
        &logger,
        eval_params(),
    );

    assert!(!result.outcome.applied);
    assert_eq!(*api.get_my_swaps_calls.borrow(), 0);
    assert_eq!(*api.get_applicable_swaps_calls.borrow(), 0);
}

#[test]
fn feerate_above_ceiling_short_circuits_before_reconcile() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    *chain.opening_feerate.borrow_mut() = Ok(999_999); // far above ceiling
    let policy = FakePolicy::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let result = evaluator_pass(
        &cfg(),
        ExecutionMode::Armed,
        &db,
        &api,
        &chain,
        Some(&policy),
        &planner,
        &logger,
        eval_params(),
    );

    assert!(!result.outcome.applied);
    assert_eq!(
        *chain.opening_feerate_calls.borrow(),
        1,
        "the feerate itself IS fetched — it's the gate"
    );
    assert_eq!(
        *api.get_my_swaps_calls.borrow(),
        0,
        "reconcile must not fire once feerate already failed the gate"
    );
    assert_eq!(*api.get_applicable_swaps_calls.borrow(), 0);
}

#[test]
fn disabled_config_short_circuits_before_even_the_feerate_read() {
    // Control: with the feature off entirely, not even the cheap feerate
    // read should happen — matches py `run_cycle`'s very first check.
    let mut c = cfg();
    c.lnplus_swaps_enabled = false;
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let result = evaluator_pass(
        &c,
        ExecutionMode::Armed,
        &db,
        &api,
        &chain,
        Some(&policy),
        &planner,
        &logger,
        eval_params(),
    );

    assert!(!result.outcome.applied);
    assert_eq!(*chain.opening_feerate_calls.borrow(), 0);
    assert_eq!(*api.get_my_swaps_calls.borrow(), 0);
}

#[test]
fn healthy_pass_proceeds_through_reconcile_and_fetches_candidates() {
    // Control for all the short-circuit tests above: when nothing blocks
    // the pass, the live calls DO fire (proves the assertions above are
    // testing suppression, not a client that never calls anything).
    let db = FakeDb::new();
    db.set_config_override(BACKFILL_FLAG, "1"); // skip backfill's own extra calls
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let result = evaluator_pass(
        &cfg(),
        ExecutionMode::Armed,
        &db,
        &api,
        &chain,
        Some(&policy),
        &planner,
        &logger,
        eval_params(),
    );

    assert_eq!(*chain.opening_feerate_calls.borrow(), 1);
    assert_eq!(*api.get_my_swaps_calls.borrow(), 1, "reconcile's one call");
    assert_eq!(*api.get_applicable_swaps_calls.borrow(), 1);
    assert!(!result.outcome.applied && !result.outcome.recommended); // no swaps fixture -> no-op, not a crash
    assert_eq!(
        result.resolved_our_id.as_deref(),
        Some(pubkey(9).as_str()),
        "our_id resolved and returned for caching"
    );
}

#[test]
fn cached_our_id_is_reused_without_a_fresh_getinfo_call() {
    let db = FakeDb::new();
    db.set_config_override(BACKFILL_FLAG, "1");
    let api = FakeApi::new();
    let chain = FakeChain::new();
    // If `our_node_id` were called again it would return `pubkey(9)`
    // (the fake default) rather than this pre-seeded value — so a
    // mismatch here proves the cache was actually used.
    let policy = FakePolicy::new();
    let planner = FakePlanner::new();
    let logger = FakeLogger::new();

    let mut params = eval_params();
    params.cached_our_id = Some("cached-id".to_string());

    let result = evaluator_pass(
        &cfg(),
        ExecutionMode::Armed,
        &db,
        &api,
        &chain,
        Some(&policy),
        &planner,
        &logger,
        params,
    );

    assert_eq!(result.resolved_our_id.as_deref(), Some("cached-id"));
}

// -------------------------------------------------------- execution mode

/// A swap that clears every gate in `evaluator::run_cycle` (3 participants
/// incl. us, satisfying D-3's `lnplus_min_participants` floor) plus its
/// inferred outbound peer, so `swap_ev` is unambiguously positive. Used by
/// BOTH the `DryRun` and `Armed` tests below — using the SAME qualifying
/// fixture in both is what makes the `DryRun` assertion meaningful (if the
/// swap were rejected by an earlier gate instead, `create_application`
/// would never be called either way and the DryRun test would pass
/// vacuously).
fn qualifying_swap_and_outbound_peer() -> (revops_lnplus::types::SwapListing, String) {
    let peer_a = pubkey(1);
    let peer_b = pubkey(2);
    let swap = listing(
        "swap-1",
        vec![participant("A", &peer_a), participant("B", &peer_b)],
    );
    // 2 participants + us = 3, satisfying `lnplus_min_participants`.
    // Free identifier is "C"; outbound = next letter (wraps to "A").
    (swap, peer_a)
}

#[test]
fn dry_run_blocks_a_live_apply_even_when_config_says_execute() {
    let db = FakeDb::new();
    db.set_config_override(BACKFILL_FLAG, "1");
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let planner = FakePlanner::new();

    let (swap, outbound_peer) = qualifying_swap_and_outbound_peer();
    let _ = api.applicable_swaps.replace(Ok(vec![swap]));
    planner.set_ev(&outbound_peer, 100_000.0);
    let logger = FakeLogger::new();

    let result = evaluator_pass(
        &cfg(), // lnplus_execute_applications: true, planner_dry_run: false
        ExecutionMode::DryRun,
        &db,
        &api,
        &chain,
        Some(&policy),
        &planner,
        &logger,
        eval_params(),
    );

    assert!(
        !result.outcome.applied,
        "DryRun must never report a real apply"
    );
    assert!(
        api.create_application_calls.borrow().is_empty(),
        "the real LN+ API must never be called in DryRun"
    );
    // The row was intent-first written then patched to failed by the
    // kernel's own error-handling path (evaluator.rs `select_and_apply`) —
    // proves the mechanism is "the live call was refused", not "the swap
    // never qualified in the first place" (see `armed_control_...` below,
    // which proves the SAME fixture succeeds when armed).
    let row = db.get_swap("swap-1").expect("intent-first row must exist");
    assert_eq!(row.status, "failed");
    assert!(row
        .outcome
        .as_deref()
        .unwrap_or_default()
        .contains("DryRun"));
}

#[test]
fn armed_control_the_same_scenario_actually_applies() {
    let db = FakeDb::new();
    db.set_config_override(BACKFILL_FLAG, "1");
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let planner = FakePlanner::new();

    let (swap, outbound_peer) = qualifying_swap_and_outbound_peer();
    let _ = api.applicable_swaps.replace(Ok(vec![swap]));
    planner.set_ev(&outbound_peer, 100_000.0);
    let logger = FakeLogger::new();

    let result = evaluator_pass(
        &cfg(),
        ExecutionMode::Armed,
        &db,
        &api,
        &chain,
        Some(&policy),
        &planner,
        &logger,
        eval_params(),
    );

    assert!(
        result.outcome.applied,
        "Armed must allow the real apply through"
    );
    assert_eq!(api.create_application_calls.borrow().as_slice(), ["swap-1"]);
}

// ------------------------------------------------------------ watcher_pass

fn open_exec() -> OpenExecParams {
    OpenExecParams {
        estimated_cost_sats: 2500,
        effective_budget_sats: None,
        budget_since_timestamp: None,
    }
}

#[test]
fn watcher_dry_run_never_connects_or_funds_a_channel() {
    let db = FakeDb::new();
    db.insert(SwapRow::new("s1", "applied", 500_000, 6, 100));
    let api = FakeApi::new();
    let peer = pubkey(3);
    let _ = api.my_swaps.replace(Ok(revops_lnplus::types::MySwaps {
        pending: vec![],
        opening: vec![MySwapEntry {
            id: "s1".to_string(),
            outgoing_peer_pubkey: Some(peer.clone()),
            deadline: Some(TsValue::Epoch(10_000.0)),
            ..Default::default()
        }],
        completed: vec![],
    }));
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    let summary = watcher_pass(
        ExecutionMode::DryRun,
        &db,
        &api,
        &chain,
        &policy,
        None,
        &logger,
        &open_exec(),
        7,
        1_000, // well before the deadline
    );

    assert!(summary.opened.is_empty());
    assert!(
        chain.connect_calls.borrow().is_empty(),
        "DryRun must never dial a peer to open a swap channel"
    );
    assert!(
        chain.fund_channel_calls.borrow().is_empty(),
        "DryRun must never fund a channel"
    );
    let row = db.get_swap("s1").unwrap();
    assert_ne!(row.status, "opened");
}

#[test]
fn watcher_armed_control_actually_opens_the_channel() {
    let db = FakeDb::new();
    db.insert(SwapRow::new("s1", "applied", 500_000, 6, 100));
    let api = FakeApi::new();
    let peer = pubkey(3);
    let _ = api.my_swaps.replace(Ok(revops_lnplus::types::MySwaps {
        pending: vec![],
        opening: vec![MySwapEntry {
            id: "s1".to_string(),
            outgoing_peer_pubkey: Some(peer.clone()),
            deadline: Some(TsValue::Epoch(10_000.0)),
            ..Default::default()
        }],
        completed: vec![],
    }));
    let chain = FakeChain::new(); // fund_channel_result defaults to Ok(txid1)
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    let summary = watcher_pass(
        ExecutionMode::Armed,
        &db,
        &api,
        &chain,
        &policy,
        None,
        &logger,
        &open_exec(),
        7,
        1_000,
    );

    assert_eq!(summary.opened, vec!["s1".to_string()]);
    assert_eq!(chain.connect_calls.borrow().len(), 1);
    assert_eq!(chain.fund_channel_calls.borrow().len(), 1);
    assert_eq!(db.get_swap("s1").unwrap().status, "opened");
}

// -------------------------------------------------------------- WatcherLoop

#[test]
fn watcher_loop_skips_a_reentrant_call() {
    let loop_ = WatcherLoop::new();
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    // Hold the lock manually, simulating a pass already in flight.
    let _held = loop_.reentry_lock.try_lock().expect("lock free initially");
    let result = loop_.try_pass(
        ExecutionMode::DryRun,
        &db,
        &api,
        &chain,
        &policy,
        None,
        &logger,
        &open_exec(),
        7,
        1_000,
    );
    assert!(
        result.is_none(),
        "a concurrent pass must be skipped, not queued"
    );
    drop(_held);

    // Control: once released, the next call proceeds normally.
    let result2 = loop_.try_pass(
        ExecutionMode::DryRun,
        &db,
        &api,
        &chain,
        &policy,
        None,
        &logger,
        &open_exec(),
        7,
        1_000,
    );
    assert!(result2.is_some());
}
