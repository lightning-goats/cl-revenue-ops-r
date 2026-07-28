//! `watcher.rs` — `run_watcher_once` end to end, phases 1-7. This is
//! where **defect #1**'s fix is exercised through the REAL orchestrator
//! (not a hand-rolled caller like `tests/finalize.rs`'s
//! `phase5_shaped_caller`).

mod common;

use common::*;
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::open::OpenExecParams;
use revops_lnplus::ports::{ChannelInfo, LnPlusDb};
use revops_lnplus::types::MySwapEntry;
use revops_lnplus::watcher::{run_watcher_once, WatcherSummary};

fn open_params() -> OpenExecParams {
    OpenExecParams {
        estimated_cost_sats: 2000,
        effective_budget_sats: None,
        budget_since_timestamp: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn run(
    db: &FakeDb,
    api: &FakeApi,
    chain: &FakeChain,
    policy: &FakePolicy,
    logger: &FakeLogger,
    now: i64,
) -> WatcherSummary {
    run_watcher_once(db, api, chain, policy, None, logger, &open_params(), 7, now)
        .expect("watcher pass")
}

#[test]
fn phase3_funds_an_applied_swap_now_showing_opening_on_lnplus() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    let peer = pubkey(1);
    db.insert(SwapRow::new("1", "applied", 1_000_000, 6, 0));
    *api.my_swaps.borrow_mut() = Ok(revops_lnplus::types::MySwaps {
        opening: vec![MySwapEntry {
            id: "1".to_string(),
            outgoing_peer_pubkey: Some(peer.clone()),
            ..Default::default()
        }],
        ..Default::default()
    });

    let summary = run(&db, &api, &chain, &policy, &logger, 1000);

    assert_eq!(summary.opened, vec!["1".to_string()]);
    let row = db.get_swap("1").unwrap();
    assert_eq!(row.status, "opened");
    assert_eq!(row.outbound_peer.as_deref(), Some(peer.as_str()));
}

#[test]
fn phase3_does_not_resurrect_a_terminal_local_row_c2() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    db.insert(SwapRow::new("1", "failed", 1_000_000, 6, 0));
    *api.my_swaps.borrow_mut() = Ok(revops_lnplus::types::MySwaps {
        opening: vec![MySwapEntry {
            id: "1".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    });

    let summary = run(&db, &api, &chain, &policy, &logger, 1000);

    assert!(summary.opened.is_empty());
    assert_eq!(
        db.get_swap("1").unwrap().status,
        "failed",
        "must stay terminal, not resurrect to 'opening'"
    );
    assert!(logger.contains("not resurrecting"));
}

#[test]
fn phase4_activates_a_locally_opened_row_lnplus_reports_completed() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    let outbound = pubkey(1);
    let incoming = pubkey(2);
    db.insert(SwapRow::new("1", "opened", 1_000_000, 6, 0).with_outbound_peer(outbound.clone()));
    *api.my_swaps.borrow_mut() = Ok(revops_lnplus::types::MySwaps {
        completed: vec![MySwapEntry {
            id: "1".to_string(),
            incoming_peer_pubkey: Some(incoming.clone()),
            ..Default::default()
        }],
        ..Default::default()
    });

    let summary = run(&db, &api, &chain, &policy, &logger, 1000);

    assert_eq!(summary.activated, vec!["1".to_string()]);
    let row = db.get_swap("1").unwrap();
    assert_eq!(row.status, "active");
    assert!(policy.has_tag(&outbound, "no_close"));
    assert!(policy.has_tag(&incoming, "no_close"));
}

#[test]
fn defect1_watcher_phase5_only_counts_genuine_finalizations() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    chain.list_peer_channels_fails.replace(true); // finalize must defer
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    let row = SwapRow::new("1", "active", 1_000_000, 6, 0)
        .with_outbound_peer(pubkey(1))
        .with_incoming_peer(pubkey(2))
        .with_ends_at(500); // already past `now`
    db.insert(row);

    let summary = run(&db, &api, &chain, &policy, &logger, 1000);

    assert!(
        summary.finalized.is_empty(),
        "defect #1: a deferred finalize (listpeerchannels failure) must not be reported as finalized"
    );
    assert_eq!(
        db.get_swap("1").unwrap().status,
        "active",
        "row must be untouched, ready for retry next pass"
    );
}

#[test]
fn control_watcher_phase5_counts_a_real_finalization() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let incoming = pubkey(2);
    chain.channels.borrow_mut().push(ChannelInfo {
        peer_id: incoming.clone(),
        state: "CHANNELD_NORMAL".to_string(),
        total_msat: 1,
        to_us_msat: 1,
        funding_txid: None,
    });
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    let row = SwapRow::new("1", "active", 1_000_000, 6, 0)
        .with_outbound_peer(pubkey(1))
        .with_incoming_peer(incoming)
        .with_ends_at(500);
    db.insert(row);

    let summary = run(&db, &api, &chain, &policy, &logger, 1000);

    assert_eq!(summary.finalized, vec!["1".to_string()]);
    assert_eq!(db.get_swap("1").unwrap().status, "ended");
}

#[test]
fn phase6_withdraws_timed_out_pending_applications() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    db.insert(SwapRow::new("1", "applied", 500_000, 6, 0)); // applied at t=0
    *api.my_swaps.borrow_mut() = Ok(revops_lnplus::types::MySwaps {
        pending: vec![MySwapEntry {
            id: "1".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    });

    let eight_days = 8 * 86_400;
    let summary = run(&db, &api, &chain, &policy, &logger, eight_days);

    assert_eq!(summary.withdrawn, vec!["1".to_string()]);
    assert_eq!(db.get_swap("1").unwrap().status, "withdrawn");
}

#[test]
fn b1_outage_path_still_drives_opening_rows_off_the_local_ledger() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    *api.my_swaps.borrow_mut() = Err(revops_lnplus::error::LnPlusError::new("lnplus unreachable"));
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    let peer = pubkey(1);
    db.insert(
        SwapRow::new("1", "opening", 1_000_000, 6, 500)
            .with_outbound_peer(peer)
            .with_deadline_at(200_000),
    );

    let summary = run(&db, &api, &chain, &policy, &logger, 1000);

    assert_eq!(summary.skipped.as_deref(), Some("lnplus unreachable"));
    assert_eq!(
        summary.opened,
        vec!["1".to_string()],
        "B1: a funded deadline must not wait on LN+ being back up"
    );
}

#[test]
fn self_heal_retags_a_contract_missing_incoming_protection() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();

    let outbound = pubkey(1);
    let incoming = pubkey(2);
    let mut row = SwapRow::new("1", "active", 1_000_000, 6, 0)
        .with_outbound_peer(outbound.clone())
        .with_incoming_peer(incoming.clone())
        .with_ends_at(999_999); // far future -- not due for finalize
    row.tag_added = Some(true); // outbound already protected
    row.incoming_tag_added = None; // incoming never evaluated
    db.insert(row);

    run(&db, &api, &chain, &policy, &logger, 1000);

    assert!(
        policy.has_tag(&incoming, "no_close"),
        "self-heal must protect the missed side"
    );
    assert_eq!(db.get_swap("1").unwrap().incoming_tag_added, Some(true));
}
