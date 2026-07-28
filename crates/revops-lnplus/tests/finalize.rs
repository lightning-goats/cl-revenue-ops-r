//! `finalize.rs` — gate 14, plus **defect #1**'s integration proof: a
//! watcher-phase-5-shaped caller must only count a swap as finalized when
//! `finalize` actually returned `Finalized`.

mod common;

use common::*;
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::finalize::{finalize, retry_pending_ratings, FinalizeOutcome};
use revops_lnplus::ports::{ChannelInfo, LnPlusDb};

/// Mirrors `watcher.rs` phase 5's exact consumption pattern: only push to
/// `finalized` when the outcome says so. This is what defect #1's fix
/// makes possible; `tests/watcher.rs` proves the real orchestrator does
/// this too.
fn phase5_shaped_caller(outcome: &FinalizeOutcome, finalized: &mut Vec<String>, sid: &str) {
    if outcome.is_finalized() {
        finalized.push(sid.to_string());
    }
}

#[test]
fn defect1_deferred_outcome_must_not_be_counted_as_finalized() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let policy = FakePolicy::new();
    let ignore = FakeIgnorePeer::default();
    let chain = FakeChain::new();
    chain.list_peer_channels_fails.replace(true);
    let logger = FakeLogger::new();

    let row = SwapRow::new("1", "active", 1_000_000, 6, 0)
        .with_outbound_peer(pubkey(1))
        .with_incoming_peer(pubkey(2));
    db.insert(row.clone());

    let outcome =
        finalize(&row, &db, &api, &policy, Some(&ignore), &chain, &logger).expect("finalize");
    assert!(matches!(outcome, FinalizeOutcome::Deferred { .. }));

    let mut finalized = Vec::new();
    phase5_shaped_caller(&outcome, &mut finalized, "1");
    assert!(
        finalized.is_empty(),
        "defect #1: a deferred outcome must never be reported as finalized"
    );

    // And the row itself must be untouched (still 'active').
    assert_eq!(db.get_swap("1").unwrap().status, "active");
}

#[test]
fn control_genuinely_finalized_outcome_is_counted() {
    // CONTROL for defect #1: a REAL finalize (channel present -> positive
    // rating) must be counted. Proves `phase5_shaped_caller` isn't
    // dropping everything.
    let db = FakeDb::new();
    let api = FakeApi::new();
    let policy = FakePolicy::new();
    let ignore = FakeIgnorePeer::default();
    let chain = FakeChain::new();
    let incoming = pubkey(2);
    chain.channels.borrow_mut().push(ChannelInfo {
        peer_id: incoming.clone(),
        state: "CHANNELD_NORMAL".to_string(),
        total_msat: 1,
        to_us_msat: 1,
        funding_txid: None,
    });
    let logger = FakeLogger::new();

    let row = SwapRow::new("1", "active", 1_000_000, 6, 0)
        .with_outbound_peer(pubkey(1))
        .with_incoming_peer(incoming);
    db.insert(row.clone());

    let outcome =
        finalize(&row, &db, &api, &policy, Some(&ignore), &chain, &logger).expect("finalize");
    assert!(outcome.is_finalized());

    let mut finalized = Vec::new();
    phase5_shaped_caller(&outcome, &mut finalized, "1");
    assert_eq!(finalized, vec!["1".to_string()]);
    assert_eq!(db.get_swap("1").unwrap().status, "ended");
}

#[test]
fn positive_rating_filed_when_incoming_channel_still_open() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let policy = FakePolicy::new();
    let ignore = FakeIgnorePeer::default();
    let chain = FakeChain::new();
    let incoming = pubkey(2);
    chain.channels.borrow_mut().push(ChannelInfo {
        peer_id: incoming.clone(),
        state: "CHANNELD_NORMAL".to_string(),
        total_msat: 1,
        to_us_msat: 1,
        funding_txid: None,
    });
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "active", 1_000_000, 6, 0)
        .with_outbound_peer(pubkey(1))
        .with_incoming_peer(incoming.clone());
    db.insert(row.clone());

    finalize(&row, &db, &api, &policy, Some(&ignore), &chain, &logger).expect("finalize");

    assert_eq!(
        api.create_rating_calls.borrow()[0],
        ("1".to_string(), revops_lnplus::types::Rating::Positive)
    );
    assert!(
        ignore.calls.borrow().is_empty(),
        "must not ignore a peer who honored the contract"
    );
    let peer = db.get_peer(&incoming).unwrap();
    assert_eq!(peer.defections, 0);
}

#[test]
fn negative_rating_and_ignore_peer_when_channel_closed_defection() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let policy = FakePolicy::new();
    let ignore = FakeIgnorePeer::default();
    let chain = FakeChain::new(); // no channels -> incoming closed
    let logger = FakeLogger::new();
    let incoming = pubkey(2);
    let row = SwapRow::new("1", "active", 1_000_000, 6, 0)
        .with_outbound_peer(pubkey(1))
        .with_incoming_peer(incoming.clone());
    db.insert(row.clone());

    finalize(&row, &db, &api, &policy, Some(&ignore), &chain, &logger).expect("finalize");

    assert_eq!(
        api.create_rating_calls.borrow()[0],
        ("1".to_string(), revops_lnplus::types::Rating::Negative)
    );
    assert_eq!(ignore.calls.borrow()[0].0, incoming);
    let peer = db.get_peer(&incoming).unwrap();
    assert_eq!(peer.defections, 1);
}

#[test]
fn c8_missing_incoming_peer_ends_unjudged_no_rating() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let policy = FakePolicy::new();
    let ignore = FakeIgnorePeer::default();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "active", 1_000_000, 6, 0).with_outbound_peer(pubkey(1));
    db.insert(row.clone());

    let outcome =
        finalize(&row, &db, &api, &policy, Some(&ignore), &chain, &logger).expect("finalize");
    assert!(outcome.is_finalized());
    assert!(api.create_rating_calls.borrow().is_empty());
    assert_eq!(
        db.get_swap("1").unwrap().outcome.as_deref(),
        Some("ended_unjudged")
    );
}

// ------------------------------------------------------------ retry ratings

#[test]
fn retry_pending_ratings_files_on_success() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "ended_rating_pending", 1, 6, 0).with_ends_at(1000);
    let mut row = row.clone();
    row.outcome = Some("positive".to_string());
    db.insert(row);

    retry_pending_ratings(&db, &api, &logger, 2000).expect("ratings");

    assert_eq!(db.get_swap("1").unwrap().status, "ended");
}

#[test]
fn retry_pending_ratings_gives_up_after_seven_days() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    api.create_rating_results.borrow_mut().insert(
        "1".to_string(),
        Err(revops_lnplus::error::LnPlusError::new("still failing")),
    );
    let logger = FakeLogger::new();
    let mut row = SwapRow::new("1", "ended_rating_pending", 1, 6, 0).with_ends_at(1000);
    row.outcome = Some("negative".to_string());
    db.insert(row);

    let far_future = 1000 + 7 * 86_400 + 1;
    retry_pending_ratings(&db, &api, &logger, far_future).expect("ratings");

    let after = db.get_swap("1").unwrap();
    assert_eq!(after.status, "ended");
    assert_eq!(after.outcome.as_deref(), Some("negative_rating_unfiled"));
}

#[test]
fn control_retry_pending_ratings_stays_pending_before_the_deadline() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    api.create_rating_results.borrow_mut().insert(
        "1".to_string(),
        Err(revops_lnplus::error::LnPlusError::new("still failing")),
    );
    let logger = FakeLogger::new();
    let mut row = SwapRow::new("1", "ended_rating_pending", 1, 6, 0).with_ends_at(1000);
    row.outcome = Some("negative".to_string());
    db.insert(row);

    let still_within_window = 1000 + 86_400; // 1 day past ends_at, well under 7
    retry_pending_ratings(&db, &api, &logger, still_within_window).expect("ratings");

    assert_eq!(
        db.get_swap("1").unwrap().status,
        "ended_rating_pending",
        "must keep retrying inside the 7-day window"
    );
}
