//! `open.rs` — gates 10-11 channel-open execution, plus **defect #4**'s
//! integration proof: a missed deadline must terminalize the row, not
//! just trip the breaker.

mod common;

use common::*;
use revops_lnplus::breaker::BreakerCause;
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::open::{
    already_past_opening, complete_and_mark_opened, execute_swap_open, maybe_trip_deadline_miss,
    OpenExecParams,
};
use revops_lnplus::ports::{ChannelInfo, Feerate, LnPlusDb};
use revops_lnplus::telemetry::reserved_sats;

fn params() -> OpenExecParams {
    OpenExecParams {
        estimated_cost_sats: 2000,
        effective_budget_sats: None,
        budget_since_timestamp: None,
    }
}

// -------------------------------------------------------------- funding path

#[test]
fn execute_swap_open_full_funding_flow() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 0)
        .with_outbound_peer(peer.clone())
        .with_deadline_at(200_000);
    db.insert(row.clone());

    let opened =
        execute_swap_open(&row, None, &params(), &db, &api, &chain, &logger, 1000).expect("open");
    assert!(opened);
    assert_eq!(
        chain.connect_calls.borrow().as_slice(),
        std::slice::from_ref(&peer)
    );
    assert_eq!(chain.fund_channel_calls.borrow().len(), 1);
    let row_after = db.get_swap("1").unwrap();
    assert_eq!(row_after.status, "opened");
    assert_eq!(row_after.channel_funding_txid.as_deref(), Some("txid1"));
    assert_eq!(
        api.complete_application_calls.borrow().as_slice(),
        &["1".to_string()]
    );
}

#[test]
fn idempotent_skip_matches_existing_channel_by_total_msat() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    chain.channels.borrow_mut().push(ChannelInfo {
        peer_id: peer.clone(),
        state: "CHANNELD_NORMAL".to_string(),
        total_msat: 1_000_000_000,
        to_us_msat: 0,
        funding_txid: Some("existing-txid".to_string()),
    });
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 0)
        .with_outbound_peer(peer.clone())
        .with_deadline_at(200_000);
    db.insert(row.clone());
    let opened =
        execute_swap_open(&row, None, &params(), &db, &api, &chain, &logger, 1000).expect("open");
    assert!(opened);
    assert!(
        chain.connect_calls.borrow().is_empty(),
        "must not fundchannel again"
    );
    assert!(chain.fund_channel_calls.borrow().is_empty());
    assert_eq!(
        db.get_swap("1").unwrap().channel_funding_txid.as_deref(),
        Some("existing-txid")
    );
}

#[test]
fn b7_dual_fund_matches_by_to_us_msat_when_total_msat_diverges() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    // total_msat inflated by peer's dual-fund contribution; our own
    // contribution (to_us_msat) still matches the committed capacity.
    chain.channels.borrow_mut().push(ChannelInfo {
        peer_id: peer.clone(),
        state: "CHANNELD_NORMAL".to_string(),
        total_msat: 5_000_000_000,
        to_us_msat: 1_000_000_000,
        funding_txid: Some("dual-fund-txid".to_string()),
    });
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 0)
        .with_outbound_peer(peer.clone())
        .with_deadline_at(200_000);
    db.insert(row.clone());
    let opened =
        execute_swap_open(&row, None, &params(), &db, &api, &chain, &logger, 1000).expect("open");
    assert!(opened);
    assert!(chain.fund_channel_calls.borrow().is_empty());
}

#[test]
fn control_capacity_mismatch_does_not_match_existing_channel_i5b() {
    // CONTROL for I5(b): an existing channel to the peer that does NOT
    // match this row's committed capacity by either measure must NOT be
    // treated as "our swap channel" -- fundchannel proceeds.
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    chain.channels.borrow_mut().push(ChannelInfo {
        peer_id: peer.clone(),
        state: "CHANNELD_NORMAL".to_string(),
        total_msat: 42_000_000, // unrelated pre-existing channel
        to_us_msat: 42_000_000,
        funding_txid: Some("unrelated-txid".to_string()),
    });
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 0)
        .with_outbound_peer(peer.clone())
        .with_deadline_at(200_000);
    db.insert(row.clone());
    let opened =
        execute_swap_open(&row, None, &params(), &db, &api, &chain, &logger, 1000).expect("open");
    assert!(opened);
    assert_eq!(
        chain.fund_channel_calls.borrow().len(),
        1,
        "must fund a NEW channel, not reuse the unrelated one"
    );
}

#[test]
fn already_funded_row_only_completes_application() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 0).with_channel_funding_txid("prior-txid");
    db.insert(row.clone());
    let opened =
        execute_swap_open(&row, None, &params(), &db, &api, &chain, &logger, 1000).expect("open");
    assert!(opened);
    assert!(chain.connect_calls.borrow().is_empty());
    assert!(chain.fund_channel_calls.borrow().is_empty());
    assert_eq!(
        api.complete_application_calls.borrow().as_slice(),
        &["1".to_string()]
    );
}

// -------------------------------------------------------------- feerate

#[test]
fn feerate_selection_by_deadline_slack() {
    let cases = [
        (1000, 200_000, Feerate::Slow),
        (1000, 50_000, Feerate::Normal),
        (1000, 5_000, Feerate::Urgent),
    ];
    for (now, deadline, expected) in cases {
        let db = FakeDb::new();
        let api = FakeApi::new();
        let chain = FakeChain::new();
        let logger = FakeLogger::new();
        let peer = pubkey(1);
        let row = SwapRow::new("1", "opening", 1_000_000, 6, 0)
            .with_outbound_peer(peer)
            .with_deadline_at(deadline);
        db.insert(row.clone());
        execute_swap_open(&row, None, &params(), &db, &api, &chain, &logger, now).expect("open");
        let (_, _, feerate) = chain.fund_channel_calls.borrow()[0].clone();
        assert_eq!(feerate, expected, "now={now} deadline={deadline}");
    }
}

// -------------------------------------------------------------- failure paths

#[test]
fn connect_failure_does_not_write_opening_intent_and_does_not_reserve() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    *chain.connect_result.borrow_mut() =
        Err(revops_lnplus::ports::PortError::new("connect refused"));
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 0)
        .with_outbound_peer(peer)
        .with_deadline_at(200_000);
    let opened =
        execute_swap_open(&row, None, &params(), &db, &api, &chain, &logger, 1000).expect("open");
    assert!(!opened);
    assert!(db.reservations.borrow().is_empty());
}

#[test]
fn fundchannel_failure_releases_the_reservation() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    *chain.fund_channel_result.borrow_mut() =
        Err(revops_lnplus::ports::PortError::new("fundchannel failed"));
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 0)
        .with_outbound_peer(peer)
        .with_deadline_at(200_000);
    db.insert(row.clone());
    let opened =
        execute_swap_open(&row, None, &params(), &db, &api, &chain, &logger, 1000).expect("open");
    assert!(!opened);
    let reservations = db.reservations.borrow();
    let (_, record) = reservations
        .iter()
        .next()
        .expect("a reservation was attempted");
    assert!(!record.active, "must be released on fundchannel failure");
}

// -------------------------------------------------------------- already_past_opening

#[test]
fn already_past_opening_structural_match() {
    let mut errors = revops_lnplus::error::ErrorsMap::new();
    errors.insert(
        "id".to_string(),
        vec!["application is not in opening state".to_string()],
    );
    let e = revops_lnplus::error::LnPlusError::with_errors("x", 422, errors);
    assert!(already_past_opening(&e));
}

#[test]
fn complete_and_mark_opened_treats_already_past_opening_as_success() {
    let db = FakeDb::new();
    db.insert(SwapRow::new("1", "opening", 1, 1, 0));
    let api = FakeApi::new();
    let mut errors = revops_lnplus::error::ErrorsMap::new();
    errors.insert("id".to_string(), vec!["not in opening state".to_string()]);
    api.complete_application_results.borrow_mut().insert(
        "1".to_string(),
        Err(revops_lnplus::error::LnPlusError::with_errors(
            "x", 422, errors,
        )),
    );
    let logger = FakeLogger::new();
    assert!(complete_and_mark_opened("1", &db, &api, &logger).expect("complete"));
    assert_eq!(db.get_swap("1").unwrap().status, "opened");
}

#[test]
fn control_generic_complete_application_failure_is_retryable() {
    let db = FakeDb::new();
    db.insert(SwapRow::new("1", "opening", 1, 1, 0));
    let api = FakeApi::new();
    api.complete_application_results.borrow_mut().insert(
        "1".to_string(),
        Err(revops_lnplus::error::LnPlusError::new("timeout")),
    );
    let logger = FakeLogger::new();
    assert!(!complete_and_mark_opened("1", &db, &api, &logger).expect("complete"));
    assert_eq!(
        db.get_swap("1").unwrap().status,
        "opening",
        "must not mark opened on a transient failure"
    );
}

// ---------------------------------------------------------------- defect #4

#[test]
fn defect4_missed_deadline_terminalizes_the_row_not_just_the_breaker() {
    let db = FakeDb::new();
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 500).with_deadline_at(1000);
    db.insert(row.clone());

    maybe_trip_deadline_miss(&row, "1", Some(1000), 2000, &db, &logger).expect("deadline");

    let state = db
        .get_breaker()
        .unwrap()
        .expect("breaker must trip on a missed deadline");
    assert_eq!(
        state.cause,
        BreakerCause::MissedOpenDeadline {
            swap_id: "1".to_string()
        }
    );

    let after = db.get_swap("1").unwrap();
    assert_eq!(
        after.status, "failed",
        "defect #4 fix: row must be terminalized, not left at 'opening' forever"
    );

    // The fix's whole point: reserved_sats must now exclude this row.
    let all_rows = db.get_swaps_by_status(&["applied", "opening", "opened", "failed"]);
    assert_eq!(reserved_sats(&all_rows), 0);
}

#[test]
fn control_deadline_not_yet_passed_does_not_trip_or_terminalize() {
    let db = FakeDb::new();
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 500).with_deadline_at(5000);
    db.insert(row.clone());

    maybe_trip_deadline_miss(&row, "1", Some(5000), 2000, &db, &logger).expect("deadline");

    assert!(db.get_breaker().unwrap().is_none());
    assert_eq!(db.get_swap("1").unwrap().status, "opening");
}

#[test]
fn control_already_funded_row_never_trips_deadline_miss() {
    let db = FakeDb::new();
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "opening", 1_000_000, 6, 500)
        .with_deadline_at(1000)
        .with_channel_funding_txid("txid1");
    db.insert(row.clone());

    maybe_trip_deadline_miss(&row, "1", Some(1000), 2000, &db, &logger).expect("deadline");

    assert!(
        db.get_breaker().unwrap().is_none(),
        "a funded row missing its deadline is not a real miss"
    );
    assert_eq!(db.get_swap("1").unwrap().status, "opening");
}
