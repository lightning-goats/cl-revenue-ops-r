//! `activate.rs` — gates 12-13: activation + no_close tag ownership.

mod common;

use common::*;
use revops_lnplus::activate::{
    activate, check_mid_contract_vanish, protect_peer_no_close, release_no_close_if_ours, TagColumn,
};
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::ports::{ChannelInfo, LnPlusDb};

#[test]
fn activate_protects_both_sides_and_marks_active() {
    let db = FakeDb::new();
    let policy = FakePolicy::new();
    let logger = FakeLogger::new();
    let outbound = pubkey(1);
    let incoming = pubkey(2);
    let row = SwapRow::new("1", "opened", 1_000_000, 6, 0)
        .with_outbound_peer(outbound.clone())
        .with_incoming_peer(incoming.clone());
    db.insert(row.clone());

    activate(&row, Some(50_000), None, &db, &policy, &logger);

    assert!(policy.has_tag(&outbound, "no_close"));
    assert!(policy.has_tag(&incoming, "no_close"));
    let after = db.get_swap("1").unwrap();
    assert_eq!(after.status, "active");
    assert_eq!(after.tag_added, Some(true));
    assert_eq!(after.incoming_tag_added, Some(true));
    assert_eq!(after.ends_at, Some(50_000));
    assert_eq!(
        db.action_status("swap_complete").as_deref(),
        Some("completed")
    );
}

#[test]
fn c3_pre_existing_tag_recorded_as_not_ours() {
    let db = FakeDb::new();
    let policy = FakePolicy::new();
    let peer = pubkey(1);
    policy.pre_tag(&peer, "no_close"); // operator already tagged this peer
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "active", 1, 1, 0);
    db.insert(row);

    protect_peer_no_close("1", &peer, TagColumn::Outbound, &db, &policy, &logger);

    let after = db.get_swap("1").unwrap();
    assert_eq!(
        after.tag_added,
        Some(false),
        "must not claim ownership of a pre-existing tag"
    );
}

#[test]
fn f3b_lookup_failure_protects_but_stamps_zero() {
    let db = FakeDb::new();
    let policy = FakePolicy::new();
    let peer = pubkey(1);
    policy.get_policy_fails_for.borrow_mut().push(peer.clone());
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "active", 1, 1, 0);
    db.insert(row);

    protect_peer_no_close("1", &peer, TagColumn::Outbound, &db, &policy, &logger);

    assert!(
        policy.has_tag(&peer, "no_close"),
        "must protect despite the lookup failure"
    );
    let after = db.get_swap("1").unwrap();
    assert_eq!(
        after.tag_added,
        Some(false),
        "F3b: never claim ownership under uncertainty"
    );
}

#[test]
fn control_fresh_tag_we_add_is_recorded_as_ours() {
    // CONTROL: when WE are the one adding the tag (no pre-existing tag,
    // lookup succeeds), ownership must be recorded as true -- otherwise
    // the two tests above proving "not ours" would be vacuous.
    let db = FakeDb::new();
    let policy = FakePolicy::new();
    let peer = pubkey(1);
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "active", 1, 1, 0);
    db.insert(row);

    protect_peer_no_close("1", &peer, TagColumn::Outbound, &db, &policy, &logger);

    assert!(policy.has_tag(&peer, "no_close"));
    assert_eq!(db.get_swap("1").unwrap().tag_added, Some(true));
}

#[test]
fn c3_release_only_removes_when_ours_and_no_other_active_contract_references_peer() {
    let db = FakeDb::new();
    let policy = FakePolicy::new();
    let peer = pubkey(1);
    policy.pre_tag(&peer, "no_close");
    let logger = FakeLogger::new();
    let mut row = SwapRow::new("1", "active", 1, 1, 0).with_outbound_peer(peer.clone());
    row.tag_added = Some(true);
    db.insert(row.clone());

    release_no_close_if_ours(
        "1",
        &row,
        Some(&peer),
        TagColumn::Outbound,
        &db,
        &policy,
        &logger,
    );
    assert!(!policy.has_tag(&peer, "no_close"));
}

#[test]
fn control_release_skipped_when_flag_not_ours() {
    let db = FakeDb::new();
    let policy = FakePolicy::new();
    let peer = pubkey(1);
    policy.pre_tag(&peer, "no_close");
    let logger = FakeLogger::new();
    let mut row = SwapRow::new("1", "active", 1, 1, 0).with_outbound_peer(peer.clone());
    row.tag_added = Some(false); // NOT ours (C-3 stamped)
    db.insert(row.clone());

    release_no_close_if_ours(
        "1",
        &row,
        Some(&peer),
        TagColumn::Outbound,
        &db,
        &policy,
        &logger,
    );
    assert!(
        policy.has_tag(&peer, "no_close"),
        "must not remove a tag we don't own"
    );
}

#[test]
fn control_release_skipped_when_another_active_contract_shares_the_peer() {
    let db = FakeDb::new();
    let policy = FakePolicy::new();
    let peer = pubkey(1);
    policy.pre_tag(&peer, "no_close");
    let logger = FakeLogger::new();
    // Another active row references the same peer as its incoming side.
    db.insert(SwapRow::new("2", "active", 1, 1, 0).with_incoming_peer(peer.clone()));
    let mut row = SwapRow::new("1", "active", 1, 1, 0).with_outbound_peer(peer.clone());
    row.tag_added = Some(true);
    db.insert(row.clone());

    release_no_close_if_ours(
        "1",
        &row,
        Some(&peer),
        TagColumn::Outbound,
        &db,
        &policy,
        &logger,
    );
    assert!(
        policy.has_tag(&peer, "no_close"),
        "shared protection must survive while another contract needs it"
    );
}

#[test]
fn check_mid_contract_vanish_logs_error_when_channel_gone() {
    let chain = FakeChain::new(); // no channels registered
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "active", 1, 1, 0).with_outbound_peer(pubkey(1));
    check_mid_contract_vanish(&row, &chain, &logger);
    assert!(logger.contains("closed mid-contract"));
}

#[test]
fn control_check_mid_contract_vanish_silent_when_channel_present() {
    let chain = FakeChain::new();
    let peer = pubkey(1);
    chain.channels.borrow_mut().push(ChannelInfo {
        peer_id: peer.clone(),
        state: "CHANNELD_NORMAL".to_string(),
        total_msat: 1,
        to_us_msat: 1,
        funding_txid: None,
    });
    let logger = FakeLogger::new();
    let row = SwapRow::new("1", "active", 1, 1, 0).with_outbound_peer(peer);
    check_mid_contract_vanish(&row, &chain, &logger);
    assert!(!logger.contains("closed mid-contract"));
}
