//! `backfill.rs` — adopting pre-existing (manual) LN+ state, rules 1-4.

mod common;

use common::*;
use revops_lnplus::backfill::backfill_from_lnplus;
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::ports::LnPlusDb;
use revops_lnplus::types::{MySwapEntry, MySwaps, Participant, SwapDetail};
use revops_lnplus::validation::TsValue;

fn entry(id: &str) -> MySwapEntry {
    MySwapEntry {
        id: id.to_string(),
        ..Default::default()
    }
}

// -------------------------------------------------------------------- Rule 1

#[test]
fn rule1_pending_import_starts_timeout_clock_now() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let mut e = entry("1");
    e.capacity_sats = Some(500_000);
    e.duration_months = Some(6);
    let my = MySwaps {
        pending: vec![e],
        ..Default::default()
    };
    let result = backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert_eq!(result.imported.pending, 1);
    let row = db.get_swap("1").unwrap();
    assert_eq!(row.status, "applied");
    assert_eq!(row.applied_at, 1000);
    assert_eq!(row.capacity_sats, 500_000);
}

#[test]
fn rule1_idempotent_skip_when_local_row_exists() {
    let db = FakeDb::new();
    db.insert(SwapRow::new("1", "applied", 1, 1, 0));
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        pending: vec![entry("1")],
        ..Default::default()
    };
    let result = backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert_eq!(result.imported.pending, 0);
    assert_eq!(result.skipped, vec!["1".to_string()]);
}

#[test]
fn rule1_missing_capacity_falls_back_to_get_swap_detail() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    api.swap_details.borrow_mut().insert(
        "1".to_string(),
        Ok(SwapDetail {
            id: "1".to_string(),
            capacity_sats: Some(750_000),
            duration_months: Some(3),
            participants: vec![],
        }),
    );
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        pending: vec![entry("1")],
        ..Default::default()
    };
    backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    let row = db.get_swap("1").unwrap();
    assert_eq!(row.capacity_sats, 750_000);
    assert_eq!(row.duration_months, 3);
}

// -------------------------------------------------------------------- Rule 2

#[test]
fn rule2_opening_import_with_valid_peer_and_deadline() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    let mut e = entry("2");
    e.capacity_sats = Some(1_000_000);
    e.duration_months = Some(6);
    e.outgoing_peer_pubkey = Some(peer.clone());
    e.deadline = Some(TsValue::Epoch(5000.0));
    let my = MySwaps {
        opening: vec![e],
        ..Default::default()
    };
    let result = backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert_eq!(result.imported.opening, 1);
    let row = db.get_swap("2").unwrap();
    assert_eq!(row.status, "opening");
    assert_eq!(row.outbound_peer.as_deref(), Some(peer.as_str()));
    assert_eq!(row.deadline_at, Some(5000));
}

#[test]
fn rule2_invalid_peer_left_null_with_warning() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let mut e = entry("2");
    e.outgoing_peer_pubkey = Some("not-a-pubkey".to_string());
    let my = MySwaps {
        opening: vec![e],
        ..Default::default()
    };
    let result = backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert!(result
        .warnings
        .iter()
        .any(|w| w.contains("invalid outgoing_peer_pubkey")));
    let row = db.get_swap("2").unwrap();
    assert!(row.outbound_peer.is_none());
}

#[test]
fn rule2_missing_deadline_falls_back_to_48h_with_warning() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        opening: vec![entry("2")],
        ..Default::default()
    };
    let result = backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert!(result
        .warnings
        .iter()
        .any(|w| w.contains("no parseable deadline")));
    let row = db.get_swap("2").unwrap();
    assert_eq!(row.deadline_at, Some(1000 + 48 * 3600));
}

// -------------------------------------------------------------------- Rule 3

#[test]
fn rule3_running_contract_derives_outbound_peer_and_protects_pending() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let our_id = pubkey(9);
    *chain.our_id.borrow_mut() = Ok(our_id.clone());
    let logger = FakeLogger::new();

    let peer_next = pubkey(2);
    api.swap_details.borrow_mut().insert(
        "3".to_string(),
        Ok(SwapDetail {
            id: "3".to_string(),
            capacity_sats: Some(2_000_000),
            duration_months: Some(6),
            participants: vec![
                Participant {
                    participant_identifier: Some("A".to_string()),
                    pubkey: Some(our_id.clone()),
                    ..Default::default()
                },
                Participant {
                    participant_identifier: Some("B".to_string()),
                    pubkey: Some(peer_next.clone()),
                    ..Default::default()
                },
            ],
        }),
    );
    let mut e = entry("3");
    e.ends = Some(TsValue::Epoch(50_000.0));
    let my = MySwaps {
        completed: vec![e],
        ..Default::default()
    };
    let result = backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert_eq!(result.imported.active, 1);
    let row = db.get_swap("3").unwrap();
    assert_eq!(row.status, "opened");
    assert_eq!(row.outbound_peer.as_deref(), Some(peer_next.as_str()));
    assert_eq!(row.ends_at, Some(50_000));
    assert!(logger.contains("phase 4 this pass will activate no_close protection"));
}

#[test]
fn rule3_get_swap_failure_imports_with_outbound_null_error_logged() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    // No fixture registered -> get_swap returns an error.
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let mut e = entry("3");
    e.ends = Some(TsValue::Epoch(50_000.0));
    e.capacity_sats = Some(1_000_000);
    let my = MySwaps {
        completed: vec![e],
        ..Default::default()
    };
    backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    let row = db.get_swap("3").unwrap();
    assert!(row.outbound_peer.is_none());
    assert!(logger.contains("operator must protect this channel manually"));
}

// -------------------------------------------------------------------- Rule 4

#[test]
fn rule4_ended_contract_imported_unrated_and_bumps_peer() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let incoming = pubkey(4);
    let mut e = entry("4");
    e.ends = Some(TsValue::Epoch(500.0)); // already in the past relative to now=1000
    e.capacity_sats = Some(300_000);
    e.duration_months = Some(3);
    e.incoming_peer_pubkey = Some(incoming.clone());
    let my = MySwaps {
        completed: vec![e],
        ..Default::default()
    };
    let result = backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert_eq!(result.imported.ended, 1);
    let row = db.get_swap("4").unwrap();
    assert_eq!(row.status, "ended");
    assert_eq!(row.outcome.as_deref(), Some("imported_pre_automation"));
    assert!(
        api.create_rating_calls.borrow().is_empty(),
        "rule 4: never rate an already-ended contract"
    );
    let peer = db.get_peer(&incoming).unwrap();
    assert_eq!(peer.swaps_count, 1);
    assert_eq!(peer.defections, 0);
}

#[test]
fn rule4_missing_ends_warns_but_still_imports_as_ended() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let my = MySwaps {
        completed: vec![entry("4")],
        ..Default::default()
    };
    let result = backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert!(result
        .warnings
        .iter()
        .any(|w| w.contains("no parseable 'ends'")));
    assert_eq!(db.get_swap("4").unwrap().status, "ended");
}

#[test]
fn control_completed_entry_still_running_takes_rule3_not_rule4() {
    // CONTROL distinguishing rule 3 vs rule 4: an `ends` timestamp in the
    // FUTURE must take the running-contract path (status "opened"), not
    // the ended path (status "ended").
    let db = FakeDb::new();
    let api = FakeApi::new();
    api.swap_details.borrow_mut().insert(
        "5".to_string(),
        Ok(SwapDetail {
            id: "5".to_string(),
            capacity_sats: Some(1_000_000),
            duration_months: Some(6),
            participants: vec![],
        }),
    );
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let mut e = entry("5");
    e.ends = Some(TsValue::Epoch(999_999.0)); // far future relative to now=1000
    let my = MySwaps {
        completed: vec![e],
        ..Default::default()
    };
    backfill_from_lnplus(&my, &db, &api, &chain, &logger, 1000).expect("backfill");
    assert_eq!(db.get_swap("5").unwrap().status, "opened");
}
