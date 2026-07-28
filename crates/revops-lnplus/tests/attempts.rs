//! Task 61 stage 4B — durable attempt/reservation identity with typed
//! NotSubmitted / Committed / OutcomeUnknown semantics, proven against the
//! REAL `SqliteLnPlusDb` (temp files only; never a production db).
//!
//! Contract pinned here (supervisor GO message):
//!  - stable attempt/reservation IDs persisted BEFORE any external submit,
//!    surviving restart;
//!  - typed resolutions; an OutcomeUnknown attempt RETAINS its reservation,
//!    enters quarantine, and blocks any new attempt for the same
//!    (swap, kind) — no auto-resubmit, no release while unknown;
//!  - quarantine survives restart (a stale in-flight 'intent' from a
//!    crashed process is promoted to outcome_unknown, never silently
//!    dropped);
//!  - the fund-committed resolution is ONE transaction: terminal row
//!    update + reservation settle + receipt event + attempt state — proven
//!    all-or-nothing with a write-only fault on the receipt half;
//!  - resolution replay is exactly-once: a second resolve is a typed
//!    `AlreadyResolved` that writes nothing (no double settle, no second
//!    receipt).

mod common;

use common::FakeLogger;
use revops_lnplus::db_types::SwapRow;
use revops_lnplus::ports::{
    AttemptIntent, AttemptKind, AttemptResolution, AttemptState, BeginAttemptAck, LnPlusDb,
    ReserveSpendRequest, ResolveAck,
};
use revops_lnplus::sqlite_db::SqliteLnPlusDb;

fn open_db() -> (tempfile::TempDir, SqliteLnPlusDb, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lnplus.db");
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).expect("open db");
    (dir, db, path)
}

fn sabotage(path: &std::path::Path, sql: &str) {
    let conn = rusqlite::Connection::open(path).expect("sabotage connection");
    conn.execute_batch(sql).expect("sabotage sql");
}

fn fund_intent(sid: &str, reservation: Option<&str>) -> AttemptIntent {
    AttemptIntent {
        attempt_id: format!("fund:{sid}:1000"),
        swap_id: sid.to_string(),
        kind: AttemptKind::Fund,
        reservation_id: reservation.map(str::to_string),
        peer_id: Some("02aa".to_string()),
        amount_sats: Some(1_000_000),
        created_at: 1000,
    }
}

fn reserve(db: &SqliteLnPlusDb, rid: &str, sats: i64) {
    let granted = db
        .reserve_spend(&ReserveSpendRequest {
            reservation_id: rid.to_string(),
            amount_sats: sats,
            category: "channel_open",
            subcategory: "lnplus_swap",
            metadata: Default::default(),
            effective_budget_sats: None,
            since_timestamp: None,
        })
        .expect("reserve acked");
    assert!(granted);
}

fn reservation_status(path: &std::path::Path, rid: &str) -> Option<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        "SELECT status FROM spend_reservations WHERE reservation_id = ?1",
        [rid],
        |r| r.get(0),
    )
    .ok()
}

fn receipt_count(path: &std::path::Path, rid: &str) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM spend_events WHERE event_id = ?1",
        [format!("resv:{rid}")],
        |r| r.get(0),
    )
    .unwrap()
}

// ------------------------------------------------- identity + quarantine gate

#[test]
fn begin_attempt_persists_intent_and_blocks_a_second_inflight_attempt() {
    let (_dir, db, _path) = open_db();
    assert_eq!(
        db.begin_attempt(&fund_intent("s1", Some("resv-1")))
            .expect("begin acked"),
        BeginAttemptAck::Started
    );
    // A second attempt for the same (swap, kind) while one is in flight
    // must be BLOCKED with the existing identity — the no-auto-resubmit
    // rail at the store, not caller discipline.
    let mut second = fund_intent("s1", Some("resv-2"));
    second.attempt_id = "fund:s1:2000".to_string();
    second.created_at = 2000;
    assert_eq!(
        db.begin_attempt(&second).expect("begin acked"),
        BeginAttemptAck::Blocked {
            existing_attempt_id: "fund:s1:1000".to_string(),
            state: AttemptState::Intent,
        }
    );
    // A DIFFERENT kind for the same swap is independent.
    let apply = AttemptIntent {
        attempt_id: "apply:s1:1000".to_string(),
        swap_id: "s1".to_string(),
        kind: AttemptKind::Apply,
        reservation_id: None,
        peer_id: None,
        amount_sats: None,
        created_at: 1000,
    };
    assert_eq!(
        db.begin_attempt(&apply).expect("begin acked"),
        BeginAttemptAck::Started
    );
}

#[test]
fn attempt_identity_and_quarantine_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    {
        let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
        reserve(&db, "resv-q", 500);
        db.begin_attempt(&fund_intent("s1", Some("resv-q")))
            .unwrap();
        db.resolve_attempt(
            "fund:s1:1000",
            &AttemptResolution::OutcomeUnknown {
                detail: "timeout after submit".to_string(),
            },
            2000,
        )
        .unwrap();
    }
    // Fresh process: the quarantined attempt is still there, with its
    // reservation identity intact and the reservation still HELD.
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let unknowns = db.unknown_attempts().expect("listable");
    assert_eq!(unknowns.len(), 1);
    assert_eq!(unknowns[0].attempt_id, "fund:s1:1000");
    assert_eq!(unknowns[0].reservation_id.as_deref(), Some("resv-q"));
    assert_eq!(unknowns[0].state, AttemptState::OutcomeUnknown);
    assert_eq!(
        reservation_status(&path, "resv-q").as_deref(),
        Some("active")
    );
    // And it still blocks a new fund attempt for that swap.
    let mut retry = fund_intent("s1", Some("resv-new"));
    retry.attempt_id = "fund:s1:9999".to_string();
    assert!(matches!(
        db.begin_attempt(&retry).unwrap(),
        BeginAttemptAck::Blocked { .. }
    ));
}

#[test]
fn quarantine_stale_intents_promotes_crashed_inflight_attempts() {
    // A process that died between begin_attempt and resolve leaves an
    // 'intent' row. On restart that is BY DEFINITION an unknown outcome —
    // it must be promoted to quarantine, never dropped or retried.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    {
        let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
        db.begin_attempt(&fund_intent("s1", Some("resv-x")))
            .unwrap();
    }
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let promoted = db
        .quarantine_stale_intents("process restarted with attempt in flight", 5000)
        .expect("promotion acked");
    assert_eq!(promoted, 1);
    let unknowns = db.unknown_attempts().unwrap();
    assert_eq!(unknowns.len(), 1);
    assert_eq!(unknowns[0].state, AttemptState::OutcomeUnknown);
    // Idempotent: nothing left to promote.
    assert_eq!(
        db.quarantine_stale_intents("again", 6000).expect("acked"),
        0
    );
}

// ------------------------------------------------------- typed resolutions

#[test]
fn resolve_not_submitted_releases_the_reservation_atomically() {
    let (_dir, db, path) = open_db();
    reserve(&db, "resv-ns", 500);
    db.begin_attempt(&fund_intent("s1", Some("resv-ns")))
        .unwrap();

    assert_eq!(
        db.resolve_attempt(
            "fund:s1:1000",
            &AttemptResolution::NotSubmitted {
                detail: "connect refused before request".to_string(),
            },
            2000,
        )
        .expect("resolve acked"),
        ResolveAck::Resolved
    );
    assert_eq!(
        reservation_status(&path, "resv-ns").as_deref(),
        Some("released"),
        "a clean not-submitted resolution frees the held budget"
    );
    let attempt = db.get_attempt("fund:s1:1000").unwrap().unwrap();
    assert_eq!(attempt.state, AttemptState::NotSubmitted);
}

#[test]
fn resolve_not_submitted_rolls_back_when_the_release_write_fails() {
    // Write-only fault on the release half: attempt state must roll back
    // with it — never "not_submitted" with the reservation still held.
    let (_dir, db, path) = open_db();
    reserve(&db, "resv-rb", 500);
    db.begin_attempt(&fund_intent("s1", Some("resv-rb")))
        .unwrap();
    sabotage(
        &path,
        "CREATE TRIGGER sabotage_release BEFORE UPDATE ON spend_reservations \
         BEGIN SELECT RAISE(ABORT, 'sabotaged release'); END;",
    );
    let result = db.resolve_attempt(
        "fund:s1:1000",
        &AttemptResolution::NotSubmitted {
            detail: "x".to_string(),
        },
        2000,
    );
    assert!(result.is_err());
    assert_eq!(
        db.get_attempt("fund:s1:1000").unwrap().unwrap().state,
        AttemptState::Intent,
        "the attempt transition must roll back with the failed release"
    );
    assert_eq!(
        reservation_status(&path, "resv-rb").as_deref(),
        Some("active")
    );
}

#[test]
fn resolve_committed_fund_is_one_transaction_row_settle_receipt_attempt() {
    let (_dir, db, path) = open_db();
    db.insert_swap_new(&SwapRow::new("s1", "opening", 1_000_000, 6, 500))
        .unwrap();
    reserve(&db, "resv-c", 2500);
    db.begin_attempt(&fund_intent("s1", Some("resv-c")))
        .unwrap();

    assert_eq!(
        db.resolve_attempt(
            "fund:s1:1000",
            &AttemptResolution::CommittedFund {
                txid: "txid-abc".to_string(),
                actual_cost_sats: Some(2500),
            },
            3000,
        )
        .expect("resolve acked"),
        ResolveAck::Resolved
    );
    let row = db.get_swap("s1").unwrap();
    assert_eq!(row.channel_funding_txid.as_deref(), Some("txid-abc"));
    assert_eq!(row.opened_at, Some(3000));
    assert_eq!(
        reservation_status(&path, "resv-c").as_deref(),
        Some("spent")
    );
    assert_eq!(
        receipt_count(&path, "resv-c"),
        1,
        "exactly one receipt event"
    );
    assert_eq!(
        db.get_attempt("fund:s1:1000").unwrap().unwrap().state,
        AttemptState::Committed
    );
}

#[test]
fn resolve_committed_fund_rolls_back_everything_when_the_receipt_write_fails() {
    // THE supervisor-required atomicity proof: a write-only fault on the
    // receipt (spend_events INSERT; reads unaffected) must roll back the
    // row's funding txid, the reservation settle, AND the attempt state
    // together — all-or-nothing, acknowledged as Err, attempt still
    // reconcilable.
    let (_dir, db, path) = open_db();
    db.insert_swap_new(&SwapRow::new("s1", "opening", 1_000_000, 6, 500))
        .unwrap();
    reserve(&db, "resv-f", 2500);
    db.begin_attempt(&fund_intent("s1", Some("resv-f")))
        .unwrap();
    sabotage(
        &path,
        "CREATE TRIGGER sabotage_receipt BEFORE INSERT ON spend_events \
         BEGIN SELECT RAISE(ABORT, 'sabotaged receipt'); END;",
    );

    let result = db.resolve_attempt(
        "fund:s1:1000",
        &AttemptResolution::CommittedFund {
            txid: "txid-abc".to_string(),
            actual_cost_sats: Some(2500),
        },
        3000,
    );
    assert!(result.is_err(), "mid-compound fault must be acknowledged");
    let row = db.get_swap("s1").unwrap();
    assert_eq!(
        row.channel_funding_txid, None,
        "row funding txid must roll back with the failed receipt"
    );
    assert_eq!(
        reservation_status(&path, "resv-f").as_deref(),
        Some("active"),
        "the settle must roll back — fail toward HOLDING budget"
    );
    assert_eq!(receipt_count(&path, "resv-f"), 0);
    assert_eq!(
        db.get_attempt("fund:s1:1000").unwrap().unwrap().state,
        AttemptState::Intent,
        "the attempt stays unresolved so restart reconciliation retries it"
    );
}

#[test]
fn resolution_replay_is_exactly_once() {
    let (_dir, db, path) = open_db();
    db.insert_swap_new(&SwapRow::new("s1", "opening", 1_000_000, 6, 500))
        .unwrap();
    reserve(&db, "resv-r", 2500);
    db.begin_attempt(&fund_intent("s1", Some("resv-r")))
        .unwrap();
    let commit = AttemptResolution::CommittedFund {
        txid: "txid-abc".to_string(),
        actual_cost_sats: Some(2500),
    };
    assert_eq!(
        db.resolve_attempt("fund:s1:1000", &commit, 3000).unwrap(),
        ResolveAck::Resolved
    );
    // Replay (e.g. reconciliation running again after a crash between
    // resolve and its caller's bookkeeping): typed AlreadyResolved, and
    // NOTHING is written twice.
    assert_eq!(
        db.resolve_attempt("fund:s1:1000", &commit, 4000).unwrap(),
        ResolveAck::AlreadyResolved {
            state: AttemptState::Committed
        }
    );
    assert_eq!(receipt_count(&path, "resv-r"), 1, "no second receipt");
    assert_eq!(
        reservation_status(&path, "resv-r").as_deref(),
        Some("spent")
    );
    assert_eq!(
        db.get_swap("s1").unwrap().opened_at,
        Some(3000),
        "no re-stamp"
    );

    // Replaying a DIFFERENT resolution against a terminal attempt is
    // equally inert — a late NotSubmitted cannot release a settled spend.
    assert_eq!(
        db.resolve_attempt(
            "fund:s1:1000",
            &AttemptResolution::NotSubmitted {
                detail: "late confusion".to_string()
            },
            5000,
        )
        .unwrap(),
        ResolveAck::AlreadyResolved {
            state: AttemptState::Committed
        }
    );
    assert_eq!(
        reservation_status(&path, "resv-r").as_deref(),
        Some("spent")
    );
}

#[test]
fn resolve_unknown_attempt_id_is_typed_not_an_error() {
    let (_dir, db, _path) = open_db();
    assert_eq!(
        db.resolve_attempt("fund:ghost:1", &AttemptResolution::CommittedApply, 1000)
            .unwrap(),
        ResolveAck::UnknownAttempt
    );
}

#[test]
fn quarantined_attempt_can_still_be_resolved_exactly_once_later() {
    // The restart-reconciliation path: unknown -> committed (or
    // not_submitted) is a legal exactly-once transition.
    let (_dir, db, path) = open_db();
    db.insert_swap_new(&SwapRow::new("s1", "opening", 1_000_000, 6, 500))
        .unwrap();
    reserve(&db, "resv-u", 2500);
    db.begin_attempt(&fund_intent("s1", Some("resv-u")))
        .unwrap();
    db.resolve_attempt(
        "fund:s1:1000",
        &AttemptResolution::OutcomeUnknown {
            detail: "disconnect after submit".to_string(),
        },
        2000,
    )
    .unwrap();
    assert_eq!(
        reservation_status(&path, "resv-u").as_deref(),
        Some("active")
    );

    assert_eq!(
        db.resolve_attempt(
            "fund:s1:1000",
            &AttemptResolution::CommittedFund {
                txid: "txid-found".to_string(),
                actual_cost_sats: Some(2500),
            },
            9000,
        )
        .unwrap(),
        ResolveAck::Resolved
    );
    assert_eq!(
        reservation_status(&path, "resv-u").as_deref(),
        Some("spent")
    );
    assert_eq!(receipt_count(&path, "resv-u"), 1);
    // And quarantining an attempt twice is inert.
    assert_eq!(
        db.resolve_attempt(
            "fund:s1:1000",
            &AttemptResolution::OutcomeUnknown {
                detail: "late".to_string()
            },
            9500,
        )
        .unwrap(),
        ResolveAck::AlreadyResolved {
            state: AttemptState::Committed
        }
    );
}

// ===================================================================
// Kernel integration: quarantine semantics through the open/apply/
// reconcile flows (in-memory fakes; enforcement proven by mutation
// kills recorded in the stage report).
// ===================================================================

use common::{listing, participant, pubkey, FakeApi, FakeChain, FakeDb, FakePlanner};
use revops_lnplus::config::LnPlusConfig;
use revops_lnplus::open::{execute_swap_open, OpenExecParams};
use revops_lnplus::ports::{ChannelInfo, FundChannelOutcome};
use revops_lnplus::reconcile::reconcile_unknown_attempts;
use revops_lnplus::types::{MySwapEntry, MySwaps};

fn open_params() -> OpenExecParams {
    OpenExecParams {
        estimated_cost_sats: 2000,
        effective_budget_sats: None,
        budget_since_timestamp: None,
    }
}

fn opening_row(sid: &str, peer: &str) -> SwapRow {
    SwapRow::new(sid, "opening", 1_000_000, 6, 0)
        .with_outbound_peer(peer)
        .with_deadline_at(200_000)
}

#[test]
fn fund_outcome_unknown_quarantines_and_retains_the_reservation() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    let row = opening_row("s1", &peer);
    db.insert(row.clone());
    *chain.fund_channel_result.borrow_mut() = Ok(FundChannelOutcome::OutcomeUnknown {
        detail: "timeout waiting for fundchannel response".to_string(),
    });

    let opened = execute_swap_open(&row, None, &open_params(), &db, &api, &chain, &logger, 1000)
        .expect("pass survives — quarantine is not an abort");
    assert!(!opened);

    let unknowns = db.unknown_attempts().unwrap();
    assert_eq!(unknowns.len(), 1, "the attempt must be quarantined");
    let rid = unknowns[0]
        .reservation_id
        .clone()
        .expect("bound reservation");
    let reservations = db.reservations.borrow();
    let record = reservations.get(&rid).expect("reservation exists");
    assert!(
        record.active && !record.spent,
        "an unknown outcome must RETAIN the reservation — never release, never settle"
    );
    drop(reservations);
    let after = db.get_swap("s1").unwrap();
    assert_eq!(
        after.status, "opening",
        "an unknown outcome must never terminalize the row (funds may be committed)"
    );
    assert!(
        db.get_breaker().unwrap().is_none(),
        "an unknown outcome must not trip the deadline-miss terminalization"
    );
}

#[test]
fn fund_success_without_txid_is_unknown_not_a_clean_failure() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    let row = opening_row("s1", &peer);
    db.insert(row.clone());
    *chain.fund_channel_result.borrow_mut() = Ok(FundChannelOutcome::Funded(
        revops_lnplus::ports::FundChannelResult { txid: None },
    ));

    let opened =
        execute_swap_open(&row, None, &open_params(), &db, &api, &chain, &logger, 1000).unwrap();
    assert!(!opened);
    assert_eq!(db.unknown_attempts().unwrap().len(), 1);
    let reservations = db.reservations.borrow();
    assert!(
        reservations.values().all(|r| r.active),
        "no release on a success-shaped response missing its txid"
    );
}

#[test]
fn quarantined_fund_attempt_blocks_the_next_open_and_releases_the_fresh_reservation() {
    let db = FakeDb::new();
    let api = FakeApi::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    let row = opening_row("s1", &peer);
    db.insert(row.clone());
    *chain.fund_channel_result.borrow_mut() = Ok(FundChannelOutcome::OutcomeUnknown {
        detail: "boom".to_string(),
    });
    execute_swap_open(&row, None, &open_params(), &db, &api, &chain, &logger, 1000).unwrap();
    assert_eq!(chain.fund_channel_calls.borrow().len(), 1);

    // Next pass: the fake would now happily fund — but the quarantine
    // must block BEFORE the wire.
    *chain.fund_channel_result.borrow_mut() = Ok(FundChannelOutcome::Funded(
        revops_lnplus::ports::FundChannelResult {
            txid: Some("txid2".to_string()),
        },
    ));
    let opened =
        execute_swap_open(&row, None, &open_params(), &db, &api, &chain, &logger, 2000).unwrap();
    assert!(!opened);
    assert_eq!(
        chain.fund_channel_calls.borrow().len(),
        1,
        "NO second fundchannel while an attempt is quarantined (no auto-resubmit)"
    );
    // The second pass's fresh reservation must not stay held.
    let reservations = db.reservations.borrow();
    let second: Vec<_> = reservations
        .iter()
        .filter(|(rid, _)| rid.contains("-2000"))
        .collect();
    assert!(!second.is_empty(), "second pass reserved before the gate");
    assert!(
        second.iter().all(|(_, r)| !r.active),
        "the blocked pass must release its fresh reservation"
    );
}

#[test]
fn reconciliation_resolves_fund_unknown_from_chain_evidence_exactly_once() {
    let db = FakeDb::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    db.insert(opening_row("s1", &peer));
    reserve_fake(&db, "resv-rec", 2000);
    db.begin_attempt(&AttemptIntent {
        attempt_id: "fund:s1:1000".to_string(),
        swap_id: "s1".to_string(),
        kind: AttemptKind::Fund,
        reservation_id: Some("resv-rec".to_string()),
        peer_id: Some(peer.clone()),
        amount_sats: Some(1_000_000),
        created_at: 1000,
    })
    .unwrap();
    db.resolve_attempt(
        "fund:s1:1000",
        &AttemptResolution::OutcomeUnknown {
            detail: "crash".to_string(),
        },
        1500,
    )
    .unwrap();

    // Leg 1: RPC failure — stays quarantined, nothing released.
    *chain.list_peer_channels_fails.borrow_mut() = true;
    reconcile_unknown_attempts(None, &db, &chain, &logger, 2000).unwrap();
    assert_eq!(db.unknown_attempts().unwrap().len(), 1);
    assert!(db.reservations.borrow().get("resv-rec").unwrap().active);

    // Leg 2: chain shows the matching channel — resolved committed.
    *chain.list_peer_channels_fails.borrow_mut() = false;
    chain.channels.borrow_mut().push(ChannelInfo {
        peer_id: peer.clone(),
        state: "CHANNELD_AWAITING_LOCKIN".to_string(),
        total_msat: 1_000_000_000,
        to_us_msat: 1_000_000_000,
        funding_txid: Some("txid-found".to_string()),
    });
    reconcile_unknown_attempts(None, &db, &chain, &logger, 3000).unwrap();
    assert!(db.unknown_attempts().unwrap().is_empty());
    let row = db.get_swap("s1").unwrap();
    assert_eq!(row.channel_funding_txid.as_deref(), Some("txid-found"));
    {
        let reservations = db.reservations.borrow();
        let r = reservations.get("resv-rec").unwrap();
        assert!(r.spent && !r.active, "settled exactly once");
    }

    // Leg 3: replay — a second reconciliation run writes nothing.
    reconcile_unknown_attempts(None, &db, &chain, &logger, 4000).unwrap();
    assert_eq!(
        db.get_swap("s1").unwrap().opened_at,
        Some(3000),
        "no re-stamp"
    );
}

#[test]
fn reconciliation_releases_fund_unknown_on_genuine_chain_absence() {
    let db = FakeDb::new();
    let chain = FakeChain::new(); // genuine empty channel list
    let logger = FakeLogger::new();
    let peer = pubkey(1);
    db.insert(opening_row("s1", &peer));
    reserve_fake(&db, "resv-abs", 2000);
    db.begin_attempt(&AttemptIntent {
        attempt_id: "fund:s1:1000".to_string(),
        swap_id: "s1".to_string(),
        kind: AttemptKind::Fund,
        reservation_id: Some("resv-abs".to_string()),
        peer_id: Some(peer),
        amount_sats: Some(1_000_000),
        created_at: 1000,
    })
    .unwrap();
    db.resolve_attempt(
        "fund:s1:1000",
        &AttemptResolution::OutcomeUnknown {
            detail: "crash".to_string(),
        },
        1500,
    )
    .unwrap();

    reconcile_unknown_attempts(None, &db, &chain, &logger, 2000).unwrap();
    assert!(db.unknown_attempts().unwrap().is_empty());
    assert!(
        !db.reservations.borrow().get("resv-abs").unwrap().active,
        "a genuine chain answer with no channel releases the hold"
    );
}

#[test]
fn apply_outcome_unknown_keeps_the_row_applied_and_quarantines() {
    let a = pubkey(1);
    let b = pubkey(2);
    let mut pa = participant("A", &a);
    pa.positive_ratings_count = 50;
    let mut pb = participant("B", &b);
    pb.positive_ratings_count = 50;
    let planner = FakePlanner::new();
    planner.set_ev(&a, 50_000.0);
    planner.set_ev(&b, 50_000.0);
    let db = FakeDb::new();
    let api = FakeApi::new();
    *api.create_application_result.borrow_mut() = Err(
        revops_lnplus::error::LnPlusError::unknown_outcome("response timeout after submit"),
    );
    let logger = FakeLogger::new();

    let inputs = revops_lnplus::evaluator::CycleInputs {
        cfg: apply_cfg(),
        preflight: revops_lnplus::evaluator::CyclePreflight {
            breaker_tripped: None,
            has_inflight: false,
            reconcile_ok: true,
        },
        opening_feerate_perkw: Some(100),
        swaps: vec![listing("1", vec![pa, pb])],
        our_id: None,
        frozen_peers_with_channels: Some(Default::default()),
        best_regular_ev: 0.0,
        confirmed_unreserved_sats: 100_000_000,
        capex_budget_sats: None,
        now: 1000,
    };
    let out =
        revops_lnplus::evaluator::run_cycle(inputs, &db, &api, None, &planner, &logger).unwrap();
    assert!(
        !out.applied,
        "an unknown outcome must not be reported applied"
    );
    let row = db.get_swap("1").expect("intent row");
    assert_eq!(
        row.status, "applied",
        "the row must NOT be falsified to failed — the application may be live on LN+"
    );
    assert_eq!(db.unknown_attempts().unwrap().len(), 1);
}

#[test]
fn reconciliation_resolves_apply_unknown_from_lnplus_listing() {
    let db = FakeDb::new();
    let chain = FakeChain::new();
    let logger = FakeLogger::new();
    db.insert(SwapRow::new("s1", "applied", 1_000_000, 6, 500));
    db.begin_attempt(&AttemptIntent {
        attempt_id: "apply:s1:1000".to_string(),
        swap_id: "s1".to_string(),
        kind: AttemptKind::Apply,
        reservation_id: None,
        peer_id: None,
        amount_sats: Some(1_000_000),
        created_at: 1000,
    })
    .unwrap();
    db.resolve_attempt(
        "apply:s1:1000",
        &AttemptResolution::OutcomeUnknown {
            detail: "crash".to_string(),
        },
        1500,
    )
    .unwrap();

    // No LN+ evidence (outage pass): stays quarantined.
    reconcile_unknown_attempts(None, &db, &chain, &logger, 2000).unwrap();
    assert_eq!(db.unknown_attempts().unwrap().len(), 1);

    // LN+ lists the swap as pending: committed.
    let my = MySwaps {
        pending: vec![MySwapEntry {
            id: "s1".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    };
    reconcile_unknown_attempts(Some(&my), &db, &chain, &logger, 3000).unwrap();
    assert!(db.unknown_attempts().unwrap().is_empty());
    assert_eq!(db.get_swap("s1").unwrap().status, "applied");

    // Control: a genuinely absent swap resolves not-submitted + failed.
    db.insert(SwapRow::new("s2", "applied", 1_000_000, 6, 500));
    db.begin_attempt(&AttemptIntent {
        attempt_id: "apply:s2:1000".to_string(),
        swap_id: "s2".to_string(),
        kind: AttemptKind::Apply,
        reservation_id: None,
        peer_id: None,
        amount_sats: Some(1_000_000),
        created_at: 1000,
    })
    .unwrap();
    db.resolve_attempt(
        "apply:s2:1000",
        &AttemptResolution::OutcomeUnknown {
            detail: "crash".to_string(),
        },
        1500,
    )
    .unwrap();
    reconcile_unknown_attempts(Some(&my), &db, &chain, &logger, 4000).unwrap();
    assert!(db.unknown_attempts().unwrap().is_empty());
    assert_eq!(db.get_swap("s2").unwrap().status, "failed");
}

fn apply_cfg() -> LnPlusConfig {
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

fn reserve_fake(db: &FakeDb, rid: &str, sats: i64) {
    let granted = db
        .reserve_spend(&ReserveSpendRequest {
            reservation_id: rid.to_string(),
            amount_sats: sats,
            category: "channel_open",
            subcategory: "lnplus_swap",
            metadata: Default::default(),
            effective_budget_sats: None,
            since_timestamp: None,
        })
        .unwrap();
    assert!(granted);
}
