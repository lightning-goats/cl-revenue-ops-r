//! `SqliteLnPlusDb` — every test opens a fresh `tempfile::TempDir`-backed
//! sqlite file. HARD RULE: no test in this crate touches a real database;
//! a throwaway temp file is not the production `revenue_ops.db` and is
//! discarded when the `TempDir` drops.

mod common;

use common::FakeLogger;
use revops_lnplus::breaker::{BreakerCause, BreakerState};
use revops_lnplus::db_types::{SwapPatch, SwapRow};
use revops_lnplus::ports::{LnPlusDb, PlannerActionRequest, ReserveSpendRequest};
use revops_lnplus::sqlite_db::{ensure_schema, SqliteLnPlusDb};
use revops_lnplus::types::Rating;
use std::collections::BTreeMap;

fn open_db() -> (tempfile::TempDir, SqliteLnPlusDb) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lnplus.db");
    let db = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).expect("open db");
    (dir, db)
}

// ------------------------------------------------------------------ schema

#[test]
fn schema_init_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    // Opening twice must not error (CREATE TABLE/INDEX IF NOT EXISTS).
    let _first = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
    let _second = SqliteLnPlusDb::open(&path, Box::new(FakeLogger::new())).unwrap();
}

#[test]
fn schema_creates_the_exact_python_table_and_column_set() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lnplus.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    ensure_schema(&conn).unwrap();

    let mut stmt = conn
        .prepare("SELECT name FROM pragma_table_info('lnplus_swaps') ORDER BY cid")
        .unwrap();
    let cols: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        cols,
        vec![
            "swap_id",
            "status",
            "capacity_sats",
            "duration_months",
            "ends_at",
            "outbound_peer",
            "incoming_peer",
            "our_identifier",
            "applied_at",
            "opened_at",
            "completed_at",
            "channel_funding_txid",
            "deadline_at",
            "planner_action_id",
            "outcome",
            "metadata_json",
            "tag_added",
            "incoming_tag_added",
        ],
        "must match database.py:1359-1409 exactly, including the unused completed_at column"
    );

    let mut stmt2 = conn
        .prepare("SELECT name FROM pragma_table_info('lnplus_peers') ORDER BY cid")
        .unwrap();
    let peer_cols: Vec<String> = stmt2
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(
        peer_cols,
        vec![
            "pubkey",
            "swaps_count",
            "ratings_given_positive",
            "ratings_given_negative",
            "defections",
            "last_swap_at",
        ]
    );
}

// ------------------------------------------------------------- swap ledger

#[test]
fn record_and_get_swap_roundtrips() {
    let (_dir, db) = open_db();
    let mut row = SwapRow::new("s1", "applied", 500_000, 6, 1000)
        .with_outbound_peer("02aa")
        .with_incoming_peer("02bb")
        .with_our_identifier("A")
        .with_planner_action_id(7);
    let mut meta = BTreeMap::new();
    meta.insert("swap_id".to_string(), "s1".to_string());
    row.metadata = Some(meta);

    db.record_swap(&row);
    let fetched = db.get_swap("s1").expect("row present");

    assert_eq!(fetched.status, "applied");
    assert_eq!(fetched.capacity_sats, 500_000);
    assert_eq!(fetched.outbound_peer.as_deref(), Some("02aa"));
    assert_eq!(fetched.incoming_peer.as_deref(), Some("02bb"));
    assert_eq!(fetched.our_identifier.as_deref(), Some("A"));
    assert_eq!(fetched.planner_action_id, Some(7));
    assert_eq!(fetched.applied_at, 1000);
    assert_eq!(
        fetched.metadata.unwrap().get("swap_id").cloned(),
        Some("s1".to_string())
    );
}

#[test]
fn get_swap_missing_returns_none() {
    // Control for the roundtrip test: a swap that was never recorded must
    // come back `None`, not a zeroed/default row.
    let (_dir, db) = open_db();
    assert!(db.get_swap("does-not-exist").is_none());
}

#[test]
fn record_swap_is_insert_or_replace_idempotent_on_swap_id() {
    let (_dir, db) = open_db();
    let row1 = SwapRow::new("s1", "applied", 100, 1, 1000);
    db.record_swap(&row1);
    let row2 = SwapRow::new("s1", "applied", 999, 1, 1000);
    db.record_swap(&row2);

    let fetched = db.get_swap("s1").unwrap();
    assert_eq!(fetched.capacity_sats, 999, "second record_swap replaces");
}

#[test]
fn update_swap_only_touches_patched_columns() {
    let (_dir, db) = open_db();
    let row = SwapRow::new("s1", "applied", 500_000, 6, 1000).with_outbound_peer("02aa");
    db.record_swap(&row);

    db.update_swap(
        "s1",
        &SwapPatch::default().status("opening").opened_at(2000),
    );

    let fetched = db.get_swap("s1").unwrap();
    assert_eq!(fetched.status, "opening");
    assert_eq!(fetched.opened_at, Some(2000));
    assert_eq!(
        fetched.outbound_peer.as_deref(),
        Some("02aa"),
        "untouched column must survive the patch"
    );
}

#[test]
fn update_swap_with_no_fields_set_is_a_no_op() {
    let (_dir, db) = open_db();
    let row = SwapRow::new("s1", "applied", 500_000, 6, 1000);
    db.record_swap(&row);
    db.update_swap("s1", &SwapPatch::default());
    assert_eq!(db.get_swap("s1").unwrap().status, "applied");
}

#[test]
fn update_swap_persists_tag_added_booleans() {
    let (_dir, db) = open_db();
    db.record_swap(&SwapRow::new("s1", "active", 1, 1, 1000));
    db.update_swap(
        "s1",
        &SwapPatch::default()
            .tag_added(true)
            .incoming_tag_added(false),
    );
    let fetched = db.get_swap("s1").unwrap();
    assert_eq!(fetched.tag_added, Some(true));
    assert_eq!(fetched.incoming_tag_added, Some(false));
}

#[test]
fn get_swaps_by_status_filters_and_orders_by_applied_at() {
    let (_dir, db) = open_db();
    db.record_swap(&SwapRow::new("s1", "applied", 1, 1, 300));
    db.record_swap(&SwapRow::new("s2", "applied", 1, 1, 100));
    db.record_swap(&SwapRow::new("s3", "opened", 1, 1, 200));

    let applied = db.get_swaps_by_status(&["applied"]);
    assert_eq!(
        applied
            .iter()
            .map(|r| r.swap_id.as_str())
            .collect::<Vec<_>>(),
        vec!["s2", "s1"],
        "ordered by applied_at ascending"
    );
}

#[test]
fn inflight_swaps_uses_the_default_trait_status_set() {
    let (_dir, db) = open_db();
    db.record_swap(&SwapRow::new("s1", "applied", 1, 1, 1));
    db.record_swap(&SwapRow::new("s2", "ended", 1, 1, 1));
    let inflight = db.inflight_swaps();
    assert_eq!(inflight.len(), 1);
    assert_eq!(inflight[0].swap_id, "s1");
}

#[test]
fn prune_terminal_deletes_old_terminal_rows_but_keeps_recent_ones() {
    let (_dir, db) = open_db();
    let now = 1_000_000i64;
    let old_cutoff = now - 200 * 86_400;
    db.record_swap(&SwapRow::new("old", "ended", 1, 1, old_cutoff - 10));
    db.record_swap(&SwapRow::new("recent", "ended", 1, 1, now - 10));
    db.record_swap(&SwapRow::new("inflight", "applied", 1, 1, old_cutoff - 10));

    let pruned = db.prune_terminal(180, now);

    assert_eq!(pruned, 1);
    assert!(db.get_swap("old").is_none());
    assert!(
        db.get_swap("recent").is_some(),
        "a recent terminal row must survive pruning"
    );
    assert!(
        db.get_swap("inflight").is_some(),
        "a non-terminal row must never be pruned regardless of age"
    );
}

// ---------------------------------------------------------- peer reputation

#[test]
fn bump_peer_creates_then_accumulates() {
    let (_dir, db) = open_db();
    assert!(db.get_peer("02aa").is_none());

    db.bump_peer("02aa", false, Some(Rating::Positive));
    let p1 = db.get_peer("02aa").unwrap();
    assert_eq!(p1.swaps_count, 1);
    assert_eq!(p1.ratings_given_positive, 1);
    assert_eq!(p1.defections, 0);

    db.bump_peer("02aa", true, Some(Rating::Negative));
    let p2 = db.get_peer("02aa").unwrap();
    assert_eq!(p2.swaps_count, 2, "count accumulates across calls");
    assert_eq!(
        p2.ratings_given_positive, 1,
        "prior positive rating preserved"
    );
    assert_eq!(p2.ratings_given_negative, 1);
    assert_eq!(p2.defections, 1);
}

// ---------------------------------------------------------- config overrides

#[test]
fn config_override_set_get_delete_roundtrip() {
    let (_dir, db) = open_db();
    assert_eq!(db.get_config_override("k"), None);
    db.set_config_override("k", "v1");
    assert_eq!(db.get_config_override("k").as_deref(), Some("v1"));
    db.set_config_override("k", "v2");
    assert_eq!(
        db.get_config_override("k").as_deref(),
        Some("v2"),
        "second set overwrites"
    );
    db.delete_config_override("k");
    assert_eq!(db.get_config_override("k"), None);
}

// ------------------------------------------------------------------ breaker

#[test]
fn breaker_set_get_clear_roundtrips_the_structured_cause() {
    let (_dir, db) = open_db();
    assert!(db.get_breaker().is_none());

    let state = BreakerState {
        tripped_at: 555,
        cause: BreakerCause::MissedOpenDeadline {
            swap_id: "s1".to_string(),
        },
    };
    db.set_breaker(&state);
    let fetched = db.get_breaker().expect("breaker present");
    assert_eq!(fetched, state);

    db.clear_breaker();
    assert!(db.get_breaker().is_none());
}

#[test]
fn breaker_roundtrips_every_cause_variant() {
    let (_dir, db) = open_db();
    let causes = vec![
        BreakerCause::OpeningGhostNoLocalRecord {
            swap_id: "1".into(),
        },
        BreakerCause::PendingGhostNoLocalRecord {
            swap_id: "2".into(),
        },
        BreakerCause::LocalRowDivergentFromRemote {
            swap_id: "3".into(),
            detail: "d".into(),
        },
        BreakerCause::AmbiguousFundedChannelDivergence {
            swap_id: "4".into(),
            detail: "d2".into(),
        },
        BreakerCause::LnPlusOutage {
            detail: "timeout".into(),
        },
    ];
    for cause in causes {
        let state = BreakerState {
            tripped_at: 1,
            cause: cause.clone(),
        };
        db.set_breaker(&state);
        assert_eq!(db.get_breaker().unwrap().cause, cause);
        db.clear_breaker();
    }
}

#[test]
fn breaker_read_fails_open_on_foreign_or_malformed_value() {
    // A Python-shaped plain-string breaker value (or any garbage) must
    // read back as "no breaker tripped", never panic or misparse.
    let (_dir, db) = open_db();
    db.set_config_override(
        revops_lnplus::breaker::BREAKER_KEY,
        "circuit breaker tripped: swap 42 ghost",
    );
    assert!(
        db.get_breaker().is_none(),
        "a foreign-format value must fail open, not panic"
    );
}

// ------------------------------------------------------------ planner actions

#[test]
fn record_and_update_planner_action_persists_status_and_completed_at() {
    let (dir, db) = open_db();
    let id = db.record_planner_action(&PlannerActionRequest {
        action_type: "swap_apply",
        peer_id: "02aa".to_string(),
        amount_sats: Some(500_000),
        estimated_cost_sats: Some(2500),
        reason: "test".to_string(),
        metadata: None,
    });
    assert!(id > 0);

    db.update_planner_action(id, "completed");

    let conn = rusqlite::Connection::open(dir.path().join("lnplus.db")).unwrap();
    let (status, completed_at): (String, Option<i64>) = conn
        .query_row(
            "SELECT status, completed_at FROM planner_actions WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "completed");
    assert!(
        completed_at.is_some(),
        "a terminal status must stamp completed_at"
    );
}

#[test]
fn update_planner_action_to_a_non_terminal_status_leaves_completed_at_null() {
    // Control for the test above: only completed/failed stamp completed_at.
    let (dir, db) = open_db();
    let id = db.record_planner_action(&PlannerActionRequest {
        action_type: "swap_apply",
        peer_id: "02aa".to_string(),
        amount_sats: None,
        estimated_cost_sats: None,
        reason: "test".to_string(),
        metadata: None,
    });
    db.update_planner_action(id, "recommended");

    let conn = rusqlite::Connection::open(dir.path().join("lnplus.db")).unwrap();
    let completed_at: Option<i64> = conn
        .query_row(
            "SELECT completed_at FROM planner_actions WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(completed_at.is_none());
}

// -------------------------------------------------------------- budget rail

#[test]
fn reserve_spend_release_and_mark_spent_roundtrip_via_composed_budgetdb() {
    let (_dir, db) = open_db();
    let req = ReserveSpendRequest {
        reservation_id: "resv-1".to_string(),
        amount_sats: 2500,
        category: "channel_open",
        subcategory: "lnplus_swap",
        metadata: BTreeMap::new(),
        effective_budget_sats: None,
        since_timestamp: None,
    };
    let granted = db.reserve_spend(&req).expect("reserve_spend call succeeds");
    assert!(
        granted,
        "best-effort (no budget cap) reservation must grant"
    );

    let settled = db
        .mark_spend_reservation_spent("resv-1", 2500, "lnplus_swaps")
        .expect("settle call succeeds");
    assert!(settled);
}

#[test]
fn reserve_spend_refuses_when_over_the_effective_budget() {
    let (_dir, db) = open_db();
    let req = ReserveSpendRequest {
        reservation_id: "resv-2".to_string(),
        amount_sats: 10_000,
        category: "channel_open",
        subcategory: "lnplus_swap",
        metadata: BTreeMap::new(),
        effective_budget_sats: Some(1_000), // cap far below the request
        since_timestamp: None,
    };
    let granted = db.reserve_spend(&req).unwrap();
    assert!(
        !granted,
        "a request over the effective budget must be refused"
    );
}

#[test]
fn release_spend_reservation_smoke() {
    let (_dir, db) = open_db();
    let req = ReserveSpendRequest {
        reservation_id: "resv-3".to_string(),
        amount_sats: 100,
        category: "channel_open",
        subcategory: "lnplus_swap",
        metadata: BTreeMap::new(),
        effective_budget_sats: None,
        since_timestamp: None,
    };
    assert!(db.reserve_spend(&req).unwrap());
    assert!(db.release_spend_reservation("resv-3").is_ok());
}
