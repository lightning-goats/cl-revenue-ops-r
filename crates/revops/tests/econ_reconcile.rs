//! Task 66 slice 4: `revenue-econ-reconcile` assembly against a REAL
//! temp econ ledger and the REAL fixture production DB — the exact
//! production path `main.rs` registers. The ledger is the Rust-owned
//! dry-run file; production `econ_ledger.db` is never involved.

use revops::rpc_econ_reconcile::{econ_reconcile_response, EconReconcileSources};
use revops_db::actor::spawn_read_only;
use revops_econ::ledger::EconLedger;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const NOW: i64 = 1_800_000_000;

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db")
}

fn append_reserved(ledger: &EconLedger, key: &str, at: i64, reserved_msat: i64) {
    let amounts: BTreeMap<String, i64> = [("reserved_msat".to_string(), reserved_msat)].into();
    ledger
        .append(
            "budget_reserved",
            &key.chars().take(16).collect::<String>(),
            key,
            "spend-test",
            at,
            &amounts,
            &json!({}),
        )
        .expect("seed ledger event");
}

fn sources<'a>(
    db: Option<&'a revops_db::actor::DbHandle>,
    ledger_path: Option<PathBuf>,
    apply: bool,
) -> EconReconcileSources<'a> {
    EconReconcileSources {
        enabled: Ok(true),
        db,
        ledger_path,
        apply,
        stale_after_seconds: 3600,
        now: NOW,
    }
}

/// Guard arms: disabled (exact hint), failed enabled-read (outer error
/// arm, never a fabricated "disabled"), missing ledger path or db (the
/// unavailable arm).
#[tokio::test]
async fn guard_arms_match_python_shapes() {
    let disabled = econ_reconcile_response(EconReconcileSources {
        enabled: Ok(false),
        ..sources(None, None, false)
    })
    .await;
    assert_eq!(
        disabled,
        json!({"enabled": false, "hint": "revenue-config set econ_shadow_enabled true"})
    );

    let failed = econ_reconcile_response(EconReconcileSources {
        enabled: Err("config read failed".to_string()),
        ..sources(None, None, false)
    })
    .await;
    assert_eq!(
        failed,
        json!({"enabled": true, "error": "config read failed"})
    );

    let unavailable = econ_reconcile_response(sources(None, None, false)).await;
    assert_eq!(
        unavailable,
        json!({"enabled": true, "error": "ledger or database unavailable"})
    );
}

/// A ledger path that cannot open (a directory) takes the SAME
/// unavailable arm as a missing one — py `ledger_for_reconciliation()`
/// returns None on open failure, and the handler must not leak an
/// internal error string instead.
#[tokio::test]
async fn unopenable_ledger_is_the_unavailable_arm() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("prod.db");
    std::fs::copy(fixture_path(), &db_path).unwrap();
    let handle = spawn_read_only(&db_path).await.unwrap();
    let v = econ_reconcile_response(sources(
        Some(&handle),
        Some(dir.path().to_path_buf()), // a directory, not a db file
        false,
    ))
    .await;
    assert_eq!(
        v,
        json!({"enabled": true, "error": "ledger or database unavailable"})
    );
}

/// The completeness bridge end-to-end: a ledgered `fee-cycle-<ts>` intent
/// plus a `fee_changes` row at the same timestamp must reconcile to
/// `complete: true` — this is the row-shape seam between the DB
/// timestamps and `fee_intent_completeness`, which reads rows by their
/// "timestamp" key.
#[tokio::test]
async fn completeness_bridges_db_timestamps_into_a_complete_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("prod.db");
    std::fs::copy(fixture_path(), &db_path).unwrap();
    let intent_ts = NOW - 50;
    let conn = Connection::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO fee_changes (channel_id, peer_id, old_fee_ppm, new_fee_ppm, timestamp) \
         VALUES ('1x1x0', ?1, 100, 110, ?2)",
        rusqlite::params!["a".repeat(66), intent_ts],
    )
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&db_path).await.unwrap();

    let ledger_path = dir.path().join("econ_ledger_dryrun.db");
    let ledger = EconLedger::open(&ledger_path).unwrap();
    ledger
        .append(
            "intent_proposed",
            "intent-1",
            "intent-key-1",
            &format!("fee-cycle-{intent_ts}"),
            intent_ts,
            &BTreeMap::new(),
            &json!({}),
        )
        .expect("seed intent");

    let v = econ_reconcile_response(sources(Some(&handle), Some(ledger_path), false)).await;
    let completeness = &v["fee_intent_completeness"];
    assert_eq!(completeness["status"], "ok", "{v}");
    assert_eq!(completeness["cycles_checked"], 1);
    assert_eq!(completeness["complete"], true);
}

/// The full dry-run: one ledger key matched by an active reservation of
/// the same amount, one ledger key with no DB row at all (a resolvable
/// db_missing divergence). `apply` stays off: the "applied" key must be
/// ABSENT and the ledger must not gain correction events.
#[tokio::test]
async fn dry_run_reports_match_and_divergence_without_writing() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("prod.db");
    std::fs::copy(fixture_path(), &db_path).unwrap();
    let conn = Connection::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO spend_reservations (reservation_id, category, reserved_sats, reserved_at, status) \
         VALUES ('key-matched', 'rebalance', 3, ?1, 'active')",
        rusqlite::params![NOW - 100],
    )
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&db_path).await.unwrap();

    let ledger_path = dir.path().join("econ_ledger_dryrun.db");
    let ledger = EconLedger::open(&ledger_path).unwrap();
    append_reserved(&ledger, "key-matched", NOW - 200, 3_000);
    append_reserved(&ledger, "key-ledger-only", NOW - 200, 5_000);
    let events_before = ledger.events(0).unwrap().len();

    let v = econ_reconcile_response(sources(Some(&handle), Some(ledger_path.clone()), false)).await;
    assert_eq!(v["enabled"], true, "{v}");
    assert_eq!(v["checked"], 2);
    assert_eq!(v["matched"], 1);
    let divergences = v["divergences"].as_array().unwrap();
    assert_eq!(divergences.len(), 1);
    assert_eq!(divergences[0]["kind"], "db_missing");
    assert_eq!(divergences[0]["key"], "key-ledger-only");
    assert_eq!(divergences[0]["ledger_reserved_msat"], 5_000);
    assert_eq!(divergences[0]["db_status"], Value::Null);
    // No intent events seeded: the completeness cross-check answers its
    // honest no-data status, not an error and not a fabricated pass.
    assert_eq!(v["fee_intent_completeness"]["status"], "no_intent_data");
    assert!(v.get("applied").is_none(), "dry-run must omit applied");
    assert_eq!(
        ledger.events(0).unwrap().len(),
        events_before,
        "dry-run must not append ledger events"
    );
}

/// apply=true appends the correction events (count in "applied") and a
/// follow-up dry-run reports the ledger clean.
#[tokio::test]
async fn apply_appends_corrections_then_a_rerun_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("prod.db");
    std::fs::copy(fixture_path(), &db_path).unwrap();
    let handle = spawn_read_only(&db_path).await.unwrap();

    let ledger_path = dir.path().join("econ_ledger_dryrun.db");
    let ledger = EconLedger::open(&ledger_path).unwrap();
    append_reserved(&ledger, "key-ledger-only", NOW - 200, 5_000);
    let events_before = ledger.events(0).unwrap().len();

    let applied =
        econ_reconcile_response(sources(Some(&handle), Some(ledger_path.clone()), true)).await;
    assert_eq!(applied["applied"], 1, "{applied}");
    assert!(
        ledger.events(0).unwrap().len() > events_before,
        "apply must append correction events to the RUST ledger"
    );

    let rerun = econ_reconcile_response(sources(Some(&handle), Some(ledger_path), false)).await;
    assert_eq!(rerun["divergences"].as_array().unwrap().len(), 0, "{rerun}");
}
