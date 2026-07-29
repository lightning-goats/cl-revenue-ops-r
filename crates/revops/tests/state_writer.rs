//! Task 65 slice 2: the ProductionStateWriter capability front -- the
//! six-way ack vocabulary and the publish/wake ordering rails, over a
//! REAL temp-schema writer actor.

use revops::state_writer::{ProductionStateWriter, StateWriteAck};
use revops_db::state_writer::{spawn_state_writer, BudgetTransition, PeerPolicyWrite};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

fn python_schema(path: &PathBuf) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE peer_policies (
            peer_id TEXT PRIMARY KEY,
            strategy TEXT NOT NULL DEFAULT 'dynamic',
            rebalance_mode TEXT NOT NULL DEFAULT 'enabled',
            fee_ppm_target INTEGER,
            tags TEXT,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE hot_channel_protection_overrides (
            peer_id TEXT PRIMARY KEY,
            added_at INTEGER NOT NULL,
            note TEXT,
            min_depletion_trigger_pct REAL
        );
        CREATE TABLE config_overrides (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            updated_at INTEGER NOT NULL
        );
        CREATE TABLE budget_reservations (
            reservation_id TEXT PRIMARY KEY,
            reserved_sats INTEGER NOT NULL,
            reserved_at INTEGER NOT NULL,
            job_channel_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active'
        );
        "#,
    )
    .unwrap();
}

async fn fixture() -> (tempfile::TempDir, PathBuf, ProductionStateWriter) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("revenue_ops.db");
    python_schema(&path);
    let handle = spawn_state_writer(&path).await.unwrap();
    let writer = ProductionStateWriter::assemble(handle);
    (dir, path, writer)
}

/// Applied / AlreadyTerminal / Denied arms over the real actor, with
/// stable codes.
#[tokio::test]
async fn ack_arms_map_from_real_actor_outcomes() {
    let (_d, path, writer) = fixture().await;

    // Applied carries the committed version.
    match writer.set_config_override("k".into(), "v".into()).await {
        StateWriteAck::Applied(version) => assert_eq!(version, 1),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        writer
            .set_config_override("k".into(), "v2".into())
            .await
            .code(),
        "applied"
    );

    // AlreadyTerminal from a guarded budget transition.
    Connection::open(&path)
        .unwrap()
        .execute_batch("INSERT INTO budget_reservations VALUES ('r1', 500, 1, 'c', 'released');")
        .unwrap();
    let ack = writer.mark_budget_spent("r1".into(), 400).await;
    assert!(matches!(ack, StateWriteAck::AlreadyTerminal), "{ack:?}");
    assert_eq!(ack.code(), "already_terminal");

    // Denied from the batch bound (refused whole).
    let oversized: Vec<PeerPolicyWrite> = (0..101)
        .map(|i| PeerPolicyWrite {
            peer_id: format!("p{i}"),
            strategy: "dynamic".into(),
            rebalance_mode: "enabled".into(),
            fee_ppm_target: None,
            tags: None,
        })
        .collect();
    let ack = writer.apply_policy_batch(oversized, 1_800_000_000).await;
    match &ack {
        StateWriteAck::Denied(detail) => assert!(detail.contains("101"), "{detail}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(ack.code(), "denied");

    // NotFound budget transition is Denied (a validation fact, not a
    // storage failure).
    let ack = writer.release_budget_reservation("ghost".into()).await;
    assert!(matches!(ack, StateWriteAck::Denied(_)), "{ack:?}");
}

/// StorageFailure from a definitively failed write (sabotaged table);
/// AdmittedOutcomeUnknown from a receipt that expires under a held lock.
#[tokio::test]
async fn storage_failure_and_admitted_unknown_arms() {
    let (_d, path, writer) = fixture().await;

    // StorageFailure: the actor replies with a real error.
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER poison BEFORE INSERT ON hot_channel_protection_overrides
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
    let ack = writer
        .set_hot_channel_override("peerX".into(), None, None, 1_800_000_000)
        .await;
    assert!(matches!(ack, StateWriteAck::StorageFailure(_)), "{ack:?}");
    assert_eq!(ack.code(), "storage_failure");

    // AdmittedOutcomeUnknown: hold a write lock so the admitted command
    // cannot answer inside a deliberately tiny receipt budget.
    let blocker = Connection::open(&path).unwrap();
    blocker
        .busy_timeout(std::time::Duration::from_secs(60))
        .unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let tight = writer.with_receipt_budget(std::time::Duration::from_millis(100));
    let ack = tight.set_config_override("held".into(), "1".into()).await;
    assert!(
        matches!(ack, StateWriteAck::AdmittedOutcomeUnknown(_)),
        "{ack:?}"
    );
    assert_eq!(ack.code(), "admitted_outcome_unknown");
    blocker.execute_batch("ROLLBACK").unwrap();

    // The admitted command was uncancellable: it lands once the lock
    // lifts -- exactly why the arm must never read as "not written".
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let n: i64 = Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM config_overrides WHERE key = 'held'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        if n == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the admitted write must eventually land"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Ordering rails: publish fires ONLY after a committed config write,
/// with the committed version; wake fires ONLY after a committed policy
/// upsert. Non-Applied acks never trigger either.
#[tokio::test]
async fn publish_and_wake_fire_only_after_commit() {
    let (_d, path, writer) = fixture().await;

    // Publish observes exactly the committed version.
    let published = Arc::new(AtomicI64::new(0));
    let p2 = published.clone();
    let ack = writer
        .set_config_override_and_publish("k".into(), "v".into(), move |version| {
            p2.store(version, Ordering::SeqCst);
        })
        .await;
    assert!(matches!(ack, StateWriteAck::Applied(1)), "{ack:?}");
    assert_eq!(published.load(Ordering::SeqCst), 1);

    // A failed write never publishes.
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER poison2 BEFORE INSERT ON config_overrides
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
    let published_on_failure = Arc::new(AtomicBool::new(false));
    let p3 = published_on_failure.clone();
    let ack = writer
        .set_config_override_and_publish("k2".into(), "v".into(), move |_| {
            p3.store(true, Ordering::SeqCst);
        })
        .await;
    assert!(matches!(ack, StateWriteAck::StorageFailure(_)), "{ack:?}");
    assert!(
        !published_on_failure.load(Ordering::SeqCst),
        "publication STRICTLY follows commit"
    );
    Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TRIGGER poison2;")
        .unwrap();

    // Wake strictly after policy commit; never on failure.
    let woke = Arc::new(AtomicBool::new(false));
    let w2 = woke.clone();
    let write = PeerPolicyWrite {
        peer_id: "peerZ".into(),
        strategy: "dynamic".into(),
        rebalance_mode: "enabled".into(),
        fee_ppm_target: Some(120),
        tags: None,
    };
    let ack = writer
        .upsert_peer_policy_then_wake(write.clone(), 1_800_000_000, move || {
            w2.store(true, Ordering::SeqCst);
        })
        .await;
    assert!(matches!(ack, StateWriteAck::Applied(())), "{ack:?}");
    assert!(woke.load(Ordering::SeqCst));

    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER poison3 BEFORE INSERT ON peer_policies
             BEGIN SELECT RAISE(ABORT, 'injected'); END;",
        )
        .unwrap();
    let woke_on_failure = Arc::new(AtomicBool::new(false));
    let w3 = woke_on_failure.clone();
    let ack = writer
        .upsert_peer_policy_then_wake(write, 1_800_000_001, move || {
            w3.store(true, Ordering::SeqCst);
        })
        .await;
    assert!(matches!(ack, StateWriteAck::StorageFailure(_)), "{ack:?}");
    assert!(
        !woke_on_failure.load(Ordering::SeqCst),
        "no wake without commit"
    );
}

/// The capability is structurally absent from every observer surface:
/// production source never names it, and nothing outside tests calls
/// `assemble`.
#[test]
fn capability_is_unreachable_from_observer_surfaces() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for file in ["src/runtime.rs", "src/lnplus_runtime.rs", "src/main.rs"] {
        let source = std::fs::read_to_string(root.join(file)).unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap();
        assert!(
            !production.contains("ProductionStateWriter"),
            "{file} must not name the state-writer capability before Task 69"
        );
    }
    // No production construction anywhere in src/ or bins.
    fn scan(dir: &std::path::Path, hits: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                scan(&path, hits);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let source = std::fs::read_to_string(&path).unwrap();
                let production = source.split("#[cfg(test)]").next().unwrap().to_string();
                if production.contains("ProductionStateWriter::assemble(")
                    && !path.ends_with("state_writer.rs")
                {
                    hits.push(path.display().to_string());
                }
            }
        }
    }
    let mut hits = Vec::new();
    scan(&root.join("src"), &mut hits);
    assert!(
        hits.is_empty(),
        "assemble() called outside the defining module/tests: {hits:?}"
    );
}
