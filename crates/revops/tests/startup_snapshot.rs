//! Task 67 slice 4: the one-shot startup-snapshot owner.

use std::collections::BTreeSet;

use revops::startup_snapshot::{plan_startup_snapshot, SnapshotDeps, SnapshotRefusal};
use serde_json::json;

const NOW: i64 = 1_800_000_000;

fn deps<'a>(recent: &'a BTreeSet<String>) -> SnapshotDeps<'a> {
    SnapshotDeps {
        peers_raw: Ok(json!({"peers": [
            {"id": "02aa", "connected": true},
            {"id": "02bb", "connected": true},
            {"id": "02cc", "connected": false},
        ]})),
        peers_with_recent_history: Ok(recent),
        now: NOW,
    }
}

/// Only CONNECTED peers without recent history are recorded (py
/// `_snapshot_peers_once`, cl-revenue-ops.py:422-457).
#[test]
fn records_connected_peers_without_recent_history() {
    let recent = BTreeSet::new();
    let plan = plan_startup_snapshot(deps(&recent)).expect("healthy");
    assert_eq!(plan.record_peer_ids, vec!["02aa", "02bb"]);
    assert_eq!(plan.skipped_disconnected, 1);
    assert_eq!(plan.skipped_recent, 0);

    // A peer with history inside the window is skipped, not re-recorded.
    let recent: BTreeSet<String> = ["02aa".to_string()].into_iter().collect();
    let plan = plan_startup_snapshot(deps(&recent)).expect("healthy");
    assert_eq!(plan.record_peer_ids, vec!["02bb"]);
    assert_eq!(plan.skipped_recent, 1);
}

/// Every required source refuses typed. An unreadable peer list is NOT
/// "no peers" -- that distinction is the whole point of the pass failing
/// rather than silently recording nothing.
#[test]
fn required_sources_refuse_typed() {
    let recent = BTreeSet::new();
    let mut d = deps(&recent);
    d.peers_raw = Err("listpeers rpc timeout".into());
    let err = plan_startup_snapshot(d).expect_err("peer failure refuses");
    assert_eq!(err.code(), "startup_snapshot_peers_unavailable");

    let mut d = deps(&recent);
    d.peers_raw = Ok(json!({"result": "ok"}));
    let err = plan_startup_snapshot(d).expect_err("shapeless reply refuses");
    assert!(matches!(err, SnapshotRefusal::PeersUnavailable(_)));

    let d = SnapshotDeps {
        peers_raw: Ok(json!({"peers": []})),
        peers_with_recent_history: Err("connection history read failed".into()),
        now: NOW,
    };
    let err = plan_startup_snapshot(d).expect_err("history failure refuses");
    assert_eq!(err.code(), "startup_snapshot_history_unavailable");
}

/// An empty-but-readable peer list is a legitimate zero-work pass, NOT a
/// refusal -- the counterpart to the distinction above.
#[test]
fn empty_peer_list_is_a_successful_zero_work_pass() {
    let recent = BTreeSet::new();
    let d = SnapshotDeps {
        peers_raw: Ok(json!({"peers": []})),
        peers_with_recent_history: Ok(&recent),
        now: NOW,
    };
    let plan = plan_startup_snapshot(d).expect("zero work is still success");
    assert!(plan.record_peer_ids.is_empty());
    assert_eq!(plan.skipped_disconnected, 0);
}

/// Python records this loop's heartbeat BEFORE the work and never marks it
/// failed (cl-revenue-ops.py:3481), so a crashed snapshot still looks
/// alive. That bug is deliberately NOT ported: the pass is reported only
/// from a plan that already succeeded, so the module exposes no
/// "mark passed" entry point a caller could invoke early.
#[test]
fn the_python_heartbeat_before_work_bug_is_not_ported() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/startup_snapshot.rs"
    ))
    .unwrap();
    for forbidden in ["finish_loop_pass", "mark_passed", "record_heartbeat"] {
        assert!(
            !source.contains(forbidden),
            "the planner must not be able to report a pass itself (found `{forbidden}`)"
        );
    }
}
