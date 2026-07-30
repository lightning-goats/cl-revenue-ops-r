//! Task 71 / F71-R16: the two analytics store READS the concrete
//! passes need, neither of which existed.
//!
//! The observer handle already carried the analytics WRITES and
//! current-state reads (`owner.rs` 1807-2013). What it had no path for at
//! all was the flow pass's own INPUT: the daily flow buckets Python
//! derives from its forwards table (`database.get_daily_flow_buckets`,
//! database.py:5008). Without them `FlowDeps::history` could only ever be
//! hand-built in a test, and a wired pass would hand every channel an
//! empty bucket list -- which the frozen kernels read as "no
//! observations", collapsing the whole fleet to zero confidence.
//!
//! The second is the one-shot startup snapshot's "who already has recent
//! connection history" set, the batched form of py
//! `has_recent_connection_history`.

use std::collections::BTreeSet;

use revops_db::analytics::{
    channel_flow_states, daily_flow_buckets, kalman_states, persist_flow_pass,
    upsert_channel_flow_state, upsert_kalman_state, ChannelFlowStateRow,
};
use revops_db::notifications::{
    init_schema, insert_forward_ignore_dup, insert_peer_connection_event,
    peers_with_recent_connection_history, ForwardRow,
};
use rusqlite::Connection;
use serde_json::json;

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;
const WINDOW_DAYS: i64 = 7;

fn db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    init_schema(&conn).expect("init schema");
    conn
}

fn forward(
    in_channel: &str,
    out_channel: &str,
    in_msat: i64,
    out_msat: i64,
    ts: i64,
) -> ForwardRow {
    ForwardRow {
        in_channel: in_channel.into(),
        out_channel: out_channel.into(),
        in_msat,
        out_msat,
        fee_msat: in_msat - out_msat,
        timestamp: ts,
        resolved_time: ts + 1,
    }
}

// -------------------------------------------------------------------
// daily flow buckets — port of py database.py:5008 get_daily_flow_buckets
// -------------------------------------------------------------------

/// py bins by `CAST((now - timestamp) / 86400 AS INTEGER)` — a ROLLING
/// age-in-days from `now`, not a calendar day. Index 0 is the last 24h.
#[test]
fn buckets_bin_by_rolling_age_days_not_calendar_days() {
    let conn = db();
    // Today (age 0) and the day before yesterday (age 2).
    insert_forward_ignore_dup(&conn, &forward("aaa", "bbb", 5_000, 4_000, NOW - 60)).unwrap();
    insert_forward_ignore_dup(
        &conn,
        &forward("aaa", "bbb", 7_000, 6_000, NOW - 2 * DAY - 60),
    )
    .unwrap();

    let history = daily_flow_buckets(&conn, NOW, WINDOW_DAYS).expect("read daily flow buckets");

    let aaa = history
        .get("aaa")
        .expect("aaa forwarded, so it must appear");
    assert_eq!(aaa.len(), WINDOW_DAYS as usize);
    // `aaa` is the IN channel on both forwards.
    assert_eq!(aaa[0].in_sats, 5);
    assert_eq!(aaa[0].out_sats, 0);
    assert_eq!(aaa[1].in_sats, 0, "age-1 day had no forwards");
    assert_eq!(aaa[2].in_sats, 7);

    let bbb = history
        .get("bbb")
        .expect("bbb forwarded, so it must appear");
    assert_eq!(bbb[0].out_sats, 4);
    assert_eq!(bbb[2].out_sats, 6);
}

/// py `base_to_sats_floor` per bucket: msat is FLOORED to sats, so 1_999
/// msat is 1 sat and 999 msat is a real, measured 0.
#[test]
fn buckets_floor_msat_to_sats() {
    let conn = db();
    insert_forward_ignore_dup(&conn, &forward("aaa", "bbb", 1_999, 999, NOW - 60)).unwrap();

    let history = daily_flow_buckets(&conn, NOW, WINDOW_DAYS).unwrap();
    assert_eq!(history["aaa"][0].in_sats, 1);
    assert_eq!(history["bbb"][0].out_sats, 0);
}

/// py fills a channel's whole `window_days` list with zero buckets the
/// moment it sees ANY forward for it — but a channel with NO forwards in
/// the window never enters the dict at all. Absent and zero are different
/// facts and the frozen decay/volatility kernels read the LENGTH of this
/// vector (`< 3` short-circuits to 1.0), so padding is load-bearing.
#[test]
fn buckets_pad_the_window_but_a_silent_channel_is_absent_not_zero_filled() {
    let conn = db();
    insert_forward_ignore_dup(&conn, &forward("aaa", "bbb", 5_000, 4_000, NOW - 3 * DAY)).unwrap();

    let history = daily_flow_buckets(&conn, NOW, WINDOW_DAYS).unwrap();
    assert_eq!(history["aaa"].len(), WINDOW_DAYS as usize);
    assert!(
        history["aaa"][0..3].iter().all(|b| b.in_sats == 0),
        "days more recent than the only forward are padded zeros"
    );
    assert!(
        !history.contains_key("ccc"),
        "a channel with no forwards in the window must be ABSENT, not zero-filled"
    );
}

/// Forwards older than the window are excluded entirely — py's
/// `WHERE timestamp >= now - window_days*86400` plus the
/// `age_days < window_days` guard.
#[test]
fn buckets_exclude_forwards_older_than_the_window() {
    let conn = db();
    insert_forward_ignore_dup(
        &conn,
        &forward("aaa", "bbb", 5_000, 4_000, NOW - WINDOW_DAYS * DAY - 60),
    )
    .unwrap();

    let history = daily_flow_buckets(&conn, NOW, WINDOW_DAYS).unwrap();
    assert!(
        !history.contains_key("aaa"),
        "a forward outside the window must not create a channel entry"
    );
}

/// The count/last_ts fields the confidence calculation reads are summed
/// and maxed per bucket across BOTH projections.
#[test]
fn buckets_carry_forward_count_and_last_timestamp() {
    let conn = db();
    insert_forward_ignore_dup(&conn, &forward("aaa", "bbb", 5_000, 4_000, NOW - 60)).unwrap();
    insert_forward_ignore_dup(&conn, &forward("aaa", "bbb", 6_000, 5_000, NOW - 30)).unwrap();

    let history = daily_flow_buckets(&conn, NOW, WINDOW_DAYS).unwrap();
    assert_eq!(history["aaa"][0].count, 2);
    assert_eq!(history["aaa"][0].last_ts, NOW - 30);
}

// -------------------------------------------------------------------
// peers_with_recent_connection_history — startup snapshot's required read
// -------------------------------------------------------------------

/// py `has_recent_connection_history(peer_id, 3600)`, batched: the
/// one-shot startup snapshot needs the whole set, and asking per-peer
/// would be one actor round-trip per peer.
#[test]
fn recent_connection_history_windows_on_the_cutoff() {
    let conn = db();
    insert_peer_connection_event(&conn, "recent", "connected", NOW - 60).unwrap();
    insert_peer_connection_event(&conn, "stale", "connected", NOW - 7_200).unwrap();

    let recent = peers_with_recent_connection_history(&conn, NOW - 3_600).unwrap();
    assert_eq!(recent, BTreeSet::from(["recent".to_string()]));
}

/// An empty result is a measured "nobody has recent history", which on a
/// fresh boot is the normal case and must not be confused with a failure.
#[test]
fn recent_connection_history_is_empty_not_absent_on_a_fresh_db() {
    let conn = db();
    assert!(peers_with_recent_connection_history(&conn, NOW - 3_600)
        .unwrap()
        .is_empty());
}

/// The cutoff is inclusive on the boundary, matching py's `ts >= cutoff`.
#[test]
fn recent_connection_history_includes_the_exact_boundary() {
    let conn = db();
    insert_peer_connection_event(&conn, "boundary", "connected", NOW - 3_600).unwrap();
    assert!(peers_with_recent_connection_history(&conn, NOW - 3_600)
        .unwrap()
        .contains("boundary"));
}

// -------------------------------------------------------------------
// persist_flow_pass — F71-R18: one pass, one transaction
// -------------------------------------------------------------------

fn state_row(scid: &str, boot_id: &str) -> ChannelFlowStateRow {
    ChannelFlowStateRow {
        scid: scid.into(),
        peer_id: format!("peer_{scid}"),
        flow_state: "source".into(),
        balance_position: "balanced".into(),
        flow_ratio: 0.25,
        velocity: 0.0,
        confidence: 0.5,
        forward_count: 3,
        updated_at: NOW,
        boot_id: boot_id.into(),
    }
}

#[test]
fn persist_flow_pass_writes_every_state_and_kalman_row() {
    let mut conn = db();
    let written = persist_flow_pass(
        &mut conn,
        &[state_row("aaa", "boot-now"), state_row("bbb", "boot-now")],
        &[
            ("aaa".to_string(), json!({"flow_ratio": 0.1})),
            ("bbb".to_string(), json!({"flow_ratio": 0.2})),
        ],
        NOW,
    )
    .expect("persist a complete pass");
    assert_eq!(written, 2);

    let states = channel_flow_states(&conn).unwrap();
    assert_eq!(states.len(), 2);
    assert!(states.iter().all(|s| s.boot_id == "boot-now"));
    assert_eq!(kalman_states(&conn).unwrap().len(), 2);
}

/// The load-bearing one. `rust_kalman_state` has NO boot_id column, so a
/// partially written filter set carries no evidence that it is partial —
/// unlike `rust_channel_flow_states`, whose boot_id at least exposes the
/// split. All-or-nothing is what makes the difference unobservable-by-
/// construction rather than merely unlikely.
#[test]
fn persist_flow_pass_rolls_back_completely_when_a_kalman_write_fails() {
    let mut conn = db();
    // A previous boot's evidence, which must survive intact.
    upsert_channel_flow_state(&conn, &state_row("aaa", "boot-previous")).unwrap();
    upsert_kalman_state(&conn, "aaa", &json!({"flow_ratio": 0.9}), NOW - 86_400).unwrap();
    // Injected failure: the Kalman store disappears mid-pass. The state
    // rows are written BEFORE the kalman rows, so this proves the earlier
    // writes are rolled back rather than merely never attempted.
    conn.execute_batch("DROP TABLE rust_kalman_state").unwrap();

    let err = persist_flow_pass(
        &mut conn,
        &[state_row("aaa", "boot-now"), state_row("bbb", "boot-now")],
        &[("aaa".to_string(), json!({"flow_ratio": 0.1}))],
        NOW,
    )
    .expect_err("a failed pass must refuse, not half-commit");
    assert!(
        format!("{err:#}").contains("flow pass"),
        "the refusal must name the pass it rolled back, got: {err:#}"
    );

    let states = channel_flow_states(&conn).unwrap();
    assert_eq!(states.len(), 1, "bbb must NOT have been committed");
    assert_eq!(
        states[0].boot_id, "boot-previous",
        "aaa must still carry the PREVIOUS boot's evidence, not a half-written new one"
    );
}

/// A pass that legitimately derived nothing commits nothing and succeeds.
/// An empty fleet and a failed pass are different facts.
#[test]
fn persist_flow_pass_accepts_a_genuinely_empty_pass() {
    let mut conn = db();
    assert_eq!(persist_flow_pass(&mut conn, &[], &[], NOW).unwrap(), 0);
    assert!(channel_flow_states(&conn).unwrap().is_empty());
}
