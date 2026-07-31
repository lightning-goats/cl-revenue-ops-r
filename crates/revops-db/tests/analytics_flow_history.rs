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

use revops_db::analytics::{
    channel_flow_states, daily_flow_buckets, hourly_flow_histogram, kalman_states,
    persist_flow_pass, temporal_profiles, upsert_channel_flow_state, upsert_kalman_state,
    upsert_temporal_profile, ChannelFlowStateRow,
};
use revops_db::notifications::{
    init_schema, insert_forward_ignore_dup, insert_peer_connection_event,
    peers_with_recent_connection_history, ForwardRow,
};
use rusqlite::Connection;
use serde_json::json;
use std::collections::BTreeSet;

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

fn retain<const N: usize>(scids: [&str; N]) -> BTreeSet<String> {
    scids.iter().map(|s| s.to_string()).collect()
}

fn state_row(scid: &str, boot_id: &str) -> ChannelFlowStateRow {
    ChannelFlowStateRow {
        scid: scid.into(),
        peer_id: format!("peer_{scid}"),
        flow_state: "source".into(),
        balance_position: "balanced".into(),
        flow_ratio: 0.25,
        velocity: 0.0,
        confidence: 0.5,
        kalman_flow_ratio: 0.0,
        kalman_velocity: 0.0,
        kalman_uncertainty: 0.0,
        kalman_regime_change: false,
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
        &[],
        &retain(["aaa", "bbb"]),
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
        &[],
        &retain(["aaa", "bbb"]),
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
    assert_eq!(
        persist_flow_pass(&mut conn, &[], &[], &[], &retain([]), NOW).unwrap(),
        0
    );
    assert!(channel_flow_states(&conn).unwrap().is_empty());
}

// -------------------------------------------------------------------
// F71-R24: closed-channel reconciliation, inside the same transaction
// -------------------------------------------------------------------

/// A channel that has left `listpeerchannels` is gone. Its rows must be
/// purged from EVERY analytics table — py deletes `channel_states` and
/// `kalman_state` together under one `BEGIN IMMEDIATE`
/// (database.py:6614-6645).
#[test]
fn closed_channels_are_purged_from_every_analytics_table() {
    let mut conn = db();
    upsert_channel_flow_state(&conn, &state_row("closed", "boot-old")).unwrap();
    upsert_kalman_state(&conn, "closed", &json!({"flow_ratio": 0.5}), NOW - DAY).unwrap();
    upsert_temporal_profile(&conn, "closed", &json!({"observation_days": 9}), NOW - DAY).unwrap();

    persist_flow_pass(
        &mut conn,
        &[state_row("live", "boot-now")],
        &[("live".to_string(), json!({"flow_ratio": 0.1}))],
        &[("live".to_string(), json!({"observation_days": 1}))],
        &retain(["live"]),
        NOW,
    )
    .expect("persist a pass that no longer sees `closed`");

    assert!(channel_flow_states(&conn)
        .unwrap()
        .iter()
        .all(|s| s.scid == "live"));
    assert!(kalman_states(&conn)
        .unwrap()
        .iter()
        .all(|(s, _, _)| s == "live"));
    assert!(temporal_profiles(&conn)
        .unwrap()
        .iter()
        .all(|(s, _, _)| s == "live"));
}

/// The load-bearing distinction. F71-R21 stops ANALYSING channels that are
/// not `CHANNELD_NORMAL`, but such a channel still EXISTS. Reconciling
/// against the analysed set instead of the observed set would delete the
/// accumulated Kalman state of every channel briefly in a transient state
/// — state that takes many observations to rebuild and cannot be recovered.
#[test]
fn a_transient_state_channel_is_retained_even_though_it_was_not_analysed() {
    let mut conn = db();
    upsert_kalman_state(&conn, "transient", &json!({"flow_ratio": 0.42}), NOW - DAY).unwrap();
    upsert_channel_flow_state(&conn, &state_row("transient", "boot-old")).unwrap();

    // The pass analysed only `live`, but BOTH scids were observed in the
    // snapshot — `transient` was merely skipped as not CHANNELD_NORMAL.
    persist_flow_pass(
        &mut conn,
        &[state_row("live", "boot-now")],
        &[("live".to_string(), json!({"flow_ratio": 0.1}))],
        &[],
        &retain(["live", "transient"]),
        NOW,
    )
    .expect("persist a pass that observed but did not analyse `transient`");

    let kalman = kalman_states(&conn).unwrap();
    let kept = kalman
        .iter()
        .find(|(s, _, _)| s == "transient")
        .expect("an observed-but-unanalysed channel keeps its Kalman state");
    assert_eq!(kept.1, json!({"flow_ratio": 0.42}), "and it is UNCHANGED");
}

/// Reconciliation is inside the pass transaction: if the pass fails,
/// nothing is deleted either.
#[test]
fn a_failed_pass_deletes_nothing() {
    let mut conn = db();
    upsert_channel_flow_state(&conn, &state_row("closed", "boot-old")).unwrap();
    conn.execute_batch("DROP TABLE rust_kalman_state").unwrap();

    persist_flow_pass(
        &mut conn,
        &[state_row("live", "boot-now")],
        &[("live".to_string(), json!({"flow_ratio": 0.1}))],
        &[],
        &retain(["live"]),
        NOW,
    )
    .expect_err("the pass must fail");

    assert_eq!(
        channel_flow_states(&conn).unwrap().len(),
        1,
        "the closed channel's row must survive a FAILED pass"
    );
    assert_eq!(channel_flow_states(&conn).unwrap()[0].scid, "closed");
}

// -------------------------------------------------------------------
// F71-R25: the hour-of-day histogram that feeds the temporal kernel
// -------------------------------------------------------------------

/// py bins on `CAST(((timestamp % 86400) / 3600) AS INTEGER)` — UTC
/// hour-of-day, NOT an age-relative bin like the daily buckets.
#[test]
fn histogram_bins_on_utc_hour_of_day() {
    let conn = db();
    // NOW is a multiple of 86400*... pick timestamps with known hours.
    let midnight = (NOW / 86_400) * 86_400;
    insert_forward_ignore_dup(
        &conn,
        &forward("aaa", "bbb", 5_000, 4_000, midnight + 3_600),
    )
    .unwrap();
    insert_forward_ignore_dup(
        &conn,
        &forward("aaa", "bbb", 7_000, 6_000, midnight + 5 * 3_600),
    )
    .unwrap();

    let hist = hourly_flow_histogram(&conn, NOW, 7).expect("read histogram");
    let aaa = &hist["aaa"];
    assert_eq!(aaa[1].in_sats, 5, "hour 1 UTC");
    assert_eq!(aaa[5].in_sats, 7, "hour 5 UTC");
    assert_eq!(aaa[2].in_sats, 0, "an untouched hour is a measured zero");
}

/// Each bucket is divided by the channel's DAYS-WITH-DATA, making it a
/// per-day average. py's F5 fix: comparing a window TOTAL against a
/// per-day threshold let ~1.4 forwards/day graduate a channel.
#[test]
fn histogram_buckets_are_averaged_per_day_with_data() {
    let conn = db();
    let midnight = (NOW / 86_400) * 86_400;
    // Same UTC hour on two different days -> 2 days with data.
    insert_forward_ignore_dup(
        &conn,
        &forward("aaa", "bbb", 10_000, 9_000, midnight + 3_600),
    )
    .unwrap();
    insert_forward_ignore_dup(
        &conn,
        &forward("aaa", "bbb", 10_000, 9_000, midnight - 86_400 + 3_600),
    )
    .unwrap();

    let hist = hourly_flow_histogram(&conn, NOW, 7).unwrap();
    assert_eq!(
        hist["aaa"][1].count, 1,
        "2 forwards across 2 days averages to 1/day, not a total of 2"
    );
    assert_eq!(hist["aaa"][1].in_sats, 10, "20 sats over 2 days");
}

/// F71-R25b: the temporal write is inside the same transaction, so a
/// failure THERE must roll back the state and Kalman writes that already
/// succeeded ahead of it. The existing rollback test drops the Kalman
/// table, which fails earlier in the sequence and so cannot prove this.
#[test]
fn a_failing_temporal_write_rolls_back_the_whole_pass() {
    let mut conn = db();
    upsert_channel_flow_state(&conn, &state_row("aaa", "boot-previous")).unwrap();
    upsert_kalman_state(&conn, "aaa", &json!({"flow_ratio": 0.9}), NOW - DAY).unwrap();
    // The temporal store disappears; states and Kalman are written first.
    conn.execute_batch("DROP TABLE rust_temporal_profiles")
        .unwrap();

    let err = persist_flow_pass(
        &mut conn,
        &[state_row("aaa", "boot-now"), state_row("bbb", "boot-now")],
        &[("aaa".to_string(), json!({"flow_ratio": 0.1}))],
        &[("aaa".to_string(), json!({"observation_days": 4}))],
        &retain(["aaa", "bbb"]),
        NOW,
    )
    .expect_err("a failing temporal write must refuse the whole pass");
    assert!(format!("{err:#}").contains("temporal profile"), "{err:#}");

    let states = channel_flow_states(&conn).unwrap();
    assert_eq!(states.len(), 1, "bbb must NOT have been committed");
    assert_eq!(
        states[0].boot_id, "boot-previous",
        "aaa's state write must roll back even though it succeeded first"
    );
    assert_eq!(
        kalman_states(&conn).unwrap()[0].1,
        json!({"flow_ratio": 0.9}),
        "and so must its Kalman write"
    );
}

/// F71-R28/R28a: the current-boot lookup returns THIS boot's newest row
/// and nothing else, and it does so without loading the series. The table
/// is Class E / never-prune, so a lookup that materialized history would
/// grow without limit for the life of the node.
#[test]
fn current_boot_financial_snapshot_picks_this_boots_newest_row() {
    let conn = db();
    let snap =
        |taken_at: i64, capacity: i64, boot: &str| revops_db::analytics::FinancialSnapshotRow {
            taken_at,
            local_balance_sats: capacity / 2,
            remote_balance_sats: capacity / 2,
            onchain_sats: 0,
            capacity_sats: capacity,
            revenue_accumulated_sats: 0,
            rebalance_cost_accumulated_sats: 0,
            channel_count: 1,
            boot_id: boot.to_string(),
        };
    // A long prior history plus two rows for this boot.
    for day in 0..50 {
        revops_db::analytics::insert_financial_snapshot(
            &conn,
            &snap(NOW - (100 - day) * DAY, 1_000, "boot-previous"),
        )
        .unwrap();
    }
    revops_db::analytics::insert_financial_snapshot(&conn, &snap(NOW - DAY, 111, "boot-now"))
        .unwrap();
    revops_db::analytics::insert_financial_snapshot(&conn, &snap(NOW, 222, "boot-now")).unwrap();
    // A LATER row from a different boot must not win.
    revops_db::analytics::insert_financial_snapshot(&conn, &snap(NOW + DAY, 999, "boot-other"))
        .unwrap();

    let row = revops_db::analytics::current_boot_financial_snapshot(&conn, "boot-now")
        .unwrap()
        .expect("this boot has measured");
    assert_eq!(row.capacity_sats, 222, "the NEWEST row for THIS boot");
    assert_eq!(row.boot_id, "boot-now");

    assert!(
        revops_db::analytics::current_boot_financial_snapshot(&conn, "boot-unseen")
            .unwrap()
            .is_none(),
        "a boot that has not measured gets None, not someone else's row"
    );
}
