//! Integration tests for `revops::fee_evidence` -- the per-cycle-frozen
//! `EvidenceSnapshot` (`FeeEvidence` over a read-only DB `Connection` +
//! prefetched RPC snapshots), Phase 4b Task 2.
//!
//! Fixture pattern follows `crates/revops-db/tests/queries.rs`: copy the
//! committed schema-only `fixtures/fixture.db` into a tempdir, seed known
//! rows via a raw read-write `rusqlite` connection, then build the
//! snapshot against the seeded copy. RPC data is a canned `RpcPrefetch`
//! (its fields are plain owned JSON, constructible without a socket).
//!
//! Expected values are hand-derived from the seeded rows by applying the
//! same SQL/arithmetic `database.py`/`fee_controller.py` implement,
//! documented inline at each assertion.

use revops::fee_evidence::{build_evidence_snapshot, prefetch_rpc, EvidenceSnapshot, RpcPrefetch};
use revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode};
use revops_fees::floors::{FlowWindow, PeerLatency};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// Fixed "now" for every test -- an arbitrary point in time, deliberately
/// not day-aligned (exercises the `(now // 86400) * 86400` bucketing in
/// the marginal-ROI queries honestly).
const NOW: i64 = 1_800_000_000;

fn peer_a() -> String {
    format!("02{}", "aa".repeat(32))
}
fn peer_b() -> String {
    format!("03{}", "bb".repeat(32))
}
fn peer_c() -> String {
    format!("02{}", "cc".repeat(32))
}
fn neighbor_1() -> String {
    format!("02{}", "11".repeat(32))
}
fn neighbor_2() -> String {
    format!("03{}", "22".repeat(32))
}

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db")
}

/// Copy the empty fixture DB into a fresh tempdir, switch it to WAL (the
/// production DB's mode -- `database.py` `PRAGMA journal_mode=WAL`), and
/// hand the seeded copy's path back with an open read-write connection for
/// further seeding/verification.
fn seeded_db() -> (tempfile::TempDir, PathBuf, Connection) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("seeded.db");
    std::fs::copy(fixture_path(), &path).expect("copy fixture.db");
    let conn = Connection::open(&path).expect("open seeded copy read-write");
    conn.pragma_update(None, "journal_mode", "WAL")
        .expect("switch to WAL");
    (dir, path, conn)
}

/// Seed the three channel_states rows most tests share. Insert order is
/// deliberately NOT the query's output order (see
/// `channel_states_preserves_python_row_order`).
fn seed_channel_states(conn: &Connection) {
    for (cid, peer, state, flow_ratio, kalman) in [
        ("300x3x0", peer_b(), "sink", 0.2_f64, 0.15_f64),
        ("100x1x0", peer_a(), "balanced", 0.1, 0.05),
        ("200x2x0", peer_a(), "balanced", 0.9, 0.85),
    ] {
        conn.execute(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, \
             sats_out, capacity, updated_at, kalman_flow_ratio, kalman_velocity) \
             VALUES (?1, ?2, ?3, ?4, 0, 0, 2000000, ?5, ?6, 0.01)",
            rusqlite::params![cid, peer, state, flow_ratio, NOW - 60, kalman],
        )
        .unwrap();
    }
}

/// Canned `listpeerchannels`-shaped rows: one plain CHANNELD_NORMAL
/// channel per peer plus one non-normal row that `channels_info` must
/// drop (but `node_channels` must keep).
fn canned_peer_channels() -> Vec<Value> {
    vec![
        json!({
            "state": "CHANNELD_NORMAL",
            // Colon form on purpose: normalize_scid must map it to 100x1x0.
            "short_channel_id": "100:1:0",
            "channel_id": "full_chan_a",
            "peer_id": peer_a(),
            "total_msat": 2_000_000_000_i64,
            "to_us_msat": 1_100_000_000_i64,
            "spendable_msat": 1_000_000_000_i64,
            "receivable_msat": 900_000_000_i64,
            "updates": {"local": {
                "fee_base_msat": 0,
                "fee_proportional_millionths": 150,
                "htlc_minimum_msat": 1000,
                "htlc_maximum_msat": 1_980_000_000_i64,
            }},
            "opener": "local",
            "max_accepted_htlcs": 483,
            "htlcs": [
                {"direction": "out"},
                {"direction": "in"},
                {"direction": "out"},
            ],
        }),
        json!({
            "state": "CHANNELD_NORMAL",
            "short_channel_id": "200x2x0",
            "channel_id": "full_chan_b",
            "peer_id": peer_b(),
            // No total_msat/capacity_msat: capacity = spendable + receivable.
            // msat-string form exercises parse_msat.
            "spendable_msat": "600000000msat",
            "receivable_msat": 400_000_000_i64,
            // No updates.local: falls back to top-level fee fields.
            "fee_base_msat": 1000,
            "fee_proportional_millionths": 250,
            "htlc_minimum_msat": 1,
            "htlc_maximum_msat": 990_000_000_i64,
            "opener": "remote",
            // No htlcs array: has_htlc_data false, our_htlcs_in_flight 0.
        }),
        json!({
            "state": "OPENINGD",
            "short_channel_id": "400x4x0",
            "channel_id": "full_chan_d",
            "peer_id": peer_c(),
            "total_msat": 5_000_000_000_i64,
            "to_us_msat": 5_000_000_000_i64,
            "spendable_msat": 4_900_000_000_i64,
            "receivable_msat": 0,
        }),
    ]
}

fn canned_gossip() -> Vec<Value> {
    vec![
        json!({
            "source": neighbor_1(),
            "destination": peer_a(),
            "active": true,
            "fee_per_millionth": 120,
            "satoshis": 5_000_000_i64,
            "amount_msat": 5_000_000_000_i64,
            "last_update": NOW - 600,
            "base_fee_millisatoshi": 0,
        }),
        json!({
            "source": neighbor_2(),
            "destination": peer_a(),
            "active": false,
            "fee_per_millionth": 250,
            // No satoshis; fee_base_msat fallback key (post-24.x shape).
            "amount_msat": 3_000_000_000_i64,
            "last_update": NOW - 1200,
            "fee_base_msat": 1000,
        }),
        json!({
            "source": neighbor_1(),
            "destination": peer_b(),
            "active": true,
            "fee_per_millionth": 400,
            "satoshis": 1_000_000_i64,
            "last_update": NOW - 100,
            // No base-fee key at all -> GossipRow.base_fee_msat None.
        }),
    ]
}

fn canned_prefetch(feerates: Option<Value>) -> RpcPrefetch {
    RpcPrefetch {
        our_node_id: format!("02{}", "ee".repeat(32)),
        peer_channels: canned_peer_channels(),
        gossip_channels: canned_gossip(),
        feerates,
    }
}

fn build(path: &Path, feerates: Option<Value>) -> EvidenceSnapshot {
    build_evidence_snapshot(path, canned_prefetch(feerates), NOW).expect("build snapshot")
}

// ---------------------------------------------------------------------------
// Contract point 1: channel_states row order
// ---------------------------------------------------------------------------

#[test]
fn channel_states_preserves_python_row_order() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    drop(conn);
    let snap = build(&path, None);

    // Python (database.py get_all_channel_states):
    //   SELECT * FROM channel_states ORDER BY state, flow_ratio DESC
    // 'balanced' < 'sink' (state ASC); within 'balanced', flow_ratio DESC
    // puts 0.9 before 0.1. Insert order was 300, 100, 200 -- natural rowid
    // order must NOT leak through.
    let rows = snap.channel_states();
    let ids: Vec<&str> = rows.iter().map(|r| r.channel_id.as_str()).collect();
    assert_eq!(ids, vec!["200x2x0", "100x1x0", "300x3x0"]);

    // Field mapping spot-checks.
    assert_eq!(rows[0].peer_id, peer_a());
    assert_eq!(rows[0].state, "balanced");
    assert_eq!(rows[0].updated_at, Some(NOW - 60));
    assert_eq!(rows[0].kalman_flow_ratio, Some(0.85));
    assert_eq!(rows[0].kalman_velocity, Some(0.01));
    assert_eq!(rows[2].state, "sink");
}

// ---------------------------------------------------------------------------
// Contract point 2: gossip via the FrozenObservations memo, ONE prefetch
// ---------------------------------------------------------------------------

#[test]
fn gossip_channels_memoizes_single_listchannels_prefetch() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    drop(conn);
    let snap = build(&path, None);

    // The single listchannels prefetch was consumed at construction; these
    // calls can only ever group/replay it (zero extra fetches by
    // construction -- there is no RPC handle anywhere in the snapshot).
    let first = snap.gossip_channels(&peer_a());
    let second = snap.gossip_channels(&peer_a());
    assert_eq!(first, second);
    assert_eq!(first.len(), 2);

    assert_eq!(first[0].source, neighbor_1());
    assert!(first[0].active);
    assert_eq!(first[0].fee_per_millionth, 120);
    assert_eq!(first[0].satoshis, Some(5_000_000));
    assert_eq!(first[0].amount_msat, Some(5_000_000_000));
    assert_eq!(first[0].last_update, NOW - 600);
    // base_fee_millisatoshi=0 present -> Some(0), NOT the -1 "missing"
    // sentinel downstream.
    assert_eq!(first[0].base_fee_msat, Some(0));

    assert_eq!(first[1].source, neighbor_2());
    assert!(!first[1].active);
    assert_eq!(first[1].satoshis, None);
    // base_fee_millisatoshi missing -> fee_base_msat fallback (py
    // _is_cln_default_fee, fee_controller.py:3424-3426).
    assert_eq!(first[1].base_fee_msat, Some(1000));

    let b = snap.gossip_channels(&peer_b());
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].base_fee_msat, None);

    // Peers with no gossip rows: empty both times (memoized empty).
    let none = snap.gossip_channels(&peer_c());
    assert!(none.is_empty());
    assert!(snap.gossip_channels(&peer_c()).is_empty());
}

// ---------------------------------------------------------------------------
// Contract point 3: chain_costs falsiness mapping
// ---------------------------------------------------------------------------

#[test]
fn chain_costs_falsiness_maps_to_none() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    drop(conn);

    // feerates RPC failed -> Python _get_dynamic_chain_costs_live returns
    // None -> `if cfg.enable_vegas_reflex and chain_costs:` (py 4584) does
    // NOT fire -> Rust None.
    let snap = build(&path, None);
    assert_eq!(snap.chain_costs(), None);

    // A successful feerates response is ALWAYS truthy in Python (the live
    // fn clamps open>=500/close>=300 and returns a 3-key dict) -> Some.
    let snap = build(&path, Some(json!({"perkb": {"opening": 15000}})));
    let costs = snap.chain_costs().expect("truthy chain costs");
    // 15000 sat/kvB -> 15.0 sat/vB; open int(15*140)=2100; close
    // int(15*200)=3000 (both inside the [500,50000]/[300,50000] clamps).
    assert_eq!(costs.open_cost_sats, 2100);
    assert_eq!(costs.close_cost_sats, 3000);
    assert_eq!(costs.sat_per_vbyte, 15.0);

    // Empty response: every perkb candidate missing -> `or 1000` fallback
    // -> 1.0 sat/vB -> open max(500, 140)=500, close max(300, 200)=300.
    // Python still returns a truthy dict -> Some, never None.
    let snap = build(&path, Some(json!({})));
    let costs = snap.chain_costs().expect("fallback chain costs");
    assert_eq!(costs.open_cost_sats, 500);
    assert_eq!(costs.close_cost_sats, 300);
    assert_eq!(costs.sat_per_vbyte, 1.0);

    // Zero-valued candidates are FALSY in Python's `or` chain: opening=0
    // is skipped, floor=2000 wins -> 2.0 sat/vB.
    let snap = build(&path, Some(json!({"perkb": {"opening": 0, "floor": 2000}})));
    let costs = snap.chain_costs().expect("or-chain skip of falsy 0");
    assert_eq!(costs.sat_per_vbyte, 2.0);
    assert_eq!(costs.open_cost_sats, 500); // int(280) clamped up to 500
    assert_eq!(costs.close_cost_sats, 400);

    // Upper sanity clamp at 50000 both sides.
    let snap = build(&path, Some(json!({"perkb": {"opening": 1_000_000}})));
    let costs = snap.chain_costs().expect("clamped chain costs");
    assert_eq!(costs.open_cost_sats, 50_000);
    assert_eq!(costs.close_cost_sats, 50_000);
}

// ---------------------------------------------------------------------------
// Contract point 4: clear_exploration_flag strict no-op
// ---------------------------------------------------------------------------

#[test]
fn clear_exploration_flag_is_strict_noop() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    conn.execute(
        "INSERT INTO channel_probes (channel_id, probe_type, started_at) VALUES (?1, ?2, ?3)",
        rusqlite::params!["100x1x0", "bounded_low_fee", NOW - 100],
    )
    .unwrap();
    // An already-expired probe: py get_channel_probe would DELETE it and
    // return None; the read-only snapshot must return false WITHOUT the
    // delete.
    conn.execute(
        "INSERT INTO channel_probes (channel_id, probe_type, started_at) VALUES (?1, ?2, ?3)",
        rusqlite::params!["200x2x0", "bounded_low_fee", NOW - 86_401],
    )
    .unwrap();

    let snap = build(&path, None);
    let states_before = snap.channel_states();

    assert!(snap.exploration_flag("100x1x0"));
    assert!(!snap.exploration_flag("200x2x0")); // expired -> auto-false

    // The strict no-op: nothing observable on the snapshot changes.
    snap.clear_exploration_flag("100x1x0");
    snap.clear_exploration_flag("200x2x0");
    assert!(snap.exploration_flag("100x1x0"));
    assert!(!snap.exploration_flag("200x2x0"));
    assert_eq!(snap.channel_states(), states_before);

    // ...and nothing in the production DB changes either (both probe rows
    // survive, including the expired one Python would have deleted).
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM channel_probes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
}

// ---------------------------------------------------------------------------
// Contract point 5: mempool_ma_24h SQL port
// ---------------------------------------------------------------------------

#[test]
fn mempool_ma_24h_matches_python_sql() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    for (sat_vb, ts) in [
        (10.0_f64, NOW - 1000),
        (20.0, NOW - 2000),
        (30.0, NOW - 86_400), // cutoff is >= : exactly-at-boundary counts
        (99.0, NOW - 90_000), // outside the 24h window: excluded
    ] {
        conn.execute(
            "INSERT INTO mempool_fee_history (sat_per_vbyte, timestamp) VALUES (?1, ?2)",
            rusqlite::params![sat_vb, ts],
        )
        .unwrap();
    }
    drop(conn);
    let snap = build(&path, None);
    // AVG(10, 20, 30) = 20.0 (the 99.0 sample is outside the window).
    assert_eq!(snap.mempool_ma_24h(), 20.0);
}

#[test]
fn mempool_ma_24h_empty_table_falls_back_to_one() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    drop(conn);
    let snap = build(&path, None);
    // py get_mempool_ma: `row['avg_fee'] if row and row['avg_fee'] else 1.0`
    // -- NULL AVG (no rows) is falsy -> 1.0.
    assert_eq!(snap.mempool_ma_24h(), 1.0);
}

// ---------------------------------------------------------------------------
// The rest of the FeeEvidence surface
// ---------------------------------------------------------------------------

#[test]
fn volume_and_forward_count_since_use_strict_greater_than() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    let since = NOW - 200;
    for (out_msat, ts) in [
        (1_500_500_i64, NOW - 100),
        (2_000_400, NOW - 50),
        (7_000_000, since), // exactly-at-boundary: excluded (strict >)
    ] {
        conn.execute(
            "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
             timestamp, resolved_time) VALUES ('900x9x0', '100x1x0', ?1, ?1, 100, ?2, ?2)",
            rusqlite::params![out_msat, ts],
        )
        .unwrap();
    }
    drop(conn);
    let snap = build(&path, None);

    // floor((1_500_500 + 2_000_400) / 1000) = floor(3500.9) = 3500 sats.
    assert_eq!(snap.volume_since("100x1x0", since), 3500);
    assert_eq!(snap.forward_count_since("100x1x0", since), 2);
    assert_eq!(snap.volume_since("200x2x0", since), 0);
    assert_eq!(snap.forward_count_since("200x2x0", since), 0);
}

#[test]
fn last_forward_time_maps_falsy_to_none() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
         timestamp, resolved_time) VALUES ('900x9x0', '100x1x0', 1000, 1000, 1, ?1, ?1)",
        rusqlite::params![NOW - 50],
    )
    .unwrap();
    // A single ts=0 row: py `row['last_ts'] if row and row['last_ts'] else
    // None` -- MAX()=0 is falsy -> None.
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
         timestamp, resolved_time) VALUES ('900x9x0', '400x4x0', 1000, 1000, 1, 0, 0)",
        [],
    )
    .unwrap();
    drop(conn);
    let snap = build(&path, None);

    assert_eq!(snap.last_forward_time("100x1x0"), Some(NOW - 50));
    assert_eq!(snap.last_forward_time("400x4x0"), None);
    assert_eq!(snap.last_forward_time("555x5x0"), None);
}

#[test]
fn peer_latency_matches_python_sample_stats() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    // Joined through channel_states on out_channel; PEER_A owns 100x1x0
    // and 200x2x0. resolution_time 0 rows are filtered (py: `if
    // row['resolution_time'] and row['resolution_time'] > 0`).
    for (out_ch, rt, ts) in [
        ("100x1x0", 12.0_f64, NOW - 100),
        ("200x2x0", 8.0, NOW - 200),
        ("100x1x0", 0.0, NOW - 300),
    ] {
        conn.execute(
            "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
             resolution_time, timestamp, resolved_time) \
             VALUES ('900x9x0', ?1, 1000, 1000, 1, ?2, ?3, ?3)",
            rusqlite::params![out_ch, rt, ts],
        )
        .unwrap();
    }
    drop(conn);
    let snap = build(&path, None);

    // times = [12.0, 8.0]: avg 10.0; sample variance ((2^2)+(2^2))/(2-1)=8;
    // std = sqrt(8).
    let lat = snap
        .peer_latency(&peer_a())
        .expect("always a dict in python");
    assert!((lat.avg - 10.0).abs() < 1e-12);
    assert!((lat.std - 8.0_f64.sqrt()).abs() < 1e-12);

    // No qualifying rows -> py returns {'avg': 0.0, 'std': 0.0}.
    assert_eq!(
        snap.peer_latency(&peer_b()),
        Some(PeerLatency { avg: 0.0, std: 0.0 })
    );
}

#[test]
fn channel_cost_history_orders_desc_and_matches_aliases() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    for (cid, cost, amount, ts) in [
        ("100x1x0", 30_i64, 100_000_i64, NOW - 100),
        ("100:1:0", 50, 200_000, NOW - 50), // legacy colon alias, same channel
        ("100x1x0", 99, 300_000, NOW - 10_000),
    ] {
        conn.execute(
            "INSERT INTO rebalance_costs (channel_id, peer_id, cost_sats, amount_sats, timestamp) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![cid, peer_a(), cost, amount, ts],
        )
        .unwrap();
    }
    drop(conn);
    let snap = build(&path, None);

    let hist = snap.channel_cost_history("100x1x0", NOW - 1000);
    // since filter drops the NOW-10_000 row; ORDER BY timestamp DESC.
    assert_eq!(hist.len(), 2);
    assert_eq!(
        (hist[0].cost_sats, hist[0].amount_sats, hist[0].timestamp),
        (50, 200_000, NOW - 50)
    );
    assert_eq!(
        (hist[1].cost_sats, hist[1].amount_sats, hist[1].timestamp),
        (30, 100_000, NOW - 100)
    );
}

#[test]
fn peer_fee_history_needs_four_samples_and_floors_the_average() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    // PEER_B owns 300x3x0 (see seed_channel_states). Four successes at
    // 150_000 msat fee / 100_000 sats each.
    for i in 0..4 {
        conn.execute(
            "INSERT INTO rebalance_history (from_channel, to_channel, amount_sats, max_fee_sats, \
             actual_fee_sats, actual_fee_msat, expected_profit_sats, status, timestamp) \
             VALUES ('900x9x0', '300x3x0', 100000, 500, 150, 150000, 0, 'success', ?1)",
            rusqlite::params![NOW - 1000 - i],
        )
        .unwrap();
    }
    // Failed rows never count.
    conn.execute(
        "INSERT INTO rebalance_history (from_channel, to_channel, amount_sats, max_fee_sats, \
         actual_fee_sats, actual_fee_msat, expected_profit_sats, status, timestamp) \
         VALUES ('900x9x0', '300x3x0', 100000, 500, 150, 150000, 0, 'failed', ?1)",
        rusqlite::params![NOW - 900],
    )
    .unwrap();
    // PEER_A gets only three successes -> below min_samples=4 -> None.
    for i in 0..3 {
        conn.execute(
            "INSERT INTO rebalance_history (from_channel, to_channel, amount_sats, max_fee_sats, \
             actual_fee_sats, actual_fee_msat, expected_profit_sats, status, timestamp) \
             VALUES ('900x9x0', '100x1x0', 100000, 500, 150, 150000, 0, 'success', ?1)",
            rusqlite::params![NOW - 1000 - i],
        )
        .unwrap();
    }
    drop(conn);
    let snap = build(&path, None);

    let hist = snap.peer_fee_history(&peer_b()).expect("4 samples");
    // avg_fee_ppm = total_fees_msat * 1000 // total_volume
    //             = 600_000 * 1000 // 400_000 = 1500 ppm; 4 samples -> low.
    assert_eq!(hist.avg_fee_ppm, 1500);
    assert_eq!(hist.confidence, "low");

    assert_eq!(snap.peer_fee_history(&peer_a()), None);
    // Unknown peer: no channel_states rows -> None (py early return).
    assert_eq!(snap.peer_fee_history(&peer_c()), None);
}

#[test]
fn flow_window_batches_seven_day_directional_flow() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    let since = NOW - 7 * 86_400; // FLOW_BALANCED_WINDOW_SECONDS
    for (in_ch, out_ch, in_msat, out_msat, ts) in [
        (
            "900x9x0",
            "100x1x0",
            2_500_700_i64,
            2_500_700_i64,
            NOW - 3600,
        ),
        ("100x1x0", "900x9x0", 1_200_300, 1_200_300, NOW - 7200),
        ("900x9x0", "100x1x0", 9_000_000, 9_000_000, since), // boundary: strict > excludes
    ] {
        conn.execute(
            "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
             timestamp, resolved_time) VALUES (?1, ?2, ?3, ?4, 10, ?5, ?5)",
            rusqlite::params![in_ch, out_ch, in_msat, out_msat, ts],
        )
        .unwrap();
    }
    drop(conn);
    let snap = build(&path, None);

    // out: floor(2_500_700/1000)=2500; in: floor(1_200_300/1000)=1200.
    assert_eq!(
        snap.flow_window("100x1x0"),
        Some(FlowWindow {
            out_sats: 2500,
            in_sats: 1200
        })
    );
    // 900x9x0 only appears as counterparty rows; both directions non-zero.
    assert_eq!(
        snap.flow_window("900x9x0"),
        Some(FlowWindow {
            out_sats: 1200,
            in_sats: 2500
        })
    );
    assert_eq!(snap.flow_window("555x5x0"), None);
}

#[test]
fn policy_rows_parse_expire_and_default() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    conn.execute(
        "INSERT INTO peer_policies (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, \
         updated_at, fee_multiplier_min, fee_multiplier_max, expires_at) \
         VALUES (?1, 'static', 'disabled', 777, '[\"vip\"]', ?2, 0.5, 2.0, NULL)",
        rusqlite::params![peer_a(), NOW - 500],
    )
    .unwrap();
    // Expired policy: py get_policy reverts to defaults (and deletes the
    // row -- which this read-only surface must NOT do).
    conn.execute(
        "INSERT INTO peer_policies (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, \
         updated_at, expires_at) VALUES (?1, 'passive', 'enabled', NULL, NULL, ?2, ?3)",
        rusqlite::params![peer_b(), NOW - 500, NOW - 1],
    )
    .unwrap();
    // Invalid strategy string degrades to dynamic (py ValueError branch).
    conn.execute(
        "INSERT INTO peer_policies (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, \
         updated_at) VALUES (?1, 'bogus', 'enabled', NULL, 'not-json', ?2)",
        rusqlite::params![peer_c(), NOW - 500],
    )
    .unwrap();
    drop(conn);
    let snap = build(&path, None);

    let a = snap.policy(&peer_a()).expect("explicit policy");
    assert_eq!(a.strategy, FeeStrategy::Static);
    assert_eq!(a.rebalance_mode, RebalanceMode::Disabled);
    assert_eq!(a.fee_ppm_target, Some(777));
    assert_eq!(a.tags, vec!["vip".to_string()]);
    assert_eq!(a.updated_at, NOW - 500);
    assert_eq!(a.fee_multiplier_min, Some(0.5));
    assert_eq!(a.fee_multiplier_max, Some(2.0));
    assert_eq!(a.expires_at, None);

    // Expired -> the default policy, exactly like py get_policy.
    assert_eq!(
        snap.policy(&peer_b()),
        Some(PeerPolicy::default_for(peer_b()))
    );

    let c = snap.policy(&peer_c()).expect("degraded policy");
    assert_eq!(c.strategy, FeeStrategy::Dynamic);
    assert!(c.tags.is_empty()); // corrupt tags JSON -> []

    // No row at all -> default policy (py returns a default PeerPolicy,
    // never None -- the trait's None means "no policy manager").
    let unknown = format!("02{}", "dd".repeat(32));
    assert_eq!(
        snap.policy(&unknown),
        Some(PeerPolicy::default_for(unknown.clone()))
    );

    // Verify the expired row survived (read-only surface, no delete).
    let check = Connection::open(&path).unwrap();
    let count: i64 = check
        .query_row("SELECT COUNT(*) FROM peer_policies", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn channels_info_ports_get_channels_info_live() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    drop(conn);
    let snap = build(&path, None);

    let infos = snap.channels_info();
    // OPENINGD row dropped; both CHANNELD_NORMAL rows keyed by normalized
    // scid.
    assert_eq!(infos.len(), 2);

    let a = &infos["100x1x0"];
    assert_eq!(a.channel_id, "100x1x0");
    assert_eq!(a.short_channel_id, "100x1x0"); // colon form normalized
    assert_eq!(a.peer_id, peer_a());
    assert_eq!(a.capacity_sats, 2_000_000); // floor(2_000_000_000/1000)
    assert_eq!(a.spendable_msat, 1_000_000_000);
    assert_eq!(a.receivable_msat, 900_000_000);
    assert_eq!(a.fee_base_msat, 0); // updates.local wins
    assert_eq!(a.fee_proportional_millionths, 150);
    assert_eq!(a.htlc_minimum_msat, 1000);
    assert_eq!(a.htlc_maximum_msat, 1_980_000_000);
    assert_eq!(a.opener, "local");
    assert!(a.has_htlc_data);
    assert_eq!(a.max_accepted_htlcs, 483);
    assert_eq!(a.our_htlcs_in_flight, 2); // only direction=="out" rows

    let b = &infos["200x2x0"];
    // No total_msat/capacity_msat: spendable+receivable = 1_000_000_000.
    assert_eq!(b.capacity_sats, 1_000_000);
    assert_eq!(b.spendable_msat, 600_000_000); // "600000000msat" parsed
    assert_eq!(b.fee_base_msat, 1000); // top-level fallback
    assert_eq!(b.fee_proportional_millionths, 250);
    assert_eq!(b.opener, "remote");
    assert!(!b.has_htlc_data); // no htlcs array
    assert_eq!(b.our_htlcs_in_flight, 0);
    assert_eq!(b.max_accepted_htlcs, 483); // default
}

#[test]
fn node_channels_keeps_every_listpeerchannels_row() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    drop(conn);
    let snap = build(&path, None);

    // Unlike channels_info, the drain aggregate receives ALL rows (the
    // CHANNELD_NORMAL filter lives inside compute_node_receivable_ratio).
    let chans = snap.node_channels();
    assert_eq!(chans.len(), 3);
    assert_eq!(chans[0].state, "CHANNELD_NORMAL");
    assert_eq!(chans[0].to_us_msat, 1_100_000_000);
    assert_eq!(chans[0].total_msat, 2_000_000_000);
    assert_eq!(chans[2].state, "OPENINGD");
}

#[test]
fn marginal_roi_percent_matches_python_arithmetic() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    let since = NOW - 30 * 86_400;
    let since_day = (since / 86_400) * 86_400;
    // A completed day inside the window (>= since_day, < today_start).
    let day = ((NOW - 10 * 86_400) / 86_400) * 86_400;
    assert!(day >= since_day && day < (NOW / 86_400) * 86_400);

    // Direct revenue: live forward 4_000_500 msat + rollup 2_000_000 msat.
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
         timestamp, resolved_time) VALUES ('900x9x0', '100x1x0', 1000, 1000, 4000500, ?1, ?1)",
        rusqlite::params![NOW - 3600],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO daily_forwarding_stats (channel_id, date, total_in_msat, total_out_msat, \
         total_fee_msat, forward_count) VALUES ('100x1x0', ?1, 0, 0, 2000000, 3)",
        rusqlite::params![day],
    )
    .unwrap();
    // Sourced contribution (smaller; max() keeps direct revenue).
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
         timestamp, resolved_time) VALUES ('100x1x0', '999x9x9', 1000, 1000, 1000000, ?1, ?1)",
        rusqlite::params![NOW - 3700],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO daily_forwarding_stats_inbound (channel_id, date, total_in_msat, \
         total_fee_msat, forward_count) VALUES ('100x1x0', ?1, 0, 500000, 2)",
        rusqlite::params![day],
    )
    .unwrap();
    // 30d rebalance costs: cost_msat wins over cost_sats*1000.
    conn.execute(
        "INSERT INTO rebalance_costs (channel_id, peer_id, cost_sats, cost_msat, amount_sats, \
         timestamp) VALUES ('100x1x0', ?1, 9999, 3000400, 100000, ?2)",
        rusqlite::params![peer_a(), NOW - 4000],
    )
    .unwrap();
    drop(conn);
    let snap = build(&path, None);

    // revenue_msat = 4_000_500 + 2_000_000 = 6_000_500
    // sourced_fee_msat = 1_000_000 + 500_000 = 1_500_000
    // contribution = max(...) = 6_000_500
    // cost_msat = 3_000_400 -> cost_sats = ceil = 3001
    // marginal_profit_30d = toward_zero(6_000_500 - 3_000_400) = 3000 sats
    // marginal_roi = 3000 / max(1, 3001); percent = *100.
    let expected = 3000.0 / 3001.0 * 100.0;
    let roi = snap
        .marginal_roi_percent("100x1x0")
        .expect("profitability data");
    assert!(
        (roi - expected).abs() < 1e-12,
        "roi={roi} expected={expected}"
    );

    // Zero cost, zero profit -> 0.0 (py: cost<=0 and profit<=0).
    assert_eq!(snap.marginal_roi_percent("200x2x0"), Some(0.0));

    // Channels absent from listpeerchannels are absent from the analyzer
    // cache -> py get_profitability returns None -> "unknown".
    assert_eq!(snap.marginal_roi_percent("555x5x0"), None);
}

#[test]
fn marginal_roi_percent_zero_cost_with_profit_is_hundred() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
         timestamp, resolved_time) VALUES ('900x9x0', '200x2x0', 1000, 1000, 2500, ?1, ?1)",
        rusqlite::params![NOW - 3600],
    )
    .unwrap();
    drop(conn);
    let snap = build(&path, None);
    // No rebalance costs: py `if cost <= 0: return 1.0 if profit > 0` -> 100%.
    assert_eq!(snap.marginal_roi_percent("200x2x0"), Some(100.0));
}

#[test]
fn misc_owned_surface() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    drop(conn);
    let snap = build(&path, None);

    assert_eq!(snap.our_node_id(), format!("02{}", "ee".repeat(32)));
    // Production python never wires temporary_fee_overlay_active (the
    // FeeController ctor default None short-circuits py 4753) -> false.
    assert!(!snap.temporary_overlay_active("100x1x0"));
}

// ---------------------------------------------------------------------------
// Per-cycle freeze: no new DB rows observed after construction
// ---------------------------------------------------------------------------

#[test]
fn snapshot_is_frozen_against_concurrent_writes() {
    let (_dir, path, conn) = seeded_db();
    seed_channel_states(&conn);
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
         timestamp, resolved_time) VALUES ('900x9x0', '100x1x0', 1000000, 1000000, 100, ?1, ?1)",
        rusqlite::params![NOW - 100],
    )
    .unwrap();
    // Keep the writer OPEN across the snapshot build (live WAL writer,
    // like Python holding the production DB).
    let snap = build(&path, None);
    assert_eq!(snap.volume_since("100x1x0", NOW - 200), 1000);

    // Python keeps writing mid-cycle; the frozen snapshot must not see it.
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, \
         timestamp, resolved_time) VALUES ('900x9x0', '100x1x0', 5000000, 5000000, 100, ?1, ?1)",
        rusqlite::params![NOW - 50],
    )
    .unwrap();
    assert_eq!(snap.volume_since("100x1x0", NOW - 200), 1000);
    assert_eq!(snap.forward_count_since("100x1x0", NOW - 200), 1);
}

// ---------------------------------------------------------------------------
// prefetch_rpc over a mock lightning-rpc socket
// ---------------------------------------------------------------------------

/// Serve `n` sequential connections on a mock `lightning-rpc` socket,
/// answering by method name with `cln_rpc`'s `\n\n` framing (same mock
/// shape as `crates/revops/tests/config_resolve.rs`).
fn serve_methods(socket_path: PathBuf, feerates_fails: bool) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("bind mock rpc socket");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 8192];
            let req: Value = loop {
                let n = stream.read(&mut chunk).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&chunk[..n]);
                if let Ok(v) = serde_json::from_slice::<Value>(&buf) {
                    break v;
                }
            };
            let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
            let id = req.get("id").cloned().unwrap_or(Value::Null);
            let body = match method {
                "getinfo" => json!({"jsonrpc": "2.0", "id": id,
                    "result": {"id": format!("02{}", "ee".repeat(32))}}),
                "listpeerchannels" => json!({"jsonrpc": "2.0", "id": id,
                    "result": {"channels": [{"state": "CHANNELD_NORMAL",
                        "short_channel_id": "100x1x0", "peer_id": format!("02{}", "aa".repeat(32)),
                        "total_msat": 1_000_000_000_i64, "to_us_msat": 500_000_000_i64,
                        "spendable_msat": 400_000_000_i64, "receivable_msat": 500_000_000_i64}]}}),
                "listchannels" => json!({"jsonrpc": "2.0", "id": id,
                    "result": {"channels": [{"source": format!("02{}", "11".repeat(32)),
                        "destination": format!("02{}", "aa".repeat(32)), "active": true,
                        "fee_per_millionth": 42, "satoshis": 1_000_000_i64,
                        "last_update": 1_800_000_000_i64, "base_fee_millisatoshi": 0}]}}),
                "feerates" => {
                    if feerates_fails {
                        json!({"jsonrpc": "2.0", "id": id,
                            "error": {"code": -32601, "message": "feerates unavailable"}})
                    } else {
                        json!({"jsonrpc": "2.0", "id": id,
                            "result": {"perkb": {"opening": 15000}}})
                    }
                }
                other => json!({"jsonrpc": "2.0", "id": id,
                    "error": {"code": -32601, "message": format!("unknown method {other}")}}),
            };
            let mut out = serde_json::to_vec(&body).unwrap();
            out.extend_from_slice(b"\n\n");
            let _ = stream.write_all(&out).await;
        }
    });
}

#[tokio::test]
async fn prefetch_rpc_collects_all_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve_methods(socket.clone(), false);

    let pre = prefetch_rpc(&socket).await.expect("prefetch");
    assert_eq!(pre.our_node_id, format!("02{}", "ee".repeat(32)));
    assert_eq!(pre.peer_channels.len(), 1);
    assert_eq!(pre.gossip_channels.len(), 1);
    assert_eq!(
        pre.feerates
            .as_ref()
            .and_then(|f| f.pointer("/perkb/opening"))
            .and_then(Value::as_i64),
        Some(15000)
    );
}

#[tokio::test]
async fn prefetch_rpc_maps_feerates_failure_to_none() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("lightning-rpc");
    serve_methods(socket.clone(), true);

    // py _get_dynamic_chain_costs_live: any feerates error -> None (never
    // a cycle abort); getinfo/listpeerchannels/listchannels failures DO
    // abort the prefetch (Err) -- covered by construction here.
    let pre = prefetch_rpc(&socket)
        .await
        .expect("prefetch survives feerates failure");
    assert_eq!(pre.feerates, None);
}
