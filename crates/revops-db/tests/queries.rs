//! Integration tests for Phase 1b Task 5's read-query functions
//! (`revops_db::queries`), against a COPY of Phase 1a's committed
//! `fixtures/fixture.db` seeded with known rows via a raw `rusqlite`
//! connection.
//!
//! `fixtures/fixture.db` itself is Phase 1a's empty, schema-only artifact,
//! shared with `crates/revops-db/tests/adoption.rs` and
//! `crates/revops-db/tests/actor_wal.rs` -- it must never be mutated in
//! place. Every test here copies it into a fresh `tempdir()` first and
//! seeds rows into THAT copy, never the committed fixture.
//!
//! Expected values are hand-derived from the seeded rows by directly
//! applying the same SQL/arithmetic the `queries` module (and, ultimately,
//! `database.py`/`profitability_analyzer.py`) implements -- documented
//! inline at each assertion so the arithmetic is auditable without
//! re-deriving it.

use revops_db::actor::spawn_read_only;
use revops_db::queries::{
    closed_channels_summary, closure_costs_windows, lifetime_stats, pnl_summary,
};
use rusqlite::Connection;
use std::path::{Path, PathBuf};

/// Fixed "now" for every test -- an arbitrary point in time, not aligned to
/// a day boundary on purpose (exercises the `(now / 86400) * 86400`
/// day-bucketing arithmetic honestly rather than by coincidence).
const NOW: i64 = 1_800_000_000;

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db")
}

/// Copy the empty fixture DB into a fresh tempdir and seed it with known
/// rows via a raw connection. Returns the `TempDir` (keep it alive for the
/// duration of the test -- dropping it deletes the file) and the seeded
/// copy's path.
fn seeded_db(now: i64) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("seeded.db");
    std::fs::copy(fixture_path(), &path).expect("copy fixture.db");

    let conn = Connection::open(&path).expect("open seeded copy read-write");

    // -- forwards: two rows, both well inside every window this test
    // exercises (last few hours). `get_lifetime_stats`'s
    // `current_revenue_msat`/`current_forwards` queries have NO date
    // filter (they sum the whole table), so these count everywhere.
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            "1x1x0", "2x2x0", 1_000_000i64, 999_000i64, 1_000i64, now - 3600, now - 3595
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO forwards (in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            "1x1x0", "3x3x0", 2_000_000i64, 1_998_000i64, 2_000i64, now - 7200, now - 7100
        ],
    )
    .unwrap();
    // fee_msat sum = 3_000; out_msat sum = 2_997_000; count = 2.

    // -- lifetime_aggregates: `fixture.db` already carries the
    // Database.initialize()-seeded `id=1` row (all zeros) -- UPDATE it
    // rather than INSERT (which would violate the `id=1` CHECK/PK).
    conn.execute(
        "UPDATE lifetime_aggregates SET pruned_revenue_msat = ?1, pruned_forward_count = ?2, last_prune_timestamp = ?3 WHERE id = 1",
        rusqlite::params![50_000i64, 10i64, now - 100 * 86400],
    )
    .unwrap();

    // -- daily_forwarding_stats: one completed day, 5 days before `now`
    // (before `today_start`, inside every 7d/30d window this test uses).
    let five_days_ago = ((now - 5 * 86400) / 86400) * 86400;
    conn.execute(
        "INSERT INTO daily_forwarding_stats (channel_id, date, total_in_msat, total_out_msat, total_fee_msat, forward_count) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params!["1x1x0", five_days_ago, 5_000_000i64, 4_990_000i64, 5_000i64, 4i64],
    )
    .unwrap();

    // -- channel_costs (opening costs).
    conn.execute(
        "INSERT INTO channel_costs (channel_id, peer_id, open_cost_sats, capacity_sats, opened_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params!["2x2x0", "0".repeat(66), 500i64, 1_000_000i64, now - 90 * 86400],
    )
    .unwrap();

    // -- rebalance_costs: one row inside every window, cost_msat set (the
    // schema always carries this column -- see `total_rebalance_fees_since`'s
    // doc comment on why there's no legacy fallback here).
    conn.execute(
        "INSERT INTO rebalance_costs (channel_id, peer_id, cost_sats, cost_msat, amount_sats, timestamp) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params!["2x2x0", "1".repeat(66), 200i64, 200_000i64, 50_000i64, now - 3600],
    )
    .unwrap();

    // -- channel_closure_costs: one row, closed within the last 24h (so
    // included in every closure-cost window this test checks).
    conn.execute(
        "INSERT INTO channel_closure_costs \
         (channel_id, peer_id, close_type, closure_fee_sats, htlc_sweep_fee_sats, penalty_fee_sats, total_closure_cost_sats, closed_at, resolution_complete) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params!["3x3x0", "2".repeat(66), "mutual", 300i64, 0i64, 0i64, 300i64, now - 3600, 1i64],
    )
    .unwrap();

    // -- closed_channels: two rows for `get_closed_channels_summary`.
    conn.execute(
        "INSERT INTO closed_channels \
         (channel_id, peer_id, capacity_sats, opened_at, closed_at, close_type, open_cost_sats, closure_cost_sats, total_revenue_sats, total_rebalance_cost_sats, forward_count, net_pnl_sats, days_open) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        rusqlite::params![
            "4x4x0", "3".repeat(66), 2_000_000i64, now - 200 * 86400, now - 100 * 86400,
            "mutual", 400i64, 100i64, 900i64, 50i64, 20i64, 350i64, 100i64
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO closed_channels \
         (channel_id, peer_id, capacity_sats, opened_at, closed_at, close_type, open_cost_sats, closure_cost_sats, total_revenue_sats, total_rebalance_cost_sats, forward_count, net_pnl_sats, days_open) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        rusqlite::params![
            "5x5x0", "4".repeat(66), 1_000_000i64, now - 150 * 86400, now - 50 * 86400,
            "remote_unilateral", 400i64, 200i64, 300i64, 20i64, 5i64, -320i64, 50i64
        ],
    )
    .unwrap();

    drop(conn);
    (dir, path)
}

#[tokio::test]
async fn lifetime_stats_matches_hand_derived_totals() {
    let (_dir, path) = seeded_db(NOW);
    let handle = spawn_read_only(&path).await.unwrap();
    let stats = lifetime_stats(&handle, NOW).await.unwrap();

    // total_revenue_msat = pruned(50_000) + rollup(5_000, the one
    // daily_forwarding_stats row, since its date < today_start) +
    // current(3_000, unconditional SUM(fee_msat) over `forwards`) = 58_000.
    assert_eq!(stats.total_revenue_msat, 58_000);
    assert_eq!(stats.total_rebalance_cost_sats, 200);
    assert_eq!(stats.total_opening_cost_sats, 500);
    assert_eq!(stats.total_closure_cost_sats, 300);
    // total_forwards = pruned(10) + rollup(4) + current(2) = 16.
    assert_eq!(stats.total_forwards, 16);
}

#[tokio::test]
async fn closed_channels_summary_matches_hand_derived_totals() {
    let (_dir, path) = seeded_db(NOW);
    let handle = spawn_read_only(&path).await.unwrap();
    let summary = closed_channels_summary(&handle).await.unwrap();

    assert_eq!(summary.channel_count, 2);
    assert_eq!(summary.total_capacity, 3_000_000);
    assert_eq!(summary.total_open_costs, 800);
    assert_eq!(summary.total_closure_costs, 300);
    assert_eq!(summary.total_revenue, 1_200);
    assert_eq!(summary.total_rebalance_costs, 70);
    assert_eq!(summary.total_forwards, 25);
    assert_eq!(summary.total_net_pnl, 30);
    // avg_days_open = (100 + 50) / 2 = 75.0
    assert!((summary.avg_days_open - 75.0).abs() < 1e-9);
}

#[tokio::test]
async fn closed_channels_summary_on_empty_table_is_all_zero() {
    // No closed_channels rows inserted -- COALESCE/AVG-over-no-rows path
    // (Python: `COALESCE(AVG(days_open), 0)` -> 0 when the table is empty).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();
    let summary = closed_channels_summary(&handle).await.unwrap();

    assert_eq!(summary.channel_count, 0);
    assert_eq!(summary.total_capacity, 0);
    assert_eq!(summary.total_net_pnl, 0);
    assert_eq!(summary.avg_days_open, 0.0);
}

#[tokio::test]
async fn closure_costs_windows_all_windows_include_recent_closure() {
    let (_dir, path) = seeded_db(NOW);
    let handle = spawn_read_only(&path).await.unwrap();
    let windows = closure_costs_windows(&handle, NOW).await.unwrap();

    // The one channel_closure_costs row closed 1h ago -- inside all three
    // windows and the unconditional total.
    assert_eq!(windows.last_24h_sats, 300);
    assert_eq!(windows.last_7d_sats, 300);
    assert_eq!(windows.last_30d_sats, 300);
    assert_eq!(windows.total_sats, 300);
}

#[tokio::test]
async fn pnl_summary_30d_matches_hand_derived_totals() {
    let (_dir, path) = seeded_db(NOW);
    let handle = spawn_read_only(&path).await.unwrap();
    let pnl = pnl_summary(&handle, 30, NOW).await.unwrap();

    assert_eq!(pnl.window_days, 30);
    // gross_revenue_msat = forwards(3_000) + rollup(5_000) = 8_000 -> ceil
    // to 8 sats (exact multiple of 1000, so ceil == floor here).
    assert_eq!(pnl.gross_revenue_sats, 8);
    // volume_msat = forwards(999_000+1_998_000=2_997_000) +
    // rollup(4_990_000) = 7_987_000 -> floor to 7987 sats.
    assert_eq!(pnl.volume_sats, 7_987);
    // forward_count = forwards(2) + rollup(4) = 6.
    assert_eq!(pnl.forward_count, 6);
    // rebalance: 200_000 msat -> ceil to 200 sats.
    assert_eq!(pnl.rebalance_cost_sats, 200);
    assert_eq!(pnl.closure_cost_sats, 300);
    assert_eq!(pnl.opex_sats, 500);
    assert_eq!(pnl.net_profit_sats, -492);
    // round((-492 / 8) * 100, 2) = round(-6150.0, 2) = -6150.0
    assert!((pnl.operating_margin_pct - (-6150.0)).abs() < 1e-9);
}

#[tokio::test]
async fn pnl_summary_clamps_window_days_below_one() {
    let (_dir, path) = seeded_db(NOW);
    let handle = spawn_read_only(&path).await.unwrap();
    let pnl = pnl_summary(&handle, 0, NOW).await.unwrap();
    assert_eq!(pnl.window_days, 1);

    let pnl_negative = pnl_summary(&handle, -30, NOW).await.unwrap();
    assert_eq!(pnl_negative.window_days, 1);
}

#[tokio::test]
async fn pnl_summary_on_empty_db_is_zero_revenue_zero_margin() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();
    let pnl = pnl_summary(&handle, 30, NOW).await.unwrap();

    assert_eq!(pnl.gross_revenue_sats, 0);
    assert_eq!(pnl.opex_sats, 0);
    assert_eq!(pnl.net_profit_sats, 0);
    // No revenue, no opex -> margin is 0.0 (Python: "no revenue - margin is
    // undefined, use 0 if no costs").
    assert_eq!(pnl.operating_margin_pct, 0.0);
}
