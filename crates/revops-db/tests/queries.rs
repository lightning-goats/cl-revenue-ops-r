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
    active_spend_reservations, all_config_overrides, all_policies, closed_channels_summary,
    closure_costs_windows, config_override, cost_evidence_coverage,
    hot_channel_protection_override_peers, last_policy_change_timestamp, lifetime_stats,
    opening_costs_since, planner_actions, planner_candidates, pnl_summary, policies_by_tag,
    policy_changes_since, policy_for_peer, rebalance_spend_component, spend_ledger_aggregates,
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

/// Port of `Database.get_opening_costs_since` (database.py:6334-6350):
/// `SUM(open_cost_sats) FROM channel_costs WHERE opened_at >= since`. The
/// seeded copy carries one 90-day-old open (500 sats); a second recent row
/// makes the window boundary discriminating in both directions.
#[tokio::test]
async fn opening_costs_since_windows_on_opened_at() {
    let (_dir, path) = seeded_db(NOW);
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO channel_costs (channel_id, peer_id, open_cost_sats, capacity_sats, opened_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params!["6x6x0", "5".repeat(66), 250i64, 2_000_000i64, NOW - 1800],
    )
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&path).await.unwrap();

    // Both rows inside a 91-day window.
    assert_eq!(
        opening_costs_since(&handle, NOW - 91 * 86400)
            .await
            .unwrap(),
        750
    );
    // Only the 30-minute-old row inside the last day.
    assert_eq!(
        opening_costs_since(&handle, NOW - 86400).await.unwrap(),
        250
    );
    // `opened_at >= since` is inclusive-of-boundary, exclusive of older rows.
    assert_eq!(opening_costs_since(&handle, NOW - 1800).await.unwrap(), 250);
    assert_eq!(opening_costs_since(&handle, NOW).await.unwrap(), 0);
}

/// Port of `Database.get_daily_rebalance_spend`'s budget-component subset
/// (database.py:4590-4678): spend from `rebalance_costs`, reserved from
/// ACTIVE `budget_reservations` PLUS active `spend_reservations` rows under
/// `category='rebalance'` (Phase 2J unified holds), job/success counts from
/// `rebalance_history` — all windowed on `now - window_hours*3600`.
#[tokio::test]
async fn rebalance_spend_component_sums_both_reservation_tables_and_windows() {
    let (_dir, path) = seeded_db(NOW);
    let conn = Connection::open(&path).unwrap();
    // Second rebalance_costs row OUTSIDE the 24h window: must not count on
    // top of the seeded in-window 200-sat row.
    conn.execute(
        "INSERT INTO rebalance_costs (channel_id, peer_id, cost_sats, cost_msat, amount_sats, timestamp) \
         VALUES ('2x2x0', ?1, 999, 999000, 10000, ?2)",
        rusqlite::params!["1".repeat(66), NOW - 2 * 86400],
    )
    .unwrap();
    // Legacy budget_reservations: one countable active hold; a released row
    // and an active-but-stale row prove the status AND window filters.
    conn.execute(
        "INSERT INTO budget_reservations (reservation_id, reserved_sats, reserved_at, job_channel_id, status) VALUES \
         ('br-1', 100, ?1, '2x2x0', 'active'), \
         ('br-2', 999, ?1, '2x2x0', 'released'), \
         ('br-3', 555, ?2, '2x2x0', 'active')",
        rusqlite::params![NOW - 1800, NOW - 2 * 86400],
    )
    .unwrap();
    // Unified holds: only category='rebalance' AND status='active' counts.
    conn.execute(
        "INSERT INTO spend_reservations (reservation_id, category, reserved_sats, reserved_at, status) VALUES \
         ('sr-1', 'rebalance', 40, ?1, 'active'), \
         ('sr-2', 'channel_open', 70, ?1, 'active'), \
         ('sr-3', 'rebalance', 80, ?1, 'spent')",
        rusqlite::params![NOW - 900],
    )
    .unwrap();
    // History: 2 success + 1 failed + 1 pending in-window (job_count counts
    // every attempt), one out-of-window success excluded.
    conn.execute(
        "INSERT INTO rebalance_history (from_channel, to_channel, amount_sats, max_fee_sats, expected_profit_sats, status, timestamp) VALUES \
         ('1x1x0','2x2x0',1000,10,5,'success',?1), \
         ('1x1x0','2x2x0',1000,10,5,'success',?1), \
         ('1x1x0','2x2x0',1000,10,5,'failed',?1), \
         ('1x1x0','2x2x0',1000,10,5,'pending',?1), \
         ('1x1x0','2x2x0',1000,10,5,'success',?2)",
        rusqlite::params![NOW - 3600, NOW - 2 * 86400],
    )
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&path).await.unwrap();

    let component = rebalance_spend_component(&handle, 24, NOW).await.unwrap();
    assert_eq!(component.total_spent_sats, 200);
    assert_eq!(
        component.total_reserved_sats, 140,
        "100 legacy + 40 unified"
    );
    assert_eq!(component.job_count, 4);
    assert_eq!(component.success_count, 2);
}

#[tokio::test]
async fn rebalance_spend_component_on_empty_tables_is_all_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();
    let component = rebalance_spend_component(&handle, 24, NOW).await.unwrap();
    assert_eq!(component.total_spent_sats, 0);
    assert_eq!(component.total_reserved_sats, 0);
    assert_eq!(component.job_count, 0);
    assert_eq!(component.success_count, 0);
}

/// Port of `Database.get_cost_evidence_coverage` (database.py:4465-4481):
/// earliest evidence across SEVEN sources (`_TOTAL_COST_EVIDENCE_SOURCES`,
/// database.py:4410-4420), then the same honest coverage math the
/// spend-ledger uses. With no evidence anywhere: unknown, never a
/// fabricated "complete".
#[tokio::test]
async fn cost_evidence_coverage_on_empty_db_is_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();
    let coverage = cost_evidence_coverage(&handle, 24, NOW).await.unwrap();
    assert_eq!(coverage.covered_hours, None);
    assert_eq!(coverage.coverage_status, "unknown");
}

/// Evidence ONLY in `channel_costs` — a source the spend-ledger scan does
/// NOT read. A scan limited to `_SPEND_LEDGER_EVIDENCE_SOURCES` would
/// answer "unknown" here; the total-cost scan must answer "complete".
#[tokio::test]
async fn cost_evidence_coverage_reads_beyond_the_spend_ledger_sources() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seed.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO channel_costs (channel_id, peer_id, open_cost_sats, capacity_sats, opened_at) \
         VALUES ('2x2x0', ?1, 500, 1000000, ?2)",
        rusqlite::params!["0".repeat(66), NOW - 90 * 86400],
    )
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&path).await.unwrap();
    let coverage = cost_evidence_coverage(&handle, 24, NOW).await.unwrap();
    assert_eq!(coverage.covered_hours, Some(24.0));
    assert_eq!(coverage.coverage_status, "complete");
}

/// Evidence younger than the window: measured hours, rounded to 2 places
/// (Python `round(span/3600.0, 2)`), status "partial".
#[tokio::test]
async fn cost_evidence_coverage_measures_partial_hours() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seed.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO rebalance_history (from_channel, to_channel, amount_sats, max_fee_sats, expected_profit_sats, status, timestamp) \
         VALUES ('1x1x0','2x2x0',1000,10,5,'success',?1)",
        rusqlite::params![NOW - 5400],
    )
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&path).await.unwrap();
    let coverage = cost_evidence_coverage(&handle, 24, NOW).await.unwrap();
    assert_eq!(coverage.covered_hours, Some(1.5));
    assert_eq!(coverage.coverage_status, "partial");
}

/// Python's non-positive filter operates on each source's MIN, not per row
/// (`if ts <= 0: continue`, database.py:4441-4442): an epoch-zero row makes
/// that WHOLE source's minimum non-positive, so the source is skipped even
/// though a genuine newer row exists in the same table. With no other
/// source, the honest answer is unknown — never evidence dated 1970, which
/// would fake a "complete" window.
#[tokio::test]
async fn cost_evidence_coverage_skips_a_source_whose_min_is_epoch_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seed.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "INSERT INTO rebalance_history (from_channel, to_channel, amount_sats, max_fee_sats, expected_profit_sats, status, timestamp) \
         VALUES ('1x1x0','2x2x0',1000,10,5,'success',?1), \
                ('1x1x0','2x2x0',1000,10,5,'success',0)",
        rusqlite::params![NOW - 5400],
    )
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&path).await.unwrap();
    let coverage = cost_evidence_coverage(&handle, 24, NOW).await.unwrap();
    assert_eq!(coverage.covered_hours, None);
    assert_eq!(coverage.coverage_status, "unknown");
}

/// Python skips a source whose query raises (`except sqlite3.Error:
/// continue`) instead of failing the whole read. Drop one source table from
/// the copy and the remaining six must still answer.
#[tokio::test]
async fn cost_evidence_coverage_tolerates_a_missing_source_table() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seed.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("DROP TABLE budget_reservations;")
        .unwrap();
    conn.execute(
        "INSERT INTO channel_closure_costs \
         (channel_id, peer_id, close_type, closure_fee_sats, htlc_sweep_fee_sats, penalty_fee_sats, total_closure_cost_sats, closed_at, resolution_complete) \
         VALUES ('3x3x0', ?1, 'mutual', 300, 0, 0, 300, ?2, 1)",
        rusqlite::params!["2".repeat(66), NOW - 30 * 86400],
    )
    .unwrap();
    drop(conn);
    let handle = spawn_read_only(&path).await.unwrap();
    let coverage = cost_evidence_coverage(&handle, 24, NOW).await.unwrap();
    assert_eq!(coverage.covered_hours, Some(24.0));
    assert_eq!(coverage.coverage_status, "complete");
}

/// Python's `COALESCE(SUM(...), 0)`: an empty table answers 0, not an error.
#[tokio::test]
async fn opening_costs_since_on_empty_table_is_zero() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();
    assert_eq!(opening_costs_since(&handle, 0).await.unwrap(), 0);
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

/// `config_override` port of `Database.get_config_override`
/// (modules/database.py:7316-7322) -- layer (a) of `revenue-r-config`'s
/// resolution order (`revops::config_resolve`). Seeds a row directly via
/// the same `config_overrides` schema `Database.set_config_override`
/// writes to, keyed by the Python `Config` field name (snake_case), not
/// the CLN option suffix.
#[tokio::test]
async fn config_override_returns_seeded_value() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("overrides.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO config_overrides (key, value, version, updated_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["min_fee_ppm", "40", 1i64, NOW],
        )
        .unwrap();
    }
    let handle = spawn_read_only(&path).await.unwrap();
    assert_eq!(
        config_override(&handle, "min_fee_ppm").await.unwrap(),
        Some("40".to_string())
    );
}

/// No row for `key` -> `Ok(None)`, never an `Err` -- the common case (most
/// settings are never overridden via `revenue-config set`).
#[tokio::test]
async fn config_override_returns_none_when_absent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("no_overrides.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();
    assert_eq!(config_override(&handle, "min_fee_ppm").await.unwrap(), None);
}

/// `all_config_overrides` port of `Database.get_all_config_overrides`
/// (modules/database.py:7307-7315). The flow pass needs FIVE keys per
/// cycle; reading them one at a time would take five independent WAL
/// snapshots, and Python's plugin can commit a write between any two of
/// them. That is not theoretical here: `source_threshold` and
/// `sink_threshold` are a validated PAIR (config.py:1143-1146), so a torn
/// read across an operator's paired update yields an inverted band that
/// Python itself would have rejected at write time. One statement, one
/// snapshot.
#[tokio::test]
async fn all_config_overrides_reads_every_row_in_one_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all_overrides.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        for (i, (key, value)) in [
            ("source_threshold", "0.25"),
            ("sink_threshold", "-0.3"),
            // Sentinel rows are filtered by Python's own comprehension.
            ("_lnplus_breaker", "1800000000: tripped"),
        ]
        .iter()
        .enumerate()
        {
            conn.execute(
                "INSERT INTO config_overrides (key, value, version, updated_at) \
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![key, value, i as i64 + 1, NOW],
            )
            .unwrap();
        }
    }
    let handle = spawn_read_only(&path).await.unwrap();
    let all = all_config_overrides(&handle).await.unwrap();

    assert_eq!(
        all.get("source_threshold").map(String::as_str),
        Some("0.25")
    );
    assert_eq!(all.get("sink_threshold").map(String::as_str), Some("-0.3"));
    assert!(
        !all.contains_key("_lnplus_breaker"),
        "Python filters `_`-prefixed sentinel rows out of the override map; \
         leaking one here would offer an internal breaker marker to \
         `hasattr`-style key matching"
    );
    assert_eq!(all.len(), 2);
}

/// An empty `config_overrides` table is `Ok({})`, never an error: no
/// override is the normal state of a fresh node.
#[tokio::test]
async fn all_config_overrides_is_empty_not_an_error_when_nothing_is_overridden() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty_overrides.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();
    assert!(all_config_overrides(&handle).await.unwrap().is_empty());
}

/// A different key's override row must not leak into an unrelated lookup
/// -- confirms the query is keyed by `key`, not "any row present".
#[tokio::test]
async fn config_override_does_not_leak_across_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("other_key.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO config_overrides (key, value, version, updated_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["daily_budget_sats", "1000", 1i64, NOW],
        )
        .unwrap();
    }
    let handle = spawn_read_only(&path).await.unwrap();
    assert_eq!(config_override(&handle, "min_fee_ppm").await.unwrap(), None);
    assert_eq!(
        config_override(&handle, "daily_budget_sats").await.unwrap(),
        Some("1000".to_string())
    );
}

#[tokio::test]
async fn planner_candidates_match_python_filter_order_limit_and_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("planner-candidates.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        let rows = [
            (
                "peer-high",
                7.0,
                "gossip",
                300i64,
                Some(3_000_000i64),
                4i64,
                1i64,
                Some(r#"{"rank":1}"#),
            ),
            ("peer-mid", 3.0, "manual", 200i64, None, 2i64, 0i64, None),
            (
                "peer-low",
                -2.0,
                "gossip",
                100i64,
                Some(1_000_000i64),
                0i64,
                5i64,
                Some(r#"{"rank":3}"#),
            ),
        ];
        for (peer, score, source, evaluated, capacity, successes, failures, metadata) in rows {
            conn.execute(
                "INSERT INTO planner_candidates
                 (peer_id, score, source, last_evaluated,
                  capacity_recommendation_sats, connect_successes,
                  connect_failures, metadata_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    peer, score, source, evaluated, capacity, successes, failures, metadata
                ],
            )
            .unwrap();
        }
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let all = planner_candidates(&handle, -999.0, None, 2).await.unwrap();
    assert_eq!(
        all.iter().map(|r| r.peer_id.as_str()).collect::<Vec<_>>(),
        vec!["peer-high", "peer-mid"],
        "Python orders score descending before applying LIMIT"
    );
    assert_eq!(all[0].capacity_recommendation_sats, Some(3_000_000));
    assert_eq!(all[0].connect_successes, 4);
    assert_eq!(all[0].metadata_json.as_deref(), Some(r#"{"rank":1}"#));
    assert_eq!(all[1].capacity_recommendation_sats, None);
    assert_eq!(all[1].metadata_json, None);

    let gossip = planner_candidates(&handle, -2.0, Some("gossip"), 10)
        .await
        .unwrap();
    assert_eq!(
        gossip
            .iter()
            .map(|r| (r.peer_id.as_str(), r.score))
            .collect::<Vec<_>>(),
        vec![("peer-high", 7.0), ("peer-low", -2.0)],
        "min_score is inclusive and source filtering happens before ordering"
    );
}

#[tokio::test]
async fn planner_actions_match_python_newest_first_limit_and_null_shape() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("planner-actions.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO planner_actions
             (action_type, peer_id, channel_id, amount_sats,
              estimated_cost_sats, actual_cost_sats, status, created_at,
              completed_at, reason, metadata_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                "open",
                "peer-old",
                Option::<String>::None,
                Some(1_000_000i64),
                Some(5_000i64),
                Option::<i64>::None,
                "planned",
                100i64,
                Option::<i64>::None,
                Option::<String>::None,
                Option::<String>::None,
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO planner_actions
             (action_type, peer_id, channel_id, amount_sats,
              estimated_cost_sats, actual_cost_sats, status, created_at,
              completed_at, reason, metadata_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            rusqlite::params![
                "close",
                "peer-new",
                Some("1x2x3"),
                Option::<i64>::None,
                Some(700i64),
                Some(650i64),
                "completed",
                200i64,
                Some(210i64),
                Some("underwater"),
                Some(r#"{"forced":true}"#),
            ],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let actions = planner_actions(&handle, None, 1).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].action_type, "close");
    assert_eq!(actions[0].peer_id, "peer-new");
    assert_eq!(actions[0].channel_id.as_deref(), Some("1x2x3"));
    assert_eq!(actions[0].amount_sats, None);
    assert_eq!(actions[0].actual_cost_sats, Some(650));
    assert_eq!(actions[0].completed_at, Some(210));
    assert_eq!(actions[0].reason.as_deref(), Some("underwater"));
    assert_eq!(
        actions[0].metadata_json.as_deref(),
        Some(r#"{"forced":true}"#)
    );

    let completed = planner_actions(&handle, Some("completed"), 20)
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].status, "completed");
}

#[tokio::test]
async fn policy_list_orders_rows_and_uses_python_decode_fallbacks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policies.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        let rows = [
            (
                "peer-a",
                "dynamic",
                "enabled",
                "[\"vip\",\"banned\"]",
                400i64,
            ),
            ("peer-b", "static", "source_only", "[\"vip\"]", 300i64),
            (
                "peer-c",
                "invalid-strategy",
                "invalid-mode",
                "{\"not\":\"an array\"}",
                200i64,
            ),
            ("peer-d", "passive", "disabled", "not-json", 100i64),
        ];
        for (peer_id, strategy, mode, tags, updated_at) in rows {
            conn.execute(
                "INSERT INTO peer_policies
                 (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at,
                  fee_multiplier_min, fee_multiplier_max, expires_at)
                 VALUES (?1, ?2, ?3, NULL, ?4, ?5, NULL, NULL, NULL)",
                rusqlite::params![peer_id, strategy, mode, tags, updated_at],
            )
            .unwrap();
        }
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let policies = all_policies(&handle, NOW).await.unwrap();

    assert_eq!(
        policies
            .iter()
            .map(|p| p.peer_id.as_str())
            .collect::<Vec<_>>(),
        vec!["peer-a", "peer-b", "peer-c", "peer-d"]
    );
    assert_eq!(policies[0].tags, vec!["vip", "banned"]);
    assert_eq!(policies[1].strategy.as_value(), "static");
    assert_eq!(policies[1].rebalance_mode.as_value(), "source_only");
    assert_eq!(policies[2].strategy.as_value(), "dynamic");
    assert_eq!(policies[2].rebalance_mode.as_value(), "enabled");
    assert!(policies[2].tags.is_empty(), "non-array JSON defaults empty");
    assert!(policies[3].tags.is_empty(), "malformed JSON defaults empty");
}

#[tokio::test]
async fn policy_for_peer_returns_the_row_or_an_exact_peer_default() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy-get.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at,
              fee_multiplier_min, fee_multiplier_max, expires_at)
             VALUES ('configured', 'static', 'disabled', 321, '[\"vip\"]', 77,
                     0.5, 2.0, 2000000000)",
            [],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let configured = policy_for_peer(&handle, "configured", NOW).await.unwrap();
    assert_eq!(configured.strategy.as_value(), "static");
    assert_eq!(configured.fee_ppm_target, Some(321));
    assert_eq!(configured.tags, vec!["vip"]);
    assert_eq!(configured.fee_multiplier_min, Some(0.5));
    assert_eq!(configured.expires_at, Some(2_000_000_000));

    let missing = policy_for_peer(&handle, "missing-peer", NOW).await.unwrap();
    assert_eq!(missing.peer_id, "missing-peer");
    assert_eq!(missing.strategy.as_value(), "dynamic");
    assert_eq!(missing.rebalance_mode.as_value(), "enabled");
    assert_eq!(missing.updated_at, 0);
    assert!(missing.tags.is_empty());
}

#[tokio::test]
async fn policy_reads_exclude_expired_rows_using_python_strict_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy-expiry.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at)
             VALUES ('expired', 'static', 'disabled', '[]', 2, ?1)",
            [NOW - 1],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at)
             VALUES ('boundary', 'static', 'disabled', '[]', 1, ?1)",
            [NOW],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let active = all_policies(&handle, NOW).await.unwrap();
    assert_eq!(
        active
            .iter()
            .map(|p| p.peer_id.as_str())
            .collect::<Vec<_>>(),
        vec!["boundary"],
        "now == expires_at is not expired; now > expires_at is expired"
    );

    let expired = policy_for_peer(&handle, "expired", NOW).await.unwrap();
    assert_eq!(expired.peer_id, "expired");
    assert_eq!(expired.strategy.as_value(), "dynamic");
    assert_eq!(expired.updated_at, 0);
}

#[tokio::test]
async fn policy_by_tag_matches_exact_active_tags_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy-tags.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        let rows = [
            ("exact", "[\"vip\"]", None),
            ("similar", "[\"vip-extra\"]", None),
            ("expired", "[\"vip\"]", Some(NOW - 1)),
        ];
        for (peer_id, tags, expires_at) in rows {
            conn.execute(
                "INSERT INTO peer_policies
                 (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at)
                 VALUES (?1, 'dynamic', 'enabled', ?2, 1, ?3)",
                rusqlite::params![peer_id, tags, expires_at],
            )
            .unwrap();
        }
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let tagged = policies_by_tag(&handle, "vip", NOW).await.unwrap();
    assert_eq!(
        tagged
            .iter()
            .map(|p| p.peer_id.as_str())
            .collect::<Vec<_>>(),
        vec!["exact"]
    );
}

#[tokio::test]
async fn policy_changes_are_strict_active_and_last_timestamp_is_raw_max() {
    let empty_dir = tempfile::tempdir().unwrap();
    let empty_path = empty_dir.path().join("policy-last-empty.db");
    std::fs::copy(fixture_path(), &empty_path).unwrap();
    let empty_handle = spawn_read_only(&empty_path).await.unwrap();
    assert_eq!(
        last_policy_change_timestamp(&empty_handle).await.unwrap(),
        0
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy-changes.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        let rows = [
            ("at-boundary", 100i64, None),
            ("active", 200i64, None),
            ("expired-newest", 300i64, Some(NOW - 1)),
        ];
        for (peer_id, updated_at, expires_at) in rows {
            conn.execute(
                "INSERT INTO peer_policies
                 (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at)
                 VALUES (?1, 'dynamic', 'enabled', '[]', ?2, ?3)",
                rusqlite::params![peer_id, updated_at, expires_at],
            )
            .unwrap();
        }
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let changes = policy_changes_since(&handle, 100, NOW).await.unwrap();
    assert_eq!(
        changes
            .iter()
            .map(|p| p.peer_id.as_str())
            .collect::<Vec<_>>(),
        vec!["active"],
        "updated_at is strict greater-than and expired changes are omitted"
    );
    assert_eq!(
        last_policy_change_timestamp(&handle).await.unwrap(),
        300,
        "Python MAX includes the newest DB row even when it is expired"
    );
}

/// Task 50 correction round, F10 (SCOPE-UPDATED to a FIX): a malformed
/// scalar column (here, `expires_at` holding the TEXT `'not-an-integer'`
/// in an INTEGER-affinity column -- SQLite's dynamic typing allows this)
/// must NOT make the whole row vanish. Python's `_row_to_policy`
/// (policy_manager.py:395-422) never validates/defaults scalar columns at
/// all -- `row['peer_id']`/`row['updated_at']`/etc. are returned as
/// whatever's stored, unchecked -- so a "corrupt" row in Python still
/// shows up in every read, just possibly carrying a garbage value in one
/// field. The OLD Rust decode (`row.get::<_, T>(i)?` with a strict target
/// type, `.ok()`-dropped on the FIRST column that failed to convert) threw
/// the ENTIRE row away instead: security-relevant, because a banned peer
/// with one malformed cell would silently vanish from
/// `revenue-r-list-banned`.
///
/// This test pins the PREFERRED fix (keep-with-defaults, per the audit's
/// §2.6): every column decodes leniently (SQLite storage class -> the
/// target Rust type, defaulting on anything that can't coerce) so the row
/// is ALWAYS kept. `expires_at` specifically defaults to `None` on
/// unparseable garbage -- for a policy row, "no expiry" is the fail-safe
/// reading (a banned/tagged peer stays visible rather than silently reads
/// as instantly-expired). Round-2 correction, P2: this is a DELIBERATE
/// FAIL-SAFE DIVERGENCE, not a "Python-exact" port -- Python's own
/// `_row_to_policy` (policy_manager.py:384-439) does NOT generally coerce
/// malformed scalar column types; it returns `row['peer_id']`,
/// `row['updated_at']`, etc. exactly as SQLite stored them, unchecked, and
/// only wraps the tags-JSON decode and the two enum conversions in
/// try/except-with-default. Coercing every scalar column too is a
/// Rust-side strengthening chosen to guarantee no row is ever silently
/// dropped, not a claim that Python does the same coercion.
#[tokio::test]
async fn corrupt_scalar_column_is_kept_with_defaults_not_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy-corrupt-row.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at)
             VALUES ('valid', 'dynamic', 'enabled', '[]', 2, NULL)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at)
             VALUES ('corrupt', 'static', 'disabled', '[\"banned\"]', 3, 'not-an-integer')",
            [],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let policies = all_policies(&handle, NOW).await.unwrap();
    let ids: Vec<&str> = policies.iter().map(|p| p.peer_id.as_str()).collect();
    assert!(
        ids.contains(&"corrupt"),
        "a row with one malformed scalar column must NOT vanish from the read \
         (fail-open on a security-relevant surface): {ids:?}"
    );
    assert!(ids.contains(&"valid"), "{ids:?}");
    assert_eq!(ids.len(), 2, "{ids:?}");

    let corrupt_row = policies
        .iter()
        .find(|p| p.peer_id == "corrupt")
        .expect("the corrupt row must be present");
    // The GOOD columns on the corrupt row must still decode correctly --
    // only the genuinely-malformed column falls back to a default.
    assert_eq!(corrupt_row.strategy.as_value(), "static");
    assert_eq!(corrupt_row.rebalance_mode.as_value(), "disabled");
    assert_eq!(corrupt_row.updated_at, 3);
    assert_eq!(corrupt_row.tags, vec!["banned".to_string()]);
    // The malformed `expires_at` defaults to `None` (never expires) --
    // the fail-safe reading for a security-relevant field.
    assert_eq!(corrupt_row.expires_at, None);
    assert!(!corrupt_row.is_expired(NOW), "must not read as expired");

    // `policy_for_peer` must return the REAL row (with its real
    // updated_at/strategy), not the synthetic peer-default fallback the
    // OLD drop-then-default behavior produced.
    let corrupt = policy_for_peer(&handle, "corrupt", NOW).await.unwrap();
    assert_eq!(corrupt.peer_id, "corrupt");
    assert_eq!(corrupt.strategy.as_value(), "static");
    assert_eq!(
        corrupt.updated_at, 3,
        "must be the REAL row's updated_at, not the synthetic default's 0"
    );
}

/// Round-2 correction, CRITICAL (F10 was fixed at the ROW level; a
/// mixed-type `tags` ARRAY is a different, still-open hole): valid SQLite
/// JSON like `["banned", 7]` is a legal Python list -- `json.loads` returns
/// it unchanged, and Python's `"banned" in tags` membership test still
/// finds `"banned"` even with the non-string `7` sitting next to it. The
/// OLD decode parsed the WHOLE tags column as `Vec<String>` via serde's
/// typed array deserializer, which fails the instant ANY element isn't a
/// JSON string; `.unwrap_or_default()` then replaced the ENTIRE array with
/// `[]` -- silently erasing the valid `"banned"` tag alongside its one
/// malformed sibling. Filtered through `queries::all_policies` +
/// `PeerPolicy::has_tag`/`policies_by_tag`, that means a banned peer with
/// one stray non-string tag element vanishes from
/// `revenue-r-list-banned`/`policies_by_tag("banned", ...)` -- recreating
/// exactly the "banned peer disappears" failure F10 was meant to close, one
/// layer down (the element level, not the row level).
///
/// This is captured RED-first against unmodified `2b3d356`: at the time
/// this test is added, `decode_policy_row`'s tags decode still parses the
/// whole array as `Vec<String>`, so this assertion fails.
#[tokio::test]
async fn mixed_type_tags_array_preserves_valid_string_members_not_dropped_wholesale() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy-mixed-tags.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, tags, updated_at)
             VALUES ('mixed-tag-peer', 'dynamic', 'enabled', '[\"banned\", 7]', 5)",
            [],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();

    // `all_policies` must still return the row, carrying the STRING tag
    // member -- never the whole array wiped to `[]`.
    let policies = all_policies(&handle, NOW).await.unwrap();
    let row = policies
        .iter()
        .find(|p| p.peer_id == "mixed-tag-peer")
        .expect("a mixed-type tags array must not drop the row");
    assert_eq!(
        row.tags,
        vec!["banned".to_string()],
        "the non-string sibling (7) must be dropped INDIVIDUALLY, not the \
         whole array: {:?}",
        row.tags
    );
    assert!(
        row.has_tag("banned"),
        "the valid \"banned\" tag must survive a malformed sibling element \
         (Python's \"banned\" in [\"banned\", 7] is True): tags={:?}",
        row.tags
    );

    // `policies_by_tag` (the query `revenue-r-list-banned` and
    // `revenue-policy find` both use) must find this peer by its
    // surviving string tag.
    let banned = policies_by_tag(&handle, "banned", NOW).await.unwrap();
    let ids: Vec<&str> = banned.iter().map(|p| p.peer_id.as_str()).collect();
    assert!(
        ids.contains(&"mixed-tag-peer"),
        "the banned peer must not disappear from a tag-filtered read over \
         one malformed sibling element: {ids:?}"
    );
}

/// Round-2 correction, CRITICAL, ignored-path equivalent: `revenue-r-list-
/// ignored`'s peer MEMBERSHIP does not depend on tags at all (it filters on
/// `strategy=Passive` + `rebalance_mode=Disabled`), so a mixed-type tags
/// array can never make an ignored peer disappear the way it can a banned
/// peer. The equivalent-severity failure on THIS path is in the reported
/// `reason` field (`rpc_list_ignored::build_list_ignored` picks the first
/// tag that isn't literally `"ignored"`): the OLD whole-array-wipe behavior
/// would silently discard a real custom reason tag next to a malformed
/// sibling and fall back to the generic `"manual"` default, hiding real
/// operator-recorded context. This test exercises the SAME
/// `decode_policy_row` fix from the query layer (not `build_list_ignored`
/// itself, which is a pure function over already-decoded tags -- the
/// defect and the fix both live one layer down, in the decode).
#[tokio::test]
async fn mixed_type_tags_array_preserves_ignored_reason_tag() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy-mixed-tags-ignored.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, tags, updated_at)
             VALUES ('mixed-tag-ignored-peer', 'passive', 'disabled', \
             '[\"low_value\", 42]', 5)",
            [],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let policies = all_policies(&handle, NOW).await.unwrap();
    let row = policies
        .iter()
        .find(|p| p.peer_id == "mixed-tag-ignored-peer")
        .expect("a mixed-type tags array must not drop the row");
    assert_eq!(
        row.tags,
        vec!["low_value".to_string()],
        "the real reason tag must survive the malformed numeric sibling, \
         not fall back to a wiped-then-defaulted [] / \"manual\": {:?}",
        row.tags
    );
}

/// A malformed `peer_id` (NULL, in a schema that allows it) or `updated_at`
/// column must default rather than drop the row too -- same convention as
/// the `expires_at` case above, exercised on the OTHER scalar columns
/// `_row_to_policy` reads unchecked.
#[tokio::test]
async fn corrupt_updated_at_column_defaults_to_zero_row_still_present() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("policy-corrupt-updated-at.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO peer_policies
             (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at)
             VALUES ('garbage-updated-at', 'dynamic', 'enabled', '[]', 'not-a-number', NULL)",
            [],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let policies = all_policies(&handle, NOW).await.unwrap();
    let row = policies
        .iter()
        .find(|p| p.peer_id == "garbage-updated-at")
        .expect("a malformed updated_at must not drop the row");
    assert_eq!(row.updated_at, 0, "unparseable updated_at defaults to 0");
}

#[tokio::test]
async fn hot_channel_overrides_are_oldest_first_and_preserve_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hot-overrides.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO hot_channel_protection_overrides
             (peer_id, added_at, note, min_depletion_trigger_pct)
             VALUES ('later', 200, 'manual', 25.5)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO hot_channel_protection_overrides
             (peer_id, added_at, note, min_depletion_trigger_pct)
             VALUES ('earlier', 100, NULL, NULL)",
            [],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let rows = hot_channel_protection_override_peers(&handle)
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.peer_id.as_str())
            .collect::<Vec<_>>(),
        vec!["earlier", "later"]
    );
    assert_eq!(rows[0].note, None);
    assert_eq!(rows[0].min_depletion_trigger_pct, None);
    assert_eq!(rows[1].note.as_deref(), Some("manual"));
    assert_eq!(rows[1].min_depletion_trigger_pct, Some(25.5));
}

#[tokio::test]
async fn spend_ledger_aggregates_match_python_windows_groups_and_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spend-ledger.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let cutoff = NOW - 24 * 3600;
    {
        let conn = Connection::open(&path).unwrap();
        let events = [
            ("e-cutoff", "rebalance", 100i64, cutoff),
            ("e-recent", "channel_open", 500i64, NOW - 60),
            ("e-old", "rebalance", 999i64, cutoff - 1),
        ];
        for (event_id, category, amount, timestamp) in events {
            conn.execute(
                "INSERT INTO spend_events (event_id, category, amount_sats, timestamp)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![event_id, category, amount, timestamp],
            )
            .unwrap();
        }
        let reservations = [
            ("r-cutoff", "rebalance", 200i64, cutoff, "active"),
            ("r-recent", "channel_open", 300i64, NOW - 30, "active"),
            ("r-spent", "rebalance", 400i64, NOW - 20, "spent"),
            ("r-old", "channel_open", 888i64, cutoff - 1, "active"),
        ];
        for (id, category, amount, reserved_at, status) in reservations {
            conn.execute(
                "INSERT INTO spend_reservations
                 (reservation_id, category, reserved_sats, reserved_at, status)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![id, category, amount, reserved_at, status],
            )
            .unwrap();
        }
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let agg = spend_ledger_aggregates(&handle, 24, NOW).await.unwrap();
    assert_eq!(agg.spent_24h_sats, 600);
    assert_eq!(agg.reserved_24h_sats, 500);
    assert_eq!(agg.spent_by_category.get("rebalance"), Some(&100));
    assert_eq!(agg.spent_by_category.get("channel_open"), Some(&500));
    assert_eq!(agg.reserved_by_category.get("rebalance"), Some(&200));
    assert_eq!(agg.reserved_by_category.get("channel_open"), Some(&300));
    assert_eq!(agg.event_count_by_category.get("rebalance"), Some(&1));
    assert_eq!(
        agg.active_reservation_count_by_category.get("channel_open"),
        Some(&1)
    );
    assert_eq!(agg.covered_hours, Some(24.0));
    assert_eq!(agg.coverage_status, "complete");
}

#[tokio::test]
async fn spend_ledger_coverage_is_unknown_empty_and_partial_from_oldest_evidence() {
    let empty_dir = tempfile::tempdir().unwrap();
    let empty_path = empty_dir.path().join("spend-empty.db");
    std::fs::copy(fixture_path(), &empty_path).unwrap();
    let empty_handle = spawn_read_only(&empty_path).await.unwrap();
    let empty = spend_ledger_aggregates(&empty_handle, 0, NOW)
        .await
        .unwrap();
    assert_eq!(empty.covered_hours, None);
    assert_eq!(empty.coverage_status, "unknown");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spend-partial.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO spend_events (event_id, category, amount_sats, timestamp)
             VALUES ('partial', 'rebalance', 1, ?1)",
            [NOW - 5_550],
        )
        .unwrap();
    }
    let handle = spawn_read_only(&path).await.unwrap();
    let partial = spend_ledger_aggregates(&handle, 24, NOW).await.unwrap();
    assert_eq!(partial.covered_hours, Some(1.54));
    assert_eq!(partial.coverage_status, "partial");

    let future_dir = tempfile::tempdir().unwrap();
    let future_path = future_dir.path().join("spend-future.db");
    std::fs::copy(fixture_path(), &future_path).unwrap();
    {
        let conn = Connection::open(&future_path).unwrap();
        conn.execute(
            "INSERT INTO spend_events (event_id, category, amount_sats, timestamp)
             VALUES ('future', 'rebalance', 1, ?1)",
            [NOW + 1],
        )
        .unwrap();
    }
    let future_handle = spawn_read_only(&future_path).await.unwrap();
    let future = spend_ledger_aggregates(&future_handle, 24, NOW)
        .await
        .unwrap();
    assert_eq!(future.covered_hours, None);
    assert_eq!(future.coverage_status, "unknown");
}

#[tokio::test]
async fn active_spend_reservations_filter_order_limit_and_preserve_fields() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("active-reservations.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let cutoff = NOW - 24 * 3600;
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO spend_reservations
             (reservation_id, category, subcategory, reserved_sats, reserved_at,
              reference_id, channel_id, status, metadata_json)
             VALUES ('first', 'channel_open', 'lnplus_swap', 200, ?1,
                     NULL, '1x1x0', 'active', '{\"swap\":1}')",
            [cutoff],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO spend_reservations
             (reservation_id, category, reserved_sats, reserved_at, status)
             VALUES ('second', 'rebalance', 300, ?1, 'active')",
            [NOW - 10],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO spend_reservations
             (reservation_id, category, reserved_sats, reserved_at, status)
             VALUES ('spent', 'rebalance', 400, ?1, 'spent')",
            [NOW - 20],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO spend_reservations
             (reservation_id, category, reserved_sats, reserved_at, status)
             VALUES ('old', 'rebalance', 500, ?1, 'active')",
            [cutoff - 1],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let one = active_spend_reservations(&handle, 24, 0, NOW)
        .await
        .unwrap();
    assert_eq!(one.len(), 1, "limit is clamped to at least one");
    assert_eq!(one[0].reservation_id, "first");
    assert_eq!(one[0].subcategory.as_deref(), Some("lnplus_swap"));
    assert_eq!(one[0].reference_id, None);
    assert_eq!(one[0].channel_id.as_deref(), Some("1x1x0"));
    assert_eq!(one[0].metadata_json.as_deref(), Some("{\"swap\":1}"));

    let all = active_spend_reservations(&handle, 24, 50, NOW)
        .await
        .unwrap();
    assert_eq!(
        all.iter()
            .map(|row| row.reservation_id.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
}

#[tokio::test]
async fn spend_ledger_rejects_an_overflowing_window_instead_of_panicking() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spend-overflow.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();

    let aggregate_err = spend_ledger_aggregates(&handle, i64::MAX, NOW)
        .await
        .unwrap_err();
    assert!(aggregate_err.to_string().contains("window_hours"));
}

#[tokio::test]
async fn active_reservations_reject_an_overflowing_window_instead_of_panicking() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reservation-overflow.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    let handle = spawn_read_only(&path).await.unwrap();

    let reservation_err = active_spend_reservations(&handle, i64::MAX, 50, NOW)
        .await
        .unwrap_err();
    assert!(reservation_err.to_string().contains("window_hours"));
}

#[tokio::test]
async fn malformed_coverage_timestamp_is_ignored_as_unknown_like_python() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spend-malformed-time.db");
    std::fs::copy(fixture_path(), &path).unwrap();
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO spend_events (event_id, category, amount_sats, timestamp)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["malformed-time", "rebalance", 1, "not-a-timestamp"],
        )
        .unwrap();
    }

    let handle = spawn_read_only(&path).await.unwrap();
    let aggregate = spend_ledger_aggregates(&handle, 24, NOW).await.unwrap();
    assert_eq!(aggregate.covered_hours, None);
    assert_eq!(aggregate.coverage_status, "unknown");
}

#[test]
fn spend_ledger_aggregate_default_is_honest_unknown_coverage() {
    let agg = revops_db::queries::SpendLedgerAggregates::default();
    assert_eq!(agg.covered_hours, None);
    assert_eq!(agg.coverage_status, "unknown");
}

// -- Task 67b slice 1: per-channel profitability inputs --

/// THE DOUBLE-COUNT TRAP. A forward enters on one channel and exits on
/// another. The EXIT channel earns the fee; the ENTRY channel gets
/// `sourced_*` attribution for protection/valuation ONLY. Summing sourced
/// into fleet revenue would count every forward twice.
#[tokio::test]
async fn per_channel_revenue_attributes_the_fee_to_the_exit_channel() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE forwards (
                id INTEGER PRIMARY KEY, in_channel TEXT, out_channel TEXT,
                in_msat INTEGER, out_msat INTEGER, fee_msat INTEGER,
                resolution_time REAL, timestamp INTEGER, resolved_time INTEGER
             );
             INSERT INTO forwards (in_channel,out_channel,in_msat,out_msat,fee_msat,timestamp)
             VALUES ('700x1x0','800x1x0', 1000000, 999000, 1000, 2000),
                    ('700x1x0','800x1x0', 2000000, 1998000, 2000, 3000),
                    ('800x1x0','700x1x0',  500000,  499500,  500, 4000);",
        )
        .unwrap();
    }
    let handle = revops_db::actor::spawn_read_only(&path).await.unwrap();
    let rev = revops_db::queries::per_channel_revenue(&handle, 0)
        .await
        .unwrap();

    // 800x1x0 EXITED two forwards worth 3000 msat of fees.
    let exit = rev.get("800x1x0").expect("exit channel present");
    assert_eq!(exit.fees_earned_msat, 3_000, "exit channel earns the fee");
    assert_eq!(exit.volume_routed_msat, 999_000 + 1_998_000);
    assert_eq!(exit.forward_count, 2);
    // ...and ENTERED one, so its sourced attribution is that one's fee.
    assert_eq!(exit.sourced_fee_contribution_msat, 500);
    assert_eq!(exit.sourced_forward_count, 1);

    // 700x1x0 is the mirror image.
    let entry = rev.get("700x1x0").expect("entry channel present");
    assert_eq!(entry.fees_earned_msat, 500, "it exited exactly one forward");
    assert_eq!(
        entry.sourced_fee_contribution_msat, 3_000,
        "entry-side attribution for the two it sourced"
    );
    assert_eq!(entry.sourced_forward_count, 2);

    // The invariant that makes double-counting detectable: fleet fees are
    // the sum of EARNED only, and equal the sum of SOURCED -- never their
    // total.
    let earned: i64 = rev.values().map(|r| r.fees_earned_msat).sum();
    let sourced: i64 = rev.values().map(|r| r.sourced_fee_contribution_msat).sum();
    assert_eq!(earned, 3_500);
    assert_eq!(
        earned, sourced,
        "every forward is on both sides exactly once"
    );
}

/// The 30-day window is a real filter, not the all-time total.
#[tokio::test]
async fn per_channel_revenue_windows_by_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE forwards (
                id INTEGER PRIMARY KEY, in_channel TEXT, out_channel TEXT,
                in_msat INTEGER, out_msat INTEGER, fee_msat INTEGER,
                resolution_time REAL, timestamp INTEGER, resolved_time INTEGER
             );
             INSERT INTO forwards (in_channel,out_channel,in_msat,out_msat,fee_msat,timestamp)
             VALUES ('700x1x0','800x1x0', 1000, 900, 100, 1000),
                    ('700x1x0','800x1x0', 2000, 1900, 200, 9000);",
        )
        .unwrap();
    }
    let handle = revops_db::actor::spawn_read_only(&path).await.unwrap();
    let all = revops_db::queries::per_channel_revenue(&handle, 0)
        .await
        .unwrap();
    assert_eq!(all.get("800x1x0").unwrap().fees_earned_msat, 300);
    let recent = revops_db::queries::per_channel_revenue(&handle, 5000)
        .await
        .unwrap();
    assert_eq!(
        recent.get("800x1x0").unwrap().fees_earned_msat,
        200,
        "the window excludes the older forward"
    );
}

/// Costs come from channel_costs (open, capacity, opened_at) and
/// rebalance_costs, with the 30-day rebalance window kept separate from
/// the all-time figure -- marginal ROI depends on that distinction.
#[tokio::test]
async fn per_channel_costs_separate_open_from_rebalance_and_window_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE channel_costs (
                channel_id TEXT PRIMARY KEY, peer_id TEXT,
                open_cost_sats INTEGER, capacity_sats INTEGER, opened_at INTEGER
             );
             CREATE TABLE rebalance_costs (
                id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
                cost_sats INTEGER, amount_sats INTEGER, timestamp INTEGER,
                cost_msat INTEGER
             );
             INSERT INTO channel_costs VALUES ('700x1x0','02aa',2500,5000000,1000);
             INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,amount_sats,timestamp,cost_msat)
             VALUES ('700x1x0','02aa',100,50000,2000,100000),
                    ('700x1x0','02aa',400,90000,9000,400000);",
        )
        .unwrap();
    }
    let handle = revops_db::actor::spawn_read_only(&path).await.unwrap();
    let costs = revops_db::queries::per_channel_costs(&handle, 5000)
        .await
        .unwrap();
    let c = costs.get("700x1x0").expect("channel present");
    assert_eq!(c.peer_id, "02aa");
    assert_eq!(c.open_cost_sats, 2_500);
    assert_eq!(c.capacity_sats, 5_000_000);
    assert_eq!(c.opened_at, 1_000);
    assert_eq!(c.rebalance_cost_sats, 500, "all-time rebalance cost");
    assert_eq!(
        c.rebalance_cost_30d_sats, 400,
        "the windowed figure excludes the older rebalance -- marginal ROI depends on it"
    );
}

/// A channel with costs but no forwards still appears (it is a real,
/// zero-revenue channel); a channel absent from BOTH sources is simply
/// absent, never a fabricated zero row.
#[tokio::test]
async fn zero_revenue_channels_appear_but_unknown_ones_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE forwards (
                id INTEGER PRIMARY KEY, in_channel TEXT, out_channel TEXT,
                in_msat INTEGER, out_msat INTEGER, fee_msat INTEGER,
                resolution_time REAL, timestamp INTEGER, resolved_time INTEGER
             );
             CREATE TABLE channel_costs (
                channel_id TEXT PRIMARY KEY, peer_id TEXT,
                open_cost_sats INTEGER, capacity_sats INTEGER, opened_at INTEGER
             );
             CREATE TABLE rebalance_costs (
                id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
                cost_sats INTEGER, amount_sats INTEGER, timestamp INTEGER,
                cost_msat INTEGER
             );
             INSERT INTO channel_costs VALUES ('700x1x0','02aa',2500,5000000,1000);",
        )
        .unwrap();
    }
    let handle = revops_db::actor::spawn_read_only(&path).await.unwrap();
    let rev = revops_db::queries::per_channel_revenue(&handle, 0)
        .await
        .unwrap();
    let costs = revops_db::queries::per_channel_costs(&handle, 0)
        .await
        .unwrap();
    assert!(rev.is_empty(), "no forwards means no revenue rows");
    assert!(costs.contains_key("700x1x0"), "a costed channel is real");
    assert!(
        !costs.contains_key("999x9x9"),
        "unknown channels are absent, not zeroed"
    );
}
