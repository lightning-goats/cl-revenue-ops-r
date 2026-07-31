//! C71-31 (RED): py `Database.get_budget_status(since)` (database.py:4686),
//! the `budget` block of `_assemble_econ_snapshot`.

use revops_db::actor::spawn_read_only;
use revops_db::budget_status::budget_status;

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;
/// py: `get_budget_status(int(time.time()) - 24 * 3600)`.
const SINCE: i64 = NOW - DAY;

const SCHEMA: &str = "
CREATE TABLE rebalance_costs (
    id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
    cost_sats INTEGER, cost_msat INTEGER, amount_sats INTEGER, timestamp INTEGER
);
CREATE TABLE budget_reservations (
    reservation_id TEXT PRIMARY KEY, reserved_sats INTEGER,
    reserved_at INTEGER, job_channel_id TEXT, status TEXT
);
CREATE TABLE spend_reservations (
    reservation_id TEXT PRIMARY KEY, category TEXT, subcategory TEXT,
    reserved_sats INTEGER, reserved_at INTEGER, reference_id TEXT,
    channel_id TEXT, status TEXT, metadata_json TEXT
);
";

/// The unified ledger table is absent -- py's documented minimal-schema case.
const SCHEMA_WITHOUT_UNIFIED: &str = "
CREATE TABLE rebalance_costs (
    id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
    cost_sats INTEGER, cost_msat INTEGER, amount_sats INTEGER, timestamp INTEGER
);
CREATE TABLE budget_reservations (
    reservation_id TEXT PRIMARY KEY, reserved_sats INTEGER,
    reserved_at INTEGER, job_channel_id TEXT, status TEXT
);
";

async fn db(schema: &str, seed: &str) -> revops_db::actor::DbHandle {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(schema).unwrap();
        if !seed.is_empty() {
            conn.execute_batch(seed).unwrap();
        }
    }
    let handle = spawn_read_only(&path).await.unwrap();
    std::mem::forget(dir);
    handle
}

#[tokio::test]
async fn reserved_sums_the_legacy_and_unified_ledgers_rather_than_choosing_one() {
    // py adds `spend_reservations` (category='rebalance') to
    // `budget_reservations`. The unification is mid-migration, so rows
    // exist on BOTH sides; taking only one silently under-reports
    // committed budget and lets the node overspend its cap.
    let handle = db(
        SCHEMA,
        &format!(
            "INSERT INTO budget_reservations
                 (reservation_id,reserved_sats,reserved_at,job_channel_id,status)
             VALUES ('legacy', 300, {SINCE}, '700x1x0', 'active');
             INSERT INTO spend_reservations
                 (reservation_id,category,reserved_sats,reserved_at,status)
             VALUES ('unified', 'rebalance', 400, {SINCE}, 'active');"
        ),
    )
    .await;

    let status = budget_status(&handle, SINCE).await.expect("reads");
    assert_eq!(status.reserved_sats, 700, "300 legacy + 400 unified");
}

#[tokio::test]
async fn only_active_reservations_inside_the_window_count() {
    let old = SINCE - DAY;
    let handle = db(
        SCHEMA,
        &format!(
            "INSERT INTO budget_reservations
                 (reservation_id,reserved_sats,reserved_at,job_channel_id,status)
             VALUES ('active-in',  100, {SINCE}, '700x1x0', 'active'),
                    ('spent',      200, {SINCE}, '700x1x0', 'spent'),
                    ('released',   400, {SINCE}, '700x1x0', 'released'),
                    ('active-old', 800, {old},   '700x1x0', 'active');
             INSERT INTO spend_reservations
                 (reservation_id,category,reserved_sats,reserved_at,status)
             VALUES ('u-active',  'rebalance', 10, {SINCE}, 'active'),
                    ('u-old',     'rebalance', 40, {old},   'active'),
                    ('u-spent',   'rebalance', 80, {SINCE}, 'spent'),
                    ('u-other',   'planner',   20, {SINCE}, 'active');"
        ),
    )
    .await;

    let status = budget_status(&handle, SINCE).await.expect("reads");
    assert_eq!(
        status.reserved_sats, 110,
        "100 legacy + 10 unified: only the active, in-window, \
         rebalance-category rows on BOTH sides"
    );
}

#[tokio::test]
async fn a_category_other_than_rebalance_is_not_this_budget() {
    // The unified ledger is shared across categories; counting planner or
    // boltz reservations here would throttle rebalancing on someone else's
    // spend.
    let handle = db(
        SCHEMA,
        &format!(
            "INSERT INTO spend_reservations
                 (reservation_id,category,reserved_sats,reserved_at,status)
             VALUES ('p', 'planner', 5000, {SINCE}, 'active');"
        ),
    )
    .await;

    let status = budget_status(&handle, SINCE).await.expect("reads");
    assert_eq!(status.reserved_sats, 0);
}

#[tokio::test]
async fn spent_sums_windowed_rebalance_costs() {
    let old = SINCE - DAY;
    let handle = db(
        SCHEMA,
        &format!(
            "INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,amount_sats,timestamp)
             VALUES ('700x1x0','02aa', 120, 0, {SINCE}),
                    ('700x1x0','02aa', 900, 0, {old});"
        ),
    )
    .await;

    let status = budget_status(&handle, SINCE).await.expect("reads");
    assert_eq!(status.spent_sats, 120, "the 900 is outside the window");
}

#[tokio::test]
async fn total_committed_is_spent_plus_reserved() {
    let handle = db(
        SCHEMA,
        &format!(
            "INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,amount_sats,timestamp)
             VALUES ('700x1x0','02aa', 120, 0, {SINCE});
             INSERT INTO budget_reservations
                 (reservation_id,reserved_sats,reserved_at,job_channel_id,status)
             VALUES ('r', 80, {SINCE}, '700x1x0', 'active');"
        ),
    )
    .await;

    let status = budget_status(&handle, SINCE).await.expect("reads");
    assert_eq!(status.spent_sats, 120);
    assert_eq!(status.reserved_sats, 80);
    assert_eq!(status.total_committed_sats, 200);
}

#[tokio::test]
async fn an_empty_node_reports_zeros_and_that_is_a_real_answer() {
    let handle = db(SCHEMA, "").await;
    let status = budget_status(&handle, SINCE).await.expect("reads");
    assert_eq!(status.total_committed_sats, 0);
}

#[tokio::test]
async fn a_schema_without_the_unified_ledger_still_reports_the_legacy_half() {
    // py's `except sqlite3.OperationalError` for "minimal/partial schemas
    // (tests, tooling)". An absent TABLE must not make the whole read
    // refuse -- but it must also not hide a real read failure, which is
    // why only the missing-table error is swallowed.
    let handle = db(
        SCHEMA_WITHOUT_UNIFIED,
        &format!(
            "INSERT INTO budget_reservations
                 (reservation_id,reserved_sats,reserved_at,job_channel_id,status)
             VALUES ('legacy', 300, {SINCE}, '700x1x0', 'active');"
        ),
    )
    .await;

    let status = budget_status(&handle, SINCE)
        .await
        .expect("an older schema is not a failure");
    assert_eq!(status.reserved_sats, 300);
}

#[tokio::test]
async fn a_missing_rebalance_costs_table_refuses_rather_than_reporting_zero_spend() {
    // Only the unified-ledger table has py's absent-table tolerance. A
    // missing spend table is a broken production DB, and reporting zero
    // spend would free the whole daily budget.
    let handle = db(
        "CREATE TABLE budget_reservations (
             reservation_id TEXT PRIMARY KEY, reserved_sats INTEGER,
             reserved_at INTEGER, job_channel_id TEXT, status TEXT);",
        "",
    )
    .await;

    assert!(
        budget_status(&handle, SINCE).await.is_err(),
        "an unreadable spend table is not zero spend"
    );
}

#[tokio::test]
async fn a_malformed_unified_ledger_refuses_rather_than_reporting_no_reservations() {
    // py tolerates the unified table being ABSENT (minimal schemas), and
    // nothing more. A table that exists but cannot be read is a broken
    // production DB: swallowing it would report zero rebalance
    // reservations and free budget that is actually committed.
    //
    // This is the control for the absent-table tolerance above -- without
    // it, broadening the catch to `Err(_) => 0` passes every other test.
    let handle = db(
        "CREATE TABLE rebalance_costs (
             id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
             cost_sats INTEGER, cost_msat INTEGER, amount_sats INTEGER, timestamp INTEGER);
         CREATE TABLE budget_reservations (
             reservation_id TEXT PRIMARY KEY, reserved_sats INTEGER,
             reserved_at INTEGER, job_channel_id TEXT, status TEXT);
         CREATE TABLE spend_reservations (
             reservation_id TEXT PRIMARY KEY, category TEXT, status TEXT);",
        "",
    )
    .await;

    assert!(
        budget_status(&handle, SINCE).await.is_err(),
        "a spend_reservations table missing reserved_sats is unreadable, \
         not empty"
    );
}

// ---------------------------------------------------------------------
// C71-33: checked arithmetic.
//
// Each SQL `SUM` is individually an i64, so two legally-representable
// component totals can still overflow their Rust sum. Debug panics;
// RELEASE WRAPS, and a wrapped committed-budget figure is a negative
// number that frees the entire daily cap. Python's integers do neither.
// ---------------------------------------------------------------------

#[tokio::test]
async fn reserved_totals_that_overflow_i64_refuse_rather_than_wrapping() {
    let handle = db(
        SCHEMA,
        &format!(
            "INSERT INTO budget_reservations
                 (reservation_id,reserved_sats,reserved_at,job_channel_id,status)
             VALUES ('legacy', {max}, {SINCE}, '700x1x0', 'active');
             INSERT INTO spend_reservations
                 (reservation_id,category,reserved_sats,reserved_at,status)
             VALUES ('unified', 'rebalance', 1, {SINCE}, 'active');",
            max = i64::MAX
        ),
    )
    .await;

    let error = budget_status(&handle, SINCE)
        .await
        .expect_err("a wrapped reserved total is worse than a refusal");
    assert!(
        format!("{error:#}").contains("reserved budget overflows"),
        "the refusal must name which sum overflowed: {error:#}"
    );
}

#[tokio::test]
async fn committed_totals_that_overflow_i64_refuse_rather_than_wrapping() {
    let handle = db(
        SCHEMA,
        &format!(
            "INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,amount_sats,timestamp)
             VALUES ('700x1x0','02aa', {max}, 0, {SINCE});
             INSERT INTO budget_reservations
                 (reservation_id,reserved_sats,reserved_at,job_channel_id,status)
             VALUES ('r', 1, {SINCE}, '700x1x0', 'active');",
            max = i64::MAX
        ),
    )
    .await;

    let error = budget_status(&handle, SINCE)
        .await
        .expect_err("a wrapped committed total frees the whole daily cap");
    assert!(
        format!("{error:#}").contains("committed budget overflows"),
        "the refusal must name which sum overflowed: {error:#}"
    );
}

/// The control: a large-but-representable total is a real answer, not a
/// refusal. Without this, "refuse on anything big" would pass both tests
/// above while being strictly worse.
#[tokio::test]
async fn a_large_but_representable_total_is_still_reported() {
    let half = i64::MAX / 4;
    let handle = db(
        SCHEMA,
        &format!(
            "INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,amount_sats,timestamp)
             VALUES ('700x1x0','02aa', {half}, 0, {SINCE});
             INSERT INTO budget_reservations
                 (reservation_id,reserved_sats,reserved_at,job_channel_id,status)
             VALUES ('r', {half}, {SINCE}, '700x1x0', 'active');"
        ),
    )
    .await;

    let status = budget_status(&handle, SINCE).await.expect("representable");
    assert_eq!(status.total_committed_sats, half * 2);
}

/// C71-31 structural: one actor turn, one transaction.
///
/// Structural for the same reason as C71-21's pins: the actor turn is
/// blocking, so no test can interleave a write into it, and a correct
/// implementation therefore offers no seam to inject.
///
/// Python states the requirement itself (audit fix C-1): a rebalance
/// settling between the reads moves sats from reserved to spent, and split
/// reads count the same sats in BOTH halves.
#[test]
fn the_budget_position_is_read_under_one_transaction() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/budget_status.rs"))
            .unwrap();

    let after = source
        .split_once("pub async fn budget_status")
        .expect("the handle-level read must exist")
        .1;
    let handle_body = &after[..after.find("\n}\n").expect("a closed top-level item")];
    assert_eq!(
        handle_body.matches(".await").count(),
        1,
        "exactly one actor round trip"
    );

    let reader = source
        .split_once("pub(crate) fn read_budget_status")
        .expect("the sync reader must exist")
        .1;
    let reader_body = &reader[..reader.find("\n}\n").expect("a closed top-level item")];
    assert_eq!(
        reader_body.matches("unchecked_transaction").count(),
        1,
        "all three SELECTs share ONE snapshot"
    );
    assert!(
        !reader_body.contains("conn.query_row"),
        "no SELECT may bypass the transaction and read the bare connection"
    );
}
