//! F71-R23 slice 2 (RED): the production-store reads profitability needs.
//!
//! `last_routed` and the diagnostic-rebalance stats both live in the
//! PRODUCTION database, which Python owns and still writes. This port
//! reads them read-only. They are fetched together, in one transaction,
//! because they are two halves of one channel's verdict and the database
//! is concurrently written under WAL -- two independent round trips could
//! land on different WAL snapshots and produce a combination Python's own
//! single read could never have produced (C71-14, and the reasoning
//! `DbHandle::query_row` already documents for multi-column aggregates).

use revops_db::actor::spawn_read_only;
use revops_db::profitability_history::channel_history;

const NOW: i64 = 1_800_000_000;
const DAY: i64 = 86_400;
const DIAG_DAYS: i64 = 14;

/// Python's production schema, only the columns these reads touch.
const SCHEMA: &str = "
CREATE TABLE forwards (
    id INTEGER PRIMARY KEY, in_channel TEXT, out_channel TEXT,
    in_msat INTEGER, out_msat INTEGER, fee_msat INTEGER,
    resolution_time REAL, timestamp INTEGER, resolved_time INTEGER
);
CREATE TABLE daily_forwarding_stats (
    channel_id TEXT, date INTEGER, forward_count INTEGER,
    total_fee_msat INTEGER, total_out_msat INTEGER
);
CREATE TABLE daily_forwarding_stats_inbound (
    channel_id TEXT, date INTEGER, forward_count INTEGER,
    total_fee_msat INTEGER, total_out_msat INTEGER
);
CREATE TABLE rebalance_history (
    id INTEGER PRIMARY KEY, from_channel TEXT, to_channel TEXT,
    rebalance_type TEXT, status TEXT, timestamp INTEGER
);
CREATE TABLE channel_costs (
    channel_id TEXT PRIMARY KEY, peer_id TEXT, open_cost_sats INTEGER,
    capacity_sats INTEGER, opened_at INTEGER
);
CREATE TABLE rebalance_costs (
    id INTEGER PRIMARY KEY, channel_id TEXT, peer_id TEXT,
    cost_sats INTEGER, timestamp INTEGER
);
";

async fn db(seed: &str) -> revops_db::actor::DbHandle {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        if !seed.is_empty() {
            conn.execute_batch(seed).unwrap();
        }
    }
    let handle = spawn_read_only(&path).await.unwrap();
    // Keep the tempdir alive for the actor's lifetime.
    std::mem::forget(dir);
    handle
}

// ---------------------------------------------------------------------
// last_routed
// ---------------------------------------------------------------------

#[tokio::test]
async fn last_routed_counts_inbound_sourcing_not_just_outbound_forwarding() {
    // py `get_last_forward_time_any_direction` (database.py:3453) checks
    // BOTH roles. A channel that only ever sourced inbound volume is
    // routing -- reading it as never-routed makes it dead capital.
    let handle = db(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,timestamp)
                     VALUES ('700x1x0','800x1x0', 10, 1799900000);",
    )
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(
        history.last_routed,
        Some(1_799_900_000),
        "the channel appears only as in_channel; ignoring that direction \
         reports an active channel as never-routed"
    );
}

#[tokio::test]
async fn last_routed_falls_back_to_rolled_up_daily_stats_when_raw_forwards_are_pruned() {
    // Raw forwards are pruned by retention; the daily rollup is what
    // survives. py approximates the time-of-day as midnight + 86399
    // (database.py:3475). Missing this makes every channel older than the
    // retention window read as never-routed.
    let day_start = 1_799_000_000 - (1_799_000_000 % DAY);
    let handle = db(&format!(
        "INSERT INTO daily_forwarding_stats (channel_id,date,forward_count)
         VALUES ('700x1x0', {day_start}, 5);"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(history.last_routed, Some(day_start + 86_399));
}

#[tokio::test]
async fn last_routed_falls_back_to_the_inbound_rollup_as_well_as_the_exit_rollup() {
    // C71-20. `daily_forwarding_stats` is EXIT-side only; py unions a
    // THIRD arm over `daily_forwarding_stats_inbound`
    // (database.py:3478-3481). A channel whose retained history is
    // inbound-only -- it sourced volume and its raw forwards have since
    // been pruned -- reads as never-routed without that arm, which is the
    // dead-capital verdict on exactly the channels the rollup exists to
    // preserve.
    let day_start = 1_799_000_000 - (1_799_000_000 % DAY);
    let handle = db(&format!(
        "INSERT INTO daily_forwarding_stats_inbound (channel_id,date,forward_count)
         VALUES ('700x1x0', {day_start}, 5);"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(
        history.last_routed,
        Some(day_start + 86_399),
        "inbound-only retained history must count as routing"
    );
}

#[tokio::test]
async fn last_routed_takes_the_latest_across_all_three_sources() {
    let older = 1_700_000_000 - (1_700_000_000 % DAY);
    let newer = 1_799_000_000 - (1_799_000_000 % DAY);
    let handle = db(&format!(
        "INSERT INTO daily_forwarding_stats (channel_id,date,forward_count)
         VALUES ('700x1x0', {older}, 5);
         INSERT INTO daily_forwarding_stats_inbound (channel_id,date,forward_count)
         VALUES ('700x1x0', {newer}, 5);"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(history.last_routed, Some(newer + 86_399));
}

#[tokio::test]
async fn a_zero_forward_count_rollup_row_is_not_routing_activity() {
    // py guards every rollup arm with `forward_count > 0`. A zero-count
    // row is a bookkeeping artefact, not evidence the channel routed.
    let day_start = 1_799_000_000 - (1_799_000_000 % DAY);
    let handle = db(&format!(
        "INSERT INTO daily_forwarding_stats (channel_id,date,forward_count)
         VALUES ('700x1x0', {day_start}, 0);
         INSERT INTO daily_forwarding_stats_inbound (channel_id,date,forward_count)
         VALUES ('700x1x0', {day_start}, 0);"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(history.last_routed, None);
}

#[tokio::test]
async fn last_routed_matches_either_scid_spelling_in_every_source() {
    // C71-20 alias regression. The alias set must reach ALL THREE arms,
    // not just `forwards` -- a rollup row written in the other spelling is
    // the same silent never-routed verdict.
    let day_start = 1_799_000_000 - (1_799_000_000 % DAY);
    let handle = db(&format!(
        "INSERT INTO daily_forwarding_stats_inbound (channel_id,date,forward_count)
         VALUES ('700:1:0', {day_start}, 5);"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(
        history.last_routed,
        Some(day_start + 86_399),
        "a `:`-spelled rollup row must be found by an `x`-spelled query"
    );
}

#[tokio::test]
async fn diagnostic_stats_match_either_scid_spelling() {
    let recent = NOW - DAY;
    let handle = db(&format!(
        "INSERT INTO rebalance_history (from_channel,to_channel,rebalance_type,status,timestamp)
         VALUES ('800x1x0','700:1:0','diagnostic','failed',{recent}),
                ('800x1x0','700:1:0','diagnostic','failed',{recent});"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(
        history.diag.attempt_count, 2,
        "diagnostic rows written in the other spelling must still count, or the \
         zombie gate silently never fires for those channels"
    );
}

#[tokio::test]
async fn last_routed_matches_either_scid_spelling() {
    // py `_scid_aliases` accepts both `x` and `:` spellings. A row written
    // in the other spelling must not read as no history at all.
    let handle = db(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,timestamp)
                     VALUES ('900x2x1','700:1:0', 10, 1799800000);",
    )
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(
        history.last_routed,
        Some(1_799_800_000),
        "a `:`-spelled row must be found by an `x`-spelled query"
    );
}

#[tokio::test]
async fn a_channel_with_no_routing_history_reports_none_and_is_not_an_error() {
    // Consulted-and-empty. A brand-new channel really has never routed;
    // refusing here would make every new channel unevaluable.
    let handle = db("").await;
    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("an empty history is a real answer");
    assert_eq!(history.last_routed, None);
}

#[tokio::test]
async fn last_routed_takes_the_latest_across_both_sources() {
    let day_start = 1_700_000_000 - (1_700_000_000 % DAY);
    let handle = db(&format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,timestamp)
         VALUES ('700x1x0','800x1x0', 10, 1799900000);
         INSERT INTO daily_forwarding_stats (channel_id,date,forward_count)
         VALUES ('700x1x0', {day_start}, 5);"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(history.last_routed, Some(1_799_900_000));
}

// ---------------------------------------------------------------------
// diagnostic rebalance stats
// ---------------------------------------------------------------------

#[tokio::test]
async fn diagnostic_stats_count_only_diagnostic_attempts_to_this_channel() {
    // py: `WHERE to_channel = ? AND rebalance_type = 'diagnostic' AND
    // timestamp >= ?` (database.py:2802-2806). Counting ordinary
    // rebalances would push channels past the `attempt_count >= 2` gate
    // and manufacture ZOMBIE verdicts.
    let recent = NOW - DAY;
    let handle = db(&format!(
        "INSERT INTO rebalance_history (from_channel,to_channel,rebalance_type,status,timestamp)
         VALUES ('800x1x0','700x1x0','diagnostic','failed',{recent}),
                ('800x1x0','700x1x0','diagnostic','failed',{recent}),
                ('800x1x0','700x1x0','cycle','success',{recent}),
                ('800x1x0','900x1x0','diagnostic','success',{recent});"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(
        history.diag.attempt_count, 2,
        "two diagnostic attempts only"
    );
    assert_eq!(
        history.diag.last_success_time, 0,
        "no diagnostic attempt to THIS channel succeeded; the 'cycle' success \
         and the other channel's success must not leak in"
    );
}

#[tokio::test]
async fn diagnostic_attempts_outside_the_window_do_not_count() {
    let old = NOW - 20 * DAY;
    let handle = db(&format!(
        "INSERT INTO rebalance_history (from_channel,to_channel,rebalance_type,status,timestamp)
         VALUES ('800x1x0','700x1x0','diagnostic','failed',{old}),
                ('800x1x0','700x1x0','diagnostic','failed',{old});"
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(
        history.diag.attempt_count, 0,
        "the 14-day window is what makes the zombie branch about CURRENT \
         diagnosis rather than ancient history"
    );
}

#[tokio::test]
async fn a_successful_diagnostic_reports_its_timestamp() {
    let success_at = NOW - 3 * DAY;
    let handle = db(&format!(
        "INSERT INTO rebalance_history (from_channel,to_channel,rebalance_type,status,timestamp)
         VALUES ('800x1x0','700x1x0','diagnostic','failed',{}),
                ('800x1x0','700x1x0','diagnostic','success',{success_at});",
        NOW - 5 * DAY
    ))
    .await;

    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("reads");
    assert_eq!(history.diag.attempt_count, 2);
    assert_eq!(history.diag.last_success_time, success_at);
}

#[tokio::test]
async fn a_channel_with_no_diagnostics_reports_zero_attempts_not_an_error() {
    let handle = db("").await;
    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .expect("no diagnostic rows is a real answer");
    assert_eq!(history.diag.attempt_count, 0);
    assert_eq!(history.diag.last_success_time, 0);
}

// ---------------------------------------------------------------------
// coupling
// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// C71-21: the whole production-DB verdict is one observation.
// ---------------------------------------------------------------------

/// The split-await failure, driven deterministically.
///
/// Revenue, costs and history are all inputs to ONE classifier verdict and
/// all live in the production database -- which Python still writes, under
/// WAL, while this port reads. Fetching them in separate actor turns lets
/// a Python write land in between, and the assembled verdict then
/// describes a node state that never existed at any instant.
///
/// The test plays Python: read half the inputs, commit a forward through a
/// separate writable connection, read the other half. Both halves are
/// individually true; together they are impossible.
#[tokio::test]
async fn split_await_reads_assemble_a_state_that_never_existed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("prod.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute_batch(
            "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
             VALUES ('900x1x0','700x1x0', 10, 1000, 1799000000);",
        )
        .unwrap();
    }
    let handle = spawn_read_only(&path).await.unwrap();

    // --- half one: revenue says this channel has forwarded once.
    let revenue = revops_db::queries::per_channel_revenue(&handle, 0)
        .await
        .unwrap();
    let before = revenue.get("700x1x0").cloned().unwrap_or_default();
    assert_eq!(before.forward_count, 1);

    // --- the window: Python routes another payment and commits.
    {
        let writer = rusqlite::Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
                 VALUES ('900x1x0','700x1x0', 20, 2000, 1799500000);",
            )
            .unwrap();
    }

    // --- half two: history sees the SECOND forward.
    let history = channel_history(&handle, "700x1x0", NOW, DIAG_DAYS)
        .await
        .unwrap();
    assert_eq!(history.last_routed, Some(1_799_500_000));

    // The pair is impossible: one forward all-time, last routed at a
    // moment the second forward created.
    assert!(
        before.forward_count == 1 && history.last_routed == Some(1_799_500_000),
        "precondition for the contrast below"
    );

    // --- ONE observation of the same final state is self-consistent.
    let snapshot =
        revops_db::profitability_history::profitability_snapshot(&handle, NOW, 30, DIAG_DAYS)
            .await
            .expect("snapshot reads");
    let rev = snapshot.revenue_all_time.get("700x1x0").unwrap();
    let hist = snapshot.history.get("700x1x0").unwrap();
    assert_eq!(
        (rev.forward_count, hist.last_routed),
        (2, Some(1_799_500_000)),
        "a single snapshot must never pair a forward count with a routing time \
         from a different instant"
    );
}

#[tokio::test]
async fn the_snapshot_carries_every_production_input_the_classifier_needs() {
    let recent = NOW - DAY;
    let handle = db(&format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
         VALUES ('900x1x0','700x1x0', 10, 1000, {recent});
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('700x1x0','02aa', 500, 1000000, 1700000000);
         INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,timestamp)
         VALUES ('700x1x0','02aa', 7, {recent});
         INSERT INTO rebalance_history (from_channel,to_channel,rebalance_type,status,timestamp)
         VALUES ('800x1x0','700x1x0','diagnostic','success',{recent});"
    ))
    .await;

    let snapshot =
        revops_db::profitability_history::profitability_snapshot(&handle, NOW, 30, DIAG_DAYS)
            .await
            .expect("snapshot reads");

    assert_eq!(
        snapshot
            .revenue_all_time
            .get("700x1x0")
            .unwrap()
            .fees_earned_msat,
        10
    );
    assert_eq!(
        snapshot
            .revenue_30d
            .get("700x1x0")
            .unwrap()
            .fees_earned_msat,
        10
    );
    let costs = snapshot.costs.get("700x1x0").unwrap();
    assert_eq!(costs.open_cost_sats, 500);
    assert_eq!(costs.rebalance_cost_sats, 7);
    assert_eq!(costs.rebalance_cost_30d_sats, 7);
    let hist = snapshot.history.get("700x1x0").unwrap();
    assert_eq!(hist.last_routed, Some(recent));
    assert_eq!(hist.diag.attempt_count, 1);
    assert_eq!(hist.diag.last_success_time, recent);
}

#[tokio::test]
async fn the_snapshot_normalizes_both_scid_spellings_onto_one_key() {
    // The per-channel reads bind the alias pair as parameters; a fleet-wide
    // `GROUP BY` cannot, so the folding happens in Rust and has to be
    // asserted on EVERY map. A map that misses it splits one channel into
    // two fleet entries: one with revenue and no history (dead capital) and
    // one with history and no revenue.
    let recent = NOW - DAY;
    let handle = db(&format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
         VALUES ('900x1x0','700:1:0', 10, 1000, {recent});
         INSERT INTO channel_costs (channel_id,peer_id,open_cost_sats,capacity_sats,opened_at)
         VALUES ('700:1:0','02aa', 500, 1000000, 1700000000);
         INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,timestamp)
         VALUES ('700:1:0','02aa', 7, {recent});"
    ))
    .await;

    let snapshot =
        revops_db::profitability_history::profitability_snapshot(&handle, NOW, 30, DIAG_DAYS)
            .await
            .expect("snapshot reads");
    assert!(
        snapshot.history.contains_key("700x1x0"),
        "a `:`-spelled row must land on the `x`-spelled key, or the fleet pass \
         silently treats it as a different channel: {:?}",
        snapshot.history.keys().collect::<Vec<_>>()
    );
    assert!(
        snapshot.revenue_all_time.contains_key("700x1x0"),
        "revenue must fold the spellings too: {:?}",
        snapshot.revenue_all_time.keys().collect::<Vec<_>>()
    );
    assert!(
        snapshot.revenue_30d.contains_key("700x1x0"),
        "the windowed revenue must fold the spellings too: {:?}",
        snapshot.revenue_30d.keys().collect::<Vec<_>>()
    );
    // Asserted by VALUE, not by key presence: `costs` is filled by two
    // separate reads (opens and rebalances), so a presence check is
    // satisfied by whichever one still folds -- and the open cost, the
    // larger of the two, is exactly the one that would go missing.
    let costs = snapshot
        .costs
        .get("700x1x0")
        .unwrap_or_else(|| panic!("costs keys: {:?}", snapshot.costs.keys()));
    assert_eq!(
        costs.open_cost_sats, 500,
        "the open cost must fold onto the `x` key, or the denominator lands \
         on a channel with no revenue"
    );
    assert_eq!(
        costs.rebalance_cost_sats, 7,
        "the rebalance cost must fold onto the `x` key too"
    );
    assert_eq!(
        snapshot
            .revenue_all_time
            .get("700x1x0")
            .unwrap()
            .fees_earned_msat,
        10
    );
}

#[tokio::test]
async fn the_snapshot_unions_all_three_last_routed_sources_per_channel() {
    // C71-20, one scope wider. The fleet read is a SEPARATE query set from
    // the per-channel read, so the three-source union has to be pinned
    // here too -- the per-channel tests would stay green while every
    // channel whose retained history is rollup-only reads as never-routed.
    let exit_day = 1_798_000_000 - (1_798_000_000 % DAY);
    let inbound_day = 1_799_000_000 - (1_799_000_000 % DAY);
    let handle = db(&format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,timestamp)
         VALUES ('900x1x0','raw_only', 10, 1797000000);
         INSERT INTO daily_forwarding_stats (channel_id,date,forward_count)
         VALUES ('exit_only', {exit_day}, 5), ('zero_only', {exit_day}, 0);
         INSERT INTO daily_forwarding_stats_inbound (channel_id,date,forward_count)
         VALUES ('inbound_only', {inbound_day}, 5), ('zero_only', {inbound_day}, 0);"
    ))
    .await;

    let snapshot =
        revops_db::profitability_history::profitability_snapshot(&handle, NOW, 30, DIAG_DAYS)
            .await
            .expect("snapshot reads");

    assert_eq!(
        snapshot.history.get("raw_only").and_then(|h| h.last_routed),
        Some(1_797_000_000)
    );
    assert_eq!(
        snapshot
            .history
            .get("exit_only")
            .and_then(|h| h.last_routed),
        Some(exit_day + 86_399),
        "the exit-side rollup arm is missing from the fleet read"
    );
    assert_eq!(
        snapshot
            .history
            .get("inbound_only")
            .and_then(|h| h.last_routed),
        Some(inbound_day + 86_399),
        "the inbound rollup arm is missing from the fleet read; every \
         inbound-only channel would read as dead capital"
    );
    assert_eq!(
        snapshot
            .history
            .get("zero_only")
            .and_then(|h| h.last_routed),
        None,
        "a zero-count rollup row is a bookkeeping artefact, not routing"
    );
}

#[tokio::test]
async fn the_snapshot_counts_inbound_sourcing_as_routing_activity() {
    // The entry-side arm of the raw-forwards union. Without it a channel
    // that only ever sourced volume reads as never-routed.
    let handle = db(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,timestamp)
                     VALUES ('700x1x0','800x1x0', 10, 1799900000);",
    )
    .await;

    let snapshot =
        revops_db::profitability_history::profitability_snapshot(&handle, NOW, 30, DIAG_DAYS)
            .await
            .expect("snapshot reads");
    assert_eq!(
        snapshot.history.get("700x1x0").and_then(|h| h.last_routed),
        Some(1_799_900_000),
        "the channel appears only as in_channel"
    );
}

#[tokio::test]
async fn the_windowed_revenue_excludes_forwards_older_than_the_window() {
    // Marginal ROI is defined over the trailing window. If the windowed map
    // silently carries all-time figures, an old channel that has stopped
    // earning looks healthy forever.
    let recent = NOW - DAY;
    let old = NOW - 200 * DAY;
    let handle = db(&format!(
        "INSERT INTO forwards (in_channel,out_channel,fee_msat,out_msat,timestamp)
         VALUES ('900x1x0','700x1x0', 10, 1000, {recent}),
                ('900x1x0','700x1x0', 90, 9000, {old});"
    ))
    .await;

    let snapshot =
        revops_db::profitability_history::profitability_snapshot(&handle, NOW, 30, DIAG_DAYS)
            .await
            .expect("snapshot reads");
    assert_eq!(
        snapshot
            .revenue_all_time
            .get("700x1x0")
            .unwrap()
            .fees_earned_msat,
        100,
        "all-time must span the whole history"
    );
    assert_eq!(
        snapshot
            .revenue_30d
            .get("700x1x0")
            .unwrap()
            .fees_earned_msat,
        10,
        "the windowed figure must exclude the 200-day-old forward"
    );
    assert_eq!(
        snapshot.revenue_30d.get("700x1x0").unwrap().forward_count,
        1
    );
}

#[tokio::test]
async fn the_windowed_rebalance_cost_is_kept_separate_from_the_all_time_one() {
    let recent = NOW - DAY;
    let old = NOW - 200 * DAY;
    let handle = db(&format!(
        "INSERT INTO rebalance_costs (channel_id,peer_id,cost_sats,timestamp)
         VALUES ('700x1x0','02aa', 7, {recent}),
                ('700x1x0','02aa', 93, {old});"
    ))
    .await;

    let snapshot =
        revops_db::profitability_history::profitability_snapshot(&handle, NOW, 30, DIAG_DAYS)
            .await
            .expect("snapshot reads");
    let costs = snapshot.costs.get("700x1x0").unwrap();
    assert_eq!(costs.rebalance_cost_sats, 100);
    assert_eq!(
        costs.rebalance_cost_30d_sats, 7,
        "conflating the two silently changes every marginal-ROI verdict"
    );
}

#[tokio::test]
async fn the_snapshot_diagnostics_are_scoped_to_type_direction_and_window() {
    // The zombie gate fires at `attempt_count >= 2`. Counting ordinary
    // rebalances, the wrong direction, or ancient history manufactures
    // ZOMBIE verdicts on live channels.
    let recent = NOW - DAY;
    let old = NOW - 20 * DAY;
    let handle = db(&format!(
        "INSERT INTO rebalance_history (from_channel,to_channel,rebalance_type,status,timestamp)
         VALUES ('800x1x0','700x1x0','diagnostic','failed',{recent}),
                ('800x1x0','700x1x0','diagnostic','success',{recent}),
                ('800x1x0','700x1x0','cycle','success',{recent}),
                ('700x1x0','800x1x0','diagnostic','success',{recent}),
                ('800x1x0','700x1x0','diagnostic','success',{old});"
    ))
    .await;

    let snapshot =
        revops_db::profitability_history::profitability_snapshot(&handle, NOW, 30, DIAG_DAYS)
            .await
            .expect("snapshot reads");
    let hist = snapshot.history.get("700x1x0").unwrap();
    assert_eq!(
        hist.diag.attempt_count, 2,
        "only in-window diagnostic attempts TO this channel count"
    );
    assert_eq!(hist.diag.last_success_time, recent);

    let outbound = snapshot.history.get("800x1x0").unwrap();
    assert_eq!(
        outbound.diag.attempt_count, 1,
        "the one diagnostic aimed AT 800x1x0 is its own, and only its own"
    );
}

/// The text of one top-level `fn`, from its signature to the closing brace
/// in column zero. Scoped to the item on purpose: taking the whole file
/// tail would make each pin's verdict depend on what happens to be defined
/// *after* it, so adding an unrelated function below could fail -- or
/// silently satisfy -- a pin about this one.
fn fn_body(source: &str, signature: &str) -> String {
    let after = source
        .split_once(signature)
        .unwrap_or_else(|| panic!("`{signature}` must exist"))
        .1;
    let end = after
        .find("\n}\n")
        .unwrap_or_else(|| panic!("`{signature}` must be a closed top-level item"));
    after[..end].to_string()
}

/// C71-21 structural: the whole production read is one actor turn.
#[test]
fn the_production_snapshot_is_issued_as_one_command() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/profitability_history.rs"
    ))
    .unwrap();
    let body = fn_body(&source, "pub async fn profitability_snapshot");

    assert_eq!(
        body.matches(".await").count(),
        1,
        "the production snapshot must await exactly once; separate turns for \
         revenue, costs and history let a Python write land in between"
    );
    for split in [
        "per_channel_revenue(",
        "per_channel_costs(",
        "channel_history(",
    ] {
        assert!(
            !body.contains(split),
            "the snapshot must not be reassembled from separate reads (found `{split}`)"
        );
    }

    // One turn is only half of it: inside that turn every SELECT must also
    // see one snapshot. Structural for the same reason as the rest -- the
    // actor turn is blocking, so no test can interleave a write into it,
    // and a correct implementation therefore offers no seam to inject.
    let reader = fn_body(&source, "pub(crate) fn read_profitability_snapshot");
    assert!(
        reader.contains("unchecked_transaction"),
        "the fleet read must take one transaction, or its SELECTs can each \
         land on a different WAL snapshot"
    );
    assert!(
        !reader.contains("(conn,"),
        "no sub-read may take the bare connection: that is exactly how the \
         transaction gets bypassed while still appearing to be one turn"
    );
}

/// C71-19: the two halves must arrive as one observation.
///
/// Structural, for the same reason the analyze slice needed one: a
/// correct producer has no seam to inject a concurrent write into, so the
/// only way to pin "one command" is from the source. The production DB is
/// concurrently written by Python under WAL, so two round trips can land
/// on different snapshots.
#[test]
fn the_two_history_reads_are_issued_as_one_command() {
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/profitability_history.rs"
    ))
    .unwrap();
    let body = fn_body(&source, "pub async fn channel_history");

    assert_eq!(
        body.matches(".await").count(),
        1,
        "channel_history must await the store exactly once; two awaits let a \
         Python write land between them and pair rows from different WAL snapshots"
    );
    assert!(
        body.contains("unchecked_transaction") || source.contains("unchecked_transaction"),
        "the coupled read must take one transaction so both SELECTs see one snapshot"
    );
}
