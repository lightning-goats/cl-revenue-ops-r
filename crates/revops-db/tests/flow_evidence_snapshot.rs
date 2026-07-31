//! F71-R23 / C71-14 / C71-15: the paired flow-evidence read.
//!
//! `flow_evidence_snapshot` exists so the two facts `revenue-r-analyze`
//! reasons about arrive as ONE observation. These tests pin the two
//! properties that make it worth having a dedicated command at all:
//! the pair is consistent, and the loop-health half decides first.

use revops_db::analytics::{
    flow_evidence_snapshot, upsert_channel_flow_state, ChannelFlowStateRow,
};
use revops_db::loop_health::{
    begin_loop_pass, finish_loop_pass, register_loop, LoopId, TerminalStatus, WiringStatus,
};
use rusqlite::Connection;

const NOW: i64 = 1_800_000_000;
const BOOT: &str = "boot-current";
const SCID: &str = "700x1x0";

fn row(boot_id: &str, forward_count: i64) -> ChannelFlowStateRow {
    ChannelFlowStateRow {
        scid: SCID.to_string(),
        peer_id: "02aa".to_string(),
        flow_state: "BALANCED".to_string(),
        balance_position: "balanced".to_string(),
        flow_ratio: 0.5,
        velocity: 1.0,
        confidence: 0.9,
        kalman_flow_ratio: 0.5,
        kalman_velocity: 1.0,
        kalman_uncertainty: 0.1,
        kalman_regime_change: false,
        forward_count,
        updated_at: NOW,
        boot_id: boot_id.to_string(),
    }
}

/// Both stores initialised — the normal production shape.
fn full_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    revops_db::loop_health::init_schema(&conn).expect("loop health schema");
    revops_db::analytics::init_schema(&conn).expect("analytics schema");
    conn
}

#[test]
fn the_snapshot_reports_the_loop_and_the_row_together() {
    let mut conn = full_db();
    register_loop(&conn, LoopId::FlowAnalysis, WiringStatus::Ready, NOW).unwrap();
    let generation = begin_loop_pass(&conn, LoopId::FlowAnalysis, BOOT, NOW).unwrap();
    upsert_channel_flow_state(&conn, &row(BOOT, generation as i64)).unwrap();
    finish_loop_pass(&conn, LoopId::FlowAnalysis, generation, BOOT, NOW).unwrap();

    let snapshot = flow_evidence_snapshot(&mut conn, SCID).unwrap();
    let flow_loop = snapshot.flow_loop.expect("the loop is registered");
    assert_eq!(flow_loop.terminal_status, TerminalStatus::Passed);
    assert_eq!(flow_loop.terminal_generation, generation);
    let got = snapshot.row.expect("the pass wrote this channel");
    assert_eq!(
        got.forward_count, generation as i64,
        "the row must be the one the reported terminal generation wrote"
    );
}

/// C71-15. An unregistered flow loop is decided BEFORE the row is read.
///
/// The precedence is not cosmetic. The row query can fail on its own --
/// here the flow-state table does not exist at all -- and that failure
/// surfaces to the caller as `store_unavailable`, which reads as
/// transient and retryable. `loop_unregistered` is neither: it is a
/// permanent wiring defect that needs an operator. Reading the row when
/// it cannot change the verdict lets the transient-looking error mask the
/// permanent one, and the caller then waits for a recovery that will
/// never come.
#[test]
fn an_unregistered_loop_is_not_masked_by_a_failing_row_read() {
    let mut conn = Connection::open_in_memory().expect("open in-memory db");
    revops_db::loop_health::init_schema(&conn).expect("loop health schema");
    // Deliberately NOT `analytics::init_schema` -- `rust_channel_flow_states`
    // does not exist, so any row query errors.
    assert!(
        conn.prepare("SELECT 1 FROM rust_channel_flow_states")
            .is_err(),
        "precondition: the flow-state table must be absent for this test to mean anything"
    );

    let snapshot = flow_evidence_snapshot(&mut conn, SCID)
        .expect("an unregistered loop must be reported, not masked by the row read failing");
    assert!(
        snapshot.flow_loop.is_none(),
        "the loop was never registered"
    );
    assert!(
        snapshot.row.is_none(),
        "no row may be reported when the loop half already decided the verdict"
    );
}

/// C71-16, the control for the test above.
///
/// `an_unregistered_loop_is_not_masked_by_a_failing_row_read` would also
/// pass if the fix had been "ignore row-read failures" -- and that fix
/// would be much worse than the bug, because a damaged or missing
/// flow-state table would then be reported as `analysis: null`, a
/// confident claim about a channel nothing was ever read for.
///
/// The narrow fix is precedence, not suppression: when the loop IS
/// registered the row read still decides the answer, so its failure must
/// still reach the caller. Same missing table, opposite verdict.
#[test]
fn a_registered_loop_still_surfaces_a_failing_row_read() {
    let mut conn = Connection::open_in_memory().expect("open in-memory db");
    revops_db::loop_health::init_schema(&conn).expect("loop health schema");
    // Same absent `rust_channel_flow_states` as the test above...
    register_loop(&conn, LoopId::FlowAnalysis, WiringStatus::Ready, NOW).unwrap();
    // ...but this time the loop exists, so the row genuinely matters.

    let error = flow_evidence_snapshot(&mut conn, SCID)
        .expect_err("a registered loop's row read must not be silently skipped or swallowed");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("rust_channel_flow_states"),
        "the failure must name the store that could not be read, got: {rendered}"
    );
}

/// The same precedence when the row read would have SUCCEEDED: a stray
/// row from an earlier process must not be carried alongside a verdict
/// that does not depend on it.
#[test]
fn an_unregistered_loop_reports_no_row_even_when_one_exists() {
    let mut conn = full_db();
    upsert_channel_flow_state(&conn, &row("boot-previous", 99)).unwrap();

    let snapshot = flow_evidence_snapshot(&mut conn, SCID).unwrap();
    assert!(snapshot.flow_loop.is_none());
    assert!(
        snapshot.row.is_none(),
        "a row that cannot affect the verdict must not travel with it"
    );
}
