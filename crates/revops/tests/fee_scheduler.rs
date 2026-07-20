//! Integration tests for `revops::fee_scheduler` -- the single-owner
//! fee-cycle scheduler (Phase 4b Task 6, checklist item 5).
//!
//! The threading shell (`spawn`: one owner `std::thread` + one tokio
//! ticker) is deliberately thin; the per-cycle contract lives in
//! `CycleOwner::run_cycle`, which these tests drive synchronously with
//! scripted seams:
//!
//! - **clock**: an injected counting `FnMut() -> i64` (the plan's
//!   "CountingClock" seam) -- production passes `revops::now_unix`.
//! - **prepared inputs**: a hand-built [`PreparedCycle`] (canned
//!   `RpcPrefetch` JSON, `FeeCfgSnapshot::default()`, explicit
//!   `min_competitors`) -- production builds it via `prepare_cycle` on the
//!   async side.
//! - **production DB**: the committed schema-only `fixtures/fixture.db`
//!   copied into a tempdir and seeded, same pattern as
//!   `tests/fee_evidence.rs`.
//! - **journal dir**: a tempdir subdirectory.

use revops::fee_evidence::RpcPrefetch;
use revops::fee_scheduler::{
    read_flush_marker, CycleMsg, CycleOutcome, CycleOwner, FeeDebugQuery, FlushWatcher,
    PollOutcome, PreparedCycle, SchedulerConfig, StateLifecycle, TriggerMode, WatchParams,
    DEFAULT_FLUSH_POLL_SECS, DEFAULT_FLUSH_SETTLE_SECS,
};
use revops::fee_state::STATE_JOURNAL_FILE_NAME;
use revops_fees::cycle::{
    ChannelCycleState, ChannelFeeState, ChannelStateRow, DecisionClock, FeeCfgSnapshot,
};
use revops_fees::journal::JOURNAL_FILE_NAME;
use revops_fees::pyrand::{DecisionInputError, PyRandom};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

/// Fixed cycle clock value -- deliberately different from [`SEED`] so a
/// buggy per-cycle reseed (which would use the cycle clock) lands on a
/// visibly different `PyRandom` stream than the spawn-time seed.
const NOW: i64 = 1_800_000_000;

/// Spawn-time RNG seed (production: `now_unix()` at scheduler start).
const SEED: i64 = 42;

fn peer_a() -> String {
    format!("02{}", "aa".repeat(32))
}

fn fixture_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db")
}

/// Tempdir with a seeded copy of the production-schema fixture DB and an
/// (initially non-existent) journal subdirectory.
struct Fixture {
    _dir: tempfile::TempDir,
    db_path: PathBuf,
    journal_dir: PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("prod.db");
    std::fs::copy(fixture_path(), &db_path).expect("copy fixture.db");
    let conn = Connection::open(&db_path).expect("open seeded copy");
    conn.pragma_update(None, "journal_mode", "WAL")
        .expect("switch to WAL");
    drop(conn);
    let journal_dir = dir.path().join("journal");
    Fixture {
        _dir: dir,
        db_path,
        journal_dir,
    }
}

/// One `channel_states` row (`100x1x0`, peer A). Whether the cycle also
/// PROCESSES the channel depends on a matching `channels_info` entry in
/// the prefetch (see [`prepared`]'s `with_peer_channel`).
fn seed_channel_state(db_path: &Path) {
    let conn = Connection::open(db_path).expect("open for seeding");
    conn.execute(
        "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, \
         sats_out, capacity, updated_at, kalman_flow_ratio, kalman_velocity) \
         VALUES ('100x1x0', ?1, 'balanced', 0.1, 0, 0, 2000000, ?2, 0.05, 0.01)",
        rusqlite::params![peer_a(), NOW - 60],
    )
    .expect("insert channel_states row");
}

/// A `fee_strategy_state` row for `channel_id` (empty v2 blob -- the
/// hydration path fills defaults), for the lifecycle tests.
fn seed_fee_strategy_row(db_path: &Path, channel_id: &str) {
    let conn = Connection::open(db_path).expect("open for seeding");
    conn.execute(
        "INSERT INTO fee_strategy_state (channel_id, v2_state_json) VALUES (?1, '{}')",
        [channel_id],
    )
    .expect("insert fee_strategy_state row");
}

fn delete_fee_strategy_row(db_path: &Path, channel_id: &str) {
    let conn = Connection::open(db_path).expect("open for deleting");
    conn.execute(
        "DELETE FROM fee_strategy_state WHERE channel_id = ?1",
        [channel_id],
    )
    .expect("delete fee_strategy_state row");
}

fn owner(fx: &Fixture, lifecycle: StateLifecycle) -> CycleOwner {
    CycleOwner::new(
        &SchedulerConfig {
            db_path: fx.db_path.clone(),
            // Never dialed by the owner half (RPC prefetch is the async
            // side's job); an obviously-dead path proves that.
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle,
            trigger: TriggerMode::default(),
        },
        SEED,
    )
}

/// `listpeerchannels`-shaped row whose colon-form scid normalizes to the
/// seeded `channel_states` row's `100x1x0` (same canned shape as
/// `tests/fee_evidence.rs`).
fn canned_peer_channel() -> Value {
    json!({
        "state": "CHANNELD_NORMAL",
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
        "htlcs": [],
    })
}

/// Canned prepared inputs. `feerates` yields `sat_per_vbyte = 3.0`; with
/// an empty `mempool_fee_history` the 24h MA is `1.0`, so every cycle sees
/// a Vegas spike ratio of exactly 3.0 (the `2.0 <= ratio < 4.0`
/// probabilistic-boost branch -- the ONE `rng.random()` call sites the RNG
/// continuity test accounts draws with).
fn prepared(min_competitors: Value, with_peer_channel: bool) -> PreparedCycle {
    PreparedCycle {
        cfg: FeeCfgSnapshot::default(),
        min_competitors,
        rpc: RpcPrefetch {
            our_node_id: format!("02{}", "ee".repeat(32)),
            peer_channels: if with_peer_channel {
                vec![canned_peer_channel()]
            } else {
                Vec::new()
            },
            gossip_channels: Vec::new(),
            feerates: Some(json!({"perkb": {"opening": 3000}})),
        },
    }
}

fn line_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .count()
}

// ---------------------------------------------------------------------------
// Per-cycle sequence point 1: ONE clock read per cycle
// ---------------------------------------------------------------------------

#[test]
fn scheduler_uses_one_clock_read_per_cycle() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);

    let reads = std::cell::Cell::new(0usize);
    let mut clock = || {
        reads.set(reads.get() + 1);
        NOW
    };

    let outcome = owner.run_cycle(prepared(json!(3), false), &mut clock);
    assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
    assert_eq!(
        reads.get(),
        1,
        "a full cycle must read the clock exactly once"
    );

    // The skip path still reads the clock exactly once (sequence point 1
    // precedes the min-competitors gate). `json!(2)` is now a VALID
    // threshold (Phase 4b Task 8a; production's own resolved value) --
    // use a genuinely unresolvable value (missing/null) to exercise the
    // skip path here instead.
    let outcome = owner.run_cycle(prepared(Value::Null, false), &mut clock);
    assert!(
        matches!(outcome, CycleOutcome::SkippedMinCompetitors),
        "{outcome:?}"
    );
    assert_eq!(
        reads.get(),
        2,
        "a skipped cycle must also read the clock exactly once"
    );
}

// ---------------------------------------------------------------------------
// ONE long-lived PyRandom, seeded once at scheduler start
// ---------------------------------------------------------------------------

#[test]
fn scheduler_seeds_pyrandom_exactly_once_across_cycles() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);
    let mut clock = || NOW;

    // Draw accounting (no processed channels -- the seeded row has no
    // matching channels_info entry, so the only RNG consumer is
    // `vegas_update`'s spike branch):
    //   cycle 1: spike ratio 3.0, consecutive_spikes 0 -> 1  => 1 draw
    //   cycle 2: spike ratio 3.0, consecutive_spikes 1 -> 2  => 0 draws
    //            (Python's short-circuited `consecutive >= 2 or random()`)
    let o1 = owner.run_cycle(prepared(json!(3), false), &mut clock);
    assert!(matches!(o1, CycleOutcome::Ran { .. }), "{o1:?}");
    let o2 = owner.run_cycle(prepared(json!(3), false), &mut clock);
    assert!(matches!(o2, CycleOutcome::Ran { .. }), "{o2:?}");
    assert_eq!(
        owner.state().vegas.consecutive_spikes,
        2,
        "both cycles must have registered the engineered 3.0x spike"
    );

    let probe = owner.rng_mut().random();

    // Continuous stream: exactly one draw was consumed since the ONE
    // spawn-time seeding, so the probe is draw #2 of the SEED stream.
    let mut reference = PyRandom::seed_from_u64(SEED as u64);
    let cycle1_draw = reference.random();
    assert_eq!(
        probe,
        reference.random(),
        "rng must continue the spawn-seeded stream across cycles (no reseed)"
    );
    assert_ne!(probe, cycle1_draw, "probe must be past cycle 1's draw");

    // Counterfactual: a per-cycle reseed would seed from the cycle clock
    // (`NOW`), landing the probe on that stream's draw #1 (reseed before
    // cycle 2, which draws nothing) or draw #2 (reseed before cycle 1).
    let mut reseeded = PyRandom::seed_from_u64(NOW as u64);
    let reseeded_d1 = reseeded.random();
    let reseeded_d2 = reseeded.random();
    assert_ne!(probe, reseeded_d1, "looks like a reseed before cycle 2");
    assert_ne!(probe, reseeded_d2, "looks like a reseed before cycle 1");
}

// ---------------------------------------------------------------------------
// Journal + state JSONL appends
// ---------------------------------------------------------------------------

struct FailingDecisionClock {
    now: i64,
    fail_label: String,
}

impl DecisionClock for FailingDecisionClock {
    fn now(&mut self, label: &str) -> Result<i64, DecisionInputError> {
        if label == self.fail_label {
            Err(DecisionInputError::new(format!(
                "scripted clock failure: {label}"
            )))
        } else {
            Ok(self.now)
        }
    }
}

#[test]
fn decision_input_error_skips_without_partial_decision_or_state_journals() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);
    let mut wall_clock = || NOW;
    let mut decision_clock = FailingDecisionClock {
        now: NOW,
        fail_label: "cycle.channel.evaluate".to_string(),
    };

    let outcome = owner.run_cycle_with_decision_clock(
        prepared(json!(3), true),
        &mut wall_clock,
        &mut decision_clock,
    );

    assert_eq!(outcome, CycleOutcome::SkippedDecisionInput);
    assert_eq!(line_count(&fx.journal_dir.join(JOURNAL_FILE_NAME)), 0);
    assert_eq!(line_count(&fx.journal_dir.join(STATE_JOURNAL_FILE_NAME)), 0);
}

#[test]
fn dryrun_cycle_appends_decisions_to_journal_and_state_jsonl() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);
    let mut clock = || NOW;

    let journal_path = fx.journal_dir.join(JOURNAL_FILE_NAME);
    let state_path = fx.journal_dir.join(STATE_JOURNAL_FILE_NAME);

    // With a matching channels_info entry the seeded channel is PROCESSED:
    // every processed channel emits exactly one FeeDecision (adjusted or
    // skip) and marks itself dirty for the end-of-cycle state flush.
    let o1 = owner.run_cycle(prepared(json!(3), true), &mut clock);
    assert!(matches!(o1, CycleOutcome::Ran { decisions: 1 }), "{o1:?}");
    let journal_after_1 = line_count(&journal_path);
    let state_after_1 = line_count(&state_path);
    assert_eq!(journal_after_1, 1, "one decision line after cycle 1");
    assert_eq!(state_after_1, 1, "one state flush line after cycle 1");

    let o2 = owner.run_cycle(prepared(json!(3), true), &mut clock);
    assert!(matches!(o2, CycleOutcome::Ran { decisions: 1 }), "{o2:?}");
    assert_eq!(
        line_count(&journal_path),
        journal_after_1 + 1,
        "journal must APPEND (grow), not truncate"
    );
    assert_eq!(
        line_count(&state_path),
        state_after_1 + 1,
        "state jsonl must APPEND (grow), not truncate"
    );

    // Every journal line is valid single-line JSON with the decision keys
    // the diff harness discriminates on.
    let body = std::fs::read_to_string(&journal_path).unwrap();
    for line in body.lines() {
        let v: Value = serde_json::from_str(line).expect("journal line is JSON");
        assert!(v.get("channel_id").is_some(), "line: {line}");
        assert!(v.get("would_broadcast").is_some(), "line: {line}");
    }
}

// ---------------------------------------------------------------------------
// Phase 4b Task 8a fail-closed rule: min competitors must resolve to ANY
// positive integer (production runs 2, not the Task 8 baked 3) --
// refusal is now reserved for genuinely unresolvable values.
// ---------------------------------------------------------------------------

#[test]
fn cycle_runs_with_any_resolvable_positive_min_competitors() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);
    let mut clock = || NOW;

    // 2 is production's actual resolved value; 3 is the old Task 8 bake;
    // 4 and 50 pin that this is a genuine threshold, not a two-value
    // special case.
    for ok in [json!(2), json!(3), json!(4), json!(50)] {
        let outcome = owner.run_cycle(prepared(ok.clone(), true), &mut clock);
        assert!(
            matches!(outcome, CycleOutcome::Ran { .. }),
            "min_competitors={ok} must run the cycle, got {outcome:?}"
        );
    }
}

#[test]
fn cycle_skips_and_logs_when_min_competitors_unresolvable() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);
    let mut clock = || NOW;

    for wrong in [json!("3"), Value::Null, json!(0), json!(-1), json!(2.5)] {
        let outcome = owner.run_cycle(prepared(wrong.clone(), true), &mut clock);
        assert!(
            matches!(outcome, CycleOutcome::SkippedMinCompetitors),
            "min_competitors={wrong} must skip the cycle, got {outcome:?}"
        );
    }

    // A skipped cycle must not have journaled anything.
    assert!(
        !fx.journal_dir.join(JOURNAL_FILE_NAME).exists(),
        "skipped cycles must not write the decision journal"
    );
    assert!(
        !fx.journal_dir.join(STATE_JOURNAL_FILE_NAME).exists(),
        "skipped cycles must not flush state"
    );

    // A subsequent cycle with a resolvable value still runs cleanly.
    let outcome = owner.run_cycle(prepared(json!(2), true), &mut clock);
    assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
}

// ---------------------------------------------------------------------------
// StateLifecycle: RehydratePerCycle (window) vs SeedOnce (cutover flip)
// ---------------------------------------------------------------------------

#[test]
fn rehydrate_per_cycle_drops_channels_deleted_from_db() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    seed_fee_strategy_row(&fx.db_path, "chan_gone");
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);
    let mut clock = || NOW;

    owner.run_cycle(prepared(json!(3), false), &mut clock);
    assert!(owner.state().fee_states.contains_key("chan_gone"));

    delete_fee_strategy_row(&fx.db_path, "chan_gone");
    owner.run_cycle(prepared(json!(3), false), &mut clock);
    assert!(
        !owner.state().fee_states.contains_key("chan_gone"),
        "RehydratePerCycle must re-read persisted state every cycle"
    );
}

#[test]
fn seed_once_hydrates_first_cycle_then_evolves_in_memory() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    seed_fee_strategy_row(&fx.db_path, "chan_kept");
    let mut owner = owner(&fx, StateLifecycle::SeedOnce);
    let mut clock = || NOW;

    owner.run_cycle(prepared(json!(3), false), &mut clock);
    assert!(
        owner.state().fee_states.contains_key("chan_kept"),
        "SeedOnce must hydrate from the DB on the FIRST cycle"
    );

    delete_fee_strategy_row(&fx.db_path, "chan_kept");
    owner.run_cycle(prepared(json!(3), false), &mut clock);
    assert!(
        owner.state().fee_states.contains_key("chan_kept"),
        "SeedOnce must NOT re-read the DB after the first cycle"
    );
}

// ---------------------------------------------------------------------------
// T6b: flush-observation triggering (Design Note 1 -- every Rust cycle is an
// independent parity trial keyed off Python's end-of-cycle state flush)
// ---------------------------------------------------------------------------

/// Watch parameters used across the trigger tests: 30s poll / 30s settle
/// (the window defaults) and staleness at 2x the default 1800s interval.
fn watch_params() -> WatchParams {
    WatchParams {
        settle_secs: 30,
        stale_after_secs: 2 * 1800,
    }
}

#[test]
fn default_trigger_mode_is_flush_triggered_for_the_window() {
    assert_eq!(
        TriggerMode::default(),
        TriggerMode::FlushTriggered {
            poll_secs: DEFAULT_FLUSH_POLL_SECS,
            settle_secs: DEFAULT_FLUSH_SETTLE_SECS,
        },
        "the dry-run window must default to flush-observation triggering"
    );
}

#[test]
fn flush_advance_triggers_exactly_one_cycle_after_settle() {
    let t0: i64 = 1_800_000_000;
    let p = watch_params();
    let mut w = FlushWatcher::new(t0);

    // First successful read is a BASELINE, never a trigger: the marker's
    // age is unknown at plugin start (could be mid-Python-cycle).
    assert_eq!(w.on_poll(Some(470), t0 + 30, &p), PollOutcome::Baselined);
    assert_eq!(w.on_poll(Some(470), t0 + 60, &p), PollOutcome::Idle);

    // Python flushes (batch INSERT OR REPLACE bumps MAX(rowid)): the
    // advance arms the settle delay -- no cycle yet.
    assert_eq!(w.on_poll(Some(517), t0 + 90, &p), PollOutcome::Advanced);
    // Settle elapsed -> exactly one cycle.
    assert_eq!(w.on_poll(Some(517), t0 + 120, &p), PollOutcome::RunCycle);
    // No further advance -> no further cycles.
    assert_eq!(w.on_poll(Some(517), t0 + 150, &p), PollOutcome::Idle);
    assert_eq!(w.on_poll(Some(517), t0 + 180, &p), PollOutcome::Idle);

    // Next flush -> next single cycle.
    assert_eq!(w.on_poll(Some(564), t0 + 210, &p), PollOutcome::Advanced);
    assert_eq!(w.on_poll(Some(564), t0 + 240, &p), PollOutcome::RunCycle);
    assert_eq!(w.on_poll(Some(564), t0 + 270, &p), PollOutcome::Idle);
}

#[test]
fn successive_writes_inside_settle_coalesce_into_one_cycle() {
    // A change observed while still settling re-arms the delay (wait for
    // quiescence) instead of double-firing: one cycle per flushed state.
    let t0: i64 = 1_800_000_000;
    let p = watch_params();
    let mut w = FlushWatcher::new(t0);
    assert_eq!(w.on_poll(Some(100), t0 + 30, &p), PollOutcome::Baselined);
    assert_eq!(w.on_poll(Some(147), t0 + 60, &p), PollOutcome::Advanced);
    // Still moving (e.g. flush then immediate out-of-cycle row write):
    assert_eq!(w.on_poll(Some(148), t0 + 90, &p), PollOutcome::Advanced);
    assert_eq!(w.on_poll(Some(148), t0 + 120, &p), PollOutcome::RunCycle);
    assert_eq!(w.on_poll(Some(148), t0 + 150, &p), PollOutcome::Idle);
}

#[test]
fn prune_shrinking_the_marker_still_counts_as_an_advance() {
    // `_prune_stale_states` DELETEs rows (can lower MAX(rowid)) and VACUUM
    // renumbers: the watcher triggers on ANY change, not just increase, so
    // a shrink can never make a later real flush unobservable.
    let t0: i64 = 1_800_000_000;
    let p = watch_params();
    let mut w = FlushWatcher::new(t0);
    assert_eq!(w.on_poll(Some(500), t0 + 30, &p), PollOutcome::Baselined);
    assert_eq!(w.on_poll(Some(430), t0 + 60, &p), PollOutcome::Advanced);
    assert_eq!(w.on_poll(Some(430), t0 + 90, &p), PollOutcome::RunCycle);
}

#[test]
fn no_flush_advance_never_cycles_and_goes_loud_after_2x_interval() {
    let t0: i64 = 1_800_000_000;
    let p = watch_params(); // stale after 3600s
    let mut w = FlushWatcher::new(t0);
    assert_eq!(w.on_poll(Some(470), t0 + 30, &p), PollOutcome::Baselined);

    // Python dead/paused: the marker never moves. Poll for 2x interval
    // (measured from the baseline observation): never a cycle.
    let mut now = t0 + 60;
    let mut first_stale_at: Option<i64> = None;
    while now <= t0 + 30 + 2 * 3600 {
        match w.on_poll(Some(470), now, &p) {
            PollOutcome::Idle => {}
            PollOutcome::StaleNoFlush { silent_secs } => {
                assert!(
                    silent_secs > 3600,
                    "stale report before 2x interval of silence ({silent_secs}s)"
                );
                if first_stale_at.is_none() {
                    first_stale_at = Some(now);
                }
            }
            other => panic!("no-advance polling must never cycle, got {other:?} at {now}"),
        }
        now += 30;
    }
    // Loud exactly when the 2x-interval bound is first crossed...
    assert_eq!(
        first_stale_at,
        Some(t0 + 30 + 3600 + 30),
        "first stale report must land on the first poll past 2x interval"
    );
    // ...and the report re-arms (rate-limited loudness) instead of firing
    // every 30s poll: two reports in the 2x-interval span polled above.
    let stale_count = {
        let mut w2 = FlushWatcher::new(t0);
        let mut n = 0;
        w2.on_poll(Some(470), t0 + 30, &p);
        let mut t = t0 + 60;
        // Long enough for two re-arms (first report at baseline+3630,
        // second one stale_after later), far short of a third.
        while t <= t0 + 30 + 3630 + 3600 + 60 {
            if matches!(
                w2.on_poll(Some(470), t, &p),
                PollOutcome::StaleNoFlush { .. }
            ) {
                n += 1;
            }
            t += 30;
        }
        n
    };
    assert_eq!(stale_count, 2, "one loud report per stale_after of silence");

    // A flush after the outage triggers normally again.
    assert_eq!(w.on_poll(Some(517), now, &p), PollOutcome::Advanced);
    assert_eq!(w.on_poll(Some(517), now + 30, &p), PollOutcome::RunCycle);
}

#[test]
fn flush_marker_advances_on_value_identical_batch_flush() {
    // The marker property everything above rests on: Python's end-of-cycle
    // flush is `INSERT OR REPLACE` inside one transaction
    // (database.update_fee_strategy_states_batch), which re-inserts every
    // row with a FRESH rowid -- so MAX(_rowid_) steps once per flush even
    // when every column value is byte-identical (a no-adjustment cycle,
    // where the `last_update` observation cursor does NOT move).
    let fx = fixture();
    let m0 = read_flush_marker(&fx.db_path).expect("read empty table");
    assert_eq!(
        m0, None,
        "schema-only fixture has no fee_strategy_state rows"
    );

    let flush = |values_tag: i64| {
        let conn = Connection::open(&fx.db_path).expect("open for flushing");
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        for chan in ["100x1x0", "200x1x0", "300x1x0"] {
            conn.execute(
                "INSERT OR REPLACE INTO fee_strategy_state \
                 (channel_id, last_update, v2_state_json) VALUES (?1, ?2, '{}')",
                rusqlite::params![chan, values_tag],
            )
            .unwrap();
        }
        conn.execute_batch("COMMIT").unwrap();
    };

    flush(1000);
    let m1 = read_flush_marker(&fx.db_path)
        .expect("read after flush 1")
        .expect("rows exist");

    // Flush the IDENTICAL rows again (the stalled-cursor cycle): the
    // marker must still advance.
    flush(1000);
    let m2 = read_flush_marker(&fx.db_path)
        .expect("read after flush 2")
        .expect("rows exist");
    assert!(
        m2 > m1,
        "value-identical INSERT OR REPLACE flush must advance MAX(rowid) ({m1} -> {m2})"
    );

    // And a DELETE (prune) changes it downward -- observable too.
    let conn = Connection::open(&fx.db_path).unwrap();
    conn.execute("DELETE FROM fee_strategy_state WHERE _rowid_ = ?1", [m2])
        .unwrap();
    let m3 = read_flush_marker(&fx.db_path).unwrap().unwrap();
    assert!(
        m3 < m2,
        "prune of the max row must be visible ({m2} -> {m3})"
    );
}

#[test]
fn jittered_python_walk_defeats_fixed_ticks_but_not_flush_triggering() {
    // Simulate production Python (cl-revenue-ops.py fee_adjustment_loop):
    // first cycle at +90s, then sleep `interval +/- 20% jitter` AFTER each
    // cycle -- an unphased random walk. The merged scheduler's fixed ticks
    // (`start + interval + 120 + k*interval`) rely on landing AFTER the
    // flush they hydrate; the walk breaks that within a few cycles, while
    // flush-observation stays paired 1:1.
    const INTERVAL: i64 = 1800;
    const POLL: i64 = 30;
    const SETTLE: i64 = 30;
    let t0: i64 = 1_800_000_000;

    // Deterministic jitter stream (PyRandom mirrors CPython's Mersenne
    // Twister; exact randint parity is irrelevant here -- only the +/-20%
    // unphased-walk SHAPE matters).
    let mut jitter = PyRandom::seed_from_u64(1337);
    let mut flushes: Vec<i64> = Vec::new();
    let mut t = t0 + 90;
    for _ in 0..12 {
        flushes.push(t); // sim: cycle duration ~0 -> flush lands at cycle start
        let j = ((jitter.random() * 2.0 - 1.0) * 0.2 * INTERVAL as f64).round() as i64;
        t += INTERVAL + j;
    }

    // MAX(rowid) marker: 47 pre-existing rows, every flush rewrites all 47.
    let marker_at = |now: i64| 47 + 47 * flushes.iter().filter(|f| **f <= now).count() as i64;

    // Flush-triggered mode: drive the watcher at the 30s poll cadence.
    let p = WatchParams {
        settle_secs: SETTLE as u64,
        stale_after_secs: (2 * INTERVAL) as u64,
    };
    let mut w = FlushWatcher::new(t0);
    let mut runs: Vec<i64> = Vec::new();
    let end = *flushes.last().unwrap() + 200;
    let mut now = t0 + POLL;
    while now <= end {
        if matches!(
            w.on_poll(Some(marker_at(now)), now, &p),
            PollOutcome::RunCycle
        ) {
            runs.push(now);
        }
        now += POLL;
    }

    // Exactly one Rust cycle per Python flush, each strictly after its
    // flush (fresh state) and within observe+settle+poll of it.
    assert_eq!(runs.len(), flushes.len(), "one parity trial per flush");
    for (k, (f, r)) in flushes.iter().zip(&runs).enumerate() {
        assert!(
            r > f && *r <= f + POLL + SETTLE + POLL,
            "trial {k} at {r} not in ({f}, {}]",
            f + POLL + SETTLE + POLL
        );
    }

    // Counterfactual: the merged fixed-interval schedule. Tick k was
    // phased to hydrate flush k+1 (first tick at interval+120 vs Python's
    // second flush at ~90+interval+j0). The jitter walk makes some tick
    // fire BEFORE its flush -- hydrating the PREVIOUS cycle's stale state,
    // a timing (not porting) decision mismatch.
    let misfire = (0..flushes.len() - 1)
        .any(|k| t0 + INTERVAL + 120 + (k as i64) * INTERVAL < flushes[k + 1]);
    assert!(
        misfire,
        "fixed ticks were expected to decay against the jitter walk; if the \
         seed no longer produces a misfire, extend the horizon"
    );
}

// ---------------------------------------------------------------------------
// T6 Minor: spawn failure must surface, not hand back a dead-letter handle
// ---------------------------------------------------------------------------

#[test]
fn spawn_surfaces_owner_thread_spawn_failure() {
    let fx = fixture();
    let result = revops::fee_scheduler::spawn_with_thread_spawner(
        SchedulerConfig {
            db_path: fx.db_path.clone(),
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle: StateLifecycle::RehydratePerCycle,
            trigger: TriggerMode::default(),
        },
        None,
        std::collections::HashMap::new(),
        |_name, _body| Err(std::io::Error::other("no threads left")),
    );
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("a failed owner-thread spawn must return Err, not a usable-looking handle"),
    };
    assert!(
        format!("{err:#}").contains("no threads left"),
        "error must carry the spawn failure cause: {err:#}"
    );
}

// ---------------------------------------------------------------------------
// Phase 4b Task 7: `revenue-r-fee-debug` query + wake/policy CycleMsg
// triggers (checklist item 7)
// ---------------------------------------------------------------------------

/// `FeeDebugQuery::Channel` must byte-match Python's `get_dts_summary`
/// shape (`fee_controller.py` 5087-5122): `{posterior_mean, posterior_std,
/// broadcast_fee_ppm, forward_count}`.
///
/// The committed fixture (`fixtures/fees/cycle/fee_debug_dts_summary.json`)
/// was generated by running the REAL Python `get_dts_summary` (from
/// `~/bin/cl_revenue_ops-port`) over a `ChannelFeeState` seeded with this
/// test's exact values (`FeeController.__new__` + a bare `_state_lock`/
/// `_channel_fee_states`, then `json.dumps(fc.get_dts_summary("c1"))`) --
/// so both the field names AND the values are Python's own output, not a
/// hand-transcription. Python also returns `None` (not a dict) for an
/// unknown channel; this port's RPC surface maps that to an error object
/// (a JSON-RPC response cannot be bare null through cln-plugin).
#[test]
fn fee_debug_query_returns_dts_summary_shape() {
    let fx = fixture();
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);

    let mut fee_state = ChannelFeeState::default();
    fee_state.thompson.posterior_mean = 345.5;
    fee_state.thompson.posterior_std = 12.25;
    fee_state.last_broadcast_fee_ppm = 777;
    fee_state.forward_count_since_update = 9;
    owner
        .state_mut()
        .fee_states
        .insert("c1".to_string(), fee_state);

    let value = owner.fee_debug(&FeeDebugQuery::Channel("c1".to_string()));

    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/fees/cycle/fee_debug_dts_summary.json");
    let expected: Value =
        serde_json::from_str(&std::fs::read_to_string(&fixture_path).expect("read fixture"))
            .expect("parse fixture");
    assert_eq!(
        value,
        expected,
        "revenue-r-fee-debug channel query must match the committed \
         get_dts_summary-shaped fixture at {}",
        fixture_path.display()
    );

    // Unknown channel_id -> an error shape, never a panic or empty object.
    let missing = owner.fee_debug(&FeeDebugQuery::Channel("nope".to_string()));
    assert!(missing.get("error").is_some(), "{missing:?}");
}

/// `FeeDebugQuery::Summary` -- `last_decision_summary` (py
/// `get_last_decision_summary`, 3031-3048) plus the per-channel map (each
/// entry the same shape as the `Channel` query above).
#[test]
fn fee_debug_query_summary_includes_last_decision_and_channel_map() {
    let fx = fixture();
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);

    let mut fee_state = ChannelFeeState::default();
    fee_state.thompson.posterior_mean = 10.0;
    fee_state.thompson.posterior_std = 1.0;
    fee_state.last_broadcast_fee_ppm = 50;
    fee_state.forward_count_since_update = 2;
    owner
        .state_mut()
        .fee_states
        .insert("chanA".to_string(), fee_state);

    let value = owner.fee_debug(&FeeDebugQuery::Summary);
    assert_eq!(
        value["last_cycle_decision"],
        json!({
            "action": "hold",
            "reason": "not_run",
            "dominant_input": "startup",
            "safety_block": false,
        }),
        "must default to Python's startup summary (py 3024-3030) before any cycle runs"
    );
    assert_eq!(
        value["channels"]["chanA"],
        json!({
            "posterior_mean": 10.0,
            "posterior_std": 1.0,
            "broadcast_fee_ppm": 50,
            "forward_count": 2,
        })
    );
}

/// [`CycleOwner::wake_all`] -- `wake_all_sleeping_channels`'s handler for
/// [`CycleMsg::WakeAll`] (the manual `revenue-r-fee-wake` RPC's trigger).
#[test]
fn wake_all_msg_clears_sleep_state() {
    let fx = fixture();
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);

    let mut sleeping = ChannelCycleState::default();
    sleeping.is_sleeping = true;
    sleeping.sleep_until = 99;
    sleeping.stable_cycles = 3;
    owner
        .state_mut()
        .cycle_states
        .insert("c1".to_string(), sleeping);

    let woken = owner.wake_all(NOW);
    assert_eq!(woken, 1);
    let c1 = &owner.state().cycle_states["c1"];
    assert!(!c1.is_sleeping);
    assert_eq!(c1.sleep_until, 0);
    assert_eq!(c1.stable_cycles, 0);
}

/// [`CycleOwner::vegas_spike_check`] -- `_maybe_wake_for_vegas_spike`'s
/// edge-trigger contract (py 4386-4411, `VEGAS_WAKE_INTENSITY_THRESHOLD`
/// 0.5 / `VEGAS_WAKE_REARM_INTENSITY` 0.3): fires exactly once per rising
/// edge, refuses to refire while still armed-off, and rearms only once
/// intensity drops back below the (lower) rearm threshold.
#[test]
fn vegas_spike_check_respects_wake_armed_edge_trigger() {
    let fx = fixture();
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);

    let mut sleeping = ChannelCycleState::default();
    sleeping.is_sleeping = true;
    owner
        .state_mut()
        .cycle_states
        .insert("c1".to_string(), sleeping);
    assert!(owner.state().vegas_wake_armed, "must start armed");

    // Below threshold: armed, no fire, channel stays asleep.
    owner.state_mut().vegas.intensity = 0.4;
    assert!(!owner.vegas_spike_check(NOW));
    assert!(owner.state().cycle_states["c1"].is_sleeping);

    // Rising edge crosses 0.5: fires exactly once, disarms.
    owner.state_mut().vegas.intensity = 0.6;
    assert!(owner.vegas_spike_check(NOW));
    assert!(!owner.state().cycle_states["c1"].is_sleeping);
    assert!(!owner.state().vegas_wake_armed);

    // Re-sleep the channel to observe the disarmed no-refire behavior.
    owner
        .state_mut()
        .cycle_states
        .get_mut("c1")
        .unwrap()
        .is_sleeping = true;
    assert!(
        !owner.vegas_spike_check(NOW),
        "must not refire while still disarmed, even with intensity still high"
    );
    assert!(
        owner.state().cycle_states["c1"].is_sleeping,
        "a disarmed check must not wake anything"
    );

    // Drop below the REARM threshold (0.3, distinct from the 0.5 fire
    // threshold): rearms, but this same call still returns false (rearm
    // and fire are different polls in Python too).
    owner.state_mut().vegas.intensity = 0.2;
    assert!(!owner.vegas_spike_check(NOW));
    assert!(owner.state().vegas_wake_armed);

    // Spiking again now that it's rearmed fires again.
    owner.state_mut().vegas.intensity = 0.6;
    assert!(owner.vegas_spike_check(NOW));
    assert!(!owner.state().cycle_states["c1"].is_sleeping);
}

/// [`CycleOwner::policy_changed`] -- `_handle_policy_change`'s handler for
/// [`CycleMsg::PolicyChanged`]: wakes ONLY the named peer's channels.
#[test]
fn policy_changed_msg_wakes_only_the_named_peers_channels() {
    let fx = fixture();
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);

    let mut sleeping_a = ChannelCycleState::default();
    sleeping_a.is_sleeping = true;
    owner
        .state_mut()
        .cycle_states
        .insert("chanA".to_string(), sleeping_a);
    let mut sleeping_b = ChannelCycleState::default();
    sleeping_b.is_sleeping = true;
    owner
        .state_mut()
        .cycle_states
        .insert("chanB".to_string(), sleeping_b);

    let other_peer = format!("03{}", "bb".repeat(32));
    let rows = vec![
        ChannelStateRow {
            channel_id: "chanA".to_string(),
            peer_id: peer_a(),
            state: "balanced".to_string(),
            updated_at: None,
            kalman_flow_ratio: None,
            kalman_velocity: None,
        },
        ChannelStateRow {
            channel_id: "chanB".to_string(),
            peer_id: other_peer.clone(),
            state: "balanced".to_string(),
            updated_at: None,
            kalman_flow_ratio: None,
            kalman_velocity: None,
        },
    ];

    let woken = owner.policy_changed(&rows, &peer_a());
    assert_eq!(woken, 1);
    assert!(!owner.state().cycle_states["chanA"].is_sleeping);
    assert!(
        owner.state().cycle_states["chanB"].is_sleeping,
        "a different peer's channel must stay untouched"
    );

    // The other peer's own policy change wakes ITS channel.
    let woken2 = owner.policy_changed(&rows, &other_peer);
    assert_eq!(woken2, 1);
    assert!(!owner.state().cycle_states["chanB"].is_sleeping);
}

/// End-to-end wiring proof: `WakeAll` and `Query` sent over the REAL
/// `SchedulerHandle::tx` reach the owner thread's actual match arms (not
/// just the `CycleOwner` methods those arms call) and the `Query` reply
/// round-trips through its `std::sync::mpsc` channel.
#[tokio::test]
async fn scheduler_dispatches_wake_and_query_messages_through_owner_thread() {
    let fx = fixture();
    let handle = revops::fee_scheduler::spawn_with_thread_spawner(
        SchedulerConfig {
            db_path: fx.db_path.clone(),
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle: StateLifecycle::RehydratePerCycle,
            // Phase offset far past this test's lifetime: no tick fires.
            trigger: TriggerMode::FixedInterval {
                phase_offset_secs: 999_999,
            },
        },
        None,
        std::collections::HashMap::new(),
        |name, body| {
            std::thread::Builder::new()
                .name(name.to_string())
                .spawn(body)
                .map(|_join| ())
        },
    )
    .expect("spawn scheduler");

    // Fire-and-forget: must not crash the owner thread.
    handle.tx.send(CycleMsg::WakeAll).expect("send WakeAll");

    // Query must round-trip through the real reply channel.
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    handle
        .tx
        .send(CycleMsg::Query(FeeDebugQuery::Summary, reply_tx))
        .expect("send Query");
    let value = reply_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("owner thread replied");
    assert!(value.get("last_cycle_decision").is_some(), "{value:?}");
    assert!(value.get("channels").is_some(), "{value:?}");

    handle.tx.send(CycleMsg::Shutdown).ok();
}

// ---------------------------------------------------------------------------
// Global Constraint: no broadcast code in this phase at all
// ---------------------------------------------------------------------------

/// Source-scan guard: the literal broadcast RPC name must not appear
/// anywhere in `crates/revops/src`. The cutover task that introduces the
/// broadcast path removes this test in the same commit (plan, "Deferred to
/// Cutover").
#[test]
fn no_setchannel_symbol_in_crate() {
    let src_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let needle: String = ["set", "channel"].concat(); // keep this file clean of the literal
    let mut scanned = 0usize;
    for entry in std::fs::read_dir(&src_dir).expect("read src dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        scanned += 1;
        let body = std::fs::read_to_string(&path).expect("read source file");
        assert!(
            !body.to_lowercase().contains(&needle),
            "broadcast symbol `{needle}` found in {} -- no broadcast code \
             is allowed in this phase (Global Constraint)",
            path.display()
        );
    }
    assert!(scanned >= 10, "scanned only {scanned} files -- wrong dir?");
}
