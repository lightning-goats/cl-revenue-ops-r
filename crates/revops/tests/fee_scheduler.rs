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

use revops::fee_evidence::{RpcPrefetch, MEMPOOL_MA_WINDOW_SECONDS};
use revops::fee_scheduler::{
    read_flush_marker, CycleMsg, CycleOutcome, CycleOwner, FailedForwardSignal, FeeDebugQuery,
    FlushWatcher, PollOutcome, PreparedCycle, SchedulerConfig, StateLifecycle, TriggerMode,
    WatchParams, DEFAULT_FLUSH_POLL_SECS, DEFAULT_FLUSH_SETTLE_SECS,
    FAILURE_NUDGE_GOSSIP_SETTLE_SECONDS, FAILURE_NUDGE_MIN_INTERVAL_SECONDS,
    TRIGGER_QUEUE_CAPACITY,
};
use revops::fee_state::STATE_JOURNAL_FILE_NAME;
use revops_fees::cycle::{ChannelCycleState, ChannelFeeState, ChannelStateRow, FeeCfgSnapshot};
use revops_fees::journal::JOURNAL_FILE_NAME;
use revops_fees::pyrand::PyRandom;
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
        // No Rust-owned store: correct for RehydratePerCycle (which never
        // touches one); SeedOnce tests build their own via the
        // seedonce_restart harness below.
        None,
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

/// [`prepared`] with Vegas Reflex disabled -- Task 6: `SeedOnce` cycles now
/// fail-closed on `mempool_ma_24h` without a fresh Rust-owned sample (see
/// `mod mempool_evidence` below), so tests whose PURPOSE is unrelated to
/// mempool evidence (hydration/lifecycle plumbing) opt out of that gate
/// the same way `seedonce_restart`'s own harness already does.
fn prepared_no_vegas(min_competitors: Value, with_peer_channel: bool) -> PreparedCycle {
    let mut p = prepared(min_competitors, with_peer_channel);
    p.cfg.enable_vegas_reflex = false;
    p
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
    // Task 5: SeedOnce is restart-persistent and REQUIRES a Rust-owned
    // store (the no-store fail-closed case is pinned in seedonce_restart).
    let mut owner = seedonce_restart::owner_with_test_store(&fx);
    let mut clock = || NOW;

    let outcome = owner.run_cycle(prepared_no_vegas(json!(3), false), &mut clock);
    assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
    assert!(
        owner.state().fee_states.contains_key("chan_kept"),
        "SeedOnce must hydrate from the DB on the FIRST cycle"
    );

    delete_fee_strategy_row(&fx.db_path, "chan_kept");
    let outcome = owner.run_cycle(prepared_no_vegas(json!(3), false), &mut clock);
    assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
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
        revops::config_resolve::PythonOptionCache::empty(),
        None,
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

/// Task 10: `FeeDebugQuery::RunwayCounters` -- the read-only in-memory
/// counters `revenue-r-fee-runway-status` surfaces. Before any cycle runs:
/// `lifecycle` reflects the constructor argument, `hydrated_once`/
/// `seed_refused`/`persistence_failures` are all at their zero defaults,
/// the trigger queue is empty, `last_cycle` is `null`, and the ledger
/// reflects whatever `GovernorWiring::open` resolved for this fixture's
/// journal dir (opened successfully, since `fixture()` gives a real
/// tempdir).
#[test]
fn fee_debug_query_runway_counters_reports_lifecycle_and_zeroed_counters() {
    let fx = fixture();
    let owner = owner(&fx, StateLifecycle::SeedOnce);

    let value = owner.fee_debug(&FeeDebugQuery::RunwayCounters);
    assert_eq!(value["lifecycle"], json!("seed_once"));
    assert_eq!(value["hydrated_once"], json!(false));
    assert_eq!(value["seed_refused"], json!(false));
    assert_eq!(value["persistence_failures"], json!(0));
    assert_eq!(
        value["trigger_queue"],
        json!({"pending": 0, "dropped_total": 0})
    );
    assert_eq!(value["last_cycle"], json!({"at": null, "outcome": null}));
    assert_eq!(value["last_profile"], json!("active"));
    assert_eq!(value["governor_ledger_open"], json!(true));
}

/// Companion: `RehydratePerCycle` reports its own lifecycle label, proving
/// the label is read live off the owner's actual configured lifecycle
/// rather than a hardcoded constant.
#[test]
fn fee_debug_query_runway_counters_reports_rehydrate_per_cycle_lifecycle() {
    let fx = fixture();
    let owner = owner(&fx, StateLifecycle::RehydratePerCycle);
    let value = owner.fee_debug(&FeeDebugQuery::RunwayCounters);
    assert_eq!(value["lifecycle"], json!("rehydrate_per_cycle"));
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
/// A minimal fee-relevant failed forward. `failcode` 4108 is
/// WIRE_FEE_INSUFFICIENT (0x1000|12), so `is_fee_relevant_failure` accepts it and the
/// signal reaches the scheduler's own guards.
fn failed_forward(channel_id: &str, now: i64) -> FailedForwardSignal {
    FailedForwardSignal {
        channel_id: channel_id.to_string(),
        amount_msat: 0,
        failcode: Some(4108),
        failreason: None,
        event_ts: now,
    }
}

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
        revops::config_resolve::PythonOptionCache::empty(),
        None,
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
// Task 9 (stateful-shadow revision plan): the cutover task that introduces
// the guarded broadcast path. Per this test's OWN prior doc comment
// ("Deferred to Cutover"), the `no_setchannel_symbol_in_crate` source-scan
// guard is removed in this same commit -- `revops::fee_execution` now
// legitimately contains the `setchannel` literal (the one guarded action
// call site). What replaces it: proof that the SHADOW/autonomous cycle
// path is still structurally connection-free -- it has no broadcaster
// field or executor capable of dialing CLN at all (see
// `revops_fees::execution::RecordingFeeExecutor`), which this test
// confirms against a REAL live listener rather than trusting the type
// alone.
// ---------------------------------------------------------------------------

/// A SeedOnce cycle (the autonomous-shadow lifecycle) must make ZERO
/// connections to a live, listening CLN socket -- it has no broadcaster to
/// call. Points `socket_path` at a real `UnixListener` (unlike every other
/// SeedOnce test in this file, which points at `/nonexistent/lightning-rpc`
/// to prove the OWNER thread never dials RPC prefetch) so a regression that
/// somehow wired a broadcaster into the cycle path would be caught even
/// though a live listener is available and ready to accept.
#[tokio::test]
async fn seedonce_cycle_makes_zero_connections_to_a_live_cln_socket() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);

    let socket_path = fx._dir.path().join("lightning-rpc");
    let listener = tokio::net::UnixListener::bind(&socket_path).expect("bind fake cln socket");
    let connections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let connections_task = connections.clone();
    tokio::spawn(async move {
        loop {
            let Ok((_stream, _)) = listener.accept().await else {
                return;
            };
            connections_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });

    let mut owner = seedonce_restart::owner_with_test_store_and_socket(&fx, socket_path);
    let mut clock = || NOW;
    owner.run_cycle(prepared(json!(3), true), &mut clock);
    let mut clock2 = || NOW + 1800;
    owner.run_cycle(prepared(json!(3), true), &mut clock2);

    assert_eq!(
        connections.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the shadow/SeedOnce cycle path must never dial a live CLN socket -- it has no \
         broadcaster to call"
    );
}

// ---------------------------------------------------------------------------
// Task 5 (stateful-shadow plan): restart-persistent SeedOnce -- Rust owns
// autonomous state in its OWN store; Python is a one-time seed source only.
// ---------------------------------------------------------------------------

mod seedonce_restart {
    use super::*;
    use revops::fee_state::RunwayStateStore;
    use revops_db::fee_runway::{self, FeeCycleCommit, FeeStateSnapshot};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    /// Direct-connection store double for the owner thread: dispatches to
    /// the SAME `revops_db::fee_runway` functions the production
    /// `ObserverHandle` actor runs (whose delegation is covered by
    /// `revops-db`'s own actor tests), against an on-disk file so a
    /// "restart" (a brand-new `CycleOwner` + store instance) reopens the
    /// same Rust-owned state.
    struct TestStore {
        path: PathBuf,
        fail_commits: Arc<AtomicBool>,
    }

    impl TestStore {
        fn open(path: &Path, fail_commits: Arc<AtomicBool>) -> TestStore {
            let store = TestStore {
                path: path.to_path_buf(),
                fail_commits,
            };
            store.conn(); // create + init schema
            store
        }

        fn conn(&self) -> Connection {
            let conn = Connection::open(&self.path).expect("open rust-owned store");
            revops_db::notifications::init_schema(&conn).expect("init store schema");
            conn
        }
    }

    impl RunwayStateStore for TestStore {
        fn load_latest_state(&self) -> anyhow::Result<FeeStateSnapshot> {
            fee_runway::load_latest_state(&self.conn())
        }

        fn commit_fee_cycle(&self, commit: FeeCycleCommit) -> anyhow::Result<u64> {
            if self.fail_commits.load(Ordering::SeqCst) {
                anyhow::bail!("injected commit failure");
            }
            fee_runway::commit_fee_cycle(&self.conn(), &commit)
        }

        fn record_seed_event(&self, event: fee_runway::FeeSeedEventRow) -> anyhow::Result<i64> {
            fee_runway::record_seed_event(&self.conn(), &event)
        }

        fn record_restart_marker(
            &self,
            marker: fee_runway::FeeRestartMarkerRow,
        ) -> anyhow::Result<i64> {
            fee_runway::record_restart_marker(&self.conn(), &marker)
        }

        fn record_mempool_sample_pruned(
            &self,
            sampled_at: i64,
            sat_per_vbyte: f64,
            retain_since: i64,
        ) -> anyhow::Result<()> {
            fee_runway::record_mempool_sample_pruned(
                &self.conn(),
                sampled_at,
                sat_per_vbyte,
                retain_since,
            )
        }

        fn query_mempool_samples_since(
            &self,
            since: i64,
        ) -> anyhow::Result<Vec<fee_runway::MempoolSampleRow>> {
            fee_runway::query_mempool_samples_since(&self.conn(), since)
        }

        fn record_mempool_ma_comparison(
            &self,
            row: fee_runway::MempoolMaComparisonRow,
        ) -> anyhow::Result<i64> {
            fee_runway::record_mempool_ma_comparison(&self.conn(), &row)
        }

        fn record_trigger_event(
            &self,
            event: fee_runway::FeeTriggerEventRow,
        ) -> anyhow::Result<()> {
            fee_runway::record_trigger_event(&self.conn(), &event)
        }
    }

    const CHANNEL: &str = "700x1x0";

    /// A SeedOnce owner over a fresh test store -- for tests OUTSIDE this
    /// module that only need "SeedOnce with a working store".
    pub fn owner_with_test_store(fx: &Fixture) -> CycleOwner {
        let store_path = fx._dir.path().join("rust-owned.db");
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        owner_with_store(fx, Some(Box::new(store)))
    }

    /// [`owner_with_test_store`] with a caller-supplied `socket_path` --
    /// Task 9's zero-connections test uses this to point a SeedOnce owner
    /// at a REAL live listener (rather than the dead path every other
    /// SeedOnce test in this file uses) and still observe no connections.
    pub fn owner_with_test_store_and_socket(fx: &Fixture, socket_path: PathBuf) -> CycleOwner {
        let store_path = fx._dir.path().join("rust-owned.db");
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path,
                journal_dir: fx.journal_dir.clone(),
                lifecycle: StateLifecycle::SeedOnce,
                trigger: TriggerMode::default(),
            },
            SEED,
            Some(Box::new(store)),
        )
    }

    pub struct SeedOnceHarness {
        fx: Fixture,
        owner: CycleOwner,
        store_path: PathBuf,
        fail_commits: Arc<AtomicBool>,
        cycles: i64,
    }

    fn owner_with_store(fx: &Fixture, store: Option<Box<dyn RunwayStateStore>>) -> CycleOwner {
        CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: fx.journal_dir.clone(),
                lifecycle: StateLifecycle::SeedOnce,
                trigger: TriggerMode::default(),
            },
            SEED,
            store,
        )
    }

    /// The amendment-R5 harness: ONE channel (`700x1x0`) present in both
    /// the production snapshot (channel_states + fee_strategy_state) and
    /// the RPC prefetch, driving real `Ran` cycles against a fresh
    /// Rust-owned store.
    pub fn seedonce_harness_with_one_channel() -> SeedOnceHarness {
        let fx = fixture();
        let conn = Connection::open(&fx.db_path).expect("open for seeding");
        conn.execute(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, \
             sats_out, capacity, updated_at, kalman_flow_ratio, kalman_velocity) \
             VALUES (?1, ?2, 'balanced', 0.1, 0, 0, 2000000, ?3, 0.05, 0.01)",
            rusqlite::params![CHANNEL, peer_a(), NOW - 60],
        )
        .expect("insert channel_states row");
        conn.execute(
            "INSERT INTO fee_strategy_state (channel_id, last_update, v2_state_json) \
             VALUES (?1, ?2, '{}')",
            rusqlite::params![CHANNEL, NOW - 900],
        )
        .expect("insert fee_strategy_state row");
        drop(conn);

        let store_path = fx.journal_dir.join("rust-owned.db");
        std::fs::create_dir_all(&fx.journal_dir).expect("journal dir");
        let fail_commits = Arc::new(AtomicBool::new(false));
        let store = TestStore::open(&store_path, Arc::clone(&fail_commits));
        let owner = owner_with_store(&fx, Some(Box::new(store)));
        SeedOnceHarness {
            fx,
            owner,
            store_path,
            fail_commits,
            cycles: 0,
        }
    }

    impl SeedOnceHarness {
        /// One SeedOnce cycle; the clock advances one `fee_interval`
        /// (1800s) per call so the second cycle genuinely re-evaluates.
        pub fn run_cycle(&mut self) -> CycleOutcome {
            self.cycles += 1;
            self.run_cycle_at(NOW + self.cycles * 1800)
        }

        /// One SeedOnce cycle at an explicit clock value (e.g. re-running
        /// at the LAST cycle's timestamp after a restart: hydration
        /// happens, but the waiting-window gate holds every adjustment, so
        /// the hydrated state itself is observable unevolved).
        pub fn run_cycle_at(&mut self, now: i64) -> CycleOutcome {
            let mut clock = || now;
            self.owner.run_cycle(self.prepared(), &mut clock)
        }

        pub fn state(&self) -> &revops_fees::cycle::ControllerState {
            self.owner.state()
        }

        /// Simulate a plugin restart: a brand-new `CycleOwner` (fresh
        /// in-memory state, fresh `hydrated_once`) over the SAME
        /// Rust-owned store file and production DB.
        pub fn restart(&mut self) {
            let store = TestStore::open(&self.store_path, Arc::clone(&self.fail_commits));
            self.owner = owner_with_store(&self.fx, Some(Box::new(store)));
        }

        pub fn store_conn(&self) -> Connection {
            Connection::open(&self.store_path).expect("open store for inspection")
        }

        pub fn prod_conn(&self) -> Connection {
            Connection::open(&self.fx.db_path).expect("open prod for mutation")
        }

        fn prepared(&self) -> PreparedCycle {
            let mut channel = canned_peer_channel();
            channel["short_channel_id"] = json!("700:1:0");
            channel["channel_id"] = json!("full_chan_700");
            PreparedCycle {
                cfg: FeeCfgSnapshot {
                    enable_vegas_reflex: false,
                    ..FeeCfgSnapshot::default()
                },
                min_competitors: json!(3),
                rpc: RpcPrefetch {
                    our_node_id: format!("02{}", "ee".repeat(32)),
                    peer_channels: vec![channel],
                    gossip_channels: Vec::new(),
                    feerates: None,
                },
            }
        }

        fn seed_event_count(&self) -> i64 {
            self.store_conn()
                .query_row("SELECT COUNT(*) FROM rust_fee_seed_events", [], |r| {
                    r.get(0)
                })
                .expect("count seed events")
        }
    }

    #[test]
    fn restart_empty_rust_db_seeds_once_from_python_and_commits_generation_1() {
        let mut h = seedonce_harness_with_one_channel();
        let outcome = h.run_cycle();
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
        assert!(
            h.state().fee_states.contains_key(CHANNEL),
            "cycle 1 must hydrate the Python snapshot"
        );

        let conn = h.store_conn();
        let seed = fee_runway::latest_seed_event(&conn)
            .unwrap()
            .expect("seed event recorded");
        assert_eq!(seed.outcome, "seeded");
        assert_eq!(seed.row_count, 1);
        assert_eq!(seed.source_max_last_update, NOW - 900);
        assert_eq!(seed.payload_sha256.len(), 64);

        let snapshot = fee_runway::load_latest_state(&conn).unwrap();
        assert_eq!(
            snapshot.generation, 1,
            "the first successful Rust commit records generation 1"
        );
        assert_eq!(snapshot.rows.len(), 1);
        assert_eq!(snapshot.rows[0].channel_id, CHANNEL);

        let marker = fee_runway::latest_restart_marker(&conn)
            .unwrap()
            .expect("restart marker recorded");
        assert_eq!(marker.hydration_source, "python_seed");
        assert_eq!(marker.prior_generation, 0);
        assert_eq!(marker.process_id, std::process::id() as i64);
        assert_eq!(marker.started_at, SEED, "startup timestamp is spawn time");
    }

    #[test]
    fn restart_scheduler_loads_rust_generation_even_if_python_state_changed() {
        let mut h = seedonce_harness_with_one_channel();
        assert!(matches!(h.run_cycle(), CycleOutcome::Ran { .. }));
        assert!(matches!(h.run_cycle(), CycleOutcome::Ran { .. }));
        let committed_fee = h.state().cycle_states[CHANNEL].last_fee_ppm;
        assert_eq!(h.seed_event_count(), 1);

        // Python's state changes (even disappears) after the seed: it must
        // never be an autonomous-state source again.
        h.prod_conn()
            .execute(
                "DELETE FROM fee_strategy_state WHERE channel_id = ?1",
                [CHANNEL],
            )
            .expect("delete python row");

        h.restart();
        // Re-run at cycle 2's own timestamp: the waiting-window gate holds
        // any further adjustment, so the state observed after this cycle
        // is exactly what hydration produced.
        let outcome = h.run_cycle_at(NOW + 2 * 1800);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
        assert!(
            h.state().fee_states.contains_key(CHANNEL),
            "restart must hydrate from the RUST generation, not Python"
        );
        assert_eq!(
            h.state().cycle_states[CHANNEL].last_fee_ppm,
            committed_fee,
            "restarted state must be the committed Rust state"
        );
        assert_eq!(h.seed_event_count(), 1, "no reseed on restart");

        let conn = h.store_conn();
        let marker = fee_runway::latest_restart_marker(&conn)
            .unwrap()
            .expect("restart marker");
        assert_eq!(marker.hydration_source, "rust_generation:2");
        assert_eq!(marker.prior_generation, 2);
        assert_eq!(
            fee_runway::load_latest_state(&conn).unwrap().generation,
            3,
            "the restarted cycle commits the next generation"
        );
    }

    #[test]
    fn restart_corrupt_rust_state_fails_closed_and_never_reseeds() {
        let mut h = seedonce_harness_with_one_channel();
        assert!(matches!(h.run_cycle(), CycleOutcome::Ran { .. }));
        assert_eq!(h.seed_event_count(), 1);

        // Corrupt the committed blob, then restart.
        h.store_conn()
            .execute("UPDATE rust_fee_state SET v2_state_json = '{corrupt'", [])
            .expect("corrupt stored state");
        h.restart();
        let outcome = h.run_cycle();
        assert_eq!(
            outcome,
            CycleOutcome::SkippedStateUnavailable,
            "{outcome:?}"
        );
        assert!(
            h.state().fee_states.is_empty(),
            "fail-closed: no state hydrated from corruption"
        );
        assert_eq!(
            h.seed_event_count(),
            1,
            "a recorded generation must NEVER fall back to reseeding from Python \
             (the Python row is still present and seedable -- refusing proves fail-closed)"
        );

        // Missing rows behind a recorded generation are equally corrupt.
        h.store_conn()
            .execute("DELETE FROM rust_fee_state", [])
            .expect("drop state rows");
        h.restart();
        let outcome = h.run_cycle();
        assert_eq!(
            outcome,
            CycleOutcome::SkippedStateUnavailable,
            "{outcome:?}"
        );
        assert_eq!(h.seed_event_count(), 1, "still no reseed");
    }

    #[test]
    fn restart_commit_failure_is_persistence_failed_and_generation_holds() {
        let mut h = seedonce_harness_with_one_channel();
        h.fail_commits.store(true, Ordering::SeqCst);
        let outcome = h.run_cycle();
        assert_eq!(outcome, CycleOutcome::PersistenceFailed, "{outcome:?}");
        assert_eq!(h.owner.persistence_failures(), 1, "red error counter");
        assert_eq!(
            fee_runway::load_latest_state(&h.store_conn())
                .unwrap()
                .generation,
            0,
            "a failed commit must not advance the generation"
        );

        // Recovery: the next cycle commits normally.
        h.fail_commits.store(false, Ordering::SeqCst);
        let outcome = h.run_cycle();
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
        assert_eq!(h.owner.persistence_failures(), 1, "counter holds");
        assert_eq!(
            fee_runway::load_latest_state(&h.store_conn())
                .unwrap()
                .generation,
            1
        );
    }

    /// 2026-07-23 gate-starvation lesson: in shadow-RehydratePerCycle the
    /// hydrated last_update is Python's POST-decision flush and the T8b
    /// pre-decision epoch differs; the decision gate consumes the T8b
    /// epoch (commit 993632d). Under SeedOnce the two MUST coincide --
    /// divergence means an epoch bug was reintroduced where the engagement
    /// gate can no longer see it.
    ///
    /// Final-review finding I1 (2026-07-26): the original form of this
    /// test asserted `skip_gate_prev[ch].last_update == cycle_states[ch]
    /// .last_update` AFTER cycle 2 -- i.e. immediately after
    /// `set_skip_gates_to_owned` had copied one into the other, so it
    /// could only ever pass. It now asserts CONSUMPTION instead:
    ///
    /// 1. the pre-decision epoch cycle 2 will read is captured BEFORE
    ///    cycle 2 runs, and cross-checked against the independently
    ///    PERSISTED cycle-1 `last_update` (the committed generation row in
    ///    the Rust-owned store -- not the in-memory map it was assigned
    ///    from);
    /// 2. that epoch is exactly one `fee_interval` behind cycle 2's own
    ///    clock, so the decision path sees ~0.5h elapsed, not ~0; and
    /// 3. cycle 2's RECORDED decision surface proves the epoch was
    ///    consumed: `skip_gate_comparable` is set (the gate had a cached
    ///    prior, so the engagement gate counts this channel) and the
    ///    disposition is NOT `waiting_window` -- the starvation signature
    ///    a fresh/zero epoch produces.
    ///
    /// NOTE on mutation coverage. Mutating `cycle.rs`'s
    /// `let epoch_last_update = ctx.pre_last_update;` (`cycle.rs:1671`) to
    /// `cycle.last_update` is NOT detected from here. That is a gap in
    /// THIS layer's coverage, not a proof that the two are equivalent --
    /// do not read it as one:
    ///
    /// * Under SeedOnce the two epochs coincide only AS HYDRATED at the
    ///   top of the cycle (`fee_scheduler.rs:917`/`:978` refresh
    ///   `skip_gate_prev` from the owned `cycle_states`).
    /// * That identity is BROKEN mid-cycle: `run_fee_cycle` calls
    ///   `maybe_wake_for_vegas_spike` at `cycle.rs:3547` -- after the
    ///   top-of-cycle refresh and before any `adjust_channel_fee` read --
    ///   which calls `wake_all_sleeping_channels` (`cycle.rs:3382`) and
    ///   BACKDATES `cycle.last_update` at `cycle.rs:3408`.
    /// * Since task 39 that same wake ALSO backdates the cached
    ///   `skip_gate_prev` epoch (`cycle.rs:3417`), matching Python, which
    ///   backdates its single in-memory `last_update` (py 4589-4593) and
    ///   then reads it in the gate (py 5299 / py 6219-6269). So for a
    ///   WOKEN channel the two values agree again -- but only for that
    ///   channel, only after that wake. On every unwoken channel and every
    ///   cycle without a wake they still differ, and the mutation still
    ///   changes the decision.
    /// * This harness reaches neither case: it sets
    ///   `enable_vegas_reflex = false` (`fee_scheduler.rs:181`, `:1330`),
    ///   so no wake ever fires here. In PRODUCTION `enable_vegas_reflex`
    ///   defaults to TRUE (`cycle.rs:200`, mirroring py `config.py:765`)
    ///   and `chain_costs` comes from real `feerates`
    ///   (`fee_evidence.rs:466`/`:855`), so both paths are live.
    ///
    /// The `pre_last_update` contract is therefore left to the kernel
    /// tests in `revops-fees/tests/cycle.rs`, which inject each divergence
    /// directly and are pinned by disjoint mutants:
    ///
    /// * `::decision_gate_uses_pre_decision_epoch_not_fresh_flush` (:2460)
    ///   and `::observation_cursor_uses_pre_decision_epoch` (:2492) go red
    ///   on the blanket `ctx.pre_last_update -> cycle.last_update` swap.
    ///   They are the ONLY guard against re-introducing the 2026-07-23
    ///   gate-starvation bug: do not weaken, merge, or "simplify" them on
    ///   the theory that SeedOnce makes the epoch contract redundant. It
    ///   does not.
    /// * `::in_cycle_vegas_wake_backdates_the_epoch_the_decision_gate_consumes`
    ///   (:2632), `::without_an_in_cycle_wake_the_gate_still_holds_on_the_pre_decision_epoch`
    ///   (:2713) and `::wake_backdates_cached_epochs_without_inventing_them`
    ///   (:2768) go red on the opposite mutant -- dropping the wake's
    ///   epoch propagation at `cycle.rs:3417`, which silently re-opens the
    ///   spike-parity divergence. They drive the vegas block with a
    ///   `chain_costs() = Some(..)` evidence double
    ///   (`revops-fees/tests/cycle.rs:1095`); the `FixtureEvidence` double
    ///   still returns `None` (`:186`).
    ///
    /// What THIS test catches is the complementary SeedOnce-layer
    /// regression class: an epoch cache that is not refreshed from the
    /// owned state, a seed or commit that persists the wrong
    /// `last_update`, or a decision that stops consuming the cached epoch
    /// at all.
    #[test]
    fn seedonce_second_cycle_consumes_the_committed_first_cycle_epoch() {
        const INTERVAL: i64 = 1800;
        let mut h = seedonce_harness_with_one_channel();

        // Cycle 1: seeds from Python, decides, and commits Rust-owned state.
        let cycle1_ts = NOW + INTERVAL;
        assert!(
            matches!(h.run_cycle_at(cycle1_ts), CycleOutcome::Ran { .. }),
            "cycle 1 must run to have an epoch to commit"
        );

        // (1) The epoch cycle 2 WILL read, captured before cycle 2 runs,
        // cross-checked against what cycle 1 actually persisted.
        let epoch_for_cycle2 = h
            .state()
            .skip_gate_prev
            .get(CHANNEL)
            .expect("cycle 1 must leave a cached pre-decision epoch")
            .last_update;
        let committed = fee_runway::load_latest_state(&h.store_conn()).unwrap();
        let persisted_last_update = committed
            .rows
            .iter()
            .find(|r| r.channel_id == CHANNEL)
            .expect("cycle 1 committed a state row")
            .last_update;
        assert_eq!(
            epoch_for_cycle2, persisted_last_update,
            "the cached pre-decision epoch must be the last_update cycle 1 COMMITTED, \
             not a value only the in-memory map holds"
        );

        // (2) One full interval old from cycle 2's point of view -- the
        // decision path must see ~1 interval elapsed, never ~0.
        let cycle2_ts = cycle1_ts + INTERVAL;
        assert_eq!(
            cycle2_ts - epoch_for_cycle2,
            INTERVAL,
            "cycle 2's pre-decision epoch must be one fee_interval behind its clock"
        );

        assert!(
            matches!(h.run_cycle_at(cycle2_ts), CycleOutcome::Ran { .. }),
            "cycle 2 must run"
        );

        // (3) The RECORDED decision surface proves consumption.
        let conn = h.store_conn();
        let (disposition, comparable) = conn
            .query_row(
                "SELECT disposition, skip_gate_comparable FROM rust_fee_shadow_outcomes \
                 WHERE cycle_ts = ?1 AND channel_id = ?2",
                rusqlite::params![cycle2_ts, CHANNEL],
                |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)?)),
            )
            .expect("cycle 2 recorded a shadow outcome row");
        assert_eq!(
            comparable, 1,
            "cycle 2 had a cached pre-decision epoch, so the engagement gate must \
             count this channel as comparable"
        );
        assert_ne!(
            disposition.as_deref(),
            Some("waiting_window"),
            "gate starvation: cycle 2 held the channel as if no time had elapsed since \
             cycle 1's committed epoch ({epoch_for_cycle2}) at clock {cycle2_ts}"
        );
    }

    #[test]
    fn restart_seed_refusal_records_event_and_stays_passive() {
        let mut h = seedonce_harness_with_one_channel();
        // Poison the Python snapshot with a from_dict raise class.
        h.prod_conn()
            .execute(
                "UPDATE fee_strategy_state SET v2_state_json = ?1 WHERE channel_id = ?2",
                rusqlite::params![
                    r#"{"fee_state": {"algorithm_version": "dts_pid_v1", "thompson_state": {"_last_fee_min": "not-a-number"}}, "cycle_state": {}}"#,
                    CHANNEL
                ],
            )
            .expect("poison python blob");

        let outcome = h.run_cycle();
        assert_eq!(
            outcome,
            CycleOutcome::SkippedStateUnavailable,
            "{outcome:?}"
        );
        assert!(h.state().fee_states.is_empty(), "passive-observer");

        let conn = h.store_conn();
        let event = fee_runway::latest_seed_event(&conn)
            .unwrap()
            .expect("refusal recorded in the Rust-owned store");
        assert_eq!(event.outcome, "seed_refused");
        assert_eq!(event.refused_channel.as_deref(), Some(CHANNEL));
        assert!(
            event
                .refused_field
                .as_deref()
                .unwrap_or_default()
                .contains("_last_fee_min"),
            "{event:?}"
        );
        assert_eq!(
            fee_runway::load_latest_state(&conn).unwrap().generation,
            0,
            "no generation from a refused seed"
        );

        // Subsequent cycles stay fail-closed WITHOUT spamming refusals.
        let outcome = h.run_cycle();
        assert_eq!(
            outcome,
            CycleOutcome::SkippedStateUnavailable,
            "{outcome:?}"
        );
        assert_eq!(
            h.seed_event_count(),
            1,
            "one refusal event, not one per cycle"
        );
    }

    /// Final-review finding I2 (2026-07-26): the fail-closed scan used to
    /// inspect `thompson_state` ONLY, so the two other Python load paths
    /// that raise on a corrupt field -- `PIDState.from_dict`'s eight bare
    /// casts (py 2060-2069, reached unconditionally from `from_v2_dict`
    /// py 2225-2226) and `_get_cycle_state`'s bare
    /// `int(congestion_quiet_cycles or 0)` (py 9027) -- were silently
    /// defaulted into a Rust-owned seed. The binding Global Constraint is
    /// that ANY such field refuses the WHOLE seed. End-to-end, per raise
    /// class.
    #[test]
    fn restart_seed_refusal_covers_pid_state_and_cycle_state_raise_classes() {
        for (blob, expected_field) in [
            // PIDState.from_dict on a truthy non-dict -> AttributeError.
            (
                r#"{"fee_state": {"algorithm_version": "dts_pid_v1", "pid_state": "not-a-dict"}, "cycle_state": {}}"#,
                "pid_state",
            ),
            // Bare float() on a non-numeric kp -> ValueError/TypeError.
            (
                r#"{"fee_state": {"algorithm_version": "dts_pid_v1", "pid_state": {"kp": "not-a-number"}}, "cycle_state": {}}"#,
                "pid_state.kp",
            ),
            // Bare int() on a non-integer last_update_time -> ValueError.
            (
                r#"{"fee_state": {"algorithm_version": "dts_pid_v1", "pid_state": {"last_update_time": "1.5"}}, "cycle_state": {}}"#,
                "pid_state.last_update_time",
            ),
            // Bare int(... or 0) on a truthy non-integer -> ValueError.
            (
                r#"{"fee_state": {"algorithm_version": "dts_pid_v1"}, "cycle_state": {"congestion_quiet_cycles": "many"}}"#,
                "cycle_state.congestion_quiet_cycles",
            ),
            // ... and on a truthy non-scalar -> TypeError.
            (
                r#"{"fee_state": {"algorithm_version": "dts_pid_v1"}, "cycle_state": {"congestion_quiet_cycles": [1]}}"#,
                "cycle_state.congestion_quiet_cycles",
            ),
        ] {
            let mut h = seedonce_harness_with_one_channel();
            h.prod_conn()
                .execute(
                    "UPDATE fee_strategy_state SET v2_state_json = ?1 WHERE channel_id = ?2",
                    rusqlite::params![blob, CHANNEL],
                )
                .expect("poison python blob");

            let outcome = h.run_cycle();
            assert_eq!(
                outcome,
                CycleOutcome::SkippedStateUnavailable,
                "{expected_field}: {outcome:?}"
            );
            assert!(
                h.state().fee_states.is_empty(),
                "{expected_field}: no partial seed, no fresh-state fallback"
            );

            let conn = h.store_conn();
            let event = fee_runway::latest_seed_event(&conn)
                .unwrap()
                .expect("refusal recorded in the Rust-owned store");
            assert_eq!(event.outcome, "seed_refused", "{expected_field}");
            assert_eq!(event.refused_channel.as_deref(), Some(CHANNEL));
            assert_eq!(
                event.refused_field.as_deref(),
                Some(expected_field),
                "{event:?}"
            );
            assert_eq!(
                fee_runway::load_latest_state(&conn).unwrap().generation,
                0,
                "{expected_field}: no generation from a refused seed"
            );
        }
    }

    #[test]
    fn restart_seed_once_without_store_fails_closed() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        seed_fee_strategy_row(&fx.db_path, "chan_kept");
        let mut owner = owner_with_store(&fx, None);
        let mut clock = || NOW;
        let outcome = owner.run_cycle(prepared(json!(3), false), &mut clock);
        assert_eq!(
            outcome,
            CycleOutcome::SkippedStateUnavailable,
            "{outcome:?}"
        );
        assert!(
            owner.state().fee_states.is_empty(),
            "SeedOnce without a Rust-owned store must never hydrate"
        );
    }

    #[test]
    fn restart_rehydrate_per_cycle_never_touches_the_rust_store() {
        // RehydratePerCycle remains available for strict replay / legacy
        // dry-run: even WITH a store wired, it must neither seed nor
        // commit -- Python stays the per-cycle source.
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        seed_fee_strategy_row(&fx.db_path, "chan_kept");
        let store_path = fx._dir.path().join("rust-owned.db");
        let fail = Arc::new(AtomicBool::new(false));
        let store = TestStore::open(&store_path, fail);
        let mut owner = CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: fx.journal_dir.clone(),
                lifecycle: StateLifecycle::RehydratePerCycle,
                trigger: TriggerMode::default(),
            },
            SEED,
            Some(Box::new(store)),
        );
        let mut clock = || NOW;
        let outcome = owner.run_cycle(prepared(json!(3), false), &mut clock);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(fee_runway::load_latest_state(&conn).unwrap().generation, 0);
        assert!(fee_runway::latest_seed_event(&conn).unwrap().is_none());
        assert!(fee_runway::latest_restart_marker(&conn).unwrap().is_none());
    }

    // -----------------------------------------------------------------
    // Task 6 step 1/2: Rust-owned mempool recorder + evidence switch
    // -----------------------------------------------------------------

    fn mempool_sample_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM rust_mempool_fee_history", [], |r| {
            r.get(0)
        })
        .unwrap()
    }

    #[test]
    fn mempool_rehydrate_per_cycle_records_a_rust_sample_every_vegas_cycle() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        seed_fee_strategy_row(&fx.db_path, "chan_kept");
        let store_path = fx._dir.path().join("rust-owned.db");
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        let mut owner = CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: fx.journal_dir.clone(),
                lifecycle: StateLifecycle::RehydratePerCycle,
                trigger: TriggerMode::default(),
            },
            SEED,
            Some(Box::new(store)),
        );
        let mut clock = || NOW;
        // `prepared()` uses `FeeCfgSnapshot::default()` (Vegas Reflex ON
        // by default, py config.py:765) with `feerates` present -> a
        // truthy `chain_costs` -- the exact gate `record_mempool_fee`'s
        // Python call site uses.
        let outcome = owner.run_cycle(prepared(json!(3), false), &mut clock);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            mempool_sample_count(&conn),
            1,
            "Rust's own recorder must write ONE sample this cycle, same cadence as Python"
        );
    }

    #[test]
    fn mempool_no_sample_recorded_when_vegas_reflex_disabled() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        seed_fee_strategy_row(&fx.db_path, "chan_kept");
        let store_path = fx._dir.path().join("rust-owned.db");
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        let mut owner = CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: fx.journal_dir.clone(),
                lifecycle: StateLifecycle::RehydratePerCycle,
                trigger: TriggerMode::default(),
            },
            SEED,
            Some(Box::new(store)),
        );
        let mut clock = || NOW;
        let outcome = owner.run_cycle(prepared_no_vegas(json!(3), false), &mut clock);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            mempool_sample_count(&conn),
            0,
            "no recorder write when Vegas Reflex is off, mirroring Python's own gate"
        );
    }

    #[test]
    fn mempool_old_samples_are_pruned_transactionally_across_cycles() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        seed_fee_strategy_row(&fx.db_path, "chan_kept");
        let store_path = fx._dir.path().join("rust-owned.db");
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        let cfg = SchedulerConfig {
            db_path: fx.db_path.clone(),
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle: StateLifecycle::RehydratePerCycle,
            trigger: TriggerMode::default(),
        };
        let mut owner = CycleOwner::new(&cfg, SEED, Some(Box::new(store)));

        let mut clock1 = || NOW;
        owner.run_cycle(prepared(json!(3), false), &mut clock1);
        // Second cycle, > 24h later: the first cycle's sample must be
        // pruned away, leaving exactly the new one.
        let far_later = NOW + MEMPOOL_MA_WINDOW_SECONDS * 2;
        let mut clock2 = || far_later;
        owner.run_cycle(prepared(json!(3), false), &mut clock2);

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            mempool_sample_count(&conn),
            1,
            "the stale first-cycle sample must be pruned once it falls outside the 24h window"
        );
    }

    #[test]
    fn mempool_seedonce_denies_a_vegas_decision_without_fresh_rust_evidence() {
        let fx = fixture();
        let conn = Connection::open(&fx.db_path).expect("open for seeding");
        conn.execute(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, \
             sats_out, capacity, updated_at, kalman_flow_ratio, kalman_velocity) \
             VALUES ('100x1x0', ?1, 'balanced', 0.1, 0, 0, 2000000, ?2, 0.05, 0.01)",
            rusqlite::params![peer_a(), NOW - 60],
        )
        .unwrap();
        drop(conn);
        let mut owner = owner_with_test_store(&fx);
        let mut clock = || NOW;

        // `prepared()`'s default cfg has Vegas Reflex ON and a truthy
        // `chain_costs` -- SeedOnce has NO Rust-owned mempool sample yet,
        // so the cycle must fail closed (Task 6: "missing/stale samples
        // deny a decision that needs Vegas evidence"), never silently
        // fabricate a `1.0` MA the way strict-replay's Python fallback
        // would.
        let outcome = owner.run_cycle(prepared(json!(3), false), &mut clock);
        assert_eq!(outcome, CycleOutcome::SkippedDecisionInput, "{outcome:?}");
    }

    #[test]
    fn mempool_seedonce_uses_fresh_rust_evidence_once_recorded() {
        let fx = fixture();
        let conn = Connection::open(&fx.db_path).expect("open for seeding");
        conn.execute(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, \
             sats_out, capacity, updated_at, kalman_flow_ratio, kalman_velocity) \
             VALUES ('100x1x0', ?1, 'balanced', 0.1, 0, 0, 2000000, ?2, 0.05, 0.01)",
            rusqlite::params![peer_a(), NOW - 60],
        )
        .unwrap();
        drop(conn);

        let store_path = fx.journal_dir.join("rust-owned.db");
        std::fs::create_dir_all(&fx.journal_dir).unwrap();
        let fail_commits = Arc::new(AtomicBool::new(false));
        let store = TestStore::open(&store_path, Arc::clone(&fail_commits));
        // Seed ONE fresh Rust-owned sample directly (as if a prior cycle,
        // or Rust's own recorder, had already written it).
        {
            let conn = store.conn();
            fee_runway::record_mempool_sample_pruned(&conn, NOW - 10, 42.0, NOW - 90_000).unwrap();
        }
        let mut owner = owner_with_store(&fx, Some(Box::new(store)));
        let mut clock = || NOW;

        let outcome = owner.run_cycle(prepared(json!(3), false), &mut clock);
        assert!(
            matches!(outcome, CycleOutcome::Ran { .. }),
            "a fresh Rust-owned sample must satisfy the Vegas evidence gate: {outcome:?}"
        );
    }

    // -----------------------------------------------------------------
    // Fix round 1 (review finding 1): the shadow-window mempool 24h-MA
    // comparison must be a PERSISTED row, not only a log line.
    // -----------------------------------------------------------------

    fn mempool_ma_comparisons(conn: &Connection) -> Vec<(i64, f64, Option<f64>, Option<f64>)> {
        let mut stmt = conn
            .prepare(
                "SELECT cycle_ts, rust_ma, python_ma, delta FROM rust_mempool_ma_comparison \
                 ORDER BY id",
            )
            .unwrap();
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, f64>(1)?,
                r.get::<_, Option<f64>>(2)?,
                r.get::<_, Option<f64>>(3)?,
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }

    #[test]
    fn mempool_ma_comparison_is_persisted_during_rehydrate_per_cycle_shadow_window() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        seed_fee_strategy_row(&fx.db_path, "chan_kept");
        let store_path = fx._dir.path().join("rust-owned.db");
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        let mut owner = CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: fx.journal_dir.clone(),
                lifecycle: StateLifecycle::RehydratePerCycle,
                trigger: TriggerMode::default(),
            },
            SEED,
            Some(Box::new(store)),
        );
        let mut clock = || NOW;
        // `prepared()`'s `feerates` yields `sat_per_vbyte = 3.0`; the
        // fixture's `mempool_fee_history` is empty, so Python's own
        // `get_mempool_ma` parity fallback is exactly `1.0`.
        let outcome = owner.run_cycle(prepared(json!(3), false), &mut clock);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        let conn = Connection::open(&store_path).unwrap();
        let rows = mempool_ma_comparisons(&conn);
        assert_eq!(
            rows.len(),
            1,
            "the shadow-window MA comparison must be persisted, not only logged"
        );
        assert_eq!(rows[0].0, NOW, "cycle_ts is this cycle's single clock read");
        assert_eq!(rows[0].1, 3.0, "rust_ma is this cycle's one fresh sample");
        assert_eq!(
            rows[0].2,
            Some(1.0),
            "python_ma is Python's own falsy-average fallback"
        );
        assert_eq!(rows[0].3, Some(2.0), "delta = rust_ma - python_ma");
    }

    #[test]
    fn mempool_ma_comparison_not_recorded_outside_rehydrate_per_cycle() {
        // SeedOnce already reads Rust rows as the decision-relevant
        // evidence itself -- there is no separate Python value to compare
        // against, so no comparison row is ever written.
        let fx = fixture();
        let conn = Connection::open(&fx.db_path).expect("open for seeding");
        conn.execute(
            "INSERT INTO channel_states (channel_id, peer_id, state, flow_ratio, sats_in, \
             sats_out, capacity, updated_at, kalman_flow_ratio, kalman_velocity) \
             VALUES ('100x1x0', ?1, 'balanced', 0.1, 0, 0, 2000000, ?2, 0.05, 0.01)",
            rusqlite::params![peer_a(), NOW - 60],
        )
        .unwrap();
        drop(conn);

        let store_path = fx.journal_dir.join("rust-owned.db");
        std::fs::create_dir_all(&fx.journal_dir).unwrap();
        let fail_commits = Arc::new(AtomicBool::new(false));
        let store = TestStore::open(&store_path, Arc::clone(&fail_commits));
        {
            let conn = store.conn();
            fee_runway::record_mempool_sample_pruned(&conn, NOW - 10, 42.0, NOW - 90_000).unwrap();
        }
        let mut owner = owner_with_store(&fx, Some(Box::new(store)));
        let mut clock = || NOW;

        let outcome = owner.run_cycle(prepared(json!(3), false), &mut clock);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        let conn = Connection::open(&store_path).unwrap();
        assert!(
            mempool_ma_comparisons(&conn).is_empty(),
            "SeedOnce has no Python evidence value to compare against"
        );
    }

    // -----------------------------------------------------------------
    // Task 6 steps 3-4: trigger receipts through the full CycleOwner
    // -----------------------------------------------------------------

    fn trigger_events(conn: &Connection) -> Vec<(String, Option<String>, bool, Option<String>)> {
        let mut stmt = conn
            .prepare(
                "SELECT trigger_type, channel_id, coalesced, detail FROM rust_fee_trigger_events \
                 ORDER BY id",
            )
            .unwrap();
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, i64>(2)? != 0,
                r.get::<_, Option<String>>(3)?,
            ))
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }

    fn owner_with_any_store(fx: &Fixture, lifecycle: StateLifecycle) -> (CycleOwner, PathBuf) {
        let store_path = fx._dir.path().join("rust-owned.db");
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        let owner = CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: fx.journal_dir.clone(),
                lifecycle,
                trigger: TriggerMode::default(),
            },
            SEED,
            Some(Box::new(store)),
        );
        (owner, store_path)
    }

    #[test]
    fn wake_all_trigger_receipt_is_persisted() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        owner.handle_wake_all(NOW);

        let conn = Connection::open(&store_path).unwrap();
        let events = trigger_events(&conn);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "wake_all");
        assert_eq!(events[0].1, None);
        assert!(!events[0].2, "the FIRST occurrence must not be coalesced");
    }

    #[test]
    fn repeated_wake_all_before_a_cycle_coalesces_the_receipt() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        owner.handle_wake_all(NOW);
        owner.handle_wake_all(NOW + 1);

        let conn = Connection::open(&store_path).unwrap();
        let events = trigger_events(&conn);
        assert_eq!(events.len(), 2);
        assert!(!events[0].2);
        assert!(
            events[1].2,
            "the SECOND occurrence must be recorded as coalesced"
        );
    }

    #[test]
    fn policy_changed_trigger_receipt_carries_the_peer_id_as_scope() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        let peer = peer_a();
        owner.handle_policy_changed(&[], &peer, NOW);

        let conn = Connection::open(&store_path).unwrap();
        let events = trigger_events(&conn);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "policy_changed");
        assert_eq!(events[0].1.as_deref(), Some(peer.as_str()));
    }

    #[test]
    fn vegas_spike_trigger_receipt_is_persisted() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        owner.handle_vegas_spike_check(NOW);

        let conn = Connection::open(&store_path).unwrap();
        let events = trigger_events(&conn);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "vegas_spike");
    }

    /// Fix round 1 (review finding 2): CLN's `forward_event` notification
    /// is wired through the trigger queue, recording-only -- same posture
    /// `handle_failed_forward` already carries (no fee-nudge/posterior
    /// effect; that stays deferred to cutover).
    #[test]
    fn forward_event_trigger_receipt_is_persisted() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        owner.handle_forward_event("100x1x0", NOW);

        let conn = Connection::open(&store_path).unwrap();
        let events = trigger_events(&conn);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "forward_event");
        assert_eq!(events[0].1.as_deref(), Some("100x1x0"));
        assert!(!events[0].2, "the FIRST occurrence must not be coalesced");
    }

    #[test]
    fn repeated_forward_event_for_the_same_channel_coalesces_the_receipt() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        owner.handle_forward_event("100x1x0", NOW);
        owner.handle_forward_event("100x1x0", NOW + 1);

        let conn = Connection::open(&store_path).unwrap();
        let events = trigger_events(&conn);
        assert_eq!(events.len(), 2);
        assert!(!events[0].2);
        assert!(
            events[1].2,
            "the SECOND occurrence must be recorded as coalesced"
        );
    }

    #[test]
    fn a_dropped_forward_event_trigger_is_a_persisted_red_event() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);

        // Saturate the bounded queue with DISTINCT forward-event channel
        // keys past its capacity.
        for i in 0..(TRIGGER_QUEUE_CAPACITY + 1) {
            owner.handle_forward_event(&format!("chan-{i}"), NOW);
        }
        assert_eq!(owner.trigger_queue_dropped_total(), 1);

        let conn = Connection::open(&store_path).unwrap();
        let events = trigger_events(&conn);
        let dropped: Vec<_> = events
            .iter()
            .filter(|(_, _, _, detail)| detail.as_deref().is_some_and(|d| d.contains("DROPPED")))
            .collect();
        assert_eq!(
            dropped.len(),
            1,
            "exactly one occurrence must be recorded as a dropped/red event"
        );
    }

    #[test]
    fn a_dropped_trigger_is_a_persisted_red_event() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);

        // Saturate the bounded queue with DISTINCT failed-forward channel
        // keys (never coalescing, never drained by a cycle) past its
        // capacity.
        for i in 0..(TRIGGER_QUEUE_CAPACITY + 1) {
            owner.handle_failed_forward(&failed_forward(&format!("chan-{i}"), NOW));
        }
        assert_eq!(owner.trigger_queue_dropped_total(), 1);

        let conn = Connection::open(&store_path).unwrap();
        let events = trigger_events(&conn);
        let dropped: Vec<_> = events
            .iter()
            .filter(|(_, _, _, detail)| detail.as_deref().is_some_and(|d| d.contains("DROPPED")))
            .collect();
        assert_eq!(
            dropped.len(),
            1,
            "exactly one occurrence must be recorded as a dropped/red event"
        );
    }

    /// R8 amendment item 3: the `FixedInterval` trigger receipt's
    /// `cycle_ts` matches the SAME cycle's `rust_fee_shadow_outcomes.
    /// cycle_ts` -- the two tables join on that column.
    #[test]
    fn fixed_interval_receipt_shares_cycle_ts_with_shadow_outcomes() {
        let mut harness = seedonce_harness_with_one_channel();
        let outcome = harness.run_cycle();
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        let conn = harness.store_conn();
        let (trigger_type, cycle_id, cycle_ts): (String, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT trigger_type, cycle_id, cycle_ts FROM rust_fee_trigger_events \
                 WHERE trigger_type = 'fixed_interval' ORDER BY id DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(trigger_type, "fixed_interval");
        let cycle_id = cycle_id.expect("a Ran SeedOnce cycle must carry a cycle_id");
        let cycle_ts = cycle_ts.expect("a Ran SeedOnce cycle must carry a cycle_ts");

        let outcome_cycle_ts: i64 = conn
            .query_row(
                "SELECT cycle_ts FROM rust_fee_shadow_outcomes WHERE cycle_id = ?1 LIMIT 1",
                [&cycle_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            cycle_ts, outcome_cycle_ts,
            "trigger receipt and shadow outcome must share the same cycle_ts key"
        );
    }

    // -----------------------------------------------------------------
    // Task 44 / A1: the failed-forward posterior nudge, through the full
    // CycleOwner. Before this wiring landed, `handle_failed_forward`
    // recorded a receipt and returned -- so after cutover, with Python
    // off, no bias nudge would ever be written again.
    // -----------------------------------------------------------------

    /// Seed a channel that already has posterior state and a live fee, so
    /// the nudge's "never a channel's first posterior evidence" guard is
    /// satisfied.
    fn seed_nudgeable_channel(owner: &mut CycleOwner, channel_id: &str, fee_ppm: i64) {
        let mut fee_state = ChannelFeeState::default();
        fee_state.last_fee_ppm = fee_ppm;
        owner
            .state_mut()
            .fee_states
            .insert(channel_id.to_string(), fee_state);
    }

    fn nudges(owner: &CycleOwner, channel_id: &str) -> usize {
        owner.state().fee_states[channel_id]
            .thompson
            .posterior_bias
            .len()
    }

    /// THE REVERT TRIPWIRE. Reverting `handle_failed_forward` to
    /// recording-only leaves `posterior_bias` empty and this reds.
    #[test]
    fn a_fee_relevant_failure_writes_a_durable_posterior_nudge() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        seed_nudgeable_channel(&mut owner, "1x1x0", 500);
        assert_eq!(nudges(&owner, "1x1x0"), 0, "precondition: no nudges yet");

        owner.handle_failed_forward(&failed_forward("1x1x0", NOW));

        assert_eq!(
            nudges(&owner, "1x1x0"),
            1,
            "a fee-relevant failure must durably nudge the posterior, not just record a receipt"
        );
        let (target, _weight, ts) = owner.state().fee_states["1x1x0"].thompson.posterior_bias[0];
        assert_eq!(target, 400.0, "py: int(current_fee_ppm * 0.8)");
        assert_eq!(
            ts, NOW,
            "stamped with the EVENT's clock, not the dispatch clock"
        );

        let conn = Connection::open(&store_path).unwrap();
        let detail = trigger_events(&conn)
            .into_iter()
            .find(|(t, ..)| t == "failed_forward")
            .and_then(|(_, _, _, d)| d)
            .expect("a failed_forward receipt");
        assert!(
            detail.contains("APPLIED"),
            "the receipt must state what actually happened, got: {detail}"
        );
    }

    /// The never-first-evidence invariant (py `has_persisted_dts`).
    /// Control for the test above: identical signal, no seeded channel.
    #[test]
    fn a_channel_without_posterior_state_is_never_nudged() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);

        owner.handle_failed_forward(&failed_forward("1x1x0", NOW));

        assert!(
            !owner.state().fee_states.contains_key("1x1x0"),
            "a failed forward must never fabricate posterior state"
        );
        let conn = Connection::open(&store_path).unwrap();
        let detail = trigger_events(&conn)
            .into_iter()
            .find(|(t, ..)| t == "failed_forward")
            .and_then(|(_, _, _, d)| d)
            .expect("a receipt is still recorded");
        assert!(detail.contains("NOT applied"), "{detail}");
    }

    /// A channel with no positive fee has nothing to imply a target from.
    #[test]
    fn a_channel_with_no_positive_fee_is_not_nudged() {
        let fx = fixture();
        let (mut owner, _p) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        seed_nudgeable_channel(&mut owner, "1x1x0", 0);

        owner.handle_failed_forward(&failed_forward("1x1x0", NOW));

        assert_eq!(nudges(&owner, "1x1x0"), 0);
    }

    /// SL-2 gossip settle: failures inside the window after OUR OWN apply
    /// are still being routed against the old fee.
    #[test]
    fn a_failure_inside_the_gossip_settle_window_is_not_nudged() {
        let fx = fixture();
        let (mut owner, _p) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        seed_nudgeable_channel(&mut owner, "1x1x0", 500);
        owner.note_fee_applied("1x1x0", NOW);

        owner.handle_failed_forward(&failed_forward(
            "1x1x0",
            NOW + FAILURE_NUDGE_GOSSIP_SETTLE_SECONDS - 1,
        ));
        assert_eq!(nudges(&owner, "1x1x0"), 0, "inside the window: suppressed");

        // ...and the boundary is not off by one: at exactly the window it
        // is allowed again.
        owner.handle_failed_forward(&failed_forward(
            "1x1x0",
            NOW + FAILURE_NUDGE_GOSSIP_SETTLE_SECONDS,
        ));
        assert_eq!(
            nudges(&owner, "1x1x0"),
            1,
            "at the window boundary: allowed"
        );
    }

    /// SL-2 rate limit: one nudge per channel per window, so a burst from
    /// a single payment attempt cannot stack into a large fake signal.
    #[test]
    fn a_second_failure_inside_the_rate_limit_window_is_not_nudged() {
        let fx = fixture();
        let (mut owner, _p) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        seed_nudgeable_channel(&mut owner, "1x1x0", 500);

        owner.handle_failed_forward(&failed_forward("1x1x0", NOW));
        assert_eq!(nudges(&owner, "1x1x0"), 1);

        owner.handle_failed_forward(&failed_forward(
            "1x1x0",
            NOW + FAILURE_NUDGE_MIN_INTERVAL_SECONDS - 1,
        ));
        assert_eq!(nudges(&owner, "1x1x0"), 1, "rate limited: still one nudge");

        // Past the window the nudge is allowed again -- but the COUNT stays
        // 1, because `record_posterior_nudge`'s M4 dedup refreshes an entry
        // within NUDGE_DEDUP_TOLERANCE of an existing target instead of
        // appending, and both nudges imply the same 400 ppm. The observable
        // proof that it fired is the refreshed timestamp.
        let before = owner.state().fee_states["1x1x0"].thompson.posterior_bias[0].2;
        owner.handle_failed_forward(&failed_forward(
            "1x1x0",
            NOW + FAILURE_NUDGE_MIN_INTERVAL_SECONDS,
        ));
        let after = owner.state().fee_states["1x1x0"].thompson.posterior_bias[0].2;
        assert_eq!(before, NOW);
        assert_eq!(
            after,
            NOW + FAILURE_NUDGE_MIN_INTERVAL_SECONDS,
            "past the window the nudge fires again and refreshes the entry (M4 dedup)"
        );
    }
}
