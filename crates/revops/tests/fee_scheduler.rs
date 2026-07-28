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
    FlushWatcher, PollOutcome, PreparedCycle, PreparedInitialFee, SchedulerConfig, StateLifecycle,
    TriggerMode, WatchParams, DEFAULT_FLUSH_POLL_SECS, DEFAULT_FLUSH_SETTLE_SECS,
    FAILURE_NUDGE_GOSSIP_SETTLE_SECONDS, FAILURE_NUDGE_MIN_INTERVAL_SECONDS,
    TRIGGER_QUEUE_CAPACITY,
};
use revops::fee_state::STATE_JOURNAL_FILE_NAME;
use revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode};
use revops_fees::cycle::{ChannelCycleState, ChannelFeeState, ChannelStateRow, FeeCfgSnapshot};
use revops_fees::journal::JOURNAL_FILE_NAME;
use revops_fees::market::FeePrior;
use revops_fees::pyrand::PyRandom;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

/// Insert or update a `peer_policies` row for `peer_a()`, which is what
/// the A2 producer watches via `updated_at`.
fn upsert_peer_policy(db_path: &Path, updated_at: i64, fee_ppm_target: i64) {
    let conn = Connection::open(db_path).expect("open for seeding");
    conn.execute(
        "INSERT INTO peer_policies (peer_id, strategy, fee_ppm_target, updated_at) \
         VALUES (?1, 'static', ?2, ?3) \
         ON CONFLICT(peer_id) DO UPDATE SET fee_ppm_target = ?2, updated_at = ?3",
        rusqlite::params![peer_a(), fee_ppm_target, updated_at],
    )
    .expect("upsert peer_policies row");
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
    handle
        .tx
        .send(CycleMsg::WakeAll)
        .await
        .expect("send WakeAll");

    // Query must round-trip through the real reply channel.
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    handle
        .tx
        .send(CycleMsg::Query(FeeDebugQuery::Summary, reply_tx))
        .await
        .expect("send Query");
    let value = reply_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("owner thread replied");
    assert!(value.get("last_cycle_decision").is_some(), "{value:?}");
    assert!(value.get("channels").is_some(), "{value:?}");

    handle.tx.send(CycleMsg::Shutdown).await.ok();
}

#[tokio::test]
async fn run_prepared_acknowledges_only_after_real_owner_outcome() {
    let fx = fixture();
    let handle = revops::fee_scheduler::spawn_owner_for_runtime(
        SchedulerConfig {
            db_path: fx.db_path.clone(),
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle: StateLifecycle::RehydratePerCycle,
            trigger: TriggerMode::FixedInterval {
                phase_offset_secs: 999_999,
            },
        },
        None,
    )
    .expect("spawn owner");
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(CycleMsg::RunPrepared(
            Box::new(prepared(json!(3), false)),
            ack_tx,
        ))
        .await
        .expect("dispatch prepared cycle");
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx)
        .await
        .expect("owner completion deadline")
        .expect("owner reply");
    assert!(
        outcome.is_ok(),
        "real owner outcome must be acknowledged: {outcome:?}"
    );
    handle.tx.send(CycleMsg::Shutdown).await.ok();
}

type HeldOwnerBody = Box<dyn FnOnce() + Send + 'static>;

fn held_owner(
    fx: &Fixture,
) -> (
    revops::fee_scheduler::SchedulerHandle,
    Arc<std::sync::Mutex<Option<HeldOwnerBody>>>,
) {
    let held = Arc::new(std::sync::Mutex::new(None));
    let held_for_spawn = held.clone();
    let handle = revops::fee_scheduler::spawn_with_thread_spawner(
        SchedulerConfig {
            db_path: fx.db_path.clone(),
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle: StateLifecycle::RehydratePerCycle,
            trigger: TriggerMode::ExternalOnly,
        },
        None,
        revops::config_resolve::PythonOptionCache::empty(),
        None,
        move |_name, body| {
            *held_for_spawn.lock().unwrap() = Some(body);
            Ok(())
        },
    )
    .expect("construct held owner");
    (handle, held)
}

#[tokio::test]
async fn bounded_owner_ingress_backpressures_notification_then_rpc_until_drain() {
    let fx = fixture();
    let (handle, held) = held_owner(&fx);
    for i in 0..revops::fee_scheduler::OWNER_QUEUE_CAPACITY {
        handle
            .tx
            .send(CycleMsg::ForwardEvent {
                channel_id: format!("queued-{i}"),
            })
            .await
            .expect("fill bounded owner ingress");
    }
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    let blocked = handle
        .tx
        .send(CycleMsg::Query(FeeDebugQuery::Summary, reply_tx));
    tokio::pin!(blocked);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut blocked)
            .await
            .is_err(),
        "RPC producer must backpressure while the bounded ingress is saturated"
    );
    let body = held.lock().unwrap().take().unwrap();
    let owner = std::thread::spawn(body);
    blocked.await.expect("RPC admitted after owner drains");
    let value = tokio::task::spawn_blocking(move || reply_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(value.get("channels").is_some());
    handle.tx.send(CycleMsg::Shutdown).await.ok();
    tokio::task::spawn_blocking(move || owner.join().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn bounded_owner_ingress_backpressures_cycle_and_acknowledges_only_after_drain() {
    let fx = fixture();
    let (handle, held) = held_owner(&fx);
    for i in 0..revops::fee_scheduler::OWNER_QUEUE_CAPACITY {
        handle
            .tx
            .send(CycleMsg::ForwardEvent {
                channel_id: format!("queued-{i}"),
            })
            .await
            .unwrap();
    }
    let (ack_tx, mut ack_rx) = tokio::sync::oneshot::channel();
    let blocked = handle.tx.send(CycleMsg::RunPrepared(
        Box::new(prepared(json!(3), false)),
        ack_tx,
    ));
    tokio::pin!(blocked);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut blocked)
            .await
            .is_err(),
        "cycle producer must backpressure while owner ingress is saturated"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut ack_rx)
            .await
            .is_err(),
        "queue admission is not cycle completion"
    );
    let body = held.lock().unwrap().take().unwrap();
    let owner = std::thread::spawn(body);
    blocked.await.unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx)
        .await
        .expect("owner completion deadline")
        .expect("owner reply after drain");
    assert!(
        outcome.is_ok(),
        "real owner outcome after drain: {outcome:?}"
    );
    handle.tx.send(CycleMsg::Shutdown).await.ok();
    tokio::task::spawn_blocking(move || owner.join().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn bounded_owner_ingress_backpressures_wake_until_drain() {
    let fx = fixture();
    let (handle, held) = held_owner(&fx);
    for i in 0..revops::fee_scheduler::OWNER_QUEUE_CAPACITY {
        handle
            .tx
            .send(CycleMsg::ForwardEvent {
                channel_id: format!("queued-{i}"),
            })
            .await
            .unwrap();
    }
    let blocked = handle.tx.send(CycleMsg::RunCycleNow);
    tokio::pin!(blocked);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut blocked)
            .await
            .is_err(),
        "wake producers must use the same bounded owner ingress, never a bypass sender"
    );
    let body = held.lock().unwrap().take().unwrap();
    let owner = std::thread::spawn(body);
    blocked.await.expect("wake admitted after owner drains");
    handle.tx.send(CycleMsg::Shutdown).await.ok();
    tokio::task::spawn_blocking(move || owner.join().unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn bounded_owner_ingress_reports_closed_owner_and_closes_outstanding_ack() {
    let fx = fixture();
    let (handle, held) = held_owner(&fx);
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(CycleMsg::RunPrepared(
            Box::new(prepared(json!(3), false)),
            ack_tx,
        ))
        .await
        .unwrap();
    drop(held.lock().unwrap().take());
    assert!(
        ack_rx.await.is_err(),
        "queued ACK must close with owner loss"
    );
    let (reply_tx, _reply_rx) = std::sync::mpsc::channel();
    assert!(handle
        .tx
        .send(CycleMsg::Query(FeeDebugQuery::Summary, reply_tx))
        .await
        .is_err());
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
    #[derive(Clone, Default)]
    struct DispatchLaunchFailures {
        idempotency: Arc<AtomicBool>,
        commit: Arc<AtomicBool>,
        receipt: Arc<AtomicBool>,
    }

    struct TestStore {
        path: PathBuf,
        fail_commits: Arc<AtomicBool>,
        /// Task 42: fail the autonomous mempool-evidence store operations
        /// (the combined refresh, and the legacy record/query pair) so the
        /// bootstrap tests can prove a refresh failure fails CLOSED before
        /// hydration instead of degrading to "no evidence".
        fail_mempool: Arc<AtomicBool>,
        launch_failures: DispatchLaunchFailures,
    }

    impl TestStore {
        fn open(path: &Path, fail_commits: Arc<AtomicBool>) -> TestStore {
            Self::open_with_dispatch_launch_failures(
                path,
                fail_commits,
                DispatchLaunchFailures::default(),
            )
        }

        fn open_with_mempool_failure(
            path: &Path,
            fail_commits: Arc<AtomicBool>,
            fail_mempool: Arc<AtomicBool>,
        ) -> TestStore {
            let mut store = Self::open_with_dispatch_launch_failures(
                path,
                fail_commits,
                DispatchLaunchFailures::default(),
            );
            store.fail_mempool = fail_mempool;
            store
        }

        fn open_with_commit_launch_failure(
            path: &Path,
            fail_commits: Arc<AtomicBool>,
            fail_commit_launch: Arc<AtomicBool>,
        ) -> TestStore {
            let launch_failures = DispatchLaunchFailures {
                commit: fail_commit_launch,
                ..DispatchLaunchFailures::default()
            };
            Self::open_with_dispatch_launch_failures(path, fail_commits, launch_failures)
        }

        fn open_with_dispatch_launch_failures(
            path: &Path,
            fail_commits: Arc<AtomicBool>,
            launch_failures: DispatchLaunchFailures,
        ) -> TestStore {
            let store = TestStore {
                path: path.to_path_buf(),
                fail_commits,
                fail_mempool: Arc::new(AtomicBool::new(false)),
                launch_failures,
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

        fn record_seed_refusal(&self, event: fee_runway::FeeSeedEventRow) -> anyhow::Result<i64> {
            fee_runway::record_seed_refusal(&self.conn(), &event)
        }

        fn refresh_mempool_window(
            &self,
            sampled_at: i64,
            sat_per_vbyte: f64,
            retain_since: i64,
        ) -> anyhow::Result<fee_runway::MempoolWindow> {
            if self.fail_mempool.load(Ordering::SeqCst) {
                anyhow::bail!("injected mempool store failure");
            }
            fee_runway::refresh_mempool_window(
                &self.conn(),
                sampled_at,
                sat_per_vbyte,
                retain_since,
            )
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
            if self.fail_mempool.load(Ordering::SeqCst) {
                anyhow::bail!("injected mempool store failure");
            }
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
            if self.fail_mempool.load(Ordering::SeqCst) {
                anyhow::bail!("injected mempool store failure");
            }
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

        fn cycle_exists(&self, cycle_id: &str) -> anyhow::Result<bool> {
            fee_runway::cycle_exists(&self.conn(), cycle_id)
        }

        // F5: a direct-connection double does its own local work inline
        // and delivers the result before returning -- deterministic for
        // tests, and legitimate under the trait contract (there is no
        // SHARED single-owner actor to stall on here; the non-blocking
        // guarantee against a stalled actor is proven by `WedgedStore` +
        // `a_wedged_store_never_wedges_the_owner_thread`).

        fn dispatch_cycle_exists_with_generation(
            &self,
            cycle_id: String,
            on_done: revops::fee_state::StoreDispatchCallback<(bool, u64)>,
        ) -> anyhow::Result<()> {
            if self.launch_failures.idempotency.load(Ordering::SeqCst) {
                anyhow::bail!("injected idempotency dispatch thread spawn failure");
            }
            on_done(fee_runway::cycle_exists_with_generation(
                &self.conn(),
                &cycle_id,
            ));
            Ok(())
        }

        fn dispatch_commit_fee_cycle_guarded(
            &self,
            commit: FeeCycleCommit,
            expected_prior_generation: u64,
            on_done: revops::fee_state::StoreDispatchCallback<fee_runway::GuardedCommitOutcome>,
        ) -> anyhow::Result<()> {
            if self.launch_failures.commit.load(Ordering::SeqCst) {
                anyhow::bail!("injected store dispatch thread spawn failure");
            }
            if self.fail_commits.load(Ordering::SeqCst) {
                on_done(Err(anyhow::anyhow!("injected commit failure")));
                return Ok(());
            }
            on_done(fee_runway::commit_fee_cycle_guarded(
                &self.conn(),
                &commit,
                expected_prior_generation,
            ));
            Ok(())
        }

        fn dispatch_record_trigger_event(
            &self,
            event: fee_runway::FeeTriggerEventRow,
            on_done: revops::fee_state::StoreDispatchCallback<()>,
        ) -> anyhow::Result<()> {
            if self.launch_failures.receipt.load(Ordering::SeqCst) {
                anyhow::bail!("injected receipt dispatch thread spawn failure");
            }
            on_done(RunwayStateStore::record_trigger_event(self, event));
            Ok(())
        }
    }

    /// F5 test plumbing: wire an owner's result-only sink to a local receiver
    /// (the production loop's stand-in) ...
    struct TestOwnerReceiver(revops::fee_scheduler::A3ResultReceiver);

    impl TestOwnerReceiver {
        fn try_recv(&self) -> Result<CycleMsg, tokio::sync::mpsc::error::TryRecvError> {
            self.0.try_recv().map(CycleMsg::InitialFeeStoreResult)
        }
    }

    fn self_channel(owner: &mut CycleOwner) -> TestOwnerReceiver {
        TestOwnerReceiver(
            owner.attach_a3_result_receiver_for_tests(revops::fee_scheduler::OWNER_QUEUE_CAPACITY),
        )
    }

    /// ... and pump every off-owner store result message back into the
    /// owner until the queue is quiet -- exactly what the production
    /// message loop's `InitialFeeStoreResult` arm does. With `TestStore`'s
    /// inline dispatch, one call drains the full
    /// idempotency -> decide -> commit -> install chain.
    fn pump_store_results(owner: &mut CycleOwner, rx: &TestOwnerReceiver) {
        let mut clock = || NOW;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                CycleMsg::InitialFeeStoreResult(result) => {
                    owner.handle_initial_fee_store_result(result, &mut clock)
                }
                _ => panic!("unexpected owner message during A3 store-result pump"),
            }
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
        fail_mempool: Arc<AtomicBool>,
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
        let fail_mempool = Arc::new(AtomicBool::new(false));
        let store = TestStore::open_with_mempool_failure(
            &store_path,
            Arc::clone(&fail_commits),
            Arc::clone(&fail_mempool),
        );
        let owner = owner_with_store(&fx, Some(Box::new(store)));
        SeedOnceHarness {
            fx,
            owner,
            store_path,
            fail_commits,
            fail_mempool,
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
            let store = TestStore::open_with_mempool_failure(
                &self.store_path,
                Arc::clone(&self.fail_commits),
                Arc::clone(&self.fail_mempool),
            );
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

        fn seeded_event_count(&self) -> i64 {
            self.store_conn()
                .query_row(
                    "SELECT COUNT(*) FROM rust_fee_seed_events WHERE outcome = 'seeded'",
                    [],
                    |r| r.get(0),
                )
                .expect("count seeded events")
        }

        fn generation(&self) -> u64 {
            fee_runway::load_latest_state(&self.store_conn())
                .expect("load state")
                .generation
        }

        fn state_row_count(&self) -> usize {
            fee_runway::load_latest_state(&self.store_conn())
                .expect("load state")
                .rows
                .len()
        }

        fn mempool_sample_count(&self) -> i64 {
            self.store_conn()
                .query_row("SELECT COUNT(*) FROM rust_mempool_fee_history", [], |r| {
                    r.get(0)
                })
                .expect("count mempool samples")
        }

        /// Task 42's indivisibility invariant (audit test 6), scoped to
        /// the scheduled bootstrap flow: at every observation point, a
        /// durable generation >= 1 and a committed `outcome='seeded'`
        /// event exist together or not at all.
        fn assert_seed_indivisible(&self, at: &str) {
            let generation = self.generation();
            let seeded = self.seeded_event_count();
            assert_eq!(
                generation >= 1,
                seeded > 0,
                "indivisibility violated at '{at}': generation={generation} but \
                 seeded-event count={seeded} (exactly one of durable generation and \
                 committed seed provenance exists)"
            );
        }

        /// [`SeedOnceHarness::prepared`] with Vegas Reflex ON and truthy
        /// chain costs (`feerates` yields `sat_per_vbyte = 3.0`) -- the
        /// configuration under which cycle 1 needs autonomous mempool
        /// evidence (Task 42: the harness's Vegas-off default is exactly
        /// why the first-cycle ordering defect was invisible here).
        fn prepared_vegas(&self) -> PreparedCycle {
            let mut p = self.prepared();
            p.cfg = FeeCfgSnapshot::default();
            p.rpc.feerates = Some(json!({"perkb": {"opening": 3000}}));
            p
        }

        pub fn run_cycle_vegas(&mut self) -> CycleOutcome {
            self.cycles += 1;
            let now = NOW + self.cycles * 1800;
            let mut clock = || now;
            let prepared = self.prepared_vegas();
            self.owner.run_cycle(prepared, &mut clock)
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

    // -----------------------------------------------------------------
    // Task 42: SeedOnce first-cycle bootstrap evidence + atomic seed
    // provenance (audit: /home/sat/agent-tasks/task-42-design-audit.md).
    // -----------------------------------------------------------------

    /// Audit test 1: a VIRGIN store with Vegas enabled must run and commit
    /// cycle 1 using its own current Rust sample — not deterministically
    /// fail into a two-cycle bootstrap because the evidence froze before
    /// the sample was recorded.
    #[test]
    fn virgin_seedonce_first_cycle_uses_current_rust_sample() {
        let mut h = seedonce_harness_with_one_channel();

        let outcome = h.run_cycle_vegas();
        assert!(
            matches!(outcome, CycleOutcome::Ran { .. }),
            "virgin first cycle with Vegas ON must consume its own current sample \
             and run, got {outcome:?}"
        );
        assert_eq!(h.generation(), 1, "cycle 1 commits generation 1");
        assert_eq!(
            h.mempool_sample_count(),
            1,
            "exactly the current cycle's sample was recorded"
        );
        assert_eq!(
            h.seeded_event_count(),
            1,
            "exactly one successful seed event, committed with generation 1"
        );
        assert_eq!(h.state_row_count(), 1);
        h.assert_seed_indivisible("after virgin first cycle");
    }

    /// Audit test 2: a refresh/store failure for autonomous evidence must
    /// fail the cycle CLOSED before hydration — never degrade to "no
    /// fresh evidence", never import Python state, never record success
    /// provenance.
    #[test]
    fn autonomous_sample_refresh_failure_fails_before_hydration() {
        let mut h = seedonce_harness_with_one_channel();
        h.fail_mempool.store(true, Ordering::SeqCst);

        let outcome = h.run_cycle_vegas();
        assert!(
            !matches!(outcome, CycleOutcome::Ran { .. }),
            "a failed evidence refresh must fail the cycle closed, got {outcome:?}"
        );
        assert!(
            h.state().fee_states.is_empty(),
            "hydration must not have run after an evidence-refresh failure"
        );
        assert_eq!(
            h.seeded_event_count(),
            0,
            "no successful seed provenance may exist after a pre-hydration failure"
        );
        assert_eq!(h.generation(), 0);
        assert_eq!(h.state_row_count(), 0);
        let attempts: i64 = h
            .store_conn()
            .query_row("SELECT COUNT(*) FROM rust_broadcast_attempts", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(attempts, 0, "no live action of any kind");
    }

    /// Audit test 3: when the generation-1 commit fails, NOTHING of the
    /// seed may survive durably — the observation sample may (it is a
    /// truthful observation, not a success claim).
    #[test]
    fn seed_success_rolls_back_with_generation_commit() {
        let mut h = seedonce_harness_with_one_channel();
        h.fail_commits.store(true, Ordering::SeqCst);

        let outcome = h.run_cycle();
        assert_eq!(outcome, CycleOutcome::PersistenceFailed, "{outcome:?}");

        assert_eq!(h.generation(), 0, "generation must not advance");
        assert_eq!(h.state_row_count(), 0, "no durable state rows");
        assert_eq!(
            h.seeded_event_count(),
            0,
            "seed provenance must roll back with the failed generation commit \
             (a standalone 'seeded' row here is a false durable success claim)"
        );
        let cycles: i64 = h
            .store_conn()
            .query_row("SELECT COUNT(*) FROM rust_fee_cycles", [], |r| r.get(0))
            .unwrap();
        assert_eq!(cycles, 0, "no cycle row");
        h.assert_seed_indivisible("after failed first commit");
    }

    /// Audit test 4: restart after a failed first commit re-derives the
    /// seed from the pinned snapshot and ends with EXACTLY ONE committed
    /// seed event — not one per attempt.
    #[test]
    fn restart_after_failed_first_commit_reseeds_once() {
        let mut h = seedonce_harness_with_one_channel();
        h.fail_commits.store(true, Ordering::SeqCst);

        let outcome = h.run_cycle();
        assert_eq!(outcome, CycleOutcome::PersistenceFailed, "{outcome:?}");
        assert_eq!(h.generation(), 0);
        assert_eq!(
            h.seeded_event_count(),
            0,
            "no committed seed provenance before the restart"
        );

        h.restart();
        h.fail_commits.store(false, Ordering::SeqCst);
        let outcome = h.run_cycle();
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        assert_eq!(h.generation(), 1);
        assert_eq!(
            h.seeded_event_count(),
            1,
            "exactly one committed seed event after recovery (the second owner's \
             verified provenance, not one per attempt)"
        );
        h.assert_seed_indivisible("after restart recovery");
    }

    /// Audit test 5: a same-process retry (no restart) carries the pending
    /// in-memory provenance and persists it exactly once, atomically with
    /// the successful commit.
    #[test]
    fn same_process_retry_carries_pending_provenance() {
        let mut h = seedonce_harness_with_one_channel();
        h.fail_commits.store(true, Ordering::SeqCst);

        let outcome = h.run_cycle();
        assert_eq!(outcome, CycleOutcome::PersistenceFailed, "{outcome:?}");
        assert_eq!(
            h.seeded_event_count(),
            0,
            "pending provenance must not be durable before a successful commit"
        );

        h.fail_commits.store(false, Ordering::SeqCst);
        let outcome = h.run_cycle();
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        assert_eq!(h.generation(), 1);
        assert_eq!(
            h.seeded_event_count(),
            1,
            "the original pending provenance, once"
        );
        let seed = fee_runway::latest_seed_event(&h.store_conn())
            .unwrap()
            .expect("committed seed event");
        assert_eq!(seed.outcome, "seeded");
        assert_eq!(
            seed.source_max_last_update,
            NOW - 900,
            "provenance still describes the ORIGINAL pinned Python snapshot"
        );
        h.assert_seed_indivisible("after same-process retry");
    }

    /// Audit test 6: across a scripted fail/fail/succeed bootstrap, no
    /// observation point may see only one of {generation >= 1, committed
    /// seed provenance}.
    #[test]
    fn successful_generation_one_and_seed_are_indivisible() {
        let mut h = seedonce_harness_with_one_channel();
        h.assert_seed_indivisible("virgin store");

        h.fail_commits.store(true, Ordering::SeqCst);
        let _ = h.run_cycle();
        h.assert_seed_indivisible("after failed commit #1");
        let _ = h.run_cycle();
        h.assert_seed_indivisible("after failed commit #2");

        h.restart();
        h.assert_seed_indivisible("after restart with generation 0");

        h.fail_commits.store(false, Ordering::SeqCst);
        let outcome = h.run_cycle();
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
        h.assert_seed_indivisible("after successful bootstrap");
        assert_eq!(h.generation(), 1);
        assert_eq!(h.seeded_event_count(), 1);
    }

    /// Task 42 guard: while seed provenance is pending its atomic
    /// generation-1 commit (a failed virgin bootstrap awaiting retry), an
    /// out-of-cycle failed-forward nudge commit must be REFUSED — it
    /// would otherwise take generation 1 without the seed row, and the
    /// DB's virgin-store gate would then reject every scheduled retry,
    /// orphaning the provenance permanently.
    #[test]
    fn pending_seed_refuses_out_of_cycle_commits_until_committed() {
        let mut h = seedonce_harness_with_one_channel();
        h.fail_commits.store(true, Ordering::SeqCst);
        let outcome = h.run_cycle();
        assert_eq!(outcome, CycleOutcome::PersistenceFailed, "{outcome:?}");

        // Pending window: a nudge for the hydrated channel must refuse.
        h.fail_commits.store(false, Ordering::SeqCst);
        h.owner
            .handle_failed_forward(&super::failed_forward(CHANNEL, NOW + 100));
        assert_eq!(
            h.generation(),
            0,
            "no out-of-cycle commit may take generation 1 while seed provenance is pending"
        );
        assert_eq!(h.seeded_event_count(), 0);

        // The next scheduled cycle commits generation 1 + seed atomically;
        // afterwards nudges are admissible again (not asserted here — the
        // existing A1 suite covers the normal path).
        let outcome = h.run_cycle();
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
        assert_eq!(h.generation(), 1);
        assert_eq!(h.seeded_event_count(), 1);
        h.assert_seed_indivisible("after retry with guard released");
    }

    /// Audit test 7: the autonomous decision consumes ONLY Rust-owned
    /// evidence and mutates nothing outside the Rust observer store. The
    /// Python mempool table is DROPPED outright: any code path that so
    /// much as reads it fails the cycle, which is stronger than a
    /// conflicting value.
    #[test]
    fn rust_only_evidence_and_no_authority_mutation() {
        let mut h = seedonce_harness_with_one_channel();
        h.prod_conn()
            .execute_batch("DROP TABLE mempool_fee_history")
            .expect("drop python mempool table");

        // A probe connection held across the cycle: `PRAGMA data_version`
        // changes iff ANOTHER connection commits a write to the
        // production DB.
        let probe = h.prod_conn();
        let before: i64 = probe
            .query_row("PRAGMA data_version", [], |r| r.get(0))
            .unwrap();

        let outcome = h.run_cycle_vegas();
        assert!(
            matches!(outcome, CycleOutcome::Ran { .. }),
            "autonomous evidence must come from the Rust store alone (the Python \
             mempool table does not even exist), got {outcome:?}"
        );
        assert_eq!(h.generation(), 1);

        let after: i64 = probe
            .query_row("PRAGMA data_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            before, after,
            "the production DB must not receive any write"
        );

        let attempts: i64 = h
            .store_conn()
            .query_row("SELECT COUNT(*) FROM rust_broadcast_attempts", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(attempts, 0, "zero broadcast attempts");
        // Prepared-request rows are legitimate would-broadcast AUDIT in
        // shadow mode; the actual mutation surface is the broadcast
        // attempt ledger (asserted zero above) and the production DB
        // (asserted unchanged above).
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

    /// Task 42 replaced `mempool_seedonce_denies_a_vegas_decision_without_
    /// fresh_rust_evidence`: with resolved chain costs the combined
    /// refresh makes autonomous evidence exist BY CONSTRUCTION (see
    /// `virgin_seedonce_first_cycle_uses_current_rust_sample`), and with
    /// unresolved costs the Vegas ratio is never computed -- the
    /// missing-evidence deny (`MempoolEvidenceSource::Rust(None)` ->
    /// `Err`) stays pinned at the unit level in `tests/fee_evidence.rs`.
    /// What still needs a scheduler-level pin is the costs-UNRESOLVED
    /// branch: existing in-window samples remain legitimate evidence and
    /// are READ without recording a new sample (the Python-parity cadence
    /// gate: no resolved chain costs, no sample).
    #[test]
    fn mempool_seedonce_costs_unresolved_reads_existing_window_without_recording() {
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
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        {
            let conn = store.conn();
            fee_runway::record_mempool_sample_pruned(&conn, NOW - 10, 42.0, NOW - 90_000).unwrap();
        }
        let mut owner = owner_with_store(&fx, Some(Box::new(store)));
        let mut clock = || NOW;

        let mut p = prepared(json!(3), false);
        p.rpc.feerates = None; // chain costs unresolved
        let outcome = owner.run_cycle(p, &mut clock);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        let conn = Connection::open(&store_path).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rust_mempool_fee_history", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            count, 1,
            "no new sample may be recorded when chain costs are unresolved \
             (Python-parity cadence gate); the existing sample is read-only evidence"
        );
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
        let mut cycle_state = ChannelCycleState::default();
        cycle_state.last_fee_ppm = fee_ppm;
        owner
            .state_mut()
            .fee_states
            .insert(channel_id.to_string(), fee_state);
        owner
            .state_mut()
            .cycle_states
            .insert(channel_id.to_string(), cycle_state);
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
        // A scheduled cycle drains the pending trigger before a later
        // occurrence is admitted. Clear that separate gate here so this
        // assertion measures ONLY the rate-limit boundary.
        owner.drain_pending_triggers_for_test();

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

    fn applied_failed_forward_receipts(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM rust_fee_trigger_events \
             WHERE trigger_type = 'failed_forward' AND detail LIKE '%APPLIED%'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn failed_forward_cycle_count(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM rust_fee_cycles WHERE cycle_id LIKE 'rust-a1-%'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Task 53 RED: the queue gate must run before the nudge. Saturating
    /// distinct scopes makes this occurrence Dropped, so both halves of
    /// the controller state must remain byte-for-byte equivalent.
    #[test]
    fn failed_forward_dropped_by_backpressure_leaves_state_byte_identical() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        seed_nudgeable_channel(&mut owner, "1x1x0", 500);
        let before_fee = owner.state().fee_states["1x1x0"].clone();
        let before_cycle = owner.state().cycle_states["1x1x0"].clone();

        for i in 0..TRIGGER_QUEUE_CAPACITY {
            owner.handle_forward_event(&format!("task53-filler-{i}"), NOW);
        }
        owner.handle_failed_forward(&failed_forward("1x1x0", NOW));

        assert_eq!(owner.state().fee_states["1x1x0"], before_fee);
        assert_eq!(owner.state().cycle_states["1x1x0"], before_cycle);
        assert_eq!(owner.trigger_queue_dropped_total(), 1);
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(applied_failed_forward_receipts(&conn), 0);
    }

    /// Task 53 RED: a second same-channel occurrence is Coalesced while
    /// the first trigger is pending and must not refresh or append a
    /// posterior nudge.
    #[test]
    fn coalesced_failed_forward_applies_exactly_one_nudge() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        seed_nudgeable_channel(&mut owner, "1x1x0", 500);

        owner.handle_failed_forward(&failed_forward("1x1x0", NOW));
        owner.handle_failed_forward(&failed_forward(
            "1x1x0",
            NOW + FAILURE_NUDGE_MIN_INTERVAL_SECONDS,
        ));

        let bias = &owner.state().fee_states["1x1x0"].thompson.posterior_bias;
        assert_eq!(bias.len(), 1);
        assert_eq!(bias[0].2, NOW, "coalesced occurrence must have zero effect");
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(applied_failed_forward_receipts(&conn), 1);
    }

    /// Task 53 RED: the first accepted event must durably advance the
    /// Rust-owned generation and publish its APPLIED receipt in that
    /// same transaction, not merely mutate memory.
    #[test]
    fn accepted_failed_forward_commits_state_and_receipt_atomically_once() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        seed_nudgeable_channel(&mut owner, "1x1x0", 500);

        owner.handle_failed_forward(&failed_forward("1x1x0", NOW));

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(generation(&conn), 1);
        assert_eq!(failed_forward_cycle_count(&conn), 1);
        assert_eq!(applied_failed_forward_receipts(&conn), 1);
        let persisted: String = conn
            .query_row(
                "SELECT v2_state_json FROM rust_fee_state WHERE channel_id = '1x1x0'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(persisted.contains("[400.0, 0.1,"), "{persisted}");
    }

    /// Task 53 RED: a failed atomic state transition may leave neither an
    /// installed in-memory nudge nor a receipt claiming APPLIED.
    #[test]
    fn failed_forward_commit_failure_leaves_no_state_or_success_receipt() {
        let fx = fixture();
        let store_path = fx._dir.path().join("rust-owned.db");
        let fail_commits = Arc::new(AtomicBool::new(true));
        let store = TestStore::open(&store_path, Arc::clone(&fail_commits));
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
        seed_nudgeable_channel(&mut owner, "1x1x0", 500);
        let before = owner.state().fee_states["1x1x0"].clone();

        owner.handle_failed_forward(&failed_forward("1x1x0", NOW));

        assert_eq!(owner.state().fee_states["1x1x0"], before);
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(generation(&conn), 0);
        assert_eq!(failed_forward_cycle_count(&conn), 0);
        assert_eq!(applied_failed_forward_receipts(&conn), 0);
    }

    /// Task 53 RED: replaying identical content through a new owner over
    /// the same store must be recognized by the stable event key. The
    /// replay may be auditable, but it may not claim or apply a second
    /// nudge.
    #[test]
    fn replayed_failed_forward_is_idempotent_across_owner_restart() {
        let fx = fixture();
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        seed_nudgeable_channel(&mut owner, "1x1x0", 500);
        let signal = failed_forward("1x1x0", NOW);
        owner.handle_failed_forward(&signal);
        let fee = owner.state().fee_states["1x1x0"].clone();
        let cycle = owner.state().cycle_states["1x1x0"].clone();
        drop(owner);

        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        let mut restarted = CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: fx.journal_dir.clone(),
                lifecycle: StateLifecycle::RehydratePerCycle,
                trigger: TriggerMode::default(),
            },
            SEED + 1,
            Some(Box::new(store)),
        );
        restarted
            .state_mut()
            .fee_states
            .insert("1x1x0".to_string(), fee);
        restarted
            .state_mut()
            .cycle_states
            .insert("1x1x0".to_string(), cycle);
        restarted.handle_failed_forward(&signal);

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(failed_forward_cycle_count(&conn), 1);
        assert_eq!(applied_failed_forward_receipts(&conn), 1);
    }
    // -----------------------------------------------------------------
    // Task 44 / A2: the policy-change PRODUCER. The effect was already
    // ported (handle_policy_change wakes the peer's channels); nothing
    // ever constructed CycleMsg::PolicyChanged, so a policy change only
    // took effect at the next full cycle.
    // -----------------------------------------------------------------

    fn policy_receipts(conn: &Connection) -> Vec<(Option<String>, Option<String>)> {
        trigger_events(conn)
            .into_iter()
            .filter(|(t, ..)| t == "policy_changed")
            .map(|(_, chan, _, detail)| (chan, detail))
            .collect()
    }

    /// A restart is not a policy change: the FIRST time a peer is seen the
    /// producer only records a baseline. Without this, every restart would
    /// wake the whole node once.
    #[test]
    fn the_first_observation_of_a_peer_is_a_baseline_not_a_change() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        upsert_peer_policy(&fx.db_path, NOW - 600, 400);
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        let mut clock = || NOW;

        owner.run_cycle(prepared(json!(3), false), &mut clock);

        let conn = Connection::open(&store_path).unwrap();
        assert!(
            policy_receipts(&conn).is_empty(),
            "a first sighting must not be reported as a policy change"
        );
    }

    /// THE REVERT TRIPWIRE for A2. Removing the `detect_policy_changes`
    /// call leaves no policy_changed receipt and this reds.
    #[test]
    fn an_advanced_policy_updated_at_produces_a_policy_change() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        upsert_peer_policy(&fx.db_path, NOW - 600, 400);
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        let mut clock = || NOW;

        // Cycle 1 establishes the baseline.
        owner.run_cycle(prepared(json!(3), false), &mut clock);
        // The operator (Python today, Rust after cutover) edits the policy.
        upsert_peer_policy(&fx.db_path, NOW - 10, 900);
        owner.run_cycle(prepared(json!(3), false), &mut clock);

        let conn = Connection::open(&store_path).unwrap();
        let receipts = policy_receipts(&conn);
        assert_eq!(
            receipts.len(),
            1,
            "an advanced updated_at must produce exactly one policy change, got {receipts:?}"
        );
        assert_eq!(
            receipts[0].0.as_deref(),
            Some(peer_a().as_str()),
            "the receipt is scoped by PEER id, not channel id"
        );
    }

    /// An unchanged policy must stay silent, however many cycles run --
    /// the control that proves the test above is not just "any cycle
    /// emits a receipt".
    #[test]
    fn an_unchanged_policy_never_produces_a_policy_change() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        upsert_peer_policy(&fx.db_path, NOW - 600, 400);
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        let mut clock = || NOW;

        for _ in 0..3 {
            owner.run_cycle(prepared(json!(3), false), &mut clock);
        }

        let conn = Connection::open(&store_path).unwrap();
        assert!(policy_receipts(&conn).is_empty());
    }

    /// A policy row going BACKWARDS (clock skew, a restored backup) is not
    /// a change -- only an advance is.
    #[test]
    fn a_regressed_updated_at_is_not_a_policy_change() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
        upsert_peer_policy(&fx.db_path, NOW - 10, 400);
        let (mut owner, store_path) = owner_with_any_store(&fx, StateLifecycle::RehydratePerCycle);
        let mut clock = || NOW;

        owner.run_cycle(prepared(json!(3), false), &mut clock);
        upsert_peer_policy(&fx.db_path, NOW - 600, 400);
        owner.run_cycle(prepared(json!(3), false), &mut clock);

        let conn = Connection::open(&store_path).unwrap();
        assert!(policy_receipts(&conn).is_empty());
    }

    // -----------------------------------------------------------------
    // Task 44 / A3: CycleOwner::handle_new_channel -- atomic commit,
    // durability, backpressure, and the mandatory recording-only
    // reversion tripwire (contract §4.3, §4.4).
    // -----------------------------------------------------------------

    fn new_channel_prepared(
        channel_id: &str,
        peer_id: &str,
        channel_fee_ppm: i64,
        strategy: FeeStrategy,
        fee_ppm_target: Option<i64>,
        prior: Option<FeePrior>,
        now: i64,
    ) -> PreparedInitialFee {
        use revops_fees::cycle::ChannelInfo;
        PreparedInitialFee {
            channel: ChannelInfo {
                channel_id: channel_id.to_string(),
                short_channel_id: channel_id.to_string(),
                full_channel_id: "deadbeef".to_string(),
                peer_id: peer_id.to_string(),
                capacity_sats: 1_000_000,
                spendable_msat: 500_000_000,
                receivable_msat: 500_000_000,
                fee_base_msat: 0,
                fee_proportional_millionths: channel_fee_ppm,
                htlc_minimum_msat: 1,
                htlc_min_msat: 1,
                htlc_maximum_msat: 100_000,
                htlc_max_msat: 100_000,
                opener: "remote".to_string(),
                has_htlc_data: false,
                max_accepted_htlcs: 483,
                our_htlcs_in_flight: 0,
            },
            peer_id: peer_id.to_string(),
            policy: PeerPolicy {
                peer_id: peer_id.to_string(),
                strategy,
                rebalance_mode: RebalanceMode::Enabled,
                fee_ppm_target,
                tags: Vec::new(),
                updated_at: 0,
                fee_multiplier_min: None,
                fee_multiplier_max: None,
                expires_at: None,
            },
            cfg: FeeCfgSnapshot {
                min_fee_ppm: 0,
                max_fee_ppm: 100_000,
                thompson_prior_std_fee: 100,
                base_fee_msat: 0,
                ..FeeCfgSnapshot::default()
            },
            prior,
            event_ts: now,
            event_key: revops::fee_scheduler::new_channel_event_key(
                channel_id,
                "OPENINGD",
                "CHANNELD_NORMAL",
                now,
            ),
        }
    }

    fn generation(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COALESCE((SELECT generation FROM rust_fee_state_generation WHERE id = 1), 0)",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn trigger_event_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM rust_fee_trigger_events", [], |r| {
            r.get(0)
        })
        .unwrap()
    }

    fn request_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM rust_fee_requests", [], |r| r.get(0))
            .unwrap()
    }

    /// Contract §4.3 (durability) + §4.4 (the MANDATORY end-to-end
    /// recording-only reversion tripwire): one A3 event, DYNAMIC policy
    /// with a known gossip prior, drives all the way through
    /// `handle_new_channel`'s offer -> decide -> atomic commit. Asserts
    /// canonical resolution (via the prepared `ChannelInfo`), persistent
    /// prior mean/std, exactly one durable `.3` nudge, one complete
    /// cycle+fee state row after reopening the store, one prepared
    /// `channel_open` action with the scripted fee, and zero live
    /// mutation attempts (no `ClnRpc`/broadcaster ever constructed on this
    /// path -- structurally enforced by `tests/action_surface.rs`).
    #[test]
    fn new_channel_end_to_end_commits_atomically_and_survives_restart() {
        let fx = fixture();
        let store_path = fx._dir.path().join("rust-owned.db");
        let fail = Arc::new(AtomicBool::new(false));
        let store = TestStore::open(&store_path, fail.clone());
        let cfg = SchedulerConfig {
            db_path: fx.db_path.clone(),
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle: StateLifecycle::RehydratePerCycle,
            trigger: TriggerMode::default(),
        };
        let mut owner = CycleOwner::new(&cfg, SEED, Some(Box::new(store)));

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let rx = self_channel(&mut owner);
        let prepared = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW,
        );
        owner.handle_new_channel(prepared);
        pump_store_results(&mut owner, &rx);

        assert_eq!(
            owner.trigger_queue_dropped_total(),
            0,
            "capacity was never exceeded"
        );

        // Reopen the store fresh (simulates a restart) and confirm the
        // complete row survived: state (with the nudge), the prepared
        // action, and the receipt -- all visible together.
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            1,
            "one atomic commit, one generation bump"
        );
        let snapshot = fee_runway::load_latest_state(&conn).unwrap();
        assert_eq!(snapshot.rows.len(), 1);
        assert!(
            snapshot.rows[0]
                .v2_state_json
                .contains("\"prior_mean_fee\": 300"),
            "the persisted state must carry the seeded prior mean: {}",
            snapshot.rows[0].v2_state_json
        );
        assert_eq!(request_count(&conn), 1, "one prepared channel_open action");
        let (new_fee, message): (i64, String) = conn
            .query_row(
                "SELECT new_fee_ppm, message FROM rust_fee_requests",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(message.contains(&new_fee.to_string()));
        assert_eq!(trigger_event_count(&conn), 1, "one atomic receipt");
    }

    /// Mutation demonstration for §4.4: replacing the owner handler with
    /// receipt-only behavior (never calling `handle_new_channel`'s real
    /// decision/commit path) must make the test above fail. This inline
    /// double proves the assertions are not vacuous -- a receipt-only
    /// stand-in produces a receipt but no state row and no prepared
    /// action, which the durability assertions above catch.
    #[test]
    fn reversion_tripwire_mutation_demonstration_receipt_only_is_caught() {
        let fx = fixture();
        let store_path = fx._dir.path().join("rust-owned.db");
        let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
        // Simulates a "receipt-only" regression: record a trigger receipt
        // with NO state/action/outcome rows (exactly what a reverted
        // owner handler that only logs would produce).
        store
            .commit_fee_cycle(FeeCycleCommit {
                cycle_id: "receipt-only".to_string(),
                started_at: NOW,
                completed_at: NOW,
                source_commit: "test".to_string(),
                binary_sha256: "test".to_string(),
                trigger_receipt: Some(revops_db::fee_runway::FeeTriggerEventRow {
                    trigger_type: "new_channel".to_string(),
                    channel_id: Some("1x1x0".to_string()),
                    cycle_id: None,
                    cycle_ts: Some(NOW),
                    received_at: NOW,
                    coalesced: false,
                    detail: Some("receipt-only regression".to_string()),
                }),
                ..Default::default()
            })
            .unwrap();

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            trigger_event_count(&conn),
            1,
            "a receipt-only regression still writes A receipt"
        );
        // The exact assertions the real test above makes would FAIL
        // against this receipt-only shape -- proving they are not
        // vacuous.
        assert_eq!(
            request_count(&conn),
            0,
            "a receipt-only regression has NO prepared action -- this is the failure the real \
             end-to-end test's `request_count(&conn) == 1` assertion would catch"
        );
        let snapshot = fee_runway::load_latest_state(&conn).unwrap();
        assert_eq!(
            snapshot.rows.len(),
            0,
            "a receipt-only regression has NO state row -- this is the failure the real \
             end-to-end test's `snapshot.rows.len() == 1` assertion would catch"
        );
    }

    /// Contract §4.3 test 15 (atomic failure): an injected commit failure
    /// leaves no partial state/action/receipt visible, in-memory state is
    /// not advanced, and the red persistence-failure counter increments.
    #[test]
    fn new_channel_commit_failure_leaves_nothing_partial() {
        let fx = fixture();
        let store_path = fx._dir.path().join("rust-owned.db");
        let fail = Arc::new(AtomicBool::new(true));
        let store = TestStore::open(&store_path, fail);
        let cfg = SchedulerConfig {
            db_path: fx.db_path.clone(),
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle: StateLifecycle::RehydratePerCycle,
            trigger: TriggerMode::default(),
        };
        let mut owner = CycleOwner::new(&cfg, SEED, Some(Box::new(store)));

        let rx = self_channel(&mut owner);
        let prepared = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Static,
            Some(999),
            None,
            NOW,
        );
        owner.handle_new_channel(prepared);
        pump_store_results(&mut owner, &rx);

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            0,
            "a failed commit must not advance the generation"
        );
        assert_eq!(request_count(&conn), 0, "no partial action visible");
        assert_eq!(trigger_event_count(&conn), 0, "no partial receipt visible");
        assert_eq!(
            owner.persistence_failures(),
            1,
            "the red persistence-failure counter must increment"
        );
    }

    /// Live-review finding F2: a COALESCED occurrence (a second
    /// `new_channel` event for a channel that already has a pending entry
    /// in the trigger queue -- e.g. a duplicate notification before the
    /// next scheduled cycle drains the queue) must produce ZERO additional
    /// effect: no second commit/generation bump, no second prepared
    /// action, no second nudge. Only the FIRST (`Enqueued`) occurrence may
    /// reach decision.
    #[test]
    fn coalesced_new_channel_event_has_zero_additional_effect() {
        let fx = fixture();
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

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let rx = self_channel(&mut owner);
        let first = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior.clone()),
            NOW,
        );
        owner.handle_new_channel(first);
        pump_store_results(&mut owner, &rx);

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(generation(&conn), 1);
        assert_eq!(request_count(&conn), 1);
        let receipts_after_first = trigger_event_count(&conn);
        drop(conn);

        // A SECOND event for the SAME channel, before anything drains the
        // trigger queue's pending entry -- must coalesce, not re-decide.
        let second = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW + 1,
        );
        owner.handle_new_channel(second);
        pump_store_results(&mut owner, &rx);

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            1,
            "a coalesced occurrence must NOT bump the generation a second time"
        );
        assert_eq!(
            request_count(&conn),
            1,
            "a coalesced occurrence must NOT prepare a second action"
        );
        assert_eq!(
            trigger_event_count(&conn),
            receipts_after_first + 1,
            "a coalesced occurrence still gets its OWN auditable receipt, just no effect"
        );
    }

    /// Live-review finding F1: a preparation refusal is DURABLE -- it
    /// survives a restart (reopening the store) as an auditable receipt,
    /// with zero state row and zero prepared action, rather than
    /// disappearing as only a log line.
    #[test]
    fn new_channel_refusal_is_durable_with_zero_effect() {
        let fx = fixture();
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

        owner.handle_new_channel_refused(
            peer_a(),
            "1x1x0".to_string(),
            NOW,
            "AMBIGUOUS: multiple NORMAL channels, no exact identifier match".to_string(),
        );

        // Reopen the store fresh (simulates a restart).
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(generation(&conn), 0, "a refusal must never advance state");
        assert_eq!(
            request_count(&conn),
            0,
            "a refusal must never prepare an action"
        );
        assert_eq!(
            trigger_event_count(&conn),
            1,
            "the refusal itself IS durably recorded"
        );
        let detail: String = conn
            .query_row("SELECT detail FROM rust_fee_trigger_events", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(detail.contains("REFUSED"));
        assert!(detail.contains("AMBIGUOUS"));
    }

    /// Contract §4.5 (A3-specific epoch guard): handling a new channel
    /// must not rewrite any EXISTING channel's `skip_gate_prev`/
    /// `skip_gate_seen`, and must not perform any global epoch refresh --
    /// only the new channel's OWN state (via the atomic commit's sync
    /// fields) is touched. The two T8b guards
    /// (`decision_gate_uses_pre_decision_epoch_not_fresh_flush`,
    /// `observation_cursor_uses_pre_decision_epoch` in
    /// `revops-fees/tests/cycle.rs`) remain byte-unmodified and continue
    /// to pass -- this test is the A3-specific complement, not a
    /// replacement.
    #[test]
    fn new_channel_never_rewrites_an_existing_channels_skip_gate_epoch() {
        use revops_fees::cycle::SkipGateEpoch;

        let fx = fixture();
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

        // Seed an EXISTING, unrelated channel's skip-gate epoch memory --
        // exactly the cross-cycle state `hydrate_from_strategy_rows`
        // maintains (fee_state.rs).
        let existing_epoch_prev = SkipGateEpoch {
            last_update: 111_111,
            is_sleeping: true,
        };
        let existing_epoch_seen = SkipGateEpoch {
            last_update: 222_222,
            is_sleeping: false,
        };
        owner
            .state_mut()
            .skip_gate_prev
            .insert("existing_chan".to_string(), existing_epoch_prev);
        owner
            .state_mut()
            .skip_gate_seen
            .insert("existing_chan".to_string(), existing_epoch_seen);

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let rx = self_channel(&mut owner);
        let prepared = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW,
        );
        owner.handle_new_channel(prepared);
        pump_store_results(&mut owner, &rx);

        // The unrelated existing channel's epoch memory is UNTOUCHED --
        // no global refresh happened inside the notification handler.
        assert_eq!(
            owner.state_mut().skip_gate_prev.get("existing_chan"),
            Some(&existing_epoch_prev),
            "handling a new channel must never rewrite an existing channel's skip_gate_prev"
        );
        assert_eq!(
            owner.state_mut().skip_gate_seen.get("existing_chan"),
            Some(&existing_epoch_seen),
            "handling a new channel must never rewrite an existing channel's skip_gate_seen"
        );
        // The new channel itself is NOT added to skip_gate_prev/seen by
        // this out-of-cycle path either (those maps are exclusively
        // maintained by `hydrate_from_strategy_rows`'s per-cycle bootstrap
        // classification) -- this out-of-cycle commit is orthogonal to
        // that RehydratePerCycle-era bookkeeping, so a first-appearance
        // row is not falsely labeled comparable by it.
        assert!(!owner.state_mut().skip_gate_prev.contains_key("1x1x0"));
        assert!(!owner.state_mut().skip_gate_seen.contains_key("1x1x0"));

        // But the new channel DOES get the event-time last_update (the
        // modeled post-broadcast sync), so the NEXT SeedOnce cycle
        // observes the same waiting-window posture Python would after a
        // successful apply.
        let cycle = owner
            .state_mut()
            .cycle_states
            .get("1x1x0")
            .expect("authorized DYNAMIC must install cycle state");
        assert_eq!(cycle.last_update, NOW);
    }

    /// Contract §4.3 test 16 / live-review finding F3 (cross-restart event
    /// idempotency): replaying the SAME event (same resolved channel, same
    /// event timestamp -- `new_channel_prepared` derives the SAME
    /// `event_key` for identical inputs) across a simulated restart (a
    /// fresh `CycleOwner` over the SAME reopened store) must NOT create a
    /// second prepared action or a second nudge, while still recording an
    /// auditable duplicate receipt and never reporting a persistence
    /// failure.
    #[test]
    fn replaying_the_same_new_channel_event_after_restart_is_a_no_op() {
        let fx = fixture();
        let store_path = fx._dir.path().join("rust-owned.db");
        let cfg = SchedulerConfig {
            db_path: fx.db_path.clone(),
            socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
            journal_dir: fx.journal_dir.clone(),
            lifecycle: StateLifecycle::RehydratePerCycle,
            trigger: TriggerMode::default(),
        };
        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };

        // First process: handle the event once.
        {
            let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
            let mut owner = CycleOwner::new(&cfg, SEED, Some(Box::new(store)));
            let rx = self_channel(&mut owner);
            let prepared = new_channel_prepared(
                "1x1x0",
                &peer_a(),
                123,
                FeeStrategy::Dynamic,
                None,
                Some(prior.clone()),
                NOW,
            );
            owner.handle_new_channel(prepared);
            pump_store_results(&mut owner, &rx);
            assert_eq!(owner.persistence_failures(), 0);
        }

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(generation(&conn), 1);
        assert_eq!(request_count(&conn), 1);
        let receipts_before_replay = trigger_event_count(&conn);
        drop(conn);

        // "Restart": a brand-new CycleOwner + a brand-new TestStore
        // instance over the SAME on-disk file, replaying the IDENTICAL
        // event (same channel, same event_ts -> same event_key).
        {
            let store = TestStore::open(&store_path, Arc::new(AtomicBool::new(false)));
            let mut owner = CycleOwner::new(&cfg, SEED, Some(Box::new(store)));
            let rx = self_channel(&mut owner);
            let replayed = new_channel_prepared(
                "1x1x0",
                &peer_a(),
                123,
                FeeStrategy::Dynamic,
                None,
                Some(prior),
                NOW,
            );
            owner.handle_new_channel(replayed);
            pump_store_results(&mut owner, &rx);
            assert_eq!(
                owner.persistence_failures(),
                0,
                "a duplicate replay is NOT a persistence failure"
            );
        }

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            1,
            "the replay must NOT bump the generation a second time"
        );
        assert_eq!(
            request_count(&conn),
            1,
            "the replay must NOT create a second prepared action"
        );
        let snapshot = fee_runway::load_latest_state(&conn).unwrap();
        assert_eq!(snapshot.rows.len(), 1);
        assert_eq!(
            snapshot.rows[0]
                .v2_state_json
                .matches("\"posterior_bias\": [[300.0, 0.3,")
                .count(),
            1,
            "exactly ONE nudge must exist after the replay, not two"
        );
        assert!(
            trigger_event_count(&conn) > receipts_before_replay,
            "the replay itself still produces its OWN auditable (duplicate) receipt"
        );
    }

    /// Contract §4.3 test 17 (backpressure): saturating distinct trigger
    /// scopes, then a new-channel event, records a RED drop and applies
    /// no state/action effect.
    #[test]
    fn new_channel_dropped_under_backpressure_has_zero_effect() {
        let fx = fixture();
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

        // Saturate the bounded queue with distinct scopes (never coalesced
        // -- distinct peer/channel ids each time) so the NEXT distinct
        // trigger (the new-channel offer below) is dropped.
        for i in 0..TRIGGER_QUEUE_CAPACITY {
            owner.handle_forward_event(&format!("filler-{i}"), NOW);
        }
        assert_eq!(
            owner.trigger_queue_dropped_total(),
            0,
            "queue not yet saturated"
        );

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let prepared = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW,
        );
        owner.handle_new_channel(prepared);

        assert_eq!(
            owner.trigger_queue_dropped_total(),
            1,
            "the new_channel offer must be the one dropped occurrence"
        );

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            0,
            "a dropped trigger must never advance state"
        );
        assert_eq!(
            request_count(&conn),
            0,
            "a dropped trigger must never prepare an action"
        );
        // The drop itself IS recorded -- backpressure must be loud, never
        // silent.
        assert!(
            trigger_event_count(&conn) >= 1,
            "the drop must be a recorded RED event"
        );
    }

    /// F7 test plumbing: a store whose GUARDED COMMITS park until the
    /// test releases them -- deterministically simulating a store actor
    /// that accepted the A3 commit command but has not executed it yet.
    /// Everything else delegates to a real [`TestStore`] over the same
    /// file.
    type ParkedGuardedCommit = (
        FeeCycleCommit,
        u64,
        revops::fee_state::StoreDispatchCallback<fee_runway::GuardedCommitOutcome>,
    );

    struct ParkingStore {
        inner: TestStore,
        path: PathBuf,
        parked: Arc<std::sync::Mutex<Vec<ParkedGuardedCommit>>>,
    }

    impl ParkingStore {
        fn open(
            path: &Path,
        ) -> (
            ParkingStore,
            Arc<std::sync::Mutex<Vec<ParkedGuardedCommit>>>,
        ) {
            let parked = Arc::new(std::sync::Mutex::new(Vec::new()));
            (
                ParkingStore {
                    inner: TestStore::open(path, Arc::new(AtomicBool::new(false))),
                    path: path.to_path_buf(),
                    parked: parked.clone(),
                },
                parked,
            )
        }
    }

    /// Execute every parked guarded commit (in arrival order) against the
    /// same on-disk store and deliver each result -- "the actor got to
    /// them".
    fn release_parked(path: &Path, parked: &Arc<std::sync::Mutex<Vec<ParkedGuardedCommit>>>) {
        let drained: Vec<ParkedGuardedCommit> = std::mem::take(&mut *parked.lock().unwrap());
        for (commit, expected_prior, on_done) in drained {
            let conn = Connection::open(path).expect("open store for parked commit");
            on_done(fee_runway::commit_fee_cycle_guarded(
                &conn,
                &commit,
                expected_prior,
            ));
        }
    }

    impl RunwayStateStore for ParkingStore {
        fn load_latest_state(&self) -> anyhow::Result<FeeStateSnapshot> {
            self.inner.load_latest_state()
        }

        fn commit_fee_cycle(&self, commit: FeeCycleCommit) -> anyhow::Result<u64> {
            RunwayStateStore::commit_fee_cycle(&self.inner, commit)
        }

        fn record_seed_refusal(&self, event: fee_runway::FeeSeedEventRow) -> anyhow::Result<i64> {
            self.inner.record_seed_refusal(event)
        }

        fn refresh_mempool_window(
            &self,
            sampled_at: i64,
            sat_per_vbyte: f64,
            retain_since: i64,
        ) -> anyhow::Result<fee_runway::MempoolWindow> {
            self.inner
                .refresh_mempool_window(sampled_at, sat_per_vbyte, retain_since)
        }

        fn record_restart_marker(
            &self,
            marker: fee_runway::FeeRestartMarkerRow,
        ) -> anyhow::Result<i64> {
            self.inner.record_restart_marker(marker)
        }

        fn record_mempool_sample_pruned(
            &self,
            sampled_at: i64,
            sat_per_vbyte: f64,
            retain_since: i64,
        ) -> anyhow::Result<()> {
            self.inner
                .record_mempool_sample_pruned(sampled_at, sat_per_vbyte, retain_since)
        }

        fn query_mempool_samples_since(
            &self,
            since: i64,
        ) -> anyhow::Result<Vec<fee_runway::MempoolSampleRow>> {
            self.inner.query_mempool_samples_since(since)
        }

        fn record_mempool_ma_comparison(
            &self,
            row: fee_runway::MempoolMaComparisonRow,
        ) -> anyhow::Result<i64> {
            self.inner.record_mempool_ma_comparison(row)
        }

        fn record_trigger_event(
            &self,
            event: fee_runway::FeeTriggerEventRow,
        ) -> anyhow::Result<()> {
            RunwayStateStore::record_trigger_event(&self.inner, event)
        }

        fn cycle_exists(&self, cycle_id: &str) -> anyhow::Result<bool> {
            RunwayStateStore::cycle_exists(&self.inner, cycle_id)
        }

        fn dispatch_cycle_exists_with_generation(
            &self,
            cycle_id: String,
            on_done: revops::fee_state::StoreDispatchCallback<(bool, u64)>,
        ) -> anyhow::Result<()> {
            self.inner
                .dispatch_cycle_exists_with_generation(cycle_id, on_done)
        }

        fn dispatch_commit_fee_cycle_guarded(
            &self,
            commit: FeeCycleCommit,
            expected_prior_generation: u64,
            on_done: revops::fee_state::StoreDispatchCallback<fee_runway::GuardedCommitOutcome>,
        ) -> anyhow::Result<()> {
            let _ = &self.path;
            self.parked
                .lock()
                .unwrap()
                .push((commit, expected_prior_generation, on_done));
            Ok(())
        }

        fn dispatch_record_trigger_event(
            &self,
            event: fee_runway::FeeTriggerEventRow,
            on_done: revops::fee_state::StoreDispatchCallback<()>,
        ) -> anyhow::Result<()> {
            self.inner.dispatch_record_trigger_event(event, on_done)
        }
    }

    /// F7 fixture bundle: python DB seeded with CHANNEL (`700x1x0`), a
    /// SeedOnce owner over a ParkingStore, first cycle run (seed +
    /// generation 1), self-channel wired.
    struct ParkedSeedOnce {
        _fx: Fixture,
        owner: CycleOwner,
        rx: TestOwnerReceiver,
        store_path: PathBuf,
        parked: Arc<std::sync::Mutex<Vec<ParkedGuardedCommit>>>,
    }

    fn parked_seedonce_after_first_cycle() -> ParkedSeedOnce {
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

        std::fs::create_dir_all(&fx.journal_dir).expect("journal dir");
        let store_path = fx.journal_dir.join("rust-owned.db");
        let (store, parked) = ParkingStore::open(&store_path);
        let mut owner = owner_with_store(&fx, Some(Box::new(store)));
        let rx = self_channel(&mut owner);

        let mut clock = || NOW + 1800;
        let outcome = owner.run_cycle(seedonce_prepared_cycle(), &mut clock);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");

        ParkedSeedOnce {
            _fx: fx,
            owner,
            rx,
            store_path,
            parked,
        }
    }

    /// The SeedOnce prepared-cycle inputs `SeedOnceHarness::prepared`
    /// uses, as a free helper for the F7 tests.
    fn seedonce_prepared_cycle() -> PreparedCycle {
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

    /// F7 (the CAS, external/inter-A3 guard): if the store's generation
    /// advances AFTER an A3 decision but BEFORE its guarded commit
    /// executes at the actor, the commit must land NOTHING (in-band
    /// `GenerationConflict`), the staged state must be discarded
    /// fail-closed, and the owner must adopt the store's real generation.
    /// (Mutation-verified: bypassing the CAS reds this test -- see
    /// TASK44-REPORT.md.)
    #[test]
    fn a3_commit_against_an_advanced_store_is_a_conflict_not_a_stale_write() {
        let ParkedSeedOnce {
            _fx,
            mut owner,
            rx,
            store_path,
            parked,
        } = parked_seedonce_after_first_cycle();

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let evt = new_channel_prepared(
            CHANNEL,
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW + 10,
        );
        let requests_before = request_count(&Connection::open(&store_path).unwrap());
        owner.handle_new_channel(evt);
        pump_store_results(&mut owner, &rx); // idempotency -> decide -> commit PARKED
        assert_eq!(owner.initial_fee_pending(), 1, "commit parked in flight");
        let memory_before = owner.state().cycle_states[CHANNEL].clone();

        // The store advances past the decision's basis (generation 1 ->
        // 2) while the A3 commit is still parked -- the exact schedule
        // the CAS exists for.
        {
            let conn = Connection::open(&store_path).unwrap();
            fee_runway::commit_fee_cycle(
                &conn,
                &FeeCycleCommit {
                    cycle_id: "external-advance".to_string(),
                    started_at: NOW + 20,
                    completed_at: NOW + 20,
                    source_commit: "test".to_string(),
                    binary_sha256: "test".to_string(),
                    ..Default::default()
                },
            )
            .unwrap();
        }

        release_parked(&store_path, &parked);
        pump_store_results(&mut owner, &rx);

        assert_eq!(
            owner.initial_fee_conflicts(),
            1,
            "the CAS refusal is a red conflict"
        );
        assert_eq!(owner.initial_fee_pending(), 0);
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            2,
            "the stale A3 commit must have written NOTHING (generation stays at the \
             external advance)"
        );
        assert_eq!(
            request_count(&conn),
            requests_before,
            "no stale prepared action landed"
        );
        assert_eq!(
            owner.state().cycle_states[CHANNEL],
            memory_before,
            "the staged state was discarded fail-closed -- memory keeps the pre-A3 epoch"
        );
    }

    /// F7 (install rule): an A3 commit that DID land, whose callback is
    /// processed only after the owner advanced past the decision's basis,
    /// must NOT install its staged (now stale) state over the newer owner
    /// epoch -- fail-closed conflict, memory keeps the newer state and
    /// matches the store's latest row for the channel. (Mutation-verified:
    /// removing the owner-unadvanced check reds this test.)
    #[test]
    fn late_a3_callback_after_owner_advance_never_installs_stale_state() {
        let ParkedSeedOnce {
            _fx,
            mut owner,
            rx,
            store_path,
            parked,
        } = parked_seedonce_after_first_cycle();

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let evt = new_channel_prepared(
            CHANNEL,
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW + 10,
        );
        owner.handle_new_channel(evt);
        pump_store_results(&mut owner, &rx); // commit parked (expected prior 1)

        // The A3 commit executes (generation 2)...
        release_parked(&store_path, &parked);
        // ...but BEFORE its callback is processed, a full cycle runs on
        // the owner (pre-install memory) and commits generation 3.
        let mut clock = || NOW + 3600;
        let outcome = owner.run_cycle(seedonce_prepared_cycle(), &mut clock);
        assert!(matches!(outcome, CycleOutcome::Ran { .. }), "{outcome:?}");
        let memory_after_cycle = owner.state().cycle_states[CHANNEL].clone();

        // The late callback must be a conflict, not an install.
        pump_store_results(&mut owner, &rx);

        assert_eq!(
            owner.initial_fee_conflicts(),
            1,
            "a stale install attempt is a red conflict"
        );
        assert_eq!(
            owner.state().cycle_states[CHANNEL],
            memory_after_cycle,
            "memory must keep the newer post-cycle epoch, not the stale staged state"
        );
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(generation(&conn), 3, "A3 (2) then the cycle (3)");
        let (latest_json,): (String,) = conn
            .query_row(
                "SELECT v2_state_json FROM rust_fee_state WHERE channel_id = ?1",
                rusqlite::params![CHANNEL],
                |r| Ok((r.get(0)?,)),
            )
            .unwrap();
        assert!(
            latest_json.contains(&format!(
                "\"last_update\": {}",
                memory_after_cycle.last_update
            )),
            "the store's latest row for the channel is the cycle's (memory == DB): {latest_json}"
        );
    }

    /// F7 refinement (Python-parity sequencing): a prepared cycle
    /// arriving while an A3 store result is pending must be DEFERRED and
    /// run only after the A3 occurrence settles -- so the cycle consumes
    /// the synchronized post-A3 state, exactly as Python's `_state_lock`
    /// serializes `_handle_channel_open` against the cycle. Running it
    /// immediately would commit a pre-A3 epoch and orphan the A3 commit
    /// into a conflict.
    #[test]
    fn run_prepared_during_inflight_a3_commit_is_deferred_until_the_install() {
        let ParkedSeedOnce {
            _fx,
            mut owner,
            rx,
            store_path,
            parked,
        } = parked_seedonce_after_first_cycle();

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let evt = new_channel_prepared(
            CHANNEL,
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW + 10,
        );
        owner.handle_new_channel(evt);
        pump_store_results(&mut owner, &rx); // commit parked, in flight

        // The production loop's RunPrepared entry, while the A3 commit is
        // in flight: must DEFER, not run.
        let mut clock = || NOW + 3600;
        let outcome = owner.run_or_defer_cycle(Box::new(seedonce_prepared_cycle()), &mut clock);
        assert!(
            outcome.is_none(),
            "the cycle must be deferred while an A3 store result is pending, got {outcome:?}"
        );
        {
            let conn = Connection::open(&store_path).unwrap();
            assert_eq!(
                generation(&conn),
                1,
                "a deferred cycle must not have committed anything yet"
            );
        }

        // The A3 commit executes and its callback settles the occurrence;
        // the deferred cycle must then run AND consume the A3 state.
        release_parked(&store_path, &parked);
        let mut release_clock = || NOW + 3600;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                CycleMsg::InitialFeeStoreResult(result) => {
                    owner.handle_initial_fee_store_result(result, &mut release_clock)
                }
                _ => panic!("unexpected owner message"),
            }
        }

        assert_eq!(owner.initial_fee_pending(), 0);
        assert_eq!(
            owner.initial_fee_conflicts(),
            0,
            "correct sequencing has no conflicts: install first, cycle after"
        );
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            3,
            "A3 commit (2), then the deferred cycle (3)"
        );
        let (latest_json,): (String,) = conn
            .query_row(
                "SELECT v2_state_json FROM rust_fee_state WHERE channel_id = ?1",
                rusqlite::params![CHANNEL],
                |r| Ok((r.get(0)?,)),
            )
            .unwrap();
        assert!(
            latest_json.contains("\"posterior_bias\": [[300.0, 0.3,"),
            "the deferred cycle's flush must carry the A3-seeded nudge (the cycle ran AFTER \
             the install): {latest_json}"
        );
    }

    /// F7 refinement: the deferral slot is BOUNDED to one prepared cycle
    /// -- a newer prepared snapshot supersedes an older deferred one
    /// (loudly, counted), and exactly ONE deferred cycle runs at release.
    #[test]
    fn deferred_cycles_are_bounded_and_superseded_loudly() {
        let ParkedSeedOnce {
            _fx,
            mut owner,
            rx,
            store_path,
            parked,
        } = parked_seedonce_after_first_cycle();

        let evt = new_channel_prepared(
            CHANNEL,
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            None,
            NOW + 10,
        );
        owner.handle_new_channel(evt);
        pump_store_results(&mut owner, &rx); // commit parked, in flight

        let mut clock = || NOW + 3600;
        assert!(owner
            .run_or_defer_cycle(Box::new(seedonce_prepared_cycle()), &mut clock)
            .is_none());
        assert!(owner
            .run_or_defer_cycle(Box::new(seedonce_prepared_cycle()), &mut clock)
            .is_none());
        assert_eq!(
            owner.deferred_cycles_superseded(),
            1,
            "the second deferred prepared cycle supersedes the first, loudly counted"
        );

        release_parked(&store_path, &parked);
        let mut release_clock = || NOW + 3600;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                CycleMsg::InitialFeeStoreResult(result) => {
                    owner.handle_initial_fee_store_result(result, &mut release_clock)
                }
                _ => panic!("unexpected owner message"),
            }
        }

        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            3,
            "A3 commit (2) plus exactly ONE deferred cycle (3) -- never two"
        );
    }

    #[test]
    fn deferred_ack_supersession_keeps_newest_pending_then_reports_real_outcome() {
        let ParkedSeedOnce {
            _fx,
            mut owner,
            rx,
            store_path: _,
            parked,
        } = parked_seedonce_after_first_cycle();
        let evt = new_channel_prepared(
            CHANNEL,
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            None,
            NOW + 10,
        );
        owner.handle_new_channel(evt);
        pump_store_results(&mut owner, &rx);

        let mut clock = || NOW + 3600;
        let (old_tx, mut old_rx) = tokio::sync::oneshot::channel();
        owner.run_or_defer_cycle_with_ack(Box::new(seedonce_prepared_cycle()), &mut clock, old_tx);
        assert!(matches!(
            old_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        let (new_tx, mut new_rx) = tokio::sync::oneshot::channel();
        owner.run_or_defer_cycle_with_ack(Box::new(seedonce_prepared_cycle()), &mut clock, new_tx);
        let old = old_rx.try_recv().expect("old deferred ACK is explicit");
        assert_eq!(
            old,
            Err("deferred cycle superseded before execution".to_string())
        );
        assert!(matches!(
            new_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        release_parked(&_fx.journal_dir.join("rust-owned.db"), &parked);
        pump_store_results(&mut owner, &rx);
        assert_eq!(
            new_rx.try_recv().expect("newest deferred ACK is terminal"),
            Ok(())
        );
    }

    #[test]
    fn immediate_skips_and_persistence_failures_are_never_acknowledged_as_success() {
        let fx = fixture();
        let mut owner = CycleOwner::new(
            &SchedulerConfig {
                db_path: fx.db_path.clone(),
                socket_path: PathBuf::from("/nonexistent/lightning-rpc"),
                journal_dir: fx.journal_dir.clone(),
                lifecycle: StateLifecycle::RehydratePerCycle,
                trigger: TriggerMode::ExternalOnly,
            },
            SEED,
            None,
        );
        let (skipped_tx, mut skipped_rx) = tokio::sync::oneshot::channel();
        let mut clock = || NOW;
        owner.run_or_defer_cycle_with_ack(
            Box::new(prepared(json!("unresolvable"), false)),
            &mut clock,
            skipped_tx,
        );
        let skipped = skipped_rx.try_recv().unwrap().unwrap_err();
        assert!(skipped.contains("neighbor_median_min_competitors"));

        let mut h = seedonce_harness_with_one_channel();
        assert!(matches!(h.run_cycle(), CycleOutcome::Ran { .. }));
        h.fail_commits.store(true, Ordering::SeqCst);
        let prepared = h.prepared();
        let (failed_tx, mut failed_rx) = tokio::sync::oneshot::channel();
        let mut clock = || NOW + 3600;
        h.owner
            .run_or_defer_cycle_with_ack(Box::new(prepared), &mut clock, failed_tx);
        let failed = failed_rx.try_recv().unwrap().unwrap_err();
        assert!(failed.contains("PersistenceFailed"), "{failed}");
    }

    #[test]
    fn owner_loss_closes_a_deferred_ack_that_never_reached_execution() {
        let ParkedSeedOnce {
            _fx,
            mut owner,
            rx,
            store_path: _,
            parked: _,
        } = parked_seedonce_after_first_cycle();
        owner.handle_new_channel(new_channel_prepared(
            CHANNEL,
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            None,
            NOW + 10,
        ));
        pump_store_results(&mut owner, &rx);
        let (completion, mut acknowledged) = tokio::sync::oneshot::channel();
        let mut clock = || NOW + 3600;
        owner.run_or_defer_cycle_with_ack(
            Box::new(seedonce_prepared_cycle()),
            &mut clock,
            completion,
        );
        assert!(matches!(
            acknowledged.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        drop(owner);
        assert!(
            matches!(
                acknowledged.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ),
            "owner loss must close every still-outstanding deferred ACK"
        );
    }

    #[test]
    fn commit_dispatch_launch_failure_is_terminal_inline_without_a_pending_leak() {
        let fx = fixture();
        let store_path = fx._dir.path().join("rust-owned.db");
        let fail_launch = Arc::new(AtomicBool::new(false));
        let store = TestStore::open_with_commit_launch_failure(
            &store_path,
            Arc::new(AtomicBool::new(false)),
            fail_launch.clone(),
        );
        let mut owner = owner_with_store(&fx, Some(Box::new(store)));
        let rx = self_channel(&mut owner);
        let prepared = new_channel_prepared(
            CHANNEL,
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            None,
            NOW + 10,
        );
        let event_key = prepared.event_key.clone();
        owner.handle_new_channel(prepared);
        let idempotency = match rx.try_recv().expect("idempotency answer was dispatched") {
            CycleMsg::InitialFeeStoreResult(result) => result,
            _ => panic!("unexpected owner message"),
        };
        assert_eq!(owner.initial_fee_pending(), 1);
        fail_launch.store(true, Ordering::SeqCst);
        let mut clock = || NOW + 10;
        owner.handle_initial_fee_store_result(idempotency, &mut clock);

        assert_eq!(
            owner.persistence_failures(),
            1,
            "one failed launch has one terminal failure"
        );
        assert_eq!(
            owner.initial_fee_pending(),
            0,
            "inline launch failure must remove the pending commit"
        );
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "launch failure must not invoke the commit callback"
        );
        assert!(
            !fee_runway::cycle_exists(
                &Connection::open(&store_path).expect("reopen store"),
                &event_key,
            )
            .expect("query cycle"),
            "failed launch must not write a commit"
        );
    }

    #[test]
    fn idempotency_dispatch_launch_failure_is_terminal_inline_without_a_pending_leak() {
        let fx = fixture();
        let store_path = fx._dir.path().join("rust-owned.db");
        let launch_failures = DispatchLaunchFailures::default();
        launch_failures.idempotency.store(true, Ordering::SeqCst);
        let store = TestStore::open_with_dispatch_launch_failures(
            &store_path,
            Arc::new(AtomicBool::new(false)),
            launch_failures,
        );
        let mut owner = owner_with_store(&fx, Some(Box::new(store)));
        let rx = self_channel(&mut owner);

        owner.handle_new_channel(new_channel_prepared(
            CHANNEL,
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            None,
            NOW + 20,
        ));

        assert_eq!(owner.persistence_failures(), 1);
        assert_eq!(owner.initial_fee_pending(), 0);
        match rx
            .try_recv()
            .expect("the refusal receipt launches after the idempotency launch failure")
        {
            CycleMsg::InitialFeeStoreResult(
                revops::fee_scheduler::InitialFeeStoreResult::Receipt { result: Ok(()), .. },
            ) => {}
            _ => panic!("expected only the successful refusal receipt result"),
        }
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn receipt_dispatch_launch_failure_is_counted_inline_once_without_a_callback() {
        let fx = fixture();
        let store_path = fx._dir.path().join("rust-owned.db");
        let launch_failures = DispatchLaunchFailures::default();
        launch_failures.receipt.store(true, Ordering::SeqCst);
        let store = TestStore::open_with_dispatch_launch_failures(
            &store_path,
            Arc::new(AtomicBool::new(false)),
            launch_failures,
        );
        let mut owner = owner_with_store(&fx, Some(Box::new(store)));
        let rx = self_channel(&mut owner);

        owner.handle_new_channel_refused(
            peer_a(),
            CHANNEL.to_string(),
            NOW + 30,
            "injected preparation refusal".to_string(),
        );

        assert_eq!(owner.persistence_failures(), 1);
        assert_eq!(owner.initial_fee_pending(), 0);
        assert!(
            matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "failed receipt launch must never invoke its callback"
        );
    }

    /// F5 (same-channel pending/race, fail-closed): once a cycle has
    /// drained the trigger queue, a NEW occurrence for a channel whose
    /// store result is STILL in flight would pass the offer as `Enqueued`
    /// -- the pending map must then refuse it fail-closed. Without the
    /// guard, the new occurrence overwrites the in-flight entry: the
    /// first (already durably committed) result is orphaned into a
    /// conflict, its staged state is never installed, and the second
    /// occurrence decides AND commits a second time (two generations, two
    /// prepared actions for one channel).
    #[test]
    fn same_channel_event_while_commit_in_flight_is_refused_fail_closed() {
        let fx = fixture();
        seed_channel_state(&fx.db_path);
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
        let rx = self_channel(&mut owner);

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let first = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior.clone()),
            NOW,
        );
        owner.handle_new_channel(first);

        // Advance ONLY to the Committing phase: handle the idempotency
        // answer, leaving the (real) commit result parked in the queue --
        // the commit is "in flight".
        {
            let mut clock = || NOW;
            match rx.try_recv().expect("idempotency result was dispatched") {
                CycleMsg::InitialFeeStoreResult(result) => {
                    owner.handle_initial_fee_store_result(result, &mut clock)
                }
                _ => panic!("unexpected owner message"),
            }
        }
        assert_eq!(owner.initial_fee_pending(), 1, "commit is in flight");

        // A scheduled cycle drains the trigger queue (its documented drain
        // point) -- so the next same-channel occurrence is NOT coalesced
        // by the queue and reaches the pending guard.
        let mut clock = || NOW + 60;
        owner.run_cycle(prepared(json!(3), true), &mut clock);

        // A THIRD occurrence for the SAME channel (different event time ->
        // different event_key) while the commit result is still in
        // flight: must be refused fail-closed, with zero decision/RNG and
        // zero dispatch.
        let racing = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW + 7,
        );
        owner.handle_new_channel(racing);
        assert_eq!(
            owner.initial_fee_pending(),
            1,
            "the racing occurrence must NOT replace or add a pending entry"
        );

        // Now let everything settle: the in-flight commit result installs
        // the FIRST occurrence's staged state; nothing else happens.
        pump_store_results(&mut owner, &rx);

        assert_eq!(
            owner.initial_fee_conflicts(),
            0,
            "the awaited commit result is NOT a conflict -- refusing the racing occurrence must \
             leave the in-flight entry to complete normally"
        );
        assert_eq!(
            owner.persistence_failures(),
            0,
            "a fail-closed race refusal is not a persistence failure"
        );
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            1,
            "exactly ONE commit -- the racing occurrence must never re-decide/re-commit"
        );
        assert_eq!(
            request_count(&conn),
            1,
            "exactly ONE prepared action for the channel"
        );
        let refusals: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM rust_fee_trigger_events WHERE detail LIKE \
                 '%REFUSED%in flight%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            refusals, 1,
            "the racing occurrence gets a typed, durable fail-closed refusal receipt"
        );
    }

    /// F5 (identity binding, idempotency phase): a result message whose
    /// event_key/generation does not match the pending entry -- a forged,
    /// stale, or foreign answer -- must be discarded as a red conflict
    /// WITHOUT consuming the pending entry, making a decision, or
    /// dispatching a commit. The real answer, arriving later, must still
    /// complete normally.
    #[test]
    fn mismatched_idempotency_result_is_a_conflict_not_a_decision() {
        use revops::fee_scheduler::InitialFeeStoreResult;

        let fx = fixture();
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
        let rx = self_channel(&mut owner);

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let prepared_evt = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW,
        );
        owner.handle_new_channel(prepared_evt);
        assert_eq!(owner.initial_fee_pending(), 1);

        // A forged/stale answer: right channel, wrong identity.
        let mut clock = || NOW;
        owner.handle_initial_fee_store_result(
            InitialFeeStoreResult::Idempotency {
                channel_id: "1x1x0".to_string(),
                event_key: "forged-event-key".to_string(),
                generation: 999,
                result: Ok((false, 0)),
            },
            &mut clock,
        );

        assert_eq!(
            owner.initial_fee_conflicts(),
            1,
            "an identity-mismatched result is a red conflict"
        );
        assert_eq!(
            owner.initial_fee_pending(),
            1,
            "the pending entry must survive a mismatched result untouched"
        );
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(
            generation(&conn),
            0,
            "a mismatched result must never trigger a decision/commit"
        );
        assert_eq!(request_count(&conn), 0);
        drop(conn);

        // The REAL answer still completes the occurrence normally.
        pump_store_results(&mut owner, &rx);
        let conn = Connection::open(&store_path).unwrap();
        assert_eq!(generation(&conn), 1, "the genuine result still commits");
        assert_eq!(request_count(&conn), 1);
        assert_eq!(owner.initial_fee_pending(), 0);
        assert_eq!(
            owner.initial_fee_conflicts(),
            1,
            "only the forged answer conflicted"
        );
    }

    /// F5 (identity binding, commit phase): a commit result bound to the
    /// wrong generation/event_key -- e.g. a forged failure -- must be a
    /// red conflict that neither uninstalls/discards the staged state nor
    /// counts a persistence failure; the REAL success result must still
    /// install the staged state afterward.
    #[test]
    fn mismatched_commit_result_is_a_conflict_not_an_install_or_discard() {
        use revops::fee_scheduler::InitialFeeStoreResult;

        let fx = fixture();
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
        let rx = self_channel(&mut owner);

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let prepared_evt = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW,
        );
        owner.handle_new_channel(prepared_evt);
        // Advance to Committing: the real commit result is now parked in
        // the queue.
        {
            let mut clock = || NOW;
            match rx.try_recv().expect("idempotency result was dispatched") {
                CycleMsg::InitialFeeStoreResult(result) => {
                    owner.handle_initial_fee_store_result(result, &mut clock)
                }
                _ => panic!("unexpected owner message"),
            }
        }
        assert_eq!(owner.initial_fee_pending(), 1, "commit in flight");

        // A forged FAILURE with the wrong generation: must not discard
        // the staged state, must not count a persistence failure.
        let mut clock = || NOW;
        owner.handle_initial_fee_store_result(
            InitialFeeStoreResult::Commit {
                channel_id: "1x1x0".to_string(),
                event_key: "forged-event-key".to_string(),
                generation: 999,
                result: Err("forged failure".to_string()),
            },
            &mut clock,
        );

        assert_eq!(
            owner.initial_fee_conflicts(),
            1,
            "an identity-mismatched commit result is a red conflict"
        );
        assert_eq!(
            owner.persistence_failures(),
            0,
            "a forged failure must NOT count as a real persistence failure"
        );
        assert_eq!(
            owner.initial_fee_pending(),
            1,
            "the staged entry must survive the mismatched result"
        );

        // The REAL success result still installs the staged state.
        pump_store_results(&mut owner, &rx);
        assert_eq!(owner.initial_fee_pending(), 0);
        assert!(
            owner.state_mut().cycle_states.contains_key("1x1x0"),
            "the genuine commit success must still install the staged state"
        );
        assert_eq!(owner.persistence_failures(), 0);
    }

    /// Live-review finding F5: a store whose every call blocks forever
    /// (a stalled/wedged SQLite single-owner actor). The A3 path must
    /// never let this wedge the OWNER thread: every store interaction on
    /// the new-channel path is dispatched off-owner, with results routed
    /// back as owner-queue messages.
    struct WedgedStore {
        /// Keeps the channel's sender alive so [`Self::wedge`]'s `recv`
        /// blocks forever instead of returning a disconnect error.
        _keep_alive: std::sync::mpsc::Sender<()>,
        block: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl WedgedStore {
        fn new() -> WedgedStore {
            let (tx, rx) = std::sync::mpsc::channel();
            WedgedStore {
                _keep_alive: tx,
                block: std::sync::Mutex::new(rx),
            }
        }

        /// Block forever (the sender lives in `self`, so `recv` never
        /// returns while the store exists).
        fn wedge<T>(&self) -> anyhow::Result<T> {
            let _ = self.block.lock().unwrap().recv();
            anyhow::bail!("wedged store unexpectedly released")
        }
    }

    impl RunwayStateStore for WedgedStore {
        fn load_latest_state(&self) -> anyhow::Result<FeeStateSnapshot> {
            self.wedge()
        }

        fn commit_fee_cycle(&self, _commit: FeeCycleCommit) -> anyhow::Result<u64> {
            self.wedge()
        }

        fn refresh_mempool_window(
            &self,
            _sampled_at: i64,
            _sat_per_vbyte: f64,
            _retain_since: i64,
        ) -> anyhow::Result<fee_runway::MempoolWindow> {
            self.wedge()
        }

        fn record_seed_refusal(&self, _event: fee_runway::FeeSeedEventRow) -> anyhow::Result<i64> {
            self.wedge()
        }

        fn record_restart_marker(
            &self,
            _marker: fee_runway::FeeRestartMarkerRow,
        ) -> anyhow::Result<i64> {
            self.wedge()
        }

        fn record_mempool_sample_pruned(
            &self,
            _sampled_at: i64,
            _sat_per_vbyte: f64,
            _retain_since: i64,
        ) -> anyhow::Result<()> {
            self.wedge()
        }

        fn query_mempool_samples_since(
            &self,
            _since: i64,
        ) -> anyhow::Result<Vec<fee_runway::MempoolSampleRow>> {
            self.wedge()
        }

        fn record_mempool_ma_comparison(
            &self,
            _row: fee_runway::MempoolMaComparisonRow,
        ) -> anyhow::Result<i64> {
            self.wedge()
        }

        fn record_trigger_event(
            &self,
            _event: fee_runway::FeeTriggerEventRow,
        ) -> anyhow::Result<()> {
            self.wedge()
        }

        fn cycle_exists(&self, _cycle_id: &str) -> anyhow::Result<bool> {
            self.wedge()
        }

        // F5: the stalled actor's dispatch shape -- the call returns
        // immediately (as the trait requires) but the reply NEVER
        // arrives, exactly like an actor that accepted the command and
        // then hung. The owner must survive this indefinitely.

        fn dispatch_cycle_exists_with_generation(
            &self,
            _cycle_id: String,
            on_done: revops::fee_state::StoreDispatchCallback<(bool, u64)>,
        ) -> anyhow::Result<()> {
            drop(on_done);
            Ok(())
        }

        fn dispatch_commit_fee_cycle_guarded(
            &self,
            _commit: FeeCycleCommit,
            _expected_prior_generation: u64,
            on_done: revops::fee_state::StoreDispatchCallback<fee_runway::GuardedCommitOutcome>,
        ) -> anyhow::Result<()> {
            drop(on_done);
            Ok(())
        }

        fn dispatch_record_trigger_event(
            &self,
            _event: fee_runway::FeeTriggerEventRow,
            on_done: revops::fee_state::StoreDispatchCallback<()>,
        ) -> anyhow::Result<()> {
            drop(on_done);
            Ok(())
        }
    }

    /// Live-review finding F5 (the binding recovery contract): the single
    /// owner must NOT block on SQLite-actor replies. With a fully wedged
    /// store, sending a Ready new-channel event must leave the owner
    /// thread responsive -- a subsequent `Query` still round-trips within
    /// the timeout. (Against the pre-fix blocking implementation this
    /// test times out: the owner wedges inside the store call and never
    /// services the query.)
    #[tokio::test]
    async fn a_wedged_store_never_wedges_the_owner_thread() {
        use revops::fee_scheduler::NewChannelPreparation;

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
            Some(Box::new(WedgedStore::new())),
            |name, body| {
                std::thread::Builder::new()
                    .name(name.to_string())
                    .spawn(body)
                    .map(|_join| ())
            },
        )
        .expect("spawn scheduler");

        let prior = FeePrior {
            mean: 300,
            std: 40,
            source: "network".to_string(),
        };
        let prepared = new_channel_prepared(
            "1x1x0",
            &peer_a(),
            123,
            FeeStrategy::Dynamic,
            None,
            Some(prior),
            NOW,
        );
        handle
            .tx
            .send(CycleMsg::NewChannel(Box::new(
                NewChannelPreparation::Ready(Box::new(prepared)),
            )))
            .await
            .expect("send NewChannel");

        // The owner must keep servicing other messages while the store
        // stalls -- THE F5 proof.
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        handle
            .tx
            .send(CycleMsg::Query(FeeDebugQuery::RunwayCounters, reply_tx))
            .await
            .expect("send Query");
        let value = reply_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("owner thread must stay responsive while the store is wedged");
        assert!(
            value.get("lifecycle").is_some(),
            "the query answer must be the real runway-counters shape: {value:?}"
        );

        handle.tx.send(CycleMsg::Shutdown).await.ok();
    }
}

// ---------------------------------------------------------------------------
// Task 44 / A3, live-review finding F6 (config half): `prepare_new_channel`
// must REFUSE -- not decide on struct defaults -- when the config store's
// queries fail. The shared per-cycle `resolve_fee_cfg` keeps its
// log-and-default posture (deliberately untouched); only the A3 preparation
// path is strict.
// ---------------------------------------------------------------------------

/// Minimal mock `lightning-rpc` for `prepare_new_channel`'s prefetch (same
/// framing as `tests/fee_evidence.rs`'s `serve_methods`): one NORMAL
/// channel for peer A resolving `100x1x0` exactly.
fn serve_new_channel_rpc(socket_path: PathBuf, listconfigs: Option<Value>) {
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
                        "short_channel_id": "100x1x0", "peer_id": peer_a(),
                        "total_msat": 1_000_000_000_i64, "to_us_msat": 500_000_000_i64,
                        "spendable_msat": 400_000_000_i64, "receivable_msat": 500_000_000_i64,
                        "fee_base_msat": 0, "fee_proportional_millionths": 150,
                        "minimum_htlc_out_msat": 1, "maximum_htlc_out_msat": 100_000}]}}),
                "listchannels" => json!({"jsonrpc": "2.0", "id": id,
                    "result": {"channels": [{"source": format!("02{}", "11".repeat(32)),
                        "destination": peer_a(), "active": true,
                        "fee_per_millionth": 42, "satoshis": 1_000_000_i64,
                        "last_update": 1_800_000_000_i64, "base_fee_millisatoshi": 0}]}}),
                "feerates" => json!({"jsonrpc": "2.0", "id": id,
                    "result": {"perkb": {"opening": 15000}}}),
                "listconfigs" => match &listconfigs {
                    Some(configs) => json!({"jsonrpc": "2.0", "id": id,
                        "result": {"configs": configs}}),
                    None => json!({"jsonrpc": "2.0", "id": id,
                        "error": {"code": -32603, "message": "listconfigs unavailable"}}),
                },
                other => json!({"jsonrpc": "2.0", "id": id,
                    "error": {"code": -32601, "message": format!("unknown method {other}")}}),
            };
            let mut out = serde_json::to_vec(&body).unwrap();
            out.extend_from_slice(b"\n\n");
            let _ = stream.write_all(&out).await;
        }
    });
}

/// F6 (config half): a config store whose override queries FAIL (the
/// table is unreadable -- not merely absent rows) must produce a typed
/// preparation REFUSAL, never a `Ready` preparation silently resolved on
/// struct defaults. A refusal becomes a durable receipt with zero
/// decision/RNG/state; a defaults-decision would be a silently wrong fee.
#[tokio::test]
async fn config_query_failure_refuses_new_channel_preparation_instead_of_defaulting() {
    use revops::fee_scheduler::{prepare_new_channel, NewChannelPreparation};

    let fx = fixture();
    let socket = fx._dir.path().join("lightning-rpc");
    serve_new_channel_rpc(socket.clone(), Some(json!({})));

    // A valid sqlite file WITHOUT `config_overrides` (one unrelated table
    // so the open-time schema probe passes): every layer-(a) override
    // query then errors with "no such table" -- a QUERY FAILURE, distinct
    // from the legitimate no-override `None`.
    let broken_cfg_db = fx._dir.path().join("broken-config.db");
    {
        let conn = Connection::open(&broken_cfg_db).unwrap();
        conn.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)", [])
            .unwrap();
    }
    let handle = revops_db::actor::spawn_read_only(&broken_cfg_db)
        .await
        .expect("open probe passes; only the override queries fail");

    let signal = revops::notify::NewChannelSignal {
        event_scid: Some("100x1x0".to_string()),
        event_channel_id: None,
        peer_id: peer_a(),
        old_state: "OPENINGD".to_string(),
        new_state: "CHANNELD_NORMAL".to_string(),
        event_ts: NOW,
    };
    let python_options = revops::config_resolve::PythonOptionCache::empty();
    let preparation =
        prepare_new_channel(&socket, &fx.db_path, Some(&handle), &python_options, signal).await;

    match preparation {
        NewChannelPreparation::Refused { reason, .. } => {
            assert!(
                reason.contains("config"),
                "the refusal must name the config resolution failure, got: {reason}"
            );
        }
        NewChannelPreparation::Ready(prepared) => panic!(
            "a config-store query failure must REFUSE, not resolve defaults and decide \
             (got Ready with cfg.max_fee_ppm={})",
            prepared.cfg.max_fee_ppm
        ),
    }
}

/// Live-review finding F8 (A3 config freshness): the preparation must
/// resolve against a FRESH `listconfigs` snapshot, not whatever the
/// shared cache last saw -- a dynamic `setconfig` between the last
/// scheduled cycle and the channel opening must reach the initial-fee
/// decision, as it would reach Python's `_refresh_dynamic_config`-driven
/// handler.
#[tokio::test]
async fn a3_preparation_uses_a_fresh_listconfigs_value_not_the_stale_cache() {
    use revops::fee_scheduler::{prepare_new_channel, NewChannelPreparation};

    let fx = fixture();
    // Prime the cache with a STALE value over a first mock socket.
    let stale_socket = fx._dir.path().join("stale-rpc");
    serve_new_channel_rpc(
        stale_socket.clone(),
        Some(json!({"revenue-ops-max-fee-ppm": {"value_str": "555", "source": "setconfig"}})),
    );
    let python_options = revops::config_resolve::PythonOptionCache::empty();
    assert!(python_options.refresh(&stale_socket).await);

    // The live socket now reports the NEW value (a setconfig happened).
    let socket = fx._dir.path().join("lightning-rpc");
    serve_new_channel_rpc(
        socket.clone(),
        Some(json!({"revenue-ops-max-fee-ppm": {"value_str": "777", "source": "setconfig"}})),
    );

    let signal = revops::notify::NewChannelSignal {
        event_scid: Some("100x1x0".to_string()),
        event_channel_id: None,
        peer_id: peer_a(),
        old_state: "OPENINGD".to_string(),
        new_state: "CHANNELD_NORMAL".to_string(),
        event_ts: NOW,
    };
    let preparation =
        prepare_new_channel(&socket, &fx.db_path, None, &python_options, signal).await;

    match preparation {
        NewChannelPreparation::Ready(prepared) => assert_eq!(
            prepared.cfg.max_fee_ppm, 777,
            "the preparation must decide on the FRESH listconfigs value, not the stale cache"
        ),
        NewChannelPreparation::Refused { reason, .. } => {
            panic!("expected Ready with the fresh value, got refusal: {reason}")
        }
    }
}

/// Live-review finding F8 (strict half): a failed `listconfigs` refresh
/// must REFUSE the preparation -- even though a stale cached snapshot
/// exists -- because deciding an initial fee on stale config is a silent
/// wrong decision. The shared scheduled-cycle path keeps its
/// keep-last-good posture; only A3 is strict.
#[tokio::test]
async fn a3_preparation_refuses_when_the_listconfigs_refresh_fails() {
    use revops::fee_scheduler::{prepare_new_channel, NewChannelPreparation};

    let fx = fixture();
    // A stale cached snapshot EXISTS (the tempting fallback).
    let stale_socket = fx._dir.path().join("stale-rpc");
    serve_new_channel_rpc(
        stale_socket.clone(),
        Some(json!({"revenue-ops-max-fee-ppm": {"value_str": "555", "source": "setconfig"}})),
    );
    let python_options = revops::config_resolve::PythonOptionCache::empty();
    assert!(python_options.refresh(&stale_socket).await);

    // The live socket refuses listconfigs (outage) but would serve
    // everything else.
    let socket = fx._dir.path().join("lightning-rpc");
    serve_new_channel_rpc(socket.clone(), None);

    let signal = revops::notify::NewChannelSignal {
        event_scid: Some("100x1x0".to_string()),
        event_channel_id: None,
        peer_id: peer_a(),
        old_state: "OPENINGD".to_string(),
        new_state: "CHANNELD_NORMAL".to_string(),
        event_ts: NOW,
    };
    let preparation =
        prepare_new_channel(&socket, &fx.db_path, None, &python_options, signal).await;

    match preparation {
        NewChannelPreparation::Refused { reason, .. } => assert!(
            reason.contains("refresh"),
            "the refusal must name the failed config refresh, got: {reason}"
        ),
        NewChannelPreparation::Ready(prepared) => panic!(
            "a failed listconfigs refresh must REFUSE, not decide on the stale cached \
             config (got Ready with cfg.max_fee_ppm={})",
            prepared.cfg.max_fee_ppm
        ),
    }
}
