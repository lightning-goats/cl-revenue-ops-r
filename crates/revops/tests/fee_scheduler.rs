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
    CycleOutcome, CycleOwner, PreparedCycle, SchedulerConfig, StateLifecycle,
};
use revops::fee_state::STATE_JOURNAL_FILE_NAME;
use revops_fees::cycle::FeeCfgSnapshot;
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
    // precedes the min-competitors gate).
    let outcome = owner.run_cycle(prepared(json!(2), false), &mut clock);
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
// T1 fail-closed rule: min competitors must equal the baked 3
// ---------------------------------------------------------------------------

#[test]
fn cycle_skips_and_logs_when_min_competitors_not_3() {
    let fx = fixture();
    seed_channel_state(&fx.db_path);
    let mut owner = owner(&fx, StateLifecycle::RehydratePerCycle);
    let mut clock = || NOW;

    for wrong in [json!(2), json!(4), json!("3"), Value::Null] {
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

    // The exact resolved value 3 passes.
    let outcome = owner.run_cycle(prepared(json!(3), true), &mut clock);
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
