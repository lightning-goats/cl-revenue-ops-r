//! Tests for Phase 4b Task 4: re-hydrate-per-cycle state lifecycle
//! (`revops::fee_state::rehydrate`) + the dry-run-safe `JournalStateSink`.
//!
//! Design Note 1 (`docs/superpowers/plans/2026-07-17-phase4b-wiring.md`):
//! both controllers start every cycle from Python's persisted
//! `v2_state_json` flush, so `rehydrate` REPLACES `cycle_states`/
//! `fee_states` wholesale from the DB every cycle while PRESERVING the
//! process-lifetime `vegas`/`vegas_wake_armed`/`last_decision_summary`
//! fields (Python keeps those as module globals, not in `v2_state_json`).
//!
//! `JournalStateSink` never touches the production DB — see the module doc
//! comment on `revops::fee_state` for the hard rule (Global Constraints:
//! "any new write target must be a Rust-owned file next to
//! `revops-r-observer.db`").

use revops::fee_state::{rehydrate, JournalStateSink};
use revops_fees::cycle::{ChannelCycleState, ChannelFeeState, ControllerState, StateSink};
use revops_fees::pyjson::dumps_python;
use revops_fees::state_store::{
    fee_state_to_v2_dict, load_cycle_state, load_fee_state, parse_v2_blob,
    serialize_cycle_state_payload, FeeStrategyRow,
};
use rusqlite::Connection;
use serde_json::Value as JsonValue;
use std::path::PathBuf;

/// Minimal `fee_strategy_state` schema (see `fixtures/schema.sql`) — just
/// the columns `read_fee_strategy_rows` selects.
fn create_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE fee_strategy_state (
            channel_id TEXT PRIMARY KEY,
            last_revenue_rate REAL NOT NULL DEFAULT 0.0,
            last_fee_ppm INTEGER NOT NULL DEFAULT 0,
            trend_direction INTEGER NOT NULL DEFAULT 1,
            step_ppm INTEGER NOT NULL DEFAULT 50,
            consecutive_same_direction INTEGER NOT NULL DEFAULT 0,
            last_update INTEGER NOT NULL DEFAULT 0,
            last_broadcast_fee_ppm INTEGER NOT NULL DEFAULT 0,
            is_sleeping INTEGER NOT NULL DEFAULT 0,
            sleep_until INTEGER NOT NULL DEFAULT 0,
            stable_cycles INTEGER NOT NULL DEFAULT 0,
            last_state TEXT DEFAULT 'balanced',
            forward_count_since_update INTEGER DEFAULT 0,
            last_volume_sats INTEGER DEFAULT 0,
            v2_state_json TEXT DEFAULT '{}'
        );",
    )
    .expect("create fee_strategy_state table");
}

fn insert_row(conn: &Connection, row: &FeeStrategyRow) {
    conn.execute(
        "INSERT INTO fee_strategy_state (
            channel_id, last_revenue_rate, last_fee_ppm, trend_direction, step_ppm,
            consecutive_same_direction, last_update, last_broadcast_fee_ppm,
            is_sleeping, sleep_until, stable_cycles, forward_count_since_update,
            last_volume_sats, last_state, v2_state_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        rusqlite::params![
            row.channel_id,
            row.last_revenue_rate,
            row.last_fee_ppm,
            row.trend_direction,
            row.step_ppm,
            row.consecutive_same_direction,
            row.last_update,
            row.last_broadcast_fee_ppm,
            row.is_sleeping as i64,
            row.sleep_until,
            row.stable_cycles,
            row.forward_count_since_update,
            row.last_volume_sats,
            row.last_state,
            row.v2_state_json,
        ],
    )
    .expect("insert fee_strategy_state row");
}

fn sample_row(channel_id: &str) -> FeeStrategyRow {
    FeeStrategyRow {
        channel_id: channel_id.to_string(),
        v2_state_json: "{}".to_string(),
        ..FeeStrategyRow::default()
    }
}

fn set_last_update(conn: &Connection, channel_id: &str, last_update: i64) {
    conn.execute(
        "UPDATE fee_strategy_state SET last_update = ?1 WHERE channel_id = ?2",
        rusqlite::params![last_update, channel_id],
    )
    .expect("update last_update");
}

/// Phase 4b Task 8b (Design Note 1 addendum): `rehydrate` retains, per
/// channel, the `last_update`/`is_sleeping` it hydrated at the top of the
/// PREVIOUS triggered cycle in `skip_gate_prev` -- the pre-decision epoch
/// Python's current-cycle skip gate was conditioned on -- while
/// `skip_gate_seen` tracks THIS cycle's fresh hydration for promotion next
/// cycle. The FIRST cycle has no prior (bootstrap).
#[test]
fn rehydrate_promotes_previous_cycle_hydration_into_skip_gate_prev() {
    let conn = Connection::open_in_memory().unwrap();
    create_schema(&conn);
    let mut row = sample_row("chan_a");
    row.last_update = 1_000_000;
    insert_row(&conn, &row);

    let mut state = ControllerState::new();

    // Cycle 1 (bootstrap): no cached prior yet; this cycle's fresh
    // last_update (T0) is recorded as `seen`, not yet in `prev`.
    rehydrate(&mut state, &conn);
    assert!(
        !state.skip_gate_prev.contains_key("chan_a"),
        "first triggered cycle has NO cached pre-decision epoch (bootstrap)"
    );
    assert_eq!(
        state.skip_gate_seen.get("chan_a").map(|e| e.last_update),
        Some(1_000_000),
        "cycle 1's fresh hydration is recorded for promotion next cycle"
    );

    // Python advances its flush (end of its next cycle): last_update -> T1.
    set_last_update(&conn, "chan_a", 1_000_500);

    // Cycle 2: the gate must now read T0 (cycle 1's fresh hydration = the
    // epoch Python's cycle-2 decision was conditioned on), NOT the freshly
    // hydrated T1.
    rehydrate(&mut state, &conn);
    assert_eq!(
        state.skip_gate_prev.get("chan_a").map(|e| e.last_update),
        Some(1_000_000),
        "skip_gate_prev is the PREVIOUS cycle's fresh hydration (Python's pre-decision epoch)"
    );
    assert_eq!(
        state.skip_gate_seen.get("chan_a").map(|e| e.last_update),
        Some(1_000_500),
        "skip_gate_seen advances to this cycle's fresh hydration"
    );
    assert_eq!(
        state.cycle_states.get("chan_a").map(|c| c.last_update),
        Some(1_000_500),
        "live cycle_states still holds the freshly rehydrated value (everything else uses it)"
    );
}

/// Phase 4b Task 8b, T6b COALESCING watch item: if two Python flushes merge
/// into one Rust cycle (settle window), `skip_gate_prev` holds Rust's OWN
/// last-observed hydration, NOT the intermediate flush Rust never saw. The
/// cached epoch is then one Python-cycle stale (biases pre_hours_elapsed
/// upward -> more permissive). This pins the documented behavior honestly.
#[test]
fn rehydrate_coalescing_caches_own_prior_observation_not_missed_flush() {
    let conn = Connection::open_in_memory().unwrap();
    create_schema(&conn);
    let mut row = sample_row("chan_a");
    row.last_update = 1_000_000; // T0
    insert_row(&conn, &row);

    let mut state = ControllerState::new();
    rehydrate(&mut state, &conn); // Rust cycle 1 observes T0

    // Python runs TWO cycles before Rust's settle window fires: it writes T1
    // (1_000_400) then T2 (1_000_800). Rust never hydrates the intermediate
    // T1 -- both flush-marker advances coalesce into ONE Rust cycle that
    // sees only the latest, T2.
    set_last_update(&conn, "chan_a", 1_000_800);
    rehydrate(&mut state, &conn); // Rust coalesced cycle 2 observes T2

    assert_eq!(
        state.skip_gate_prev.get("chan_a").map(|e| e.last_update),
        Some(1_000_000),
        "coalesced cycle's cached epoch is Rust's OWN prior observation (T0), \
         NOT Python's true pre-decision epoch (T1) it never saw -- stale-by-one \
         (documented window watch item, biases toward evaluate)"
    );
}

#[test]
fn rehydrate_replaces_channel_maps_and_preserves_vegas() {
    let conn = Connection::open_in_memory().unwrap();
    create_schema(&conn);
    insert_row(&conn, &sample_row("chan_a"));
    insert_row(&conn, &sample_row("chan_b"));

    let mut state = ControllerState::new();
    // A stale channel not present in the DB must be dropped on rehydrate.
    state
        .cycle_states
        .insert("stale_chan".to_string(), ChannelCycleState::default());
    state
        .fee_states
        .insert("stale_chan".to_string(), ChannelFeeState::default());
    // Process-lifetime fields (Python module globals) must survive.
    state.vegas.intensity = 0.77;
    state.vegas.last_sat_vb = 42.0;
    state.vegas_wake_armed = false;
    state.last_decision_summary.action = "poisoned".to_string();

    rehydrate(&mut state, &conn);

    assert!(state.cycle_states.contains_key("chan_a"));
    assert!(state.cycle_states.contains_key("chan_b"));
    assert!(
        !state.cycle_states.contains_key("stale_chan"),
        "stale channel absent from the DB must be dropped by rehydrate"
    );
    assert!(state.fee_states.contains_key("chan_a"));
    assert!(state.fee_states.contains_key("chan_b"));
    assert!(!state.fee_states.contains_key("stale_chan"));

    assert_eq!(
        state.vegas.intensity, 0.77,
        "vegas state is process-lifetime, not part of v2_state_json — rehydrate must not touch it"
    );
    assert_eq!(state.vegas.last_sat_vb, 42.0);
    assert!(
        !state.vegas_wake_armed,
        "vegas_wake_armed is process-lifetime — rehydrate must not touch it"
    );
    assert_eq!(
        state.last_decision_summary.action, "poisoned",
        "last_decision_summary must be preserved across rehydration"
    );
}

fn fixture_blobs() -> JsonValue {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/fees/state_roundtrip/blobs.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn row_from_fixture(v: &JsonValue) -> FeeStrategyRow {
    FeeStrategyRow {
        channel_id: v["channel_id"].as_str().unwrap().to_string(),
        last_revenue_rate: v["last_revenue_rate"].as_f64().unwrap(),
        last_fee_ppm: v["last_fee_ppm"].as_i64().unwrap(),
        trend_direction: v["trend_direction"].as_i64().unwrap(),
        step_ppm: v["step_ppm"].as_i64().unwrap(),
        consecutive_same_direction: v["consecutive_same_direction"].as_i64().unwrap(),
        last_update: v["last_update"].as_i64().unwrap(),
        last_broadcast_fee_ppm: v["last_broadcast_fee_ppm"].as_i64().unwrap(),
        is_sleeping: v["is_sleeping"].as_i64().unwrap_or(0) != 0,
        sleep_until: v["sleep_until"].as_i64().unwrap(),
        stable_cycles: v["stable_cycles"].as_i64().unwrap(),
        forward_count_since_update: v["forward_count_since_update"].as_i64().unwrap(),
        last_volume_sats: v["last_volume_sats"].as_i64().unwrap(),
        last_state: v["last_state"].as_str().unwrap().to_string(),
        v2_state_json: String::new(),
    }
}

/// T9's production gate proved `state_store`'s load path byte-exact over
/// 40/40 real blobs; this test proves `rehydrate` (the DB-plumbing wrapper
/// this task adds) doesn't lose or alter anything relative to calling
/// `parse_v2_blob`/`load_fee_state`/`load_cycle_state` directly over a
/// committed T9 fixture blob (`fixtures/fees/state_roundtrip/blobs.json`,
/// case `current_nested_layout`).
#[test]
fn rehydrate_round_trips_t9_fixture_blob_byte_identically() {
    let fixture = fixture_blobs();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "current_nested_layout")
        .expect("current_nested_layout case present in blobs.json");
    let channel_id = case["channel_id"].as_str().unwrap();
    let input_blob = case["input_blob"].as_str().unwrap();

    let mut row = row_from_fixture(&case["row"]);
    row.v2_state_json = input_blob.to_string();

    // Direct path — no DB involved — is the byte-exact truth to compare
    // against (this is exactly what T9's production gate already pinned).
    let direct_env = parse_v2_blob(input_blob, &row);
    let direct_fee = load_fee_state(&direct_env, &row);
    let direct_cycle = load_cycle_state(&direct_env, &row);
    let direct_fee_bytes = dumps_python(&fee_state_to_v2_dict(&direct_fee));
    let direct_cycle_bytes = dumps_python(&serialize_cycle_state_payload(&direct_cycle));

    // DB-backed path via rehydrate.
    let conn = Connection::open_in_memory().unwrap();
    create_schema(&conn);
    insert_row(&conn, &row);
    let mut state = ControllerState::new();
    rehydrate(&mut state, &conn);

    let hydrated_fee = state
        .fee_states
        .get(channel_id)
        .expect("fee state hydrated from DB");
    let hydrated_cycle = state
        .cycle_states
        .get(channel_id)
        .expect("cycle state hydrated from DB");
    let hydrated_fee_bytes = dumps_python(&fee_state_to_v2_dict(hydrated_fee));
    let hydrated_cycle_bytes = dumps_python(&serialize_cycle_state_payload(hydrated_cycle));

    assert_eq!(
        hydrated_fee_bytes, direct_fee_bytes,
        "fee_state must round-trip byte-identically via rehydrate"
    );
    assert_eq!(
        hydrated_cycle_bytes, direct_cycle_bytes,
        "cycle_state must round-trip byte-identically via rehydrate"
    );
}

#[test]
fn journal_state_sink_writes_one_line_per_row_and_never_opens_production_db() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = JournalStateSink::open_dir(tmp.path()).expect("open journal state sink dir");

    let mut cycle_a = ChannelCycleState::default();
    cycle_a.last_fee_ppm = 111;
    let mut fee_a = ChannelFeeState::default();
    fee_a.last_fee_ppm = 111;

    let mut cycle_b = ChannelCycleState::default();
    cycle_b.last_fee_ppm = 222;
    let mut fee_b = ChannelFeeState::default();
    fee_b.last_fee_ppm = 222;

    sink.flush_batch(&[
        ("chan_a".to_string(), cycle_a, fee_a),
        ("chan_b".to_string(), cycle_b, fee_b),
    ])
    .expect("state journal flush");

    // JournalStateSink's only state is a Rust-owned file path — it never
    // holds or opens a `rusqlite::Connection` at all, structurally
    // guaranteeing it can't reach the production DB.
    let journal_path = tmp.path().join("fee_dryrun_state.jsonl");
    let contents = std::fs::read_to_string(&journal_path).expect("journal file written");
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(lines.len(), 2, "one JSONL line per flushed row");

    for line in &lines {
        let parsed: JsonValue = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line not valid JSON: {e}: {line}"));
        assert!(
            parsed.get("channel_id").is_some(),
            "line missing channel_id: {line}"
        );
        let v2 = parsed["v2_state_json"]
            .as_str()
            .expect("v2_state_json must be a string");
        let v2_parsed: JsonValue =
            serde_json::from_str(v2).expect("v2_state_json must itself be valid JSON");
        assert!(v2_parsed.get("fee_state").is_some());
        assert!(v2_parsed.get("cycle_state").is_some());
    }
    assert!(lines[0].contains("chan_a"));
    assert!(lines[1].contains("chan_b"));
}

#[test]
fn journal_state_sink_staging_failure_preserves_prior_complete_artifact() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().unwrap();
    let sink = JournalStateSink::open_dir(tmp.path()).expect("open journal state sink dir");

    let journal_path = tmp.path().join("fee_dryrun_state.jsonl");
    let prior = "{\"prior\":\"complete\"}\n";
    std::fs::write(&journal_path, prior).expect("seed prior complete artifact");

    let mut perms = std::fs::metadata(tmp.path()).unwrap().permissions();
    perms.set_mode(0o500);
    std::fs::set_permissions(tmp.path(), perms).unwrap();

    let result = sink.flush_batch(&[(
        "chan_a".to_string(),
        ChannelCycleState::default(),
        ChannelFeeState::default(),
    )]);

    let mut perms = std::fs::metadata(tmp.path()).unwrap().permissions();
    perms.set_mode(0o700);
    std::fs::set_permissions(tmp.path(), perms).unwrap();

    let error = result.expect_err("staging failure must be returned");
    assert!(error.to_string().contains("staging open"), "{error}");
    assert_eq!(
        std::fs::read_to_string(&journal_path).expect("prior artifact remains readable"),
        prior,
        "failed atomic replacement must leave the prior complete artifact intact"
    );
    let staging: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with(".fee_dryrun_state.jsonl.") && name.ends_with(".tmp")
        })
        .collect();
    assert!(
        staging.is_empty(),
        "failed flush left staging files: {staging:?}"
    );
}

/// Reviewer finding (Minor): proves `flush_batch` appends (never
/// truncates/overwrites) across repeated calls, matching "ONE flush per
/// cycle" semantics over multiple dry-run cycles.
#[test]
fn journal_state_sink_flush_batch_appends_across_multiple_cycles() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = JournalStateSink::open_dir(tmp.path()).expect("open journal state sink dir");

    let mut cycle1 = ChannelCycleState::default();
    cycle1.last_fee_ppm = 100;
    let mut fee1 = ChannelFeeState::default();
    fee1.last_fee_ppm = 100;
    sink.flush_batch(&[("chan_a".to_string(), cycle1, fee1)])
        .expect("first state journal flush");

    let mut cycle2 = ChannelCycleState::default();
    cycle2.last_fee_ppm = 200;
    let mut fee2 = ChannelFeeState::default();
    fee2.last_fee_ppm = 200;
    sink.flush_batch(&[("chan_a".to_string(), cycle2, fee2)])
        .expect("second state journal flush");

    let journal_path = tmp.path().join("fee_dryrun_state.jsonl");
    let contents = std::fs::read_to_string(&journal_path).expect("journal file written");
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "each cycle's flush_batch call must append a new line, not overwrite the previous one"
    );
    assert!(lines[0].contains("100"), "first cycle's row: {}", lines[0]);
    assert!(lines[1].contains("200"), "second cycle's row: {}", lines[1]);
}

/// Reviewer finding (Minor): `rows.is_empty()` early return must not
/// create or otherwise touch the journal file at all.
#[test]
fn journal_state_sink_flush_batch_empty_rows_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let sink = JournalStateSink::open_dir(tmp.path()).expect("open journal state sink dir");

    sink.flush_batch(&[]).expect("empty state journal flush");

    let journal_path = tmp.path().join("fee_dryrun_state.jsonl");
    assert!(
        !journal_path.exists(),
        "empty rows must not create/touch the journal file"
    );
}

#[cfg(unix)]
#[test]
fn journal_state_sink_does_not_follow_preexisting_legacy_staging_symlink() {
    use std::os::unix::fs::symlink;

    let tmp = tempfile::tempdir().unwrap();
    let sink = JournalStateSink::open_dir(tmp.path()).expect("open journal state sink dir");

    let victim_path = tmp.path().join("unrelated-operator-file");
    let victim_contents = "must remain intact\n";
    std::fs::write(&victim_path, victim_contents).expect("seed unrelated target");

    let legacy_staging_path = tmp.path().join("fee_dryrun_state.jsonl.tmp");
    symlink(&victim_path, &legacy_staging_path).expect("seed legacy staging symlink");

    sink.flush_batch(&[(
        "chan_a".to_string(),
        ChannelCycleState::default(),
        ChannelFeeState::default(),
    )])
    .expect("journal flush must ignore unrelated legacy staging entry");

    assert_eq!(
        std::fs::read_to_string(&victim_path).expect("unrelated target remains readable"),
        victim_contents,
        "flush must never follow or truncate a pre-existing staging symlink"
    );
}

#[test]
fn journal_state_sink_separate_instances_serialize_concurrent_appends() {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Barrier};

    const WRITERS: usize = 16;
    let tmp = tempfile::tempdir().unwrap();
    let dir = Arc::new(tmp.path().to_path_buf());
    let barrier = Arc::new(Barrier::new(WRITERS));
    let mut threads = Vec::new();

    for index in 0..WRITERS {
        let dir = Arc::clone(&dir);
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            let sink = JournalStateSink::open_dir(&dir).expect("open separate sink instance");
            let mut cycle = ChannelCycleState::default();
            cycle.last_fee_ppm = index as i64;
            let mut fee = ChannelFeeState::default();
            fee.last_fee_ppm = index as i64;
            barrier.wait();
            sink.flush_batch(&[(format!("chan_{index}"), cycle, fee)])
                .expect("serialized concurrent flush");
        }));
    }
    for thread in threads {
        thread.join().expect("writer thread");
    }

    let journal =
        std::fs::read_to_string(dir.join("fee_dryrun_state.jsonl")).expect("concurrent journal");
    let ids: BTreeSet<String> = journal
        .lines()
        .map(|line| {
            serde_json::from_str::<JsonValue>(line).expect("valid JSON")["channel_id"]
                .as_str()
                .expect("channel id")
                .to_string()
        })
        .collect();
    let expected: BTreeSet<String> = (0..WRITERS).map(|i| format!("chan_{i}")).collect();
    assert_eq!(ids, expected);
    assert_eq!(journal.lines().count(), WRITERS);
}

#[cfg(unix)]
#[test]
fn journal_state_sink_preserves_restrictive_permissions() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let tmp = tempfile::tempdir().unwrap();
    let sink = JournalStateSink::open_dir(tmp.path()).expect("open journal state sink dir");
    sink.flush_batch(&[(
        "first".to_string(),
        ChannelCycleState::default(),
        ChannelFeeState::default(),
    )])
    .expect("initial flush");

    let journal = tmp.path().join("fee_dryrun_state.jsonl");
    std::fs::set_permissions(&journal, std::fs::Permissions::from_mode(0o600))
        .expect("tighten journal mode");
    sink.flush_batch(&[(
        "second".to_string(),
        ChannelCycleState::default(),
        ChannelFeeState::default(),
    )])
    .expect("replacement flush");

    assert_eq!(
        std::fs::metadata(&journal).unwrap().mode() & 0o777,
        0o600,
        "atomic replacement must preserve an operator-tightened mode"
    );
}

// ---------------------------------------------------------------------------
// Task 5 (stateful-shadow plan): restart-persistent SeedOnce -- the
// fail-closed one-time seed from Python's `fee_strategy_state` (revision
// plan Task R6) and strict hydration from Rust-owned rows.
// ---------------------------------------------------------------------------

use revops::fee_state::{
    rehydrate_from_rows, seed_once_from_python, seed_payload_sha256, serialize_state_envelope,
    HydrationSource, SeedOutcome,
};
use revops_db::fee_runway::FeeStateRow;

const SEED_NOW: i64 = 1_800_000_000;

/// A v2 blob whose `fee_state.thompson_state` is the given JSON value --
/// `algorithm_version` is a KNOWN version, so Python's `from_v2_dict`
/// WOULD call `GaussianThompsonState.from_dict` on it (py 2174-2183).
fn blob_with_thompson(thompson: serde_json::Value) -> String {
    serde_json::json!({
        "fee_state": {
            "algorithm_version": "dts_pid_v1",
            "thompson_state": thompson,
        },
        "cycle_state": {},
    })
    .to_string()
}

fn valid_thompson() -> serde_json::Value {
    serde_json::json!({
        "weight_scheme": "exposure_v2",
        "posterior_mean": 250.0,
        "posterior_std": 80.0,
        "_last_fee_min": 10.0,
        "_last_fee_max": 900.0,
        "posterior_precision": [[0.01, 0.0, 0.0], [0.0, 0.01, 0.0], [0.0, 0.0, 0.01]],
        "posterior_bias": [[100.0, 0.5, 1_700_000_000_i64]],
    })
}

fn seed_fixture_conn(rows: &[(&str, String, i64)]) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    create_schema(&conn);
    for (channel_id, blob, last_update) in rows {
        let mut row = sample_row(channel_id);
        row.v2_state_json = blob.clone();
        row.last_update = *last_update;
        insert_row(&conn, &row);
    }
    conn
}

/// Runs the seed against one corrupt channel plus one VALID channel and
/// asserts the WHOLE seed is refused: no partial import, state untouched,
/// and the refusal names the corrupt channel + field.
fn assert_whole_seed_refused(corrupt_thompson: serde_json::Value, expect_field_fragment: &str) {
    let conn = seed_fixture_conn(&[
        (
            "chan_bad",
            blob_with_thompson(corrupt_thompson),
            SEED_NOW - 100,
        ),
        (
            "chan_good",
            blob_with_thompson(valid_thompson()),
            SEED_NOW - 50,
        ),
    ]);
    let mut state = ControllerState::new();
    let outcome = seed_once_from_python(
        &mut state,
        &conn,
        "/prod/revenue_ops.db",
        SEED_NOW,
        "649c320",
    );
    let event = match outcome {
        SeedOutcome::Refused(event) => event,
        SeedOutcome::Seeded(event) => {
            panic!("seed must be refused, but imported: {event:?}")
        }
    };
    assert_eq!(event.outcome, "seed_refused");
    assert_eq!(event.refused_channel.as_deref(), Some("chan_bad"));
    let field = event.refused_field.as_deref().unwrap_or_default();
    assert!(
        field.contains(expect_field_fragment),
        "refused_field {field:?} must name {expect_field_fragment:?}"
    );
    assert!(
        state.fee_states.is_empty() && state.cycle_states.is_empty(),
        "a refused seed must leave the controller state untouched (no partial import)"
    );
    // Provenance is still recorded on refusal (auditable evidence).
    assert_eq!(event.row_count, 2);
    assert_eq!(event.source_db_path, "/prod/revenue_ops.db");
    assert_eq!(event.seeded_at, SEED_NOW);
}

/// Task R6 refusal class 1: non-numeric `_last_fee_min` -- Python's
/// `float(d.get("_last_fee_min", 0.0))` (py 1873) has no try/except and
/// raises `ValueError`; Rust's total parity path would silently default to
/// 0.0, so the seed must refuse instead.
#[test]
fn seed_once_refuses_non_numeric_last_fee_min() {
    let mut thompson = valid_thompson();
    thompson["_last_fee_min"] = serde_json::json!("not-a-number");
    assert_whole_seed_refused(thompson, "_last_fee_min");
}

/// Task R6 refusal class 2: a string row inside `posterior_precision` --
/// Python's L5 guard `all(len(r) == 3 ...)` passes for a 3-char string,
/// then `raw_prec[i][i] > 0` compares a `str` to `int` and raises
/// `TypeError`; Rust's `parse_m3_pd` would silently fall back to the
/// default matrix.
#[test]
fn seed_once_refuses_string_rows_in_posterior_precision() {
    let mut thompson = valid_thompson();
    thompson["posterior_precision"] =
        serde_json::json!(["abc", [0.0, 0.01, 0.0], [0.0, 0.0, 0.01]]);
    assert_whole_seed_refused(thompson, "posterior_precision");
}

/// Task R6 refusal class 3: a dict entry inside `posterior_bias` --
/// Python's restore loop catches `TypeError`/`ValueError`/`IndexError`
/// but NOT `KeyError`, and `entry[0]` on a dict raises `KeyError`; Rust's
/// `convert_posterior_bias` would silently skip the entry.
#[test]
fn seed_once_refuses_dict_entries_in_posterior_bias() {
    let mut thompson = valid_thompson();
    thompson["posterior_bias"] = serde_json::json!([{ "target_fee": 100.0 }]);
    assert_whole_seed_refused(thompson, "posterior_bias");
}

/// Python's `float()` accepts numeric STRINGS (`float("1.5")` == 1.5), and
/// the Rust parity path (`try_float`) parses them identically -- a numeric
/// string must NOT trigger a spurious refusal.
#[test]
fn seed_once_accepts_numeric_string_scalars() {
    let mut thompson = valid_thompson();
    thompson["_last_fee_min"] = serde_json::json!("1.5");
    let conn = seed_fixture_conn(&[("chan_a", blob_with_thompson(thompson), SEED_NOW - 10)]);
    let mut state = ControllerState::new();
    let outcome = seed_once_from_python(
        &mut state,
        &conn,
        "/prod/revenue_ops.db",
        SEED_NOW,
        "649c320",
    );
    assert!(
        matches!(outcome, SeedOutcome::Seeded(_)),
        "numeric strings are Python-float()-accepted and must seed: {outcome:?}"
    );
    assert!(state.fee_states.contains_key("chan_a"));
}

/// Task R6 provenance: a successful seed records source DB path,
/// MAX(last_update) at seed time, row count, the sha256 of the serialized
/// seed payload, and the binary source commit -- and hydrates the state
/// byte-identically to the plain `rehydrate` path.
#[test]
fn seed_once_records_provenance_readback() {
    let conn = seed_fixture_conn(&[
        (
            "chan_a",
            blob_with_thompson(valid_thompson()),
            SEED_NOW - 500,
        ),
        (
            "chan_b",
            blob_with_thompson(valid_thompson()),
            SEED_NOW - 100,
        ),
    ]);
    let mut state = ControllerState::new();
    let outcome = seed_once_from_python(
        &mut state,
        &conn,
        "/prod/revenue_ops.db",
        SEED_NOW,
        "649c320",
    );
    let event = match outcome {
        SeedOutcome::Seeded(event) => event,
        SeedOutcome::Refused(event) => panic!("valid rows must seed, got refusal: {event:?}"),
    };
    assert_eq!(event.outcome, "seeded");
    assert_eq!(event.source_db_path, "/prod/revenue_ops.db");
    assert_eq!(event.source_max_last_update, SEED_NOW - 100);
    assert_eq!(event.row_count, 2);
    assert_eq!(event.source_commit, "649c320");
    assert_eq!(event.seeded_at, SEED_NOW);
    assert!(event.refused_channel.is_none() && event.refused_field.is_none());

    // The payload hash is the canonical serialization's sha256: 64 hex
    // chars, deterministic over the same rows, sensitive to any change.
    let rows = revops_fees::state_store::read_fee_strategy_rows(&conn);
    assert_eq!(event.payload_sha256, seed_payload_sha256(&rows));
    assert_eq!(event.payload_sha256.len(), 64);
    assert!(event.payload_sha256.chars().all(|c| c.is_ascii_hexdigit()));
    let mut altered = rows.clone();
    altered[0].v2_state_json.push(' ');
    assert_ne!(event.payload_sha256, seed_payload_sha256(&altered));

    // Round-trip through the Rust-owned store and read it back. Task 42:
    // successful seed provenance has no standalone write path -- it rides
    // the atomic generation-1 commit (`FeeCycleCommit::pending_seed`).
    let store = Connection::open_in_memory().unwrap();
    revops_db::notifications::init_schema(&store).unwrap();
    let commit = revops_db::fee_runway::FeeCycleCommit {
        cycle_id: "seed-roundtrip-cycle".to_string(),
        started_at: event.seeded_at,
        completed_at: event.seeded_at,
        source_commit: event.source_commit.clone(),
        binary_sha256: "0".repeat(64),
        pending_seed: Some(event.clone()),
        ..Default::default()
    };
    assert_eq!(
        revops_db::fee_runway::commit_fee_cycle(&store, &commit).unwrap(),
        1,
        "seed provenance commits with generation 1"
    );
    let read = revops_db::fee_runway::latest_seed_event(&store)
        .unwrap()
        .expect("seed event persisted");
    assert_eq!(read, event);

    // Hydration matches the plain rehydrate path exactly.
    let mut direct = ControllerState::new();
    rehydrate(&mut direct, &conn);
    for chan in ["chan_a", "chan_b"] {
        assert_eq!(
            dumps_python(&fee_state_to_v2_dict(&state.fee_states[chan])),
            dumps_python(&fee_state_to_v2_dict(&direct.fee_states[chan])),
            "seeded fee state must match rehydrate for {chan}"
        );
        assert_eq!(
            dumps_python(&serialize_cycle_state_payload(&state.cycle_states[chan])),
            dumps_python(&serialize_cycle_state_payload(&direct.cycle_states[chan])),
            "seeded cycle state must match rehydrate for {chan}"
        );
    }
}

/// `HydrationSource` labels round-trip into the restart marker's
/// `hydration_source` column format.
#[test]
fn seed_once_hydration_source_labels() {
    assert_eq!(HydrationSource::PythonSeed.label(), "python_seed");
    assert_eq!(
        HydrationSource::RustGeneration(7).label(),
        "rust_generation:7"
    );
}

/// Task 5 step 2: `rehydrate_from_rows` hydrates from Rust-owned rows
/// byte-identically to what was committed, and REPLACES the maps wholesale
/// while preserving process-lifetime fields (same contract as `rehydrate`).
#[test]
fn seed_once_rehydrate_from_rows_round_trips_committed_state() {
    // Build a source state via the Python-path loader over a real blob.
    let conn = seed_fixture_conn(&[("chan_a", blob_with_thompson(valid_thompson()), SEED_NOW)]);
    let mut source = ControllerState::new();
    rehydrate(&mut source, &conn);

    // Serialize exactly as the SeedOnce commit does.
    let rows: Vec<FeeStateRow> = source
        .cycle_states
        .iter()
        .map(|(id, cycle)| FeeStateRow {
            channel_id: id.clone(),
            v2_state_json: serialize_state_envelope(cycle, &source.fee_states[id]),
            last_update: cycle.last_update,
        })
        .collect();

    let mut restored = ControllerState::new();
    restored
        .fee_states
        .insert("stale".to_string(), ChannelFeeState::default());
    restored.vegas.intensity = 0.5;
    restored.vegas_wake_armed = false;
    rehydrate_from_rows(&mut restored, &rows).expect("hydrate from Rust-owned rows");

    assert!(!restored.fee_states.contains_key("stale"), "maps replaced");
    assert_eq!(restored.vegas.intensity, 0.5, "vegas preserved");
    assert!(!restored.vegas_wake_armed, "wake-armed preserved");
    assert_eq!(
        dumps_python(&fee_state_to_v2_dict(&restored.fee_states["chan_a"])),
        dumps_python(&fee_state_to_v2_dict(&source.fee_states["chan_a"])),
        "fee state must survive the Rust-owned round trip byte-identically"
    );
    assert_eq!(
        dumps_python(&serialize_cycle_state_payload(
            &restored.cycle_states["chan_a"]
        )),
        dumps_python(&serialize_cycle_state_payload(
            &source.cycle_states["chan_a"]
        )),
        "cycle state must survive the Rust-owned round trip byte-identically"
    );
    // Rust owns this state: the pre-decision epoch caches hold the
    // hydrated epochs (amendment R5 -- gate epoch == owned last_update).
    assert_eq!(
        restored.skip_gate_prev.get("chan_a").map(|e| e.last_update),
        Some(restored.cycle_states["chan_a"].last_update),
    );
}

/// Rust-owned state is NEVER silently defaulted: a corrupt stored blob
/// fails hydration closed instead of falling back to an empty envelope
/// (which is `parse_v2_blob`'s Python-parity behavior for PYTHON blobs,
/// wrong for state Rust itself wrote).
#[test]
fn seed_once_rehydrate_from_rows_fails_closed_on_corrupt_rows() {
    let corrupt = FeeStateRow {
        channel_id: "chan_a".to_string(),
        v2_state_json: "{not json".to_string(),
        last_update: SEED_NOW,
    };
    let mut state = ControllerState::new();
    let err = rehydrate_from_rows(&mut state, &[corrupt]).expect_err("corrupt blob must fail");
    assert!(err.to_string().contains("chan_a"), "{err}");
    assert!(state.fee_states.is_empty(), "no partial hydration");

    // A structurally-valid JSON blob MISSING the envelope keys Rust always
    // writes is equally corrupt for Rust-owned rows.
    let missing_keys = FeeStateRow {
        channel_id: "chan_b".to_string(),
        v2_state_json: "{}".to_string(),
        last_update: SEED_NOW,
    };
    let mut state = ControllerState::new();
    let err = rehydrate_from_rows(&mut state, &[missing_keys]).expect_err("missing envelope keys");
    assert!(err.to_string().contains("chan_b"), "{err}");
}
