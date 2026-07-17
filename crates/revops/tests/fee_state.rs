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
    ]);

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
