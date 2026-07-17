//! `v2_state_json` lossless round-trip (Phase 4 Task 9), pinned against
//! `fixtures/fees/state_roundtrip/{blobs,merge_matrix}.json` — generated
//! from the REAL Python `FeeController`/`ChannelFeeState`/
//! `ChannelCycleState` classes by `tools/port/gen_fees_fixtures.py v2_blob`
//! in the port worktree (see that generator for the exact scripted-double
//! construction).
//!
//! Two DIFFERENT round-trip contracts are pinned here, and they are
//! intentionally different assertions — see the `state_store` module doc
//! comment for why:
//!
//! (a) **raw structural fidelity**: `dumps_python(parse(blob))` is
//!     byte-identical to `blob` for every fixture blob (this is a property
//!     of [`revops_fees::pyjson::OValue`] itself, not of anything in
//!     `state_store` — proven once here per case as a regression guard).
//! (b) **merged-write fidelity**: `build_merged_row(channel_id, None, None,
//!     parse_v2_blob(blob, row))` reproduces the generator's
//!     `python_roundtrip_blob` (the real `_build_merged_fee_strategy_row`
//!     output) byte-for-byte.
//!
//! Plus (c) `load_fee_state`'s pinned field values (floats via `py_repr`),
//! and the 9-case explicit-shared-field merge matrix.

use revops_econ::pyfloat::py_repr;
use revops_fees::pyjson::{dumps_python, parse, OValue};
use revops_fees::state_store::{
    build_merged_row, load_fee_state, parse_v2_blob, ChannelCycleState, ChannelFeeState,
    FeeStrategyRow,
};
use serde_json::Value;
use std::path::PathBuf;

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/fees/state_roundtrip")
        .join(name);
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("valid JSON")
}

fn case<'a>(fx: &'a Value, name: &str) -> &'a Value {
    fx["cases"]
        .as_array()
        .expect("cases array")
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("case {name} not found"))
}

fn row_from_json(v: &Value) -> FeeStrategyRow {
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

fn assert_blob_case(c: &Value) {
    let name = c["name"].as_str().unwrap();
    let channel_id = c["channel_id"].as_str().unwrap();
    let row = row_from_json(&c["row"]);
    let input_blob = c["input_blob"].as_str().expect("input_blob string");
    let python_roundtrip_blob = c["python_roundtrip_blob"]
        .as_str()
        .expect("python_roundtrip_blob string");

    // (a) raw structural fidelity.
    let raw = parse(input_blob).unwrap_or_else(|e| panic!("{name}: parse input_blob: {e}"));
    assert_eq!(
        dumps_python(&raw),
        input_blob,
        "{name}: raw parse->re-emit must be byte-identical to input_blob"
    );

    // (b) merged-write fidelity.
    let env = parse_v2_blob(input_blob, &row);
    let (_row_fields, merged) = build_merged_row(channel_id, None, None, &env);
    assert_eq!(
        dumps_python(&merged),
        python_roundtrip_blob,
        "{name}: build_merged_row(channel_id, None, None, ..) must match the generator's python_roundtrip_blob"
    );

    // (c) load_fee_state pinned field values.
    let fee_state = load_fee_state(&env, &row);
    let pinned = &c["fee_state_pinned"];
    assert_eq!(
        fee_state.algorithm_version,
        pinned["algorithm_version"].as_str().unwrap(),
        "{name}: algorithm_version"
    );
    assert_eq!(
        py_repr(fee_state.last_revenue_rate),
        pinned["last_revenue_rate"].as_str().unwrap(),
        "{name}: last_revenue_rate"
    );
    assert_eq!(
        fee_state.last_fee_ppm,
        pinned["last_fee_ppm"].as_i64().unwrap(),
        "{name}: last_fee_ppm"
    );
    assert_eq!(
        fee_state.last_broadcast_fee_ppm,
        pinned["last_broadcast_fee_ppm"].as_i64().unwrap(),
        "{name}: last_broadcast_fee_ppm"
    );
    assert_eq!(
        fee_state.last_update,
        pinned["last_update"].as_i64().unwrap(),
        "{name}: last_update"
    );
    assert_eq!(
        fee_state.last_broadcast_at(),
        pinned["last_broadcast_at"].as_i64().unwrap(),
        "{name}: last_broadcast_at"
    );
    assert_eq!(
        fee_state.last_state,
        pinned["last_state"].as_str().unwrap(),
        "{name}: last_state"
    );
    assert_eq!(
        fee_state.is_sleeping,
        pinned["is_sleeping"].as_bool().unwrap(),
        "{name}: is_sleeping"
    );
    assert_eq!(
        fee_state.sleep_until,
        pinned["sleep_until"].as_i64().unwrap(),
        "{name}: sleep_until"
    );
    assert_eq!(
        fee_state.stable_cycles,
        pinned["stable_cycles"].as_i64().unwrap(),
        "{name}: stable_cycles"
    );
    assert_eq!(
        fee_state.forward_count_since_update,
        pinned["forward_count_since_update"].as_i64().unwrap(),
        "{name}: forward_count_since_update"
    );
    assert_eq!(
        fee_state.last_volume_sats,
        pinned["last_volume_sats"].as_i64().unwrap(),
        "{name}: last_volume_sats"
    );
    assert_eq!(
        fee_state.last_gossip_refresh(),
        pinned["last_gossip_refresh"].as_i64().unwrap(),
        "{name}: last_gossip_refresh"
    );
    assert_eq!(
        py_repr(fee_state.last_vegas_multiplier),
        pinned["last_vegas_multiplier"].as_str().unwrap(),
        "{name}: last_vegas_multiplier"
    );
    assert_eq!(
        fee_state.last_fee_profile,
        pinned["last_fee_profile"].as_str().unwrap(),
        "{name}: last_fee_profile"
    );
    assert_eq!(
        fee_state.last_context_key,
        pinned["last_context_key"].as_str().unwrap(),
        "{name}: last_context_key"
    );
    assert_eq!(
        fee_state.last_time_bucket,
        pinned["last_time_bucket"].as_str().unwrap(),
        "{name}: last_time_bucket"
    );
    assert_eq!(
        fee_state.last_corridor_role,
        pinned["last_corridor_role"].as_str().unwrap(),
        "{name}: last_corridor_role"
    );
    assert_eq!(
        fee_state.last_contextual_sample_used,
        pinned["last_contextual_sample_used"].as_bool().unwrap(),
        "{name}: last_contextual_sample_used"
    );
    assert_eq!(
        fee_state.dynamic_htlcmin_baseline_msat(),
        pinned["dynamic_htlcmin_baseline_msat"].as_i64(),
        "{name}: dynamic_htlcmin_baseline_msat"
    );
    assert_eq!(
        py_repr(fee_state.pid.kp),
        pinned["pid_kp"].as_str().unwrap(),
        "{name}: pid.kp"
    );
    assert_eq!(
        fee_state.thompson.observations.len() as i64,
        pinned["thompson_observations_count"].as_i64().unwrap(),
        "{name}: thompson_observations_count"
    );
}

#[test]
fn all_blob_cases_round_trip() {
    let fx = fixture("blobs.json");
    for c in fx["cases"].as_array().unwrap() {
        assert_blob_case(c);
    }
}

#[test]
fn current_nested_layout_round_trips() {
    let fx = fixture("blobs.json");
    assert_blob_case(case(&fx, "current_nested_layout"));
}

#[test]
fn pre_nesting_flat_layout_round_trips() {
    let fx = fixture("blobs.json");
    assert_blob_case(case(&fx, "pre_nesting_flat_layout"));
}

#[test]
fn thompson_aimd_v1_legacy_round_trips() {
    let fx = fixture("blobs.json");
    assert_blob_case(case(&fx, "thompson_aimd_v1_legacy"));
}

#[test]
fn five_tuple_observations_only_round_trips() {
    let fx = fixture("blobs.json");
    assert_blob_case(case(&fx, "five_tuple_observations_only"));
}

#[test]
fn six_tuple_congestion_and_zero_probe_round_trips() {
    let fx = fixture("blobs.json");
    assert_blob_case(case(&fx, "six_tuple_congestion_and_zero_probe"));
}

#[test]
fn three_tuple_contextual_posteriors_round_trips() {
    let fx = fixture("blobs.json");
    assert_blob_case(case(&fx, "three_tuple_contextual_posteriors"));
}

#[test]
fn missing_pid_state_stamps_unknown_version_and_round_trips() {
    let fx = fixture("blobs.json");
    let c = case(&fx, "missing_pid_state");
    assert_blob_case(c);
    // The whole point of this case: fee_state had no "algorithm_version" ->
    // from_v2_dict defaults to "migrated" -> the unknown-version stamp
    // overwrites it to "dts_pid_v1".
    assert_eq!(c["fee_state_pinned"]["algorithm_version"], "dts_pid_v1");
}

#[test]
fn missing_cycle_state_round_trips() {
    let fx = fixture("blobs.json");
    assert_blob_case(case(&fx, "missing_cycle_state"));
}

#[test]
fn unknown_keys_three_levels_round_trips() {
    let fx = fixture("blobs.json");
    let c = case(&fx, "unknown_keys_three_levels");
    assert_blob_case(c);

    // The RAW round trip (a) preserves the top-level junk key even though
    // the MERGED round trip (b) intentionally drops it - both assertions
    // already ran inside assert_blob_case; spot-check the raw side here.
    let input_blob = c["input_blob"].as_str().unwrap();
    let raw = parse(input_blob).unwrap();
    assert!(raw.get("future_top_level_field").is_some());
    let python_roundtrip_blob = c["python_roundtrip_blob"].as_str().unwrap();
    assert!(!python_roundtrip_blob.contains("future_top_level_field"));
    assert!(python_roundtrip_blob.contains("future_fee_field"));
    assert!(python_roundtrip_blob.contains("future_thompson_field"));
}

#[test]
fn non_ascii_last_context_key_round_trips() {
    let fx = fixture("blobs.json");
    let c = case(&fx, "non_ascii_last_context_key");
    assert_blob_case(c);
    assert_eq!(c["fee_state_pinned"]["last_context_key"], "café:☃:P");
}

#[test]
fn unrecognized_algorithm_version_discards_persisted_thompson_state() {
    let fx = fixture("blobs.json");
    let c = case(&fx, "unrecognized_algorithm_version_populated_thompson");
    assert_blob_case(c);
    // The whole point of this case (mirrors `from_v2_dict`, py 2181-2187):
    // an unrecognized `algorithm_version` with an otherwise POPULATED
    // persisted thompson_state must still be discarded in favor of a fresh
    // `GaussianThompsonState()` -- NOT retained. `missing_pid_state` can't
    // prove this because its thompson_state is already at fresh defaults
    // (resetting a fresh state to a fresh state is unobservable); here the
    // input has a real observation + posterior, so a stale-state bug would
    // surface as `thompson_observations_count != 0`.
    assert_eq!(c["fee_state_pinned"]["algorithm_version"], "dts_pid_v1");
    assert_eq!(c["fee_state_pinned"]["thompson_observations_count"], 0);
}

// ---------------------------------------------------------------------------
// merge_matrix: explicit-shared-field resolution (9 cases).
// ---------------------------------------------------------------------------

fn cycle_state_from_case(c: &Value) -> Option<ChannelCycleState> {
    let value = c["cycle_state_value"].as_i64()?;
    let explicit = c["cycle_state_explicit"].as_bool().unwrap();
    let mut cs = ChannelCycleState::default();
    if explicit {
        cs.set_last_gossip_refresh(value);
    }
    // (untouched-default case: value is always 0 per the generator, and we
    // leave the field untouched — ChannelCycleState::default() already is 0.)
    Some(cs)
}

fn fee_state_from_case(c: &Value) -> Option<ChannelFeeState> {
    let value = c["fee_state_value"].as_i64()?;
    let explicit = c["fee_state_explicit"].as_bool().unwrap();
    let mut fs = ChannelFeeState::default();
    if explicit {
        fs.set_last_gossip_refresh(value);
    }
    Some(fs)
}

#[test]
fn merge_matrix_explicit_shared_field_resolution() {
    let fx = fixture("merge_matrix.json");
    for c in fx["cases"].as_array().unwrap() {
        let name = c["name"].as_str().unwrap();
        let channel_id = c["channel_id"].as_str().unwrap();
        let row = row_from_json(&c["row"]);
        let input_blob = c["input_blob"].as_str().unwrap();
        let env = parse_v2_blob(input_blob, &row);

        let cycle_state = cycle_state_from_case(c);
        let fee_state = fee_state_from_case(c);

        let (_row_fields, merged) =
            build_merged_row(channel_id, cycle_state.as_ref(), fee_state.as_ref(), &env);

        let expected = c["expected_last_gossip_refresh"].as_i64().unwrap();
        assert_eq!(
            merged.get("last_gossip_refresh"),
            Some(&OValue::Int(expected)),
            "{name}: resolved last_gossip_refresh"
        );
    }
}
