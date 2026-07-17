//! Production `v2_state_json` blobs round-trip losslessly — the "NOT
//! compressible" Phase 4 gate item (Task 9 Step 2). `#[ignore]`d: never
//! runs in ordinary `cargo test` / CI, and never reads anything from
//! lnnode itself — the implementer does not ssh (see the module doc
//! comment below for the exact controller-run commands).
//!
//! # Running (controller only)
//!
//! 1. Dump the production `fee_strategy_state` table (READ-ONLY; find
//!    `REVOPS_DB_PATH` via `ssh lnnode 'lightning-cli listconfigs
//!    revenue-ops-db-path'`):
//!
//!    ```text
//!    ssh lnnode 'sqlite3 -readonly "$REVOPS_DB_PATH" \
//!      "SELECT channel_id, last_revenue_rate, last_fee_ppm, trend_direction, step_ppm,
//!              consecutive_same_direction, last_update, last_broadcast_fee_ppm,
//!              is_sleeping, sleep_until, stable_cycles, forward_count_since_update,
//!              last_volume_sats, last_state, v2_state_json
//!       FROM fee_strategy_state;" -json' > fee_strategy_state.json
//!    ```
//!
//!    (Verified against `modules/database.py:655-666` + the
//!    `v2_state_json`/`last_state`/`forward_count_since_update`/
//!    `last_volume_sats` migration `ALTER TABLE`s at lines 1099-1164, and
//!    `fixtures/schema.sql`'s vendored `CREATE TABLE fee_strategy_state`
//!    statement — table and column names are exact.)
//!
//! 2. Compute the Python truth for the `from_v2_dict -> to_v2_dict`
//!    round-trip path over the SAME dump, in the port worktree:
//!
//!    ```text
//!    python3 tools/port/gen_fees_fixtures.py v2_prod_check \
//!        fee_strategy_state.json /path/to/expected_dir
//!    ```
//!
//!    This writes `/path/to/expected_dir/expected_to_v2_dict.json`
//!    (`{channel_id: expected_json_string}`) and prints any
//!    unrecognized-`algorithm_version` warnings to stderr.
//!
//! 3. Run this test, pointing `REVOPS_STATE_BLOBS` at EITHER the single
//!    `fee_strategy_state.json` dump from step 1 OR a directory containing
//!    one such row (or one JSON array of rows) per file:
//!
//!    ```text
//!    REVOPS_STATE_BLOBS=/path/to/fee_strategy_state.json \
//!        cargo test -p revops-fees --test production_blobs -- --ignored
//!    ```
//!
//!    The expected-bytes file is located next to it by convention: if
//!    `REVOPS_STATE_BLOBS` names a file, the expected file is that file's
//!    sibling `<stem>.expected_to_v2_dict.json` (e.g.
//!    `fee_strategy_state.expected_to_v2_dict.json`); if it names a
//!    directory, the expected file is `<dir>/expected_to_v2_dict.json`.
//!    Override either half of the convention explicitly with
//!    `REVOPS_STATE_BLOBS_EXPECTED=/path/to/expected_to_v2_dict.json` if
//!    the naming convention doesn't fit an operator's layout.

use revops_fees::pyjson::{dumps_python, parse};
use revops_fees::state_store::{load_fee_state, parse_v2_blob, FeeStrategyRow};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn row_from_json(v: &Value) -> FeeStrategyRow {
    let get_i64 = |k: &str, default: i64| v.get(k).and_then(Value::as_i64).unwrap_or(default);
    let get_bool_from_int = |k: &str| v.get(k).and_then(Value::as_i64).unwrap_or(0) != 0;
    FeeStrategyRow {
        channel_id: v["channel_id"].as_str().unwrap_or("<unknown>").to_string(),
        last_revenue_rate: v
            .get("last_revenue_rate")
            .and_then(Value::as_f64)
            .unwrap_or(0.0),
        last_fee_ppm: get_i64("last_fee_ppm", 0),
        trend_direction: get_i64("trend_direction", 1),
        step_ppm: get_i64("step_ppm", 50),
        consecutive_same_direction: get_i64("consecutive_same_direction", 0),
        last_update: get_i64("last_update", 0),
        last_broadcast_fee_ppm: get_i64("last_broadcast_fee_ppm", 0),
        is_sleeping: get_bool_from_int("is_sleeping"),
        sleep_until: get_i64("sleep_until", 0),
        stable_cycles: get_i64("stable_cycles", 0),
        forward_count_since_update: get_i64("forward_count_since_update", 0),
        last_volume_sats: get_i64("last_volume_sats", 0),
        last_state: v
            .get("last_state")
            .and_then(Value::as_str)
            .unwrap_or("balanced")
            .to_string(),
        v2_state_json: v
            .get("v2_state_json")
            .and_then(Value::as_str)
            .unwrap_or("{}")
            .to_string(),
    }
}

/// Loads rows from either a single sqlite3-`-json`-shaped array file, a
/// single row object file, or a directory containing any mixture of
/// those (one file per channel, or a handful of batched arrays) — see the
/// module doc comment for the exact controller-run commands that produce
/// these.
fn load_rows(path: &Path) -> Vec<FeeStrategyRow> {
    fn rows_from_file(path: &Path) -> Vec<FeeStrategyRow> {
        let raw = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let v: Value = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("{}: invalid JSON: {e}", path.display()));
        match v {
            Value::Array(items) => items.iter().map(row_from_json).collect(),
            Value::Object(_) => vec![row_from_json(&v)],
            other => panic!(
                "{}: expected a JSON array or object, got {other:?}",
                path.display()
            ),
        }
    }

    if path.is_dir() {
        let mut out = Vec::new();
        let mut entries: Vec<PathBuf> = std::fs::read_dir(path)
            .unwrap_or_else(|e| panic!("read_dir {}: {e}", path.display()))
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
            .filter(|p| p.file_name().and_then(|n| n.to_str()) != Some("expected_to_v2_dict.json"))
            .collect();
        entries.sort();
        for entry in entries {
            out.extend(rows_from_file(&entry));
        }
        out
    } else {
        rows_from_file(path)
    }
}

fn load_expected(blobs_path: &Path) -> HashMap<String, String> {
    let expected_path = if let Ok(p) = std::env::var("REVOPS_STATE_BLOBS_EXPECTED") {
        PathBuf::from(p)
    } else if blobs_path.is_dir() {
        blobs_path.join("expected_to_v2_dict.json")
    } else {
        let stem = blobs_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("fee_strategy_state");
        blobs_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{stem}.expected_to_v2_dict.json"))
    };
    let raw = std::fs::read_to_string(&expected_path).unwrap_or_else(|e| {
        panic!(
            "read expected-bytes file {} (run `gen_fees_fixtures.py v2_prod_check` first; \
             override with REVOPS_STATE_BLOBS_EXPECTED): {e}",
            expected_path.display()
        )
    });
    let v: Value = serde_json::from_str(&raw).expect("valid JSON");
    v["expected"]
        .as_object()
        .expect("expected object")
        .iter()
        .map(|(k, val)| (k.clone(), val.as_str().unwrap().to_string()))
        .collect()
}

/// The Task 9 Step 2 gate: for EVERY production row, (a) parse succeeds
/// (or the divergence is enumerated below rather than silently
/// swallowed), (b) parse->re-emit is content-identical (in fact
/// byte-identical, since every real row was written by `json.dumps` in
/// the canonical default-separator form this port's writer reproduces),
/// (c) parse->`load_fee_state`->`to_v2_dict`->re-emit is byte-identical
/// to Python performing the same load->save (the `v2_prod_check`
/// generator subcommand's expected bytes).
#[test]
#[ignore]
fn production_blobs_round_trip_losslessly() {
    let blobs_path = PathBuf::from(
        std::env::var("REVOPS_STATE_BLOBS")
            .expect("set REVOPS_STATE_BLOBS=/path/to/fee_strategy_state.json (or a directory of such dumps) — see this file's module doc comment for the exact controller-run dump command"),
    );
    let rows = load_rows(&blobs_path);
    assert!(
        !rows.is_empty(),
        "no rows loaded from {}",
        blobs_path.display()
    );
    let expected = load_expected(&blobs_path);

    let mut parse_failures: Vec<String> = Vec::new();
    let mut content_mismatches: Vec<String> = Vec::new();
    let mut to_v2_dict_mismatches: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for row in &rows {
        let blob = &row.v2_state_json;
        let raw = match parse(blob) {
            Ok(v) => v,
            Err(e) => {
                parse_failures.push(format!("{}: {e}", row.channel_id));
                continue;
            }
        };

        // (b) content/byte-identical raw round trip.
        let reemitted = dumps_python(&raw);
        if &reemitted != blob {
            content_mismatches.push(row.channel_id.clone());
        }

        // (c) load_fee_state -> to_v2_dict -> re-emit vs. Python truth.
        if let Some(expected_bytes) = expected.get(&row.channel_id) {
            let env = parse_v2_blob(blob, row);
            let fee_state = load_fee_state(&env, row);
            let actual = dumps_python(&revops_fees::state_store::fee_state_to_v2_dict(&fee_state));
            if &actual != expected_bytes {
                to_v2_dict_mismatches.push(row.channel_id.clone());
            }
            checked += 1;
        }
    }

    if !parse_failures.is_empty() {
        eprintln!(
            "production_blobs: {} parse failure(s):",
            parse_failures.len()
        );
        for f in &parse_failures {
            eprintln!("  {f}");
        }
    }

    eprintln!(
        "production_blobs: checked {checked}/{} rows against expected_to_v2_dict.json",
        rows.len()
    );

    assert!(
        parse_failures.is_empty(),
        "{} row(s) failed to parse v2_state_json — see stderr above",
        parse_failures.len()
    );
    assert!(
        content_mismatches.is_empty(),
        "{} row(s) failed raw content/byte-identical round trip: {content_mismatches:?}",
        content_mismatches.len()
    );
    assert!(
        to_v2_dict_mismatches.is_empty(),
        "{} row(s) failed load_fee_state->to_v2_dict byte-identical round trip vs. Python truth: {to_v2_dict_mismatches:?}",
        to_v2_dict_mismatches.len()
    );
}
