use std::fs;
use std::process::{Command, Output};

use serde_json::{json, Value};

const COMPLETE_ADJUSTMENT: &str =
    include_str!("../../../fixtures/fees/replay/complete_adjustment.v0.json");

fn replay_bin() -> &'static str {
    env!("CARGO_BIN_EXE_replay_fee_capture")
}

fn run(args: &[&str]) -> Output {
    Command::new(replay_bin())
        .args(args)
        .output()
        .expect("run replay_fee_capture")
}

fn one_json(output: &Output) -> Value {
    assert!(
        output.stderr.is_empty(),
        "stderr must stay empty; machine output belongs on stdout: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = std::str::from_utf8(&output.stdout).expect("stdout UTF-8");
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "stdout must contain exactly one JSON object"
    );
    serde_json::from_str(lines[0]).expect("stdout JSON")
}

fn fixture_path() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/fees/replay/complete_adjustment.v0.json"
    )
}

#[test]
fn single_capture_reports_one_exact_machine_result() {
    let output = run(&["--capture", fixture_path()]);
    assert_eq!(output.status.code(), Some(0));
    let verdict = one_json(&output);

    assert_eq!(
        verdict["commit"],
        json!("2c9340128e892ec4ec828e12b63e85e89de76c32")
    );
    assert_eq!(verdict["run_id"], json!("cc07a7a72583441398dfb3719beedf51"));
    assert_eq!(verdict["capture_count"], json!(1));
    assert_eq!(verdict["evaluated_channel_count"], json!(1));
    assert_eq!(verdict["adjustment_count"], json!(1));
    assert_eq!(verdict["mismatch_count"], json!(0));
    assert_eq!(verdict["results"].as_array().unwrap().len(), 1);
    assert_eq!(verdict["results"][0]["status"], json!("exact"));
    assert_eq!(verdict["results"][0]["mismatch_count"], json!(0));
    assert_eq!(verdict["results"][0]["capture_seq"], json!(1));
    assert_eq!(verdict["results"][0]["evaluated_channel_count"], json!(1));
    assert_eq!(verdict["results"][0]["adjustment_count"], json!(1));
}

#[test]
fn replay_value_mismatch_exits_one_and_is_reported_as_parity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mismatch.json");
    let mut capture: Value = serde_json::from_str(COMPLETE_ADJUSTMENT).unwrap();
    capture["expected"]["ordered_outcomes"][0]["adjustment"]["new_fee_ppm"] = json!(251);
    capture["payload_sha256"] =
        json!("b2fa77ed954e7bbe8c3f19822a56875a1fdefd7eb95c0b64c1437c7f57ff7876");
    fs::write(&path, serde_json::to_vec(&capture).unwrap()).unwrap();

    let output = run(&["--capture", path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(1));
    let verdict = one_json(&output);
    assert_eq!(verdict["capture_count"], json!(1));
    assert_eq!(verdict["mismatch_count"], json!(1));
    assert_eq!(verdict["results"][0]["status"], json!("mismatch"));
    assert_eq!(verdict["results"][0]["mismatch_count"], json!(1));
    assert!(verdict["results"][0]["error"]
        .as_str()
        .unwrap()
        .contains("$.expected.ordered_outcomes"));
}

#[test]
fn malformed_capture_and_io_errors_exit_two_with_one_json_result() {
    let dir = tempfile::tempdir().unwrap();
    let malformed = dir.path().join("malformed.json");
    fs::write(&malformed, b"{not-json").unwrap();

    for path in [
        malformed.to_string_lossy().into_owned(),
        dir.path()
            .join("missing.json")
            .to_string_lossy()
            .into_owned(),
    ] {
        let output = run(&["--capture", &path]);
        assert_eq!(output.status.code(), Some(2), "path={path}");
        let verdict = one_json(&output);
        assert_eq!(verdict["mismatch_count"], json!(0));
        assert_eq!(verdict["results"][0]["status"], json!("error"));
        assert!(verdict["results"][0]["error"].is_string());
    }
}

#[test]
fn manifest_mode_reads_only_the_explicit_capture_directory() {
    let dir = tempfile::tempdir().unwrap();
    let capture_name = "capture-00000001.v0.json";
    fs::write(dir.path().join(capture_name), COMPLETE_ADJUSTMENT).unwrap();
    let manifest = json!({
        "schema_name": "fee_cycle_capture_manifest",
        "schema_version": 0,
        "capture_run_id": "cc07a7a72583441398dfb3719beedf51",
        "state": "closed",
        "queue_drained": true,
        "started_at": "2026-07-20T00:35:14.825144+00:00",
        "updated_at": "2026-07-20T00:35:15.825144+00:00",
        "attempted": 1,
        "completed": 1,
        "failed": 0,
        "dropped": 0,
        "last_attempted_seq": 1,
        "last_completed_seq": 1,
        "retained_sequence_range": {"first": 1, "last": 1},
        "writer_health": "ok",
        "last_error_category": null,
        "attempts": [{
            "capture_seq": 1,
            "cycle_id": "cc07a7a72583441398dfb3719beedf51:00000001",
            "status": "completed",
            "eligible": true,
            "filename": capture_name,
            "bytes": COMPLETE_ADJUSTMENT.len(),
            "error_category": null,
            "error": null,
            "rotation_error": null
        }]
    });
    let manifest_path = dir.path().join("manifest.v0.json");
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let output = run(&[
        "--manifest",
        manifest_path.to_str().unwrap(),
        "--capture-dir",
        dir.path().to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(0));
    let verdict = one_json(&output);
    assert_eq!(verdict["capture_count"], json!(1));
    assert_eq!(verdict["mismatch_count"], json!(0));
    assert_eq!(verdict["results"][0]["status"], json!("exact"));
}

#[test]
fn parser_rejects_every_unknown_flag_including_live_surfaces() {
    for flag in [
        "--node",
        "--rpc-file",
        "--lightning-dir",
        "--db",
        "--network",
        "--unknown",
    ] {
        let output = run(&[flag, "forbidden"]);
        assert_eq!(output.status.code(), Some(2), "flag={flag}");
        let verdict = one_json(&output);
        assert_eq!(verdict["capture_count"], json!(0));
        assert_eq!(verdict["mismatch_count"], json!(0));
        assert!(
            verdict["error"].as_str().unwrap().contains(flag),
            "verdict={verdict}"
        );
    }
}

#[test]
fn parser_rejects_ambiguous_duplicate_and_incomplete_modes() {
    let invalid: &[&[&str]] = &[
        &[],
        &["--capture"],
        &["--manifest", "manifest.json"],
        &["--capture-dir", "."],
        &["--capture", fixture_path(), "--capture", fixture_path()],
        &[
            "--capture",
            fixture_path(),
            "--manifest",
            "manifest.json",
            "--capture-dir",
            ".",
        ],
        &["positional.json"],
    ];

    for args in invalid {
        let output = run(args);
        assert_eq!(output.status.code(), Some(2), "args={args:?}");
        let verdict = one_json(&output);
        assert_eq!(verdict["capture_count"], json!(0));
        assert!(verdict["error"].is_string(), "verdict={verdict}");
    }
}
