import importlib.util
import json
import subprocess
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "replay_fee_capture_window.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("replay_fee_capture_window", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def make_window(
    tmp_path: Path,
    *,
    cycles: int = 6,
    evaluations: int = 100,
    adjustments: int = 10,
    state: str = "closed",
    queue_drained: bool = True,
    failed: int = 0,
    dropped: int = 0,
    sequences: list[int] | None = None,
):
    run_id = "0123456789abcdef0123456789abcdef"
    sequences = sequences or list(range(1, cycles + 1))
    evaluation_counts = [evaluations // cycles] * cycles
    for index in range(evaluations % cycles):
        evaluation_counts[index] += 1
    adjustment_counts = [adjustments // cycles] * cycles
    for index in range(adjustments % cycles):
        adjustment_counts[index] += 1

    attempts = []
    for index, sequence in enumerate(sequences):
        filename = f"capture-{sequence:08}.v0.json"
        outcomes = [{"adjustment": {"new_fee_ppm": 100 + offset}} for offset in range(adjustment_counts[index])]
        outcomes.extend(
            {"skip": {"reason": "fixture"}}
            for _ in range(evaluation_counts[index] - adjustment_counts[index])
        )
        capture = {
            "schema_name": "fee_cycle_replay",
            "schema_version": 0,
            "capture_run_id": run_id,
            "capture_seq": sequence,
            "cycle_id": f"{run_id}:{sequence:08}",
            "completeness": {
                "complete": True,
                "evaluated_channels": evaluation_counts[index],
                "terminal_outcomes": evaluation_counts[index],
            },
            "expected": {"ordered_outcomes": outcomes},
        }
        (tmp_path / filename).write_text(json.dumps(capture), encoding="utf-8")
        attempts.append(
            {
                "capture_seq": sequence,
                "cycle_id": capture["cycle_id"],
                "status": "completed",
                "eligible": True,
                "filename": filename,
            }
        )

    manifest = {
        "schema_name": "fee_cycle_capture_manifest",
        "schema_version": 0,
        "capture_run_id": run_id,
        "state": state,
        "queue_drained": queue_drained,
        "attempted": cycles + failed + dropped,
        "completed": cycles,
        "failed": failed,
        "dropped": dropped,
        "last_attempted_seq": sequences[-1] if sequences else None,
        "last_completed_seq": sequences[-1] if sequences else None,
        "retained_sequence_range": {
            "first": sequences[0] if sequences else None,
            "last": sequences[-1] if sequences else None,
        },
        "attempts": attempts,
    }
    manifest_path = tmp_path / "manifest.v0.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path, run_id


def parse_one_json(capsys):
    captured = capsys.readouterr()
    assert captured.err == ""
    lines = captured.out.splitlines()
    assert len(lines) == 1
    return json.loads(lines[0])


def exact_child_result(run_id):
    return {
        "commit": "2c9340128e892ec4ec828e12b63e85e89de76c32",
        "run_id": run_id,
        "capture_count": 6,
        "evaluated_channel_count": 100,
        "adjustment_count": 10,
        "mismatch_count": 0,
        "results": [{"status": "exact"}] * 6,
        "error": None,
    }


def test_closed_qualified_window_invokes_explicit_local_binary_once(
    tmp_path, monkeypatch, capsys
):
    runner = load_runner()
    manifest, run_id = make_window(tmp_path)
    replay_bin = tmp_path / "replay_fee_capture"
    replay_bin.write_text("#!/bin/false\n", encoding="utf-8")
    replay_bin.chmod(0o755)
    calls = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(
            args, 0, json.dumps(exact_child_result(run_id)) + "\n", ""
        )

    monkeypatch.setattr(runner.subprocess, "run", fake_run)
    code = runner.main(
        [
            "--manifest",
            str(manifest),
            "--capture-dir",
            str(tmp_path),
            "--replay-bin",
            str(replay_bin),
        ]
    )

    assert code == 0
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args == [
        str(replay_bin.resolve()),
        "--manifest",
        str(manifest.resolve()),
        "--capture-dir",
        str(tmp_path.resolve()),
    ]
    assert kwargs == {
        "capture_output": True,
        "text": True,
        "check": False,
    }
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "exact"
    assert verdict["capture_count"] == 6
    assert verdict["evaluated_channel_count"] == 100
    assert verdict["adjustment_count"] == 10
    assert verdict["mismatch_count"] == 0
    assert all(gate["met"] for gate in verdict["thresholds"].values())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("state", "open"),
        ("queue_drained", False),
        ("failed", 1),
        ("dropped", 1),
    ],
)
def test_manifest_must_be_closed_drained_and_lossless(
    tmp_path, monkeypatch, capsys, field, value
):
    runner = load_runner()
    manifest, _ = make_window(tmp_path)
    body = json.loads(manifest.read_text(encoding="utf-8"))
    body[field] = value
    manifest.write_text(json.dumps(body), encoding="utf-8")
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("invalid window must not invoke replay"),
    )

    code = runner.main(
        [
            "--manifest",
            str(manifest),
            "--capture-dir",
            str(tmp_path),
            "--replay-bin",
            "/bin/false",
        ]
    )

    assert code == 2
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "error"
    assert field in verdict["error"]


def test_retained_sequences_must_be_consecutive(tmp_path, monkeypatch, capsys):
    runner = load_runner()
    manifest, _ = make_window(tmp_path, sequences=[1, 2, 3, 5, 6, 7])
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("invalid window must not invoke replay"),
    )

    code = runner.main(
        [
            "--manifest",
            str(manifest),
            "--capture-dir",
            str(tmp_path),
            "--replay-bin",
            "/bin/false",
        ]
    )

    assert code == 2
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "error"
    assert "consecutive" in verdict["error"]


@pytest.mark.parametrize(
    ("cycles", "evaluations", "adjustments", "failed_gate"),
    [
        (5, 100, 10, "cycles"),
        (6, 99, 10, "evaluations"),
        (6, 100, 9, "adjustments"),
    ],
)
def test_minimum_window_gates_fail_before_replay(
    tmp_path,
    monkeypatch,
    capsys,
    cycles,
    evaluations,
    adjustments,
    failed_gate,
):
    runner = load_runner()
    manifest, _ = make_window(
        tmp_path,
        cycles=cycles,
        evaluations=evaluations,
        adjustments=adjustments,
    )
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("insufficient window must not invoke replay"),
    )

    code = runner.main(
        [
            "--manifest",
            str(manifest),
            "--capture-dir",
            str(tmp_path),
            "--replay-bin",
            "/bin/false",
        ]
    )

    assert code == 1
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "insufficient_window"
    assert verdict["thresholds"][failed_gate]["met"] is False


def test_capture_filename_cannot_escape_explicit_directory(
    tmp_path, monkeypatch, capsys
):
    runner = load_runner()
    manifest, _ = make_window(tmp_path)
    body = json.loads(manifest.read_text(encoding="utf-8"))
    body["attempts"][0]["filename"] = "../outside.json"
    manifest.write_text(json.dumps(body), encoding="utf-8")
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("escaped path must not invoke replay"),
    )

    code = runner.main(
        [
            "--manifest",
            str(manifest),
            "--capture-dir",
            str(tmp_path),
            "--replay-bin",
            "/bin/false",
        ]
    )

    assert code == 2
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "error"
    assert "basename" in verdict["error"]


@pytest.mark.parametrize(
    ("child_code", "child_status", "expected_code"),
    [
        (1, "mismatch", 1),
        (2, "error", 2),
    ],
)
def test_replay_failure_is_forwarded_as_machine_json(
    tmp_path, monkeypatch, capsys, child_code, child_status, expected_code
):
    runner = load_runner()
    manifest, run_id = make_window(tmp_path)
    replay_bin = tmp_path / "replay_fee_capture"
    replay_bin.write_text("#!/bin/false\n", encoding="utf-8")
    replay_bin.chmod(0o755)
    child = exact_child_result(run_id)
    child["mismatch_count"] = 1 if child_code == 1 else 0
    child["error"] = "fixture failure"
    child["results"][0] = {"status": child_status}
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(
            args, child_code, json.dumps(child) + "\n", ""
        ),
    )

    code = runner.main(
        [
            "--manifest",
            str(manifest),
            "--capture-dir",
            str(tmp_path),
            "--replay-bin",
            str(replay_bin),
        ]
    )

    assert code == expected_code
    verdict = parse_one_json(capsys)
    assert verdict["status"] == child_status
    assert verdict["replay_exit_code"] == child_code


@pytest.mark.parametrize(
    "flag", ["--node", "--rpc-file", "--lightning-dir", "--db", "--unknown"]
)
def test_unknown_flags_including_live_surfaces_are_rejected(
    monkeypatch, capsys, flag
):
    runner = load_runner()
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("parser failure must not invoke replay"),
    )

    code = runner.main([flag, "forbidden"])

    assert code == 2
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "error"
    assert flag in verdict["error"]
