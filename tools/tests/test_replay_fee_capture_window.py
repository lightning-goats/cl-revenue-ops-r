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
        "results": [],
        "error": None,
    }


def exact_results(tmp_path: Path, cycles: int = 6):
    return [
        {
            "file": str((tmp_path / f"capture-{sequence:08}.v0.json").resolve()),
            "status": "exact",
            "capture_seq": sequence,
            "evaluated_channel_count": 17 if sequence <= 4 else 16,
            "adjustment_count": 2 if sequence <= 4 else 1,
            "mismatch_count": 0,
            "error": None,
        }
        for sequence in range(1, cycles + 1)
    ]


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
        child = exact_child_result(run_id)
        child["results"] = exact_results(tmp_path)
        return subprocess.CompletedProcess(
            args, 0, (json.dumps(child) + "\n").encode(), b""
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
    child["results"] = exact_results(tmp_path)
    child["mismatch_count"] = 1 if child_code == 1 else 0
    child["error"] = "fixture failure"
    child["results"][0]["status"] = child_status
    child["results"][0]["mismatch_count"] = 1 if child_code == 1 else 0
    child["results"][0]["error"] = "fixture failure"
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(
            args, child_code, (json.dumps(child) + "\n").encode(), b""
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


@pytest.mark.parametrize(
    "expected",
    [
        None,
        [],
        {},
        {"ordered_outcomes": None},
        {"ordered_outcomes": "not-an-array"},
    ],
)
def test_malformed_expected_shapes_are_machine_json_input_errors(
    tmp_path, monkeypatch, capsys, expected
):
    runner = load_runner()
    manifest, _ = make_window(tmp_path)
    capture_path = tmp_path / "capture-00000001.v0.json"
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    capture["expected"] = expected
    capture_path.write_text(json.dumps(capture), encoding="utf-8")
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("malformed input must not invoke replay"),
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
    assert verdict["error"]


@pytest.mark.parametrize("adjustment", [None, 1, "bad", [], True])
def test_malformed_adjustment_values_are_not_counted_as_actual_adjustments(
    tmp_path, monkeypatch, capsys, adjustment
):
    runner = load_runner()
    manifest, _ = make_window(tmp_path)
    capture_path = tmp_path / "capture-00000001.v0.json"
    capture = json.loads(capture_path.read_text(encoding="utf-8"))
    capture["expected"]["ordered_outcomes"][0] = {"adjustment": adjustment}
    capture_path.write_text(json.dumps(capture), encoding="utf-8")
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("malformed input must not invoke replay"),
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
    assert "adjustment" in verdict["error"]


def test_child_launch_oserror_is_one_machine_json_error(
    tmp_path, monkeypatch, capsys
):
    runner = load_runner()
    manifest, _ = make_window(tmp_path)
    replay_bin = tmp_path / "replay_fee_capture"
    replay_bin.write_text("#!/bin/false\n", encoding="utf-8")
    replay_bin.chmod(0o755)
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("launch failed")),
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

    assert code == 2
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "error"
    assert "launch failed" in verdict["error"]


def test_non_utf8_child_output_is_one_machine_json_error(
    tmp_path, monkeypatch, capsys
):
    runner = load_runner()
    manifest, _ = make_window(tmp_path)
    replay_bin = tmp_path / "replay_fee_capture"
    replay_bin.write_text("#!/bin/false\n", encoding="utf-8")
    replay_bin.chmod(0o755)
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, b"\xff\n", b""),
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

    assert code == 2
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "error"
    assert "UTF-8" in verdict["error"]


def test_unexpected_parse_or_preflight_exception_is_one_machine_json_error(
    tmp_path, monkeypatch, capsys
):
    runner = load_runner()
    manifest, _ = make_window(tmp_path)
    monkeypatch.setattr(
        runner,
        "_preflight",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("preflight bug")),
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
    assert "preflight bug" in verdict["error"]


@pytest.mark.parametrize(
    ("mutation", "needle"),
    [
        (lambda child: child.update(commit=""), "commit"),
        (lambda child: child.update(commit=None), "commit"),
        (lambda child: child.update(error="impossible exact error"), "error"),
        (lambda child: child.update(run_id="wrong"), "run_id"),
        (lambda child: child.update(capture_count=5), "capture_count"),
        (
            lambda child: child.update(evaluated_channel_count=99),
            "evaluated_channel_count",
        ),
        (lambda child: child.update(adjustment_count=9), "adjustment_count"),
        (lambda child: child.update(results=None), "results"),
        (lambda child: child["results"].pop(), "results"),
        (
            lambda child: child["results"].append(dict(child["results"][0])),
            "results",
        ),
        (
            lambda child: child["results"][0].update(
                file=child["results"][1]["file"]
            ),
            "results",
        ),
        (
            lambda child: child["results"][0].update(capture_seq=True),
            "capture_seq",
        ),
        (lambda child: child["results"][0].update(status="mismatch"), "status"),
        (
            lambda child: child["results"][0].update(mismatch_count=1),
            "mismatch",
        ),
    ],
)
def test_incomplete_or_wrong_success_verdict_cannot_pass(
    tmp_path, monkeypatch, capsys, mutation, needle
):
    runner = load_runner()
    manifest, run_id = make_window(tmp_path)
    replay_bin = tmp_path / "replay_fee_capture"
    replay_bin.write_text("#!/bin/false\n", encoding="utf-8")
    replay_bin.chmod(0o755)
    child = exact_child_result(run_id)
    child["results"] = exact_results(tmp_path)
    mutation(child)
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(
            args, 0, (json.dumps(child) + "\n").encode(), b""
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

    assert code == 2
    verdict = parse_one_json(capsys)
    assert verdict["status"] == "error"
    assert needle in verdict["error"]


@pytest.mark.parametrize("kind", ["manifest", "capture", "aggregate"])
def test_named_byte_limits_fail_closed_before_replay(
    tmp_path, monkeypatch, capsys, kind
):
    runner = load_runner()
    manifest, _ = make_window(tmp_path)
    if kind == "manifest":
        monkeypatch.setattr(
            runner, "MAX_MANIFEST_BYTES", manifest.stat().st_size - 1
        )
    elif kind == "capture":
        first_capture = tmp_path / "capture-00000001.v0.json"
        monkeypatch.setattr(
            runner, "MAX_CAPTURE_BYTES", first_capture.stat().st_size - 1
        )
    else:
        total = sum(
            (tmp_path / f"capture-{sequence:08}.v0.json").stat().st_size
            for sequence in range(1, 7)
        )
        monkeypatch.setattr(runner, "MAX_SELECTED_WINDOW_BYTES", total - 1)
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("oversize input must not invoke replay"),
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
    assert "maximum" in verdict["error"] or "aggregate" in verdict["error"]
