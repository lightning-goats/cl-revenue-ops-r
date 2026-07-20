#!/usr/bin/env python3
"""Validate and replay one frozen, local fee-capture window."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable


EXIT_EXACT = 0
EXIT_GATE_FAILED = 1
EXIT_INPUT = 2
MIN_CYCLES = 6
MIN_EVALUATIONS = 100
MIN_ADJUSTMENTS = 10


class WindowError(ValueError):
    """A frozen window or local runner input is malformed."""


def _parse_args(argv: Iterable[str]) -> tuple[Path, Path, Path]:
    values: dict[str, Path] = {}
    args = iter(argv)
    for flag in args:
        if flag not in {"--manifest", "--capture-dir", "--replay-bin"}:
            raise WindowError(f"unknown argument {flag!r}")
        if flag in values:
            raise WindowError(f"duplicate argument {flag!r}")
        try:
            value = next(args)
        except StopIteration as error:
            raise WindowError(f"{flag} requires one local path") from error
        if value.startswith("--"):
            raise WindowError(f"{flag} requires one local path")
        values[flag] = Path(value)
    missing = [
        flag
        for flag in ("--manifest", "--capture-dir", "--replay-bin")
        if flag not in values
    ]
    if missing:
        raise WindowError(
            "expected --manifest <file> --capture-dir <dir> --replay-bin <file>; "
            f"missing {', '.join(missing)}"
        )
    return values["--manifest"], values["--capture-dir"], values["--replay-bin"]


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise WindowError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise WindowError(f"{label} {path} must contain one JSON object")
    return value


def _required_int(obj: dict[str, Any], field: str, *, minimum: int = 0) -> int:
    value = obj.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise WindowError(f"{field} must be an integer >= {minimum}")
    return value


def _required_string(obj: dict[str, Any], field: str) -> str:
    value = obj.get(field)
    if not isinstance(value, str) or not value:
        raise WindowError(f"{field} must be a nonempty string")
    return value


def _resolve_file(path: Path, label: str, *, executable: bool = False) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise WindowError(f"cannot open {label} {path}: {error}") from error
    if not resolved.is_file():
        raise WindowError(f"{label} {resolved} is not a regular file")
    if executable and not os.access(resolved, os.X_OK):
        raise WindowError(f"{label} {resolved} is not executable")
    return resolved


def _resolve_dir(path: Path) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise WindowError(f"cannot open capture directory {path}: {error}") from error
    if not resolved.is_dir():
        raise WindowError(f"capture directory {resolved} is not a directory")
    return resolved


def _capture_path(capture_dir: Path, filename: Any) -> Path:
    if (
        not isinstance(filename, str)
        or not filename
        or Path(filename).name != filename
        or filename in {".", ".."}
    ):
        raise WindowError(f"capture filename {filename!r} must be one local basename")
    resolved = _resolve_file(capture_dir / filename, "capture")
    try:
        resolved.relative_to(capture_dir)
    except ValueError as error:
        raise WindowError(
            f"capture {resolved} escapes the explicit capture directory"
        ) from error
    return resolved


def _preflight(
    manifest_path: Path, capture_dir: Path
) -> tuple[dict[str, Any], list[Path], dict[str, int]]:
    manifest = _load_object(manifest_path, "manifest")
    if manifest.get("schema_name") != "fee_cycle_capture_manifest":
        raise WindowError("schema_name must be 'fee_cycle_capture_manifest'")
    if manifest.get("schema_version") != 0:
        raise WindowError("schema_version must be 0")
    if manifest.get("state") != "closed":
        raise WindowError(f"state must be 'closed', got {manifest.get('state')!r}")
    if manifest.get("queue_drained") is not True:
        raise WindowError("queue_drained must be true")
    if _required_int(manifest, "failed") != 0:
        raise WindowError("failed must be zero")
    if _required_int(manifest, "dropped") != 0:
        raise WindowError("dropped must be zero")

    run_id = _required_string(manifest, "capture_run_id")
    attempts = manifest.get("attempts")
    if not isinstance(attempts, list):
        raise WindowError("attempts must be an array")
    sequences: list[int] = []
    paths: list[Path] = []
    evaluation_count = 0
    adjustment_count = 0
    for index, attempt in enumerate(attempts):
        if not isinstance(attempt, dict):
            raise WindowError(f"attempts[{index}] must be an object")
        sequence = _required_int(attempt, "capture_seq", minimum=1)
        if attempt.get("status") != "completed":
            raise WindowError(f"attempts[{index}].status must be 'completed'")
        if attempt.get("eligible") is not True:
            raise WindowError(f"attempts[{index}].eligible must be true")
        path = _capture_path(capture_dir, attempt.get("filename"))
        capture = _load_object(path, "capture")
        if capture.get("capture_run_id") != run_id:
            raise WindowError(
                f"capture {path} run ID does not match manifest capture_run_id"
            )
        if capture.get("capture_seq") != sequence:
            raise WindowError(
                f"capture {path} sequence does not match manifest attempt"
            )
        cycle_id = f"{run_id}:{sequence:08}"
        if capture.get("cycle_id") != cycle_id or attempt.get("cycle_id") != cycle_id:
            raise WindowError(
                f"capture sequence {sequence} does not have canonical cycle_id"
            )
        completeness = capture.get("completeness")
        if not isinstance(completeness, dict):
            raise WindowError(f"capture {path} completeness must be an object")
        if completeness.get("complete") is not True:
            raise WindowError(f"capture {path} completeness.complete must be true")
        evaluations = _required_int(completeness, "evaluated_channels")
        outcomes = capture.get("expected", {}).get("ordered_outcomes")
        if not isinstance(outcomes, list) or len(outcomes) != evaluations:
            raise WindowError(
                f"capture {path} ordered_outcomes must match evaluated_channels"
            )
        adjustments = sum(
            1
            for outcome in outcomes
            if isinstance(outcome, dict) and "adjustment" in outcome
        )
        sequences.append(sequence)
        paths.append(path)
        evaluation_count += evaluations
        adjustment_count += adjustments

    if sequences:
        expected_sequences = list(range(sequences[0], sequences[-1] + 1))
        if sequences != expected_sequences:
            raise WindowError(
                f"retained capture sequences must be strictly consecutive: {sequences}"
            )
    retained = manifest.get("retained_sequence_range")
    if not isinstance(retained, dict):
        raise WindowError("retained_sequence_range must be an object")
    expected_first = sequences[0] if sequences else None
    expected_last = sequences[-1] if sequences else None
    if retained.get("first") != expected_first or retained.get("last") != expected_last:
        raise WindowError(
            "retained_sequence_range must match the selected consecutive attempts"
        )

    return (
        manifest,
        paths,
        {
            "cycles": len(paths),
            "evaluations": evaluation_count,
            "adjustments": adjustment_count,
        },
    )


def _thresholds(counts: dict[str, int]) -> dict[str, dict[str, Any]]:
    minimums = {
        "cycles": MIN_CYCLES,
        "evaluations": MIN_EVALUATIONS,
        "adjustments": MIN_ADJUSTMENTS,
    }
    return {
        name: {
            "actual": counts[name],
            "minimum": minimum,
            "met": counts[name] >= minimum,
        }
        for name, minimum in minimums.items()
    }


def _base_verdict(
    *,
    status: str,
    error: str | None = None,
    run_id: str | None = None,
    counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    counts = counts or {"cycles": 0, "evaluations": 0, "adjustments": 0}
    return {
        "status": status,
        "commit": None,
        "run_id": run_id,
        "capture_count": counts["cycles"],
        "evaluated_channel_count": counts["evaluations"],
        "adjustment_count": counts["adjustments"],
        "mismatch_count": 0,
        "thresholds": _thresholds(counts),
        "replay_exit_code": None,
        "results": [],
        "error": error,
    }


def _emit(verdict: dict[str, Any]) -> None:
    print(json.dumps(verdict, sort_keys=True, separators=(",", ":")))


def main(argv: list[str] | None = None) -> int:
    try:
        manifest_arg, capture_dir_arg, replay_bin_arg = _parse_args(
            sys.argv[1:] if argv is None else argv
        )
        manifest_path = _resolve_file(manifest_arg, "manifest")
        capture_dir = _resolve_dir(capture_dir_arg)
        replay_bin = _resolve_file(replay_bin_arg, "replay binary", executable=True)
        manifest, _capture_paths, counts = _preflight(manifest_path, capture_dir)
    except WindowError as error:
        _emit(_base_verdict(status="error", error=str(error)))
        return EXIT_INPUT

    thresholds = _thresholds(counts)
    if not all(gate["met"] for gate in thresholds.values()):
        verdict = _base_verdict(
            status="insufficient_window",
            error="capture window does not meet the minimum parity thresholds",
            run_id=manifest["capture_run_id"],
            counts=counts,
        )
        verdict["thresholds"] = thresholds
        _emit(verdict)
        return EXIT_GATE_FAILED

    completed = subprocess.run(
        [
            str(replay_bin),
            "--manifest",
            str(manifest_path),
            "--capture-dir",
            str(capture_dir),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    lines = completed.stdout.splitlines()
    if completed.stderr or len(lines) != 1:
        verdict = _base_verdict(
            status="error",
            error="replay binary must emit exactly one JSON object on stdout and no stderr",
            run_id=manifest["capture_run_id"],
            counts=counts,
        )
        verdict["replay_exit_code"] = completed.returncode
        _emit(verdict)
        return EXIT_INPUT
    try:
        replay = json.loads(lines[0])
    except json.JSONDecodeError as error:
        verdict = _base_verdict(
            status="error",
            error=f"replay binary emitted invalid JSON: {error}",
            run_id=manifest["capture_run_id"],
            counts=counts,
        )
        verdict["replay_exit_code"] = completed.returncode
        _emit(verdict)
        return EXIT_INPUT
    if not isinstance(replay, dict):
        verdict = _base_verdict(
            status="error",
            error="replay binary verdict must be one JSON object",
            run_id=manifest["capture_run_id"],
            counts=counts,
        )
        verdict["replay_exit_code"] = completed.returncode
        _emit(verdict)
        return EXIT_INPUT

    expected = {
        "run_id": manifest["capture_run_id"],
        "capture_count": counts["cycles"],
        "evaluated_channel_count": counts["evaluations"],
        "adjustment_count": counts["adjustments"],
    }
    inconsistencies = [
        f"{field} expected {value!r}, got {replay.get(field)!r}"
        for field, value in expected.items()
        if replay.get(field) != value
    ]
    if inconsistencies:
        verdict = _base_verdict(
            status="error",
            error="replay summary disagrees with frozen window: "
            + "; ".join(inconsistencies),
            run_id=manifest["capture_run_id"],
            counts=counts,
        )
        verdict["replay_exit_code"] = completed.returncode
        _emit(verdict)
        return EXIT_INPUT

    verdict = _base_verdict(
        status="error",
        error=replay.get("error"),
        run_id=manifest["capture_run_id"],
        counts=counts,
    )
    for field in ("commit", "mismatch_count", "results"):
        verdict[field] = replay.get(field)
    verdict["thresholds"] = thresholds
    verdict["replay_exit_code"] = completed.returncode

    if completed.returncode == EXIT_EXACT and replay.get("mismatch_count") == 0:
        verdict["status"] = "exact"
        verdict["error"] = None
        _emit(verdict)
        return EXIT_EXACT
    if completed.returncode == EXIT_GATE_FAILED:
        verdict["status"] = "mismatch"
        _emit(verdict)
        return EXIT_GATE_FAILED
    if completed.returncode == EXIT_INPUT:
        verdict["status"] = "error"
        _emit(verdict)
        return EXIT_INPUT

    verdict["status"] = "error"
    verdict["error"] = f"replay binary returned unsupported exit code {completed.returncode}"
    _emit(verdict)
    return EXIT_INPUT


if __name__ == "__main__":
    raise SystemExit(main())
