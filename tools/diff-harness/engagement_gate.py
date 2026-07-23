#!/usr/bin/env python3
# tools/diff-harness/engagement_gate.py
"""Live decision-surface engagement gate (runway doc
`docs/superpowers/specs/2026-07-20-rust-fee-cutover-runway-design.md`,
section "Live decision-surface engagement gate", added 2026-07-23).

Measures whether the Rust shadow dry-run's fee engine is actually
EXERCISED live, against Python's applied fee changes over the same
window. Exists because the pre-fix shadow held 75% of non-sleeping rows
at `waiting_window` (1 Rust would-broadcast vs 247 Python fee changes
over the first soak day) while every parity suite stayed green — a soak
can validate the gate instead of the engine, and this measurement is the
guard against that.

Usage (timers / manual, once per soak day):

    ./tools/diff-harness/engagement_gate.py --node lnnode \\
        --since <first-comparable-cycle-ts> [--until <ts>]

`--since` must be the FIRST COMPARABLE cycle timestamp for the candidate
under soak — exclude the documented bootstrap cycle (the first cycle
after any plugin (re)start runs with an empty `skip_gate_prev` and is
gate-held non-comparable by design). Per-row bootstrap artifacts are
additionally excluded via the trace's `skip_gate_comparable: false`
marker (same contract as `diff_fee_decisions.py`).

The three gate metrics, verbatim from the runway doc:

  1. STARVATION — `waiting_window` share of non-sleeping rows:
     green < 20%, red > 50%, yellow between.
  2. RATE — Rust would-broadcast count within 0.5x-2.0x of Python's
     applied (non-manual) `fee_changes` count over the same window:
     in-band green, out-of-band red. Fewer than MIN_PYTHON_SAMPLE
     Python changes is YELLOW (insufficient natural occurrences —
     runway "Yellow gates": never counts as green soak).
  3. FLAPPER — any channel Python adjusted in >= FLAPPER_RUN_LENGTH
     consecutive cycles must show at least one non-`waiting_window`
     Rust evaluation inside that span: violation red, none green.

Rust adjustment classification follows `diff_fee_decisions.py`'s binding
T10 contract: a would-be broadcast iff `would_broadcast is True` AND
`algorithm_values is not None` — never by `reason_code` alone.

Exit codes (timers key off these): 0 = GREEN (all metrics green),
3 = YELLOW (no red, at least one yellow — does NOT count as green soak),
1 = RED (any metric red), 2 = transport failure (ssh/sqlite/journal
unreadable or unparseable). A cadence gap > CADENCE_GAP_MAX_SECONDS
between consecutive cycles is reported and yellows the run (missed
sample — runway "Yellow gates").
"""

import argparse
import json
import shlex
import subprocess
import sys

DEFAULT_JOURNAL = "/data/lightningd/.lightning/fee_dryrun_journal.jsonl"
DEFAULT_PYTHON_DB = "/data/lightningd/.lightning/revenue_ops.db"

# Runway-doc thresholds (keep in sync with the gate section).
STARVATION_GREEN_BELOW = 0.20
STARVATION_RED_ABOVE = 0.50
RATE_BAND_LOW = 0.5
RATE_BAND_HIGH = 2.0
FLAPPER_RUN_LENGTH = 5
# "Insufficient natural occurrences" floor for the RATE metric: below
# this many Python changes the ratio is noise, not signal.
MIN_PYTHON_SAMPLE = 10
# Python's fee cycle is ~30 min flush-to-flush; a consecutive-cycle gap
# beyond this is a missed sample (yellow), matching the shadow watcher's
# stale threshold of 2x fee_interval.
CADENCE_GAP_MAX_SECONDS = 90 * 60
# Two Python fee_changes on the same channel belong to the same
# consecutive-cycle run when their timestamps are within this gap
# (one fee interval plus scheduling slack).
FLAPPER_CONSECUTIVE_GAP_SECONDS = 45 * 60

GREEN, YELLOW, RED = "GREEN", "YELLOW", "RED"


def read_journal(node, path):
    """Raw JSONL via `ssh <node> cat <path>`; CalledProcessError is a
    transport failure (same contract as diff_fee_decisions.py)."""
    return subprocess.run(["ssh", node, "cat", path],
                          capture_output=True, text=True, check=True).stdout


def sqlite_json(node, db_path, query):
    """Read-only `-json` sqlite over ssh; blank stdout == empty result
    set (see diff_fee_decisions.py's sqlite_json docstring)."""
    remote_cmd = f"sqlite3 -readonly {shlex.quote(db_path)} {shlex.quote(query)} -json"
    return subprocess.run(["ssh", node, remote_cmd],
                          capture_output=True, text=True, check=True).stdout


def parse_journal(raw):
    """JSONL -> decision dicts; malformed line raises (transport-class)."""
    decisions = []
    for line in raw.splitlines():
        line = line.strip()
        if line:
            decisions.append(json.loads(line))
    return decisions


def cycle_ts(decision):
    """The cycle timestamp embedded in `cycle_id` ("fee-dryrun-<ts>"),
    or None for an unrecognized id."""
    cid = decision.get("cycle_id") or ""
    try:
        return int(cid.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


def is_adjustment(decision):
    return (decision.get("would_broadcast") is True
            and decision.get("algorithm_values") is not None)


def is_non_comparable(decision):
    trace = decision.get("trace")
    return isinstance(trace, dict) and trace.get("skip_gate_comparable") is False


def disposition(decision):
    trace = decision.get("trace")
    return trace.get("disposition") if isinstance(trace, dict) else None


def fetch_python_changes(sqlite_fn, node, python_db, since_ts, until_ts):
    where = f"WHERE timestamp >= {int(since_ts)} AND manual = 0"
    if until_ts is not None:
        where += f" AND timestamp <= {int(until_ts)}"
    query = ("SELECT channel_id, timestamp FROM fee_changes "
             f"{where} ORDER BY channel_id, timestamp")
    raw = sqlite_fn(node, python_db, query).strip()
    return json.loads(raw) if raw else []


def collect_cycles(decisions, since_ts, until_ts):
    """Rust journal rows grouped by cycle ts, comparable rows only,
    within [since_ts, until_ts]."""
    cycles = {}
    excluded_non_comparable = 0
    for d in decisions:
        ts = cycle_ts(d)
        if ts is None or ts < since_ts or (until_ts is not None and ts > until_ts):
            continue
        if is_non_comparable(d):
            excluded_non_comparable += 1
            continue
        cycles.setdefault(ts, []).append(d)
    return cycles, excluded_non_comparable


def measure_starvation(cycles):
    non_sleeping = 0
    waiting = 0
    for rows in cycles.values():
        for d in rows:
            if disposition(d) == "sleeping_hold":
                continue
            non_sleeping += 1
            if disposition(d) == "waiting_window":
                waiting += 1
    share = (waiting / non_sleeping) if non_sleeping else 0.0
    if non_sleeping == 0:
        verdict = YELLOW  # nothing evaluated at all: insufficient sample
    elif share < STARVATION_GREEN_BELOW:
        verdict = GREEN
    elif share > STARVATION_RED_ABOVE:
        verdict = RED
    else:
        verdict = YELLOW
    return {"verdict": verdict, "waiting": waiting,
            "non_sleeping": non_sleeping, "share": share}


def measure_rate(cycles, python_changes):
    rust_broadcasts = sum(1 for rows in cycles.values()
                          for d in rows if is_adjustment(d))
    python_count = len(python_changes)
    ratio = (rust_broadcasts / python_count) if python_count else None
    if python_count < MIN_PYTHON_SAMPLE:
        verdict = YELLOW
    elif RATE_BAND_LOW <= ratio <= RATE_BAND_HIGH:
        verdict = GREEN
    else:
        verdict = RED
    return {"verdict": verdict, "rust_broadcasts": rust_broadcasts,
            "python_changes": python_count, "ratio": ratio}


def flapper_runs(python_changes):
    """Spans (channel_id, start_ts, end_ts, length) where one channel's
    consecutive Python changes (gap <= FLAPPER_CONSECUTIVE_GAP_SECONDS)
    reach FLAPPER_RUN_LENGTH."""
    by_channel = {}
    for row in python_changes:
        by_channel.setdefault(row["channel_id"], []).append(int(row["timestamp"]))
    runs = []
    for channel_id, stamps in by_channel.items():
        stamps.sort()
        start = 0
        for i in range(1, len(stamps) + 1):
            ended = (i == len(stamps)
                     or stamps[i] - stamps[i - 1] > FLAPPER_CONSECUTIVE_GAP_SECONDS)
            if ended:
                length = i - start
                if length >= FLAPPER_RUN_LENGTH:
                    runs.append((channel_id, stamps[start], stamps[i - 1], length))
                start = i
    return runs


def measure_flappers(cycles, python_changes):
    runs = flapper_runs(python_changes)
    violations = []
    for channel_id, start_ts, end_ts, length in runs:
        evaluated = False
        for ts, rows in cycles.items():
            if ts < start_ts or ts > end_ts:
                continue
            for d in rows:
                if (d.get("channel_id") == channel_id
                        and disposition(d) not in ("waiting_window", "sleeping_hold")):
                    evaluated = True
                    break
            if evaluated:
                break
        if not evaluated:
            violations.append((channel_id, start_ts, end_ts, length))
    verdict = RED if violations else GREEN
    return {"verdict": verdict, "runs": len(runs), "violations": violations}


def measure_cadence(cycles):
    stamps = sorted(cycles)
    max_gap = 0
    for a, b in zip(stamps, stamps[1:]):
        max_gap = max(max_gap, b - a)
    verdict = YELLOW if max_gap > CADENCE_GAP_MAX_SECONDS else GREEN
    return {"verdict": verdict, "cycles": len(stamps), "max_gap_seconds": max_gap}


def overall_verdict(metrics):
    verdicts = [m["verdict"] for m in metrics]
    if RED in verdicts:
        return RED
    if YELLOW in verdicts:
        return YELLOW
    return GREEN


def run_gate(journal_fn, sqlite_fn, node, journal_path, python_db,
             since_ts, until_ts):
    decisions = parse_journal(journal_fn(node, journal_path))
    python_changes = fetch_python_changes(sqlite_fn, node, python_db,
                                          since_ts, until_ts)
    cycles, excluded = collect_cycles(decisions, since_ts, until_ts)
    starvation = measure_starvation(cycles)
    rate = measure_rate(cycles, python_changes)
    flappers = measure_flappers(cycles, python_changes)
    cadence = measure_cadence(cycles)
    return {
        "starvation": starvation,
        "rate": rate,
        "flappers": flappers,
        "cadence": cadence,
        "excluded_non_comparable": excluded,
        "overall": overall_verdict([starvation, rate, flappers, cadence]),
    }


def report(results):
    s, r, f, c = (results["starvation"], results["rate"],
                  results["flappers"], results["cadence"])
    ratio = "n/a" if r["ratio"] is None else f"{r['ratio']:.2f}x"
    print(f"cycles measured          {c['cycles']} "
          f"(max gap {c['max_gap_seconds'] // 60} min)   [{c['verdict']}]")
    print(f"non-comparable excluded  {results['excluded_non_comparable']} rows")
    print(f"1 STARVATION  waiting_window {s['waiting']}/{s['non_sleeping']} "
          f"non-sleeping = {s['share']:.1%}   [{s['verdict']}]")
    print(f"2 RATE        rust would-broadcast {r['rust_broadcasts']} vs "
          f"python {r['python_changes']} = {ratio}   [{r['verdict']}]")
    print(f"3 FLAPPER     {f['runs']} python runs >= {FLAPPER_RUN_LENGTH} "
          f"cycles, {len(f['violations'])} unevaluated   [{f['verdict']}]")
    for channel_id, start_ts, end_ts, length in f["violations"]:
        print(f"    VIOLATION {channel_id}: {length} python changes "
              f"{start_ts}..{end_ts} with zero rust evaluations")
    print(f"OVERALL: {results['overall']}")


# ---------------------------------------------------------------------------
# Self-test (stubbed journal + sqlite; no node access)
# ---------------------------------------------------------------------------

def _row(cycle, channel="100x1x0", dispo="broadcast", would_broadcast=False,
         algorithm_values=None, comparable=True):
    trace = {"disposition": dispo}
    if not comparable:
        trace["skip_gate_comparable"] = False
    return {"channel_id": channel, "cycle_id": f"fee-dryrun-{cycle}",
            "would_broadcast": would_broadcast,
            "algorithm_values": algorithm_values, "trace": trace}


def _journal_fn(rows):
    return lambda node, path: "\n".join(json.dumps(r) for r in rows)


def _sqlite_fn(rows):
    return lambda node, db, query: json.dumps(rows)


def _py_changes(channel, stamps):
    return [{"channel_id": channel, "timestamp": ts} for ts in stamps]


def self_test():
    base = 1_000_000
    cycle_stamps = [base + i * 1800 for i in range(12)]

    # GREEN: every cycle fully evaluated, 1 broadcast per cycle, python
    # ~1 change per cycle, flapper run present AND evaluated.
    journal = []
    for ts in cycle_stamps:
        journal.append(_row(ts, "100x1x0", "broadcast", True, {"k": 1}))
        journal.append(_row(ts, "200x1x0", "alpha_guard"))
        journal.append(_row(ts, "300x1x0", "sleeping_hold"))
    python = _py_changes("100x1x0", cycle_stamps)  # 12-cycle flapper run
    results = run_gate(_journal_fn(journal), _sqlite_fn(python), "n", "j", "db",
                       base, None)
    assert results["overall"] == GREEN, results
    assert results["starvation"]["share"] == 0.0
    assert results["rate"]["ratio"] == 1.0
    assert results["flappers"]["runs"] == 1 and not results["flappers"]["violations"]

    # RED starvation + RED rate + flapper violation: the pre-fix shadow
    # shape — everything waiting_window, no broadcasts.
    journal = []
    for ts in cycle_stamps:
        journal.append(_row(ts, "100x1x0", "waiting_window"))
        journal.append(_row(ts, "200x1x0", "waiting_window"))
        journal.append(_row(ts, "300x1x0", "sleeping_hold"))
    results = run_gate(_journal_fn(journal), _sqlite_fn(python), "n", "j", "db",
                       base, None)
    assert results["overall"] == RED, results
    assert results["starvation"]["verdict"] == RED
    assert results["rate"]["verdict"] == RED and results["rate"]["ratio"] == 0.0
    assert results["flappers"]["violations"], results["flappers"]

    # YELLOW: insufficient python sample (below MIN_PYTHON_SAMPLE).
    journal = [_row(ts, "100x1x0", "alpha_guard") for ts in cycle_stamps]
    results = run_gate(_journal_fn(journal),
                       _sqlite_fn(_py_changes("100x1x0", cycle_stamps[:3])),
                       "n", "j", "db", base, None)
    assert results["rate"]["verdict"] == YELLOW
    assert results["overall"] == YELLOW, results

    # Non-comparable rows are excluded from every metric (bootstrap).
    journal = [_row(cycle_stamps[0], "100x1x0", "waiting_window",
                    comparable=False)]
    journal += [_row(ts, "100x1x0", "broadcast", True, {"k": 1})
                for ts in cycle_stamps[1:]]
    results = run_gate(_journal_fn(journal), _sqlite_fn(python), "n", "j", "db",
                       base, None)
    assert results["excluded_non_comparable"] == 1
    assert results["starvation"]["share"] == 0.0

    # --since excludes earlier cycles entirely.
    results = run_gate(_journal_fn(journal), _sqlite_fn([]), "n", "j", "db",
                       cycle_stamps[6], None)
    assert results["cadence"]["cycles"] == 6

    # Cadence gap > CADENCE_GAP_MAX_SECONDS yellows the run.
    gappy = [_row(base, "100x1x0", "broadcast", True, {"k": 1}),
             _row(base + 2 * CADENCE_GAP_MAX_SECONDS, "100x1x0", "broadcast",
                  True, {"k": 1})]
    results = run_gate(_journal_fn(gappy), _sqlite_fn(python), "n", "j", "db",
                       base, None)
    assert results["cadence"]["verdict"] == YELLOW

    # Flapper run boundary: a gap beyond FLAPPER_CONSECUTIVE_GAP_SECONDS
    # splits runs; two 4-cycle halves never reach FLAPPER_RUN_LENGTH.
    split = cycle_stamps[:4] + [cycle_stamps[3] + 4 * 3600 + i * 1800
                                for i in range(1, 5)]
    assert flapper_runs(_py_changes("100x1x0", split)) == []

    print("self-test OK")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", default="lnnode")
    parser.add_argument("--journal", default=DEFAULT_JOURNAL)
    parser.add_argument("--python-db", default=DEFAULT_PYTHON_DB)
    parser.add_argument("--since", type=int,
                        help="first COMPARABLE cycle ts for the candidate "
                             "(exclude the restart bootstrap cycle)")
    parser.add_argument("--until", type=int, default=None)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    if args.since is None:
        parser.error("--since is required (first comparable cycle ts)")

    try:
        results = run_gate(read_journal, sqlite_json, args.node, args.journal,
                           args.python_db, args.since, args.until)
    except (subprocess.CalledProcessError, json.JSONDecodeError, OSError) as exc:
        print(f"TRANSPORT FAILURE: {exc}", file=sys.stderr)
        return 2

    report(results)
    return {"GREEN": 0, "YELLOW": 3, "RED": 1}[results["overall"]]


if __name__ == "__main__":
    sys.exit(main())
