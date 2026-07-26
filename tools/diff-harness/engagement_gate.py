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
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile

DEFAULT_JOURNAL = "/data/lightningd/.lightning/fee_dryrun_journal.jsonl"
DEFAULT_PYTHON_DB = "/data/lightningd/.lightning/revenue_ops.db"
# The Rust-owned observer db lives on the node, not in this repo -- unlike
# --journal/--python-db (read remotely via ssh+sqlite3), --source sqlite
# opens --observer-db directly as a local filesystem path (see
# fetch_sqlite_rows()'s docstring): the caller is responsible for handing
# it a path this process can actually open (e.g. a local mount or a
# fetched copy), any path works. This default names the SAME file
# diff_read_rpcs.py's --observer-db documents, for consistency only.
DEFAULT_OBSERVER_DB = "/data/lightningd/.lightning/revops-r-observer.db"

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


def _decision_from_sqlite_row(row):
    """Map one `rust_fee_shadow_outcomes` row (the column contract:
    cycle_ts, channel_id, would_broadcast, has_algorithm_values,
    disposition, skip_gate_comparable -- task-R7-brief.md) into EXACTLY
    the decision-dict shape `collect_cycles()` (and everything downstream
    of it) already consumes from the JSONL journal, so no metric logic
    forks on source.

    `cycle_id` is synthesized from `cycle_ts` (not read from the table's
    own nullable `cycle_id` column, which isn't part of the column
    contract and isn't guaranteed to encode a parseable timestamp) purely
    so `cycle_ts()` extracts the same value it would from a real journal
    line's "fee-dryrun-<ts>" id.

    `has_algorithm_values` (a 0/1 presence flag) maps to a truthy
    placeholder dict-or-None -- `is_adjustment()` only ever tests
    `algorithm_values is not None`, it never reads *into* the value, so
    the placeholder is behaviorally identical to a real journal line's
    populated dict.

    A NULL `disposition` is stored as `trace["disposition"] = None`,
    which `disposition()`'s `trace.get("disposition")` returns as `None`
    either way -- identical to a JSONL trace that omits the key entirely
    (ambiguity ruling: no invented sentinel).

    `skip_gate_comparable` follows the JSONL convention exactly: the key
    is present (and False) only when the row is NOT comparable; a
    comparable row leaves it out, matching `_row()`'s own convention and
    `is_non_comparable()`'s `trace.get(...) is False` check.
    """
    trace = {"disposition": row["disposition"]}
    if not bool(row["skip_gate_comparable"]):
        trace["skip_gate_comparable"] = False
    return {
        "channel_id": row["channel_id"],
        "cycle_id": f"fee-dryrun-{row['cycle_ts']}",
        "would_broadcast": bool(row["would_broadcast"]),
        "algorithm_values": ({} if row["has_algorithm_values"] else None),
        "trace": trace,
    }


def fetch_sqlite_rows(observer_db, since_ts, until_ts):
    """Read shadow-cycle-outcome rows from the Rust-owned sqlite store
    (the `rust_fee_shadow_outcomes` table `crates/revops-db/src/
    fee_runway.rs` owns) and map them into the decision-dict shape
    `collect_cycles()` consumes -- see `_decision_from_sqlite_row()`.

    `observer_db` is a plain filesystem path opened READ-ONLY (`mode=ro`
    URI) directly via the stdlib `sqlite3` module -- unlike `--journal`/
    `--python-db`, which stay ssh-remote-on-`--node` (JSONL mode is
    unchanged), the Rust-owned observer db is read locally: the caller is
    responsible for handing this a path the process can actually open
    (a local mount, a synced copy, or a literal local path when this tool
    runs on the node itself); this function works with ANY such path, it
    does not assume a fixed location.

    Raises `sqlite3.Error` (missing file, missing table, corrupt db --
    verified: `mode=ro` on a nonexistent path raises at `connect()` time,
    a missing table raises `OperationalError` at `execute()` time) --
    both are transport-class failures, caught the same way a
    `subprocess.CalledProcessError` from the ssh-based fetches is.
    """
    uri = f"file:{observer_db}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        conn.row_factory = sqlite3.Row
        where = "cycle_ts >= ?"
        params = [int(since_ts)]
        if until_ts is not None:
            where += " AND cycle_ts <= ?"
            params.append(int(until_ts))
        query = ("SELECT cycle_ts, channel_id, would_broadcast, "
                 "has_algorithm_values, disposition, skip_gate_comparable "
                 f"FROM rust_fee_shadow_outcomes WHERE {where} "
                 "ORDER BY cycle_ts, channel_id")
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()
    return [_decision_from_sqlite_row(dict(r)) for r in rows]


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
             since_ts, until_ts, source="jsonl", observer_db=None):
    """`source` selects where the Rust-side decisions come from -- the
    JSONL journal (default, unchanged) or the Rust-owned sqlite store
    (`observer_db`, via `fetch_sqlite_rows()`). This is the ONLY branch
    point: `collect_cycles()` and every metric below it run identically
    regardless of which source produced the decision dicts."""
    if source == "jsonl":
        decisions = parse_journal(journal_fn(node, journal_path))
    elif source == "sqlite":
        decisions = fetch_sqlite_rows(observer_db, since_ts, until_ts)
    else:
        raise ValueError(f"unknown --source: {source!r} (expected 'jsonl' or 'sqlite')")
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


def _unused_journal_fn(node, path):
    """Passed wherever a scenario asserts source='sqlite' never touches
    the journal at all (no metric logic may fork on source, but the
    ACQUISITION step must genuinely pick one or the other)."""
    raise AssertionError("journal_fn must not be called when source='sqlite'")


def _outcome_row(cycle, channel="100x1x0", dispo="broadcast", would_broadcast=False,
                  algorithm_values=None, comparable=True):
    """Mirrors `_row()`'s exact scenario-building signature (same
    defaults, same argument order) but returns a `rust_fee_shadow_outcomes`
    insert tuple -- (cycle_ts, channel_id, would_broadcast,
    has_algorithm_values, disposition, skip_gate_comparable), the column
    contract from task-R7-brief.md -- instead of a JSONL decision dict.
    Used to build the SAME self-test scenarios `_row()` builds against a
    real temp sqlite db, so both sources can be asserted to reach
    identical verdicts from identical scenario inputs."""
    return (cycle, channel, int(bool(would_broadcast)),
            int(algorithm_values is not None), dispo, int(bool(comparable)))


def _build_observer_db(path, rows):
    """Create a real temp sqlite file shaped like the Rust-owned
    `rust_fee_shadow_outcomes` shadow-cycle-outcome table (schema owned by
    crates/revops-db/src/fee_runway.rs) and insert `rows` (as produced by
    `_outcome_row()`). Returns `path`. Deliberately omits the `cycle_id`
    FK/UNIQUE constraints the real DDL carries -- the column CONTRACT this
    tool binds to is just the 6 contract columns, not the full table
    shape ("however the table is otherwise shaped", task-R7-brief.md)."""
    conn = sqlite3.connect(path)
    try:
        conn.execute("""
            CREATE TABLE rust_fee_shadow_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id TEXT,
                cycle_ts INTEGER NOT NULL,
                channel_id TEXT NOT NULL,
                would_broadcast INTEGER NOT NULL,
                has_algorithm_values INTEGER NOT NULL,
                disposition TEXT,
                skip_gate_comparable INTEGER NOT NULL
            )
        """)
        conn.executemany(
            "INSERT INTO rust_fee_shadow_outcomes "
            "(cycle_ts, channel_id, would_broadcast, has_algorithm_values, "
            "disposition, skip_gate_comparable) VALUES (?, ?, ?, ?, ?, ?)",
            rows)
        conn.commit()
    finally:
        conn.close()
    return path


def _assert_sources_agree(specs, python_changes, since_ts, until_ts, tmp_dir, name):
    """Build the SAME scenario (`specs`, a list of `_row()`/`_outcome_row()`
    kwargs dicts) against both sources and assert the two verdict dicts
    are byte-for-byte identical -- the brief's binding requirement that
    zero metric logic forks on source. Returns the shared results dict."""
    journal = [_row(**s) for s in specs]
    jsonl_results = run_gate(_journal_fn(journal), _sqlite_fn(python_changes),
                             "n", "j", "db", since_ts, until_ts)

    db_path = _build_observer_db(os.path.join(tmp_dir, f"{name}.db"),
                                 [_outcome_row(**s) for s in specs])
    sqlite_results = run_gate(_unused_journal_fn, _sqlite_fn(python_changes),
                              "n", "j", "db", since_ts, until_ts,
                              source="sqlite", observer_db=db_path)
    assert jsonl_results == sqlite_results, (name, jsonl_results, sqlite_results)
    return jsonl_results


def self_test():
    base = 1_000_000
    cycle_stamps = [base + i * 1800 for i in range(12)]

    with tempfile.TemporaryDirectory() as tmp_dir:
        # GREEN: every cycle fully evaluated, 1 broadcast per cycle, python
        # ~1 change per cycle, flapper run present AND evaluated.
        green_specs = []
        for ts in cycle_stamps:
            green_specs.append(dict(cycle=ts, channel="100x1x0", dispo="broadcast",
                                    would_broadcast=True, algorithm_values={"k": 1}))
            green_specs.append(dict(cycle=ts, channel="200x1x0", dispo="alpha_guard"))
            green_specs.append(dict(cycle=ts, channel="300x1x0", dispo="sleeping_hold"))
        python = _py_changes("100x1x0", cycle_stamps)  # 12-cycle flapper run
        results = _assert_sources_agree(green_specs, python, base, None,
                                        tmp_dir, "green")
        assert results["overall"] == GREEN, results
        assert results["starvation"]["share"] == 0.0
        assert results["rate"]["ratio"] == 1.0
        assert results["flappers"]["runs"] == 1 and not results["flappers"]["violations"]

        # RED starvation + RED rate + flapper violation: the pre-fix shadow
        # shape — everything waiting_window, no broadcasts.
        red_specs = []
        for ts in cycle_stamps:
            red_specs.append(dict(cycle=ts, channel="100x1x0", dispo="waiting_window"))
            red_specs.append(dict(cycle=ts, channel="200x1x0", dispo="waiting_window"))
            red_specs.append(dict(cycle=ts, channel="300x1x0", dispo="sleeping_hold"))
        results = _assert_sources_agree(red_specs, python, base, None, tmp_dir, "red")
        assert results["overall"] == RED, results
        assert results["starvation"]["verdict"] == RED
        assert results["rate"]["verdict"] == RED and results["rate"]["ratio"] == 0.0
        assert results["flappers"]["violations"], results["flappers"]

        # YELLOW: insufficient python sample (below MIN_PYTHON_SAMPLE).
        yellow_specs = [dict(cycle=ts, channel="100x1x0", dispo="alpha_guard")
                        for ts in cycle_stamps]
        low_sample_python = _py_changes("100x1x0", cycle_stamps[:3])
        results = _assert_sources_agree(yellow_specs, low_sample_python, base, None,
                                        tmp_dir, "yellow")
        assert results["rate"]["verdict"] == YELLOW
        assert results["overall"] == YELLOW, results

        # Non-comparable rows are excluded from every metric (bootstrap).
        nc_specs = [dict(cycle=cycle_stamps[0], channel="100x1x0",
                         dispo="waiting_window", comparable=False)]
        nc_specs += [dict(cycle=ts, channel="100x1x0", dispo="broadcast",
                         would_broadcast=True, algorithm_values={"k": 1})
                    for ts in cycle_stamps[1:]]
        results = _assert_sources_agree(nc_specs, python, base, None, tmp_dir,
                                        "non_comparable")
        assert results["excluded_non_comparable"] == 1
        assert results["starvation"]["share"] == 0.0

        # NULL disposition (rust_fee_shadow_outcomes.disposition IS
        # nullable) must map identically to the JSONL missing-disposition
        # shape (ambiguity ruling: no sentinel invented -- disposition()
        # returns None for both a trace that omits the key entirely and a
        # sqlite row whose disposition column is NULL). First, exercise it
        # through a full scenario on both sources via the shared
        # _outcome_row/_build_observer_db helpers, mixed with a
        # sleeping_hold row and a real broadcast so the None-disposition
        # row's categorization (non_sleeping, non-waiting) is asserted,
        # not just "doesn't crash".
        null_dispo_specs = [
            dict(cycle=cycle_stamps[0], channel="100x1x0", dispo=None),
            dict(cycle=cycle_stamps[0], channel="200x1x0", dispo="sleeping_hold"),
            dict(cycle=cycle_stamps[0], channel="300x1x0", dispo="broadcast",
                would_broadcast=True, algorithm_values={"k": 1}),
        ]
        results = _assert_sources_agree(
            null_dispo_specs, _py_changes("300x1x0", [cycle_stamps[0]]),
            cycle_stamps[0], None, tmp_dir, "null_disposition")
        # None is neither "sleeping_hold" nor "waiting_window": it counts
        # toward non_sleeping but not toward waiting (same rule a JSONL
        # trace with an absent/None disposition follows).
        assert results["starvation"]["non_sleeping"] == 2, results
        assert results["starvation"]["waiting"] == 0, results

        # Second, lock the "identical to the JSONL missing-disposition
        # shape" wording literally: disposition() must return None for
        # BOTH a hand-built trace that omits the "disposition" key
        # entirely (what a real journal line with no disposition field at
        # all looks like -- _row() always sets the key, even to None, so
        # this shape can only be constructed by hand) and a sqlite row
        # mapped through fetch_sqlite_rows() whose disposition column is
        # NULL.
        missing_key_decision = {"channel_id": "100x1x0",
                                "cycle_id": "fee-dryrun-0",
                                "would_broadcast": False,
                                "algorithm_values": None, "trace": {}}
        null_db_path = _build_observer_db(
            os.path.join(tmp_dir, "null_dispo_single.db"),
            [_outcome_row(0, "100x1x0", dispo=None)])
        sqlite_decision = fetch_sqlite_rows(null_db_path, 0, None)[0]
        assert disposition(missing_key_decision) is None
        assert disposition(sqlite_decision) is None
        assert disposition(missing_key_decision) == disposition(sqlite_decision)

        # `--source` must reject an unknown value rather than silently
        # falling back to the journal (or any other source).
        try:
            run_gate(_unused_journal_fn, _sqlite_fn(python), "n", "j", "db",
                     base, None, source="not-a-real-source")
            raised = None
        except ValueError as exc:
            raised = exc
        assert raised is not None, "run_gate must reject an unknown --source"

        # A missing observer-db FILE must raise (never a silent empty-
        # decisions green verdict) -- and the exception must be one of the
        # types main()'s top-level try/except actually catches (transport/
        # error exit 2).
        missing_path = os.path.join(tmp_dir, "does-not-exist.db")
        try:
            fetch_sqlite_rows(missing_path, base, None)
            raised = None
        except Exception as exc:  # noqa: BLE001 - deliberately broad, see assert below
            raised = exc
        assert raised is not None and isinstance(raised, MAIN_CAUGHT_EXCEPTIONS), \
            f"missing observer-db must raise a caught exception, got {raised!r}"

        # A db that EXISTS but is missing the rust_fee_shadow_outcomes table
        # (corrupt/mismigrated observer db) must raise the same way.
        no_table_path = os.path.join(tmp_dir, "no-table.db")
        conn = sqlite3.connect(no_table_path)
        conn.execute("CREATE TABLE unrelated (x INTEGER)")
        conn.commit()
        conn.close()
        try:
            fetch_sqlite_rows(no_table_path, base, None)
            raised = None
        except Exception as exc:  # noqa: BLE001
            raised = exc
        assert raised is not None and isinstance(raised, MAIN_CAUGHT_EXCEPTIONS), \
            f"missing table must raise a caught exception, got {raised!r}"

        # A file that EXISTS at the path but is not a sqlite database at
        # all (corrupt/truncated/garbage bytes -- e.g. a partial rsync, a
        # disk-full write, or a non-db file handed to --observer-db by
        # mistake) must raise the same way: the transport/error exit path,
        # never a silent empty-decisions green verdict.
        corrupt_path = os.path.join(tmp_dir, "corrupt.db")
        with open(corrupt_path, "wb") as fh:
            fh.write(b"not a sqlite file at all, just garbage bytes 1234567890")
        try:
            fetch_sqlite_rows(corrupt_path, base, None)
            raised = None
        except Exception as exc:  # noqa: BLE001
            raised = exc
        assert raised is not None and isinstance(raised, MAIN_CAUGHT_EXCEPTIONS), \
            f"corrupt observer-db must raise a caught exception, got {raised!r}"
        assert isinstance(raised, sqlite3.Error), \
            f"corrupt observer-db should raise a sqlite3.Error specifically, got {raised!r}"

        # --since excludes earlier cycles entirely.
        journal = [_row(**s) for s in nc_specs]
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


# Exceptions main() treats as a transport/error failure (exit 2), regardless
# of which source produced them: ssh/RPC nonzero exit or unreadable JSONL
# (subprocess.CalledProcessError, json.JSONDecodeError, OSError -- the
# original JSONL-path contract) or a sqlite3 failure opening/querying
# --observer-db (missing file, missing table, corrupt db -- see
# fetch_sqlite_rows()'s docstring). Shared with self_test()'s own
# error-handling assertions so the two never drift apart.
MAIN_CAUGHT_EXCEPTIONS = (subprocess.CalledProcessError, json.JSONDecodeError,
                         OSError, sqlite3.Error)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", default="lnnode")
    parser.add_argument("--journal", default=DEFAULT_JOURNAL)
    parser.add_argument("--python-db", default=DEFAULT_PYTHON_DB)
    parser.add_argument("--source", choices=["jsonl", "sqlite"], default="jsonl",
                        help="where Rust-side decisions come from: the JSONL "
                             "journal over ssh (default), or the Rust-owned "
                             "sqlite store at --observer-db (a local "
                             "filesystem path, read directly)")
    parser.add_argument("--observer-db", default=DEFAULT_OBSERVER_DB,
                        help="local filesystem path to the Rust-owned observer "
                             "db's rust_fee_shadow_outcomes table (only used "
                             "when --source sqlite; default: %(default)s)")
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
                           args.python_db, args.since, args.until,
                           source=args.source, observer_db=args.observer_db)
    except MAIN_CAUGHT_EXCEPTIONS as exc:
        print(f"TRANSPORT FAILURE: {exc}", file=sys.stderr)
        return 2

    report(results)
    return {"GREEN": 0, "YELLOW": 3, "RED": 1}[results["overall"]]


if __name__ == "__main__":
    sys.exit(main())
