#!/usr/bin/env python3
# tools/diff-harness/diff_fee_decisions.py
"""Diff the Rust fee-controller's dry-run journal against Python's
recorded fee decisions, on lnnode (Phase 4 Task 11).

Usage: ./diff_fee_decisions.py [--node lnnode]
                                [--journal ~/.lightning/fee_dryrun_journal.jsonl]
                                [--python-db /data/lightningd/.lightning/revenue_ops.db]
                                [--since <unix-ts>] [--tolerance-ppm 0]
                                [--live]

Controller-run command (this file ships the tool + self-test only; the
controller executes the live comparison window per the plan -- Task 11
Step 2):

    ./tools/diff-harness/diff_fee_decisions.py --node lnnode \\
        --journal ~/.lightning/fee_dryrun_journal.jsonl \\
        --python-db /data/lightningd/.lightning/revenue_ops.db

`--python-db`'s default is the CONFIRMED absolute production path
(`docs/runbooks/observer-deploy.md`'s wiring table, cross-checked live via
`lightning-cli revenue-config get db_path`), not a `~`-relative guess: on
lnnode, `$HOME/.lightning` is a symlink to `/data/lightningd` (the
top-level data dir), NOT to the nested `/data/lightningd/.lightning/`
directory that actually holds `revenue_ops.db` -- so `~/.lightning/
revenue_ops.db` silently resolves to the WRONG file even when tilde
expansion happens at all. And it doesn't: `sqlite_json()` below hands ssh
one already-`shlex.quote()`-d command string (needed so the free-text
`reason` column's query text survives the remote shell), and quoting a
leading `~` suppresses the remote shell's tilde expansion entirely, so a
`~`-based default would try to open a literal path named `~` and fail.
Using the confirmed absolute path sidesteps both problems at once; the
same fix applies to `diff_read_rpcs.py`'s `sqlite_query()` default for
the identical reason -- see that file's own default.

`--journal`'s default path is this tool's best inference from
`crates/revops-fees/src/journal.rs`'s `JOURNAL_FILE_NAME` constant
("fee_dryrun_journal.jsonl") joined onto the flat `~/.lightning/` dir the
same way `--observer-db` defaults in `diff_read_rpcs.py`
("~/.lightning/revops-r-observer.db") -- the live cutover wiring that
picks the orchestrator's actual `db_dir` is explicitly out of this
plan's scope (Task 10's own doc comment), so confirm the real path via
whatever wires `Journal::open_dir`/`Journal::at_path` before trusting
the default on a live run; override with `--journal` either way.

Exits 0 on full parity (every matched adjustment pair identical, modulo
`--tolerance-ppm`), 1 if any deterministic-field mismatch or unmatched
decision was seen on either side, 2 if a transport failure (ssh/RPC
nonzero exit, unparseable JSON/JSONL, or a bad sqlite3 call) occurred
anywhere. Prints an aligned-column report.

COMPARISON SEMANTICS (binding contract from the T10 review, ledgered in
`.superpowers/sdd/progress.md`'s "T10 REVIEW CONTRACTS" entry -- this
supersedes the plan's original field-by-field wishlist, which assumed
Python's `fee_changes` audit table records floor/ceiling/damping/
gossip-gate/congestion-episode internals it never actually stores; only
`old_fee_ppm`, `new_fee_ppm`, and `reason` are common, comparable ground
truth on both sides):

  1. SKIP DISCRIMINATION: a Rust journal line is a "skip" (scheduler
     declined to evaluate / declined to change anything) iff
     `would_broadcast is False` OR `algorithm_values is None`; it is an
     "adjustment" (a would-be broadcast, comparable against a Python
     `fee_changes` row) iff `would_broadcast is True` AND
     `algorithm_values is not None`. NEVER classify by `reason_code`
     alone: `crates/revops-fees/src/cycle.rs`'s `static_policy_branch`
     (and the passive-policy branch) prove `reason_code` values like
     "policy_static"/"policy_passive" appear on BOTH a skip line (no
     fee change needed, `would_broadcast: false`, `algorithm_values:
     null`, `cycle.rs:880-884`) AND a real adjustment (the policy pinned
     a fee and it moved, `would_broadcast: true`, `algorithm_values`
     populated, `cycle.rs:953-970`) -- `reason_code` cannot tell those
     apart, `would_broadcast`/`algorithm_values` can (verified against
     `cycle.rs:2972-3009`'s `FeeDecision` construction, where exactly
     these two fields flip between the `Adjusted`/`Skipped` arms).

  2. MATCHING: adjustments and Python `fee_changes` rows are matched
     per (channel_id, cycle-window) pair. A "cycle-window" is NOT the
     journal's own `cycle_id` string -- it is a time cluster, because
     the Rust `at` stamp and the Python `timestamp` are two independent
     wall-clock reads seconds apart within one cycle (same reason
     `diff_read_rpcs.py` tolerates clock drift on `generated_at`).
     Cluster by the SAME 120s tolerance `fee_intent_completeness()`
     already uses for exactly this purpose (`crates/revops-econ/src/
     reconcile.rs::fee_intent_completeness`, called with
     `tolerance_seconds=120` at `crates/revops-econ/src/shadow.rs:252`)
     -- the plan explicitly says to reuse it here rather than invent a
     second clustering constant. `cluster_by_time()` below is a direct
     Python port of that Rust loop (sort timestamps, extend the last
     cluster if `ts - cluster_end <= tolerance` else start a new one),
     applied per-channel over the UNION of that channel's Python
     timestamps and Rust `at` stamps, so both sides land in the same
     window when they belong together.

     PAIRING WITHIN A CLUSTER, once formed, is NOT plain positional
     zip -- a cluster can hold more than one same-channel event per side
     (a controller cycle changing a channel's fee twice in one window is
     untested-but-possible, and content-identical repeats are a real
     failure mode: naive positional-by-time pairing can pair a real,
     content-different drop against an unrelated identical-content row --
     reporting a false interior field mismatch -- while blaming a
     different, actually-matching row for the "miss"). `match_channel()`
     below instead pairs in two passes: (1) greedy exact-content match on
     `(old_fee_ppm, new_fee_ppm, reason)` -- safe regardless of order,
     since rows sharing that key compare equal however they're paired;
     then (2) whatever's left (deliberate mismatches, and any surplus past
     a content group's overlap) is paired positionally in ascending-`ts`
     order same as before. See `match_channel()`'s docstring and the
     self-test "content-identical repeats do not absorb a real drop".

  3. On matched pairs, compare EXACTLY (tolerance-ppm applies only to
     the two fee fields, default 0): `new_fee_ppm`, `old_fee_ppm`
     (allowed to drift by `--tolerance-ppm`, default 0 -- sampling is
     unseeded in production, but this tool's job is to prove the
     DETERMINISTIC decision path matches, so the default is exact
     equality; the flag exists as an escape hatch, not an expectation),
     and `reason` (the wire-contract reason STRING, compared
     byte-for-byte, no normalization -- see `journal.rs`'s module doc
     comment: "the reason STRING format is the wire contract").

  4. Skip lines have no Python counterpart at all -- Python's
     `fee_changes` table only ever gets a row when a broadcast actually
     happens (`database.py`'s `record_fee_change` is called from the
     broadcast path only), so a Rust skip is reported as an INFO row,
     never a mismatch, regardless of its `reason_code`.

  5. Symmetrically, a Python `fee_changes` row with `manual=1` (an
     operator's manual override, `revenue-fee set` or similar) has no
     Rust dry-run counterpart either -- the algorithm never decided it,
     so there is nothing for the Rust journal to have replayed. These
     are reported as INFO rows too (never "rust missed a decision"),
     the same asymmetry as point 4 but in the other direction. Without
     this exclusion every manual override in the window would be a
     guaranteed false-positive MISMATCH.

  6. Within a (channel, cycle-window) group, remaining unmatched rows
     after pairing are MISMATCHes in both directions: a leftover Python
     (non-manual) row means the Rust controller failed to make a
     decision Python made ("rust missed a decision"); a leftover Rust
     adjustment means the Rust controller made a decision Python never
     recorded ("rust invented a decision").

`--live`: additionally fetches `revenue-fee-debug` (Python's
last-decision-summary RPC) from `--node` for informational display only
-- it is NOT part of the pass/fail contract above (Python's RPC-level
summary and the audit-log `fee_changes` table are two different views
of the same decisions; the plan lists it as a debugging aid, not a
comparison input) and a fetch failure here is logged to stderr but never
raises the exit code.

Requires Python >= 3.8.
"""
import argparse, json, shlex, subprocess, sys

# ---------------------------------------------------------------------------
# Cluster tolerance -- reused verbatim from
# crates/revops-econ/src/reconcile.rs::fee_intent_completeness, called with
# tolerance_seconds=120 at crates/revops-econ/src/shadow.rs:252 (see module
# docstring point 2).
# ---------------------------------------------------------------------------
CYCLE_WINDOW_TOLERANCE_SECONDS = 120

# Best-inferred default; see module docstring's `--journal` paragraph.
# Updated to T3 wiring: lnnode observer-db dir + JOURNAL_FILE_NAME
DEFAULT_JOURNAL_PATH = "/data/lightningd/.lightning/fee_dryrun_journal.jsonl"

# CONFIRMED absolute production path (not a `~`-relative guess -- see the
# module docstring's `--python-db` paragraph for why `~` is wrong here both
# in principle, on lnnode, since `$HOME/.lightning` is a symlink that skips
# the actual nested `.lightning` dir, and in practice, since sqlite_json()
# must shlex.quote() the whole path, which suppresses remote tilde
# expansion outright).
DEFAULT_PYTHON_DB = "/data/lightningd/.lightning/revenue_ops.db"


def read_journal(node, path):
    """Read the Rust dry-run journal's raw JSONL text via `ssh <node> cat
    <path>`. Raises subprocess.CalledProcessError on nonzero exit (missing
    file, permission error, etc.) -- a transport-layer failure for our
    purposes, caught by diff_fee_decisions()."""
    return subprocess.run(["ssh", node, "cat", path],
                         capture_output=True, text=True, check=True).stdout


def sqlite_json(node, db_path, query):
    """Run `query` against `db_path` on `node`, read-only, via sqlite3's
    `-readonly` and `-json` flags, and return raw stdout.

    `-json` (not the hand-rolled CSV `diff_read_rpcs.py`'s `sqlite_query`
    uses for its scalar COUNT/MIN queries) because `fee_changes.reason` is
    free-text that can legitimately contain commas, quotes, and newlines --
    a real JSON array round-trips that safely where CSV parsing would not.
    Verified live on lnnode's sqlite3 3.46.1 (2026-07-16): `-json` is
    supported and, per the CLI's own behavior, prints NOTHING (not "[]")
    when the query matches zero rows -- callers must treat blank/whitespace
    stdout as an empty result set, not a parse error (see
    fetch_python_rows()). Same "one pre-quoted remote command string"
    reasoning as `diff_read_rpcs.py`'s `sqlite_query()` docstring: the
    query contains parens/spaces the remote shell would otherwise
    re-split.
    """
    remote_cmd = f"sqlite3 -readonly {shlex.quote(db_path)} {shlex.quote(query)} -json"
    return subprocess.run(["ssh", node, remote_cmd],
                         capture_output=True, text=True, check=True).stdout


def cli(node, *args):
    """Run `lightning-cli <args>` on `node` over ssh and parse JSON stdout.
    Used only by `--live`'s supplementary `revenue-fee-debug` fetch."""
    out = subprocess.run(["ssh", node, "lightning-cli", *args],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def _one_line(exc_or_text):
    """Render an exception (or raw text) as a single-line cause string."""
    text = str(exc_or_text).strip()
    return text.splitlines()[0] if text else repr(exc_or_text)


def parse_journal(raw):
    """Parse JSONL text into a list of decision dicts, one per line. Blank
    lines are skipped. Raises json.JSONDecodeError on a malformed line --
    caught by diff_fee_decisions() as a transport-layer failure (an
    unreadable journal is not a comparable-content problem, it's a "we
    cannot even see what Rust decided" problem)."""
    decisions = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        decisions.append(json.loads(line))
    return decisions


def fetch_python_rows(sqlite_fn, node, python_db, since_ts, until_ts=None):
    """Fetch `fee_changes` rows (channel_id, peer_id, old_fee_ppm,
    new_fee_ppm, reason, reason_code, manual, timestamp) with
    `timestamp >= since_ts` (and `timestamp <= until_ts` if provided),
    via sqlite_json(). Returns [] for a genuinely empty result set
    (see sqlite_json()'s blank-stdout note)."""
    where_clause = f"WHERE timestamp >= {int(since_ts)}"
    if until_ts is not None:
        where_clause += f" AND timestamp <= {int(until_ts)}"
    query = (
        "SELECT channel_id, peer_id, old_fee_ppm, new_fee_ppm, reason, "
        "reason_code, manual, timestamp FROM fee_changes "
        f"{where_clause} ORDER BY timestamp"
    )
    raw = sqlite_fn(node, python_db, query).strip()
    if not raw:
        return []
    return json.loads(raw)


def is_adjustment(decision):
    """True iff a Rust journal line is a would-be broadcast comparable
    against a Python `fee_changes` row -- see module docstring point 1.
    NEVER classify via `reason_code` alone."""
    return decision.get("would_broadcast") is True and decision.get("algorithm_values") is not None


def cluster_by_time(events, tolerance_seconds=CYCLE_WINDOW_TOLERANCE_SECONDS):
    """Assign a 0-based cluster index to each event (a dict with a numeric
    `ts` key), in ascending-`ts` order. Direct port of
    `fee_intent_completeness()`'s clustering loop (crates/revops-econ/src/
    reconcile.rs): extend the last cluster if `ts - cluster_end <=
    tolerance_seconds`, else start a new one. Returns a list of
    `(event, cluster_index)` pairs, same order as the input after sorting.
    """
    ordered = sorted(events, key=lambda e: e["ts"])
    cluster_ends = []
    out = []
    for e in ordered:
        ts = e["ts"]
        if cluster_ends and ts - cluster_ends[-1] <= tolerance_seconds:
            cluster_ends[-1] = ts
        else:
            cluster_ends.append(ts)
        out.append((e, len(cluster_ends) - 1))
    return out


def compare_pair(py_row, rs_dec, tolerance_ppm):
    """Compare one matched (Python fee_changes row, Rust adjustment)
    pair -- see module docstring point 3. Returns a "match" or "mismatch"
    result dict; `fields` lists every differing (name, py_value,
    rs_value) triple."""
    fields = []
    if abs(py_row["old_fee_ppm"] - rs_dec["old_fee_ppm"]) > tolerance_ppm:
        fields.append(("old_fee_ppm", py_row["old_fee_ppm"], rs_dec["old_fee_ppm"]))
    if abs(py_row["new_fee_ppm"] - rs_dec["new_fee_ppm"]) > tolerance_ppm:
        fields.append(("new_fee_ppm", py_row["new_fee_ppm"], rs_dec["new_fee_ppm"]))
    if py_row["reason"] != rs_dec["reason"]:
        fields.append(("reason", py_row["reason"], rs_dec["reason"]))
    status = "mismatch" if fields else "match"
    return {"status": status, "kind": "pair", "channel_id": py_row["channel_id"],
            "fields": fields, "py": py_row, "rs": rs_dec}


def _content_key(row):
    """The three fields `compare_pair()` actually judges (module docstring
    point 3), as a tuple key. Rows sharing this key are content-identical
    -- pairing any of them against any other of them can never manufacture
    a spurious mismatch, which is what makes pass 1 below safe to do
    order-independently."""
    return (row["old_fee_ppm"], row["new_fee_ppm"], row["reason"])


def match_channel(py_rows, rs_adjustments, tolerance_ppm):
    """Match one channel's Python `fee_changes` rows against its Rust
    adjustments, per (channel, cycle-window) -- module docstring points
    2 and 6. Both lists are already filtered to a single channel_id and
    to non-manual/would-be-broadcast rows respectively by the caller.

    Pairing within each cycle-window cluster is two-pass, content-aware
    first (see the module docstring's "PAIRING WITHIN A CLUSTER" note):

      pass 1 (greedy exact-content): group both sides of the cluster by
      `_content_key()`; for every key present on both sides, pair off
      `min(count)` rows from each. Order within a key doesn't matter --
      by construction every such pair compares as a "match".

      pass 2 (positional-by-time fallback): whatever's left after pass 1
      (deliberate mismatches with no same-content counterpart, and any
      surplus beyond a content group's overlap) is paired positionally in
      ascending-`ts` order, same as the old behavior, so intentional
      fee/reason mismatches are still caught and reported.

    This keeps identical-content repeats in a cluster from "absorbing" an
    unrelated, content-different real decision into an arbitrary interior
    pairing (a false content mismatch) while blaming the wrong row for the
    resulting miss -- see self-test "content-identical repeats do not
    absorb a real drop".
    """
    events = [{"ts": r["timestamp"], "side": "python", "row": r} for r in py_rows]
    events += [{"ts": d["at"], "side": "rust", "row": d} for d in rs_adjustments]
    clustered = cluster_by_time(events)

    by_cluster = {}
    for e, idx in clustered:
        bucket = by_cluster.setdefault(idx, {"python": [], "rust": []})
        bucket[e["side"]].append(e["row"])

    results = []
    for idx in sorted(by_cluster):
        py_list = by_cluster[idx]["python"]
        rs_list = by_cluster[idx]["rust"]

        # -- pass 1: greedy exact-content matching (order-independent) --
        py_by_key = {}
        for r in py_list:
            py_by_key.setdefault(_content_key(r), []).append(r)
        rs_by_key = {}
        for d in rs_list:
            rs_by_key.setdefault(_content_key(d), []).append(d)

        matched_py_ids = set()
        matched_rs_ids = set()
        for key, py_group in py_by_key.items():
            rs_group = rs_by_key.get(key)
            if not rs_group:
                continue
            n = min(len(py_group), len(rs_group))
            for py_row, rs_dec in zip(py_group[:n], rs_group[:n]):
                results.append(compare_pair(py_row, rs_dec, tolerance_ppm))
                matched_py_ids.add(id(py_row))
                matched_rs_ids.add(id(rs_dec))

        # -- pass 2: positional-by-time fallback for the remainder --
        py_remaining = [r for r in py_list if id(r) not in matched_py_ids]
        rs_remaining = [d for d in rs_list if id(d) not in matched_rs_ids]
        n = min(len(py_remaining), len(rs_remaining))
        for py_row, rs_dec in zip(py_remaining[:n], rs_remaining[:n]):
            results.append(compare_pair(py_row, rs_dec, tolerance_ppm))
        for extra in py_remaining[n:]:
            results.append({"status": "mismatch", "kind": "rust_missed",
                           "channel_id": extra["channel_id"], "py": extra})
        for extra in rs_remaining[n:]:
            results.append({"status": "mismatch", "kind": "rust_invented",
                           "channel_id": extra["channel_id"], "rs": extra})
    return results


def diff_fee_decisions(journal_fn, sqlite_fn, node, journal_path, python_db,
                       since_ts=None, until_ts=None, tolerance_ppm=0):
    """Fetch the journal + fee_changes rows and return a list of result
    dicts:
      {"status": "match",     "kind": "pair", "channel_id", "fields": [], "py", "rs"}
      {"status": "mismatch",  "kind": "pair"|"rust_missed"|"rust_invented", ...}
      {"status": "info",      "kind": "rust_skip"|"python_manual", "channel_id", "detail"}
      {"status": "transport", "side": "journal"|"python", "cause": "..."}

    Never raises -- transport-layer failures (ssh/RPC exit code, bad JSON,
    unreadable JSONL) come back as a single "transport" result, matching
    the shape family diff_read_rpcs.py/diff_config.py use for the same
    purpose.

    `since_ts=None` derives the window floor from the journal's own
    earliest `at` timestamp (0 if the journal is empty) rather than
    requiring the caller to know it up front -- `--since` overrides this
    outright, mirroring diff_read_rpcs.py's derived-then-overridable
    window pattern.

    `until_ts=None` excludes rows/decisions with `timestamp > until_ts`
    from matching on both sides, bounding the comparison window.
    """
    try:
        raw_journal = journal_fn(node, journal_path)
    except subprocess.CalledProcessError as exc:
        return [{"status": "transport", "side": "journal", "cause": _one_line(exc)}]

    try:
        decisions = parse_journal(raw_journal)
    except json.JSONDecodeError as exc:
        return [{"status": "transport", "side": "journal", "cause": _one_line(exc)}]

    if since_ts is None:
        since_ts = min((d.get("at", 0) for d in decisions), default=0)

    try:
        python_rows = fetch_python_rows(sqlite_fn, node, python_db, since_ts, until_ts)
    except subprocess.CalledProcessError as exc:
        return [{"status": "transport", "side": "python", "cause": _one_line(exc)}]
    except (ValueError, json.JSONDecodeError) as exc:
        return [{"status": "transport", "side": "python", "cause": _one_line(exc)}]

    decisions = [d for d in decisions if d.get("at", 0) >= since_ts]
    if until_ts is not None:
        decisions = [d for d in decisions if d.get("at", 0) <= until_ts]

    python_rows = [r for r in python_rows if r.get("timestamp", 0) >= since_ts]
    if until_ts is not None:
        python_rows = [r for r in python_rows if r.get("timestamp", 0) <= until_ts]

    skips = [d for d in decisions if not is_adjustment(d)]
    adjustments = [d for d in decisions if is_adjustment(d)]
    manual_rows = [r for r in python_rows if r.get("manual")]
    algo_rows = [r for r in python_rows if not r.get("manual")]

    results = []
    for d in skips:
        results.append({"status": "info", "kind": "rust_skip",
                       "channel_id": d.get("channel_id"), "detail": d})
    for r in manual_rows:
        results.append({"status": "info", "kind": "python_manual",
                       "channel_id": r.get("channel_id"), "detail": r})

    channels = sorted({d["channel_id"] for d in adjustments} | {r["channel_id"] for r in algo_rows})
    for ch in channels:
        py_ch = [r for r in algo_rows if r["channel_id"] == ch]
        rs_ch = [d for d in adjustments if d["channel_id"] == ch]
        results.extend(match_channel(py_ch, rs_ch, tolerance_ppm))

    return results


def report(results):
    """Print an aligned-column report and return the process exit code:
    0 parity, 1 mismatch, 2 transport failure. INFO rows (skips/manual
    overrides) are always printed but never affect the exit code."""
    matches = [r for r in results if r["status"] == "match"]
    mismatches = [r for r in results if r["status"] == "mismatch"]
    infos = [r for r in results if r["status"] == "info"]
    transport = [r for r in results if r["status"] == "transport"]

    rows = []
    for r in mismatches:
        if r["kind"] == "pair":
            detail = "; ".join(f"{name}: python={pv!r} rust={rv!r}" for name, pv, rv in r["fields"])
            rows.append(("MISMATCH", r["channel_id"], detail))
        elif r["kind"] == "rust_missed":
            rows.append(("MISMATCH", r["channel_id"],
                        f"python fee_changes row has no rust counterpart (rust missed a decision): {r['py']!r}"))
        elif r["kind"] == "rust_invented":
            rows.append(("MISMATCH", r["channel_id"],
                        f"rust adjustment has no python counterpart (rust invented a decision): {r['rs']!r}"))
    for r in transport:
        rows.append(("TRANSPORT", r["side"], r["cause"]))
    for r in infos:
        if r["kind"] == "rust_skip":
            rows.append(("INFO", r["channel_id"] or "?",
                        f"rust skip (reason_code={r['detail'].get('reason_code')!r}, no python counterpart expected)"))
        else:
            rows.append(("INFO", r["channel_id"] or "?",
                        "python manual fee change (no rust dry-run counterpart expected)"))

    print("--- fee decision diff (rust dry-run journal vs python fee_changes) ---")
    if rows:
        w_status = max(len(row[0]) for row in rows)
        w_key = max(len(row[1]) for row in rows)
        for status, key, rest in rows:
            print(f"{status:<{w_status}}  {key:<{w_key}}  {rest}")

    if transport:
        print(f"transport errors: {len(transport)}", file=sys.stderr)
        return 2
    if mismatches:
        return 1
    print(f"parity: {len(matches)} adjustment pairs matched, {len(infos)} info rows (skips/manual)")
    return 0


# ---------------------------------------------------------------------------
# Self-test fixtures -- shapes read directly from
# crates/revops-fees/src/journal.rs::FeeDecision::to_ovalue (Rust journal
# line) and modules/database.py:672-682/1207-1216 (Python `fee_changes`
# schema, incl. the later reason_code/heuristic_modifiers migrations), in
# ~/bin/cl_revenue_ops.
# ---------------------------------------------------------------------------

def _rs_adjustment(channel_id="123x1x0", peer_id="peer1", old=100, new=120,
                   reason="Policy: STATIC fee override", reason_code="policy_static",
                   at=1_700_000_000):
    return {
        "channel_id": channel_id, "peer_id": peer_id,
        "old_fee_ppm": old, "new_fee_ppm": new,
        "reason": reason, "reason_code": reason_code,
        "algorithm_values": {"policy": "static", "requested_fee_ppm": new},
        "trace": {"disposition": "policy_static", "would_broadcast": True},
        "would_broadcast": True, "governed": None,
        "cycle_id": f"fee-cycle-{at}", "at": at,
    }


def _rs_skip(channel_id="123x1x0", peer_id="peer1", fee=100,
            reason_code="policy_static", at=1_700_000_000):
    # Mirrors cycle.rs:2990-3007: a skip carries the SAME reason_code a
    # real adjustment can carry (policy_static/policy_passive), which is
    # exactly why classification must never use reason_code alone.
    return {
        "channel_id": channel_id, "peer_id": peer_id,
        "old_fee_ppm": fee, "new_fee_ppm": fee,
        "reason": "skip: policy_static", "reason_code": reason_code,
        "algorithm_values": None,
        "trace": {"skip_reason": "policy_static"},
        "would_broadcast": False, "governed": None,
        "cycle_id": f"fee-cycle-{at}", "at": at,
    }


def _py_row(channel_id="123x1x0", peer_id="peer1", old=100, new=120,
           reason="Policy: STATIC fee override", reason_code="policy_static",
           manual=0, timestamp=1_700_000_000):
    return {
        "channel_id": channel_id, "peer_id": peer_id,
        "old_fee_ppm": old, "new_fee_ppm": new,
        "reason": reason, "reason_code": reason_code,
        "manual": manual, "timestamp": timestamp,
    }


def _journal_fn_for(decisions):
    def fn(node, path):
        return "\n".join(json.dumps(d) for d in decisions) + ("\n" if decisions else "")
    return fn


def _sqlite_fn_for(rows):
    def fn(node, db_path, query):
        return json.dumps(rows)
    return fn


def self_test():
    """Exercise the contract paths with stubbed journal/sqlite calls (no
    ssh/node needed). Invoke via: python3 diff_fee_decisions.py --self-test"""
    ok = True

    # -- 1. adjustment parity pass --
    rs = [_rs_adjustment()]
    py = [_py_row()]
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    print(f"[self-test] adjustment parity pass: exit={rc} (expect 0)")
    ok = ok and rc == 0
    ok = ok and any(r["status"] == "match" for r in results)

    # -- 2. reason-string mismatch --
    rs = [_rs_adjustment(reason="Policy: STATIC fee override")]
    py = [_py_row(reason="Policy: static override (legacy wording)")]
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    mismatched_fields = {name for r in results if r["status"] == "mismatch" and r["kind"] == "pair"
                        for name, _, _ in r["fields"]}
    print(f"[self-test] reason-string mismatch: exit={rc} fields={mismatched_fields} "
          f"(expect 1, {{'reason'}})")
    ok = ok and rc == 1 and mismatched_fields == {"reason"}

    # -- 3. fee mismatch --
    rs = [_rs_adjustment(new=125)]
    py = [_py_row(new=120)]
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0, tolerance_ppm=0)
    rc = report(results)
    mismatched_fields = {name for r in results if r["status"] == "mismatch" and r["kind"] == "pair"
                        for name, _, _ in r["fields"]}
    print(f"[self-test] fee mismatch: exit={rc} fields={mismatched_fields} "
          f"(expect 1, {{'new_fee_ppm'}})")
    ok = ok and rc == 1 and mismatched_fields == {"new_fee_ppm"}

    # -- 3b. fee mismatch tolerated by --tolerance-ppm --
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0, tolerance_ppm=5)
    rc = report(results)
    print(f"[self-test] fee mismatch within tolerance_ppm=5: exit={rc} (expect 0)")
    ok = ok and rc == 0

    # -- 4. skip-line INFO (not mismatch), including the shared-reason_code
    # trap: a skip and a real adjustment both carrying reason_code
    # "policy_static" must classify correctly via would_broadcast/
    # algorithm_values alone.
    rs = [_rs_skip(channel_id="111x1x0", reason_code="policy_static"),
         _rs_adjustment(channel_id="222x1x0", reason_code="policy_static")]
    py = [_py_row(channel_id="222x1x0")]
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    statuses_111 = {r["status"] for r in results if r.get("channel_id") == "111x1x0"}
    statuses_222 = {r["status"] for r in results if r.get("channel_id") == "222x1x0"}
    print(f"[self-test] skip-line INFO despite shared reason_code: exit={rc} "
          f"111x1x0={statuses_111} 222x1x0={statuses_222} "
          f"(expect 0, 111x1x0=={{'info'}}, 222x1x0=={{'match'}})")
    ok = ok and rc == 0 and statuses_111 == {"info"} and statuses_222 == {"match"}

    # -- 4b. python manual row is INFO, not "rust missed a decision" --
    rs = []
    py = [_py_row(manual=1)]
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    statuses = {r["status"] for r in results}
    kinds = {r["kind"] for r in results}
    print(f"[self-test] python manual row is INFO not mismatch: exit={rc} "
          f"statuses={statuses} kinds={kinds} (expect 0, {{'info'}}, {{'python_manual'}})")
    ok = ok and rc == 0 and statuses == {"info"} and kinds == {"python_manual"}

    # -- 5. unmatched-both-directions: channel A has a python row with no
    # rust adjustment (rust missed it); channel B has a rust adjustment
    # with no python row (rust invented it).
    rs = [_rs_adjustment(channel_id="bbb")]
    py = [_py_row(channel_id="aaa")]
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    kinds_by_channel = {r["channel_id"]: r["kind"] for r in results if r["status"] == "mismatch"}
    print(f"[self-test] unmatched both directions: exit={rc} kinds={kinds_by_channel} "
          f"(expect 1, aaa='rust_missed', bbb='rust_invented')")
    ok = ok and rc == 1 and kinds_by_channel == {"aaa": "rust_missed", "bbb": "rust_invented"}

    # -- 6. transport failure: journal read --
    def journal_transport(node, path):
        raise subprocess.CalledProcessError(255, ["ssh"])

    results = diff_fee_decisions(journal_transport, _sqlite_fn_for([]), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    print(f"[self-test] transport failure (journal): exit={rc} (expect 2)")
    ok = ok and rc == 2

    # -- 6b. transport failure: sqlite query --
    def sqlite_transport(node, db_path, query):
        raise subprocess.CalledProcessError(1, ["sqlite3"])

    results = diff_fee_decisions(_journal_fn_for([_rs_adjustment()]), sqlite_transport,
                                "node", "journal", "db", since_ts=0)
    rc = report(results)
    print(f"[self-test] transport failure (sqlite): exit={rc} (expect 2)")
    ok = ok and rc == 2

    # -- 6c. malformed journal line is also a transport-level failure --
    def journal_malformed(node, path):
        return "{not json}\n"

    results = diff_fee_decisions(journal_malformed, _sqlite_fn_for([]), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    print(f"[self-test] malformed journal line: exit={rc} (expect 2)")
    ok = ok and rc == 2

    # -- 7. cycle-window clustering: python timestamp and rust `at` land a
    # few seconds apart within the same 120s-tolerance window and must
    # still be paired (module docstring point 2 / cluster_by_time()).
    rs = [_rs_adjustment(at=1_700_000_050)]
    py = [_py_row(timestamp=1_700_000_000)]
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    print(f"[self-test] cycle-window clustering (50s apart, tolerance=120s): exit={rc} (expect 0)")
    ok = ok and rc == 0 and any(r["status"] == "match" for r in results)

    # -- 8. multi-event pairing: two DISTINCT same-channel adjustments land
    # in the same 120s cluster, appended out of time order on purpose, to
    # prove pairing matches by content rather than depending on original
    # list order or python/rust `ts` order lining up (Important-1 review:
    # untested for >1 event per channel per window before this).
    rs = [_rs_adjustment(at=1_700_000_000, new=110, reason="Policy: STATIC fee override"),
         _rs_adjustment(at=1_700_000_090, new=130, reason="Policy: STATIC fee override (2)")]
    py = [_py_row(timestamp=1_700_000_095, new=130, reason="Policy: STATIC fee override (2)"),
         _py_row(timestamp=1_700_000_005, new=110, reason="Policy: STATIC fee override")]
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    matched = [r for r in results if r["status"] == "match"]
    print(f"[self-test] multi-event same-channel pairing (2 distinct events/120s window): "
          f"exit={rc} matched_pairs={len(matched)} (expect 0, 2)")
    ok = ok and rc == 0 and len(matched) == 2

    # -- 9. content-identical drop case (Important-1 review, the specific
    # scenario called out): 3 python rows, 2 CONTENT-IDENTICAL rust
    # adjustments, all in one 120s cluster. The real, content-different
    # drop (py_b) sits BETWEEN the two identical rust `at` stamps in time,
    # which is exactly the shape that fooled plain positional-by-time zip:
    # it would pair py_b against the second identical-content rust row
    # (a false "reason"/"new_fee_ppm" mismatch) and then blame the THIRD,
    # actually-matching python row for the miss instead. Content-aware
    # matching must consume both identical-content rust rows against their
    # identical-content python counterparts and flag ONLY py_b as missed.
    rs = [_rs_adjustment(at=1_700_000_000, new=120, reason="Policy: STATIC fee override"),
         _rs_adjustment(at=1_700_000_100, new=120, reason="Policy: STATIC fee override")]
    py = [_py_row(timestamp=1_699_999_995, new=120, reason="Policy: STATIC fee override"),   # py_a
         _py_row(timestamp=1_700_000_050, new=999, reason="Policy: REAL drop, no rust counterpart"),  # py_b
         _py_row(timestamp=1_700_000_105, new=120, reason="Policy: STATIC fee override")]    # py_c
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0)
    rc = report(results)
    matched = [r for r in results if r["status"] == "match"]
    missed = [r for r in results if r["status"] == "mismatch" and r["kind"] == "rust_missed"]
    invented = [r for r in results if r["status"] == "mismatch" and r["kind"] == "rust_invented"]
    matched_clean = all(r["fields"] == [] for r in matched)
    missed_is_py_b = len(missed) == 1 and missed[0]["py"]["new_fee_ppm"] == 999
    print(f"[self-test] content-identical repeats do not absorb a real drop: exit={rc} "
          f"matched_pairs={len(matched)} (content-consistent={matched_clean}) "
          f"rust_missed={len(missed)} (is py_b={missed_is_py_b}) rust_invented={len(invented)} "
          f"(expect 1, 2 (True), 1 (True), 0)")
    ok = (ok and rc == 1 and len(matched) == 2 and matched_clean
         and len(missed) == 1 and missed_is_py_b and len(invented) == 0)

    # -- 10. --until window boundary: decisions/rows outside the [since, until]
    # window are excluded symmetrically. Two adjustments with one inside and
    # one outside the window; same for Python rows; the outside-window pair
    # must be excluded entirely.
    #
    # MINOR: this scenario drives `diff_fee_decisions`'s POST-FETCH filters
    # (the `until_ts is not None` list comprehensions around lines 429/434),
    # not `fetch_python_rows`'s SQL `AND timestamp <= {until_ts}` clause
    # construction (line 244) -- `_sqlite_fn_for` below is a stub that
    # returns its captured `rows` unconditionally and ignores the `query`
    # string entirely, so the SQL text itself is never parsed or executed
    # here. The post-fetch filter is what's actually enforced against a live
    # node (it re-checks every row regardless of what the SQL already
    # excluded), so self-test coverage of that guarantee is sufficient; the
    # SQL clause is a query-side optimization only a live/integration run
    # against real sqlite would exercise.
    rs = [_rs_adjustment(at=1_700_000_000, new=110),
         _rs_adjustment(at=1_700_000_200, new=130)]  # outside until window
    py = [_py_row(timestamp=1_700_000_005, new=110),
         _py_row(timestamp=1_700_000_205, new=130)]  # outside until window
    results = diff_fee_decisions(_journal_fn_for(rs), _sqlite_fn_for(py), "node",
                                "journal", "db", since_ts=0, until_ts=1_700_000_100)
    rc = report(results)
    matched = [r for r in results if r["status"] == "match"]
    print(f"[self-test] --until excludes out-of-window pair: exit={rc} "
          f"matched_pairs={len(matched)} (expect 0, 1)")
    ok = ok and rc == 0 and len(matched) == 1

    print("[self-test] ALL PASS" if ok else "[self-test] FAILURE")
    return 0 if ok else 1


def main(argv=None, journal_fn=read_journal, sqlite_fn=sqlite_json, cli_fn=cli):
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="lnnode")
    ap.add_argument("--self-test", action="store_true",
                    help="run the built-in self-test (stubbed journal/sqlite, no live node) and exit")
    ap.add_argument("--journal", default=DEFAULT_JOURNAL_PATH,
                    help=f"path to the Rust dry-run journal on --node, read via "
                         f"`ssh <node> cat <path>` (default: {DEFAULT_JOURNAL_PATH})")
    ap.add_argument("--python-db", default=DEFAULT_PYTHON_DB,
                    help=f"Python plugin's production sqlite db, on --node (default: {DEFAULT_PYTHON_DB})")
    ap.add_argument("--since", type=int, default=None,
                    help="unix ts floor for both the journal and the python fee_changes "
                         "query; default: derive from the journal's own earliest `at` "
                         "timestamp (0 if the journal is empty)")
    ap.add_argument("--until", type=int, default=None,
                    help="unix ts ceiling for both the journal and the python fee_changes "
                         "query; rows/decisions with timestamp > until are excluded from "
                         "matching on both sides (default: no ceiling)")
    ap.add_argument("--tolerance-ppm", type=int, default=0,
                    help="allowed ppm drift for old_fee_ppm/new_fee_ppm on matched "
                         "adjustment pairs (default: 0 -- exact match; deterministic "
                         "fields are not expected to drift, this is an escape hatch)")
    ap.add_argument("--live", action="store_true",
                    help="also fetch revenue-fee-debug from --node for informational "
                         "display (does not affect the exit code)")
    args = ap.parse_args(argv)

    if args.self_test:
        return self_test()

    results = diff_fee_decisions(journal_fn, sqlite_fn, args.node, args.journal,
                                args.python_db, since_ts=args.since, until_ts=args.until,
                                tolerance_ppm=args.tolerance_ppm)
    rc = report(results)

    if args.live:
        try:
            debug = cli_fn(args.node, "revenue-fee-debug")
            print("--- live revenue-fee-debug (informational; not part of the pass/fail contract) ---")
            print(json.dumps(debug, indent=2, sort_keys=True))
        except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            print(f"--live: revenue-fee-debug fetch failed (informational only, ignored): {_one_line(exc)}",
                  file=sys.stderr)

    return rc


if __name__ == "__main__":
    sys.exit(main())
