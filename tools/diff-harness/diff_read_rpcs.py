#!/usr/bin/env python3
# tools/diff-harness/diff_read_rpcs.py
"""Diff the Rust observer's read RPCs against Python's, on lnnode.

Usage: ./diff_read_rpcs.py [--node lnnode] [--window-days 30]
                            [--observer-db /data/lightningd/.lightning/revops-r-observer.db]
                            [--python-db /data/lightningd/.lightning/revenue_ops.db]
                            [--tolerance 2] [--since <unix-ts>]

Runs four comparisons in sequence and exits with the WORST severity seen
across all of them: 0 full parity, 1 if any field mismatch or per-RPC
error envelope was seen, 2 if a transport failure (ssh/RPC nonzero exit,
unparseable JSON, or a bad sqlite3 call) occurred anywhere. Prints an
aligned-column report per comparison.

Comparisons:
  1. revenue-r-history  vs revenue-history  -- recursive field-by-field
     diff of the full JSON (fully DB-backed on both sides, no gaps).
  2. revenue-r-report costs vs revenue-report costs -- same recursive
     diff, the only fully-ported `report_type`. The `generated_at` leaf
     is always skipped (see NOTE below).
  3. revenue-r-dashboard vs revenue-dashboard (--window-days N) --
     recursive diff, but every path listed in the RUST response's own
     `_phase1b_gaps` array is skipped rather than compared (self-derived
     at runtime from that response), and the `_phase1b_gaps` key itself
     is always skipped too (Python has no such key -- comparing it would
     be a spurious "missing key" mismatch, not a real one).
  4. Ingestion cross-check -- row count of the Rust observer's own
     `ingested_forwards` table (in --observer-db, on the node) vs
     Python's `forwards` table (in --python-db, on the node), read-only,
     over "the same time window" -- see NOTE below for how that window
     is derived. Allow --tolerance N rows (default 2 -- in-flight
     settlements race between the two independent ingestion paths).

NOTE -- generated_at: `revenue-report costs`/`revenue-r-report costs`
both stamp `generated_at` with their OWN wall-clock read at RPC-handling
time (Python: `int(time.time())`; Rust: `now_unix()`), taken from two
separate sequential ssh+lightning-cli round trips. Real clock drift
between those two calls (0-2s is typical) is not a port defect, so this
one leaf is always skipped rather than compared -- unlike the dashboard's
`_phase1b_gaps`, this is not a phase gap, just an unavoidable artifact of
diffing two live processes one after another.

NOTE -- ingestion window: naively using the Rust observer's own
`MIN(timestamp)` from `ingested_forwards` as the shared window start is
NOT safe on its own. It's true the Rust observer's table only ever
contains rows ingested since ITS OWN process started PLUS whatever startup
hydration backfilled -- but hydration backfills up to 14 days
(`_hydration_fetch_settled_forwards`'s warm-start window,
cl-revenue-ops.py:602-625 / `compute_forward_hydration_start`), while
Python's OWN `forwards` table is pruned at roughly 8 days
(`modules/database.py`'s forward-retention housekeeping). So a `MIN(
timestamp)` from 12 days ago is a real, honest value on the Rust side, but
Python's `forwards` table simply no longer has any rows that old -- not a
port defect, just two different retention windows read naively as if they
were the same window. Without a clamp this is a GUARANTEED spurious
mismatch on any node where hydration's backfill reached further back than
Python's pruning horizon (a near-certainty in production: 14 > 8).

We derive `since_ts` at runtime, then clamp it forward to never look
further back than Python's own pruning horizon:
  1. fetch `MIN(timestamp)` from `ingested_forwards` (or the LOCAL wall
     clock via `time.time()` if the table is empty -- a fresh deploy with
     no rows yet, which is always >= any historical Python forward, so an
     empty observer naturally compares 0 against ~0).
  2. `since_ts = max(since_ts, now - 7*86400)` -- 7 days rather than
     Python's exact ~8-day prune point, leaving a one-day safety margin
     against prune-timing jitter rather than clamping to the exact edge.
  3. `--since <unix ts>` overrides both of the above outright, for a
     caller who wants to pin an exact window (e.g. reproducing a specific
     day's mismatch) rather than trust the derived value.

NOTE -- numeric comparison: ints and strings compare with plain `==`.
Floats compare with `==` after `round(x, 6)` on BOTH sides. Both ports
independently round money-percentage fields to 2 decimals with their own
float arithmetic (Python's `round()`, Rust's `py_round2`, designed to
mirror it) -- functionally identical values can differ in a float's
low-order bits. Rounding both operands to 6 decimals before comparing
absorbs that representation noise without masking a genuine 2-decimal
divergence (which would still show up well before the 6th digit).

NOTE -- ssh argv vs single string: `cli()` (lightning-cli calls) passes
plain argv tokens to ssh, exactly like diff_config.py, because every
token involved (method names, `-k`, `key=value` pairs) is a single
shell-safe word -- OpenSSH just concatenates multi-arg invocations with
spaces and hands the result to the remote shell for a SECOND
word-splitting pass, which is harmless when no token itself needs
quoting. `sqlite_query()` (the ingestion cross-check) can NOT use that
shortcut: a `SELECT COUNT(*) FROM ...` query contains parens and spaces
that the remote shell would reinterpret as shell syntax after that
second split. So it builds ONE already shell-quoted command string
(`shlex.quote` on the URI and the SQL text) and hands ssh that single
argument instead.

Requires Python >= 3.8.
"""
import argparse, copy, json, shlex, subprocess, sys, time

# Fields whose values are expected to differ between two independent,
# sequential wall-clock reads and are never real port mismatches.
GENERATED_AT_SKIP = frozenset({"generated_at"})

# Key inside the Rust dashboard response that lists its own not-yet-ported
# leaf paths (Phase 1b gap table). Always skipped itself; its listed
# leaves are self-derived and skipped too (see diff_rpc()).
GAP_KEY = "_phase1b_gaps"

# Confirmed production observer database path. A tilde-relative default is
# unsafe because sqlite_query() shell-quotes the path for the remote command,
# which suppresses tilde expansion.
DEFAULT_OBSERVER_DB = "/data/lightningd/.lightning/revops-r-observer.db"


def cli(node, *args):
    """Run `lightning-cli <args>` on `node` over ssh and parse JSON stdout.

    Raises subprocess.CalledProcessError on nonzero exit and
    json.JSONDecodeError on unparseable stdout -- both are transport-layer
    failures for our purposes and are caught by diff_rpc().
    """
    out = subprocess.run(["ssh", node, "lightning-cli", *args],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def sqlite_query(node, db_path, query):
    """Run `query` against `db_path` on `node`, read-only, via sqlite3's
    `-readonly` flag, and return raw stdout (not parsed -- see
    diff_ingestion for how the scalar COUNT/MIN results are interpreted).

    `-readonly` rather than `-uri file:...?mode=ro`: lnnode's sqlite3 CLI
    (3.46.1, Ubuntu 25.04) rejects `-uri` ("unknown option", found live
    2026-07-16); `-readonly` is the portable spelling of the same intent.

    See the module docstring's "ssh argv vs single string" note for why
    this builds one pre-quoted remote command string instead of passing
    positional argv tokens the way `cli()` does.
    """
    remote_cmd = f"sqlite3 -readonly {shlex.quote(db_path)} {shlex.quote(query)}"
    return subprocess.run(["ssh", node, remote_cmd],
                         capture_output=True, text=True, check=True).stdout


def _one_line(exc_or_text):
    """Render an exception (or raw text) as a single-line cause string."""
    text = str(exc_or_text).strip()
    return text.splitlines()[0] if text else repr(exc_or_text)


def _method(args):
    """Extract the RPC method name from a `cli()` argv tuple, whether
    called positionally (`("revenue-history",)`) or via `-k` keyword form
    (`("-k", "revenue-report", "report_type=costs")`)."""
    return args[1] if args and args[0] == "-k" else args[0]


def recursive_diff(py, rs, path="", skip_paths=frozenset()):
    """Walk two JSON-like values in parallel, yielding one result dict per
    leaf (or per shape mismatch), with a dotted `path` naming its
    location (e.g. "closed_channels_summary.avg_days_open").

    Returns a list of:
      {"path": ..., "status": "ok",      "py": ..., "rs": ...}
      {"path": ..., "status": "mismatch","py": ..., "rs": ...}
      {"path": ..., "status": "skipped"}

    Any path in `skip_paths` is rendered "skipped" and NOT recursed into
    or compared -- this is how phase1b-gap fields (and lists/dicts of
    mismatched shape underneath them, e.g. `warnings: [] ` vs a
    non-empty Python list) are kept out of the mismatch tally.

    Numeric comparison: see the module docstring's numeric-comparison
    note (ints/strings exact, floats via round(x, 6)).
    """
    if path in skip_paths:
        return [{"path": path, "status": "skipped"}]

    if isinstance(py, dict) and isinstance(rs, dict):
        results = []
        for key in sorted(set(py) | set(rs)):
            subpath = f"{path}.{key}" if path else key
            if subpath in skip_paths:
                # Must be checked BEFORE the missing-key branches below:
                # a gap-only key like `_phase1b_gaps` exists on only one
                # side by design, and that is not a real mismatch.
                results.append({"path": subpath, "status": "skipped"})
            elif key not in py:
                results.append({"path": subpath, "status": "mismatch",
                               "py": "<missing>", "rs": rs[key]})
            elif key not in rs:
                results.append({"path": subpath, "status": "mismatch",
                               "py": py[key], "rs": "<missing>"})
            else:
                results.extend(recursive_diff(py[key], rs[key], subpath, skip_paths))
        return results

    if isinstance(py, list) and isinstance(rs, list):
        if len(py) != len(rs):
            return [{"path": path or "<root>", "status": "mismatch", "py": py, "rs": rs}]
        results = []
        for i, (pv, rv) in enumerate(zip(py, rs)):
            results.extend(recursive_diff(pv, rv, f"{path}[{i}]", skip_paths))
        return results

    if isinstance(py, float) or isinstance(rs, float):
        try:
            equal = round(float(py), 6) == round(float(rs), 6)
        except (TypeError, ValueError):
            equal = py == rs
    else:
        equal = py == rs

    label = path or "<root>"
    if equal:
        return [{"path": label, "status": "ok", "py": py, "rs": rs}]
    return [{"path": label, "status": "mismatch", "py": py, "rs": rs}]


def diff_rpc(cli_fn, node, py_args, rs_args, skip_paths=frozenset()):
    """Fetch the python/rust JSON responses for one read-RPC pair and
    return a list of leaf-level result dicts (see recursive_diff), plus
    "error"/"transport" single-element results for whole-response
    failures.

    "error" means one side's TOP-LEVEL response is a well-formed
    `{"error": ...}` envelope -- never treated as a field mismatch.
    "transport" means the CLI call itself failed (ssh/RPC exit code or
    bad JSON), a harder failure than either "error" or "mismatch".

    The Rust response's own `_phase1b_gaps` array (if present) is
    self-derived at runtime and merged into `skip_paths`, along with the
    `_phase1b_gaps` key itself (see module docstring, comparison 3).
    """
    try:
        py = cli_fn(node, *py_args)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        return [{"path": "<root>", "status": "transport", "side": "python", "cause": _one_line(exc)}]

    try:
        rs = cli_fn(node, *rs_args)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        return [{"path": "<root>", "status": "transport", "side": "rust", "cause": _one_line(exc)}]

    if isinstance(py, dict) and "error" in py:
        return [{"path": "<root>", "status": "error", "side": "python", "cause": _one_line(py["error"])}]
    if isinstance(rs, dict) and "error" in rs:
        return [{"path": "<root>", "status": "error", "side": "rust", "cause": _one_line(rs["error"])}]

    all_skips = set(skip_paths)
    if isinstance(rs, dict) and GAP_KEY in rs:
        gaps = rs.get(GAP_KEY) or []
        if isinstance(gaps, list):
            all_skips.update(str(g) for g in gaps)
        all_skips.add(GAP_KEY)

    return recursive_diff(py, rs, "", all_skips)


def report_section(name, results):
    """Print an aligned-column report for one RPC comparison's results
    and return its severity: 0 parity, 1 mismatch/error, 2 transport."""
    mismatches = [r for r in results if r["status"] == "mismatch"]
    errors = [r for r in results if r["status"] == "error"]
    transport = [r for r in results if r["status"] == "transport"]
    skipped = [r for r in results if r["status"] == "skipped"]
    ok = [r for r in results if r["status"] == "ok"]

    rows = []
    for r in mismatches:
        rows.append(("MISMATCH", r["path"], f"python={r['py']!r} rust={r['rs']!r}"))
    for r in errors:
        rows.append(("ERROR", r["path"], f"({r['side']}): {r['cause']}"))
    for r in transport:
        rows.append(("TRANSPORT", r["path"], f"({r['side']}): {r['cause']}"))
    for r in skipped:
        rows.append(("SKIPPED", r["path"], "phase1b gap / volatile field"))

    print(f"--- {name} ---")
    if rows:
        w_status = max(len(row[0]) for row in rows)
        w_path = max(len(row[1]) for row in rows)
        for status, path, rest in rows:
            print(f"{status:<{w_status}}  {path:<{w_path}}  {rest}")

    if transport:
        print(f"{name}: transport errors: {len(transport)}", file=sys.stderr)
        return 2
    if mismatches or errors:
        return 1
    print(f"{name}: parity ({len(ok)} fields identical, {len(skipped)} skipped)")
    return 0


# Clamp floor: never look further back than this many seconds, regardless
# of what the observer's own MIN(timestamp) says -- see module docstring's
# "ingestion window" note (hydration backfills 14d, Python prunes ~8d; a
# 7-day floor leaves a one-day margin against prune-timing jitter).
INGESTION_WINDOW_CLAMP_SECONDS = 7 * 86400


def diff_ingestion(sqlite_fn, node, observer_db, python_db, tolerance=2, since_override=None):
    """Compare `ingested_forwards` row count (Rust observer db) against
    `forwards` row count (Python production db) over the window derived
    from the observer's own earliest row, clamped forward so it never
    predates Python's own forward-retention horizon -- see module
    docstring's "ingestion window" note. Never raises; failures come back
    as a "transport" result dict, same shape family as diff_rpc()'s
    results.

    `since_override`, when given, is used verbatim as `since_ts` and skips
    the `MIN(timestamp)` query (and the clamp) entirely -- for a caller
    who wants to pin an exact window rather than trust the derived value
    (the harness's own `--since` flag).
    """
    if since_override is not None:
        since_ts = since_override
    else:
        try:
            min_raw = sqlite_fn(node, observer_db, "SELECT MIN(timestamp) FROM ingested_forwards")
        except subprocess.CalledProcessError as exc:
            return {"status": "transport", "side": "rust", "cause": _one_line(exc)}

        min_raw = (min_raw or "").strip()
        derived_ts = int(min_raw) if min_raw else int(time.time())
        since_ts = max(derived_ts, int(time.time()) - INGESTION_WINDOW_CLAMP_SECONDS)

    try:
        # Both sides MUST be filtered by the same since_ts: the observer
        # hydrates a 14-day backfill while the window clamps to 7 days, so
        # an unfiltered Rust count over-reports by the whole clamped-out
        # backfill (found live 2026-07-16: 1560 unfiltered vs 847 == 847
        # in-window on both sides).
        rs_raw = sqlite_fn(
            node,
            observer_db,
            f"SELECT COUNT(*) FROM ingested_forwards WHERE timestamp >= {since_ts}",
        )
        rs_count = int(rs_raw.strip())
    except subprocess.CalledProcessError as exc:
        return {"status": "transport", "side": "rust", "cause": _one_line(exc)}
    except ValueError as exc:
        return {"status": "transport", "side": "rust", "cause": _one_line(exc)}

    try:
        py_raw = sqlite_fn(node, python_db, f"SELECT COUNT(*) FROM forwards WHERE timestamp >= {since_ts}")
        py_count = int(py_raw.strip())
    except subprocess.CalledProcessError as exc:
        return {"status": "transport", "side": "python", "cause": _one_line(exc)}
    except ValueError as exc:
        return {"status": "transport", "side": "python", "cause": _one_line(exc)}

    diff = abs(rs_count - py_count)
    status = "ok" if diff <= tolerance else "mismatch"
    return {"status": status, "py": py_count, "rs": rs_count,
            "diff": diff, "tolerance": tolerance, "since_ts": since_ts}


def report_ingestion(result):
    """Print the ingestion cross-check result and return its severity:
    0 within tolerance, 1 outside tolerance, 2 transport."""
    print("--- ingestion cross-check (ingested_forwards vs forwards) ---")
    if result["status"] == "transport":
        print(f"TRANSPORT  row_count  ({result['side']}): {result['cause']}", file=sys.stderr)
        return 2
    if result["status"] == "mismatch":
        print(f"MISMATCH  row_count  python={result['py']} rust={result['rs']} "
              f"diff={result['diff']} tolerance={result['tolerance']} since_ts={result['since_ts']}")
        return 1
    print(f"ingestion cross-check: parity within tolerance "
          f"(python={result['py']} rust={result['rs']} diff={result['diff']} "
          f"tolerance={result['tolerance']} since_ts={result['since_ts']})")
    return 0


# ---------------------------------------------------------------------------
# Self-test fixtures -- shapes read directly from crates/revops/src/
# rpc_history.rs, rpc_report.rs, rpc_dashboard.rs (Rust) and
# modules/profitability_analyzer.py:1372 / cl-revenue-ops.py:5493-5521 /
# cl-revenue-ops.py:5803-5821 (Python), in ~/bin/cl_revenue_ops-port.
# ---------------------------------------------------------------------------

HISTORY_STUB = {
    "lifetime_revenue_sats": 100000,
    "lifetime_opening_costs_sats": 5000,
    "lifetime_closure_costs_sats": 3000,
    "lifetime_rebalance_costs_sats": 2000,
    "lifetime_total_costs_sats": 10000,
    "lifetime_net_profit_sats": 90000,
    "lifetime_roi_percent": 900.0,
    "lifetime_forward_count": 500,
    "closed_channels_summary": {
        "channel_count": 3,
        "total_capacity": 3000000,
        "total_open_costs": 15000,
        "total_closure_costs": 9000,
        "total_revenue": 50000,
        "total_rebalance_costs": 6000,
        "total_forwards": 120,
        "total_net_pnl": 20000,
        "avg_days_open": 45.5,
    },
}

REPORT_COSTS_STUB = {
    "type": "costs",
    "closure_costs": {
        "last_24h_sats": 0,
        "last_7d_sats": 1000,
        "last_30d_sats": 4000,
        "total_sats": 9000,
    },
    "estimated_defaults": {
        "channel_open_sats": 5000,
        "channel_close_sats": 3000,
    },
    "generated_at": 1700000000,
}

# Python has real values for the four Phase 1b gap fields; Rust returns
# null for each and lists their dotted paths in _phase1b_gaps (per
# rpc_dashboard.rs's build_dashboard).
DASHBOARD_PY = {
    "financial_health": {
        "tlv_sats": 2000000,
        "net_profit_sats": 40000,
        "operating_margin_pct": 55.5,
        "annualized_roc_pct": 12.3,
    },
    "period": {
        "window_days": 30,
        "gross_revenue_sats": 90000,
        "opex_sats": 50000,
        "rebalance_cost_sats": 20000,
        "closure_cost_sats": 5000,
        "volume_sats": 500000,
        "forward_count": 300,
    },
    "warnings": ["Channel 123x1x0 is bleeding: Spent 500 sats rebalancing, earned 10 sats."],
    "bleeder_count": 1,
}

DASHBOARD_RS = {
    "financial_health": {
        "tlv_sats": None,
        "net_profit_sats": 40000,
        "operating_margin_pct": 55.5,
        "annualized_roc_pct": None,
    },
    "period": {
        "window_days": 30,
        "gross_revenue_sats": 90000,
        "opex_sats": 50000,
        "rebalance_cost_sats": 20000,
        "closure_cost_sats": 5000,
        "volume_sats": 500000,
        "forward_count": 300,
    },
    "warnings": [],
    "bleeder_count": None,
    "_phase1b_gaps": [
        "financial_health.tlv_sats",
        "financial_health.annualized_roc_pct",
        "warnings",
        "bleeder_count",
    ],
}


def self_test():
    """Exercise the contract paths with stubbed CLI/sqlite calls (no
    ssh/node needed). Invoke via: python3 diff_read_rpcs.py --self-test"""
    ok = True

    # -- 0. parser default is the confirmed absolute observer DB path --
    # build_parser() is shared with main(), so this pins the live parser rather
    # than a self-test-only duplicate.
    observer_default = build_parser().parse_args([]).observer_db
    default_ok = (
        observer_default == DEFAULT_OBSERVER_DB
        and not observer_default.startswith("~")
    )
    print(
        f"[self-test] observer-db default: {observer_default!r} "
        f"(expect {DEFAULT_OBSERVER_DB!r}, absolute) => "
        f"{'PASS' if default_ok else 'FAIL'}"
    )
    ok = ok and default_ok

    # -- 1. parity pass (revenue-history, no params) --
    def cli_hist_parity(node, *args):
        return copy.deepcopy(HISTORY_STUB)

    results = diff_rpc(cli_hist_parity, "node", ("revenue-history",), ("revenue-r-history",))
    rc = report_section("history parity", results)
    print(f"[self-test] parity pass: exit={rc} (expect 0)")
    ok = ok and rc == 0

    # -- 2. nested mismatch --
    def cli_hist_mismatch(node, *args):
        payload = copy.deepcopy(HISTORY_STUB)
        payload["closed_channels_summary"]["avg_days_open"] = (
            45.5 if _method(args) == "revenue-history" else 99.9)
        return payload

    results = diff_rpc(cli_hist_mismatch, "node", ("revenue-history",), ("revenue-r-history",))
    rc = report_section("history nested mismatch", results)
    mismatch_paths = {r["path"] for r in results if r["status"] == "mismatch"}
    print(f"[self-test] nested mismatch: exit={rc} paths={mismatch_paths} "
          f"(expect 1, {{'closed_channels_summary.avg_days_open'}})")
    ok = ok and rc == 1 and mismatch_paths == {"closed_channels_summary.avg_days_open"}

    # -- 3. report-costs parity, incl. generated_at always-skip --
    def cli_report_parity(node, *args):
        payload = copy.deepcopy(REPORT_COSTS_STUB)
        payload["generated_at"] = (
            1700000000 if _method(args) == "revenue-report" else 1700000005)
        return payload

    results = diff_rpc(cli_report_parity, "node",
                      ("-k", "revenue-report", "report_type=costs"),
                      ("-k", "revenue-r-report", "report_type=costs"),
                      skip_paths=GENERATED_AT_SKIP)
    rc = report_section("report costs parity", results)
    skipped_paths = {r["path"] for r in results if r["status"] == "skipped"}
    print(f"[self-test] report costs parity (generated_at skip): exit={rc} "
          f"skipped={skipped_paths} (expect 0, {{'generated_at'}})")
    ok = ok and rc == 0 and skipped_paths == {"generated_at"}

    # -- 4. gap-skip behavior (dashboard) --
    def cli_dashboard_gaps(node, *args):
        return copy.deepcopy(DASHBOARD_PY if _method(args) == "revenue-dashboard" else DASHBOARD_RS)

    results = diff_rpc(cli_dashboard_gaps, "node",
                      ("-k", "revenue-dashboard", "window_days=30"),
                      ("-k", "revenue-r-dashboard", "window_days=30"))
    rc = report_section("dashboard gap-skip", results)
    skipped_paths = {r["path"] for r in results if r["status"] == "skipped"}
    mismatches = [r for r in results if r["status"] == "mismatch"]
    expected_skips = {
        "financial_health.tlv_sats", "financial_health.annualized_roc_pct",
        "warnings", "bleeder_count", GAP_KEY,
    }
    print(f"[self-test] dashboard gap-skip: exit={rc} skipped={skipped_paths} "
          f"mismatches={len(mismatches)} (expect 0, skipped=={expected_skips!r}, 0 mismatches)")
    ok = ok and rc == 0 and skipped_paths == expected_skips and not mismatches

    # -- 5. transport failure --
    def cli_transport(node, *args):
        raise subprocess.CalledProcessError(255, ["ssh"])

    results = diff_rpc(cli_transport, "node", ("revenue-history",), ("revenue-r-history",))
    rc = report_section("transport failure", results)
    print(f"[self-test] transport failure: exit={rc} (expect 2)")
    ok = ok and rc == 2

    # -- 5b. error-envelope (bonus: ERROR-vs-MISMATCH separation) --
    def cli_error(node, *args):
        if _method(args) == "revenue-r-report":
            return {"error": "not_yet_ported", "report_type": "costs", "reason": "Phase 3"}
        return copy.deepcopy(REPORT_COSTS_STUB)

    results = diff_rpc(cli_error, "node",
                      ("-k", "revenue-report", "report_type=costs"),
                      ("-k", "revenue-r-report", "report_type=costs"))
    rc = report_section("error envelope", results)
    statuses = {r["status"] for r in results}
    print(f"[self-test] error envelope: exit={rc} statuses={statuses} "
          f"(expect 1, {{'error'}} not 'mismatch')")
    ok = ok and rc == 1 and statuses == {"error"}

    # -- 6. ingestion count-tolerance pass (diff=1, tolerance=2) --
    def sqlite_pass(node, db_path, query):
        if "ingested_forwards" in query:
            return "1000\n" if "MIN" in query else "50\n"
        return "51\n"

    result = diff_ingestion(sqlite_pass, "node",
                           "~/.lightning/revops-r-observer.db",
                           "~/.lightning/revenue_ops.db", tolerance=2)
    rc = report_ingestion(result)
    print(f"[self-test] ingestion tolerance pass (diff=1, tolerance=2): exit={rc} (expect 0)")
    ok = ok and rc == 0

    # -- 6b. ingestion count-tolerance fail (diff=10, tolerance=2) --
    def sqlite_fail(node, db_path, query):
        if "ingested_forwards" in query:
            return "1000\n" if "MIN" in query else "50\n"
        return "60\n"

    result = diff_ingestion(sqlite_fail, "node",
                           "~/.lightning/revops-r-observer.db",
                           "~/.lightning/revenue_ops.db", tolerance=2)
    rc = report_ingestion(result)
    print(f"[self-test] ingestion tolerance fail (diff=10, tolerance=2): exit={rc} (expect 1)")
    ok = ok and rc == 1

    # -- 6c. ingestion transport failure --
    def sqlite_transport(node, db_path, query):
        raise subprocess.CalledProcessError(1, ["sqlite3"])

    result = diff_ingestion(sqlite_transport, "node",
                           "~/.lightning/revops-r-observer.db",
                           "~/.lightning/revenue_ops.db", tolerance=2)
    rc = report_ingestion(result)
    print(f"[self-test] ingestion transport failure: exit={rc} (expect 2)")
    ok = ok and rc == 2

    # -- 6d. ingestion window clamp (IMPORTANT 2): the observer's own
    # MIN(timestamp) can legitimately be ~14 days old (hydration's own
    # backfill window), but Python's `forwards` table is pruned at ~8
    # days -- an unclamped since_ts guarantees a spurious mismatch on any
    # real node. Confirm since_ts is clamped to the 7-day floor, not the
    # raw 14-day-old value.
    old_min = int(time.time()) - 14 * 86400

    def sqlite_old_min(node, db_path, query):
        if "ingested_forwards" in query:
            return f"{old_min}\n" if "MIN" in query else "50\n"
        return "51\n"

    result = diff_ingestion(sqlite_old_min, "node",
                           "~/.lightning/revops-r-observer.db",
                           "~/.lightning/revenue_ops.db", tolerance=2)
    rc = report_ingestion(result)
    clamp_floor = int(time.time()) - INGESTION_WINDOW_CLAMP_SECONDS
    print(f"[self-test] ingestion window clamp: exit={rc} since_ts={result['since_ts']} "
          f"clamp_floor={clamp_floor} old_min={old_min} "
          f"(expect since_ts >= clamp_floor, since_ts != old_min)")
    ok = ok and result["since_ts"] >= clamp_floor and result["since_ts"] != old_min

    # -- 6e. --since override skips the derived MIN() query (and the
    # clamp) entirely, using the caller's value verbatim.
    def sqlite_since_override(node, db_path, query):
        if "MIN" in query:
            raise AssertionError("since_override must skip the MIN(timestamp) query entirely")
        if "ingested_forwards" in query:
            return "50\n"
        return "51\n"

    result = diff_ingestion(sqlite_since_override, "node",
                           "~/.lightning/revops-r-observer.db",
                           "~/.lightning/revenue_ops.db", tolerance=2,
                           since_override=1_600_000_000)
    rc = report_ingestion(result)
    print(f"[self-test] ingestion --since override: exit={rc} since_ts={result['since_ts']} "
          f"(expect since_ts==1600000000, MIN(timestamp) query never run)")
    ok = ok and result["since_ts"] == 1_600_000_000

    print("[self-test] ALL PASS" if ok else "[self-test] FAILURE")
    return 0 if ok else 1


def build_parser():
    """Build the CLI parser shared by main() and the no-node self-test."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="lnnode")
    ap.add_argument("--self-test", action="store_true",
                    help="run the built-in self-test (stubbed CLI/sqlite, no live node) and exit")
    ap.add_argument("--window-days", type=int, default=30,
                    help="window_days passed to both revenue-dashboard and revenue-r-dashboard (default: 30)")
    ap.add_argument("--observer-db", default=DEFAULT_OBSERVER_DB,
                    help=f"Rust observer's own writable sqlite db, on --node (default: {DEFAULT_OBSERVER_DB})")
    ap.add_argument("--python-db", default="/data/lightningd/.lightning/revenue_ops.db",
                    help="Python plugin's production sqlite db, on --node (default: "
                         "/data/lightningd/.lightning/revenue_ops.db -- the CONFIRMED absolute "
                         "path, not a `~`-relative guess: on lnnode $HOME/.lightning symlinks to "
                         "/data/lightningd, not to the nested .lightning/ dir revenue_ops.db "
                         "actually lives in, and sqlite_query()'s shlex.quote() suppresses "
                         "remote tilde expansion anyway -- see docs/runbooks/observer-deploy.md)")
    ap.add_argument("--tolerance", type=int, default=2,
                    help="allowed row-count drift for the ingestion cross-check (default: 2)")
    ap.add_argument("--since", type=int, default=None,
                    help="override the ingestion cross-check's window start (unix ts) instead of "
                         "deriving it from the observer db's own MIN(timestamp), clamped to a "
                         "7-day floor (see module docstring's ingestion-window note)")
    return ap


def main(argv=None, cli_fn=cli, sqlite_fn=sqlite_query):
    args = build_parser().parse_args(argv)

    if args.self_test:
        return self_test()

    codes = []

    results = diff_rpc(cli_fn, args.node, ("revenue-history",), ("revenue-r-history",))
    codes.append(report_section("revenue-history vs revenue-r-history", results))

    results = diff_rpc(cli_fn, args.node,
                      ("-k", "revenue-report", "report_type=costs"),
                      ("-k", "revenue-r-report", "report_type=costs"),
                      skip_paths=GENERATED_AT_SKIP)
    codes.append(report_section("revenue-report costs vs revenue-r-report costs", results))

    results = diff_rpc(cli_fn, args.node,
                      ("-k", "revenue-dashboard", f"window_days={args.window_days}"),
                      ("-k", "revenue-r-dashboard", f"window_days={args.window_days}"))
    codes.append(report_section(
        f"revenue-dashboard vs revenue-r-dashboard (window_days={args.window_days})", results))

    result = diff_ingestion(sqlite_fn, args.node, args.observer_db, args.python_db,
                           tolerance=args.tolerance, since_override=args.since)
    codes.append(report_ingestion(result))

    return max(codes) if codes else 0


if __name__ == "__main__":
    sys.exit(main())
