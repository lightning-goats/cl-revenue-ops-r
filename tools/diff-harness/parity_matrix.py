#!/usr/bin/env python3
# tools/diff-harness/parity_matrix.py
"""Whole-plugin RPC parity matrix: every Python RPC vs its Rust counterpart.

Usage: ./parity_matrix.py [--node lnnode] [--only <substr>] [--self-test]

`diff_read_rpcs.py` compares three hand-picked RPC pairs. This is the
table-driven generalisation: ONE row per Python RPC, so the report always
covers the full 69-method surface and an unported method shows up as a
tracked gap rather than as silence.

Exit codes (worst seen wins, same convention as `diff_read_rpcs.py`):
  0  every PAIRED rpc matched
  1  at least one field mismatch or per-RPC error envelope
  2  a transport failure (ssh/RPC nonzero exit, unparseable JSON)

Note that exit 0 does NOT mean "full parity" -- it means everything that
is currently paired agrees. The coverage line is the number that answers
"how much of the plugin is ported", and the two are deliberately reported
separately so a high pass rate over a small paired set can never read as
completeness.

## Why the gap convention is load-bearing

Rust builders emit `null` plus a `_gaps` / `_phase1b_gaps` array for any
field they cannot yet produce. This harness reads that array OUT OF THE
RESPONSE ITSELF and skips exactly those paths. That means:

  * a declared gap is not counted as a mismatch (it is known and tracked);
  * a field that Rust silently omits or invents IS counted;
  * removing a field from `_gaps` without implementing it turns the run
    red immediately.

The alternative -- a skip list maintained here -- would drift from the
code and quietly hide regressions, which is the failure mode this whole
project keeps hitting.
"""

import argparse
import json
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from diff_read_rpcs import cli, recursive_diff  # noqa: E402

GAP_KEYS = ("_gaps", "_phase1b_gaps")

# (python_method, rust_method_or_None, extra_args)
# rust_method None == not ported yet. Keep every Python RPC listed so the
# denominator is honest.
MATRIX = [
    # --- paired today ---
    ("revenue-history", "revenue-r-history", ()),
    ("revenue-report", "revenue-r-report", ("report_type=costs",)),
    ("revenue-dashboard", "revenue-r-dashboard", ()),
    # NOT paired, despite the similar names -- caught by this harness on its
    # first live run. `revenue-r-status` is the Rust PORT's own health probe
    # (version, db table count, seed provenance); Python's `revenue-status`
    # returns per-channel state rows. Different responses, different purpose.
    # Pairing them produced a huge spurious mismatch, so the real Python
    # `revenue-status` is recorded as UNPORTED, which it is.
    ("revenue-status", None, ()),
    # Likewise `revenue-r-config` reads ONE option by key; Python's
    # `revenue-config` is a get/set surface over the whole config. Only the
    # read half exists, and it takes a required `key` param, so a
    # no-arg whole-config diff is not the right comparison.
    ("revenue-config", None, ()),
    ("revenue-fee-debug", "revenue-r-fee-debug", ()),
    # --- read-only batches in flight (rust side may not exist yet) ---
    ("revenue-health", "revenue-r-health", ()),
    ("revenue-profitability", "revenue-r-profitability", ()),
    ("revenue-analyze", "revenue-r-analyze", ()),
    ("revenue-policy", "revenue-r-policy", ()),
    ("revenue-list-banned", "revenue-r-list-banned", ()),
    ("revenue-list-ignored", "revenue-r-list-ignored", ()),
    ("revenue-hot-channel-protection-peers", "revenue-r-hot-channel-protection-peers", ()),
    ("revenue-capacity-report", "revenue-r-capacity-report", ()),
    ("revenue-econ-snapshot", "revenue-r-econ-snapshot", ()),
    ("revenue-spend-ledger", "revenue-r-spend-ledger", ()),
    ("revenue-planner-status", "revenue-r-planner-status", ()),
    ("revenue-planner-candidates", "revenue-r-planner-candidates", ()),
    ("revenue-planner-candidate-sources", "revenue-r-planner-candidate-sources", ()),
    ("revenue-planner-history", "revenue-r-planner-history", ()),
    ("revenue-capex-status", "revenue-r-capex-status", ()),
    ("revenue-total-cost-budget", "revenue-r-total-cost-budget", ()),
    ("revenue-lnplus-status", "revenue-r-lnplus-status", ()),
    ("revenue-boltz-status", "revenue-r-boltz-status", ()),
    ("revenue-boltz-budget", "revenue-r-boltz-budget", ()),
    ("revenue-boltz-history", "revenue-r-boltz-history", ()),
    ("revenue-rebalance-debug", "revenue-r-rebalance-debug", ()),
    ("revenue-econ-reconcile", "revenue-r-econ-reconcile", ()),
    # --- deliberately unported: MUTATING or execution-path RPCs ---
    # Listed so the denominator is the real 69 and so nobody has to
    # rediscover that these are intentionally absent from Rust.
    ("revenue-set-fee", None, ()),
    ("revenue-fee-cycle", None, ()),
    ("revenue-rebalance", None, ()),
    ("revenue-rebalance-cycle", None, ()),
    ("revenue-wake-all", None, ()),
    ("revenue-planner-execute", None, ()),
    ("revenue-ban", None, ()),
    ("revenue-unban", None, ()),
    ("revenue-ignore", None, ()),
    ("revenue-unignore", None, ()),
    ("revenue-cleanup-closed", None, ()),
    ("revenue-clear-reservations", None, ()),
    ("revenue-spend-reserve", None, ()),
    ("revenue-spend-release", None, ()),
    ("revenue-spend-release-stale", None, ()),
    ("revenue-spend-settle", None, ()),
    ("revenue-econ-cycle", None, ()),
    ("revenue-profile-preview", None, ()),
    ("revenue-lnplus-abandon", None, ()),
    ("revenue-lnplus-backfill", None, ()),
    ("revenue-lnplus-breaker-clear", None, ()),
    ("revenue-boltz-auto-cycle-run-now", None, ()),
    ("revenue-boltz-balance-cycle", None, ()),
    ("revenue-boltz-chainswap", None, ()),
    ("revenue-boltz-claim", None, ()),
    ("revenue-boltz-refund", None, ()),
    ("revenue-boltz-deposit", None, ()),
    ("revenue-boltz-withdraw", None, ()),
    ("revenue-boltz-loop-in", None, ()),
    ("revenue-boltz-loop-out", None, ()),
    ("revenue-boltz-quote", None, ()),
    ("revenue-boltz-backup", None, ()),
    ("revenue-boltz-backup-verify", None, ()),
    ("revenue-boltz-wallet", None, ()),
    ("revenue-boltz-expansion-treasury-cycle", None, ()),
    ("revenue-boltz-expansion-treasury-recommendations", None, ()),
    ("revenue-boltz-expansion-treasury-status", None, ()),
    ("revenue-boltz-external-pay-ignores", None, ()),
    ("revenue-boltz-balance-recommendations", None, ()),
    ("revenue-boltz-auto-cycle-status", None, ()),
    ("revenue-fee-authority-status", None, ()),
]


def gap_paths(resp):
    """Paths the RUST response itself declares it cannot produce."""
    out = set()
    if not isinstance(resp, dict):
        return out
    for key in GAP_KEYS:
        for entry in resp.get(key) or []:
            if isinstance(entry, str):
                out.add(entry)
        out.add(key)
    return out


def compare_one(cli_fn, node, py_method, rs_method, extra):
    """Return (status, detail). status in {match, mismatch, missing, error, unported}."""
    if rs_method is None:
        return ("unported", "no rust counterpart declared")
    py_args = ("-k", py_method, *extra) if extra else (py_method,)
    rs_args = ("-k", rs_method, *extra) if extra else (rs_method,)
    try:
        py_raw = cli_fn(node, *py_args)
        rs_raw = cli_fn(node, *rs_args)
    except Exception as exc:  # transport
        return ("error", f"transport: {exc}")
    # `cli()` already returns parsed JSON; the fakes in self_test return
    # raw strings. Accept both rather than assuming one.
    try:
        py = json.loads(py_raw) if isinstance(py_raw, (str, bytes, bytearray)) else py_raw
        rs = json.loads(rs_raw) if isinstance(rs_raw, (str, bytes, bytearray)) else rs_raw
    except Exception as exc:
        return ("error", f"unparseable JSON: {exc}")
    # An unknown Rust method comes back as a CLN error envelope.
    if isinstance(rs, dict) and rs.get("code") and "message" in rs:
        return ("missing", str(rs.get("message"))[:90])
    # recursive_diff returns one entry per LEAF, including status "ok" and
    # "skipped" -- only "mismatch" entries are failures. (Truthiness on the
    # whole list would mark every identical response as a mismatch.)
    results = recursive_diff(py, rs, skip_paths=frozenset(gap_paths(rs)))
    bad = [d for d in results if d.get("status") == "mismatch"]
    if bad:
        detail = "; ".join(f"{d['path']}: py={d['py']!r} rs={d['rs']!r}" for d in bad[:3])
        return ("mismatch", detail)
    skipped = sum(1 for d in results if d.get("status") == "skipped")
    return ("match", f"{skipped} declared-gap path(s) skipped")


def main(argv=None, cli_fn=cli):
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="lnnode")
    ap.add_argument("--only", default=None, help="substring filter on the python method")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args(argv)

    if args.self_test:
        return self_test()

    rows = [r for r in MATRIX if not args.only or args.only in r[0]]
    counts = {}
    worst = 0
    print(f"{'python rpc':<45} {'status':<10} detail")
    print("-" * 100)
    for py_method, rs_method, extra in rows:
        status, detail = compare_one(cli_fn, args.node, py_method, rs_method, extra)
        counts[status] = counts.get(status, 0) + 1
        if status == "error":
            worst = max(worst, 2)
        elif status in ("mismatch", "missing"):
            worst = max(worst, 1)
        print(f"{py_method:<45} {status:<10} {detail}")

    total = len(MATRIX)
    paired = sum(1 for _, r, _ in MATRIX if r is not None)
    print("-" * 100)
    print(f"coverage : {paired}/{total} python rpcs have a rust counterpart declared")
    print(f"results  : " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    print(
        "note     : exit 0 means every PAIRED rpc agreed; it does NOT mean full "
        "parity. Read the coverage line for that."
    )
    return worst


def _mismatches(py, rs):
    res = recursive_diff(py, rs, skip_paths=frozenset(gap_paths(rs)))
    return [d for d in res if d.get("status") == "mismatch"]


def self_test():
    """Offline checks -- no node, no ssh."""
    # A declared gap must be skipped, an undeclared difference must not be.
    py = {"a": 1, "b": 2}
    rs = {"a": 1, "b": None, "_gaps": ["b"]}
    assert not _mismatches(py, rs), "declared gap must be skipped"

    rs_bad = {"a": 1, "b": None}
    assert _mismatches(py, rs_bad), (
        "an UNDECLARED null must be a mismatch -- this is the control proving "
        "the gap-skipping above is not simply skipping everything"
    )

    # Removing a field from _gaps without implementing it must go red.
    rs_lying = {"a": 1, "b": None, "_gaps": []}
    assert _mismatches(py, rs_lying), (
        "un-declaring a gap without implementing the field must turn red"
    )

    fake = {
        ("revenue-history",): '{"x": 1}',
        ("revenue-r-history",): '{"x": 1}',
    }

    def cli_fn(_node, *a):
        return fake[a]

    assert compare_one(cli_fn, "n", "revenue-history", "revenue-r-history", ())[0] == "match"
    assert compare_one(cli_fn, "n", "revenue-set-fee", None, ())[0] == "unported"
    print("parity_matrix self-test OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
