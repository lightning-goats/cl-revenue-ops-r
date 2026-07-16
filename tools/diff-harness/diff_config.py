#!/usr/bin/env python3
# tools/diff-harness/diff_config.py
"""Diff Python plugin config values against the Rust shadow plugin.

Usage: ./diff_config.py [--node lnnode]
Exits 0 on full parity; prints a table of mismatches and exits 1 otherwise.

Note: Rust RPC is invoked with `lightning-cli -k` (keyword form) to simplify
quoting over ssh: `lightning-cli -k revenue-r-config key=<suffix>`.
"""
import argparse, json, subprocess, sys, pathlib

# Keys to skip in comparison. revenue-ops-db-path is deliberately not shadow-registered
# by the Rust plugin per the design spec's db-path ruling.
SKIP_KEYS = {"db-path"}

def cli(node, *args):
    out = subprocess.run(["ssh", node, "lightning-cli", *args],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="lnnode")
    args = ap.parse_args()
    table = json.load(open(pathlib.Path(__file__).parents[2] / "fixtures/options.json"))
    mismatches = []
    for opt in table:
        suffix = opt["name"].removeprefix("revenue-ops-")
        if suffix in SKIP_KEYS:
            continue
        py = cli(args.node, "revenue-config", "get", suffix)
        rs = cli(args.node, "-k", "revenue-r-config", f"key={suffix}")
        if py.get("value") != rs.get("value"):
            mismatches.append((suffix, py.get("value"), rs.get("value")))
    if mismatches:
        for k, p, r in mismatches:
            print(f"MISMATCH {k}: python={p!r} rust={r!r}")
        sys.exit(1)
    print(f"parity: {len(table) - len(SKIP_KEYS)} keys identical")

if __name__ == "__main__":
    main()
