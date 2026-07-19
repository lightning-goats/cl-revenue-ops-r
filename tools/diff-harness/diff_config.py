#!/usr/bin/env python3
# tools/diff-harness/diff_config.py
"""Diff Python plugin config values against the Rust shadow plugin.

Usage: ./diff_config.py [--node lnnode]
Exits 0 on full parity, 1 if any value mismatch or per-key error envelope
was seen, 2 if a transport failure (ssh/RPC nonzero exit, or unparseable
JSON) occurred talking to either side. Prints an aligned-column report of
whatever went wrong.

Requires Python >= 3.9 (uses str.removeprefix).

Note: Rust RPC is invoked with `lightning-cli -k` (keyword form) to simplify
quoting over ssh: `lightning-cli -k revenue-r-config key=<suffix>`.

Key namespaces: the fixture's option suffix is hyphenated (e.g. "foo-bar"),
which is exactly what the Rust side expects for `-k revenue-r-config
key=<suffix>`. The Python `revenue-config get` subcommand, however,
resolves keys via getattr() on a dataclass whose fields are underscored,
so the Python-side call uses PYTHON_FIELD_MAP for the four non-mechanical
Config field names and suffix.replace("-", "_") for all others. The
hyphenated suffix would raise/return an error envelope instead of a value.
Constructor-only options use CLN listconfigs instead because no Config field
exists for them. Two distinct key namespaces, one fixture.

Type normalization: Phase 1a's Rust shadow plugin's `revenue-r-config`
response can carry a resolved value that is already stringified (e.g.
the string `"3600"`), while the Python plugin's `revenue-config get`
returns a typed JSON scalar (the int `3600`, the bool `true`, a float).
By default both comparison paths normalize values to a canonical string
before comparing: everything goes through `str()`, except Python bools,
which are mapped explicitly to the lowercase JSON spelling
("true"/"false") rather than `str()`'s "True"/"False" -- this is the ONE
canonical form both sides are normalized to, so a bool on either side
compares equal to the matching lowercase string on the other. Pass
--strict to disable this normalization and compare raw values as-is --
intended for Phase 1b, once the Rust port returns properly typed values
and a real type mismatch should once again fail the diff.
"""
import argparse, contextlib, io, json, pathlib, subprocess, sys

# Keys to skip in comparison. revenue-ops-db-path is deliberately not shadow-registered
# by the Rust plugin per the design spec's db-path ruling.
SKIP_KEYS = {"db-path"}

# Option suffixes whose effective Python Config field is not the mechanical
# hyphen-to-underscore spelling. Rust continues to resolve the original suffix.
PYTHON_FIELD_MAP = {
    "vegas-reflex": "enable_vegas_reflex",
    "vegas-decay": "vegas_decay_rate",
    "planner-max-fee-rate": "planner_max_fee_rate_sat_vb",
    "boltz-structural-budget-sats": "boltz_structural_budget_sats_per_day",
}

# These startup-only Boltz constructor options have no Python Config dataclass
# field. Compare their effective CLN option values through `listconfigs
# revenue-ops-<suffix>` instead of skipping them.
CONSTRUCTOR_ONLY_KEYS = {
    "boltz-enabled",
    "boltz-cli-path",
    "boltz-datadir",
    "boltz-use-sudo",
    "boltz-sudo-user",
    "boltz-timeout-seconds",
    "boltz-daily-budget-sats",
    "boltz-enforce-budget",
    "boltz-btc-wallet",
    "boltz-lbtc-wallet",
    "boltz-routing-fee-limit-ppm",
    "boltz-max-withdraw-sats",
}


def cli(node, *args):
    """Run `lightning-cli <args>` on `node` over ssh and parse JSON stdout.

    Raises subprocess.CalledProcessError on nonzero exit and
    json.JSONDecodeError on unparseable stdout -- both are transport-layer
    failures for our purposes and are caught by diff_key().
    """
    out = subprocess.run(["ssh", node, "lightning-cli", *args],
                         capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def _one_line(exc_or_text):
    """Render an exception (or raw text) as a single-line cause string."""
    text = str(exc_or_text).strip()
    return text.splitlines()[0] if text else repr(exc_or_text)


def normalize(value):
    """Canonicalize a value for cross-language comparison.

    `str()` for everything except Python bools, which map explicitly to
    the lowercase JSON/Rust spelling ("true"/"false") instead of str()'s
    "True"/"False" -- this is the one canonical form both sides normalize
    to. `None` (a JSON null, e.g. an unset no-default option) stays `None`
    rather than becoming the string "None", so it only ever compares equal
    to the other side's actual absence of a value, never to a literal
    string.

    NOTE: check bool before int -- in Python `bool` is an `int` subclass,
    so an `isinstance(value, int)` check would also match booleans.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def diff_key(cli_fn, node, suffix, strict=False):
    """Fetch python/rust values for one key and classify the result.

    Returns one of:
      {"key": suffix, "status": "ok",        "py": ..., "rs": ...}
      {"key": suffix, "status": "mismatch",  "py": ..., "rs": ...}
      {"key": suffix, "status": "error",     "side": "python"|"rust", "cause": "..."}
      {"key": suffix, "status": "transport", "side": "python"|"rust", "cause": "..."}

    "error" means one side returned a well-formed {"error": ...} envelope --
    that is never treated as a value mismatch. "transport" means the CLI
    call itself failed (ssh/RPC exit code or bad JSON), which is a harder
    failure than either of the above.

    By default, values are compared after `normalize()` (see above), so
    Python's typed `3600` matches Rust's stringified `"3600"`. Pass
    `strict=True` to compare raw values with no normalization.
    """
    # Python dataclass fields are underscored (revenue-config get resolves via
    # getattr); the Rust RPC keeps the hyphenated fixture suffix as-is. Same
    # fixture, two key namespaces -- translate only for the Python call.
    py_key = PYTHON_FIELD_MAP.get(suffix, suffix.replace("-", "_"))

    try:
        py = cli_fn(node, "revenue-config", "get", py_key)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        return {"key": suffix, "status": "transport", "side": "python", "cause": _one_line(exc)}

    try:
        rs = cli_fn(node, "-k", "revenue-r-config", f"key={suffix}")
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        return {"key": suffix, "status": "transport", "side": "rust", "cause": _one_line(exc)}

    if isinstance(py, dict) and "error" in py:
        return {"key": suffix, "status": "error", "side": "python", "cause": _one_line(py["error"])}
    if isinstance(rs, dict) and "error" in rs:
        return {"key": suffix, "status": "error", "side": "rust", "cause": _one_line(rs["error"])}

    py_val, rs_val = py.get("value"), rs.get("value")
    cmp_py, cmp_rs = (py_val, rs_val) if strict else (normalize(py_val), normalize(rs_val))
    if cmp_py != cmp_rs:
        return {"key": suffix, "status": "mismatch", "py": py_val, "rs": rs_val}
    return {"key": suffix, "status": "ok", "py": py_val, "rs": rs_val}


def diff_constructor_option(cli_fn, node, suffix, strict=False):
    """Compare a constructor-only Python CLN option with Rust's resolved value.

    Python has no Config field for these options, so fetch the effective startup
    value from `listconfigs revenue-ops-<suffix>`. CLN encodes the scalar under
    exactly one of value_bool/value_int/value_str; an explicit JSON null is a
    valid value, while a missing/ambiguous value field is a malformed response.
    """
    option_name = f"revenue-ops-{suffix}"
    result_base = {"key": suffix, "surface": "constructor"}

    try:
        py = cli_fn(node, "listconfigs", option_name)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        return {**result_base, "status": "transport", "side": "python", "cause": _one_line(exc)}

    if isinstance(py, dict) and "error" in py:
        return {**result_base, "status": "error", "side": "python", "cause": _one_line(py["error"])}

    try:
        configs = py["configs"]
        entry = configs[option_name]
        if not isinstance(configs, dict) or not isinstance(entry, dict):
            raise TypeError("configs entry is not an object")
        value_fields = [name for name in ("value_bool", "value_int", "value_str") if name in entry]
        if len(value_fields) != 1:
            raise ValueError(f"expected exactly one value_* field, got {len(value_fields)}")
        py_val = entry[value_fields[0]]
    except (KeyError, TypeError, ValueError) as exc:
        return {
            **result_base,
            "status": "error",
            "side": "python",
            "cause": f"malformed listconfigs response: {_one_line(exc)}",
        }

    try:
        rs = cli_fn(node, "-k", "revenue-r-config", f"key={suffix}")
    except (subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        return {**result_base, "status": "transport", "side": "rust", "cause": _one_line(exc)}

    if isinstance(rs, dict) and "error" in rs:
        return {**result_base, "status": "error", "side": "rust", "cause": _one_line(rs["error"])}
    if not isinstance(rs, dict) or "value" not in rs:
        return {
            **result_base,
            "status": "error",
            "side": "rust",
            "cause": "malformed revenue-r-config response: missing value",
        }

    rs_val = rs["value"]
    cmp_py, cmp_rs = (py_val, rs_val) if strict else (normalize(py_val), normalize(rs_val))
    if cmp_py != cmp_rs:
        return {**result_base, "status": "mismatch", "py": py_val, "rs": rs_val}
    return {**result_base, "status": "ok", "py": py_val, "rs": rs_val}


def load_fixtures(path):
    with open(path) as f:
        return json.load(f)


def run_diff(cli_fn, node, table, strict=False):
    """Diff every comparable key in `table`; capture failures per key."""
    results = []
    for opt in table:
        suffix = opt["name"].removeprefix("revenue-ops-")
        if suffix in SKIP_KEYS:
            continue
        if suffix in CONSTRUCTOR_ONLY_KEYS:
            results.append(
                diff_constructor_option(cli_fn, node, suffix, strict=strict)
            )
        else:
            results.append(diff_key(cli_fn, node, suffix, strict=strict))
    return results, len(results)


def report(results, total):
    """Print an aligned-column report of any non-ok results and return the
    process exit code: 0 parity, 1 mismatch/error, 2 transport failure.

    SKIPPED rows, if introduced for an explicitly non-comparable key, are
    informational only and never affect the exit code.
    """
    mismatches = [r for r in results if r["status"] == "mismatch"]
    errors = [r for r in results if r["status"] == "error"]
    transport = [r for r in results if r["status"] == "transport"]
    skipped = [r for r in results if r["status"] == "skipped"]

    rows = []
    for r in mismatches:
        rows.append(("MISMATCH", r["key"], f"python={r['py']!r} rust={r['rs']!r}"))
    for r in errors:
        rows.append(("ERROR", r["key"], f"({r['side']}): {r['cause']}"))
    for r in transport:
        rows.append(("TRANSPORT", r["key"], f"({r['side']}): {r['cause']}"))
    for r in skipped:
        rows.append(("SKIPPED", r["key"], r["cause"]))

    if rows:
        w_status = max(len(row[0]) for row in rows)
        w_key = max(len(row[1]) for row in rows)
        for status, key, rest in rows:
            print(f"{status:<{w_status}}  {key:<{w_key}}  {rest}")

    if transport:
        print(f"transport errors: {len(transport)}", file=sys.stderr)
        return 2
    if mismatches or errors:
        return 1
    compared = total - len(skipped)
    option_surface = sum(r.get("surface") == "constructor" for r in results)
    print(
        f"parity: {compared} keys identical "
        f"({option_surface} option-surface checks, {len(skipped)} skipped)"
    )
    return 0


def self_test():
    """Exercise comparison contracts with stubbed CLI calls (no ssh/node
    needed). Invoke via: python3 diff_config.py --self-test"""
    ok = True
    table = [{"name": "revenue-ops-foo-bar"}]

    def cli_parity(node, *a):
        return {"value": 42}

    results, total = run_diff(cli_parity, "node", table)
    rc = report(results, total)
    print(f"[self-test] parity case: exit={rc} (expect 0)")
    ok = ok and rc == 0

    def cli_mismatch(node, *a):
        return {"value": 42} if a[0] == "revenue-config" else {"value": 43}

    results, total = run_diff(cli_mismatch, "node", table)
    rc = report(results, total)
    print(f"[self-test] mismatch case: exit={rc} (expect 1)")
    ok = ok and rc == 1

    def cli_error(node, *a):
        return {"error": "unknown option"} if a[0] == "revenue-config" else {"value": 43}

    results, total = run_diff(cli_error, "node", table)
    rc = report(results, total)
    statuses = {r["status"] for r in results}
    print(f"[self-test] error-envelope case: exit={rc} statuses={statuses} (expect 1, {{'error'}} not 'mismatch')")
    ok = ok and rc == 1 and statuses == {"error"}

    def cli_transport(node, *a):
        raise subprocess.CalledProcessError(255, ["ssh"])

    results, total = run_diff(cli_transport, "node", table)
    rc = report(results, total)
    print(f"[self-test] transport-failure case: exit={rc} (expect 2)")
    ok = ok and rc == 2

    # Phase 1a's real-world shape: Python returns a typed scalar, Rust
    # returns the same value stringified. Default (normalized) comparison
    # must treat these as parity; --strict must catch it as a mismatch.
    def cli_typed_vs_string(node, *a):
        return {"value": 3600} if a[0] == "revenue-config" else {"value": "3600"}

    results, total = run_diff(cli_typed_vs_string, "node", table)
    rc = report(results, total)
    print(f"[self-test] typed-vs-string (int 3600 vs \"3600\") normalized: exit={rc} (expect 0)")
    ok = ok and rc == 0

    results, total = run_diff(cli_typed_vs_string, "node", table, strict=True)
    rc = report(results, total)
    print(f"[self-test] typed-vs-string (int 3600 vs \"3600\") --strict: exit={rc} (expect 1)")
    ok = ok and rc == 1

    # Same shape for bools: Python's True must normalize to "true", not
    # str()'s "True", to match Rust's lowercase string.
    def cli_bool_vs_string(node, *a):
        return {"value": True} if a[0] == "revenue-config" else {"value": "true"}

    results, total = run_diff(cli_bool_vs_string, "node", table)
    rc = report(results, total)
    print(f"[self-test] typed-vs-string (bool True vs \"true\") normalized: exit={rc} (expect 0)")
    ok = ok and rc == 0

    results, total = run_diff(cli_bool_vs_string, "node", table, strict=True)
    rc = report(results, total)
    print(f"[self-test] typed-vs-string (bool True vs \"true\") --strict: exit={rc} (expect 1)")
    ok = ok and rc == 1

    # Remapped Config fields: Python receives the effective dataclass field,
    # while Rust keeps the original hyphenated option suffix.
    for suffix, py_field in PYTHON_FIELD_MAP.items():
        calls = []

        def cli_remapped(node, *a):
            calls.append(a)
            return {"value": "same"}

        result = diff_key(cli_remapped, "node", suffix)
        expected_calls = [
            ("revenue-config", "get", py_field),
            ("-k", "revenue-r-config", f"key={suffix}"),
        ]
        mapped_ok = result["status"] == "ok" and calls == expected_calls
        print(
            f"[self-test] mapped field {suffix}: calls={calls!r} "
            f"(expect {expected_calls!r}) => {'PASS' if mapped_ok else 'FAIL'}"
        )
        ok = ok and mapped_ok

    # Constructor-only option surface: these twelve options do not exist on
    # Python's Config dataclass, but their effective startup values are still
    # comparable through CLN listconfigs. Exercise every supported listconfigs
    # scalar shape plus malformed and transport failures on that alternate
    # path.
    expected_constructor_keys = {
        "boltz-enabled",
        "boltz-cli-path",
        "boltz-datadir",
        "boltz-use-sudo",
        "boltz-sudo-user",
        "boltz-timeout-seconds",
        "boltz-daily-budget-sats",
        "boltz-enforce-budget",
        "boltz-btc-wallet",
        "boltz-lbtc-wallet",
        "boltz-routing-fee-limit-ppm",
        "boltz-max-withdraw-sats",
    }
    actual_constructor_keys = set(globals().get("CONSTRUCTOR_ONLY_KEYS", set()))
    constructor_set_ok = actual_constructor_keys == expected_constructor_keys
    print(
        f"[self-test] constructor-only key set: {len(actual_constructor_keys)} keys "
        f"(expect exactly 12) => {'PASS' if constructor_set_ok else 'FAIL'}"
    )
    ok = ok and constructor_set_ok

    constructor_cases = [
        ("bool", {"value_bool": True}, {"value": "true"}, "ok"),
        ("int", {"value_int": 3600}, {"value": "3600"}, "ok"),
        ("string", {"value_str": "CLN"}, {"value": "CLN"}, "ok"),
        ("null", {"value_str": None}, {"value": None}, "ok"),
        ("malformed", {"source": "default"}, {"value": "unused"}, "error"),
    ]
    constructor_suffix = "boltz-btc-wallet"
    constructor_name = f"revenue-ops-{constructor_suffix}"
    for label, entry, rust_response, expected_status in constructor_cases:
        calls = []

        def cli_constructor(node, *a, _entry=entry, _rs=rust_response):
            calls.append(a)
            if a[0] == "listconfigs":
                return {"configs": {constructor_name: _entry}}
            return _rs

        results, total = run_diff(
            cli_constructor, "node", [{"name": constructor_name}]
        )
        result = results[0]
        expected_first_call = ("listconfigs", constructor_name)
        case_ok = (
            result["status"] == expected_status
            and calls
            and calls[0] == expected_first_call
        )
        print(
            f"[self-test] constructor option {label}: status={result['status']!r} "
            f"calls={calls!r} (expect {expected_status!r}, first call "
            f"{expected_first_call!r}) => {'PASS' if case_ok else 'FAIL'}"
        )
        ok = ok and case_ok

    def cli_constructor_transport(node, *a):
        raise subprocess.CalledProcessError(255, ["ssh"])

    results, total = run_diff(
        cli_constructor_transport, "node", [{"name": constructor_name}]
    )
    result = results[0]
    transport_ok = result["status"] == "transport" and result.get("side") == "python"
    print(
        f"[self-test] constructor option transport: result={result!r} "
        f"(expect python transport) => {'PASS' if transport_ok else 'FAIL'}"
    )
    ok = ok and transport_ok

    def cli_constructor_report(node, *a):
        if a[0] == "listconfigs":
            name = a[1]
            return {"configs": {name: {"value_str": "same"}}}
        return {"value": "same"}

    constructor_table = [
        {"name": f"revenue-ops-{suffix}"}
        for suffix in sorted(expected_constructor_keys)
    ]
    results, total = run_diff(cli_constructor_report, "node", constructor_table)
    report_out = io.StringIO()
    with contextlib.redirect_stdout(report_out):
        rc = report(results, total)
    report_text = report_out.getvalue().strip()
    report_ok = rc == 0 and "12 option-surface checks" in report_text
    print(
        f"[self-test] constructor option report: exit={rc} output={report_text!r} "
        f"(expect '12 option-surface checks') => {'PASS' if report_ok else 'FAIL'}"
    )
    ok = ok and report_ok

    print("[self-test] ALL PASS" if ok else "[self-test] FAILURE")
    return 0 if ok else 1


def main(argv=None, cli_fn=cli):
    ap = argparse.ArgumentParser()
    ap.add_argument("--node", default="lnnode")
    ap.add_argument("--self-test", action="store_true",
                    help="run the built-in self-test (stubbed CLI, no live node) and exit")
    ap.add_argument("--strict", action="store_true",
                    help="disable typed-vs-string normalization and compare raw values "
                         "(for Phase 1b, once Rust returns properly typed values)")
    args = ap.parse_args(argv)

    if args.self_test:
        return self_test()

    fixtures_path = pathlib.Path(__file__).parents[2] / "fixtures/options.json"
    table = load_fixtures(fixtures_path)
    results, total = run_diff(cli_fn, args.node, table, strict=args.strict)
    return report(results, total)


if __name__ == "__main__":
    sys.exit(main())
