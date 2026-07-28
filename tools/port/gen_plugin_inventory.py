#!/usr/bin/env python3
"""Generate the checked-in whole-plugin Rust-port inventory.

The Python surface is always read from a named git object. No working-tree
Python file and no running service is consulted. Rust reachability is derived
from the checked-out ``main.rs`` registration chain; classifications use a
small reviewable map whose vocabulary distinguishes a compiled placeholder
from an effective handler.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_PYTHON_COMMIT = "e579de8df523f174283fc2aa21f395c8ef006ac6"
RUST_AUDIT_BASE = "c68fddd707a4f53dda691dcba7c04d659581b880"
GENERATOR_VERSION = 1

PYTHON_FILES = (
    "cl-revenue-ops.py",
    "modules/boltz_manager.py",
    "modules/capacity_planner.py",
    "modules/data_service.py",
    "modules/lnplus_swaps.py",
    "modules/rebalance_native_executor_v2.py",
)

EXPECTED_LOOPS = {
    "flow-analysis",
    "fee-adjustment",
    "rebalance-check",
    "startup-snapshot",
    "financial-snapshot",
    "boltz-auto-cycle",
    "capacity-planner",
    "lnplus-watcher",
}

PLACEHOLDER_RPCS = {
    "revenue-analyze",
    "revenue-capacity-report",
    "revenue-econ-snapshot",
    "revenue-profitability",
}

FULL_EFFECTIVE_RPCS = {
    "revenue-history",
    "revenue-list-banned",
    "revenue-list-ignored",
    "revenue-planner-candidate-sources",
    "revenue-planner-candidates",
    "revenue-planner-history",
    "revenue-planner-status",
    "revenue-spend-ledger",
}

# Compiled means an exact-contract response module/builder exists, not merely
# that a subsystem kernel with a vaguely related purpose compiles.
EXTRA_COMPILED_RPCS = {
    "revenue-boltz-budget",
    "revenue-boltz-history",
    "revenue-boltz-status",
    "revenue-capex-status",
    "revenue-econ-reconcile",
    "revenue-lnplus-status",
    "revenue-rebalance-debug",
    "revenue-total-cost-budget",
}


def git_show(repo: Path, commit: str, path: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo), "show", f"{commit}:{path}"],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"cannot read {path} at {commit}: "
            f"{result.stderr.decode(errors='replace').strip()}"
        )
    return result.stdout


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def literal(node: ast.AST) -> Any:
    """Evaluate only constant expression forms used by add_option calls."""
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left = literal(node.left)
            right = literal(node.right)
            if isinstance(left, str) and isinstance(right, str):
                return left + right
        raise ValueError(
            f"non-literal source expression at line {getattr(node, 'lineno', '?')}: "
            f"{ast.unparse(node)}"
        )


def is_plugin_call(node: ast.Call, name: str) -> bool:
    return (
        isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "plugin"
        and node.func.attr == name
    )


def extract_options(tree: ast.AST) -> list[dict[str, Any]]:
    options = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not is_plugin_call(node, "add_option"):
            continue
        kwargs = {kw.arg: kw.value for kw in node.keywords if kw.arg is not None}
        required = {"name", "default", "description"}
        missing = required - kwargs.keys()
        if missing:
            raise ValueError(f"add_option at line {node.lineno} lacks {sorted(missing)}")
        options.append(
            {
                "name": literal(kwargs["name"]),
                "opt_type": literal(kwargs["opt_type"])
                if "opt_type" in kwargs
                else "string",
                "default": literal(kwargs["default"]),
                "description": literal(kwargs["description"]),
                "dynamic": bool(literal(kwargs["dynamic"]))
                if "dynamic" in kwargs
                else False,
                "source_line": node.lineno,
            }
        )
    options.sort(key=lambda entry: entry["source_line"])
    names = [entry["name"] for entry in options]
    if len(names) != 121 or len(set(names)) != 121:
        raise ValueError(
            f"expected 121 unique Python options, got {len(names)}/{len(set(names))}"
        )
    return options


def default_value(node: ast.AST | None) -> tuple[bool, Any]:
    if node is None:
        return False, None
    return True, literal(node)


def extract_rpcs(
    tree: ast.Module,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rpcs = []
    schemas = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        decorator = next(
            (
                dec
                for dec in node.decorator_list
                if isinstance(dec, ast.Call) and is_plugin_call(dec, "method")
            ),
            None,
        )
        if decorator is None:
            continue
        if len(decorator.args) != 1:
            raise ValueError(
                f"plugin.method at line {decorator.lineno} is not a literal one-name call"
            )
        name = literal(decorator.args[0])
        positional = list(node.args.posonlyargs) + list(node.args.args)
        defaults = [None] * (len(positional) - len(node.args.defaults)) + list(
            node.args.defaults
        )
        parameter_nodes = list(
            zip(positional, defaults, ["positional"] * len(positional))
        )
        parameter_nodes.extend(
            zip(
                node.args.kwonlyargs,
                node.args.kw_defaults,
                ["keyword_only"] * len(node.args.kwonlyargs),
            )
        )
        params = []
        for arg, default_node, python_kind in parameter_nodes:
            if arg.arg == "plugin":
                continue
            has_default, value = default_value(default_node)
            params.append(
                {
                    "name": arg.arg,
                    "required": not has_default,
                    "has_default": has_default,
                    "default": value,
                    # Handler internals, not signature defaults, define Python's
                    # coercion. Do not invent a stronger contract here.
                    "coercion": "handler_defined",
                    "python_kind": python_kind,
                }
            )
        if node.args.vararg:
            raise ValueError(f"variadic RPC signature is not representable: {name}")
        rpcs.append(
            {
                "name": name,
                "python_function": node.name,
                "source_file": "cl-revenue-ops.py",
                "source_line": decorator.lineno,
            }
        )
        schemas.append(
            {
                "name": name,
                "python_binding": "positional_or_named",
                "allow_extra_named": node.args.kwarg is not None,
                "params": params,
            }
        )
    rpcs.sort(key=lambda entry: entry["name"])
    schemas.sort(key=lambda entry: entry["name"])
    names = [entry["name"] for entry in rpcs]
    if len(names) != 69 or len(set(names)) != 69:
        raise ValueError(
            f"expected 69 unique Python RPCs, got {len(names)}/{len(set(names))}"
        )
    return rpcs, schemas


def keyword_literal(call: ast.Call, name: str) -> Any | None:
    for keyword in call.keywords:
        if keyword.arg == name:
            if isinstance(keyword.value, ast.Name):
                return keyword.value.id
            return literal(keyword.value)
    return None


def extract_loops(tree: ast.AST) -> list[dict[str, Any]]:
    loops = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "threading"
            and node.func.attr == "Thread"
        ):
            continue
        name = keyword_literal(node, "name")
        if name not in EXPECTED_LOOPS:
            continue
        loops.append(
            {
                "name": name,
                "python_target": keyword_literal(node, "target"),
                "source_file": "cl-revenue-ops.py",
                "source_line": node.lineno,
                "kind": "one_shot" if name == "startup-snapshot" else "recurring",
                "rust_state": loop_state(name),
            }
        )
    loops.sort(key=lambda entry: entry["source_line"])
    names = {entry["name"] for entry in loops}
    if names != EXPECTED_LOOPS or len(loops) != 8:
        raise ValueError(f"expected exact eight startup loops, got {sorted(names)}")
    return loops


def loop_state(name: str) -> dict[str, Any]:
    if name == "fee-adjustment":
        return {
            "compiled": True,
            "reachable": True,
            "effective": "full",
            "review": "passed",
            "soak": "passed",
            "owner_task": "fee-port-reviewed",
        }
    owners = {
        "rebalance-check": "hexmem-60",
        "capacity-planner": "hexmem-62",
        "lnplus-watcher": "hexmem-61",
        "boltz-auto-cycle": "hexmem-63",
        "flow-analysis": "task-8-core-parity",
        "startup-snapshot": "task-8-core-parity",
        "financial-snapshot": "task-8-core-parity",
    }
    return {
        "compiled": name
        in {
            "rebalance-check",
            "capacity-planner",
            "lnplus-watcher",
            "boltz-auto-cycle",
            "flow-analysis",
        },
        "reachable": False,
        "effective": "absent",
        "review": "pending",
        "soak": "pending",
        "owner_task": owners[name],
    }


def derive_rust_methods(repo_root: Path) -> set[str]:
    source = (repo_root / "crates/revops/src/main.rs").read_text(encoding="utf-8")
    bindings = {
        variable: f"revenue-{suffix}"
        for variable, suffix in re.findall(
            r'let\s+([a-zA-Z0-9_]+)\s*=\s*rpc_name\("([a-z0-9-]+)"\);', source
        )
    }
    bindings.update(
        {
            variable: value
            for variable, value in re.findall(
                r'let\s+([a-zA-Z0-9_]+)\s*=\s*"([a-z0-9-]+)";', source
            )
            if "status" in variable or "rpc" in variable
        }
    )
    registered_vars = re.findall(
        r'\.rpcmethod\(\s*&?([a-zA-Z0-9_]+)\s*,', source
    )
    missing = sorted(set(registered_vars) - bindings.keys())
    if missing:
        raise ValueError(f"unresolved Rust rpcmethod name bindings: {missing}")
    registered = {bindings[variable] for variable in registered_vars}
    if len(registered_vars) != 24 or len(registered) != 24:
        raise ValueError(
            "expected 24 unique Rust registrations at audit base, got "
            f"{len(registered_vars)}/{len(registered)}"
        )
    return registered


def owner_task(name: str) -> str:
    if name.startswith("revenue-boltz-"):
        return "hexmem-63"
    if name.startswith("revenue-lnplus-"):
        return "hexmem-61"
    if name.startswith("revenue-rebalance"):
        return "hexmem-60"
    if name.startswith("revenue-planner-") or name in {
        "revenue-capacity-report",
        "revenue-capex-status",
    }:
        return "hexmem-62"
    return "task-8-core-parity"


def transport_state(name: str) -> str:
    if name in {"revenue-set-fee", "revenue-fee-cycle"}:
        return "local_fake_proven"
    if name.startswith("revenue-boltz-"):
        return "local_fake_proven_unreachable"
    if name in {
        "revenue-rebalance",
        "revenue-rebalance-cycle",
        "revenue-planner-execute",
    } or name.startswith("revenue-lnplus-"):
        return "missing" if name != "revenue-lnplus-status" else "not_required"
    return "not_required"


def rpc_state(name: str, reachable: set[str]) -> dict[str, Any]:
    is_reachable = name in reachable
    compiled = is_reachable or name in EXTRA_COMPILED_RPCS
    if not compiled:
        effective = "absent"
    elif not is_reachable:
        effective = "unreachable"
    elif name in PLACEHOLDER_RPCS:
        effective = "placeholder"
    elif name in FULL_EFFECTIVE_RPCS:
        effective = "full"
    else:
        effective = "partial"
    external = transport_state(name)
    return {
        "compiled": compiled,
        "reachable": is_reachable,
        "effective": effective,
        "transport_proven": external,
        "review": "passed" if is_reachable else "pending",
        "soak": "pending" if external != "not_required" else "not_required",
    }


def first_line(sources: dict[str, bytes], path: str, needle: bytes) -> int:
    for index, line in enumerate(sources[path].splitlines(), start=1):
        if needle in line:
            return index
    raise ValueError(f"cannot find {needle!r} in pinned {path}")


def ref(sources: dict[str, bytes], path: str, needle: bytes) -> dict[str, Any]:
    return {"source_file": path, "source_line": first_line(sources, path, needle)}


def external_boundaries(sources: dict[str, bytes]) -> list[dict[str, Any]]:
    rows = [
        {
            "id": "setchannel",
            "python_evidence": [ref(sources, "modules/data_service.py", b"setchannel")],
            "rust_adapter": "fee_execution::ClnFeeBroadcaster",
            "rust_transport": "local_fake_proven",
            "owner_task": "fee-port-reviewed",
        },
        {
            "id": "sendpay_waitsendpay",
            "python_evidence": [
                ref(sources, "modules/data_service.py", b"sendpay"),
                ref(
                    sources,
                    "modules/rebalance_native_executor_v2.py",
                    b"waitsendpay",
                ),
            ],
            "rust_adapter": None,
            "rust_transport": "missing",
            "owner_task": "hexmem-60",
        },
        {
            "id": "fundchannel",
            "python_evidence": [
                ref(sources, "modules/lnplus_swaps.py", b"fundchannel"),
                ref(sources, "modules/capacity_planner.py", b"fundchannel"),
            ],
            "rust_adapter": None,
            "rust_transport": "missing",
            "owner_task": "hexmem-61-and-62",
        },
        {
            "id": "close",
            "python_evidence": [
                ref(
                    sources,
                    "modules/capacity_planner.py",
                    b'plugin.rpc.call("close"',
                )
            ],
            "rust_adapter": None,
            "rust_transport": "missing",
            "owner_task": "hexmem-62",
        },
        {
            "id": "signmessage",
            "python_evidence": [
                ref(sources, "modules/data_service.py", b"signmessage")
            ],
            "rust_adapter": "revops_lnplus::Signer trait only",
            "rust_transport": "trait_fake_only",
            "owner_task": "hexmem-61",
        },
        {
            "id": "datastore",
            "python_evidence": [ref(sources, "modules/data_service.py", b"datastore")],
            "rust_adapter": None,
            "rust_transport": "missing",
            "owner_task": "task-8-core-parity",
        },
        {
            "id": "boltzcli",
            "python_evidence": [
                ref(sources, "modules/boltz_manager.py", b"subprocess.run")
            ],
            "rust_adapter": "revops_boltz::ProcessBoltzCli",
            "rust_transport": "local_fake_proven_unreachable",
            "owner_task": "hexmem-63",
        },
        {
            "id": "lnplus_https",
            "python_evidence": [
                ref(sources, "modules/lnplus_swaps.py", b"urllib.request")
            ],
            "rust_adapter": "revops_lnplus::HttpTransport trait only",
            "rust_transport": "missing",
            "owner_task": "hexmem-61",
        },
        {
            "id": "dynamic_config",
            "python_evidence": [
                ref(sources, "cl-revenue-ops.py", b"def _refresh_dynamic_config")
            ],
            "rust_adapter": "PythonOptionCache::refresh",
            "rust_transport": "local_fake_proven",
            "owner_task": "completed",
        },
    ]
    return sorted(rows, key=lambda row: row["id"])


def generate(
    python_repo: Path, python_commit: str, repo_root: Path
) -> dict[str, Any]:
    sources = {
        path: git_show(python_repo, python_commit, path) for path in PYTHON_FILES
    }
    tree = ast.parse(sources["cl-revenue-ops.py"].decode("utf-8"))
    rpcs, parameter_methods = extract_rpcs(tree)
    options = extract_options(tree)
    loops = extract_loops(tree)
    registered = derive_rust_methods(repo_root)
    python_names = {entry["name"] for entry in rpcs}
    reachable = registered & python_names
    rust_only = registered - python_names
    if len(reachable) != 20 or len(rust_only) != 4:
        raise ValueError(
            "expected 20 Python-equivalent plus four Rust-only methods, got "
            f"{len(reachable)}+{len(rust_only)}"
        )
    for entry in rpcs:
        entry["owner_task"] = owner_task(entry["name"])
        entry["state"] = rpc_state(entry["name"], reachable)

    provenance = {
        "generator": "tools/port/gen_plugin_inventory.py",
        "generator_version": GENERATOR_VERSION,
        "python_source_commit": python_commit,
        "rust_audit_base_commit": RUST_AUDIT_BASE,
        "rust_main_sha256": sha256(
            (repo_root / "crates/revops/src/main.rs").read_bytes()
        ),
        "source_sha256": {
            path: sha256(sources[path]) for path in sorted(sources)
        },
    }
    inventory = {
        "schema_version": 1,
        "provenance": provenance,
        "python_rpcs": rpcs,
        "rust_only_methods": sorted(rust_only),
        "python_options": options,
        "loops": loops,
        "shutdown": {
            "name": "rpc-shutdown",
            "source_file": "cl-revenue-ops.py",
            "source_line": first_line(
                sources, "cl-revenue-ops.py", b'name="rpc-shutdown"'
            ),
            "bounded": True,
            "join_timeout_seconds": 10.0,
            "semantics": (
                "daemon drain thread; bounded wait; process exit proceeds on timeout"
            ),
        },
        "external_boundaries": external_boundaries(sources),
    }
    parameter_contract = {
        "schema_version": 1,
        "python_source_commit": python_commit,
        "python_source_sha256": provenance["source_sha256"]["cl-revenue-ops.py"],
        "methods": parameter_methods,
    }
    option_fixture = [
        {key: value for key, value in entry.items() if key != "source_line"}
        for entry in options
    ]
    return {
        "fixtures/options.json": option_fixture,
        "fixtures/port/plugin_inventory.json": inventory,
        "fixtures/port/rpc_params.json": parameter_contract,
    }


def rendered(relative: str, value: Any) -> bytes:
    # Keep the established option fixture's compact one-space indentation and
    # field order so adding two source options does not produce a noisy full-file
    # rewrite. New inventory artifacts use sorted keys for canonical review.
    if relative == "fixtures/options.json":
        return (json.dumps(value, indent=1, ensure_ascii=True) + "\n").encode()
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-repo", type=Path, required=True)
    parser.add_argument("--python-commit", default=DEFAULT_PYTHON_COMMIT)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    try:
        artifacts = generate(args.python_repo, args.python_commit, args.repo_root)
    except (OSError, RuntimeError, ValueError, SyntaxError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    stale = []
    for relative, value in artifacts.items():
        path = args.repo_root / relative
        expected = rendered(relative, value)
        if args.check:
            if not path.is_file() or path.read_bytes() != expected:
                stale.append(relative)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(expected)
    if stale:
        print(
            "stale generated artifacts: " + ", ".join(sorted(stale)),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
