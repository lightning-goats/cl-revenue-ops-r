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


DEFAULT_PYTHON_COMMIT = "a5c2e2f65019df5cefe4e1261b7de2823a03e448"
GENERATOR_VERSION = 5

RPC_BOUNDARY_BY_METHOD = {
    "askrene-age": "askrene_age",
    "askrene-bias-channel": "askrene_bias_channel",
    "askrene-bias-node": "askrene_bias_node",
    "askrene-create-layer": "askrene_create_layer",
    "askrene-disable-node": "askrene_disable_node",
    "askrene-inform-channel": "askrene_inform_channel",
    "askrene-remove-layer": "askrene_remove_layer",
    "askrene-reserve": "askrene_reserve",
    "askrene-unreserve": "askrene_unreserve",
    "askrene-update-channel": "askrene_update_channel",
    "datastore": "datastore",
    "delinvoice": "delinvoice",
    "delpay": "delpay",
    "invoice": "invoice",
    "sendpay": "sendpay_waitsendpay",
    "setchannel": "setchannel",
    "waitsendpay": "sendpay_waitsendpay",
}

READ_ONLY_RPC_METHODS = {
    "askrene-listlayers",
    "bkpr-inspect",
    "bkpr-listaccountevents",
    "bkpr-listbalances",
    "decode",
    "feerates",
    "getinfo",
    "getroute",
    "getroutes",
    "listchannels",
    "listclosedchannels",
    "listconfigs",
    "listforwards",
    "listfunds",
    "listnodes",
    "listpays",
    "listpeerchannels",
    "listpeers",
    "listplugins",
    "listsendpays",
    "plugin",
}

# The capture writer asks git for observational source identity. It neither
# mutates CLN nor invokes an action adapter, so it is an explicit audited
# subprocess exclusion rather than an execution boundary.
OBSERVATIONAL_SUBPROCESS_EXCLUSIONS = {
    ("modules/fee_cycle_capture.py", 418): "git rev-parse source identity",
}

# The native executor's one dynamic proxy is represented by its literal
# _rpc_call call sites. These three forwarding calls are not independent
# operation evidence.
DYNAMIC_RPC_PROXY_EXCLUSIONS = {
    ("modules/rebalance_native_executor_v2.py", 45),
    ("modules/rebalance_native_executor_v2.py", 47),
    ("modules/rebalance_native_executor_v2.py", 48),
}

EXPECTED_EXTERNAL_CALLS = {
    ("askrene_age", "modules/data_service.py", 380),
    ("askrene_bias_channel", "modules/data_service.py", 369),
    ("askrene_bias_node", "modules/data_service.py", 362),
    ("askrene_create_layer", "modules/data_service.py", 340),
    ("askrene_create_layer", "modules/rebalance_router_v3.py", 643),
    ("askrene_disable_node", "modules/data_service.py", 373),
    ("askrene_inform_channel", "modules/data_service.py", 390),
    ("askrene_remove_layer", "modules/data_service.py", 346),
    ("askrene_remove_layer", "modules/rebalance_engine_v2.py", 355),
    ("askrene_remove_layer", "modules/rebalance_router_v3.py", 696),
    ("askrene_reserve", "modules/data_service.py", 394),
    ("askrene_unreserve", "modules/data_service.py", 398),
    ("askrene_update_channel", "modules/data_service.py", 355),
    ("askrene_update_channel", "modules/rebalance_router_v3.py", 659),
    ("askrene_update_channel", "modules/rebalance_router_v3.py", 677),
    ("datastore", "modules/data_service.py", 434),
    ("datastore", "modules/rebalance_engine_v2.py", 3678),
    ("delinvoice", "modules/data_service.py", 318),
    ("delinvoice", "modules/rebalance_native_executor_v2.py", 411),
    ("delpay", "modules/data_service.py", 314),
    ("delpay", "modules/rebalance_engine_v2.py", 3601),
    ("delpay", "modules/rebalance_native_executor_v2.py", 406),
    ("dynamic_config", "cl-revenue-ops.py", 5560),
    ("invoice", "modules/data_service.py", 302),
    ("invoice", "modules/rebalance_native_executor_v2.py", 451),
    ("sendpay_waitsendpay", "modules/data_service.py", 306),
    ("sendpay_waitsendpay", "modules/data_service.py", 310),
    ("sendpay_waitsendpay", "modules/rebalance_native_executor_v2.py", 481),
    ("sendpay_waitsendpay", "modules/rebalance_native_executor_v2.py", 482),
    ("setchannel", "modules/data_service.py", 275),
}


EXPECTED_LOOPS = {
    "flow-analysis",
    "fee-adjustment",
    "rebalance-check",
    "startup-snapshot",
    "financial-snapshot",
}

# Emptied in Task 66 slice 8: all four former members (analyze,
# capacity-report, econ-snapshot, profitability) have shipped real
# evidence-backed handlers since Tasks 49/50/67b-c and now classify
# "partial". revenue-analyze still answers `not_yet_ported` on its
# whole-fleet arm (rpc_analyze.rs — the single-channel path is real);
# that residue is a declared partial, not a whole-handler placeholder.
PLACEHOLDER_RPCS: set[str] = set()

FULL_EFFECTIVE_RPCS = {
    "revenue-history",
    "revenue-list-banned",
    "revenue-list-ignored",
    "revenue-spend-ledger",
    # Task 66: exact-contract reads serving REAL evidence through the
    # registered runtime path (e2e-pinned through the spawned binary).
    # Deliberately NOT in REVIEWED_FULL_RPCS below: full describes the
    # response contract, review describes independent verification, and
    # these await the tier-1 Python review of the task-66 commit queue.
    "revenue-capex-status",
    "revenue-econ-reconcile",
    "revenue-total-cost-budget",
}

# Independently reviewed subset of FULL_EFFECTIVE_RPCS. Split from the
# effective classification (Task 66 slice 8): FULL means the exact
# response contract is implemented and tested; it must never IMPLY a
# review that has not happened.
REVIEWED_FULL_RPCS = {
    "revenue-history",
    "revenue-list-banned",
    "revenue-list-ignored",
    "revenue-spend-ledger",
}

# Registration is mechanically derived; effective/review state is not. New
# registrations must be classified here before inventory generation succeeds.
CLASSIFIED_REACHABLE_RPCS = {
    "revenue-analyze",
    "revenue-budget",
    "revenue-config",
    "revenue-cycle",
    "revenue-dashboard",
    "revenue-econ-snapshot",
    "revenue-fee-debug",
    "revenue-health",
    "revenue-history",
    "revenue-hot-channel-protection-peers",
    "revenue-list-banned",
    "revenue-list-ignored",
    "revenue-policy",
    "revenue-profitability",
    "revenue-report",
    "revenue-spend-ledger",
    "revenue-status",
    # Task 66 (RPC-set closure): every remaining Python-equivalent name is
    # now registered through the main.rs chain. Reads that serve complete
    # real evidence at runtime are additionally in FULL_EFFECTIVE_RPCS;
    # everything below classifies "partial" — either a success-shaped
    # subset, or a COMPLETE contract whose result-bearing execution path
    # is sealed until Task 69's authority-gated assembly (the core-state
    # mutators, cleanup-closed, set-fee, fee-cycle, wake-all, and
    # econ-cycle's candidate source), answering Python's exact
    # uninitialized/denial arms until then.
    "revenue-ban",
    "revenue-capex-status",
    "revenue-cleanup-closed",
    "revenue-clear-reservations",
    "revenue-econ-cycle",
    "revenue-econ-reconcile",
    "revenue-fee-authority-status",
    "revenue-fee-cycle",
    "revenue-ignore",
    "revenue-profile-preview",
    "revenue-set-fee",
    "revenue-spend-release",
    "revenue-spend-release-stale",
    "revenue-spend-reserve",
    "revenue-spend-settle",
    "revenue-total-cost-budget",
    "revenue-unban",
    "revenue-unignore",
    "revenue-wake-all",
    "revenue-rebalance",
    "revenue-rebalance-cycle",
    "revenue-rebalance-debug",
}

CLASSIFIED_RUST_ONLY_METHODS = {
    "revenue-fee-wake",
    "revenue-ping",
    "revenue-rebalance-plan",
    "revops-fee-runway-status",
}

REVIEW_EVIDENCE = {
    name: "task-8-core-parity-audit"
    for name in REVIEWED_FULL_RPCS
}

# Compiled means an exact-contract response module/builder exists, not merely
# that a subsystem kernel with a vaguely related purpose compiles.
EXTRA_COMPILED_RPCS = {
    "revenue-capex-status",
    "revenue-econ-reconcile",
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


def production_python_files(repo: Path, commit: str) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "-r", "--name-only", commit],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"cannot list pinned production tree at {commit}: {result.stderr.strip()}"
        )
    files = tuple(
        sorted(
            path
            for path in result.stdout.splitlines()
            if path == "cl-revenue-ops.py"
            or (path.startswith("modules/") and path.endswith(".py"))
        )
    )
    if len(files) != 46 or len(set(files)) != 46:
        raise ValueError(f"expected 46 pinned production Python files, got {len(files)}")
    return files


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
    if len(names) != 71 or len(set(names)) != 71:
        raise ValueError(
            f"expected 71 unique Python options, got {len(names)}/{len(set(names))}"
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
    if len(names) != 39 or len(set(names)) != 39:
        raise ValueError(
            f"expected 39 unique Python RPCs, got {len(names)}/{len(set(names))}"
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
    if names != EXPECTED_LOOPS or len(loops) != 5:
        raise ValueError(f"expected exact five startup loops, got {sorted(names)}")
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
        "flow-analysis": "task-8-core-parity",
        "startup-snapshot": "task-8-core-parity",
        "financial-snapshot": "task-8-core-parity",
    }
    return {
        "compiled": True,
        "reachable": True,
        "effective": "full",
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
    # Mode-conditional bindings (`let x = if canonical_names() {
    # rpc_name("wake-all") } else { rpc_name("fee-wake") };`): the
    # inventory tracks the CANONICAL surface, so the canonical branch's
    # suffix names the binding.
    bindings.update(
        {
            variable: f"revenue-{suffix}"
            for variable, suffix in re.findall(
                r'let\s+([a-zA-Z0-9_]+)\s*=\s*if\s+canonical_names\(\)\s*\{\s*'
                r'rpc_name\("([a-z0-9-]+)"\)',
                source,
            )
        }
    )
    bindings.update(
        {
            variable: value
            for variable, value in re.findall(
                r'let\s+([a-zA-Z0-9_]+)\s*=\s*"([a-z0-9-]+)";', source
            )
            if "status" in variable or "rpc" in variable
        }
    )
    # Direct registrations. `name` is every register_* helper's parameter
    # (each helper contains exactly one `.rpcmethod(name, ...)` body); the
    # real name arrives at the helper's CALL SITE, captured below.
    registered_vars = [
        variable
        for variable in re.findall(r'\.rpcmethod\(\s*&?([a-zA-Z0-9_]+)\s*,', source)
        if variable != "name"
    ]
    # Helper-fn registrations (Task 66 pattern):
    # `let builder = register_x(builder, &y_name, ...)`. Requiring the
    # `_name` suffix keeps non-RPC helpers (register_option's `&name`
    # loop variable) out of the registration set.
    registered_vars += re.findall(
        r'register_[a-z0-9_]+\(\s*builder,\s*&([a-zA-Z0-9_]+_name)\s*,', source
    )
    # One real registration can be seen by BOTH scans when a helper's
    # parameter shadows the caller's binding name (register_rust_
    # diagnostics' ping_name). Dedupe variables; the name-level
    # uniqueness check below still catches two bindings mapping onto one
    # method name, and the runtime manifest count test catches genuine
    # double registration.
    registered_vars = list(dict.fromkeys(registered_vars))
    missing = sorted(set(registered_vars) - bindings.keys())
    if missing:
        raise ValueError(f"unresolved Rust rpcmethod name bindings: {missing}")
    registered = {bindings[variable] for variable in registered_vars}
    if len(registered_vars) != len(registered):
        raise ValueError(
            "Rust RPC registrations are not unique: "
            f"{len(registered_vars)} registrations/{len(registered)} names"
        )
    classified = CLASSIFIED_REACHABLE_RPCS | CLASSIFIED_RUST_ONLY_METHODS
    unclassified = sorted(registered - classified)
    if unclassified:
        raise ValueError(f"unclassified new Rust RPC registrations: {unclassified}")
    return registered


def owner_task(name: str) -> str:
    if name.startswith("revenue-rebalance"):
        return "hexmem-60"
    if name == "revenue-capex-status":
        return "hexmem-62"
    return "task-8-core-parity"


def transport_state(name: str) -> str:
    if name in {"revenue-set-fee", "revenue-fee-cycle"}:
        return "local_fake_proven"
    if name in {
        "revenue-rebalance",
        "revenue-rebalance-cycle",
    }:
        return "missing"
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
        "review": "passed" if name in REVIEW_EVIDENCE else "pending",
        "review_evidence": REVIEW_EVIDENCE.get(name),
        "soak": "pending" if external != "not_required" else "not_required",
    }


def first_line(sources: dict[str, bytes], path: str, needle: bytes) -> int:
    for index, line in enumerate(sources[path].splitlines(), start=1):
        if needle in line:
            return index
    raise ValueError(f"cannot find {needle!r} in pinned {path}")


def dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return None


def boundary(
    boundary_id: str,
    evidence: list[dict[str, Any]],
    owner_task: str,
    rust_adapter: str | None = None,
    rust_transport: str = "missing",
) -> dict[str, Any]:
    return {
        "id": boundary_id,
        "python_evidence": evidence,
        "rust_adapter": rust_adapter,
        "rust_transport": rust_transport,
        "owner_task": owner_task,
    }


def function_ref(
    sources: dict[str, bytes], path: str, function_name: str
) -> tuple[str, str, int]:
    tree = ast.parse(sources[path].decode("utf-8"))
    lines = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == function_name
    ]
    if len(lines) != 1:
        raise ValueError(
            f"expected one function {function_name!r} in pinned {path}, got {lines}"
        )
    return ("dynamic_config", path, lines[0])


def scan_external_calls(sources: dict[str, bytes]) -> set[tuple[str, str, int]]:
    found: set[tuple[str, str, int]] = set()
    for path in sorted(sources):
        tree = ast.parse(sources[path].decode("utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            function = dotted_name(node.func) or ""
            location = (path, node.lineno)
            method = None
            if function.endswith("._rpc_call"):
                if node.args and isinstance(node.args[0], ast.Constant):
                    method = node.args[0].value
            elif function.endswith(".rpc.call"):
                if node.args and isinstance(node.args[0], ast.Constant):
                    method = node.args[0].value
                elif location not in DYNAMIC_RPC_PROXY_EXCLUSIONS:
                    raise ValueError(f"unclassified dynamic RPC proxy call at {path}:{node.lineno}")
            elif ".rpc." in function:
                method = function.rsplit(".", 1)[-1]

            if isinstance(method, str):
                boundary_id = RPC_BOUNDARY_BY_METHOD.get(method)
                if boundary_id:
                    found.add((boundary_id, path, node.lineno))
                elif method not in READ_ONLY_RPC_METHODS:
                    raise ValueError(
                        f"unclassified direct RPC method {method!r} at {path}:{node.lineno}"
                    )

            if function in {"subprocess.run", "subprocess.Popen"}:
                if location in OBSERVATIONAL_SUBPROCESS_EXCLUSIONS:
                    continue
                found.add(("subprocess_exec", path, node.lineno))
            elif function == "urllib.request.urlopen":
                found.add(("https_request", path, node.lineno))

    found.add(function_ref(sources, "cl-revenue-ops.py", "_refresh_dynamic_config"))
    missing = sorted(EXPECTED_EXTERNAL_CALLS - found)
    unexpected = sorted(found - EXPECTED_EXTERNAL_CALLS)
    if missing or unexpected:
        raise ValueError(
            "pinned production external-call drift: "
            f"missing={missing}, unexpected={unexpected}"
        )
    return found


def external_boundaries(sources: dict[str, bytes]) -> list[dict[str, Any]]:
    calls = scan_external_calls(sources)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for boundary_id, path, line in sorted(calls):
        grouped.setdefault(boundary_id, []).append(
            {"source_file": path, "source_line": line}
        )
    metadata = {
        "dynamic_config": (
            "completed",
            "PythonOptionCache::refresh",
            "local_fake_proven",
        ),
        "setchannel": (
            "fee-port-reviewed",
            "fee_execution::ClnFeeBroadcaster",
            "local_fake_proven",
        ),
    }
    owners = {
        "close": "python-data-service",
        "connect": "python-data-service",
        "datastore": "task-8-core-parity",
        "delinvoice": "hexmem-60",
        "delpay": "hexmem-60",
        "fundchannel": "python-data-service",
        "invoice": "hexmem-60",
        "pay": "hexmem-60",
        "sendpay_waitsendpay": "hexmem-60",
        "signmessage": "python-data-service",
    }
    for boundary_id in grouped:
        if boundary_id.startswith("askrene_"):
            owners[boundary_id] = "hexmem-60"
    rows = []
    for boundary_id in sorted(grouped):
        if boundary_id in metadata:
            owner, adapter, transport = metadata[boundary_id]
        else:
            owner, adapter, transport = owners[boundary_id], None, "missing"
        rows.append(
            boundary(boundary_id, grouped[boundary_id], owner, adapter, transport)
        )
    if len(rows) != 17:
        raise ValueError(f"expected 17 exact external boundary classes, got {len(rows)}")
    return rows


def git_text(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def rust_source_identity(repo_root: Path) -> dict[str, str]:
    relative = "crates/revops/src/main.rs"
    source_commit = git_text(repo_root, "log", "-1", "--format=%H", "--", relative)
    source_tree = git_text(repo_root, "rev-parse", f"{source_commit}^{{tree}}")
    source_blob = git_text(repo_root, "rev-parse", f"{source_commit}:{relative}")
    working_blob = git_text(repo_root, "hash-object", relative)
    if working_blob != source_blob:
        raise ValueError(
            "inspected Rust main.rs is not committed; commit it before regenerating"
        )
    return {
        "rust_source_commit": source_commit,
        "rust_source_tree": source_tree,
        "rust_main_blob_oid": source_blob,
        "rust_main_sha256": sha256((repo_root / relative).read_bytes()),
    }

def generate(
    python_repo: Path, python_commit: str, repo_root: Path
) -> dict[str, Any]:
    production_files = production_python_files(python_repo, python_commit)
    sources = {
        path: git_show(python_repo, python_commit, path) for path in production_files
    }
    tree = ast.parse(sources["cl-revenue-ops.py"].decode("utf-8"))
    rpcs, parameter_methods = extract_rpcs(tree)
    options = extract_options(tree)
    loops = extract_loops(tree)
    registered = derive_rust_methods(repo_root)
    python_names = {entry["name"] for entry in rpcs}
    reachable = registered & python_names
    rust_only = registered - python_names
    unclassified_reachable = sorted(reachable - CLASSIFIED_REACHABLE_RPCS)
    unclassified_rust_only = sorted(rust_only - CLASSIFIED_RUST_ONLY_METHODS)
    if unclassified_reachable or unclassified_rust_only:
        raise ValueError(
            "unclassified Rust registrations: "
            f"python={unclassified_reachable}, rust_only={unclassified_rust_only}"
        )
    for entry in rpcs:
        entry["owner_task"] = owner_task(entry["name"])
        entry["state"] = rpc_state(entry["name"], reachable)

    provenance = {
        "generator": "tools/port/gen_plugin_inventory.py",
        "generator_version": GENERATOR_VERSION,
        "python_source_commit": python_commit,
        **rust_source_identity(repo_root),
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
