import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "tools" / "port" / "gen_plugin_inventory.py"
PYTHON_REPO = Path("/home/sat/bin/cl_revenue_ops")
PYTHON_COMMIT = "e579de8df523f174283fc2aa21f395c8ef006ac6"
RPC_SET_SHA256 = "8413e4ab99af64e5617ef074730e6e3747deca437634cf5f35a63b41a005db68"
OPTION_SET_SHA256 = "44d54e01db31943734489e5d5913930fa9ea9399424f68cbb29fa302d20db295"


def load_generator():
    spec = importlib.util.spec_from_file_location("gen_plugin_inventory", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def names_digest(names):
    return hashlib.sha256("\n".join(sorted(names)).encode()).hexdigest()


def generated_inventory():
    module = load_generator()
    return module.generate(PYTHON_REPO, PYTHON_COMMIT, ROOT)


def test_pinned_python_rpc_and_option_sets_are_exact_not_count_only():
    generated = generated_inventory()
    inventory = generated["fixtures/port/plugin_inventory.json"]
    rpc_names = [entry["name"] for entry in inventory["python_rpcs"]]
    option_names = [entry["name"] for entry in inventory["python_options"]]

    assert len(rpc_names) == len(set(rpc_names)) == 69
    assert names_digest(rpc_names) == RPC_SET_SHA256
    assert len(option_names) == len(set(option_names)) == 121
    assert names_digest(option_names) == OPTION_SET_SHA256


def test_two_fee_authority_and_capture_options_are_restored_with_exact_definitions():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    by_name = {entry["name"]: entry for entry in inventory["python_options"]}

    assert by_name["revenue-ops-fee-authority-enabled"] == {
        "default": True,
        "description": "Permit Python fee evaluation and setchannel authority",
        "dynamic": True,
        "name": "revenue-ops-fee-authority-enabled",
        "opt_type": "bool",
        "source_line": 1170,
    }
    assert by_name["revenue-ops-fee-replay-capture-enabled"] == {
        "default": "false",
        "description": (
            "Internal observational fee-cycle replay capture. Disabled by "
            "default; enabling observes the next naturally scheduled cycle "
            "without starting a cycle."
        ),
        "dynamic": True,
        "name": "revenue-ops-fee-replay-capture-enabled",
        "opt_type": "string",
        "source_line": 1179,
    }


def test_eight_startup_loops_and_shutdown_are_separate_exact_registries():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    assert {entry["name"] for entry in inventory["loops"]} == {
        "flow-analysis",
        "fee-adjustment",
        "rebalance-check",
        "startup-snapshot",
        "financial-snapshot",
        "boltz-auto-cycle",
        "capacity-planner",
        "lnplus-watcher",
    }
    assert len(inventory["loops"]) == 8
    assert inventory["shutdown"] == {
        "bounded": True,
        "join_timeout_seconds": 10.0,
        "name": "rpc-shutdown",
        "semantics": "daemon drain thread; bounded wait; process exit proceeds on timeout",
        "source_file": "cl-revenue-ops.py",
        "source_line": 599,
    }


def test_external_adapter_registry_has_exact_classes_and_never_claims_missing_transport():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    by_id = {entry["id"]: entry for entry in inventory["external_boundaries"]}
    assert set(by_id) == {
        "boltzcli",
        "close",
        "datastore",
        "dynamic_config",
        "fundchannel",
        "lnplus_https",
        "sendpay_waitsendpay",
        "setchannel",
        "signmessage",
    }
    assert by_id["lnplus_https"]["rust_transport"] == "missing"
    assert by_id["sendpay_waitsendpay"]["rust_transport"] == "missing"
    assert by_id["boltzcli"]["rust_transport"] == "local_fake_proven_unreachable"


def test_rust_only_methods_are_separate_and_placeholders_are_not_effective():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    assert set(inventory["rust_only_methods"]) == {
        "revenue-fee-wake",
        "revenue-ping",
        "revenue-rebalance-plan",
        "revops-fee-runway-status",
    }
    by_name = {entry["name"]: entry for entry in inventory["python_rpcs"]}
    assert sum(entry["state"]["reachable"] for entry in by_name.values()) == 20
    for name in (
        "revenue-analyze",
        "revenue-capacity-report",
        "revenue-econ-snapshot",
        "revenue-profitability",
    ):
        assert by_name[name]["state"]["effective"] == "placeholder"
        assert by_name[name]["state"]["effective"] != "full"


def test_only_eight_audited_reads_are_full_and_false_successes_stay_partial():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    by_name = {entry["name"]: entry for entry in inventory["python_rpcs"]}
    assert {
        name
        for name, entry in by_name.items()
        if entry["state"]["effective"] == "full"
    } == {
        "revenue-history",
        "revenue-list-banned",
        "revenue-list-ignored",
        "revenue-planner-candidate-sources",
        "revenue-planner-candidates",
        "revenue-planner-history",
        "revenue-planner-status",
        "revenue-spend-ledger",
    }
    for name in ("revenue-config", "revenue-fee-debug", "revenue-status"):
        assert by_name[name]["state"]["effective"] == "partial"


def test_parameter_schema_has_one_entry_per_exact_python_rpc():
    generated = generated_inventory()
    inventory = generated["fixtures/port/plugin_inventory.json"]
    contract = generated["fixtures/port/rpc_params.json"]
    assert contract["schema_version"] == 1
    assert contract["python_source_commit"] == PYTHON_COMMIT
    assert len(contract["methods"]) == 69
    assert {method["name"] for method in contract["methods"]} == {
        rpc["name"] for rpc in inventory["python_rpcs"]
    }
    assert all(method["python_binding"] == "positional_or_named" for method in contract["methods"])


def test_provenance_hashes_are_exact_and_generator_is_byte_deterministic():
    first = generated_inventory()
    second = generated_inventory()
    assert first == second
    inventory = first["fixtures/port/plugin_inventory.json"]
    assert inventory["provenance"]["python_source_commit"] == PYTHON_COMMIT
    assert inventory["provenance"]["generator"] == "tools/port/gen_plugin_inventory.py"
    assert inventory["provenance"]["generator_version"] == 1
    assert set(inventory["provenance"]["source_sha256"]) == {
        "cl-revenue-ops.py",
        "modules/boltz_manager.py",
        "modules/capacity_planner.py",
        "modules/data_service.py",
        "modules/lnplus_swaps.py",
        "modules/rebalance_native_executor_v2.py",
    }
    assert all(
        len(digest) == 64
        for digest in inventory["provenance"]["source_sha256"].values()
    )


def test_checked_in_artifacts_are_exact_generator_output():
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--python-repo",
            str(PYTHON_REPO),
            "--python-commit",
            PYTHON_COMMIT,
            "--repo-root",
            str(ROOT),
            "--check",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
