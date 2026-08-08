import hashlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "tools" / "port" / "gen_plugin_inventory.py"
PYTHON_REPO = (
    Path(os.environ["REVOPS_PYTHON_REPO"])
    if "REVOPS_PYTHON_REPO" in os.environ
    else next(
        candidate
        for candidate in (
            ROOT.parent / "cl_revenue_ops",
            ROOT.parents[2] / "cl_revenue_ops",
        )
        if candidate.is_dir()
    )
)
PYTHON_COMMIT = "a5c2e2f65019df5cefe4e1261b7de2823a03e448"
RPC_SET_SHA256 = "ccb4011905c3c45764cec989ec67791b03bc73be084ce5a2ba77bb2a2ab42eed"
OPTION_SET_SHA256 = "3507fe3322b2550fc58f34bd8e55c8253afeac6ca87fa1c33353d1eb851bff29"


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

    assert len(rpc_names) == len(set(rpc_names)) == 39
    assert names_digest(rpc_names) == RPC_SET_SHA256
    assert len(option_names) == len(set(option_names)) == 71
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
        "source_line": 1111,
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
        "source_line": 1120,
    }


def test_five_retained_startup_loops_and_shutdown_are_separate_exact_registries():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    assert {entry["name"] for entry in inventory["loops"]} == {
        "flow-analysis",
        "fee-adjustment",
        "rebalance-check",
        "startup-snapshot",
        "financial-snapshot",
    }
    assert len(inventory["loops"]) == 5
    assert inventory["shutdown"] == {
        "bounded": True,
        "join_timeout_seconds": 10.0,
        "name": "rpc-shutdown",
        "semantics": "daemon drain thread; bounded wait; process exit proceeds on timeout",
        "source_file": "cl-revenue-ops.py",
        "source_line": 583,
    }


def test_external_adapter_registry_has_exact_classes_and_never_claims_missing_transport():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    by_id = {entry["id"]: entry for entry in inventory["external_boundaries"]}
    assert set(by_id) == {
        "askrene_age",
        "askrene_bias_channel",
        "askrene_bias_node",
        "askrene_create_layer",
        "askrene_disable_node",
        "askrene_inform_channel",
        "askrene_remove_layer",
        "askrene_reserve",
        "askrene_unreserve",
        "askrene_update_channel",
        "datastore",
        "delinvoice",
        "delpay",
        "dynamic_config",
        "invoice",
        "sendpay_waitsendpay",
        "setchannel",
    }
    assert by_id["sendpay_waitsendpay"]["rust_transport"] == "missing"
    assert by_id["setchannel"]["python_evidence"] == [
        {"source_file": "modules/data_service.py", "source_line": 275}
    ]
    assert by_id["sendpay_waitsendpay"]["python_evidence"][0] == {
        "source_file": "modules/data_service.py",
        "source_line": 306,
    }
    assert by_id["datastore"]["python_evidence"] == [
        {"source_file": "modules/data_service.py", "source_line": 434},
        {"source_file": "modules/rebalance_engine_v2.py", "source_line": 3678},
    ]


def test_external_evidence_is_the_exact_pinned_production_callsite_set():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    actual = {
        (entry["id"], ref["source_file"], ref["source_line"])
        for entry in inventory["external_boundaries"]
        for ref in entry["python_evidence"]
    }
    expected = {
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
    assert actual == expected
    assert not {"boltzcli", "close", "connect", "fundchannel", "lnplus_https", "pay", "signmessage"} & {entry["id"] for entry in inventory["external_boundaries"]}


def test_production_scan_provenance_hashes_every_inspected_python_file():
    module = load_generator()
    production_files = module.production_python_files(PYTHON_REPO, PYTHON_COMMIT)
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    assert len(production_files) == 46
    assert set(inventory["provenance"]["source_sha256"]) == set(production_files)


def test_new_direct_production_bypass_fails_closed():
    module = load_generator()
    files = module.production_python_files(PYTHON_REPO, PYTHON_COMMIT)
    sources = {
        path: module.git_show(PYTHON_REPO, PYTHON_COMMIT, path) for path in files
    }
    sources["modules/__init__.py"] += b'\nplugin.rpc.call("invoice", {})\n'
    with pytest.raises(ValueError, match="unexpected=.*modules/__init__.py"):
        module.scan_external_calls(sources)


def test_observational_git_subprocess_exclusion_is_exact_and_documented():
    module = load_generator()
    assert module.OBSERVATIONAL_SUBPROCESS_EXCLUSIONS == {
        ("modules/fee_cycle_capture.py", 418): "git rev-parse source identity"
    }
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    assert "subprocess_exec" not in {
        entry["id"] for entry in inventory["external_boundaries"]
    }
    assert "boltzcli" not in {
        entry["id"] for entry in inventory["external_boundaries"]
    }


def test_reachability_never_implies_independent_review():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    by_name = {entry["name"]: entry for entry in inventory["python_rpcs"]}
    for name in (
        "revenue-analyze",
        "revenue-config",
        "revenue-dashboard",
        "revenue-fee-debug",
        "revenue-profitability",
        "revenue-status",
    ):
        assert by_name[name]["state"]["reachable"] is True
        assert by_name[name]["state"]["review"] == "pending"
        assert by_name[name]["state"]["review_evidence"] is None
    for name in (
        "revenue-history",
        "revenue-list-banned",
        "revenue-list-ignored",
        "revenue-spend-ledger",
    ):
        assert by_name[name]["state"]["review"] == "passed"
        assert by_name[name]["state"]["review_evidence"]


def test_rust_only_methods_are_separate_and_retained_python_rpcs_are_reachable():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    assert set(inventory["rust_only_methods"]) == {
        "revenue-ping",
        "revenue-rebalance-plan",
        "revops-fee-runway-status",
    }
    by_name = {entry["name"]: entry for entry in inventory["python_rpcs"]}
    assert sum(entry["state"]["reachable"] for entry in by_name.values()) == 39
    assert not [
        entry for entry in by_name.values()
        if entry["state"]["effective"] == "placeholder"
    ]


def test_reviewed_reads_and_retained_status_rpcs_are_full_and_false_successes_stay_partial():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    by_name = {entry["name"]: entry for entry in inventory["python_rpcs"]}
    assert {
        name
        for name, entry in by_name.items()
        if entry["state"]["effective"] == "full"
    } == {
        "revenue-capex-status",
        "revenue-econ-reconcile",
        "revenue-history",
        "revenue-list-banned",
        "revenue-list-ignored",
        "revenue-spend-ledger",
        "revenue-total-cost-budget",
    }
    for name in ("revenue-config", "revenue-fee-debug", "revenue-status"):
        assert by_name[name]["state"]["effective"] == "partial"


def test_parameter_schema_has_one_entry_per_exact_python_rpc():
    generated = generated_inventory()
    inventory = generated["fixtures/port/plugin_inventory.json"]
    contract = generated["fixtures/port/rpc_params.json"]
    assert contract["schema_version"] == 1
    assert contract["python_source_commit"] == PYTHON_COMMIT
    assert len(contract["methods"]) == 39
    assert {method["name"] for method in contract["methods"]} == {
        rpc["name"] for rpc in inventory["python_rpcs"]
    }
    assert all(method["python_binding"] == "positional_or_named" for method in contract["methods"])


def test_new_rust_registration_fails_closed_until_explicitly_classified(tmp_path):
    module = load_generator()
    main = tmp_path / "crates" / "revops" / "src" / "main.rs"
    main.parent.mkdir(parents=True)
    source = (ROOT / "crates" / "revops" / "src" / "main.rs").read_text()
    source += (
        "\nlet unreviewed_rpc = rpc_name(\"unreviewed\");\n"
        "builder.rpcmethod(&unreviewed_rpc, handler);\n"
    )
    main.write_text(source)
    with pytest.raises(ValueError, match="unclassified new Rust RPC registrations"):
        module.derive_rust_methods(tmp_path)


def test_provenance_refuses_an_uncommitted_rust_main_replacement(tmp_path):
    module = load_generator()
    main = tmp_path / "crates" / "revops" / "src" / "main.rs"
    main.parent.mkdir(parents=True)
    main.write_text("fn main() {}\n")
    subprocess.run(["git", "init", str(tmp_path)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", str(main)], check=True)
    subprocess.run(
        [
            "git", "-C", str(tmp_path), "-c", "user.name=Task64",
            "-c", "user.email=task64@example.invalid", "commit", "-m", "base",
        ],
        check=True,
        capture_output=True,
    )
    main.write_text("fn main() { panic!(\"dirty\"); }\n")
    with pytest.raises(ValueError, match="main.rs is not committed"):
        module.rust_source_identity(tmp_path)


def test_provenance_hashes_are_exact_and_generator_is_byte_deterministic():
    first = generated_inventory()
    second = generated_inventory()
    assert first == second
    inventory = first["fixtures/port/plugin_inventory.json"]
    assert inventory["provenance"]["python_source_commit"] == PYTHON_COMMIT
    assert inventory["provenance"]["generator"] == "tools/port/gen_plugin_inventory.py"
    assert inventory["provenance"]["generator_version"] == 5
    assert "rust_audit_base_commit" not in inventory["provenance"]
    source_commit = subprocess.run(
        [
            "git", "-C", str(ROOT), "log", "-1", "--format=%H", "--",
            "crates/revops/src/main.rs",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    source_tree = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", f"{source_commit}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    source_blob = subprocess.run(
        ["git", "-C", str(ROOT), "hash-object", "crates/revops/src/main.rs"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert inventory["provenance"]["rust_source_commit"] == source_commit
    assert inventory["provenance"]["rust_source_tree"] == source_tree
    assert inventory["provenance"]["rust_main_blob_oid"] == source_blob
    assert set(inventory["provenance"]["source_sha256"]) == set(
        load_generator().production_python_files(PYTHON_REPO, PYTHON_COMMIT)
    )
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


def test_ci_checks_out_pinned_python_source_and_refuses_generator_drift():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert "with: { fetch-depth: 0 }" in workflow
    assert "repository: lightning-goats/cl_revenue_ops" in workflow
    assert f"ref: {PYTHON_COMMIT}" in workflow
    assert "REVOPS_PYTHON_REPO:" in workflow
    assert "gen_plugin_inventory.py" in workflow
    assert "--check" in workflow
