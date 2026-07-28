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
        "boltzcli",
        "close",
        "connect",
        "datastore",
        "delinvoice",
        "delpay",
        "dynamic_config",
        "fundchannel",
        "invoice",
        "lnplus_https",
        "pay",
        "sendpay_waitsendpay",
        "setchannel",
        "signmessage",
    }
    assert by_id["lnplus_https"]["rust_transport"] == "missing"
    assert by_id["sendpay_waitsendpay"]["rust_transport"] == "missing"
    assert by_id["boltzcli"]["rust_transport"] == "local_fake_proven_unreachable"
    assert by_id["setchannel"]["python_evidence"] == [
        {"source_file": "modules/data_service.py", "source_line": 275}
    ]
    assert by_id["sendpay_waitsendpay"]["python_evidence"][0] == {
        "source_file": "modules/data_service.py",
        "source_line": 332,
    }
    assert by_id["datastore"]["python_evidence"] == [
        {"source_file": "modules/data_service.py", "source_line": 473},
        {"source_file": "modules/rebalance_engine_v2.py", "source_line": 3186},
    ]


def test_external_evidence_is_the_exact_pinned_production_callsite_set():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    actual = {
        (entry["id"], ref["source_file"], ref["source_line"])
        for entry in inventory["external_boundaries"]
        for ref in entry["python_evidence"]
    }
    expected = {
        ("askrene_age", "modules/data_service.py", 419),
        ("askrene_bias_channel", "modules/data_service.py", 408),
        ("askrene_bias_node", "modules/data_service.py", 401),
        ("askrene_create_layer", "modules/data_service.py", 379),
        ("askrene_create_layer", "modules/rebalance_router_v3.py", 615),
        ("askrene_disable_node", "modules/data_service.py", 412),
        ("askrene_inform_channel", "modules/data_service.py", 429),
        ("askrene_remove_layer", "modules/data_service.py", 385),
        ("askrene_remove_layer", "modules/rebalance_engine_v2.py", 319),
        ("askrene_remove_layer", "modules/rebalance_router_v3.py", 668),
        ("askrene_reserve", "modules/data_service.py", 433),
        ("askrene_unreserve", "modules/data_service.py", 437),
        ("askrene_update_channel", "modules/data_service.py", 394),
        ("askrene_update_channel", "modules/rebalance_router_v3.py", 631),
        ("askrene_update_channel", "modules/rebalance_router_v3.py", 649),
        ("boltzcli", "cl-revenue-ops.py", 2801),
        ("boltzcli", "modules/boltz_manager.py", 449),
        ("close", "modules/capacity_planner.py", 3977),
        ("close", "modules/data_service.py", 288),
        ("connect", "modules/data_service.py", 296),
        ("connect", "modules/lnplus_swaps.py", 1604),
        ("datastore", "modules/data_service.py", 473),
        ("datastore", "modules/rebalance_engine_v2.py", 3186),
        ("delinvoice", "modules/data_service.py", 344),
        ("delinvoice", "modules/rebalance_native_executor_v2.py", 391),
        ("delpay", "modules/data_service.py", 340),
        ("delpay", "modules/rebalance_engine_v2.py", 3109),
        ("delpay", "modules/rebalance_native_executor_v2.py", 386),
        ("dynamic_config", "cl-revenue-ops.py", 6723),
        ("fundchannel", "modules/capacity_planner.py", 3060),
        ("fundchannel", "modules/data_service.py", 281),
        ("fundchannel", "modules/lnplus_swaps.py", 1672),
        ("invoice", "modules/data_service.py", 328),
        ("invoice", "modules/rebalance_native_executor_v2.py", 431),
        ("lnplus_https", "modules/lnplus_swaps.py", 93),
        ("pay", "modules/boltz_manager.py", 844),
        ("pay", "modules/data_service.py", 349),
        ("sendpay_waitsendpay", "modules/data_service.py", 332),
        ("sendpay_waitsendpay", "modules/data_service.py", 336),
        ("sendpay_waitsendpay", "modules/rebalance_native_executor_v2.py", 461),
        ("sendpay_waitsendpay", "modules/rebalance_native_executor_v2.py", 462),
        ("setchannel", "modules/data_service.py", 275),
        ("signmessage", "modules/data_service.py", 303),
        ("signmessage", "modules/lnplus_swaps.py", 130),
    }
    assert actual == expected
    assert ("boltzcli", "modules/fee_cycle_capture.py", 418) not in actual


def test_production_scan_provenance_hashes_every_inspected_python_file():
    module = load_generator()
    production_files = module.production_python_files(PYTHON_REPO, PYTHON_COMMIT)
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    assert len(production_files) == 51
    assert set(inventory["provenance"]["source_sha256"]) == set(production_files)


def test_new_direct_production_bypass_fails_closed():
    module = load_generator()
    files = module.production_python_files(PYTHON_REPO, PYTHON_COMMIT)
    sources = {
        path: module.git_show(PYTHON_REPO, PYTHON_COMMIT, path) for path in files
    }
    sources["modules/__init__.py"] += b'\nplugin.rpc.call("pay", {})\n'
    with pytest.raises(ValueError, match="unexpected=.*modules/__init__.py"):
        module.scan_external_calls(sources)


def test_observational_git_subprocess_exclusion_is_exact_and_documented():
    module = load_generator()
    assert module.OBSERVATIONAL_SUBPROCESS_EXCLUSIONS == {
        ("modules/fee_cycle_capture.py", 418): "git rev-parse source identity"
    }
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    boltz_refs = next(
        entry["python_evidence"]
        for entry in inventory["external_boundaries"]
        if entry["id"] == "boltzcli"
    )
    assert {
        (ref["source_file"], ref["source_line"]) for ref in boltz_refs
    } == {
        ("cl-revenue-ops.py", 2801),
        ("modules/boltz_manager.py", 449),
    }


def test_reachability_never_implies_independent_review():
    inventory = generated_inventory()["fixtures/port/plugin_inventory.json"]
    by_name = {entry["name"]: entry for entry in inventory["python_rpcs"]}
    for name in (
        "revenue-analyze",
        "revenue-capacity-report",
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
        "revenue-planner-candidate-sources",
        "revenue-planner-candidates",
        "revenue-planner-history",
        "revenue-planner-status",
        "revenue-spend-ledger",
    ):
        assert by_name[name]["state"]["review"] == "passed"
        assert by_name[name]["state"]["review_evidence"]


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
    assert inventory["provenance"]["generator_version"] == 3
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
