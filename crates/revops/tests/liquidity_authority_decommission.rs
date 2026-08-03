use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("revops crate is nested under workspace/crates")
        .to_path_buf()
}

fn manifest(canonical: bool) -> serde_json::Value {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_revops"));
    if canonical {
        cmd.env("REVOPS_CANONICAL_NAMES", "1");
    } else {
        cmd.env_remove("REVOPS_CANONICAL_NAMES");
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn revops");
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getmanifest",
        "params": {}
    });
    writeln!(child.stdin.take().unwrap(), "{request}\n").unwrap();

    let mut body = String::new();
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        let line = line.expect("read manifest line");
        if line.trim().is_empty() {
            break;
        }
        body.push_str(&line);
    }
    child.kill().ok();
    child.wait().ok();
    serde_json::from_str::<serde_json::Value>(&body).expect("manifest json")["result"].clone()
}

#[test]
fn retired_executor_sources_are_physically_absent() {
    let root = workspace_root();
    let retired = [
        "crates/revops-boltz/Cargo.toml",
        "crates/revops-lnplus/Cargo.toml",
        "crates/revops-capital/src/boltz_reservation.rs",
        "crates/revops-capital/src/planner/mod.rs",
        "crates/revops/src/boltz_boundaries.rs",
        "crates/revops/src/boltz_config.rs",
        "crates/revops/src/boltz_owner.rs",
        "crates/revops/src/lnplus_adapters.rs",
        "crates/revops/src/lnplus_runtime.rs",
        "crates/revops/src/capital_adapters.rs",
        "crates/revops/src/capital_owner.rs",
        "crates/revops/src/rpc_planner_execute.rs",
        "crates/revops/src/rpc_boltz_ops.rs",
        "crates/revops/src/rpc_lnplus_status.rs",
    ];
    for relative in retired {
        assert!(
            !root.join(relative).exists(),
            "retired authority source still exists: {relative}"
        );
    }

    let workspace = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    for retired_member in ["crates/revops-boltz", "crates/revops-lnplus"] {
        assert!(
            !workspace.contains(retired_member),
            "retired workspace member remains: {retired_member}"
        );
    }

    for retained in [
        "crates/revops-capital/src/capex.rs",
        "crates/revops/src/capex_evidence.rs",
        "crates/revops/src/rpc_capex_status.rs",
        "crates/revops/src/rpc_total_cost_budget.rs",
        "crates/revops/src/rpc_spend_ledger.rs",
        "crates/revops/src/rpc_policy.rs",
        "crates/revops/src/rebalance_execution.rs",
        "crates/revops/src/rebalance_owner.rs",
    ] {
        assert!(
            root.join(retained).is_file(),
            "retained core missing: {retained}"
        );
    }
}

#[test]
fn manifest_has_no_retired_rpc_or_option_aliases_and_keeps_the_core() {
    for canonical in [false, true] {
        let manifest = manifest(canonical);
        let methods: Vec<&str> = manifest["rpcmethods"]
            .as_array()
            .unwrap()
            .iter()
            .map(|method| method["name"].as_str().unwrap())
            .collect();
        let options: Vec<&str> = manifest["options"]
            .as_array()
            .unwrap()
            .iter()
            .map(|option| option["name"].as_str().unwrap())
            .collect();

        for retired in ["boltz", "lnplus", "planner"] {
            assert!(
                methods.iter().all(|name| !name.contains(retired)),
                "retired {retired} RPC remains in manifest: {methods:?}"
            );
            assert!(
                options.iter().all(|name| !name.contains(retired)),
                "retired {retired} option remains in manifest: {options:?}"
            );
        }
        assert!(
            methods
                .iter()
                .all(|name| !name.ends_with("capacity-report")),
            "retired capacity-report RPC remains: {methods:?}"
        );

        let prefix = if canonical { "revenue-" } else { "revenue-r-" };
        for retained in [
            "status",
            "budget",
            "cycle",
            "profitability",
            "capex-status",
            "total-cost-budget",
            "spend-ledger",
            "policy",
            "fee-debug",
            "rebalance",
            "rebalance-debug",
        ] {
            let expected = format!("{prefix}{retained}");
            assert!(
                methods.contains(&expected.as_str()),
                "retained RPC missing: {expected}; methods={methods:?}"
            );
        }
    }
}
