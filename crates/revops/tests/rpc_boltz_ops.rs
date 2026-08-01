//! Task 63 slice 6: the 22 Python-equivalent Boltz RPC handlers --
//! uninitialized arms verbatim, usage short-circuits that fire even with
//! Boltz dead, and the mnemonic gate.

use std::sync::Arc;

use revops::boltz_owner::{
    spawn_boltz_owner, BoltzOwnerConfig, BoltzOwnerDeps, BoltzOwnerHandle, StructuralSpendDb,
};
use revops::rpc_boltz_ops::{self as ops, BoltzRpcDeps};
use revops_boltz::cli::{BoltzCli, FakeBoltzCli};
use revops_boltz::error::CliError;
use revops_db::owner::spawn_read_write;
use serde_json::json;

const NOW: i64 = 1_800_000_000;

struct DeadCli;
impl BoltzCli for DeadCli {
    fn run(&self, _args: &[&str], _timeout_secs: u64) -> Result<String, CliError> {
        Err(CliError::Disabled)
    }
}

struct NoStructural;
impl StructuralSpendDb for NoStructural {
    fn structural_spend_sats_24h(&self) -> Result<i64, String> {
        Err("unassembled".into())
    }
}

fn config() -> BoltzOwnerConfig {
    BoltzOwnerConfig {
        daily_budget_sats: 3_000,
        budget_window_hours: 24,
        structural_envelope_sats: 0,
        allow_concurrent_swaps: false,
        default_cooldown_seconds: 3_600,
        auto_cycle_enabled: false,
        create_timeout_secs: 5,
    }
}

/// A pre-cutover owner: no capability, no governor, a disabled query
/// transport -- exactly what production has until Task 69.
async fn unassembled_owner() -> (BoltzOwnerHandle, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let handle = spawn_boltz_owner(BoltzOwnerDeps {
        capability: None,
        governor: None,
        query: Arc::new(DeadCli),
        structural: Arc::new(NoStructural),
        store,
        config: config(),
        clock: Box::new(|| NOW),
    });
    (handle, dir)
}

const UNINITIALIZED: &str = "Boltz CLI integration not initialized";

/// Every manager-backed RPC returns Python's EXACT 1-key arm when the
/// owner is absent (module never constructed).
#[tokio::test]
async fn absent_owner_returns_python_uninitialized_arm() {
    let deps = BoltzRpcDeps {
        owner: None,
        query: Arc::new(DeadCli),
        now: NOW,
        cfg: Default::default(),
    };
    let expected = json!({"error": UNINITIALIZED});

    assert_eq!(
        ops::handle_quote(&deps, Some(&json!(50_000)), None, None).await,
        expected
    );
    assert_eq!(
        ops::handle_loop_out(&deps, Some(&json!(500_000)), None, None, None, None, None).await,
        expected
    );
    assert_eq!(
        ops::handle_loop_in(&deps, Some(&json!(500_000)), None, None, None).await,
        expected
    );
    assert_eq!(
        ops::handle_status(&deps, Some(&json!("swap-1"))).await,
        expected
    );
    assert_eq!(ops::handle_history(&deps, None).await, expected);
    assert_eq!(ops::handle_external_pay_ignores(&deps).await, expected);
    assert_eq!(ops::handle_budget(&deps).await, expected);
    assert_eq!(ops::handle_wallet(&deps).await, expected);
    assert_eq!(
        ops::handle_refund(&deps, Some(&json!("swap-1")), None).await,
        expected
    );
    assert_eq!(
        ops::handle_claim(&deps, Some(&json!("swap-1")), None).await,
        expected
    );
    assert_eq!(
        ops::handle_chainswap(&deps, Some(&json!(500_000)), None, None, None, None).await,
        expected
    );
    assert_eq!(
        ops::handle_withdraw(
            &deps,
            Some(&json!("w")),
            Some(&json!("dest")),
            None,
            None,
            None,
            false,
            false
        )
        .await,
        expected
    );
    assert_eq!(ops::handle_deposit(&deps, None).await, expected);
    assert_eq!(ops::handle_backup(&deps, false).await, expected);
    assert_eq!(
        ops::handle_backup_verify(&deps, Some(&json!("words"))).await,
        expected
    );
    assert_eq!(ops::handle_balance_recommendations(&deps).await, expected);
    assert_eq!(ops::handle_balance_cycle(&deps, true).await, expected);
    assert_eq!(ops::handle_expansion_treasury_status(&deps).await, expected);
    assert_eq!(
        ops::handle_expansion_treasury_recommendations(&deps).await,
        expected
    );
    assert_eq!(
        ops::handle_expansion_treasury_cycle(&deps, true).await,
        expected
    );
}

/// The three usage short-circuits fire BEFORE the initialization guard --
/// Python checks them first, so they appear even with Boltz dead.
#[tokio::test]
async fn usage_short_circuits_precede_the_initialization_guard() {
    let deps = BoltzRpcDeps {
        owner: None,
        query: Arc::new(DeadCli),
        now: NOW,
        cfg: Default::default(),
    };

    assert_eq!(
        ops::handle_status(&deps, None).await,
        json!({"error": "usage: revenue-boltz-status swap_id (per-swap status; \
                see revenue-boltz-wallet/-budget/-history for global state)"})
    );
    assert_eq!(
        ops::handle_refund(&deps, None, None).await,
        json!({"error": "usage: revenue-boltz-refund swap_id [destination]"})
    );
    assert_eq!(
        ops::handle_claim(&deps, None, None).await,
        json!({"error": "usage: revenue-boltz-claim swap_ids [destination]"})
    );
}

/// The auto-cycle pair does NOT use the error arm (py parity):
/// run-now returns the disabled status shape; status never errors and
/// reports `boltz_enabled: false`.
#[tokio::test]
async fn auto_cycle_rpcs_use_status_shapes_not_the_error_arm() {
    let deps = BoltzRpcDeps {
        owner: None,
        query: Arc::new(DeadCli),
        now: NOW,
        cfg: Default::default(),
    };
    let run_now = ops::handle_auto_cycle_run_now(&deps, false, true).await;
    assert_eq!(run_now["status"], "disabled");
    assert_eq!(run_now["reason"], "boltz integration disabled");
    assert_eq!(run_now["trigger"], "manual");
    assert!(run_now.get("error").is_none());

    let status = ops::handle_auto_cycle_status(&deps).await;
    assert_eq!(status["boltz_enabled"], json!(false));
    assert!(status.get("error").is_none());
}

/// An owner whose CAPABILITY is unassembled (pre-cutover production, the
/// real deploy state) is equally on the uninitialized arm for
/// fund-moving RPCs -- the owner exists but can't spend.
#[tokio::test]
async fn unassembled_capability_is_also_the_uninitialized_arm() {
    let (owner, _dir) = unassembled_owner().await;
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(DeadCli),
        now: NOW,
        cfg: Default::default(),
    };
    let expected = json!({"error": UNINITIALIZED});
    assert_eq!(
        ops::handle_loop_out(&deps, Some(&json!(500_000)), None, None, None, None, None).await,
        expected
    );
    assert_eq!(
        ops::handle_refund(&deps, Some(&json!("swap-1")), None).await,
        expected
    );
    // The mnemonic is never reachable without the capability.
    assert_eq!(ops::handle_backup(&deps, true).await, expected);
}

/// Read RPCs go through the QUERY transport and surface its typed
/// failures rather than inventing empty success.
#[tokio::test]
async fn read_rpcs_surface_query_transport_failures() {
    let (owner, _dir) = unassembled_owner().await;
    let cli = FakeBoltzCli::new();
    cli.push_err(CliError::Timeout {
        timeout_secs: 30,
        command: "listswaps (1 args redacted)".into(),
    });
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(SyncFake(cli)),
        now: NOW,
        cfg: Default::default(),
    };
    let history = ops::handle_history(&deps, None).await;
    assert!(
        history["error"].as_str().unwrap().contains("timed out"),
        "{history:?}"
    );
    // The redacted label leaks no values.
    assert!(!history["error"].as_str().unwrap().contains("--json"));
}

/// A healthy query transport returns real read payloads built by the
/// frozen kernel builders.
#[tokio::test]
async fn read_rpcs_build_kernel_payloads() {
    let (owner, _dir) = unassembled_owner().await;
    let cli = FakeBoltzCli::new();
    cli.push_ok(
        json!({"swaps": [
            {"id": "swap-aa", "status": "swap.created", "createdAt": NOW - 100},
        ]})
        .to_string(),
    );
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(SyncFake(cli)),
        now: NOW,
        cfg: Default::default(),
    };
    let history = ops::handle_history(&deps, Some(&json!(10))).await;
    // The frozen kernel's shape: swaps + cost_summary (no invented
    // top-level count).
    assert_eq!(history["cost_summary"]["swap_count"], 1, "{history:?}");
    assert_eq!(history["swaps"][0]["id"], "swap-aa");
}

/// `FakeBoltzCli` is `RefCell`-based (single-threaded); wrap it so the
/// async handlers can hold it across a `Send` bound in tests.
struct SyncFake(FakeBoltzCli);
unsafe impl Send for SyncFake {}
unsafe impl Sync for SyncFake {}
impl BoltzCli for SyncFake {
    fn run(&self, args: &[&str], timeout_secs: u64) -> Result<String, CliError> {
        self.0.run(args, timeout_secs)
    }
}

/// No production surface names the mnemonic egress or the armed
/// transport; the RPC module routes the secret through exactly one call.
#[test]
fn mnemonic_egress_is_single_sited() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let ops_src = std::fs::read_to_string(root.join("src/rpc_boltz_ops.rs")).unwrap();
    assert_eq!(
        ops_src.matches("into_rpc_value()").count(),
        1,
        "the mnemonic must have exactly ONE egress call site"
    );
    for file in ["src/runtime.rs", "src/lnplus_runtime.rs"] {
        let source = std::fs::read_to_string(root.join(file)).unwrap();
        assert!(
            !source.contains("into_rpc_value"),
            "{file} must not touch the secret"
        );
        assert!(
            !source.contains("swapmnemonic"),
            "{file} must not name swapmnemonic"
        );
    }
    // main.rs may register the RPC but must never name the raw verb or
    // the egress itself.
    let main_src = std::fs::read_to_string(root.join("src/main.rs")).unwrap();
    let production = main_src.split("#[cfg(test)]").next().unwrap();
    assert!(!production.contains("swapmnemonic"));
    assert!(!production.contains("into_rpc_value"));
}

/// Every Boltz RPC name registers exactly once through `rpc_name()`, and
/// main.rs hardcodes no raw Boltz method literal.
#[test]
fn boltz_rpcs_register_exactly_once_through_rpc_name() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let main_src = std::fs::read_to_string(root.join("src/main.rs")).unwrap();
    for suffix in BOLTZ_SUFFIXES {
        assert_eq!(
            main_src.matches(&format!("rpc_name(\"{suffix}\")")).count(),
            1,
            "{suffix} must be named exactly once through rpc_name()"
        );
        assert!(
            !main_src.contains(&format!("\"revenue-{suffix}\"")),
            "main.rs must not hardcode revenue-{suffix}"
        );
    }
}

/// The exact 22 (py `@plugin.method("revenue-boltz-*")`).
pub const BOLTZ_SUFFIXES: &[&str] = &[
    "boltz-quote",
    "boltz-loop-out",
    "boltz-loop-in",
    "boltz-status",
    "boltz-history",
    "boltz-external-pay-ignores",
    "boltz-budget",
    "boltz-wallet",
    "boltz-refund",
    "boltz-claim",
    "boltz-chainswap",
    "boltz-withdraw",
    "boltz-deposit",
    "boltz-backup",
    "boltz-backup-verify",
    "boltz-balance-recommendations",
    "boltz-auto-cycle-status",
    "boltz-auto-cycle-run-now",
    "boltz-balance-cycle",
    "boltz-expansion-treasury-status",
    "boltz-expansion-treasury-recommendations",
    "boltz-expansion-treasury-cycle",
];

#[test]
fn suffix_list_is_exactly_twenty_two() {
    assert_eq!(BOLTZ_SUFFIXES.len(), 22);
}

// -- parity-matrix findings (2026-07-29). NOTE: these pins were written
// AFTER the fixes, not RED-first -- the defects were found by
// parity_matrix.py against live Python, which IS the failing-first
// evidence, but it is not a repo test. Disclosed rather than dressed up.

/// `revenue-boltz-backup` WITHOUT the mnemonic is a READ: Python answers
/// it with no capability, so gating it behind the action capability was
/// over-gating. The mnemonic branch still requires the capability.
#[tokio::test]
async fn backup_without_mnemonic_is_a_read_not_an_action() {
    let (owner, _dir) = unassembled_owner().await;
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(DeadCli),
        now: NOW,
        cfg: Default::default(),
    };
    // Read half: answers despite NO action capability, with Python's keys.
    let read = ops::handle_backup(&deps, false).await;
    assert_eq!(
        read["note"],
        json!("Swap mnemonic omitted. Pass include_mnemonic=true to include.")
    );
    assert_eq!(read["pending_swaps"], json!([]));
    assert!(read.get("error").is_none(), "{read:?}");

    // Mnemonic half: still capability-gated.
    assert_eq!(
        ops::handle_backup(&deps, true).await,
        json!({"error": "Boltz CLI integration not initialized"})
    );
}

/// Gapped analytics fields use the project's `_gaps` ARRAY convention so
/// the parity harness tracks them instead of counting them as
/// mismatches, and they carry PYTHON's key names.
#[tokio::test]
async fn analytics_gaps_use_the_gaps_array_convention() {
    let (owner, _dir) = unassembled_owner().await;
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(DeadCli),
        now: NOW,
        cfg: Default::default(),
    };
    for value in [
        ops::handle_balance_recommendations(&deps).await,
        ops::handle_expansion_treasury_status(&deps).await,
        ops::handle_expansion_treasury_recommendations(&deps).await,
    ] {
        let gaps = value["_gaps"]
            .as_array()
            .unwrap_or_else(|| panic!("must declare _gaps as an array: {value:?}"));
        assert!(!gaps.is_empty());
        // Every declared gap names a key that is actually present and null.
        for gap in gaps {
            let key = gap.as_str().unwrap();
            assert!(
                value.get(key).is_some(),
                "declared gap `{key}` must be a present-but-null field: {value:?}"
            );
        }
        assert!(
            value.get("evidence_gap").is_none(),
            "the bare evidence_gap string is not the convention: {value:?}"
        );
    }
}

/// External-pay-ignores uses Python's contract keys, not invented ones.
#[tokio::test]
async fn external_pay_ignores_uses_pythons_keys() {
    let (owner, _dir) = unassembled_owner().await;
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(DeadCli),
        now: NOW,
        cfg: Default::default(),
    };
    let value = ops::handle_external_pay_ignores(&deps).await;
    assert_eq!(value["action"], json!("list"));
    assert!(value["ignores"].is_array());
    assert!(value.get("ignored_external_swaps").is_none());
    assert!(value.get("count").is_none());
}

/// `auto-cycle-status` reports Python's 7-key config block from the
/// RESOLVED config, and `boltz_enabled` is the CONFIGURED enablement
/// (Python's `bool(boltz_manager and boltz_manager.enabled)`) -- not
/// whether an action capability is assembled. Reporting the latter was
/// the mismatch the parity matrix flagged.
#[tokio::test]
async fn auto_cycle_status_reports_pythons_config_block() {
    let (owner, _dir) = unassembled_owner().await;
    let cfg = revops::boltz_config::BoltzCfgSnapshot {
        enabled: true,
        auto_cycle_enabled: true,
        auto_cycle_interval_minutes: 15,
        auto_cycle_max_actions: 1,
        auto_cycle_startup_delay_seconds: 120,
        expansion_treasury_enabled: true,
        expansion_treasury_onchain_target_sats: 1_000_000,
        expansion_treasury_min_deficit_sats: 250_000,
        ..Default::default()
    };
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(DeadCli),
        now: NOW,
        cfg,
    };
    let v = ops::handle_auto_cycle_status(&deps).await;
    // Configured-enabled, even though NO action capability is assembled.
    assert_eq!(v["boltz_enabled"], json!(true));
    let c = &v["config"];
    assert_eq!(c["boltz_auto_cycle_enabled"], json!(true));
    assert_eq!(c["boltz_auto_cycle_interval_minutes"], json!(15));
    assert_eq!(c["boltz_auto_cycle_max_actions"], json!(1));
    assert_eq!(c["boltz_auto_cycle_startup_delay_seconds"], json!(120));
    assert_eq!(c["expansion_treasury_enabled"], json!(true));
    assert_eq!(
        c["expansion_treasury_onchain_target_sats"],
        json!(1_000_000)
    );
    assert_eq!(c["expansion_treasury_min_deficit_sats"], json!(250_000));
    assert!(v.get("error").is_none(), "this RPC never errors: {v:?}");
}

/// Task 66 `revenue-total-cost-budget`: the Boltz liquidity-cost component
/// (py `_boltz_liquidity_cost_components`, cl-revenue-ops.py:8136-8176).
/// With no Boltz manager constructed, Python answers a zeros dict with
/// `available: false` — NOT an error, and NOT a fabricated live reading.
#[tokio::test]
async fn total_cost_boltz_component_without_owner_is_pythons_unavailable_dict() {
    let deps = BoltzRpcDeps {
        owner: None,
        query: Arc::new(DeadCli),
        now: NOW,
        cfg: Default::default(),
    };
    assert_eq!(
        ops::total_cost_boltz_component(&deps, 24, Some(5_000)).await,
        json!({
            "source": "boltz",
            "spent_24h_sats": 0,
            "reserved_24h_sats": 0,
            "available": false,
        })
    );
}

/// Live path: completed swaps inside the window count as spend, pending
/// (non-terminal) swaps as reserved, and the dict carries Python's
/// available-path keys (source/available/spent/reserved/counted_swaps).
#[tokio::test]
async fn total_cost_boltz_component_counts_live_swaps() {
    let (owner, _dir) = unassembled_owner().await;
    let cli = FakeBoltzCli::new();
    cli.push_ok(
        json!({"swaps": [
            {"id": "s1", "status": "swap.claimed", "updatedAt": NOW - 3600,
             "serviceFee": 120, "networkFee": 30},
            {"id": "s2", "status": "swap.created", "createdAt": NOW - 600,
             "networkFee": 40},
            {"id": "s3", "status": "swap.claimed", "updatedAt": NOW - 200 * 3600,
             "serviceFee": 999},
        ]})
        .to_string(),
    );
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(SyncFake(cli)),
        now: NOW,
        cfg: Default::default(),
    };
    assert_eq!(
        ops::total_cost_boltz_component(&deps, 24, Some(5_000)).await,
        json!({
            "source": "boltz",
            "available": true,
            "spent_24h_sats": 150,
            "reserved_24h_sats": 40,
            "counted_swaps": 1,
        })
    );
}

/// A failed listswaps read is Python's exception arm: available:false with
/// the error text and honest zeros — never a silent zero-spend that looks
/// like a live reading.
#[tokio::test]
async fn total_cost_boltz_component_cli_failure_is_unavailable_with_error() {
    let (owner, _dir) = unassembled_owner().await;
    let cli = FakeBoltzCli::new();
    cli.push_err(CliError::Disabled);
    let deps = BoltzRpcDeps {
        owner: Some(owner),
        query: Arc::new(SyncFake(cli)),
        now: NOW,
        cfg: Default::default(),
    };
    let v = ops::total_cost_boltz_component(&deps, 24, None).await;
    assert_eq!(v["source"], "boltz");
    assert_eq!(v["available"], json!(false));
    assert_eq!(v["spent_24h_sats"], 0);
    assert_eq!(v["reserved_24h_sats"], 0);
    assert!(
        v["error"].as_str().is_some_and(|e| !e.is_empty()),
        "error text must be carried: {v:?}"
    );
}
