use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use revops::now_unix;
use revops_db::fee_runway::FeeSeedEventRow;

/// Speak the first half of the CLN plugin handshake to the compiled binary
/// and return the parsed `getmanifest` response's `"result"` object.
///
/// `canonical` selects the plugin's name-mapping mode (see `main.rs`'s
/// `canonical_names()`): `false` explicitly clears `REVOPS_CANONICAL_NAMES`
/// from the child's environment, so shadow-mode tests never accidentally
/// inherit it from the outer test-runner's environment; `true` sets it, to
/// exercise the canonical (`revenue-ops-*` option / `revenue-*` rpc) name
/// mapping instead of the shadow (`revops-r-*` / `revenue-r-*`) mapping.
fn manifest_with(canonical: bool) -> serde_json::Value {
    let bin = env!("CARGO_BIN_EXE_revops");
    let mut cmd = Command::new(bin);
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

    let req = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getmanifest", "params": {}
    });
    let mut stdin = child.stdin.take().unwrap();
    // CLN frames messages with double newline.
    write!(stdin, "{}\n\n", req).unwrap();

    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut body = String::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("read manifest line");
        if line.trim().is_empty() {
            break;
        }
        body.push_str(&line);
    }
    child.kill().ok();
    child.wait().ok();

    let resp: serde_json::Value = serde_json::from_str(&body).expect("manifest json");
    resp["result"].clone()
}

/// Shadow mode (the default -- no `REVOPS_CANONICAL_NAMES` in the
/// environment) is what every other test in this file exercises.
fn manifest() -> serde_json::Value {
    manifest_with(false)
}

#[test]
fn manifest_advertises_dynamic_plugin() {
    // lightningd refuses `plugin start` for any plugin whose manifest says
    // dynamic=false — found live on lnnode ("Not a dynamic plugin"). The
    // whole deployment model (dynamically started shadow observer) rides
    // on this flag.
    let result = manifest();
    assert_eq!(
        result["dynamic"],
        serde_json::json!(true),
        "manifest: {result}"
    );
}

#[test]
fn manifest_advertises_shadow_names() {
    let result = manifest();
    let opts: Vec<&str> = result["options"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["name"].as_str().unwrap())
        .collect();
    assert!(opts.contains(&"revops-r-observer"), "options: {opts:?}");
    assert!(opts.contains(&"revops-r-db-path"), "options: {opts:?}");
    assert!(opts.contains(&"revops-r-journal-dir"), "options: {opts:?}");
    assert!(opts.contains(&"revops-r-fee-dryrun"), "options: {opts:?}");
    // Task 10: the stateful-shadow mode-matrix options.
    assert!(
        opts.contains(&"revops-r-fee-stateful-shadow"),
        "options: {opts:?}"
    );
    assert!(
        opts.contains(&"revops-r-fee-broadcast"),
        "options: {opts:?}"
    );
    assert!(
        opts.contains(&"revops-r-cutover-arm-path"),
        "options: {opts:?}"
    );
    let methods: Vec<&str> = result["rpcmethods"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(methods.contains(&"revenue-r-ping"), "methods: {methods:?}");
    assert!(
        methods.contains(&"revenue-r-status"),
        "methods: {methods:?}"
    );
    assert!(
        methods.contains(&"revenue-r-config"),
        "methods: {methods:?}"
    );
    assert!(
        methods.contains(&"revenue-r-history"),
        "methods: {methods:?}"
    );
    assert!(
        methods.contains(&"revenue-r-report"),
        "methods: {methods:?}"
    );
    assert!(
        methods.contains(&"revenue-r-dashboard"),
        "methods: {methods:?}"
    );
    // Phase 4b Task 7.
    assert!(
        methods.contains(&"revenue-r-fee-debug"),
        "methods: {methods:?}"
    );
    assert!(
        methods.contains(&"revenue-r-fee-wake"),
        "methods: {methods:?}"
    );
    // Task 10: the read-only runway status RPC -- a fixed name in every
    // mode (see `main.rs`'s `fee_runway_status_name` doc comment), not run
    // through the shadow/canonical `rpc_name()` mapping.
    assert!(
        methods.contains(&"revops-fee-runway-status"),
        "methods: {methods:?}"
    );
    // Task 61 4E: the exact four Python-equivalent LN+ operator RPCs.
    for lnplus in [
        "revenue-r-lnplus-status",
        "revenue-r-lnplus-breaker-clear",
        "revenue-r-lnplus-abandon",
        "revenue-r-lnplus-backfill",
    ] {
        assert!(methods.contains(&lnplus), "missing {lnplus}: {methods:?}");
    }
}

#[test]
fn manifest_registers_all_python_options_under_shadow_prefix() {
    let table: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/options.json")).unwrap();
    let expected = table.as_array().unwrap().len();
    let manifest = manifest();
    let opts = manifest["options"].as_array().unwrap();
    let shadow: Vec<&str> = opts
        .iter()
        .map(|o| o["name"].as_str().unwrap())
        .filter(|n| n.starts_with("revops-r-"))
        .collect();
    // +7 for our own revops-r-observer, revops-r-observer-db-path (Task 2),
    // revops-r-journal-dir (Task 3), revops-r-fee-dryrun (Phase 4b Task 6),
    // and Task 10's revops-r-fee-stateful-shadow / revops-r-fee-broadcast /
    // revops-r-cutover-arm-path -- no Python analogs.
    assert_eq!(shadow.len(), expected + 7, "shadow options registered");
}

/// Phase 4b Task 6 (non-negotiable plan constraint): `revops-r-fee-dryrun`
/// is a bool option, default FALSE (a deploy/restart without explicit
/// opt-in changes nothing).
///
/// Final-review finding I4 (2026-07-26): it is NOT `dynamic`. It used to
/// be, on the Task 6 assumption that a later `setconfig` could flip it --
/// but under the Task 10 mode matrix the operating mode is validated
/// exactly once, at init, and never re-read, so `setconfig
/// revops-r-fee-dryrun false` returned success and changed nothing: a fake
/// off-switch on a live node. The three mode-matrix options next to it are
/// non-dynamic for exactly this reason; this one now matches them.
#[test]
fn manifest_fee_dryrun_is_bool_default_false_not_dynamic() {
    let result = manifest();
    let opt = result["options"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["name"] == "revops-r-fee-dryrun")
        .expect("revops-r-fee-dryrun registered")
        .clone();
    assert_eq!(opt["type"], serde_json::json!("bool"), "opt: {opt}");
    assert_eq!(opt["default"], serde_json::json!(false), "opt: {opt}");
    assert_eq!(opt["dynamic"], serde_json::json!(false), "opt: {opt}");
}

/// Fix round 1 (I-2): pin `revops-r-fee-stateful-shadow`'s type, default,
/// and NOT-dynamic (the operating mode is validated once at init; a
/// runtime `setconfig` flip would be silently ineffective) -- flipping any
/// of the three would otherwise fail no test.
#[test]
fn manifest_fee_stateful_shadow_is_bool_default_false_not_dynamic() {
    let result = manifest();
    let opt = result["options"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["name"] == "revops-r-fee-stateful-shadow")
        .expect("revops-r-fee-stateful-shadow registered")
        .clone();
    assert_eq!(opt["type"], serde_json::json!("bool"), "opt: {opt}");
    assert_eq!(opt["default"], serde_json::json!(false), "opt: {opt}");
    assert_eq!(opt["dynamic"], serde_json::json!(false), "opt: {opt}");
}

/// Same pin, `revops-r-fee-broadcast`.
#[test]
fn manifest_fee_broadcast_is_bool_default_false_not_dynamic() {
    let result = manifest();
    let opt = result["options"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["name"] == "revops-r-fee-broadcast")
        .expect("revops-r-fee-broadcast registered")
        .clone();
    assert_eq!(opt["type"], serde_json::json!("bool"), "opt: {opt}");
    assert_eq!(opt["default"], serde_json::json!(false), "opt: {opt}");
    assert_eq!(opt["dynamic"], serde_json::json!(false), "opt: {opt}");
}

/// Same pin, `revops-r-cutover-arm-path` (string, empty default, not
/// dynamic).
#[test]
fn manifest_cutover_arm_path_is_string_default_empty_not_dynamic() {
    let result = manifest();
    let opt = result["options"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["name"] == "revops-r-cutover-arm-path")
        .expect("revops-r-cutover-arm-path registered")
        .clone();
    assert_eq!(opt["type"], serde_json::json!("string"), "opt: {opt}");
    assert_eq!(opt["default"], serde_json::json!(""), "opt: {opt}");
    assert_eq!(opt["dynamic"], serde_json::json!(false), "opt: {opt}");
}

#[test]
fn manifest_canonical_mode_advertises_revenue_ops_names() {
    let table: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/options.json")).unwrap();
    let expected = table.as_array().unwrap().len();

    let result = manifest_with(true);
    let opts = result["options"].as_array().unwrap();
    let opt_names: Vec<&str> = opts.iter().map(|o| o["name"].as_str().unwrap()).collect();
    assert!(
        opt_names.contains(&"revenue-ops-observer"),
        "options: {opt_names:?}"
    );
    assert!(
        opt_names.contains(&"revenue-ops-db-path"),
        "options: {opt_names:?}"
    );
    assert!(
        opt_names.contains(&"revenue-ops-observer-db-path"),
        "options: {opt_names:?}"
    );
    assert!(
        opt_names.contains(&"revenue-ops-journal-dir"),
        "options: {opt_names:?}"
    );
    assert!(
        opt_names.contains(&"revenue-ops-fee-stateful-shadow"),
        "options: {opt_names:?}"
    );
    assert!(
        opt_names.contains(&"revenue-ops-fee-broadcast"),
        "options: {opt_names:?}"
    );
    assert!(
        opt_names.contains(&"revenue-ops-cutover-arm-path"),
        "options: {opt_names:?}"
    );

    let canonical: Vec<&&str> = opt_names
        .iter()
        .filter(|n| n.starts_with("revenue-ops-"))
        .collect();
    // +7 for our own revenue-ops-observer, revenue-ops-observer-db-path,
    // revenue-ops-journal-dir, revenue-ops-fee-dryrun, and Task 10's
    // revenue-ops-fee-stateful-shadow / revenue-ops-fee-broadcast /
    // revenue-ops-cutover-arm-path (revenue-ops-db-path is registered
    // exactly once, under the fixture's own canonical name -- see
    // register_python_options' doc comment on the db-path skip).
    assert_eq!(
        canonical.len(),
        expected + 7,
        "canonical options registered"
    );

    let methods: Vec<&str> = result["rpcmethods"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(methods.contains(&"revenue-ping"), "methods: {methods:?}");
    assert!(methods.contains(&"revenue-status"), "methods: {methods:?}");
    assert!(methods.contains(&"revenue-config"), "methods: {methods:?}");
    assert!(methods.contains(&"revenue-history"), "methods: {methods:?}");
    assert!(methods.contains(&"revenue-report"), "methods: {methods:?}");
    assert!(
        methods.contains(&"revenue-dashboard"),
        "methods: {methods:?}"
    );
    // Phase 4b Task 7's fee-debug/fee-wake pair, canonical-mapped.
    assert!(
        methods.contains(&"revenue-fee-debug"),
        "methods: {methods:?}"
    );
    assert!(
        methods.contains(&"revenue-fee-wake"),
        "methods: {methods:?}"
    );
    // Task 10's runway status RPC keeps its one fixed name in every mode
    // (never canonical-mapped to "revenue-fee-runway-status").
    assert!(
        methods.contains(&"revops-fee-runway-status"),
        "methods: {methods:?}"
    );
    // Task 61 4E: the exact four Python-equivalent LN+ operator RPCs
    // under their canonical Python names.
    for lnplus in [
        "revenue-lnplus-status",
        "revenue-lnplus-breaker-clear",
        "revenue-lnplus-abandon",
        "revenue-lnplus-backfill",
    ] {
        assert!(methods.contains(&lnplus), "missing {lnplus}: {methods:?}");
    }
    // 2026-07-27: the first production surface over the revops-rebalance
    // crate, which until then was not even linked into this binary.
    // READ-ONLY -- it plans, it never sends.
    assert!(
        methods.contains(&"revenue-rebalance-plan"),
        "methods: {methods:?}"
    );
    // Task 49 (Wave 2 / RPC Batch A): ten more read-only builders --
    // health, profitability, analyze, policy, list-banned, list-ignored,
    // hot-channel-protection-peers, capacity-report, econ-snapshot,
    // spend-ledger -- registered as real `.rpcmethod()` handlers. See
    // `manifest_batch_a_methods_registered_canonical_mode` /
    // `_shadow_mode` below for the exact-name assertions.
    for name in BATCH_A_CANONICAL_METHODS {
        assert!(methods.contains(name), "methods: {methods:?}");
    }
    // Task 60: the three rebalance operator RPCs (owner-backed;
    // Python-parity uninitialized arms until engine assembly at cutover).
    for rebalance in [
        "revenue-rebalance-cycle",
        "revenue-rebalance-debug",
        "revenue-rebalance",
    ] {
        assert!(
            methods.contains(&rebalance),
            "missing {rebalance}: {methods:?}"
        );
    }
    // Task 62: the write-shaped planner cycle RPC (capital owner;
    // Python-parity uninitialized arm until Task 69 authority assembly).
    assert!(
        methods.contains(&"revenue-planner-execute"),
        "missing revenue-planner-execute: {methods:?}"
    );
    // Task 63: the exact 22 Python-equivalent Boltz RPCs (owner-backed;
    // Python-parity uninitialized arm until Task 69 assembles the action
    // capability).
    for boltz in [
        "revenue-boltz-quote",
        "revenue-boltz-loop-out",
        "revenue-boltz-loop-in",
        "revenue-boltz-status",
        "revenue-boltz-history",
        "revenue-boltz-external-pay-ignores",
        "revenue-boltz-budget",
        "revenue-boltz-wallet",
        "revenue-boltz-refund",
        "revenue-boltz-claim",
        "revenue-boltz-chainswap",
        "revenue-boltz-withdraw",
        "revenue-boltz-deposit",
        "revenue-boltz-backup",
        "revenue-boltz-backup-verify",
        "revenue-boltz-balance-recommendations",
        "revenue-boltz-auto-cycle-status",
        "revenue-boltz-auto-cycle-run-now",
        "revenue-boltz-balance-cycle",
        "revenue-boltz-expansion-treasury-status",
        "revenue-boltz-expansion-treasury-recommendations",
        "revenue-boltz-expansion-treasury-cycle",
    ] {
        assert!(methods.contains(&boltz), "missing {boltz}: {methods:?}");
    }
    // Exactly 54 rpc methods total (no leftover revenue-r-* names bleeding
    // through from shadow mode) -- ping/status/config (Phase 1a), Phase 1b
    // Task 5's history/report/dashboard read-RPC subset, Phase 4b Task 7's
    // fee-debug/fee-wake, Task 10's runway status RPC, the read-only
    // rebalance planner, Task 49's ten Batch A builders, Task 56's four
    // DB-backed planner read RPCs, Task 61 4E's four LN+ operator RPCs
    // (status/breaker-clear/abandon/backfill through the LN+ owner),
    // Task 60's three rebalance operator RPCs (cycle/debug/manual), and
    // Task 62's planner-execute (capital owner), and Task 63's 22 Boltz
    // RPCs (serialized Boltz owner).
    //
    // This count is a GUARD, not bookkeeping: it is what forces a new RPC
    // to be named here deliberately rather than appearing unannounced.
    // Python exposes 69; every increment to this number should be a
    // decision someone made on purpose.
    assert_eq!(
        result["rpcmethods"].as_array().unwrap().len(),
        54,
        "methods: {methods:?}"
    );

    // Per the design spec's db-path ruling (docs/superpowers/specs/
    // 2026-07-16-rust-port-design.md lines 78-87): in canonical mode (Python
    // unloaded, this Rust plugin IS the only plugin) the db-path option's
    // default must equal Python's own fixture default
    // (`~/.lightning/revenue_ops.db`, `fixtures/options.json`'s
    // `revenue-ops-db-path` entry), not the shadow-mode opt-in-empty
    // default -- an operator relying on the option's default must still get
    // DB access post-cutover.
    let db_path_opt = opts
        .iter()
        .find(|o| o["name"].as_str() == Some("revenue-ops-db-path"))
        .expect("revenue-ops-db-path registered");
    let table_default = table
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["name"] == "revenue-ops-db-path")
        .expect("fixture has revenue-ops-db-path")["default"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        db_path_opt["default"].as_str(),
        Some(table_default.as_str()),
        "canonical-mode db-path default must equal Python's fixture default: {db_path_opt:?}"
    );
}

/// Shadow mode (both plugins loaded) must keep the opt-in-empty default --
/// this is a companion pin to the canonical-mode assertion above so a
/// future change can't accidentally flip both defaults at once.
#[test]
fn manifest_shadow_mode_db_path_default_stays_empty() {
    let result = manifest_with(false);
    let opts = result["options"].as_array().unwrap();
    let db_path_opt = opts
        .iter()
        .find(|o| o["name"].as_str() == Some("revops-r-db-path"))
        .expect("revops-r-db-path registered");
    assert_eq!(
        db_path_opt["default"].as_str(),
        Some(""),
        "shadow default must stay opt-in-empty"
    );
}

/// Speak the full `getmanifest` -> `init` handshake and return the `init`
/// response's `"result"` object (an `InitResponse`: `{}` on success, or
/// `{"disable": "<reason>"}` if the plugin voluntarily disabled itself).
///
/// `db_path_override`, if `Some`, is sent as an explicit value for the
/// db-path option in the `init` message (under whichever name --
/// `revenue-ops-db-path` or `revops-r-db-path` -- matches `canonical`);
/// `None` omits it entirely from the `init` options map, so `cln-plugin`
/// fills in whatever default this plugin registered (see
/// `cln-plugin-0.7.0`'s `handle_init`: `(None, Some(default)) =>
/// Some(default.clone())`) -- i.e. the exact "operator never touched
/// db-path" case CRITICAL 2 is about.
///
/// `home` pins the child's `$HOME` to a directory that provably has no
/// `.lightning/revenue_ops.db`, so the "default path doesn't exist" case
/// is deterministic regardless of what happens to live in the test
/// runner's real `$HOME`.
fn init_with(
    canonical: bool,
    db_path_override: Option<&str>,
    home: &std::path::Path,
) -> serde_json::Value {
    init_with_extra(canonical, db_path_override, home, &[])
}

/// [`init_with`] plus arbitrary extra `(option_name, value)` pairs sent
/// verbatim in the `init` message's options map -- `option_name` must
/// already be the fully resolved (shadow- or canonical-mapped) name, e.g.
/// `"revops-r-fee-stateful-shadow"`, and `value` must already be the
/// correctly-typed JSON value for that option (a JSON bool for a bool
/// option, matching what lightningd itself sends). Used by Task 10's
/// mode-matrix init tests, which need to set the new mode-matrix options
/// without a live `lightning-rpc` socket (every scenario they cover
/// resolves without dialing one -- see each test's own comment).
fn init_with_extra(
    canonical: bool,
    db_path_override: Option<&str>,
    home: &std::path::Path,
    extra: &[(&str, serde_json::Value)],
) -> serde_json::Value {
    let bin = env!("CARGO_BIN_EXE_revops");
    let mut cmd = Command::new(bin);
    if canonical {
        cmd.env("REVOPS_CANONICAL_NAMES", "1");
    } else {
        cmd.env_remove("REVOPS_CANONICAL_NAMES");
    }
    cmd.env("HOME", home);
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn revops");

    let mut stdin = child.stdin.take().unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());

    let manifest_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getmanifest", "params": {}
    });
    write!(stdin, "{}\n\n", manifest_req).unwrap();
    drain_one_frame(&mut reader);

    let db_path_name = if canonical {
        "revenue-ops-db-path"
    } else {
        "revops-r-db-path"
    };
    let mut options = serde_json::Map::new();
    if let Some(p) = db_path_override {
        options.insert(db_path_name.to_string(), serde_json::json!(p));
    }
    for (name, value) in extra {
        options.insert((*name).to_string(), value.clone());
    }
    let init_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "init",
        "params": {
            "options": options,
            "configuration": {
                "lightning-dir": home.join(".lightning").to_string_lossy(),
                "rpc-file": "lightning-rpc",
                "startup": true,
                "network": "regtest",
                "feature_set": {
                    "init": "", "node": "", "channel": "", "invoice": ""
                }
            }
        }
    });
    write!(stdin, "{}\n\n", init_req).unwrap();
    let body = drain_one_frame(&mut reader);

    child.kill().ok();
    child.wait().ok();

    let resp: serde_json::Value = serde_json::from_str(&body).expect("init json");
    resp["result"].clone()
}

/// Fix round 1 (coordinator rulings I-1/I-3): [`init_with_extra`] plus ONE
/// additional RPC call, keeping the child process alive across all three
/// exchanges (`getmanifest` -> `init` -> `method`) instead of killing it
/// right after `init`. Panics if `init` disabled the plugin -- every
/// caller of this helper expects a live, running plugin to call `method`
/// against. Returns `method`'s own `"result"` object.
fn call_after_init(
    canonical: bool,
    db_path_override: Option<&str>,
    home: &std::path::Path,
    init_extra: &[(&str, serde_json::Value)],
    method: &str,
) -> serde_json::Value {
    let bin = env!("CARGO_BIN_EXE_revops");
    let mut cmd = Command::new(bin);
    if canonical {
        cmd.env("REVOPS_CANONICAL_NAMES", "1");
    } else {
        cmd.env_remove("REVOPS_CANONICAL_NAMES");
    }
    cmd.env("HOME", home);
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn revops");

    let mut stdin = child.stdin.take().unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());

    let manifest_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getmanifest", "params": {}
    });
    write!(stdin, "{}\n\n", manifest_req).unwrap();
    drain_one_frame(&mut reader);

    let db_path_name = if canonical {
        "revenue-ops-db-path"
    } else {
        "revops-r-db-path"
    };
    let mut options = serde_json::Map::new();
    if let Some(p) = db_path_override {
        options.insert(db_path_name.to_string(), serde_json::json!(p));
    }
    for (name, value) in init_extra {
        options.insert((*name).to_string(), value.clone());
    }
    let init_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "init",
        "params": {
            "options": options,
            "configuration": {
                "lightning-dir": home.join(".lightning").to_string_lossy(),
                "rpc-file": "lightning-rpc",
                "startup": true,
                "network": "regtest",
                "feature_set": {
                    "init": "", "node": "", "channel": "", "invoice": ""
                }
            }
        }
    });
    write!(stdin, "{}\n\n", init_req).unwrap();
    let init_body = drain_one_frame(&mut reader);
    let init_resp: serde_json::Value = serde_json::from_str(&init_body).expect("init json");
    assert!(
        init_resp["result"].get("disable").is_none(),
        "call_after_init: init must not disable for this scenario: {init_resp:?}"
    );

    let call_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 3, "method": method, "params": {}
    });
    write!(stdin, "{}\n\n", call_req).unwrap();
    let call_body = drain_one_frame(&mut reader);

    child.kill().ok();
    child.wait().ok();

    let resp: serde_json::Value = serde_json::from_str(&call_body).expect("call json");
    resp["result"].clone()
}

/// Read one newline-terminated JSON-RPC frame (a run of non-blank lines up
/// to the blank-line frame terminator cln-plugin uses), returning the
/// accumulated body.
fn drain_one_frame(reader: &mut BufReader<std::process::ChildStdout>) -> String {
    let mut body = String::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("read frame line");
        if line.trim().is_empty() {
            break;
        }
        body.push_str(&line);
    }
    body
}

/// CRITICAL 2 regression: canonical mode, no explicit db-path override --
/// the option resolves to Python's live default
/// (`~/.lightning/revenue_ops.db`), which is unopenable on a fresh
/// `$HOME` (no such file). Before the fix this disabled the *entire*
/// plugin at init; per the default-path-miss ruling (see `main.rs`), it
/// must instead come up cleanly with `db=None`.
#[test]
fn init_canonical_mode_default_db_path_miss_does_not_disable() {
    let home = tempfile::tempdir().expect("tempdir");
    let result = init_with(true, None, home.path());
    assert!(
        result.get("disable").is_none(),
        "canonical-mode init with no db-path override must not disable: {result:?}"
    );
}

/// Companion: an explicit db-path override pointing at a file that will
/// never exist must still disable the plugin (existing Phase 1a
/// behavior for a genuine misconfiguration) -- pins the other half of the
/// default-vs-explicit split so a future change can't accidentally make
/// both paths lenient.
#[test]
fn init_canonical_mode_explicit_db_path_miss_still_disables() {
    let home = tempfile::tempdir().expect("tempdir");
    let bogus = home.path().join("nope").join("revenue_ops.db");
    let result = init_with(true, Some(bogus.to_str().unwrap()), home.path());
    assert!(
        result.get("disable").is_some(),
        "canonical-mode init with a bad explicit db-path must disable: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Task 10: end-to-end (real plugin process) confirmation that `main()`'s
// mode-matrix wiring reaches the same conclusions as the pure
// `resolve_startup_mode` unit tests in `src/main.rs`. These two scenarios
// need no live `lightning-rpc` socket (neither touches a cutover arm, so
// `resolve_running_node_id` is never called) -- "arm consumption in live
// mode" is covered by `src/main.rs`'s inline tests instead (a live-socket
// fake-RPC rehearsal harness is Task 11's job, not this one's).
// ---------------------------------------------------------------------------

/// Mandatory writable Rust state: `revops-r-fee-stateful-shadow=true` with
/// `revops-r-observer-db-path` explicitly cleared (no Rust-owned store)
/// must disable the plugin loudly at init, naming the gate that refused.
#[test]
fn init_stateful_shadow_without_observer_db_disables() {
    let home = tempfile::tempdir().expect("tempdir");
    let result = init_with_extra(
        false,
        None,
        home.path(),
        &[
            ("revops-r-fee-stateful-shadow", serde_json::json!(true)),
            ("revops-r-fee-dryrun", serde_json::json!(true)),
            ("revops-r-observer-db-path", serde_json::json!("")),
        ],
    );
    let disable = result
        .get("disable")
        .and_then(|d| d.as_str())
        .expect("must disable when the Rust-owned store isn't configured: {result:?}");
    assert!(
        disable.contains("missing_rust_state"),
        "disable reason must name the missing-rust-state gate: {disable}"
    );
}

/// Arm absence in shadow: `revops-r-fee-stateful-shadow=true` +
/// `revops-r-fee-dryrun=true` with the DEFAULT (unset) observer-db-path
/// (which resolves under `$HOME/.lightning/`, writable in this tempdir
/// `$HOME`) and no cutover arm must come up cleanly -- a virgin store
/// defers seeding to the first cycle.
#[test]
fn init_stateful_shadow_without_arm_and_with_observer_db_does_not_disable() {
    let home = tempfile::tempdir().expect("tempdir");
    let result = init_with_extra(
        false,
        None,
        home.path(),
        &[
            ("revops-r-fee-stateful-shadow", serde_json::json!(true)),
            ("revops-r-fee-dryrun", serde_json::json!(true)),
        ],
    );
    assert!(
        result.get("disable").is_none(),
        "autonomous shadow with a configured store and no arm must not disable: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Fix round 1 (coordinator review): I-1 (seed provenance on the runway
// status RPC), I-3 (pin the autonomous-shadow spawn config).
// ---------------------------------------------------------------------------

const RUNWAY_STATUS_METHOD: &str = "revops-fee-runway-status";

/// I-1: no seed event has ever been recorded (a fresh observer db) -- the
/// runway status RPC's `seed_provenance` field must be `null`, not an
/// error and not a missing key.
#[test]
fn runway_status_seed_provenance_is_null_when_absent() {
    let home = tempfile::tempdir().expect("tempdir");
    let result = call_after_init(false, None, home.path(), &[], RUNWAY_STATUS_METHOD);
    assert!(
        result.get("seed_provenance").is_some(),
        "seed_provenance key must be present (null, not absent): {result:?}"
    );
    assert!(
        result["seed_provenance"].is_null(),
        "seed_provenance must be null with no recorded seed event: {result:?}"
    );
}

/// I-1: a previously-recorded seed event must be surfaced verbatim --
/// source db path, MAX(last_update), row count, payload sha256, and
/// source commit (the fields the runway controller consumes), pre-seeded
/// directly into the observer db file (via `revops_db::fee_runway`, the
/// same schema the plugin's own actor opens) BEFORE the plugin process
/// ever starts.
#[test]
fn runway_status_seed_provenance_reports_a_recorded_seed_event() {
    let home = tempfile::tempdir().expect("tempdir");
    let observer_db_path = home.path().join("observer.db");

    {
        let conn = rusqlite::Connection::open(&observer_db_path).expect("open observer db");
        revops_db::notifications::init_schema(&conn).expect("init observer db schema");
        // Task 42: a successful 'seeded' row only exists atomically with
        // its generation-1 commit -- the fixture uses the same path.
        revops_db::fee_runway::commit_fee_cycle(
            &conn,
            &revops_db::fee_runway::FeeCycleCommit {
                cycle_id: "manifest-fixture-cycle".to_string(),
                started_at: 1_700_000_000,
                completed_at: 1_700_000_000,
                source_commit: "deadbeefcafef00d".to_string(),
                binary_sha256: "0".repeat(64),
                pending_seed: Some(FeeSeedEventRow {
                    seeded_at: 1_700_000_000,
                    outcome: "seeded".to_string(),
                    source_db_path: "/var/lib/lightning/revenue_ops.db".to_string(),
                    source_max_last_update: 1_699_999_000,
                    row_count: 7,
                    payload_sha256: "ab".repeat(32),
                    source_commit: "deadbeefcafef00d".to_string(),
                    refused_channel: None,
                    refused_field: None,
                    detail: None,
                }),
                ..Default::default()
            },
        )
        .expect("seed the observer db with committed seed provenance");
    }

    let result = call_after_init(
        false,
        None,
        home.path(),
        &[(
            "revops-r-observer-db-path",
            serde_json::json!(observer_db_path.to_str().unwrap()),
        )],
        RUNWAY_STATUS_METHOD,
    );

    let seed = &result["seed_provenance"];
    assert!(
        !seed.is_null(),
        "seed_provenance must be populated: {result:?}"
    );
    assert_eq!(seed["outcome"], serde_json::json!("seeded"));
    assert_eq!(
        seed["source_db_path"],
        serde_json::json!("/var/lib/lightning/revenue_ops.db")
    );
    assert_eq!(
        seed["source_max_last_update"],
        serde_json::json!(1_699_999_000)
    );
    assert_eq!(seed["row_count"], serde_json::json!(7));
    assert_eq!(seed["payload_sha256"], serde_json::json!("ab".repeat(32)));
    assert_eq!(seed["source_commit"], serde_json::json!("deadbeefcafef00d"));
}

/// I-3: nothing pinned the autonomous-shadow spawn config to
/// `StateLifecycle::SeedOnce` + `TriggerMode::FixedInterval` -- reverting
/// either to the legacy `RehydratePerCycle`/`FlushTriggered` default would
/// fail no test. Drive a REAL plugin process into autonomous-shadow mode
/// (a production-schema fixture db as the production db-path, so the
/// scheduler's required-paths gate is satisfied and it actually spawns)
/// and assert the runway status RPC's own `counters.lifecycle` field
/// reads back `"seed_once"`.
#[test]
fn runway_status_autonomous_shadow_reports_seed_once_lifecycle() {
    let home = tempfile::tempdir().expect("tempdir");
    let fixture_db =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db");
    let prod_db_path = home.path().join("prod.db");
    std::fs::copy(&fixture_db, &prod_db_path).expect("copy fixture.db");

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[
            ("revops-r-fee-stateful-shadow", serde_json::json!(true)),
            ("revops-r-fee-dryrun", serde_json::json!(true)),
        ],
        RUNWAY_STATUS_METHOD,
    );

    assert_eq!(
        result["mode"],
        serde_json::json!("autonomous_shadow"),
        "{result:?}"
    );
    assert_eq!(
        result["counters"]["lifecycle"],
        serde_json::json!("seed_once"),
        "autonomous shadow must spawn with StateLifecycle::SeedOnce: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Fix round 1 (coordinator ruling I-7): `consumed_arm_dir` is the
// nonce-replay ledger -- it must be pinned to `journal_dir` unconditionally
// and refuse outright (never fall back to the arm file's own parent
// directory) when `journal_dir` cannot be resolved at all.
// ---------------------------------------------------------------------------

/// A cutover-arm-path with NEITHER journal-dir NOR observer-db-path
/// resolved (both explicitly cleared) must refuse at init, naming the
/// missing ledger location -- and, critically, the (deliberately
/// nonexistent) arm path must never even be opened: the old
/// `arm_path.parent()`/`PathBuf::from(".")` fallback would instead have
/// attempted (and failed differently -- `ArmInvalid: not_found`) to
/// validate a real file at that fallback location.
#[test]
fn init_cutover_arm_path_without_journal_dir_refuses_before_touching_arm() {
    let home = tempfile::tempdir().expect("tempdir");
    let arm_path = home.path().join("would-be-arm.json");
    let result = init_with_extra(
        false,
        None,
        home.path(),
        &[
            ("revops-r-journal-dir", serde_json::json!("")),
            ("revops-r-observer-db-path", serde_json::json!("")),
            (
                "revops-r-cutover-arm-path",
                serde_json::json!(arm_path.to_str().unwrap()),
            ),
        ],
    );
    let disable = result
        .get("disable")
        .and_then(|d| d.as_str())
        .unwrap_or_else(|| panic!("must disable when journal-dir cannot be resolved: {result:?}"));
    assert!(
        disable.contains("no journal-dir"),
        "disable reason must name the missing consumed-arm ledger location, not an unrelated \
         arm-file error: {disable}"
    );
    assert!(
        !disable.contains("not_found"),
        "must refuse BEFORE ever attempting to open the (nonexistent) arm file, not fail on a \
         file-not-found from a parent-directory fallback: {disable}"
    );
    assert!(
        !arm_path.exists(),
        "the arm path must never be created or touched by this refusal"
    );
}

// ---------------------------------------------------------------------------
// Task 49 (Wave 2 / RPC Batch A): register the ten read-only response
// builders documented in `crates/revops/RPC_BATCH_A.md` as real
// `.rpcmethod()` handlers. Before this change all ten `rpc_*` modules were
// COMPILED (declared in `lib.rs`, see that module's doc comment) but
// UNREACHABLE -- nothing in `main.rs` ever called them, so no manifest
// test could see them either.
//
// The tests below pin two separate things per the task's reachability
// contract: (1) the exact registered method name in both naming modes,
// and (2) that each handler which HAS a real `revops-db` read query calls
// THAT query rather than returning a parallel hand-built shape -- proven
// by seeding a distinctive row directly into a copy of the production
// schema and asserting it round-trips through the live RPC call. Handlers
// that intentionally have no wired evidence source yet (capacity-report,
// econ-snapshot, and most of health) are instead pinned to their honest
// null/`_gaps` contract, so a future change that fabricates data to
// "complete" the response reds here too. C71-27: profitability and analyze
// have LEFT that list -- both are served from real evidence now, and their
// unavailable branches are pinned to refusal shapes that cannot be
// mistaken for Python's own answers.
// ---------------------------------------------------------------------------

/// Batch A's ten methods, shadow-mode names (the `revops-r-*` /
/// `revenue-r-*` mapping `rpc_name()` produces when `REVOPS_CANONICAL_NAMES`
/// is unset).
const BATCH_A_SHADOW_METHODS: &[&str] = &[
    "revenue-r-health",
    "revenue-r-profitability",
    "revenue-r-analyze",
    "revenue-r-policy",
    "revenue-r-list-banned",
    "revenue-r-list-ignored",
    "revenue-r-hot-channel-protection-peers",
    "revenue-r-capacity-report",
    "revenue-r-econ-snapshot",
    "revenue-r-spend-ledger",
];

/// Same ten, canonical-mode names.
const BATCH_A_CANONICAL_METHODS: &[&str] = &[
    "revenue-health",
    "revenue-profitability",
    "revenue-analyze",
    "revenue-policy",
    "revenue-list-banned",
    "revenue-list-ignored",
    "revenue-hot-channel-protection-peers",
    "revenue-capacity-report",
    "revenue-econ-snapshot",
    "revenue-spend-ledger",
];

#[test]
fn manifest_batch_a_methods_registered_shadow_mode() {
    let result = manifest_with(false);
    let methods: Vec<&str> = result["rpcmethods"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    for name in BATCH_A_SHADOW_METHODS {
        assert!(
            methods.contains(name),
            "Batch A shadow method {name} not registered: {methods:?}"
        );
    }
}

#[test]
fn manifest_batch_a_methods_registered_canonical_mode() {
    let result = manifest_with(true);
    let methods: Vec<&str> = result["rpcmethods"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    for name in BATCH_A_CANONICAL_METHODS {
        assert!(
            methods.contains(name),
            "Batch A canonical method {name} not registered: {methods:?}"
        );
    }
}

/// Copy the empty, schema-only `fixtures/fixture.db` into `<home>/prod.db`
/// and return its path -- the same pattern
/// `runway_status_autonomous_shadow_reports_seed_once_lifecycle` uses for
/// the observer db, reused here so the caller-tripwire tests below can
/// seed real production-schema rows via a raw `rusqlite` connection BEFORE
/// the plugin process ever starts.
fn copy_fixture_db(home: &std::path::Path) -> std::path::PathBuf {
    let fixture_db =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db");
    let prod_db_path = home.join("prod.db");
    std::fs::copy(&fixture_db, &prod_db_path).expect("copy fixture.db");
    prod_db_path
}

/// [`call_after_init`] plus an explicit `params` object for the final RPC
/// call, instead of the fixed `{}` that helper always sends -- needed by
/// the mutation-gate tripwire below, which must send `action: "set"`.
fn call_after_init_with_params(
    canonical: bool,
    db_path_override: Option<&str>,
    home: &std::path::Path,
    init_extra: &[(&str, serde_json::Value)],
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let bin = env!("CARGO_BIN_EXE_revops");
    let mut cmd = Command::new(bin);
    if canonical {
        cmd.env("REVOPS_CANONICAL_NAMES", "1");
    } else {
        cmd.env_remove("REVOPS_CANONICAL_NAMES");
    }
    cmd.env("HOME", home);
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn revops");

    let mut stdin = child.stdin.take().unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());

    let manifest_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getmanifest", "params": {}
    });
    write!(stdin, "{}\n\n", manifest_req).unwrap();
    drain_one_frame(&mut reader);

    let db_path_name = if canonical {
        "revenue-ops-db-path"
    } else {
        "revops-r-db-path"
    };
    let mut options = serde_json::Map::new();
    if let Some(p) = db_path_override {
        options.insert(db_path_name.to_string(), serde_json::json!(p));
    }
    for (name, value) in init_extra {
        options.insert((*name).to_string(), value.clone());
    }
    let init_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "init",
        "params": {
            "options": options,
            "configuration": {
                "lightning-dir": home.join(".lightning").to_string_lossy(),
                "rpc-file": "lightning-rpc",
                "startup": true,
                "network": "regtest",
                "feature_set": {
                    "init": "", "node": "", "channel": "", "invoice": ""
                }
            }
        }
    });
    write!(stdin, "{}\n\n", init_req).unwrap();
    let init_body = drain_one_frame(&mut reader);
    let init_resp: serde_json::Value = serde_json::from_str(&init_body).expect("init json");
    assert!(
        init_resp["result"].get("disable").is_none(),
        "call_after_init_with_params: init must not disable for this scenario: {init_resp:?}"
    );

    let call_req = serde_json::json!({
        "jsonrpc": "2.0", "id": 3, "method": method, "params": params
    });
    write!(stdin, "{}\n\n", call_req).unwrap();
    let call_body = drain_one_frame(&mut reader);

    child.kill().ok();
    child.wait().ok();

    let resp: serde_json::Value = serde_json::from_str(&call_body).expect("call json");
    resp["result"].clone()
}

/// 66 lowercase hex chars, `prefix` (2 chars) + `fill` repeated 32 times --
/// a syntactically valid peer_id shape (`is_valid_peer_id`) built without
/// depending on any real node's pubkey.
fn fake_peer_id(prefix: &str, fill: char) -> String {
    format!("{prefix}{}", fill.to_string().repeat(64))
}

/// `revenue-r-policy list` must read the REAL `peer_policies` table
/// through `queries::all_policies`, not a hand-built empty/fixed shape:
/// seed two distinguishable rows directly into a production-schema copy
/// and assert both come back with their exact seeded fields.
#[test]
fn revenue_r_policy_list_reflects_real_peer_policies_rows() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let peer_a = fake_peer_id("02", 'a');
    let peer_b = fake_peer_id("03", 'b');
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at) \
             VALUES (?1, 'dynamic', 'enabled', 777, '[\"batch-a-tripwire\"]', ?2)",
            rusqlite::params![peer_a, 1_800_000_000i64],
        )
        .expect("seed peer_a row");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at) \
             VALUES (?1, 'static', 'disabled', 250, '[]', ?2)",
            rusqlite::params![peer_b, 1_800_000_100i64],
        )
        .expect("seed peer_b row");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-policy",
    );

    assert_eq!(result["count"], serde_json::json!(2), "result: {result:?}");
    let policies = result["policies"].as_array().expect("policies array");
    let a = policies
        .iter()
        .find(|p| p["peer_id"] == serde_json::json!(peer_a))
        .unwrap_or_else(|| panic!("seeded peer_a not in response: {result:?}"));
    assert_eq!(a["fee_ppm_target"], serde_json::json!(777));
    assert_eq!(a["tags"], serde_json::json!(["batch-a-tripwire"]));
    let b = policies
        .iter()
        .find(|p| p["peer_id"] == serde_json::json!(peer_b))
        .unwrap_or_else(|| panic!("seeded peer_b not in response: {result:?}"));
    assert_eq!(b["strategy"], serde_json::json!("static"));
    assert_eq!(b["rebalance_mode"], serde_json::json!("disabled"));
}

/// `revenue-r-policy` mutation actions must be refused BEFORE any DB
/// access (task rule 3) -- no db-path override is supplied here, so a
/// handler that checked `s.db` first would answer "Plugin not
/// initialized" instead of the tactical-action refusal. Asserting the
/// refusal text specifically pins the ordering, not just "some error".
#[test]
fn revenue_r_policy_set_action_is_refused_before_any_db_access() {
    let home = tempfile::tempdir().expect("tempdir");
    let result = call_after_init_with_params(
        false,
        None,
        home.path(),
        &[],
        "revenue-r-policy",
        serde_json::json!({"action": "set", "peer_id": "irrelevant"}),
    );
    assert_eq!(
        result["error"]["code"], "state_writer_authority_absent",
        "must be the stable authority refusal, not a DB-access error: {result:?}"
    );
    assert!(
        result["error"]["message"]
            .as_str()
            .unwrap()
            .contains("write authority"),
        "{result:?}"
    );
}

/// Task 50 correction round, F9: an EXPLICIT `action: null` must be
/// refused as an unknown action (Python: `str(None or "")` -> `""` ->
/// unknown-action error), NOT silently treated as `list` -- the OLD wiring
/// collapsed `v.get("action").and_then(as_str)` to `None` for both an
/// ABSENT key and an explicit `null`, so both defaulted to "list".
#[test]
fn revenue_r_policy_explicit_null_action_is_refused_not_treated_as_list() {
    let home = tempfile::tempdir().expect("tempdir");
    let result = call_after_init_with_params(
        false,
        None,
        home.path(),
        &[],
        "revenue-r-policy",
        serde_json::json!({"action": null}),
    );
    let err = result["error"]
        .as_str()
        .unwrap_or_else(|| panic!("expected an unknown-action refusal, got: {result:?}"));
    assert!(
        err.starts_with("Unknown action:"),
        "explicit null action must be refused like Python's str(None or \"\"): {err}"
    );
    assert!(
        result.get("policies").is_none(),
        "must not silently succeed as `list`: {result:?}"
    );
}

/// Task 50 correction round, F6: `action=changes` with a non-numeric
/// `since` must return Python's exact coercion-error string
/// (cl-revenue-ops.py:5537-5538), not silently substitute `0` and return
/// the full policy table as "changes since the epoch". A valid numeric
/// STRING must still coerce like Python's `int()`.
#[test]
fn revenue_r_policy_changes_since_coercion_matches_python() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let peer = fake_peer_id("02", 'f');
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at) \
             VALUES (?1, 'dynamic', 'enabled', NULL, '[]', ?2)",
            rusqlite::params![peer, 1_800_000_500i64],
        )
        .expect("seed row");
    }

    let garbage = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-policy",
        serde_json::json!({"action": "changes", "since": "abc"}),
    );
    assert_eq!(
        garbage["error"],
        serde_json::json!("Invalid 'since' timestamp. Must be a Unix timestamp."),
        "garbage since: {garbage:?}"
    );
    assert!(
        garbage.get("changes").is_none(),
        "must not fall through to the full policy table: {garbage:?}"
    );

    // A valid numeric STRING must still coerce like Python's int().
    let numeric_string = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-policy",
        serde_json::json!({"action": "changes", "since": "1800000000"}),
    );
    assert_eq!(numeric_string["since"], serde_json::json!(1_800_000_000));
    assert_eq!(
        numeric_string["count"],
        serde_json::json!(1),
        "since=1800000000 is strictly before the seeded row's updated_at \
         (1800000500): {numeric_string:?}"
    );

    // `since` strictly AFTER the seeded row's updated_at excludes it.
    let after = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-policy",
        serde_json::json!({"action": "changes", "since": "1800000500"}),
    );
    assert_eq!(
        after["count"],
        serde_json::json!(0),
        "since == updated_at is NOT strictly greater, so excluded: {after:?}"
    );
}

/// `revenue-r-list-banned` and `revenue-r-list-ignored` both derive from
/// the SAME real `queries::all_policies` read (task rule 4), filtered
/// differently -- seed one banned-tagged row and one passive+disabled row
/// into a production-schema copy and confirm each RPC returns exactly its
/// own peer, not the other's, and not a fixed empty shape.
#[test]
fn revenue_r_list_banned_and_list_ignored_reflect_real_peer_policies_rows() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let banned_peer = fake_peer_id("02", 'c');
    let ignored_peer = fake_peer_id("03", 'd');
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at) \
             VALUES (?1, 'dynamic', 'enabled', NULL, '[\"banned\",\"whale\"]', ?2)",
            rusqlite::params![banned_peer, 1_800_000_200i64],
        )
        .expect("seed banned row");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at) \
             VALUES (?1, 'passive', 'disabled', NULL, '[]', ?2)",
            rusqlite::params![ignored_peer, 1_800_000_300i64],
        )
        .expect("seed ignored row");
    }

    let banned = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-list-banned",
    );
    assert_eq!(banned["count"], serde_json::json!(1), "banned: {banned:?}");
    assert_eq!(
        banned["banned_peers"][0]["peer_id"],
        serde_json::json!(banned_peer)
    );

    let ignored = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-list-ignored",
    );
    assert_eq!(
        ignored["count"],
        serde_json::json!(1),
        "ignored: {ignored:?}"
    );
    assert_eq!(
        ignored["ignored_peers"][0]["peer_id"],
        serde_json::json!(ignored_peer)
    );
    assert_eq!(ignored["ignored_peers"][0]["reason"], "manual");
}

/// Task 50 correction round, F10 (RPC-level tripwire): a banned peer whose
/// row has ONE malformed scalar column (`expires_at` holding non-numeric
/// text) must still appear in `revenue-r-list-banned` -- proving the
/// `revops-db::queries` fix reaches the actual RPC, not just the query
/// layer's own unit tests.
#[test]
fn revenue_r_list_banned_does_not_drop_a_banned_peer_with_a_malformed_column() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let banned_peer = fake_peer_id("02", 'g');
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, tags, updated_at, expires_at) \
             VALUES (?1, 'dynamic', 'enabled', '[\"banned\"]', ?2, 'not-an-integer')",
            rusqlite::params![banned_peer, 1_800_000_600i64],
        )
        .expect("seed banned row with a malformed expires_at");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-list-banned",
    );
    assert_eq!(
        result["count"],
        serde_json::json!(1),
        "the banned peer must not silently vanish over one malformed \
         column: {result:?}"
    );
    assert_eq!(
        result["banned_peers"][0]["peer_id"],
        serde_json::json!(banned_peer)
    );
}

/// Round-2 correction, CRITICAL (codex re-review): the F10 fix above only
/// covers a malformed SCALAR column. Valid SQLite JSON like `["banned", 7]`
/// is a perfectly legal Python list -- `"banned" in tags` is still `True`
/// in Python with the stray `7` present. The pre-round-2 decode parsed the
/// WHOLE tags column as `Vec<String>`, which fails outright on the first
/// non-string element, and defaulted the ENTIRE array to `[]` --
/// re-creating exactly the "banned peer vanishes from
/// revenue-r-list-banned" failure F10 was supposed to close, through a
/// tags-array-shaped hole instead of a scalar-column-shaped one. Captured
/// RED-first against unmodified `2b3d356`.
#[test]
fn revenue_r_list_banned_does_not_drop_a_peer_over_a_mixed_type_tags_array() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let banned_peer = fake_peer_id("02", 'h');
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, tags, updated_at) \
             VALUES (?1, 'dynamic', 'enabled', '[\"banned\", 7]', ?2)",
            rusqlite::params![banned_peer, 1_800_000_700i64],
        )
        .expect("seed banned row with a mixed-type tags array");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-list-banned",
    );
    assert_eq!(
        result["count"],
        serde_json::json!(1),
        "a peer whose real \"banned\" tag sits next to one malformed \
         non-string sibling element must NOT vanish from \
         revenue-r-list-banned: {result:?}"
    );
    assert_eq!(
        result["banned_peers"][0]["peer_id"],
        serde_json::json!(banned_peer)
    );
    assert_eq!(
        result["banned_peers"][0]["tags"],
        serde_json::json!(["banned"]),
        "the surviving tags must keep the valid string member, dropping \
         only the malformed element individually: {result:?}"
    );
}

/// Round-2 correction, CRITICAL, ignored-path equivalent (see the
/// query-layer test's doc comment for why `revenue-r-list-ignored`'s
/// MEMBERSHIP can't disappear over tags the way a banned peer's can --
/// this proves the equivalent-severity failure on the `reason` field
/// reaches the real RPC).
#[test]
fn revenue_r_list_ignored_preserves_reason_tag_despite_mixed_type_tags_array() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let ignored_peer = fake_peer_id("03", 'i');
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, tags, updated_at) \
             VALUES (?1, 'passive', 'disabled', '[\"low_value\", 42]', ?2)",
            rusqlite::params![ignored_peer, 1_800_000_800i64],
        )
        .expect("seed ignored row with a mixed-type tags array");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-list-ignored",
    );
    assert_eq!(result["count"], serde_json::json!(1), "result: {result:?}");
    assert_eq!(
        result["ignored_peers"][0]["reason"],
        serde_json::json!("low_value"),
        "the real reason tag must survive the malformed numeric sibling, \
         not silently fall back to the generic \"manual\" default: {result:?}"
    );
}

/// `revenue-r-hot-channel-protection-peers` (`list` action) must read the
/// real `hot_channel_protection_overrides` table.
#[test]
fn revenue_r_hot_channel_protection_peers_list_reflects_a_real_row() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let peer_id = fake_peer_id("02", 'e');
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO hot_channel_protection_overrides \
             (peer_id, added_at, note, min_depletion_trigger_pct) \
             VALUES (?1, ?2, 'batch-a-tripwire-note', 0.42)",
            rusqlite::params![peer_id, 1_800_000_400i64],
        )
        .expect("seed hot-channel-protection override row");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-hot-channel-protection-peers",
    );
    assert_eq!(result["count"], serde_json::json!(1), "result: {result:?}");
    assert_eq!(result["peers"][0]["peer_id"], serde_json::json!(peer_id));
    assert_eq!(
        result["peers"][0]["note"],
        serde_json::json!("batch-a-tripwire-note")
    );
    assert_eq!(result["peers"][0]["min_depletion_trigger_pct"], 0.42);

    // `action != "list"` must be refused, never treated as an implicit
    // list -- add/remove/clear are DB writes and stay out of this
    // read-only port's scope.
    let refused = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-hot-channel-protection-peers",
        serde_json::json!({"action": "add"}),
    );
    let err = refused["error"]
        .as_str()
        .unwrap_or_else(|| panic!("expected a refusal error, got: {refused:?}"));
    assert!(
        err.contains("not available in this read-only port"),
        "{err}"
    );
}

/// Task 50 correction round, F8 + H6: `action="LIST"` must succeed
/// (Python lowercases, no strip); `action=""`/`null` must default to
/// `list` and succeed (Python's `action or "list"`); `action=" list"`
/// (leading space) must be REFUSED as unknown (Python does NOT strip);
/// and the write-action refusal ("add" -- a real scope boundary) must be a
/// DIFFERENT message from the unknown-action refusal (a typo/garbage
/// string), so a caller can tell "not implemented here" from "doesn't
/// exist at all".
#[test]
fn revenue_r_hot_channel_protection_peers_action_normalization_matches_python() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());

    let uppercase = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-hot-channel-protection-peers",
        serde_json::json!({"action": "LIST"}),
    );
    assert_eq!(
        uppercase["status"],
        serde_json::json!("success"),
        "action=LIST must succeed like Python: {uppercase:?}"
    );

    let empty_string = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-hot-channel-protection-peers",
        serde_json::json!({"action": ""}),
    );
    assert_eq!(
        empty_string["status"],
        serde_json::json!("success"),
        "action=\"\" must default to list like Python: {empty_string:?}"
    );

    let null_action = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-hot-channel-protection-peers",
        serde_json::json!({"action": null}),
    );
    assert_eq!(
        null_action["status"],
        serde_json::json!("success"),
        "action=null must default to list like Python: {null_action:?}"
    );

    // Leading whitespace: Python does NOT strip, so " list" != "list" ->
    // unknown action, refused.
    let leading_space = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-hot-channel-protection-peers",
        serde_json::json!({"action": " list"}),
    );
    let leading_space_err = leading_space["error"]
        .as_str()
        .unwrap_or_else(|| panic!("expected an unknown-action refusal: {leading_space:?}"));
    assert!(
        leading_space_err.starts_with("Unknown action:"),
        "{leading_space_err}"
    );

    // H6: the write-action refusal and the unknown-action refusal must be
    // DIFFERENT messages.
    let write_refused = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-hot-channel-protection-peers",
        serde_json::json!({"action": "remove"}),
    );
    let write_refused_err = write_refused["error"].as_str().unwrap();
    assert!(write_refused_err.contains("not available in this read-only port"));
    assert_ne!(
        write_refused_err, leading_space_err,
        "a real write action's refusal must read differently from an unknown-action refusal"
    );
}

/// `revenue-r-spend-ledger` must read the real `spend_events` table
/// through `queries::spend_ledger_aggregates`.
#[test]
fn revenue_r_spend_ledger_reflects_a_real_spend_events_row() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let event_ts = now_unix() - 60;
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO spend_events \
             (event_id, category, subcategory, amount_sats, timestamp) \
             VALUES ('batch-a-ev1', 'batch_a_tripwire', NULL, 12345, ?1)",
            rusqlite::params![event_ts],
        )
        .expect("seed spend_events row");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-spend-ledger",
    );
    assert_eq!(
        result["spent_24h_sats"],
        serde_json::json!(12345),
        "result: {result:?}"
    );
    assert_eq!(
        result["spent_by_category"]["batch_a_tripwire"],
        serde_json::json!(12345)
    );
    assert_eq!(result["_gaps"], serde_json::json!([]));
}

/// Task 50 correction round, F7: `window_hours` as a numeric STRING
/// (`"48"`) must coerce like Python's `int()`, not silently fall back to
/// the 24h default; garbage must return an in-band error, not a confident
/// ledger for the wrong window; `include_reservations` must match Python
/// truthiness (`"false"` the STRING is truthy).
#[test]
fn revenue_r_spend_ledger_window_hours_and_truthiness_match_python() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    // A row 36h old: inside a 48h window, outside the 24h default.
    let event_ts = now_unix() - 36 * 3600;
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO spend_events \
             (event_id, category, subcategory, amount_sats, timestamp) \
             VALUES ('batch-a-ev2', 'batch_a_tripwire', NULL, 777, ?1)",
            rusqlite::params![event_ts],
        )
        .expect("seed spend_events row");
    }

    // Numeric STRING window_hours must coerce, not silently default to 24h
    // (which would miss the 36h-old row entirely).
    let string_window = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-spend-ledger",
        serde_json::json!({"window_hours": "48"}),
    );
    assert_eq!(string_window["window_hours"], serde_json::json!(48));
    assert_eq!(
        string_window["spent_24h_sats"],
        serde_json::json!(777),
        "a 48h numeric-string window must include the 36h-old row: {string_window:?}"
    );

    // Garbage window_hours must be an in-band error, not a silent default.
    let garbage_window = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-spend-ledger",
        serde_json::json!({"window_hours": "abc"}),
    );
    assert!(
        garbage_window["error"]
            .as_str()
            .unwrap_or_default()
            .contains("invalid literal for int()"),
        "garbage window_hours must be a Python-style int() error, not a silent \
         default: {garbage_window:?}"
    );
    assert!(garbage_window.get("spent_24h_sats").is_none());

    // include_reservations=(the STRING) "false" is Python-truthy.
    let string_false = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-spend-ledger",
        serde_json::json!({"include_reservations": "false"}),
    );
    assert!(
        string_false.get("active_reservations").is_some(),
        "the STRING \"false\" is Python-truthy, so include_reservations must \
         fire: {string_false:?}"
    );
}

/// Round-2 correction, P1 (codex re-review): `python_int` used
/// `u as i64`/`f.trunc() as i64`, which WRAP/SATURATE instead of erroring
/// on a value outside `i64`'s range. `u64::MAX` as `window_hours` wraps to
/// `-1`, then `parse_window_hours`'s `.max(1)` silently turns that into a
/// CLEAN 1-hour ledger -- a confident, wrong-window response instead of a
/// loud rejection of the impossible request. This is the exact scenario
/// the audit named; asserting on the REAL `revenue-r-spend-ledger` handler
/// (not just the `python_int` unit tests) proves the fix reaches the
/// query path, not just the helper.
#[test]
fn revenue_r_spend_ledger_rejects_out_of_range_window_hours_instead_of_wrapping() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    // A row far outside any real window a wrapped/saturated value could
    // honestly cover, so a silently-substituted window would still show a
    // WRONG (but plausible-looking) answer rather than an obvious one.
    let event_ts = now_unix() - 10 * 3600;
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO spend_events \
             (event_id, category, subcategory, amount_sats, timestamp) \
             VALUES ('batch-a-ev3', 'batch_a_tripwire', NULL, 999, ?1)",
            rusqlite::params![event_ts],
        )
        .expect("seed spend_events row");
    }

    // u64::MAX: the OLD `u as i64` cast wraps to -1; `.max(1)` then ran a
    // "clean" 1-hour ledger instead of rejecting the request.
    let huge_u64 = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-spend-ledger",
        serde_json::json!({"window_hours": u64::MAX}),
    );
    assert!(
        huge_u64.get("window_hours") != Some(&serde_json::json!(1)),
        "u64::MAX window_hours must NOT silently become a clean 1-hour \
         ledger: {huge_u64:?}"
    );
    assert!(
        huge_u64["error"].as_str().is_some(),
        "u64::MAX window_hours must be a loud in-band error, not a \
         successful response: {huge_u64:?}"
    );
    assert!(huge_u64.get("spent_24h_sats").is_none());

    // An out-of-i64-range FLOAT: the OLD `f.trunc() as i64` SATURATES
    // (Rust's float->int cast behavior) instead of erroring -- a
    // different-but-still-wrong value succeeding silently.
    let huge_float = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-spend-ledger",
        serde_json::json!({"window_hours": 1e20}),
    );
    assert!(
        huge_float["error"].as_str().is_some(),
        "an out-of-range float window_hours must be a loud in-band error, \
         not a saturated success: {huge_float:?}"
    );
    assert!(huge_float.get("spent_24h_sats").is_none());
}

/// Round-2 correction, P1: the same `python_int` helper feeds
/// `reservation_limit` -- an out-of-range value there must error the same
/// way, not silently run the query with a wrapped/saturated limit.
#[test]
fn revenue_r_spend_ledger_rejects_out_of_range_reservation_limit() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());

    let result = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-spend-ledger",
        serde_json::json!({"include_reservations": true, "reservation_limit": u64::MAX}),
    );
    assert!(
        result["error"].as_str().is_some(),
        "u64::MAX reservation_limit must be a loud in-band error: {result:?}"
    );
    assert!(result.get("active_reservations").is_none());
}

/// Round-2 correction, P1: `revenue-r-policy`'s `changes` action feeds
/// `since` through the SAME `python_int` helper (via `coerce_since`). An
/// out-of-range `since` must be refused via `invalid_since_error`, never
/// silently coerced to a wrapped/saturated value that returns a
/// materially wrong "changes since" answer.
#[test]
fn revenue_r_policy_changes_rejects_out_of_range_since() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let peer = fake_peer_id("02", 'j');
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO peer_policies \
             (peer_id, strategy, rebalance_mode, fee_ppm_target, tags, updated_at) \
             VALUES (?1, 'dynamic', 'enabled', NULL, '[]', ?2)",
            rusqlite::params![peer, 1_800_000_900i64],
        )
        .expect("seed row");
    }

    let result = call_after_init_with_params(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-policy",
        serde_json::json!({"action": "changes", "since": u64::MAX}),
    );
    assert_eq!(
        result["error"],
        serde_json::json!("Invalid 'since' timestamp. Must be a Unix timestamp."),
        "u64::MAX since must be refused, not wrapped to -1 and treated as \
         \"changes since before every row\": {result:?}"
    );
    assert!(
        result.get("changes").is_none(),
        "must not fall through to the full policy table: {result:?}"
    );
}

/// Task 50 correction round, F11: a DB failure inside `pnl_summary` (e.g.
/// a schema mismatch -- SOME tables exist, so the actor's own
/// `table_names` open-time probe succeeds, but `forwards` specifically
/// doesn't) must become an in-band `financials: {"error": ...}` section
/// with the other eight sections still present and still honestly
/// gap-marked -- NOT a whole-call JSON-RPC error that loses every other
/// section (the OLD `?` on `pnl_summary(...).await?` behavior; Python's
/// own per-section try/except never does this, cl-revenue-ops.py:
/// 6217-6218).
#[test]
fn revenue_r_health_pnl_failure_is_an_in_band_financials_error_not_a_whole_call_failure() {
    let home = tempfile::tempdir().expect("tempdir");
    // A DB with SOME table (so the actor's open-time `table_names` probe
    // succeeds and the plugin does not disable) but NOT the `forwards`
    // table `pnl_summary` needs -- a real, reachable SQL failure.
    let prod_db_path = home.path().join("no-forwards.db");
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open db");
        conn.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)", [])
            .expect("seed one unrelated table");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-health",
    );
    assert!(
        result["financials"].is_object(),
        "financials must be an in-band error OBJECT, not a whole-call failure: {result:?}"
    );
    assert!(
        result["financials"].get("error").is_some(),
        "financials must carry an in-band error key: {result:?}"
    );
    // The other eight sections must still be present (not lost to a
    // whole-call JSON-RPC error).
    for field in [
        "channels",
        "fees",
        "rebalancer",
        "budget",
        "boltz",
        "planner",
        "top_routes",
    ] {
        assert!(
            result.get(field).is_some(),
            "{field} must still be present when only financials failed: {result:?}"
        );
    }
    assert!(
        result.get("generated_at").is_some(),
        "top-level shape must survive a financials-only failure: {result:?}"
    );
}

/// Round-2 correction, P1 (codex re-review): `db=None` is a REAL,
/// reachable degraded state -- a fresh `$HOME` with no explicit db-path
/// override misses the fixture default and comes up with `db=None` BY
/// DESIGN (see the "default-path miss" ruling in `main.rs`'s init
/// wiring; `init_canonical_mode_default_db_path_miss_does_not_disable`
/// pins that the plugin does not disable for it). The pre-round-2
/// `revenue-r-health` handler collapsed this into a bare
/// `{"error": "Plugin not initialized"}` -- Python's own `revenue_health`
/// NEVER carries a top-level `error` key; every section is independently
/// populated or gap-marked (Task 50's own F11 fix already established this
/// convention for a LIVE pnl failure -- this is the same contract for the
/// upfront no-DB case, which the F11 fix didn't cover). Captured RED-first
/// against unmodified `2b3d356`: no db-path override, a fresh tempdir
/// `$HOME`, so the plugin comes up running with `db=None`.
#[test]
fn revenue_r_health_with_no_db_returns_honest_shape_not_a_top_level_error() {
    let home = tempfile::tempdir().expect("tempdir");
    let result = call_after_init(false, None, home.path(), &[], "revenue-r-health");

    assert!(
        result.get("error").is_none(),
        "revenue-r-health must never carry a top-level error, even with no \
         DB -- Python's own revenue_health never does: {result:?}"
    );
    assert!(
        result.get("generated_at").is_some(),
        "generated_at must survive a no-DB call: {result:?}"
    );
    assert_eq!(
        result["financials"],
        serde_json::Value::Null,
        "financials must be the honest null+gap shape with no DB, not \
         fabricated: {result:?}"
    );
    assert_eq!(
        result["boltz"],
        serde_json::json!({"enabled": false}),
        "boltz's honest computed answer must survive a no-DB call too: {result:?}"
    );
    let gaps: Vec<&str> = result["_gaps"]
        .as_array()
        .unwrap_or_else(|| panic!("_gaps must be present: {result:?}"))
        .iter()
        .map(|g| g.as_str().unwrap())
        .collect();
    assert!(
        gaps.contains(&"financials"),
        "financials must be gap-declared with no DB: {gaps:?}"
    );
    for field in [
        "channels",
        "fees",
        "rebalancer",
        "budget",
        "planner",
        "top_routes",
        "loops",
    ] {
        assert!(
            result.get(field).is_some(),
            "{field} must still be present (null) with no DB, not lost to \
             a whole-call failure: {result:?}"
        );
    }
}

/// `revenue-r-health`'s `financials` section must be built from the real
/// `pnl_summary` query (task rule 1) while every other section stays an
/// honest gap (per `RPC_BATCH_A.md`'s wiring note: `total_capacity_sats`
/// is not yet wired, so `annualized_roc_pct` and sections 2-9 stay null).
#[test]
fn revenue_r_health_financials_reflect_a_real_forwards_row_rest_stays_gapped() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let observer_db_path = home.path().join("observer-health.db");
    {
        let conn = revops_db::owner::open_observer_db(&observer_db_path).expect("open observer db");
        for (index, id) in revops_db::loop_health::REQUIRED_LOOPS
            .into_iter()
            .enumerate()
        {
            revops_db::loop_health::register_loop(
                &conn,
                id,
                revops_db::loop_health::WiringStatus::NotWired,
                100 + index as i64,
            )
            .unwrap();
            revops_db::loop_health::increment_loop_backpressure(
                &conn,
                id,
                10 + index as u64,
                20 + index as u64,
                200 + index as i64,
            )
            .unwrap();
        }
    }
    let fwd_ts = now_unix() - 60;
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO forwards \
             (in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp, resolved_time) \
             VALUES ('1x1x0', '2x2x0', 101000, 100000, 5000, ?1, ?1)",
            rusqlite::params![fwd_ts],
        )
        .expect("seed forwards row");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[(
            "revops-r-observer-db-path",
            serde_json::json!(observer_db_path.to_str().unwrap()),
        )],
        "revenue-r-health",
    );
    assert_eq!(
        result["financials"]["today"]["revenue_sats"],
        serde_json::json!(5),
        "result: {result:?}"
    );
    assert_eq!(result["financials"]["today"]["forward_count"], 1);
    assert_eq!(result["financials"]["week"]["forward_count"], 1);
    assert_eq!(
        result["financials"]["week"]["annualized_roc_pct"],
        serde_json::Value::Null
    );
    let gaps: Vec<&str> = result["_gaps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g.as_str().unwrap())
        .collect();
    for g in [
        "financials.week.annualized_roc_pct",
        "channels",
        "fees",
        "rebalancer",
        "budget",
        "planner",
        "top_routes",
    ] {
        assert!(gaps.contains(&g), "gaps missing {g}: {gaps:?}");
    }
    assert!(!gaps.contains(&"financials"), "gaps: {gaps:?}");
    assert!(
        !gaps.contains(&"loops"),
        "durable loop inventory is wired: {gaps:?}"
    );
    // Task 67: the eight Python business/startup loops, live through the
    // full plugin, in REQUIRED_LOOPS order.
    assert_eq!(result["loops"].as_array().unwrap().len(), 8);
    assert_eq!(result["loops"][0]["loop_name"], "flow-analysis");
    assert_eq!(result["loops"][1]["loop_name"], "fee-adjustment");
    assert_eq!(result["loops"][7]["loop_name"], "lnplus-watcher");
    assert_eq!(result["loops"][7]["wiring_status"], "not_wired");
    // Task 50 correction round ("should NOT stay gaps"): boltz is no
    // longer a null+gap -- it's Python's own honest `{"enabled": false}`
    // answer with no Boltz manager wired.
    assert!(!gaps.contains(&"boltz"), "gaps: {gaps:?}");
    assert_eq!(result["boltz"], serde_json::json!({"enabled": false}));
}

/// Task 50 correction round, supervisor scope update #2: EVERY Batch-A
/// handler must explicitly refuse a NON-EMPTY positional (array) params
/// value -- `lightning-cli revenue-r-spend-ledger 48` must NOT silently
/// run with the 24h default (the audit's example of pyln's positional
/// binding falling through to defaults with no error at all). An EMPTY
/// array (`[]`) is `lightning-cli`'s own no-argument call shape and must
/// still mean "no params", so every bare invocation continues to succeed.
#[test]
fn revenue_r_batch_a_methods_reject_nonempty_positional_params_empty_array_still_succeeds() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());

    for method in BATCH_A_SHADOW_METHODS {
        // Non-empty positional array -> explicit in-band refusal, never a
        // silent fallback to named-param defaults.
        let rejected = call_after_init_with_params(
            false,
            Some(prod_db_path.to_str().unwrap()),
            home.path(),
            &[],
            method,
            serde_json::json!([48]),
        );
        let err = rejected["error"].as_str().unwrap_or_else(|| {
            panic!("{method}: expected a positional-params refusal, got: {rejected:?}")
        });
        assert!(
            err.contains("positional parameters are not supported"),
            "{method}: {err}"
        );

        // Empty array ([]) is lightning-cli's own bare-call shape -- must
        // NOT be refused (every no-argument call must keep working).
        let bare = call_after_init_with_params(
            false,
            Some(prod_db_path.to_str().unwrap()),
            home.path(),
            &[],
            method,
            serde_json::json!([]),
        );
        let bare_err = bare.get("error").and_then(|e| e.as_str());
        assert!(
            bare_err
                != Some(
                    "positional parameters are not supported by this port; \
                              use named parameters (a JSON object), e.g. {\"window_hours\": 48}"
                ),
            "{method}: an EMPTY array must be treated as no-params, not refused: {bare:?}"
        );
    }
}

/// Recursively replace the VALUE of any object key in `DOCUMENTED_
/// NONDETERMINISTIC_FIELDS` with a fixed placeholder, everywhere it
/// appears in the tree (nested objects/arrays included) -- used below so
/// two responses captured from two SEPARATE process invocations (which
/// can legitimately land in different wall-clock seconds) can still be
/// compared for semantic equality without a real behavioral difference
/// being masked by an unrelated clock tick.
/// Task 67 addition: `boot_id` is `<unix>-<pid>`, so it differs between
/// two separate process invocations for exactly the same reason
/// `generated_at` does. Normalizing it keeps this cross-process
/// semantic-equality check honest; a real behavioural difference in any
/// other field still surfaces. (The per-loop `boot_id`/`terminal_boot_id`
/// are NOT normalized -- those are evidence about which process produced a
/// terminal, and a divergence there would be a real finding.)
const DOCUMENTED_NONDETERMINISTIC_FIELDS: &[&str] = &["generated_at", "timestamp"];

fn normalize_nondeterministic_fields(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                let is_runtime_loop_timestamp = k == "updated_at"
                    && map.contains_key("loop_name")
                    && map.contains_key("wiring_status");
                // Top-level per-process boot identity only -- NOT the
                // per-loop boot columns, which are real evidence.
                let is_process_boot_id = k == "boot_id" && map.contains_key("generated_at");
                if DOCUMENTED_NONDETERMINISTIC_FIELDS.contains(&k.as_str())
                    || is_runtime_loop_timestamp
                    || is_process_boot_id
                {
                    out.insert(k.clone(), serde_json::json!("<normalized>"));
                } else {
                    out.insert(k.clone(), normalize_nondeterministic_fields(val));
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(normalize_nondeterministic_fields)
                .collect(),
        ),
        other => other.clone(),
    }
}

#[test]
fn nondeterminism_normalizer_only_masks_runtime_loop_updated_at() {
    let input = serde_json::json!({
        "policy": {"updated_at": 1234},
        "loop": {
            "loop_name": "fee",
            "wiring_status": "ready",
            "updated_at": 5678
        }
    });

    let normalized = normalize_nondeterministic_fields(&input);
    assert_eq!(normalized["policy"]["updated_at"], 1234);
    assert_eq!(normalized["loop"]["updated_at"], "<normalized>");
}

/// Round-2 correction, P3 (codex re-review): the positional-params test
/// above only proves an empty array (`[]`) response is not the EXACT
/// positional-refusal error string -- it does NOT prove `[]` behaves like
/// the no-params `{}` call. A handler that returned some OTHER error, or a
/// divergent default result, for `[]` vs `{}` would still pass that
/// assertion. This test asserts full SEMANTIC EQUALITY (after normalizing
/// only the documented nondeterministic wall-clock fields) between the two
/// call shapes, table-driven over every Batch-A no-argument handler --
/// closing the gap the empty-array-only check left open.
#[test]
fn revenue_r_batch_a_methods_empty_array_and_empty_object_params_are_semantically_equal() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());

    for method in BATCH_A_SHADOW_METHODS {
        let via_empty_array = call_after_init_with_params(
            false,
            Some(prod_db_path.to_str().unwrap()),
            home.path(),
            &[],
            method,
            serde_json::json!([]),
        );
        let via_empty_object = call_after_init_with_params(
            false,
            Some(prod_db_path.to_str().unwrap()),
            home.path(),
            &[],
            method,
            serde_json::json!({}),
        );

        let normalized_array = normalize_nondeterministic_fields(&via_empty_array);
        let normalized_object = normalize_nondeterministic_fields(&via_empty_object);
        assert_eq!(
            normalized_array, normalized_object,
            "{method}: an EMPTY array ([]) call must be semantically \
             equivalent to a no-params ({{}}) call, not merely \"not the \
             positional-refusal string\" -- []: {via_empty_array:?} vs \
             {{}}: {via_empty_object:?}"
        );
    }
}

/// Task 50 correction round (F1-F5), updated by C71-27: the builders that
/// still have no wired live-evidence pipeline (capacity-report,
/// econ-snapshot) must come back through their real registered handler in
/// an UNMISTAKABLY-MARKED not-wired shape, and the two that ARE now wired
/// (profitability, analyze) must return refusal shapes -- never a shape that collides
/// with one of Python's own legitimate answers (a real empty fleet, a real
/// unknown channel/SCID, a real disabled-by-config answer) and never a
/// success-shaped stub. These need no DB at all (none of the four handlers
/// touches `s.db`), so no db-path override is supplied.
///
/// This replaces the pre-Task-50 version of this test, which pinned the
/// OLD (fabrication/collision) shapes the audit flagged as F1-F5 -- see
/// `crates/revops/TASK49-REPORT.md`'s "Task-50 correction round" section
/// for the red transcript this test produced against unmodified `a61ccee`.
#[test]
fn revenue_r_gap_only_batch_a_methods_stay_honest() {
    let home = tempfile::tempdir().expect("tempdir");

    // C71-27: the `ChannelProfitability` pipeline now EXISTS, so this is no
    // longer a `not_yet_ported` surface. With no stores configured the
    // handler must say the stores are unavailable -- and, critically, must
    // still not collide with Python's own vocabulary: not a real empty-fleet
    // summary, and not "No data available", which is Python's answer for a
    // channel that does not exist. Telling an operator their channel is
    // unknown because a database was unreachable points at the wrong action.
    let profitability = call_after_init(false, None, home.path(), &[], "revenue-r-profitability");
    assert_eq!(
        profitability["error"],
        serde_json::json!("profitability_store_not_configured"),
        "{profitability:?}"
    );
    assert!(
        profitability.get("summary").is_none(),
        "must not reuse Python's real fleet-summary shape: {profitability:?}"
    );
    assert_ne!(
        profitability["error"],
        serde_json::json!("not_yet_ported"),
        "the pipeline is wired; an unwired marker here would hide a real outage"
    );

    let profitability_single = call_after_init_with_params(
        false,
        None,
        home.path(),
        &[],
        "revenue-r-profitability",
        serde_json::json!({"channel_id": "123x456x789"}),
    );
    assert_eq!(
        profitability_single["error"],
        serde_json::json!("evidence_unavailable"),
        "{profitability_single:?}"
    );
    assert_eq!(
        profitability_single["channel_id"],
        serde_json::json!("123x456x789")
    );
    assert_ne!(
        profitability_single["error"],
        serde_json::json!("No data available"),
        "must not collide with Python's real unknown-channel answer: {profitability_single:?}"
    );

    // No channel_id: the whole-fleet sweep is a mutating background job
    // and is deliberately NOT ported here (RPC_BATCH_A.md's contract) --
    // the handler must say so honestly, not silently return an empty
    // single-channel shape.
    let analyze_no_id = call_after_init(false, None, home.path(), &[], "revenue-r-analyze");
    assert_eq!(analyze_no_id["error"], serde_json::json!("not_yet_ported"));

    // F71-R23: with a channel_id, analyze is no longer a declared gap --
    // it is served from the flow pass's persisted state. This harness
    // calls immediately after init, so the flow loop is registered but its
    // first pass (30s, F71-R26) has not completed, and the honest answer
    // is a LIVE refusal naming that state.
    //
    // The original F5 intent still holds and is now stronger: Python's
    // real unknown-channel answer is `{"channel": ..., "analysis": null}`
    // with no error key, and this response cannot be mistaken for it
    // because it carries no `analysis` key at all.
    let analyze_with_id = call_after_init_with_params(
        false,
        None,
        home.path(),
        &[],
        "revenue-r-analyze",
        serde_json::json!({"channel_id": "123x456x789"}),
    );
    assert_eq!(analyze_with_id["channel"], serde_json::json!("123x456x789"));
    assert_eq!(
        analyze_with_id["error"],
        serde_json::json!("flow_evidence_no_pass_this_boot"),
        "a live not-ready state must not be reported as an unported gap: {analyze_with_id:?}"
    );
    assert_eq!(
        analyze_with_id["boot_status"],
        serde_json::json!("never_run_this_boot")
    );
    assert!(
        analyze_with_id.get("analysis").is_none(),
        "must not collide with Python's real unknown-channel null: {analyze_with_id:?}"
    );
    assert_ne!(
        analyze_with_id["error"],
        serde_json::json!("not_yet_ported"),
        "analyze is wired now; claiming otherwise is a false statement about this port"
    );

    // F2: no capacity planner exists -- must be Python's EXACT error shape
    // (cl-revenue-ops.py:4586-4587), 1 key, no timestamp, not a
    // success-shaped stub.
    let capacity_report =
        call_after_init(false, None, home.path(), &[], "revenue-r-capacity-report");
    assert_eq!(
        capacity_report,
        serde_json::json!({"error": "Capacity planner not initialized"}),
        "must be Python's exact 1-key error shape: {capacity_report:?}"
    );

    // Task 62: planner-execute pre-cutover -- the capital adapters are
    // never assembled in production, so the arm is Python's EXACT 1-key
    // error shape (cl-revenue-ops.py:4699-4700), same as capacity-report
    // above. NOT a success-shaped stub, NOT a typed Rust refusal.
    let planner_execute =
        call_after_init(false, None, home.path(), &[], "revenue-r-planner-execute");
    assert_eq!(
        planner_execute,
        serde_json::json!({"error": "Capacity planner not initialized"}),
        "must be Python's exact 1-key error shape: {planner_execute:?}"
    );

    // Task 63: the Boltz action capability is never assembled in
    // production, so every fund-moving Boltz RPC returns Python's EXACT
    // 1-key arm (cl-revenue-ops.py:7690-7692). The usage short-circuits
    // fire FIRST for status/refund/claim, exactly like Python.
    let boltz_loop_out = call_after_init_with_params(
        false,
        None,
        home.path(),
        &[],
        "revenue-r-boltz-loop-out",
        serde_json::json!({"amount_sats": 500_000}),
    );
    assert_eq!(
        boltz_loop_out,
        serde_json::json!({"error": "Boltz CLI integration not initialized"}),
        "must be Python's exact 1-key error shape: {boltz_loop_out:?}"
    );
    let boltz_status = call_after_init(false, None, home.path(), &[], "revenue-r-boltz-status");
    assert_eq!(
        boltz_status["error"],
        serde_json::json!(
            "usage: revenue-boltz-status swap_id (per-swap status; see \
             revenue-boltz-wallet/-budget/-history for global state)"
        ),
        "the usage short-circuit precedes the init guard: {boltz_status:?}"
    );
    // auto-cycle-status never errors (py parity) and reports disabled.
    let auto_cycle = call_after_init(
        false,
        None,
        home.path(),
        &[],
        "revenue-r-boltz-auto-cycle-status",
    );
    assert_eq!(auto_cycle["boltz_enabled"], serde_json::json!(false));
    assert!(auto_cycle.get("error").is_none(), "{auto_cycle:?}");

    // F1, updated by C71-30/C71-34: the `econ_shadow_enabled` surface IS
    // wired now -- it is a PUBLIC_RUNTIME_KEYS override with no registered
    // CLN option, so it resolves from `config_overrides`. With no
    // production database configured here it cannot be READ, and the
    // original F1 invariant still holds and still matters: the response
    // must not claim any enabled state, because `enabled: false` is a
    // hardcoded lie on any node whose operator turned the shadow on. Only
    // the error code changes -- from "not ported" to "could not read".
    let econ_snapshot = call_after_init(false, None, home.path(), &[], "revenue-r-econ-snapshot");
    assert!(
        econ_snapshot.get("enabled").is_none(),
        "must not claim any enabled state (true or false) when the config \
         surface cannot be read: {econ_snapshot:?}"
    );
    assert_eq!(
        econ_snapshot["error"],
        serde_json::json!("econ_shadow_config_unavailable")
    );
    assert_ne!(
        econ_snapshot["error"],
        serde_json::json!("econ shadow not_yet_ported"),
        "the surface is wired; an unported marker would hide a real outage"
    );
}

// ---------------------------------------------------------------------------
// Task 56: the four planner read RPCs. The pure builders existed before this
// task, but none was registered and none could reach production evidence.
// These tests pin the public names, the real read-only DB round-trip, and the
// deliberate named-parameter-only boundary.
// ---------------------------------------------------------------------------

const PLANNER_READ_SHADOW_METHODS: &[&str] = &[
    "revenue-r-planner-candidate-sources",
    "revenue-r-planner-candidates",
    "revenue-r-planner-history",
    "revenue-r-planner-status",
];

const PLANNER_READ_CANONICAL_METHODS: &[&str] = &[
    "revenue-planner-candidate-sources",
    "revenue-planner-candidates",
    "revenue-planner-history",
    "revenue-planner-status",
];

#[test]
fn manifest_registers_planner_read_quartet_in_both_naming_modes() {
    for (canonical, expected) in [
        (false, PLANNER_READ_SHADOW_METHODS),
        (true, PLANNER_READ_CANONICAL_METHODS),
    ] {
        let result = manifest_with(canonical);
        let methods: Vec<&str> = result["rpcmethods"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        for name in expected {
            assert!(
                methods.contains(name),
                "planner read method {name} not registered (canonical={canonical}): {methods:?}"
            );
        }
    }
}

fn seed_planner_read_tripwires(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).expect("open planner tripwire db");
    conn.execute(
        "INSERT INTO planner_candidates
         (peer_id, score, source, last_evaluated, capacity_recommendation_sats,
          connect_successes, connect_failures, metadata_json)
         VALUES ('peer-low', 1.25, 'manual', 100, NULL, 2, 3, NULL)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO planner_candidates
         (peer_id, score, source, last_evaluated, capacity_recommendation_sats,
          connect_successes, connect_failures, metadata_json)
         VALUES ('peer-high', 9.87654, 'gossip', 200, 3000000, 7, 1, '{\"tripwire\":true}')",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO planner_actions
         (action_type, peer_id, channel_id, amount_sats, estimated_cost_sats,
          actual_cost_sats, status, created_at, completed_at, reason, metadata_json)
         VALUES ('open', 'peer-old', NULL, 1000000, 5000, NULL,
                 'planned', 100, NULL, NULL, NULL)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO planner_actions
         (action_type, peer_id, channel_id, amount_sats, estimated_cost_sats,
          actual_cost_sats, status, created_at, completed_at, reason, metadata_json)
         VALUES ('close', 'peer-new', '123x1x0', NULL, 321, 299,
                 'completed', 200, 210, 'tripwire-reason', '{\"source\":\"task56\"}')",
        [],
    )
    .unwrap();
}

#[test]
fn planner_read_rpcs_round_trip_distinctive_database_rows() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    seed_planner_read_tripwires(&prod_db_path);
    let db_path = Some(prod_db_path.to_str().unwrap());

    let sources = call_after_init_with_params(
        false,
        db_path,
        home.path(),
        &[],
        "revenue-r-planner-candidate-sources",
        serde_json::json!({}),
    );
    assert_eq!(sources["total"], 2, "sources: {sources:?}");
    assert_eq!(sources["by_source"]["gossip"], 1);
    assert_eq!(sources["by_source"]["manual"], 1);
    assert_eq!(sources["candidates"][0]["peer_id"], "peer-high");
    assert_eq!(sources["candidates"][0]["score"], 9.8765);

    let candidates = call_after_init_with_params(
        false,
        db_path,
        home.path(),
        &[],
        "revenue-r-planner-candidates",
        serde_json::json!({"limit": 1}),
    );
    assert_eq!(candidates["count"], 1, "candidates: {candidates:?}");
    assert_eq!(candidates["candidates"][0]["peer_id"], "peer-high");
    assert_eq!(
        candidates["candidates"][0]["metadata_json"],
        "{\"tripwire\":true}"
    );

    let history = call_after_init_with_params(
        false,
        db_path,
        home.path(),
        &[],
        "revenue-r-planner-history",
        serde_json::json!({"limit": 1}),
    );
    assert_eq!(history["count"], 1, "history: {history:?}");
    assert_eq!(history["actions"][0]["peer_id"], "peer-new");
    assert_eq!(history["actions"][0]["completed_at"], 210);
    assert_eq!(history["actions"][0]["reason"], "tripwire-reason");
    assert_eq!(
        history["actions"][0]["metadata_json"],
        "{\"source\":\"task56\"}"
    );

    let status = call_after_init_with_params(
        false,
        db_path,
        home.path(),
        &[
            ("revops-r-planner-enabled", serde_json::json!(true)),
            ("revops-r-planner-dry-run", serde_json::json!(true)),
            ("revops-r-planner-execute-closes", serde_json::json!(true)),
            (
                "revops-r-planner-max-closes-per-cycle",
                serde_json::json!(2),
            ),
        ],
        "revenue-r-planner-status",
        serde_json::json!({}),
    );
    assert_eq!(status["enabled"], true, "status: {status:?}");
    assert_eq!(status["dry_run"], true);
    assert_eq!(status["execute_closes"], true);
    assert_eq!(status["max_closes_per_cycle"], 2);
    assert_eq!(status["close_execution_effective"], true);
    assert_eq!(status["candidate_pool_size"], 2);
    assert_eq!(status["recent_actions"].as_array().unwrap().len(), 2);
    assert_eq!(status["recent_actions"][0]["peer_id"], "peer-new");
}

#[test]
fn planner_read_rpcs_refuse_nonempty_positional_params() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    for method in PLANNER_READ_SHADOW_METHODS {
        let result = call_after_init_with_params(
            false,
            Some(prod_db_path.to_str().unwrap()),
            home.path(),
            &[],
            method,
            serde_json::json!([1]),
        );
        assert_eq!(
            result["error"],
            "positional parameters are not supported by this port; use named parameters (a JSON object), e.g. {\"window_hours\": 48}",
            "{method}: {result:?}"
        );
    }
}

/// C71-27: an unreachable node is not an empty fleet.
///
/// With BOTH stores configured, the profitability pass still needs one
/// fresh bounded `listpeerchannels`. There is no lightning socket in this
/// harness, so that call fails -- and the handler must SAY so. Treating
/// the failure as an empty channel list would skip every channel for "no
/// opener", which reads as a fleet of unevaluable channels rather than as
/// a node this plugin could not reach; and treating it as a real fleet
/// would report `total_channels: 0` for a node that has channels.
#[test]
fn revenue_r_profitability_refuses_when_the_channel_snapshot_cannot_be_fetched() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    let observer_db_path = home.path().join("observer.db");

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[(
            "revops-r-observer-db-path",
            serde_json::json!(observer_db_path.to_str().unwrap()),
        )],
        "revenue-r-profitability",
    );

    assert_eq!(
        result["error"],
        serde_json::json!("profitability_channels_unavailable"),
        "an unreachable node must be named, not reported as an empty or \
         unevaluable fleet: {result:?}"
    );
    assert!(
        result.get("summary").is_none(),
        "must not produce a zeroed fleet summary: {result:?}"
    );
}

/// C71-28: the dashboard's four formerly-gapped fields need a live node.
///
/// With the production DB configured but no lightning socket, `listfunds`
/// cannot be reached -- and the handler must say so rather than emit the
/// shape it used to. `tlv_sats: 0` is a node worth nothing and
/// `warnings: []` is a node with nothing wrong; both are answers Python
/// emits for real, so neither can stand in for "we could not look".
#[test]
fn revenue_r_dashboard_refuses_when_the_node_cannot_be_reached() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-dashboard",
    );

    assert_eq!(
        result["error"],
        serde_json::json!("dashboard_funds_unavailable"),
        "{result:?}"
    );
    assert!(
        result.get("financial_health").is_none(),
        "must not emit a zeroed net worth: {result:?}"
    );
    assert!(
        result.get("warnings").is_none(),
        "an empty warnings list would read as a healthy node: {result:?}"
    );
    assert!(
        result.get("_phase1b_gaps").is_none(),
        "the gap marker is retired; a refusal is not a gap: {result:?}"
    );
}

/// C71-34: `revenue-r-econ-snapshot` is served, not marked unported.
///
/// With no stores configured, the gate itself is unreadable -- and an
/// unreadable config surface is NOT a disabled shadow. Reporting
/// `enabled: false` here would be a false statement about node state on any
/// node whose operator turned the shadow on, which is exactly what the old
/// `not_yet_ported` marker existed to prevent.
#[test]
fn revenue_r_econ_snapshot_refuses_when_the_config_surface_is_unreadable() {
    let home = tempfile::tempdir().expect("tempdir");
    let result = call_after_init(false, None, home.path(), &[], "revenue-r-econ-snapshot");

    assert_eq!(
        result["error"],
        serde_json::json!("econ_shadow_config_unavailable"),
        "{result:?}"
    );
    assert!(
        result.get("enabled").is_none(),
        "neither a true nor a false enabled claim may be made: {result:?}"
    );
    assert_ne!(
        result["error"],
        serde_json::json!("econ shadow not_yet_ported"),
        "the surface is wired now; an unported marker would hide a real outage"
    );
}

/// The shadow is genuinely OFF (no override row, Python's dataclass
/// default), so Python's exact two-key disabled shape is returned -- not a
/// refusal, and not an assembled snapshot.
#[test]
fn revenue_r_econ_snapshot_reports_pythons_disabled_shape_when_genuinely_off() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-econ-snapshot",
    );

    assert_eq!(result["enabled"], serde_json::json!(false), "{result:?}");
    assert_eq!(
        result["hint"],
        serde_json::json!("revenue-config set econ_shadow_enabled true")
    );
    assert!(
        result.get("snapshot").is_none(),
        "the disabled shape is exactly two keys: {result:?}"
    );
}

/// Enabled, but there is no lightning socket in this harness, so the ONE
/// channel fetch fails. Python reports that as a channel-read failure with
/// a null snapshot rather than fabricating one, and so must this.
#[test]
fn revenue_r_econ_snapshot_reports_a_channel_read_failure_without_fabricating() {
    let home = tempfile::tempdir().expect("tempdir");
    let prod_db_path = copy_fixture_db(home.path());
    {
        let conn = rusqlite::Connection::open(&prod_db_path).expect("open prod db");
        conn.execute(
            "INSERT INTO config_overrides (key, value, version, updated_at) \
             VALUES ('econ_shadow_enabled', 'true', 1, 1800000000)",
            [],
        )
        .expect("enable the shadow");
    }

    let result = call_after_init(
        false,
        Some(prod_db_path.to_str().unwrap()),
        home.path(),
        &[],
        "revenue-r-econ-snapshot",
    );

    assert_eq!(result["enabled"], serde_json::json!(true), "{result:?}");
    assert_eq!(result["snapshot"], serde_json::Value::Null);
    let approximations = result["approximations"].as_array().expect("declared");
    assert!(
        approximations
            .iter()
            .any(|a| a.as_str().unwrap_or("").starts_with("channel read failed")),
        "the failure must be named: {approximations:?}"
    );
}
