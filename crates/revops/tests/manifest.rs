use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

use revops_db::fee_runway::{record_seed_event, FeeSeedEventRow};

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
/// opt-in changes nothing), advertised `dynamic` so `setconfig` can flip
/// it at runtime.
#[test]
fn manifest_fee_dryrun_is_bool_default_false_dynamic() {
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
    assert_eq!(opt["dynamic"], serde_json::json!(true), "opt: {opt}");
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
    // Exactly 9 rpc methods total (no leftover revenue-r-* names bleeding
    // through from shadow mode) -- ping/status/config (Phase 1a), Phase 1b
    // Task 5's history/report/dashboard read-RPC subset, Phase 4b Task 7's
    // fee-debug/fee-wake, plus Task 10's runway status RPC.
    assert_eq!(
        result["rpcmethods"].as_array().unwrap().len(),
        9,
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
        record_seed_event(
            &conn,
            &FeeSeedEventRow {
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
            },
        )
        .expect("seed the observer db with a seed-provenance event");
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
