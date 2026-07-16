use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

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
    // +2 for our own revops-r-observer and revops-r-observer-db-path (Task
    // 2's own writable notification-ingestion db -- no Python analog).
    assert_eq!(shadow.len(), expected + 2, "shadow options registered");
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

    let canonical: Vec<&&str> = opt_names
        .iter()
        .filter(|n| n.starts_with("revenue-ops-"))
        .collect();
    // +2 for our own revenue-ops-observer and revenue-ops-observer-db-path
    // (revenue-ops-db-path is registered exactly once, under the fixture's
    // own canonical name -- see register_python_options' doc comment on
    // the db-path skip).
    assert_eq!(
        canonical.len(),
        expected + 2,
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
    // Exactly 6 rpc methods total (no leftover revenue-r-* names bleeding
    // through from shadow mode) -- ping/status/config (Phase 1a) plus
    // Phase 1b Task 5's history/report/dashboard read-RPC subset.
    assert_eq!(
        result["rpcmethods"].as_array().unwrap().len(),
        6,
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
