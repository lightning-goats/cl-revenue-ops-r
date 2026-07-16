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
    // +1 for our own revops-r-observer
    assert_eq!(shadow.len(), expected + 1, "shadow options registered");
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

    let canonical: Vec<&&str> = opt_names
        .iter()
        .filter(|n| n.starts_with("revenue-ops-"))
        .collect();
    // +1 for our own revenue-ops-observer (revenue-ops-db-path is registered
    // exactly once, under the fixture's own canonical name -- see
    // register_python_options' doc comment on the db-path skip).
    assert_eq!(
        canonical.len(),
        expected + 1,
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
    // Exactly 3 rpc methods total (no leftover revenue-r-* names bleeding
    // through from shadow mode).
    assert_eq!(
        result["rpcmethods"].as_array().unwrap().len(),
        3,
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
