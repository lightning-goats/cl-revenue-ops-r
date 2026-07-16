use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

/// Speak the first half of the CLN plugin handshake to the compiled binary
/// and assert the manifest advertises our option and RPC method.
#[test]
fn manifest_advertises_shadow_names() {
    let bin = env!("CARGO_BIN_EXE_revops");
    let mut child = Command::new(bin)
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
    let result = &resp["result"];
    let opts: Vec<&str> = result["options"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["name"].as_str().unwrap())
        .collect();
    assert!(opts.contains(&"revops-r-observer"), "options: {opts:?}");
    let methods: Vec<&str> = result["rpcmethods"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert!(methods.contains(&"revenue-r-ping"), "methods: {methods:?}");
}
