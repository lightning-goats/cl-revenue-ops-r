//! Integration tests for `revops::config_resolve::fetch_python_option_values`
//! -- the `cln_rpc::ClnRpc`-backed `listconfigs` client that caches layer
//! (b) of `revenue-r-config`'s resolution order at init. Follows
//! `crates/revops/tests/hydration.rs`'s mock `lightning-rpc` unix-socket
//! pattern (a bare `tokio::net::UnixListener` replying with `cln_rpc`'s own
//! `\n\n`-delimited framing) rather than requiring a real lightningd.
//!
//! Pure-parsing coverage (`parse_listconfigs_response`, `extract_value`,
//! `resolve_option_value`, `python_option_name`, `db_override_key`) lives in
//! `config_resolve.rs`'s own inline `#[cfg(test)]` module -- these tests
//! only cover the socket round trip `fetch_python_option_values` adds on
//! top of that pure parsing.

use revops::config_resolve::fetch_python_option_values;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

fn serve_once(socket_path: std::path::PathBuf, result: Value) {
    let listener = UnixListener::bind(&socket_path).expect("bind mock rpc socket");
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = stream.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                return;
            }
            buf.extend_from_slice(&chunk[..n]);
            if serde_json::from_slice::<Value>(&buf).is_ok() {
                break;
            }
        }
        let body = json!({"jsonrpc": "2.0", "id": "revops-r-config-resolve", "result": result});
        let mut out = serde_json::to_vec(&body).unwrap();
        // `cln_rpc`'s `MultiLineCodec` splits on `\n\n` -- see
        // `hydration.rs`'s mock server for the same note.
        out.extend_from_slice(b"\n\n");
        let _ = stream.write_all(&out).await;
    });
}

fn socket_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("lightning-rpc")
}

#[tokio::test]
async fn fetches_and_filters_to_revenue_ops_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = socket_path(&dir);
    serve_once(
        path.clone(),
        json!({
            "configs": {
                "revenue-ops-min-fee-ppm": {"value_str": "40", "source": "/config:35"},
                "revenue-ops-daily-budget-sats": {"value_str": "1000", "source": "/config:47"},
                "bind-addr": {"value_str": "127.0.0.1", "source": "default"},
            }
        }),
    );

    let map = fetch_python_option_values(&path).await;
    assert_eq!(map.len(), 2);
    assert!(map.contains_key("revenue-ops-min-fee-ppm"));
    assert!(map.contains_key("revenue-ops-daily-budget-sats"));
    assert!(!map.contains_key("bind-addr"));
}

/// RPC error response -> empty map (fails open), never a panic -- degrades
/// to fixture-default-only resolution.
#[tokio::test]
async fn rpc_error_response_yields_empty_map() {
    let dir = tempfile::tempdir().unwrap();
    let path = socket_path(&dir);
    let listener = UnixListener::bind(&path).unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
            if serde_json::from_slice::<Value>(&buf).is_ok() {
                break;
            }
        }
        let body = json!({
            "jsonrpc": "2.0", "id": "revops-r-config-resolve",
            "error": {"code": -32601, "message": "Unknown command 'listconfigs'"}
        });
        let mut out = serde_json::to_vec(&body).unwrap();
        out.extend_from_slice(b"\n\n");
        stream.write_all(&out).await.unwrap();
    });

    let map = fetch_python_option_values(&path).await;
    assert!(map.is_empty());
}

/// No lightningd socket at all (e.g. very early in startup, or a bad
/// derived path) -> empty map, never a panic or a blocked init.
#[tokio::test]
async fn missing_socket_yields_empty_map() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("does-not-exist");
    let map = fetch_python_option_values(&path).await;
    assert!(map.is_empty());
}
