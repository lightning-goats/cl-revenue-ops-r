//! Tests for `revops::hydration::fetch_settled_forwards` -- the
//! `cln_rpc::ClnRpc`-backed client used for startup hydration's paged
//! `listforwards` call. Mirrors the paging/fallback contract at
//! cl-revenue-ops.py:632-667 (`_hydration_fetch_settled_forwards`): pages
//! via `index="created"`, stops when a page returns fewer than the page
//! limit, and errors out (never silently truncates) if a full page is
//! missing `created_index` or the RPC itself errors.
//!
//! These tests stand up a tiny mock `lightning-rpc` unix-socket server (a
//! bare `tokio::net::UnixListener`) rather than requiring a real
//! lightningd -- lnnode is the only real node available (design spec
//! constraint), so parity here is pinned at the wire-protocol level. The
//! mock replies using the real `\n\n`-delimited framing `cln_rpc`'s own
//! `MultiLineCodec` expects (see `cln_rpc::codec`) -- a prior hand-rolled
//! client here used a single `\n`, which diverged from what both
//! `cln-rpc` and `cln-plugin` actually speak on the wire and would have
//! hung every real hydration call until the timeout (see the phase1b
//! task-2 report's Fix Round 1 correction).

use revops::hydration::{fetch_settled_forwards, run_startup_hydration};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

/// Serve exactly `responses.len()` connections, replying with each
/// `result` value in order (one per accepted connection, matching our
/// client's "one fresh connection per page" behavior).
fn serve(socket_path: std::path::PathBuf, responses: Vec<Value>) {
    let listener = UnixListener::bind(&socket_path).expect("bind mock rpc socket");
    let responses = Arc::new(Mutex::new(responses.into_iter()));
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let responses = responses.clone();
            tokio::spawn(async move {
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
                let next = responses.lock().unwrap().next();
                let body = match next {
                    Some(result) => {
                        json!({"jsonrpc": "2.0", "id": "revops-r-hydration", "result": result})
                    }
                    None => json!({
                        "jsonrpc": "2.0",
                        "id": "revops-r-hydration",
                        "error": {"code": -1, "message": "mock server exhausted"}
                    }),
                };
                let mut out = serde_json::to_vec(&body).unwrap();
                // `cln_rpc`'s `MultiLineCodec` splits on a `\n\n`
                // delimiter (see `cln_rpc::codec`) -- a single trailing
                // `\n` here would never be recognized as a complete
                // message and every call would hang to the client's
                // timeout.
                out.extend_from_slice(b"\n\n");
                let _ = stream.write_all(&out).await;
            });
        }
    });
}

fn socket_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path().join("lightning-rpc")
}

#[tokio::test]
async fn single_short_page_filters_by_start_time() {
    let dir = tempfile::tempdir().unwrap();
    let path = socket_path(&dir);
    serve(
        path.clone(),
        vec![json!({
            "forwards": [
                {"received_time": 1_800_000_100, "created_index": 5},
                {"received_time": 1_800_000_050, "created_index": 6},
            ]
        })],
    );

    let collected = fetch_settled_forwards(&path, 1_800_000_075).await.unwrap();
    assert_eq!(collected.len(), 1, "only the entry newer than start_time");
    assert_eq!(collected[0]["received_time"].as_i64(), Some(1_800_000_100));
}

/// Regression for CRITICAL 1: real CLN `listforwards` entries carry
/// `received_time` as a FLOAT (decimal seconds, e.g. `1560696342.368`),
/// not an integer. Before the fix, the page filter's `.as_i64()` returned
/// `None` for every such row, defaulted to `rt = 0`, and `0 > start_time`
/// was always false for any positive `start_time` -- so hydration silently
/// backfilled nothing against real production data despite the mock tests
/// (which use plain integer literals) passing throughout.
#[tokio::test]
async fn float_received_time_is_compared_correctly_against_start_time() {
    let dir = tempfile::tempdir().unwrap();
    let path = socket_path(&dir);
    serve(
        path.clone(),
        vec![json!({
            "forwards": [
                {"received_time": 1560696342.368, "created_index": 5},
                {"received_time": 1560696300.5, "created_index": 6},
            ]
        })],
    );

    let collected = fetch_settled_forwards(&path, 1_560_696_320).await.unwrap();
    assert_eq!(
        collected.len(),
        1,
        "only the float-timestamped row newer than start_time survives the filter"
    );
    assert_eq!(collected[0]["received_time"].as_f64(), Some(1560696342.368));
}

#[tokio::test]
async fn empty_page_returns_empty_collection() {
    let dir = tempfile::tempdir().unwrap();
    let path = socket_path(&dir);
    serve(path.clone(), vec![json!({"forwards": []})]);

    let collected = fetch_settled_forwards(&path, 0).await.unwrap();
    assert!(collected.is_empty());
}

#[tokio::test]
async fn pages_until_a_short_page_and_advances_start_by_created_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = socket_path(&dir);

    // Page 1: exactly PAGE_LIMIT (1000) entries -> triggers another page.
    let page1: Vec<Value> = (0..1000)
        .map(|i| json!({"received_time": 1_800_000_000 + i, "created_index": i}))
        .collect();
    // Page 2: short (2 entries) -> stops paging here.
    let page2 = vec![
        json!({"received_time": 1_800_001_500, "created_index": 1000}),
        json!({"received_time": 1_800_001_600, "created_index": 1001}),
    ];

    serve(
        path.clone(),
        vec![json!({"forwards": page1}), json!({"forwards": page2})],
    );

    let collected = fetch_settled_forwards(&path, 0).await.unwrap();
    assert_eq!(collected.len(), 1002, "both pages collected in full");
}

#[tokio::test]
async fn full_page_missing_created_index_is_an_error_not_silent_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let path = socket_path(&dir);
    // A full page (>= PAGE_LIMIT) with no created_index on the last entry
    // looks like an older CLN without index-paging support -- must error,
    // never silently stop as if this were the last page.
    let page: Vec<Value> = (0..1000)
        .map(|i| json!({"received_time": 1_800_000_000 + i}))
        .collect();
    serve(path.clone(), vec![json!({"forwards": page})]);

    let err = fetch_settled_forwards(&path, 0).await.unwrap_err();
    assert!(
        err.to_string().contains("created_index"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn rpc_error_response_propagates_as_err() {
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
            "jsonrpc": "2.0", "id": "revops-r-hydration",
            "error": {"code": -32601, "message": "Unknown command 'listforwards'"}
        });
        let mut out = serde_json::to_vec(&body).unwrap();
        out.extend_from_slice(b"\n\n");
        stream.write_all(&out).await.unwrap();
    });

    let err = fetch_settled_forwards(&path, 0).await.unwrap_err();
    assert!(
        err.to_string().contains("listforwards"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn missing_socket_is_a_clean_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("does-not-exist");
    assert!(fetch_settled_forwards(&path, 0).await.is_err());
}

#[tokio::test]
async fn run_startup_hydration_inserts_settled_forwards_on_empty_table() {
    let dir = tempfile::tempdir().unwrap();
    let socket = socket_path(&dir);
    // Empty observer db -> compute_forward_hydration_start returns a full
    // warm-start window, so hydration always fetches on this path.
    serve(
        socket.clone(),
        vec![json!({"forwards": [
            {"in_channel": "1x1x0", "out_channel": "2x2x0", "in_msat": 100_000,
             "out_msat": 99_000, "fee_msat": 1_000,
             "received_time": 1_800_000_000, "resolved_time": 1_800_000_005},
        ]})],
    );

    let observer = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
        .await
        .unwrap();
    let inserted = run_startup_hydration(&observer, &socket, 7, 1_800_000_100).await;
    assert_eq!(inserted, 1);
    assert_eq!(
        observer.last_forward_ts().await.unwrap(),
        Some(1_800_000_000)
    );
}

/// End-to-end CRITICAL 1 regression: a float-timestamped forward from a
/// real `listforwards` page must be ingested with its truncated integer
/// timestamp, not silently dropped/zeroed by the paging filter or the
/// `ForwardRow` extraction.
#[tokio::test]
async fn run_startup_hydration_handles_float_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let socket = socket_path(&dir);
    serve(
        socket.clone(),
        vec![json!({"forwards": [
            {"in_channel": "1x1x0", "out_channel": "2x2x0", "in_msat": 100_000,
             "out_msat": 99_000, "fee_msat": 1_000,
             "received_time": 1_800_000_000.5, "resolved_time": 1_800_000_005.9},
        ]})],
    );

    let observer = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
        .await
        .unwrap();
    let inserted = run_startup_hydration(&observer, &socket, 7, 1_800_000_100).await;
    assert_eq!(inserted, 1);
    assert_eq!(
        observer.last_forward_ts().await.unwrap(),
        Some(1_800_000_000),
        "float received_time must truncate to a real timestamp, never 0"
    );
}

#[tokio::test]
async fn run_startup_hydration_dedups_against_existing_row() {
    let dir = tempfile::tempdir().unwrap();
    let socket = socket_path(&dir);
    serve(
        socket.clone(),
        vec![json!({"forwards": [
            {"in_channel": "1x1x0", "out_channel": "2x2x0", "in_msat": 100_000,
             "out_msat": 99_000, "fee_msat": 1_000,
             "received_time": 1_800_000_000, "resolved_time": 1_800_000_005},
        ]})],
    );

    let observer = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
        .await
        .unwrap();
    observer
        .insert_forward(revops_db::notifications::ForwardRow {
            in_channel: "1x1x0".into(),
            out_channel: "2x2x0".into(),
            in_msat: 100_000,
            out_msat: 99_000,
            fee_msat: 1_000,
            timestamp: 1_800_000_000,
            resolved_time: 1_800_000_005,
        })
        .await
        .unwrap();

    let inserted = run_startup_hydration(&observer, &socket, 7, 1_800_000_100).await;
    assert_eq!(
        inserted, 0,
        "already-present forward is deduped, not double-counted"
    );
}

#[tokio::test]
async fn run_startup_hydration_returns_zero_when_recent_enough() {
    let dir = tempfile::tempdir().unwrap();
    let socket = socket_path(&dir); // never bound -- must not be dialed at all
    let observer = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
        .await
        .unwrap();
    observer
        .insert_forward(revops_db::notifications::ForwardRow {
            in_channel: "1x1x0".into(),
            out_channel: "2x2x0".into(),
            in_msat: 1,
            out_msat: 1,
            fee_msat: 0,
            timestamp: 1_800_000_000,
            resolved_time: 1_800_000_000,
        })
        .await
        .unwrap();

    // now - last_forward_ts = 50s, well within the 300s jitter window ->
    // compute_forward_hydration_start returns None -> no RPC call at all.
    let inserted = run_startup_hydration(&observer, &socket, 7, 1_800_000_050).await;
    assert_eq!(inserted, 0);
}

#[tokio::test]
async fn run_startup_hydration_is_safe_when_rpc_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("does-not-exist"); // nothing listening
    let observer = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
        .await
        .unwrap();
    let inserted = run_startup_hydration(&observer, &socket, 7, 1_800_000_000).await;
    assert_eq!(inserted, 0, "RPC failure aborts hydration, never panics");
}
