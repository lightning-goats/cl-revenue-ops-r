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

/// CRITICAL 2 end-to-end: a DB override row seeded under the Python
/// `Config` FIELD name (`enable_vegas_reflex`) is found via
/// `db_override_key`'s remap of the `revenue-r-config` OPTION-SUFFIX key
/// (`vegas-reflex`) -- exactly the query `main.rs`'s `revenue-r-config`
/// handler runs. Before CRITICAL 2's fix, `db_override_key("vegas-reflex")`
/// naively produced `"vegas_reflex"`, which never matches this row, so the
/// override was silently invisible to layer (a).
#[tokio::test]
async fn db_override_key_resolves_seeded_override_for_a_renamed_field() {
    use revops::config_resolve::db_override_key;
    use revops_db::queries::config_override;

    let fixture_db =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fixture.db");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seeded.db");
    std::fs::copy(&fixture_db, &path).unwrap();
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO config_overrides (key, value, version, updated_at) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["enable_vegas_reflex", "true", 1i64, 1_800_000_000i64],
        )
        .unwrap();
    }
    let handle = revops_db::actor::spawn_read_only(&path).await.unwrap();

    let db_key = db_override_key("vegas-reflex");
    assert_eq!(db_key, "enable_vegas_reflex");
    assert_eq!(
        config_override(&handle, &db_key).await.unwrap(),
        Some("true".to_string())
    );
}

// ---------------------------------------------------------------------------
// PythonOptionCache (2026-07-22 audit M3): the listconfigs snapshot must be
// refreshable (Python re-reads listconfigs each boltz/planner cycle via
// _refresh_dynamic_config, cl-revenue-ops.py:6597-6685, so setconfig on a
// dynamic option takes effect without a restart) and a failed refresh must
// keep the last good snapshot rather than blanking it.
// ---------------------------------------------------------------------------

mod python_option_cache {
    use cln_plugin::options::Value;
    use revops::config_resolve::PythonOptionCache;
    use std::collections::HashMap;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
            .collect()
    }

    #[test]
    fn starts_empty() {
        let cache = PythonOptionCache::empty();
        assert!(cache.snapshot().is_empty());
    }

    #[test]
    fn ok_fetch_replaces_snapshot() {
        let cache = PythonOptionCache::empty();
        assert!(cache.apply_fetch(Ok(map(&[("revenue-ops-min-fee-ppm", "50")]))));
        assert!(cache.apply_fetch(Ok(map(&[("revenue-ops-min-fee-ppm", "60")]))));
        let snap = cache.snapshot();
        assert!(
            matches!(snap.get("revenue-ops-min-fee-ppm"), Some(Value::String(s)) if s == "60"),
            "snapshot must hold the latest fetched value"
        );
        assert_eq!(snap.len(), 1);
    }

    #[test]
    fn failed_fetch_keeps_previous_snapshot() {
        let cache = PythonOptionCache::empty();
        assert!(cache.apply_fetch(Ok(map(&[("revenue-ops-min-fee-ppm", "50")]))));
        assert!(!cache.apply_fetch(Err("socket gone".to_string())));
        let snap = cache.snapshot();
        assert!(
            matches!(snap.get("revenue-ops-min-fee-ppm"), Some(Value::String(s)) if s == "50"),
            "a failed refresh must keep the last good snapshot"
        );
    }
}
