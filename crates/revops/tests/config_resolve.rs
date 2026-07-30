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

    // F71-R27 / C71-6: keeping the last good snapshot is only half the
    // contract. `snapshot()` alone cannot distinguish "lightningd holds no
    // `revenue-ops-*` options" from "we have never successfully asked" —
    // both are an empty map. A consumer that resolves defaults out of the
    // second case is fabricating a default from a source it never read.
    // The freshness of the snapshot must therefore be observable.

    #[test]
    fn a_never_refreshed_cache_is_distinguishable_from_a_successful_empty_fetch() {
        use revops::config_resolve::SnapshotFreshness;

        let never = PythonOptionCache::empty();
        assert_eq!(
            never.freshness(),
            SnapshotFreshness::NeverRefreshed,
            "an untouched cache must not claim lightningd reported nothing"
        );

        // lightningd answered, and the answer was genuinely "no
        // revenue-ops-* options set". Same empty map, different meaning.
        let asked = PythonOptionCache::empty();
        assert!(asked.apply_fetch(Ok(HashMap::new())));
        assert_eq!(asked.freshness(), SnapshotFreshness::Fresh);
        assert!(
            asked.snapshot().is_empty() && never.snapshot().is_empty(),
            "the two cases are indistinguishable by snapshot alone -- which is \
             exactly why freshness has to be asked separately"
        );
    }

    #[test]
    fn a_failed_refresh_reports_last_good_and_counts_consecutive_failures() {
        use revops::config_resolve::SnapshotFreshness;

        let cache = PythonOptionCache::empty();
        assert!(cache.apply_fetch(Ok(map(&[("revenue-ops-min-fee-ppm", "50")]))));
        assert_eq!(cache.freshness(), SnapshotFreshness::Fresh);

        assert!(!cache.apply_fetch(Err("socket gone".to_string())));
        assert_eq!(
            cache.freshness(),
            SnapshotFreshness::LastGood {
                consecutive_failures: 1
            }
        );
        assert!(!cache.apply_fetch(Err("still gone".to_string())));
        assert_eq!(
            cache.freshness(),
            SnapshotFreshness::LastGood {
                consecutive_failures: 2
            },
            "a lengthening outage must be visible, not a fixed flag"
        );

        // Healing resets the count: a stale-forever reading would make the
        // signal useless for deciding when an outage stopped being benign.
        assert!(cache.apply_fetch(Ok(map(&[("revenue-ops-min-fee-ppm", "70")]))));
        assert_eq!(cache.freshness(), SnapshotFreshness::Fresh);
    }

    /// F71-R29: the paired accessor must agree with the individual ones in
    /// every state. A pair that disagrees with `snapshot()`/`freshness()`
    /// would make the atomic path and the reporting path tell two
    /// different stories about the same cache.
    #[test]
    fn the_paired_accessor_agrees_with_the_individual_accessors_in_every_state() {
        use revops::config_resolve::SnapshotFreshness;

        let cache = PythonOptionCache::empty();
        for expected in [
            SnapshotFreshness::NeverRefreshed,
            SnapshotFreshness::Fresh,
            SnapshotFreshness::LastGood {
                consecutive_failures: 1,
            },
        ] {
            // Drive the cache into `expected`.
            match expected {
                SnapshotFreshness::NeverRefreshed => {}
                SnapshotFreshness::Fresh => {
                    cache.apply_fetch(Ok(map(&[("revenue-ops-min-fee-ppm", "50")])));
                }
                SnapshotFreshness::LastGood { .. } => {
                    cache.apply_fetch(Err("socket gone".to_string()));
                }
            }
            let (paired_values, paired_freshness) = cache.snapshot_with_freshness();
            assert_eq!(paired_freshness, expected);
            assert_eq!(paired_freshness, cache.freshness());
            assert_eq!(paired_values.len(), cache.snapshot().len());
        }
    }

    /// F71-R29, the race itself. A concurrent refresh must never be able to
    /// hand a reader an EMPTY map labelled `Fresh` — that pair reads as
    /// "lightningd holds no revenue-ops options" and makes a consumer
    /// resolve defaults from a snapshot it never saw.
    ///
    /// This is opportunistic, not exhaustive: it can only ever FAIL on a
    /// torn read, never on a correct one, so it is safe to keep, but it
    /// does not by itself prove atomicity. The guarantee rests on
    /// `snapshot_with_freshness` taking a single lock; see the note in the
    /// R29 commit about what this does and does not pin.
    #[test]
    fn a_concurrent_refresh_never_yields_an_empty_snapshot_labelled_fresh() {
        use revops::config_resolve::SnapshotFreshness;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        for _ in 0..200 {
            let cache = PythonOptionCache::empty();
            let stop = Arc::new(AtomicBool::new(false));

            let writer = {
                let cache = cache.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    cache.apply_fetch(Ok(map(&[("revenue-ops-min-fee-ppm", "50")])));
                    stop.store(true, Ordering::Release);
                })
            };

            while !stop.load(Ordering::Acquire) {
                let (values, freshness) = cache.snapshot_with_freshness();
                if freshness == SnapshotFreshness::Fresh {
                    assert!(
                        !values.is_empty(),
                        "a `Fresh` label was paired with values from before the fetch"
                    );
                }
            }
            writer.join().unwrap();
        }
    }

    /// A refresh that fails BEFORE any success stays `NeverRefreshed`.
    /// Reporting `LastGood` here would name a good snapshot that does not
    /// exist — the empty map it would be describing is not "last good", it
    /// is "never read".
    #[test]
    fn a_failure_before_any_success_is_still_never_refreshed() {
        use revops::config_resolve::SnapshotFreshness;

        let cache = PythonOptionCache::empty();
        assert!(!cache.apply_fetch(Err("socket gone".to_string())));
        assert_eq!(cache.freshness(), SnapshotFreshness::NeverRefreshed);
    }
}

// ---------------------------------------------------------------------------
// Audit low #10b (2026-07-22): Python's `int()`/`float()` accept
// leading/trailing whitespace, so a whitespace-padded `config_overrides`
// row is APPLIED by Python (`_apply_override`'s typed_value = int(raw))
// but was silently discarded by the Rust validator (strict str::parse),
// falling through to layer (b)/(c) — a divergent effective config.
// ---------------------------------------------------------------------------

mod whitespace_padded_overrides {
    use revops::config_resolve::validate_override;

    #[test]
    fn padded_int_override_accepted_like_python_int() {
        assert_eq!(
            validate_override("daily_budget_sats", " 5000 "),
            Some("5000".to_string()),
            "Python int(' 5000 ') == 5000: the override applies"
        );
    }

    #[test]
    fn padded_float_override_accepted_like_python_float() {
        assert_eq!(
            validate_override("htlc_congestion_threshold", "\t0.5\n"),
            Some("0.5".to_string()),
            "Python float('\\t0.5\\n') == 0.5: the override applies"
        );
    }

    #[test]
    fn genuinely_unparseable_still_rejected() {
        assert_eq!(validate_override("daily_budget_sats", " 5 000 "), None);
        assert_eq!(validate_override("daily_budget_sats", ""), None);
    }
}
