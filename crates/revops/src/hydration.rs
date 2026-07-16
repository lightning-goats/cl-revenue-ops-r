//! Startup hydration: page `listforwards` via a hand-rolled unix-socket
//! JSON-RPC client against lightningd's own RPC socket
//! (`{lightning-dir}/{rpc-file}`, from `cln-plugin`'s `Configuration`).
//!
//! **Why hand-rolled instead of the `cln-rpc` crate**: the phase1b plan's
//! Task 2 self-review flags `cln-rpc` as the "obvious" choice but allows a
//! hand-rolled unix-socket client as an explicit fallback if the crate
//! isn't a good fit. It isn't vendored in this workspace's registry cache
//! and this phase needs exactly one read-only RPC method (`listforwards`),
//! so a small dedicated client -- reusing `revops-rpc`'s
//! `call_with_timeout` for the same "RPC timeout after {n}s on {method}"
//! parity string Python's `ThreadSafeRpcProxy` produces
//! (cl-revenue-ops.py:881) -- is the narrower, fully-auditable surface.
//!
//! Mirrors the paging/fallback contract at cl-revenue-ops.py:632-667
//! (`_hydration_fetch_settled_forwards`): page via
//! `listforwards(status="settled", index="created", start=.., limit=1000)`,
//! stop once a page returns fewer than the limit, and treat any RPC error
//! or a full page missing `created_index` (older CLN without index-paging
//! support) as an abort for this boot -- never a silent truncation. Dedup
//! at the DB layer (`revops_db::notifications::insert_forward_ignore_dup`)
//! remains the correctness backstop, so a partial/aborted hydration is
//! safe, never wrong.

use crate::notify::forward_row_from_json;
use anyhow::{bail, Context, Result};
use revops_db::notifications::compute_forward_hydration_start;
use revops_db::owner::ObserverHandle;
use serde_json::{json, Value};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// Matches Python's `_HYDRATION_PAGE_LIMIT` (cl-revenue-ops.py:628).
const HYDRATION_PAGE_LIMIT: i64 = 1000;
/// Matches Python's `_HYDRATION_MAX_PAGES` (cl-revenue-ops.py:629): hard
/// stop at 10M forwards, prevents runaway paging.
const HYDRATION_MAX_PAGES: usize = 10_000;
/// Matches `Config.rpc_timeout_seconds`'s default (modules/config.py:734);
/// Phase 1b has no live-config wiring into this module yet (that is Task
/// 3's typed-config surface, read-only), so this is a documented constant
/// rather than a silent hardcode of an arbitrary number.
const RPC_TIMEOUT_SECONDS: u64 = 15;

/// Fetch every settled forward newer than `start_time` by paging
/// `listforwards` against the unix socket at `socket_path`. Returns the
/// raw per-forward JSON objects (as lightningd reports them); the caller
/// extracts the handful of fields the observer's own schema needs.
pub async fn fetch_settled_forwards(socket_path: &Path, start_time: i64) -> Result<Vec<Value>> {
    let mut collected = Vec::new();
    let mut next_start: i64 = 0;
    for _ in 0..HYDRATION_MAX_PAGES {
        let page = revops_rpc::call_with_timeout(
            "listforwards",
            RPC_TIMEOUT_SECONDS,
            call_listforwards(socket_path, next_start, HYDRATION_PAGE_LIMIT),
        )
        .await
        .map_err(anyhow::Error::from)?;

        let forwards = page
            .get("forwards")
            .and_then(|f| f.as_array())
            .context("listforwards response missing 'forwards' array")?;

        for fwd in forwards {
            let rt = fwd
                .get("received_time")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if rt > start_time {
                collected.push(fwd.clone());
            }
        }

        if (forwards.len() as i64) < HYDRATION_PAGE_LIMIT {
            return Ok(collected);
        }

        // Advance past the last created_index. A full page missing this
        // field means this CLN doesn't support index paging -- surfaced as
        // an error (never a silent "treat as last page"), matching
        // Python's KeyError-triggers-fallback semantics translated to
        // Phase 1b's "abort this boot's hydration" contract.
        let last_created_index = forwards
            .last()
            .and_then(|f| f.get("created_index"))
            .and_then(|v| v.as_i64())
            .context(
                "listforwards paging requires 'created_index' on a full page \
                 (unsupported CLN version, or index paging disabled)",
            )?;
        let advanced = last_created_index + 1;
        if advanced <= next_start {
            bail!("listforwards paging did not advance (created_index non-increasing)");
        }
        next_start = advanced;
    }
    bail!("listforwards paging exceeded max page count ({HYDRATION_MAX_PAGES})")
}

/// One `listforwards` call: connect fresh, write the request, read back
/// exactly one JSON-RPC response. A fresh connection per page keeps the
/// client trivially sequential (no request-id multiplexing needed) --
/// lightningd's RPC socket accepts unlimited concurrent connections, so
/// this has no meaningful cost over reusing one.
async fn call_listforwards(socket_path: &Path, start: i64, limit: i64) -> Result<Value> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("connect lightning-rpc socket {}", socket_path.display()))?;

    let request = json!({
        "jsonrpc": "2.0",
        "id": "revops-r-hydration",
        "method": "listforwards",
        "params": {"status": "settled", "index": "created", "start": start, "limit": limit},
    });
    let mut body = serde_json::to_vec(&request)?;
    body.push(b'\n');
    stream
        .write_all(&body)
        .await
        .context("write listforwards request")?;
    stream.flush().await.context("flush listforwards request")?;

    // lightningd's RPC socket streams back a JSON document with no
    // length-prefix framing. Since exactly one request is ever in flight
    // per connection here, reading until the accumulated bytes parse as a
    // single complete JSON value is sufficient and avoids depending on any
    // particular delimiter convention.
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stream
            .read(&mut chunk)
            .await
            .context("read listforwards response")?;
        if n == 0 {
            bail!("lightning-rpc socket closed before a complete response was read");
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Ok(value) = serde_json::from_slice::<Value>(&buf) {
            if let Some(err) = value.get("error") {
                bail!("listforwards RPC error: {err}");
            }
            return value
                .get("result")
                .cloned()
                .context("listforwards response missing 'result'");
        }
        // Not a complete JSON document yet -- keep reading.
    }
}

/// Run once at startup: compute a bounded backfill window via
/// `compute_forward_hydration_start`, page settled forwards newer than
/// that start time via [`fetch_settled_forwards`], and dedup-insert each
/// one into the observer's own db.
///
/// Infallible from the caller's perspective (mirrors every other handler
/// in this crate's notification-ingestion surface): any error -- RPC
/// unavailable, socket missing, a malformed page -- is logged to stderr
/// and this simply returns the count of forwards it managed to ingest
/// before stopping (`0` on total failure). Dedup at the DB layer
/// (`insert_forward_ignore_dup`'s `INSERT OR IGNORE`) is the correctness
/// backstop, so a partial or zero-count hydration is always safe, never
/// wrong -- matching the fallback contract at cl-revenue-ops.py:632-667 /
/// 2793-2848.
///
/// `now` is threaded through explicitly (rather than read via
/// `SystemTime::now()` internally) so this stays deterministically
/// testable, matching `compute_forward_hydration_start`'s own signature.
pub async fn run_startup_hydration(
    observer: &ObserverHandle,
    rpc_socket_path: &Path,
    flow_window_days: i64,
    now: i64,
) -> usize {
    let last = match observer.last_forward_ts().await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("revops: hydration aborted, could not read last_forward_ts: {e}");
            return 0;
        }
    };

    let Some(start) = compute_forward_hydration_start(last, flow_window_days, now) else {
        // Table already has recent-enough data (within the jitter window)
        // -- the live forward_event stream covers this restart, no RPC
        // call needed at all.
        return 0;
    };

    let forwards = match fetch_settled_forwards(rpc_socket_path, start).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("revops: startup hydration unavailable ({e}); continuing without backfill");
            return 0;
        }
    };

    let mut inserted = 0usize;
    for fwd in &forwards {
        let row = forward_row_from_json(fwd);
        match observer.insert_forward(row).await {
            Ok(true) => inserted += 1,
            Ok(false) => {} // exact-duplicate dedup no-op, not an error
            Err(e) => eprintln!("revops: hydration insert failed: {e}"),
        }
    }
    inserted
}
