//! Subscription handlers for the Rust plugin's own dedup-insert ingestion
//! of CLN notifications, mirroring the dedup half of Python's
//! `on_forward_event` / `on_peer_connect` / `on_peer_disconnect` /
//! `on_channel_state_changed` (cl-revenue-ops.py:6661-7000ish) into the
//! observer's own db (`revops_db::owner::ObserverHandle` -- never the
//! production DB).
//!
//! Each handler is infallible from the caller's point of view: a parse or
//! DB failure is logged and swallowed, never propagated -- mirroring
//! Python's per-handler top-level `try/except` (e.g.
//! `on_forward_event`/`_on_forward_event_impl`, cl-revenue-ops.py:6661-6677)
//! that exists specifically so one bad notification can never crash CLN's
//! event dispatch.
//!
//! **Explicit scope boundary** (see the phase1b plan's Task 2 self-review
//! note): `on_forward_event` here ports ONLY the dedup-insert concern
//! (cl-revenue-ops.py:6757-6797, `record_forward` /
//! `record_forward_and_reputation`). It deliberately does NOT port the
//! failed-forward fee-controller DTS nudge (lines 6704-6755), which
//! mutates fee-controller state under a cycle-spanning lock shared with
//! the (unported) fee-adjustment cycle -- that is Phase 4 scope, once a
//! real fee controller exists to hold the lock correctly. It also does not
//! port peer-reputation tracking (`update_peer_reputation`) -- also
//! unported state, also out of this task's dedup-insert scope.

use revops_core::msat::parse_msat;
use revops_db::notifications::ForwardRow;
use revops_db::owner::ObserverHandle;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

/// Mirrors `modules/utils.py::normalize_scid`: CLN APIs return SCIDs with
/// either `x` or `:` separators depending on context/version; the
/// plugin's internal convention is `x`. `pub(crate)` so `hydration.rs` can
/// reuse the same normalization when building `ForwardRow`s from
/// `listforwards` entries -- keeping the dedup key's SCID format
/// consistent between the live `forward_event` path and startup
/// hydration.
pub(crate) fn normalize_scid(scid: &str) -> String {
    scid.replace(':', "x")
}

pub(crate) fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `forward_event` (cl-revenue-ops.py:6661): dedup-insert of settled
/// forwards only (see module-level scope-boundary note). Non-`settled`
/// statuses (`failed`, `local_failed`) are a no-op here.
pub async fn on_forward_event(handle: &ObserverHandle, event: &Value) {
    if let Err(e) = try_on_forward_event(handle, event).await {
        eprintln!("revops: error in forward_event handler: {e}");
    }
}

async fn try_on_forward_event(handle: &ObserverHandle, event: &Value) -> anyhow::Result<()> {
    // The notification payload may arrive nested under a "forward_event"
    // key (CLN's own envelope) or flat -- accept both.
    let event = event.get("forward_event").unwrap_or(event);
    let status = event.get("status").and_then(|v| v.as_str()).unwrap_or("");
    if status != "settled" {
        return Ok(());
    }
    handle.insert_forward(forward_row_from_json(event)).await?;
    Ok(())
}

/// Extract a [`ForwardRow`] out of either a `forward_event` notification
/// payload or a raw `listforwards` entry -- both shapes carry the same
/// field names (`in_channel`/`out_channel`/`*_msat`/`*_msatoshi`/
/// `received_time`/`resolved_time`), so startup hydration (`hydration.rs`)
/// and the live notification handler share this one parser. `pub(crate)`
/// for that cross-module reuse.
pub(crate) fn forward_row_from_json(event: &Value) -> ForwardRow {
    let in_channel = event
        .get("in_channel")
        .and_then(|v| v.as_str())
        .map(normalize_scid)
        .unwrap_or_default();
    let out_channel = event
        .get("out_channel")
        .and_then(|v| v.as_str())
        .map(normalize_scid)
        .unwrap_or_default();
    // CLN v23.05+ uses in_msat/out_msat/fee_msat; older versions used
    // *_msatoshi (cl-revenue-ops.py:6762-6765 / 2834-2836).
    let in_msat = parse_msat(
        event
            .get("in_msat")
            .or_else(|| event.get("in_msatoshi"))
            .unwrap_or(&Value::Null),
    );
    let out_msat = parse_msat(
        event
            .get("out_msat")
            .or_else(|| event.get("out_msatoshi"))
            .unwrap_or(&Value::Null),
    );
    let fee_msat = parse_msat(
        event
            .get("fee_msat")
            .or_else(|| event.get("fee_msatoshi"))
            .unwrap_or(&Value::Null),
    );
    let received_time = event
        .get("received_time")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let resolved_time = event
        .get("resolved_time")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    ForwardRow {
        in_channel,
        out_channel,
        in_msat,
        out_msat,
        fee_msat,
        timestamp: received_time,
        resolved_time,
    }
}

/// Three-method peer-id extraction fallback, matching
/// `_on_peer_connect_impl`/`_on_peer_disconnect_impl`
/// (cl-revenue-ops.py:6822-6835, 6865-6877): nested under the topic key,
/// a direct `id`, or a nested `peer_id`.
fn extract_peer_id(event: &Value, nest_key: &str) -> Option<String> {
    if let Some(id) = event
        .get(nest_key)
        .and_then(|v| v.get("id"))
        .and_then(|v| v.as_str())
    {
        return Some(id.to_string());
    }
    if let Some(id) = event.get("id").and_then(|v| v.as_str()) {
        return Some(id.to_string());
    }
    if let Some(id) = event
        .get(nest_key)
        .and_then(|v| v.get("peer_id"))
        .and_then(|v| v.as_str())
    {
        return Some(id.to_string());
    }
    None
}

/// `connect` (cl-revenue-ops.py:6800).
pub async fn on_connect(handle: &ObserverHandle, event: &Value) {
    on_peer_presence(handle, event, "connect", "connected").await;
}

/// `disconnect` (cl-revenue-ops.py:6843).
pub async fn on_disconnect(handle: &ObserverHandle, event: &Value) {
    on_peer_presence(handle, event, "disconnect", "disconnected").await;
}

async fn on_peer_presence(
    handle: &ObserverHandle,
    event: &Value,
    nest_key: &str,
    event_type: &str,
) {
    match extract_peer_id(event, nest_key) {
        Some(peer_id) => {
            if let Err(e) = handle
                .insert_peer_connection_event(peer_id, event_type.to_string(), now_unix())
                .await
            {
                eprintln!("revops: error recording {event_type} event: {e}");
            }
        }
        None => {
            eprintln!("revops: {nest_key} event - could not extract peer_id from: {event}");
        }
    }
}

/// States indicating the channel is closing or closed
/// (cl-revenue-ops.py:6977, `closure_states`).
const CLOSURE_STATES: [&str; 4] = [
    "ONCHAIN",
    "CLOSED",
    "FUNDING_SPEND_SEEN",
    "CLOSINGD_COMPLETE",
];

/// `channel_state_changed` (cl-revenue-ops.py:6886). Deliberately narrower
/// than production: only the dedup-insert of a closure event (SCID +
/// cause + timestamp) into the observer's own `channel_closure_events`
/// table -- no bookkeeper on-chain-fee enrichment or channel-open
/// handling (`_handle_channel_open`), both out of this task's scope (see
/// the plan's Task 2 interface note: "deliberately narrower than
/// production's full closure-accounting schema").
pub async fn on_channel_state_changed(handle: &ObserverHandle, event: &Value) {
    if let Err(e) = try_on_channel_state_changed(handle, event).await {
        eprintln!("revops: error in channel_state_changed handler: {e}");
    }
}

async fn try_on_channel_state_changed(
    handle: &ObserverHandle,
    event: &Value,
) -> anyhow::Result<()> {
    // May be nested under a "channel_state_changed" key, or flat.
    let inner = event.get("channel_state_changed").unwrap_or(event);
    let new_state = inner
        .get("new_state")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !CLOSURE_STATES.contains(&new_state) {
        return Ok(());
    }
    let raw_scid = inner
        .get("short_channel_id")
        .and_then(|v| v.as_str())
        .or_else(|| inner.get("channel_id").and_then(|v| v.as_str()));
    let Some(raw_scid) = raw_scid else {
        return Ok(());
    };
    let cause = inner
        .get("cause")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    handle
        .insert_channel_closure_event(normalize_scid(raw_scid), cause.to_string(), now_unix())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalize_scid_replaces_colons() {
        assert_eq!(normalize_scid("931308:1256:1"), "931308x1256x1");
        assert_eq!(normalize_scid("931308x1256x1"), "931308x1256x1");
    }

    #[test]
    fn extract_peer_id_method_1_nested_under_topic() {
        let event = json!({"connect": {"id": "03aa"}});
        assert_eq!(extract_peer_id(&event, "connect"), Some("03aa".to_string()));
    }

    #[test]
    fn extract_peer_id_method_2_direct_id() {
        let event = json!({"id": "03bb"});
        assert_eq!(extract_peer_id(&event, "connect"), Some("03bb".to_string()));
    }

    #[test]
    fn extract_peer_id_method_3_nested_peer_id() {
        let event = json!({"connect": {"peer_id": "03cc"}});
        assert_eq!(extract_peer_id(&event, "connect"), Some("03cc".to_string()));
    }

    #[test]
    fn extract_peer_id_none_when_absent() {
        let event = json!({"unrelated": true});
        assert_eq!(extract_peer_id(&event, "connect"), None);
    }

    #[tokio::test]
    async fn on_forward_event_ignores_non_settled_status() {
        let dir = tempfile::tempdir().unwrap();
        let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
            .await
            .unwrap();
        let event = json!({"status": "failed", "out_channel": "1x1x0"});
        on_forward_event(&handle, &event).await;
        assert_eq!(handle.last_forward_ts().await.unwrap(), None);
    }

    #[tokio::test]
    async fn on_forward_event_inserts_settled_forward() {
        let dir = tempfile::tempdir().unwrap();
        let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
            .await
            .unwrap();
        let event = json!({
            "status": "settled",
            "in_channel": "1:1:0",
            "out_channel": "2:2:0",
            "in_msat": 100_000,
            "out_msat": 99_000,
            "fee_msat": 1_000,
            "received_time": 1_800_000_000,
            "resolved_time": 1_800_000_005,
        });
        on_forward_event(&handle, &event).await;
        assert_eq!(handle.last_forward_ts().await.unwrap(), Some(1_800_000_000));
    }

    #[tokio::test]
    async fn on_forward_event_dedups_duplicate_settled_events() {
        let dir = tempfile::tempdir().unwrap();
        let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
            .await
            .unwrap();
        let event = json!({
            "status": "settled",
            "in_channel": "1x1x0",
            "out_channel": "2x2x0",
            "in_msat": 100_000,
            "out_msat": 99_000,
            "fee_msat": 1_000,
            "received_time": 1_800_000_000,
            "resolved_time": 1_800_000_005,
        });
        on_forward_event(&handle, &event).await;
        on_forward_event(&handle, &event).await;
        let conn = rusqlite::Connection::open(dir.path().join("obs.db")).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ingested_forwards", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn on_connect_records_presence_event() {
        let dir = tempfile::tempdir().unwrap();
        let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
            .await
            .unwrap();
        on_connect(&handle, &json!({"id": "03aa"})).await;
        let conn = rusqlite::Connection::open(dir.path().join("obs.db")).unwrap();
        let event_type: String = conn
            .query_row(
                "SELECT event_type FROM peer_connection_events WHERE peer_id = '03aa'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(event_type, "connected");
    }

    #[tokio::test]
    async fn on_disconnect_records_presence_event() {
        let dir = tempfile::tempdir().unwrap();
        let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
            .await
            .unwrap();
        on_disconnect(&handle, &json!({"disconnect": {"id": "03bb"}})).await;
        let conn = rusqlite::Connection::open(dir.path().join("obs.db")).unwrap();
        let event_type: String = conn
            .query_row(
                "SELECT event_type FROM peer_connection_events WHERE peer_id = '03bb'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(event_type, "disconnected");
    }

    #[tokio::test]
    async fn on_channel_state_changed_records_closure_states() {
        let dir = tempfile::tempdir().unwrap();
        let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
            .await
            .unwrap();
        let event = json!({
            "short_channel_id": "1:1:0",
            "new_state": "ONCHAIN",
            "cause": "remote",
        });
        on_channel_state_changed(&handle, &event).await;
        let conn = rusqlite::Connection::open(dir.path().join("obs.db")).unwrap();
        let (scid, cause): (String, String) = conn
            .query_row("SELECT scid, cause FROM channel_closure_events", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(scid, "1x1x0");
        assert_eq!(cause, "remote");
    }

    #[tokio::test]
    async fn on_channel_state_changed_ignores_non_closure_states() {
        let dir = tempfile::tempdir().unwrap();
        let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
            .await
            .unwrap();
        let event = json!({
            "short_channel_id": "1x1x0",
            "new_state": "CHANNELD_NORMAL",
            "cause": "unknown",
        });
        on_channel_state_changed(&handle, &event).await;
        let conn = rusqlite::Connection::open(dir.path().join("obs.db")).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM channel_closure_events", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn on_channel_state_changed_falls_back_to_channel_id() {
        let dir = tempfile::tempdir().unwrap();
        let handle = revops_db::owner::spawn_read_write(&dir.path().join("obs.db"))
            .await
            .unwrap();
        let event = json!({
            "channel_id": "deadbeef",
            "new_state": "CLOSED",
            "cause": "local",
        });
        on_channel_state_changed(&handle, &event).await;
        let conn = rusqlite::Connection::open(dir.path().join("obs.db")).unwrap();
        let scid: String = conn
            .query_row("SELECT scid FROM channel_closure_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(scid, "deadbeef");
    }
}
