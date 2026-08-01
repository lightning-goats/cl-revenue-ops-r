//! Pure logic for `revenue-cleanup-closed` (py `revenue_cleanup_closed`,
//! cl-revenue-ops.py:6359-6490, plus `_archive_closed_channel`,
//! :7561-7679): the backfill that archives channels which are tracked in
//! the DB but no longer open, then purges their tracking rows.
//!
//! Everything here is pure over already-fetched CLN blobs; the DB reads
//! and the sealed archive write live in
//! `rpc_state_mutators::CoreStateMutationOwner::cleanup_closed`.

use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The already-fetched CLN evidence `cleanup_closed` consumes. Handler
/// gathers it (socket reads live in `main.rs`); the owner never touches
/// the socket.
pub struct CleanupClosedEvidence {
    /// `listpeerchannels` reply, or the fetch error's text (py 6403-6411:
    /// a failed read short-circuits with `Failed to get open channels`).
    pub peer_channels: Result<Value, String>,
    /// `listclosedchannels` reply; `None` when unavailable (py treats a
    /// failed read as best-effort empty info, 6427-6435).
    pub closed_list: Option<Value>,
    /// Chain tip height for the opened_at plausibility repair; 0 when
    /// unavailable (py 7594-7600).
    pub block_height: i64,
    pub now: i64,
}

/// py `normalize_scid` (modules/utils.py:13-19).
pub fn normalize_scid(scid: &str) -> String {
    scid.replace(':', "x")
}

/// The close-type mapping (py 6449-6462): a `close_cause` containing
/// "mutual" wins, else the `closer` field picks the unilateral side. An
/// EMPTY info dict (channel absent from listclosedchannels) stays fully
/// unknown — Python's `if ch_info:` guard.
pub fn close_type_from_info(ch_info: &Map<String, Value>) -> String {
    if ch_info.is_empty() {
        return "unknown".to_string();
    }
    let cause = ch_info
        .get("close_cause")
        .and_then(Value::as_str)
        .unwrap_or("");
    let closer = ch_info.get("closer").and_then(Value::as_str).unwrap_or("");
    if cause.to_lowercase().contains("mutual") {
        "mutual"
    } else if closer == "local" {
        "local_unilateral"
    } else if closer == "remote" {
        "remote_unilateral"
    } else {
        "unknown"
    }
    .to_string()
}

/// py `_determine_closer` (cl-revenue-ops.py:7263-7280).
pub fn determine_closer(close_type: &str) -> &'static str {
    match close_type {
        "mutual" => "mutual",
        "local_unilateral" => "local",
        "remote_unilateral" => "remote",
        _ => "unknown",
    }
}

/// The opened_at plausibility repair (py 7586-7614): anchor on the live
/// chain tip — a stored opened_at deviating from the SCID block-height
/// estimate by more than `max(7d, 15% of estimated age)` is replaced with
/// the estimate, as is a missing opened_at. Without a usable tip or SCID
/// block height the stored value passes through unchanged (Python's
/// except/guard arms).
pub fn repair_opened_at(
    channel_id: &str,
    opened_at: Option<i64>,
    tip: i64,
    now: i64,
) -> Option<i64> {
    if !channel_id.contains('x') {
        return opened_at;
    }
    let Some(height) = channel_id
        .split('x')
        .next()
        .and_then(|h| h.parse::<i64>().ok())
    else {
        return opened_at;
    };
    if !(tip >= height && height > 0) {
        return opened_at;
    }
    let estimate = now - (tip - height) * 600;
    // py: `max(7 * 86400, int(0.15 * max(0, _now - _estimate)))`.
    let slack = (7 * 86400).max((0.15 * (now - estimate).max(0) as f64) as i64);
    match opened_at {
        Some(stored) if (stored - estimate).abs() <= slack => Some(stored),
        _ => Some(estimate),
    }
}

/// The open-channel SCID set from a `listpeerchannels` reply (py
/// 6403-6409): normalized spellings, empty ones dropped.
pub fn open_scids(peer_channels: &Value) -> BTreeSet<String> {
    peer_channels
        .get("channels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|ch| ch.get("short_channel_id").and_then(Value::as_str))
        .map(normalize_scid)
        .filter(|scid| !scid.is_empty())
        .collect()
}

/// Closure info keyed by normalized SCID from a `listclosedchannels`
/// reply (py 6427-6434).
pub fn closed_info_by_scid(closed_list: &Value) -> BTreeMap<String, Map<String, Value>> {
    closed_list
        .get("closedchannels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|ch| {
            let scid = normalize_scid(ch.get("short_channel_id").and_then(Value::as_str)?);
            if scid.is_empty() {
                return None;
            }
            Some((scid, ch.as_object().cloned().unwrap_or_default()))
        })
        .collect()
}

/// The base result dict every arm carries (py 6388-6393).
pub fn cleanup_result(
    archived: i64,
    cleaned: i64,
    channels: &[String],
    errors: &[String],
) -> Value {
    json!({
        "archived": archived,
        "cleaned": cleaned,
        "channels": channels,
        "errors": errors,
    })
}

/// py 6400-6401: nothing tracked at all.
pub fn no_tracked_channels() -> Value {
    with_message("No tracked channels found")
}

/// py 6415-6416: everything tracked is still open.
pub fn no_closed_channels() -> Value {
    with_message("No closed channels found to clean up")
}

fn with_message(message: &str) -> Value {
    json!({
        "message": message,
        "archived": 0,
        "cleaned": 0,
        "channels": [],
        "errors": [],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(fields: Value) -> Map<String, Value> {
        fields.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn close_type_mapping_matches_python_arms() {
        assert_eq!(close_type_from_info(&Map::new()), "unknown");
        assert_eq!(
            close_type_from_info(&info(json!({"close_cause": "user MUTUAL close"}))),
            "mutual",
            "cause wins case-insensitively"
        );
        assert_eq!(
            close_type_from_info(&info(json!({"closer": "local"}))),
            "local_unilateral"
        );
        assert_eq!(
            close_type_from_info(&info(json!({"closer": "remote"}))),
            "remote_unilateral"
        );
        assert_eq!(
            close_type_from_info(&info(json!({"closer": "themself?"}))),
            "unknown"
        );
    }

    #[test]
    fn determine_closer_matches_python() {
        assert_eq!(determine_closer("mutual"), "mutual");
        assert_eq!(determine_closer("local_unilateral"), "local");
        assert_eq!(determine_closer("remote_unilateral"), "remote");
        assert_eq!(determine_closer("force"), "unknown");
    }

    #[test]
    fn repair_keeps_a_plausible_opened_at_and_replaces_an_implausible_one() {
        let now = 1_800_000_000i64;
        let tip = 900_000i64;
        // Channel opened ~1000 blocks ago: estimate = now - 600_000.
        let estimate = now - 1000 * 600;
        // Stored within the 7-day slack: kept.
        assert_eq!(
            repair_opened_at("899000x1x0", Some(estimate - 86400), tip, now),
            Some(estimate - 86400)
        );
        // Stored 30 days off (slack is max(7d, 15% of ~7 days) = 7d): repaired.
        assert_eq!(
            repair_opened_at("899000x1x0", Some(estimate - 30 * 86400), tip, now),
            Some(estimate)
        );
        // Missing: repaired to the estimate.
        assert_eq!(
            repair_opened_at("899000x1x0", None, tip, now),
            Some(estimate)
        );
    }

    #[test]
    fn repair_passes_through_without_a_usable_tip_or_scid() {
        // No tip (0): unchanged, including a None.
        assert_eq!(repair_opened_at("899000x1x0", Some(5), 0, 100), Some(5));
        assert_eq!(repair_opened_at("899000x1x0", None, 0, 100), None);
        // Height above tip: unchanged.
        assert_eq!(repair_opened_at("901x1x0", Some(5), 900, 100), Some(5));
        // Unparseable / non-scid ids: unchanged (py ValueError arm).
        assert_eq!(repair_opened_at("nonsense", Some(5), 900, 100), Some(5));
        assert_eq!(repair_opened_at("axbxc", Some(5), 900, 100), Some(5));
    }

    #[test]
    fn open_scids_normalizes_and_drops_empties() {
        let blob = json!({"channels": [
            {"short_channel_id": "1:2:3"},
            {"short_channel_id": "4x5x6"},
            {"short_channel_id": ""},
            {"no_scid": true},
        ]});
        let scids = open_scids(&blob);
        assert_eq!(
            scids.into_iter().collect::<Vec<_>>(),
            vec!["1x2x3".to_string(), "4x5x6".to_string()]
        );
    }

    #[test]
    fn closed_info_keys_by_normalized_scid() {
        let blob = json!({"closedchannels": [
            {"short_channel_id": "7:8:9", "closer": "remote"},
        ]});
        let map = closed_info_by_scid(&blob);
        assert_eq!(map["7x8x9"]["closer"], "remote");
    }

    #[test]
    fn early_out_messages_carry_the_full_zeroed_result() {
        assert_eq!(
            no_tracked_channels(),
            json!({
                "message": "No tracked channels found",
                "archived": 0, "cleaned": 0, "channels": [], "errors": [],
            })
        );
        assert_eq!(
            no_closed_channels(),
            json!({
                "message": "No closed channels found to clean up",
                "archived": 0, "cleaned": 0, "channels": [], "errors": [],
            })
        );
    }
}
