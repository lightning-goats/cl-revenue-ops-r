//! Pure response builder for `revenue-r-hot-channel-protection-peers`,
//! `list` action only (READ-ONLY batch scope -- `add`/`remove`/`clear` are
//! DB writes and stay out of scope).
//!
//! Port of `revenue_hot_channel_protection_peers`'s `list` branch
//! (cl-revenue-ops.py:5738-5741), which is
//! `database.list_hot_channel_protection_override_peers()`
//! (modules/database.py:7281-7287): `SELECT peer_id, added_at, note,
//! min_depletion_trigger_pct FROM hot_channel_protection_overrides ORDER
//! BY added_at ASC`, returned row-for-row.

use crate::rpc_params::is_truthy_py;
use serde_json::{json, Value};

/// Shared DB-owned row contract. Re-exported here to preserve the response
/// builder's public API while the later plugin wiring fetches rows directly
/// from `revops_db::queries`.
pub use revops_db::queries::HotChannelProtectionOverridePeer;

/// The three write actions this read-only port refuses
/// (cl-revenue-ops.py:5723-5776's `add`/`remove`/`clear` branches).
pub const WRITE_ACTIONS: [&str; 3] = ["add", "remove", "clear"];

/// Task 50 correction round, F8: `action = str(action or "list").lower()`
/// (cl-revenue-ops.py:5735, confirmed) -- Python-falsy `action` (absent,
/// `null`, `0`, `false`, `""`, an empty array/object) defaults to
/// `"list"`; anything else is lowercased with **NO `.strip()`** (unlike
/// `revenue-r-policy`'s `normalize_action`, which DOES strip -- the two
/// RPCs use genuinely different Python normalization, confirmed against
/// both source lines, not a copy-paste of one convention onto the other).
/// The OLD Rust wiring compared the raw JSON string directly against
/// `"list"` with no lowercasing at all, so `action="LIST"` was refused
/// (Python succeeds) and `action=""`/`null` were also refused (Python
/// defaults to `list` and succeeds).
pub fn normalize_action(raw: Option<&Value>) -> String {
    match raw {
        None => "list".to_string(),
        Some(v) if !is_truthy_py(v) => "list".to_string(),
        Some(Value::String(s)) => s.to_lowercase(), // NO .strip() -- " list" stays " list".
        Some(other) => python_str_lossy(other).to_lowercase(),
    }
}

/// A best-effort `str(x)` for the non-string JSON values Python's
/// `action or "list"` can carry through truthy (numbers, `true`, non-empty
/// arrays/objects) -- none of these can ever equal `"list"`/`"add"`/
/// `"remove"`/`"clear"`, so this only needs to be distinct enough to reach
/// the same "Unknown action" family Python reaches for all of them.
fn python_str_lossy(v: &Value) -> String {
    match v {
        Value::Bool(b) => {
            if *b {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

/// Task 50 correction round, H6: the OLD refusal message ("... is not
/// available in this read-only port; use 'list'") was returned for BOTH a
/// real write action (`add`/`remove`/`clear` -- a genuine scope boundary)
/// AND any unrecognized string, conflating "you asked for something this
/// port doesn't implement" with "you asked for something that doesn't
/// exist at all". Split them: this is ONLY for the three real write
/// actions.
pub fn write_action_refused_error(action: &str) -> Value {
    json!({
        "error": format!(
            "revenue-hot-channel-protection-peers {action} is not available \
             in this read-only port; use 'list'"
        )
    })
}

/// Python's `Unknown action` error (cl-revenue-ops.py:5772) for anything
/// that isn't `list`/`add`/`remove`/`clear`.
pub fn unknown_action_error(action: &str) -> Value {
    json!({"error": format!("Unknown action: {action}. Use list|add|remove|clear")})
}

/// Port of the `list` branch. Row order (`ORDER BY added_at ASC`) is the
/// caller's responsibility -- this builder does not sort, mirroring
/// Python's straight `[dict(r) for r in rows]` pass-through of the SQL
/// result order.
pub fn build_hot_channel_protection_peers_list(rows: &[HotChannelProtectionOverridePeer]) -> Value {
    let peers: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "peer_id": r.peer_id,
                "added_at": r.added_at,
                "note": r.note,
                "min_depletion_trigger_pct": r.min_depletion_trigger_pct,
            })
        })
        .collect();
    json!({
        "status": "success",
        "count": peers.len(),
        "peers": peers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_action_absent_and_falsy_default_to_list() {
        assert_eq!(normalize_action(None), "list");
        assert_eq!(normalize_action(Some(&Value::Null)), "list");
        assert_eq!(normalize_action(Some(&json!(""))), "list");
        assert_eq!(normalize_action(Some(&json!(false))), "list");
        assert_eq!(normalize_action(Some(&json!(0))), "list");
    }

    #[test]
    fn normalize_action_uppercase_succeeds_lowercased() {
        assert_eq!(normalize_action(Some(&json!("LIST"))), "list");
        assert_eq!(normalize_action(Some(&json!("Add"))), "add");
    }

    /// F8: NO `.strip()` -- unlike `revenue-r-policy`'s `normalize_action`.
    #[test]
    fn normalize_action_does_not_strip_whitespace() {
        assert_eq!(normalize_action(Some(&json!(" list"))), " list");
        assert_ne!(normalize_action(Some(&json!(" list"))), "list");
    }

    #[test]
    fn write_action_refused_and_unknown_action_are_distinct_messages() {
        let refused = write_action_refused_error("add");
        let unknown = unknown_action_error("bogus");
        assert!(refused["error"]
            .as_str()
            .unwrap()
            .contains("not available in this read-only port"));
        assert!(unknown["error"]
            .as_str()
            .unwrap()
            .starts_with("Unknown action: bogus."));
        assert_ne!(refused, unknown);
    }

    #[test]
    fn rows_map_field_for_field_in_input_order() {
        let rows = vec![
            HotChannelProtectionOverridePeer {
                peer_id: "peer-a".to_string(),
                added_at: 100,
                note: Some("manual override".to_string()),
                min_depletion_trigger_pct: Some(25.5),
            },
            HotChannelProtectionOverridePeer {
                peer_id: "peer-b".to_string(),
                added_at: 200,
                note: None,
                min_depletion_trigger_pct: None,
            },
        ];
        let v = build_hot_channel_protection_peers_list(&rows);
        assert_eq!(v["status"], "success");
        assert_eq!(v["count"], 2);
        let peers = v["peers"].as_array().unwrap();
        assert_eq!(peers[0]["peer_id"], "peer-a");
        assert_eq!(peers[0]["min_depletion_trigger_pct"], 25.5);
        assert_eq!(peers[1]["peer_id"], "peer-b");
        assert_eq!(peers[1]["note"], Value::Null);
        // NULL column must stay null, never a fabricated 0.0.
        assert_eq!(peers[1]["min_depletion_trigger_pct"], Value::Null);
    }

    #[test]
    fn empty_table_yields_empty_list_not_null() {
        let v = build_hot_channel_protection_peers_list(&[]);
        assert_eq!(v["count"], 0);
        assert_eq!(v["peers"], json!([]));
        assert_eq!(v["status"], "success");
    }
}
