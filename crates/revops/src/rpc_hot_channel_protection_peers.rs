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

use serde_json::{json, Value};

/// One row of `hot_channel_protection_overrides` (modules/database.py:
/// 855-865's DDL) -- there is no `revops-db` query for this table yet
/// (see `crates/revops/RPC_BATCH_A.md`), so this small row shape lives
/// here rather than being imported from `revops_db::queries`.
#[derive(Debug, Clone, PartialEq)]
pub struct HotChannelProtectionOverridePeer {
    pub peer_id: String,
    pub added_at: i64,
    pub note: String,
    /// `NULL`-able REAL column (modules/database.py:864).
    pub min_depletion_trigger_pct: Option<f64>,
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
    fn rows_map_field_for_field_in_input_order() {
        let rows = vec![
            HotChannelProtectionOverridePeer {
                peer_id: "peer-a".to_string(),
                added_at: 100,
                note: "manual override".to_string(),
                min_depletion_trigger_pct: Some(25.5),
            },
            HotChannelProtectionOverridePeer {
                peer_id: "peer-b".to_string(),
                added_at: 200,
                note: String::new(),
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
