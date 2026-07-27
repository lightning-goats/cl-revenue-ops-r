//! Pure response builders for `revenue-r-policy`'s READ-ONLY actions
//! (`list`/`get`/`find`/`changes`) -- port of `revenue_policy`
//! (cl-revenue-ops.py:5325-5573), gated to `READ_ONLY_POLICY_ACTIONS`
//! (policy_manager.py:54). This batch is read-only reporting only: the
//! tactical actions (`set`/`delete`/`tag`/`untag`/`batch`,
//! `TACTICAL_POLICY_ACTIONS`, policy_manager.py:55) are DB writes and
//! stay out of scope -- [`policy_action_gate`] always returns Python's
//! "deprecated for normal operator use" refusal for them (never applying
//! Python's `internal`/`admin` override, since this port implements no
//! write path for those actions to unlock).
//!
//! Every builder consumes already-fetched [`PeerPolicy`] values (ported in
//! `revops_analytics::policy`, itself a port of `modules/policy_manager
//! .py`'s `PeerPolicy`) -- fetching them from the production DB is the
//! wiring layer's job (see `crates/revops/RPC_BATCH_A.md`: `policy_manager`
//! has no `revops-db` query equivalent yet).

use revops_analytics::policy::PeerPolicy;
use serde_json::{json, Value};

/// Read-only actions this builder supports (policy_manager.py:54's
/// `READ_ONLY_POLICY_ACTIONS`, alphabetized for the "Unknown action"
/// message below).
const READ_ONLY_ACTIONS: [&str; 4] = ["changes", "find", "get", "list"];
/// Tactical (write) actions -- always refused by this read-only port
/// (policy_manager.py:55's `TACTICAL_POLICY_ACTIONS`).
const TACTICAL_ACTIONS: [&str; 5] = ["batch", "delete", "set", "tag", "untag"];

/// `PeerPolicy.to_dict()` (policy_manager.py:135-149) as JSON. `now`
/// stands in for Python's internal `time.time()` read inside `is_expired`.
pub fn peer_policy_to_json(p: &PeerPolicy, now: i64) -> Value {
    json!({
        "peer_id": p.peer_id,
        "strategy": p.strategy.as_value(),
        "rebalance_mode": p.rebalance_mode.as_value(),
        "fee_ppm_target": p.fee_ppm_target,
        "tags": p.tags,
        "updated_at": p.updated_at,
        "fee_multiplier_min": p.fee_multiplier_min,
        "fee_multiplier_max": p.fee_multiplier_max,
        "expires_at": p.expires_at,
        "is_expired": p.is_expired(now),
    })
}

/// `action or "list"`, then `.strip().lower()` (cl-revenue-ops.py:5397).
pub fn normalize_action(raw: Option<&str>) -> String {
    raw.unwrap_or("list").trim().to_lowercase()
}

/// Routes `action` before any DB fetch happens: `Some(error)` for a
/// tactical or unrecognized action (the caller should return it as-is,
/// no query needed); `None` means "one of the 4 read actions -- proceed
/// to fetch and call the matching `build_policy_*` below".
pub fn policy_action_gate(action: &str) -> Option<Value> {
    if TACTICAL_ACTIONS.contains(&action) {
        return Some(json!({
            "error": format!(
                "revenue-policy {action} is deprecated for normal operator use. \
                 Use revenue-policy list/get/find/changes for diagnostics."
            )
        }));
    }
    if READ_ONLY_ACTIONS.contains(&action) {
        return None;
    }
    let mut allowed: Vec<&str> = READ_ONLY_ACTIONS
        .iter()
        .chain(TACTICAL_ACTIONS.iter())
        .copied()
        .collect();
    allowed.sort_unstable();
    let allowed_text = allowed
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ");
    Some(json!({"error": format!("Unknown action: {action}. Use {allowed_text}")}))
}

/// `action=get` usage error (cl-revenue-ops.py:5414): `peer_id` missing.
pub fn get_usage_error() -> Value {
    json!({"error": "Usage: revenue-policy get <peer_id>"})
}

/// `action=find` usage error (cl-revenue-ops.py:5514): `tag` missing.
pub fn find_usage_error() -> Value {
    json!({"error": "Usage: revenue-policy find <tag>"})
}

/// `L-22` peer_id format validation error, shared by `get`
/// (cl-revenue-ops.py:5416-5417) -- also reused by `revenue-ban`/
/// `-unban`/`-hot-channel-protection-peers add`, but those are write RPCs
/// out of this batch's scope.
pub fn invalid_peer_id_error() -> Value {
    json!({"error": "Invalid peer_id format: expected 66-character hex pubkey"})
}

/// `action=changes`'s `since` coercion error (cl-revenue-ops.py:5537-5538).
pub fn invalid_since_error() -> Value {
    json!({"error": "Invalid 'since' timestamp. Must be a Unix timestamp."})
}

/// `action=list` (cl-revenue-ops.py:5407-5411).
pub fn build_policy_list(policies: &[PeerPolicy], now: i64) -> Value {
    json!({
        "policies": policies.iter().map(|p| peer_policy_to_json(p, now)).collect::<Vec<_>>(),
        "count": policies.len(),
    })
}

/// `action=get <peer_id>` (cl-revenue-ops.py:5418-5420). Caller must have
/// already validated `peer_id` ([`invalid_peer_id_error`]) and fetched the
/// policy (`policy_manager.get_policy` always returns SOME policy --
/// caller-defaults if none exists explicitly, see `PeerPolicy::default_for`).
pub fn build_policy_get(policy: &PeerPolicy, now: i64) -> Value {
    json!({"policy": peer_policy_to_json(policy, now)})
}

/// `action=find <tag>` (cl-revenue-ops.py:5510-5519).
pub fn build_policy_find(tag: &str, policies: &[PeerPolicy], now: i64) -> Value {
    json!({
        "peers": policies.iter().map(|p| peer_policy_to_json(p, now)).collect::<Vec<_>>(),
        "count": policies.len(),
        "tag": tag,
    })
}

/// `action=changes [since=<ts>]` (cl-revenue-ops.py:5524-5535). `changes`
/// must already exclude expired policies -- `PolicyManager
/// .get_policy_changes_since` filters those out before `to_dict()`
/// (policy_manager.py:521-522), so this builder mirrors that by requiring
/// the caller to pre-filter (`PeerPolicy::is_expired`) rather than
/// re-filtering here.
pub fn build_policy_changes(
    since: i64,
    changes: &[PeerPolicy],
    last_change_timestamp: i64,
    now: i64,
) -> Value {
    json!({
        "changes": changes.iter().map(|p| peer_policy_to_json(p, now)).collect::<Vec<_>>(),
        "count": changes.len(),
        "since": since,
        "last_change_timestamp": last_change_timestamp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_analytics::policy::{FeeStrategy, RebalanceMode};

    fn policy(peer_id: &str) -> PeerPolicy {
        let mut p = PeerPolicy::default_for(peer_id);
        p.strategy = FeeStrategy::Static;
        p.rebalance_mode = RebalanceMode::Disabled;
        p.fee_ppm_target = Some(500);
        p.tags = vec!["whale".to_string()];
        p.updated_at = 1_700_000_000;
        p
    }

    #[test]
    fn peer_policy_to_json_matches_python_to_dict_shape() {
        let peer_id = "02".to_string() + &"a".repeat(64);
        let p = policy(&peer_id);
        let v = peer_policy_to_json(&p, 1_700_000_100);
        assert_eq!(v["strategy"], "static");
        assert_eq!(v["rebalance_mode"], "disabled");
        assert_eq!(v["fee_ppm_target"], 500);
        assert_eq!(v["tags"], json!(["whale"]));
        assert_eq!(v["is_expired"], false);
        assert_eq!(v["expires_at"], Value::Null);
    }

    #[test]
    fn is_expired_reflects_injected_now_not_a_clock() {
        let mut p = policy("peer-a");
        p.expires_at = Some(1000);
        assert_eq!(peer_policy_to_json(&p, 999)["is_expired"], false);
        assert_eq!(peer_policy_to_json(&p, 1001)["is_expired"], true);
    }

    #[test]
    fn action_gate_blocks_tactical_actions_with_exact_python_message() {
        let v = policy_action_gate("set").unwrap();
        assert_eq!(
            v["error"],
            "revenue-policy set is deprecated for normal operator use. \
             Use revenue-policy list/get/find/changes for diagnostics."
        );
    }

    #[test]
    fn action_gate_allows_read_actions() {
        for a in ["list", "get", "find", "changes"] {
            assert!(
                policy_action_gate(a).is_none(),
                "expected {a} to be allowed"
            );
        }
    }

    #[test]
    fn action_gate_unknown_action_lists_all_nine_alphabetized() {
        let v = policy_action_gate("bogus").unwrap();
        assert_eq!(
            v["error"],
            "Unknown action: bogus. Use 'batch', 'changes', 'delete', 'find', 'get', 'list', \
             'set', 'tag', 'untag'"
        );
    }

    #[test]
    fn normalize_action_defaults_and_lowercases() {
        assert_eq!(normalize_action(None), "list");
        assert_eq!(normalize_action(Some(" GET ")), "get");
    }

    #[test]
    fn build_policy_list_reports_count() {
        let policies = vec![policy("a"), policy("b")];
        let v = build_policy_list(&policies, 0);
        assert_eq!(v["count"], 2);
        assert_eq!(v["policies"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn build_policy_find_echoes_tag() {
        let policies = vec![policy("a")];
        let v = build_policy_find("whale", &policies, 0);
        assert_eq!(v["tag"], "whale");
        assert_eq!(v["count"], 1);
    }

    #[test]
    fn build_policy_changes_shape() {
        let policies = vec![policy("a")];
        let v = build_policy_changes(500, &policies, 1_700_000_000, 1_700_000_100);
        assert_eq!(v["since"], 500);
        assert_eq!(v["last_change_timestamp"], 1_700_000_000);
        assert_eq!(v["count"], 1);
    }
}
