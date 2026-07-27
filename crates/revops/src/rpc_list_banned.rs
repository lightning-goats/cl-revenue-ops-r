//! Pure response builder for `revenue-r-list-banned`.
//!
//! Port of `revenue_list_banned` (cl-revenue-ops.py:5308-5317):
//! `policy_manager.get_peers_by_tag(BANNED_TAG)` filters the full policy
//! set down to peers carrying the `"banned"` tag (`revenue-ban`'s durable
//! marker, policy_manager.py:59). This builder does that filtering itself
//! given an already-fetched policy list -- fetching "all policies" from
//! the production DB is the wiring layer's job (see
//! `crates/revops/RPC_BATCH_A.md`: no `revops-db` "all policies" query
//! exists yet).

use revops_analytics::policy::PeerPolicy;
use serde_json::{json, Value};

/// `PolicyManager.BANNED_TAG` (policy_manager.py:59).
pub const BANNED_TAG: &str = "banned";

/// Port of `revenue_list_banned`. `all_policies` should be every peer with
/// an explicit policy row (Python's `get_peers_by_tag` queries the DB
/// directly for tag membership; filtering an already-fetched full list
/// here is behaviorally identical -- same tag test, same fields).
pub fn build_list_banned(all_policies: &[PeerPolicy]) -> Value {
    let banned: Vec<Value> = all_policies
        .iter()
        .filter(|p| p.has_tag(BANNED_TAG))
        .map(|p| {
            json!({
                "peer_id": p.peer_id,
                "tags": p.tags,
                "banned_at": p.updated_at,
            })
        })
        .collect();
    json!({
        "count": banned.len(),
        "banned_peers": banned,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use revops_analytics::policy::{FeeStrategy, RebalanceMode};

    fn policy_with_tags(peer_id: &str, tags: &[&str]) -> PeerPolicy {
        let mut p = PeerPolicy::default_for(peer_id);
        p.strategy = FeeStrategy::Passive;
        p.rebalance_mode = RebalanceMode::Disabled;
        p.tags = tags.iter().map(|t| t.to_string()).collect();
        p.updated_at = 42;
        p
    }

    #[test]
    fn only_banned_tagged_peers_are_included() {
        let policies = vec![
            policy_with_tags("peer-a", &["banned", "operator"]),
            policy_with_tags("peer-b", &["whale"]),
            policy_with_tags("peer-c", &["banned"]),
        ];
        let v = build_list_banned(&policies);
        assert_eq!(v["count"], 2);
        let peers = v["banned_peers"].as_array().unwrap();
        let ids: Vec<&str> = peers
            .iter()
            .map(|p| p["peer_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["peer-a", "peer-c"]);
        assert_eq!(peers[0]["banned_at"], 42);
    }

    #[test]
    fn empty_input_yields_empty_list_not_null() {
        let v = build_list_banned(&[]);
        assert_eq!(v["count"], 0);
        assert_eq!(v["banned_peers"], json!([]));
    }
}
