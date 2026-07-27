//! Pure response builder for `revenue-r-list-ignored` (DEPRECATED, ported
//! verbatim for parity -- Python still serves it).
//!
//! Port of `revenue_list_ignored` (cl-revenue-ops.py:5216-5245): a peer is
//! "ignored" when its policy is `strategy=PASSIVE` AND
//! `rebalance_mode=DISABLED`; the reported `reason` is its first tag that
//! isn't literally `"ignored"`, defaulting to `"manual"`.

use revops_analytics::policy::{FeeStrategy, PeerPolicy, RebalanceMode};
use serde_json::{json, Value};

/// Port of `revenue_list_ignored`. `all_policies` is
/// `policy_manager.get_all_policies()`'s already-fetched result (see
/// `crates/revops/RPC_BATCH_A.md` for the missing `revops-db` query).
pub fn build_list_ignored(all_policies: &[PeerPolicy]) -> Value {
    let ignored: Vec<Value> = all_policies
        .iter()
        .filter(|p| {
            p.strategy == FeeStrategy::Passive && p.rebalance_mode == RebalanceMode::Disabled
        })
        .map(|p| {
            let reason = p
                .tags
                .iter()
                .find(|t| t.as_str() != "ignored")
                .cloned()
                .unwrap_or_else(|| "manual".to_string());
            json!({
                "peer_id": p.peer_id,
                "reason": reason,
                "ignored_at": p.updated_at,
            })
        })
        .collect();
    json!({
        "ignored_peers": ignored,
        "count": ignored.len(),
        "warning": "DEPRECATED: Use 'revenue-policy list' instead.",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(
        peer_id: &str,
        strategy: FeeStrategy,
        rebalance: RebalanceMode,
        tags: &[&str],
    ) -> PeerPolicy {
        let mut p = PeerPolicy::default_for(peer_id);
        p.strategy = strategy;
        p.rebalance_mode = rebalance;
        p.tags = tags.iter().map(|t| t.to_string()).collect();
        p.updated_at = 99;
        p
    }

    #[test]
    fn only_passive_and_disabled_peers_counted() {
        let policies = vec![
            policy("a", FeeStrategy::Passive, RebalanceMode::Disabled, &[]),
            policy("b", FeeStrategy::Passive, RebalanceMode::Enabled, &[]),
            policy("c", FeeStrategy::Dynamic, RebalanceMode::Disabled, &[]),
        ];
        let v = build_list_ignored(&policies);
        assert_eq!(v["count"], 1);
        assert_eq!(v["ignored_peers"][0]["peer_id"], "a");
    }

    #[test]
    fn reason_skips_the_literal_ignored_tag() {
        let policies = vec![policy(
            "a",
            FeeStrategy::Passive,
            RebalanceMode::Disabled,
            &["ignored", "low_value"],
        )];
        let v = build_list_ignored(&policies);
        assert_eq!(v["ignored_peers"][0]["reason"], "low_value");
    }

    #[test]
    fn reason_defaults_to_manual_when_no_other_tag() {
        let policies = vec![policy(
            "a",
            FeeStrategy::Passive,
            RebalanceMode::Disabled,
            &["ignored"],
        )];
        let v = build_list_ignored(&policies);
        assert_eq!(v["ignored_peers"][0]["reason"], "manual");
    }

    #[test]
    fn warning_is_always_present() {
        let v = build_list_ignored(&[]);
        assert_eq!(
            v["warning"],
            "DEPRECATED: Use 'revenue-policy list' instead."
        );
    }
}
