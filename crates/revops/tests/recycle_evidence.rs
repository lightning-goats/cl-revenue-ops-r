//! Task 67c slice 4: recycle nomination evidence.
//!
//! The headline trap is one Python's OWN audit had to fix (py 3292-3317,
//! "lazy-eval audit F1"): `_recycle_protected_peers` returned a silently
//! EMPTY set when the policy source failed, so peers tagged `protect` /
//! `no_close` were nominatable as Boltz loop-out targets. The fix was to
//! return `None` and fail closed. Collapsing `None` into "no protected
//! peers" here would reintroduce that exact defect -- and it would look
//! like a tidy simplification.

use std::collections::BTreeSet;

use revops::recycle_evidence::{recycle_protected_peers, PolicySource};
use revops_analytics::policy::PeerPolicy;
use revops_capital::planner::ev::{is_recycle_eligible, RecycleEligibilityInput};

fn tagged(peer_id: &str, tags: &[&str]) -> PeerPolicy {
    PeerPolicy {
        tags: tags.iter().map(|t| (*t).to_string()).collect(),
        ..PeerPolicy::default_for(peer_id)
    }
}

/// An eligible loser: old enough, negative marginal ROI, unprotected.
fn eligible_loser<'a>(peer_id: &'a str, scid: &'a str) -> RecycleEligibilityInput<'a> {
    RecycleEligibilityInput {
        scid,
        peer_id,
        // 60 days needs (current - open) * 10 / 1440 >= 60 => >= 8640 blocks
        current_block_height: 800_000 + 9_000,
        marginal_roi_percent: -5.0,
    }
}

/// Both `protect` and `no_close` exclude a peer; unrelated tags do not.
#[test]
fn protect_and_no_close_tags_both_protect() {
    let policies = vec![
        tagged("02protect", &["protect"]),
        tagged("02noclose", &["no_close"]),
        tagged("02other", &["banned"]),
        tagged("02untagged", &[]),
    ];
    let protected =
        recycle_protected_peers(PolicySource::Policies(policies)).expect("source succeeded");
    assert!(protected.contains("02protect"));
    assert!(protected.contains("02noclose"));
    assert!(
        !protected.contains("02other"),
        "an unrelated tag must not protect"
    );
    assert!(!protected.contains("02untagged"));
}

/// A FAILED policy source is `None`, not an empty set -- and the frozen
/// kernel then refuses to nominate anything. This is the audit-F1
/// regression guard: with an empty set the same loser becomes eligible.
#[test]
fn a_failed_policy_source_protects_everything() {
    let failed = recycle_protected_peers(PolicySource::Unavailable("db read failed".into()));
    assert!(failed.is_none(), "source failure must be None, not empty");

    let loser = eligible_loser("02anyone", "800000x1x0");
    let (ok, reason) = is_recycle_eligible(&loser, failed.as_ref(), &BTreeSet::new());
    assert!(!ok, "unknown protection must block nomination");
    assert!(reason.contains("Policy protection unknown"), "{reason}");

    // The regression this pins: an EMPTY set nominates the same peer.
    let (ok_if_empty, _) = is_recycle_eligible(&loser, Some(&BTreeSet::new()), &BTreeSet::new());
    assert!(
        ok_if_empty,
        "sanity: an empty set really is permissive, so None must not collapse to it"
    );
}

/// No policy manager at all is an EMPTY set, not a failure (py 3300:
/// `if not self.policy_manager: return set()`). Absent configuration is
/// not the same as a broken read, and conflating them would permanently
/// disable recycling on a node that simply has no policies.
#[test]
fn absent_policy_manager_is_empty_not_unknown() {
    let protected =
        recycle_protected_peers(PolicySource::NoPolicyManager).expect("absent != failed");
    assert!(protected.is_empty());

    let loser = eligible_loser("02anyone", "800000x1x0");
    let (ok, _) = is_recycle_eligible(&loser, Some(&protected), &BTreeSet::new());
    assert!(ok, "a node with no policies can still recycle");
}
