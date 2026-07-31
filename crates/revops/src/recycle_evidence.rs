//! Task 67c slice 4 — recycle nomination evidence.
//!
//! Assembly only: [`revops_capital::planner::ev::is_recycle_eligible`],
//! `calculate_recycle_ev` and `calculate_redeployment_ev` are frozen and
//! are not touched here.
//!
//! The one behaviour worth stating plainly is the three-way protection
//! state, because Python's own "lazy-eval audit F1" (py 3292-3317) was a
//! fix for getting it wrong: a FAILED policy read must fail CLOSED. It
//! previously returned a silently empty set, and peers tagged `protect` /
//! `no_close` were nominatable as Boltz loop-out targets. Collapsing the
//! failure case into "no protected peers" here would reintroduce that
//! defect while looking like a simplification.

use std::collections::BTreeSet;

use revops_analytics::policy::PeerPolicy;

/// Tags that exclude a peer from recycle nomination (py 3304).
pub const PROTECTING_TAGS: [&str; 2] = ["protect", "no_close"];

/// Where the protection set came from. The three cases are genuinely
/// distinct and must not be collapsed to two.
pub enum PolicySource {
    /// Policies were read successfully (possibly an empty list).
    Policies(Vec<PeerPolicy>),
    /// There is no policy manager configured at all (py 3300). Absent
    /// configuration is NOT a broken read: conflating them would
    /// permanently disable recycling on a node that simply has no
    /// policies.
    NoPolicyManager,
    /// The policy source FAILED. Everything is treated as protected.
    Unavailable(String),
}

/// Port of `_recycle_protected_peers` (py 3292-3317).
///
/// `None` means "protection unknown"; the frozen
/// [`revops_capital::planner::ev::is_recycle_eligible`] refuses to nominate
/// anything when it sees `None`. `Some(empty)` means "nothing is
/// protected" and is permissive. The difference decides whether a
/// policy-tagged channel can be closed.
pub fn recycle_protected_peers(source: PolicySource) -> Option<BTreeSet<String>> {
    match source {
        PolicySource::Unavailable(_) => None,
        PolicySource::NoPolicyManager => Some(BTreeSet::new()),
        PolicySource::Policies(policies) => Some(
            policies
                .into_iter()
                .filter(|p| p.tags.iter().any(|t| PROTECTING_TAGS.contains(&t.as_str())))
                .map(|p| p.peer_id)
                .collect(),
        ),
    }
}
