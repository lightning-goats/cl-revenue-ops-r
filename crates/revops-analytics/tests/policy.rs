//! Public-API integration tests for `revops_analytics::policy` — a pure
//! port of `modules/policy_manager.py`'s `FeeStrategy`/`RebalanceMode`/
//! `PeerPolicy`/`set_policy`-validation-core (cl_revenue_ops-port).
//!
//! Unit-level edge cases (frozen error-message text, boundary values) live
//! alongside the implementation in `src/policy.rs`'s `#[cfg(test)]`
//! module; this file exercises the same surface from outside the crate,
//! as a caller would, and pins the traps called out in the Task 5 brief
//! that are easy to regress at the integration boundary: the peer-id
//! trailing-newline reject (PM-I1), `is_expired`'s strict `>` semantics,
//! and `validate_policy_update`'s "only touch what's provided" contract.

use revops_analytics::policy::{
    is_valid_peer_id, validate_policy_update, FeeStrategy, PeerPolicy, PolicyUpdate, RebalanceMode,
};

const PEER: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[test]
fn peer_id_trailing_newline_is_rejected() {
    // PM-I1: the historical Python bug used `^...$`, and Python's `$`
    // matches just before a trailing `\n`, so a 67-byte
    // "<66 hex chars>\n" string slipped through validation and got
    // persisted. The fixed pattern is anchored `\A...\Z`; this Rust port
    // has no `regex` crate available at all (workspace constraint), so it
    // reimplements the FIXED semantics directly via an exact-length scan.
    let sneaky = format!("{PEER}\n");
    assert!(!is_valid_peer_id(&sneaky));
    assert!(is_valid_peer_id(PEER));
}

#[test]
fn default_policy_is_dynamic_enabled_untagged() {
    let p = PeerPolicy::default_for(PEER);
    assert_eq!(p.strategy, FeeStrategy::Dynamic);
    assert_eq!(p.rebalance_mode, RebalanceMode::Enabled);
    assert!(p.tags.is_empty());
    assert_eq!(p.fee_ppm_target, None);
    assert_eq!(p.expires_at, None);
}

#[test]
fn set_static_strategy_without_target_is_rejected_end_to_end() {
    let existing = PeerPolicy::default_for(PEER);
    let update = PolicyUpdate {
        strategy: Some("static".to_string()),
        ..Default::default()
    };
    let err = validate_policy_update(&existing, &update, 1_700_000_000).unwrap_err();
    assert_eq!(err.to_string(), "strategy=static requires fee_ppm_target");
}

#[test]
fn set_static_strategy_with_target_round_trips() {
    let existing = PeerPolicy::default_for(PEER);
    let update = PolicyUpdate {
        strategy: Some("static".to_string()),
        fee_ppm_target: Some(1200),
        tags: Some(vec!["tier1".to_string()]),
        ..Default::default()
    };
    let updated = validate_policy_update(&existing, &update, 1_700_000_000).unwrap();
    assert_eq!(updated.strategy, FeeStrategy::Static);
    assert_eq!(updated.fee_ppm_target, Some(1200));
    assert_eq!(updated.tags, vec!["tier1".to_string()]);
    assert_eq!(updated.updated_at, 1_700_000_000);
    // peer_id and rebalance_mode carry over untouched from `existing`.
    assert_eq!(updated.peer_id, PEER);
    assert_eq!(updated.rebalance_mode, RebalanceMode::Enabled);
}

#[test]
fn partial_update_only_touches_provided_fields() {
    let mut existing = PeerPolicy::default_for(PEER);
    existing.strategy = FeeStrategy::Static;
    existing.fee_ppm_target = Some(500);
    existing.tags = vec!["existing_tag".to_string()];
    existing.fee_multiplier_min = Some(0.5);
    existing.fee_multiplier_max = Some(2.0);

    // Only touch rebalance_mode.
    let update = PolicyUpdate {
        rebalance_mode: Some("source_only".to_string()),
        ..Default::default()
    };
    let updated = validate_policy_update(&existing, &update, 2_000).unwrap();

    assert_eq!(updated.rebalance_mode, RebalanceMode::SourceOnly);
    assert_eq!(
        updated.strategy,
        FeeStrategy::Static,
        "untouched field must carry over"
    );
    assert_eq!(updated.fee_ppm_target, Some(500));
    assert_eq!(updated.tags, vec!["existing_tag".to_string()]);
    assert_eq!(updated.fee_multiplier_min, Some(0.5));
    assert_eq!(updated.fee_multiplier_max, Some(2.0));
}

#[test]
fn expiry_is_strictly_greater_than_at_the_boundary() {
    let mut p = PeerPolicy::default_for(PEER);
    p.expires_at = Some(10_000);
    assert!(
        !p.is_expired(10_000),
        "now == expires_at must not be expired"
    );
    assert!(p.is_expired(10_001));
}

#[test]
fn set_expiry_via_update_then_it_expires_after_injected_clock_advances() {
    let existing = PeerPolicy::default_for(PEER);
    let update = PolicyUpdate {
        expires_in_hours: Some(1),
        ..Default::default()
    };
    let now = 1_000_000;
    let updated = validate_policy_update(&existing, &update, now).unwrap();
    assert_eq!(updated.expires_at, Some(now + 3600));
    assert!(!updated.is_expired(now + 3599));
    assert!(updated.is_expired(now + 3601));
}

#[test]
fn banned_tag_workflow_via_has_tag() {
    let mut p = PeerPolicy::default_for(PEER);
    p.tags = vec!["banned".to_string()];
    assert!(p.has_tag("banned"));

    let update = PolicyUpdate {
        tags: Some(vec![]),
        ..Default::default()
    };
    let unbanned = validate_policy_update(&p, &update, 1000).unwrap();
    assert!(!unbanned.has_tag("banned"));
}

#[test]
fn invalid_strategy_and_rebalance_mode_reject_with_frozen_messages() {
    let existing = PeerPolicy::default_for(PEER);

    let bad_strategy = PolicyUpdate {
        strategy: Some("aggressive".to_string()),
        ..Default::default()
    };
    let err = validate_policy_update(&existing, &bad_strategy, 1000).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid strategy 'aggressive'. Valid: ['dynamic', 'static', 'passive']"
    );

    let bad_mode = PolicyUpdate {
        rebalance_mode: Some("turbo".to_string()),
        ..Default::default()
    };
    let err = validate_policy_update(&existing, &bad_mode, 1000).unwrap_err();
    assert_eq!(
        err.to_string(),
        "Invalid rebalance_mode 'turbo'. Valid: ['enabled', 'disabled', 'source_only', 'sink_only']"
    );
}

#[test]
fn fee_multiplier_min_exceeds_max_is_rejected_l_r5_8() {
    let existing = PeerPolicy::default_for(PEER);
    let update = PolicyUpdate {
        fee_multiplier_min: Some(3.5),
        fee_multiplier_max: Some(1.5),
        ..Default::default()
    };
    let err = validate_policy_update(&existing, &update, 1000).unwrap_err();
    assert_eq!(
        err.to_string(),
        "fee_multiplier_min (3.5) cannot exceed fee_multiplier_max (1.5)"
    );
}

#[test]
fn fee_multiplier_bounds_reads_back_clamped_and_ordered() {
    let mut p = PeerPolicy::default_for(PEER);
    p.fee_multiplier_min = Some(9.0); // above global max
    p.fee_multiplier_max = Some(0.01); // below global min
                                       // Both clamp to the global max/min respectively, then get swapped
                                       // since (clamped) min (5.0) > (clamped) max (0.1).
    assert_eq!(p.fee_multiplier_bounds(), (0.1, 5.0));
}
