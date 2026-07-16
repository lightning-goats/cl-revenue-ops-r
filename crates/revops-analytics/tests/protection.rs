//! Golden-fixture replay for `revops_analytics::protection`, against the
//! vendored `fixtures/golden/close_protection/*.json` (byte-for-byte
//! copies of `tests/golden/fixtures/close_protection/` in
//! `cl_revenue_ops-port`). Evidence tables below are transcribed from
//! `tests/golden/test_golden_close_protection.py::SCENARIOS` and
//! `CLOSE_ALLOWED_SCENARIOS` in that same repo, and are the parity oracle
//! for this extraction (semantics fixed at baseline commit `5e8f747`).

use std::collections::BTreeSet;
use std::path::PathBuf;

use revops_analytics::policy::FeeStrategy;
use revops_analytics::protection::{
    close_protection_reason, policy_close_block, ChannelRole, FlowEvidence, ProtProfEvidence,
};

const SCID: &str = "111x222x0";
const FLOW_WINDOW_DAYS: i64 = 7;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/golden/close_protection")
        .join(format!("{name}.json"))
}

fn fixture_reason(name: &str) -> Option<String> {
    let text = std::fs::read_to_string(fixture_path(name))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"));
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    v.get("reason").and_then(|r| r.as_str()).map(str::to_string)
}

fn fixture_allowed(name: &str) -> (bool, Option<String>) {
    let text = std::fs::read_to_string(fixture_path(name))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"));
    let v: serde_json::Value = serde_json::from_str(&text).unwrap();
    let allowed = v
        .get("allowed")
        .and_then(|a| a.as_bool())
        .expect("allowed field");
    let reason = v.get("reason").and_then(|r| r.as_str()).map(str::to_string);
    (allowed, reason)
}

/// Mirrors `test_golden_close_protection.py::_prof`'s defaults exactly:
/// `role_30d=BALANCED, marginal_roi_percent=0.0, window_30d_available=True,
/// sourced_fee_30d_msat=0, lifetime_sourced_fee_sats=0, days_open=100`.
/// `channel_role` (Python's lifetime-role attribute) is always `BALANCED`
/// in every golden scenario, so `lifetime_role` is fixed here too.
fn prof(
    role_30d: ChannelRole,
    marginal_roi_percent: f64,
    window_30d_available: bool,
    sourced_fee_30d_msat: i64,
    lifetime_sourced_fee_sats: i64,
    days_open: i64,
) -> ProtProfEvidence {
    ProtProfEvidence {
        role_30d: Some(role_30d),
        lifetime_role: ChannelRole::Balanced,
        marginal_roi_percent,
        window_30d_available,
        sourced_fee_30d_msat,
        lifetime_sourced_fee_sats,
        days_open,
    }
}

/// Mirrors `_flow`'s defaults: `confidence=0.9, forward_count=50`.
fn flow(confidence: f64, forward_count: i64) -> FlowEvidence {
    FlowEvidence {
        confidence: Some(confidence),
        forward_count: Some(forward_count),
    }
}

// --- close_protection_reason scenarios (SCENARIOS dict, golden fixtures
// without the `allowed_` prefix) ---

#[test]
fn golden_unprotected_dead_channel() {
    let p = prof(ChannelRole::Balanced, -90.0, true, 0, 0, 100);
    let f = flow(0.9, 50);
    let reason = close_protection_reason(SCID, &p, Some(&f), &BTreeSet::new(), FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("unprotected_dead_channel")
    );
}

#[test]
fn golden_gateway_30d_protected() {
    let p = prof(ChannelRole::InboundGateway, -10.0, true, 0, 0, 100);
    let f = flow(0.9, 50);
    let reason = close_protection_reason(SCID, &p, Some(&f), &BTreeSet::new(), FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("gateway_30d_protected")
    );
    assert_eq!(reason, Some("INBOUND_GATEWAY"));
}

#[test]
fn golden_gateway_30d_but_deep_loser_unprotected() {
    let p = prof(ChannelRole::InboundGateway, -60.0, true, 0, 0, 100);
    let f = flow(0.9, 50);
    let reason = close_protection_reason(SCID, &p, Some(&f), &BTreeSet::new(), FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("gateway_30d_but_deep_loser_unprotected")
    );
    assert_eq!(reason, None);
}

#[test]
fn golden_sourced_fee_30d_protected() {
    let p = prof(ChannelRole::Balanced, -20.0, true, 500_000, 0, 100);
    let f = flow(0.9, 50);
    let reason = close_protection_reason(SCID, &p, Some(&f), &BTreeSet::new(), FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("sourced_fee_30d_protected")
    );
    assert_eq!(reason, Some("SOURCED_FEE_CONTRIBUTION"));
}

#[test]
fn golden_sourced_fee_lifetime_fallback_protected() {
    let p = prof(ChannelRole::Balanced, -20.0, false, 0, 500, 100);
    let f = flow(0.9, 50);
    let reason = close_protection_reason(SCID, &p, Some(&f), &BTreeSet::new(), FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("sourced_fee_lifetime_fallback_protected")
    );
    assert_eq!(reason, Some("SOURCED_FEE_CONTRIBUTION"));
}

#[test]
fn golden_stale_lifetime_sourcing_not_used_when_window_present() {
    // THE 5e8f747 anchor: empty 30d window + rich lifetime sourcing must
    // NOT protect.
    let p = prof(ChannelRole::Balanced, -90.0, true, 0, 500, 100);
    let f = flow(0.9, 50);
    let reason = close_protection_reason(SCID, &p, Some(&f), &BTreeSet::new(), FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("stale_lifetime_sourcing_not_used_when_window_present")
    );
    assert_eq!(reason, None);
}

#[test]
fn golden_low_confidence_active_channel_gated() {
    let p = prof(ChannelRole::Balanced, -90.0, true, 0, 0, 100);
    let f = flow(0.2, 5);
    let reason = close_protection_reason(SCID, &p, Some(&f), &BTreeSet::new(), FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("low_confidence_active_channel_gated")
    );
    assert_eq!(reason, Some("KALMAN_LOW_CONFIDENCE"));
}

#[test]
fn golden_low_confidence_dead_mature_inactivity_is_signal() {
    // F3: zero forwards over a full window on a mature channel IS the
    // signal -- the confidence gate must not block closure.
    let p = prof(ChannelRole::Balanced, -90.0, true, 0, 0, 100);
    let f = flow(0.2, 0);
    let reason = close_protection_reason(SCID, &p, Some(&f), &BTreeSet::new(), FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("low_confidence_dead_mature_inactivity_is_signal")
    );
    assert_eq!(reason, None);
}

#[test]
fn golden_revenue_route_pair_protected() {
    let p = prof(ChannelRole::Balanced, -10.0, true, 0, 0, 100);
    let f = flow(0.9, 50);
    let mut route_pairs = BTreeSet::new();
    route_pairs.insert(SCID.to_string());
    let reason = close_protection_reason(SCID, &p, Some(&f), &route_pairs, FLOW_WINDOW_DAYS);
    assert_eq!(
        reason.map(str::to_string),
        fixture_reason("revenue_route_pair_protected")
    );
    assert_eq!(reason, Some("REVENUE_ROUTE"));
}

// --- policy_close_block scenarios (CLOSE_ALLOWED_SCENARIOS, `allowed_*`
// golden fixtures). The golden's `(allowed, reason)` shape maps onto
// `policy_close_block`'s `Option<String>` as `(reason.is_none(), reason)`.

fn allowed_and_reason(strategy: FeeStrategy, tags: &[&str]) -> (bool, Option<String>) {
    let tags: Vec<String> = tags.iter().map(|s| s.to_string()).collect();
    let blocked = policy_close_block(&strategy, &tags);
    (blocked.is_none(), blocked)
}

#[test]
fn golden_allowed_dynamic_no_tags_allowed() {
    let (allowed, reason) = allowed_and_reason(FeeStrategy::Dynamic, &[]);
    let (fixture_allowed, fixture_reason) = fixture_allowed("allowed_dynamic_no_tags_allowed");
    assert_eq!(allowed, fixture_allowed);
    assert!(allowed);
    assert_eq!(reason, None);
    // The golden's non-blocked reason text ("Close allowed") is the
    // capacity planner's fail-open wrapper message, produced by the
    // caller shim (`_check_close_allowed`), not by `policy_close_block`
    // itself (which returns `None` when nothing blocks) -- so only
    // `allowed` is compared for this scenario, and `fixture_reason` is
    // asserted to be that wrapper text for documentation purposes.
    assert_eq!(fixture_reason.as_deref(), Some("Close allowed"));
}

#[test]
fn golden_allowed_static_policy_blocks() {
    let (allowed, reason) = allowed_and_reason(FeeStrategy::Static, &[]);
    let (fixture_allowed, fixture_reason) = fixture_allowed("allowed_static_policy_blocks");
    assert_eq!(allowed, fixture_allowed);
    assert_eq!(reason, fixture_reason);
}

#[test]
fn golden_allowed_passive_policy_blocks() {
    let (allowed, reason) = allowed_and_reason(FeeStrategy::Passive, &[]);
    let (fixture_allowed, fixture_reason) = fixture_allowed("allowed_passive_policy_blocks");
    assert_eq!(allowed, fixture_allowed);
    assert_eq!(reason, fixture_reason);
}

#[test]
fn golden_allowed_no_close_tag_blocks() {
    let (allowed, reason) = allowed_and_reason(FeeStrategy::Dynamic, &["no_close"]);
    let (fixture_allowed, fixture_reason) = fixture_allowed("allowed_no_close_tag_blocks");
    assert_eq!(allowed, fixture_allowed);
    assert_eq!(reason, fixture_reason);
}

#[test]
fn golden_allowed_protect_tag_blocks() {
    let (allowed, reason) = allowed_and_reason(FeeStrategy::Dynamic, &["protect"]);
    let (fixture_allowed, fixture_reason) = fixture_allowed("allowed_protect_tag_blocks");
    assert_eq!(allowed, fixture_allowed);
    assert_eq!(reason, fixture_reason);
}

/// All 14 vendored fixtures are exercised above; this just asserts the
/// vendored set hasn't silently grown/shrunk without a matching test.
#[test]
fn all_fourteen_fixtures_present() {
    let dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/golden/close_protection");
    let count = std::fs::read_dir(&dir).unwrap().count();
    assert_eq!(
        count, 14,
        "expected all 14 close_protection golden fixtures vendored in {dir:?}"
    );
}
