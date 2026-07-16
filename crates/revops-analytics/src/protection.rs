//! Lifecycle & protection ownership: "may this channel be closed, and why
//! not" — a pure port of `modules/protection_service.py`
//! (cl_revenue_ops-port), which itself is the extraction (Phase 3C, F5) of
//! `CapacityPlanner._close_protection_reason` /
//! `CapacityPlanner._check_close_allowed`'s policy-evaluation core, with
//! semantics fixed at baseline commit `5e8f747`.
//!
//! Decisions here are pure and evidence-injected: no plugin/RPC/DB/clock.
//! The vendored `fixtures/golden/close_protection/*.json` fixtures (ported
//! byte-for-byte from `tests/golden/fixtures/close_protection/` in the
//! Python repo) are this extraction's parity oracle and are replayed in
//! `tests/protection.rs`.
//!
//! ## `ChannelRole` — a deliberate task-local duplicate
//!
//! Python's `protection_service.py` imports `ChannelRole` from
//! `.classification`. In this Rust port, `classification.rs` is owned by a
//! DIFFERENT parallel task (Wave 1 Task 2) running in its own git worktree;
//! per this task's file-discipline constraint, `classification.rs` must
//! not be touched here. `ChannelRole` is therefore redefined locally with
//! the exact same variants/values Task 2's interface spec calls for
//! (`InboundGateway`/`OutboundGateway`/`Balanced`/`Dormant`,
//! `"inbound_gateway"`/... wire values). Reconciling this duplicate (having
//! `protection.rs` reference `crate::classification::ChannelRole` instead)
//! is merge/follow-up work for whichever task lands last (T10's
//! conformance flip depends on both T2 and T5).
use std::collections::BTreeSet;

use revops_econ::snapshot::Protection;
use revops_econ::types::UnixTime;

/// Baseline thresholds, verbatim from `capacity_planner.py` / `5e8f747`
/// (`protection_service.py` lines 27-31).
pub const KALMAN_CONFIDENCE_FLOOR: f64 = 0.5;
pub const INBOUND_GATEWAY_ROI_FLOOR_PCT: f64 = -30.0;
pub const SOURCED_FEE_PROTECT_SATS: i64 = 100;
pub const SOURCED_FEE_ROI_FLOOR_PCT: f64 = -50.0;
pub const REVENUE_ROUTE_ROI_FLOOR_PCT: f64 = -30.0;

/// See the module-level doc comment: a task-local duplicate of Task 2's
/// `classification::ChannelRole`, pending merge-time reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelRole {
    InboundGateway,
    OutboundGateway,
    Balanced,
    Dormant,
}

impl ChannelRole {
    pub fn as_value(&self) -> &'static str {
        match self {
            ChannelRole::InboundGateway => "inbound_gateway",
            ChannelRole::OutboundGateway => "outbound_gateway",
            ChannelRole::Balanced => "balanced",
            ChannelRole::Dormant => "dormant",
        }
    }

    pub fn as_name(&self) -> &'static str {
        match self {
            ChannelRole::InboundGateway => "INBOUND_GATEWAY",
            ChannelRole::OutboundGateway => "OUTBOUND_GATEWAY",
            ChannelRole::Balanced => "BALANCED",
            ChannelRole::Dormant => "DORMANT",
        }
    }
}

/// Spec Workstream F5 lifecycle model (distinct from economic role) —
/// `protection_service.py`'s `ChannelLifecycle`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelLifecycle {
    Candidate,
    Opening,
    Evaluating,
    Productive,
    Protected,
    Underperforming,
    Recycling,
    Closing,
}

impl ChannelLifecycle {
    /// Python `.value` — also the wire vocabulary in
    /// `revops_econ::snapshot::LIFECYCLES` (uppercase there; lowercase
    /// here, matching Python's `Enum.value` vs `Enum.name` duality).
    pub fn as_value(&self) -> &'static str {
        match self {
            ChannelLifecycle::Candidate => "candidate",
            ChannelLifecycle::Opening => "opening",
            ChannelLifecycle::Evaluating => "evaluating",
            ChannelLifecycle::Productive => "productive",
            ChannelLifecycle::Protected => "protected",
            ChannelLifecycle::Underperforming => "underperforming",
            ChannelLifecycle::Recycling => "recycling",
            ChannelLifecycle::Closing => "closing",
        }
    }

    pub fn as_name(&self) -> &'static str {
        match self {
            ChannelLifecycle::Candidate => "CANDIDATE",
            ChannelLifecycle::Opening => "OPENING",
            ChannelLifecycle::Evaluating => "EVALUATING",
            ChannelLifecycle::Productive => "PRODUCTIVE",
            ChannelLifecycle::Protected => "PROTECTED",
            ChannelLifecycle::Underperforming => "UNDERPERFORMING",
            ChannelLifecycle::Recycling => "RECYCLING",
            ChannelLifecycle::Closing => "CLOSING",
        }
    }
}

/// Profitability evidence for [`close_protection_reason`] — makes
/// Python's duck-typed `prof: Any` (a `ChannelProfitability`-shaped
/// `SimpleNamespace` in the golden tests) explicit. `role_30d: None`
/// mirrors `getattr(prof, 'role_30d', None)` returning `None` (falls back
/// to `lifetime_role`, i.e. Python's `channel_role` attribute).
#[derive(Debug, Clone, Copy)]
pub struct ProtProfEvidence {
    pub role_30d: Option<ChannelRole>,
    pub lifetime_role: ChannelRole,
    pub marginal_roi_percent: f64,
    pub window_30d_available: bool,
    pub sourced_fee_30d_msat: i64,
    pub lifetime_sourced_fee_sats: i64,
    pub days_open: i64,
}

/// Flow evidence for [`close_protection_reason`] — makes Python's
/// duck-typed `flow_metrics: Any` explicit. `None` fields mirror a
/// `getattr(..., default)` miss; passing `flow: None` to
/// `close_protection_reason` mirrors Python's falsy `if flow_metrics:`
/// guard (no `flow_metrics` object at all -> Kalman gate skipped
/// entirely).
#[derive(Debug, Clone, Copy, Default)]
pub struct FlowEvidence {
    pub confidence: Option<f64>,
    pub forward_count: Option<i64>,
}

/// `inactivity_is_signal`: true when zero observed flow IS the evidence,
/// not missing data. A mature channel (older than the flow window plus a
/// 7-day buffer) with zero forwards across the entire window is
/// confidently inactive (F3). `None` inputs mirror Python's
/// `int(None)` raising `TypeError` -> caught -> `False` (stay
/// conservative, keep the gate).
pub fn inactivity_is_signal(
    forward_count: Option<i64>,
    days_open: Option<i64>,
    flow_window_days: i64,
) -> bool {
    let (Some(forwards), Some(age_days)) = (forward_count, days_open) else {
        return false;
    };
    forwards == 0 && age_days > flow_window_days + 7
}

/// Return why a channel must not be recommended for closure, or `None`.
/// Single source of truth for the protective closure gates; reason
/// strings are a pinned contract (see `fixtures/golden/close_protection/`).
///
/// Gate order (first match wins — the contract, verbatim from
/// `protection_service.py`):
/// 1. Kalman confidence — `flow` present, `confidence < 0.5`, NOT an
///    inactivity signal.
/// 2. Inbound-gateway role (30d window, falling back to lifetime) with
///    `marginal_roi_percent >= -30.0` (inclusive).
/// 3. Sourced-fee contribution — 30d window when `window_30d_available`
///    is *exactly* `true`, else lifetime fallback; `> 100` sats AND
///    `roi > -50.0` (both strict).
/// 4. Revenue-route pair membership with `roi > -30.0` (strict).
pub fn close_protection_reason(
    scid_display: &str,
    prof: &ProtProfEvidence,
    flow: Option<&FlowEvidence>,
    route_pair_channels: &BTreeSet<String>,
    flow_window_days: i64,
) -> Option<&'static str> {
    // --- Gate 1: Kalman confidence ---
    // Python: `if flow_metrics:` — an evidence object entirely absent
    // (`None`/falsy) skips this gate outright; it does not default
    // confidence to anything.
    if let Some(flow) = flow {
        // `confidence = getattr(flow_metrics, 'confidence', 1.0)` then
        // `confidence = confidence or 1.0` — TRUTHINESS PIN: Python
        // treats 0.0 as falsy, so an explicit confidence of EXACTLY 0.0
        // is silently replaced by 1.0 (no gate), same as a missing
        // attribute. This is mirrored verbatim, not "fixed" — see
        // `tests/protection.rs::confidence_zero_is_treated_as_no_gate`.
        let raw = flow.confidence.unwrap_or(1.0);
        let confidence = if raw == 0.0 { 1.0 } else { raw };
        if confidence < KALMAN_CONFIDENCE_FLOOR
            && !inactivity_is_signal(flow.forward_count, Some(prof.days_open), flow_window_days)
        {
            return Some("KALMAN_LOW_CONFIDENCE");
        }
    }

    // --- Gate 2: inbound-gateway role protection ---
    // Judged on the TRAILING 30d WINDOW (role_30d) when present, else the
    // lifetime role (audit F2 / 5e8f747).
    let role = prof.role_30d.unwrap_or(prof.lifetime_role);
    if role == ChannelRole::InboundGateway
        && prof.marginal_roi_percent >= INBOUND_GATEWAY_ROI_FLOOR_PCT
    {
        return Some("INBOUND_GATEWAY");
    }

    // --- Gate 3: sourced-fee protection ---
    // 30d window when available; lifetime is the fail-safe fallback
    // (missing data must not weaken protection). `window_30d_available
    // is True` is a STRICT bool check in Python (`is True`, not
    // truthiness) — mirrored by matching on `prof.window_30d_available
    // == true` (the field is already a `bool`, so this is definitionally
    // the same check).
    let sourced_fee_sats = if prof.window_30d_available {
        // Python: `int(sourced_fee_raw) // 1000` — floor division; the
        // divisor (1000) is positive, so `div_euclid` matches Python `//`
        // exactly per the Global Constraints floor-division rule.
        prof.sourced_fee_30d_msat.div_euclid(1000)
    } else {
        prof.lifetime_sourced_fee_sats
    };
    if sourced_fee_sats > SOURCED_FEE_PROTECT_SATS
        && prof.marginal_roi_percent > SOURCED_FEE_ROI_FLOOR_PCT
    {
        return Some("SOURCED_FEE_CONTRIBUTION");
    }

    // --- Gate 4: revenue-route pair protection ---
    // Channels on proven revenue routes have network value beyond their
    // individual ROI.
    if route_pair_channels.contains(scid_display)
        && prof.marginal_roi_percent > REVENUE_ROUTE_ROI_FLOOR_PCT
    {
        return Some("REVENUE_ROUTE");
    }

    None
}

/// Why an operator policy forbids auto-close, or `None`. Reason strings
/// are a pinned contract (close_protection goldens). Order — verbatim
/// from `protection_service.py::policy_close_block` — is static ->
/// passive -> protect -> no_close ("protect" is checked before
/// "no_close": if BOTH tags are present, "protect" wins; the golden
/// fixtures only pin single-tag cases, but this ordering is load-bearing
/// for any future multi-tag case). Protection strings contain a literal
/// U+2014 EM DASH, not a hyphen.
pub fn policy_close_block(
    strategy: &crate::policy::FeeStrategy,
    tags: &[String],
) -> Option<String> {
    use crate::policy::FeeStrategy;

    match strategy {
        FeeStrategy::Static => {
            return Some("Channel has static policy \u{2014} close blocked".to_string())
        }
        FeeStrategy::Passive => {
            return Some("Channel has passive policy \u{2014} close blocked".to_string())
        }
        FeeStrategy::Dynamic => {}
    }

    let has_tag = |tag: &str| tags.iter().any(|t| t == tag);
    if has_tag("protect") {
        return Some("Channel tagged 'protect' \u{2014} close blocked".to_string());
    }
    if has_tag("no_close") {
        return Some("Channel tagged 'no_close' \u{2014} close blocked".to_string());
    }
    None
}

/// An accepted LN+ swap obligates the channel to stay open for the swap's
/// duration: an owned, expiring [`Protection`] (invariant 6). `None`
/// inputs, or `opened_at <= 0` / `duration_months <= 0`, mirror Python's
/// `try: int(...) except (TypeError, ValueError): return None` followed
/// by the `start <= 0 or months <= 0` guard. Python's `swap_id` parameter
/// is accepted but entirely unused in the original function body, so it
/// is dropped here.
pub fn lnplus_contract_protection(
    opened_at: Option<i64>,
    duration_months: Option<i64>,
) -> Option<Protection> {
    let start = opened_at?;
    let months = duration_months?;
    if start <= 0 || months <= 0 {
        return None;
    }
    // expires = start + months * 30 * 86400; use checked arithmetic so a
    // pathological input can't panic on overflow (Python ints don't
    // overflow; failing safe to "no protection" is the closest Rust
    // analogue for an out-of-range result).
    let seconds_per_month = 30i64.checked_mul(86_400)?;
    let span = months.checked_mul(seconds_per_month)?;
    let expires = start.checked_add(span)?;
    let expires_at = UnixTime::new(expires).ok()?;
    Protection::new("lnplus_contract", "lnplus", Some(expires_at)).ok()
}

/// Every protection currently blocking closure, as owned typed data —
/// `protection_service.py::close_protections`. `policy` is `(strategy,
/// tags)` evidence (mirrors Python's `policy: Any = None` — `None` here
/// means "no policy gate applies", matching the `if policy is not None`
/// guard); `lnplus_obligation` is `(opened_at, duration_months)` evidence
/// (Python's `swap_id` is dropped — see [`lnplus_contract_protection`]).
#[allow(clippy::too_many_arguments)]
pub fn close_protections(
    scid_display: &str,
    prof: &ProtProfEvidence,
    flow: Option<&FlowEvidence>,
    route_pair_channels: &BTreeSet<String>,
    flow_window_days: i64,
    policy: Option<(&crate::policy::FeeStrategy, &[String])>,
    lnplus_obligation: Option<(Option<i64>, Option<i64>)>,
) -> Vec<Protection> {
    let mut protections = Vec::new();

    if let Some(reason) = close_protection_reason(
        scid_display,
        prof,
        flow,
        route_pair_channels,
        flow_window_days,
    ) {
        if let Ok(p) = Protection::new(reason, "close_protection", None) {
            protections.push(p);
        }
    }
    if let Some((strategy, tags)) = policy {
        if let Some(blocked) = policy_close_block(strategy, tags) {
            if let Ok(p) = Protection::new(blocked, "operator_policy", None) {
                protections.push(p);
            }
        }
    }
    if let Some((opened_at, duration_months)) = lnplus_obligation {
        if let Some(contract) = lnplus_contract_protection(opened_at, duration_months) {
            protections.push(contract);
        }
    }

    protections
}

/// v0 lifecycle derivation (grows with Workstream F5). Precedence:
/// closing-track states outrank protection outranks performance —
/// `recycling > opening > protected > underperforming > productive`.
pub fn derive_lifecycle(
    staged_for_close: bool,
    opening: bool,
    protections: &[Protection],
    underperforming: bool,
) -> ChannelLifecycle {
    if staged_for_close {
        return ChannelLifecycle::Recycling;
    }
    if opening {
        return ChannelLifecycle::Opening;
    }
    if !protections.is_empty() {
        return ChannelLifecycle::Protected;
    }
    if underperforming {
        return ChannelLifecycle::Underperforming;
    }
    ChannelLifecycle::Productive
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::FeeStrategy;

    fn prof(
        role_30d: Option<ChannelRole>,
        marginal_roi_percent: f64,
        window_30d_available: bool,
        sourced_fee_30d_msat: i64,
        lifetime_sourced_fee_sats: i64,
        days_open: i64,
    ) -> ProtProfEvidence {
        ProtProfEvidence {
            role_30d,
            lifetime_role: ChannelRole::Balanced,
            marginal_roi_percent,
            window_30d_available,
            sourced_fee_30d_msat,
            lifetime_sourced_fee_sats,
            days_open,
        }
    }

    fn flow(confidence: f64, forward_count: i64) -> FlowEvidence {
        FlowEvidence {
            confidence: Some(confidence),
            forward_count: Some(forward_count),
        }
    }

    // --- inactivity_is_signal ---

    #[test]
    fn inactivity_signal_zero_forwards_mature_channel() {
        assert!(inactivity_is_signal(Some(0), Some(100), 7));
    }

    #[test]
    fn inactivity_signal_false_when_forwards_nonzero() {
        assert!(!inactivity_is_signal(Some(5), Some(100), 7));
    }

    #[test]
    fn inactivity_signal_false_when_not_mature_enough() {
        // age must be STRICTLY greater than window + 7
        assert!(!inactivity_is_signal(Some(0), Some(14), 7));
        assert!(inactivity_is_signal(Some(0), Some(15), 7));
    }

    #[test]
    fn inactivity_signal_false_on_missing_data() {
        assert!(!inactivity_is_signal(None, Some(100), 7));
        assert!(!inactivity_is_signal(Some(0), None, 7));
    }

    // --- close_protection_reason: Kalman gate ---

    #[test]
    fn kalman_low_confidence_blocks_active_channel() {
        let p = prof(Some(ChannelRole::Balanced), -90.0, true, 0, 0, 100);
        let f = flow(0.2, 5);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            Some("KALMAN_LOW_CONFIDENCE")
        );
    }

    #[test]
    fn kalman_gate_bypassed_when_zero_forwards_is_the_signal() {
        let p = prof(Some(ChannelRole::Balanced), -90.0, true, 0, 0, 100);
        let f = flow(0.2, 0);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            None
        );
    }

    #[test]
    fn confidence_zero_is_treated_as_no_gate_truthiness_pin() {
        // `confidence or 1.0`: an explicit 0.0 confidence is Python-falsy
        // and gets replaced by 1.0 -- i.e. NO gate, even though 0.0 is
        // objectively the LOWEST possible confidence. This is a verbatim
        // port of the Python bug/quirk, not a fix.
        let p = prof(Some(ChannelRole::Balanced), -90.0, true, 0, 0, 5);
        let f = FlowEvidence {
            confidence: Some(0.0),
            forward_count: Some(5),
        };
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            None,
            "confidence == 0.0 must NOT trigger KALMAN_LOW_CONFIDENCE (truthiness pin)"
        );
    }

    #[test]
    fn no_flow_evidence_skips_kalman_gate_entirely() {
        let p = prof(Some(ChannelRole::Balanced), -90.0, true, 0, 0, 100);
        assert_eq!(
            close_protection_reason("scid", &p, None, &BTreeSet::new(), 7),
            None
        );
    }

    // --- close_protection_reason: inbound-gateway gate ---

    #[test]
    fn inbound_gateway_protected_when_roi_at_or_above_floor() {
        let p = prof(Some(ChannelRole::InboundGateway), -10.0, true, 0, 0, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            Some("INBOUND_GATEWAY")
        );
    }

    #[test]
    fn inbound_gateway_inclusive_boundary_at_floor() {
        let p = prof(Some(ChannelRole::InboundGateway), -30.0, true, 0, 0, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            Some("INBOUND_GATEWAY"),
            ">= -30.0 is inclusive"
        );
    }

    #[test]
    fn inbound_gateway_unprotected_when_deep_loser() {
        let p = prof(Some(ChannelRole::InboundGateway), -60.0, true, 0, 0, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            None
        );
    }

    #[test]
    fn gateway_role_falls_back_to_lifetime_when_30d_absent() {
        let mut p = prof(None, -10.0, true, 0, 0, 100);
        p.lifetime_role = ChannelRole::InboundGateway;
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            Some("INBOUND_GATEWAY")
        );
    }

    // --- close_protection_reason: sourced-fee gate ---

    #[test]
    fn sourced_fee_30d_protected() {
        let p = prof(Some(ChannelRole::Balanced), -20.0, true, 500_000, 0, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            Some("SOURCED_FEE_CONTRIBUTION")
        );
    }

    #[test]
    fn sourced_fee_lifetime_fallback_protected() {
        let p = prof(Some(ChannelRole::Balanced), -20.0, false, 0, 500, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            Some("SOURCED_FEE_CONTRIBUTION")
        );
    }

    #[test]
    fn stale_lifetime_sourcing_not_used_when_window_present_5e8f747_anchor() {
        // THE 5e8f747 fix: lifetime sourcing must not protect when the
        // 30d window exists and shows nothing.
        let p = prof(Some(ChannelRole::Balanced), -90.0, true, 0, 500, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            None
        );
    }

    #[test]
    fn fail_safe_fallback_no_window_lifetime_500_protected() {
        let p = prof(Some(ChannelRole::Balanced), -20.0, false, 0, 500, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            Some("SOURCED_FEE_CONTRIBUTION")
        );
    }

    #[test]
    fn sourced_fee_floor_division_matches_python() {
        // 1999 msat // 1000 == 1 sat -- not protected (needs > 100 sats).
        let p = prof(Some(ChannelRole::Balanced), -20.0, true, 1999, 0, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            None
        );
    }

    #[test]
    fn sourced_fee_strict_boundaries() {
        // exactly 100 sats: NOT protected (`> 100` strict)
        let p = prof(Some(ChannelRole::Balanced), -20.0, true, 100_999, 0, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            None
        );
        // 101 sats: protected
        let p2 = prof(Some(ChannelRole::Balanced), -20.0, true, 101_000, 0, 100);
        assert_eq!(
            close_protection_reason("scid", &p2, Some(&f), &BTreeSet::new(), 7),
            Some("SOURCED_FEE_CONTRIBUTION")
        );
        // roi exactly -50.0: NOT protected (`> -50.0` strict)
        let p3 = prof(Some(ChannelRole::Balanced), -50.0, true, 500_000, 0, 100);
        assert_eq!(
            close_protection_reason("scid", &p3, Some(&f), &BTreeSet::new(), 7),
            None
        );
    }

    // --- close_protection_reason: route-pair gate ---

    #[test]
    fn revenue_route_pair_protected() {
        let p = prof(Some(ChannelRole::Balanced), -10.0, true, 0, 0, 100);
        let f = flow(0.9, 50);
        let mut set = BTreeSet::new();
        set.insert("scid".to_string());
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &set, 7),
            Some("REVENUE_ROUTE")
        );
    }

    #[test]
    fn revenue_route_pair_strict_roi_boundary() {
        let p = prof(Some(ChannelRole::Balanced), -30.0, true, 0, 0, 100);
        let f = flow(0.9, 50);
        let mut set = BTreeSet::new();
        set.insert("scid".to_string());
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &set, 7),
            None,
            "> -30.0 is strict"
        );
    }

    #[test]
    fn unprotected_dead_channel() {
        let p = prof(Some(ChannelRole::Balanced), -90.0, true, 0, 0, 100);
        let f = flow(0.9, 50);
        assert_eq!(
            close_protection_reason("scid", &p, Some(&f), &BTreeSet::new(), 7),
            None
        );
    }

    // --- policy_close_block ---

    #[test]
    fn policy_block_dynamic_no_tags_allowed() {
        assert_eq!(policy_close_block(&FeeStrategy::Dynamic, &[]), None);
    }

    #[test]
    fn policy_block_static() {
        assert_eq!(
            policy_close_block(&FeeStrategy::Static, &[]),
            Some("Channel has static policy \u{2014} close blocked".to_string())
        );
    }

    #[test]
    fn policy_block_passive() {
        assert_eq!(
            policy_close_block(&FeeStrategy::Passive, &[]),
            Some("Channel has passive policy \u{2014} close blocked".to_string())
        );
    }

    #[test]
    fn policy_block_no_close_tag() {
        assert_eq!(
            policy_close_block(&FeeStrategy::Dynamic, &["no_close".to_string()]),
            Some("Channel tagged 'no_close' \u{2014} close blocked".to_string())
        );
    }

    #[test]
    fn policy_block_protect_tag() {
        assert_eq!(
            policy_close_block(&FeeStrategy::Dynamic, &["protect".to_string()]),
            Some("Channel tagged 'protect' \u{2014} close blocked".to_string())
        );
    }

    #[test]
    fn policy_block_order_static_before_tags() {
        // Even with a protect tag, static strategy wins (order pinned).
        assert_eq!(
            policy_close_block(&FeeStrategy::Static, &["protect".to_string()]),
            Some("Channel has static policy \u{2014} close blocked".to_string())
        );
    }

    #[test]
    fn policy_block_protect_wins_over_no_close_when_both_present() {
        let tags = vec!["no_close".to_string(), "protect".to_string()];
        assert_eq!(
            policy_close_block(&FeeStrategy::Dynamic, &tags),
            Some("Channel tagged 'protect' \u{2014} close blocked".to_string()),
            "protect must be checked before no_close"
        );
    }

    // --- lnplus_contract_protection ---

    #[test]
    fn lnplus_expiry_start_plus_months_times_30_days() {
        let start = 1_700_000_000i64;
        let months = 6i64;
        let p = lnplus_contract_protection(Some(start), Some(months)).unwrap();
        assert_eq!(p.reason, "lnplus_contract");
        assert_eq!(p.owner, "lnplus");
        assert_eq!(p.expires_at.unwrap().value(), start + months * 30 * 86_400);
    }

    #[test]
    fn lnplus_none_on_missing_or_nonpositive_inputs() {
        assert!(lnplus_contract_protection(None, Some(1)).is_none());
        assert!(lnplus_contract_protection(Some(1), None).is_none());
        assert!(lnplus_contract_protection(Some(0), Some(1)).is_none());
        assert!(lnplus_contract_protection(Some(1), Some(0)).is_none());
        assert!(lnplus_contract_protection(Some(-5), Some(1)).is_none());
    }

    // --- close_protections ---

    #[test]
    fn close_protections_aggregates_all_sources() {
        let p = prof(Some(ChannelRole::InboundGateway), -10.0, true, 0, 0, 100);
        let f = flow(0.9, 50);
        let tags = vec!["protect".to_string()];
        let out = close_protections(
            "scid",
            &p,
            Some(&f),
            &BTreeSet::new(),
            7,
            Some((&FeeStrategy::Dynamic, &tags)),
            Some((Some(1_700_000_000), Some(1))),
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].owner, "close_protection");
        assert_eq!(out[0].reason, "INBOUND_GATEWAY");
        assert_eq!(out[1].owner, "operator_policy");
        assert_eq!(out[2].owner, "lnplus");
    }

    #[test]
    fn close_protections_empty_when_nothing_blocks() {
        let p = prof(Some(ChannelRole::Balanced), -90.0, true, 0, 0, 100);
        let f = flow(0.9, 50);
        let out = close_protections("scid", &p, Some(&f), &BTreeSet::new(), 7, None, None);
        assert!(out.is_empty());
    }

    // --- derive_lifecycle ---

    #[test]
    fn lifecycle_precedence_recycling_beats_everything() {
        let protections = vec![Protection::new("x", "y", None).unwrap()];
        assert_eq!(
            derive_lifecycle(true, true, &protections, true),
            ChannelLifecycle::Recycling
        );
    }

    #[test]
    fn lifecycle_precedence_opening_beats_protected_and_underperforming() {
        let protections = vec![Protection::new("x", "y", None).unwrap()];
        assert_eq!(
            derive_lifecycle(false, true, &protections, true),
            ChannelLifecycle::Opening
        );
    }

    #[test]
    fn lifecycle_precedence_protected_beats_underperforming() {
        let protections = vec![Protection::new("x", "y", None).unwrap()];
        assert_eq!(
            derive_lifecycle(false, false, &protections, true),
            ChannelLifecycle::Protected
        );
    }

    #[test]
    fn lifecycle_precedence_underperforming_beats_productive() {
        assert_eq!(
            derive_lifecycle(false, false, &[], true),
            ChannelLifecycle::Underperforming
        );
    }

    #[test]
    fn lifecycle_default_productive() {
        assert_eq!(
            derive_lifecycle(false, false, &[], false),
            ChannelLifecycle::Productive
        );
    }

    #[test]
    fn channel_lifecycle_values_and_names_match_wire_vocabulary() {
        assert_eq!(ChannelLifecycle::Candidate.as_value(), "candidate");
        assert_eq!(ChannelLifecycle::Candidate.as_name(), "CANDIDATE");
        assert_eq!(ChannelLifecycle::Closing.as_value(), "closing");
        assert_eq!(ChannelLifecycle::Closing.as_name(), "CLOSING");
    }
}
