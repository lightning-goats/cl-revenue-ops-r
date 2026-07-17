//! Error-string contracts: centralized constants + the failure classifier.
//! Substring/prefix-matched by the engine's failure classifier, the audit,
//! and the defibrillator (Global Constraints, phase5 plan). Never retyped
//! inline anywhere else in this crate.
//!
//! Citations below are to `~/bin/cl_revenue_ops-port` (branch `port`),
//! `modules/`.

/// `rebalancer.py:1495,1505,1884` (bare form); `rebalance_engine_v2.py:1919,
/// 1969,1986,2108` (`"local_budget_block: {reason}"` form); the bare-prefix
/// check lives at `rebalance_engine_v2.py:2883`
/// (`error.startswith(("local_budget_block", "zero_budget_blocks"))`).
pub const LOCAL_BUDGET_BLOCK: &str = "local_budget_block";
/// `rebalance_engine_v2.py:1910`.
pub const ZERO_BUDGET_BLOCKS_AUTO_REBALANCE: &str = "zero_budget_blocks_auto_rebalance";
/// `rebalance_engine_v2.py:3225,3231` (`execute_candidate`'s non-blocking
/// cycle-lock acquire).
pub const ENGINE_BUSY: &str = "engine_busy";
/// `rebalance_engine_v2.py:1282,3271` (P4-008 in-flight-dest guard).
pub const DEST_INFLIGHT: &str = "dest_inflight";
/// `rebalance_engine_v2.py:3369,3377` (`run_cycle`'s non-blocking cycle-lock
/// acquire).
pub const CYCLE_ALREADY_RUNNING: &str = "cycle_already_running";
/// `rebalance_engine_v2.py:3314,3336`. Confirmed: v2.18.1's early-return on
/// pricing failure IS present in this Python at both the pricing-exception
/// site (3312-3317) and the `route_result.success == False` site
/// (3333-3341) — `_execute_candidate_locked` returns immediately in both
/// cases rather than falling through to `_execute_pair` with `route=None`.
pub const ROUTE_PRICING_FAILED_PREFIX: &str = "route_pricing_failed: ";
/// `rebalance_native_executor_v2.py:421` (`prefix = ... "native_route_invalid"`).
pub const NATIVE_ROUTE_INVALID_PREFIX: &str = "native_route_invalid: ";
/// `rebalance_native_executor_v2.py:421` (`prefix = "native_route_over_budget"
/// if reason.startswith("route_over_budget") else ...`).
pub const NATIVE_ROUTE_OVER_BUDGET_PREFIX: &str = "native_route_over_budget: ";
/// `rebalance_native_executor_v2.py:525`.
pub const NATIVE_SENDPAY_ERROR_PREFIX: &str = "native_sendpay_error: ";
/// `rebalance_native_executor_v2.py:500`
/// (`result.error = f"payment_pending_timeout: {error_text}"`).
pub const PAYMENT_PENDING_TIMEOUT_PREFIX: &str = "payment_pending_timeout: ";
/// `rebalance_router_v3.py:110,480`; `rebalance_engine_v2.py:432,1555,1561,
/// 1566` (rejection/route-status marker).
pub const NO_ROUTE: &str = "no_route";
/// `rebalance_engine_v2.py:179` (cooldown table key), `1719-1720`
/// (`_classify_failure_kind`), `2521`.
pub const TEMPORARY_CHANNEL_FAILURE: &str = "temporary_channel_failure";
/// `rebalance_engine_v2.py:180,1725-1726`.
pub const FEE_INSUFFICIENT: &str = "fee_insufficient";
/// `rebalance_engine_v2.py:181,1727-1728`.
pub const INCORRECT_CLTV_EXPIRY: &str = "incorrect_cltv_expiry";
/// `rebalance_engine_v2.py:182,1729-1730`.
pub const PERMANENT_FAILURE: &str = "permanent_failure";
/// `rebalance_engine_v2.py:2116,2131`.
pub const GOVERNOR_BLOCK_PREFIX: &str = "governor_block:";

/// Failure-kind classification, port of `rebalance_engine_v2.py:1717-1735`
/// (`_classify_failure_kind`), EXTENDED to also recognize the bare
/// [`NO_ROUTE`] contract string as [`FailureKind::TemporaryChannelFailure`]
/// (the plan's explicit contract; Python's own substring set — `"noroutes"`,
/// `"no_routes"`, `"no route"` — does not literally match the canonical
/// `"no_route"` singular/underscore spelling used as the rejection marker
/// elsewhere in the same file, e.g. lines 1555/1566/2583; the module's own
/// comment — "Treat NoRoutes as transient" — makes the intent unambiguous,
/// so this port closes that gap rather than reproducing it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    TemporaryChannelFailure,
    FeeInsufficient,
    IncorrectCltv,
    Permanent,
    PaymentPendingTimeout,
    LocalOrOther,
}

/// Port of `_classify_failure_kind` (`rebalance_engine_v2.py:1717-1735`),
/// checked in Python's exact order (case-insensitive substring match).
pub fn classify_failure(error: &str) -> FailureKind {
    let error = error.to_lowercase();
    if error.contains(TEMPORARY_CHANNEL_FAILURE) {
        return FailureKind::TemporaryChannelFailure;
    }
    if error.contains("noroutes")
        || error.contains("no_routes")
        || error.contains("no route")
        || error.contains(NO_ROUTE)
    {
        return FailureKind::TemporaryChannelFailure;
    }
    if error.contains(FEE_INSUFFICIENT) {
        return FailureKind::FeeInsufficient;
    }
    if error.contains(INCORRECT_CLTV_EXPIRY) {
        return FailureKind::IncorrectCltv;
    }
    if error.contains(PERMANENT_FAILURE) {
        return FailureKind::Permanent;
    }
    if error.contains("payment_pending_timeout") {
        return FailureKind::PaymentPendingTimeout;
    }
    FailureKind::LocalOrOther
}

/// Port of the `_pair_failure_cooldowns` table
/// (`rebalance_engine_v2.py:178-186`); `LocalOrOther` merges Python's
/// `local_execution_failed` and `other_retriable` keys, both `600`.
pub fn cooldown_base_secs(kind: FailureKind) -> i64 {
    match kind {
        FailureKind::TemporaryChannelFailure => 300,
        FailureKind::FeeInsufficient => 1800,
        FailureKind::IncorrectCltv => 3600,
        FailureKind::Permanent => 21_600,
        FailureKind::PaymentPendingTimeout => 3600,
        FailureKind::LocalOrOther => 600,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_failure_matches_contract_strings() {
        let cases: &[(&str, FailureKind)] = &[
            (LOCAL_BUDGET_BLOCK, FailureKind::LocalOrOther),
            (ZERO_BUDGET_BLOCKS_AUTO_REBALANCE, FailureKind::LocalOrOther),
            (ENGINE_BUSY, FailureKind::LocalOrOther),
            (DEST_INFLIGHT, FailureKind::LocalOrOther),
            (CYCLE_ALREADY_RUNNING, FailureKind::LocalOrOther),
            (ROUTE_PRICING_FAILED_PREFIX, FailureKind::LocalOrOther),
            (NATIVE_ROUTE_INVALID_PREFIX, FailureKind::LocalOrOther),
            (NATIVE_ROUTE_OVER_BUDGET_PREFIX, FailureKind::LocalOrOther),
            (NATIVE_SENDPAY_ERROR_PREFIX, FailureKind::LocalOrOther),
            (
                PAYMENT_PENDING_TIMEOUT_PREFIX,
                FailureKind::PaymentPendingTimeout,
            ),
            (NO_ROUTE, FailureKind::TemporaryChannelFailure),
            (
                TEMPORARY_CHANNEL_FAILURE,
                FailureKind::TemporaryChannelFailure,
            ),
            (FEE_INSUFFICIENT, FailureKind::FeeInsufficient),
            (INCORRECT_CLTV_EXPIRY, FailureKind::IncorrectCltv),
            (PERMANENT_FAILURE, FailureKind::Permanent),
            (GOVERNOR_BLOCK_PREFIX, FailureKind::LocalOrOther),
        ];
        for (input, expected) in cases {
            assert_eq!(classify_failure(input), *expected, "input={input:?}");
        }
    }

    #[test]
    fn classify_failure_prefixed_detail_still_classifies() {
        // A prefix constant with real appended detail that itself contains a
        // contract substring still classifies on that substring (matches
        // Python: the whole lowercased string is substring-matched).
        assert_eq!(
            classify_failure("payment_pending_timeout: waitsendpay code 200"),
            FailureKind::PaymentPendingTimeout
        );
        assert_eq!(
            classify_failure("native_sendpay_error: WIRE_TEMPORARY_CHANNEL_FAILURE"),
            FailureKind::TemporaryChannelFailure
        );
    }

    #[test]
    fn cooldown_base_secs_table() {
        assert_eq!(
            cooldown_base_secs(FailureKind::TemporaryChannelFailure),
            300
        );
        assert_eq!(cooldown_base_secs(FailureKind::FeeInsufficient), 1800);
        assert_eq!(cooldown_base_secs(FailureKind::IncorrectCltv), 3600);
        assert_eq!(cooldown_base_secs(FailureKind::Permanent), 21_600);
        assert_eq!(cooldown_base_secs(FailureKind::PaymentPendingTimeout), 3600);
        assert_eq!(cooldown_base_secs(FailureKind::LocalOrOther), 600);
    }
}
