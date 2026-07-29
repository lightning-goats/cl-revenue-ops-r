//! Task 60 slice 2: the four-way typed submission-outcome rail.
//!
//! Classification consumes ONLY the executor's `ExecutionResult` encoding
//! (prefix-typed error strings, `payment_pending`, `success`) and maps
//! every terminal onto the durable settle vocabulary. Ambiguity defaults
//! to OUTCOME UNKNOWN, which retains its reservation (quarantined) and is
//! structurally unable to resubmit.

use revops::rebalance_execution::{classify_execution, settlement_for, RebalanceSubmitOutcome};
use revops_db::owner::spawn_read_write;
use revops_rebalance::errors::{
    NATIVE_ROUTE_OVER_BUDGET_PREFIX, NATIVE_SENDPAY_ERROR_PREFIX, PAYMENT_PENDING_TIMEOUT_PREFIX,
};
use revops_rebalance::executor::DRYRUN_GATE_SENDPAY_DISABLED;
use revops_rebalance::types::ExecutionResult;
use serde_json::json;

fn base_result() -> ExecutionResult {
    ExecutionResult {
        success: false,
        attempts: 1,
        fee_sats: 0,
        fee_msat: 0,
        fee_ppm: 0,
        hops: 3,
        parts: 1,
        error: None,
        amount_sats: 250_000,
        payment_pending: false,
        payment_hash: None,
        excluded_channels: Vec::new(),
        route_type: "native",
        failure_data: json!({}),
    }
}

#[test]
fn classification_covers_all_four_arms_and_defaults_unknown() {
    // Success.
    let mut ok = base_result();
    ok.success = true;
    ok.fee_sats = 42;
    ok.fee_msat = 41_500;
    assert!(matches!(
        classify_execution(&ok),
        RebalanceSubmitOutcome::Success {
            fee_sats: 42,
            fee_msat: 41_500,
            hops: 3,
            parts: 1
        }
    ));

    // Pending -> unknown, hash carried.
    let mut pending = base_result();
    pending.payment_pending = true;
    pending.payment_hash = Some("abc123".into());
    pending.error = Some(format!("{PAYMENT_PENDING_TIMEOUT_PREFIX}code 200"));
    match classify_execution(&pending) {
        RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit {
            payment_hash,
            detail,
        } => {
            assert_eq!(payment_hash.as_deref(), Some("abc123"));
            assert!(detail.contains("code 200"));
        }
        other => panic!("pending must be UNKNOWN, got {other:?}"),
    }

    // Pre-write prefixes -> clean failure (dryrun gate + route budget).
    for pre_write in [
        DRYRUN_GATE_SENDPAY_DISABLED.to_string(),
        format!("{NATIVE_ROUTE_OVER_BUDGET_PREFIX}route_over_budget fee=9"),
    ] {
        let mut clean = base_result();
        clean.error = Some(pre_write.clone());
        assert!(
            matches!(
                classify_execution(&clean),
                RebalanceSubmitOutcome::CleanFailureBeforeWrite { .. }
            ),
            "{pre_write} must classify clean"
        );
    }

    // Terminal sendpay failure with proof -> rejected.
    let mut rejected = base_result();
    rejected.error = Some(format!(
        "{NATIVE_SENDPAY_ERROR_PREFIX}WIRE_TEMPORARY_CHANNEL_FAILURE"
    ));
    assert!(matches!(
        classify_execution(&rejected),
        RebalanceSubmitOutcome::Rejected { .. }
    ));

    // An unrecognized shape (no success, no pending, unprefixed error)
    // must DEFAULT to unknown -- never to a clean failure.
    let mut weird = base_result();
    weird.error = Some("some future error shape".into());
    assert!(matches!(
        classify_execution(&weird),
        RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
    ));
    let bare = base_result();
    assert!(matches!(
        classify_execution(&bare),
        RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
    ));
}

/// The unknown arm's durable consequence: reservation QUARANTINED (still
/// counted against the budget window), attempt terminal as
/// `outcome_unknown` -- and the module is structurally execution-free, so
/// a resubmit cannot even be expressed from the settlement layer.
#[tokio::test]
async fn unknown_outcome_retains_reservation_and_never_resubmits() {
    let dir = tempfile::tempdir().unwrap();
    let handle = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    handle
        .insert_rebalance_attempt(revops_db::fee_runway::RebalanceAttemptIntent {
            request_id: "rb-u1".into(),
            source_channel: "100x1x0".into(),
            dest_channel: "200x2x0".into(),
            amount_sats: 250_000,
            max_fee_sats: 300,
            trigger: "cycle".into(),
            submitted_at: 1_800_000_100,
        })
        .await
        .unwrap();

    let mut pending = base_result();
    pending.payment_pending = true;
    pending.payment_hash = Some("hash-u1".into());
    pending.error = Some(format!("{PAYMENT_PENDING_TIMEOUT_PREFIX}code 200"));
    let outcome = classify_execution(&pending);
    let settle = settlement_for(&outcome, "rb-u1", 1_800_000_200);
    assert_eq!(settle.outcome, "outcome_unknown");
    assert_eq!(settle.reservation_status, "quarantined");
    assert_eq!(settle.payment_hash.as_deref(), Some("hash-u1"));

    handle.settle_rebalance_attempt(settle).await.unwrap();
    assert!(
        handle
            .unresolved_rebalance_attempts()
            .await
            .unwrap()
            .is_empty(),
        "the unknown terminal IS recorded"
    );
    assert_eq!(
        handle
            .active_rebalance_reserved_sats(1_800_000_000)
            .await
            .unwrap(),
        250_000,
        "the quarantined reservation keeps holding the budget"
    );

    // Structural no-resubmit: the settlement layer cannot name the
    // execution surface at all.
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/rebalance_execution.rs"
    ))
    .unwrap();
    // Forbid the CALLABLE execution surface (trait/entry-point names and
    // method-call shapes), not incidental words -- the module legitimately
    // names the executor's error-prefix constants in its classification.
    for forbidden in [
        "execute_candidate",
        "PaymentRpc",
        "run_cycle",
        "NativeRouteExecutor",
        ".sendpay(",
        ".invoice(",
    ] {
        assert!(
            !source.contains(forbidden),
            "rebalance_execution.rs must be execution-free (found `{forbidden}`)"
        );
    }
}

/// The other three settlement mappings: success settles with the paid
/// fee, rejection and clean failure both RELEASE the reservation.
#[test]
fn settlement_mapping_for_terminal_outcomes() {
    let ok = RebalanceSubmitOutcome::Success {
        fee_sats: 42,
        fee_msat: 41_500,
        hops: 3,
        parts: 1,
    };
    let settle = settlement_for(&ok, "rb-s", 1_800_000_300);
    assert_eq!(settle.outcome, "success");
    assert_eq!(settle.reservation_status, "settled");
    assert_eq!(settle.fee_paid_sats, Some(42));

    let rejected = RebalanceSubmitOutcome::Rejected {
        detail: "native_sendpay_error: WIRE_FEE_INSUFFICIENT".into(),
    };
    let settle = settlement_for(&rejected, "rb-r", 1_800_000_300);
    assert_eq!(settle.outcome, "rejected");
    assert_eq!(settle.reservation_status, "released");
    assert_eq!(settle.fee_paid_sats, None);

    let clean = RebalanceSubmitOutcome::CleanFailureBeforeWrite {
        detail: DRYRUN_GATE_SENDPAY_DISABLED.into(),
    };
    let settle = settlement_for(&clean, "rb-c", 1_800_000_300);
    assert_eq!(settle.outcome, "clean_failure_before_write");
    assert_eq!(settle.reservation_status, "released");
}
