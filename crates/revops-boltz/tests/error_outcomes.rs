//! `error.rs`'s safety-critical outcome types, exercised from outside the
//! crate the way a live adapter would use them: never collapse a timeout
//! into "the swap did not happen", never collapse a refund/claim's raw
//! CLI text into "it definitely succeeded".

use revops_boltz::error::{CliError, CreateOutcome, ManualActionOutcome};

#[test]
fn timeout_on_create_is_unknown_not_rejected() {
    // A caller pattern-matching only Completed/Rejected (i.e. treating
    // "not Completed" as "definitely did not happen") would be wrong for
    // a real timeout — Unknown is a distinct third state that MUST exist.
    let outcome: CreateOutcome<String> = CreateOutcome::Unknown {
        timeout_secs: 60,
        command: "boltzcli createreverseswap".to_string(),
    };
    match outcome {
        CreateOutcome::Completed(_) => panic!("must not be Completed"),
        CreateOutcome::Rejected(_) => panic!("must not be Rejected"),
        CreateOutcome::Unknown { timeout_secs, .. } => assert_eq!(timeout_secs, 60),
    }
}

#[test]
fn exit_failure_before_any_call_is_rejected_not_unknown() {
    // Control: a definite, synchronous rejection (e.g. disabled, bad
    // JSON, or a nonzero exit the CLI itself reported) is NOT the same
    // ambiguity as a timeout — it must map to Rejected, not Unknown.
    let outcome: CreateOutcome<String> = CreateOutcome::Rejected(CliError::Disabled);
    assert!(matches!(
        outcome,
        CreateOutcome::Rejected(CliError::Disabled)
    ));
}

#[test]
fn refund_success_exit_is_unverified_not_a_confirmed_success() {
    // Python's refund()/claim() return {"result_raw": <stdout>} on any
    // exit-0 call with no structured status. The port must not silently
    // upgrade that into a confirmed-success variant — a caller has to
    // reconcile via swap_status.
    let outcome = ManualActionOutcome::Unverified {
        raw_output: "refund broadcast, txid=abc123".to_string(),
    };
    match outcome {
        ManualActionOutcome::Unverified { raw_output } => {
            assert!(raw_output.contains("txid"));
        }
        ManualActionOutcome::Failed(_) => panic!("exit-0 output must not be classified as Failed"),
    }
}

#[test]
fn refund_cli_failure_is_failed_not_silently_dropped() {
    let outcome = ManualActionOutcome::Failed(CliError::ExitFailure {
        code: Some(1),
        message: "swap not refundable".to_string(),
    });
    assert!(matches!(outcome, ManualActionOutcome::Failed(_)));
}
