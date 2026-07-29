//! Task 63 slice 3: the four-way Boltz outcome vocabulary, its
//! settlement mapping, the fail-closed classifiers, and the capability/
//! secret discipline.

use revops::boltz_boundaries::{
    classify_boltz_create, classify_boltz_manual, settlement_for_boltz, BoltzSubmitOutcome,
    MnemonicSecret,
};
use revops_boltz::error::{CliError, CreateOutcome, ManualActionOutcome};
use serde_json::json;

const NOW: i64 = 1_800_000_000;

/// Create classification is fail-closed: only an id-bearing non-error
/// reply commits; only provably-nothing-sent shapes are NotSubmitted;
/// everything ambiguous (timeout, unparseable output, id-less replies)
/// is unknown -- boltzd MAY have made the swap.
#[test]
fn create_classification_is_fail_closed() {
    // Committed: reply carries an id and no error markers.
    let outcome = classify_boltz_create(&CreateOutcome::Completed(json!({"id": "swap-aa"})));
    match outcome {
        BoltzSubmitOutcome::Committed { swap_id } => {
            assert_eq!(swap_id.as_deref(), Some("swap-aa"))
        }
        other => panic!("{other:?}"),
    }

    // An ERROR swap in the reply is a rejection with proof.
    let outcome = classify_boltz_create(&CreateOutcome::Completed(
        json!({"id": "swap-bb", "error": "invalid pair"}),
    ));
    assert!(matches!(
        outcome,
        BoltzSubmitOutcome::RejectedWithProof { .. }
    ));

    // A reply WITHOUT an id is ambiguous, never success and never clean.
    let outcome = classify_boltz_create(&CreateOutcome::Completed(json!({"status": "??"})));
    assert!(matches!(
        outcome,
        BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
    ));

    // Provably nothing reached boltzd.
    for clean in [
        CliError::Disabled,
        CliError::NotFound {
            message: "boltzcli: no such file".into(),
        },
        CliError::TransportRefused {
            subcommand: "createswap (5 args redacted)".into(),
        },
    ] {
        let outcome = classify_boltz_create(&CreateOutcome::Rejected(clean.clone()));
        assert!(
            matches!(outcome, BoltzSubmitOutcome::NotSubmitted { .. }),
            "{clean:?} -> {outcome:?}"
        );
    }

    // A definite nonzero exit is a rejection with proof.
    let outcome = classify_boltz_create(&CreateOutcome::Rejected(CliError::ExitFailure {
        code: Some(1),
        message: "insufficient balance".into(),
    }));
    match outcome {
        BoltzSubmitOutcome::RejectedWithProof { detail } => {
            assert!(detail.contains("insufficient balance"), "{detail}")
        }
        other => panic!("{other:?}"),
    }

    // Unparseable output: the command RAN -- the swap may exist.
    let outcome = classify_boltz_create(&CreateOutcome::Rejected(CliError::InvalidJson {
        message: "expected value: <html>".into(),
    }));
    assert!(matches!(
        outcome,
        BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
    ));

    // The explicit unknown arm.
    let outcome = classify_boltz_create(&CreateOutcome::Unknown {
        timeout_secs: 120,
        command: "createswap (5 args redacted)".into(),
    });
    assert!(matches!(
        outcome,
        BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
    ));
}

/// Manual actions (refund/claim): exit-0 is UNVERIFIED, never success --
/// it quarantines until a terminal swap status is observed.
#[test]
fn manual_classification_never_trusts_exit_zero() {
    let outcome = classify_boltz_manual(&ManualActionOutcome::Unverified {
        raw_output: "refund broadcast".into(),
    });
    assert!(matches!(
        outcome,
        BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
    ));

    let outcome = classify_boltz_manual(&ManualActionOutcome::Failed(CliError::ExitFailure {
        code: Some(2),
        message: "swap not refundable".into(),
    }));
    assert!(matches!(
        outcome,
        BoltzSubmitOutcome::RejectedWithProof { .. }
    ));

    let outcome = classify_boltz_manual(&ManualActionOutcome::Failed(CliError::Disabled));
    assert!(matches!(outcome, BoltzSubmitOutcome::NotSubmitted { .. }));

    let outcome = classify_boltz_manual(&ManualActionOutcome::Failed(CliError::Timeout {
        timeout_secs: 60,
        command: "refundswap (2 args redacted)".into(),
    }));
    assert!(matches!(
        outcome,
        BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { .. }
    ));
}

/// Settlement mapping: committed settles (with the actual fee), rejected
/// and not-submitted release, unknown QUARANTINES.
#[test]
fn settlement_mapping_quarantines_unknown() {
    let settle = settlement_for_boltz(
        &BoltzSubmitOutcome::Committed {
            swap_id: Some("swap-aa".into()),
        },
        "b-1",
        1_500,
        NOW,
    );
    assert_eq!(settle.outcome, "committed");
    assert_eq!(settle.reservation_status, "settled");
    assert_eq!(settle.swap_id.as_deref(), Some("swap-aa"));
    assert_eq!(settle.settled_sats, Some(1_500));

    let settle = settlement_for_boltz(
        &BoltzSubmitOutcome::RejectedWithProof {
            detail: "insufficient balance".into(),
        },
        "b-2",
        1_500,
        NOW,
    );
    assert_eq!(settle.outcome, "rejected");
    assert_eq!(settle.reservation_status, "released");

    let settle = settlement_for_boltz(
        &BoltzSubmitOutcome::NotSubmitted {
            detail: "transport refused".into(),
        },
        "b-3",
        1_500,
        NOW,
    );
    assert_eq!(settle.outcome, "not_submitted");
    assert_eq!(settle.reservation_status, "released");

    let settle = settlement_for_boltz(
        &BoltzSubmitOutcome::OutcomeUnknownAfterSubmit {
            detail: "createswap timed out".into(),
        },
        "b-4",
        1_500,
        NOW,
    );
    assert_eq!(settle.outcome, "outcome_unknown");
    assert_eq!(settle.reservation_status, "quarantined");
    assert_eq!(settle.settled_sats, None);
}

/// The mnemonic's single sanctioned egress consumes the secret; the type
/// itself is opaque (no Debug/Display -- compile-fail pinned in the
/// module's doctests).
#[test]
fn mnemonic_secret_single_egress() {
    let secret = MnemonicSecret::new("abandon ability able about".to_string());
    let value = secret.into_rpc_value();
    assert_eq!(value, json!("abandon ability able about"));
}

/// Boundaries module is execution-free, and no production surface names
/// the capability or the armed transport before Task 69.
#[test]
fn capability_and_transport_unreachable_from_production() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let boundaries = std::fs::read_to_string(root.join("src/boltz_boundaries.rs")).unwrap();
    for forbidden in ["createswap", "ExecutionMode", ".run("] {
        assert!(
            !boundaries.contains(forbidden),
            "boltz_boundaries.rs must be execution-free (found `{forbidden}`)"
        );
    }
    for file in ["src/runtime.rs", "src/lnplus_runtime.rs", "src/main.rs"] {
        let source = std::fs::read_to_string(root.join(file)).unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap();
        for name in ["ArmedBoltzCli", "BoltzActionCapability"] {
            assert!(
                !production.contains(name),
                "{file} must not name {name} before Task 69 authority assembly"
            );
        }
    }
}
