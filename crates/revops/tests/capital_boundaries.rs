//! Task 62 slice 2: GovernorFacade / BudgetDb / ActiveIntentRegistry
//! boundaries, the four-way capital outcome vocabulary, and the
//! execution-free settlement discipline.

use revops::capital_boundaries::{
    check_budget_evidence, settlement_for_capital, ActiveIntentRegistry, BudgetDb, BudgetEvidence,
    CapitalSubmitOutcome, RegistryVerdict, BUDGET_EVIDENCE_MAX_AGE_SECONDS,
};
use revops_db::owner::spawn_read_write;

struct ScriptedBudget {
    result: Result<BudgetEvidence, String>,
}

impl BudgetDb for ScriptedBudget {
    fn positive_budget_evidence(&self, _now: i64) -> Result<BudgetEvidence, String> {
        self.result.clone()
    }
}

const NOW: i64 = 1_800_000_000;

/// Mandatory positive budget evidence: error, stale, and non-positive
/// all refuse typed; only fresh-positive passes.
#[test]
fn budget_evidence_gate_is_fail_closed() {
    // Read failure.
    let db = ScriptedBudget {
        result: Err("spend_reservations read failed".into()),
    };
    let err = check_budget_evidence(&db, NOW).expect_err("read failure refuses");
    assert_eq!(err.code(), "capital_budget_evidence_unavailable");

    // Stale observation.
    let db = ScriptedBudget {
        result: Ok(BudgetEvidence {
            available_sats: 500_000,
            window_reserved_sats: 0,
            observed_at: NOW - BUDGET_EVIDENCE_MAX_AGE_SECONDS - 1,
        }),
    };
    let err = check_budget_evidence(&db, NOW).expect_err("stale evidence refuses");
    assert_eq!(err.code(), "capital_budget_evidence_stale");

    // Non-positive availability.
    let db = ScriptedBudget {
        result: Ok(BudgetEvidence {
            available_sats: 0,
            window_reserved_sats: 5_000,
            observed_at: NOW,
        }),
    };
    let err = check_budget_evidence(&db, NOW).expect_err("zero headroom refuses");
    assert_eq!(err.code(), "capital_budget_exhausted");

    // Fresh positive passes with the evidence carried.
    let db = ScriptedBudget {
        result: Ok(BudgetEvidence {
            available_sats: 500_000,
            window_reserved_sats: 100_000,
            observed_at: NOW - 5,
        }),
    };
    let evidence = check_budget_evidence(&db, NOW).expect("fresh positive passes");
    assert_eq!(evidence.available_sats, 500_000);
}

/// The registry refuses duplicates in flight AND unresolved intents
/// seeded from the durable store; resolve releases.
#[tokio::test]
async fn registry_refuses_duplicates_and_seeds_from_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    // One unresolved intent survives from a prior process.
    store
        .insert_capital_intent(revops_db::fee_runway::CapitalIntent {
            request_id: "old-open-peerA".into(),
            kind: "open".into(),
            peer_id: "peerA".into(),
            channel_id: None,
            amount_sats: 1_000_000,
            reason: None,
            submitted_at: NOW - 900,
        })
        .await
        .unwrap();

    let unresolved = store.unresolved_capital_intents().await.unwrap();
    let mut registry = ActiveIntentRegistry::seeded_from(&unresolved);

    // The unresolved (peer, kind) pair is busy.
    match registry.begin("new-open-peerA", "open", "peerA") {
        RegistryVerdict::Busy { existing } => assert_eq!(existing, "old-open-peerA"),
        other => panic!("unresolved pair must refuse, got {other:?}"),
    }
    // A different pair proceeds, then blocks its own duplicate.
    assert!(matches!(
        registry.begin("close-1", "close", "peerB"),
        RegistryVerdict::Admitted
    ));
    match registry.begin("close-2", "close", "peerB") {
        RegistryVerdict::Busy { existing } => assert_eq!(existing, "close-1"),
        other => panic!("in-flight pair must refuse, got {other:?}"),
    }
    // Resolve releases.
    registry.resolve("close-1");
    assert!(matches!(
        registry.begin("close-3", "close", "peerB"),
        RegistryVerdict::Admitted
    ));
}

/// Settlement mapping: success settles with sats+txid, rejected/clean
/// release, unknown quarantines -- and the module is execution-free.
#[test]
fn capital_settlement_mapping_and_execution_free_pin() {
    let settle = settlement_for_capital(
        &CapitalSubmitOutcome::Success {
            txid: Some("deadbeef".into()),
        },
        "cap-1",
        1_000_000,
        NOW,
    );
    assert_eq!(settle.outcome, "success");
    assert_eq!(settle.reservation_status, "settled");
    assert_eq!(settle.txid.as_deref(), Some("deadbeef"));
    assert_eq!(settle.settled_sats, Some(1_000_000));

    let settle = settlement_for_capital(
        &CapitalSubmitOutcome::Rejected {
            detail: "channel too small".into(),
        },
        "cap-2",
        1_000_000,
        NOW,
    );
    assert_eq!(settle.outcome, "rejected");
    assert_eq!(settle.reservation_status, "released");

    let settle = settlement_for_capital(
        &CapitalSubmitOutcome::CleanRefusal {
            detail: "connect refused before any write".into(),
        },
        "cap-3",
        1_000_000,
        NOW,
    );
    assert_eq!(settle.outcome, "clean_refusal");
    assert_eq!(settle.reservation_status, "released");

    let settle = settlement_for_capital(
        &CapitalSubmitOutcome::OutcomeUnknown {
            detail: "fundchannel reply lost; may have broadcast".into(),
        },
        "cap-4",
        1_000_000,
        NOW,
    );
    assert_eq!(settle.outcome, "outcome_unknown");
    assert_eq!(settle.reservation_status, "quarantined");

    // Execution-free: the settlement layer cannot name the transports.
    let source = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/capital_boundaries.rs"
    ))
    .unwrap();
    for forbidden in ["fundchannel", ".close(", "execute_cycle", "plan_cycle"] {
        assert!(
            !source.contains(forbidden),
            "capital_boundaries.rs must be execution-free (found `{forbidden}`)"
        );
    }
}
