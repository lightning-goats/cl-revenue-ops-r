//! Tests for Task 8 step 1: the operating-mode matrix (`revops::fee_mode`).
//! Encodes the Task 1 table verbatim plus the Task R8 amendment (seed
//! provenance for `fee-stateful-shadow=true`).

use revops::cutover_arm::{
    self, validate_and_consume, RunningIdentity, CUTOVER_ARM_SCHEMA, CUTOVER_SUBSYSTEM_FEES,
};
use revops::fee_mode::{
    validate_fee_mode, AuthorityPlan, FeeModeDenyReason, ModeFlags, ShadowSeedStatus,
    ValidatedFeeMode,
};
use revops_db::fee_runway::{FeeSeedEventRow, FeeStateSnapshot};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

const NODE_ID: &str = "lnnode";
const SOURCE_COMMIT: &str = "7d8e79ec307fd10bd1a775a236148a642a0a506f";
const BINARY_SHA256: &str = "ff648376758b9a97de7642adbf1c258494744c54e33c31a712dcc8c742d1428c";

fn matching_identity(owner_uid: u32) -> RunningIdentity {
    RunningIdentity {
        node_id: NODE_ID.to_string(),
        subsystem: CUTOVER_SUBSYSTEM_FEES.to_string(),
        source_commit: SOURCE_COMMIT.to_string(),
        binary_sha256: BINARY_SHA256.to_string(),
        owner_uid,
        now: 1_000_000,
    }
}

fn valid_arm_json(nonce: &str) -> String {
    format!(
        r#"{{
            "schema": "{schema}",
            "node_id": "{node}",
            "subsystem": "{subsystem}",
            "source_commit": "{commit}",
            "binary_sha256": "{hash}",
            "not_before": 999900,
            "expires_at": 1000100,
            "nonce": "{nonce}"
        }}"#,
        schema = CUTOVER_ARM_SCHEMA,
        node = NODE_ID,
        subsystem = CUTOVER_SUBSYSTEM_FEES,
        commit = SOURCE_COMMIT,
        hash = BINARY_SHA256,
        nonce = nonce,
    )
}

fn write_arm(dir: &Path, name: &str, json: &str) -> PathBuf {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let path = dir.join(name);
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .expect("create arm file")
        .write_all(json.as_bytes())
        .expect("write arm json");
    path
}

/// Builds a genuinely validated-and-consumed `LiveSessionArm` -- the only
/// way `fee_mode`'s tests (or any code) can obtain one, matching the "no
/// forged live capability" contract.
fn real_consumed_arm(tmp: &Path, nonce: &str) -> cutover_arm::LiveSessionArm {
    let arm_path = write_arm(tmp, &format!("{nonce}.json"), &valid_arm_json(nonce));
    let owner_uid = std::fs::metadata(&arm_path).expect("stat arm").uid();
    let consumed_dir = tmp.join("consumed");
    let identity = matching_identity(owner_uid);
    validate_and_consume(&arm_path, &consumed_dir, &identity).expect("valid arm consumes cleanly")
}

fn virgin_state() -> FeeStateSnapshot {
    FeeStateSnapshot::default()
}

fn non_virgin_state() -> FeeStateSnapshot {
    FeeStateSnapshot {
        generation: 3,
        rows: vec![],
    }
}

fn some_seed_event() -> FeeSeedEventRow {
    FeeSeedEventRow {
        seeded_at: 1_000,
        outcome: "seeded".to_string(),
        source_db_path: "/var/lib/lightning/revops.db".to_string(),
        source_max_last_update: 999,
        row_count: 12,
        payload_sha256: "0".repeat(64),
        source_commit: SOURCE_COMMIT.to_string(),
        refused_channel: None,
        refused_field: None,
        detail: None,
    }
}

// ---------------------------------------------------------------------------
// Step 1: the mode matrix, encoded verbatim
// ---------------------------------------------------------------------------

#[test]
fn passive_observer_row_accepted_without_arm() {
    let flags = ModeFlags {
        observer: true,
        fee_dryrun: false,
        fee_broadcast: false,
        fee_stateful_shadow: false,
    };
    let result = validate_fee_mode(flags, None, &virgin_state(), None).expect("passive is valid");
    assert!(matches!(result, ValidatedFeeMode::PassiveObserver(_)));
}

#[test]
fn autonomous_shadow_row_accepted_without_arm_when_store_virgin() {
    let flags = ModeFlags {
        observer: true,
        fee_dryrun: true,
        fee_broadcast: false,
        fee_stateful_shadow: true,
    };
    let result =
        validate_fee_mode(flags, None, &virgin_state(), None).expect("shadow row is valid");
    match result {
        ValidatedFeeMode::AutonomousShadow(shadow) => {
            assert_eq!(shadow.seed_status(), ShadowSeedStatus::PendingFirstCycle);
        }
        other => panic!("expected AutonomousShadow, got {other:?}"),
    }
}

#[test]
fn live_authority_row_accepted_with_valid_consumed_arm_and_seeded_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm = real_consumed_arm(tmp.path(), "live-nonce-1");
    let flags = ModeFlags {
        observer: false,
        fee_dryrun: false,
        fee_broadcast: true,
        fee_stateful_shadow: false,
    };
    let seed_event = some_seed_event();
    let result = validate_fee_mode(flags, Some(arm), &non_virgin_state(), Some(&seed_event))
        .expect("live row with a real consumed arm and seeded state is valid");
    match result {
        ValidatedFeeMode::LiveAuthority(live) => {
            assert_eq!(live.arm().nonce(), "live-nonce-1");
        }
        other => panic!("expected LiveAuthority, got {other:?}"),
    }
}

#[test]
fn validated_live_mode_invokes_action_factory_exactly_once_with_the_real_capability() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm = real_consumed_arm(tmp.path(), "live-action-factory-control");
    let seed_event = some_seed_event();
    let mode = validate_fee_mode(
        ModeFlags {
            observer: false,
            fee_dryrun: false,
            fee_broadcast: true,
            fee_stateful_shadow: false,
        },
        Some(arm),
        &non_virgin_state(),
        Some(&seed_event),
    )
    .unwrap();
    let calls = std::cell::Cell::new(0);
    let plan = mode.into_authority_plan(|live| {
        calls.set(calls.get() + 1);
        live.arm().nonce().to_string()
    });
    assert_eq!(calls.get(), 1);
    match plan {
        AuthorityPlan::Live(nonce) => assert_eq!(nonce, "live-action-factory-control"),
        AuthorityPlan::Observer(_) => panic!("validated live mode became observer"),
    }
}

#[test]
fn live_authority_row_denied_without_arm() {
    let flags = ModeFlags {
        observer: false,
        fee_dryrun: false,
        fee_broadcast: true,
        fee_stateful_shadow: false,
    };
    let err = validate_fee_mode(flags, None, &virgin_state(), None).unwrap_err();
    assert_eq!(err, FeeModeDenyReason::LiveModeRequiresArm);
    assert_eq!(err.code(), "live_mode_requires_arm");
}

// ---------------------------------------------------------------------------
// Coordinator ruling I-6 (fix round 1): live authority must not start from a
// virgin/unseeded Rust-owned store. The cutover sequence guarantees shadow
// seeds and accumulates state before an arm is ever minted, so "healthy
// persisted state" means seeded -- generation > 0 AND a recorded seed event.
// ---------------------------------------------------------------------------

/// A valid, consumed arm over a VIRGIN store (generation 0, no seed event)
/// must be denied -- a live process must never be the first thing to ever
/// touch the Rust-owned store.
#[test]
fn live_authority_row_denied_when_store_is_virgin() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm = real_consumed_arm(tmp.path(), "live-nonce-virgin");
    let flags = ModeFlags {
        observer: false,
        fee_dryrun: false,
        fee_broadcast: true,
        fee_stateful_shadow: false,
    };
    let err = validate_fee_mode(flags, Some(arm), &virgin_state(), None).unwrap_err();
    assert_eq!(err, FeeModeDenyReason::LiveModeRequiresSeededState);
    assert_eq!(err.code(), "live_mode_requires_seeded_state");
}

/// A valid, consumed arm over a NON-virgin store that nonetheless carries
/// NO seed-provenance event must also be denied -- committed state with no
/// seed record is exactly the same misconfiguration `NeverSeeded` catches
/// for the shadow row, now enforced for the live row too.
#[test]
fn live_authority_row_denied_when_non_virgin_store_has_no_seed_event() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm = real_consumed_arm(tmp.path(), "live-nonce-unseeded");
    let flags = ModeFlags {
        observer: false,
        fee_dryrun: false,
        fee_broadcast: true,
        fee_stateful_shadow: false,
    };
    let err = validate_fee_mode(flags, Some(arm), &non_virgin_state(), None).unwrap_err();
    assert_eq!(err, FeeModeDenyReason::LiveModeRequiresSeededState);
}

/// Check order: an ABSENT arm is still reported as `LiveModeRequiresArm`,
/// never `LiveModeRequiresSeededState` -- arm presence is checked first,
/// exactly as before this amendment.
#[test]
fn live_authority_row_without_arm_reports_arm_reason_even_when_store_is_unseeded() {
    let flags = ModeFlags {
        observer: false,
        fee_dryrun: false,
        fee_broadcast: true,
        fee_stateful_shadow: false,
    };
    let err = validate_fee_mode(flags, None, &non_virgin_state(), None).unwrap_err();
    assert_eq!(err, FeeModeDenyReason::LiveModeRequiresArm);
}

#[test]
fn arm_present_in_passive_row_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm = real_consumed_arm(tmp.path(), "stray-nonce-1");
    let flags = ModeFlags {
        observer: true,
        fee_dryrun: false,
        fee_broadcast: false,
        fee_stateful_shadow: false,
    };
    let err = validate_fee_mode(flags, Some(arm), &virgin_state(), None).unwrap_err();
    assert_eq!(err, FeeModeDenyReason::ArmPresentInNonLiveMode);
}

#[test]
fn arm_present_in_shadow_row_is_denied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let arm = real_consumed_arm(tmp.path(), "stray-nonce-2");
    let flags = ModeFlags {
        observer: true,
        fee_dryrun: true,
        fee_broadcast: false,
        fee_stateful_shadow: true,
    };
    let err = validate_fee_mode(flags, Some(arm), &virgin_state(), None).unwrap_err();
    assert_eq!(err, FeeModeDenyReason::ArmPresentInNonLiveMode);
}

/// Every partial/conflicting combination outside the three accepted rows
/// fails with a stable `InvalidCombination`, carrying the offending flags.
#[test]
fn every_other_combination_is_a_stable_invalid_combination_error() {
    let accepted = [
        (true, false, false, false),
        (true, true, false, true),
        (false, false, true, false),
    ];
    for observer in [true, false] {
        for fee_dryrun in [true, false] {
            for fee_broadcast in [true, false] {
                for fee_stateful_shadow in [true, false] {
                    let tuple = (observer, fee_dryrun, fee_broadcast, fee_stateful_shadow);
                    if accepted.contains(&tuple) {
                        continue;
                    }
                    let flags = ModeFlags {
                        observer,
                        fee_dryrun,
                        fee_broadcast,
                        fee_stateful_shadow,
                    };
                    let err = validate_fee_mode(flags, None, &virgin_state(), None).unwrap_err();
                    assert_eq!(err, FeeModeDenyReason::InvalidCombination(flags));
                    assert_eq!(err.code(), "invalid_mode_combination");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Amendment (Task R8 item 4): seed provenance row-level test, all 3 cases
// ---------------------------------------------------------------------------

fn shadow_flags() -> ModeFlags {
    ModeFlags {
        observer: true,
        fee_dryrun: true,
        fee_broadcast: false,
        fee_stateful_shadow: true,
    }
}

#[test]
fn shadow_row_virgin_store_no_seed_event_is_pending_first_cycle() {
    let result = validate_fee_mode(shadow_flags(), None, &virgin_state(), None)
        .expect("virgin store defers seeding to the first cycle");
    match result {
        ValidatedFeeMode::AutonomousShadow(shadow) => {
            assert_eq!(shadow.seed_status(), ShadowSeedStatus::PendingFirstCycle);
        }
        other => panic!("expected AutonomousShadow, got {other:?}"),
    }
}

#[test]
fn shadow_row_non_virgin_store_with_seed_event_is_already_seeded() {
    let seed_event = some_seed_event();
    let result = validate_fee_mode(shadow_flags(), None, &non_virgin_state(), Some(&seed_event))
        .expect("a recorded seed event satisfies provenance");
    match result {
        ValidatedFeeMode::AutonomousShadow(shadow) => {
            assert_eq!(shadow.seed_status(), ShadowSeedStatus::AlreadySeeded);
        }
        other => panic!("expected AutonomousShadow, got {other:?}"),
    }
}

#[test]
fn shadow_row_non_virgin_store_without_seed_event_is_never_seeded_misconfiguration() {
    let err = validate_fee_mode(shadow_flags(), None, &non_virgin_state(), None).unwrap_err();
    assert_eq!(err, FeeModeDenyReason::NeverSeeded);
    assert_eq!(err.code(), "stateful_shadow_never_seeded");
}

// ---------------------------------------------------------------------------
// Fix round 1 (coordinator ruling I-4): `InvalidCombination`'s `Display`
// must name the real `revops-r-*` option names AND every accepted row, so
// an operator hitting this exact string on a real restart (e.g. lnnode
// today: observer=true + fee-dryrun=true + no stateful-shadow) can see
// what to change. `code()` (Task 8's stable machine-matchable string) must
// NOT change.
// ---------------------------------------------------------------------------

#[test]
fn invalid_combination_display_names_option_flags_and_every_accepted_row() {
    // lnnode's actual current combination per the coordinator's note.
    let flags = ModeFlags {
        observer: true,
        fee_dryrun: true,
        fee_broadcast: false,
        fee_stateful_shadow: false,
    };
    let err = validate_fee_mode(flags, None, &virgin_state(), None).unwrap_err();
    assert_eq!(err, FeeModeDenyReason::InvalidCombination(flags));
    // The stable code() must be untouched by this fix.
    assert_eq!(err.code(), "invalid_mode_combination");

    let message = err.to_string();
    // The real option names an operator can act on.
    for option_name in [
        "revops-r-observer",
        "revops-r-fee-dryrun",
        "revops-r-fee-broadcast",
        "revops-r-fee-stateful-shadow",
    ] {
        assert!(
            message.contains(option_name),
            "message must name {option_name}: {message}"
        );
    }
    // Every accepted row, by description.
    for row_description in [
        "passive observer",
        "autonomous fee shadow",
        "live fee authority",
    ] {
        assert!(
            message.contains(row_description),
            "message must describe the {row_description} row: {message}"
        );
    }
    // Arm posture for each row.
    assert!(message.contains("arm absent"), "message: {message}");
    assert!(
        message.contains("arm valid and consumed") || message.contains("valid and consumed"),
        "message: {message}"
    );
}
