//! Task 63 slice 7 (7G): end-to-end through the FULL production stack --
//! the serialized owner driving the REAL `ProcessBoltzCli` (query) and
//! `ArmedBoltzCli` (action) transports against fake executables. No
//! scripted CLI seams: the only fakes are the boltzcli binaries.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use revops::boltz_boundaries::{BoltzActionCapability, BoltzSubmitOutcome};
use revops::boltz_owner::{
    spawn_boltz_owner, BoltzAction, BoltzOwnerConfig, BoltzOwnerDeps, BoltzOwnerHandle,
    BoltzRefusal, StructuralSpendDb,
};
use revops::capital_boundaries::{GovernorFacade, GovernorVerdict};
use revops_boltz::cli::BoltzCli;
use revops_boltz::process::{ArmedBoltzCli, BoltzCliProcessConfig, ProcessBoltzCli};
use revops_db::owner::{spawn_read_write, ObserverHandle};
use tempfile::TempDir;

const NOW: i64 = 1_800_000_000;

struct AllowAll;
impl GovernorFacade for AllowAll {
    fn authorize(&self, _kind: &str, _amount_sats: i64) -> GovernorVerdict {
        GovernorVerdict::Authorized {
            reason_code: "e2e".into(),
        }
    }
}

struct NoStructuralSpend;
impl StructuralSpendDb for NoStructuralSpend {
    fn structural_spend_sats_24h(&self) -> Result<i64, String> {
        Ok(0)
    }
}

/// A fake boltzcli that dispatches on its first non-`--datadir`
/// argument, so ONE binary serves both transports.
fn fake_boltzcli(dir: &Path, name: &str, create_body: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(
        &path,
        format!(
            r#"#!/bin/sh
set -eu
# skip the leading `--datadir <dir>`
shift 2 || true
verb="${{1:-}}"
case "$verb" in
  listswaps) printf '{{"swaps": []}}' ;;
  swapinfo) printf '{{"id": "swap-e2e", "status": "swap.created"}}' ;;
  createreverseswap) {create_body} ;;
  *) printf '{{"error": "unexpected verb: %s"}}' "$verb"; exit 3 ;;
esac
"#
        ),
    )
    .unwrap();
    let mut perms = fs::metadata(&path).unwrap().permissions();
    perms.set_mode(0o700);
    fs::set_permissions(&path, perms).unwrap();
    path
}

fn enabled_config(cli_path: &Path, datadir: &Path, timeout_seconds: u64) -> BoltzCliProcessConfig {
    BoltzCliProcessConfig::new(
        true,
        cli_path.display().to_string(),
        datadir.display().to_string(),
        false,
        "unused".to_string(),
        timeout_seconds,
    )
}

struct E2e {
    handle: BoltzOwnerHandle,
    store: ObserverHandle,
    _dir: TempDir,
}

async fn e2e(create_body: &str, timeout_seconds: u64) -> E2e {
    let dir = TempDir::new().unwrap();
    let fake = fake_boltzcli(dir.path(), "fake-boltzcli", create_body);
    let store = spawn_read_write(&dir.path().join("observer.db"))
        .await
        .unwrap();
    let config = enabled_config(&fake, dir.path(), timeout_seconds);
    // The REAL transports: query (allowlisted) and armed (inside the
    // capability), exactly as Task 69 will assemble them.
    let query: Arc<dyn BoltzCli + Send + Sync> = Arc::new(ProcessBoltzCli::new(config.clone()));
    let capability = BoltzActionCapability::assemble(ArmedBoltzCli::new(config), 1_000_000);
    let handle = spawn_boltz_owner(BoltzOwnerDeps {
        capability: Some(Arc::new(capability)),
        governor: Some(Arc::new(AllowAll)),
        query,
        structural: Arc::new(NoStructuralSpend),
        store: store.clone(),
        config: BoltzOwnerConfig {
            daily_budget_sats: 3_000,
            budget_window_hours: 24,
            structural_envelope_sats: 0,
            allow_concurrent_swaps: false,
            default_cooldown_seconds: 3_600,
            auto_cycle_enabled: false,
            create_timeout_secs: timeout_seconds,
        },
        clock: Box::new(|| NOW),
    });
    E2e {
        handle,
        store,
        _dir: dir,
    }
}

fn loop_out() -> BoltzAction {
    BoltzAction::LoopOut {
        amount_sats: 500_000,
        currency: "BTC".into(),
        address: None,
        wallet_name: None,
        chan_ids: vec!["700x1x0".into()],
        routing_fee_limit_ppm: 2_000,
        channel_id: Some("700x1x0".into()),
        estimated_fee_sats: 1_000,
        structural: false,
    }
}

/// Happy path through the whole stack: real subprocess create, committed
/// classification, exactly-once settlement, durable journal, durable
/// cooldown.
#[tokio::test]
async fn full_stack_loop_out_commits_and_settles() {
    let h = e2e(
        r#"printf '{"id": "swap-e2e", "status": "swap.created"}'"#,
        10,
    )
    .await;

    let result = h.handle.execute(loop_out()).await.expect("submits");
    match &result.outcome {
        BoltzSubmitOutcome::Committed { swap_id } => {
            assert_eq!(swap_id.as_deref(), Some("swap-e2e"))
        }
        other => panic!("{other:?}"),
    }
    assert!(h
        .store
        .unresolved_boltz_attempts()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(h.store.active_boltz_reserved_sats(0).await.unwrap(), 0);
    let journal = h.store.boltz_journal().await.unwrap();
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].swap_id, "swap-e2e");
    assert_eq!(h.store.boltz_cooldowns().await.unwrap()[0].0, "700x1x0");
}

/// A real subprocess TIMEOUT on create quarantines through the whole
/// stack: the fee stays held, the redacted detail leaks no argv values,
/// and the pending gate blocks the next submission.
#[tokio::test]
async fn full_stack_timeout_quarantines_and_blocks() {
    let h = e2e("exec sleep 30", 1).await;

    let result = h.handle.execute(loop_out()).await.expect("settles");
    match &result.outcome {
        BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { detail } => {
            assert!(detail.contains("createreverseswap"), "{detail}");
            assert!(
                !detail.contains("500000"),
                "argv values leaked end-to-end: {detail}"
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(h.store.quarantined_boltz_attempts().await.unwrap().len(), 1);
    assert_eq!(
        h.store.active_boltz_reserved_sats(0).await.unwrap(),
        1_000,
        "a quarantined swap keeps holding its fee"
    );

    // Structurally no resubmit: the quarantine blocks the pending gate.
    let err = h.handle.execute(loop_out()).await.unwrap_err();
    assert!(
        matches!(err, BoltzRefusal::PendingSwapsBlocked { .. }),
        "{err:?}"
    );
}

/// A definite nonzero exit rejects WITH PROOF and releases the fee hold
/// -- the swap provably never happened.
#[tokio::test]
async fn full_stack_rejection_releases_the_hold() {
    let h = e2e(r#"printf 'insufficient balance\n' >&2; exit 7"#, 10).await;

    let result = h.handle.execute(loop_out()).await.expect("settles");
    match &result.outcome {
        BoltzSubmitOutcome::RejectedWithProof { detail } => {
            assert!(detail.contains("insufficient balance"), "{detail}")
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(h.store.active_boltz_reserved_sats(0).await.unwrap(), 0);
    assert!(h
        .store
        .quarantined_boltz_attempts()
        .await
        .unwrap()
        .is_empty());
    // Clean failure restores the cooldown slot: a retry is allowed.
    let cooldowns = h.store.boltz_cooldowns().await.unwrap();
    assert_eq!(cooldowns[0].1, 0, "clean failure restores the cooldown");
}

/// The QUERY transport really is allowlisted end-to-end: the owner's own
/// pending-swap read works, while a fund-moving verb through the same
/// production-constructible transport refuses without spawning.
#[tokio::test]
async fn query_transport_stays_read_only_in_the_full_stack() {
    let dir = TempDir::new().unwrap();
    let fake = fake_boltzcli(dir.path(), "fake-boltzcli", "printf '{}'");
    let query = ProcessBoltzCli::new(enabled_config(&fake, dir.path(), 5));

    query
        .run(&["listswaps", "--json"], 5)
        .expect("reads are allowed");
    let err = query
        .run(&["createreverseswap", "--json"], 5)
        .expect_err("fund-moving verbs refuse");
    assert!(
        matches!(err, revops_boltz::error::CliError::TransportRefused { .. }),
        "{err:?}"
    );
}
