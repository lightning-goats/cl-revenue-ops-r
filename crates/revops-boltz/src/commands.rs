//! Execution-gated command wrappers: the ONLY place in this crate allowed
//! to actually call `BoltzCli::run` for a fund-moving command
//! (create/claim/refund/withdraw).
//!
//! Each `execute_*` function takes an [`ExecutionMode`]. In
//! [`ExecutionMode::DryRun`] (the default — see `execution.rs`) it builds
//! the argv via `argv.rs`, runs every pre-subprocess safety check
//! (destination validation, amount gates, the withdraw cap), and returns
//! [`ActionOutcome::Preview`] WITHOUT calling `cli.run` at all — mirroring
//! py's balance-cycle `dry_run` branch (cl-revenue-ops.py:10114-10131),
//! which never calls `bm.loop_in`/`bm.loop_out` in a preview. Only
//! [`ExecutionMode::Armed`] reaches the `cli.run`/`cli::run_json` call.
//!
//! Outcome classification never collapses ambiguity: a create-type call
//! that times out becomes [`crate::error::CreateOutcome::Unknown`] (never
//! `Rejected`, per `error.rs`'s safety contract); a refund/claim exit-0
//! call becomes [`crate::error::ManualActionOutcome::Unverified`] (never a
//! bespoke "success" variant).

use crate::argv;
use crate::cli::{run_json, BoltzCli};
use crate::error::{CliError, CreateOutcome, ManualActionOutcome};
use crate::execution::ExecutionMode;
use serde_json::Value;

/// Result of an execution-gated command: either a dry-run preview (argv
/// only, nothing executed) or the outcome of an actually-armed call.
#[derive(Debug, Clone, PartialEq)]
pub enum ActionOutcome<T> {
    /// [`ExecutionMode::DryRun`]: the argv that WOULD have been run. No
    /// subprocess call was made.
    Preview { argv: Vec<String> },
    /// [`ExecutionMode::Armed`]: the subprocess call was made; `T` is the
    /// command-specific outcome type.
    Executed(T),
}

impl<T> ActionOutcome<T> {
    pub fn is_preview(&self) -> bool {
        matches!(self, ActionOutcome::Preview { .. })
    }
}

fn as_str_refs(argv: &[String]) -> Vec<&str> {
    argv.iter().map(|s| s.as_str()).collect()
}

/// Run a create-type command (`createswap`/`createreverseswap`/
/// `createchainswap`) already assembled into `argv`, classifying the
/// result per `error.rs`'s [`CreateOutcome`] contract: exit-0 valid JSON ->
/// `Completed`, timeout -> `Unknown` (never `Rejected` — boltzd may have
/// processed the request before the local read timed out), any other
/// failure -> `Rejected`.
fn run_create(cli: &dyn BoltzCli, argv: &[String], timeout_secs: u64) -> CreateOutcome<Value> {
    match run_json(cli, &as_str_refs(argv), timeout_secs) {
        Ok(v) => CreateOutcome::Completed(v),
        Err(CliError::Timeout {
            timeout_secs,
            command,
        }) => CreateOutcome::Unknown {
            timeout_secs,
            command,
        },
        Err(e) => CreateOutcome::Rejected(e),
    }
}

/// Run a manual refund/claim-type command already assembled into `argv`,
/// classifying per `error.rs`'s [`ManualActionOutcome`] contract: an exit-0
/// call is `Unverified` (boltzcli's raw stdout, no structured confirmation
/// — a caller MUST follow up with `swap_status`), any subprocess failure is
/// `Failed`.
fn run_manual_action(
    cli: &dyn BoltzCli,
    argv: &[String],
    timeout_secs: u64,
) -> ManualActionOutcome {
    match cli.run(&as_str_refs(argv), timeout_secs) {
        Ok(raw_output) => ManualActionOutcome::Unverified { raw_output },
        Err(e) => ManualActionOutcome::Failed(e),
    }
}

/// py `loop_in`'s subprocess step (boltz_manager.py:1970-1971), gated by
/// [`ExecutionMode`]. Budget reservation/journal recording are the caller's
/// job (see `budget.rs`/`journal.rs`/`ENTRYPOINTS.md`) — this wraps only
/// the `createswap` call itself.
pub fn execute_loop_in(
    cli: &dyn BoltzCli,
    mode: ExecutionMode,
    wallet_name: &str,
    currency: Option<&str>,
    amount_sats: i64,
    timeout_secs: u64,
) -> Result<ActionOutcome<CreateOutcome<Value>>, argv::ArgvError> {
    let a = argv::create_swap_argv(wallet_name, currency, amount_sats)?;
    if !mode.is_armed() {
        return Ok(ActionOutcome::Preview { argv: a });
    }
    Ok(ActionOutcome::Executed(run_create(cli, &a, timeout_secs)))
}

/// py `_loop_out_locked`'s plain (non-external-pay) subprocess step
/// (boltz_manager.py:2301), gated by [`ExecutionMode`]. See `argv.rs`'s
/// module docs for what is NOT reproduced here (first-hop pinning /
/// chanIds retry).
#[allow(clippy::too_many_arguments)]
pub fn execute_loop_out(
    cli: &dyn BoltzCli,
    mode: ExecutionMode,
    amount_sats: i64,
    currency: Option<&str>,
    address: Option<&str>,
    wallet_name: Option<&str>,
    chan_ids: &[String],
    routing_fee_limit_ppm: i64,
    timeout_secs: u64,
) -> Result<ActionOutcome<CreateOutcome<Value>>, argv::ArgvError> {
    let a = argv::create_reverse_swap_argv(
        amount_sats,
        currency,
        address,
        wallet_name,
        chan_ids,
        routing_fee_limit_ppm,
    )?;
    if !mode.is_armed() {
        return Ok(ActionOutcome::Preview { argv: a });
    }
    Ok(ActionOutcome::Executed(run_create(cli, &a, timeout_secs)))
}

/// py `chainswap`'s subprocess step (boltz_manager.py:2560), gated by
/// [`ExecutionMode`].
#[allow(clippy::too_many_arguments)]
pub fn execute_chain_swap(
    cli: &dyn BoltzCli,
    mode: ExecutionMode,
    amount_sats: i64,
    from_currency: Option<&str>,
    to_currency: Option<&str>,
    from_wallet_name: &str,
    to_address: Option<&str>,
    to_wallet_name: Option<&str>,
    timeout_secs: u64,
) -> Result<ActionOutcome<CreateOutcome<Value>>, argv::ArgvError> {
    let a = argv::create_chain_swap_argv(
        amount_sats,
        from_currency,
        to_currency,
        from_wallet_name,
        to_address,
        to_wallet_name,
    )?;
    if !mode.is_armed() {
        return Ok(ActionOutcome::Preview { argv: a });
    }
    Ok(ActionOutcome::Executed(run_create(cli, &a, timeout_secs)))
}

/// py `refund` (boltz_manager.py:2461-2472), gated by [`ExecutionMode`].
pub fn execute_refund(
    cli: &dyn BoltzCli,
    mode: ExecutionMode,
    swap_id: &str,
    destination: Option<&str>,
    timeout_secs: u64,
) -> Result<ActionOutcome<ManualActionOutcome>, argv::ArgvError> {
    let a = argv::refund_swap_argv(swap_id, destination)?;
    if !mode.is_armed() {
        return Ok(ActionOutcome::Preview { argv: a });
    }
    Ok(ActionOutcome::Executed(run_manual_action(
        cli,
        &a,
        timeout_secs,
    )))
}

/// py `claim` (boltz_manager.py:2474-2488), gated by [`ExecutionMode`].
pub fn execute_claim(
    cli: &dyn BoltzCli,
    mode: ExecutionMode,
    swap_ids: &[String],
    destination: Option<&str>,
    timeout_secs: u64,
) -> Result<ActionOutcome<ManualActionOutcome>, argv::ArgvError> {
    let a = argv::claim_swaps_argv(swap_ids, destination)?;
    if !mode.is_armed() {
        return Ok(ActionOutcome::Preview { argv: a });
    }
    Ok(ActionOutcome::Executed(run_manual_action(
        cli,
        &a,
        timeout_secs,
    )))
}

/// py `withdraw` (boltz_manager.py:2581-2637), gated by [`ExecutionMode`].
/// Runs [`argv::withdraw_gate`] FIRST regardless of execution mode — even a
/// dry-run preview must surface a would-be-rejected withdraw as an error,
/// not a misleadingly "valid" preview argv.
#[allow(clippy::too_many_arguments)]
pub fn execute_withdraw(
    cli: &dyn BoltzCli,
    mode: ExecutionMode,
    wallet_name: &str,
    destination: &str,
    currency: &str,
    amount_sats: i64,
    sat_per_vbyte: Option<i64>,
    sweep: bool,
    confirm_sweep: bool,
    max_withdraw_sats: i64,
    timeout_secs: u64,
) -> Result<ActionOutcome<ManualActionOutcome>, argv::WithdrawGateError> {
    argv::withdraw_gate(
        destination,
        currency,
        amount_sats,
        sweep,
        confirm_sweep,
        max_withdraw_sats,
    )?;
    let a = argv::wallet_send_argv(wallet_name, destination, amount_sats, sat_per_vbyte, sweep);
    if !mode.is_armed() {
        return Ok(ActionOutcome::Preview { argv: a });
    }
    Ok(ActionOutcome::Executed(run_manual_action(
        cli,
        &a,
        timeout_secs,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::FakeBoltzCli;
    use serde_json::json;

    // --- dry-run never touches the CLI ---

    #[test]
    fn dry_run_loop_in_never_calls_cli() {
        let cli = FakeBoltzCli::new();
        // No scripted response queued: if execute_loop_in called cli.run,
        // FakeBoltzCli::run would panic ("no scripted response left").
        let outcome =
            execute_loop_in(&cli, ExecutionMode::DryRun, "w", Some("LBTC"), 1000, 60).unwrap();
        assert!(outcome.is_preview());
        assert_eq!(cli.call_count(), 0);
    }

    #[test]
    fn dry_run_refund_never_calls_cli() {
        let cli = FakeBoltzCli::new();
        let outcome = execute_refund(&cli, ExecutionMode::DryRun, "swap-1", None, 60).unwrap();
        assert!(outcome.is_preview());
        assert_eq!(cli.call_count(), 0);
    }

    #[test]
    fn dry_run_withdraw_never_calls_cli() {
        let cli = FakeBoltzCli::new();
        let outcome = execute_withdraw(
            &cli,
            ExecutionMode::DryRun,
            "w",
            "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
            "BTC",
            1000,
            None,
            false,
            false,
            0,
            60,
        )
        .unwrap();
        assert!(outcome.is_preview());
        assert_eq!(cli.call_count(), 0);
    }

    #[test]
    fn dry_run_withdraw_still_enforces_gate() {
        // Control: a dry-run preview of an invalid withdraw must still
        // surface the gate error, not silently "preview" something that
        // would never be allowed to execute.
        let cli = FakeBoltzCli::new();
        let err = execute_withdraw(
            &cli,
            ExecutionMode::DryRun,
            "w",
            "garbage",
            "BTC",
            1000,
            None,
            false,
            false,
            0,
            60,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            argv::WithdrawGateError::InvalidDestination { .. }
        ));
        assert_eq!(cli.call_count(), 0);
    }

    // --- armed mode calls through, with control against dry-run ---

    #[test]
    fn armed_loop_in_calls_cli_exactly_once_with_expected_argv() {
        let cli = FakeBoltzCli::new();
        cli.push_ok(json!({"id": "s1", "state": "swap.created"}).to_string());
        let outcome =
            execute_loop_in(&cli, ExecutionMode::Armed, "w", Some("LBTC"), 1000, 60).unwrap();
        assert_eq!(cli.call_count(), 1);
        assert_eq!(cli.calls.borrow()[0].0[0], "createswap");
        match outcome {
            ActionOutcome::Executed(CreateOutcome::Completed(v)) => {
                assert_eq!(v["id"], "s1");
            }
            other => panic!("expected Executed(Completed(_)), got {other:?}"),
        }
    }

    #[test]
    fn armed_loop_in_timeout_maps_to_unknown_not_rejected() {
        let cli = FakeBoltzCli::new();
        cli.push_err(CliError::Timeout {
            timeout_secs: 30,
            command: "boltzcli createswap".to_string(),
        });
        let outcome =
            execute_loop_in(&cli, ExecutionMode::Armed, "w", Some("LBTC"), 1000, 30).unwrap();
        match outcome {
            ActionOutcome::Executed(CreateOutcome::Unknown { timeout_secs, .. }) => {
                assert_eq!(timeout_secs, 30);
            }
            other => panic!("expected Executed(Unknown{{..}}), got {other:?}"),
        }
    }

    #[test]
    fn armed_loop_in_exit_failure_maps_to_rejected_not_unknown() {
        // Control: a definite synchronous failure is Rejected, distinct
        // from the Timeout->Unknown case above.
        let cli = FakeBoltzCli::new();
        cli.push_err(CliError::ExitFailure {
            code: Some(1),
            message: "budget exceeded".to_string(),
        });
        let outcome =
            execute_loop_in(&cli, ExecutionMode::Armed, "w", Some("LBTC"), 1000, 30).unwrap();
        assert!(matches!(
            outcome,
            ActionOutcome::Executed(CreateOutcome::Rejected(CliError::ExitFailure { .. }))
        ));
    }

    #[test]
    fn armed_refund_exit_zero_is_unverified_not_confirmed_success() {
        let cli = FakeBoltzCli::new();
        cli.push_ok("refund broadcast, txid=abc123");
        let outcome = execute_refund(&cli, ExecutionMode::Armed, "swap-1", None, 60).unwrap();
        match outcome {
            ActionOutcome::Executed(ManualActionOutcome::Unverified { raw_output }) => {
                assert!(raw_output.contains("txid"));
            }
            other => panic!("expected Executed(Unverified{{..}}), got {other:?}"),
        }
    }

    #[test]
    fn armed_refund_cli_failure_is_failed() {
        let cli = FakeBoltzCli::new();
        cli.push_err(CliError::ExitFailure {
            code: Some(1),
            message: "swap not refundable".to_string(),
        });
        let outcome = execute_refund(&cli, ExecutionMode::Armed, "swap-1", None, 60).unwrap();
        assert!(matches!(
            outcome,
            ActionOutcome::Executed(ManualActionOutcome::Failed(_))
        ));
    }

    #[test]
    fn armed_refund_invalid_destination_never_reaches_cli() {
        let cli = FakeBoltzCli::new();
        let err =
            execute_refund(&cli, ExecutionMode::Armed, "swap-1", Some("garbage"), 60).unwrap_err();
        assert_eq!(
            err,
            argv::ArgvError::InvalidDestination("garbage".to_string())
        );
        assert_eq!(cli.call_count(), 0);
    }

    #[test]
    fn armed_withdraw_calls_cli_when_gate_passes() {
        let cli = FakeBoltzCli::new();
        cli.push_ok("txid=deadbeef");
        let outcome = execute_withdraw(
            &cli,
            ExecutionMode::Armed,
            "w",
            "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
            "BTC",
            1000,
            None,
            false,
            false,
            0,
            60,
        )
        .unwrap();
        assert_eq!(cli.call_count(), 1);
        assert!(matches!(
            outcome,
            ActionOutcome::Executed(ManualActionOutcome::Unverified { .. })
        ));
    }

    #[test]
    fn armed_withdraw_over_cap_never_reaches_cli() {
        let cli = FakeBoltzCli::new();
        let err = execute_withdraw(
            &cli,
            ExecutionMode::Armed,
            "w",
            "bc1qqqqsyqcyq5rqwzqfpg9scrgwpugpzysn4v0345",
            "BTC",
            10_000,
            None,
            false,
            false,
            5_000,
            60,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            argv::WithdrawGateError::ExceedsMaxWithdrawCap { .. }
        ));
        assert_eq!(cli.call_count(), 0);
    }
}
