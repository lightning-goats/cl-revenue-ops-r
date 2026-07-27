//! Autocycle loop driver: one balance-cycle PASS wired from
//! `autocycle.rs`'s kernels, `commands.rs`'s execution-gated create calls,
//! and an injected [`BoltzCli`].
//!
//! Ports the per-candidate execution loop of py `_execute_boltz_balance_cycle`
//! (cl-revenue-ops.py:10036-10230) — profit-guard skip, budget skip, the
//! cooldown pre-claim/TOCTOU-guard/restore sequence, the create call, and
//! outcome classification — MINUS everything that depends on an unported
//! subsystem (see `ENTRYPOINTS.md`):
//! - the structural-envelope gate (needs the capex engine's category-spend
//!   query, boltz_manager.py:1102ff / cl-revenue-ops.py:10061-10088);
//! - `_boltz_exec_policy_recheck` (an unported governor-facade hook,
//!   cl-revenue-ops.py:10144-10151);
//! - the atomic pre-create budget RESERVATION (`budget::reservation_gate`/
//!   `finalize_reservation_attempt` exist and are ready, but wiring them
//!   requires the capex engine's `reserve_boltz_swap_budget` call, which
//!   does not exist yet — this driver only does the simpler
//!   `remaining_budget_sats -= estimated_fee_sats` bookkeeping py's
//!   dry-run/live loop ALSO does at cl-revenue-ops.py:10130/10201, which is
//!   the pre-reservation-era behaviour and still exactly what happens here
//!   when `reservation_gate` returns `NotApplicable`).
//!
//! What IS wired end-to-end, purely from this crate's own kernels plus one
//! injected [`BoltzCli`]:
//! `cooldown_check` -> pre-claim -> (dry-run: preview, no CLI call) OR
//! (armed: `commands::execute_loop_in`/`execute_loop_out` ->
//! [`crate::error::CreateOutcome`] -> [`crate::autocycle::SwapAttemptOutcome`])
//! -> `cooldown_after_attempt` -> budget bookkeeping -> per-pass
//! `AutoCycleErrorState::on_result`.

use crate::autocycle::{
    cooldown_after_attempt, cooldown_check, AutoCycleErrorState, SwapAttemptOutcome,
};
use crate::cli::BoltzCli;
use crate::commands::{self, ActionOutcome};
use crate::error::{CliError, CreateOutcome, ManualActionOutcome};
use crate::execution::ExecutionMode;
use crate::state::is_error_swap;
use std::collections::HashMap;

/// Which swap-creating call a candidate wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapDirection {
    LoopIn,
    LoopOut,
}

/// One balance-cycle candidate, mirroring the fields py's balance-plan
/// recommendation dict supplies to the execution loop
/// (cl-revenue-ops.py:10040-10045, 10136-10179). `currency`/`wallet_name`
/// are pre-resolved by the caller (currency `"auto"` selection and wallet
/// lookup are both I/O-touching concerns outside this pure/injected-IO
/// boundary — see `ENTRYPOINTS.md`).
#[derive(Debug, Clone)]
pub struct BalanceCandidate {
    pub channel_id: String,
    pub peer_id: String,
    pub direction: SwapDirection,
    pub amount_sats: i64,
    pub estimated_fee_sats: i64,
    pub passes_profit_guard: bool,
    pub currency: String,
    /// Resolved wallet name for `loop_in`/`loop_out`'s `--from-wallet`/
    /// `--to-wallet`. Unused for the address-destination `loop_out` case
    /// (not modeled here — see module docs).
    pub wallet_name: String,
    /// Overrides `default_cooldown_seconds` when set (py
    /// `execution_hints.recommended_cooldown_hours`,
    /// cl-revenue-ops.py:10090-10097).
    pub cooldown_seconds_override: Option<i64>,
}

/// Why a candidate was skipped without any cooldown pre-claim or CLI call.
#[derive(Debug, Clone, PartialEq)]
pub enum SkipReason {
    /// py cl-revenue-ops.py:10047-10049.
    ProfitGuardFailed,
    /// py cl-revenue-ops.py:10050-10059.
    InsufficientRemainingBudget {
        estimated_fee_sats: i64,
        remaining_budget_sats: i64,
    },
    /// py cl-revenue-ops.py:10102-10110.
    CooldownActive { remaining_sec: i64 },
}

/// The executed-call outcome, classified the same way py's
/// status-string branch does (cl-revenue-ops.py:10187-10218) but through
/// the crate's typed [`CreateOutcome`]/[`SwapAttemptOutcome`] instead of a
/// bare string, so a caller cannot silently treat `Unknown` as success.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutedOutcome {
    /// [`ExecutionMode::DryRun`] preview (py's `"would_execute"`,
    /// cl-revenue-ops.py:10121-10129). No CLI call was made.
    WouldExecute,
    Accepted {
        swap_id: Option<String>,
    },
    RejectedOrError {
        detail: String,
    },
    /// The local call's outcome is unknown (subprocess timeout). Cooldown
    /// stays burned and budget bookkeeping is NOT rolled back — a caller
    /// MUST reconcile via `swap_status` (see `error.rs`).
    Unknown {
        timeout_secs: u64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CandidateResult {
    Skipped {
        channel_id: String,
        reason: SkipReason,
    },
    Attempted {
        channel_id: String,
        outcome: ExecutedOutcome,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BalanceCyclePassResult {
    /// py's `"status": "dry_run" if dry_run else "executed"`
    /// (cl-revenue-ops.py:10232).
    pub status: &'static str,
    pub results: Vec<CandidateResult>,
    pub remaining_budget_sats: i64,
}

fn create_outcome_to_swap_attempt(
    outcome: &CreateOutcome<serde_json::Value>,
) -> SwapAttemptOutcome {
    match outcome {
        CreateOutcome::Completed(v) => match v.as_object() {
            Some(obj) if is_error_swap(obj) => SwapAttemptOutcome::RejectedOrError,
            _ => SwapAttemptOutcome::Accepted,
        },
        // A definite, synchronous rejection (bad JSON, nonzero exit the
        // CLI reported, disabled) is not the same ambiguity as a timeout —
        // it did not happen.
        CreateOutcome::Rejected(_) => SwapAttemptOutcome::RejectedOrError,
        // py has no direct equivalent (its balance-cycle `except Exception`
        // catch-all can swallow a timeout indistinguishably from any other
        // exception, ExceptionDuringExecution). This port routes a raw CLI
        // timeout through Unknown instead, per `autocycle.rs`'s own
        // documented recommendation for a live adapter -- keep the
        // cooldown burned, do not treat it as "did not happen".
        CreateOutcome::Unknown { .. } => SwapAttemptOutcome::Unknown,
    }
}

fn executed_outcome_of(outcome: CreateOutcome<serde_json::Value>) -> ExecutedOutcome {
    match outcome {
        CreateOutcome::Completed(v) => {
            let swap_id = v.get("id").and_then(|x| x.as_str()).map(|s| s.to_string());
            match v.as_object() {
                Some(obj) if is_error_swap(obj) => ExecutedOutcome::RejectedOrError {
                    detail: crate::state::swap_entry_error_text(obj),
                },
                _ => ExecutedOutcome::Accepted { swap_id },
            }
        }
        CreateOutcome::Rejected(e) => ExecutedOutcome::RejectedOrError {
            detail: e.to_string(),
        },
        CreateOutcome::Unknown { timeout_secs, .. } => ExecutedOutcome::Unknown { timeout_secs },
    }
}

/// Run one balance-cycle pass over `candidates`, in order, stopping once
/// `max_actions` candidates have been ATTEMPTED (py's `len(executed) >=
/// max_actions` check, cl-revenue-ops.py:10037-10038 — a skip does not
/// count against the cap, matching Python exactly).
///
/// `last_action_ts` is the live adapter's per-channel cooldown map (py's
/// `_boltz_balance_last_action` dict) — mutated in place, mirroring the
/// pre-claim/restore pattern `tests/balance_cycle_candidate.rs` and
/// `autocycle.rs` demonstrate.
#[allow(clippy::too_many_arguments)]
pub fn run_balance_cycle_pass(
    cli: &dyn BoltzCli,
    now: i64,
    mode: ExecutionMode,
    candidates: &[BalanceCandidate],
    max_actions: usize,
    mut remaining_budget_sats: i64,
    default_cooldown_seconds: i64,
    last_action_ts: &mut HashMap<String, i64>,
    error_state: &mut AutoCycleErrorState,
    create_timeout_secs: u64,
) -> BalanceCyclePassResult {
    let mut results = Vec::with_capacity(candidates.len());
    let mut attempted = 0usize;

    for cand in candidates {
        if attempted >= max_actions {
            break;
        }

        if !cand.passes_profit_guard {
            results.push(CandidateResult::Skipped {
                channel_id: cand.channel_id.clone(),
                reason: SkipReason::ProfitGuardFailed,
            });
            continue;
        }
        if cand.estimated_fee_sats > remaining_budget_sats {
            results.push(CandidateResult::Skipped {
                channel_id: cand.channel_id.clone(),
                reason: SkipReason::InsufficientRemainingBudget {
                    estimated_fee_sats: cand.estimated_fee_sats,
                    remaining_budget_sats,
                },
            });
            continue;
        }

        let cooldown_seconds = cand
            .cooldown_seconds_override
            .unwrap_or(default_cooldown_seconds);
        let prior_ts = *last_action_ts.get(&cand.channel_id).unwrap_or(&0);
        let cd = cooldown_check(now, prior_ts, cooldown_seconds);
        if !cd.allowed {
            results.push(CandidateResult::Skipped {
                channel_id: cand.channel_id.clone(),
                reason: SkipReason::CooldownActive {
                    remaining_sec: cd.remaining_sec,
                },
            });
            continue;
        }

        // C1 FIX pre-claim (py cl-revenue-ops.py:10112): claim the slot
        // before the outcome is known, restore below if it did not burn.
        last_action_ts.insert(cand.channel_id.clone(), now);
        attempted += 1;

        if !mode.is_armed() {
            let ts = cooldown_after_attempt(prior_ts, now, SwapAttemptOutcome::DryRun);
            last_action_ts.insert(cand.channel_id.clone(), ts);
            remaining_budget_sats = (remaining_budget_sats - cand.estimated_fee_sats).max(0);
            results.push(CandidateResult::Attempted {
                channel_id: cand.channel_id.clone(),
                outcome: ExecutedOutcome::WouldExecute,
            });
            continue;
        }

        let create_outcome = match cand.direction {
            SwapDirection::LoopIn => commands::execute_loop_in(
                cli,
                mode,
                &cand.wallet_name,
                Some(cand.currency.as_str()),
                cand.amount_sats,
                create_timeout_secs,
            ),
            SwapDirection::LoopOut => commands::execute_loop_out(
                cli,
                mode,
                cand.amount_sats,
                Some(cand.currency.as_str()),
                None,
                Some(cand.wallet_name.as_str()),
                &[],
                0,
                create_timeout_secs,
            ),
        };

        let create_outcome = match create_outcome {
            Ok(ActionOutcome::Executed(o)) => o,
            // ArgvError (e.g. non-positive amount) — a definite, local
            // rejection before any subprocess call.
            Err(e) => CreateOutcome::Rejected(CliError::ExitFailure {
                code: None,
                message: e.to_string(),
            }),
            // mode is Armed here by construction, so Preview is
            // unreachable; keep the match exhaustive rather than panic.
            Ok(ActionOutcome::Preview { .. }) => CreateOutcome::Rejected(CliError::Disabled),
        };

        let attempt_outcome = create_outcome_to_swap_attempt(&create_outcome);
        let ts = cooldown_after_attempt(prior_ts, now, attempt_outcome);
        last_action_ts.insert(cand.channel_id.clone(), ts);

        if matches!(attempt_outcome, SwapAttemptOutcome::Accepted) {
            remaining_budget_sats = (remaining_budget_sats - cand.estimated_fee_sats).max(0);
        }

        results.push(CandidateResult::Attempted {
            channel_id: cand.channel_id.clone(),
            outcome: executed_outcome_of(create_outcome),
        });
    }

    let status: &'static str = if mode.is_armed() {
        "executed"
    } else {
        "dry_run"
    };
    // py cl-revenue-ops.py:10341-2351 (via `_boltz_auto_cycle_state`): a
    // clean pass (no top-level plan-build error — candidates were already
    // supplied) with status executed/dry_run resets the consecutive-error
    // counter. This driver never raises, so `has_error` is always false
    // here; a caller wrapping this in a try/catch-equivalent should call
    // `error_state.on_exception()` itself on a panic/early-return path
    // this function cannot see.
    error_state.on_result(false, status);

    BalanceCyclePassResult {
        status,
        results,
        remaining_budget_sats,
    }
}

/// Refund/claim's `ManualActionOutcome` never reaches this driver (the
/// balance cycle only creates swaps) — re-exported so a live adapter's
/// manual-action RPC handlers can match on the same type this module uses,
/// without importing `error` directly for that one purpose.
pub type ManualOutcome = ManualActionOutcome;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::FakeBoltzCli;
    use serde_json::json;

    fn candidate(channel_id: &str, direction: SwapDirection) -> BalanceCandidate {
        BalanceCandidate {
            channel_id: channel_id.to_string(),
            peer_id: "peer1".to_string(),
            direction,
            amount_sats: 50_000,
            estimated_fee_sats: 500,
            passes_profit_guard: true,
            currency: "LBTC".to_string(),
            wallet_name: "w1".to_string(),
            cooldown_seconds_override: None,
        }
    }

    #[test]
    fn profit_guard_failure_is_skipped_without_touching_cooldown_or_cli() {
        let cli = FakeBoltzCli::new();
        let mut cand = candidate("111x1x0", SwapDirection::LoopIn);
        cand.passes_profit_guard = false;
        let mut ts_map = HashMap::new();
        let mut err_state = AutoCycleErrorState::new();
        let result = run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::Armed,
            &[cand],
            10,
            100_000,
            3600,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(cli.call_count(), 0);
        assert!(ts_map.is_empty());
        assert_eq!(
            result.results[0],
            CandidateResult::Skipped {
                channel_id: "111x1x0".to_string(),
                reason: SkipReason::ProfitGuardFailed
            }
        );
    }

    #[test]
    fn insufficient_budget_is_skipped() {
        let cli = FakeBoltzCli::new();
        let cand = candidate("111x1x0", SwapDirection::LoopIn);
        let mut ts_map = HashMap::new();
        let mut err_state = AutoCycleErrorState::new();
        let result = run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::Armed,
            &[cand],
            10,
            100, /* less than fee 500 */
            3600,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(cli.call_count(), 0);
        assert!(matches!(
            result.results[0],
            CandidateResult::Skipped {
                reason: SkipReason::InsufficientRemainingBudget { .. },
                ..
            }
        ));
    }

    #[test]
    fn active_cooldown_skips_without_preclaim() {
        let cli = FakeBoltzCli::new();
        let cand = candidate("111x1x0", SwapDirection::LoopIn);
        let mut ts_map = HashMap::new();
        ts_map.insert("111x1x0".to_string(), 900);
        let mut err_state = AutoCycleErrorState::new();
        let result = run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::Armed,
            &[cand],
            10,
            100_000,
            3600,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(cli.call_count(), 0);
        // Cooldown map must be untouched (still the pre-existing 900), not
        // overwritten by a pre-claim that then had to be restored.
        assert_eq!(ts_map.get("111x1x0"), Some(&900));
        assert!(matches!(
            result.results[0],
            CandidateResult::Skipped {
                reason: SkipReason::CooldownActive { .. },
                ..
            }
        ));
    }

    #[test]
    fn dry_run_never_calls_cli_and_restores_cooldown_but_previews_budget() {
        let cli = FakeBoltzCli::new();
        let cand = candidate("111x1x0", SwapDirection::LoopIn);
        let mut ts_map = HashMap::new();
        let mut err_state = AutoCycleErrorState::new();
        let result = run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::DryRun,
            &[cand],
            10,
            100_000,
            3600,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(cli.call_count(), 0, "dry-run must never call the CLI");
        assert_eq!(
            ts_map.get("111x1x0"),
            Some(&0),
            "dry-run must not burn the cooldown slot (restores to prior_ts=0)"
        );
        assert_eq!(result.status, "dry_run");
        assert_eq!(result.remaining_budget_sats, 99_500);
        assert_eq!(
            result.results[0],
            CandidateResult::Attempted {
                channel_id: "111x1x0".to_string(),
                outcome: ExecutedOutcome::WouldExecute
            }
        );
        assert_eq!(err_state.consecutive_errors, 0);
    }

    #[test]
    fn armed_accepted_burns_cooldown_and_decrements_budget() {
        let cli = FakeBoltzCli::new();
        cli.push_ok(json!({"id": "swap-a", "state": "swap.created"}).to_string());
        let cand = candidate("111x1x0", SwapDirection::LoopIn);
        let mut ts_map = HashMap::new();
        let mut err_state = AutoCycleErrorState::new();
        let result = run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::Armed,
            &[cand],
            10,
            100_000,
            3600,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(cli.call_count(), 1);
        assert_eq!(
            ts_map.get("111x1x0"),
            Some(&1000),
            "accepted swap must burn cooldown"
        );
        assert_eq!(result.remaining_budget_sats, 99_500);
        assert_eq!(
            result.results[0],
            CandidateResult::Attempted {
                channel_id: "111x1x0".to_string(),
                outcome: ExecutedOutcome::Accepted {
                    swap_id: Some("swap-a".to_string())
                }
            }
        );
    }

    #[test]
    fn armed_error_swap_restores_cooldown_and_does_not_decrement_budget() {
        let cli = FakeBoltzCli::new();
        cli.push_ok(json!({"id": "swap-b", "state": "pending", "error": "boom"}).to_string());
        let cand = candidate("222x2x0", SwapDirection::LoopOut);
        let mut ts_map = HashMap::new();
        // prior_ts=400, now=1000: with a 3600s cooldown this candidate
        // would be skipped (still in cooldown) rather than attempted, so
        // use a short cooldown here — this test is about the RESTORE
        // behaviour on an error outcome, not the cooldown-skip path
        // (covered separately by `active_cooldown_skips_without_preclaim`).
        ts_map.insert("222x2x0".to_string(), 400);
        let mut err_state = AutoCycleErrorState::new();
        let result = run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::Armed,
            &[cand],
            10,
            100_000,
            300,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(
            ts_map.get("222x2x0"),
            Some(&400),
            "error swap must restore prior cooldown timestamp"
        );
        assert_eq!(
            result.remaining_budget_sats, 100_000,
            "error swap must not deplete budget"
        );
        assert!(matches!(
            result.results[0],
            CandidateResult::Attempted {
                outcome: ExecutedOutcome::RejectedOrError { .. },
                ..
            }
        ));
    }

    #[test]
    fn armed_timeout_maps_to_unknown_keeps_cooldown_burned_and_does_not_decrement_budget() {
        let cli = FakeBoltzCli::new();
        cli.push_err(CliError::Timeout {
            timeout_secs: 60,
            command: "boltzcli createswap".to_string(),
        });
        let cand = candidate("333x3x0", SwapDirection::LoopIn);
        let mut ts_map = HashMap::new();
        let mut err_state = AutoCycleErrorState::new();
        let result = run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::Armed,
            &[cand],
            10,
            100_000,
            3600,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(
            ts_map.get("333x3x0"),
            Some(&1000),
            "ambiguous (timeout) outcome must keep the cooldown slot burned"
        );
        assert_eq!(
            result.remaining_budget_sats, 100_000,
            "ambiguous outcome must not be treated as a confirmed spend"
        );
        assert_eq!(
            result.results[0],
            CandidateResult::Attempted {
                channel_id: "333x3x0".to_string(),
                outcome: ExecutedOutcome::Unknown { timeout_secs: 60 }
            }
        );
    }

    #[test]
    fn max_actions_stops_after_attempted_count_not_skip_count() {
        let cli = FakeBoltzCli::new();
        cli.push_ok(json!({"id": "swap-x", "state": "swap.created"}).to_string());
        let mut c1 = candidate("a", SwapDirection::LoopIn);
        c1.passes_profit_guard = false; // skipped, must not count toward cap
        let c2 = candidate("b", SwapDirection::LoopIn);
        let c3 = candidate("c", SwapDirection::LoopIn);
        let mut ts_map = HashMap::new();
        let mut err_state = AutoCycleErrorState::new();
        let result = run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::Armed,
            &[c1, c2, c3],
            1, /* max_actions */
            100_000,
            3600,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(
            cli.call_count(),
            1,
            "only one candidate should be attempted"
        );
        assert_eq!(
            result.results.len(),
            2,
            "skip(a) + attempt(b); c never reached"
        );
    }

    #[test]
    fn error_state_resets_on_executed_status() {
        let cli = FakeBoltzCli::new();
        cli.push_ok(json!({"id": "swap-y", "state": "swap.created"}).to_string());
        let cand = candidate("a", SwapDirection::LoopIn);
        let mut ts_map = HashMap::new();
        let mut err_state = AutoCycleErrorState {
            consecutive_errors: 4,
        };
        run_balance_cycle_pass(
            &cli,
            1000,
            ExecutionMode::Armed,
            &[cand],
            10,
            100_000,
            3600,
            &mut ts_map,
            &mut err_state,
            60,
        );
        assert_eq!(err_state.consecutive_errors, 0);
    }
}
