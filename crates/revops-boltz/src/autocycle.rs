//! `BoltzAutoCycle` decision kernel: mode selection, the consecutive-error
//! state machine, and the cooldown pre-claim/restore pattern.
//!
//! Ports py `_treasury_recommendation_executable` / `_select_boltz_auto_cycle_mode`
//! (cl-revenue-ops.py:2008-2079), the `consecutive_errors` bookkeeping inside
//! `_run_boltz_auto_cycle_once` (cl-revenue-ops.py:2118-2367), and the
//! cooldown pre-claim/TOCTOU-guard/restore pattern inside
//! `_execute_boltz_balance_cycle` (cl-revenue-ops.py:9941-10245).
//!
//! Two invariants named explicitly in the port brief are encoded here as
//! types, not comments:
//! - **"Boltz cooldown not burned on dry-run"** — [`cooldown_after_attempt`]
//!   restores the prior timestamp for [`SwapAttemptOutcome::DryRun`],
//!   mirroring py cl-revenue-ops.py:10114-10120.
//! - **"budget blocks don't poison stats"** — [`AutoCycleErrorState::on_result`]
//!   only reads a top-level `has_error`/`status`; a per-candidate budget
//!   skip (which lives in the `skipped` list, never the top-level `error`
//!   field) cannot reach this function as an error at all, by
//!   construction — see `budget_block_does_not_poison_error_counter` below.

// ---------------------------------------------------------------------
// DD4 / P1-018 (operator ruling 2026-07-02): dry_run defaults False for
// BOTH the scheduled daemon path and the manual RPC
// (revenue-boltz-auto-cycle-run-now) — force=true alone runs one live
// cycle. dry_run=true is an opt-in preview. Pinned as a constant (not
// just a doc comment) so a config/handler regression that flips the
// default is a compile-time-visible diff and a test failure, not a
// silent behavior change.
// ---------------------------------------------------------------------
pub const DRY_RUN_DEFAULT: bool = false;

// ---------------------------------------------------------------------
// Mode selection (py cl-revenue-ops.py:2008-2079)
// ---------------------------------------------------------------------

/// py `_treasury_recommendation_executable` (cl-revenue-ops.py:2008-2022):
/// counting a structural-credit-only-passing rec toward treasury-mode
/// selection deadlocks the cycle (treasury wins, executes 0, and the
/// balance cycle where the structural envelope lives never gets a turn).
pub fn treasury_recommendation_executable(passes_profit_guard: bool, structural: bool) -> bool {
    passes_profit_guard && !structural
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoltzMode {
    Treasury,
    Balance,
    Idle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModeSelection {
    pub mode: BoltzMode,
    pub reason: &'static str,
    pub reserve_deficit_sats: i64,
}

/// py `_select_boltz_auto_cycle_mode` (cl-revenue-ops.py:2025-2079).
/// `treasury_status_ok`: `treasury_plan.status == "ok"`.
/// `treasury_executable_count`: recs passing [`treasury_recommendation_executable`].
pub fn select_boltz_auto_cycle_mode(
    treasury_status_ok: bool,
    treasury_executable_count: usize,
    balance_recommendation_count: usize,
    reserve_deficit_sats: i64,
) -> ModeSelection {
    if treasury_status_ok && treasury_executable_count > 0 {
        return ModeSelection {
            mode: BoltzMode::Treasury,
            reason: "standing_onchain_reserve_below_target",
            reserve_deficit_sats,
        };
    }
    if balance_recommendation_count > 0 {
        return ModeSelection {
            mode: BoltzMode::Balance,
            reason: "onchain_reserve_healthy_use_balance_mode",
            reserve_deficit_sats,
        };
    }
    ModeSelection {
        mode: BoltzMode::Idle,
        reason: "no_eligible_boltz_actions",
        reserve_deficit_sats,
    }
}

// ---------------------------------------------------------------------
// consecutive_errors state machine (py cl-revenue-ops.py:2159-2367)
// ---------------------------------------------------------------------

/// The auto-cycle's `_boltz_auto_cycle_state['consecutive_errors']`
/// counter, ported as an explicit state machine instead of the inline
/// increment/reset scattered across `_run_boltz_auto_cycle_once`'s try/
/// except branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AutoCycleErrorState {
    pub consecutive_errors: u32,
}

impl AutoCycleErrorState {
    pub fn new() -> Self {
        Self::default()
    }

    /// py cl-revenue-ops.py:2341-2351 (the non-exception result path).
    ///
    /// `has_error`: the result dict carried a top-level `error` key
    /// (treasury/balance plan build failure). `status`: the result's
    /// `status` field. C3 FIX (py comment): the counter resets ONLY on
    /// `executed`/`dry_run` — any other status (idle, blocked, disabled,
    /// skipped) leaves the counter untouched, it does not reset it AND
    /// does not increment it.
    pub fn on_result(&mut self, has_error: bool, status: &str) {
        if has_error {
            self.consecutive_errors += 1;
        } else if status == "executed" || status == "dry_run" {
            self.consecutive_errors = 0;
        }
        // else: untouched — idle/blocked/disabled/skipped are not errors
        // and not successes.
    }

    /// py cl-revenue-ops.py:2353-2357 (`except Exception` catch-all
    /// around the whole cycle body, re-raised after bookkeeping).
    pub fn on_exception(&mut self) {
        self.consecutive_errors += 1;
    }
}

// ---------------------------------------------------------------------
// Cooldown pre-claim / restore (py cl-revenue-ops.py:10099-10229)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CooldownDecision {
    pub allowed: bool,
    pub remaining_sec: i64,
}

/// py cl-revenue-ops.py:10101-10110 (cooldown check ONLY, before any
/// pre-claim). `last_ts == 0` means "no prior action recorded for this
/// channel" (Python: `int(_boltz_balance_last_action.get(ch_id, 0) or
/// 0)`), which is never on cooldown regardless of `cooldown_seconds`.
pub fn cooldown_check(now: i64, last_ts: i64, cooldown_seconds: i64) -> CooldownDecision {
    if cooldown_seconds > 0 && last_ts > 0 && (now - last_ts) < cooldown_seconds {
        return CooldownDecision {
            allowed: false,
            remaining_sec: cooldown_seconds - (now - last_ts),
        };
    }
    CooldownDecision {
        allowed: true,
        remaining_sec: 0,
    }
}

/// How a candidate's swap attempt concluded, for the purpose of deciding
/// whether the pre-claimed cooldown slot (py cl-revenue-ops.py:10111-10112,
/// `_boltz_balance_last_action[ch_id] = now`, set BEFORE the outcome is
/// known — a TOCTOU guard, C1 FIX) should be kept (cooldown burned) or
/// rolled back to the prior timestamp (cooldown NOT burned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapAttemptOutcome {
    /// py cl-revenue-ops.py:10114-10131: a preview must not consume the
    /// live cooldown slot, or a dry-run pass would suppress the next live
    /// run for `cooldown_seconds` per channel.
    DryRun,
    /// Live execution, boltzcli reported `status == "accepted"`
    /// (cl-revenue-ops.py:10199-10201). Cooldown stays burned.
    Accepted,
    /// Live execution, boltzcli reported `status` in `("rejected",
    /// "error")` (cl-revenue-ops.py:10202-10207), or a policy-recheck
    /// blocked it before the call (cl-revenue-ops.py:10144-10151,
    /// 10162-10169). The swap definitely did not happen — restore.
    RejectedOrError,
    /// Live execution threw (cl-revenue-ops.py:10219-10223). Same
    /// direction as `RejectedOrError`: the local call failed, so restore
    /// — UNLIKE the create-subprocess-timeout site in `error.rs`, this
    /// path in Python is a generic `except Exception` around the whole
    /// `bm.loop_in`/`bm.loop_out` call, which can also swallow a timeout.
    /// A live port SHOULD route a raw CLI timeout through
    /// `SwapAttemptOutcome::Unknown` instead of this variant — see the
    /// `ENTRYPOINTS.md` note on wiring `CreateOutcome::Unknown`.
    ExceptionDuringExecution,
    /// Live execution, boltzcli reported a `status` NOT in `("accepted",
    /// "rejected", "error")` (cl-revenue-ops.py:10208-10218, the `else`
    /// branch — Python falls through with NO cooldown restore here).
    /// Ambiguous outcome: might have executed. Cooldown stays burned —
    /// deliberately conservative, matching the "never swallow an unknown
    /// outcome as if it were 'definitely did not happen'" safety rule.
    Unknown,
}

/// py's pre-claim-then-maybe-restore pattern
/// (cl-revenue-ops.py:10111-10112 pre-claim, then one of 10118-10120 /
/// 10146-10148 / 10164-10166 / 10204-10206 / 10220-10223 restore sites).
/// Returns the timestamp that should end up stored in the cooldown map
/// for this channel after the attempt.
pub fn cooldown_after_attempt(
    prior_ts: i64,
    pre_claimed_ts: i64,
    outcome: SwapAttemptOutcome,
) -> i64 {
    match outcome {
        SwapAttemptOutcome::DryRun
        | SwapAttemptOutcome::RejectedOrError
        | SwapAttemptOutcome::ExceptionDuringExecution => prior_ts,
        SwapAttemptOutcome::Accepted | SwapAttemptOutcome::Unknown => pre_claimed_ts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn dry_run_default_is_false() {
        // DD4/P1-018: both the daemon loop and the manual RPC must run
        // live by default. Asserting directly on the const (rather than
        // through a function-call indirection) keeps this test failing
        // loudly — not just a diff review catch — if the constant is ever
        // flipped.
        assert!(!DRY_RUN_DEFAULT);
    }

    // --- treasury_recommendation_executable ---

    #[test]
    fn executable_requires_profit_guard_and_non_structural() {
        assert!(treasury_recommendation_executable(true, false));
        assert!(!treasury_recommendation_executable(false, false));
        assert!(!treasury_recommendation_executable(true, true));
        assert!(!treasury_recommendation_executable(false, true));
    }

    // --- select_boltz_auto_cycle_mode ---

    #[test]
    fn treasury_wins_when_ok_and_executable() {
        let sel = select_boltz_auto_cycle_mode(true, 1, 5, 100_000);
        assert_eq!(sel.mode, BoltzMode::Treasury);
        assert_eq!(sel.reserve_deficit_sats, 100_000);
    }

    #[test]
    fn treasury_ok_with_zero_executable_does_not_win() {
        // Deadlock guard: treasury status "ok" with candidates present
        // but none EXECUTABLE must fall through to balance, not stick in
        // a zero-action treasury mode.
        let sel = select_boltz_auto_cycle_mode(true, 0, 3, 0);
        assert_eq!(sel.mode, BoltzMode::Balance);
    }

    #[test]
    fn balance_wins_when_treasury_not_ok() {
        let sel = select_boltz_auto_cycle_mode(false, 0, 2, 0);
        assert_eq!(sel.mode, BoltzMode::Balance);
    }

    #[test]
    fn idle_when_nothing_eligible() {
        let sel = select_boltz_auto_cycle_mode(false, 0, 0, 0);
        assert_eq!(sel.mode, BoltzMode::Idle);
        assert_eq!(sel.reason, "no_eligible_boltz_actions");
    }

    // --- AutoCycleErrorState ---

    #[test]
    fn error_result_increments_counter() {
        let mut st = AutoCycleErrorState::new();
        st.on_result(true, "unknown");
        assert_eq!(st.consecutive_errors, 1);
        st.on_result(true, "unknown");
        assert_eq!(st.consecutive_errors, 2);
    }

    #[test]
    fn executed_or_dry_run_resets_counter() {
        let mut st = AutoCycleErrorState {
            consecutive_errors: 5,
        };
        st.on_result(false, "executed");
        assert_eq!(st.consecutive_errors, 0);

        let mut st2 = AutoCycleErrorState {
            consecutive_errors: 5,
        };
        st2.on_result(false, "dry_run");
        assert_eq!(st2.consecutive_errors, 0);
    }

    #[test]
    fn idle_or_blocked_leaves_counter_untouched() {
        // C3 FIX: only actual success resets; non-error, non-success
        // states must not silently clear an accumulating error streak.
        let mut st = AutoCycleErrorState {
            consecutive_errors: 3,
        };
        st.on_result(false, "idle");
        assert_eq!(st.consecutive_errors, 3);
        st.on_result(false, "blocked");
        assert_eq!(st.consecutive_errors, 3);
        st.on_result(false, "disabled");
        assert_eq!(st.consecutive_errors, 3);
    }

    #[test]
    fn exception_increments_counter() {
        let mut st = AutoCycleErrorState::new();
        st.on_exception();
        assert_eq!(st.consecutive_errors, 1);
    }

    #[test]
    fn budget_block_does_not_poison_error_counter() {
        // A per-candidate budget skip surfaces only in the `skipped` list
        // (never the top-level `error` field), and the cycle-level status
        // is still "executed" when at least the cycle itself ran cleanly
        // (0 executed, N skipped for budget is a normal, successful,
        // empty cycle in Python). has_error=false / status="executed"
        // must reset, not poison, the counter.
        let mut st = AutoCycleErrorState {
            consecutive_errors: 2,
        };
        st.on_result(false, "executed");
        assert_eq!(st.consecutive_errors, 0);
    }

    // --- cooldown_check ---

    #[test]
    fn cooldown_blocks_within_window() {
        let d = cooldown_check(1000, 900, 200);
        assert!(!d.allowed);
        assert_eq!(d.remaining_sec, 100);
    }

    #[test]
    fn cooldown_allows_after_window() {
        let d = cooldown_check(1000, 700, 200);
        assert!(d.allowed);
    }

    #[test]
    fn cooldown_allows_when_no_prior_action() {
        // last_ts == 0 => never on cooldown, no matter cooldown_seconds.
        let d = cooldown_check(1000, 0, 999_999);
        assert!(d.allowed);
    }

    #[test]
    fn cooldown_disabled_when_zero_seconds() {
        let d = cooldown_check(1000, 999, 0);
        assert!(d.allowed);
    }

    // --- cooldown_after_attempt: the "not burned on dry-run" invariant ---

    #[test]
    fn dry_run_restores_prior_timestamp() {
        let ts = cooldown_after_attempt(500, 1000, SwapAttemptOutcome::DryRun);
        assert_eq!(ts, 500, "dry-run must not burn the cooldown slot");
    }

    #[test]
    fn accepted_keeps_burned_timestamp() {
        // Control: a genuinely executed swap DOES burn the cooldown.
        let ts = cooldown_after_attempt(500, 1000, SwapAttemptOutcome::Accepted);
        assert_eq!(ts, 1000);
    }

    #[test]
    fn rejected_or_error_restores_prior_timestamp() {
        let ts = cooldown_after_attempt(500, 1000, SwapAttemptOutcome::RejectedOrError);
        assert_eq!(ts, 500);
    }

    #[test]
    fn exception_restores_prior_timestamp() {
        let ts = cooldown_after_attempt(500, 1000, SwapAttemptOutcome::ExceptionDuringExecution);
        assert_eq!(ts, 500);
    }

    #[test]
    fn unknown_outcome_keeps_burned_timestamp() {
        // Ambiguous local outcome: conservatively treated as "might have
        // executed" — do not free the slot for an immediate retry.
        let ts = cooldown_after_attempt(500, 1000, SwapAttemptOutcome::Unknown);
        assert_eq!(ts, 1000);
    }

    #[test]
    fn first_ever_action_has_prior_ts_zero_and_dry_run_restores_to_zero() {
        // A channel with no prior action (prior_ts=0) previewed via
        // dry_run must end up back at 0 (still "no prior action"), not at
        // the pre-claimed `now`.
        let ts = cooldown_after_attempt(0, 12345, SwapAttemptOutcome::DryRun);
        assert_eq!(ts, 0);
    }
}
