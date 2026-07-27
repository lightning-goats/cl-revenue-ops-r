//! Composed scenario: one balance-cycle candidate going through the
//! reservation gate, a scripted `boltzcli` create call, outcome
//! classification, the cooldown pre-claim/restore decision, and a journal
//! merge — the same sequence `_execute_boltz_balance_cycle` performs per
//! candidate (cl-revenue-ops.py:10036-10229), wired here purely from this
//! crate's kernels plus [`FakeBoltzCli`]. This is not a claim that the
//! plugin calls this sequence (see `ENTRYPOINTS.md` — it does not yet);
//! it demonstrates the kernels compose correctly end-to-end and gives a
//! live adapter a template.

use revops_boltz::autocycle::{cooldown_after_attempt, cooldown_check, SwapAttemptOutcome};
use revops_boltz::budget::{
    finalize_reservation_attempt, reservation_gate, ReservationGate, ReservationOutcome,
};
use revops_boltz::cli::{run_json, FakeBoltzCli};
use revops_boltz::fee::estimate_swap_fee_sats;
use revops_boltz::journal::merge_swap_results;
use revops_boltz::state::is_error_swap;
use serde_json::json;

#[test]
fn accepted_swap_burns_cooldown_reserves_and_settles_budget_and_journals() {
    let now = 1_700_000_000i64;
    let prior_cooldown_ts = 0i64; // never run before on this channel
    let cooldown_seconds = 4 * 3600;

    // 1. Cooldown check (before any pre-claim).
    let cd = cooldown_check(now, prior_cooldown_ts, cooldown_seconds);
    assert!(cd.allowed);

    // 2. Pre-claim (py: _boltz_balance_last_action[ch_id] = now).
    let pre_claimed_ts = now;

    // 3. Reservation gate + attempt (engine call is external; we script
    // its outcome as Some(true) = reserved).
    let gate = reservation_gate(true, true, 500, Some(10_000));
    let ReservationGate::Attempt { fee_sats, .. } = gate else {
        panic!("expected an attempt")
    };
    let reservation_id = "boltz-swap:test-1".to_string();
    let outcome = finalize_reservation_attempt(&reservation_id, Some(true));
    assert_eq!(
        outcome,
        ReservationOutcome::Reserved(reservation_id.clone())
    );

    // 4. Scripted boltzcli createreverseswap call.
    let cli = FakeBoltzCli::new();
    cli.push_ok(json!({"id": "swap-xyz", "state": "swap.created", "boltzFee": 500}).to_string());
    let result = run_json(&cli, &["createreverseswap", "--json", "--", "500000"], 180).unwrap();
    let primary = result.as_object().unwrap();
    assert!(!is_error_swap(primary));
    assert_eq!(estimate_swap_fee_sats(&result), fee_sats);

    // 5. Outcome classification -> cooldown decision.
    let final_ts = cooldown_after_attempt(
        prior_cooldown_ts,
        pre_claimed_ts,
        SwapAttemptOutcome::Accepted,
    );
    assert_eq!(
        final_ts, pre_claimed_ts,
        "accepted swap must burn the cooldown"
    );

    // 6. Journal merge (source="loop_out" is one of the spend-recording
    // sources).
    let merged = merge_swap_results(&[], std::slice::from_ref(primary), "loop_out", None, now);
    assert_eq!(merged.len(), 1);
    assert!(
        merged[0].should_record_spend,
        "an executed, non-error swap must deplete the boltz spend category"
    );
    assert_eq!(merged[0].fee_sats, 500);
}

#[test]
fn rejected_quote_does_not_burn_cooldown_or_reserve_budget() {
    let now = 1_700_000_000i64;
    let prior_ts = 900i64;
    let pre_claimed_ts = now;

    // Budget too small: reservation gate fires, engine reports "would
    // exceed" (Some(false)).
    let gate = reservation_gate(true, true, 5_000, Some(1_000));
    assert!(matches!(gate, ReservationGate::Attempt { .. }));
    let outcome = finalize_reservation_attempt("boltz-swap:test-2", Some(false));
    assert_eq!(outcome, ReservationOutcome::Rejected);

    // A rejected reservation means the swap is never attempted — the
    // cooldown pre-claim (if one had been made speculatively) must be
    // rolled back.
    let final_ts = cooldown_after_attempt(
        prior_ts,
        pre_claimed_ts,
        SwapAttemptOutcome::RejectedOrError,
    );
    assert_eq!(final_ts, prior_ts);

    // No swap entry exists to journal (the create call never ran).
    let merged = merge_swap_results(&[], &[], "loop_out", None, now);
    assert!(merged.is_empty());
}

#[test]
fn dry_run_candidate_never_burns_cooldown_even_when_reservation_would_succeed() {
    // Control against a subtle bug class: a passing budget gate must NOT
    // by itself burn the cooldown slot in a dry-run preview.
    let prior_ts = 500i64;
    let pre_claimed_ts = 1000i64;
    let gate = reservation_gate(true, true, 100, Some(10_000));
    assert!(matches!(gate, ReservationGate::Attempt { .. }));
    // In a real dry-run, no reservation call and no CLI call are made at
    // all; the cooldown outcome is decided purely by DryRun regardless.
    let final_ts = cooldown_after_attempt(prior_ts, pre_claimed_ts, SwapAttemptOutcome::DryRun);
    assert_eq!(final_ts, prior_ts);
}
