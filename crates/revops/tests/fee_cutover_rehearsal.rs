//! Task 11: copied-state fake-RPC fee-cutover rehearsal harness.
//!
//! Every case here runs the harness binary against a throwaway rehearsal root
//! containing COPIES of a Python and a Rust SQLite database plus a fake Unix
//! socket. Nothing in this file may name a reachable live path except inside an
//! explicit rejection assertion (see `refuses_*`).
//!
//! The harness exists to rehearse the one-time fee-authority handoff end to
//! end -- activation, every arm denial, each failure injection, the ambiguous
//! transport outcome, restart quarantine, reconciliation, and ordered rollback
//! -- without ever touching production. The one property worth more than all
//! the others: a rehearsal must be structurally incapable of reaching the real
//! node, so a mistake here can never move a real fee.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const SCHEMA: &str = "revops_fee_cutover_rehearsal/v1";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_rehearse_fee_cutover")
}

fn run(args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .output()
        .expect("run rehearse_fee_cutover")
}

/// Exactly one JSON object on stdout, stderr empty -- same machine-output
/// contract as `replay_fee_capture` (see `tests/replay_cli.rs`).
fn one_json(output: &Output) -> Value {
    assert!(
        output.stderr.is_empty(),
        "stderr must stay empty; machine output belongs on stdout: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = std::str::from_utf8(&output.stdout).expect("stdout UTF-8");
    let lines: Vec<_> = stdout.lines().collect();
    assert_eq!(lines.len(), 1, "stdout must be exactly one JSON object");
    serde_json::from_str(lines[0]).expect("stdout JSON")
}

/// A fresh, short, explicitly temporary rehearsal root — never under any
/// production directory.
///
/// Deliberately NOT `CARGO_TARGET_TMPDIR`: a Unix socket path must fit in
/// `sockaddr_un` (~108 bytes) and the real broadcaster connects by absolute
/// path, so a deep target dir under a worktree overflows `SUN_LEN` before the
/// harness can be exercised at all. A short `/tmp` root keeps every artefact
/// under one explicit root — which is what the isolation contract actually
/// requires — while staying inside the socket limit.
fn rehearsal_root(case: &str) -> PathBuf {
    let root = Path::new("/tmp").join(format!("revops-rh-{case}"));
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("clear stale rehearsal root");
    }
    std::fs::create_dir_all(&root).expect("create rehearsal root");
    root
}

fn rehearse(case: &str, scenario: &str) -> Value {
    let root = rehearsal_root(case);
    let out = run(&[
        "--rehearsal-root",
        root.to_str().expect("utf-8 root"),
        "--scenario",
        scenario,
    ]);
    let value = one_json(&out);
    assert_eq!(
        value["schema_version"], SCHEMA,
        "versioned evidence required"
    );
    assert_eq!(value["scenario"], scenario);
    value
}

// ---------------------------------------------------------------- isolation

#[test]
fn help_lists_the_required_flags_and_exits_zero() {
    let out = run(&["--help"]);
    assert!(out.status.success(), "--help must exit 0");
    let text = String::from_utf8_lossy(&out.stdout);
    for flag in ["--rehearsal-root", "--scenario", "--list-scenarios"] {
        assert!(text.contains(flag), "--help must document {flag}");
    }
}

#[test]
fn refuses_to_run_without_an_explicit_rehearsal_root() {
    let out = run(&["--scenario", "valid_activation"]);
    assert!(
        !out.status.success(),
        "a rehearsal without an explicit root must be refused, not defaulted"
    );
}

#[test]
fn refuses_a_live_looking_socket_path() {
    let root = rehearsal_root("live-socket");
    let out = run(&[
        "--rehearsal-root",
        root.to_str().unwrap(),
        "--scenario",
        "valid_activation",
        // The one place a production-looking path may appear: proving refusal.
        "--socket-path",
        "/data/lightningd/lightning-rpc",
    ]);
    assert!(!out.status.success(), "live socket path must be refused");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.to_lowercase().contains("refus") || text.to_lowercase().contains("reject"),
        "refusal must say so explicitly, got: {text}"
    );
}

#[test]
fn refuses_a_live_looking_database_path() {
    let root = rehearsal_root("live-db");
    let out = run(&[
        "--rehearsal-root",
        root.to_str().unwrap(),
        "--scenario",
        "valid_activation",
        "--rust-db",
        "/data/lightningd/revops-observer.sqlite3",
    ]);
    assert!(!out.status.success(), "live db path must be refused");
}

#[test]
fn every_database_it_opens_lives_under_the_rehearsal_root() {
    let evidence = rehearse("isolation-paths", "valid_activation");
    let root = evidence["rehearsal_root"]
        .as_str()
        .expect("evidence records the root");
    for key in ["python_db", "rust_db", "socket_path"] {
        let p = evidence["isolation"][key]
            .as_str()
            .unwrap_or_else(|| panic!("evidence must record isolation.{key}"));
        assert!(
            p.starts_with(root),
            "isolation.{key} = {p} must live under the rehearsal root {root}"
        );
    }
    assert_eq!(
        evidence["isolation"]["fake_rpc"], true,
        "the rehearsal must record that RPC was faked"
    );
}

#[test]
fn copies_the_source_databases_and_never_opens_them_writable() {
    let evidence = rehearse("isolation-copies", "valid_activation");
    assert_eq!(
        evidence["isolation"]["source_dbs_copied"], true,
        "source databases must be copied, never used in place"
    );
    assert_eq!(
        evidence["isolation"]["source_opened_writable"], false,
        "no source database may be opened writable"
    );
}

// ------------------------------------------------------- activation + denials

#[test]
fn valid_activation_succeeds_exactly_once() {
    let evidence = rehearse("valid", "valid_activation");
    assert_eq!(evidence["outcome"], "activated");
    assert_eq!(
        evidence["arm_consumed"], true,
        "a successful activation consumes its arm"
    );
    assert_eq!(
        evidence["replay_refused"], true,
        "re-presenting the same consumed arm must be refused within the same rehearsal"
    );
}

#[test]
fn python_still_authoritative_blocks_activation() {
    let evidence = rehearse("py-auth", "python_still_authoritative");
    assert_eq!(evidence["outcome"], "denied");
    assert!(
        evidence["deny_reason"]
            .as_str()
            .expect("deny_reason")
            .to_lowercase()
            .contains("authority"),
        "denial must name Python authority, got {:?}",
        evidence["deny_reason"]
    );
}

/// Each arm-shaped denial must be reached and reported distinctly -- a harness
/// that collapsed them all to one "invalid arm" would hide which gate fired.
#[test]
fn every_arm_denial_is_reported_distinctly() {
    let cases = [
        "arm_early",
        "arm_expired",
        "arm_wrong_node",
        "arm_wrong_commit",
        "arm_wrong_hash",
    ];
    let mut seen: Vec<String> = Vec::new();
    for scenario in cases {
        let evidence = rehearse(scenario, scenario);
        assert_eq!(evidence["outcome"], "denied", "{scenario} must be denied");
        let reason = evidence["deny_reason"]
            .as_str()
            .unwrap_or_else(|| panic!("{scenario} must report a deny_reason"))
            .to_string();
        assert!(
            !seen.contains(&reason),
            "{scenario} reused deny_reason {reason:?}; each gate must be distinguishable"
        );
        seen.push(reason);
        // Verified against cutover_arm::validate_and_consume at 7bf47de: every
        // content check (WrongNode/WrongCommit/WrongBinaryHash/NotYetValid/
        // Expired) returns Err BEFORE the RENAME_NOREPLACE step, so a DENIED
        // arm is deliberately left in place. That is the safer design -- the
        // nonce is only burned by a real consumption, and the operator can
        // inspect and correct the rejected file. An earlier draft of this test
        // asserted the opposite; the source, not the assumption, won.
        assert_eq!(
            evidence["arm_consumed"], false,
            "{scenario}: a denied arm must NOT be consumed -- only success burns the nonce"
        );
    }
}

// -------------------------------------------------------- failure injections

#[test]
fn state_flush_failure_aborts_before_any_broadcast() {
    let evidence = rehearse("flush-fail", "state_flush_failure");
    assert_eq!(evidence["outcome"], "aborted");
    assert_eq!(
        evidence["mutation_calls"], 0,
        "a state flush failure must abort before any mutation call is attempted"
    );
}

#[test]
fn governor_denial_aborts_before_any_broadcast() {
    let evidence = rehearse("gov-deny", "governor_denial");
    assert_eq!(evidence["outcome"], "aborted");
    assert_eq!(evidence["mutation_calls"], 0);
}

#[test]
fn ledger_failure_aborts_and_is_recorded() {
    let evidence = rehearse("ledger-fail", "ledger_failure");
    assert_eq!(evidence["outcome"], "aborted");
    assert!(
        evidence["ledger_error"].is_string(),
        "a ledger failure must be recorded, not swallowed"
    );
}

#[test]
fn explicit_rejection_is_a_clean_failure_with_no_quarantine() {
    let evidence = rehearse("explicit-reject", "explicit_rejection");
    assert_eq!(evidence["outcome"], "rejected");
    assert_eq!(
        evidence["quarantined"], false,
        "an explicit rejection is a known outcome and must not quarantine"
    );
}

#[test]
fn ambiguous_result_quarantines_rather_than_guessing() {
    let evidence = rehearse("ambiguous", "ambiguous_result");
    assert_eq!(evidence["outcome"], "ambiguous");
    assert_eq!(
        evidence["quarantined"], true,
        "an ambiguous transport outcome must quarantine; guessing either way is worse"
    );
}

// ------------------------------------------ restart, reconciliation, rollback

#[test]
fn restart_reconciles_quarantine_before_accepting_any_arm() {
    let evidence = rehearse("restart", "restart_quarantine");
    assert_eq!(evidence["outcome"], "reconciled");
    let order = evidence["order"]
        .as_array()
        .expect("restart must record its step order");
    let steps: Vec<&str> = order.iter().filter_map(|v| v.as_str()).collect();
    let recon = steps
        .iter()
        .position(|s| s.contains("reconcile"))
        .expect("order must contain reconcile");
    let arm = steps
        .iter()
        .position(|s| s.contains("arm"))
        .expect("order must contain the arm step");
    assert!(
        recon < arm,
        "quarantine reconciliation must precede any arm acceptance, got {steps:?}"
    );
}

#[test]
fn reconciliation_resolves_a_pending_quarantine_entry() {
    let evidence = rehearse("reconcile", "reconciliation");
    assert_eq!(evidence["outcome"], "reconciled");
    // Corrected against revops-db source: reconcile_quarantine_on_restart acts
    // on orphaned broadcast ATTEMPTS (intent recorded, no result), marks them
    // Ambiguous, and then INSERTS a quarantine — it does not clear one. An
    // earlier draft of this test asserted the opposite and was wrong.
    assert!(
        evidence["attempts_reconciled"].as_u64().unwrap_or(0) >= 1,
        "reconciliation must reconcile at least one orphaned broadcast attempt"
    );
    assert_eq!(
        evidence["quarantine_active_after"], true,
        "reconciliation must leave execution quarantined, not cleared"
    );
}

#[test]
fn rollback_unwinds_in_reverse_order() {
    let evidence = rehearse("rollback", "ordered_rollback");
    assert_eq!(evidence["outcome"], "rolled_back");
    let applied: Vec<String> = evidence["order"]
        .as_array()
        .expect("applied order")
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    let undone: Vec<String> = evidence["rollback_order"]
        .as_array()
        .expect("rollback order")
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    assert!(
        !applied.is_empty(),
        "rollback needs applied steps to unwind"
    );
    let mut expected = applied.clone();
    expected.reverse();
    assert_eq!(
        undone, expected,
        "rollback must unwind in exactly reverse order"
    );
}

// ------------------------------------------------------------ coverage guard

/// The task enumerates the scenarios the harness must cover. If the binary
/// stops offering one, that is a silent loss of rehearsal coverage -- so the
/// list itself is asserted rather than left implicit.
#[test]
fn harness_offers_every_required_scenario() {
    let out = run(&["--list-scenarios"]);
    assert!(out.status.success(), "--list-scenarios must exit 0");
    let listed = String::from_utf8_lossy(&out.stdout);
    for required in [
        "valid_activation",
        "python_still_authoritative",
        "arm_early",
        "arm_expired",
        "arm_wrong_node",
        "arm_wrong_commit",
        "arm_wrong_hash",
        "state_flush_failure",
        "governor_denial",
        "ledger_failure",
        "explicit_rejection",
        "ambiguous_result",
        "restart_quarantine",
        "reconciliation",
        "ordered_rollback",
    ] {
        assert!(
            listed.contains(required),
            "--list-scenarios must offer {required}, got: {listed}"
        );
    }
}

// ------------------------------------------- false-clean outcome regressions
//
// Found by the Rust verifier in task-29 review round 1: the transport arm
// matched `(_, "ambiguous_result")`, wildcarding the RESULT. So a socket that
// could not even be reached (SUN_LEN overflow -> `CleanFailure`, zero bytes
// sent) was still headlined `outcome: "ambiguous"` — a harness reporting a
// rehearsed ambiguous transport outcome that never happened. Same bug class as
// the LN+ 422, `_finalize`, and `params_json` defects audited the same day.

/// A rehearsal root deep enough that `<root>/fake-cln.sock` cannot fit in
/// `sockaddr_un` (~108 bytes).
fn oversized_root() -> PathBuf {
    let deep = "d".repeat(120);
    let root = Path::new("/tmp").join(format!("revops-rh-long-{deep}"));
    let _ = std::fs::create_dir_all(&root);
    root
}

#[test]
fn an_unreachable_socket_is_never_reported_as_a_rehearsed_ambiguous_outcome() {
    let root = oversized_root();
    let out = run(&[
        "--rehearsal-root",
        root.to_str().expect("utf-8"),
        "--scenario",
        "ambiguous_result",
    ]);
    // The headline must not claim a rehearsed outcome the run never reached.
    if out.status.success() {
        let value = one_json(&out);
        panic!(
            "a root whose socket path cannot fit SUN_LEN must be REFUSED, not reported as a \
             rehearsed outcome; got outcome={:?} mutation_calls={:?} quarantined={:?}",
            value["outcome"], value["mutation_calls"], value["quarantined"]
        );
    }
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let lower = text.to_lowercase();
    assert!(
        lower.contains("refus") || lower.contains("reject"),
        "the refusal must say so explicitly, got: {text}"
    );
    assert!(
        lower.contains("sun_len") || lower.contains("socket path") || lower.contains("too long"),
        "the refusal must name the socket-length cause so the operator can fix the root, got: {text}"
    );
}

/// The fail-fast guard must fire for EVERY scenario, not just the ones that
/// happen to bind a socket — an oversized root is a bad input, not a per-
/// scenario accident.
#[test]
fn an_oversized_rehearsal_root_is_refused_for_every_scenario() {
    let root = oversized_root();
    let out = run(&["--list-scenarios"]);
    let listed = String::from_utf8_lossy(&out.stdout);
    for scenario in listed.lines().filter(|l| !l.trim().is_empty()) {
        let out = run(&[
            "--rehearsal-root",
            root.to_str().expect("utf-8"),
            "--scenario",
            scenario,
        ]);
        assert!(
            !out.status.success(),
            "{scenario} accepted an oversized rehearsal root; the guard must be input-level"
        );
    }
}

/// When `ambiguous_result` DOES report ambiguous, the evidence must name the
/// exact `BroadcastError` variant that justified it. Without this the headline
/// is unfalsifiable — which is how the wildcard survived review in the first
/// place.
#[test]
fn ambiguous_outcome_names_the_variant_that_justified_it() {
    let evidence = rehearse("ambiguous-variant", "ambiguous_result");
    assert_eq!(evidence["outcome"], "ambiguous");
    let variant = evidence["matched_variant"]
        .as_str()
        .expect("evidence must name the matched BroadcastError variant");
    assert_eq!(
        variant, "Ambiguous",
        "outcome=ambiguous must be justified by BroadcastError::Ambiguous, not any error"
    );
}

/// Mirror of the above for the rejection path, so both transport headlines are
/// tied to a named variant rather than to the scenario name.
#[test]
fn rejected_outcome_names_the_variant_that_justified_it() {
    let evidence = rehearse("rejected-variant", "explicit_rejection");
    assert_eq!(evidence["outcome"], "rejected");
    assert_eq!(
        evidence["matched_variant"].as_str(),
        Some("Rejected"),
        "outcome=rejected must be justified by BroadcastError::Rejected"
    );
}

/// Discriminating guard for EVERY risky arm (review round 1 sweep): each
/// scenario must attribute its headline to the one variant it exists to
/// exercise. Before this, any deny reason satisfied any scenario — so the wrong
/// gate firing would still have produced a correct-looking headline, which is
/// the same false-clean shape as the `ambiguous_result` wildcard.
#[test]
fn every_denial_arm_names_the_exact_variant_it_exercises() {
    let expected = [
        ("python_still_authoritative", "PythonAuthority"),
        ("arm_early", "NotYetValid"),
        ("arm_expired", "Expired"),
        ("arm_wrong_node", "WrongNode"),
        ("arm_wrong_commit", "WrongCommit"),
        ("arm_wrong_hash", "WrongBinaryHash"),
        ("state_flush_failure", "StateGenerationStale"),
        ("governor_denial", "GovernorDenied"),
        ("ledger_failure", "LedgerReservationMissing"),
        ("explicit_rejection", "Rejected"),
        ("ambiguous_result", "Ambiguous"),
    ];
    for (scenario, variant) in expected {
        let evidence = rehearse(&format!("variant-{scenario}"), scenario);
        assert_eq!(
            evidence["matched_variant"].as_str(),
            Some(variant),
            "{scenario} must attribute its outcome to {variant}, got {:?}",
            evidence["matched_variant"]
        );
        // And the recorded reason must actually begin with that variant, so the
        // label cannot drift away from the reason it claims to summarise.
        let reason = evidence["deny_reason"]
            .as_str()
            .or_else(|| evidence["result"].as_str())
            .unwrap_or("");
        assert!(
            reason.contains(variant),
            "{scenario}: recorded reason {reason:?} must contain {variant}"
        );
    }
}

/// `restart_quarantine` must not invent a reconciliation tally.
///
/// `ClnFeeBroadcaster::new` reconciles internally and returns no count, so the
/// harness previously reported a hardcoded `1`. It now reports no count at all
/// and is judged on the measured quarantine transition.
#[test]
fn restart_quarantine_reports_no_invented_tally() {
    let evidence = rehearse("restart-no-tally", "restart_quarantine");
    assert_eq!(evidence["outcome"], "reconciled");
    assert!(
        evidence["attempts_reconciled"].is_null(),
        "restart_quarantine must not report a tally it cannot measure, got {:?}",
        evidence["attempts_reconciled"]
    );
    assert_eq!(
        evidence["reconciliation_observed"], true,
        "the measured absent->present quarantine transition must be the evidence instead"
    );
}

/// The reconciliation scenario, by contrast, CAN measure a count because it
/// calls the API that returns one — so it must report a real number.
#[test]
fn reconciliation_reports_a_measured_tally() {
    let evidence = rehearse("recon-tally", "reconciliation");
    assert!(
        evidence["attempts_reconciled"].as_u64().unwrap_or(0) >= 1,
        "reconciliation calls the API that returns a tally and must report it"
    );
    assert_eq!(evidence["reconciliation_observed"], true);
}
