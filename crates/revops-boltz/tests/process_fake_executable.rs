#![cfg(unix)]

use revops_boltz::cli::BoltzCli;
use revops_boltz::commands::{self, ActionOutcome};
use revops_boltz::error::{CliError, CreateOutcome};
use revops_boltz::execution::ExecutionMode;
use revops_boltz::process::{
    ArmedBoltzCli, BoltzCliProcessConfig, ProcessBoltzCli, MAX_CAPTURE_BYTES,
};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tempfile::TempDir;

static PROCESS_TEST_LOCK: Mutex<()> = Mutex::new(());

fn process_test_guard() -> MutexGuard<'static, ()> {
    PROCESS_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn fake_executable(dir: &TempDir, name: &str, body: &str) -> PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&path, permissions).unwrap();
    path
}

fn config(cli_path: &Path, datadir: &Path, timeout_seconds: u64) -> BoltzCliProcessConfig {
    // `enabled` (and every other field) is private post-hardening: a
    // constructed config can never be re-armed by field mutation.
    BoltzCliProcessConfig::new(
        true,
        cli_path.display().to_string(),
        datadir.display().to_string(),
        false,
        "unused".to_string(),
        timeout_seconds,
    )
}

fn adapter(cli_path: &Path, datadir: &Path, timeout_seconds: u64) -> ProcessBoltzCli {
    ProcessBoltzCli::new(config(cli_path, datadir, timeout_seconds))
}

fn armed(cli_path: &Path, datadir: &Path, timeout_seconds: u64) -> ArmedBoltzCli {
    ArmedBoltzCli::new(config(cli_path, datadir, timeout_seconds))
}

fn assert_pid_gone(pid_file: &Path) {
    let pid = fs::read_to_string(pid_file).unwrap();
    let proc_path = PathBuf::from(format!("/proc/{}", pid.trim()));
    let deadline = Instant::now() + Duration::from_secs(2);
    while proc_path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        !proc_path.exists(),
        "timed-out fake executable survived at {}",
        proc_path.display()
    );
}

#[test]
fn success_runs_real_fake_executable_with_exact_argv_and_trimmed_stdout() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let datadir = dir.path().join("isolated data");
    fs::create_dir(&datadir).unwrap();
    let fake = fake_executable(
        &dir,
        "fake-boltzcli",
        "for arg in \"$@\"; do printf '<%s>\\n' \"$arg\"; done",
    );

    let output = adapter(&fake, &datadir, 5)
        .run(&["listswaps", "--json"], 5)
        .unwrap();

    assert_eq!(
        output,
        format!(
            "<--datadir>\n<{}>\n<listswaps>\n<--json>",
            datadir.display()
        )
    );
}

#[test]
fn nonzero_exit_prefers_trimmed_stderr_and_preserves_exit_code() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let fake = fake_executable(
        &dir,
        "stderr-failure",
        "printf 'stdout detail\\n'; printf '  stderr detail  \\n' >&2; exit 23",
    );

    let err = adapter(&fake, dir.path(), 5)
        .run(&["stats"], 5)
        .unwrap_err();
    assert_eq!(
        err,
        CliError::ExitFailure {
            code: Some(23),
            message: "stderr detail".to_string(),
        }
    );
}

#[test]
fn nonzero_exit_falls_back_to_trimmed_stdout_when_stderr_is_empty() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let fake = fake_executable(
        &dir,
        "stdout-failure",
        "printf '  stdout detail  \\n'; exit 24",
    );

    let err = adapter(&fake, dir.path(), 5)
        .run(&["stats"], 5)
        .unwrap_err();
    assert_eq!(
        err,
        CliError::ExitFailure {
            code: Some(24),
            message: "stdout detail".to_string(),
        }
    );
}

#[test]
fn missing_executable_error_names_the_exact_program_that_failed() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("does-not-exist-boltzcli");

    let err = adapter(&missing, dir.path(), 5)
        .run(&["stats"], 5)
        .unwrap_err();
    match err {
        CliError::NotFound { message } => assert!(
            message.contains(&missing.display().to_string()),
            "missing executable path absent from error: {message}"
        ),
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[test]
fn simultaneous_large_stdout_and_stderr_do_not_deadlock() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let fake = fake_executable(
        &dir,
        "chatty-boltzcli",
        "i=0; while [ \"$i\" -lt 4096 ]; do printf '0123456789abcdef0123456789abcdef'; printf 'fedcba9876543210fedcba9876543210' >&2; i=$((i + 1)); done",
    );

    let output = adapter(&fake, dir.path(), 5).run(&["stats"], 5).unwrap();
    assert_eq!(output.len(), 4096 * 32);
    assert!(output.starts_with("0123456789abcdef"));
    assert!(output.ends_with("0123456789abcdef"));
}

#[test]
fn zero_timeout_uses_configured_timeout_and_kills_and_reaps_child() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let pid_file = dir.path().join("configured-timeout.pid");
    let fake = fake_executable(
        &dir,
        "configured-timeout",
        &format!(
            "printf '%s' \"$$\" > '{}'\nexec sleep 30",
            pid_file.display()
        ),
    );

    let err = adapter(&fake, dir.path(), 1)
        .run(&["listswaps"], 0)
        .unwrap_err();
    match err {
        CliError::Timeout {
            timeout_secs,
            command,
        } => {
            assert_eq!(timeout_secs, 1);
            assert!(command.contains("listswaps"));
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
    assert_pid_gone(&pid_file);
}

#[test]
fn explicit_timeout_overrides_config_and_kills_and_reaps_child() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let pid_file = dir.path().join("override-timeout.pid");
    let fake = fake_executable(
        &dir,
        "override-timeout",
        &format!(
            "printf '%s' \"$$\" > '{}'\nexec sleep 30",
            pid_file.display()
        ),
    );

    let err = adapter(&fake, dir.path(), 30)
        .run(&["listswaps"], 1)
        .unwrap_err();
    assert!(matches!(
        err,
        CliError::Timeout {
            timeout_secs: 1,
            ..
        }
    ));
    assert_pid_gone(&pid_file);
}

#[test]
fn real_process_timeout_on_create_is_unknown_with_redacted_command() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let fake = fake_executable(&dir, "ambiguous-create", "exec sleep 30");
    // A create is a fund-moving verb: only the ARMED transport can carry
    // it (the query transport refuses it structurally -- see the
    // allowlist test below).
    let cli = armed(&fake, dir.path(), 30);

    let outcome = commands::execute_loop_in(
        &cli,
        ExecutionMode::Armed,
        "test-wallet",
        Some("BTC"),
        100_000,
        1,
    )
    .unwrap();

    match outcome {
        ActionOutcome::Executed(CreateOutcome::Unknown {
            timeout_secs,
            command,
        }) => {
            assert_eq!(timeout_secs, 1);
            // The redacted label names the SUBCOMMAND, never the values:
            // amounts, wallets, and destinations must not leak through
            // timeout errors into RPC responses or logs.
            assert!(command.contains("createswap"), "{command}");
            assert!(
                !command.contains("100000"),
                "argv values leaked into the timeout error: {command}"
            );
            assert!(
                !command.contains("test-wallet"),
                "wallet name leaked into the timeout error: {command}"
            );
        }
        other => panic!("timed-out real create must be Unknown, got {other:?}"),
    }
}

/// The QUERY transport refuses every non-allowlisted subcommand WITHOUT
/// spawning: fund-moving verbs and the mnemonic are structurally
/// unreachable through a `ProcessBoltzCli`, no matter what argv a caller
/// hands it.
#[test]
fn query_transport_refuses_fund_moving_verbs_without_spawning() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("spawned.marker");
    let fake = fake_executable(
        &dir,
        "marking-boltzcli",
        &format!("touch '{}'\nprintf '{{}}'", marker.display()),
    );
    let cli = adapter(&fake, dir.path(), 5);

    for refused in [
        vec!["createswap", "--json"],
        vec!["createreverseswap", "--json"],
        vec!["createchainswap", "--json"],
        vec!["refundswap", "--", "swap-1", "wallet"],
        vec!["claimswaps", "--", "wallet", "a"],
        vec!["wallet", "send", "w", "dest", "1000"],
        vec!["swapmnemonic", "get"],
        vec!["backup"],
    ] {
        let err = cli.run(&refused, 5).unwrap_err();
        assert!(
            matches!(err, CliError::TransportRefused { .. }),
            "{refused:?} must refuse at the transport, got {err:?}"
        );
    }
    assert!(
        !marker.exists(),
        "a refused subcommand must never reach the wire (no spawn)"
    );

    // Allowlisted reads still run.
    for allowed in [
        vec!["listswaps", "--json"],
        vec!["swapinfo", "--", "swap-1"],
        vec![
            "quote", "--json", "--send", "50000", "--to", "BTC", "reverse",
        ],
        vec!["wallet", "list", "--json"],
        vec!["wallet", "receive", "w"],
    ] {
        cli.run(&allowed, 5)
            .unwrap_or_else(|e| panic!("{allowed:?} must be allowlisted, got {e:?}"));
    }
    assert!(marker.exists());
}

/// The ARMED transport carries fund-moving verbs (it exists only inside
/// the Task-69-minted capability; the transport itself does not gate).
#[test]
fn armed_transport_runs_fund_moving_verbs() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let fake = fake_executable(&dir, "create-ok", "printf '{\"id\": \"swap-xx\"}'");
    let out = armed(&fake, dir.path(), 5)
        .run(&["createswap", "--json"], 5)
        .unwrap();
    assert!(out.contains("swap-xx"));
}

/// A timed-out child's whole PROCESS TREE dies -- including grandchildren
/// (the sudo shape would otherwise orphan the real boltzcli).
#[test]
fn timeout_kills_the_whole_process_tree() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let gchild_pid = dir.path().join("grandchild.pid");
    let fake = fake_executable(
        &dir,
        "tree-spawner",
        &format!(
            "sleep 30 &\nprintf '%s' \"$!\" > '{}'\nwait",
            gchild_pid.display()
        ),
    );

    let err = adapter(&fake, dir.path(), 1)
        .run(&["listswaps"], 1)
        .unwrap_err();
    assert!(matches!(err, CliError::Timeout { .. }), "{err:?}");
    assert_pid_gone(&gchild_pid);
}

/// Output capture is BOUNDED: a chatty child cannot OOM the plugin. Over
/// the cap the result is truncated with an explicit marker, and the
/// child still exits cleanly (drained, not deadlocked).
#[test]
fn oversized_stdout_is_truncated_with_marker() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    // 512 KiB of stdout -- double the cap.
    let fake = fake_executable(
        &dir,
        "flooder",
        "i=0; while [ \"$i\" -lt 16384 ]; do printf '0123456789abcdef0123456789abcdef'; i=$((i + 1)); done",
    );

    let output = adapter(&fake, dir.path(), 10)
        .run(&["listswaps"], 10)
        .unwrap();
    assert!(
        output.len() <= MAX_CAPTURE_BYTES + 64,
        "capture must be bounded, got {} bytes",
        output.len()
    );
    assert!(
        output.contains("[truncated"),
        "over-cap output must carry an explicit truncation marker"
    );
}

/// Failure messages are sanitized: any line naming the mnemonic is
/// dropped, and the whole message is capped at 300 CHARS (char-safe --
/// multibyte text at the boundary must not panic).
#[test]
fn failure_messages_are_scrubbed_and_char_capped() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let fake = fake_executable(
        &dir,
        "leaky-failure",
        "printf 'Swap Mnemonic: abandon ability able about\\nplain detail\\n' >&2; exit 9",
    );
    let err = adapter(&fake, dir.path(), 5)
        .run(&["listswaps"], 5)
        .unwrap_err();
    match err {
        CliError::ExitFailure { code, message } => {
            assert_eq!(code, Some(9));
            assert!(
                !message.to_ascii_lowercase().contains("abandon"),
                "mnemonic words leaked: {message}"
            );
            assert!(message.contains("[line redacted]"), "{message}");
            assert!(message.contains("plain detail"), "{message}");
        }
        other => panic!("{other:?}"),
    }

    // Char cap: 400 multibyte chars of stderr -> exactly <=300 chars,
    // no byte-boundary panic.
    let fake = fake_executable(
        &dir,
        "multibyte-failure",
        "i=0; while [ \"$i\" -lt 400 ]; do printf 'é'; i=$((i + 1)); done >&2; exit 8",
    );
    let err = adapter(&fake, dir.path(), 5)
        .run(&["listswaps"], 5)
        .unwrap_err();
    match err {
        CliError::ExitFailure { message, .. } => {
            assert!(
                message.chars().count() <= 300,
                "{}",
                message.chars().count()
            );
        }
        other => panic!("{other:?}"),
    }
}
