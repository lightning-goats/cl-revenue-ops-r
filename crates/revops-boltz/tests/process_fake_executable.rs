#![cfg(unix)]

use revops_boltz::cli::BoltzCli;
use revops_boltz::commands::{self, ActionOutcome};
use revops_boltz::error::{CliError, CreateOutcome};
use revops_boltz::execution::ExecutionMode;
use revops_boltz::process::{BoltzCliProcessConfig, ProcessBoltzCli};
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

fn adapter(cli_path: &Path, datadir: &Path, timeout_seconds: u64) -> ProcessBoltzCli {
    ProcessBoltzCli::new(BoltzCliProcessConfig {
        enabled: true,
        cli_path: cli_path.display().to_string(),
        datadir: datadir.display().to_string(),
        use_sudo: false,
        sudo_user: "unused".to_string(),
        timeout_seconds,
    })
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
        .run(&["getinfo"], 5)
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
        .run(&["getinfo"], 5)
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
        .run(&["getinfo"], 5)
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

    let output = adapter(&fake, dir.path(), 5).run(&["getinfo"], 5).unwrap();
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
fn real_process_timeout_on_create_is_unknown_not_rejected() {
    let _guard = process_test_guard();
    let dir = TempDir::new().unwrap();
    let fake = fake_executable(&dir, "ambiguous-create", "exec sleep 30");
    let cli = adapter(&fake, dir.path(), 30);

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
            assert!(command.contains("createswap"));
            assert!(command.contains("100000"));
        }
        other => panic!("timed-out real create must be Unknown, got {other:?}"),
    }
}
