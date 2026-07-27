//! The real, subprocess-backed [`BoltzCli`] implementation.
//!
//! Ports py `_base_cmd`/`_run`/`_ensure_enabled` (boltz_manager.py:433-467):
//! `sudo -n -u <user>` prefixing (when configured), `<cli_path> --datadir
//! <dir>` base, then the caller's argv tail, executed with a timeout and
//! trimmed stdout/stderr capture.
//!
//! Split deliberately into two halves per the crate-wide rule that no test
//! may ever spawn a real process:
//! - [`base_argv`] is PURE (config + args in, `Vec<String>` out) and is the
//!   only part of this module exercised by this crate's tests.
//! - [`ProcessBoltzCli::run`] performs the actual `std::process::Command`
//!   spawn/wait/kill-on-timeout. Nothing in this crate's test suite calls
//!   it (there is no `boltzcli` binary in a test/CI sandbox, and the HARD
//!   RULES forbid a test from spawning a real subprocess regardless) — its
//!   correctness rests on matching [`base_argv`] (tested) plus review, the
//!   same boundary `cli.rs`'s module docs describe for this exact seam.

use crate::cli::BoltzCli;
use crate::error::CliError;
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Ports py `BoltzCliConfig` (boltz_manager.py:236-242) — field-for-field,
/// including its defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoltzCliProcessConfig {
    pub enabled: bool,
    pub cli_path: String,
    pub datadir: String,
    pub use_sudo: bool,
    pub sudo_user: String,
    pub timeout_seconds: u64,
}

impl Default for BoltzCliProcessConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cli_path: "/usr/local/bin/boltzcli".to_string(),
            datadir: "/var/lib/boltz".to_string(),
            use_sudo: false,
            sudo_user: "boltz".to_string(),
            timeout_seconds: 60,
        }
    }
}

/// py `_base_cmd` (boltz_manager.py:437-442): `["sudo", "-n", "-u",
/// sudo_user]` prefix (only when `use_sudo`), then `[cli_path, "--datadir",
/// datadir]`. Pure — takes the argv tail and returns the full argv,
/// `argv[0]` being the program to exec (`sudo` or `cli_path`).
pub fn base_argv(config: &BoltzCliProcessConfig, args: &[&str]) -> Vec<String> {
    let mut cmd = Vec::new();
    if config.use_sudo {
        cmd.push("sudo".to_string());
        cmd.push("-n".to_string());
        cmd.push("-u".to_string());
        cmd.push(config.sudo_user.clone());
    }
    cmd.push(config.cli_path.clone());
    cmd.push("--datadir".to_string());
    cmd.push(config.datadir.clone());
    cmd.extend(args.iter().map(|s| s.to_string()));
    cmd
}

/// Real [`BoltzCli`] adapter. Construct with [`BoltzCliProcessConfig`] —
/// note `enabled` defaults to `false` (py `_ensure_enabled`,
/// boltz_manager.py:433-435: `run` returns [`CliError::Disabled`] unless
/// explicitly turned on), independent of and in addition to this crate's
/// [`crate::execution::ExecutionMode`] gate on any fund-moving call.
#[derive(Debug, Clone)]
pub struct ProcessBoltzCli {
    pub config: BoltzCliProcessConfig,
}

impl ProcessBoltzCli {
    pub fn new(config: BoltzCliProcessConfig) -> Self {
        Self { config }
    }
}

/// Poll-and-kill-on-timeout process execution. `argv[0]` is the program;
/// the rest are its arguments. Reader threads drain stdout/stderr
/// concurrently with the poll loop so a chatty child cannot deadlock on a
/// full pipe buffer while we are only watching `try_wait`.
fn run_with_timeout(argv: &[String], timeout_secs: u64) -> Result<String, CliError> {
    let command_str = argv.join(" ");
    let Some((program, rest)) = argv.split_first() else {
        return Err(CliError::NotFound {
            message: "empty argv".to_string(),
        });
    };

    let mut child = match Command::new(program)
        .args(rest)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            return Err(CliError::NotFound {
                message: e.to_string(),
            })
        }
    };

    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();
    let stdout_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(p) = stdout_pipe.as_mut() {
            let _ = p.read_to_end(&mut buf);
        }
        buf
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(p) = stderr_pipe.as_mut() {
            let _ = p.read_to_end(&mut buf);
        }
        buf
    });

    let timeout = Duration::from_secs(timeout_secs.max(1));
    let start = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {
                if start.elapsed() >= timeout {
                    break None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break None,
        }
    };

    match status {
        Some(status) => {
            let stdout_bytes = stdout_reader.join().unwrap_or_default();
            let stderr_bytes = stderr_reader.join().unwrap_or_default();
            let stdout = String::from_utf8_lossy(&stdout_bytes).trim().to_string();
            let stderr = String::from_utf8_lossy(&stderr_bytes).trim().to_string();
            if status.success() {
                Ok(stdout)
            } else {
                let message = if !stderr.is_empty() {
                    stderr
                } else if !stdout.is_empty() {
                    stdout
                } else {
                    format!("boltzcli exited with code {:?}", status.code())
                };
                Err(CliError::ExitFailure {
                    code: status.code(),
                    message,
                })
            }
        }
        None => {
            // Timed out: kill so a hung boltzcli does not linger holding
            // boltzd's lock, matching py subprocess.run(timeout=...)'s
            // kill-on-TimeoutExpired behaviour.
            let _ = child.kill();
            let _ = child.wait();
            Err(CliError::Timeout {
                timeout_secs,
                command: command_str,
            })
        }
    }
}

impl BoltzCli for ProcessBoltzCli {
    fn run(&self, args: &[&str], timeout_secs: u64) -> Result<String, CliError> {
        if !self.config.enabled {
            return Err(CliError::Disabled);
        }
        let str_args: Vec<&str> = args.to_vec();
        let full_argv = base_argv(&self.config, &str_args);
        let effective_timeout = if timeout_secs > 0 {
            timeout_secs
        } else {
            self.config.timeout_seconds
        };
        run_with_timeout(&full_argv, effective_timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BoltzCliProcessConfig {
        BoltzCliProcessConfig {
            enabled: true,
            cli_path: "/usr/local/bin/boltzcli".to_string(),
            datadir: "/var/lib/boltz".to_string(),
            use_sudo: false,
            sudo_user: "boltz".to_string(),
            timeout_seconds: 60,
        }
    }

    // --- base_argv: the only part of this module a test may exercise;
    // NOTHING below spawns a process. ---

    #[test]
    fn base_argv_without_sudo_matches_python_shape() {
        let argv = base_argv(&cfg(), &["listswaps", "--json"]);
        assert_eq!(
            argv,
            vec![
                "/usr/local/bin/boltzcli",
                "--datadir",
                "/var/lib/boltz",
                "listswaps",
                "--json"
            ]
        );
    }

    #[test]
    fn base_argv_with_sudo_prefixes_sudo_dash_n_dash_u() {
        let mut c = cfg();
        c.use_sudo = true;
        c.sudo_user = "boltzsvc".to_string();
        let argv = base_argv(&c, &["getinfo"]);
        assert_eq!(
            argv,
            vec![
                "sudo",
                "-n",
                "-u",
                "boltzsvc",
                "/usr/local/bin/boltzcli",
                "--datadir",
                "/var/lib/boltz",
                "getinfo"
            ]
        );
    }

    #[test]
    fn base_argv_without_sudo_never_includes_sudo_token() {
        // Control: with use_sudo=false (the default), "sudo" must not
        // appear anywhere in the argv, even if sudo_user is set to
        // something non-default.
        let mut c = cfg();
        c.sudo_user = "whatever".to_string();
        c.use_sudo = false;
        let argv = base_argv(&c, &["getinfo"]);
        assert!(!argv.iter().any(|a| a == "sudo"));
    }

    #[test]
    fn base_argv_honours_configured_cli_path_and_datadir() {
        let mut c = cfg();
        c.cli_path = "/opt/custom/boltzcli".to_string();
        c.datadir = "/mnt/boltz-data".to_string();
        let argv = base_argv(&c, &[]);
        assert_eq!(
            argv,
            vec!["/opt/custom/boltzcli", "--datadir", "/mnt/boltz-data"]
        );
    }

    #[test]
    fn default_config_is_disabled() {
        // Safety: matches py's cfg.enabled default (False) — a
        // ProcessBoltzCli built from Default must refuse to run.
        assert!(!BoltzCliProcessConfig::default().enabled);
    }

    #[test]
    fn default_config_matches_python_defaults() {
        let c = BoltzCliProcessConfig::default();
        assert_eq!(c.cli_path, "/usr/local/bin/boltzcli");
        assert_eq!(c.datadir, "/var/lib/boltz");
        assert!(!c.use_sudo);
        assert_eq!(c.sudo_user, "boltz");
        assert_eq!(c.timeout_seconds, 60);
    }

    // --- run() disabled-gate: exercises the CliError::Disabled early
    // return WITHOUT spawning anything (no Command is ever constructed on
    // this path). ---

    #[test]
    fn disabled_config_run_returns_disabled_without_spawning() {
        let mut c = cfg();
        c.enabled = false;
        let adapter = ProcessBoltzCli::new(c);
        let err = adapter.run(&["listswaps"], 10).unwrap_err();
        assert_eq!(err, CliError::Disabled);
    }
}
