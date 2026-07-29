//! The real, subprocess-backed [`BoltzCli`] implementations.
//!
//! Ports py `_base_cmd`/`_run`/`_ensure_enabled` (boltz_manager.py:433-467)
//! and then HARDENS the boundary beyond Python (Task 63 7B):
//!
//! - **Two transports.** [`ProcessBoltzCli`] is QUERY-ONLY: a read-only
//!   subcommand allowlist is enforced BEFORE any spawn, so fund-moving
//!   verbs (createswap/createreverseswap/createchainswap/refundswap/
//!   claimswaps/`wallet send`) and the mnemonic (`swapmnemonic`,
//!   `backup`) are structurally unreachable through it -- no matter what
//!   argv a caller hands the trait object. [`ArmedBoltzCli`] carries the
//!   full argv and exists ONLY inside the Task-69-minted action
//!   capability (source-scan pinned downstream).
//! - **Bounded capture.** Each stream is stored up to
//!   [`MAX_CAPTURE_BYTES`]; beyond that the reader keeps DRAINING (so the
//!   child never deadlocks on a full pipe) but stops storing, and the
//!   result carries an explicit `[truncated N bytes]` marker. A chatty or
//!   hostile child cannot OOM the plugin.
//! - **Process-TREE kill.** The child starts in its own process group
//!   (`process_group(0)`); on timeout the whole group gets SIGKILL, so a
//!   `sudo`-wrapped boltzcli grandchild dies with its parent instead of
//!   lingering with boltzd's lock.
//! - **Redaction.** A timeout error names the SUBCOMMAND and the argv
//!   COUNT, never the values (amounts, wallets, destinations). Failure
//!   messages drop any line naming the mnemonic and are capped at 300
//!   chars (char-safe).
//! - **stdin is null**, and the config is immutable after construction
//!   (`enabled` cannot be flipped on a cloned adapter).

use crate::cli::BoltzCli;
use crate::error::CliError;
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Per-stream capture bound. boltzcli's real replies are a few KiB; the
/// biggest legitimate one (`listswaps --json` with 200 journal entries)
/// stays far under this.
pub const MAX_CAPTURE_BYTES: usize = 256 * 1024;

/// Failure-message cap, in CHARS (py truncated nothing; 300 matches the
/// `_run_json` snippet bound).
const MESSAGE_CAP_CHARS: usize = 300;

/// Ports py `BoltzCliConfig` (boltz_manager.py:236-242) — field-for-field,
/// including its defaults. Every field is PRIVATE: a constructed config is
/// immutable, so a cloned adapter can never be re-armed by field mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoltzCliProcessConfig {
    enabled: bool,
    cli_path: String,
    datadir: String,
    use_sudo: bool,
    sudo_user: String,
    timeout_seconds: u64,
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

impl BoltzCliProcessConfig {
    pub fn new(
        enabled: bool,
        cli_path: String,
        datadir: String,
        use_sudo: bool,
        sudo_user: String,
        timeout_seconds: u64,
    ) -> Self {
        Self {
            enabled,
            cli_path,
            datadir,
            use_sudo,
            sudo_user,
            timeout_seconds,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds
    }

    pub fn cli_path(&self) -> &str {
        &self.cli_path
    }

    pub fn datadir(&self) -> &str {
        &self.datadir
    }

    pub fn use_sudo(&self) -> bool {
        self.use_sudo
    }

    pub fn sudo_user(&self) -> &str {
        &self.sudo_user
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

/// The read-only subcommand allowlist the QUERY transport enforces.
/// Widening this list is a deliberate decision (test-pinned), never a
/// side effect. `wallet` is allowed only for `list`/`receive` --
/// `wallet send` moves funds.
fn query_allowed(args: &[&str]) -> bool {
    match args.first() {
        Some(&"quote") | Some(&"listswaps") | Some(&"swapinfo") | Some(&"stats") => true,
        Some(&"wallet") => matches!(args.get(1), Some(&"list") | Some(&"receive")),
        _ => false,
    }
}

/// The redacted command label errors carry: the subcommand (plus the
/// `wallet` sub-verb) and the argument COUNT -- never values.
fn redacted_label(args: &[&str]) -> String {
    match args.first() {
        None => "(empty argv)".to_string(),
        Some(&"wallet") => match args.get(1) {
            Some(sub) => format!(
                "wallet {sub} ({} args redacted)",
                args.len().saturating_sub(2)
            ),
            None => "wallet (0 args redacted)".to_string(),
        },
        Some(first) => format!("{first} ({} args redacted)", args.len() - 1),
    }
}

/// Drop any line naming the mnemonic, then cap at [`MESSAGE_CAP_CHARS`]
/// chars (char-safe).
fn sanitize_message(raw: &str) -> String {
    let scrubbed: Vec<&str> = raw
        .lines()
        .map(|line| {
            if line.to_ascii_lowercase().contains("mnemonic") {
                "[line redacted]"
            } else {
                line
            }
        })
        .collect();
    scrubbed
        .join("\n")
        .chars()
        .take(MESSAGE_CAP_CHARS)
        .collect()
}

/// Real QUERY-ONLY [`BoltzCli`] adapter. Construct with
/// [`BoltzCliProcessConfig`] — `enabled` defaults to `false` (py
/// `_ensure_enabled`, boltz_manager.py:433-435). Fund-moving verbs refuse
/// at the transport (see [`ArmedBoltzCli`]).
#[derive(Debug, Clone)]
pub struct ProcessBoltzCli {
    config: BoltzCliProcessConfig,
}

impl ProcessBoltzCli {
    pub fn new(config: BoltzCliProcessConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &BoltzCliProcessConfig {
        &self.config
    }
}

/// The full-argv transport for fund-moving verbs. NO allowlist: the
/// discipline is that this type exists only inside the Task-69 action
/// capability -- production surfaces never name it (source-scan pinned in
/// the plugin crate).
#[derive(Debug)]
pub struct ArmedBoltzCli {
    config: BoltzCliProcessConfig,
}

impl ArmedBoltzCli {
    pub fn new(config: BoltzCliProcessConfig) -> Self {
        Self { config }
    }
}

/// Capped-capture pipe reader: stores up to [`MAX_CAPTURE_BYTES`], then
/// keeps draining to EOF so the child never blocks on a full pipe.
fn read_capped(pipe: &mut Option<impl Read>) -> (Vec<u8>, usize) {
    let mut stored = Vec::new();
    let mut dropped = 0usize;
    if let Some(p) = pipe.as_mut() {
        let mut chunk = [0u8; 8192];
        loop {
            match p.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let room = MAX_CAPTURE_BYTES.saturating_sub(stored.len());
                    let keep = n.min(room);
                    stored.extend_from_slice(&chunk[..keep]);
                    dropped += n - keep;
                }
            }
        }
    }
    (stored, dropped)
}

fn captured_string(bytes: Vec<u8>, dropped: usize) -> String {
    let mut s = String::from_utf8_lossy(&bytes).trim().to_string();
    if dropped > 0 {
        s.push_str(&format!(" [truncated {dropped} bytes]"));
    }
    s
}

/// Poll-and-kill-on-timeout process execution. `argv[0]` is the program;
/// the rest are its arguments. The child runs in its OWN process group so
/// a timeout kill takes the whole tree (sudo grandchildren included).
fn run_with_timeout(
    argv: &[String],
    timeout_secs: u64,
    redacted: &str,
) -> Result<String, CliError> {
    let Some((program, rest)) = argv.split_first() else {
        return Err(CliError::NotFound {
            message: "empty argv".to_string(),
        });
    };

    let mut command = Command::new(program);
    command
        .args(rest)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = match command.spawn() {
        Ok(c) => c,
        Err(e) => {
            return Err(CliError::NotFound {
                message: format!("{program}: {e}"),
            })
        }
    };

    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();
    let stdout_reader = std::thread::spawn(move || read_capped(&mut stdout_pipe));
    let stderr_reader = std::thread::spawn(move || read_capped(&mut stderr_pipe));

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
            let (stdout_bytes, stdout_dropped) = stdout_reader.join().unwrap_or((Vec::new(), 0));
            let (stderr_bytes, stderr_dropped) = stderr_reader.join().unwrap_or((Vec::new(), 0));
            let stdout = captured_string(stdout_bytes, stdout_dropped);
            let stderr = captured_string(stderr_bytes, stderr_dropped);
            if status.success() {
                Ok(stdout)
            } else {
                let message = if !stderr.is_empty() {
                    sanitize_message(&stderr)
                } else if !stdout.is_empty() {
                    sanitize_message(&stdout)
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
            // Timed out: kill the WHOLE process group so a sudo-wrapped
            // boltzcli grandchild dies too (py subprocess.run only killed
            // the direct child).
            #[cfg(unix)]
            {
                let pgid = child.id() as i32;
                unsafe {
                    libc::kill(-pgid, libc::SIGKILL);
                }
            }
            let _ = child.kill();
            let _ = child.wait();
            Err(CliError::Timeout {
                timeout_secs,
                command: redacted.to_string(),
            })
        }
    }
}

fn run_transport(
    config: &BoltzCliProcessConfig,
    args: &[&str],
    timeout_secs: u64,
) -> Result<String, CliError> {
    if !config.enabled {
        return Err(CliError::Disabled);
    }
    let full_argv = base_argv(config, args);
    let effective_timeout = if timeout_secs > 0 {
        timeout_secs
    } else {
        config.timeout_seconds
    };
    run_with_timeout(&full_argv, effective_timeout, &redacted_label(args))
}

impl BoltzCli for ProcessBoltzCli {
    fn run(&self, args: &[&str], timeout_secs: u64) -> Result<String, CliError> {
        if !query_allowed(args) {
            return Err(CliError::TransportRefused {
                subcommand: redacted_label(args),
            });
        }
        run_transport(&self.config, args, timeout_secs)
    }
}

impl BoltzCli for ArmedBoltzCli {
    fn run(&self, args: &[&str], timeout_secs: u64) -> Result<String, CliError> {
        run_transport(&self.config, args, timeout_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BoltzCliProcessConfig {
        BoltzCliProcessConfig::new(
            true,
            "/usr/local/bin/boltzcli".to_string(),
            "/var/lib/boltz".to_string(),
            false,
            "boltz".to_string(),
            60,
        )
    }

    // --- Pure base_argv unit coverage. The real process boundary is covered
    // separately by sandbox-only integration tests with fake executables. ---

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
        let c = BoltzCliProcessConfig::new(
            true,
            "/usr/local/bin/boltzcli".to_string(),
            "/var/lib/boltz".to_string(),
            true,
            "boltzsvc".to_string(),
            60,
        );
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
        let c = BoltzCliProcessConfig::new(
            true,
            "/usr/local/bin/boltzcli".to_string(),
            "/var/lib/boltz".to_string(),
            false,
            "whatever".to_string(),
            60,
        );
        let argv = base_argv(&c, &["getinfo"]);
        assert!(!argv.iter().any(|a| a == "sudo"));
    }

    #[test]
    fn base_argv_honours_configured_cli_path_and_datadir() {
        let c = BoltzCliProcessConfig::new(
            true,
            "/opt/custom/boltzcli".to_string(),
            "/mnt/boltz-data".to_string(),
            false,
            "boltz".to_string(),
            60,
        );
        let argv = base_argv(&c, &[]);
        assert_eq!(
            argv,
            vec!["/opt/custom/boltzcli", "--datadir", "/mnt/boltz-data"]
        );
    }

    #[test]
    fn default_config_is_disabled() {
        assert!(!BoltzCliProcessConfig::default().enabled());
    }

    #[test]
    fn default_config_matches_python_defaults() {
        let c = BoltzCliProcessConfig::default();
        assert_eq!(c.cli_path(), "/usr/local/bin/boltzcli");
        assert_eq!(c.datadir(), "/var/lib/boltz");
        assert!(!c.use_sudo());
        assert_eq!(c.sudo_user(), "boltz");
        assert_eq!(c.timeout_seconds(), 60);
    }

    #[test]
    fn disabled_config_run_returns_disabled_without_spawning() {
        let c = BoltzCliProcessConfig::new(
            false,
            "/usr/local/bin/boltzcli".to_string(),
            "/var/lib/boltz".to_string(),
            false,
            "boltz".to_string(),
            60,
        );
        let adapter = ProcessBoltzCli::new(c);
        let err = adapter.run(&["listswaps"], 10).unwrap_err();
        assert_eq!(err, CliError::Disabled);
    }

    #[test]
    fn query_allowlist_is_exact() {
        for allowed in [
            vec!["quote", "--json"],
            vec!["listswaps", "--json"],
            vec!["swapinfo", "--", "id"],
            vec!["stats"],
            vec!["wallet", "list", "--json"],
            vec!["wallet", "receive", "w"],
        ] {
            assert!(query_allowed(&allowed), "{allowed:?}");
        }
        for refused in [
            vec!["createswap"],
            vec!["createreverseswap"],
            vec!["createchainswap"],
            vec!["refundswap"],
            vec!["claimswaps"],
            vec!["wallet", "send", "w"],
            vec!["wallet"],
            vec!["swapmnemonic", "get"],
            vec!["backup"],
            vec![],
        ] {
            assert!(!query_allowed(&refused), "{refused:?}");
        }
    }

    #[test]
    fn redacted_label_names_verbs_never_values() {
        assert_eq!(
            redacted_label(&[
                "createswap",
                "--json",
                "--from-wallet",
                "w",
                "BTC",
                "100000"
            ]),
            "createswap (5 args redacted)"
        );
        assert_eq!(
            redacted_label(&["wallet", "send", "w", "dest", "1000"]),
            "wallet send (3 args redacted)"
        );
    }

    #[test]
    fn sanitize_message_drops_mnemonic_lines_and_caps_chars() {
        let raw = "Swap Mnemonic: abandon ability able\nplain detail";
        let cleaned = sanitize_message(raw);
        assert!(!cleaned.contains("abandon"));
        assert!(cleaned.contains("[line redacted]"));
        assert!(cleaned.contains("plain detail"));

        let long: String = "€".repeat(400);
        assert_eq!(sanitize_message(&long).chars().count(), 300);
    }
}
