//! Injected `boltzcli` subprocess boundary.
//!
//! Ports py `BoltzCliManager._run`/`_run_json` (boltz_manager.py:437-474).
//! The real implementation (not provided by this crate — it belongs in the
//! plugin binary, see `ENTRYPOINTS.md`) shells out to `boltzcli`; every
//! test in this crate uses [`FakeBoltzCli`] instead. Nothing in this crate
//! may construct a `std::process::Command` — that is enforced by review,
//! not by the compiler, so keep it that way.

use crate::error::CliError;

/// Abstraction over "run this boltzcli command and get stdout back",
/// mirroring py `_run` (boltz_manager.py:444-467): a Vec of argv-style
/// arguments (already including any `--` terminator the caller wants) in,
/// trimmed stdout out, or a [`CliError`] distinguishing not-found /
/// timeout / nonzero-exit (py collapses all three into `BoltzCliError`;
/// this trait's `Result<String, CliError>` keeps them distinct so callers
/// can special-case [`CliError::Timeout`] per the safety context).
pub trait BoltzCli {
    /// Run `boltzcli <args>` (the caller supplies the full argv tail —
    /// `--datadir` / `sudo -n -u` prefixing per py `_base_cmd`,
    /// boltz_manager.py:437-442, is the live adapter's job, not this
    /// trait's) and return trimmed stdout on exit 0.
    fn run(&self, args: &[&str], timeout_secs: u64) -> Result<String, CliError>;
}

/// Ports py `_run_json` (boltz_manager.py:469-474): run, then parse stdout
/// as JSON, mapping a parse failure to [`CliError::InvalidJson`].
pub fn run_json(
    cli: &dyn BoltzCli,
    args: &[&str],
    timeout_secs: u64,
) -> Result<serde_json::Value, CliError> {
    let out = cli.run(args, timeout_secs)?;
    serde_json::from_str(&out).map_err(|e| CliError::InvalidJson {
        message: format!("{e}: {}", &out[..out.len().min(300)]),
    })
}

/// Scripted, in-memory `BoltzCli` for tests. Never touches a real process,
/// wallet, or daemon — this is the ONLY `BoltzCli` implementation any test
/// in this crate (or a downstream caller's tests) should use.
#[derive(Debug, Default, Clone)]
pub struct FakeBoltzCli {
    /// Queue of scripted responses, consumed in call order (FIFO). Each
    /// call to `run` pops the front entry; calling `run` with an empty
    /// queue panics (a test that runs out of script is a test that didn't
    /// predict its own call count — fail loudly, not silently).
    pub responses: std::cell::RefCell<std::collections::VecDeque<Result<String, CliError>>>,
    /// Every `(args, timeout_secs)` pair passed to `run`, in call order —
    /// lets a test assert on exact argv construction (e.g. the `--`
    /// terminator, `--chan-id` pinning).
    pub calls: std::cell::RefCell<Vec<(Vec<String>, u64)>>,
}

impl FakeBoltzCli {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a successful response.
    pub fn push_ok(&self, stdout: impl Into<String>) {
        self.responses.borrow_mut().push_back(Ok(stdout.into()));
    }

    /// Queue a failing response.
    pub fn push_err(&self, err: CliError) {
        self.responses.borrow_mut().push_back(Err(err));
    }

    pub fn call_count(&self) -> usize {
        self.calls.borrow().len()
    }
}

impl BoltzCli for FakeBoltzCli {
    fn run(&self, args: &[&str], timeout_secs: u64) -> Result<String, CliError> {
        self.calls
            .borrow_mut()
            .push((args.iter().map(|s| s.to_string()).collect(), timeout_secs));
        self.responses
            .borrow_mut()
            .pop_front()
            .expect("FakeBoltzCli: no scripted response left for this call")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fake_cli_replays_scripted_responses_in_order() {
        let cli = FakeBoltzCli::new();
        cli.push_ok("first");
        cli.push_ok("second");
        assert_eq!(cli.run(&["a"], 5).unwrap(), "first");
        assert_eq!(cli.run(&["b"], 5).unwrap(), "second");
        assert_eq!(cli.call_count(), 2);
    }

    #[test]
    fn fake_cli_records_exact_argv_and_timeout() {
        let cli = FakeBoltzCli::new();
        cli.push_ok("{}");
        cli.run(&["createreverseswap", "--", "amount"], 42).unwrap();
        let calls = cli.calls.borrow();
        assert_eq!(
            calls[0].0,
            vec![
                "createreverseswap".to_string(),
                "--".to_string(),
                "amount".to_string()
            ]
        );
        assert_eq!(calls[0].1, 42);
    }

    #[test]
    #[should_panic(expected = "no scripted response left")]
    fn fake_cli_panics_on_exhausted_script() {
        let cli = FakeBoltzCli::new();
        let _ = cli.run(&["x"], 1);
    }

    #[test]
    fn run_json_parses_valid_json() {
        let cli = FakeBoltzCli::new();
        cli.push_ok(json!({"id": "abc"}).to_string());
        let v = run_json(&cli, &["swapinfo", "--", "abc"], 30).unwrap();
        assert_eq!(v["id"], "abc");
    }

    #[test]
    fn run_json_maps_invalid_json_to_invalid_json_error() {
        let cli = FakeBoltzCli::new();
        cli.push_ok("not json at all");
        let err = run_json(&cli, &["listswaps", "--json"], 30).unwrap_err();
        assert!(matches!(err, CliError::InvalidJson { .. }));
    }

    #[test]
    fn run_json_propagates_underlying_cli_error() {
        let cli = FakeBoltzCli::new();
        cli.push_err(CliError::Timeout {
            timeout_secs: 60,
            command: "boltzcli createswap".to_string(),
        });
        let err = run_json(&cli, &["createswap"], 60).unwrap_err();
        assert!(matches!(err, CliError::Timeout { .. }));
    }
}
