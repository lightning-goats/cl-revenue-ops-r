//! Error and outcome types for the Boltz subsystem port.
//!
//! Boltz performs ON-CHAIN swaps via a `boltzcli` subprocess against real
//! funds. Two sites in the Python source (py `boltz_manager.py:444` — the
//! `_run` subprocess call — and py `boltz_manager.py:2461`/`2474` — the
//! manual `refund`/`claim` paths) can end in a state where the *local*
//! call fails or returns unstructured text while the *real-world* outcome
//! (did boltzd actually create/refund/claim the swap?) is genuinely
//! unknown. Python swallows this ambiguity: `_run` on `TimeoutExpired`
//! raises `BoltzCliError` with no distinction from "definitely did not
//! happen", and `refund`/`claim` just return `{"result_raw": <stdout>}`
//! with no structured status field at all — a caller has to know to
//! re-query `swapinfo` to find out what really happened.
//!
//! This port makes that ambiguity a first-class, unignorable type instead
//! of an error string: [`CliError::Timeout`] and
//! [`ManualActionOutcome::Unverified`] force a caller to explicitly decide
//! what to do about an unknown outcome rather than let it fall through as
//! a generic failure (which the Python's exception-based control flow
//! effectively does).

use std::fmt;

/// Failure modes of running the `boltzcli` subprocess.
///
/// Ports the exception surface of py `BoltzCliManager._run` (boltz_manager.py:444-467):
/// `FileNotFoundError` -> [`CliError::NotFound`], `subprocess.TimeoutExpired`
/// -> [`CliError::Timeout`] (an explicit, distinct variant — Python's
/// `except subprocess.TimeoutExpired` also just re-raises `BoltzCliError`
/// with a formatted message, collapsing "timed out" and "process failed"
/// into the same exception type; we do not repeat that collapse), and a
/// nonzero exit code -> [`CliError::ExitFailure`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliError {
    /// py boltz_manager.py:457-458 (`FileNotFoundError`).
    NotFound { message: String },
    /// py boltz_manager.py:459-460 (`subprocess.TimeoutExpired`).
    ///
    /// SAFETY-CRITICAL: on a `create`-type command (loop_in/loop_out/
    /// chainswap), a timeout here does NOT mean the swap was not created —
    /// boltzd may have received and processed the request before the local
    /// CLI call's read timed out. Callers MUST treat this as
    /// [`CreateOutcome::Unknown`], never as "the swap did not happen".
    Timeout { timeout_secs: u64, command: String },
    /// py boltz_manager.py:464-466 (nonzero `proc.returncode`).
    ExitFailure { code: Option<i32>, message: String },
    /// py boltz_manager.py:473-474 (`_run_json`'s `json.JSONDecodeError`).
    InvalidJson { message: String },
    /// py boltz_manager.py:433-435 (`_ensure_enabled`).
    Disabled,
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::NotFound { message } => write!(f, "boltzcli executable not found: {message}"),
            CliError::Timeout {
                timeout_secs,
                command,
            } => {
                write!(f, "boltzcli timed out after {timeout_secs}s: {command}")
            }
            CliError::ExitFailure { code, message } => {
                write!(f, "boltzcli exited with code {code:?}: {message}")
            }
            CliError::InvalidJson { message } => write!(f, "invalid JSON from boltzcli: {message}"),
            CliError::Disabled => write!(
                f,
                "Boltz CLI integration disabled (set revenue-ops-boltz-enabled=true)"
            ),
        }
    }
}

impl std::error::Error for CliError {}

/// Outcome of a swap-*creating* CLI call (loop_in / loop_out / chainswap),
/// distinguishing "definitely did not happen" from "unknown — must be
/// reconciled via `swapinfo`". Python (py boltz_manager.py:444-467)
/// collapses both into the same raised `BoltzCliError`; this type is the
/// deliberate un-collapse required by the CRITICAL SAFETY CONTEXT
/// (subprocess timeout handling on create is a known unknown-outcome
/// site).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOutcome<T> {
    /// The CLI call completed and returned a definite result.
    Completed(T),
    /// The CLI call failed in a way that means the command was rejected
    /// before doing anything irreversible (e.g. disabled, bad JSON,
    /// nonzero exit with an error the CLI itself reported synchronously).
    Rejected(CliError),
    /// The call's *local* outcome is unknown (timeout). The swap MAY have
    /// been created on the Boltz side. A caller must reconcile via
    /// `swapinfo`/`listswaps` before assuming either outcome, and must
    /// NOT release any budget reservation or free a cooldown slot as if
    /// the swap definitely did not happen.
    Unknown { timeout_secs: u64, command: String },
}

/// Outcome of a manual `refund`/`claim` call (py boltz_manager.py:2461-2488).
///
/// Python returns `{"result_raw": <raw stdout>}` on any exit-0 call with
/// no structured status — a human has to read the text or re-run
/// `swap_status` to know whether the refund/claim actually succeeded.
/// This type keeps that same reality (the CLI genuinely does not tell us
/// more) but makes it explicit in the type system: [`ManualActionOutcome::Unverified`]
/// cannot be silently treated as success by a caller that only pattern-matches
/// `Ok`/`Err`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualActionOutcome {
    /// The subprocess exited 0; boltzcli's raw stdout is `raw_output`, but
    /// no structured field confirms the refund/claim actually completed.
    /// The caller MUST treat this as "requested, unconfirmed" and follow up
    /// with `swap_status` before recording it as spend or closing out a
    /// journal entry.
    Unverified { raw_output: String },
    /// The subprocess call itself failed (nonzero exit, timeout, etc.) —
    /// still not proof the on-chain action didn't happen (a refund
    /// broadcast can succeed on-chain while the CLI call times out), but at
    /// least the CLI did not claim success.
    Failed(CliError),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BoltzError {
    #[error("{0}")]
    Cli(#[from] CliError),
    #[error("{0}")]
    Rejected(String),
    #[error("invalid on-chain destination address: {0}")]
    InvalidAddress(String),
}
