//! Task 63 slice 3: the Boltz execution boundaries.
//!
//! The four-way outcome vocabulary the whole Boltz rail speaks, the
//! fail-closed classifiers over the kernel's [`CreateOutcome`] /
//! [`ManualActionOutcome`], the settlement mapping, the non-forgeable
//! action capability, and the mnemonic secret type. This module is
//! deliberately EXECUTION-FREE: it can hold and express, never invoke
//! (source-scan pinned), so "retry on unknown" is not writable here.
//!
//! Capability discipline: [`BoltzActionCapability`] has private fields
//! and no Clone -- it is assembled exactly once by Task 69's
//! whole-plugin authority bracket; ZERO production construction sites
//! exist until then (source-scan pinned in `tests/boltz_boundaries.rs`).
//! Structurally, even a forged `revops_boltz` armed mode cannot spend
//! without it: the query transport's allowlist refuses fund-moving
//! verbs, and the armed transport lives only inside this capability.

use revops_boltz::error::{CliError, CreateOutcome, ManualActionOutcome};
use revops_boltz::process::ArmedBoltzCli;
use revops_boltz::state::{is_error_swap, swap_entry_error_text};
use revops_db::fee_runway::BoltzSettle;
use serde_json::Value;

/// One Boltz submission's classified terminal, in the Task 63 contract's
/// vocabulary. `OutcomeUnknownAfterSubmit` is the fail-closed default
/// everywhere ambiguity exists: boltzd MAY have created the swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoltzSubmitOutcome {
    /// Provably nothing reached boltzd (disabled, transport refusal,
    /// missing executable).
    NotSubmitted {
        detail: String,
    },
    /// A definite refusal with proof (nonzero exit, or an error swap in
    /// the reply).
    RejectedWithProof {
        detail: String,
    },
    Committed {
        swap_id: Option<String>,
    },
    /// The reply was lost or ambiguous: quarantine, never retry.
    OutcomeUnknownAfterSubmit {
        detail: String,
    },
}

fn classify_cli_error(error: &CliError) -> BoltzSubmitOutcome {
    match error {
        CliError::Disabled | CliError::NotFound { .. } | CliError::TransportRefused { .. } => {
            BoltzSubmitOutcome::NotSubmitted {
                detail: error.to_string(),
            }
        }
        CliError::ExitFailure { .. } => BoltzSubmitOutcome::RejectedWithProof {
            detail: error.to_string(),
        },
        // The command ran but the outcome is unreadable or the reply
        // never arrived: the swap may exist.
        CliError::Timeout { .. } | CliError::InvalidJson { .. } => {
            BoltzSubmitOutcome::OutcomeUnknownAfterSubmit {
                detail: error.to_string(),
            }
        }
    }
}

/// Classify one create-shaped result (loop-in/loop-out/chainswap).
pub fn classify_boltz_create(outcome: &CreateOutcome<Value>) -> BoltzSubmitOutcome {
    match outcome {
        CreateOutcome::Completed(reply) => {
            let map = reply.as_object().cloned().unwrap_or_default();
            if is_error_swap(&map) {
                return BoltzSubmitOutcome::RejectedWithProof {
                    detail: swap_entry_error_text(&map),
                };
            }
            match reply.get("id").and_then(Value::as_str) {
                Some(id) if !id.trim().is_empty() => BoltzSubmitOutcome::Committed {
                    swap_id: Some(id.to_string()),
                },
                _ => BoltzSubmitOutcome::OutcomeUnknownAfterSubmit {
                    detail: "create reply carried no swap id; the swap may exist".to_string(),
                },
            }
        }
        CreateOutcome::Rejected(error) => classify_cli_error(error),
        CreateOutcome::Unknown {
            timeout_secs,
            command,
        } => BoltzSubmitOutcome::OutcomeUnknownAfterSubmit {
            detail: format!("{command} produced no reply within {timeout_secs}s"),
        },
    }
}

/// Classify one manual action (refund/claim): exit-0 is UNVERIFIED --
/// it quarantines until a terminal swap status is observed.
pub fn classify_boltz_manual(outcome: &ManualActionOutcome) -> BoltzSubmitOutcome {
    match outcome {
        ManualActionOutcome::Unverified { raw_output: _ } => {
            BoltzSubmitOutcome::OutcomeUnknownAfterSubmit {
                detail: "manual action exited 0 but exit status is not proof; awaiting a \
                         terminal swap status"
                    .to_string(),
            }
        }
        ManualActionOutcome::Failed(error) => classify_cli_error(error),
    }
}

/// Map one classified outcome onto its durable settlement: committed
/// SETTLES (fee held becomes fee spent), rejection and not-submitted
/// RELEASE, unknown QUARANTINES (the fee may be committed).
pub fn settlement_for_boltz(
    outcome: &BoltzSubmitOutcome,
    request_id: &str,
    reserved_fee_sats: i64,
    resolved_at: i64,
) -> BoltzSettle {
    match outcome {
        BoltzSubmitOutcome::Committed { swap_id } => BoltzSettle {
            request_id: request_id.to_string(),
            outcome: "committed".to_string(),
            outcome_detail: None,
            swap_id: swap_id.clone(),
            reservation_status: "settled".to_string(),
            settled_sats: Some(reserved_fee_sats),
            resolved_at,
        },
        BoltzSubmitOutcome::RejectedWithProof { detail } => BoltzSettle {
            request_id: request_id.to_string(),
            outcome: "rejected".to_string(),
            outcome_detail: Some(detail.clone()),
            swap_id: None,
            reservation_status: "released".to_string(),
            settled_sats: None,
            resolved_at,
        },
        BoltzSubmitOutcome::NotSubmitted { detail } => BoltzSettle {
            request_id: request_id.to_string(),
            outcome: "not_submitted".to_string(),
            outcome_detail: Some(detail.clone()),
            swap_id: None,
            reservation_status: "released".to_string(),
            settled_sats: None,
            resolved_at,
        },
        BoltzSubmitOutcome::OutcomeUnknownAfterSubmit { detail } => BoltzSettle {
            request_id: request_id.to_string(),
            outcome: "outcome_unknown".to_string(),
            outcome_detail: Some(detail.clone()),
            swap_id: None,
            reservation_status: "quarantined".to_string(),
            settled_sats: None,
            resolved_at,
        },
    }
}

/// The Boltz spend capability: everything that can move funds through
/// boltzcli. Private fields, no Clone, assembled ONLY by Task 69's
/// whole-plugin authority bracket -- production surfaces never name it
/// until then (source-scan pinned). The withdraw hard cap lives INSIDE
/// the capability so no caller can pass `i64::MAX` to disable it.
pub struct BoltzActionCapability {
    transport: ActionTransport,
    max_withdraw_sats: i64,
}

enum ActionTransport {
    Process(ArmedBoltzCli),
    /// Test seam: scripted fakes stand in for the process transport.
    Injected(std::sync::Arc<dyn revops_boltz::cli::BoltzCli + Send + Sync>),
}

impl BoltzActionCapability {
    /// Assemble the process-backed capability. Task 69's authority
    /// bracket is the only sanctioned production caller (source-scan
    /// pinned); the e2e proof mints one against fake executables.
    pub fn assemble(armed: ArmedBoltzCli, max_withdraw_sats: i64) -> Self {
        Self {
            transport: ActionTransport::Process(armed),
            max_withdraw_sats,
        }
    }

    /// Assemble around an injected transport (scripted fakes in tests).
    /// Same scan discipline: zero production callers.
    pub fn assemble_injected(
        transport: std::sync::Arc<dyn revops_boltz::cli::BoltzCli + Send + Sync>,
        max_withdraw_sats: i64,
    ) -> Self {
        Self {
            transport: ActionTransport::Injected(transport),
            max_withdraw_sats,
        }
    }

    pub fn armed(&self) -> &dyn revops_boltz::cli::BoltzCli {
        match &self.transport {
            ActionTransport::Process(armed) => armed,
            ActionTransport::Injected(injected) => injected.as_ref(),
        }
    }

    pub fn max_withdraw_sats(&self) -> i64 {
        self.max_withdraw_sats
    }
}

/// The backup mnemonic, opaque by construction. No `Debug`, `Display`,
/// `Clone`, or `Serialize` exists; the ONLY egress is
/// [`MnemonicSecret::into_rpc_value`], which consumes the secret -- so a
/// mnemonic can reach exactly one direct RPC reply and nothing else
/// (never logs, errors, durable payloads, health, or snapshots).
///
/// The opacity is compile-pinned:
///
/// ```compile_fail
/// let secret = revops::boltz_boundaries::MnemonicSecret::new("words".into());
/// let _ = format!("{:?}", secret); // MnemonicSecret is deliberately not Debug
/// ```
///
/// ```compile_fail
/// let secret = revops::boltz_boundaries::MnemonicSecret::new("words".into());
/// let _ = secret.clone(); // MnemonicSecret is deliberately not Clone
/// ```
pub struct MnemonicSecret(String);

impl MnemonicSecret {
    pub fn new(mnemonic: String) -> Self {
        Self(mnemonic)
    }

    /// The single sanctioned egress: consume the secret into the direct
    /// RPC reply value.
    pub fn into_rpc_value(self) -> Value {
        Value::String(self.0)
    }
}
