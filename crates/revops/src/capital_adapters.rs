//! Task 62 slice 4: the on-chain capital transports (`fundchannel`,
//! `close`) and the four-way submit classifier.
//!
//! Blocking on purpose, exactly like the rebalance adapters: one fresh
//! Unix-socket connection per call with hard read/write deadlines,
//! driven off the async runtime by the capital owner. The wire calls
//! reuse [`crate::rebalance_adapters::call_blocking`] so both seams
//! share one framing and one error-encoding convention.
//!
//! Classification is FAIL-CLOSED for on-chain ambiguity: a fundchannel
//! whose reply was lost MAY have broadcast, so only three shapes escape
//! [`CapitalSubmitOutcome::OutcomeUnknown`]:
//!
//! - **CleanRefusal**: the transport provably wrote nothing (connect
//!   failure, request serialization, socket-deadline setup).
//! - **Rejected**: CLN answered with a definite error dict (carried as
//!   JSON text in `RpcFailure.message`, the shared seam convention).
//! - **Success**: a reply carrying a `txid`. The rule is uniform --
//!   CLN's `close` returns the closing txid for both mutual and
//!   unilateral closes -- so a success-shaped reply WITHOUT a txid stays
//!   unknown rather than settling a reservation on guesswork.
//!
//! Deliberately absent: any defibrillation transport. Python's
//! `_execute_defibrillation` (capacity_planner.py:3666) is a bounded
//! diagnostic REBALANCE through the rebalancer, not an on-chain action;
//! the Rust port routes it through the Task 60 rebalance owner seam at
//! the capital-owner layer (slice 5), never through this module.
//!
//! No capability is minted here: the concrete adapters exist but nothing
//! in production constructs them until Task 69's authority assembly
//! (source-scan pinned in `tests/capital_adapters.rs`).

use std::path::PathBuf;

use revops_rebalance::router::RpcFailure;
use serde_json::{json, Value};

use crate::capital_boundaries::CapitalSubmitOutcome;
use crate::rebalance_adapters::call_blocking;

/// The channel-open transport seam (py `_rpc_fundchannel:3054`).
pub trait FundchannelRpc: Send + Sync {
    fn fundchannel(
        &self,
        peer_id: &str,
        amount_sats: i64,
        request_amt: Option<i64>,
        compact_lease: Option<String>,
    ) -> Result<Value, RpcFailure>;
}

/// The channel-close transport seam (py `_execute_close:3767`).
pub trait CloseRpc: Send + Sync {
    fn close(&self, channel_id: &str, unilateral_timeout: Option<i64>)
        -> Result<Value, RpcFailure>;
}

/// Live `fundchannel` adapter. No mode field, no capability.
pub struct ClnFundchannelRpc {
    socket_path: PathBuf,
    timeout_seconds: u64,
}

impl ClnFundchannelRpc {
    pub fn new(socket_path: PathBuf, timeout_seconds: u64) -> Self {
        Self {
            socket_path,
            timeout_seconds,
        }
    }
}

impl FundchannelRpc for ClnFundchannelRpc {
    fn fundchannel(
        &self,
        peer_id: &str,
        amount_sats: i64,
        request_amt: Option<i64>,
        compact_lease: Option<String>,
    ) -> Result<Value, RpcFailure> {
        let mut params = json!({
            "id": peer_id,
            "amount": amount_sats,
        });
        if let Some(request_amt) = request_amt {
            params["request_amt"] = json!(request_amt);
        }
        if let Some(compact_lease) = compact_lease {
            params["compact_lease"] = json!(compact_lease);
        }
        call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "fundchannel",
            params,
        )
    }
}

/// Live `close` adapter. No mode field, no capability.
pub struct ClnCloseRpc {
    socket_path: PathBuf,
    timeout_seconds: u64,
}

impl ClnCloseRpc {
    pub fn new(socket_path: PathBuf, timeout_seconds: u64) -> Self {
        Self {
            socket_path,
            timeout_seconds,
        }
    }
}

impl CloseRpc for ClnCloseRpc {
    fn close(
        &self,
        channel_id: &str,
        unilateral_timeout: Option<i64>,
    ) -> Result<Value, RpcFailure> {
        let mut params = json!({ "id": channel_id });
        if let Some(timeout) = unilateral_timeout {
            params["unilateraltimeout"] = json!(timeout);
        }
        call_blocking(&self.socket_path, self.timeout_seconds, "close", params)
    }
}

/// The READ-ONLY restart-reconciliation lookups (`listfunds`,
/// `listclosedchannels`). Unlike the transports above, this is
/// production-constructible: it can observe, never move funds.
pub struct ClnCapitalReconcileRpc {
    socket_path: PathBuf,
    timeout_seconds: u64,
}

impl ClnCapitalReconcileRpc {
    pub fn new(socket_path: PathBuf, timeout_seconds: u64) -> Self {
        Self {
            socket_path,
            timeout_seconds,
        }
    }
}

impl crate::capital_owner::CapitalReconcileLookup for ClnCapitalReconcileRpc {
    fn listfunds(&self) -> Result<Value, String> {
        call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "listfunds",
            json!({}),
        )
        .map_err(|e| e.message)
    }
    fn listclosedchannels(&self) -> Result<Value, String> {
        call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "listclosedchannels",
            json!({}),
        )
        .map_err(|e| e.message)
    }
}

/// Message prefixes `call_blocking` emits ONLY before any request byte
/// reaches the wire.
const NOTHING_SENT_PREFIXES: [&str; 3] = ["connect ", "serialize ", "set socket deadline"];

/// Classify one capital submit result into the four-way vocabulary.
/// Unknown is the default: only provably-nothing-sent, a definite CLN
/// error dict, or a txid-bearing reply escape it.
pub fn classify_capital_submit(result: &Result<Value, RpcFailure>) -> CapitalSubmitOutcome {
    match result {
        Ok(reply) => match reply.get("txid").and_then(Value::as_str) {
            Some(txid) => CapitalSubmitOutcome::Success {
                txid: Some(txid.to_string()),
            },
            None => CapitalSubmitOutcome::OutcomeUnknown {
                detail: format!("reply carried no txid: {reply}"),
            },
        },
        Err(failure) => {
            // The seam convention: a definite CLN error response travels
            // as the error dict rendered to JSON text.
            if matches!(
                serde_json::from_str::<Value>(&failure.message),
                Ok(Value::Object(_))
            ) {
                return CapitalSubmitOutcome::Rejected {
                    detail: failure.message.clone(),
                };
            }
            if NOTHING_SENT_PREFIXES
                .iter()
                .any(|prefix| failure.message.starts_with(prefix))
            {
                return CapitalSubmitOutcome::CleanRefusal {
                    detail: failure.message.clone(),
                };
            }
            CapitalSubmitOutcome::OutcomeUnknown {
                detail: failure.message.clone(),
            }
        }
    }
}
