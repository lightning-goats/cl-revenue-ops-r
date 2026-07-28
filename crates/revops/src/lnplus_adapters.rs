//! Task 61 4C — concrete CLN adapters for the LN+ port traits: a
//! [`Signer`] over `signmessage` and a [`ChainPort`] over
//! `getinfo`/`listpeerchannels`/`feerates`/`listfunds`/`connect`/
//! `fundchannel`, all via `cln_rpc` on a Unix socket.
//!
//! ## Phase discipline (feeds Task 61 4B's typed outcomes)
//!
//! `fund_channel` follows the exact model `fee_execution::attempt_send`
//! established for the one other money-moving call in this crate:
//!  - connect-phase failure/timeout → `Err(PortError)` — CLEAN, no bytes
//!    could have reached lightningd;
//!  - a genuine JSON-RPC error object (has a code) → `Err(PortError)` —
//!    a DEFINITE rejection by lightningd, outcome known;
//!  - call-phase timeout, EOF, undecodable framing — anything after the
//!    request bytes may have left this process →
//!    [`FundChannelOutcome::OutcomeUnknown`], never a clean failure.
//!
//! Read-only calls map every failure to a plain `Err` (no phase concern).
//!
//! ## Safety boundary
//!
//! This module only DEFINES the adapters. Construction/wiring is the 4D
//! runtime's job, where the action-capable [`ChainPort`] half stays
//! structurally out of observer composition and behind
//! `GatedChainPort`/`ExecutionMode`. No test exercises these against
//! anything but a throwaway Unix-socket fake (`tests/lnplus_adapters.rs`);
//! no default path resolves to a real `lightning-rpc`.
//!
//! ## Sync bridge
//!
//! The LN+ kernel is synchronous; these adapters own a private
//! current-thread tokio runtime and `block_on` each call. They must be
//! used from a blocking context (the 4D owner runs passes inside
//! `spawn_blocking`), never from inside an async task.

use std::path::PathBuf;
use std::time::Duration;

use revops_lnplus::http::{SignError, Signer};
use revops_lnplus::ports::{
    ChainPort, ChannelInfo, Feerate, FundChannelOutcome, FundChannelResult, PortError, PortResult,
};
use serde_json::{json, Value};

/// Per-call budget for both the connect and the call phase, matching the
/// fee broadcaster's default.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(20);

/// Shared plumbing: one socket path + one private current-thread runtime.
struct ClnCall {
    socket_path: PathBuf,
    timeout: Duration,
    rt: tokio::runtime::Runtime,
}

/// How a raw call ended, phase-classified per the module doc.
enum CallOutcome {
    Success(Value),
    /// lightningd itself answered with a JSON-RPC error object.
    Rejected(String),
    /// Nothing can have reached lightningd.
    CleanFailure(String),
    /// The request may have arrived; the response never did.
    Ambiguous(String),
}

impl ClnCall {
    fn new(socket_path: PathBuf, timeout: Duration) -> Result<Self, std::io::Error> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            socket_path,
            timeout,
            rt,
        })
    }

    fn call(&self, method: &str, params: Value) -> CallOutcome {
        let budget = self.timeout;
        self.rt.block_on(async {
            let connected =
                tokio::time::timeout(budget, cln_rpc::ClnRpc::new(&self.socket_path)).await;
            let mut rpc = match connected {
                Err(_) => {
                    return CallOutcome::CleanFailure(
                        "connect to lightning-rpc timed out before any write was attempted"
                            .to_string(),
                    )
                }
                Ok(Err(e)) => return CallOutcome::CleanFailure(format!("connect failed: {e}")),
                Ok(Ok(rpc)) => rpc,
            };
            let called =
                tokio::time::timeout(budget, rpc.call_raw::<Value, Value>(method, &params)).await;
            match called {
                Err(_) => CallOutcome::Ambiguous(format!(
                    "no response to {method} within the timeout budget after the request was sent"
                )),
                Ok(Ok(value)) => CallOutcome::Success(value),
                Ok(Err(rpc_err)) => {
                    if rpc_err.code.is_some() {
                        CallOutcome::Rejected(rpc_err.to_string())
                    } else {
                        // Synthesized locally (write failure, EOF,
                        // undecodable framing): bytes may already have
                        // left this process.
                        CallOutcome::Ambiguous(rpc_err.to_string())
                    }
                }
            }
        })
    }

    /// Read-only calls: ANY failure is a plain error (no phase concern —
    /// reads are trivially re-issuable).
    fn read(&self, method: &str, params: Value) -> PortResult<Value> {
        match self.call(method, params) {
            CallOutcome::Success(v) => Ok(v),
            CallOutcome::Rejected(e) => Err(PortError::new(format!("{method} rejected: {e}"))),
            CallOutcome::CleanFailure(e) | CallOutcome::Ambiguous(e) => {
                Err(PortError::new(format!("{method} failed: {e}")))
            }
        }
    }
}

/// Concrete [`Signer`]: CLN `signmessage`, returning the `zbase` field.
pub struct ClnSigner {
    call: ClnCall,
}

impl ClnSigner {
    pub fn new(socket_path: PathBuf, timeout: Duration) -> Result<Self, std::io::Error> {
        Ok(Self {
            call: ClnCall::new(socket_path, timeout)?,
        })
    }
}

impl Signer for ClnSigner {
    fn signmessage(&self, message: &str) -> Result<String, SignError> {
        let value = match self.call.call("signmessage", json!({"message": message})) {
            CallOutcome::Success(v) => v,
            CallOutcome::Rejected(e) => {
                return Err(SignError(format!("signmessage rejected: {e}")))
            }
            CallOutcome::CleanFailure(e) | CallOutcome::Ambiguous(e) => {
                return Err(SignError(format!("signmessage failed: {e}")))
            }
        };
        match value.get("zbase").and_then(Value::as_str) {
            Some(zbase) if !zbase.is_empty() => Ok(zbase.to_string()),
            _ => Err(SignError(
                "signmessage response carried no zbase signature".to_string(),
            )),
        }
    }
}

/// Concrete [`ChainPort`] over CLN RPC.
pub struct ClnChainAdapter {
    call: ClnCall,
}

impl ClnChainAdapter {
    pub fn new(socket_path: PathBuf, timeout: Duration) -> Result<Self, std::io::Error> {
        Ok(Self {
            call: ClnCall::new(socket_path, timeout)?,
        })
    }
}

fn msat_to_sat_field(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0) / 1000
}

impl ChainPort for ClnChainAdapter {
    fn our_node_id(&self) -> PortResult<String> {
        let info = self.call.read("getinfo", json!({}))?;
        info.get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| PortError::new("getinfo response carried no id"))
    }

    fn list_peer_channels(&self, peer: Option<&str>) -> PortResult<Vec<ChannelInfo>> {
        let params = match peer {
            Some(p) => json!({"id": p}),
            None => json!({}),
        };
        let value = self.call.read("listpeerchannels", params)?;
        let channels = value
            .get("channels")
            .and_then(Value::as_array)
            .ok_or_else(|| PortError::new("listpeerchannels response carried no channels"))?;
        Ok(channels
            .iter()
            .map(|ch| ChannelInfo {
                peer_id: ch
                    .get("peer_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                state: ch
                    .get("state")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                total_msat: ch.get("total_msat").and_then(Value::as_i64).unwrap_or(0),
                to_us_msat: ch.get("to_us_msat").and_then(Value::as_i64).unwrap_or(0),
                funding_txid: ch
                    .get("funding_txid")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
            .collect())
    }

    fn opening_feerate_perkw(&self) -> PortResult<i64> {
        let value = self.call.read("feerates", json!({"style": "perkw"}))?;
        value
            .get("perkw")
            .and_then(|p| p.get("opening"))
            .and_then(Value::as_i64)
            .ok_or_else(|| PortError::new("feerates response carried no perkw.opening"))
    }

    fn confirmed_unreserved_sats(&self) -> PortResult<i64> {
        let value = self.call.read("listfunds", json!({}))?;
        let outputs = value
            .get("outputs")
            .and_then(Value::as_array)
            .ok_or_else(|| PortError::new("listfunds response carried no outputs"))?;
        Ok(outputs
            .iter()
            .filter(|o| {
                o.get("status").and_then(Value::as_str) == Some("confirmed")
                    && o.get("reserved").and_then(Value::as_bool) != Some(true)
            })
            .map(|o| msat_to_sat_field(o, "amount_msat"))
            .sum())
    }

    fn connect(&self, target: &str) -> PortResult<()> {
        match self.call.call("connect", json!({"id": target})) {
            CallOutcome::Success(_) => Ok(()),
            CallOutcome::Rejected(e) => Err(PortError::new(format!("connect rejected: {e}"))),
            // `connect` is idempotent and commits nothing — an ambiguous
            // failure is safely retried next pass, so a plain Err is the
            // honest mapping (the caller treats it as retry-next-pass).
            CallOutcome::CleanFailure(e) | CallOutcome::Ambiguous(e) => {
                Err(PortError::new(format!("connect failed: {e}")))
            }
        }
    }

    fn fund_channel(
        &self,
        peer: &str,
        amount_sats: i64,
        feerate: Feerate,
    ) -> PortResult<FundChannelOutcome> {
        let params = json!({
            "id": peer,
            "amount": format!("{amount_sats}sat"),
            "feerate": feerate.as_str(),
        });
        match self.call.call("fundchannel", params) {
            CallOutcome::Success(value) => Ok(FundChannelOutcome::Funded(FundChannelResult {
                txid: value
                    .get("txid")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })),
            CallOutcome::Rejected(e) => Err(PortError::new(format!("fundchannel rejected: {e}"))),
            CallOutcome::CleanFailure(e) => {
                Err(PortError::new(format!("fundchannel not submitted: {e}")))
            }
            CallOutcome::Ambiguous(detail) => Ok(FundChannelOutcome::OutcomeUnknown { detail }),
        }
    }
}
