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

use revops_lnplus::error::LnPlusError;
use revops_lnplus::http::{LnPlusApiClient, SignError, Signer};
use revops_lnplus::http_ureq::UreqTransport;
use revops_lnplus::ports::{
    ChainPort, ChannelInfo, Feerate, FundChannelOutcome, FundChannelResult, LnPlusApi, PortError,
    PortResult,
};
use revops_lnplus::types::{MySwaps, NotificationEntry, Rating, SwapDetail, SwapListing};
use serde_json::{json, Value};

/// Per-call budget for both the connect and the call phase, matching the
/// fee broadcaster's default.
pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(20);

/// Shared plumbing: one socket path + one private current-thread runtime.
///
/// The runtime is held in an `Option` so [`Drop`] can hand it to
/// `shutdown_background()`: a plain `Runtime` drop BLOCKS and panics when
/// it happens inside an async context — exactly what would occur if an
/// async startup error path (e.g. the passive-observer refusal in
/// `ObserverRuntime::start`) discarded an adapter-holding pass.
struct ClnCall {
    socket_path: PathBuf,
    timeout: Duration,
    rt: Option<tokio::runtime::Runtime>,
}

impl Drop for ClnCall {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
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
            rt: Some(rt),
        })
    }

    fn call(&self, method: &str, params: Value) -> CallOutcome {
        let budget = self.timeout;
        let rt = self.rt.as_ref().expect("runtime present until drop");
        rt.block_on(async {
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

/// Task 61 4C correction F4C-1 (single-sourced for BOTH the armed and the
/// observer chain types): channel-row decoding is STRICT on the four
/// critical fields (peer_id, state, total_msat, to_us_msat). A well-formed
/// EMPTY `channels` array is authoritative absence; a missing container or
/// an undecodable row is an ERROR — defaulted empty/zero rows would let
/// reconciliation misread malformed evidence as absence and release a
/// quarantined reservation. `funding_txid` alone stays optional
/// (legitimately absent pre-lockin).
fn decode_channel_rows(value: &Value) -> PortResult<Vec<ChannelInfo>> {
    let channels = value
        .get("channels")
        .and_then(Value::as_array)
        .ok_or_else(|| PortError::new("listpeerchannels response carried no channels"))?;
    let mut out = Vec::with_capacity(channels.len());
    for ch in channels {
        let critical_str = |key: &str| -> PortResult<String> {
            ch.get(key)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| {
                    PortError::new(format!(
                        "listpeerchannels row missing/mistyped critical field {key:?} — \
                         refusing to decode malformed channel evidence"
                    ))
                })
        };
        let critical_i64 = |key: &str| -> PortResult<i64> {
            ch.get(key).and_then(Value::as_i64).ok_or_else(|| {
                PortError::new(format!(
                    "listpeerchannels row missing/mistyped critical field {key:?} — \
                     refusing to decode malformed channel evidence"
                ))
            })
        };
        out.push(ChannelInfo {
            peer_id: critical_str("peer_id")?,
            state: critical_str("state")?,
            total_msat: critical_i64("total_msat")?,
            to_us_msat: critical_i64("to_us_msat")?,
            funding_txid: ch
                .get("funding_txid")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    Ok(out)
}

/// Confirmed + unreserved on-chain sats from a `listfunds` response.
fn sum_confirmed_unreserved(value: &Value) -> PortResult<i64> {
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

impl ChainPort for ClnChainAdapter {
    fn our_node_id(&self) -> PortResult<String> {
        let info = self.call.read("getinfo", json!({}))?;
        info.get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| PortError::new("getinfo response carried no id"))
    }

    /// Task 61 4C correction F4C-1: channel-row decoding is STRICT on the
    /// four critical fields (peer_id, state, total_msat, to_us_msat). A
    /// well-formed EMPTY `channels` array is authoritative absence; a
    /// missing container or an undecodable row is an ERROR — defaulted
    /// empty/zero rows would let reconciliation misread malformed
    /// evidence as absence and release a quarantined reservation.
    /// `funding_txid` alone stays optional (legitimately absent
    /// pre-lockin).
    fn list_peer_channels(&self, peer: Option<&str>) -> PortResult<Vec<ChannelInfo>> {
        let params = match peer {
            Some(p) => json!({"id": p}),
            None => json!({}),
        };
        let value = self.call.read("listpeerchannels", params)?;
        decode_channel_rows(&value)
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
        sum_confirmed_unreserved(&value)
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

// ---------------------------------------------------------------------------
// Task 61 4D — OBSERVER-side types: the read half of each surface with
// every action method a PURE refusal (no inner action object, no RPC
// constructed, nothing to arm). These are the ONLY LN+ adapter types the
// observer runtime composition (`lnplus_runtime.rs`) may name — enforced
// by `tests/action_surface.rs`.
// ---------------------------------------------------------------------------

fn observer_refusal(what: &str) -> String {
    format!("observer runtime holds no LN+ action capability — refused {what}")
}

/// Read-only CLN surface for observer composition. Reads share the
/// concrete plumbing; `connect`/`fund_channel` refuse WITHOUT building a
/// request — there is no action path in this type to reach.
pub struct ObserverClnChain {
    call: ClnCall,
}

impl ObserverClnChain {
    pub fn new(socket_path: PathBuf, timeout: Duration) -> Result<Self, std::io::Error> {
        Ok(Self {
            call: ClnCall::new(socket_path, timeout)?,
        })
    }
}

impl ChainPort for ObserverClnChain {
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
        decode_channel_rows(&value)
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
        sum_confirmed_unreserved(&value)
    }

    fn connect(&self, target: &str) -> PortResult<()> {
        Err(PortError::new(observer_refusal(&format!(
            "connect({target})"
        ))))
    }

    fn fund_channel(
        &self,
        peer: &str,
        amount_sats: i64,
        _feerate: Feerate,
    ) -> PortResult<FundChannelOutcome> {
        Err(PortError::new(observer_refusal(&format!(
            "fund_channel({peer}, {amount_sats} sats)"
        ))))
    }
}

/// Read-only LN+ API surface for observer composition, over the concrete
/// production client (ureq transport + CLN signer). Every method that
/// applies/withdraws/completes/rates/marks-read refuses WITHOUT issuing a
/// request — no wire write, no auth challenge, nothing.
pub struct ObserverLnPlusApi {
    client: LnPlusApiClient<UreqTransport, ClnSigner>,
}

impl ObserverLnPlusApi {
    pub fn new(
        base_url: impl Into<String>,
        http_timeout: Duration,
        signer_socket: PathBuf,
        rpc_timeout: Duration,
    ) -> Result<Self, std::io::Error> {
        let transport = UreqTransport::with_timeout(http_timeout);
        let signer = ClnSigner::new(signer_socket, rpc_timeout)?;
        Ok(Self {
            client: LnPlusApiClient::with_base_url(transport, signer, base_url),
        })
    }
}

impl LnPlusApi for ObserverLnPlusApi {
    fn get_applicable_swaps(&self) -> Result<Vec<SwapListing>, LnPlusError> {
        self.client.get_applicable_swaps()
    }
    fn get_swap(&self, swap_id: &str) -> Result<SwapDetail, LnPlusError> {
        self.client.get_swap(swap_id)
    }
    fn get_my_swaps(&self) -> Result<MySwaps, LnPlusError> {
        self.client.get_my_swaps()
    }
    fn create_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        Err(LnPlusError::new(observer_refusal(&format!(
            "create_application({swap_id})"
        ))))
    }
    fn delete_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        Err(LnPlusError::new(observer_refusal(&format!(
            "delete_application({swap_id})"
        ))))
    }
    fn complete_application(&self, swap_id: &str) -> Result<(), LnPlusError> {
        Err(LnPlusError::new(observer_refusal(&format!(
            "complete_application({swap_id})"
        ))))
    }
    fn get_notifications(&self) -> Result<Vec<NotificationEntry>, LnPlusError> {
        self.client.get_notifications()
    }
    fn mark_read_notifications(&self) -> Result<(), LnPlusError> {
        Err(LnPlusError::new(observer_refusal(
            "mark_read_notifications",
        )))
    }
    fn create_rating(&self, swap_id: &str, rating: Rating) -> Result<(), LnPlusError> {
        Err(LnPlusError::new(observer_refusal(&format!(
            "create_rating({swap_id}, {})",
            rating.as_str()
        ))))
    }
}
