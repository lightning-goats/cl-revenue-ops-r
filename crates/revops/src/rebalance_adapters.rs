//! Task 60 slice 3: concrete CLN JSON-RPC adapters for the rebalance
//! executor's [`PaymentRpc`] seam and the owner's reconciliation read.
//!
//! Blocking on purpose: the executor trait is synchronous and the
//! rebalance owner runs it off the async runtime (dedicated thread /
//! `spawn_blocking`), exactly like the fee scheduler's owner thread. One
//! fresh connection per call with hard read/write deadlines -- a wedged
//! node can stall one call for at most the configured budget, never hang
//! the owner forever.
//!
//! Error-encoding conventions (the executor seam contract, mirrored by
//! its fixtures -- see `revops_rebalance::executor`'s module docs):
//!
//! - A CLN JSON-RPC **error response** is carried as
//!   `RpcFailure { message: <the error dict as JSON text> }`, so
//!   `error_details` recovers `code`/`message`/`data` exactly like
//!   Python's `_error_details` (waitsendpay `code` 200 = payment still
//!   pending).
//! - A **deadline expiry** (no reply within budget) is PLAIN TEXT
//!   containing `"rpc timeout"` -- the pending detector's proxy-timeout
//!   arm.
//! - Every other transport failure (missing socket, connection refused,
//!   EOF, undecodable frame) is plain text WITHOUT `"rpc timeout"`: those
//!   are provably-nothing-sent or definitively-answered shapes and must
//!   never read as payment-possibly-pending.
//!
//! `PaymentMode` is untouched here: these adapters carry no mode and mint
//! no capability -- the dry-run/live gate stays inside
//! `NativeRouteExecutor`, and the live payment mode remains constructible
//! only by the cutover task (workspace source scan in
//! `revops-rebalance/tests/executor.rs` -- which is also why this comment
//! does not spell the variant path).

use std::io::{ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use revops_rebalance::executor::PaymentRpc;
use revops_rebalance::router::{RpcFailure, SendpayHop};
use serde_json::{json, Value};

static REQUEST_SEQ: AtomicU64 = AtomicU64::new(1);

/// One blocking JSON-RPC call over a fresh Unix-socket connection.
fn call_blocking(
    socket_path: &PathBuf,
    timeout_seconds: u64,
    method: &str,
    params: Value,
) -> Result<Value, RpcFailure> {
    let budget = Duration::from_secs(timeout_seconds.max(1));
    let mut stream = UnixStream::connect(socket_path).map_err(|e| RpcFailure {
        message: format!("connect {} for {method}: {e}", socket_path.display()),
    })?;
    stream
        .set_read_timeout(Some(budget))
        .and_then(|()| stream.set_write_timeout(Some(budget)))
        .map_err(|e| RpcFailure {
            message: format!("set socket deadline for {method}: {e}"),
        })?;

    let id = REQUEST_SEQ.fetch_add(1, Ordering::Relaxed);
    let request = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    let body = serde_json::to_vec(&request).map_err(|e| RpcFailure {
        message: format!("serialize {method} request: {e}"),
    })?;
    stream.write_all(&body).map_err(|e| RpcFailure {
        message: if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut {
            format!("rpc timeout after {timeout_seconds}s writing {method}")
        } else {
            format!("write {method} request: {e}")
        },
    })?;

    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let response: Value = loop {
        let n = stream.read(&mut chunk).map_err(|e| RpcFailure {
            message: if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut {
                format!("rpc timeout after {timeout_seconds}s awaiting {method} reply")
            } else {
                format!("read {method} reply: {e}")
            },
        })?;
        if n == 0 {
            return Err(RpcFailure {
                message: format!("{method}: connection closed before a full reply"),
            });
        }
        buf.extend_from_slice(&chunk[..n]);
        // CLN frames replies as JSON followed by a blank line; parse the
        // accumulated buffer (trailing whitespace tolerated) each read.
        if let Ok(v) =
            serde_json::from_slice::<Value>(String::from_utf8_lossy(&buf).trim_end().as_bytes())
        {
            break v;
        }
    };

    if let Some(error) = response.get("error") {
        return Err(RpcFailure {
            // The seam convention: the message IS the error dict as JSON.
            message: error.to_string(),
        });
    }
    match response.get("result") {
        Some(result) => Ok(result.clone()),
        None => Err(RpcFailure {
            message: format!("{method}: reply carries neither result nor error"),
        }),
    }
}

fn hop_json(hop: &SendpayHop) -> Value {
    json!({
        "id": hop.id,
        "channel": hop.channel,
        "direction": hop.direction,
        "delay": hop.delay,
        "amount_msat": hop.amount_msat,
        "style": hop.style,
    })
}

/// The live [`PaymentRpc`] adapter. No mode field, no capability: it can
/// speak the six payment-path methods and nothing else.
pub struct ClnPaymentRpc {
    socket_path: PathBuf,
    timeout_seconds: u64,
}

impl ClnPaymentRpc {
    pub fn new(socket_path: PathBuf, timeout_seconds: u64) -> Self {
        Self {
            socket_path,
            timeout_seconds,
        }
    }
}

impl PaymentRpc for ClnPaymentRpc {
    fn getinfo_id(&self) -> Result<String, RpcFailure> {
        let value = call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "getinfo",
            json!({}),
        )?;
        value
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| RpcFailure {
                message: "getinfo reply carries no id".to_string(),
            })
    }

    fn invoice(
        &self,
        amount_msat: i64,
        label: &str,
        expiry_secs: i64,
    ) -> Result<Value, RpcFailure> {
        call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "invoice",
            json!({
                "amount_msat": amount_msat,
                "label": label,
                "description": label,
                "expiry": expiry_secs,
            }),
        )
    }

    fn sendpay(
        &self,
        route: &[SendpayHop],
        payment_hash: &str,
        bolt11: &str,
        payment_secret: &str,
    ) -> Result<Value, RpcFailure> {
        let hops: Vec<Value> = route.iter().map(hop_json).collect();
        call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "sendpay",
            json!({
                "route": hops,
                "payment_hash": payment_hash,
                "bolt11": bolt11,
                "payment_secret": payment_secret,
            }),
        )
    }

    fn waitsendpay(&self, payment_hash: &str, timeout_secs: i64) -> Result<Value, RpcFailure> {
        // The socket deadline must outlive CLN's own wait window, or every
        // legitimate wait would misreport as a proxy timeout.
        let budget = self.timeout_seconds.max(timeout_secs.max(0) as u64 + 5);
        call_blocking(
            &self.socket_path,
            budget,
            "waitsendpay",
            json!({
                "payment_hash": payment_hash,
                "timeout": timeout_secs,
            }),
        )
    }

    fn delpay(&self, payment_hash: &str, status: &str) -> Result<(), RpcFailure> {
        call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "delpay",
            json!({
                "payment_hash": payment_hash,
                "status": status,
            }),
        )
        .map(|_| ())
    }

    fn delinvoice(&self, label: &str, status: &str) -> Result<(), RpcFailure> {
        call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "delinvoice",
            json!({
                "label": label,
                "status": status,
            }),
        )
        .map(|_| ())
    }
}

/// The restart-reconciliation read surface: exactly one method, read-only.
pub struct ClnReconcileRpc {
    socket_path: PathBuf,
    timeout_seconds: u64,
}

impl ClnReconcileRpc {
    pub fn new(socket_path: PathBuf, timeout_seconds: u64) -> Self {
        Self {
            socket_path,
            timeout_seconds,
        }
    }

    /// `listsendpays payment_hash=<hash>` -- the owner disambiguates
    /// complete/failed/pending from the verbatim result.
    pub fn listsendpays(&self, payment_hash: &str) -> Result<Value, RpcFailure> {
        call_blocking(
            &self.socket_path,
            self.timeout_seconds,
            "listsendpays",
            json!({ "payment_hash": payment_hash }),
        )
    }
}
