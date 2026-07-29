//! Task 60 slice 5: the three rebalance operator RPCs, Python-equivalent
//! contracts (`cl-revenue-ops.py:3802` cycle, `:3896` debug, `:4826`
//! manual). Handlers are pure functions over injected state so tests
//! drive them without a plugin runtime.
//!
//! Parity notes:
//! - Validation/error strings on the manual path are VERBATIM Python
//!   (usage line, SCID format error with repr quoting, amount/fee
//!   coercion errors, the hard-cap shape "rejected even under force").
//! - The uninitialized arms match Python exactly: cycle/debug say
//!   `"Rebalancer not initialized"`, manual says `"Plugin not fully
//!   initialized"` -- and the owner's `EngineNotAssembled` refusal maps
//!   onto them (pre-cutover production is permanently in that arm).
//! - The rate limiter is Python's `ForceRateLimiter` sliding window
//!   (10 calls / 60 s keyed by command, refusal text verbatim), applied
//!   to BOTH force values (DD2/P1-001).
//! - SUCCESS-arm response shapes are Rust-typed (owner summaries), NOT
//!   byte-parity: Python composes them from rebalancer internals that do
//!   not exist before engine assembly. They are reachable only with an
//!   assembled engine (tests; cutover later) and are pinned by tests as
//!   Rust shapes -- never a fabricated Python lookalike.

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::{json, Value};

use crate::rebalance_execution::RebalanceSubmitOutcome;
use crate::rebalance_owner::{ManualRebalanceParams, RebalanceOwnerHandle, RebalanceRefusal};

/// Python's `ForceRateLimiter` (cl-revenue-ops.py:238-311): sliding
/// window, per-command, refusal message verbatim. The caller supplies
/// `now` (seconds) so tests are deterministic.
pub struct ForceRateLimiter {
    max_calls: usize,
    window_seconds: f64,
    timestamps: Mutex<HashMap<String, Vec<f64>>>,
}

impl ForceRateLimiter {
    /// The production limits (py: global `ForceRateLimiter(10, 60)`).
    pub fn production() -> Self {
        Self::new(10, 60.0)
    }

    pub fn new(max_calls: usize, window_seconds: f64) -> Self {
        Self {
            max_calls,
            window_seconds,
            timestamps: Mutex::new(HashMap::new()),
        }
    }

    /// `Ok(())` records the call; `Err(message)` is the verbatim Python
    /// refusal text.
    pub fn check_rate_limit(&self, command: &str, now: f64) -> Result<(), String> {
        let cutoff = now - self.window_seconds;
        let mut map = self.timestamps.lock().expect("rate limiter poisoned");
        let stamps = map.entry(command.to_string()).or_default();
        stamps.retain(|ts| *ts > cutoff);
        if stamps.len() >= self.max_calls {
            // Python indexes [0] unconditionally; a zero-max limiter (test
            // configs only) has no first stamp -- report a full window.
            let remaining = stamps
                .first()
                .map(|first| first + self.window_seconds - now)
                .unwrap_or(self.window_seconds);
            return Err(format!(
                "Rate limit exceeded for force={command}. Try again in {}s. ({} calls per {}s)",
                remaining as i64, self.max_calls, self.window_seconds as i64
            ));
        }
        stamps.push(now);
        Ok(())
    }
}

/// Python's `{cid!r}` for the SCID error: strings repr with single
/// quotes, everything else as its JSON text.
fn py_repr(v: &Value) -> String {
    match v {
        Value::String(s) => format!("'{s}'"),
        other => other.to_string(),
    }
}

/// Python's `int(x)` over the JSON shapes CLN hands us: ints pass,
/// floats truncate, digit strings parse; everything else fails.
fn py_int(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f.trunc() as i64)),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

const USAGE: &str =
    "usage: revenue-rebalance from_channel to_channel amount_sats [max_fee_sats] [force=false]";

fn scid_valid(cid: &str) -> bool {
    let parts: Vec<&str> = cid.split(['x', ':']).collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

/// `revenue-rebalance` (py:4826). `hard_cap` is the same
/// `rebalance_max_amount` the owner enforces (0 = unset). The parameter
/// list mirrors the Python signature positionally -- grouping them would
/// only obscure the parity mapping.
#[allow(clippy::too_many_arguments)]
pub async fn handle_manual_rebalance(
    owner: Option<&RebalanceOwnerHandle>,
    limiter: &ForceRateLimiter,
    hard_cap: i64,
    now: f64,
    from_channel: Option<&Value>,
    to_channel: Option<&Value>,
    amount_sats: Option<&Value>,
    max_fee_sats: Option<&Value>,
    force: bool,
) -> Value {
    // 1. Usage (py: falsy from/to or amount None).
    let usable = |v: Option<&Value>| {
        matches!(v, Some(Value::String(s)) if !s.is_empty())
            || matches!(v, Some(x) if !x.is_null() && !x.is_string())
    };
    if !usable(from_channel)
        || !usable(to_channel)
        || !matches!(amount_sats, Some(v) if !v.is_null())
    {
        return json!({"error": USAGE});
    }
    let from_v = from_channel.expect("checked");
    let to_v = to_channel.expect("checked");
    let amount_v = amount_sats.expect("checked");

    // 2. Initialization arm (py: rebalancer None).
    let Some(owner) = owner else {
        return json!({"error": "Plugin not fully initialized"});
    };

    // 3. Rate limit, BOTH force values (DD2/P1-001).
    if let Err(msg) = limiter.check_rate_limit("revenue-rebalance", now) {
        return json!({"status": "error", "error": msg});
    }

    // 4. SCID validation with the P1-012 non-string guard.
    for cid in [from_v, to_v] {
        let ok = matches!(cid, Value::String(s) if scid_valid(s));
        if !ok {
            return json!({
                "status": "error",
                "error": format!(
                    "Invalid channel format for {}. Use SCID format (e.g., 123x456x789).",
                    py_repr(cid)
                ),
            });
        }
    }

    // 5. Amount validation.
    let Some(amount) = py_int(amount_v) else {
        return json!({"status": "error", "error": "amount_sats must be an integer"});
    };
    if amount < 1 {
        return json!({"status": "error", "error": "amount_sats must be at least 1"});
    }

    // 6. Hard cap: binds regardless of force (DD2/P1-004).
    if hard_cap > 0 && amount > hard_cap {
        return json!({
            "status": "error",
            "error": format!(
                "amount_sats {amount} exceeds hard rebalance cap {hard_cap} \
                 (rebalance_max_amount); rejected even under force."
            ),
            "requested_sats": amount,
            "max_amount_sats": hard_cap,
        });
    }

    // 7. max_fee_sats: integer-or-null, non-negative.
    let max_fee = match max_fee_sats {
        None | Some(Value::Null) => None,
        Some(v) => match py_int(v) {
            None => {
                return json!({
                    "status": "error",
                    "error": "max_fee_sats must be an integer or null"
                });
            }
            Some(fee) if fee < 0 => {
                return json!({
                    "status": "error",
                    "error": "max_fee_sats must be non-negative"
                });
            }
            Some(fee) => Some(fee),
        },
    };

    // 8. The owner runs the rail.
    let from = from_v.as_str().expect("validated SCID").to_string();
    let to = to_v.as_str().expect("validated SCID").to_string();
    match owner
        .manual(ManualRebalanceParams {
            from_channel: from,
            to_channel: to,
            amount_sats: amount,
            max_fee_sats: max_fee,
            force,
        })
        .await
    {
        Err(RebalanceRefusal::EngineNotAssembled) => {
            json!({"error": "Plugin not fully initialized"})
        }
        Err(refusal) => json!({
            "status": "error",
            "code": refusal.code(),
            "error": format!("{refusal:?}"),
        }),
        Ok(outcome) => match &outcome.outcome {
            RebalanceSubmitOutcome::Success {
                fee_sats,
                fee_msat,
                hops,
                parts,
            } => json!({
                "status": "success",
                "request_id": outcome.request_id,
                "outcome": "success",
                "fee_sats": fee_sats,
                "fee_msat": fee_msat,
                "hops": hops,
                "parts": parts,
            }),
            RebalanceSubmitOutcome::CleanFailureBeforeWrite { detail } => json!({
                "status": "error",
                "request_id": outcome.request_id,
                "outcome": "clean_failure_before_write",
                "error": detail,
            }),
            RebalanceSubmitOutcome::Rejected { detail } => json!({
                "status": "error",
                "request_id": outcome.request_id,
                "outcome": "rejected",
                "error": detail,
            }),
            RebalanceSubmitOutcome::OutcomeUnknownAfterSubmit {
                payment_hash,
                detail,
            } => json!({
                "status": "error",
                "request_id": outcome.request_id,
                "outcome": "outcome_unknown",
                "error": detail,
                "payment_hash": payment_hash,
            }),
        },
    }
}

/// `revenue-rebalance-cycle` (py:3802).
pub async fn handle_rebalance_cycle(owner: Option<&RebalanceOwnerHandle>) -> Value {
    let Some(owner) = owner else {
        return json!({"error": "Rebalancer not initialized"});
    };
    match owner.run_cycle().await {
        Err(RebalanceRefusal::EngineNotAssembled) => {
            json!({"error": "Rebalancer not initialized"})
        }
        Err(refusal) => json!({
            "status": "error",
            "code": refusal.code(),
            "error": format!("{refusal:?}"),
        }),
        Ok(summary) => json!({
            "status": "success",
            "last_cycle": {
                "executions": summary.executions,
                "recorded": summary.recorded,
                "skips": summary.skips,
            },
        }),
    }
}

/// `revenue-rebalance-debug` (py:3896): filter coercion parity, then the
/// owner's diagnostic state (the rebalancer-internal buckets arrive with
/// engine assembly).
pub async fn handle_rebalance_debug(
    owner: Option<&RebalanceOwnerHandle>,
    channel_id: Option<&Value>,
    peer_id: Option<&Value>,
    summary_only: bool,
    include_hot_markers: bool,
    max_candidates: Option<&Value>,
) -> Value {
    let Some(owner) = owner else {
        return json!({"error": "Rebalancer not initialized"});
    };
    let Some(owner_debug) = owner.debug().await else {
        return json!({"error": "Rebalancer not initialized"});
    };
    if owner_debug["engine_assembled"] != json!(true) {
        return json!({"error": "Rebalancer not initialized"});
    }

    // Filter coercion, py parity: strings trimmed (peer lowercased),
    // include_hot_markers forced false under summary_only, and a
    // non-int max_candidates coerces to 0 -- never an exception out of
    // a diagnostic handler (P1-012 class).
    let filter_channel = channel_id
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("")
        .to_string();
    let filter_peer = peer_id
        .and_then(Value::as_str)
        .map(|s| s.trim().to_lowercase())
        .unwrap_or_default();
    let include_hot_markers = include_hot_markers && !summary_only;
    let max_candidates = max_candidates.and_then(py_int).unwrap_or(0).max(0);

    json!({
        "executor_available": true,
        "dry_run": true,
        "filters": {
            "channel_id": if filter_channel.is_empty() { Value::Null } else { json!(filter_channel) },
            "peer_id": if filter_peer.is_empty() { Value::Null } else { json!(filter_peer) },
            "summary_only": summary_only,
            "include_hot_markers": include_hot_markers,
            "max_candidates": if max_candidates == 0 { Value::Null } else { json!(max_candidates) },
        },
        "rust_owner": owner_debug,
    })
}
