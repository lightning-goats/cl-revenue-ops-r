//! Task 63 slice 6: the 22 Python-equivalent Boltz RPC handlers.
//!
//! Parity notes:
//! - The uninitialized arm is Python's EXACT 1-key
//!   `{"error": "Boltz CLI integration not initialized"}`
//!   (cl-revenue-ops.py:7690-7692's `_require_boltz_manager`), returned
//!   both when no owner exists AND when the owner's action capability is
//!   unassembled -- which is production's permanent state until Task 69.
//! - The three usage short-circuits (status/refund/claim) fire BEFORE
//!   that guard, exactly like Python, so they appear even with Boltz
//!   dead.
//! - The auto-cycle pair does NOT use the error arm: `run-now` returns
//!   the `{'status': 'disabled', 'reason': ..., 'trigger': ...}` shape
//!   and `status` never errors (it reports `boltz_enabled: false`).
//! - READ payloads come from the FROZEN kernel builders
//!   (`revops_boltz::rpc`) over the QUERY transport; a transport failure
//!   surfaces typed (with its redacted label), never as empty success.
//! - The mnemonic reaches exactly one egress call site
//!   (`MnemonicSecret::into_rpc_value`), gated behind the capability.

use std::sync::Arc;

use revops_boltz::cli::{run_json, BoltzCli};
use revops_boltz::parsing::extract_swap_list;
use serde_json::{json, Value};

use crate::boltz_owner::{BoltzAction, BoltzOwnerHandle, BoltzRefusal};

const UNINITIALIZED: &str = "Boltz CLI integration not initialized";
const READ_TIMEOUT_SECS: u64 = 60;

/// Everything the handlers read. `owner: None` is "the module was never
/// constructed"; an owner whose capability is unassembled lands on the
/// same arm for anything fund-moving.
pub struct BoltzRpcDeps {
    pub owner: Option<BoltzOwnerHandle>,
    pub query: Arc<dyn BoltzCli + Send + Sync>,
    pub now: i64,
}

fn uninitialized() -> Value {
    json!({ "error": UNINITIALIZED })
}

fn py_int(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f.trunc() as i64)),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn py_str(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        _ => None,
    }
}

/// The owner must exist AND hold an assembled capability before any
/// fund-moving path runs.
async fn armed_owner(deps: &BoltzRpcDeps) -> Option<&BoltzOwnerHandle> {
    let owner = deps.owner.as_ref()?;
    let debug = owner.debug().await?;
    if debug["capability_assembled"] == json!(true) {
        Some(owner)
    } else {
        None
    }
}

/// Read paths need the owner (module constructed) but not the
/// capability.
fn read_owner(deps: &BoltzRpcDeps) -> Option<&BoltzOwnerHandle> {
    deps.owner.as_ref()
}

fn refusal_json(refusal: &BoltzRefusal) -> Value {
    match refusal {
        BoltzRefusal::CapabilityNotAssembled => uninitialized(),
        other => json!({
            "status": "error",
            "code": other.code(),
            "error": format!("{other:?}"),
        }),
    }
}

async fn submit(deps: &BoltzRpcDeps, action: BoltzAction) -> Value {
    let Some(owner) = armed_owner(deps).await else {
        return uninitialized();
    };
    match owner.execute(action).await {
        Err(refusal) => refusal_json(&refusal),
        Ok(result) => json!({
            "status": "submitted",
            "request_id": result.request_id,
            "outcome": format!("{:?}", result.outcome),
        }),
    }
}

/// Read the live swap list through the QUERY transport.
fn live_swaps(deps: &BoltzRpcDeps) -> Result<Vec<serde_json::Map<String, Value>>, Value> {
    match run_json(
        deps.query.as_ref(),
        &["listswaps", "--json"],
        READ_TIMEOUT_SECS,
    ) {
        Ok(value) => Ok(extract_swap_list(&value)),
        Err(e) => Err(json!({ "error": e.to_string() })),
    }
}

// -- 1. quote ---------------------------------------------------------------

pub async fn handle_quote(
    deps: &BoltzRpcDeps,
    amount_sats: Option<&Value>,
    swap_type: Option<&Value>,
    currency: Option<&Value>,
) -> Value {
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    let Some(amount) = py_int(amount_sats) else {
        return json!({"error": "amount_sats must be an integer"});
    };
    let swap_type = py_str(swap_type).unwrap_or_else(|| "reverse".to_string());
    let currency_label = revops_boltz::argv::normalize_currency(py_str(currency).as_deref(), "BTC");
    // py echoes the caller's label verbatim while classifying it for
    // argv purposes.
    let classified = match revops_boltz::argv::classify_swap_type(&swap_type) {
        Ok(classified) => classified,
        Err(e) => return json!({"error": format!("{e:?}")}),
    };
    let argv = match revops_boltz::argv::quote_argv(classified, amount, py_str(currency).as_deref())
    {
        Ok(argv) => argv,
        Err(e) => return json!({"error": format!("{e:?}")}),
    };
    let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    match run_json(deps.query.as_ref(), &argv_refs, READ_TIMEOUT_SECS) {
        Ok(quote) => {
            // py: the routing estimate is reverse-only.
            let routing = if matches!(classified, revops_boltz::argv::SwapType::Reverse) {
                revops_boltz::fee::estimate_reverse_routing_fee_sats(amount, 0)
            } else {
                0
            };
            revops_boltz::rpc::build_quote_response(
                &swap_type,
                amount,
                &currency_label,
                &quote,
                routing,
            )
        }
        Err(e) => json!({ "error": e.to_string() }),
    }
}

// -- 2/3. loop-out / loop-in ------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub async fn handle_loop_out(
    deps: &BoltzRpcDeps,
    amount_sats: Option<&Value>,
    address: Option<&Value>,
    channel_id: Option<&Value>,
    _peer_id: Option<&Value>,
    currency: Option<&Value>,
    routing_fee_limit_ppm: Option<&Value>,
) -> Value {
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    let Some(amount) = py_int(amount_sats) else {
        return json!({"error": "amount_sats must be an integer"});
    };
    let ppm = py_int(routing_fee_limit_ppm).unwrap_or(0);
    let channel = py_str(channel_id);
    submit(
        deps,
        BoltzAction::LoopOut {
            amount_sats: amount,
            currency: revops_boltz::argv::normalize_currency(py_str(currency).as_deref(), "BTC"),
            address: py_str(address),
            wallet_name: None,
            chan_ids: channel.clone().into_iter().collect(),
            routing_fee_limit_ppm: ppm,
            channel_id: channel,
            estimated_fee_sats: revops_boltz::fee::estimate_reverse_routing_fee_sats(amount, ppm),
            structural: false,
        },
    )
    .await
}

pub async fn handle_loop_in(
    deps: &BoltzRpcDeps,
    amount_sats: Option<&Value>,
    channel_id: Option<&Value>,
    _peer_id: Option<&Value>,
    currency: Option<&Value>,
) -> Value {
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    let Some(amount) = py_int(amount_sats) else {
        return json!({"error": "amount_sats must be an integer"});
    };
    submit(
        deps,
        BoltzAction::LoopIn {
            wallet_name: "wallet".to_string(),
            currency: py_str(currency),
            amount_sats: amount,
            channel_id: py_str(channel_id),
            estimated_fee_sats: 0,
        },
    )
    .await
}

// -- 4. status (usage short-circuit FIRST) ----------------------------------

pub async fn handle_status(deps: &BoltzRpcDeps, swap_id: Option<&Value>) -> Value {
    let Some(swap_id) = py_str(swap_id) else {
        return crate::rpc_boltz_status::build_boltz_status_usage_error();
    };
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    let argv = revops_boltz::argv::swap_info_argv(&swap_id);
    let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let raw = match deps.query.run(&argv_refs, READ_TIMEOUT_SECS) {
        Ok(raw) => raw,
        Err(e) => return json!({ "error": e.to_string() }),
    };
    let swapinfo_entry = serde_json::from_str::<Value>(&raw)
        .ok()
        .and_then(|v| v.as_object().cloned());
    revops_boltz::rpc::build_status_response(
        &swap_id,
        &raw,
        swapinfo_entry.as_ref(),
        None,
        false,
        None,
        None,
    )
}

// -- 5. history -------------------------------------------------------------

pub async fn handle_history(deps: &BoltzRpcDeps, limit: Option<&Value>) -> Value {
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    let swaps = match live_swaps(deps) {
        Ok(swaps) => swaps,
        Err(e) => return e,
    };
    let limit = py_int(limit).map(|l| l.max(0) as usize);
    revops_boltz::rpc::build_swap_history_response(&swaps, limit, |s| {
        s.get("createdAt").and_then(Value::as_i64).unwrap_or(0)
    })
}

// -- 6. external-pay-ignores ------------------------------------------------

pub async fn handle_external_pay_ignores(deps: &BoltzRpcDeps) -> Value {
    let Some(owner) = read_owner(deps) else {
        return uninitialized();
    };
    let _ = owner;
    // Ignores live in the Rust-owned store; the owner surface for
    // mutating them lands with the operator RPCs (Task 66's ignore/
    // unignore family). Reading is safe here.
    json!({"ignored_external_swaps": [], "count": 0,
           "note": "operator ignore mutations land with Task 66's ignore/unignore family"})
}

// -- 7/8. budget / wallet ---------------------------------------------------

pub async fn handle_budget(deps: &BoltzRpcDeps) -> Value {
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    let swaps = match live_swaps(deps) {
        Ok(swaps) => swaps,
        Err(e) => return e,
    };
    json!({
        "swap_count": swaps.len(),
        "note": "full budget aggregation needs the shared spend/capex evidence \
                 (Task 66/67 owners)",
    })
}

pub async fn handle_wallet(deps: &BoltzRpcDeps) -> Value {
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    let argv = revops_boltz::argv::wallet_list_argv();
    let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    match run_json(deps.query.as_ref(), &argv_refs, READ_TIMEOUT_SECS) {
        Ok(wallets) => json!({ "wallets": wallets }),
        Err(e) => json!({ "error": e.to_string() }),
    }
}

// -- 9/10. refund / claim (usage short-circuits FIRST) ----------------------

pub async fn handle_refund(
    deps: &BoltzRpcDeps,
    swap_id: Option<&Value>,
    destination: Option<&Value>,
) -> Value {
    let Some(swap_id) = py_str(swap_id) else {
        return json!({"error": "usage: revenue-boltz-refund swap_id [destination]"});
    };
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    submit(
        deps,
        BoltzAction::Refund {
            swap_id,
            destination: py_str(destination),
        },
    )
    .await
}

pub async fn handle_claim(
    deps: &BoltzRpcDeps,
    swap_ids: Option<&Value>,
    destination: Option<&Value>,
) -> Value {
    let ids: Vec<String> = match swap_ids {
        Some(Value::String(s)) if !s.trim().is_empty() => s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
        Some(Value::Array(items)) => items.iter().filter_map(|v| py_str(Some(v))).collect(),
        _ => Vec::new(),
    };
    if ids.is_empty() {
        return json!({"error": "usage: revenue-boltz-claim swap_ids [destination]"});
    }
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    submit(
        deps,
        BoltzAction::Claim {
            swap_ids: ids,
            destination: py_str(destination),
        },
    )
    .await
}

// -- 11/12/13. chainswap / withdraw / deposit -------------------------------

pub async fn handle_chainswap(
    deps: &BoltzRpcDeps,
    amount_sats: Option<&Value>,
    from_currency: Option<&Value>,
    to_currency: Option<&Value>,
    to_address: Option<&Value>,
    to_wallet_name: Option<&Value>,
) -> Value {
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    let Some(amount) = py_int(amount_sats) else {
        return json!({"error": "amount_sats must be an integer"});
    };
    submit(
        deps,
        BoltzAction::ChainSwap {
            amount_sats: amount,
            from_currency: revops_boltz::argv::normalize_currency(
                py_str(from_currency).as_deref(),
                "BTC",
            ),
            to_currency: revops_boltz::argv::normalize_currency(
                py_str(to_currency).as_deref(),
                "LBTC",
            ),
            from_wallet_name: "wallet".to_string(),
            to_address: py_str(to_address),
            to_wallet_name: py_str(to_wallet_name),
            estimated_fee_sats: 0,
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_withdraw(
    deps: &BoltzRpcDeps,
    wallet_name: Option<&Value>,
    destination: Option<&Value>,
    currency: Option<&Value>,
    amount_sats: Option<&Value>,
    sat_per_vbyte: Option<&Value>,
    sweep: bool,
    confirm_sweep: bool,
) -> Value {
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    let (Some(wallet), Some(dest)) = (py_str(wallet_name), py_str(destination)) else {
        return json!({"error": "usage: revenue-boltz-withdraw wallet_name destination \
                                [amount_sats] [currency]"});
    };
    submit(
        deps,
        BoltzAction::Withdraw {
            wallet_name: wallet,
            destination: dest,
            currency: py_str(currency),
            amount_sats: py_int(amount_sats).unwrap_or(0),
            sat_per_vbyte: py_int(sat_per_vbyte),
            sweep,
            confirm_sweep,
        },
    )
    .await
}

pub async fn handle_deposit(deps: &BoltzRpcDeps, wallet_name: Option<&Value>) -> Value {
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    let wallet = py_str(wallet_name).unwrap_or_else(|| "wallet".to_string());
    let argv = revops_boltz::argv::wallet_receive_argv(&wallet);
    let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    match deps.query.run(&argv_refs, READ_TIMEOUT_SECS) {
        Ok(address) => json!({ "wallet": wallet, "address": address.trim() }),
        Err(e) => json!({ "error": e.to_string() }),
    }
}

// -- 14/15. backup / backup-verify (mnemonic) -------------------------------

pub async fn handle_backup(deps: &BoltzRpcDeps, include_mnemonic: bool) -> Value {
    // The mnemonic verb travels ONLY through the armed capability: the
    // query transport's allowlist refuses `swapmnemonic` outright, so a
    // pre-cutover plugin cannot read the seed at all.
    let Some(owner) = armed_owner(deps).await else {
        return uninitialized();
    };
    let _ = owner;
    if !include_mnemonic {
        return json!({
            "note": "Swap mnemonic omitted. Pass include_mnemonic=true to include.",
        });
    }
    // Task 69 wires the capability-backed read; until then no assembled
    // capability exists, so this arm is unreachable in production. The
    // single sanctioned egress:
    let secret = crate::boltz_boundaries::MnemonicSecret::new(String::new());
    json!({
        "swap_mnemonic": secret.into_rpc_value(),
        "warning": "Contains plaintext swap mnemonic. Store securely.",
    })
}

pub async fn handle_backup_verify(deps: &BoltzRpcDeps, _candidate: Option<&Value>) -> Value {
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    json!({"matches": false, "note": "capability-backed verification lands with Task 69"})
}

// -- 16..22. balance / auto-cycle / treasury --------------------------------

pub async fn handle_balance_recommendations(deps: &BoltzRpcDeps) -> Value {
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    json!({
        "recommendations": [],
        "count": 0,
        "evidence_gap": "balance recommendation analytics land with Task 67",
    })
}

pub async fn handle_balance_cycle(deps: &BoltzRpcDeps, dry_run: bool) -> Value {
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    let Some(owner) = deps.owner.as_ref() else {
        return uninitialized();
    };
    let mut result = owner.auto_cycle_run_now(true, dry_run).await;
    if let Some(map) = result.as_object_mut() {
        map.insert("cycle".to_string(), json!("balance"));
    }
    result
}

pub async fn handle_auto_cycle_status(deps: &BoltzRpcDeps) -> Value {
    // py: this RPC NEVER errors -- it reports state.
    let debug = match deps.owner.as_ref() {
        Some(owner) => owner.debug().await,
        None => None,
    };
    let enabled = debug
        .as_ref()
        .map(|d| d["capability_assembled"] == json!(true))
        .unwrap_or(false);
    json!({
        "boltz_enabled": enabled,
        "config": {
            "auto_cycle_enabled": debug
                .as_ref()
                .map(|d| d["auto_cycle_enabled"].clone())
                .unwrap_or(json!(false)),
        },
        "owner": debug.unwrap_or(json!(null)),
    })
}

pub async fn handle_auto_cycle_run_now(deps: &BoltzRpcDeps, force: bool, dry_run: bool) -> Value {
    // py: the disabled shape, NOT the error arm.
    let Some(owner) = deps.owner.as_ref() else {
        return json!({
            "status": "disabled",
            "reason": "boltz integration disabled",
            "trigger": "manual",
        });
    };
    owner.auto_cycle_run_now(force, dry_run).await
}

pub async fn handle_expansion_treasury_status(deps: &BoltzRpcDeps) -> Value {
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    json!({
        "status": "unavailable",
        "evidence_gap": "treasury target/on-chain balance analytics land with Task 67",
    })
}

pub async fn handle_expansion_treasury_recommendations(deps: &BoltzRpcDeps) -> Value {
    if read_owner(deps).is_none() {
        return uninitialized();
    }
    json!({
        "recommendations": [],
        "count": 0,
        "evidence_gap": "treasury analytics land with Task 67",
    })
}

pub async fn handle_expansion_treasury_cycle(deps: &BoltzRpcDeps, dry_run: bool) -> Value {
    if armed_owner(deps).await.is_none() {
        return uninitialized();
    }
    let Some(owner) = deps.owner.as_ref() else {
        return uninitialized();
    };
    let mut result = owner.auto_cycle_run_now(true, dry_run).await;
    if let Some(map) = result.as_object_mut() {
        map.insert("cycle".to_string(), json!("expansion_treasury"));
    }
    result
}
