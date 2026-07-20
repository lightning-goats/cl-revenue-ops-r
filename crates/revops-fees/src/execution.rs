//! `set_channel_fee` ported as a PURE decision function: clamps, frozen
//! log strings, governor gate. No `setchannel` broadcast in this crate —
//! the side-effecting call is added at fee cutover, behind the
//! per-subsystem flag.
//!
//! Mirrors `set_channel_fee` (py 7627-7923) execution-layer clamping and
//! `_fee_governor_enabled`/`_governed_authorize_fee_broadcast`
//! (py 7521-7626). The governed gate REUSES `revops_econ`'s
//! `GovernorFacade` — no re-implementation (Phase 2 contract).

use std::collections::BTreeMap;

use revops_analytics::policy::{FeeStrategy, PeerPolicy};
use revops_econ::arbiter::ActiveIntentRegistry;
use revops_econ::governor::{authority_allows, GovernorFacade};
use revops_econ::intents::{make_intent, Explanation, IntentFields};
use revops_econ::ledger::EconLedger;
use revops_econ::pyfloat::py_repr;
use revops_econ::types::{EconResult, Micro, Msat, SignedMsat, UnixTime};
use serde_json::{json, Value};

use crate::cycle::FeeCfgSnapshot;
use crate::pyjson::OValue;
use crate::pyrand::DecisionInputError;

/// `FeeController.ABS_MIN_FEE_PPM` (py 2917).
pub const ABS_MIN_FEE_PPM: i64 = 0;
/// `FeeController.ABS_MAX_FEE_PPM` (py 2918).
pub const ABS_MAX_FEE_PPM: i64 = 100_000;

/// The would-be `setchannel` request (py `set_channel_fee` kwargs subset
/// that reaches the clamp + RPC-params stage).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetFeeRequest {
    pub channel_id: String,
    pub fee_ppm: i64,
    pub enforce_limits: bool,
    /// E-2: class-aware floor from the fee cycle; may only LOWER the
    /// min-fee clamp term (py 7666-7674).
    pub effective_min_fee_ppm: Option<i64>,
    pub htlcmax_msat: Option<i64>,
    pub base_fee_msat: i64,
}

/// Pure outcome of `set_channel_fee`'s decision layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetFeeDecision {
    pub success: bool,
    pub clamped_fee_ppm: i64,
    pub message: String,
    /// The frozen `FEE_LIMIT:` warn line, byte-exact with py 7683-7687,
    /// emitted only when the clamp changed the requested fee.
    pub clamp_log: Option<String>,
}

/// One in-kernel fee execution request. `wire_request` is the exact Python
/// `set_channel_fee` kwargs object captured at the observational boundary.
#[derive(Debug, Clone, PartialEq)]
pub struct FeeExecutionRequest {
    pub decision: SetFeeRequest,
    pub wire_request: OValue,
}

/// Strict execution boundary used by production dry-run and offline replay.
pub trait FeeExecutor {
    fn execute(
        &self,
        request: &FeeExecutionRequest,
        cfg: &FeeCfgSnapshot,
        policy: Option<&PeerPolicy>,
    ) -> Result<SetFeeDecision, DecisionInputError>;
}

/// Production-safe adapter: preserves the existing pure decision semantics.
/// It has no RPC handle and cannot perform a live action.
#[derive(Debug, Default, Clone, Copy)]
pub struct PureFeeExecutor;

impl FeeExecutor for PureFeeExecutor {
    fn execute(
        &self,
        request: &FeeExecutionRequest,
        cfg: &FeeCfgSnapshot,
        policy: Option<&PeerPolicy>,
    ) -> Result<SetFeeDecision, DecisionInputError> {
        Ok(decide_set_channel_fee(&request.decision, cfg, policy))
    }
}

/// `set_channel_fee` (py 7627-7923) as a PURE decision: the ABS clamp
/// `[0, 100000]` ALWAYS applies; the economic clamp `[econ_min,
/// max_fee_ppm]` applies unless `enforce_limits == false`;
/// `effective_min_fee_ppm` may only LOWER the min term (never raise it,
/// never exceed the global `min_fee_ppm`; py 7666-7674). Policy handling
/// (the loop-level semantics, py 4764-4818): PASSIVE skips, STATIC pins
/// the request to `fee_ppm_target` before the clamps.
///
/// The clamp log string is byte-exact with Python (Global Constraints):
/// `FEE_LIMIT: Clamped fee for {channel_id[:16]}... from {orig} to {fee}
/// (limits: {econ_min}-{max} PPM)` (or the `absolute:` variant when
/// economic limits are bypassed).
pub fn decide_set_channel_fee(
    req: &SetFeeRequest,
    cfg: &FeeCfgSnapshot,
    policy: Option<&PeerPolicy>,
) -> SetFeeDecision {
    let mut requested = req.fee_ppm;

    if let Some(p) = policy {
        match p.strategy {
            FeeStrategy::Passive => {
                // Loop-level PASSIVE skip (py 4769-4771): no broadcast is
                // ever attempted. Message text is Rust-side diagnostics
                // (Python has no counterpart string — the loop just
                // `continue`s), NOT a wire contract.
                return SetFeeDecision {
                    success: false,
                    clamped_fee_ppm: requested,
                    message: "policy_passive: fee change skipped".to_string(),
                    clamp_log: None,
                };
            }
            FeeStrategy::Static => {
                // Loop-level STATIC pin (py 4774-4785): the requested fee
                // is replaced by the policy target before the execution
                // clamps run.
                if let Some(target) = p.fee_ppm_target {
                    requested = target;
                }
            }
            FeeStrategy::Dynamic => {}
        }
    }

    // Python: `fee_ppm = max(ABS_MIN, min(ABS_MAX, int(fee_ppm)))`
    // (py 7660) — the absolute safety clamp always applies.
    let original_fee_ppm = requested;
    let mut fee_ppm = requested.clamp(ABS_MIN_FEE_PPM, ABS_MAX_FEE_PPM);

    // Economic clamp (py 7666-7676).
    let mut econ_min_fee_ppm = cfg.min_fee_ppm;
    if let Some(eff) = req.effective_min_fee_ppm {
        econ_min_fee_ppm = ABS_MIN_FEE_PPM.max(eff.min(cfg.min_fee_ppm));
    }
    if req.enforce_limits {
        fee_ppm = econ_min_fee_ppm.max(cfg.max_fee_ppm.min(fee_ppm));
    }

    let clamp_log = if fee_ppm != original_fee_ppm {
        let clamp_note = if req.enforce_limits {
            format!("(limits: {econ_min_fee_ppm}-{} PPM)", cfg.max_fee_ppm)
        } else {
            format!("(absolute: {ABS_MIN_FEE_PPM}-{ABS_MAX_FEE_PPM} PPM; economic limits bypassed)")
        };
        let cid16: String = req.channel_id.chars().take(16).collect();
        Some(format!(
            "FEE_LIMIT: Clamped fee for {cid16}... from {original_fee_ppm} to {fee_ppm} {clamp_note}"
        ))
    } else {
        None
    };

    SetFeeDecision {
        success: true,
        clamped_fee_ppm: fee_ppm,
        // py 7822: set on the success path after the (dry-run-elided) RPC.
        message: format!("Fee set to {fee_ppm} PPM"),
        clamp_log,
    }
}

/// Injected governor plumbing (py `self.econ_shadow` accessors + config
/// gates). All optional, mirroring Python's None-tolerant lookups.
pub struct GovernedDeps<'a> {
    pub ledger: Option<&'a EconLedger>,
    pub registry: Option<&'a ActiveIntentRegistry>,
    /// `getattr(cfg, "paused", False) is True` (py 7565).
    pub paused: bool,
    /// `getattr(cfg, "authority_level", "capital")` (py 7569).
    pub authority_level: Option<String>,
}

/// Audit record of one governed authorization, journaled with the
/// decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernedTrace {
    pub authorized: bool,
    pub reason_code: String,
    pub intent_id: String,
    pub idempotency_key: String,
}

/// One replayable request to authorize a would-be fee broadcast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeAuthorizationRequest {
    pub channel_id: String,
    pub fee_ppm: i64,
    pub old_fee_ppm: Option<i64>,
    pub reason: String,
    pub reason_code: Option<String>,
    pub now: i64,
}

/// The authorization result consumed by the fee kernel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeAuthorizationResult {
    pub authorized: bool,
    pub reason_code: String,
    pub trace: Option<GovernedTrace>,
}

/// Replayable fee-governor decision boundary.
pub trait FeeAuthorizer {
    fn authorize(
        &self,
        request: &FeeAuthorizationRequest,
    ) -> Result<FeeAuthorizationResult, DecisionInputError>;
}

/// Production adapter that preserves the existing governed facade, ledger,
/// registry, trace, and fail-closed reason-string behavior.
pub struct GovernedFeeAuthorizer<'deps, 'resources> {
    deps: &'deps GovernedDeps<'resources>,
}

impl<'deps, 'resources> GovernedFeeAuthorizer<'deps, 'resources> {
    pub fn new(deps: &'deps GovernedDeps<'resources>) -> Self {
        Self { deps }
    }
}

impl FeeAuthorizer for GovernedFeeAuthorizer<'_, '_> {
    fn authorize(
        &self,
        request: &FeeAuthorizationRequest,
    ) -> Result<FeeAuthorizationResult, DecisionInputError> {
        let (authorized, reason_code, trace) = governed_authorize_fee_broadcast(
            self.deps,
            &request.channel_id,
            request.fee_ppm,
            request.old_fee_ppm,
            &request.reason,
            request.reason_code.as_deref(),
            request.now,
        );
        Ok(FeeAuthorizationResult {
            authorized,
            reason_code,
            trace,
        })
    }
}

/// Convert a float component to the py_repr string form BEFORE it enters
/// an [`Explanation`] — `Explanation::render` panics on float-typed
/// `Value::Number` components (Phase 2 carry-obligation: floats must be
/// routed through the py_repr-aware renderer). Today's `fee_broadcast`
/// explanation carries only ints/strings; any future float component MUST
/// go through this helper.
pub fn py_float_component(x: f64) -> Value {
    Value::String(py_repr(x))
}

/// `_governed_authorize_fee_broadcast` (py 7531-7625): authorize one
/// automated fee broadcast. Zero worst-case cost (reversible policy
/// change) — the facade authorizes without reserving (`max_cost_msat ==
/// 0` means `reserve_spend` is never called and NO `budget_reserved`
/// ledger event is written; Phase 2 contract). Gates: paused /
/// authority("fees") / staleness, plus the `intent_proposed` ledger trail.
///
/// Returns `(authorized, reason_code, trace)`. FAILS CLOSED: any internal
/// `Err` yields `(false, format!("internal_error ({e})"), None)` —
/// mirroring Python's blanket `except Exception as e` (py 7624-7625).
pub fn governed_authorize_fee_broadcast(
    deps: &GovernedDeps<'_>,
    channel_id: &str,
    fee_ppm: i64,
    old_fee_ppm: Option<i64>,
    reason: &str,
    reason_code: Option<&str>,
    now: i64,
) -> (bool, String, Option<GovernedTrace>) {
    match governed_authorize_inner(
        deps,
        channel_id,
        fee_ppm,
        old_fee_ppm,
        reason,
        reason_code,
        now,
    ) {
        Ok((authorized, code, trace)) => (authorized, code, Some(trace)),
        Err(e) => fail_closed(&e),
    }
}

/// The frozen fail-closed wrapper (py 7625): `(False, f"internal_error
/// ({e})")`. Split out so the exact string shape is unit-testable without
/// forcing a real error through the facade.
pub fn fail_closed(e: &revops_econ::types::EconError) -> (bool, String, Option<GovernedTrace>) {
    (false, format!("internal_error ({})", e.msg), None)
}

fn governed_authorize_inner(
    deps: &GovernedDeps<'_>,
    channel_id: &str,
    fee_ppm: i64,
    old_fee_ppm: Option<i64>,
    reason: &str,
    reason_code: Option<&str>,
    now: i64,
) -> EconResult<(bool, String, GovernedTrace)> {
    // py 7571-7594: zero-cost SET_FEE intent, expires now+600, priority
    // 50, budget bucket "fees", origin "fee_controller_governed",
    // reversible.
    let env = make_intent(IntentFields {
        intent_type: "SET_FEE".to_string(),
        snapshot_id: format!("fee-broadcast-{now}"),
        created_at: UnixTime::new(now)?,
        expires_at: UnixTime::new(now + 600)?,
        target: channel_id.to_string(),
        amount_msat: None,
        expected_benefit_msat: SignedMsat(0),
        max_cost_msat: Msat::new(0)?,
        capital_committed_msat: Msat::new(0)?,
        confidence_micro: Micro::new(0)?,
        reason_codes: Vec::new(),
        explanation: Explanation {
            kind: "fee_broadcast".to_string(),
            components: vec![
                // py: ("old_fee_ppm", int(old_fee_ppm or 0)) — ints and
                // strings only; a float here MUST use
                // `py_float_component` (Explanation::render panics on raw
                // floats — Phase 2 carry).
                ("old_fee_ppm".to_string(), json!(old_fee_ppm.unwrap_or(0))),
                ("new_fee_ppm".to_string(), json!(fee_ppm)),
                ("reason".to_string(), json!(reason)),
                (
                    "controller_reason_code".to_string(),
                    json!(reason_code.unwrap_or("")),
                ),
            ],
        },
        preconditions: Vec::new(),
        priority: 50,
        budget_bucket: "fees".to_string(),
        origin_policy: "fee_controller_governed".to_string(),
        reversible: true,
    })?;

    // py 7595-7621: best-effort `intent_proposed` trail with the rendered
    // explanation (floats would already be py_repr strings — see
    // `py_float_component`). Failures are swallowed exactly like Python's
    // inner `except Exception: pass`.
    if let Some(ledger) = deps.ledger {
        let details = json!({
            "target": env.target,
            "governed": true,
            "explanation": env.explanation.render(),
        });
        let _ = ledger.append(
            "intent_proposed",
            env.intent_id.as_str(),
            &env.idempotency_key,
            &env.snapshot_id,
            now,
            &BTreeMap::new(),
            &details,
        );
    }

    // py 7562-7570: facade with reserve_spend stubbed always-true (zero
    // cost: never called), paused/authority gates from the snapshot.
    let reserve = |_rid: &str, _sats: i64, _cat: &str| -> EconResult<bool> { Ok(true) };
    let release = |_rid: &str| -> EconResult<bool> { Ok(true) };
    let paused = deps.paused;
    let is_paused = move || paused;
    let level = deps.authority_level.clone();
    let authority = move || -> EconResult<bool> { Ok(authority_allows(level.as_deref(), "fees")) };
    let facade = GovernorFacade {
        reserve_spend: &reserve,
        release_spend: &release,
        is_paused: &is_paused,
        ledger: deps.ledger,
        registry: deps.registry,
        authority_check: Some(&authority),
    };

    let decision = facade.authorize(&env, now, None)?;
    let trace = GovernedTrace {
        authorized: decision.authorized,
        reason_code: decision.reason_code.clone(),
        intent_id: env.intent_id.as_str().to_string(),
        idempotency_key: env.idempotency_key.clone(),
    };
    Ok((decision.authorized, decision.reason_code, trace))
}
